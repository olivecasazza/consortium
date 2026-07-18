//! Production [`RoundExecutor`] implementations — drive real `nix copy`
//! commands, runnable against actual nixlab hosts.
//!
//! [`NixCopyExecutor`] is the realistic counterpart to
//! `consortium_fanout_sim::DeterministicExecutor`. The sim does
//! `closure_size / bandwidth + latency` math; this one runs
//! `nix copy --no-check-sigs --to ssh-ng://user@host store_path` through an
//! [`Executor`] for every (src, tgt) edge in a round, in parallel via
//! `std::thread`.
//!
//! ## Edge-source handling
//!
//! For an edge `(src, tgt)` with the cascade's seed at `seed_id`:
//! - If `src == seed_id`: run `nix copy ...` LOCALLY (the host running
//!   cascade-copy IS the seed)
//! - Otherwise: SSH into `src` and run `nix copy ...` THERE (the source
//!   forwards the closure it received in a prior round)
//!
//! ## Trust + signing
//!
//! `--no-check-sigs` is passed because closures built locally are NOT
//! signed by a key the remote trusts. Trust boundary is the SSH
//! connection itself (root-over-ssh deploy assumption — same as
//! `consortium_nix::copy::copy_closure_with`).
//!
//! ## Failure mapping
//!
//! `nix copy` exit status maps to:
//! - exit 0 → `Ok(elapsed_duration)`
//! - non-zero with stderr containing "Connection refused"/"Permission
//!   denied" → `CascadeError::SshHandshake` (permanent — no retry from
//!   alt source will help if the tgt itself is unreachable)
//! - non-zero otherwise → `CascadeError::Copy` (transient — retry from
//!   alt source might succeed, e.g. if it was source-side bandwidth)

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use consortium_integration::exec::{CommandSpec, Executor, ProcessExecutor, SshTarget};

use crate::cascade::{CascadeError, CascadeNode, NetworkProfile, NodeId, RoundExecutor};

/// Real-world `RoundExecutor` that drives `nix copy` over SSH.
pub struct NixCopyExecutor {
    /// Command executor every `nix copy` / `ssh` invocation runs through.
    /// `ProcessExecutor` in production, `ScriptedExecutor` in tests.
    pub exec: Arc<dyn Executor>,
    /// NodeId → SSH address (e.g. `"root@hp01"` or `"olive@seir"`).
    /// Seed node also has an entry here for symmetry, even though
    /// edges originating from it run locally.
    pub addrs: HashMap<NodeId, String>,
    /// The store path being distributed, e.g. `/nix/store/xxx-foo-1.0`.
    pub store_path: String,
    /// NodeId of the seed — edges originating from it run via local
    /// `nix copy`; all other src edges run via `ssh <src> 'nix copy …'`.
    pub seed: NodeId,
    /// Per-edge command timeout. Cascade halts the edge if the SSH
    /// or `nix copy` hasn't returned by this point — typically the
    /// remote is unreachable. Default 5 minutes.
    pub timeout: Duration,
}

impl NixCopyExecutor {
    /// An executor that runs every edge through `exec`.
    pub fn new(
        exec: Arc<dyn Executor>,
        addrs: HashMap<NodeId, String>,
        store_path: impl Into<String>,
        seed: NodeId,
    ) -> Self {
        Self {
            exec,
            addrs,
            store_path: store_path.into(),
            seed,
            timeout: Duration::from_secs(300),
        }
    }

    /// An executor that spawns real subprocesses via [`ProcessExecutor`].
    pub fn process(
        addrs: HashMap<NodeId, String>,
        store_path: impl Into<String>,
        seed: NodeId,
    ) -> Self {
        Self::new(Arc::new(ProcessExecutor::new()), addrs, store_path, seed)
    }

    pub fn with_timeout(mut self, t: Duration) -> Self {
        self.timeout = t;
        self
    }

    /// Run a single edge: src copies the closure to tgt. Blocks until
    /// the command completes or errors out.
    ///
    /// Returns `Ok(elapsed)` on success, `Err(CascadeError)` otherwise.
    fn run_edge(&self, src: NodeId, tgt: NodeId) -> Result<Duration, CascadeError> {
        let Some(tgt_addr) = self.addrs.get(&tgt) else {
            return Err(CascadeError::Copy {
                node: tgt,
                stderr: format!("no SSH address registered for tgt {tgt}"),
            });
        };
        let store_uri = format!("ssh-ng://{tgt_addr}");
        let copy_args = [
            "copy",
            "--no-check-sigs",
            "--to",
            store_uri.as_str(),
            self.store_path.as_str(),
        ];

        let started = Instant::now();
        let spec = if src == self.seed {
            // Local nix copy from the seed.
            CommandSpec::new("nix").args(copy_args)
        } else {
            // SSH into src and have it run nix copy.
            let Some(src_addr) = self.addrs.get(&src) else {
                return Err(CascadeError::Copy {
                    node: tgt,
                    stderr: format!("no SSH address registered for src {src}"),
                });
            };
            let (user, host) = split_ssh_addr(src_addr);
            // accept-new overrides the DEFAULT_SSH_OPTS StrictHostKeyChecking=no
            // (extra opts come after the defaults).
            CommandSpec::new("nix").args(copy_args).ssh(
                SshTarget::new(user, host).extra_opt("-oStrictHostKeyChecking=accept-new"),
            )
        };

        let result = self.exec.exec(&spec);
        let elapsed = started.elapsed();
        match result {
            Ok(output) if output.success() => Ok(elapsed),
            Ok(output) => Err(classify_copy_error(tgt, src, &output.stderr)),
            Err(exec_err) => Err(CascadeError::Copy {
                node: tgt,
                stderr: format!("nix copy exec failed: {exec_err}"),
            }),
        }
    }
}

impl RoundExecutor for NixCopyExecutor {
    fn dispatch(
        &self,
        _nodes: &[CascadeNode],
        edges: &[(NodeId, NodeId)],
        _net: &NetworkProfile,
    ) -> HashMap<(NodeId, NodeId), Result<Duration, CascadeError>> {
        // Spawn one thread per edge; collect via channel so we don't
        // need to box the closures or hold thread handles.
        let (tx, rx) = mpsc::channel();
        let n = edges.len();
        thread::scope(|scope| {
            for &(src, tgt) in edges {
                let tx = tx.clone();
                let me = &*self;
                scope.spawn(move || {
                    let outcome = me.run_edge(src, tgt);
                    let _ = tx.send(((src, tgt), outcome));
                });
            }
        });
        drop(tx); // close the channel so the rx loop terminates
        let mut out = HashMap::with_capacity(n);
        while let Ok(item) = rx.try_recv() {
            out.insert(item.0, item.1);
        }
        out
    }
}

/// Map a non-zero `nix copy` stderr to the right `CascadeError` variant.
/// Permanent vs transient distinction matters for orphan re-routing
/// (see cascade.rs's `is_transient()` discussion).
fn classify_copy_error(tgt: NodeId, src: NodeId, stderr: &str) -> CascadeError {
    let lower = stderr.to_lowercase();
    if lower.contains("connection refused")
        || lower.contains("connection timed out")
        || lower.contains("no route to host")
        || lower.contains("permission denied")
        || lower.contains("host key verification failed")
    {
        // Target host is permanently unreachable — orphan re-routing
        // should kick in for any descendants in level-tree.
        CascadeError::SshHandshake {
            node: tgt,
            parent: src,
        }
    } else {
        // Default to transient — could be source-side bandwidth, a
        // flaky relay, or a substituter being slow. Retry from an
        // alternate source on the next round may succeed.
        CascadeError::Copy {
            node: tgt,
            stderr: stderr.lines().take(5).collect::<Vec<_>>().join("\n"),
        }
    }
}

/// Split a `user@host` SSH address into its parts.
///
/// Fleet addresses are always `user@host`; a bare host falls back to
/// `root` so hand-rolled inventories keep working.
fn split_ssh_addr(addr: &str) -> (String, String) {
    match addr.split_once('@') {
        Some((user, host)) => (user.to_string(), host.to_string()),
        None => ("root".to_string(), addr.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    #[test]
    fn split_ssh_addr_user_at_host() {
        assert_eq!(
            split_ssh_addr("root@hp01"),
            ("root".to_string(), "hp01".to_string())
        );
        assert_eq!(
            split_ssh_addr("olive@192.168.1.121"),
            ("olive".to_string(), "192.168.1.121".to_string())
        );
    }

    #[test]
    fn split_ssh_addr_bare_host_defaults_to_root() {
        assert_eq!(
            split_ssh_addr("hp01"),
            ("root".to_string(), "hp01".to_string())
        );
    }

    #[test]
    fn classify_known_permanent_errors() {
        let perm_cases = [
            "ssh: connect to host hp01 port 22: Connection refused",
            "ssh: connect to host hp01 port 22: Connection timed out",
            "Permission denied (publickey).",
            "Host key verification failed.",
            "ssh: connect to host hp01 port 22: No route to host",
        ];
        for stderr in perm_cases {
            let err = classify_copy_error(NodeId(1), NodeId(0), stderr);
            assert!(
                matches!(err, CascadeError::SshHandshake { .. }),
                "expected SshHandshake for stderr={stderr:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn classify_transient_errors_default_to_copy() {
        let cases = [
            "error: writing to file: No space left on device",
            "warning: substituter 'https://cache.nixos.org' returned HTTP 503",
            "some random stderr nobody categorized",
        ];
        for stderr in cases {
            let err = classify_copy_error(NodeId(1), NodeId(0), stderr);
            assert!(
                matches!(err, CascadeError::Copy { .. }),
                "expected Copy (transient) for stderr={stderr:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn dispatch_runs_edges_through_injected_executor() {
        let scripted = Arc::new(ScriptedExecutor::new().on("", ExecOutput::ok("")));
        let mut addrs = HashMap::new();
        addrs.insert(NodeId(0), "root@seed".to_string());
        addrs.insert(NodeId(1), "root@n1".to_string());
        addrs.insert(NodeId(2), "root@n2".to_string());
        let exec = NixCopyExecutor::new(scripted.clone(), addrs, "/nix/store/abc", NodeId(0));
        let net = NetworkProfile::default();

        // seed → n1 runs locally; n1 → n2 is relayed over ssh via n1.
        let out = exec.dispatch(
            &[],
            &[(NodeId(0), NodeId(1)), (NodeId(1), NodeId(2))],
            &net,
        );
        assert!(out[&(NodeId(0), NodeId(1))].is_ok());
        assert!(out[&(NodeId(1), NodeId(2))].is_ok());

        // The seed edge is a bare local nix copy.
        scripted.assert_invoked_containing(
            "nix copy --no-check-sigs --to ssh-ng://root@n1 /nix/store/abc",
        );
        // The relayed edge is ssh-wrapped: ssh … -l root … n1 'nix' 'copy' …
        let invocations = scripted.invocations();
        let relay = invocations
            .iter()
            .find(|c| c.starts_with("ssh ") && c.contains("ssh-ng://root@n2"))
            .expect("relayed edge should be ssh-wrapped");
        assert!(relay.contains("-l root"), "{}", relay);
        assert!(relay.contains("accept-new n1 "), "{}", relay);
    }

    #[test]
    fn dispatch_maps_exec_error_to_copy_failure() {
        let scripted = Arc::new(ScriptedExecutor::new().on_error("nix copy", "spawn blew up"));
        let mut addrs = HashMap::new();
        addrs.insert(NodeId(0), "root@seed".to_string());
        addrs.insert(NodeId(1), "root@n1".to_string());
        let exec = NixCopyExecutor::new(scripted, addrs, "/nix/store/abc", NodeId(0));
        let net = NetworkProfile::default();

        let out = exec.dispatch(&[], &[(NodeId(0), NodeId(1))], &net);
        match &out[&(NodeId(0), NodeId(1))] {
            Err(CascadeError::Copy { node, stderr }) => {
                assert_eq!(*node, NodeId(1));
                assert!(stderr.contains("spawn blew up"), "{}", stderr);
            }
            other => panic!("expected transient Copy error, got: {:?}", other),
        }
    }
}
