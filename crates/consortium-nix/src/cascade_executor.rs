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
//! Source addresses may carry a port (`root@10.0.2.2:22201`); it is parsed
//! and forwarded to the ssh invocation. This is what lets a fleet of guests
//! behind per-node host forwards cascade to each other. The destination URI
//! (`ssh-ng://user@host:port`) carries the port natively, so targets need no
//! special handling.
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
    /// NodeId → SSH address. Accepts `user`, `user@host`, `user@host:port`,
    /// and bracketed IPv6 with an optional port (`root@[2001:db8::1]:22201`);
    /// a bare host defaults to `root`. The port, when present, is passed to
    /// the ssh invocation via [`consortium_integration::exec::SshTarget::port`].
    /// Seed node also has an entry here for symmetry, even though
    /// edges originating from it run locally.
    pub addrs: HashMap<NodeId, String>,
    /// The store path being distributed, e.g. `/nix/store/xxx-foo-1.0`.
    pub store_path: String,
    /// NodeId of the seed — edges originating from it run via local
    /// `nix copy`; all other src edges run via `ssh <src> 'nix copy …'`.
    pub seed: NodeId,
    /// Intended per-edge command timeout, default 5 minutes.
    ///
    /// NOT CURRENTLY APPLIED. [`crate::cascade::run_cascade`] has no edge
    /// deadline, [`consortium_integration::exec::CommandSpec`] carries no
    /// timeout, and `ProcessExecutor` blocks on `wait_with_output()`, so a
    /// hung `nix copy` over ssh blocks its dispatch thread indefinitely.
    /// Enforcing this would mean adding a duration to the `Executor` trait
    /// and every implementation — a workspace-wide published-API change.
    /// Until then the CALLER must bound execution (the fanout benchmark
    /// does so in `nix/fanout-vms/bench.py`). Kept, rather than deleted,
    /// because it is a public field of a published crate.
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
            let (user, host, port) = split_ssh_addr(src_addr)
                .map_err(|stderr| CascadeError::Copy { node: tgt, stderr })?;
            let mut ssh_target = SshTarget::new(user, host);
            if let Some(port) = port {
                ssh_target = ssh_target.port(port);
            }
            // accept-new overrides the DEFAULT_SSH_OPTS StrictHostKeyChecking=no
            // (extra opts come after the defaults).
            CommandSpec::new("nix")
                .args(copy_args)
                .ssh(ssh_target.extra_opt("-oStrictHostKeyChecking=accept-new"))
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
                let me = self;
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

/// Split a `user@host[:port]` SSH address into its parts.
///
/// A bare host defaults to `root`. Brackets disambiguate an IPv6 host
/// with a port; unbracketed IPv6 remains a host without a port.
fn split_ssh_addr(addr: &str) -> Result<(&str, &str, Option<u16>), String> {
    let (user, host) = addr.split_once('@').unwrap_or(("root", addr));
    let (host, port) = if let Some(bracketed) = host.strip_prefix('[') {
        let (host, suffix) = bracketed
            .split_once(']')
            .ok_or_else(|| format!("invalid SSH address {addr:?}: missing closing bracket"))?;
        let port = if suffix.is_empty() {
            None
        } else {
            Some(suffix.strip_prefix(':').ok_or_else(|| {
                format!("invalid SSH address {addr:?}: expected port after closing bracket")
            })?)
        };
        (host, port)
    } else if host.bytes().filter(|&byte| byte == b':').count() == 1 {
        let (host, port) = host.split_once(':').expect("one colon is present");
        (host, Some(port))
    } else {
        (host, None)
    };
    let port = port
        .map(|value| {
            value
                .parse::<u16>()
                .ok()
                .filter(|&port| port != 0 && value.bytes().all(|byte| byte.is_ascii_digit()))
                .ok_or_else(|| format!("invalid SSH port in address {addr:?}"))
        })
        .transpose()?;
    Ok((user, host, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    #[test]
    fn split_ssh_addr_user_at_host() {
        assert_eq!(split_ssh_addr("root@hp01"), Ok(("root", "hp01", None)));
        assert_eq!(
            split_ssh_addr("olive@192.168.1.121"),
            Ok(("olive", "192.168.1.121", None))
        );
    }

    #[test]
    fn split_ssh_addr_bare_host_defaults_to_root() {
        assert_eq!(split_ssh_addr("hp01"), Ok(("root", "hp01", None)));
    }

    #[test]
    fn split_ssh_addr_preserves_ipv6_hosts() {
        assert_eq!(
            split_ssh_addr("root@2001:db8::1"),
            Ok(("root", "2001:db8::1", None))
        );
        assert_eq!(
            split_ssh_addr("root@[2001:db8::1]:22201"),
            Ok(("root", "2001:db8::1", Some(22201)))
        );
    }

    #[test]
    fn split_ssh_addr_rejects_invalid_port_ranges() {
        for addr in ["root@host:0", "root@host:65536", "root@host:"] {
            assert!(split_ssh_addr(addr).is_err(), "{addr}");
        }
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
        let out = exec.dispatch(&[], &[(NodeId(0), NodeId(1)), (NodeId(1), NodeId(2))], &net);
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
        assert!(!relay.contains(" -p "), "{}", relay);
        assert!(relay.contains("accept-new n1 "), "{}", relay);
    }

    #[test]
    fn dispatch_relays_via_explicit_source_ssh_port() {
        let scripted = Arc::new(ScriptedExecutor::new().on("", ExecOutput::ok("")));
        let addrs = HashMap::from([
            (NodeId(1), "root@10.0.2.2:22201".to_string()),
            (NodeId(2), "root@10.0.2.2:22202".to_string()),
        ]);
        let exec = NixCopyExecutor::new(scripted.clone(), addrs, "/nix/store/abc", NodeId(0));

        let out = exec.dispatch(&[], &[(NodeId(1), NodeId(2))], &NetworkProfile::default());

        assert!(out[&(NodeId(1), NodeId(2))].is_ok());
        let commands = scripted.invocations();
        assert_eq!(commands.len(), 1);
        assert!(commands[0].contains("-l root -p 22201 "), "{}", commands[0]);
        assert!(
            commands[0].contains("accept-new 10.0.2.2 "),
            "{}",
            commands[0]
        );
        assert!(
            commands[0].contains("ssh-ng://root@10.0.2.2:22202"),
            "{}",
            commands[0]
        );
    }

    #[test]
    fn dispatch_rejects_invalid_source_ssh_port_before_invoking_ssh() {
        let scripted = Arc::new(ScriptedExecutor::new());
        let addrs = HashMap::from([
            (NodeId(1), "root@10.0.2.2:invalid".to_string()),
            (NodeId(2), "root@10.0.2.2:22202".to_string()),
        ]);
        let exec = NixCopyExecutor::new(scripted.clone(), addrs, "/nix/store/abc", NodeId(0));

        let out = exec.dispatch(&[], &[(NodeId(1), NodeId(2))], &NetworkProfile::default());

        match &out[&(NodeId(1), NodeId(2))] {
            Err(CascadeError::Copy { node, stderr }) => {
                assert_eq!(*node, NodeId(2));
                assert!(stderr.contains("invalid SSH port"), "{stderr}");
            }
            other => panic!("expected invalid port error, got {other:?}"),
        }
        assert!(scripted.invocations().is_empty());
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
