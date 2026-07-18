//! `SimExecutor` — a network/failure-aware
//! [`Executor`] implementation that
//! runs integration pipelines (`deploy`, `submit_job`, `run_playbook`,
//! `launch_task`, …) under simulated network and failure conditions,
//! reproducibly from a seed.
//!
//! It is the bridge between the executor abstraction
//! ([`consortium_integration::exec`]) and the sim harness
//! ([`NetworkProfile`], [`FailureSchedule`]): every command an integration
//! issues through `Arc<dyn Executor>` is classified onto a simulated edge
//! (or not), judged against the failure schedule, timed against the
//! network profile, and answered from scripted success outputs — without
//! real networking, ssh, or sleeping.
//!
//! ## Command classification (precedence)
//!
//! Commands are classified from the *structured*
//! [`CommandSpec`], not by
//! parsing the rendered line wherever structure exists:
//!
//! 1. **`spec.ssh = Some(target)` → control edge.** A remote command
//!    targeting `target.host` maps to the edge *sentinel → node(host)*.
//!    This rule wins even when the args contain an `ssh-ng://` store URI:
//!    nix cascade relay edges render as ssh-wrapped `nix copy`, and the
//!    copy is attributed to the ssh login host (the relay source) — that
//!    is the host a kill/partition schedule must strike to interrupt the
//!    relay.
//! 2. **`--to ssh-ng://<user>@<host>` / `--from ssh-ng://…` args → data
//!    edge.** The staging `nix copy` shape. `--to` absent → target is the
//!    sentinel; `--from` absent → source is the sentinel. Only `ssh-ng://`
//!    endpoints form edges; a host that is not part of the fleet renders
//!    a resolution failure instead of an edge.
//! 3. **Anything else → local.** `nix build`, `nix eval`, `sky`, `ray`,
//!    and any other command with neither `spec.ssh` nor an `ssh-ng://`
//!    endpoint forms no edge. The failure schedule does not apply: local
//!    work is not network-bound, and the sim models the network, not the
//!    build host's reliability. Output comes from the scripted rules
//!    alone.
//!
//! ## Fleet topology
//!
//! `.hosts([..])` assigns `NodeId(0..N)` in order; the *build-host
//! sentinel* — where local commands and staging copies originate — gets
//! `NodeId(N)`. Bandwidth/uplink/latency distributions are sampled from
//! the seed (via [`rng_from_seed`]) over
//! all `N + 1` nodes, so sentinel↔host edges are populated too.
//! `.network(profile)` substitutes a hand-built profile (the caller is
//! then responsible for including sentinel edges) and takes precedence
//! over the distribution knobs.
//!
//! ## Order-independent failure decisions
//!
//! [`DeterministicExecutor`](crate::executor::DeterministicExecutor) is
//! round-keyed: the cascade coordinator dispatches edges round by round,
//! so a monotonically increasing round number keys
//! [`FailureSchedule::failure_for`]. Integration pipelines instead run DAG
//! tasks on *concurrent worker threads*, so the global invocation order
//! varies run to run. `SimExecutor` therefore keys failure decisions per
//! **(edge, per-edge attempt index)**: the r-th invocation on edge
//! `(src, tgt)` — assigned under a lock, so it is deterministic per edge —
//! is judged by `failure_for(r, src, tgt)`. `KillNodeAtRound { node,
//! round }` then deterministically means "attempts ≥ round targeting node
//! fail" regardless of thread interleaving, matching the schedule's
//! `round >= r` semantics per edge. Static partitions
//! (`net.partitions`) are consulted for every edge, after the schedule.
//!
//! ## Failure rendering (never panics, never `Err`)
//!
//! A schedule/partition hit renders as ordinary command-failure output —
//! integrations already handle non-zero exits:
//!
//! - ssh control edge → `Ok(ExecOutput { status: 255, stderr: "ssh:
//!   connect to host <h> port <p>: <reason>", .. })` (ssh convention).
//! - data copy edge → `Ok(ExecOutput { status: 1, stderr: <message naming
//!   the error kind: activation / partition / copy / …>, .. })`.
//! - host not in the fleet → 255 (ssh) / 1 (copy) with `ssh: Could not
//!   resolve hostname …` — a loud failure, so fleet typos surface.
//!
//! ## Success outputs
//!
//! Successful commands (and all local commands) get their output from an
//! embedded [`ScriptedExecutor`]
//! fed the same rendered line — `.on()`, `.on_error()`, `.rule()` on the
//! builder forward to it. Its
//! [`Unexpected`](consortium_integration::exec::ExecError::Unexpected)
//! error for unscripted commands is *correct* behavior and surfaces
//! verbatim: script every success output your pipeline parses. The
//! embedded executor's own invocation recording is discarded — assertions
//! use [`SimExecutor::invocation_log`].
//!
//! ## Virtual clock
//!
//! [`SimExecutor::simulated_transfer_time`] sums the durations of
//! successful data copies: `transfer_bytes / effective_bandwidth(src,
//! tgt, 1, 1, default) + latency`. This is **aggregate transfer work, not
//! concurrent wall-clock** — the harness does not model wall time. The
//! contention args are pinned to `1/1`: the executor observes one command
//! at a time and cannot know how many transfers are concurrently in
//! flight, so each transfer is modeled uncontended (per-host DAG
//! serialization makes that the honest default). Transfer sizes come from
//! `.transfer_bytes(pattern, bytes)` — first substring match on the
//! rendered line wins; unmatched data copies fall back to
//! `.default_transfer_bytes(..)` (0 by default) and unmatched ssh control
//! commands always use 0 bytes (just latency).
//!
//! ## Determinism contract
//!
//! Same builder config + same pipeline inputs ⇒
//!
//! - identical per-edge outcome sequences ([`per_edge_outcomes`]),
//! - identical [`SimExecutor::invocation_log`] contents as a multiset
//!   ([`logs_equivalent`] / [`canonical_log`]),
//! - identical [`SimExecutor::simulated_transfer_time`],
//!
//! even though worker-thread interleaving changes the log's ORDER.
//! [`assert_deterministic_equivalence`] checks the first two. One caveat:
//! the binding of a command line to an attempt index is deterministic only
//! per edge — two *different* commands sharing one edge issued from
//! unordered (non-dependency-ordered) tasks may swap attempt indices
//! between runs. Integration pipelines are immune: per-host command
//! chains are dependency-ordered. Keep test DAGs that way.
//!
//! ## Writing a sim test for an integration
//!
//! 1. **Build the executor** mirroring the fleet: `.hosts(..)` in the
//!    order the integration addresses them; pin `.seed(..)`,
//!    `.failure_schedule(..)`, `.transfer_bytes(..)`; script every success
//!    output the pipeline parses (sbatch job ids, store paths,
//!    `sky`/`ray` statuses) via `.on(..)` — rules match substrings of the
//!    rendered command line, first match wins.
//! 2. **Wire it in**: wrap in `Arc`, store as `Arc<dyn Executor>` under
//!    the `"executor"` context state key, then call the integration's
//!    entry fn (or drive its DAG directly, as below).
//! 3. **Assert semantics, not order**: the `DagReport` / pipeline result
//!    plus edge/kind-filtered views of the invocation log. Never assert on
//!    log order (worker threads interleave) or wall-clock time (none
//!    passes — the clock is virtual).
//! 4. **Assert determinism**: run the identical scenario twice and call
//!    [`assert_deterministic_equivalence`] on both logs.
//!
//! Complete example — a 2-host fleet, a tiny `stage → run-per-host` DAG
//! standing in for an integration pipeline, host2 killed, determinism
//! checked across two identical runs:
//!
//! ```
//! use std::sync::Arc;
//!
//! use consortium::dag::{DagBuilder, DagContext, DagReport, FnTask, TaskId, TaskOutcome};
//! use consortium_fanout_sim::fixtures::FailureSchedule;
//! use consortium_fanout_sim::simexec::{
//!     assert_deterministic_equivalence, SimExecutor, SimOutcome,
//! };
//! use consortium_integration::exec::{CommandSpec, ExecOutput, Executor, SshTarget};
//! use consortium_nix::cascade::NodeId;
//!
//! /// The pipeline: one local build stage, then an ssh "activate" per
//! /// host — the same shape the integrations' DAGs have.
//! fn run_pipeline(sim: &Arc<SimExecutor>) -> DagReport {
//!     let exec: Arc<dyn Executor> = sim.clone();
//!     let ctx = DagContext::new();
//!     ctx.set_state("executor", exec);
//!
//!     let mut dag = DagBuilder::new();
//!     dag.add_task("stage", FnTask::new("stage build", |ctx| {
//!         let exec = ctx.get_state::<Arc<dyn Executor>>("executor").unwrap();
//!         let spec = CommandSpec::new("nix").args(["build", ".#fleet", "--no-link"]);
//!         match exec.exec(&spec) {
//!             Ok(out) if out.success() => TaskOutcome::Success,
//!             Ok(out) => TaskOutcome::Failed(format!("stage exited {}", out.status)),
//!             Err(e) => TaskOutcome::Failed(e.to_string()),
//!         }
//!     }));
//!     for host in ["node01", "node02"] {
//!         let h = host.to_string();
//!         dag.add_task(
//!             format!("run:{host}"),
//!             FnTask::new(format!("activate {host}"), move |ctx| {
//!                 let exec = ctx.get_state::<Arc<dyn Executor>>("executor").unwrap();
//!                 let spec = CommandSpec::new("activate").ssh(SshTarget::new("root", &h));
//!                 match exec.exec(&spec) {
//!                     Ok(out) if out.success() => TaskOutcome::Success,
//!                     Ok(out) => TaskOutcome::Failed(format!(
//!                         "activate {h}: {}",
//!                         out.stderr.trim()
//!                     )),
//!                     Err(e) => TaskOutcome::Failed(e.to_string()),
//!                 }
//!             }),
//!         );
//!         dag.add_dep(format!("run:{host}"), "stage");
//!     }
//!     dag.context(ctx);
//!     dag.build().unwrap().run().unwrap()
//! }
//!
//! let make_sim = || {
//!     SimExecutor::builder()
//!         .hosts(["node01", "node02"])
//!         .failure_schedule(FailureSchedule::KillNodeAtRound {
//!             node: NodeId(1), // node02
//!             round: 0,
//!         })
//!         .on("nix build", ExecOutput::ok("/nix/store/abc-fleet\n"))
//!         .on("activate", ExecOutput::ok("activated\n"))
//!         .build()
//! };
//!
//! let sim_a = Arc::new(make_sim());
//! let report_a = run_pipeline(&sim_a);
//! assert!(report_a.completed.contains(&TaskId::from("stage")));
//! assert!(report_a.completed.contains(&TaskId::from("run:node01")));
//! assert!(report_a.failed.contains_key(&TaskId::from("run:node02")));
//!
//! // The log tells the same story without relying on event order:
//! // exactly one ssh attempt went to node02, and it failed with 255.
//! let sentinel = sim_a.sentinel_node();
//! let to_node02: Vec<_> = sim_a
//!     .invocation_log()
//!     .into_iter()
//!     .filter(|e| e.edge == Some((sentinel, NodeId(1))))
//!     .collect();
//! assert_eq!(to_node02.len(), 1);
//! assert!(matches!(
//!     to_node02[0].outcome,
//!     SimOutcome::Failed { status: 255, .. }
//! ));
//!
//! // Determinism: an identical executor running the identical pipeline
//! // produces an equivalent log, even though thread interleaving may
//! // differ between runs.
//! let sim_b = Arc::new(make_sim());
//! let report_b = run_pipeline(&sim_b);
//! assert_eq!(report_a.is_success(), report_b.is_success());
//! assert_deterministic_equivalence(&sim_a.invocation_log(), &sim_b.invocation_log());
//! ```

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use consortium_integration::exec::{
    CommandSpec, ExecError, ExecOutput, Executor, Rule, ScriptedExecutor,
};
use consortium_nix::cascade::{CascadeError, NetworkProfile, NodeId};

use crate::fixtures::{
    populate_uniform_latency, rng_from_seed, BandwidthDistribution, FailureSchedule,
    UplinkDistribution,
};

/// Default per-edge bandwidth used when the network profile has no entry
/// (mirrors [`crate::executor::DeterministicExecutor`]).
pub const DEFAULT_BANDWIDTH_BYTES_SEC: u64 = 100 * 1024 * 1024;

/// How [`SimExecutor`] classified a command. See the module docs for the
/// precedence rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SimCommandKind {
    /// `spec.ssh` was set: a control/remote command targeting a fleet
    /// host.
    SshControl,
    /// Args carried an `ssh-ng://` store endpoint (the `nix copy` staging
    /// shape).
    DataCopy,
    /// Everything else: local work (`nix build`, `nix eval`, `sky`, …).
    Local,
}

/// The recorded outcome of one simulated invocation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SimOutcome {
    /// The command succeeded. `duration` is the simulated transfer time
    /// (`transfer_bytes / effective_bandwidth + latency` for edge
    /// commands, zero for local commands and unmatched control commands).
    Ok { duration: Duration },
    /// A simulated network/failure-schedule failure rendered as a
    /// non-zero exit status: 255 for ssh edges (ssh convention), 1 for
    /// data copies.
    Failed {
        /// The rendered exit status.
        status: i32,
        /// The rendered stderr message.
        stderr: String,
    },
    /// The success-output lookup failed: no scripted rule matched
    /// ([`ExecError::Unexpected`]) or a failing rule fired
    /// ([`ExecError::Scripted`]). `Executor::exec` returned `Err`.
    ExecError(String),
}

/// One recorded invocation. See [`SimExecutor::invocation_log`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SimEvent {
    /// The rendered command line (`CommandSpec::render()`).
    pub command_line: String,
    /// How the command was classified.
    pub kind: SimCommandKind,
    /// The simulated edge `(src, tgt)` for edge commands; `None` for
    /// local commands and unresolvable hosts.
    pub edge: Option<(NodeId, NodeId)>,
    /// Per-edge attempt index: the r-th invocation on `edge` gets
    /// `attempt = r` (0-based) and is judged by
    /// `FailureSchedule::failure_for(r, src, tgt)`. Always 0 for
    /// edge-less commands — attempt indexing is per edge only.
    pub attempt: u32,
    /// What happened.
    pub outcome: SimOutcome,
}

/// Builder for [`SimExecutor`]; see [`SimExecutor::builder`].
pub struct SimExecutorBuilder {
    hosts: Vec<String>,
    seed: u64,
    bandwidth: Option<BandwidthDistribution>,
    uplinks: Option<UplinkDistribution>,
    latency: Option<Duration>,
    network: Option<NetworkProfile>,
    schedule: FailureSchedule,
    default_bandwidth: u64,
    default_transfer_bytes: u64,
    transfer_table: Vec<(String, u64)>,
    outputs: ScriptedExecutor,
}

impl Default for SimExecutorBuilder {
    fn default() -> Self {
        Self {
            hosts: Vec::new(),
            seed: 0,
            bandwidth: None,
            uplinks: None,
            latency: None,
            network: None,
            schedule: FailureSchedule::None,
            default_bandwidth: DEFAULT_BANDWIDTH_BYTES_SEC,
            default_transfer_bytes: 0,
            transfer_table: Vec::new(),
            outputs: ScriptedExecutor::new(),
        }
    }
}

impl SimExecutorBuilder {
    /// Fleet hostnames, mapped to `NodeId(0..N)` in order. The build-host
    /// sentinel gets `NodeId(N)` automatically. Required.
    pub fn hosts<I, S>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.hosts = hosts.into_iter().map(Into::into).collect();
        self
    }

    /// Master seed for sampling the network distributions.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Per-edge bandwidth distribution, sampled over all `N + 1` nodes
    /// (sentinel included).
    pub fn bandwidth(mut self, dist: BandwidthDistribution) -> Self {
        self.bandwidth = Some(dist);
        self
    }

    /// Optional per-node uplink/downlink distribution (engages contention
    /// math in [`NetworkProfile::effective_bandwidth`]).
    pub fn uplinks(mut self, dist: UplinkDistribution) -> Self {
        self.uplinks = Some(dist);
        self
    }

    /// Uniform latency applied to every edge (sentinel included).
    pub fn latency(mut self, latency: Duration) -> Self {
        self.latency = Some(latency);
        self
    }

    /// Hand-built network profile. Takes precedence over
    /// `.bandwidth()/.uplinks()/.latency()`; the caller is responsible
    /// for including edges to/from the sentinel (`NodeId(N)`).
    pub fn network(mut self, net: NetworkProfile) -> Self {
        self.network = Some(net);
        self
    }

    /// The failure schedule consulted per (edge, attempt index).
    pub fn failure_schedule(mut self, schedule: FailureSchedule) -> Self {
        self.schedule = schedule;
        self
    }

    /// Fallback per-edge bandwidth for edges absent from the profile
    /// (default 100 MB/s, mirroring `DeterministicExecutor`).
    pub fn default_bandwidth(mut self, bw: u64) -> Self {
        self.default_bandwidth = bw;
        self
    }

    /// Fallback transfer size for data copies matching no
    /// `.transfer_bytes()` pattern (default 0 → latency only).
    pub fn default_transfer_bytes(mut self, bytes: u64) -> Self {
        self.default_transfer_bytes = bytes;
        self
    }

    /// Transfer size for edge commands whose rendered line contains
    /// `pattern`. First matching pattern wins.
    pub fn transfer_bytes(mut self, pattern: &str, bytes: u64) -> Self {
        self.transfer_table.push((pattern.to_string(), bytes));
        self
    }

    /// Script a success output (forwarded to the embedded
    /// [`ScriptedExecutor`]; first matching rule wins).
    pub fn on(mut self, substring: &str, output: ExecOutput) -> Self {
        self.outputs = self.outputs.on(substring, output);
        self
    }

    /// Script a failing output (forwarded to the embedded
    /// [`ScriptedExecutor`]).
    pub fn on_error(mut self, substring: &str, message: &str) -> Self {
        self.outputs = self.outputs.on_error(substring, message);
        self
    }

    /// Append a raw [`Rule`] to the embedded scripted executor.
    pub fn rule(mut self, rule: Rule) -> Self {
        self.outputs = self.outputs.rule(rule);
        self
    }

    /// Build the executor. Panics on an empty or duplicate host list
    /// (builder misuse).
    pub fn build(self) -> SimExecutor {
        assert!(
            !self.hosts.is_empty(),
            "SimExecutor::builder().hosts(..) must list at least one host"
        );
        let mut host_to_node = HashMap::new();
        for (i, h) in self.hosts.iter().enumerate() {
            let prev = host_to_node.insert(h.clone(), NodeId(i as u32));
            assert!(prev.is_none(), "duplicate host {h:?} in SimExecutor hosts");
        }
        let n = self.hosts.len() as u32;
        let sentinel = NodeId(n);
        let net = match self.network {
            Some(net) => net,
            None => {
                let mut net = NetworkProfile::default();
                let mut rng = rng_from_seed(self.seed);
                if let Some(bw) = &self.bandwidth {
                    bw.populate(&mut rng, &mut net, n + 1);
                }
                if let Some(up) = &self.uplinks {
                    up.populate(&mut rng, &mut net, n + 1);
                }
                if let Some(lat) = self.latency {
                    populate_uniform_latency(&mut net, lat, n + 1);
                }
                net
            }
        };
        SimExecutor {
            hosts: self.hosts,
            host_to_node,
            sentinel,
            net,
            schedule: self.schedule,
            default_bandwidth: self.default_bandwidth,
            default_transfer_bytes: self.default_transfer_bytes,
            transfer_table: self.transfer_table,
            outputs: self.outputs,
            state: Mutex::new(SimState::default()),
        }
    }
}

/// Mutable, lock-guarded runtime state.
#[derive(Default)]
struct SimState {
    /// Per-edge attempt counters — the order-independence key.
    edge_attempts: HashMap<(NodeId, NodeId), u32>,
    /// Invocation log in completion order (order is *not* deterministic
    /// across threads; contents are).
    log: Vec<SimEvent>,
    /// Sum of successful data-copy durations (virtual clock).
    transfer_time: Duration,
}

impl SimState {
    /// Take and increment the attempt counter for `(src, tgt)`.
    fn next_attempt(&mut self, src: NodeId, tgt: NodeId) -> u32 {
        let e = self.edge_attempts.entry((src, tgt)).or_insert(0);
        let r = *e;
        *e += 1;
        r
    }
}

/// What a [`CommandSpec`] classified into (internal).
enum Target {
    /// No edge: local command.
    Local,
    /// Would be an edge, but the host is not in the fleet.
    UnknownHost {
        kind: SimCommandKind,
        host: String,
    },
    /// A simulated edge `src → tgt`.
    Edge {
        kind: SimCommandKind,
        src: NodeId,
        tgt: NodeId,
        /// ssh port from the spec (for realistic failure messages).
        ssh_port: Option<u16>,
    },
}

/// Network/failure-aware [`Executor`] for integration sim tests. See the
/// module docs for classification, failure-keying, and determinism
/// semantics.
///
/// Cheap to share: all mutable state sits behind one `Mutex`, so a single
/// `Arc<SimExecutor>` can serve every DAG worker thread.
pub struct SimExecutor {
    hosts: Vec<String>,
    host_to_node: HashMap<String, NodeId>,
    sentinel: NodeId,
    net: NetworkProfile,
    schedule: FailureSchedule,
    default_bandwidth: u64,
    default_transfer_bytes: u64,
    transfer_table: Vec<(String, u64)>,
    /// Scripted success outputs. Used for output lookup ONLY; its private
    /// invocation recording is never exposed.
    outputs: ScriptedExecutor,
    state: Mutex<SimState>,
}

impl SimExecutor {
    /// Start building a `SimExecutor`.
    pub fn builder() -> SimExecutorBuilder {
        SimExecutorBuilder::default()
    }

    /// Every invocation so far, in completion order. The ORDER is not
    /// deterministic across threads; the contents are (see
    /// [`logs_equivalent`]).
    pub fn invocation_log(&self) -> Vec<SimEvent> {
        self.state.lock().unwrap().log.clone()
    }

    /// Sum of successful data-copy durations. Aggregate transfer work,
    /// NOT concurrent wall-clock — the harness does not model wall time.
    pub fn simulated_transfer_time(&self) -> Duration {
        self.state.lock().unwrap().transfer_time
    }

    /// The build-host sentinel's `NodeId` (`NodeId(N)` for N hosts).
    pub fn sentinel_node(&self) -> NodeId {
        self.sentinel
    }

    /// The `NodeId` assigned to a fleet host, if any.
    pub fn host_node(&self, host: &str) -> Option<NodeId> {
        self.host_to_node.get(host).copied()
    }

    /// Fleet hostnames in `NodeId` order.
    pub fn hosts(&self) -> &[String] {
        &self.hosts
    }

    /// The network profile in use.
    pub fn network(&self) -> &NetworkProfile {
        &self.net
    }

    /// Classify a command per the module-docs precedence rules.
    fn classify(&self, spec: &CommandSpec) -> Target {
        // 1. ssh target wins — even over ssh-ng:// args (relay copies are
        //    attributed to the login host).
        if let Some(ssh) = &spec.ssh {
            return match self.host_to_node.get(&ssh.host) {
                Some(&tgt) => Target::Edge {
                    kind: SimCommandKind::SshControl,
                    src: self.sentinel,
                    tgt,
                    ssh_port: ssh.port,
                },
                None => Target::UnknownHost {
                    kind: SimCommandKind::SshControl,
                    host: ssh.host.clone(),
                },
            };
        }
        // 2. ssh-ng:// store endpoints → data edge.
        let to = store_endpoint(&spec.args, "--to");
        let from = store_endpoint(&spec.args, "--from");
        if to.is_none() && from.is_none() {
            return Target::Local;
        }
        let src = match &from {
            Some(h) => match self.host_to_node.get(h) {
                Some(&n) => n,
                None => {
                    return Target::UnknownHost {
                        kind: SimCommandKind::DataCopy,
                        host: h.clone(),
                    }
                }
            },
            None => self.sentinel,
        };
        let tgt = match &to {
            Some(h) => match self.host_to_node.get(h) {
                Some(&n) => n,
                None => {
                    return Target::UnknownHost {
                        kind: SimCommandKind::DataCopy,
                        host: h.clone(),
                    }
                }
            },
            None => self.sentinel,
        };
        Target::Edge {
            kind: SimCommandKind::DataCopy,
            src,
            tgt,
            ssh_port: None,
        }
    }

    /// Static partition check, consulted after the failure schedule.
    fn partition_error(&self, src: NodeId, tgt: NodeId) -> Option<CascadeError> {
        if self.net.is_partitioned(src, tgt) {
            Some(CascadeError::Partitioned { src, tgt })
        } else {
            None
        }
    }

    /// `transfer_bytes / effective_bandwidth + latency` for an edge
    /// command. Contention args are pinned to 1/1 (see module docs).
    fn edge_duration(
        &self,
        kind: SimCommandKind,
        src: NodeId,
        tgt: NodeId,
        rendered: &str,
    ) -> Duration {
        let bytes = match self
            .transfer_table
            .iter()
            .find(|(pattern, _)| rendered.contains(pattern))
        {
            Some((_, b)) => *b,
            None => match kind {
                SimCommandKind::DataCopy => self.default_transfer_bytes,
                _ => 0,
            },
        };
        // max(1): a zero-capacity uplink would otherwise divide by zero;
        // treat it as "slowest possible link" rather than a panic.
        let bw = self
            .net
            .effective_bandwidth(src, tgt, 1, 1, self.default_bandwidth)
            .max(1);
        let secs = bytes as f64 / bw as f64;
        Duration::from_secs_f64(secs) + self.net.latency_of(src, tgt, Duration::ZERO)
    }

    fn record(&self, event: SimEvent) {
        self.state.lock().unwrap().log.push(event);
    }
}

impl Executor for SimExecutor {
    fn exec(&self, spec: &CommandSpec) -> Result<ExecOutput, ExecError> {
        let rendered = spec.render();
        match self.classify(spec) {
            Target::Local => {
                let result = self.outputs.exec(spec);
                let outcome = match &result {
                    Ok(_) => SimOutcome::Ok {
                        duration: Duration::ZERO,
                    },
                    Err(e) => SimOutcome::ExecError(e.to_string()),
                };
                self.record(SimEvent {
                    command_line: rendered,
                    kind: SimCommandKind::Local,
                    edge: None,
                    attempt: 0,
                    outcome,
                });
                result
            }
            Target::UnknownHost { kind, host } => {
                let output = unknown_host_output(kind, &host);
                self.record(SimEvent {
                    command_line: rendered,
                    kind,
                    edge: None,
                    attempt: 0,
                    outcome: SimOutcome::Failed {
                        status: output.status,
                        stderr: output.stderr.clone(),
                    },
                });
                Ok(output)
            }
            Target::Edge {
                kind,
                src,
                tgt,
                ssh_port,
            } => {
                // Attempt keying under one lock hold: the r-th invocation
                // on this edge is judged by failure_for(r, src, tgt).
                let (attempt, failure) = {
                    let mut st = self.state.lock().unwrap();
                    let attempt = st.next_attempt(src, tgt);
                    let failure = self
                        .schedule
                        .failure_for(attempt, src, tgt)
                        .or_else(|| self.partition_error(src, tgt));
                    (attempt, failure)
                };

                if let Some(err) = failure {
                    let host = &self.hosts[tgt.0 as usize];
                    let output = match kind {
                        SimCommandKind::SshControl => ssh_failure_output(host, ssh_port, &err),
                        _ => copy_failure_output(host, &err),
                    };
                    self.record(SimEvent {
                        command_line: rendered,
                        kind,
                        edge: Some((src, tgt)),
                        attempt,
                        outcome: SimOutcome::Failed {
                            status: output.status,
                            stderr: output.stderr.clone(),
                        },
                    });
                    return Ok(output);
                }

                let duration = self.edge_duration(kind, src, tgt, &rendered);
                let result = self.outputs.exec(spec);
                let outcome = match &result {
                    Ok(_) => SimOutcome::Ok { duration },
                    Err(e) => SimOutcome::ExecError(e.to_string()),
                };
                {
                    let mut st = self.state.lock().unwrap();
                    if kind == SimCommandKind::DataCopy
                        && matches!(outcome, SimOutcome::Ok { .. })
                    {
                        st.transfer_time += duration;
                    }
                    st.log.push(SimEvent {
                        command_line: rendered,
                        kind,
                        edge: Some((src, tgt)),
                        attempt,
                        outcome,
                    });
                }
                result
            }
        }
    }
}

/// Extract the host of an `ssh-ng://` endpoint given as `--flag <uri>` or
/// `--flag=<uri>`. Non-`ssh-ng://` values are ignored.
fn store_endpoint(args: &[String], flag: &str) -> Option<String> {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if a == flag {
            if let Some(uri) = it.next() {
                if let Some(h) = ssh_ng_host(uri) {
                    return Some(h);
                }
            }
        } else if let Some(uri) = a.strip_prefix(format!("{flag}=").as_str()) {
            if let Some(h) = ssh_ng_host(uri) {
                return Some(h);
            }
        }
    }
    None
}

/// Host part of an `ssh-ng://[user@]host[:port][/...]` store URI.
fn ssh_ng_host(uri: &str) -> Option<String> {
    let rest = uri.strip_prefix("ssh-ng://")?;
    let authority = rest.split('/').next().unwrap_or(rest);
    let after_user = authority.rsplit('@').next().unwrap_or(authority);
    let host = after_user.split(':').next().unwrap_or(after_user);
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

/// ssh-convention failure for a dead/unreachable control edge.
fn ssh_failure_output(host: &str, port: Option<u16>, err: &CascadeError) -> ExecOutput {
    let reason = match err {
        CascadeError::Activation { .. } => "Connection refused",
        CascadeError::Partitioned { .. } => "No route to host",
        CascadeError::Copy { .. } => "Connection timed out",
        CascadeError::SshHandshake { .. } => "Connection reset by peer",
        CascadeError::SubtreeAggregate { .. } => "Connection timed out",
    };
    ExecOutput::new(
        255,
        "",
        format!(
            "ssh: connect to host {host} port {}: {reason}\n",
            port.unwrap_or(22)
        ),
    )
}

/// Status-1 failure for a broken data copy, naming the error kind.
fn copy_failure_output(host: &str, err: &CascadeError) -> ExecOutput {
    let kind = match err {
        CascadeError::Activation { .. } => "activation",
        CascadeError::Partitioned { .. } => "partition",
        CascadeError::Copy { .. } => "copy",
        CascadeError::SshHandshake { .. } => "ssh-handshake",
        CascadeError::SubtreeAggregate { .. } => "subtree",
    };
    ExecOutput::new(
        1,
        "",
        format!("error: copying to ssh-ng://{host} failed ({kind}): {err}\n"),
    )
}

/// Failure for a host outside the fleet (255 for ssh, 1 for copies).
fn unknown_host_output(kind: SimCommandKind, host: &str) -> ExecOutput {
    let status = match kind {
        SimCommandKind::SshControl => 255,
        _ => 1,
    };
    ExecOutput::new(
        status,
        "",
        format!("ssh: Could not resolve hostname {host}: Name or service not known\n"),
    )
}

/// Canonically sorted copy of a log (by command line, kind, edge,
/// attempt). Two logs whose canonical forms are equal are equal as
/// multisets.
pub fn canonical_log(log: &[SimEvent]) -> Vec<SimEvent> {
    let mut v = log.to_vec();
    v.sort_by(|a, b| {
        (&a.command_line, a.kind, a.edge, a.attempt).cmp(&(
            &b.command_line,
            b.kind,
            b.edge,
            b.attempt,
        ))
    });
    v
}

/// Whether two logs contain the same events, ignoring order.
pub fn logs_equivalent(a: &[SimEvent], b: &[SimEvent]) -> bool {
    canonical_log(a) == canonical_log(b)
}

/// Per-edge outcome sequences, ordered by attempt index. Deterministic
/// for a fixed builder config + pipeline inputs regardless of thread
/// interleaving (see the module's determinism contract).
pub fn per_edge_outcomes(log: &[SimEvent]) -> BTreeMap<(NodeId, NodeId), Vec<SimOutcome>> {
    let mut by_edge: BTreeMap<(NodeId, NodeId), Vec<(u32, SimOutcome)>> = BTreeMap::new();
    for e in log {
        if let Some(edge) = e.edge {
            by_edge
                .entry(edge)
                .or_default()
                .push((e.attempt, e.outcome.clone()));
        }
    }
    by_edge
        .into_iter()
        .map(|(edge, mut v)| {
            v.sort_by_key(|(attempt, _)| *attempt);
            (edge, v.into_iter().map(|(_, outcome)| outcome).collect())
        })
        .collect()
}

/// Assert the determinism contract: identical per-edge outcome sequences
/// AND identical logs as multisets. Panics with both canonical logs on
/// mismatch.
pub fn assert_deterministic_equivalence(run_a: &[SimEvent], run_b: &[SimEvent]) {
    assert_eq!(
        per_edge_outcomes(run_a),
        per_edge_outcomes(run_b),
        "per-edge outcome sequences diverged"
    );
    assert!(
        logs_equivalent(run_a, run_b),
        "invocation logs diverged as multisets\nrun A: {:#?}\nrun B: {:#?}",
        canonical_log(run_a),
        canonical_log(run_b)
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::SshTarget;
    use std::sync::Arc;

    const MB: u64 = 1024 * 1024;

    fn ssh_uptime(host: &str) -> CommandSpec {
        CommandSpec::new("uptime").ssh(SshTarget::new("root", host))
    }

    fn copy_to(host: &str) -> CommandSpec {
        let uri = format!("ssh-ng://root@{host}");
        CommandSpec::new("nix").args(["copy", "--to", uri.as_str(), "/nix/store/abc"])
    }

    // ---------- classification ----------

    #[test]
    fn ssh_spec_maps_to_edge_to_that_host() {
        let sim = SimExecutor::builder()
            .hosts(["node01", "node02"])
            .on("", ExecOutput::ok(""))
            .build();
        sim.exec(&ssh_uptime("node02")).unwrap();
        let log = sim.invocation_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].kind, SimCommandKind::SshControl);
        assert_eq!(
            log[0].edge,
            Some((sim.sentinel_node(), NodeId(1))),
            "ssh to node02 → sentinel→node02 edge"
        );
    }

    #[test]
    fn nix_copy_to_ssh_ng_maps_to_sentinel_data_edge() {
        let sim = SimExecutor::builder()
            .hosts(["node01", "node02"])
            .on("", ExecOutput::ok(""))
            .build();
        sim.exec(&copy_to("node02")).unwrap();
        let log = sim.invocation_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].kind, SimCommandKind::DataCopy);
        assert_eq!(log[0].edge, Some((sim.sentinel_node(), NodeId(1))));
    }

    #[test]
    fn local_nix_build_forms_no_edge() {
        let sim = SimExecutor::builder()
            .hosts(["node01"])
            .on("", ExecOutput::ok(""))
            .build();
        sim.exec(&CommandSpec::new("nix").args(["build", ".#x", "--no-link"]))
            .unwrap();
        let log = sim.invocation_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].kind, SimCommandKind::Local);
        assert_eq!(log[0].edge, None);
        assert_eq!(log[0].attempt, 0);
    }

    #[test]
    fn ssh_target_wins_over_ssh_ng_args_relay_precedence() {
        // Relay shape: ssh into node01 running `nix copy --to node02`.
        // Classified by the ssh login host, per documented precedence.
        let sim = SimExecutor::builder()
            .hosts(["node01", "node02"])
            .on("", ExecOutput::ok(""))
            .build();
        let spec = CommandSpec::new("nix")
            .args(["copy", "--to", "ssh-ng://root@node02", "/nix/store/abc"])
            .ssh(SshTarget::new("root", "node01"));
        sim.exec(&spec).unwrap();
        let log = sim.invocation_log();
        assert_eq!(log[0].kind, SimCommandKind::SshControl);
        assert_eq!(log[0].edge, Some((sim.sentinel_node(), NodeId(0))));
    }

    #[test]
    fn copy_from_fleet_host_forms_node_to_sentinel_edge() {
        let sim = SimExecutor::builder()
            .hosts(["node01", "node02"])
            .on("", ExecOutput::ok(""))
            .build();
        let spec = CommandSpec::new("nix").args([
            "copy",
            "--from=ssh-ng://root@node01",
            "/nix/store/abc",
        ]);
        sim.exec(&spec).unwrap();
        let log = sim.invocation_log();
        assert_eq!(log[0].kind, SimCommandKind::DataCopy);
        assert_eq!(log[0].edge, Some((NodeId(0), sim.sentinel_node())));
    }

    #[test]
    fn unknown_fleet_host_renders_resolution_failure() {
        let sim = SimExecutor::builder()
            .hosts(["node01"])
            .on("", ExecOutput::ok(""))
            .build();
        let ssh = sim.exec(&ssh_uptime("ghost")).unwrap();
        assert_eq!(ssh.status, 255);
        assert!(ssh.stderr.contains("Could not resolve hostname ghost"));
        let copy = sim.exec(&copy_to("ghost")).unwrap();
        assert_eq!(copy.status, 1);
        let log = sim.invocation_log();
        assert!(log.iter().all(|e| e.edge.is_none()));
        assert!(log
            .iter()
            .all(|e| matches!(e.outcome, SimOutcome::Failed { .. })));
    }

    // ---------- failure semantics ----------

    #[test]
    fn kill_node_at_round_zero_fails_first_ssh_and_first_copy() {
        let sim = SimExecutor::builder()
            .hosts(["node01", "node02"])
            .failure_schedule(FailureSchedule::KillNodeAtRound {
                node: NodeId(1),
                round: 0,
            })
            .on("uptime", ExecOutput::ok("up\n"))
            .on("nix copy", ExecOutput::ok(""))
            .build();

        let ssh_dead = sim.exec(&ssh_uptime("node02")).unwrap();
        assert_eq!(ssh_dead.status, 255);
        assert!(ssh_dead
            .stderr
            .contains("ssh: connect to host node02 port 22: Connection refused"));

        let copy_dead = sim.exec(&copy_to("node02")).unwrap();
        assert_eq!(copy_dead.status, 1);
        assert!(copy_dead.stderr.contains("activation"), "{}", copy_dead.stderr);

        // Other hosts are unaffected.
        let ssh_live = sim.exec(&ssh_uptime("node01")).unwrap();
        assert!(ssh_live.success());
        let copy_live = sim.exec(&copy_to("node01")).unwrap();
        assert!(copy_live.success());
    }

    #[test]
    fn static_partition_fails_edge_but_not_local_commands() {
        let mut net = NetworkProfile::default();
        // 2 hosts → sentinel is NodeId(2).
        net.partitions.insert((NodeId(2), NodeId(0)));
        let sim = SimExecutor::builder()
            .hosts(["node01", "node02"])
            .network(net)
            .on("", ExecOutput::ok("ok\n"))
            .build();

        let copy = sim.exec(&copy_to("node01")).unwrap();
        assert_eq!(copy.status, 1);
        assert!(copy.stderr.contains("partition"), "{}", copy.stderr);

        let ssh = sim.exec(&ssh_uptime("node01")).unwrap();
        assert_eq!(ssh.status, 255);
        assert!(ssh.stderr.contains("No route to host"), "{}", ssh.stderr);

        // Local work is not network-bound: unaffected.
        let local = sim
            .exec(&CommandSpec::new("nix").args(["build", ".#x"]))
            .unwrap();
        assert!(local.success());
    }

    #[test]
    fn partition_at_round_strikes_only_the_named_edge() {
        let sim = SimExecutor::builder()
            .hosts(["node01", "node02"])
            .failure_schedule(FailureSchedule::PartitionAtRound {
                src: NodeId(2),
                tgt: NodeId(0),
                round: 0,
            })
            .on("nix copy", ExecOutput::ok(""))
            .build();
        assert_eq!(sim.exec(&copy_to("node01")).unwrap().status, 1);
        assert!(sim.exec(&copy_to("node02")).unwrap().success());
    }

    // ---------- output delegation ----------

    #[test]
    fn scripted_rule_supplies_success_output() {
        let sim = SimExecutor::builder()
            .hosts(["submit01"])
            .on("sbatch", ExecOutput::ok("Submitted batch job 12345\n"))
            .build();
        let spec = CommandSpec::new("sbatch")
            .args(["--job-name=train", "train.sh"])
            .ssh(SshTarget::new("root", "submit01"));
        let out = sim.exec(&spec).unwrap();
        assert!(out.success());
        assert_eq!(out.stdout, "Submitted batch job 12345\n");
    }

    #[test]
    fn unscripted_success_surfaces_unexpected_error() {
        let sim = SimExecutor::builder().hosts(["node01"]).build();
        let err = sim
            .exec(&CommandSpec::new("nix").args(["eval", ".#x"]))
            .unwrap_err();
        match err {
            ExecError::Unexpected(rendered) => {
                assert!(rendered.contains("nix eval"), "{rendered}")
            }
            other => panic!("expected Unexpected, got: {other}"),
        }
        // ...and the rejection is visible in the log.
        let log = sim.invocation_log();
        assert!(matches!(log[0].outcome, SimOutcome::ExecError(_)));
    }

    // ---------- virtual clock ----------

    #[test]
    fn transfer_time_aggregates_data_copy_durations() {
        let sim = SimExecutor::builder()
            .hosts(["node01", "node02"])
            .bandwidth(BandwidthDistribution::Uniform(100 * MB))
            .latency(Duration::from_millis(10))
            .transfer_bytes("nix copy", 100 * MB)
            .on("nix copy", ExecOutput::ok(""))
            .build();
        sim.exec(&copy_to("node01")).unwrap();
        sim.exec(&copy_to("node02")).unwrap();
        // 100 MiB / 100 MiB/s + 10 ms = 1.01 s per copy; 2 copies.
        assert_eq!(sim.simulated_transfer_time(), Duration::from_millis(2020));
    }

    #[test]
    fn control_commands_contribute_no_transfer_time() {
        let sim = SimExecutor::builder()
            .hosts(["node01"])
            .bandwidth(BandwidthDistribution::Uniform(100 * MB))
            .on("uptime", ExecOutput::ok("up\n"))
            .build();
        sim.exec(&ssh_uptime("node01")).unwrap();
        assert_eq!(sim.simulated_transfer_time(), Duration::ZERO);
        // ...but the event still records a (latency-only) duration.
        let log = sim.invocation_log();
        assert!(matches!(log[0].outcome, SimOutcome::Ok { .. }));
    }

    // ---------- determinism under thread interleaving ----------

    fn run_shuffled(sim: &Arc<SimExecutor>, shuffle_seed: u64) {
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        // Per-host chunks keep a FIXED within-edge order; only the
        // cross-edge interleaving varies with the shuffle seed.
        let mut chunks: Vec<Vec<CommandSpec>> = ["node01", "node02", "node03"]
            .iter()
            .map(|h| {
                vec![
                    ssh_uptime(h),
                    ssh_uptime(h),
                    copy_to(h),
                    copy_to(h),
                ]
            })
            .collect();
        chunks.shuffle(&mut rand::rngs::StdRng::seed_from_u64(shuffle_seed));
        std::thread::scope(|s| {
            for chunk in chunks {
                s.spawn(move || {
                    for spec in &chunk {
                        sim.exec(spec).ok();
                    }
                });
            }
        });
    }

    fn determinism_sim() -> SimExecutor {
        SimExecutor::builder()
            .hosts(["node01", "node02", "node03"])
            .seed(0x1234)
            .bandwidth(BandwidthDistribution::Uniform(50 * MB))
            .transfer_bytes("nix copy", 100 * MB)
            .failure_schedule(FailureSchedule::KillNodeAtRound {
                node: NodeId(1),
                round: 1,
            })
            .on("uptime", ExecOutput::ok("up\n"))
            .on("nix copy", ExecOutput::ok(""))
            .build()
    }

    #[test]
    fn interleaved_runs_are_deterministic_per_edge() {
        let sim_a = Arc::new(determinism_sim());
        let sim_b = Arc::new(determinism_sim());
        run_shuffled(&sim_a, 1);
        run_shuffled(&sim_b, 99);
        let log_a = sim_a.invocation_log();
        let log_b = sim_b.invocation_log();
        assert_deterministic_equivalence(&log_a, &log_b);
        assert_eq!(sim_a.simulated_transfer_time(), sim_b.simulated_transfer_time());

        // The schedule keyed attempts, not arrival order: on node02's
        // edge, attempt 0 succeeded and attempts 1..=3 failed.
        let outcomes = per_edge_outcomes(&log_a);
        let node02_edge = outcomes
            .get(&(NodeId(3), NodeId(1)))
            .expect("edge sentinel→node02");
        assert!(matches!(node02_edge[0], SimOutcome::Ok { .. }));
        for o in &node02_edge[1..] {
            assert!(matches!(o, SimOutcome::Failed { .. }), "{o:?}");
        }
        // ssh failures are 255, copy failures are 1.
        let ssh_fail = node02_edge[1].clone();
        assert!(matches!(ssh_fail, SimOutcome::Failed { status: 255, .. }));
        for o in &node02_edge[2..] {
            assert!(matches!(o, SimOutcome::Failed { status: 1, .. }));
        }

        // Aggregate clock: node01+node03 copies succeed (2 × 2 s each).
        assert_eq!(sim_a.simulated_transfer_time(), Duration::from_secs(8));
    }

    // ---------- builder validation ----------

    #[test]
    #[should_panic(expected = "at least one host")]
    fn builder_panics_without_hosts() {
        SimExecutor::builder().build();
    }

    #[test]
    #[should_panic(expected = "duplicate host")]
    fn builder_panics_on_duplicate_hosts() {
        SimExecutor::builder().hosts(["node01", "node01"]).build();
    }
}
