//! Seed-pinned [`SimExecutor`] sim tests for the nix deploy pipeline.
//!
//! `deploy()` runs its per-host eval → build → copy → activate DAG
//! against an `Arc<SimExecutor>`; the sim classifies every command
//! (local / ssh control / data copy), applies the failure schedule per
//! (edge, attempt index), and times copies on a virtual clock.
//! Assertions target pipeline semantics — report contents, edge/kind
//! filtered log views, simulated transfer time — never log order, which
//! the DAG's worker threads make nondeterministic.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use consortium_fanout_sim::fixtures::{BandwidthDistribution, FailureSchedule};
use consortium_fanout_sim::simexec::{
    assert_deterministic_equivalence, SimCommandKind, SimEvent, SimExecutor, SimExecutorBuilder,
    SimOutcome,
};
use consortium_integration::exec::{ExecOutput, Executor};
use consortium_nix::cascade::NodeId;
use consortium_nix::config::{Builder, DeploymentNode, FleetConfig, ProfileType};
use consortium_nix::{deploy, DeployAction, DeployReport};

const MIB: u64 = 1024 * 1024;

/// The fleet every scenario deploys to.
fn targets() -> Vec<String> {
    vec!["node01".to_string(), "node02".to_string()]
}

/// A NixOS node whose `target_host` is its own name — gotcha: the sim's
/// `ssh-ng://<user>@<host>` classification matches on `target_host`, so
/// it must equal the `.hosts([..])` entry exactly.
fn node(name: &str) -> DeploymentNode {
    DeploymentNode {
        name: name.into(),
        target_host: name.into(),
        target_user: "root".into(),
        target_port: None,
        system: "x86_64-linux".into(),
        profile_type: ProfileType::Nixos,
        build_on_target: false,
        tags: vec![],
        drv_path: None,
        toplevel: None,
    }
}

fn fleet_config() -> FleetConfig {
    let mut nodes = HashMap::new();
    nodes.insert("node01".to_string(), node("node01"));
    nodes.insert("node02".to_string(), node("node02"));
    FleetConfig {
        nodes,
        builders: HashMap::new(),
        flake_uri: ".".into(),
        ansible_config: None,
        slurm_config: None,
        ray_config: None,
        skypilot_config: None,
    }
}

/// Fleet with one remote builder configured (used with
/// `use_builders=true`, which engages the health probes).
fn fleet_config_with_builder() -> FleetConfig {
    let mut config = fleet_config();
    config.builders.insert(
        "builder01".to_string(),
        Builder {
            host: "builder01".into(),
            user: "nix".into(),
            max_jobs: 8,
            speed_factor: 2,
            systems: vec!["x86_64-linux".into()],
            features: vec![],
            ssh_key: None,
            protocol: "ssh-ng".into(),
        },
    );
    config
}

/// Base sim builder: pinned seed, uniform 100 MiB/s links, 10 ms edge
/// latency, 100 MiB closures for `nix copy`. Per-copy duration is then
/// exactly `1 s + 10 ms` on every edge.
fn base_sim_builder<I, S>(hosts: I) -> SimExecutorBuilder
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    SimExecutor::builder()
        .hosts(hosts)
        .seed(0x1234)
        .bandwidth(BandwidthDistribution::Uniform(100 * MIB))
        .latency(Duration::from_millis(10))
        .transfer_bytes("nix copy", 100 * MIB)
}

/// Scripted success outputs for a healthy deploy (first match wins, so
/// the per-host attr rules come before the broad command rules).
///
/// Both the `nix eval` and `nix build` lines for a host contain
/// `nixosConfigurations.<host>`, so one rule answers both with that
/// host's toplevel path. Every success output the pipeline parses must
/// be scripted — an unscripted success returns `Err(Unexpected)`.
fn script_healthy_outputs(builder: SimExecutorBuilder) -> SimExecutorBuilder {
    builder
        .on(
            "nixosConfigurations.node01",
            ExecOutput::ok("/nix/store/aaa-node01-toplevel\n"),
        )
        .on(
            "nixosConfigurations.node02",
            ExecOutput::ok("/nix/store/bbb-node02-toplevel\n"),
        )
        .on("nix copy", ExecOutput::ok(""))
        .on("nix-env", ExecOutput::ok(""))
        .on("switch-to-configuration", ExecOutput::ok(""))
}

fn healthy_sim() -> SimExecutor {
    script_healthy_outputs(base_sim_builder(["node01", "node02"])).build()
}

/// Run `deploy()` against `sim` (Switch-style 4-wide parallelism) and
/// unwrap the report.
fn run_deploy(
    sim: &Arc<SimExecutor>,
    config: &FleetConfig,
    action: DeployAction,
    use_builders: bool,
) -> DeployReport {
    let exec: Arc<dyn Executor> = sim.clone();
    deploy(exec, config, &targets(), action, 4, use_builders).expect("deploy pipeline error")
}

/// Log events of `kind` on `edge`, in log (completion) order.
fn edge_events(
    log: &[SimEvent],
    kind: SimCommandKind,
    edge: (NodeId, NodeId),
) -> Vec<&SimEvent> {
    log.iter()
        .filter(|e| e.kind == kind && e.edge == Some(edge))
        .collect()
}

/// 2-host switch deploy on a healthy fleet converges; both staged
/// copies land and the virtual clock matches the bandwidth math.
#[test]
fn healthy_baseline_converges() {
    let config = fleet_config();
    let sim = Arc::new(healthy_sim());

    let report = run_deploy(&sim, &config, DeployAction::Switch, false);

    assert!(report.is_success(), "expected clean deploy: {report:?}");
    assert_eq!(report.built, targets());
    assert_eq!(report.copied, targets());
    assert_eq!(report.activated, targets());

    let log = sim.invocation_log();
    let sentinel = sim.sentinel_node();
    let node01 = sim.host_node("node01").unwrap();
    let node02 = sim.host_node("node02").unwrap();

    for node in [node01, node02] {
        // One staged copy per host: sentinel → host, successful.
        let copies = edge_events(&log, SimCommandKind::DataCopy, (sentinel, node));
        assert_eq!(copies.len(), 1, "expected exactly one copy to {node:?}");
        assert!(matches!(copies[0].outcome, SimOutcome::Ok { .. }));
        // Activation is two ssh control commands per host:
        // `nix-env --set` then `switch-to-configuration switch`.
        let ssh = edge_events(&log, SimCommandKind::SshControl, (sentinel, node));
        assert_eq!(ssh.len(), 2, "expected both activation ssh calls for {node:?}");
        assert!(ssh.iter().all(|e| matches!(e.outcome, SimOutcome::Ok { .. })));
    }

    // Every success output was scripted — no Unexpected/Scripted errors.
    assert!(
        log.iter().all(|e| !matches!(e.outcome, SimOutcome::ExecError(_))),
        "unscripted command reached the executor: {log:?}"
    );

    // Virtual clock: 2 copies × (100 MiB / 100 MiB/s + 10 ms latency).
    let expected = 2 * (Duration::from_secs(1) + Duration::from_millis(10));
    assert_eq!(sim.simulated_transfer_time(), expected);
}

/// node02 killed at round 0: its copy fails (status 1), its activation
/// is cancelled before any ssh attempt, and node01 still converges.
#[test]
fn killed_target_fails_copy_but_fleet_continues() {
    let config = fleet_config();
    // node02 = NodeId(1) (second .hosts entry); round 0 = dead from the
    // first attempt on any edge targeting it.
    let sim = Arc::new(
        script_healthy_outputs(
            base_sim_builder(["node01", "node02"]).failure_schedule(
                FailureSchedule::KillNodeAtRound {
                    node: NodeId(1),
                    round: 0,
                },
            ),
        )
        .build(),
    );

    let report = run_deploy(&sim, &config, DeployAction::Switch, false);

    assert!(!report.is_success());
    // eval/build are local commands: the network kill cannot reach them.
    assert_eq!(report.built, targets());
    assert_eq!(report.copied, vec!["node01".to_string()]);
    assert_eq!(report.activated, vec!["node01".to_string()]);
    assert_eq!(report.copy_failures.len(), 1);
    assert_eq!(report.copy_failures[0].0, "node02");
    assert!(report.activation_failures.is_empty());

    let log = sim.invocation_log();
    let sentinel = sim.sentinel_node();
    let node01 = sim.host_node("node01").unwrap();
    let node02 = sim.host_node("node02").unwrap();

    // The copy to node02 was attempted exactly once and failed as a
    // data copy (status 1) — a rendered failure, never an Err.
    let dead_copies = edge_events(&log, SimCommandKind::DataCopy, (sentinel, node02));
    assert_eq!(dead_copies.len(), 1);
    assert!(matches!(
        dead_copies[0].outcome,
        SimOutcome::Failed { status: 1, .. }
    ));

    // A copy failure cancels activation: no ssh control command may
    // have been attempted toward the dead host.
    assert!(
        edge_events(&log, SimCommandKind::SshControl, (sentinel, node02)).is_empty(),
        "activation must not be attempted after a failed copy"
    );

    // node01's full chain is intact: one copy + two activation ssh calls.
    assert_eq!(
        edge_events(&log, SimCommandKind::DataCopy, (sentinel, node01)).len(),
        1
    );
    assert_eq!(
        edge_events(&log, SimCommandKind::SshControl, (sentinel, node01)).len(),
        2
    );

    // Only the successful copy counts toward the virtual clock.
    assert_eq!(
        sim.simulated_transfer_time(),
        Duration::from_secs(1) + Duration::from_millis(10)
    );
}

/// `DeployAction::Build` runs only the local stages: no copy, no
/// activation, zero edge commands — the kill schedule is irrelevant.
#[test]
fn build_action_ignores_network_kill() {
    let config = fleet_config();
    let sim = Arc::new(
        script_healthy_outputs(
            base_sim_builder(["node01", "node02"]).failure_schedule(
                FailureSchedule::KillNodeAtRound {
                    node: NodeId(1),
                    round: 0,
                },
            ),
        )
        .build(),
    );

    let report = run_deploy(&sim, &config, DeployAction::Build, false);

    assert!(
        report.is_success(),
        "build-only deploy must succeed despite the kill: {report:?}"
    );
    assert_eq!(report.built, targets());
    assert!(report.copied.is_empty());
    assert!(report.activated.is_empty());

    // eval + build only: four local commands, no edges at all.
    let log = sim.invocation_log();
    assert_eq!(log.len(), 4);
    assert!(
        log.iter()
            .all(|e| e.kind == SimCommandKind::Local && e.edge.is_none()),
        "build-only deploy must issue no edge commands: {log:?}"
    );
    assert_eq!(sim.simulated_transfer_time(), Duration::ZERO);
}

/// With `use_builders=true` and a configured builder whose ssh health
/// probe fails, deploy warns and falls back to building locally (no
/// machines file, no `--builders` flag).
#[test]
fn unhealthy_builder_falls_back_to_local_build() {
    let config = fleet_config_with_builder();
    // The builder IS part of the sim fleet, so its ssh health probe
    // forms an edge and reaches the scripted rules (an unlisted host
    // would short-circuit as UnknownHost instead). The probe's remote
    // command renders as 'true' — quoted, so match the quoted token.
    let sim = Arc::new(
        script_healthy_outputs(base_sim_builder(["node01", "node02", "builder01"]).on_error(
            "'true'",
            "ssh: connect to host builder01 port 22: Connection refused",
        ))
        .build(),
    );

    let report = run_deploy(&sim, &config, DeployAction::Switch, true);

    assert!(
        report.is_success(),
        "deploy must fall back to local builds: {report:?}"
    );
    assert_eq!(report.activated, targets());

    let log = sim.invocation_log();
    let sentinel = sim.sentinel_node();
    let builder01 = sim.host_node("builder01").unwrap();

    // The probe ran (one ssh control command to the builder) and the
    // scripted failure surfaced as an executor error → builder unhealthy.
    let probes = edge_events(&log, SimCommandKind::SshControl, (sentinel, builder01));
    assert_eq!(probes.len(), 1);
    assert!(probes[0].command_line.contains("-oConnectTimeout=5"));
    assert!(matches!(probes[0].outcome, SimOutcome::ExecError(_)));

    // A failed ssh probe short-circuits before `nix store ping`.
    assert!(log.iter().all(|e| !e.command_line.contains("nix store ping")));

    // Fallback evidence: builds ran locally — no `--builders @<file>`.
    let builds: Vec<_> = log
        .iter()
        .filter(|e| e.command_line.contains("nix build"))
        .collect();
    assert_eq!(builds.len(), 2);
    assert!(builds.iter().all(|e| !e.command_line.contains("--builders")));
}

/// The identical scenario built twice from one `make_sim()` closure and
/// run twice yields equivalent logs, equal virtual clocks, and identical
/// reports — worker-thread interleaving changes log ORDER only.
#[test]
fn deterministic_under_threads() {
    let make_sim = || script_healthy_outputs(base_sim_builder(["node01", "node02"])).build();
    let config = fleet_config();

    let sim_a = Arc::new(make_sim());
    let report_a = run_deploy(&sim_a, &config, DeployAction::Switch, false);

    let sim_b = Arc::new(make_sim());
    let report_b = run_deploy(&sim_b, &config, DeployAction::Switch, false);

    assert!(report_a.is_success() && report_b.is_success());
    assert_deterministic_equivalence(&sim_a.invocation_log(), &sim_b.invocation_log());
    assert_eq!(
        sim_a.simulated_transfer_time(),
        sim_b.simulated_transfer_time()
    );
    assert_eq!(
        format!("{report_a:?}"),
        format!("{report_b:?}"),
        "identical scenario must yield identical reports"
    );
}
