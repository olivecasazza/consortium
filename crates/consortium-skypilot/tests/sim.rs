//! Seed-pinned `SimExecutor` tests for the SkyPilot launch pipeline.
//!
//! Every command `launch_task` issues (`nix build`, `sky launch`,
//! `sky down`) runs on the operator's machine against local cloud
//! credentials, so `SimExecutor` classifies all of them `Local`: no
//! simulated edges, no failure-schedule involvement, zero simulated
//! transfer time. What the sim adds over a bare `ScriptedExecutor` here is
//! the shared harness idiom, scripted cloud-CLI failure modes (non-zero
//! exits via rules), and the determinism / invocation-log assertions
//! (`assert_deterministic_equivalence`, per-command presence/absence).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use consortium::dag::{DagReport, TaskId};
use consortium_fanout_sim::simexec::{
    assert_deterministic_equivalence, SimCommandKind, SimEvent, SimExecutor,
};
use consortium_integration::exec::{ExecOutput, Executor, Rule};
use consortium_integration::fleet::{FleetConfig, SkypilotFleetConfig};
use consortium_skypilot::{launch_task, SkyOptions};

/// Cluster name used across scenarios; task ids derive from it.
const CLUSTER: &str = "test-cluster";
/// The yaml handed to `sky launch` (the launch task writes it to a unique
/// temp file whose path lands in the rendered command line).
const TASK_YAML: &str = "resources:\n  cloud: gcp\n";
/// Pinned seed. The pipeline is all-local, so the seed only shapes the
/// (unused) network profile — pin it anyway to keep the harness idiom.
const SEED: u64 = 0x1234;

fn fleet_config() -> FleetConfig {
    FleetConfig {
        nodes: HashMap::new(),
        builders: HashMap::new(),
        flake_uri: ".".into(),
        ansible_config: None,
        slurm_config: None,
        ray_config: None,
        skypilot_config: Some(SkypilotFleetConfig {
            cloud: "gcp".into(),
            region: Some("us-central1".into()),
            instance_type: None,
        }),
    }
}

/// Rewrite the unique temp yaml path (pid + sequence counter) inside
/// `sky launch` command lines to a fixed placeholder. Two runs of the
/// same pipeline in one process render *different* temp paths, so the
/// raw rendered lines can never be multiset-equal; installed as the
/// executor's `normalize_command_line` hook, this rewrites the line
/// BEFORE rule matching and recording, which makes the determinism
/// contract checkable on the raw logs. Everything else in the line
/// (program, flags, cluster, cloud, region) is pipeline-determined.
fn normalize_task_yaml(line: &str) -> String {
    line.split_whitespace()
        .map(|tok| {
            if tok.contains("consortium-sky-") && tok.ends_with(".yaml") {
                "<task-yaml>"
            } else {
                tok
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The happy-path executor. The pipeline never addresses a fleet host
/// (every command classifies `Local`), so the fleet is left EMPTY — the
/// sentinel is still allocated and no dummy host is needed.
fn make_sim() -> SimExecutor {
    SimExecutor::builder()
        .seed(SEED)
        .normalize_command_line(normalize_task_yaml)
        .on("nix build", ExecOutput::ok("/nix/store/sky-env\n"))
        .on("sky launch", ExecOutput::ok("Cluster launched: test-cluster\n"))
        .on("sky down", ExecOutput::ok("Terminating cluster\n"))
        .build()
}

fn run(sim: &Arc<SimExecutor>, opts: &SkyOptions) -> DagReport {
    let exec: Arc<dyn Executor> = sim.clone();
    launch_task(exec, &fleet_config(), CLUSTER, TASK_YAML, opts).unwrap()
}

fn build_id() -> TaskId {
    TaskId::from(format!("build-sky-env:{CLUSTER}"))
}

fn launch_id() -> TaskId {
    TaskId::from(format!("sky-launch:{CLUSTER}"))
}

fn down_id() -> TaskId {
    TaskId::from(format!("sky-down:{CLUSTER}"))
}

/// Index of the first log event whose command line contains `marker`.
fn invocation_index(log: &[SimEvent], marker: &str) -> Option<usize> {
    log.iter().position(|e| e.command_line.contains(marker))
}

/// Whether any logged command line contains `marker`.
fn invoked(log: &[SimEvent], marker: &str) -> bool {
    invocation_index(log, marker).is_some()
}

#[test]
fn happy_path_launch_and_teardown() {
    let sim = Arc::new(make_sim());
    let report = run(&sim, &SkyOptions::default());

    assert!(report.is_success(), "failures: {:?}", report.failed);
    for id in [build_id(), launch_id(), down_id()] {
        assert!(report.completed.contains(&id), "{id} not in completed");
    }

    // The pipeline is a single linear chain, so log order is meaningful
    // here — but compare indices rather than assuming raw positions.
    let log = sim.invocation_log();
    let build = invocation_index(&log, "nix build").expect("nix build invoked");
    let launch = invocation_index(&log, "sky launch").expect("sky launch invoked");
    let down = invocation_index(&log, "sky down").expect("sky down invoked");
    assert!(build < launch, "build ({build}) must precede launch ({launch})");
    assert!(launch < down, "launch ({launch}) must precede down ({down})");

    // The launch line carries the fleet's cloud/region and the cluster.
    let launch_line = &log[launch].command_line;
    assert!(launch_line.contains(&format!("-c {CLUSTER}")), "{launch_line}");
    assert!(launch_line.contains("--cloud gcp"), "{launch_line}");
    assert!(launch_line.contains("--region us-central1"), "{launch_line}");

    // All-local pipeline: no edges, no simulated transfer time.
    assert!(
        log.iter()
            .all(|e| e.kind == SimCommandKind::Local && e.edge.is_none()),
        "every skypilot command must classify Local: {log:?}"
    );
    assert_eq!(sim.simulated_transfer_time(), Duration::ZERO);
}

#[test]
fn no_teardown_leaves_cluster_up() {
    let sim = Arc::new(make_sim());
    let report = run(&sim, &SkyOptions { teardown: false });

    assert!(report.is_success(), "failures: {:?}", report.failed);
    assert!(report.completed.contains(&build_id()));
    assert!(report.completed.contains(&launch_id()));
    assert!(!report.completed.contains(&down_id()));

    let log = sim.invocation_log();
    assert!(invoked(&log, "sky launch"));
    assert!(
        !invoked(&log, "sky down"),
        "sky down must never run with teardown=false: {log:?}"
    );
}

#[test]
fn launch_failure_skips_teardown() {
    // The failing rule comes FIRST: the scripted executor is
    // first-match-wins, so the specific failure must precede the broad
    // success rules.
    let sim = Arc::new(
        SimExecutor::builder()
            .seed(SEED)
            .rule(Rule::containing(
                "sky launch",
                ExecOutput::new(1, "", "cloud quota exceeded"),
            ))
            .on("nix build", ExecOutput::ok("/nix/store/sky-env\n"))
            .on("sky launch", ExecOutput::ok("Cluster launched: test-cluster\n"))
            .on("sky down", ExecOutput::ok("Terminating cluster\n"))
            .build(),
    );
    let report = run(&sim, &SkyOptions::default());

    assert!(!report.is_success());
    assert!(report.completed.contains(&build_id()));
    let msg = report.failed.get(&launch_id()).expect("sky-launch failed");
    assert!(msg.contains("cloud quota exceeded"), "{msg}");

    // The DAG must not tear down a cluster that never came up.
    assert!(!report.completed.contains(&down_id()));
    let log = sim.invocation_log();
    assert!(invoked(&log, "sky launch"));
    assert!(
        !invoked(&log, "sky down"),
        "sky down must never run after a failed launch: {log:?}"
    );
}

#[test]
fn env_build_failure_aborts_before_any_sky_command() {
    let sim = Arc::new(
        SimExecutor::builder()
            .seed(SEED)
            .rule(Rule::containing(
                "nix build",
                ExecOutput::new(1, "", "hash mismatch in fixed-output derivation"),
            ))
            .on("nix build", ExecOutput::ok("/nix/store/sky-env\n"))
            .on("sky launch", ExecOutput::ok("Cluster launched: test-cluster\n"))
            .on("sky down", ExecOutput::ok("Terminating cluster\n"))
            .build(),
    );
    let report = run(&sim, &SkyOptions::default());

    assert!(!report.is_success());
    let msg = report.failed.get(&build_id()).expect("build-sky-env failed");
    assert!(msg.contains("hash mismatch"), "{msg}");
    assert!(!report.completed.contains(&launch_id()));

    // Not one `sky` CLI command was issued. ("sky " with a trailing
    // space: the `.#skyEnvs.` fragment in the nix build line must not
    // count as a sky CLI invocation.)
    let log = sim.invocation_log();
    assert!(
        !log.iter().any(|e| e.command_line.contains("sky ")),
        "no sky command may run after a failed env build: {log:?}"
    );
    assert_eq!(log.len(), 1, "only the failed nix build was attempted");
}

#[test]
fn deterministic_runs() {
    let sim_a = Arc::new(make_sim());
    let report_a = run(&sim_a, &SkyOptions::default());
    let sim_b = Arc::new(make_sim());
    let report_b = run(&sim_b, &SkyOptions::default());

    assert!(report_a.is_success() && report_b.is_success());
    assert_eq!(report_a.completed, report_b.completed);
    let failed_a: HashSet<_> = report_a.failed.keys().collect();
    let failed_b: HashSet<_> = report_b.failed.keys().collect();
    assert_eq!(failed_a, failed_b);

    // Temp yaml paths differ between runs (pid + sequence); the
    // executor's normalize hook (see make_sim) rewrites them before
    // recording, so the raw logs satisfy the determinism contract.
    let log_a = sim_a.invocation_log();
    let log_b = sim_b.invocation_log();
    assert_deterministic_equivalence(&log_a, &log_b);

    // All commands classify `Local` → no data edges in the log → zero
    // simulated transfer time in both runs.
    assert!(
        log_a.iter().all(|e| e.edge.is_none()),
        "no data edges in an all-local pipeline: {log_a:?}"
    );
    assert_eq!(sim_a.simulated_transfer_time(), Duration::ZERO);
    assert_eq!(
        sim_a.simulated_transfer_time(),
        sim_b.simulated_transfer_time()
    );
}
