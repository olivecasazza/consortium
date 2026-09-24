//! SimExecutor-based sim tests for the ansible playbook pipeline.
//!
//! `run_playbook` runs under `Arc<SimExecutor>` with a pinned seed. Command
//! classification for this pipeline:
//!
//! - `nix build .#ansibleEnvs.<env>` (per host, limit 1) — Local: no edge,
//!   no failure schedule, output from the scripted rules alone.
//! - `nix copy --to ssh-ng://root@ctrl01` (per host, limit 1) — DataCopy
//!   edge sentinel→ctrl01; transfer_bytes drives the virtual clock.
//! - `ansible-playbook --limit <host> <playbook>` over ssh on the control
//!   node — SshControl edge sentinel→ctrl01 (0 bytes, latency only).
//!
//! All four edge commands share the single control edge, and the per-(edge,
//! attempt-index) failure keying can't distinguish commands sharing an
//! edge — so per-COMMAND failures (one playbook run fails, the other
//! succeeds) are scripted with response rules placed FIRST (first match
//! wins), never with failure schedules.
//!
//! Assertions use edge/kind filters over the invocation log, never log
//! order: DAG worker threads interleave completions run to run.

use std::sync::Arc;
use std::time::Duration;

use consortium::dag::{DagReport, TaskId};
use consortium_ansible::{run_playbook, AnsibleOptions};
use consortium_fanout_sim::fixtures::{BandwidthDistribution, FailureSchedule};
use consortium_fanout_sim::simexec::{
    assert_deterministic_equivalence, SimCommandKind, SimEvent, SimExecutor, SimOutcome,
};
use consortium_fanout_sim::NodeId;
use consortium_integration::exec::{ExecOutput, Executor, Rule};
use consortium_integration::fleet::{AnsibleFleetConfig, DeploymentNode, FleetConfig, ProfileType};

const PLAYBOOK: &str = "site.yml";
const ENV_NAME: &str = "default";
const SEED: u64 = 0x1234;
/// `.hosts(..)` order: ctrl01 = NodeId(0); the build-host sentinel is
/// NodeId(3).
const SIM_HOSTS: [&str; 3] = ["ctrl01", "node01", "node02"];
const COPY_BYTES: u64 = 50 * 1024 * 1024;
const BANDWIDTH: u64 = 100 * 1024 * 1024;
const PLAY_RECAP: &str = "PLAY RECAP: ok=2 changed=0 unreachable=0 failed=0\n";

fn targets() -> Vec<String> {
    vec!["node01".to_string(), "node02".to_string()]
}

fn fleet_config() -> FleetConfig {
    let node = |name: &str| DeploymentNode {
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
    };
    let mut nodes = std::collections::HashMap::new();
    nodes.insert("node01".to_string(), node("node01"));
    nodes.insert("node02".to_string(), node("node02"));
    FleetConfig {
        nodes,
        builders: std::collections::HashMap::new(),
        flake_uri: ".".into(),
        ansible_config: Some(AnsibleFleetConfig {
            control_node: "ctrl01".into(),
            ansible_version: None,
            collections: vec![],
            playbook_dir: None,
            host_groups: std::collections::HashMap::new(),
        }),
        slurm_config: None,
        ray_config: None,
        skypilot_config: None,
    }
}

/// Happy-path sim: every phase scripted to succeed, seed-pinned uniform
/// network. The `nix copy` rule comes before any rule that could match the
/// copy line's `ssh-ng://` URI via an `"ssh"` substring trap — here the
/// playbook rule matches `ansible-playbook`, which the copy line lacks, so
/// the three rules are mutually exclusive.
fn make_sim() -> SimExecutor {
    SimExecutor::builder()
        .hosts(SIM_HOSTS)
        .seed(SEED)
        .bandwidth(BandwidthDistribution::Uniform(BANDWIDTH))
        .transfer_bytes("nix copy", COPY_BYTES)
        .on("nix build", ExecOutput::ok("/nix/store/ansible-env\n"))
        .on("nix copy", ExecOutput::ok(""))
        .on("ansible-playbook", ExecOutput::ok(PLAY_RECAP))
        .build()
}

fn run(sim: &Arc<SimExecutor>, targets: &[String], opts: &AnsibleOptions) -> DagReport {
    let exec: Arc<dyn Executor> = sim.clone();
    run_playbook(exec, &fleet_config(), targets, PLAYBOOK, ENV_NAME, opts)
        .expect("run_playbook returns Ok(report); task failures live in the report")
}

/// Every completed `<stage>:<host>` task id for both targets.
fn assert_all_stages_completed(report: &DagReport) {
    for host in ["node01", "node02"] {
        for stage in ["build-env", "copy-env", "run-playbook"] {
            let id = TaskId(format!("{stage}:{host}"));
            assert!(
                report.completed.contains(&id),
                "missing completed task {stage}:{host} (failed: {:?})",
                report.failed
            );
        }
    }
}

/// Playbook invocations: ssh control commands running ansible-playbook.
fn playbook_events(log: &[SimEvent]) -> Vec<&SimEvent> {
    log.iter()
        .filter(|e| {
            e.kind == SimCommandKind::SshControl && e.command_line.contains("ansible-playbook")
        })
        .collect()
}

/// Both per-target playbook runs reached the control node, each carrying
/// its `--limit <host>` token (rendered quoted, so interior substrings).
fn assert_both_playbooks_attempted(log: &[SimEvent]) {
    let playbooks = playbook_events(log);
    assert_eq!(playbooks.len(), 2, "one playbook run per target");
    for host in ["node01", "node02"] {
        assert!(
            playbooks.iter().any(|e| e.command_line.contains(host)),
            "no playbook invocation for {host}"
        );
    }
}

// ─── Scenario 1: happy path ─────────────────────────────────────────────────

#[test]
fn happy_path_all_hosts_configured() {
    let sim = Arc::new(make_sim());
    let report = run(&sim, &targets(), &AnsibleOptions::default());

    assert!(report.is_success(), "failed: {:?}", report.failed);
    assert_all_stages_completed(&report);

    let log = sim.invocation_log();
    let sentinel = sim.sentinel_node();
    let ctrl = sim.host_node("ctrl01").unwrap();
    let ctrl_edge = Some((sentinel, ctrl));

    // Two identical per-host `nix build`s: local work — no edge, no
    // failure-schedule involvement.
    let builds: Vec<_> = log
        .iter()
        .filter(|e| e.kind == SimCommandKind::Local)
        .collect();
    assert_eq!(builds.len(), 2);
    assert!(builds
        .iter()
        .all(|e| e.command_line.contains("nix build") && e.edge.is_none()));

    // Two identical per-host copies onto the control edge.
    let copies: Vec<_> = log
        .iter()
        .filter(|e| e.kind == SimCommandKind::DataCopy)
        .collect();
    assert_eq!(copies.len(), 2);
    assert!(copies.iter().all(|e| e.edge == ctrl_edge));
    assert!(copies
        .iter()
        .all(|e| matches!(e.outcome, SimOutcome::Ok { .. })));

    // Both playbook runs ssh to the control node (SshControl on the same
    // edge), each limited to its own target.
    assert_both_playbooks_attempted(&log);
    assert!(playbook_events(&log).iter().all(|e| e.edge == ctrl_edge));

    // Virtual clock: 2 × 50 MiB at 100 MiB/s of aggregate transfer work.
    assert_eq!(sim.simulated_transfer_time(), Duration::from_secs(1));
}

// ─── Scenario 2: control node killed ────────────────────────────────────────

#[test]
fn killed_control_node_aborts_playbooks() {
    let sim = Arc::new(
        SimExecutor::builder()
            .hosts(SIM_HOSTS)
            .seed(SEED)
            .bandwidth(BandwidthDistribution::Uniform(BANDWIDTH))
            .transfer_bytes("nix copy", COPY_BYTES)
            .failure_schedule(FailureSchedule::KillNodeAtRound {
                node: NodeId(0), // ctrl01
                round: 0,        // dead from the first attempt on every edge to it
            })
            .on("nix build", ExecOutput::ok("/nix/store/ansible-env\n"))
            .on("nix copy", ExecOutput::ok(""))
            .on("ansible-playbook", ExecOutput::ok(PLAY_RECAP))
            .build(),
    );
    let report = run(&sim, &targets(), &AnsibleOptions::default());

    assert!(!report.is_success());
    // Both copies onto the dead control node failed (status 1 = copy-edge
    // failure); both playbook tasks were cancelled before running.
    assert!(report
        .failed
        .contains_key(&TaskId("copy-env:node01".into())));
    assert!(report
        .failed
        .contains_key(&TaskId("copy-env:node02".into())));
    assert!(report
        .cancelled
        .contains(&TaskId("run-playbook:node01".into())));
    assert!(report
        .cancelled
        .contains(&TaskId("run-playbook:node02".into())));
    // Builds are local: the network kill does not touch them.
    assert!(report
        .completed
        .contains(&TaskId("build-env:node01".into())));
    assert!(report
        .completed
        .contains(&TaskId("build-env:node02".into())));

    let log = sim.invocation_log();
    // Zero playbook attempts: no ssh command ever reached the executor.
    assert!(
        !log.iter()
            .any(|e| e.command_line.contains("ansible-playbook")),
        "no playbook may be attempted against a dead control node"
    );
    let copies: Vec<_> = log
        .iter()
        .filter(|e| e.kind == SimCommandKind::DataCopy)
        .collect();
    assert_eq!(copies.len(), 2);
    assert!(copies
        .iter()
        .all(|e| matches!(e.outcome, SimOutcome::Failed { status: 1, .. })));
    // Failed copies transfer nothing.
    assert_eq!(sim.simulated_transfer_time(), Duration::ZERO);
}

// ─── Scenario 3: one playbook fails, the other continues ────────────────────

#[test]
fn one_target_playbook_fails_others_continue() {
    // node02's playbook exits 1. Both playbook runs share the control edge,
    // so an edge-keyed failure schedule can't tell them apart — script the
    // per-COMMAND failure as a response rule, placed FIRST (first match
    // wins).
    let sim = Arc::new(
        SimExecutor::builder()
            .hosts(SIM_HOSTS)
            .seed(SEED)
            .bandwidth(BandwidthDistribution::Uniform(BANDWIDTH))
            .transfer_bytes("nix copy", COPY_BYTES)
            .rule(Rule::containing_all(
                ["ansible-playbook", "node02"],
                ExecOutput::new(1, "", "FAILED - node02 unreachable"),
            ))
            .on("nix build", ExecOutput::ok("/nix/store/ansible-env\n"))
            .on("nix copy", ExecOutput::ok(""))
            .on("ansible-playbook", ExecOutput::ok(PLAY_RECAP))
            .build(),
    );
    let report = run(&sim, &targets(), &AnsibleOptions::default());

    // ContinueIndependent: node02's failure fails the report but not
    // node01's chain.
    assert!(!report.is_success());
    assert!(report
        .completed
        .contains(&TaskId("run-playbook:node01".into())));
    assert!(report
        .failed
        .contains_key(&TaskId("run-playbook:node02".into())));

    // Both playbooks WERE attempted: both reached the control edge.
    let log = sim.invocation_log();
    assert_both_playbooks_attempted(&log);
    // The scripted non-zero exit is an *executed* command, not a sim-level
    // failure — the schedule never fired, so nothing renders as
    // SimOutcome::Failed.
    assert!(!log
        .iter()
        .any(|e| matches!(e.outcome, SimOutcome::Failed { .. })));
}

// ─── Scenario 4: check mode runs every phase ────────────────────────────────

#[test]
fn check_mode_still_runs_all_phases() {
    let sim = Arc::new(make_sim());
    let opts = AnsibleOptions {
        check_mode: true,
        ..AnsibleOptions::default()
    };
    let report = run(&sim, &targets(), &opts);

    assert!(report.is_success(), "failed: {:?}", report.failed);
    assert_all_stages_completed(&report);

    let log = sim.invocation_log();
    // Build and copy phases still executed for both hosts.
    assert_eq!(
        log.iter()
            .filter(|e| e.kind == SimCommandKind::Local)
            .count(),
        2
    );
    assert_eq!(
        log.iter()
            .filter(|e| e.kind == SimCommandKind::DataCopy)
            .count(),
        2
    );
    // Every playbook invocation carries --check.
    let playbooks = playbook_events(&log);
    assert_eq!(playbooks.len(), 2);
    for e in &playbooks {
        assert!(
            e.command_line.contains("--check"),
            "check-mode invocation missing --check: {}",
            e.command_line
        );
    }
}

// ─── Scenario 5: determinism ────────────────────────────────────────────────

#[test]
fn deterministic_under_threads() {
    let run_once = || {
        let sim = Arc::new(make_sim());
        let report = run(&sim, &targets(), &AnsibleOptions::default());
        (sim, report)
    };
    let (sim_a, report_a) = run_once();
    let (sim_b, report_b) = run_once();

    assert!(report_a.is_success() && report_b.is_success());
    // Identical task-level outcomes.
    assert_eq!(report_a.completed, report_b.completed);
    assert_eq!(report_a.failed, report_b.failed);

    // Identical virtual clocks.
    assert_eq!(
        sim_a.simulated_transfer_time(),
        sim_b.simulated_transfer_time()
    );
    assert_eq!(sim_a.simulated_transfer_time(), Duration::from_secs(1));

    // Both copies AND both playbooks share the single control edge across
    // per-host chains, so the attempt-index binding is arrival-order
    // dependent. The harness's native assertion compares logs and
    // per-edge outcomes attempt-insensitively (multisets), so thread
    // interleaving cannot flake this.
    assert_deterministic_equivalence(&sim_a.invocation_log(), &sim_b.invocation_log());
}

/// With a single target the pipeline is one dependency-ordered chain, so
/// attempt indices on the control edge are stable and the harness's native
/// determinism assertion applies directly.
#[test]
fn deterministic_single_target_native_equivalence() {
    let one = vec!["node01".to_string()];
    let run_once = || {
        let sim = Arc::new(make_sim());
        let report = run(&sim, &one, &AnsibleOptions::default());
        (sim, report)
    };
    let (sim_a, report_a) = run_once();
    let (sim_b, report_b) = run_once();

    assert!(report_a.is_success() && report_b.is_success());
    assert_eq!(report_a.completed, report_b.completed);
    assert_eq!(report_a.failed, report_b.failed);
    assert_eq!(
        sim_a.simulated_transfer_time(),
        sim_b.simulated_transfer_time()
    );
    assert_deterministic_equivalence(&sim_a.invocation_log(), &sim_b.invocation_log());
}
