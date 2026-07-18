//! Sim tests for the slurm submit pipeline under [`SimExecutor`].
//!
//! Runs `submit_job` against a seed-pinned simulated fleet (one submit node
//! plus the build-host sentinel) and asserts pipeline semantics from the
//! `DagReport` and edge/kind-filtered views of the invocation log — never
//! from log order, which worker-thread interleaving makes nondeterministic.
//!
//! Rule-matching notes (substring, first match wins):
//!
//! - Every ssh rendering contains `cat` (via `-oPasswordAuthentication`), so
//!   the collect rule matches on the unique output pattern filename instead.
//! - `ssh` is a substring of `ssh-ng://`, so no broad `ssh` rule may appear
//!   before the nix rules; the rules below are phase-specific and do not
//!   overlap, so order among them is not load-bearing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use consortium::dag::{DagReport, TaskId};
use consortium_fanout_sim::fixtures::{BandwidthDistribution, FailureSchedule};
use consortium_fanout_sim::NodeId;
use consortium_fanout_sim::simexec::{
    assert_deterministic_equivalence, SimCommandKind, SimExecutor, SimOutcome,
};
use consortium_integration::exec::{ExecOutput, Executor};
use consortium_integration::fleet::{FleetConfig, SlurmFleetConfig};
use consortium_slurm::{submit_job, SubmitOptions};

const SUBMIT_HOST: &str = "submit01";
const SUBMIT_USER: &str = "root";
const JOB: &str = "train";
const SCRIPT: &str = "train.sh";
/// Scripted slurm job id (parsed out of the sbatch output).
const JOB_ID: u64 = 12345;
/// Size of the nix env closure copied to the submit node (100 MiB).
const ENV_BYTES: u64 = 100 * 1024 * 1024;
/// Gibibyte-per-100s bandwidth used by the slow-uplink scenario (1 MiB/s).
const SLOW_BYTES_SEC: u64 = 1024 * 1024;

// ── Shared fixtures ─────────────────────────────────────────────────────────

/// Minimal fleet config: `submit_job` only reads `slurm_config` and
/// `flake_uri`. `submit_node` must exactly match a `SimExecutor` hosts entry
/// — ssh control edges and the `ssh-ng://` copy edge resolve against it.
fn fleet_config() -> FleetConfig {
    FleetConfig {
        nodes: HashMap::new(),
        builders: HashMap::new(),
        flake_uri: ".".into(),
        ansible_config: None,
        slurm_config: Some(SlurmFleetConfig {
            submit_node: SUBMIT_HOST.into(),
            submit_user: SUBMIT_USER.into(),
            control_node: SUBMIT_HOST.into(),
            partitions: HashMap::new(),
        }),
        ray_config: None,
        skypilot_config: None,
    }
}

/// Full pipeline: wait for completion, then collect the output file.
fn full_opts() -> SubmitOptions {
    SubmitOptions {
        wait: true,
        collect: Some(format!("slurm-{JOB_ID}.out")),
        poll_interval: Duration::ZERO,
    }
}

/// Wait without collecting; zero poll interval (the first sacct poll happens
/// before any sleep, so scripted terminal states never wait anyway).
fn wait_opts() -> SubmitOptions {
    SubmitOptions {
        wait: true,
        collect: None,
        poll_interval: Duration::ZERO,
    }
}

/// Builder stage shared by every scenario: one submit node, pinned seed,
/// 100 MiB env copy. Rules script every success output the pipeline parses.
/// The collect rule keys on the pattern filename — matching on `cat` would
/// hit every ssh rendering via `-oPasswordAuthentication`.
fn base_sim() -> consortium_fanout_sim::simexec::SimExecutorBuilder {
    SimExecutor::builder()
        .hosts([SUBMIT_HOST])
        .seed(0x1234)
        .transfer_bytes("nix copy", ENV_BYTES)
        .on("nix build", ExecOutput::ok("/nix/store/env\n"))
        .on("nix copy", ExecOutput::ok(""))
        .on("sbatch", ExecOutput::ok("Submitted batch job 12345\n"))
        .on("sacct", ExecOutput::ok("COMPLETED\n"))
        .on("slurm-12345.out", ExecOutput::ok("results-data\n"))
}

/// The happy-path executor factory: a fresh executor per call so two runs of
/// the identical scenario can be compared for determinism.
fn make_happy_sim() -> SimExecutor {
    base_sim()
        .bandwidth(BandwidthDistribution::Uniform(100 * 1024 * 1024))
        .build()
}

fn run_submit(sim: &Arc<SimExecutor>, opts: &SubmitOptions) -> DagReport {
    let exec: Arc<dyn Executor> = sim.clone();
    submit_job(exec, &fleet_config(), JOB, SCRIPT, Some("gpu"), opts)
        .expect("submit_job itself should not error")
}

/// All invocations classified onto the edge sentinel → submit node.
fn submit_edge_events(sim: &Arc<SimExecutor>) -> Vec<consortium_fanout_sim::simexec::SimEvent> {
    let sentinel = sim.sentinel_node();
    let submit = sim.host_node(SUBMIT_HOST).expect("submit host is in the fleet");
    sim.invocation_log()
        .into_iter()
        .filter(|e| e.edge == Some((sentinel, submit)))
        .collect()
}

// ── Scenarios ───────────────────────────────────────────────────────────────

/// Full pipeline succeeds: build → copy → sbatch → sacct COMPLETED → collect.
/// Every phase lands in `report.completed`, the env copy is the single
/// DataCopy edge to the submit node, and the collect `cat` is invoked.
#[test]
fn happy_path_full_pipeline() {
    let sim = Arc::new(make_happy_sim());
    let report = run_submit(&sim, &full_opts());

    assert!(report.is_success(), "failures: {:?}", report.failed);
    for phase in [
        format!("build-job-env:{JOB}"),
        format!("copy-job-env:{JOB}"),
        format!("slurm-submit:{JOB}"),
        format!("slurm-wait:{JOB}"),
        format!("slurm-collect:{JOB}"),
    ] {
        assert!(
            report.completed.contains(&TaskId(phase.clone())),
            "phase {phase} not completed: {:?}",
            report.completed
        );
    }

    let log = sim.invocation_log();

    // The build is local work: no edge, no failure-schedule involvement.
    let builds: Vec<_> = log
        .iter()
        .filter(|e| e.kind == SimCommandKind::Local && e.command_line.contains("nix build"))
        .collect();
    assert_eq!(builds.len(), 1);
    assert!(builds[0].edge.is_none());

    // Exactly one DataCopy edge, sentinel → submit node, and it succeeded.
    let copies: Vec<_> = submit_edge_events(&sim)
        .into_iter()
        .filter(|e| e.kind == SimCommandKind::DataCopy)
        .collect();
    assert_eq!(copies.len(), 1, "expected exactly one env copy: {copies:?}");
    assert!(copies[0].command_line.contains("--to ssh-ng://root@submit01"));
    assert!(matches!(copies[0].outcome, SimOutcome::Ok { .. }));

    // sbatch and the sacct poll are ssh control edges to the submit node.
    let ssh: Vec<_> = submit_edge_events(&sim)
        .into_iter()
        .filter(|e| e.kind == SimCommandKind::SshControl)
        .collect();
    assert!(ssh.iter().any(|e| e.command_line.contains("sbatch")));
    assert!(ssh.iter().any(|e| e.command_line.contains("sacct")));

    // DagReport does not carry the DAG context, so the collected content is
    // pinned via its invocation: one ssh control event cats the pattern file.
    let collects: Vec<_> = ssh
        .iter()
        .filter(|e| e.command_line.contains("slurm-12345.out"))
        .collect();
    assert_eq!(collects.len(), 1, "collect cat not invoked: {ssh:?}");
    assert!(matches!(collects[0].outcome, SimOutcome::Ok { .. }));

    // Virtual clock sanity: 100 MiB over a uniform 100 MiB/s link ≈ 1s.
    let t = sim.simulated_transfer_time().as_secs_f64();
    assert!((0.5..=1.5).contains(&t), "transfer time off: {t}s");
}

/// A 1 MiB/s uplink still succeeds — the sim clock is virtual, so a 100 MiB
/// env copy "takes" ~100s without any real waiting.
#[test]
fn slow_submit_uplink_still_succeeds() {
    let sim = Arc::new(
        base_sim()
            .bandwidth(BandwidthDistribution::Uniform(SLOW_BYTES_SEC))
            .build(),
    );
    let report = run_submit(&sim, &wait_opts());

    assert!(report.is_success(), "failures: {:?}", report.failed);

    // ENV_BYTES / SLOW_BYTES_SEC = 100 MiB / 1 MiB/s = exactly 100s.
    let t = sim.simulated_transfer_time().as_secs_f64();
    assert!(
        (99.5..=100.5).contains(&t),
        "100 MiB over 1 MiB/s should take ~100s, got {t}s"
    );
}

/// Submit node killed at round 0: the env copy (DataCopy, sentinel → submit)
/// fails with status 1, FailFast aborts the pipeline, and sbatch/sacct are
/// never attempted.
#[test]
fn killed_submit_node_aborts_before_sbatch() {
    let sim = Arc::new(
        base_sim()
            .failure_schedule(FailureSchedule::KillNodeAtRound {
                node: NodeId(0), // submit01
                round: 0,
            })
            .build(),
    );
    let report = run_submit(&sim, &full_opts());

    assert!(!report.is_success());
    let copy_id = TaskId(format!("copy-job-env:{JOB}"));
    assert!(
        report.failed.contains_key(&copy_id),
        "copy task should be the failure: {:?}",
        report.failed
    );
    // The local build ran before the copy and is unaffected by the kill.
    assert!(report.completed.contains(&TaskId(format!("build-job-env:{JOB}"))));

    let log = sim.invocation_log();

    // The dead node rendered the copy as an ordinary status-1 failure.
    let copies: Vec<_> = log
        .iter()
        .filter(|e| e.kind == SimCommandKind::DataCopy)
        .collect();
    assert_eq!(copies.len(), 1);
    assert!(
        matches!(copies[0].outcome, SimOutcome::Failed { status: 1, .. }),
        "copy should fail with status 1: {:?}",
        copies[0].outcome
    );

    // FailFast: nothing downstream of the copy was ever invoked — no sbatch,
    // no sacct, no collect cat, no ssh control edge at all.
    assert!(!log.iter().any(|e| e.command_line.contains("sbatch")));
    assert!(!log.iter().any(|e| e.command_line.contains("sacct")));
    assert!(!log.iter().any(|e| e.command_line.contains("slurm-12345.out")));
    assert!(!log.iter().any(|e| e.kind == SimCommandKind::SshControl));
}

/// sacct reports a terminal failure state: the wait task fails, and with it
/// the pipeline.
#[test]
fn sacct_reports_failed_state() {
    let sim = Arc::new(
        SimExecutor::builder()
            .hosts([SUBMIT_HOST])
            .seed(0x1234)
            .transfer_bytes("nix copy", ENV_BYTES)
            .on("nix build", ExecOutput::ok("/nix/store/env\n"))
            .on("nix copy", ExecOutput::ok(""))
            .on("sbatch", ExecOutput::ok("Submitted batch job 12345\n"))
            .on("sacct", ExecOutput::ok("FAILED\n"))
            .build(),
    );
    let report = run_submit(&sim, &wait_opts());

    assert!(!report.is_success());
    let wait_id = TaskId(format!("slurm-wait:{JOB}"));
    let message = report
        .failed
        .get(&wait_id)
        .expect("slurm-wait should be the failure");
    assert!(message.contains("FAILED"), "unexpected failure: {message}");

    // Everything upstream of the wait completed; the sbatch parse fed the
    // job id into the (failed) sacct poll.
    for phase in [
        format!("build-job-env:{JOB}"),
        format!("copy-job-env:{JOB}"),
        format!("slurm-submit:{JOB}"),
    ] {
        assert!(report.completed.contains(&TaskId(phase.clone())));
    }
    let log = sim.invocation_log();
    assert!(log.iter().any(|e| e.command_line.contains("sbatch")));
    assert!(log.iter().any(|e| e.command_line.contains("sacct")));
}

/// Two identical runs of the happy path — each on a fresh executor from one
/// factory — produce equivalent invocation logs, equal virtual clocks, and
/// identical report task sets despite worker-thread interleaving.
#[test]
fn deterministic_under_threads() {
    let sim_a = Arc::new(make_happy_sim());
    let report_a = run_submit(&sim_a, &full_opts());

    let sim_b = Arc::new(make_happy_sim());
    let report_b = run_submit(&sim_b, &full_opts());

    assert!(report_a.is_success() && report_b.is_success());
    assert_eq!(report_a.completed, report_b.completed);
    assert_eq!(report_a.failed, report_b.failed);
    assert_eq!(
        sim_a.simulated_transfer_time(),
        sim_b.simulated_transfer_time()
    );
    assert_deterministic_equivalence(&sim_a.invocation_log(), &sim_b.invocation_log());
}
