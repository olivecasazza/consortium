//! SimExecutor-based sim tests for the ray job pipeline.
//!
//! Unlike the ssh-based integrations, every ray-pipeline command runs
//! LOCALLY: `nix build` builds the env on the build host, and the `ray`
//! CLI talks plain HTTP to the cluster head (`ray job submit --address
//! http://head:port ...`, `ray job status --address ... <id>`) — no ssh,
//! no ssh-ng. SimExecutor therefore classifies all three commands as
//! `Local`: no simulated edges, the failure schedule does not apply, and
//! all outputs come from the scripted rules. What the sim harness still
//! buys this crate:
//!
//! - the shared sim-test idiom (seed-pinned builder, invocation log,
//!   determinism-equivalence assertions),
//! - scripted head-endpoint failure modes (submit refused, terminal
//!   FAILED, status endpoint flapping into a timeout) via
//!   first-match-wins rules,
//! - invocation-count/order assertions that prove pipeline control flow
//!   (e.g. a failed submit never reaches the status poller).
//!
//! Rule-matching note: `job submit` and `job status` share the `job`
//! prefix, and a status line embeds the `raysubmit_<id>` job id (which
//! itself contains "submit") — so every rule matches on the full
//! `job submit` / `job status` tokens, and failure modes use
//! `Rule::containing_all` placed BEFORE the broad success rules (first
//! match wins).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use consortium::dag::{DagReport, TaskId};
use consortium_fanout_sim::simexec::{
    assert_deterministic_equivalence, assert_scenario_replays, SimCommandKind, SimEvent,
    SimExecutor,
};
use consortium_fanout_sim::CompletionSchedule;
use consortium_integration::exec::{ExecOutput, Rule};
use consortium_integration::fleet::{DeploymentNode, FleetConfig, ProfileType, RayFleetConfig};
use consortium_integration::grammar::Integration;
use consortium_ray::{submit_job, RayIntegration, RayOptions};

// ── Shared fixtures ─────────────────────────────────────────────────────────

const SEED: u64 = 0x1234;
const JOB_NAME: &str = "train";
const ENTRYPOINT: &str = "python train.py";
const SUBMIT_ACK: &str = "Job 'raysubmit_abc123' submitted successfully\n";

/// Instant-poll options: the scripted head answers SUCCEEDED on the first
/// status check, so no timeout is needed on the happy path.
const TEST_OPTS: RayOptions = RayOptions {
    wait: true,
    poll_interval: Duration::from_millis(1),
    timeout: None,
};

fn fleet_config() -> FleetConfig {
    let mut nodes = HashMap::new();
    nodes.insert(
        "node01".to_string(),
        DeploymentNode {
            name: "node01".into(),
            target_host: "node01".into(),
            target_user: "root".into(),
            target_port: None,
            system: "x86_64-linux".into(),
            profile_type: ProfileType::Nixos,
            build_on_target: false,
            tags: vec![],
            drv_path: None,
            toplevel: None,
        },
    );
    FleetConfig {
        nodes,
        builders: HashMap::new(),
        flake_uri: ".".into(),
        ansible_config: None,
        slurm_config: None,
        ray_config: Some(RayFleetConfig {
            head_address: "ray-head.local".into(),
            dashboard_port: 8265,
            kubernetes: false,
            worker_groups: HashMap::new(),
        }),
        skypilot_config: None,
    }
}

/// The happy-path head endpoint: env builds, submit yields a job id, the
/// first status poll reports SUCCEEDED. The fleet is left EMPTY: every
/// command classifies `Local`, so no dummy host is needed.
fn make_happy_sim() -> SimExecutor {
    SimExecutor::builder()
        .seed(SEED)
        .on("nix build", ExecOutput::ok("/nix/store/ray-env\n"))
        .on("job submit", ExecOutput::ok(SUBMIT_ACK))
        .on("job status", ExecOutput::ok("Status: SUCCEEDED\n"))
        .build()
}

fn task_id(prefix: &str) -> TaskId {
    TaskId(format!("{prefix}:{JOB_NAME}"))
}

fn run_submit(
    sim: Arc<SimExecutor>,
    opts: &RayOptions,
    order: Option<&mut CompletionSchedule>,
) -> DagReport {
    let config = fleet_config();
    let Some(order) = order else {
        return submit_job(sim, &config, JOB_NAME, ENTRYPOINT, opts)
            .expect("pipeline failures surface in the report, not as Err");
    };
    let plan = RayIntegration
        .plan(&config, &opts.request(JOB_NAME, ENTRYPOINT))
        .expect("valid ray job request");
    RayIntegration
        .execute_scheduled(sim, &config, &plan, Some(order))
        .expect("pipeline failures surface in the report, not as Err")
        .dag
}

/// Count invocations whose rendered command line contains `needle`.
fn count_invocations(log: &[SimEvent], needle: &str) -> usize {
    log.iter()
        .filter(|e| e.command_line.contains(needle))
        .count()
}

// ── Sim scenarios ───────────────────────────────────────────────────────────

#[test]
fn happy_path_submit_and_succeeded() {
    assert_scenario_replays(SEED, check_happy_path);
}

fn check_happy_path(order: Option<&mut CompletionSchedule>) -> Vec<SimEvent> {
    let sim = Arc::new(make_happy_sim());
    let report = run_submit(sim.clone(), &TEST_OPTS, order);

    assert!(report.is_success(), "report: {report:?}");
    for prefix in ["build-ray-env", "ray-submit", "ray-wait"] {
        assert!(
            report.completed.contains(&task_id(prefix)),
            "missing completed task {prefix}:{JOB_NAME}"
        );
    }

    // The pipeline is a strictly linear dependency chain, so the log order
    // is deterministic even on a multi-threaded DAG runner.
    let log = sim.invocation_log();
    let idx_of = |needle: &str| {
        log.iter()
            .position(|e| e.command_line.contains(needle))
            .unwrap_or_else(|| panic!("no invocation containing {needle:?}"))
    };
    let i_build = idx_of("nix build");
    let i_submit = idx_of("job submit");
    let i_status = idx_of("job status");
    assert!(
        i_build < i_submit && i_submit < i_status,
        "expected nix build → job submit → job status order; log: {log:#?}"
    );

    // Exactly one status poll: SUCCEEDED is terminal on the first check.
    assert_eq!(count_invocations(&log, "job status"), 1);

    // Every command in this pipeline is classified Local (the ray CLI
    // talks HTTP to the head; no ssh/ssh-ng): no edges, no transfer work.
    assert!(
        log.iter()
            .all(|e| e.kind == SimCommandKind::Local && e.edge.is_none()),
        "ray pipeline commands should all be Local: {log:#?}"
    );
    assert_eq!(sim.simulated_transfer_time(), Duration::ZERO);
    log
}

#[test]
fn no_wait_submits_and_returns() {
    assert_scenario_replays(SEED, check_no_wait);
}

fn check_no_wait(order: Option<&mut CompletionSchedule>) -> Vec<SimEvent> {
    let sim = Arc::new(make_happy_sim());
    let opts = RayOptions {
        wait: false,
        ..TEST_OPTS
    };
    let report = run_submit(sim.clone(), &opts, order);
    let log = sim.invocation_log();

    assert!(report.is_success(), "report: {report:?}");
    assert!(report.completed.contains(&task_id("ray-submit")));
    assert!(
        !report.completed.contains(&task_id("ray-wait")),
        "wait: false must not schedule a wait task"
    );
    assert_eq!(
        count_invocations(&log, "job status"),
        0,
        "wait: false must never poll job status"
    );
    log
}

#[test]
fn submit_failure_skips_wait() {
    assert_scenario_replays(SEED, check_submit_failure);
}

fn check_submit_failure(order: Option<&mut CompletionSchedule>) -> Vec<SimEvent> {
    let sim = Arc::new(
        SimExecutor::builder()
            .seed(SEED)
            // Failure mode FIRST (first match wins): the head endpoint
            // refuses the submit.
            .rule(Rule::containing_all(
                ["job submit", "--address"],
                ExecOutput::new(1, "", "connection refused"),
            ))
            .on("nix build", ExecOutput::ok("/nix/store/ray-env\n"))
            .on("job status", ExecOutput::ok("Status: SUCCEEDED\n"))
            .build(),
    );
    let report = run_submit(sim.clone(), &TEST_OPTS, order);
    let log = sim.invocation_log();

    assert!(!report.is_success());
    let submit = task_id("ray-submit");
    assert!(
        report.failed.contains_key(&submit),
        "expected ray-submit failure: {report:?}"
    );
    assert!(report.failed[&submit].contains("connection refused"));
    assert_eq!(
        count_invocations(&log, "job status"),
        0,
        "a failed submit must never reach the status poller"
    );
    log
}

#[test]
fn job_failed_terminal_state() {
    assert_scenario_replays(SEED, check_failed_state);
}

fn check_failed_state(order: Option<&mut CompletionSchedule>) -> Vec<SimEvent> {
    let sim = Arc::new(
        SimExecutor::builder()
            .seed(SEED)
            .on("nix build", ExecOutput::ok("/nix/store/ray-env\n"))
            .on("job submit", ExecOutput::ok(SUBMIT_ACK))
            .on("job status", ExecOutput::ok("Status: FAILED\n"))
            .build(),
    );
    let report = run_submit(sim.clone(), &TEST_OPTS, order);
    let log = sim.invocation_log();

    assert!(!report.is_success());
    let wait = task_id("ray-wait");
    assert!(
        report.failed.contains_key(&wait),
        "expected ray-wait failure: {report:?}"
    );
    assert!(report.failed[&wait].contains("raysubmit_abc123"));
    assert_eq!(
        count_invocations(&log, "job status"),
        1,
        "a FAILED terminal state must fail fast, without re-polling"
    );
    log
}

#[test]
fn status_endpoint_flapping_then_timeout() {
    assert_scenario_replays(SEED, check_flapping_timeout);
}

fn check_flapping_timeout(order: Option<&mut CompletionSchedule>) -> Vec<SimEvent> {
    let sim = Arc::new(
        SimExecutor::builder()
            .seed(SEED)
            // Failure mode FIRST (first match wins): the status endpoint
            // stays unreachable — a non-zero exit, which the wait task
            // retries until the timeout fires.
            .rule(Rule::containing_all(
                ["job status", "--address"],
                ExecOutput::new(1, "", "head node unreachable"),
            ))
            .on("nix build", ExecOutput::ok("/nix/store/ray-env\n"))
            .on("job submit", ExecOutput::ok(SUBMIT_ACK))
            .build(),
    );
    let opts = RayOptions {
        wait: true,
        poll_interval: Duration::from_millis(1),
        timeout: Some(Duration::from_millis(50)),
    };
    let start = Instant::now();
    let report = run_submit(sim.clone(), &opts, order);
    let elapsed = start.elapsed();
    let log = sim.invocation_log();

    assert!(!report.is_success());
    let wait = task_id("ray-wait");
    assert!(
        report.failed.contains_key(&wait),
        "expected ray-wait failure: {report:?}"
    );
    assert!(
        report.failed[&wait].contains("timed out"),
        "expected a timeout failure: {report:?}"
    );
    assert!(
        count_invocations(&log, "job status") > 1,
        "non-zero status exits are retried until the timeout; log: {log:#?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the timeout spin should be fast; took {elapsed:?}"
    );
    log
}

#[test]
fn deterministic_runs() {
    let config = fleet_config();

    let sim_a = Arc::new(make_happy_sim());
    let report_a = submit_job(sim_a.clone(), &config, JOB_NAME, ENTRYPOINT, &TEST_OPTS)
        .expect("run A should not error");
    let sim_b = Arc::new(make_happy_sim());
    let report_b = submit_job(sim_b.clone(), &config, JOB_NAME, ENTRYPOINT, &TEST_OPTS)
        .expect("run B should not error");

    assert_eq!(report_a.completed, report_b.completed);
    assert_eq!(report_a.failed, report_b.failed);
    assert_deterministic_equivalence(&sim_a.invocation_log(), &sim_b.invocation_log());
    assert_eq!(
        sim_a.simulated_transfer_time(),
        sim_b.simulated_transfer_time()
    );
    // All commands are Local → no data copies → no simulated transfer work.
    assert_eq!(sim_a.simulated_transfer_time(), Duration::ZERO);
}
