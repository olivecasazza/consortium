//! Miniature end-to-end: a tiny `DagBuilder` pipeline driven through
//! `Arc<SimExecutor>`, mirroring how the integrations' DAG tasks consume
//! the `"executor"` context state key. Exercises kill semantics,
//! dependent cancellation, and cross-run determinism at the DAG level.

use std::sync::Arc;

use consortium::dag::{
    CacheStrategy, DagBuilder, DagContext, DagExecutor, ErrorPolicy, FnTask, TaskId, TaskOutcome,
};
use consortium_fanout_sim::fixtures::FailureSchedule;
use consortium_fanout_sim::simexec::{
    assert_deterministic_equivalence, SimCommandKind, SimExecutor, SimOutcome,
};
use consortium_fanout_sim::{CompletionSchedule, NodeId};
use consortium_integration::exec::{CommandSpec, ExecOutput, Executor, SshTarget};

/// One local build stage, then `run` + `verify` per host — the same
/// stage→run-per-host shape the integrations' pipelines have.
fn build_pipeline(sim: &Arc<SimExecutor>) -> DagExecutor {
    let exec: Arc<dyn Executor> = sim.clone();
    let ctx = DagContext::new();
    ctx.set_state("executor", exec);

    let mut dag = DagBuilder::new();
    dag.add_task(
        "stage",
        FnTask::new("stage build", |ctx| {
            let exec = ctx.get_state::<Arc<dyn Executor>>("executor").unwrap();
            let spec = CommandSpec::new("nix").args(["build", ".#fleet", "--no-link"]);
            match exec.exec(&spec) {
                Ok(out) if out.success() => TaskOutcome::Success,
                Ok(out) => TaskOutcome::Failed(format!("stage exited {}", out.status)),
                Err(e) => TaskOutcome::Failed(e.to_string()),
            }
        }),
    );

    for host in ["node01", "node02"] {
        let run_host = host.to_string();
        dag.add_task(
            format!("run:{host}"),
            FnTask::new(format!("activate {host}"), move |ctx| {
                let exec = ctx.get_state::<Arc<dyn Executor>>("executor").unwrap();
                let spec = CommandSpec::new("activate").ssh(SshTarget::new("root", &run_host));
                match exec.exec(&spec) {
                    Ok(out) if out.success() => TaskOutcome::Success,
                    Ok(out) => {
                        TaskOutcome::Failed(format!("activate {run_host}: {}", out.stderr.trim()))
                    }
                    Err(e) => TaskOutcome::Failed(e.to_string()),
                }
            }),
        );
        dag.add_dep(format!("run:{host}"), "stage");

        let verify_host = host.to_string();
        dag.add_task(
            format!("verify:{host}"),
            FnTask::new(format!("healthcheck {host}"), move |ctx| {
                let exec = ctx.get_state::<Arc<dyn Executor>>("executor").unwrap();
                let spec =
                    CommandSpec::new("healthcheck").ssh(SshTarget::new("root", &verify_host));
                match exec.exec(&spec) {
                    Ok(out) if out.success() => TaskOutcome::Success,
                    Ok(out) => TaskOutcome::Failed(format!(
                        "healthcheck {verify_host}: {}",
                        out.stderr.trim()
                    )),
                    Err(e) => TaskOutcome::Failed(e.to_string()),
                }
            }),
        );
        dag.add_dep(format!("verify:{host}"), format!("run:{host}"));
    }

    dag.context(ctx);
    dag.build().unwrap()
}

fn pipeline_sim(schedule: FailureSchedule) -> SimExecutor {
    SimExecutor::builder()
        .hosts(["node01", "node02"])
        .failure_schedule(schedule)
        .on("nix build", ExecOutput::ok("/nix/store/abc-fleet\n"))
        .on("activate", ExecOutput::ok("activated\n"))
        .on("healthcheck", ExecOutput::ok("healthy\n"))
        .build()
}

#[test]
fn killed_host_fails_run_and_cancels_dependent() {
    let sim = Arc::new(pipeline_sim(FailureSchedule::KillNodeAtRound {
        node: NodeId(1), // node02
        round: 0,
    }));
    let report = build_pipeline(&sim).run().unwrap();

    // host1 path completes; host2's run fails and its dependent is
    // cancelled without ever issuing a command.
    assert!(report.completed.contains(&TaskId::from("stage")));
    assert!(report.completed.contains(&TaskId::from("run:node01")));
    assert!(report.completed.contains(&TaskId::from("verify:node01")));
    assert!(report.failed.contains_key(&TaskId::from("run:node02")));
    assert!(report.cancelled.contains(&TaskId::from("verify:node02")));

    let sentinel = sim.sentinel_node();
    let log = sim.invocation_log();

    // Exactly one ssh attempt went to node02: the failed `run`. The
    // cancelled `verify:node02` never reached the executor.
    let to_node02: Vec<_> = log
        .iter()
        .filter(|e| e.edge == Some((sentinel, NodeId(1))))
        .collect();
    assert_eq!(to_node02.len(), 1);
    assert!(to_node02[0].command_line.contains("activate"));
    assert!(matches!(
        to_node02[0].outcome,
        SimOutcome::Failed { status: 255, .. }
    ));
    assert!(!log
        .iter()
        .any(|e| e.command_line.contains("healthcheck") && e.edge == Some((sentinel, NodeId(1)))));

    // host1 saw two successful ssh control attempts (run + verify).
    let to_node01: Vec<_> = log
        .iter()
        .filter(|e| e.edge == Some((sentinel, NodeId(0))))
        .collect();
    assert_eq!(to_node01.len(), 2);
    assert!(to_node01
        .iter()
        .all(|e| matches!(e.outcome, SimOutcome::Ok { .. })));

    // The stage ran locally: no edge, no schedule involvement.
    let stage_events: Vec<_> = log
        .iter()
        .filter(|e| e.kind == SimCommandKind::Local)
        .collect();
    assert_eq!(stage_events.len(), 1);
    assert!(stage_events[0].command_line.contains("nix build"));
}

#[test]
fn healthy_pipeline_is_deterministic_across_runs() {
    let sim_a = Arc::new(pipeline_sim(FailureSchedule::None));
    let report_a = build_pipeline(&sim_a).run().unwrap();
    assert!(report_a.is_success());

    let sim_b = Arc::new(pipeline_sim(FailureSchedule::None));
    let report_b = build_pipeline(&sim_b).run().unwrap();
    assert!(report_b.is_success());

    assert_deterministic_equivalence(&sim_a.invocation_log(), &sim_b.invocation_log());
    assert_eq!(
        sim_a.simulated_transfer_time(),
        sim_b.simulated_transfer_time()
    );
}

#[test]
fn seeded_completions_replay_exact_invocations() {
    let run = |order: &mut CompletionSchedule| {
        let sim = Arc::new(pipeline_sim(FailureSchedule::None));
        let report = build_pipeline(&sim).run_simulated(order).unwrap();
        assert!(report.is_success());
        sim.invocation_log()
    };
    let mut generated = CompletionSchedule::seeded(17);
    let log = run(&mut generated);
    let mut repeated = CompletionSchedule::seeded(17);
    assert_eq!(run(&mut repeated), log);
    assert_eq!(repeated.trace(), generated.trace());
    let mut replay = CompletionSchedule::replay(generated.trace().to_vec());
    assert_eq!(run(&mut replay), log);
    assert_eq!(replay.trace(), generated.trace());
}

#[test]
fn replay_rejects_unknown_blocked_short_and_extra_choices() {
    let complete = [
        "stage",
        "run:node02",
        "verify:node02",
        "run:node01",
        "verify:node01",
    ];
    let cases = [
        (vec!["unknown"], "unknown"),
        (vec!["verify:node01"], "verify:node01"),
        (vec!["stage"], "run:node01"),
        ([&complete[..], &["extra"]].concat(), "extra"),
    ];
    for (script, diagnostic) in cases {
        let sim = Arc::new(pipeline_sim(FailureSchedule::None));
        let mut order = CompletionSchedule::replay(script.into_iter().map(str::to_owned).collect());
        let error = build_pipeline(&sim).run_simulated(&mut order).unwrap_err();
        assert!(error.to_string().contains(diagnostic), "{error}");
    }
}

#[test]
fn fail_fast_drains_dispatched_peers_but_not_group_waiters() {
    let make = || {
        let mut dag = DagBuilder::new();
        dag.error_policy(ErrorPolicy::FailFast);
        dag.concurrency_group("limited", 1);
        dag.add_task(
            "fail",
            FnTask::new("fail", |_| TaskOutcome::Failed("offline".into())),
        );
        dag.add_task(
            "queued",
            FnTask::new("queued", |_| panic!("never admitted")),
        );
        dag.assign_group("fail", "limited");
        dag.assign_group("queued", "limited");
        dag.add_task("peer", FnTask::new("peer", |_| TaskOutcome::Success));
        dag.build().unwrap()
    };
    let mut invalid = CompletionSchedule::replay(vec!["queued".into()]);
    assert!(make().run_simulated(&mut invalid).is_err());
    let mut order = CompletionSchedule::replay(vec!["fail".into(), "peer".into()]);
    let report = make().run_simulated(&mut order).unwrap();
    assert_eq!(report.failed[&TaskId::from("fail")], "offline");
    assert!(report.completed.contains(&TaskId::from("peer")));
    assert_eq!(report.cancelled, [TaskId::from("queued")].into());
}

#[test]
fn replay_starts_after_a_cached_dependency_and_reads_its_output() {
    struct BuildCache;
    impl CacheStrategy for BuildCache {
        fn check(&self, id: &TaskId, _: &DagContext) -> Option<Box<dyn std::any::Any + Send>> {
            (id.0 == "build").then(|| Box::new("artifact") as _)
        }
    }
    let mut dag = DagBuilder::new();
    dag.cache(BuildCache);
    dag.add_task("build", FnTask::new("build", |_| panic!("cached task ran")));
    dag.add_task(
        "deploy",
        FnTask::new("deploy", |ctx| {
            if ctx.get_output::<&str>(&TaskId::from("build")) == Some("artifact") {
                TaskOutcome::Success
            } else {
                TaskOutcome::Failed("cached artifact unavailable to deploy".into())
            }
        }),
    );
    dag.add_dep("deploy", "build");
    let mut order = CompletionSchedule::replay(vec!["deploy".into()]);
    let report = dag.build().unwrap().run_simulated(&mut order).unwrap();
    assert!(report.is_success(), "{:?}", report.failed);
    assert!(report.skipped.contains(&TaskId::from("build")));
    assert!(report.completed.contains(&TaskId::from("deploy")));
}
