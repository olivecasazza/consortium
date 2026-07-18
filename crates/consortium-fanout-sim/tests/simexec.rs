//! Miniature end-to-end: a tiny `DagBuilder` pipeline driven through
//! `Arc<SimExecutor>`, mirroring how the integrations' DAG tasks consume
//! the `"executor"` context state key. Exercises kill semantics,
//! dependent cancellation, and cross-run determinism at the DAG level.

use std::sync::Arc;

use consortium::dag::{DagBuilder, DagContext, DagReport, FnTask, TaskId, TaskOutcome};
use consortium_fanout_sim::fixtures::FailureSchedule;
use consortium_fanout_sim::simexec::{
    assert_deterministic_equivalence, SimCommandKind, SimExecutor, SimOutcome,
};
use consortium_integration::exec::{CommandSpec, ExecOutput, Executor, SshTarget};
use consortium_nix::cascade::NodeId;

/// One local build stage, then `run` + `verify` per host — the same
/// stage→run-per-host shape the integrations' pipelines have.
fn build_pipeline(sim: &Arc<SimExecutor>) -> DagReport {
    let exec: Arc<dyn Executor> = sim.clone();
    let ctx = DagContext::new();
    ctx.set_state("executor", exec);

    let mut dag = DagBuilder::new();
    dag.add_task("stage", FnTask::new("stage build", |ctx| {
        let exec = ctx.get_state::<Arc<dyn Executor>>("executor").unwrap();
        let spec = CommandSpec::new("nix").args(["build", ".#fleet", "--no-link"]);
        match exec.exec(&spec) {
            Ok(out) if out.success() => TaskOutcome::Success,
            Ok(out) => TaskOutcome::Failed(format!("stage exited {}", out.status)),
            Err(e) => TaskOutcome::Failed(e.to_string()),
        }
    }));

    for host in ["node01", "node02"] {
        let run_host = host.to_string();
        dag.add_task(
            format!("run:{host}"),
            FnTask::new(format!("activate {host}"), move |ctx| {
                let exec = ctx.get_state::<Arc<dyn Executor>>("executor").unwrap();
                let spec = CommandSpec::new("activate").ssh(SshTarget::new("root", &run_host));
                match exec.exec(&spec) {
                    Ok(out) if out.success() => TaskOutcome::Success,
                    Ok(out) => TaskOutcome::Failed(format!(
                        "activate {run_host}: {}",
                        out.stderr.trim()
                    )),
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
                let spec = CommandSpec::new("healthcheck").ssh(SshTarget::new("root", &verify_host));
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
    dag.build().unwrap().run().unwrap()
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
    let report = build_pipeline(&sim);

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
        .any(|e| e.command_line.contains("healthcheck")
            && e.edge == Some((sentinel, NodeId(1)))));

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
    let report_a = build_pipeline(&sim_a);
    assert!(report_a.is_success());

    let sim_b = Arc::new(pipeline_sim(FailureSchedule::None));
    let report_b = build_pipeline(&sim_b);
    assert!(report_b.is_success());

    assert_deterministic_equivalence(&sim_a.invocation_log(), &sim_b.invocation_log());
    assert_eq!(
        sim_a.simulated_transfer_time(),
        sim_b.simulated_transfer_time()
    );
}
