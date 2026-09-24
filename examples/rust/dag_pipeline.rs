//! DAG pipelines: explicit task graphs and staged multi-resource pipelines.
//!
//! `consortium::dag` is the execution engine underneath every consortium
//! integration. Two construction styles:
//!
//!   1. `DagBuilder` — explicit tasks (`ShellTask`, `FnTask`) and explicit
//!      dependencies (`add_dep(dependent, dependency)`), with a worker pool
//!      and an error policy.
//!   2. `StageBuilder` — the "apply N stages to M resources" pattern used by
//!      deploy pipelines: task ids are auto-generated as `stage:resource`
//!      and each resource flows through the stages independently.
//!
//! The staged half intentionally fails one resource's last stage to show
//! `ErrorPolicy::ContinueIndependent` semantics: the failing host stops,
//! every other host runs to completion, and the report records both.
//!
//! Prerequisites: none — fully offline.
//!
//! Run with:
//!   cargo run -p consortium-examples --example dag_pipeline

use consortium::dag::{
    DagBuilder, DagReport, ErrorPolicy, FixedPool, FnTask, ShellTask, StageBuilder, TaskId,
    TaskOutcome,
};

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn print_report(name: &str, report: &DagReport) {
    let mut completed: Vec<_> = report.completed.iter().map(|t| t.0.clone()).collect();
    completed.sort();
    let mut skipped: Vec<_> = report.skipped.iter().map(|t| t.0.clone()).collect();
    skipped.sort();
    let mut failed: Vec<_> = report
        .failed
        .iter()
        .map(|(t, e)| (t.0.clone(), e.clone()))
        .collect();
    failed.sort();
    let mut cancelled: Vec<_> = report.cancelled.iter().map(|t| t.0.clone()).collect();
    cancelled.sort();

    println!("--- {name} ---");
    println!("success   : {}", report.is_success());
    println!("completed : {completed:?}");
    println!("skipped   : {skipped:?}");
    println!("failed    : {failed:?}");
    println!("cancelled : {cancelled:?}");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("DagBuilder — explicit graph (build -> package -> deploy)");
    let mut b = DagBuilder::new();
    b.add_task(
        "build",
        ShellTask {
            command: "echo compiling".into(),
            description: "compile sources".into(),
            resource: None,
        },
    );
    b.add_task(
        "package",
        ShellTask {
            command: "echo packaging".into(),
            description: "package artifacts".into(),
            resource: None,
        },
    );
    // FnTask runs an in-process closure instead of a shell command; it can
    // also pass values to downstream tasks through the shared DagContext.
    b.add_task(
        "deploy",
        FnTask::new("deploy artifacts", |ctx| {
            ctx.set_output(TaskId::from("deploy"), "v1.2.3 deployed".to_string());
            println!("deploying");
            TaskOutcome::Success
        }),
    );
    b.add_dep("package", "build"); // package waits for build
    b.add_dep("deploy", "package"); // deploy waits for package
    b.error_policy(ErrorPolicy::FailFast)
        .pool(FixedPool::new(4));

    let report = b.build()?.run()?;
    print_report("explicit graph", &report);

    banner("StageBuilder — 2 stages x 2 hosts, one staged failure");
    let report = StageBuilder::new()
        .resources(vec!["web1".into(), "web2".into()])
        .stage("build", Some(4), |host| {
            Box::new(ShellTask {
                command: format!("echo build {host}"),
                description: format!("build {host}"),
                resource: None,
            })
        })
        // web2's deploy fails on purpose: with ContinueIndependent the
        // failure cancels only web2's remaining stages (none left here),
        // while web1 completes the whole chain.
        .stage("deploy", Some(2), |host| {
            let host = host.to_string();
            Box::new(FnTask::new(format!("deploy {host}"), move |_ctx| {
                if host == "web2" {
                    TaskOutcome::Failed(format!("{host}: connection refused (simulated)"))
                } else {
                    println!("deploying {host}");
                    TaskOutcome::Success
                }
            }))
        })
        .error_policy(ErrorPolicy::ContinueIndependent)
        .build()?
        .run()?;
    print_report("staged pipeline", &report);

    println!("\nnote: task ids in a staged pipeline are auto-generated as");
    println!("\"stage:resource\" — e.g. build:web1, deploy:web2 above.");

    Ok(())
}
