//! Integration: SkyPilot cluster launch (consortium-skypilot) without infrastructure.
//!
//! `consortium_skypilot::launch_task` runs a linear FailFast DAG, entirely
//! local (the `sky` CLI talks to the cloud provider itself):
//!
//!   build-sky-env -> sky-launch -> [sky-down]
//!
//! The task yaml is written to a temp file so its path shows up in the
//! rendered command line. `SkyOptions { teardown: false }` skips the
//! sky-down stage (leave the cluster running); the default tears it down.
//! Requires `skypilotConfig` in the fleet file — without it, `launch_task`
//! returns `SkypilotError::NoConfig` before issuing any command.
//!
//! Prerequisites: none — fully offline (ScriptedExecutor).
//!
//! Run with:
//!   cargo run -p consortium-examples --example integration_skypilot

use std::path::Path;
use std::sync::Arc;

use consortium_integration::exec::{ExecOutput, Executor, ScriptedExecutor};
use consortium_integration::fleet::FleetConfig;
use consortium_skypilot::{launch_task, SkyOptions};

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("Load fleet configuration");
    let fleet_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("inventories/fleet.json");
    let config = FleetConfig::from_file(&fleet_path)?;
    let sky = config
        .skypilot_config
        .as_ref()
        .expect("fleet.json has skypilotConfig");
    println!(
        "cloud: {} / {} ({})",
        sky.cloud,
        sky.region.as_deref().unwrap_or("?"),
        sky.instance_type.as_deref().unwrap_or("?")
    );

    banner("Script the executor");
    let scripted = Arc::new(
        ScriptedExecutor::new()
            .on("nix build", ExecOutput::ok("/nix/store/sky-env\n"))
            .on("sky launch", ExecOutput::ok("Cluster launched: ml-demo\n"))
            .on("sky down", ExecOutput::ok("Terminating cluster ml-demo\n")),
    );
    let exec: Arc<dyn Executor> = scripted.clone();

    banner("launch_task — train cluster with teardown");
    let task_yaml = r#"
name: ml-demo
resources:
  cloud: aws
  instance_type: p3.2xlarge
setup: pip install torch
run: python train.py --epochs 1
"#;
    // SkyOptions::default() == SkyOptions { teardown: true }.
    // Set teardown: false to skip the sky-down stage and keep the cluster.
    let report = launch_task(exec, &config, "ml-demo", task_yaml, &SkyOptions::default())?;

    banner("DagReport");
    println!("is_success: {}", report.is_success());
    let mut completed: Vec<_> = report.completed.iter().map(|t| t.0.clone()).collect();
    completed.sort();
    println!("completed : {completed:?}");
    println!(
        "failed    : {:?}",
        report
            .failed
            .iter()
            .map(|(t, e)| format!("{}: {}", t.0, e))
            .collect::<Vec<_>>()
    );

    banner("Commands that would have run (recorded invocations)");
    for cmd in scripted.invocations() {
        println!("  {cmd}");
    }
    scripted.assert_invoked_containing("sky launch");
    scripted.assert_invoked_containing("sky down");

    Ok(())
}
