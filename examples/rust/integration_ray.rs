//! Integration: Ray job submission (consortium-ray) without infrastructure.
//!
//! `consortium_ray::submit_job` runs a linear FailFast DAG, entirely local
//! (the `ray` CLI talks to the cluster's `headAddress:dashboardPort`):
//!
//!   build-ray-env -> ray-submit -> [ray-wait]
//!
//! `ray-submit` parses the job id out of the CLI output; `ray-wait` polls
//! `ray job status` until a terminal state (SUCCEEDED/FAILED/STOPPED).
//! Requires `rayConfig` in the fleet file — without it, `submit_job`
//! returns `RayError::NoConfig` before issuing any command.
//!
//! Prerequisites: none — fully offline (ScriptedExecutor).
//!
//! Run with:
//!   cargo run -p consortium-examples --example integration_ray

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use consortium_integration::exec::{ExecOutput, Executor, ScriptedExecutor};
use consortium_integration::fleet::FleetConfig;
use consortium_ray::{submit_job, RayOptions};

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("Load fleet configuration");
    let fleet_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("inventories/fleet.json");
    let config = FleetConfig::from_file(&fleet_path)?;
    let ray = config
        .ray_config
        .as_ref()
        .expect("fleet.json has rayConfig");
    println!("head: {}:{}", ray.head_address, ray.dashboard_port);

    banner("Script the executor");
    let scripted = Arc::new(
        ScriptedExecutor::new()
            .on("nix build", ExecOutput::ok("/nix/store/ray-env\n"))
            // submit output must contain the job id line for ray-wait to poll.
            .on(
                "job submit",
                ExecOutput::ok("Job submitted successfully\nraysubmit_abc123\n"),
            )
            .on("job status", ExecOutput::ok("Status: SUCCEEDED\n")),
    );
    let exec: Arc<dyn Executor> = scripted.clone();

    banner("submit_job — entrypoint 'python train.py', wait for terminal state");
    let opts = RayOptions {
        wait: true,
        // Real deployments poll every 10s (the default); keep the demo fast.
        poll_interval: Duration::from_millis(1),
        timeout: None,
    };
    let report = submit_job(exec, &config, "train", "python train.py", &opts)?;

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
    scripted.assert_invoked_containing("job submit");
    scripted.assert_invoked_containing("job status");

    Ok(())
}
