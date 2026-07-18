//! Integration: Slurm job submission (consortium-slurm) without infrastructure.
//!
//! `consortium_slurm::submit_job` runs a linear FailFast DAG:
//!
//!   build-job-env -> copy-job-env -> slurm-submit -> slurm-wait -> [slurm-collect]
//!
//! The nix stages run locally; sbatch/sacct/cat run over ssh on the
//! fleet's `submitNode`. All commands go through the `Executor` trait, so
//! a `ScriptedExecutor` demonstrates the full pipeline offline. The job
//! script is staged to the submit node and `sbatch`'ed; `slurm-wait` polls
//! `sacct` until a terminal state; `slurm-collect` cats the output glob.
//!
//! Requires `slurmConfig` in the fleet file — without it, `submit_job`
//! returns `SlurmError::NoConfig` before issuing any command.
//!
//! Prerequisites: none — fully offline (ScriptedExecutor).
//!
//! Run with:
//!   cargo run -p consortium-examples --example integration_slurm

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use consortium_integration::exec::{ExecOutput, Executor, ScriptedExecutor};
use consortium_integration::fleet::FleetConfig;
use consortium_slurm::{submit_job, SubmitOptions};

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn print_report(report: &consortium::dag::DagReport) {
    let mut completed: Vec<_> = report.completed.iter().map(|t| t.0.clone()).collect();
    completed.sort();
    let mut failed: Vec<_> = report.failed.iter().map(|(t, e)| format!("{}: {}", t.0, e)).collect();
    failed.sort();
    println!("is_success: {}", report.is_success());
    println!("completed : {completed:?}");
    println!("failed    : {failed:?}");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("Load fleet configuration");
    let fleet_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("inventories/fleet.json");
    let config = FleetConfig::from_file(&fleet_path)?;
    let slurm = config.slurm_config.as_ref().expect("fleet.json has slurmConfig");
    println!("submit node: {} (user {})", slurm.submit_node, slurm.submit_user);

    banner("Script the executor");
    let scripted = Arc::new(
        ScriptedExecutor::new()
            .on("nix build", ExecOutput::ok("/nix/store/job-env\n"))
            .on("nix copy", ExecOutput::ok(""))
            .on("sbatch", ExecOutput::ok("Submitted batch job 12345\n"))
            // sacct output parsed for a terminal state — COMPLETED ends the wait.
            .on("sacct", ExecOutput::ok("COMPLETED\n"))
            // The collect stage cats the output glob on the submit node.
            .on("cat", ExecOutput::ok("epoch 10: loss=0.043\n")),
    );
    let exec: Arc<dyn Executor> = scripted.clone();

    banner("submit_job — partition gpu, wait + collect");
    let script = "#!/bin/sh\n#SBATCH --nodes=2\nsrun python train.py\n";
    let opts = SubmitOptions {
        wait: true,
        collect: Some("results/*.out".to_string()),
        // Real deployments poll every 10s (the default); keep the demo fast.
        poll_interval: Duration::from_millis(1),
    };
    let report = submit_job(exec, &config, "train", script, Some("gpu"), &opts)?;
    print_report(&report);

    banner("Commands that would have run (recorded invocations)");
    for cmd in scripted.invocations() {
        println!("  {cmd}");
    }
    scripted.assert_invoked_containing("sbatch");
    scripted.assert_invoked_containing("sacct");

    Ok(())
}
