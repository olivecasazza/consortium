//! # consortium-slurm
//!
//! Slurm job orchestration with nix-built environments.
//!
//! Uses nix to build hermetic job environments, copies them to the submit
//! node, then submits jobs with slurm. Supports arbitrary DAG pipelines
//! (e.g., RNA-seq bioinformatics workflows) via DagBuilder.

pub mod error;
pub mod tasks;

pub use error::{Result, SlurmError};

use consortium::dag::{DagBuilder, DagContext, DagReport, ErrorPolicy};
use consortium_nix::FleetConfig;

/// Submit a single slurm job with a nix-built environment.
pub fn submit_job(
    config: &FleetConfig,
    job_name: &str,
    script: &str,
    partition: Option<&str>,
    wait: bool,
) -> Result<DagReport> {
    let slurm_config = config.slurm_config.as_ref().ok_or(SlurmError::NoConfig)?;

    let ctx = DagContext::new();
    ctx.set_state("fleet_config", config.clone());

    let mut dag = DagBuilder::new();

    // Build job environment
    let build_id = format!("build-job-env:{}", job_name);
    dag.add_task(
        &build_id,
        tasks::NixBuildJobEnvTask::new(job_name, &config.flake_uri),
    );

    // Copy to submit node
    let copy_id = format!("copy-job-env:{}", job_name);
    dag.add_task(
        &copy_id,
        tasks::NixCopyToSubmitTask {
            job_name: job_name.to_string(),
            submit_host: slurm_config.submit_node.clone(),
            submit_user: slurm_config.submit_user.clone(),
        },
    );
    dag.add_dep(&copy_id, &build_id);

    // Submit
    let submit_id = format!("slurm-submit:{}", job_name);
    dag.add_task(
        &submit_id,
        tasks::SlurmSubmitTask {
            job_name: job_name.to_string(),
            script: script.to_string(),
            partition: partition.map(|s| s.to_string()),
            submit_host: slurm_config.submit_node.clone(),
            submit_user: slurm_config.submit_user.clone(),
        },
    );
    dag.add_dep(&submit_id, &copy_id);

    // Wait (optional)
    if wait {
        let wait_id = format!("slurm-wait:{}", job_name);
        dag.add_task(
            &wait_id,
            tasks::SlurmWaitTask::new(
                job_name,
                &slurm_config.submit_node,
                &slurm_config.submit_user,
            ),
        );
        dag.add_dep(&wait_id, &submit_id);
    }

    dag.error_policy(ErrorPolicy::FailFast);
    dag.context(ctx);

    let report = dag
        .build()
        .map_err(|e| SlurmError::Dag(e.to_string()))?
        .run()
        .map_err(|e| SlurmError::Dag(e.to_string()))?;

    Ok(report)
}
