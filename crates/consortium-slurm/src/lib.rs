//! # consortium-slurm
//!
//! Slurm job orchestration with nix-built environments.
//!
//! Uses nix to build hermetic job environments, copies them to the submit
//! node, then submits jobs with slurm. Supports arbitrary DAG pipelines
//! (e.g., RNA-seq bioinformatics workflows) via DagBuilder.
//!
//! All external commands run through an injected
//! `consortium_integration::exec::Executor`: production callers pass a
//! `ProcessExecutor`, tests pass a `ScriptedExecutor`.

pub mod error;
pub mod tasks;

pub use error::{Result, SlurmError};

use std::sync::Arc;
use std::time::Duration;

use consortium::dag::{DagBuilder, DagContext, DagReport, ErrorPolicy};
use consortium_integration::exec::Executor;
use consortium_integration::fleet::FleetConfig;

/// Options for [`submit_job`].
///
/// The default exercises the full pipeline — build → copy → submit → wait —
/// with no result collection.
#[derive(Debug, Clone)]
pub struct SubmitOptions {
    /// Wait for the job to reach a terminal state (polls sacct).
    /// Default: `true`.
    pub wait: bool,
    /// Glob pattern of output files to `cat` on the submit node once the job
    /// has finished (e.g. `"slurm-12345.out"`). Default: `None` (no
    /// collection).
    pub collect: Option<String>,
    /// sacct poll interval used while waiting. Default: 10s.
    pub poll_interval: Duration,
}

impl Default for SubmitOptions {
    fn default() -> Self {
        Self {
            wait: true,
            collect: None,
            poll_interval: Duration::from_secs(10),
        }
    }
}

/// Submit a single slurm job with a nix-built environment.
///
/// Runs the pipeline `build-job-env → copy-to-submit → submit → wait →
/// collect` as a DAG: the wait stage is included when
/// [`SubmitOptions::wait`] is set, and the collect stage when
/// [`SubmitOptions::collect`] is a pattern — wired after `slurm-wait` when
/// waiting is enabled, directly after `slurm-submit` otherwise. All commands
/// run through `exec`.
///
/// Returns [`SlurmError::NoConfig`] before any command is issued when the
/// fleet config has no slurm sub-config.
pub fn submit_job(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    job_name: &str,
    script: &str,
    partition: Option<&str>,
    opts: &SubmitOptions,
) -> Result<DagReport> {
    let slurm_config = config.slurm_config.as_ref().ok_or(SlurmError::NoConfig)?;

    let ctx = DagContext::new();
    ctx.set_state("fleet_config", config.clone());
    ctx.set_state("executor", exec);

    let mut dag = DagBuilder::new();

    // Build job environment
    let build_id = format!("build-job-env:{job_name}");
    dag.add_task(
        &build_id,
        tasks::NixBuildJobEnvTask::new(job_name, &config.flake_uri),
    );

    // Copy to submit node
    let copy_id = format!("copy-job-env:{job_name}");
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
    let submit_id = format!("slurm-submit:{job_name}");
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

    // Wait (optional) — collect hangs off whichever stage ran last.
    let mut last_id = submit_id.clone();
    if opts.wait {
        let wait_id = format!("slurm-wait:{job_name}");
        dag.add_task(
            &wait_id,
            tasks::SlurmWaitTask::new(
                job_name,
                &slurm_config.submit_node,
                &slurm_config.submit_user,
            )
            .with_poll_interval(opts.poll_interval),
        );
        dag.add_dep(&wait_id, &submit_id);
        last_id = wait_id;
    }

    // Collect (optional)
    if let Some(ref pattern) = opts.collect {
        let collect_id = format!("slurm-collect:{job_name}");
        dag.add_task(
            &collect_id,
            tasks::SlurmCollectTask {
                job_name: job_name.to_string(),
                output_pattern: pattern.clone(),
                submit_host: slurm_config.submit_node.clone(),
                submit_user: slurm_config.submit_user.clone(),
            },
        );
        dag.add_dep(&collect_id, &last_id);
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
