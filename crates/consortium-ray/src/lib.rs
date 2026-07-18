//! # consortium-ray
//!
//! Ray job orchestration with nix-built environments.
//!
//! Supports both KubeRay (Kubernetes-native) and bare-metal Ray clusters.
//! Uses nix to build hermetic job environments and the DAG executor for
//! pipeline orchestration.
//!
//! All external commands run through the [`Executor`] abstraction, so the
//! pipeline can be driven by a `ProcessExecutor` in production or a
//! `ScriptedExecutor` in tests.

pub mod error;
pub mod tasks;

pub use error::{RayError, Result};

use std::sync::Arc;
use std::time::Duration;

use consortium::dag::{DagBuilder, DagContext, DagReport, ErrorPolicy};
use consortium_integration::exec::Executor;
use consortium_integration::fleet::FleetConfig;

/// Options controlling a ray job submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RayOptions {
    /// Whether to wait for the job to reach a terminal state before
    /// returning. Defaults to `true`.
    pub wait: bool,

    /// Interval between `ray job status` polls while waiting. Defaults to
    /// 10 seconds; test fixtures typically set this to ~0.
    pub poll_interval: Duration,

    /// Maximum time to wait for the job before failing the wait task.
    /// `None` waits indefinitely.
    pub timeout: Option<Duration>,
}

impl Default for RayOptions {
    fn default() -> Self {
        Self {
            wait: true,
            poll_interval: Duration::from_secs(10),
            timeout: None,
        }
    }
}

/// Submit a ray job with a nix-built environment.
///
/// Builds the DAG `NixBuildRayEnvTask → RaySubmitTask (→ RayWaitTask when
/// `opts.wait`)` and runs it to completion, with all commands executed
/// through `exec`.
pub fn submit_job(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    job_name: &str,
    entrypoint: &str,
    opts: &RayOptions,
) -> Result<DagReport> {
    let ray_config = config.ray_config.as_ref().ok_or(RayError::NoConfig)?;

    let ctx = DagContext::new();
    ctx.set_state("fleet_config", config.clone());
    ctx.set_state("executor", exec);

    let mut dag = DagBuilder::new();

    // Build ray environment
    let build_id = format!("build-ray-env:{}", job_name);
    dag.add_task(
        &build_id,
        tasks::NixBuildRayEnvTask::new(job_name, &config.flake_uri),
    );

    // Submit job
    let submit_id = format!("ray-submit:{}", job_name);
    dag.add_task(
        &submit_id,
        tasks::RaySubmitTask {
            job_name: job_name.to_string(),
            entrypoint: entrypoint.to_string(),
            head_address: ray_config.head_address.clone(),
            dashboard_port: ray_config.dashboard_port,
            working_dir: None,
        },
    );
    dag.add_dep(&submit_id, &build_id);

    // Wait (optional)
    if opts.wait {
        let wait_id = format!("ray-wait:{}", job_name);
        dag.add_task(
            &wait_id,
            tasks::RayWaitTask {
                job_name: job_name.to_string(),
                head_address: ray_config.head_address.clone(),
                dashboard_port: ray_config.dashboard_port,
                poll_interval: opts.poll_interval,
                timeout: opts.timeout,
            },
        );
        dag.add_dep(&wait_id, &submit_id);
    }

    dag.error_policy(ErrorPolicy::FailFast);
    dag.context(ctx);

    let report = dag
        .build()
        .map_err(|e| RayError::Dag(e.to_string()))?
        .run()
        .map_err(|e| RayError::Dag(e.to_string()))?;

    Ok(report)
}
