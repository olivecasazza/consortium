//! # consortium-ray
//!
//! Ray job orchestration with nix-built environments.
//!
//! Supports both KubeRay (Kubernetes-native) and bare-metal Ray clusters.
//! Uses nix to build hermetic job environments and the DAG executor for
//! pipeline orchestration.

pub mod error;
pub mod tasks;

pub use error::{RayError, Result};

use std::time::Duration;

use consortium::dag::{DagBuilder, DagContext, DagReport, ErrorPolicy};
use consortium_nix::FleetConfig;

/// Submit a ray job with a nix-built environment.
pub fn submit_job(
    config: &FleetConfig,
    job_name: &str,
    entrypoint: &str,
    wait: bool,
) -> Result<DagReport> {
    let ray_config = config.ray_config.as_ref().ok_or(RayError::NoConfig)?;

    let ctx = DagContext::new();
    ctx.set_state("fleet_config", config.clone());

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
    if wait {
        let wait_id = format!("ray-wait:{}", job_name);
        dag.add_task(
            &wait_id,
            tasks::RayWaitTask {
                job_name: job_name.to_string(),
                head_address: ray_config.head_address.clone(),
                dashboard_port: ray_config.dashboard_port,
                poll_interval: Duration::from_secs(10),
                timeout: None,
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
