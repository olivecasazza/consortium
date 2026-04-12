//! # consortium-skypilot
//!
//! SkyPilot multi-cloud job orchestration with nix-built environments.
//!
//! Launches cloud clusters via SkyPilot, executes commands, and tears
//! down resources. Uses nix for reproducible task definitions.

pub mod error;
pub mod tasks;

pub use error::{Result, SkypilotError};

use consortium::dag::{DagBuilder, DagContext, DagReport, ErrorPolicy};
use consortium_nix::FleetConfig;

/// Launch a SkyPilot task on a cloud cluster.
pub fn launch_task(
    config: &FleetConfig,
    cluster_name: &str,
    task_yaml: &str,
    teardown: bool,
) -> Result<DagReport> {
    let sky_config = config
        .skypilot_config
        .as_ref()
        .ok_or(SkypilotError::NoConfig)?;

    let ctx = DagContext::new();
    ctx.set_state("fleet_config", config.clone());

    let mut dag = DagBuilder::new();

    // Build environment
    let build_id = format!("build-sky-env:{}", cluster_name);
    dag.add_task(
        &build_id,
        tasks::NixBuildSkyEnvTask::new(cluster_name, &config.flake_uri),
    );

    // Launch cluster
    let launch_id = format!("sky-launch:{}", cluster_name);
    dag.add_task(
        &launch_id,
        tasks::SkyLaunchTask {
            cluster_name: cluster_name.to_string(),
            task_yaml: task_yaml.to_string(),
            cloud: Some(sky_config.cloud.clone()),
            region: sky_config.region.clone(),
        },
    );
    dag.add_dep(&launch_id, &build_id);

    // Teardown (optional)
    if teardown {
        let down_id = format!("sky-down:{}", cluster_name);
        dag.add_task(
            &down_id,
            tasks::SkyDownTask {
                cluster_name: cluster_name.to_string(),
            },
        );
        dag.add_dep(&down_id, &launch_id);
    }

    dag.error_policy(ErrorPolicy::FailFast);
    dag.context(ctx);

    let report = dag
        .build()
        .map_err(|e| SkypilotError::Dag(e.to_string()))?
        .run()
        .map_err(|e| SkypilotError::Dag(e.to_string()))?;

    Ok(report)
}
