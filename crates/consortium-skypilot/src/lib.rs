//! # consortium-skypilot
//!
//! SkyPilot multi-cloud job orchestration with nix-built environments.
//!
//! Launches cloud clusters via SkyPilot and tears them down afterwards,
//! using nix for reproducible task environments. Every external command
//! (`nix build`, `sky launch`, `sky down`) runs through an injected
//! [`Executor`], which the DAG tasks share via the `"executor"` context
//! state key — so the whole pipeline is testable with a
//! [`ScriptedExecutor`](consortium_integration::exec::ScriptedExecutor)
//! and needs neither a cloud account nor a nix installation.
//!
//! # Example
//!
//! ```
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! use consortium_integration::exec::{ExecOutput, Executor, ScriptedExecutor};
//! use consortium_integration::fleet::{FleetConfig, SkypilotFleetConfig};
//! use consortium_skypilot::{launch_task, SkyOptions};
//!
//! let exec: Arc<dyn Executor> = Arc::new(
//!     ScriptedExecutor::new()
//!         .on("nix build", ExecOutput::ok("/nix/store/abc-sky-env\n"))
//!         .on("sky launch", ExecOutput::ok("Cluster launched\n"))
//!         .on("sky down", ExecOutput::ok("Terminating cluster\n")),
//! );
//! let config = FleetConfig {
//!     nodes: HashMap::new(),
//!     builders: HashMap::new(),
//!     flake_uri: ".".into(),
//!     ansible_config: None,
//!     slurm_config: None,
//!     ray_config: None,
//!     skypilot_config: Some(SkypilotFleetConfig {
//!         cloud: "gcp".into(),
//!         region: Some("us-central1".into()),
//!         instance_type: None,
//!     }),
//! };
//! let report = launch_task(exec, &config, "train", "resources: {}\n", &SkyOptions::default())?;
//! assert!(report.failed.is_empty());
//! # Ok::<(), consortium_skypilot::SkypilotError>(())
//! ```

pub mod integration;
pub use integration::SkypilotIntegration;

pub mod error;
pub mod tasks;

pub use error::{Result, SkypilotError};

use std::sync::Arc;

use consortium::dag::DagReport;
use consortium_integration::exec::Executor;
use consortium_integration::fleet::FleetConfig;
use consortium_integration::grammar::{Integration, Request};

/// Options for [`launch_task`].
///
/// The `Default` runs the full pipeline: build → launch → teardown.
#[derive(Debug, Clone, Copy)]
pub struct SkyOptions {
    /// Tear the cluster down after it launches (`sky down`).
    pub teardown: bool,
}

impl Default for SkyOptions {
    fn default() -> Self {
        Self { teardown: true }
    }
}

/// Launch a SkyPilot task on a cloud cluster.
///
/// Pipeline: build the sky environment with nix → `sky launch` (the task
/// yaml is written to a temp file so its path shows up in the rendered
/// command line) → `sky down` when `opts.teardown`. Requires
/// [`FleetConfig::skypilot_config`]; returns [`SkypilotError::NoConfig`]
/// before issuing any command when it is absent.
pub fn launch_task(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    cluster_name: &str,
    task_yaml: &str,
    opts: &SkyOptions,
) -> Result<DagReport> {
    config
        .skypilot_config
        .as_ref()
        .ok_or(SkypilotError::NoConfig)?;
    let request = Request::default()
        .with("name", cluster_name)
        .with("task", task_yaml)
        .with("teardown", opts.teardown.to_string());
    SkypilotIntegration
        .run_detailed(exec, config, &request)
        .map(|result| result.dag)
        .map_err(|error| SkypilotError::Dag(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};
    use consortium_integration::fleet::SkypilotFleetConfig;

    fn config(with_skypilot: bool) -> FleetConfig {
        FleetConfig {
            nodes: HashMap::new(),
            builders: HashMap::new(),
            flake_uri: ".".into(),
            ansible_config: None,
            slurm_config: None,
            ray_config: None,
            skypilot_config: with_skypilot.then(|| SkypilotFleetConfig {
                cloud: "gcp".into(),
                region: Some("us-central1".into()),
                instance_type: None,
            }),
        }
    }

    #[test]
    fn launch_task_errors_without_skypilot_config() {
        let exec: Arc<dyn Executor> = Arc::new(ScriptedExecutor::new());
        let err =
            launch_task(exec, &config(false), "c", "yaml", &SkyOptions::default()).unwrap_err();
        assert!(matches!(err, SkypilotError::NoConfig));
    }

    #[test]
    fn launch_task_happy_path_orders_phases() {
        let scripted = Arc::new(
            ScriptedExecutor::new()
                .on("nix build", ExecOutput::ok("/nix/store/abc-sky-env\n"))
                .on("sky launch", ExecOutput::ok("Cluster launched\n"))
                .on("sky down", ExecOutput::ok("Terminating cluster\n")),
        );
        let exec: Arc<dyn Executor> = scripted.clone();
        let report = launch_task(
            exec,
            &config(true),
            "train",
            "resources: {}\n",
            &SkyOptions::default(),
        )
        .unwrap();
        assert!(report.failed.is_empty(), "failures: {:?}", report.failed);
        let build = scripted.invocation_index_containing("nix build").unwrap();
        let launch = scripted.invocation_index_containing("sky launch").unwrap();
        let down = scripted.invocation_index_containing("sky down").unwrap();
        assert!(build < launch && launch < down);
    }

    #[test]
    fn launch_task_no_teardown_skips_down() {
        let scripted = Arc::new(
            ScriptedExecutor::new()
                .on("nix build", ExecOutput::ok("/nix/store/abc-sky-env\n"))
                .on("sky launch", ExecOutput::ok("Cluster launched\n")),
        );
        let exec: Arc<dyn Executor> = scripted.clone();
        let report = launch_task(
            exec,
            &config(true),
            "train",
            "resources: {}\n",
            &SkyOptions { teardown: false },
        )
        .unwrap();
        assert!(report.failed.is_empty(), "failures: {:?}", report.failed);
        scripted.assert_not_invoked_containing("sky down");
    }
}
