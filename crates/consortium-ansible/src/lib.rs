//! # consortium-ansible
//!
//! Ansible orchestration with nix-built environments.
//!
//! Uses nix to build hermetic ansible environments (pinned version,
//! collections, roles), copies them to the control node, then runs
//! playbooks against targets over ssh using the DAG executor for
//! parallelism. All external commands go through the [`Executor`]
//! abstraction, so the pipeline is fully testable with
//! [`ScriptedExecutor`](consortium_integration::ScriptedExecutor).

pub mod integration;
pub use integration::AnsibleIntegration;

pub mod error;
pub mod inventory;
pub mod tasks;

pub use error::{AnsibleError, Result};

use std::sync::Arc;

use consortium::dag::DagReport;
use consortium_integration::fleet::FleetConfig;
use consortium_integration::grammar::{Integration, Request};
use consortium_integration::Executor;

/// Options for a playbook run.
#[derive(Debug, Clone)]
pub struct AnsibleOptions {
    /// Pass `--check` to ansible-playbook (dry run).
    pub check_mode: bool,
    /// Maximum number of hosts running the playbook concurrently.
    pub max_parallel: usize,
}

impl Default for AnsibleOptions {
    fn default() -> Self {
        Self {
            check_mode: false,
            max_parallel: 4,
        }
    }
}
/// Run a playbook against target hosts with a nix-built ansible environment.
///
/// All external commands — the local `nix build` / `nix copy` staging steps
/// and the remote `ansible-playbook` run on the control node — are executed
/// through `exec`.
pub fn run_playbook(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    targets: &[String],
    playbook: &str,
    env_name: &str,
    opts: &AnsibleOptions,
) -> Result<DagReport> {
    config
        .ansible_config
        .as_ref()
        .ok_or(AnsibleError::NoConfig)?;
    if targets.is_empty() {
        return Err(AnsibleError::General(
            "ansible: explicit targets must not be empty".into(),
        ));
    }
    let mut request = Request::default()
        .with("playbook", playbook)
        .with("environment", env_name)
        .with("check", opts.check_mode.to_string())
        .with("fanout", opts.max_parallel.to_string());
    request.targets = targets.to_vec();
    AnsibleIntegration
        .run_detailed(exec, config, &request)
        .map(|result| result.dag)
        .map_err(|error| AnsibleError::Dag(error.to_string()))
}

#[cfg(test)]
mod typed_contract_tests {
    use super::*;
    use consortium_integration::fleet::AnsibleFleetConfig;
    use consortium_integration::ScriptedExecutor;

    #[test]
    fn empty_explicit_targets_fail_before_effects() {
        let config = FleetConfig {
            nodes: Default::default(),
            builders: Default::default(),
            flake_uri: ".".into(),
            ansible_config: Some(AnsibleFleetConfig {
                control_node: "ctrl".into(),
                ansible_version: None,
                collections: vec![],
                playbook_dir: None,
                host_groups: Default::default(),
            }),
            slurm_config: None,
            ray_config: None,
            skypilot_config: None,
        };
        let exec = Arc::new(ScriptedExecutor::new());
        assert!(run_playbook(
            exec.clone(),
            &config,
            &[],
            "site.yml",
            "env",
            &AnsibleOptions::default()
        )
        .is_err());
        assert!(exec.effects().is_empty());
    }
}
