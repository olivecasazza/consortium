//! # consortium-ansible
//!
//! Ansible orchestration with nix-built environments.
//!
//! Uses nix to build hermetic ansible environments (pinned version,
//! collections, roles), copies them to the control node, then runs
//! playbooks against targets using the DAG executor for parallelism.

pub mod error;
pub mod inventory;
pub mod tasks;

pub use error::{AnsibleError, Result};

use consortium::dag::{DagContext, DagReport, ErrorPolicy, StageBuilder};
use consortium_nix::FleetConfig;

/// Run a playbook against target hosts with a nix-built ansible environment.
pub fn run_playbook(
    config: &FleetConfig,
    targets: &[String],
    playbook: &str,
    env_name: &str,
    check_mode: bool,
    max_parallel: usize,
) -> Result<DagReport> {
    let ansible_config = config
        .ansible_config
        .as_ref()
        .ok_or(AnsibleError::NoConfig)?;

    let ctx = DagContext::new();
    ctx.set_state("fleet_config", config.clone());

    // Build and copy the ansible env (shared across all hosts)
    // Then run playbook per host
    let report = StageBuilder::new()
        .resources(targets.to_vec())
        .stage("build-env", Some(1), {
            let flake_uri = config.flake_uri.clone();
            let env = env_name.to_string();
            move |_host| Box::new(tasks::NixBuildAnsibleEnvTask::new(&env, &flake_uri))
        })
        .stage("copy-env", Some(1), {
            let control = ansible_config.control_node.clone();
            let env = env_name.to_string();
            move |_host| {
                Box::new(tasks::NixCopyAnsibleEnvTask {
                    env_name: env.clone(),
                    target_host: control.clone(),
                    target_user: "root".to_string(),
                })
            }
        })
        .stage("run-playbook", Some(max_parallel), {
            let pb = playbook.to_string();
            let env = env_name.to_string();
            move |host| {
                Box::new(tasks::AnsiblePlaybookTask::new(host, &pb, &env).with_check(check_mode))
            }
        })
        .error_policy(ErrorPolicy::ContinueIndependent)
        .context(ctx)
        .build()
        .map_err(|e| AnsibleError::Dag(e.to_string()))?
        .run()
        .map_err(|e| AnsibleError::Dag(e.to_string()))?;

    Ok(report)
}
