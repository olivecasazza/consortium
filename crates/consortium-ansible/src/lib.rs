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

pub mod error;
pub mod inventory;
pub mod tasks;

pub use error::{AnsibleError, Result};

use std::sync::Arc;

use consortium::dag::{DagContext, DagReport, ErrorPolicy, StageBuilder};
use consortium_integration::fleet::FleetConfig;
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
    let ansible_config = config
        .ansible_config
        .as_ref()
        .ok_or(AnsibleError::NoConfig)?;

    let ctx = DagContext::new();
    ctx.set_state("executor", exec);

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
        .stage("run-playbook", Some(opts.max_parallel), {
            let pb = playbook.to_string();
            let env = env_name.to_string();
            let control = ansible_config.control_node.clone();
            let check = opts.check_mode;
            move |host| {
                Box::new(
                    tasks::AnsiblePlaybookTask::new(host, &pb, &env, &control, "root")
                        .with_check(check),
                )
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
