//! DagTask implementations for each NixOS deployment phase.
//!
//! Each task reads its inputs from DagContext (predecessor outputs and
//! shared fleet config) and writes its outputs for dependent tasks.
//!
//! All command execution runs through the `Arc<dyn Executor>` stored in
//! the context under the `"executor"` state key (see `deploy()`); a task
//! fails with `executor not in context` when the key is absent. The
//! optional `"deploy_options"` state key ([`DeployOptions`]) carries extra
//! nix arguments and the set of hosts activated locally.

use std::sync::Arc;

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};
use consortium_integration::exec::Executor;

use crate::activate;
use crate::build;
use crate::config::{DeployAction, DeploymentNode, FleetConfig};
use crate::copy;
use crate::eval;
use crate::options::DeployOptions;

/// Fetch the shared executor from the DAG context.
fn executor_from(ctx: &DagContext) -> Result<Arc<dyn Executor>, TaskOutcome> {
    ctx.get_state::<Arc<dyn Executor>>("executor")
        .ok_or_else(|| TaskOutcome::Failed("executor not in context".into()))
}

/// Fetch the fleet config from the DAG context.
fn config_from(ctx: &DagContext) -> Result<FleetConfig, TaskOutcome> {
    ctx.get_state("fleet_config")
        .ok_or_else(|| TaskOutcome::Failed("fleet_config not in context".into()))
}

/// Fetch the deploy options from the DAG context (defaults when unset).
fn options_from(ctx: &DagContext) -> DeployOptions {
    ctx.get_state("deploy_options").unwrap_or_default()
}

/// Look up `host` in the fleet config.
fn node_from<'c>(config: &'c FleetConfig, host: &str) -> Result<&'c DeploymentNode, TaskOutcome> {
    config
        .nodes
        .get(host)
        .ok_or_else(|| TaskOutcome::Failed(format!("unknown host: {}", host)))
}

/// Evaluate a single host — resolve its toplevel store path.
///
/// Writes output: `eval:{host}` → `String` (toplevel store path)
pub struct NixEvalTask {
    pub host: String,
}

impl NixEvalTask {
    pub fn new(host: &str) -> Self {
        Self {
            host: host.to_string(),
        }
    }
}

impl DagTask for NixEvalTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };
        let config = match config_from(ctx) {
            Ok(c) => c,
            Err(outcome) => return outcome,
        };
        let node = match node_from(&config, &self.host) {
            Ok(n) => n,
            Err(outcome) => return outcome,
        };
        let options = options_from(ctx);

        match eval::eval_system_toplevel(
            &*exec,
            &config.flake_uri,
            &self.host,
            &node.profile_type,
            options.nix_args.for_profile(&node.profile_type),
        ) {
            Ok(path) => {
                ctx.set_output(TaskId(format!("eval:{}", self.host)), path);
                TaskOutcome::Success
            }
            Err(e) => TaskOutcome::Failed(format!("eval {}: {}", self.host, e)),
        }
    }

    fn describe(&self) -> String {
        format!("evaluate {}", self.host)
    }
}

/// Build the system closure for a single host.
///
/// Reads: `eval:{host}` → toplevel path (to verify eval completed)
/// Reads state: `machines_file` → `Option<String>` (path to machines file for distributed builds)
/// Writes output: `build:{host}` → `String` (built store path)
pub struct NixBuildTask {
    pub host: String,
}

impl NixBuildTask {
    pub fn new(host: &str) -> Self {
        Self {
            host: host.to_string(),
        }
    }
}

impl DagTask for NixBuildTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };
        let config = match config_from(ctx) {
            Ok(c) => c,
            Err(outcome) => return outcome,
        };
        let node = match node_from(&config, &self.host) {
            Ok(n) => n,
            Err(outcome) => return outcome,
        };
        let options = options_from(ctx);
        let machines_file: Option<String> = ctx.get_state("machines_file");

        match build::build_system_toplevel(
            &*exec,
            &config.flake_uri,
            &self.host,
            &node.profile_type,
            machines_file.as_deref(),
            options.nix_args.for_profile(&node.profile_type),
        ) {
            Ok(path) => {
                ctx.set_output(TaskId(format!("build:{}", self.host)), path);
                TaskOutcome::Success
            }
            Err(e) => TaskOutcome::Failed(format!("build {}: {}", self.host, e)),
        }
    }

    fn describe(&self) -> String {
        format!("build {}", self.host)
    }
}

/// Copy the built closure to the target host.
///
/// Reads: `build:{host}` → store path to copy
/// Writes output: `copy:{host}` → `String` (copied store path)
///
/// Hosts listed in `DeployOptions::local_hosts` already have the closure
/// (it was built here): the copy is skipped and the output written as-is.
pub struct NixCopyTask {
    pub host: String,
}

impl NixCopyTask {
    pub fn new(host: &str) -> Self {
        Self {
            host: host.to_string(),
        }
    }
}

impl DagTask for NixCopyTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };
        let config = match config_from(ctx) {
            Ok(c) => c,
            Err(outcome) => return outcome,
        };

        let toplevel_path: String = match ctx.get_output(&TaskId(format!("build:{}", self.host))) {
            Some(p) => p,
            None => {
                return TaskOutcome::Failed(format!("no build output for {} in context", self.host))
            }
        };

        let node = match node_from(&config, &self.host) {
            Ok(n) => n,
            Err(outcome) => return outcome,
        };

        if options_from(ctx).is_local(&self.host) {
            ctx.set_output(TaskId(format!("copy:{}", self.host)), toplevel_path);
            return TaskOutcome::Success;
        }

        let store_uri = format!("ssh-ng://{}@{}", node.target_user, node.target_host);

        match copy::copy_closure_with(&*exec, &toplevel_path, &store_uri) {
            Ok(()) => {
                ctx.set_output(TaskId(format!("copy:{}", self.host)), toplevel_path);
                TaskOutcome::Success
            }
            Err(e) => TaskOutcome::Failed(format!("copy to {}: {}", self.host, e)),
        }
    }

    fn describe(&self) -> String {
        format!("copy closure to {}", self.host)
    }
}

/// Activate the system profile on the target host.
///
/// Reads: `copy:{host}` → store path (the closure that was copied)
/// Reads state: `action` → DeployAction
///
/// Hosts listed in `DeployOptions::local_hosts` are activated on this
/// machine through `sudo` instead of over ssh.
pub struct NixActivateTask {
    pub host: String,
}

impl NixActivateTask {
    pub fn new(host: &str) -> Self {
        Self {
            host: host.to_string(),
        }
    }
}

impl DagTask for NixActivateTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };
        let config = match config_from(ctx) {
            Ok(c) => c,
            Err(outcome) => return outcome,
        };

        let action: DeployAction = match ctx.get_state("action") {
            Some(a) => a,
            None => return TaskOutcome::Failed("action not in context".into()),
        };

        // Build-only: skip activation
        if action == DeployAction::Build {
            return TaskOutcome::Success;
        }

        let toplevel_path: String = match ctx.get_output(&TaskId(format!("copy:{}", self.host))) {
            Some(p) => p,
            None => {
                return TaskOutcome::Failed(format!("no copy output for {} in context", self.host))
            }
        };

        let node = match node_from(&config, &self.host) {
            Ok(n) => n,
            Err(outcome) => return outcome,
        };

        let result = if options_from(ctx).is_local(&self.host) {
            activate::activate_local(
                &*exec,
                &self.host,
                &toplevel_path,
                &node.profile_type,
                action,
            )
        } else {
            activate::activate_host(
                &*exec,
                &node.target_host,
                &node.target_user,
                &toplevel_path,
                &node.profile_type,
                action,
            )
        };
        match result {
            Ok(()) => TaskOutcome::Success,
            Err(e) => TaskOutcome::Failed(format!("activate {}: {}", self.host, e)),
        }
    }

    fn describe(&self) -> String {
        format!("activate {}", self.host)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DeploymentNode, ProfileType};
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};
    use std::collections::HashMap;

    fn test_fleet_config() -> FleetConfig {
        let mut nodes = HashMap::new();
        nodes.insert(
            "hp01".to_string(),
            DeploymentNode {
                name: "hp01".to_string(),
                target_host: "192.168.1.121".to_string(),
                target_user: "root".to_string(),
                target_port: None,
                system: "x86_64-linux".to_string(),
                profile_type: ProfileType::Nixos,
                build_on_target: false,
                tags: vec![],
                drv_path: None,
                toplevel: None,
            },
        );
        FleetConfig {
            nodes,
            builders: HashMap::new(),
            flake_uri: ".".to_string(),
            ansible_config: None,
            slurm_config: None,
            ray_config: None,
            skypilot_config: None,
        }
    }

    #[test]
    fn test_eval_task_fails_without_executor_in_context() {
        let ctx = DagContext::new();
        ctx.set_state("fleet_config", test_fleet_config());
        match NixEvalTask::new("hp01").execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("executor not in context")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_build_task_fails_without_executor_in_context() {
        let ctx = DagContext::new();
        ctx.set_state("fleet_config", test_fleet_config());
        match NixBuildTask::new("hp01").execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("executor not in context")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_copy_task_runs_scripted_nix_copy() {
        let scripted = Arc::new(ScriptedExecutor::new().on("nix copy", ExecOutput::ok("")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("fleet_config", test_fleet_config());
        ctx.set_state("executor", exec);
        ctx.set_output(
            TaskId("build:hp01".to_string()),
            "/nix/store/abc-toplevel".to_string(),
        );

        let outcome = NixCopyTask::new("hp01").execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing(
            "nix copy --no-check-sigs --to ssh-ng://root@192.168.1.121 /nix/store/abc-toplevel",
        );
        let copied: Option<String> = ctx.get_output(&TaskId("copy:hp01".to_string()));
        assert_eq!(copied.as_deref(), Some("/nix/store/abc-toplevel"));
    }

    #[test]
    fn test_activate_task_runs_scripted_ssh_switch() {
        let scripted = Arc::new(ScriptedExecutor::new().on("ssh", ExecOutput::ok("")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("fleet_config", test_fleet_config());
        ctx.set_state("action", DeployAction::Switch);
        ctx.set_state("executor", exec);
        ctx.set_output(
            TaskId("copy:hp01".to_string()),
            "/nix/store/abc-toplevel".to_string(),
        );

        let outcome = NixActivateTask::new("hp01").execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("ssh ");
        scripted.assert_invoked_containing(
            "'/nix/store/abc-toplevel/bin/switch-to-configuration' 'switch'",
        );
    }

    #[test]
    fn test_local_host_skips_copy_and_activates_without_ssh() {
        let scripted = Arc::new(ScriptedExecutor::new().on("", ExecOutput::ok("")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("fleet_config", test_fleet_config());
        ctx.set_state("action", DeployAction::Switch);
        ctx.set_state("executor", exec);
        ctx.set_state("deploy_options", DeployOptions::new().local_hosts(["hp01"]));
        ctx.set_output(
            TaskId("build:hp01".to_string()),
            "/nix/store/abc-toplevel".to_string(),
        );

        assert!(matches!(
            NixCopyTask::new("hp01").execute(&ctx),
            TaskOutcome::Success
        ));
        assert_eq!(
            scripted.invocation_count(),
            0,
            "local host must not be copied to"
        );
        let copied: Option<String> = ctx.get_output(&TaskId("copy:hp01".to_string()));
        assert_eq!(copied.as_deref(), Some("/nix/store/abc-toplevel"));

        assert!(matches!(
            NixActivateTask::new("hp01").execute(&ctx),
            TaskOutcome::Success
        ));
        scripted.assert_not_invoked_containing("ssh");
        assert_eq!(
            scripted.invocations(),
            vec![
                "sudo nix-env -p /nix/var/nix/profiles/system --set /nix/store/abc-toplevel",
                "sudo /nix/store/abc-toplevel/bin/switch-to-configuration switch",
            ]
        );
    }

    #[test]
    fn test_eval_and_build_tasks_apply_platform_nix_args() {
        let scripted = Arc::new(
            ScriptedExecutor::new()
                .on("nix eval", ExecOutput::ok("/nix/store/abc-toplevel\n"))
                .on("nix build", ExecOutput::ok("/nix/store/abc-toplevel\n")),
        );
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("fleet_config", test_fleet_config());
        ctx.set_state("executor", exec);
        ctx.set_state(
            "deploy_options",
            DeployOptions::new().nix_args(crate::options::NixArgs {
                darwin: vec!["--impure".to_string()],
                nixos: vec![
                    "--override-input".to_string(),
                    "x".to_string(),
                    "path:/s".to_string(),
                ],
            }),
        );

        assert!(matches!(
            NixEvalTask::new("hp01").execute(&ctx),
            TaskOutcome::Success
        ));
        assert!(matches!(
            NixBuildTask::new("hp01").execute(&ctx),
            TaskOutcome::Success
        ));
        // hp01 is NixOS: nixos words present, darwin words absent.
        scripted.assert_invoked_containing(
            "nix eval --raw .#nixosConfigurations.hp01.config.system.build.toplevel.outPath \
             --override-input x path:/s",
        );
        scripted
            .assert_invoked_containing("--no-link --print-out-paths --override-input x path:/s");
        scripted.assert_not_invoked_containing("--impure");
    }
}
