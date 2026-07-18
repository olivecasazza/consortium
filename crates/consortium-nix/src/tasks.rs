//! DagTask implementations for each NixOS deployment phase.
//!
//! Each task reads its inputs from DagContext (predecessor outputs and
//! shared fleet config) and writes its outputs for dependent tasks.
//!
//! All command execution runs through the `Arc<dyn Executor>` stored in
//! the context under the `"executor"` state key (see `deploy()`); a task
//! fails with `executor not in context` when the key is absent.

use std::sync::Arc;

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};
use consortium_integration::exec::Executor;

use crate::activate;
use crate::build;
use crate::config::{DeployAction, FleetConfig};
use crate::copy;
use crate::eval;

/// Fetch the shared executor from the DAG context.
fn executor_from(ctx: &DagContext) -> Result<Arc<dyn Executor>, TaskOutcome> {
    ctx.get_state::<Arc<dyn Executor>>("executor")
        .ok_or_else(|| TaskOutcome::Failed("executor not in context".into()))
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

        let config: FleetConfig = match ctx.get_state("fleet_config") {
            Some(c) => c,
            None => return TaskOutcome::Failed("fleet_config not in context".into()),
        };

        match eval::eval_toplevel(&*exec, &config.flake_uri, &self.host) {
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

        let config: FleetConfig = match ctx.get_state("fleet_config") {
            Some(c) => c,
            None => return TaskOutcome::Failed("fleet_config not in context".into()),
        };

        let machines_file: Option<String> = ctx.get_state("machines_file");

        match build::build_host(
            &*exec,
            &config.flake_uri,
            &self.host,
            machines_file.as_deref(),
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

        let config: FleetConfig = match ctx.get_state("fleet_config") {
            Some(c) => c,
            None => return TaskOutcome::Failed("fleet_config not in context".into()),
        };

        let toplevel_path: String = match ctx.get_output(&TaskId(format!("build:{}", self.host))) {
            Some(p) => p,
            None => {
                return TaskOutcome::Failed(format!("no build output for {} in context", self.host))
            }
        };

        let node = match config.nodes.get(&self.host) {
            Some(n) => n,
            None => return TaskOutcome::Failed(format!("unknown host: {}", self.host)),
        };

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

        let config: FleetConfig = match ctx.get_state("fleet_config") {
            Some(c) => c,
            None => return TaskOutcome::Failed("fleet_config not in context".into()),
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

        let node = match config.nodes.get(&self.host) {
            Some(n) => n,
            None => return TaskOutcome::Failed(format!("unknown host: {}", self.host)),
        };

        match activate::activate_host(
            &*exec,
            &node.target_host,
            &node.target_user,
            &toplevel_path,
            &node.profile_type,
            action,
        ) {
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
        scripted.assert_invoked_containing(
            "/nix/store/abc-toplevel/bin/switch-to-configuration switch",
        );
    }
}
