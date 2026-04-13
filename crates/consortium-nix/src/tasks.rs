//! DagTask implementations for each NixOS deployment phase.
//!
//! Each task reads its inputs from DagContext (predecessor outputs and
//! shared fleet config) and writes its outputs for dependent tasks.

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};

use crate::activate;
use crate::build;
use crate::config::{DeployAction, FleetConfig};
use crate::copy;
use crate::eval;

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
        let config: FleetConfig = match ctx.get_state("fleet_config") {
            Some(c) => c,
            None => return TaskOutcome::Failed("fleet_config not in context".into()),
        };

        match eval::eval_toplevel(&config.flake_uri, &self.host) {
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
        let config: FleetConfig = match ctx.get_state("fleet_config") {
            Some(c) => c,
            None => return TaskOutcome::Failed("fleet_config not in context".into()),
        };

        let machines_file: Option<String> = ctx.get_state("machines_file");

        match build::build_host(&config.flake_uri, &self.host, machines_file.as_deref()) {
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

        match copy::copy_closure(&toplevel_path, &store_uri) {
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
