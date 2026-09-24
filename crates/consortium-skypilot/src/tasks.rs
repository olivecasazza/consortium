//! DagTask implementations for SkyPilot orchestration.
//!
//! Every task reads the shared `Arc<dyn Executor>` from the DAG context
//! (state key `"executor"`, set by `launch_task()`) and fails with
//! `executor not in context` when the key is absent. All commands run
//! locally: the nix environment build goes through
//! [`staging::build_flake_attr`], and the SkyPilot CLI commands
//! (`sky launch` / `sky exec` / `sky down`) run against the operator's
//! local cloud credentials.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};
use consortium_integration::exec::{CommandSpec, Executor};
use consortium_integration::staging;

/// Fetch the shared executor from the DAG context.
fn executor_from(ctx: &DagContext) -> Result<Arc<dyn Executor>, TaskOutcome> {
    ctx.get_state::<Arc<dyn Executor>>("executor")
        .ok_or_else(|| TaskOutcome::Failed("executor not in context".into()))
}

/// Write a SkyPilot task yaml to a unique temp file and return its path.
///
/// Purely local fs, no executor: `sky launch` takes a yaml *path*, and the
/// path then appears in the rendered command line, keeping the invocation
/// matchable for scripted executors.
fn write_task_yaml(cluster_name: &str, task_yaml: &str) -> std::io::Result<PathBuf> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "consortium-sky-{}-{}-{}.yaml",
        cluster_name,
        std::process::id(),
        seq
    ));
    std::fs::write(&path, task_yaml)?;
    Ok(path)
}

/// Build a skypilot task environment via nix.
pub struct NixBuildSkyEnvTask {
    pub env_name: String,
    pub flake_attr: String,
}

impl NixBuildSkyEnvTask {
    pub fn new(env_name: &str, flake_uri: &str) -> Self {
        Self {
            env_name: env_name.to_string(),
            flake_attr: format!("{}#skyEnvs.{}", flake_uri, env_name),
        }
    }
}

impl DagTask for NixBuildSkyEnvTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        match staging::build_flake_attr(&*exec, &self.flake_attr, None) {
            Ok(path) => {
                ctx.set_output(TaskId(format!("build-sky-env:{}", self.env_name)), path);
                TaskOutcome::Success
            }
            Err(e) => TaskOutcome::Failed(format!("build sky env: {}", e)),
        }
    }

    fn describe(&self) -> String {
        format!("build skypilot environment '{}'", self.env_name)
    }
}

/// Launch a SkyPilot cluster.
pub struct SkyLaunchTask {
    pub cluster_name: String,
    pub task_yaml: String,
    pub cloud: Option<String>,
    pub region: Option<String>,
}

impl DagTask for SkyLaunchTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let yaml_path = match write_task_yaml(&self.cluster_name, &self.task_yaml) {
            Ok(p) => p,
            Err(e) => return TaskOutcome::Failed(format!("sky launch failed: {}", e)),
        };

        let mut spec = CommandSpec::new("sky").args([
            "launch".to_string(),
            "-c".to_string(),
            self.cluster_name.clone(),
            yaml_path.to_string_lossy().into_owned(),
            "-y".to_string(),
        ]);
        if let Some(ref cloud) = self.cloud {
            spec = spec.args(["--cloud".to_string(), cloud.clone()]);
        }
        if let Some(ref region) = self.region {
            spec = spec.args(["--region".to_string(), region.clone()]);
        }

        match exec.exec(&spec) {
            Ok(out) if out.success() => {
                ctx.set_output(
                    TaskId(format!("sky-launch:{}", self.cluster_name)),
                    self.cluster_name.clone(),
                );
                TaskOutcome::Success
            }
            Ok(out) => TaskOutcome::Failed(format!("sky launch failed: {}", out.stderr.trim())),
            Err(e) => TaskOutcome::Failed(format!("sky launch failed: {}", e)),
        }
    }

    fn describe(&self) -> String {
        format!("launch sky cluster '{}'", self.cluster_name)
    }
}

/// Execute a command on a SkyPilot cluster.
pub struct SkyExecTask {
    pub cluster_name: String,
    pub command: String,
}

impl DagTask for SkyExecTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let spec = CommandSpec::new("sky").args([
            "exec".to_string(),
            self.cluster_name.clone(),
            "--".to_string(),
            self.command.clone(),
        ]);

        match exec.exec(&spec) {
            Ok(out) if out.success() => {
                ctx.set_output(
                    TaskId(format!("sky-exec:{}", self.cluster_name)),
                    out.stdout,
                );
                TaskOutcome::Success
            }
            Ok(out) => TaskOutcome::Failed(format!("sky exec failed: {}", out.stderr.trim())),
            Err(e) => TaskOutcome::Failed(format!("sky exec failed: {}", e)),
        }
    }

    fn describe(&self) -> String {
        format!("exec on sky cluster '{}'", self.cluster_name)
    }
}

/// Tear down a SkyPilot cluster.
pub struct SkyDownTask {
    pub cluster_name: String,
}

impl DagTask for SkyDownTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let spec = CommandSpec::new("sky").args([
            "down".to_string(),
            self.cluster_name.clone(),
            "-y".to_string(),
        ]);

        match exec.exec(&spec) {
            Ok(out) if out.success() => TaskOutcome::Success,
            Ok(out) => TaskOutcome::Failed(format!("sky down failed: {}", out.stderr.trim())),
            Err(e) => TaskOutcome::Failed(format!("sky down failed: {}", e)),
        }
    }

    fn describe(&self) -> String {
        format!("tear down sky cluster '{}'", self.cluster_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    #[test]
    fn test_describe_methods() {
        let build = NixBuildSkyEnvTask::new("train", ".");
        assert!(build.describe().contains("train"));
        assert!(build.flake_attr.contains("skyEnvs.train"));

        let launch = SkyLaunchTask {
            cluster_name: "my-cluster".to_string(),
            task_yaml: "task.yaml".to_string(),
            cloud: Some("gcp".to_string()),
            region: Some("us-central1".to_string()),
        };
        assert!(launch.describe().contains("my-cluster"));

        let exec = SkyExecTask {
            cluster_name: "my-cluster".to_string(),
            command: "python train.py".to_string(),
        };
        assert!(exec.describe().contains("my-cluster"));

        let down = SkyDownTask {
            cluster_name: "my-cluster".to_string(),
        };
        assert!(down.describe().contains("my-cluster"));
    }

    #[test]
    fn test_flake_attr_generation() {
        let build = NixBuildSkyEnvTask::new("train-gpt", "github:user/repo");
        assert_eq!(build.flake_attr, "github:user/repo#skyEnvs.train-gpt");
    }

    #[test]
    fn test_build_task_fails_without_executor_in_context() {
        let ctx = DagContext::new();
        match NixBuildSkyEnvTask::new("train", ".").execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("executor not in context")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_launch_task_fails_without_executor_in_context() {
        let ctx = DagContext::new();
        let launch = SkyLaunchTask {
            cluster_name: "my-cluster".to_string(),
            task_yaml: "resources: {}\n".to_string(),
            cloud: None,
            region: None,
        };
        match launch.execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("executor not in context")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_down_task_fails_without_executor_in_context() {
        let ctx = DagContext::new();
        let down = SkyDownTask {
            cluster_name: "my-cluster".to_string(),
        };
        match down.execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("executor not in context")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_build_task_runs_scripted_nix_build() {
        let scripted = Arc::new(
            ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/abc-sky-env\n")),
        );
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);

        let outcome = NixBuildSkyEnvTask::new("train", ".").execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("nix build .#skyEnvs.train --no-link --print-out-paths");
        let path: Option<String> = ctx.get_output(&TaskId("build-sky-env:train".to_string()));
        assert_eq!(path.as_deref(), Some("/nix/store/abc-sky-env"));
    }

    #[test]
    fn test_launch_task_runs_scripted_sky_launch() {
        let scripted = Arc::new(ScriptedExecutor::new().on(
            "sky launch",
            ExecOutput::ok("Cluster launched: my-cluster\n"),
        ));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);

        let launch = SkyLaunchTask {
            cluster_name: "my-cluster".to_string(),
            task_yaml: "resources: {}\n".to_string(),
            cloud: Some("gcp".to_string()),
            region: Some("us-central1".to_string()),
        };
        let outcome = launch.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("sky launch -c my-cluster");
        scripted.assert_invoked_containing("--cloud gcp");
        scripted.assert_invoked_containing("--region us-central1");
        let launched: Option<String> = ctx.get_output(&TaskId("sky-launch:my-cluster".to_string()));
        assert_eq!(launched.as_deref(), Some("my-cluster"));
    }

    #[test]
    fn test_down_task_runs_scripted_sky_down() {
        let scripted = Arc::new(ScriptedExecutor::new().on("sky down", ExecOutput::ok("")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);

        let down = SkyDownTask {
            cluster_name: "my-cluster".to_string(),
        };
        let outcome = down.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("sky down my-cluster -y");
    }

    #[test]
    fn test_exec_task_runs_scripted_sky_exec() {
        let scripted =
            Arc::new(ScriptedExecutor::new().on("sky exec", ExecOutput::ok("training done\n")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);

        let task = SkyExecTask {
            cluster_name: "my-cluster".to_string(),
            command: "python train.py".to_string(),
        };
        let outcome = task.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("sky exec my-cluster -- python train.py");
        let stdout: Option<String> = ctx.get_output(&TaskId("sky-exec:my-cluster".to_string()));
        assert_eq!(stdout.as_deref(), Some("training done\n"));
    }
}
