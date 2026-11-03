//! DagTask implementations for SkyPilot orchestration.
//!
//! Every task reads the shared `Arc<dyn Executor>` from the DAG context
//! (state key `"executor"`, set by `launch_task()`) and fails with
//! `executor not in context` when the key is absent. All commands run
//! locally: the nix environment build goes through
//! [`staging::build_flake_attr`], and the SkyPilot CLI commands
//! (`sky launch` / `sky exec` / `sky down`) run against the operator's
//! local cloud credentials.

use std::sync::{Arc, Mutex};

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};
use consortium_integration::exec::{CommandSpec, Executor};
use consortium_integration::staging;

type TaskFile = Arc<Mutex<Option<consortium_integration::TemporaryFile>>>;

/// Release invocation files after resource teardown has had a chance to run.
pub fn cleanup_task_yaml(ctx: &DagContext, cluster_name: &str) -> std::io::Result<()> {
    if let Some(file) = ctx.get_state::<TaskFile>(&format!("sky-task-file:{cluster_name}")) {
        if let Some(file) = file.lock().unwrap().take() {
            file.close()?;
        }
    }
    Ok(())
}

/// Fetch the shared executor from the DAG context.
fn executor_from(ctx: &DagContext) -> Result<Arc<dyn Executor>, TaskOutcome> {
    ctx.get_state::<Arc<dyn Executor>>("executor")
        .ok_or_else(|| TaskOutcome::Failed("executor not in context".into()))
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

        let yaml_path = match exec.temporary_file("consortium-sky", self.task_yaml.as_bytes()) {
            Ok(p) => p,
            Err(e) => return TaskOutcome::Failed(format!("sky launch failed: {}", e)),
        };

        let mut spec = CommandSpec::new("sky").args([
            "launch".to_string(),
            "-c".to_string(),
            self.cluster_name.clone(),
            yaml_path.path().to_string_lossy().into_owned(),
            "-y".to_string(),
        ]);
        if let Some(ref cloud) = self.cloud {
            spec = spec.args(["--cloud".to_string(), cloud.clone()]);
        }
        if let Some(ref region) = self.region {
            spec = spec.args(["--region".to_string(), region.clone()]);
        }

        ctx.set_state(
            &format!("sky-task-file:{}", self.cluster_name),
            Arc::new(Mutex::new(Some(yaml_path))),
        );
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

#[cfg(test)]
mod temporary_effect_tests {
    use super::*;
    use consortium_integration::{Effect, ExecError, ExecOutput, ScriptedExecutor, TemporaryFile};

    fn launch(exec: Arc<dyn Executor>) -> TaskOutcome {
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);
        SkyLaunchTask {
            cluster_name: "test".into(),
            task_yaml: "resources: {}".into(),
            cloud: None,
            region: None,
        }
        .execute(&ctx)
    }

    #[test]
    fn yaml_is_scoped_around_launch_for_success_and_failure() {
        for output in [ExecOutput::ok("launched"), ExecOutput::new(1, "", "denied")] {
            let exec = Arc::new(ScriptedExecutor::new().on("sky launch", output.clone()));
            let result = launch(exec.clone());
            assert_eq!(matches!(result, TaskOutcome::Success), output.success());
            assert!(
                matches!(&exec.effects()[..], [Effect::TemporaryFileCreated { path, contents }, Effect::Command { spec, .. }, Effect::TemporaryFileRemoved { path: removed }] if contents == b"resources: {}" && path == removed && spec.args.iter().any(|arg| std::path::Path::new(arg) == path))
            );
        }
    }

    #[test]
    fn launch_spawn_error_also_removes_yaml() {
        let exec = Arc::new(ScriptedExecutor::new().on_error("sky launch", "spawn failed"));
        assert!(
            matches!(launch(exec.clone()), TaskOutcome::Failed(message) if message.contains("spawn failed"))
        );
        assert!(matches!(
            exec.effects().last(),
            Some(Effect::TemporaryFileRemoved { .. })
        ));
    }

    #[test]
    fn unsupported_temporary_files_stop_before_launch() {
        struct NoFiles;
        impl Executor for NoFiles {
            fn exec(&self, _: &CommandSpec) -> Result<ExecOutput, ExecError> {
                panic!("launch must not execute")
            }
        }
        assert!(
            matches!(launch(Arc::new(NoFiles)), TaskOutcome::Failed(message) if message.contains("does not implement"))
        );
    }

    #[test]
    fn cleanup_errors_do_not_change_the_acquisition_outcome() {
        struct CleanupFailure {
            command_fails: bool,
        }
        impl Executor for CleanupFailure {
            fn exec(&self, _: &CommandSpec) -> Result<ExecOutput, ExecError> {
                if self.command_fails {
                    Err(ExecError::Scripted("command failed".into()))
                } else {
                    Ok(ExecOutput::ok("launched"))
                }
            }
            fn temporary_file(&self, _: &str, _: &[u8]) -> std::io::Result<TemporaryFile> {
                Ok(TemporaryFile::new("/virtual/task".into(), |_| {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "cleanup denied",
                    ))
                }))
            }
        }
        for command_fails in [false, true] {
            let ctx = DagContext::new();
            ctx.set_state(
                "executor",
                Arc::new(CleanupFailure { command_fails }) as Arc<dyn Executor>,
            );
            let result = SkyLaunchTask {
                cluster_name: "test".into(),
                task_yaml: "resources: {}".into(),
                cloud: None,
                region: None,
            }
            .execute(&ctx);
            assert_eq!(result.is_success(), !command_fails);
            assert_eq!(
                ctx.has_output(&TaskId("sky-launch:test".into())),
                !command_fails
            );
            assert!(cleanup_task_yaml(&ctx, "test")
                .unwrap_err()
                .to_string()
                .contains("cleanup denied"));
        }
    }
}
