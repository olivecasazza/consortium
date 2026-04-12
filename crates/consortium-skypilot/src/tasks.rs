//! DagTask implementations for SkyPilot orchestration.

use std::process::Command;

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};
use consortium_nix::build;

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
        match build::build_flake_attr(&self.flake_attr, None) {
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
        let mut cmd = Command::new("sky");
        cmd.args(["launch", "-c", &self.cluster_name, &self.task_yaml, "-y"]);

        if let Some(ref cloud) = self.cloud {
            cmd.args(["--cloud", cloud]);
        }
        if let Some(ref region) = self.region {
            cmd.args(["--region", region]);
        }

        let output = match cmd.output() {
            Ok(o) => o,
            Err(e) => return TaskOutcome::Failed(format!("sky launch failed: {}", e)),
        };

        if output.status.success() {
            ctx.set_output(
                TaskId(format!("sky-launch:{}", self.cluster_name)),
                self.cluster_name.clone(),
            );
            TaskOutcome::Success
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            TaskOutcome::Failed(format!("sky launch failed: {}", stderr.trim()))
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
        let output = Command::new("sky")
            .args(["exec", &self.cluster_name, "--", &self.command])
            .output();

        match output {
            Ok(o) if o.status.success() => {
                let stdout = String::from_utf8_lossy(&o.stdout).to_string();
                ctx.set_output(TaskId(format!("sky-exec:{}", self.cluster_name)), stdout);
                TaskOutcome::Success
            }
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                TaskOutcome::Failed(format!("sky exec failed: {}", stderr.trim()))
            }
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
    fn execute(&self, _ctx: &DagContext) -> TaskOutcome {
        let output = Command::new("sky")
            .args(["down", &self.cluster_name, "-y"])
            .output();

        match output {
            Ok(o) if o.status.success() => TaskOutcome::Success,
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                TaskOutcome::Failed(format!("sky down failed: {}", stderr.trim()))
            }
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
}
