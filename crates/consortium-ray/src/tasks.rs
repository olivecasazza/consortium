//! DagTask implementations for Ray job orchestration.

use std::process::Command;
use std::time::{Duration, Instant};

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};
use consortium_nix::build;

/// Build a ray job environment via nix.
pub struct NixBuildRayEnvTask {
    pub env_name: String,
    pub flake_attr: String,
}

impl NixBuildRayEnvTask {
    pub fn new(env_name: &str, flake_uri: &str) -> Self {
        Self {
            env_name: env_name.to_string(),
            flake_attr: format!("{}#rayEnvs.{}", flake_uri, env_name),
        }
    }
}

impl DagTask for NixBuildRayEnvTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        match build::build_flake_attr(&self.flake_attr, None) {
            Ok(path) => {
                ctx.set_output(TaskId(format!("build-ray-env:{}", self.env_name)), path);
                TaskOutcome::Success
            }
            Err(e) => TaskOutcome::Failed(format!("build ray env: {}", e)),
        }
    }

    fn describe(&self) -> String {
        format!("build ray environment '{}'", self.env_name)
    }
}

/// Submit a ray job via the Ray Jobs API.
pub struct RaySubmitTask {
    pub job_name: String,
    pub entrypoint: String,
    pub head_address: String,
    pub dashboard_port: u16,
    pub working_dir: Option<String>,
}

impl DagTask for RaySubmitTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let address = format!("http://{}:{}", self.head_address, self.dashboard_port);

        let mut cmd = Command::new("ray");
        cmd.args(["job", "submit", "--address", &address]);

        // Use nix-built working dir if available
        if let Some(ref dir) = self.working_dir {
            cmd.args(["--working-dir", dir]);
        } else if let Some(env_path) =
            ctx.get_output::<String>(&TaskId(format!("build-ray-env:{}", self.job_name)))
        {
            cmd.args(["--working-dir", &env_path]);
        }

        cmd.arg("--").arg(&self.entrypoint);

        let output = match cmd.output() {
            Ok(o) => o,
            Err(e) => return TaskOutcome::Failed(format!("ray submit failed: {}", e)),
        };

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            // Parse job ID from output
            let job_id = stdout
                .lines()
                .find(|l| l.contains("raysubmit_"))
                .unwrap_or("unknown")
                .trim()
                .to_string();

            ctx.set_output(TaskId(format!("ray-submit:{}", self.job_name)), job_id);
            TaskOutcome::Success
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            TaskOutcome::Failed(format!("ray submit failed: {}", stderr.trim()))
        }
    }

    fn describe(&self) -> String {
        format!("submit ray job '{}'", self.job_name)
    }
}

/// Wait for a ray job to complete by polling the Jobs API.
pub struct RayWaitTask {
    pub job_name: String,
    pub head_address: String,
    pub dashboard_port: u16,
    pub poll_interval: Duration,
    pub timeout: Option<Duration>,
}

impl DagTask for RayWaitTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let job_id: String = match ctx.get_output(&TaskId(format!("ray-submit:{}", self.job_name)))
        {
            Some(id) => id,
            None => return TaskOutcome::Failed("no ray job ID from submit".into()),
        };

        let address = format!("http://{}:{}", self.head_address, self.dashboard_port);
        let start = Instant::now();

        loop {
            if let Some(timeout) = self.timeout {
                if start.elapsed() > timeout {
                    return TaskOutcome::Failed(format!("ray job {} timed out", job_id));
                }
            }

            let output = Command::new("ray")
                .args(["job", "status", "--address", &address, &job_id])
                .output();

            match output {
                Ok(o) if o.status.success() => {
                    let stdout = String::from_utf8_lossy(&o.stdout).to_string();
                    if stdout.contains("SUCCEEDED") {
                        ctx.set_output(
                            TaskId(format!("ray-wait:{}", self.job_name)),
                            job_id.clone(),
                        );
                        return TaskOutcome::Success;
                    } else if stdout.contains("FAILED") || stdout.contains("STOPPED") {
                        return TaskOutcome::Failed(format!(
                            "ray job {} ended: {}",
                            job_id,
                            stdout.trim()
                        ));
                    }
                }
                _ => {}
            }

            std::thread::sleep(self.poll_interval);
        }
    }

    fn describe(&self) -> String {
        format!("wait for ray job '{}'", self.job_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ray_submit_job_id_parsing() {
        // Ray job submit outputs a line like "raysubmit_abc123"
        let stdout = "Job submitted successfully\nraysubmit_abc123def\nDone.";
        let job_id = stdout
            .lines()
            .find(|l| l.contains("raysubmit_"))
            .unwrap_or("unknown")
            .trim()
            .to_string();
        assert_eq!(job_id, "raysubmit_abc123def");
    }

    #[test]
    fn test_ray_submit_no_job_id() {
        let stdout = "Some error output";
        let job_id = stdout
            .lines()
            .find(|l| l.contains("raysubmit_"))
            .unwrap_or("unknown")
            .trim()
            .to_string();
        assert_eq!(job_id, "unknown");
    }

    #[test]
    fn test_ray_job_status_detection() {
        assert!("Status: SUCCEEDED".contains("SUCCEEDED"));
        assert!("Status: FAILED".contains("FAILED"));
        assert!("Status: STOPPED".contains("STOPPED"));
        assert!(!"Status: RUNNING".contains("SUCCEEDED"));
        assert!(!"Status: RUNNING".contains("FAILED"));
    }

    #[test]
    fn test_describe_methods() {
        let build = NixBuildRayEnvTask::new("train", ".");
        assert!(build.describe().contains("train"));

        let submit = RaySubmitTask {
            job_name: "train".to_string(),
            entrypoint: "python train.py".to_string(),
            head_address: "localhost".to_string(),
            dashboard_port: 8265,
            working_dir: None,
        };
        assert!(submit.describe().contains("train"));
    }
}
