//! DagTask implementations for Ray job orchestration.
//!
//! All command execution runs through the `Arc<dyn Executor>` stored in
//! the context under the `"executor"` state key (see `submit_job()`); a
//! task fails with `executor not in context` when the key is absent.

use std::sync::Arc;
use std::time::{Duration, Instant};

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};
use consortium_integration::exec::{CommandSpec, Executor};
use consortium_integration::staging;

/// Fetch the shared executor from the DAG context.
fn executor_from(ctx: &DagContext) -> Result<Arc<dyn Executor>, TaskOutcome> {
    ctx.get_state::<Arc<dyn Executor>>("executor")
        .ok_or_else(|| TaskOutcome::Failed("executor not in context".into()))
}

/// Parse the ray job ID out of `ray job submit` stdout.
///
/// Ray prints a line containing the `raysubmit_...` identifier; the whole
/// line (trimmed) is taken as the ID, matching historical behavior.
fn parse_job_id(stdout: &str) -> String {
    stdout
        .lines()
        .find(|l| l.contains("raysubmit_"))
        .unwrap_or("unknown")
        .trim()
        .to_string()
}

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
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        match staging::build_flake_attr(&*exec, &self.flake_attr, None) {
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
///
/// The `ray` CLI runs locally against the cluster's head address.
pub struct RaySubmitTask {
    pub job_name: String,
    pub entrypoint: String,
    pub head_address: String,
    pub dashboard_port: u16,
    pub working_dir: Option<String>,
}

impl DagTask for RaySubmitTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let address = format!("http://{}:{}", self.head_address, self.dashboard_port);

        let mut spec = CommandSpec::new("ray").args([
            "job".to_string(),
            "submit".to_string(),
            "--address".to_string(),
            address,
        ]);

        // Use nix-built working dir if available
        if let Some(ref dir) = self.working_dir {
            spec = spec.args(["--working-dir", dir.as_str()]);
        } else if let Some(env_path) =
            ctx.get_output::<String>(&TaskId(format!("build-ray-env:{}", self.job_name)))
        {
            spec = spec.args(["--working-dir".to_string(), env_path]);
        }

        spec = spec.arg("--").arg(&self.entrypoint);

        let output = match exec.exec(&spec) {
            Ok(o) => o,
            Err(e) => return TaskOutcome::Failed(format!("ray submit failed: {}", e)),
        };

        if output.success() {
            let job_id = parse_job_id(&output.stdout);
            ctx.set_output(TaskId(format!("ray-submit:{}", self.job_name)), job_id);
            TaskOutcome::Success
        } else {
            TaskOutcome::Failed(format!("ray submit failed: {}", output.stderr.trim()))
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
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

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

            let spec = CommandSpec::new("ray").args([
                "job".to_string(),
                "status".to_string(),
                "--address".to_string(),
                address.clone(),
                job_id.clone(),
            ]);

            match exec.exec(&spec) {
                Ok(o) if o.success() => {
                    if o.stdout.contains("SUCCEEDED") {
                        ctx.set_output(TaskId(format!("ray-wait:{}", self.job_name)), job_id);
                        return TaskOutcome::Success;
                    } else if o.stdout.contains("FAILED") || o.stdout.contains("STOPPED") {
                        return TaskOutcome::Failed(format!(
                            "ray job {} ended: {}",
                            job_id,
                            o.stdout.trim()
                        ));
                    }
                    // RUNNING/PENDING: fall through and poll again.
                }
                Ok(_) => {
                    // Non-zero exit (transient API hiccup): poll again.
                }
                Err(e) => {
                    // The CLI itself could not run; retrying is pointless.
                    return TaskOutcome::Failed(format!("ray status failed: {}", e));
                }
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
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    fn test_ctx(exec: Arc<dyn Executor>) -> DagContext {
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);
        ctx
    }

    #[test]
    fn test_ray_submit_job_id_parsing() {
        // Ray job submit outputs a line like "raysubmit_abc123"
        let stdout = "Job submitted successfully\nraysubmit_abc123def\nDone.";
        assert_eq!(parse_job_id(stdout), "raysubmit_abc123def");
    }

    #[test]
    fn test_ray_submit_no_job_id() {
        assert_eq!(parse_job_id("Some error output"), "unknown");
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

        let wait = RayWaitTask {
            job_name: "train".to_string(),
            head_address: "localhost".to_string(),
            dashboard_port: 8265,
            poll_interval: Duration::from_secs(10),
            timeout: None,
        };
        assert!(wait.describe().contains("train"));
    }

    #[test]
    fn test_build_task_fails_without_executor_in_context() {
        let ctx = DagContext::new();
        match NixBuildRayEnvTask::new("train", ".").execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("executor not in context")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_submit_task_fails_without_executor_in_context() {
        let ctx = DagContext::new();
        let submit = RaySubmitTask {
            job_name: "train".to_string(),
            entrypoint: "python train.py".to_string(),
            head_address: "localhost".to_string(),
            dashboard_port: 8265,
            working_dir: None,
        };
        match submit.execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("executor not in context")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_build_task_runs_scripted_nix_build() {
        let scripted = Arc::new(
            ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/abc-ray-env\n")),
        );
        let ctx = test_ctx(scripted.clone());

        let outcome = NixBuildRayEnvTask::new("train", ".").execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("nix build .#rayEnvs.train --no-link --print-out-paths");
        let path: Option<String> = ctx.get_output(&TaskId("build-ray-env:train".to_string()));
        assert_eq!(path.as_deref(), Some("/nix/store/abc-ray-env"));
    }

    #[test]
    fn test_submit_task_runs_scripted_ray_submit() {
        let scripted = Arc::new(ScriptedExecutor::new().on(
            "job submit",
            ExecOutput::ok("Job submitted successfully\nraysubmit_abc123\n"),
        ));
        let ctx = test_ctx(scripted.clone());
        ctx.set_output(
            TaskId("build-ray-env:train".to_string()),
            "/nix/store/abc-ray-env".to_string(),
        );

        let submit = RaySubmitTask {
            job_name: "train".to_string(),
            entrypoint: "python train.py".to_string(),
            head_address: "ray-head.local".to_string(),
            dashboard_port: 8265,
            working_dir: None,
        };
        let outcome = submit.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing(
            "ray job submit --address http://ray-head.local:8265 \
             --working-dir /nix/store/abc-ray-env -- python train.py",
        );
        let job_id: Option<String> = ctx.get_output(&TaskId("ray-submit:train".to_string()));
        assert_eq!(job_id.as_deref(), Some("raysubmit_abc123"));
    }

    #[test]
    fn test_wait_task_succeeds_on_succeeded_status() {
        let scripted = Arc::new(
            ScriptedExecutor::new().on("job status", ExecOutput::ok("Status: SUCCEEDED\n")),
        );
        let ctx = test_ctx(scripted.clone());
        ctx.set_output(
            TaskId("ray-submit:train".to_string()),
            "raysubmit_abc123".to_string(),
        );

        let wait = RayWaitTask {
            job_name: "train".to_string(),
            head_address: "ray-head.local".to_string(),
            dashboard_port: 8265,
            poll_interval: Duration::from_millis(0),
            timeout: None,
        };
        let outcome = wait.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing(
            "ray job status --address http://ray-head.local:8265 raysubmit_abc123",
        );
    }

    #[test]
    fn test_wait_task_fails_on_failed_status() {
        let scripted = Arc::new(
            ScriptedExecutor::new().on("job status", ExecOutput::ok("Status: FAILED\n")),
        );
        let ctx = test_ctx(scripted);
        ctx.set_output(
            TaskId("ray-submit:train".to_string()),
            "raysubmit_abc123".to_string(),
        );

        let wait = RayWaitTask {
            job_name: "train".to_string(),
            head_address: "ray-head.local".to_string(),
            dashboard_port: 8265,
            poll_interval: Duration::from_millis(0),
            timeout: None,
        };
        match wait.execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("raysubmit_abc123")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_wait_task_fails_without_submit_output() {
        let scripted = Arc::new(ScriptedExecutor::new());
        let ctx = test_ctx(scripted);

        let wait = RayWaitTask {
            job_name: "train".to_string(),
            head_address: "ray-head.local".to_string(),
            dashboard_port: 8265,
            poll_interval: Duration::from_millis(0),
            timeout: None,
        };
        match wait.execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("no ray job ID")),
            _ => panic!("expected Failed outcome"),
        }
    }
}
