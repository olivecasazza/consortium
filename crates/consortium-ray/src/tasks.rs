//! DagTask implementations for Ray job orchestration.
//!
//! All command execution runs through the `Arc<dyn Executor>` stored in
//! the context under the `"executor"` state key (see `submit_job()`); a
//! task fails with `executor not in context` when the key is absent.

use std::sync::Arc;
use std::time::Duration;

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
/// Only one distinct, nonempty identifier is accepted.
fn parse_job_id(stdout: &str) -> Option<String> {
    let mut ids = stdout
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .filter(|word| word.starts_with("raysubmit_") && word.len() > "raysubmit_".len());
    let id = ids.next()?;
    ids.all(|other| other == id).then(|| id.to_owned())
}

fn parse_status(stdout: &str, job_id: &str) -> Option<&'static str> {
    let mut found = None;
    for line in stdout.lines().map(str::trim) {
        let state = if line == format!("Job '{job_id}' succeeded") {
            Some("SUCCEEDED")
        } else if line == format!("Job '{job_id}' failed") {
            Some("FAILED")
        } else if line == format!("Job '{job_id}' was stopped") {
            Some("STOPPED")
        } else {
            let candidate = line
                .strip_prefix("Status: ")
                .or_else(|| line.strip_prefix(&format!("Status for job '{job_id}': ")))
                .unwrap_or(line);
            ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "STOPPED"]
                .into_iter()
                .find(|state| *state == candidate)
        };
        if let Some(state) = state {
            if found.is_some_and(|previous| previous != state) {
                return None;
            }
            found = Some(state);
        }
    }
    found
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
            "--no-wait".to_string(),
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

        spec = spec.args(["--", "sh", "-c", &self.entrypoint]);

        let output = match exec.exec(&spec) {
            Ok(o) => o,
            Err(e) => return TaskOutcome::Failed(format!("ray submit failed: {}", e)),
        };

        if output.success() {
            let Some(job_id) = parse_job_id(&output.stdout) else {
                return TaskOutcome::Failed("ray submit returned no unambiguous job ID".into());
            };
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
        if self.poll_interval.is_zero() {
            return TaskOutcome::Failed("poll interval must be positive".into());
        }
        let start = exec.monotonic_now();

        loop {
            if let Some(timeout) = self.timeout {
                if exec.monotonic_now().saturating_sub(start) >= timeout {
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

            let output = match self.timeout {
                Some(timeout) => exec.exec_timeout(
                    &spec,
                    timeout.saturating_sub(exec.monotonic_now().saturating_sub(start)),
                ),
                None => exec.exec(&spec),
            };
            match output {
                Ok(o) if o.success() => {
                    let Some(state) = parse_status(&o.stdout, &job_id) else {
                        return TaskOutcome::Failed(
                            "ray status returned no unambiguous state".into(),
                        );
                    };
                    if state == "SUCCEEDED" {
                        ctx.set_output(TaskId(format!("ray-wait:{}", self.job_name)), job_id);
                        return TaskOutcome::Success;
                    } else if matches!(state, "FAILED" | "STOPPED") {
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

            let delay = match self.timeout {
                Some(timeout) => self
                    .poll_interval
                    .min(timeout.saturating_sub(exec.monotonic_now().saturating_sub(start))),
                None => self.poll_interval,
            };
            if let Err(error) = exec.sleep(delay) {
                return TaskOutcome::Failed(format!("poll delay failed: {error}"));
            }
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
        assert_eq!(parse_job_id(stdout).as_deref(), Some("raysubmit_abc123def"));
    }

    #[test]
    fn test_ray_submit_no_job_id() {
        assert_eq!(parse_job_id("Some error output"), None);
    }

    #[test]
    fn job_id_requires_one_unambiguous_identifier() {
        for malformed in [
            "",
            "raysubmit_",
            "notraysubmit_abc",
            "raysubmit_a raysubmit_b",
        ] {
            assert_eq!(parse_job_id(malformed), None, "{malformed:?}");
        }
        assert_eq!(
            parse_job_id("Job 'raysubmit_abc-123' submitted").as_deref(),
            Some("raysubmit_abc-123")
        );
    }

    #[test]
    fn test_ray_job_status_detection() {
        for state in ["SUCCEEDED", "FAILED", "STOPPED", "RUNNING", "PENDING"] {
            assert_eq!(
                parse_status(&format!("Status: {state}"), "raysubmit_test"),
                Some(state)
            );
        }
        assert_eq!(
            parse_status(
                "Status: FAILED\nStatus message: SUCCEEDED",
                "raysubmit_test"
            ),
            Some("FAILED")
        );
        assert_eq!(
            parse_status("Status: FAILED\nStatus: SUCCEEDED", "raysubmit_test"),
            None
        );
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
            "ray job submit --no-wait --address http://ray-head.local:8265 \
             --working-dir /nix/store/abc-ray-env -- sh -c python train.py",
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
            poll_interval: Duration::from_millis(1),
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
        let scripted =
            Arc::new(ScriptedExecutor::new().on("job status", ExecOutput::ok("Status: FAILED\n")));
        let ctx = test_ctx(scripted);
        ctx.set_output(
            TaskId("ray-submit:train".to_string()),
            "raysubmit_abc123".to_string(),
        );

        let wait = RayWaitTask {
            job_name: "train".to_string(),
            head_address: "ray-head.local".to_string(),
            dashboard_port: 8265,
            poll_interval: Duration::from_millis(1),
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
            poll_interval: Duration::from_millis(1),
            timeout: None,
        };
        match wait.execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("no ray job ID")),
            _ => panic!("expected Failed outcome"),
        }
    }
}

#[cfg(test)]
mod poll_effect_tests {
    use super::*;
    use consortium_integration::exec::{Effect, ExecOutput, Rule, ScriptedExecutor};

    fn ctx(exec: Arc<dyn Executor>) -> DagContext {
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);
        ctx.set_output(TaskId("ray-submit:job".into()), "job-17".to_owned());
        ctx
    }
    fn task() -> RayWaitTask {
        RayWaitTask {
            job_name: "job".into(),
            head_address: "head".into(),
            dashboard_port: 8265,
            poll_interval: Duration::from_secs(10),
            timeout: Some(Duration::from_secs(3)),
        }
    }

    #[test]
    fn timeout_bounds_poll_command_and_sleep_without_wall_clock_waiting() {
        let exec = Arc::new(ScriptedExecutor::new().on("job status", ExecOutput::ok("RUNNING")));
        let outcome = task().execute(&ctx(exec.clone()));
        assert!(matches!(outcome, TaskOutcome::Failed(message) if message.contains("timed out")));
        assert_eq!(exec.invocation_count(), 1);
        assert_eq!(exec.monotonic_now(), Duration::from_secs(3));
        assert!(
            matches!(&exec.effects()[..], [Effect::Command { timeout: Some(limit), .. }, Effect::Sleep { duration }] if *limit == Duration::from_secs(3) && *duration == Duration::from_secs(3))
        );
    }

    #[test]
    fn hung_status_poll_is_bounded() {
        let exec = Arc::new(ScriptedExecutor::new().rule(Rule::slow_containing_all(
            ["job status"],
            Duration::from_secs(20),
            ExecOutput::ok("SUCCEEDED"),
        )));
        assert!(
            matches!(task().execute(&ctx(exec.clone())), TaskOutcome::Failed(message) if message.contains("status failed"))
        );
        assert_eq!(exec.invocation_count(), 1);
        assert_eq!(exec.monotonic_now(), Duration::from_secs(3));
    }

    #[test]
    fn zero_poll_interval_is_rejected_before_any_effect() {
        let exec = Arc::new(ScriptedExecutor::new());
        let mut task = task();
        task.poll_interval = Duration::ZERO;
        assert!(
            matches!(task.execute(&ctx(exec.clone())), TaskOutcome::Failed(message) if message.contains("positive"))
        );
        assert!(exec.effects().is_empty());
    }

    #[test]
    fn unsupported_sleep_stops_retrying_instead_of_using_host_sleep() {
        struct NoSleep;
        impl Executor for NoSleep {
            fn exec(
                &self,
                _: &CommandSpec,
            ) -> Result<ExecOutput, consortium_integration::ExecError> {
                Ok(ExecOutput::ok("RUNNING"))
            }
        }
        let mut task = task();
        task.timeout = None;
        assert!(
            matches!(task.execute(&ctx(Arc::new(NoSleep))), TaskOutcome::Failed(message) if message.contains("poll delay failed"))
        );
    }
}
