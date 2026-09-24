//! DagTask implementations for Slurm job orchestration.
//!
//! Pipeline: build-job-env → copy-to-submit → submit → wait → collect
//!
//! All command execution runs through the `Arc<dyn Executor>` stored in
//! the context under the `"executor"` state key (see `submit_job`); a task
//! fails with `executor not in context` when the key is absent.

use std::sync::Arc;
use std::time::{Duration, Instant};

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};
use consortium_integration::exec::{CommandSpec, Executor, SshTarget};
use consortium_integration::staging;

/// Fetch the shared executor from the DAG context.
fn executor_from(ctx: &DagContext) -> Result<Arc<dyn Executor>, TaskOutcome> {
    ctx.get_state::<Arc<dyn Executor>>("executor")
        .ok_or_else(|| TaskOutcome::Failed("executor not in context".into()))
}

/// Build a hermetic job environment via nix.
///
/// Writes output: `build-job-env:{job}` → `String` (env store path)
pub struct NixBuildJobEnvTask {
    pub job_name: String,
    pub flake_attr: String,
}

impl NixBuildJobEnvTask {
    pub fn new(job_name: &str, flake_uri: &str) -> Self {
        Self {
            job_name: job_name.to_string(),
            flake_attr: format!("{flake_uri}#slurmEnvs.{job_name}"),
        }
    }
}

impl DagTask for NixBuildJobEnvTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        match staging::build_flake_attr(&*exec, &self.flake_attr, None) {
            Ok(path) => {
                ctx.set_output(TaskId(format!("build-job-env:{}", self.job_name)), path);
                TaskOutcome::Success
            }
            Err(e) => TaskOutcome::Failed(format!("build job env: {e}")),
        }
    }

    fn describe(&self) -> String {
        format!("build slurm job env '{}'", self.job_name)
    }
}

/// Copy the job environment to the submit node.
///
/// Reads: `build-job-env:{job}` → store path to copy
/// Writes output: `copy-job-env:{job}` → `String` (copied store path)
pub struct NixCopyToSubmitTask {
    pub job_name: String,
    pub submit_host: String,
    pub submit_user: String,
}

impl DagTask for NixCopyToSubmitTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let store_path: String =
            match ctx.get_output(&TaskId(format!("build-job-env:{}", self.job_name))) {
                Some(p) => p,
                None => return TaskOutcome::Failed("no job env build output".into()),
            };

        let store_uri = format!("ssh-ng://{}@{}", self.submit_user, self.submit_host);
        match staging::copy_closure(&*exec, &store_path, &store_uri) {
            Ok(()) => {
                ctx.set_output(
                    TaskId(format!("copy-job-env:{}", self.job_name)),
                    store_path,
                );
                TaskOutcome::Success
            }
            Err(e) => TaskOutcome::Failed(format!("copy job env: {e}")),
        }
    }

    fn describe(&self) -> String {
        format!("copy job env '{}' to {}", self.job_name, self.submit_host)
    }
}

/// Submit a slurm job via sbatch on the submit node (over ssh).
///
/// Reads: `copy-job-env:{job}` → env store path (exported into the job's PATH)
/// Writes output: `slurm-submit:{job}` → `u64` (slurm job id)
pub struct SlurmSubmitTask {
    pub job_name: String,
    pub script: String,
    pub partition: Option<String>,
    pub submit_host: String,
    pub submit_user: String,
}

impl DagTask for SlurmSubmitTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let mut args = vec![format!("--job-name={}", self.job_name)];
        if let Some(ref partition) = self.partition {
            args.push(format!("--partition={partition}"));
        }
        // If we have a nix env, set PATH in the job.
        if let Some(env_path) =
            ctx.get_output::<String>(&TaskId(format!("copy-job-env:{}", self.job_name)))
        {
            args.push(format!("--export=ALL,PATH={env_path}/bin:$PATH"));
        }
        args.push(self.script.clone());

        let spec = CommandSpec::new("sbatch")
            .args(args)
            .ssh(SshTarget::new(&self.submit_user, &self.submit_host));

        match exec.exec(&spec) {
            Ok(out) if out.success() => {
                // sbatch output: "Submitted batch job 12345"
                let job_id: u64 = out
                    .stdout
                    .trim()
                    .rsplit_once(' ')
                    .and_then(|(_, id)| id.parse().ok())
                    .unwrap_or(0);
                ctx.set_output(TaskId(format!("slurm-submit:{}", self.job_name)), job_id);
                TaskOutcome::Success
            }
            Ok(out) => TaskOutcome::Failed(format!("sbatch failed: {}", out.stderr.trim())),
            Err(e) => TaskOutcome::Failed(format!("sbatch exec failed: {e}")),
        }
    }

    fn describe(&self) -> String {
        format!("submit slurm job '{}'", self.job_name)
    }
}

/// Wait for a slurm job to complete by polling sacct on the submit node.
///
/// Reads: `slurm-submit:{job}` → `u64` job id
/// Writes output: `slurm-wait:{job}` → `u64` job id (once COMPLETED)
pub struct SlurmWaitTask {
    pub job_name: String,
    pub submit_host: String,
    pub submit_user: String,
    pub poll_interval: Duration,
    pub timeout: Option<Duration>,
}

impl SlurmWaitTask {
    pub fn new(job_name: &str, submit_host: &str, submit_user: &str) -> Self {
        Self {
            job_name: job_name.to_string(),
            submit_host: submit_host.to_string(),
            submit_user: submit_user.to_string(),
            poll_interval: Duration::from_secs(10),
            timeout: None,
        }
    }

    /// Set the sacct poll interval (default: 10s).
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Set an overall wait timeout (default: none).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl DagTask for SlurmWaitTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let job_id: u64 = match ctx.get_output(&TaskId(format!("slurm-submit:{}", self.job_name))) {
            Some(id) => id,
            None => return TaskOutcome::Failed("no job ID from submit".into()),
        };

        let start = Instant::now();

        loop {
            // Check timeout
            if let Some(timeout) = self.timeout {
                if start.elapsed() > timeout {
                    return TaskOutcome::Failed(format!(
                        "job {} timed out after {}s",
                        job_id,
                        start.elapsed().as_secs()
                    ));
                }
            }

            // Poll sacct for job state; the first output line is the state
            // (the pre-executor version piped through `| head -1`).
            let spec = CommandSpec::new("sacct")
                .args([
                    "-j".to_string(),
                    job_id.to_string(),
                    "--format=State".to_string(),
                    "--noheader".to_string(),
                    "--parsable2".to_string(),
                ])
                .ssh(SshTarget::new(&self.submit_user, &self.submit_host));

            match exec.exec(&spec) {
                Ok(out) if out.success() => {
                    let state = out.stdout.lines().next().unwrap_or("").trim().to_string();
                    match state.as_str() {
                        "COMPLETED" => {
                            ctx.set_output(TaskId(format!("slurm-wait:{}", self.job_name)), job_id);
                            return TaskOutcome::Success;
                        }
                        "FAILED" | "CANCELLED" | "TIMEOUT" | "OUT_OF_MEMORY" | "NODE_FAIL" => {
                            return TaskOutcome::Failed(format!(
                                "job {job_id} ended with state: {state}"
                            ));
                        }
                        // PENDING, RUNNING, etc. — keep polling
                        _ => {}
                    }
                }
                _ => {} // exec/ssh error — retry next poll
            }

            std::thread::sleep(self.poll_interval);
        }
    }

    fn describe(&self) -> String {
        format!("wait for slurm job '{}'", self.job_name)
    }
}

/// Collect results from a completed slurm job by cat-ing output files on the
/// submit node (over ssh).
///
/// The job id is read from the wait task's output when present and from the
/// submit task's output otherwise — `submit_job` wires this task after
/// `slurm-wait` when waiting is enabled and directly after `slurm-submit`
/// when it is not, so the read order mirrors the wiring.
///
/// Writes output: `slurm-collect:{job}` → `String` (file contents)
pub struct SlurmCollectTask {
    pub job_name: String,
    pub output_pattern: String,
    pub submit_host: String,
    pub submit_user: String,
}

impl DagTask for SlurmCollectTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let job_id: Option<u64> = ctx
            .get_output(&TaskId(format!("slurm-wait:{}", self.job_name)))
            .or_else(|| ctx.get_output(&TaskId(format!("slurm-submit:{}", self.job_name))));
        if job_id.is_none() {
            return TaskOutcome::Failed("no job ID from wait or submit".into());
        }

        let spec = CommandSpec::new("cat")
            .arg(&self.output_pattern)
            .ssh(SshTarget::new(&self.submit_user, &self.submit_host));

        match exec.exec(&spec) {
            Ok(out) if out.success() => {
                ctx.set_output(
                    TaskId(format!("slurm-collect:{}", self.job_name)),
                    out.stdout,
                );
                TaskOutcome::Success
            }
            Ok(out) => TaskOutcome::Failed(format!("collect failed: {}", out.stderr.trim())),
            Err(e) => TaskOutcome::Failed(format!("collect failed: {e}")),
        }
    }

    fn describe(&self) -> String {
        format!("collect results for '{}'", self.job_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    #[test]
    fn test_sbatch_output_parsing() {
        // sbatch outputs: "Submitted batch job 12345"
        let output = "Submitted batch job 12345";
        let job_id: u64 = output
            .trim()
            .rsplit_once(' ')
            .and_then(|(_, id)| id.parse().ok())
            .unwrap_or(0);
        assert_eq!(job_id, 12345);
    }

    #[test]
    fn test_sbatch_output_parsing_with_whitespace() {
        let output = "Submitted batch job 99999\n";
        let job_id: u64 = output
            .trim()
            .rsplit_once(' ')
            .and_then(|(_, id)| id.parse().ok())
            .unwrap_or(0);
        assert_eq!(job_id, 99999);
    }

    #[test]
    fn test_sbatch_output_parsing_unexpected() {
        let output = "Error: something went wrong";
        let job_id: u64 = output
            .trim()
            .rsplit_once(' ')
            .and_then(|(_, id)| id.parse().ok())
            .unwrap_or(0);
        assert_eq!(job_id, 0); // fallback to 0 on parse failure
    }

    #[test]
    fn test_slurm_job_states() {
        // Verify the wait task would recognize these terminal states
        let terminal_failure = [
            "FAILED",
            "CANCELLED",
            "TIMEOUT",
            "OUT_OF_MEMORY",
            "NODE_FAIL",
        ];
        let running = ["PENDING", "RUNNING", "COMPLETING"];

        for state in &terminal_failure {
            assert!(
                matches!(
                    *state,
                    "FAILED" | "CANCELLED" | "TIMEOUT" | "OUT_OF_MEMORY" | "NODE_FAIL"
                ),
                "{} should be terminal failure",
                state
            );
        }

        for state in &running {
            assert!(
                !matches!(
                    *state,
                    "COMPLETED"
                        | "FAILED"
                        | "CANCELLED"
                        | "TIMEOUT"
                        | "OUT_OF_MEMORY"
                        | "NODE_FAIL"
                ),
                "{} should continue polling",
                state
            );
        }
    }

    #[test]
    fn test_describe_methods() {
        let build = NixBuildJobEnvTask::new("rnaseq", ".");
        assert!(build.describe().contains("rnaseq"));

        let submit = SlurmSubmitTask {
            job_name: "test".to_string(),
            script: "test.sh".to_string(),
            partition: Some("gpu".to_string()),
            submit_host: "ctrl".to_string(),
            submit_user: "root".to_string(),
        };
        assert!(submit.describe().contains("test"));

        let wait = SlurmWaitTask::new("myjob", "ctrl", "root");
        assert!(wait.describe().contains("myjob"));
        assert_eq!(wait.poll_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_build_task_fails_without_executor_in_context() {
        let ctx = DagContext::new();
        match NixBuildJobEnvTask::new("train", ".").execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("executor not in context")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_build_task_runs_scripted_nix_build() {
        let scripted =
            Arc::new(ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/env\n")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);

        let outcome = NixBuildJobEnvTask::new("train", ".").execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted
            .assert_invoked_containing("nix build .#slurmEnvs.train --no-link --print-out-paths");
        let path: Option<String> = ctx.get_output(&TaskId("build-job-env:train".to_string()));
        assert_eq!(path.as_deref(), Some("/nix/store/env"));
    }

    #[test]
    fn test_copy_task_runs_scripted_nix_copy() {
        let scripted = Arc::new(ScriptedExecutor::new().on("nix copy", ExecOutput::ok("")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);
        ctx.set_output(
            TaskId("build-job-env:train".to_string()),
            "/nix/store/env".to_string(),
        );

        let task = NixCopyToSubmitTask {
            job_name: "train".to_string(),
            submit_host: "submit01".to_string(),
            submit_user: "root".to_string(),
        };
        let outcome = task.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing(
            "nix copy --no-check-sigs --to ssh-ng://root@submit01 /nix/store/env",
        );
        let copied: Option<String> = ctx.get_output(&TaskId("copy-job-env:train".to_string()));
        assert_eq!(copied.as_deref(), Some("/nix/store/env"));
    }

    #[test]
    fn test_submit_task_fails_without_executor_in_context() {
        let ctx = DagContext::new();
        let task = SlurmSubmitTask {
            job_name: "train".to_string(),
            script: "train.sh".to_string(),
            partition: None,
            submit_host: "submit01".to_string(),
            submit_user: "root".to_string(),
        };
        match task.execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("executor not in context")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_submit_task_runs_sbatch_over_ssh_and_parses_job_id() {
        let scripted = Arc::new(
            ScriptedExecutor::new().on("sbatch", ExecOutput::ok("Submitted batch job 12345\n")),
        );
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);
        ctx.set_output(
            TaskId("copy-job-env:train".to_string()),
            "/nix/store/env".to_string(),
        );

        let task = SlurmSubmitTask {
            job_name: "train".to_string(),
            script: "train.sh".to_string(),
            partition: Some("gpu".to_string()),
            submit_host: "submit01".to_string(),
            submit_user: "root".to_string(),
        };
        let outcome = task.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("sbatch");
        scripted.assert_invoked_containing("--export=ALL,PATH=/nix/store/env/bin:$PATH");
        let job_id: Option<u64> = ctx.get_output(&TaskId("slurm-submit:train".to_string()));
        assert_eq!(job_id, Some(12345));
    }

    #[test]
    fn test_wait_task_succeeds_on_completed_state() {
        let scripted = Arc::new(ScriptedExecutor::new().on("sacct", ExecOutput::ok("COMPLETED\n")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);
        ctx.set_output(TaskId("slurm-submit:train".to_string()), 12345u64);

        let task = SlurmWaitTask::new("train", "submit01", "root")
            .with_poll_interval(Duration::from_millis(1));
        let outcome = task.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("sacct");
        let job_id: Option<u64> = ctx.get_output(&TaskId("slurm-wait:train".to_string()));
        assert_eq!(job_id, Some(12345));
    }

    #[test]
    fn test_wait_task_fails_on_terminal_failure_state() {
        let scripted = Arc::new(ScriptedExecutor::new().on("sacct", ExecOutput::ok("FAILED\n")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);
        ctx.set_output(TaskId("slurm-submit:train".to_string()), 12345u64);

        let task = SlurmWaitTask::new("train", "submit01", "root")
            .with_poll_interval(Duration::from_millis(1));
        match task.execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("FAILED")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_collect_task_reads_job_id_from_submit_output() {
        let scripted = Arc::new(ScriptedExecutor::new().on("cat", ExecOutput::ok("payload\n")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);
        // Only the submit output is present (wait was disabled).
        ctx.set_output(TaskId("slurm-submit:train".to_string()), 12345u64);

        let task = SlurmCollectTask {
            job_name: "train".to_string(),
            output_pattern: "slurm-12345.out".to_string(),
            submit_host: "submit01".to_string(),
            submit_user: "root".to_string(),
        };
        let outcome = task.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("slurm-12345.out");
        let content: Option<String> = ctx.get_output(&TaskId("slurm-collect:train".to_string()));
        assert_eq!(content.as_deref(), Some("payload\n"));
    }

    #[test]
    fn test_collect_task_reads_job_id_from_wait_output() {
        let scripted = Arc::new(ScriptedExecutor::new().on("cat", ExecOutput::ok("payload\n")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);
        // The wait output is present (wait was enabled).
        ctx.set_output(TaskId("slurm-wait:train".to_string()), 12345u64);

        let task = SlurmCollectTask {
            job_name: "train".to_string(),
            output_pattern: "slurm-12345.out".to_string(),
            submit_host: "submit01".to_string(),
            submit_user: "root".to_string(),
        };
        let outcome = task.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
    }

    #[test]
    fn test_collect_task_fails_without_job_id() {
        let scripted = Arc::new(ScriptedExecutor::new().on("cat", ExecOutput::ok("")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);

        let task = SlurmCollectTask {
            job_name: "train".to_string(),
            output_pattern: "slurm-*.out".to_string(),
            submit_host: "submit01".to_string(),
            submit_user: "root".to_string(),
        };
        match task.execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("no job ID")),
            _ => panic!("expected Failed outcome"),
        }
        assert_eq!(scripted.invocation_count(), 0);
    }
}
