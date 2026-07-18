//! DagTask implementations for Ansible orchestration.
//!
//! Pipeline: build-env → copy-env → run-playbook
//!
//! All command execution runs through the `Arc<dyn Executor>` stored in
//! the context under the `"executor"` state key (see
//! [`crate::run_playbook`]); a task fails with `executor not in context`
//! when the key is absent.

use std::sync::Arc;

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};
use consortium_integration::exec::{CommandSpec, Executor, SshTarget};
use consortium_integration::staging;

/// Fetch the shared executor from the DAG context.
fn executor_from(ctx: &DagContext) -> Result<Arc<dyn Executor>, TaskOutcome> {
    ctx.get_state::<Arc<dyn Executor>>("executor")
        .ok_or_else(|| TaskOutcome::Failed("executor not in context".into()))
}

/// Build a hermetic ansible environment via nix.
///
/// Writes output: `build-ansible-env:{env_name}` → String (store path)
pub struct NixBuildAnsibleEnvTask {
    pub env_name: String,
    pub flake_attr: String,
}

impl NixBuildAnsibleEnvTask {
    pub fn new(env_name: &str, flake_uri: &str) -> Self {
        Self {
            env_name: env_name.to_string(),
            flake_attr: format!("{}#ansibleEnvs.{}", flake_uri, env_name),
        }
    }
}

impl DagTask for NixBuildAnsibleEnvTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        match staging::build_flake_attr(&*exec, &self.flake_attr, None) {
            Ok(path) => {
                ctx.set_output(TaskId(format!("build-ansible-env:{}", self.env_name)), path);
                TaskOutcome::Success
            }
            Err(e) => TaskOutcome::Failed(format!("build ansible env: {}", e)),
        }
    }

    fn describe(&self) -> String {
        format!("build ansible environment '{}'", self.env_name)
    }
}

/// Copy the ansible environment to the control node.
///
/// Reads: `build-ansible-env:{env_name}` → store path
/// Writes: `copy-ansible-env:{env_name}` → store path
pub struct NixCopyAnsibleEnvTask {
    pub env_name: String,
    pub target_host: String,
    pub target_user: String,
}

impl DagTask for NixCopyAnsibleEnvTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let store_path: String =
            match ctx.get_output(&TaskId(format!("build-ansible-env:{}", self.env_name))) {
                Some(p) => p,
                None => return TaskOutcome::Failed("no ansible env build output".into()),
            };

        let store_uri = format!("ssh-ng://{}@{}", self.target_user, self.target_host);
        match staging::copy_closure(&*exec, &store_path, &store_uri) {
            Ok(()) => {
                ctx.set_output(
                    TaskId(format!("copy-ansible-env:{}", self.env_name)),
                    store_path,
                );
                TaskOutcome::Success
            }
            Err(e) => TaskOutcome::Failed(format!("copy ansible env: {}", e)),
        }
    }

    fn describe(&self) -> String {
        format!(
            "copy ansible env '{}' to {}",
            self.env_name, self.target_host
        )
    }
}

/// Run an ansible playbook against a specific host.
///
/// The playbook executes on the ansible control node over ssh.
///
/// Reads: `copy-ansible-env:{env_name}` → ansible store path
pub struct AnsiblePlaybookTask {
    pub host: String,
    pub playbook: String,
    pub env_name: String,
    pub control_host: String,
    pub control_user: String,
    pub check_mode: bool,
}

impl AnsiblePlaybookTask {
    pub fn new(
        host: &str,
        playbook: &str,
        env_name: &str,
        control_host: &str,
        control_user: &str,
    ) -> Self {
        Self {
            host: host.to_string(),
            playbook: playbook.to_string(),
            env_name: env_name.to_string(),
            control_host: control_host.to_string(),
            control_user: control_user.to_string(),
            check_mode: false,
        }
    }

    pub fn with_check(mut self, check: bool) -> Self {
        self.check_mode = check;
        self
    }
}

impl DagTask for AnsiblePlaybookTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let ansible_env: String =
            match ctx.get_output(&TaskId(format!("copy-ansible-env:{}", self.env_name))) {
                Some(p) => p,
                None => return TaskOutcome::Failed("no ansible env in context".into()),
            };

        let ansible_bin = format!("{}/bin/ansible-playbook", ansible_env);

        let mut spec = CommandSpec::new(ansible_bin).args([
            "--limit",
            self.host.as_str(),
            self.playbook.as_str(),
        ]);
        if self.check_mode {
            spec = spec.arg("--check");
        }
        let spec = spec.ssh(SshTarget::new(&self.control_user, &self.control_host));

        match exec.exec(&spec) {
            Ok(out) if out.success() => TaskOutcome::Success,
            Ok(out) => TaskOutcome::Failed(format!(
                "playbook failed on {}: {}",
                self.host,
                out.stderr.trim()
            )),
            Err(e) => TaskOutcome::Failed(format!("playbook failed on {}: {}", self.host, e)),
        }
    }

    fn describe(&self) -> String {
        format!("run {} on {}", self.playbook, self.host)
    }
}

/// Optional post-playbook verification (runs over ssh).
pub struct AnsibleVerifyTask {
    pub host: String,
    pub user: String,
    pub check_command: String,
}

impl DagTask for AnsibleVerifyTask {
    fn execute(&self, ctx: &DagContext) -> TaskOutcome {
        let exec = match executor_from(ctx) {
            Ok(e) => e,
            Err(outcome) => return outcome,
        };

        let spec = CommandSpec::new("sh")
            .args(["-c", self.check_command.as_str()])
            .ssh(SshTarget::new(&self.user, &self.host));

        match exec.exec(&spec) {
            Ok(out) if out.success() => TaskOutcome::Success,
            Ok(out) => TaskOutcome::Failed(format!(
                "verify failed on {}: {}",
                self.host,
                out.stderr.trim()
            )),
            Err(e) => TaskOutcome::Failed(format!("verify failed on {}: {}", self.host, e)),
        }
    }

    fn describe(&self) -> String {
        format!("verify {}", self.host)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    #[test]
    fn test_build_task_fails_without_executor_in_context() {
        let ctx = DagContext::new();
        match NixBuildAnsibleEnvTask::new("default", ".").execute(&ctx) {
            TaskOutcome::Failed(msg) => assert!(msg.contains("executor not in context")),
            _ => panic!("expected Failed outcome"),
        }
    }

    #[test]
    fn test_build_task_runs_scripted_nix_build() {
        let scripted = Arc::new(
            ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/abc-env\n")),
        );
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);

        let outcome = NixBuildAnsibleEnvTask::new("default", ".").execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("nix build .#ansibleEnvs.default");
        let built: Option<String> = ctx.get_output(&TaskId("build-ansible-env:default".into()));
        assert_eq!(built.as_deref(), Some("/nix/store/abc-env"));
    }

    #[test]
    fn test_playbook_task_runs_scripted_ssh_playbook() {
        let scripted = Arc::new(ScriptedExecutor::new().on("ansible-playbook", ExecOutput::ok("")));
        let exec: Arc<dyn Executor> = scripted.clone();
        let ctx = DagContext::new();
        ctx.set_state("executor", exec);
        ctx.set_output(
            TaskId("copy-ansible-env:default".into()),
            "/nix/store/abc-env".to_string(),
        );

        let task = AnsiblePlaybookTask::new("node01", "site.yml", "default", "ctrl", "root")
            .with_check(true);
        let outcome = task.execute(&ctx);
        assert!(matches!(outcome, TaskOutcome::Success));
        scripted.assert_invoked_containing("ssh -oStrictHostKeyChecking=no");
        scripted.assert_invoked_containing("/nix/store/abc-env/bin/ansible-playbook");
        scripted.assert_invoked_containing("--check");
        scripted.assert_invoked_containing("node01");
    }
}
