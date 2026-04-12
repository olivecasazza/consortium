//! DagTask implementations for Ansible orchestration.
//!
//! Pipeline: build-env → copy-env → run-playbook → verify

use std::process::Command;

use consortium::dag::{DagContext, DagTask, TaskId, TaskOutcome};
use consortium_nix::{build, copy};

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
        match build::build_flake_attr(&self.flake_attr, None) {
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
        let store_path: String =
            match ctx.get_output(&TaskId(format!("build-ansible-env:{}", self.env_name))) {
                Some(p) => p,
                None => return TaskOutcome::Failed("no ansible env build output".into()),
            };

        let store_uri = format!("ssh-ng://{}@{}", self.target_user, self.target_host);
        match copy::copy_closure(&store_path, &store_uri) {
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
/// Reads: `copy-ansible-env:{env_name}` → ansible store path
pub struct AnsiblePlaybookTask {
    pub host: String,
    pub playbook: String,
    pub env_name: String,
    pub check_mode: bool,
}

impl AnsiblePlaybookTask {
    pub fn new(host: &str, playbook: &str, env_name: &str) -> Self {
        Self {
            host: host.to_string(),
            playbook: playbook.to_string(),
            env_name: env_name.to_string(),
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
        let ansible_env: String =
            match ctx.get_output(&TaskId(format!("copy-ansible-env:{}", self.env_name))) {
                Some(p) => p,
                None => return TaskOutcome::Failed("no ansible env in context".into()),
            };

        let ansible_bin = format!("{}/bin/ansible-playbook", ansible_env);

        let mut cmd = Command::new(&ansible_bin);
        cmd.args(["--limit", &self.host, &self.playbook]);

        if self.check_mode {
            cmd.arg("--check");
        }

        let output = match cmd.output() {
            Ok(o) => o,
            Err(e) => return TaskOutcome::Failed(format!("failed to run ansible: {}", e)),
        };

        if output.status.success() {
            TaskOutcome::Success
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            TaskOutcome::Failed(format!(
                "playbook failed on {}: {}",
                self.host,
                stderr.trim()
            ))
        }
    }

    fn describe(&self) -> String {
        format!("run {} on {}", self.playbook, self.host)
    }
}

/// Optional post-playbook verification.
pub struct AnsibleVerifyTask {
    pub host: String,
    pub check_command: String,
}

impl DagTask for AnsibleVerifyTask {
    fn execute(&self, _ctx: &DagContext) -> TaskOutcome {
        let output = Command::new("ssh")
            .args([
                "-oStrictHostKeyChecking=no",
                "-oPasswordAuthentication=no",
                "-oConnectTimeout=10",
                &self.host,
                &self.check_command,
            ])
            .output();

        match output {
            Ok(o) if o.status.success() => TaskOutcome::Success,
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                TaskOutcome::Failed(format!("verify failed on {}: {}", self.host, stderr.trim()))
            }
            Err(e) => TaskOutcome::Failed(format!("verify failed on {}: {}", self.host, e)),
        }
    }

    fn describe(&self) -> String {
        format!("verify {}", self.host)
    }
}
