//! Profile activation — switch NixOS/nix-darwin systems to new configurations.
//!
//! Activation runs either over ssh ([`activate_host`]) or on the machine
//! running the deploy ([`activate_local`], used when a target *is* this
//! host). Both sites issue the same command sequence:
//!
//! - NixOS: `nix-env -p /nix/var/nix/profiles/system --set <toplevel>`
//!   (switch/boot only), then `<toplevel>/bin/switch-to-configuration <action>`.
//! - nix-darwin: the same profile set (switch/boot only), then the legacy
//!   `<toplevel>/activate-user` *only if* it is executable and is not
//!   nix-darwin's deprecated stub (a file containing the line
//!   `# nix-darwin: deprecated`, which `darwin-rebuild` skips the same way),
//!   then `<toplevel>/activate`.
//!
//! Privileged commands (profile set, `switch-to-configuration`, darwin
//! `activate`) are prefixed with `sudo` unless the remote user is `root`;
//! local activation always uses `sudo`. `activate-user` runs as the login
//! user, matching `darwin-rebuild`'s `sudo --user=$SUDO_USER` behaviour.

use std::collections::HashMap;

use consortium_integration::exec::{CommandSpec, ExecOutput, Executor, SshTarget};

use crate::config::{DeployAction, DeploymentPlan, ProfileType};
use crate::error::{NixError, Result};

/// Activation results keyed by hostname.
pub struct ActivationResults {
    /// Hosts that were successfully activated.
    pub succeeded: Vec<String>,
    /// Map of hostname -> activation error.
    pub errors: HashMap<String, NixError>,
}

/// Activate profiles on all targets in the deployment plan.
pub fn activate_all(exec: &dyn Executor, plan: &DeploymentPlan) -> Result<ActivationResults> {
    let mut results = ActivationResults {
        succeeded: Vec::new(),
        errors: HashMap::new(),
    };

    if plan.action == DeployAction::Build {
        // Build-only mode, skip activation
        results.succeeded = plan.targets.iter().map(|t| t.node.name.clone()).collect();
        return Ok(results);
    }

    // TODO: parallelize with consortium's SshWorker + fanout
    // TODO: support rolling activation (sequential with health checks)
    for target in &plan.targets {
        match activate_host(
            exec,
            &target.node.target_host,
            &target.node.target_user,
            &target.toplevel_path,
            &target.node.profile_type,
            plan.action,
        ) {
            Ok(()) => {
                results.succeeded.push(target.node.name.clone());
            }
            Err(e) => {
                results.errors.insert(target.node.name.clone(), e);
            }
        }
    }

    Ok(results)
}

/// Activate a profile on a single host over ssh as `user`.
pub fn activate_host(
    exec: &dyn Executor,
    host: &str,
    user: &str,
    toplevel_path: &str,
    profile_type: &ProfileType,
    action: DeployAction,
) -> Result<()> {
    let site = Site::Remote {
        target: SshTarget::new(user, host).extra_opt("-oConnectTimeout=30"),
        sudo: user != "root",
    };
    activate_at(exec, &site, host, toplevel_path, profile_type, action)
}

/// Activate a profile on the machine running the deploy: no ssh, privileged
/// steps via `sudo`. `label` names the host in error messages.
pub fn activate_local(
    exec: &dyn Executor,
    label: &str,
    toplevel_path: &str,
    profile_type: &ProfileType,
    action: DeployAction,
) -> Result<()> {
    activate_at(
        exec,
        &Site::Local,
        label,
        toplevel_path,
        profile_type,
        action,
    )
}

/// Where activation commands run.
enum Site {
    /// Over ssh; `sudo` says whether privileged steps need a prefix.
    Remote { target: SshTarget, sudo: bool },
    /// On this machine; privileged steps always go through `sudo`.
    Local,
}

impl Site {
    fn needs_sudo(&self) -> bool {
        match self {
            Site::Remote { sudo, .. } => *sudo,
            Site::Local => true,
        }
    }

    fn spec(&self, program: &str, args: &[&str]) -> CommandSpec {
        let spec = CommandSpec::new(program).args(args.iter().copied());
        match self {
            Site::Remote { target, .. } => spec.ssh(target.clone()),
            Site::Local => spec,
        }
    }

    /// A command that must run as root.
    fn privileged(&self, program: &str, args: &[&str]) -> CommandSpec {
        if self.needs_sudo() {
            let mut all = Vec::with_capacity(args.len() + 1);
            all.push(program);
            all.extend_from_slice(args);
            self.spec("sudo", &all)
        } else {
            self.spec(program, args)
        }
    }
}

fn activate_at(
    exec: &dyn Executor,
    site: &Site,
    label: &str,
    toplevel_path: &str,
    profile_type: &ProfileType,
    action: DeployAction,
) -> Result<()> {
    // Only set the system profile for actions that should persist across reboots.
    // dry-activate and test should NOT modify the profile.
    match action {
        DeployAction::Switch | DeployAction::Boot => {
            let spec = site.privileged(
                "nix-env",
                &["-p", "/nix/var/nix/profiles/system", "--set", toplevel_path],
            );
            run_checked(exec, label, &spec)?;
        }
        DeployAction::Test | DeployAction::DryActivate | DeployAction::Build => {}
    }

    match profile_type {
        ProfileType::Nixos => {
            let program = format!("{}/bin/switch-to-configuration", toplevel_path);
            let spec = site.privileged(&program, &[&action.to_string()]);
            run_checked(exec, label, &spec)?;
        }
        ProfileType::NixDarwin => {
            let activate_user = format!("{}/activate-user", toplevel_path);
            if has_legacy_activate_user(exec, site, label, &activate_user)? {
                run_checked(exec, label, &site.spec(&activate_user, &[]))?;
            }
            let activate = format!("{}/activate", toplevel_path);
            run_checked(exec, label, &site.privileged(&activate, &[]))?;
        }
    }

    Ok(())
}

/// nix-darwin's deprecation marker: `darwin-rebuild` skips `activate-user`
/// when the script contains exactly this line.
const ACTIVATE_USER_DEPRECATED_MARKER: &str = "# nix-darwin: deprecated";

/// Whether `<toplevel>/activate-user` exists, is executable, and is a real
/// legacy user-activation script rather than the deprecated stub. Mirrors
/// the `darwin-rebuild` check; the probe's exit status is the answer.
fn has_legacy_activate_user(
    exec: &dyn Executor,
    site: &Site,
    label: &str,
    activate_user: &str,
) -> Result<bool> {
    let script = format!(
        "test -x {p} && ! grep -q \"^{marker}$\" {p}",
        p = activate_user,
        marker = ACTIVATE_USER_DEPRECATED_MARKER
    );
    let output = run(exec, label, &site.spec("sh", &["-c", &script]))?;
    Ok(output.success())
}

/// Run `spec`, mapping a spawn/transport failure to [`NixError::ActivationFailed`].
fn run(exec: &dyn Executor, label: &str, spec: &CommandSpec) -> Result<ExecOutput> {
    exec.exec(spec).map_err(|e| NixError::ActivationFailed {
        host: label.to_string(),
        message: format!("failed to run activation: {}", e),
    })
}

/// Run `spec` and require exit status 0, surfacing stderr otherwise.
fn run_checked(exec: &dyn Executor, label: &str, spec: &CommandSpec) -> Result<()> {
    let output = run(exec, label, spec)?;
    if !output.success() {
        return Err(NixError::ActivationFailed {
            host: label.to_string(),
            message: output.stderr,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    const DARWIN: &str = "/nix/store/abc-darwin-system";

    /// Invocations that *run* `activate-user` (as opposed to the probe,
    /// which mentions it inside a `grep` script).
    fn activate_user_runs(exec: &ScriptedExecutor) -> Vec<String> {
        exec.invocations()
            .into_iter()
            .filter(|c| c.contains("activate-user") && !c.contains("grep"))
            .collect()
    }

    /// A darwin host whose `activate-user` probe exits with `probe_status`.
    fn darwin_exec(probe_status: i32) -> ScriptedExecutor {
        ScriptedExecutor::new()
            .on("grep -q", ExecOutput::new(probe_status, "", ""))
            .on("ssh", ExecOutput::ok(""))
    }

    #[test]
    fn test_activate_host_switch_renders_ssh_commands() {
        let exec = ScriptedExecutor::new().on("ssh", ExecOutput::ok(""));
        activate_host(
            &exec,
            "hp01",
            "root",
            "/nix/store/abc-nixos-system",
            &ProfileType::Nixos,
            DeployAction::Switch,
        )
        .unwrap();
        // Switch: profile set + switch-to-configuration, both over ssh.
        // Match on interior substrings — the remote command is single-quoted
        // inside the rendered ssh line.
        exec.assert_invoked_containing("ssh -oStrictHostKeyChecking=no");
        exec.assert_invoked_containing("-l root");
        exec.assert_invoked_containing("hp01");
        exec.assert_invoked_containing(
            "'nix-env' '-p' '/nix/var/nix/profiles/system' '--set' '/nix/store/abc-nixos-system'",
        );
        exec.assert_invoked_containing(
            "'/nix/store/abc-nixos-system/bin/switch-to-configuration' 'switch'",
        );
        // root needs no sudo.
        exec.assert_not_invoked_containing("sudo");
        assert_eq!(exec.invocation_count(), 2);
    }

    #[test]
    fn test_activate_host_non_root_user_uses_sudo() {
        let exec = ScriptedExecutor::new().on("ssh", ExecOutput::ok(""));
        activate_host(
            &exec,
            "hp01",
            "deploy",
            "/nix/store/abc-nixos-system",
            &ProfileType::Nixos,
            DeployAction::Switch,
        )
        .unwrap();
        exec.assert_invoked_containing("'sudo' 'nix-env' '-p' '/nix/var/nix/profiles/system'");
        exec.assert_invoked_containing(
            "'sudo' '/nix/store/abc-nixos-system/bin/switch-to-configuration' 'switch'",
        );
    }

    #[test]
    fn test_activate_host_test_action_skips_profile_set() {
        let exec = ScriptedExecutor::new().on("ssh", ExecOutput::ok(""));
        activate_host(
            &exec,
            "hp01",
            "root",
            "/nix/store/abc-nixos-system",
            &ProfileType::Nixos,
            DeployAction::Test,
        )
        .unwrap();
        exec.assert_not_invoked_containing("nix-env");
        exec.assert_invoked_containing("switch-to-configuration' 'test'");
        assert_eq!(exec.invocation_count(), 1);
    }

    #[test]
    fn test_activate_host_failure_surfaces_stderr() {
        let exec = ScriptedExecutor::new().on(
            "ssh",
            ExecOutput::new(1, "", "switch-to-configuration: permission denied"),
        );
        let err = activate_host(
            &exec,
            "hp01",
            "root",
            "/nix/store/abc-nixos-system",
            &ProfileType::Nixos,
            DeployAction::Switch,
        )
        .unwrap_err();
        match err {
            NixError::ActivationFailed { host, message } => {
                assert_eq!(host, "hp01");
                assert!(message.contains("permission denied"), "{}", message);
            }
            other => panic!("expected ActivationFailed, got: {}", other),
        }
    }

    #[test]
    fn test_darwin_switch_runs_legacy_activate_user_then_sudo_activate() {
        let exec = darwin_exec(0);
        activate_host(
            &exec,
            "mac01",
            "admin",
            DARWIN,
            &ProfileType::NixDarwin,
            DeployAction::Switch,
        )
        .unwrap();

        let calls = exec.invocations();
        assert_eq!(calls.len(), 4, "{calls:?}");
        assert!(calls[0].contains("'sudo' 'nix-env' '-p' '/nix/var/nix/profiles/system' '--set'"));
        assert!(
            calls[1].contains("test -x /nix/store/abc-darwin-system/activate-user && ! grep -q \"^# nix-darwin: deprecated$\" /nix/store/abc-darwin-system/activate-user"),
            "{}", calls[1]
        );
        assert_eq!(
            activate_user_runs(&exec).len(),
            1,
            "activate-user must run once when it is a real script"
        );
        assert!(calls[2].contains("'/nix/store/abc-darwin-system/activate-user'"));
        assert!(
            !calls[2].contains("sudo"),
            "activate-user runs as the login user"
        );
        assert!(calls[3].contains("'sudo' '/nix/store/abc-darwin-system/activate'"));
    }

    #[test]
    fn test_darwin_deprecated_activate_user_stub_is_skipped() {
        let exec = darwin_exec(1);
        activate_host(
            &exec,
            "mac01",
            "admin",
            DARWIN,
            &ProfileType::NixDarwin,
            DeployAction::Switch,
        )
        .unwrap();

        assert!(
            activate_user_runs(&exec).is_empty(),
            "deprecated stub must not run: {:?}",
            exec.invocations()
        );
        exec.assert_invoked_containing("'sudo' '/nix/store/abc-darwin-system/activate'");
        assert_eq!(exec.invocation_count(), 3);
    }

    #[test]
    fn test_darwin_test_action_skips_profile_but_activates() {
        let exec = darwin_exec(1);
        activate_host(
            &exec,
            "mac01",
            "admin",
            DARWIN,
            &ProfileType::NixDarwin,
            DeployAction::Test,
        )
        .unwrap();
        exec.assert_not_invoked_containing("nix-env");
        exec.assert_invoked_containing("'sudo' '/nix/store/abc-darwin-system/activate'");
    }

    #[test]
    fn test_activate_local_uses_sudo_without_ssh() {
        let exec = ScriptedExecutor::new()
            .on("grep -q", ExecOutput::new(1, "", ""))
            .on("", ExecOutput::ok(""));
        activate_local(
            &exec,
            "mac01",
            DARWIN,
            &ProfileType::NixDarwin,
            DeployAction::Switch,
        )
        .unwrap();
        exec.assert_not_invoked_containing("ssh");
        let calls = exec.invocations();
        assert_eq!(
            calls,
            vec![
                format!("sudo nix-env -p /nix/var/nix/profiles/system --set {DARWIN}"),
                format!(
                    "sh -c test -x {DARWIN}/activate-user && ! grep -q \"^# nix-darwin: deprecated$\" {DARWIN}/activate-user"
                ),
                format!("sudo {DARWIN}/activate"),
            ]
        );
    }

    #[test]
    fn test_activate_local_nixos_switch() {
        let exec = ScriptedExecutor::new().on("", ExecOutput::ok(""));
        activate_local(
            &exec,
            "box01",
            "/nix/store/abc-nixos-system",
            &ProfileType::Nixos,
            DeployAction::Switch,
        )
        .unwrap();
        assert_eq!(
            exec.invocations(),
            vec![
                "sudo nix-env -p /nix/var/nix/profiles/system --set /nix/store/abc-nixos-system"
                    .to_string(),
                "sudo /nix/store/abc-nixos-system/bin/switch-to-configuration switch".to_string(),
            ]
        );
    }
}
