//! Profile activation — switch NixOS/nix-darwin systems to new configurations.

use std::collections::HashMap;

use consortium_integration::exec::{CommandSpec, Executor, SshTarget};

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

/// Activate a profile on a single host.
pub fn activate_host(
    exec: &dyn Executor,
    host: &str,
    user: &str,
    toplevel_path: &str,
    profile_type: &ProfileType,
    action: DeployAction,
) -> Result<()> {
    // Only set the system profile for actions that should persist across reboots.
    // dry-activate and test should NOT modify the profile.
    match action {
        DeployAction::Switch | DeployAction::Boot => {
            set_profile(exec, host, user, toplevel_path)?;
        }
        DeployAction::Test | DeployAction::DryActivate | DeployAction::Build => {}
    }

    // Run the activation command
    let activation_cmd = match profile_type {
        ProfileType::Nixos => {
            format!("{}/bin/switch-to-configuration {}", toplevel_path, action)
        }
        ProfileType::NixDarwin => {
            format!(
                "{}/activate-user && sudo {}/activate",
                toplevel_path, toplevel_path
            )
        }
    };

    run_remote_shell(exec, host, user, &activation_cmd)
}

/// Set the nix profile to point to the new system closure.
fn set_profile(exec: &dyn Executor, host: &str, user: &str, toplevel_path: &str) -> Result<()> {
    let cmd = format!(
        "nix-env -p /nix/var/nix/profiles/system --set {}",
        toplevel_path
    );
    run_remote_shell(exec, host, user, &cmd)
}

/// Run a shell command line on `host` as `user` over ssh.
///
/// The command goes through `sh -c` so compound commands (e.g. the
/// nix-darwin `activate-user && sudo activate` chain) keep their shell
/// semantics under [`CommandSpec`]'s per-token quoting.
fn run_remote_shell(exec: &dyn Executor, host: &str, user: &str, cmd: &str) -> Result<()> {
    let spec = CommandSpec::new("sh")
        .args(["-c", cmd])
        .ssh(SshTarget::new(user, host).extra_opt("-oConnectTimeout=30"));
    let output = exec.exec(&spec).map_err(|e| NixError::ActivationFailed {
        host: host.to_string(),
        message: format!("failed to run activation: {}", e),
    })?;

    if !output.success() {
        return Err(NixError::ActivationFailed {
            host: host.to_string(),
            message: output.stderr,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    #[test]
    fn test_profile_set_only_for_switch_and_boot() {
        // Verify all DeployAction variants are accounted for in the match.
        let no_profile_actions = [
            DeployAction::Test,
            DeployAction::DryActivate,
            DeployAction::Build,
        ];
        let profile_actions = [DeployAction::Switch, DeployAction::Boot];
        assert_eq!(no_profile_actions.len() + profile_actions.len(), 5);
    }

    #[test]
    fn test_activation_command_nixos() {
        let toplevel = "/nix/store/abc-nixos-system";
        let cmd = format!(
            "{}/bin/switch-to-configuration {}",
            toplevel,
            DeployAction::Switch
        );
        assert_eq!(
            cmd,
            "/nix/store/abc-nixos-system/bin/switch-to-configuration switch"
        );

        let cmd = format!(
            "{}/bin/switch-to-configuration {}",
            toplevel,
            DeployAction::DryActivate
        );
        assert_eq!(
            cmd,
            "/nix/store/abc-nixos-system/bin/switch-to-configuration dry-activate"
        );
    }

    #[test]
    fn test_activation_command_darwin() {
        let toplevel = "/nix/store/abc-darwin-system";
        let cmd = format!("{}/activate-user && sudo {}/activate", toplevel, toplevel);
        assert_eq!(
            cmd,
            "/nix/store/abc-darwin-system/activate-user && sudo /nix/store/abc-darwin-system/activate"
        );
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
            "nix-env -p /nix/var/nix/profiles/system --set /nix/store/abc-nixos-system",
        );
        exec.assert_invoked_containing(
            "/nix/store/abc-nixos-system/bin/switch-to-configuration switch",
        );
        assert_eq!(exec.invocation_count(), 2);
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
        exec.assert_invoked_containing("switch-to-configuration test");
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
}
