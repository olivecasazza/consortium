//! Nix evaluation — resolve toplevel store paths and detect changes.

use std::collections::HashMap;

use consortium_integration::exec::{CommandSpec, Executor, SshTarget};

use crate::config::{DeployAction, DeploymentPlan, DeploymentTarget, FleetConfig, ProfileType};
use crate::error::{NixError, Result};

/// Evaluate which hosts need deployment by comparing desired vs current state.
pub fn evaluate(
    exec: &dyn Executor,
    config: &FleetConfig,
    target_nodes: &[String],
    action: DeployAction,
    max_parallel: usize,
) -> Result<DeploymentPlan> {
    let mut plan = DeploymentPlan::new(action, max_parallel);

    for name in target_nodes {
        let node = config
            .nodes
            .get(name)
            .ok_or_else(|| NixError::General(format!("unknown node: {}", name)))?;

        // Get the expected toplevel path via nix eval
        let toplevel_path = eval_toplevel(exec, &config.flake_uri, name)?;

        plan.targets.push(DeploymentTarget {
            node: node.clone(),
            toplevel_path,
            current_system: None, // filled in during deploy if needed
            needs_build: true,
            needs_copy: true,
        });
    }

    Ok(plan)
}

/// The flake attribute holding a host's system toplevel:
/// `nixosConfigurations.<name>.config.system.build.toplevel` for NixOS,
/// `darwinConfigurations.<name>.config.system.build.toplevel` for nix-darwin.
/// Names that are not plain Nix identifiers are quoted.
pub fn toplevel_attr(flake_uri: &str, name: &str, profile_type: &ProfileType) -> String {
    let outputs = match profile_type {
        ProfileType::Nixos => "nixosConfigurations",
        ProfileType::NixDarwin => "darwinConfigurations",
    };
    format!(
        "{}#{}.{}.config.system.build.toplevel",
        flake_uri,
        outputs,
        attr_segment(name)
    )
}

/// Quote an attribute path segment unless it is a plain Nix identifier
/// (`[A-Za-z_][A-Za-z0-9_'-]*`), so `my.host` becomes `"my.host"`.
fn attr_segment(name: &str) -> String {
    let mut chars = name.chars();
    let plain = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '\'' | '-'));
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

/// Evaluate the toplevel store path for a single NixOS host via `nix eval`.
///
/// Runs `nix eval --raw <flake>#nixosConfigurations.<host>.config.system.build.toplevel.outPath`
/// through `exec`. Use [`eval_system_toplevel`] for nix-darwin hosts or
/// when extra nix arguments are needed.
pub fn eval_toplevel(exec: &dyn Executor, flake_uri: &str, hostname: &str) -> Result<String> {
    eval_system_toplevel(exec, flake_uri, hostname, &ProfileType::Nixos, &[])
}

/// Evaluate the toplevel store path for a host of either profile type.
///
/// Runs `nix eval --raw <toplevel attr>.outPath <extra_args...>` through
/// `exec`, where the attribute comes from [`toplevel_attr`].
pub fn eval_system_toplevel(
    exec: &dyn Executor,
    flake_uri: &str,
    hostname: &str,
    profile_type: &ProfileType,
    extra_args: &[String],
) -> Result<String> {
    let attr = format!(
        "{}.outPath",
        toplevel_attr(flake_uri, hostname, profile_type)
    );

    let spec = CommandSpec::new("nix")
        .args(["eval", "--raw", attr.as_str()])
        .args(extra_args.iter().cloned());
    let output = exec.exec(&spec).map_err(|e| NixError::EvalFailed {
        host: hostname.to_string(),
        message: format!("failed to run nix eval: {}", e),
    })?;

    if !output.success() {
        return Err(NixError::EvalFailed {
            host: hostname.to_string(),
            message: output.stderr,
        });
    }

    let path = output.stdout.trim().to_string();
    if path.is_empty() {
        return Err(NixError::EvalFailed {
            host: hostname.to_string(),
            message: "nix eval returned empty path".to_string(),
        });
    }

    Ok(path)
}

/// Query the current system generation on a remote host.
///
/// Runs `readlink /run/current-system` over ssh; a failed probe (host
/// unreachable, no current system) maps to `Ok(None)`.
pub fn query_current_system(exec: &dyn Executor, host: &str, user: &str) -> Result<Option<String>> {
    let spec = CommandSpec::new("readlink")
        .arg("/run/current-system")
        .ssh(SshTarget::new(user, host).extra_opt("-oConnectTimeout=10"));
    let output = exec.exec(&spec).map_err(|e| NixError::SshFailed {
        host: host.to_string(),
        message: format!("failed to query current system: {}", e),
    })?;

    if !output.success() {
        return Ok(None);
    }

    let path = output.stdout.trim().to_string();
    if path.is_empty() {
        Ok(None)
    } else {
        Ok(Some(path))
    }
}

/// Evaluate all hosts and return a map of hostname -> toplevel path.
pub fn eval_all(
    exec: &dyn Executor,
    flake_uri: &str,
    hostnames: &[String],
) -> Result<HashMap<String, String>> {
    let mut results = HashMap::new();
    // TODO: parallelize with consortium's Task/Worker infrastructure
    for hostname in hostnames {
        let path = eval_toplevel(exec, flake_uri, hostname)?;
        results.insert(hostname.clone(), path);
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    #[test]
    fn test_toplevel_attr_selects_outputs_by_profile_and_quotes_odd_names() {
        assert_eq!(
            toplevel_attr(".", "contra", &ProfileType::Nixos),
            ".#nixosConfigurations.contra.config.system.build.toplevel"
        );
        assert_eq!(
            toplevel_attr("github:me/cfg", "mac-01", &ProfileType::NixDarwin),
            "github:me/cfg#darwinConfigurations.mac-01.config.system.build.toplevel"
        );
        assert_eq!(
            toplevel_attr(".", "my.host", &ProfileType::Nixos),
            ".#nixosConfigurations.\"my.host\".config.system.build.toplevel"
        );
        assert_eq!(
            toplevel_attr(".", "01box", &ProfileType::Nixos),
            ".#nixosConfigurations.\"01box\".config.system.build.toplevel"
        );
    }

    #[test]
    fn test_eval_system_toplevel_darwin_appends_extra_args() {
        let exec = ScriptedExecutor::new().on(
            "nix eval --raw",
            ExecOutput::ok("/nix/store/abc-darwin-system\n"),
        );
        let extra = vec![
            "--override-input".to_string(),
            "foo".to_string(),
            "path:./stub".to_string(),
        ];
        let path =
            eval_system_toplevel(&exec, ".", "mac01", &ProfileType::NixDarwin, &extra).unwrap();
        assert_eq!(path, "/nix/store/abc-darwin-system");
        exec.assert_invoked_containing(
            "nix eval --raw .#darwinConfigurations.mac01.config.system.build.toplevel.outPath \
             --override-input foo path:./stub",
        );
    }

    #[test]
    fn test_eval_toplevel_success_returns_path() {
        let exec = ScriptedExecutor::new().on(
            "nix eval --raw",
            ExecOutput::ok("/nix/store/abc-nixos-system\n"),
        );
        let path = eval_toplevel(&exec, ".", "contra").unwrap();
        assert_eq!(path, "/nix/store/abc-nixos-system");
        exec.assert_invoked_containing(
            "nix eval --raw .#nixosConfigurations.contra.config.system.build.toplevel.outPath",
        );
        assert_eq!(exec.invocation_count(), 1);
    }

    #[test]
    fn test_eval_toplevel_failure_surfaces_stderr() {
        let exec = ScriptedExecutor::new().on(
            "nix eval",
            ExecOutput::new(1, "", "error: attribute 'nixosConfigurations.nope' missing"),
        );
        let err = eval_toplevel(&exec, ".", "nope").unwrap_err();
        match err {
            NixError::EvalFailed { host, message } => {
                assert_eq!(host, "nope");
                assert!(
                    message.contains("attribute 'nixosConfigurations.nope' missing"),
                    "stderr not propagated: {}",
                    message
                );
            }
            other => panic!("expected EvalFailed, got: {}", other),
        }
    }

    #[test]
    fn test_query_current_system_renders_ssh_readlink() {
        let exec =
            ScriptedExecutor::new().on("readlink", ExecOutput::ok("/nix/store/cur-system\n"));
        let current = query_current_system(&exec, "hp01", "root").unwrap();
        assert_eq!(current.as_deref(), Some("/nix/store/cur-system"));
        exec.assert_invoked_containing("ssh -oStrictHostKeyChecking=no");
        exec.assert_invoked_containing("-l root");
        exec.assert_invoked_containing("hp01");
        exec.assert_invoked_containing("readlink");
    }

    #[test]
    fn test_query_current_system_failure_is_none() {
        let exec = ScriptedExecutor::new().on("readlink", ExecOutput::new(1, "", "no such file"));
        let current = query_current_system(&exec, "hp01", "root").unwrap();
        assert_eq!(current, None);
    }
}
