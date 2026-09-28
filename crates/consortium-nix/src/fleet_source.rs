//! Fleet discovery straight from a flake — no hand-built `fleet.json`.
//!
//! Two sources, tried in order by [`discover_fleet`]:
//!
//! 1. The flake's `fleet` output: an `mkFleet` result (its `configJson`
//!    string is parsed directly) or a derivation producing the JSON file
//!    (built with `nix build`, then read).
//! 2. The flake's `darwinConfigurations` and `nixosConfigurations` attribute
//!    names: each becomes a node whose `targetHost` is the attribute name,
//!    with the profile type implied by the output it came from and a
//!    matching `nixos` / `nix-darwin` tag. Only attribute names are
//!    evaluated, so this stays cheap even for large flakes.
//!
//! Every `nix` invocation goes through the [`Executor`], so the precedence
//! is testable with `ScriptedExecutor`.

use std::collections::HashMap;

use consortium_integration::exec::{CommandSpec, ExecOutput, Executor};

use crate::config::{DeploymentNode, FleetConfig, ProfileType};
use crate::error::{NixError, Result};

/// Where a discovered [`FleetConfig`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetOrigin {
    /// The flake's `fleet` output.
    FleetOutput,
    /// Derived from `darwinConfigurations` / `nixosConfigurations` names.
    Configurations,
}

impl std::fmt::Display for FleetOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FleetOrigin::FleetOutput => write!(f, "flake output `fleet`"),
            FleetOrigin::Configurations => {
                write!(f, "flake `darwinConfigurations` / `nixosConfigurations`")
            }
        }
    }
}

/// Load the fleet from `flake_uri`: its `fleet` output when present,
/// otherwise nodes derived from its system configurations (deployed as
/// `target_user`). Errors when the flake has neither.
pub fn discover_fleet(
    exec: &dyn Executor,
    flake_uri: &str,
    target_user: &str,
) -> Result<(FleetConfig, FleetOrigin)> {
    if let Some(config) = fleet_from_flake_output(exec, flake_uri)? {
        return Ok((config, FleetOrigin::FleetOutput));
    }
    let config = fleet_from_configurations(exec, flake_uri, target_user)?;
    if config.nodes.is_empty() {
        return Err(NixError::General(format!(
            "flake '{}' has no `fleet` output and no darwinConfigurations or nixosConfigurations",
            flake_uri
        )));
    }
    Ok((config, FleetOrigin::Configurations))
}

/// Nix expression applied to the `fleet` output to classify it without
/// forcing anything but the JSON string.
const CLASSIFY_FLEET: &str = "f: if f ? configJson then { json = f.configJson; } \
     else if (f.type or null) == \"derivation\" then { drv = true; } else { }";

/// The flake's `fleet` output as a [`FleetConfig`], or `None` when the
/// flake does not provide one.
pub fn fleet_from_flake_output(
    exec: &dyn Executor,
    flake_uri: &str,
) -> Result<Option<FleetConfig>> {
    let installable = format!("{}#fleet", flake_uri);
    let output = match eval_json(exec, &installable, CLASSIFY_FLEET)? {
        Some(o) => o,
        None => return Ok(None),
    };

    #[derive(serde::Deserialize)]
    struct Classified {
        #[serde(default)]
        json: Option<String>,
        #[serde(default)]
        drv: bool,
    }
    let classified: Classified = serde_json::from_str(output.stdout.trim()).map_err(|e| {
        NixError::General(format!(
            "unexpected output classifying {}: {} ({:?})",
            installable,
            e,
            output.stdout.trim()
        ))
    })?;

    let json = if let Some(json) = classified.json {
        json
    } else if classified.drv {
        let spec = CommandSpec::new("nix").args([
            "build",
            "--no-link",
            "--print-out-paths",
            installable.as_str(),
        ]);
        let built = run_nix(exec, &spec, &installable)?;
        if !built.success() {
            return Err(NixError::General(format!(
                "nix build {} failed: {}",
                installable,
                built.stderr.trim()
            )));
        }
        let path = built.stdout.trim();
        std::fs::read_to_string(path)
            .map_err(|e| NixError::General(format!("failed to read fleet JSON {}: {}", path, e)))?
    } else {
        return Err(NixError::General(format!(
            "{} is neither an mkFleet result (no `configJson`) nor a derivation",
            installable
        )));
    };

    Ok(Some(FleetConfig::from_json(&json)?))
}

/// A minimal fleet from the flake's `darwinConfigurations` and
/// `nixosConfigurations` attribute names. Nodes: `targetHost` = name,
/// `targetUser` = `target_user`, tag = profile type. An absent output
/// contributes no nodes; the result may be empty.
pub fn fleet_from_configurations(
    exec: &dyn Executor,
    flake_uri: &str,
    target_user: &str,
) -> Result<FleetConfig> {
    let mut nodes = HashMap::new();
    for (output, profile_type, system) in [
        ("darwinConfigurations", ProfileType::NixDarwin, "darwin"),
        ("nixosConfigurations", ProfileType::Nixos, "linux"),
    ] {
        for name in configuration_names(exec, flake_uri, output)? {
            nodes.insert(
                name.clone(),
                DeploymentNode {
                    target_host: name.clone(),
                    target_user: target_user.to_string(),
                    target_port: None,
                    system: system.to_string(),
                    profile_type: profile_type.clone(),
                    build_on_target: false,
                    tags: vec![profile_tag(&profile_type).to_string()],
                    drv_path: None,
                    toplevel: None,
                    name,
                },
            );
        }
    }
    Ok(FleetConfig {
        nodes,
        builders: HashMap::new(),
        flake_uri: flake_uri.to_string(),
        ansible_config: None,
        slurm_config: None,
        ray_config: None,
        skypilot_config: None,
    })
}

/// The tag given to derived nodes: the profile type's serialized name.
fn profile_tag(profile_type: &ProfileType) -> &'static str {
    match profile_type {
        ProfileType::Nixos => "nixos",
        ProfileType::NixDarwin => "nix-darwin",
    }
}

/// Attribute names of `<flake>#<output>`, empty when the output is absent.
fn configuration_names(exec: &dyn Executor, flake_uri: &str, output: &str) -> Result<Vec<String>> {
    let installable = format!("{}#{}", flake_uri, output);
    match eval_json(exec, &installable, "builtins.attrNames")? {
        Some(out) => serde_json::from_str(out.stdout.trim()).map_err(|e| {
            NixError::General(format!(
                "unexpected output listing {}: {} ({:?})",
                installable,
                e,
                out.stdout.trim()
            ))
        }),
        None => Ok(Vec::new()),
    }
}

/// `nix eval --json <installable> --apply <apply>`. `Ok(None)` when the
/// flake does not provide the attribute; any other failure is an error
/// carrying nix's stderr.
fn eval_json(exec: &dyn Executor, installable: &str, apply: &str) -> Result<Option<ExecOutput>> {
    let spec = CommandSpec::new("nix").args(["eval", "--json", installable, "--apply", apply]);
    let output = run_nix(exec, &spec, installable)?;
    if output.success() {
        return Ok(Some(output));
    }
    if is_missing_attribute(&output.stderr) {
        return Ok(None);
    }
    Err(NixError::General(format!(
        "nix eval {} failed: {}",
        installable,
        output.stderr.trim()
    )))
}

/// Nix's wording when a flake lacks the requested output attribute.
fn is_missing_attribute(stderr: &str) -> bool {
    stderr.contains("does not provide attribute")
}

fn run_nix(exec: &dyn Executor, spec: &CommandSpec, what: &str) -> Result<ExecOutput> {
    exec.exec(spec)
        .map_err(|e| NixError::General(format!("failed to run nix for {}: {}", what, e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::ScriptedExecutor;

    const MISSING: &str =
        "error: flake 'git+file:///cfg' does not provide attribute 'packages.x.fleet', \
         'legacyPackages.x.fleet' or 'fleet'";

    fn mk_fleet_json() -> String {
        r#"{"nodes":{"web1":{"name":"web1","targetHost":"192.0.2.10","targetUser":"root",
            "targetPort":null,"system":"x86_64-linux","profileType":"nixos",
            "buildOnTarget":false,"tags":["web"]}},"builders":{},"flakeUri":"."}"#
            .to_string()
    }

    #[test]
    fn fleet_output_wins_over_configurations() {
        let exec = ScriptedExecutor::new()
            .on(
                "#fleet --apply",
                ExecOutput::ok(serde_json::json!({ "json": mk_fleet_json() }).to_string()),
            )
            .on("Configurations", ExecOutput::ok(r#"["web1","spare"]"#));
        let (config, origin) = discover_fleet(&exec, ".", "me").unwrap();
        assert_eq!(origin, FleetOrigin::FleetOutput);
        assert_eq!(config.node_names(), vec!["web1"]);
        assert_eq!(config.nodes["web1"].target_host, "192.0.2.10");
        exec.assert_not_invoked_containing("Configurations");
    }

    #[test]
    fn derives_nodes_from_configuration_names_when_fleet_output_is_absent() {
        let exec = ScriptedExecutor::new()
            .on("#fleet --apply", ExecOutput::new(1, "", MISSING))
            .on(
                "#darwinConfigurations --apply builtins.attrNames",
                ExecOutput::ok(r#"["mac01"]"#),
            )
            .on(
                "#nixosConfigurations --apply builtins.attrNames",
                ExecOutput::ok(r#"["box01","box02"]"#),
            );
        let (config, origin) = discover_fleet(&exec, "github:me/cfg", "olive").unwrap();
        assert_eq!(origin, FleetOrigin::Configurations);
        assert_eq!(config.node_names(), vec!["box01", "box02", "mac01"]);
        assert_eq!(config.flake_uri, "github:me/cfg");

        let mac = &config.nodes["mac01"];
        assert_eq!(mac.profile_type, ProfileType::NixDarwin);
        assert_eq!(mac.target_host, "mac01");
        assert_eq!(mac.target_user, "olive");
        assert_eq!(mac.tags, vec!["nix-darwin"]);

        let b = &config.nodes["box01"];
        assert_eq!(b.profile_type, ProfileType::Nixos);
        assert_eq!(b.tags, vec!["nixos"]);
        assert_eq!(config.nodes_by_tags(&["nixos".to_string()]).len(), 2);
    }

    #[test]
    fn absent_configuration_output_contributes_nothing() {
        let exec = ScriptedExecutor::new()
            .on("#fleet --apply", ExecOutput::new(1, "", MISSING))
            .on(
                "#darwinConfigurations",
                ExecOutput::new(
                    1,
                    "",
                    "error: flake 'git+file:///cfg' does not provide attribute \
                     'darwinConfigurations'",
                ),
            )
            .on("#nixosConfigurations", ExecOutput::ok(r#"["box01"]"#));
        let (config, _) = discover_fleet(&exec, ".", "me").unwrap();
        assert_eq!(config.node_names(), vec!["box01"]);
    }

    #[test]
    fn flake_with_nothing_deployable_is_an_error() {
        let exec = ScriptedExecutor::new().on("nix eval", ExecOutput::new(1, "", MISSING));
        let err = discover_fleet(&exec, ".", "me").unwrap_err();
        assert!(
            err.to_string()
                .contains("no `fleet` output and no darwinConfigurations or nixosConfigurations"),
            "{err}"
        );
    }

    #[test]
    fn fleet_output_eval_errors_are_not_masked_as_absence() {
        let exec = ScriptedExecutor::new().on(
            "#fleet --apply",
            ExecOutput::new(1, "", "error: attribute 'hostPlatform' missing"),
        );
        let err = discover_fleet(&exec, ".", "me").unwrap_err();
        assert!(err.to_string().contains("hostPlatform"), "{err}");
        exec.assert_not_invoked_containing("Configurations");
    }

    #[test]
    fn derivation_fleet_output_is_built_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("consortium-fleet.json");
        std::fs::write(&path, mk_fleet_json()).unwrap();
        let exec = ScriptedExecutor::new()
            .on("#fleet --apply", ExecOutput::ok(r#"{"drv":true}"#))
            .on(
                "nix build --no-link --print-out-paths .#fleet",
                ExecOutput::ok(format!("{}\n", path.display())),
            );
        let config = fleet_from_flake_output(&exec, ".").unwrap().unwrap();
        assert_eq!(config.node_names(), vec!["web1"]);
    }
}
