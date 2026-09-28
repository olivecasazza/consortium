//! `DeployOptions::activate_timeout`: a host whose activation outlives the
//! limit fails on its own; the rest of the fleet still activates.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use consortium_integration::exec::{ExecOutput, Rule, ScriptedExecutor};
use consortium_integration::fleet::{DeploymentNode, FleetConfig, ProfileType};
use consortium_nix::config::DeployAction;
use consortium_nix::{deploy_with_options, DeployOptions};

fn fleet() -> FleetConfig {
    let node = |name: &str| DeploymentNode {
        name: name.into(),
        target_host: name.into(),
        target_user: "root".into(),
        target_port: None,
        system: "x86_64-linux".into(),
        profile_type: ProfileType::Nixos,
        build_on_target: false,
        tags: vec![],
        drv_path: None,
        toplevel: None,
    };
    FleetConfig {
        nodes: HashMap::from([
            ("fast".to_string(), node("fast")),
            ("stuck".to_string(), node("stuck")),
        ]),
        builders: HashMap::new(),
        flake_uri: ".".into(),
        ansible_config: None,
        slurm_config: None,
        ray_config: None,
        skypilot_config: None,
    }
}

/// `stuck`'s activation takes an hour; everything else succeeds at once.
fn executor() -> Arc<ScriptedExecutor> {
    Arc::new(
        ScriptedExecutor::new()
            .rule(Rule::slow_containing_all(
                ["stuck", "switch-to-configuration"],
                Duration::from_secs(3600),
                ExecOutput::ok(""),
            ))
            .on("nix eval", ExecOutput::ok("/nix/store/abc-toplevel\n"))
            .on("nix build", ExecOutput::ok("/nix/store/abc-toplevel\n"))
            .on("", ExecOutput::ok("")),
    )
}

fn targets() -> Vec<String> {
    vec!["fast".to_string(), "stuck".to_string()]
}

#[test]
fn activation_past_the_limit_fails_only_that_host() {
    let options = DeployOptions::new().activate_timeout(Duration::from_secs(60));
    let report = deploy_with_options(
        executor(),
        &fleet(),
        &targets(),
        DeployAction::Switch,
        4,
        false,
        &options,
    )
    .unwrap();

    assert_eq!(report.activated, vec!["fast".to_string()]);
    assert_eq!(report.activation_failures.len(), 1);
    let (host, message) = &report.activation_failures[0];
    assert_eq!(host, "stuck");
    assert!(
        message.contains("time limit"),
        "failure should say the host timed out: {message}"
    );
    assert!(!report.is_success());
}

#[test]
fn activation_is_unbounded_by_default() {
    let report = deploy_with_options(
        executor(),
        &fleet(),
        &targets(),
        DeployAction::Switch,
        4,
        false,
        &DeployOptions::new(),
    )
    .unwrap();

    let mut activated = report.activated.clone();
    activated.sort();
    assert_eq!(activated, targets());
    assert!(report.is_success());
}
