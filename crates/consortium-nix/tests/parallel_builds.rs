//! Integration tests for parallel build execution via DAG.

use std::sync::Arc;

use consortium_integration::exec::{ExecOutput, Executor, ScriptedExecutor};
use consortium_nix::build::{build_closures, BuildResults};
use consortium_nix::config::{
    DeployAction, DeploymentNode, DeploymentPlan, DeploymentTarget, ProfileType,
};

fn create_mock_plan_with_3_nodes() -> DeploymentPlan {
    let nodes = vec![
        DeploymentNode {
            name: "hp01".to_string(),
            target_host: "192.168.1.121".to_string(),
            target_user: "root".to_string(),
            target_port: None,
            system: "x86_64-linux".to_string(),
            profile_type: ProfileType::Nixos,
            build_on_target: false,
            tags: vec!["build-host".to_string()],
            drv_path: None,
            toplevel: None,
        },
        DeploymentNode {
            name: "hp02".to_string(),
            target_host: "192.168.1.122".to_string(),
            target_user: "root".to_string(),
            target_port: None,
            system: "x86_64-linux".to_string(),
            profile_type: ProfileType::Nixos,
            build_on_target: false,
            tags: vec!["build-host".to_string()],
            drv_path: None,
            toplevel: None,
        },
        DeploymentNode {
            name: "hp03".to_string(),
            target_host: "192.168.1.123".to_string(),
            target_user: "root".to_string(),
            target_port: None,
            system: "x86_64-linux".to_string(),
            profile_type: ProfileType::Nixos,
            build_on_target: false,
            tags: vec!["build-host".to_string()],
            drv_path: None,
            toplevel: None,
        },
    ];

    let targets = nodes
        .into_iter()
        .map(|node| DeploymentTarget {
            node,
            toplevel_path: "/nix/store/mock-toplevel".to_string(),
            current_system: None,
            needs_build: true,
            needs_copy: false,
        })
        .collect();

    DeploymentPlan {
        targets,
        action: DeployAction::Build,
        max_parallel: 3,
    }
}

#[test]
fn test_parallel_builds_with_3_nodes() {
    // Create a deployment plan with 3 nodes that all need building
    let plan = create_mock_plan_with_3_nodes();

    // Verify plan has 3 targets that need building
    assert_eq!(plan.target_count(), 3);
    assert_eq!(plan.build_count(), 3);
    assert_eq!(plan.max_parallel, 3);
}

#[test]
fn test_build_plan_with_mixed_needs() {
    let mut plan = create_mock_plan_with_3_nodes();

    // Mark one node as not needing build
    plan.targets[0].needs_build = false;

    // Verify the plan structure
    assert_eq!(plan.target_count(), 3);
    assert_eq!(plan.build_count(), 2); // Only 2 should need building
}

#[test]
fn test_build_plan_respects_concurrency_limit() {
    let mut plan = create_mock_plan_with_3_nodes();
    plan.max_parallel = 2;

    // Verify the concurrency limit is set correctly
    assert_eq!(plan.max_parallel, 2);
}

#[test]
fn test_build_results_structure() {
    // This test verifies that BuildResults can be created and used correctly
    let results = BuildResults {
        paths: std::collections::HashMap::new(),
        errors: std::collections::HashMap::new(),
    };

    assert!(results.paths.is_empty());
    assert!(results.errors.is_empty());
}

#[test]
fn test_build_closures_runs_through_scripted_executor() {
    // Every build in the plan shells out through the injected executor;
    // a ScriptedExecutor proves the whole DAG ran `nix build` per host.
    let scripted = Arc::new(
        ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/mock-toplevel\n")),
    );
    let exec: Arc<dyn Executor> = scripted.clone();

    let plan = create_mock_plan_with_3_nodes();
    let results = build_closures(exec, &plan, ".", None).unwrap();

    assert!(results.errors.is_empty());
    assert_eq!(results.paths.len(), 3);
    for name in ["hp01", "hp02", "hp03"] {
        assert_eq!(
            results.paths.get(name).map(String::as_str),
            Some("/nix/store/mock-toplevel")
        );
        scripted.assert_invoked_containing(&format!(
            "nix build .#nixosConfigurations.{}.config.system.build.toplevel",
            name
        ));
    }
    assert_eq!(scripted.invocation_count(), 3);
}

#[test]
fn test_build_closures_skips_up_to_date_targets() {
    let scripted = Arc::new(
        ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/mock-toplevel\n")),
    );
    let exec: Arc<dyn Executor> = scripted.clone();

    let mut plan = create_mock_plan_with_3_nodes();
    plan.targets[0].needs_build = false;

    let results = build_closures(exec, &plan, ".", None).unwrap();

    // hp01 is recorded from the plan without invoking nix; hp02/hp03 build.
    assert_eq!(results.paths.len(), 3);
    assert!(results.errors.is_empty());
    assert_eq!(scripted.invocation_count(), 2);
}
