//! Contract-suite enrollment for the SkyPilot integration.
//!
//! The pipeline is a single-cluster linear chain — nix build → `sky launch`
//! → `sky down` — so `partial_failure()` is `None` (there are no
//! independent per-host chains to partially fail).

use std::collections::HashMap;
use std::sync::Arc;

use consortium::dag::{DagReport, DagTask};
use consortium_integration::exec::{ExecOutput, Executor, Rule, ScriptedExecutor};
use consortium_integration::fleet::{
    DeploymentNode, FleetConfig, ProfileType, SkypilotFleetConfig,
};
use consortium_integration_testkit::{
    integration_contract_tests, Contract, OptionVariant, PartialFailure, Phase,
};
use consortium_skypilot::{launch_task, tasks, SkyOptions, SkypilotError};

/// The yaml handed to `sky launch` in every fixture run.
const TASK_YAML: &str = "resources:\n  cloud: gcp\n";

fn fleet_config(with_skypilot: bool) -> FleetConfig {
    let node = DeploymentNode {
        name: "ctrl01".into(),
        target_host: "ctrl01".into(),
        target_user: "root".into(),
        target_port: None,
        system: "x86_64-linux".into(),
        profile_type: ProfileType::Nixos,
        build_on_target: false,
        tags: vec![],
        drv_path: None,
        toplevel: None,
    };
    let mut nodes = HashMap::new();
    nodes.insert("ctrl01".to_string(), node);
    FleetConfig {
        nodes,
        builders: HashMap::new(),
        flake_uri: ".".into(),
        ansible_config: None,
        slurm_config: None,
        ray_config: None,
        skypilot_config: with_skypilot.then(|| SkypilotFleetConfig {
            cloud: "gcp".into(),
            region: Some("us-central1".into()),
            instance_type: None,
        }),
    }
}

struct SkypilotContract;

impl Contract for SkypilotContract {
    type Report = DagReport;
    type Error = SkypilotError;
    type Options = SkyOptions;

    fn name() -> &'static str {
        "skypilot"
    }

    fn config_missing() -> FleetConfig {
        fleet_config(false)
    }

    fn config_complete() -> FleetConfig {
        fleet_config(true)
    }

    fn happy_executor() -> ScriptedExecutor {
        ScriptedExecutor::new()
            .on("nix build", ExecOutput::ok("/nix/store/sky-env\n"))
            .on(
                "sky launch",
                ExecOutput::ok("Cluster launched: test-cluster\n"),
            )
            .on("sky down", ExecOutput::ok("Terminating cluster\n"))
    }

    fn executor_failing_phase(phase_marker: &str) -> ScriptedExecutor {
        // The failing rule comes FIRST: ScriptedExecutor is first-match-wins.
        ScriptedExecutor::new()
            .rule(Rule::containing(
                phase_marker,
                ExecOutput::new(1, "", "boom"),
            ))
            .on("nix build", ExecOutput::ok("/nix/store/sky-env\n"))
            .on(
                "sky launch",
                ExecOutput::ok("Cluster launched: test-cluster\n"),
            )
            .on("sky down", ExecOutput::ok("Terminating cluster\n"))
    }

    fn run(
        config: &FleetConfig,
        exec: Arc<dyn Executor>,
        opts: &Self::Options,
    ) -> Result<Self::Report, Self::Error> {
        launch_task(exec, config, "test-cluster", TASK_YAML, opts)
    }

    fn phases() -> Vec<Phase> {
        vec![
            Phase::new("build", "nix build", &[]),
            Phase::new("launch", "sky launch", &["build"]),
            Phase::new("down", "sky down", &["launch"]),
        ]
    }

    fn option_variants() -> Vec<OptionVariant<Self::Options>> {
        vec![OptionVariant::new(
            "no-teardown",
            SkyOptions { teardown: false },
            vec!["down"],
        )]
    }

    fn partial_failure() -> Option<PartialFailure> {
        // Single-cluster linear pipeline: no independent per-host chains.
        None
    }

    fn task_descriptions(config: &FleetConfig, _exec: Arc<dyn Executor>) -> Vec<String> {
        let sky_config = config
            .skypilot_config
            .as_ref()
            .expect("task_descriptions needs the complete config");
        vec![
            tasks::NixBuildSkyEnvTask::new("test-cluster", &config.flake_uri).describe(),
            tasks::SkyLaunchTask {
                cluster_name: "test-cluster".to_string(),
                task_yaml: TASK_YAML.to_string(),
                cloud: Some(sky_config.cloud.clone()),
                region: sky_config.region.clone(),
            }
            .describe(),
            tasks::SkyDownTask {
                cluster_name: "test-cluster".to_string(),
            }
            .describe(),
        ]
    }
}

integration_contract_tests!(SkypilotContract);
