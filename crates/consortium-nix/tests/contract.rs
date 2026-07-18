//! Integration contract suite for the nix deployment pipeline.
//!
//! Implements [`Contract`] for [`deploy()`] and instantiates the
//! macro-generated semantic checks. The phases are the per-host
//! eval → build → copy → activate pipeline; the harness drives them
//! against [`ScriptedExecutor`] fixtures and asserts abort/cancel/
//! partial-failure/option-skip semantics.

use std::collections::HashMap;
use std::sync::Arc;

use consortium::dag::DagTask;
use consortium_integration::exec::{ExecOutput, Executor, Rule, ScriptedExecutor};
use consortium_integration::fleet::{DeploymentNode, FleetConfig, ProfileType};
use consortium_integration_testkit::{
    integration_contract_tests, Contract, OptionVariant, PartialFailure, Phase,
};
use consortium_nix::config::DeployAction;
use consortium_nix::tasks::{NixActivateTask, NixBuildTask, NixCopyTask, NixEvalTask};
use consortium_nix::{deploy, DeployReport, NixError};

/// Run options for the contract suite. `Default` exercises the full
/// pipeline (Switch over 4-wide parallelism, no remote builders).
struct NixOptions {
    action: DeployAction,
    max_parallel: usize,
    use_builders: bool,
}

impl Default for NixOptions {
    fn default() -> Self {
        Self {
            action: DeployAction::Switch,
            max_parallel: 4,
            use_builders: false,
        }
    }
}

/// The targets `run()` always deploys. `config_missing()` omits them from
/// the fleet, which `deploy()` must reject before issuing any command.
const TARGETS: [&str; 2] = ["node01", "node02"];

fn fleet_config(with_nodes: bool) -> FleetConfig {
    let node = |name: &str| DeploymentNode {
        name: name.into(),
        target_host: name.into(), // hostname doubles as the per-host marker
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
    if with_nodes {
        nodes.insert("node01".to_string(), node("node01"));
        nodes.insert("node02".to_string(), node("node02"));
    }
    FleetConfig {
        nodes,
        builders: HashMap::new(),
        flake_uri: ".".into(),
        ansible_config: None,
        slurm_config: None,
        ray_config: None,
        skypilot_config: None,
    }
}

/// Success rules shared by every scripted executor below.
///
/// Ordering matters: `ScriptedExecutor` is first-match-wins and `"ssh"` is
/// a substring of the `ssh-ng://...` target in `nix copy` lines, so no
/// broad ssh rule may precede the copy rule. The catch-all `.on("", ..)`
/// guards against unscripted commands and must stay LAST.
fn success_rules(exec: ScriptedExecutor) -> ScriptedExecutor {
    exec.on("nix eval", ExecOutput::ok("/nix/store/abc-toplevel\n"))
        .on("nix build", ExecOutput::ok("/nix/store/abc-toplevel\n"))
        .on("nix copy", ExecOutput::ok(""))
        .on("switch-to-configuration", ExecOutput::ok(""))
        .on("nix-env", ExecOutput::ok(""))
        .on("", ExecOutput::ok(""))
}

fn happy() -> ScriptedExecutor {
    success_rules(ScriptedExecutor::new())
}

fn failing(marker: &str) -> ScriptedExecutor {
    // The failing rule comes FIRST: ScriptedExecutor is first-match-wins.
    success_rules(
        ScriptedExecutor::new().rule(Rule::containing(marker, ExecOutput::new(1, "", "boom"))),
    )
}

// ── The contract under test ───────────────────────────────────────────

struct NixContract;

impl Contract for NixContract {
    type Report = DeployReport;
    type Error = NixError;
    type Options = NixOptions;

    fn name() -> &'static str {
        "nix"
    }

    fn run(
        config: &FleetConfig,
        exec: Arc<dyn Executor>,
        opts: &Self::Options,
    ) -> Result<Self::Report, Self::Error> {
        deploy(
            exec,
            config,
            &TARGETS.map(String::from),
            opts.action,
            opts.max_parallel,
            opts.use_builders,
        )
    }

    fn phases() -> Vec<Phase> {
        // eval/build/copy run locally, so space markers match the raw argv
        // rendering. activate runs over ssh (remote tokens single-quoted),
        // so its marker is a single token.
        vec![
            Phase::new("eval", "nix eval", &[]),
            Phase::new("build", "nix build", &["eval"]),
            Phase::new("copy", "nix copy", &["build"]),
            Phase::new("activate", "switch-to-configuration", &["copy"]),
        ]
    }

    fn config_missing() -> FleetConfig {
        fleet_config(false)
    }

    fn config_complete() -> FleetConfig {
        fleet_config(true)
    }

    fn happy_executor() -> ScriptedExecutor {
        happy()
    }

    fn executor_failing_phase(marker: &str) -> ScriptedExecutor {
        failing(marker)
    }

    fn option_variants() -> Vec<OptionVariant<Self::Options>> {
        vec![OptionVariant::new(
            "build-only",
            NixOptions {
                action: DeployAction::Build,
                ..Default::default()
            },
            vec!["copy", "activate"],
        )]
    }

    fn partial_failure() -> Option<PartialFailure> {
        // node02 dies at the build phase; its copy/activate must never run,
        // while node01's full chain completes. The failing rule is FIRST.
        let executor = success_rules(ScriptedExecutor::new().rule(Rule::containing_all(
            ["nix build", "node02"],
            ExecOutput::new(1, "", "build boom"),
        )));
        Some(PartialFailure::new(
            executor,
            "node02",
            "node01",
            "switch-to-configuration",
        ))
    }

    fn task_descriptions(config: &FleetConfig, _exec: Arc<dyn Executor>) -> Vec<String> {
        // Task construction is side-effect free: describe() only reads the
        // host name, and the executor is never touched.
        let mut hosts: Vec<&String> = config.nodes.keys().collect();
        hosts.sort();
        hosts
            .into_iter()
            .flat_map(|host| {
                [
                    NixEvalTask::new(host).describe(),
                    NixBuildTask::new(host).describe(),
                    NixCopyTask::new(host).describe(),
                    NixActivateTask::new(host).describe(),
                ]
            })
            .collect()
    }
}

integration_contract_tests!(NixContract);
