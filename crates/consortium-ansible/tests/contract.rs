//! Contract-suite enrollment for the ansible integration.
//!
//! Pipeline: build-env (local `nix build`) → copy-env (local `nix copy` to
//! the control node) → run-playbook (`ansible-playbook` over ssh on the
//! control node, once per target host).

use std::collections::HashMap;
use std::sync::Arc;

use consortium::dag::{DagReport, DagTask, TaskId};
use consortium_ansible::{run_playbook, AnsibleError, AnsibleOptions};
use consortium_integration::exec::{ExecOutput, Executor, Rule, ScriptedExecutor};
use consortium_integration::fleet::{
    AnsibleFleetConfig, DeploymentNode, FleetConfig, ProfileType,
};
use consortium_integration_testkit::{
    integration_contract_tests, Contract, OptionVariant, PartialFailure, Phase,
};

const PLAYBOOK: &str = "site.yml";
const ENV_NAME: &str = "default";

fn fleet_config(with_ansible: bool) -> FleetConfig {
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
    nodes.insert("node01".to_string(), node("node01"));
    nodes.insert("node02".to_string(), node("node02"));
    FleetConfig {
        nodes,
        builders: HashMap::new(),
        flake_uri: ".".into(),
        ansible_config: with_ansible.then(|| AnsibleFleetConfig {
            control_node: "ctrl".into(),
            ansible_version: None,
            collections: vec![],
            playbook_dir: None,
            host_groups: HashMap::new(),
        }),
        slurm_config: None,
        ray_config: None,
        skypilot_config: None,
    }
}

fn targets() -> Vec<String> {
    vec!["node01".to_string(), "node02".to_string()]
}

fn happy() -> ScriptedExecutor {
    ScriptedExecutor::new()
        .on("nix build", ExecOutput::ok("/nix/store/ansible-env\n"))
        .on("nix copy", ExecOutput::ok(""))
        .on(
            "ansible-playbook",
            ExecOutput::ok("PLAY RECAP: ok=1 changed=0 failed=0\n"),
        )
}

fn failing(marker: &str) -> ScriptedExecutor {
    // The failing rule comes FIRST: ScriptedExecutor is first-match-wins.
    ScriptedExecutor::new()
        .rule(Rule::containing(marker, ExecOutput::new(1, "", "boom")))
        .on("nix build", ExecOutput::ok("/nix/store/ansible-env\n"))
        .on("nix copy", ExecOutput::ok(""))
        .on(
            "ansible-playbook",
            ExecOutput::ok("PLAY RECAP: ok=1 changed=0 failed=0\n"),
        )
}

struct AnsibleContract;

impl Contract for AnsibleContract {
    type Report = DagReport;
    type Error = AnsibleError;
    type Options = AnsibleOptions;

    fn name() -> &'static str {
        "ansible"
    }

    fn run(
        config: &FleetConfig,
        exec: Arc<dyn Executor>,
        opts: &Self::Options,
    ) -> Result<Self::Report, Self::Error> {
        run_playbook(exec, config, &targets(), PLAYBOOK, ENV_NAME, opts)
    }

    fn phases() -> Vec<Phase> {
        vec![
            Phase::new("build-env", "nix build", &[]),
            Phase::new("copy-env", "nix copy", &["build-env"]),
            Phase::new("run-playbook", "ansible-playbook", &["copy-env"]),
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
        // check_mode adds a flag instead of skipping a phase, so there are no
        // skip-variants; --check is covered by an integration-specific test.
        vec![]
    }

    fn partial_failure() -> Option<PartialFailure> {
        // The per-host failing phase (run-playbook) is the last phase, so
        // there is no later per-host marker to assert blocked. Per-host
        // failure continuation is covered by an integration-specific test.
        None
    }

    fn task_descriptions(config: &FleetConfig, _exec: Arc<dyn Executor>) -> Vec<String> {
        use consortium_ansible::tasks::{
            AnsiblePlaybookTask, NixBuildAnsibleEnvTask, NixCopyAnsibleEnvTask,
        };

        let control = config
            .ansible_config
            .as_ref()
            .map(|a| a.control_node.clone())
            .unwrap_or_else(|| "ctrl".to_string());
        let host = &config.node_names()[0];
        vec![
            NixBuildAnsibleEnvTask::new(ENV_NAME, &config.flake_uri).describe(),
            NixCopyAnsibleEnvTask {
                env_name: ENV_NAME.to_string(),
                target_host: control.clone(),
                target_user: "root".to_string(),
            }
            .describe(),
            AnsiblePlaybookTask::new(host, PLAYBOOK, ENV_NAME, &control, "root").describe(),
        ]
    }
}

integration_contract_tests!(AnsibleContract);

// ── Integration-specific tests ──────────────────────────────────────────────

/// check_mode does not skip a phase; it passes `--check` to every playbook
/// invocation.
#[test]
fn check_mode_passes_check_flag_to_playbook() {
    let exec = Arc::new(happy());
    let opts = AnsibleOptions {
        check_mode: true,
        ..AnsibleOptions::default()
    };
    let report = run_playbook(
        exec.clone(),
        &fleet_config(true),
        &targets(),
        PLAYBOOK,
        ENV_NAME,
        &opts,
    )
    .expect("check-mode run must succeed on the happy executor");
    assert!(report.is_success());

    let playbook_runs: Vec<String> = exec
        .invocations()
        .into_iter()
        .filter(|cmd| cmd.contains("ansible-playbook"))
        .collect();
    assert_eq!(playbook_runs.len(), 2, "one playbook run per target");
    for cmd in &playbook_runs {
        assert!(
            cmd.contains("--check"),
            "check-mode invocation missing --check: {}",
            cmd
        );
    }
}

/// A failing playbook run on one host must not stop the other host's chain
/// (ContinueIndependent semantics).
#[test]
fn per_host_playbook_failure_continues_independents() {
    // node02's playbook run fails; the failing rule comes first
    // (first-match-wins).
    let exec = Arc::new(
        ScriptedExecutor::new()
            .rule(Rule::containing_all(
                ["ansible-playbook", "node02"],
                ExecOutput::new(1, "", "unreachable"),
            ))
            .on("nix build", ExecOutput::ok("/nix/store/ansible-env\n"))
            .on("nix copy", ExecOutput::ok(""))
            .on(
                "ansible-playbook",
                ExecOutput::ok("PLAY RECAP: ok=1 changed=0 failed=0\n"),
            ),
    );
    let report = run_playbook(
        exec,
        &fleet_config(true),
        &targets(),
        PLAYBOOK,
        ENV_NAME,
        &AnsibleOptions::default(),
    )
    .expect("partial failure returns Ok(report) under ContinueIndependent");

    assert!(!report.is_success());
    assert!(report
        .completed
        .contains(&TaskId("run-playbook:node01".to_string())));
    assert!(report
        .failed
        .contains_key(&TaskId("run-playbook:node02".to_string())));
}
