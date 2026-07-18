//! Self-test for the contract-test harness.
//!
//! Defines a minimal but realistic pipeline integration — `stage` →
//! `execute` → `collect` over two hosts, built on
//! `consortium::dag::DagBuilder` with a continue-independent error policy —
//! implements [`Contract`] for it, and instantiates the full macro-generated
//! suite against it. Two deliberately broken contracts then prove (via
//! `#[should_panic]` meta-tests) that the checks actually catch violations.

use std::collections::HashMap;
use std::sync::Arc;

use consortium::dag::{DagBuilder, DagContext, DagReport, ErrorPolicy, FnTask, TaskOutcome};
use consortium_integration::exec::{
    CommandSpec, ExecOutput, Executor, Rule, ScriptedExecutor, SshTarget,
};
use consortium_integration::fleet::{DeploymentNode, FleetConfig, ProfileType, SlurmFleetConfig};
use consortium_integration_testkit::{
    checks, integration_contract_tests, Contract, OptionVariant, PartialFailure, Phase,
};

// ── The dummy integration (stands in for a real integration crate) ──────────

/// Run options. `Default` is the full pipeline; one variant skips `collect`.
#[derive(Default)]
struct DummyOptions {
    skip_collect: bool,
}

/// One planned DAG task, as data (id, dependency, description, command).
struct PlannedTask {
    id: String,
    after: Option<String>,
    desc: String,
    spec: CommandSpec,
}

/// Build the pipeline plan for every node in the fleet.
///
/// Requires the (arbitrarily chosen) slurm sub-config to be present — that
/// is what `config_missing()` omits.
fn plan(config: &FleetConfig, opts: &DummyOptions) -> Result<Vec<PlannedTask>, String> {
    if config.slurm_config.is_none() {
        return Err("missing slurm config".into());
    }
    let mut tasks = Vec::new();
    for name in config.node_names() {
        let host = config.nodes[&name].target_host.clone();
        let stage = format!("stage:{name}");
        let execute = format!("execute:{name}");
        tasks.push(PlannedTask {
            id: stage.clone(),
            after: None,
            desc: format!("stage closure to {name}"),
            spec: CommandSpec::new("nix").args([
                "copy".into(),
                "--to".into(),
                format!("ssh-ng://root@{host}"),
                "/nix/store/env".into(),
            ]),
        });
        tasks.push(PlannedTask {
            id: execute.clone(),
            after: Some(stage),
            desc: format!("run payload on {name}"),
            spec: CommandSpec::new("dummy-run")
                .args(["--node", name.as_str()])
                .ssh(SshTarget::new("root", &host)),
        });
        if !opts.skip_collect {
            tasks.push(PlannedTask {
                id: format!("collect:{name}"),
                after: Some(execute),
                desc: format!("collect results from {name}"),
                spec: CommandSpec::new("dummy-collect")
                    .args(["--node", name.as_str()])
                    .ssh(SshTarget::new("root", &host)),
            });
        }
    }
    Ok(tasks)
}

/// A DAG task that runs one command through the executor.
fn cmd_task(
    desc: String,
    exec: Arc<dyn Executor>,
    spec: CommandSpec,
) -> FnTask<impl Fn(&DagContext) -> TaskOutcome + Send> {
    FnTask::new(desc, move |_| match exec.exec(&spec) {
        Ok(out) if out.success() => TaskOutcome::Success,
        Ok(out) => TaskOutcome::Failed(format!("exit {}: {}", out.status, out.stderr.trim())),
        Err(e) => TaskOutcome::Failed(e.to_string()),
    })
}

/// The dummy integration's entry point.
fn deploy(
    config: &FleetConfig,
    exec: Arc<dyn Executor>,
    opts: &DummyOptions,
) -> Result<DagReport, String> {
    let mut dag = DagBuilder::new();
    dag.error_policy(ErrorPolicy::ContinueIndependent);
    for task in plan(config, opts)? {
        dag.add_task(task.id.clone(), cmd_task(task.desc, Arc::clone(&exec), task.spec));
        if let Some(after) = task.after {
            dag.add_dep(task.id, after);
        }
    }
    dag.build().map_err(|e| e.to_string())?.run().map_err(|e| e.to_string())
}

// ── Shared fixtures ─────────────────────────────────────────────────────────

fn fleet_config(with_slurm: bool) -> FleetConfig {
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
        ansible_config: None,
        slurm_config: with_slurm.then(|| SlurmFleetConfig {
            submit_node: "node01".into(),
            submit_user: "root".into(),
            control_node: "node01".into(),
            partitions: HashMap::new(),
        }),
        ray_config: None,
        skypilot_config: None,
    }
}

fn happy() -> ScriptedExecutor {
    ScriptedExecutor::new()
        .on("nix copy", ExecOutput::ok(""))
        .on("dummy-run", ExecOutput::ok("ok\n"))
        .on("dummy-collect", ExecOutput::ok("collected\n"))
}

fn failing(marker: &str) -> ScriptedExecutor {
    // The failing rule comes FIRST: ScriptedExecutor is first-match-wins.
    ScriptedExecutor::new()
        .rule(Rule::containing(marker, ExecOutput::new(1, "", "boom")))
        .on("nix copy", ExecOutput::ok(""))
        .on("dummy-run", ExecOutput::ok("ok\n"))
        .on("dummy-collect", ExecOutput::ok("collected\n"))
}

// ── The contract under test ─────────────────────────────────────────────────

struct DummyContract;

impl Contract for DummyContract {
    type Report = DagReport;
    type Error = String;
    type Options = DummyOptions;

    fn name() -> &'static str {
        "dummy-pipeline"
    }

    fn run(
        config: &FleetConfig,
        exec: Arc<dyn Executor>,
        opts: &Self::Options,
    ) -> Result<Self::Report, Self::Error> {
        deploy(config, exec, opts)
    }

    fn phases() -> Vec<Phase> {
        vec![
            Phase::new("stage", "nix copy", &[]),
            Phase::new("execute", "dummy-run", &["stage"]),
            Phase::new("collect", "dummy-collect", &["execute"]),
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
            "no-collect",
            DummyOptions { skip_collect: true },
            vec!["collect"],
        )]
    }

    fn partial_failure() -> Option<PartialFailure> {
        // node02 dies at the execute phase; its collect must never run,
        // while node01's full chain completes.
        let executor = ScriptedExecutor::new()
            .rule(Rule::containing_all(
                ["node02", "dummy-run"],
                ExecOutput::new(1, "", "node02 unreachable"),
            ))
            .on("nix copy", ExecOutput::ok(""))
            .on("dummy-run", ExecOutput::ok("ok\n"))
            .on("dummy-collect", ExecOutput::ok("collected\n"));
        Some(PartialFailure::new(executor, "node02", "node01", "dummy-collect"))
    }

    fn task_descriptions(config: &FleetConfig, _exec: Arc<dyn Executor>) -> Vec<String> {
        plan(config, &DummyOptions::default())
            .map(|tasks| tasks.into_iter().map(|t| t.desc).collect())
            .unwrap_or_default()
    }
}

// The full generated suite runs against the dummy integration.
integration_contract_tests!(DummyContract);

// ── Negative meta-tests: broken contracts must be caught ────────────────────

/// Deliberately broken: the declared collect-phase marker is never emitted
/// by the pipeline, so the happy path must be rejected.
struct BrokenMarkerContract;

impl Contract for BrokenMarkerContract {
    type Report = DagReport;
    type Error = String;
    type Options = DummyOptions;

    fn name() -> &'static str {
        "broken-marker"
    }

    fn run(
        config: &FleetConfig,
        exec: Arc<dyn Executor>,
        opts: &Self::Options,
    ) -> Result<Self::Report, Self::Error> {
        deploy(config, exec, opts)
    }

    fn phases() -> Vec<Phase> {
        vec![
            Phase::new("stage", "nix copy", &[]),
            Phase::new("execute", "dummy-run", &["stage"]),
            Phase::new("collect", "dummy-never-emitted", &["execute"]),
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

    fn task_descriptions(config: &FleetConfig, _exec: Arc<dyn Executor>) -> Vec<String> {
        plan(config, &DummyOptions::default())
            .map(|tasks| tasks.into_iter().map(|t| t.desc).collect())
            .unwrap_or_default()
    }
}

#[test]
#[should_panic(expected = "never invoked")]
fn negative_happy_path_rejects_unemitted_marker() {
    checks::happy_path::<BrokenMarkerContract>();
}

/// Deliberately broken: `run` ignores the missing sub-config and happily
/// deploys anyway, so the missing-config check must reject it.
struct LenientConfigContract;

impl Contract for LenientConfigContract {
    type Report = DagReport;
    type Error = String;
    type Options = DummyOptions;

    fn name() -> &'static str {
        "lenient-config"
    }

    fn run(
        _config: &FleetConfig,
        exec: Arc<dyn Executor>,
        opts: &Self::Options,
    ) -> Result<Self::Report, Self::Error> {
        // Broken on purpose: always deploys the complete fixture config.
        deploy(&fleet_config(true), exec, opts)
    }

    fn phases() -> Vec<Phase> {
        vec![
            Phase::new("stage", "nix copy", &[]),
            Phase::new("execute", "dummy-run", &["stage"]),
            Phase::new("collect", "dummy-collect", &["execute"]),
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

    fn task_descriptions(config: &FleetConfig, _exec: Arc<dyn Executor>) -> Vec<String> {
        plan(config, &DummyOptions::default())
            .map(|tasks| tasks.into_iter().map(|t| t.desc).collect())
            .unwrap_or_default()
    }
}

#[test]
#[should_panic(expected = "must return Err")]
fn negative_missing_config_rejects_lenient_run() {
    checks::missing_config_errors::<LenientConfigContract>();
}
