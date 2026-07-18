//! # consortium-integration-testkit
//!
//! The **abstract contract-test harness** for consortium's integration
//! crates (`consortium-nix`, `consortium-slurm`, `consortium-ansible`,
//! `consortium-skypilot`, `consortium-ray`). Every integration runs commands
//! in pipeline phases over a [`FleetConfig`], reports success/failure through
//! an [`IntegrationReport`], and must behave identically in the face of
//! missing configuration, phase failures, per-host partial failures, and
//! option-driven phase skips. This crate encodes those semantics once, as
//! seven generic checks, so each integration keeps ~100 lines of glue and
//! zero duplicated test logic.
//!
//! # The generated suite
//!
//! [`integration_contract_tests!`] generates one `#[test]` per semantic
//! contract, with uniform names across all integrations:
//!
//! | test name | semantic |
//! |---|---|
//! | `contract_missing_config_errors` | missing sub-config → `Err`, zero commands issued |
//! | `contract_plan_has_no_side_effects` | task descriptions exist, are printable, and required no execution |
//! | `contract_happy_path` | all phases invoked in declared `after` order; report successful |
//! | `contract_first_phase_failure_aborts_pipeline` | first phase fails → later phases never invoked |
//! | `contract_mid_pipeline_failure_cancels_dependents` | mid phase fails → later phases never invoked, earlier ones ran |
//! | `contract_partial_host_failure_continues_independents` | dead host's chain stops; surviving host finishes every phase |
//! | `contract_option_variants_skip_declared_phases` | each option variant skips exactly its declared phases |
//!
//! # Usage — three steps for a new integration
//!
//! 1. **Implement [`Contract`]** for a marker struct in your integration's
//!    `tests/contract.rs`: associated types (`Report`, `Error`, `Options` —
//!    use `()` when there are no options), `run()` delegating to your real
//!    entry point, and `phases()` declaring the pipeline with
//!    quoting-safe markers (see [`Phase`]).
//! 2. **Write the fixtures**: `config_missing()` / `config_complete()`,
//!    `happy_executor()`, `executor_failing_phase()`, and optionally
//!    `option_variants()` and `partial_failure()`.
//! 3. **Instantiate the macro**: `integration_contract_tests!(MyContract);`
//!    then run `cargo test -p <your-integration>`.
//!
//! Failure reporting is deliberately tolerant: a scenario that must fail may
//! either return `Err(..)` or return `Ok(report)` with
//! `report.is_success() == false` and `report.failure_count() >= 1`. That
//! tolerance *is* the semantic — everything else is asserted strictly, with
//! messages naming the integration and the phase/marker involved.
//!
//! # Complete example
//!
//! A full, self-contained `tests/contract.rs` for a fictional
//! stage → execute → collect integration. Copy it, rename, and adjust the
//! phases and fixtures to match your integration (this exact shape is
//! exercised by this crate's own test suite, so it is guaranteed to pass):
//!
//! ```rust,no_run
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! use consortium::dag::{DagBuilder, DagContext, DagReport, ErrorPolicy, FnTask, TaskOutcome};
//! use consortium_integration::exec::{
//!     CommandSpec, ExecOutput, Executor, Rule, ScriptedExecutor, SshTarget,
//! };
//! use consortium_integration::fleet::{
//!     DeploymentNode, FleetConfig, ProfileType, SlurmFleetConfig,
//! };
//! use consortium_integration_testkit::{
//!     integration_contract_tests, Contract, OptionVariant, PartialFailure, Phase,
//! };
//!
//! // ── Integration code under test (normally lives in your crate's src/) ────
//!
//! /// Run options. `Default` is the baseline used by most contract tests.
//! #[derive(Default)]
//! struct DeployOptions {
//!     skip_collect: bool,
//! }
//!
//! /// One planned DAG task, as data (id, dependency, description, command).
//! struct PlannedTask {
//!     id: String,
//!     after: Option<String>,
//!     desc: String,
//!     spec: CommandSpec,
//! }
//!
//! /// Build the pipeline plan for every node in the fleet.
//! fn plan(config: &FleetConfig, opts: &DeployOptions) -> Result<Vec<PlannedTask>, String> {
//!     if config.slurm_config.is_none() {
//!         return Err("missing slurm config".into());
//!     }
//!     let mut tasks = Vec::new();
//!     for name in config.node_names() {
//!         let host = config.nodes[&name].target_host.clone();
//!         let stage = format!("stage:{name}");
//!         let execute = format!("execute:{name}");
//!         tasks.push(PlannedTask {
//!             id: stage.clone(),
//!             after: None,
//!             desc: format!("stage closure to {name}"),
//!             spec: CommandSpec::new("nix").args([
//!                 "copy".into(),
//!                 "--to".into(),
//!                 format!("ssh-ng://root@{host}"),
//!                 "/nix/store/env".into(),
//!             ]),
//!         });
//!         tasks.push(PlannedTask {
//!             id: execute.clone(),
//!             after: Some(stage),
//!             desc: format!("run payload on {name}"),
//!             spec: CommandSpec::new("dummy-run")
//!                 .args(["--node", name.as_str()])
//!                 .ssh(SshTarget::new("root", &host)),
//!         });
//!         if !opts.skip_collect {
//!             tasks.push(PlannedTask {
//!                 id: format!("collect:{name}"),
//!                 after: Some(execute),
//!                 desc: format!("collect results from {name}"),
//!                 spec: CommandSpec::new("dummy-collect")
//!                     .args(["--node", name.as_str()])
//!                     .ssh(SshTarget::new("root", &host)),
//!             });
//!         }
//!     }
//!     Ok(tasks)
//! }
//!
//! fn cmd_task(
//!     desc: String,
//!     exec: Arc<dyn Executor>,
//!     spec: CommandSpec,
//! ) -> FnTask<impl Fn(&DagContext) -> TaskOutcome + Send> {
//!     FnTask::new(desc, move |_| match exec.exec(&spec) {
//!         Ok(out) if out.success() => TaskOutcome::Success,
//!         Ok(out) => TaskOutcome::Failed(format!("exit {}: {}", out.status, out.stderr.trim())),
//!         Err(e) => TaskOutcome::Failed(e.to_string()),
//!     })
//! }
//!
//! /// The integration's entry point: build the DAG and run it.
//! fn deploy(
//!     config: &FleetConfig,
//!     exec: Arc<dyn Executor>,
//!     opts: &DeployOptions,
//! ) -> Result<DagReport, String> {
//!     let mut dag = DagBuilder::new();
//!     // Independent hosts must survive a sibling host's failure.
//!     dag.error_policy(ErrorPolicy::ContinueIndependent);
//!     for task in plan(config, opts)? {
//!         dag.add_task(task.id.clone(), cmd_task(task.desc, Arc::clone(&exec), task.spec));
//!         if let Some(after) = task.after {
//!             dag.add_dep(task.id, after);
//!         }
//!     }
//!     dag.build().map_err(|e| e.to_string())?.run().map_err(|e| e.to_string())
//! }
//!
//! // ── Step 1 + 2: implement Contract, including fixtures ────────────────────
//!
//! struct MyContract;
//!
//! impl Contract for MyContract {
//!     type Report = DagReport;
//!     type Error = String;
//!     type Options = DeployOptions;
//!
//!     fn name() -> &'static str {
//!         "my-integration"
//!     }
//!
//!     fn run(
//!         config: &FleetConfig,
//!         exec: Arc<dyn Executor>,
//!         opts: &Self::Options,
//!     ) -> Result<Self::Report, Self::Error> {
//!         deploy(config, exec, opts)
//!     }
//!
//!     fn phases() -> Vec<Phase> {
//!         // Markers: single tokens (quoting-safe for ssh renderings), no
//!         // substring relations, declared in topological order.
//!         vec![
//!             Phase::new("stage", "nix copy", &[]),
//!             Phase::new("execute", "dummy-run", &["stage"]),
//!             Phase::new("collect", "dummy-collect", &["execute"]),
//!         ]
//!     }
//!
//!     fn config_missing() -> FleetConfig {
//!         fleet_config(false)
//!     }
//!
//!     fn config_complete() -> FleetConfig {
//!         fleet_config(true)
//!     }
//!
//!     fn happy_executor() -> ScriptedExecutor {
//!         ScriptedExecutor::new()
//!             .on("nix copy", ExecOutput::ok(""))
//!             .on("dummy-run", ExecOutput::ok("ok\n"))
//!             .on("dummy-collect", ExecOutput::ok("collected\n"))
//!     }
//!
//!     fn executor_failing_phase(marker: &str) -> ScriptedExecutor {
//!         // The failing rule comes FIRST: ScriptedExecutor is first-match-wins.
//!         ScriptedExecutor::new()
//!             .rule(Rule::containing(marker, ExecOutput::new(1, "", "boom")))
//!             .on("nix copy", ExecOutput::ok(""))
//!             .on("dummy-run", ExecOutput::ok("ok\n"))
//!             .on("dummy-collect", ExecOutput::ok("collected\n"))
//!     }
//!
//!     fn option_variants() -> Vec<OptionVariant<Self::Options>> {
//!         vec![OptionVariant::new(
//!             "no-collect",
//!             DeployOptions { skip_collect: true },
//!             vec!["collect"],
//!         )]
//!     }
//!
//!     fn partial_failure() -> Option<PartialFailure> {
//!         let executor = ScriptedExecutor::new()
//!             .rule(Rule::containing_all(
//!                 ["node02", "dummy-run"],
//!                 ExecOutput::new(1, "", "node02 unreachable"),
//!             ))
//!             .on("nix copy", ExecOutput::ok(""))
//!             .on("dummy-run", ExecOutput::ok("ok\n"))
//!             .on("dummy-collect", ExecOutput::ok("collected\n"));
//!         Some(PartialFailure::new(executor, "node02", "node01", "dummy-collect"))
//!     }
//!
//!     fn task_descriptions(config: &FleetConfig, _exec: Arc<dyn Executor>) -> Vec<String> {
//!         plan(config, &DeployOptions::default())
//!             .map(|tasks| tasks.into_iter().map(|t| t.desc).collect())
//!             .unwrap_or_default()
//!     }
//! }
//!
//! // ── Step 2b: shared config fixture ────────────────────────────────────────
//!
//! fn fleet_config(with_slurm: bool) -> FleetConfig {
//!     let node = |name: &str| DeploymentNode {
//!         name: name.into(),
//!         target_host: name.into(), // hostname doubles as the per-host marker
//!         target_user: "root".into(),
//!         target_port: None,
//!         system: "x86_64-linux".into(),
//!         profile_type: ProfileType::Nixos,
//!         build_on_target: false,
//!         tags: vec![],
//!         drv_path: None,
//!         toplevel: None,
//!     };
//!     let mut nodes = HashMap::new();
//!     nodes.insert("node01".to_string(), node("node01"));
//!     nodes.insert("node02".to_string(), node("node02"));
//!     FleetConfig {
//!         nodes,
//!         builders: HashMap::new(),
//!         flake_uri: ".".into(),
//!         ansible_config: None,
//!         slurm_config: with_slurm.then(|| SlurmFleetConfig {
//!             submit_node: "node01".into(),
//!             submit_user: "root".into(),
//!             control_node: "node01".into(),
//!             partitions: HashMap::new(),
//!         }),
//!         ray_config: None,
//!         skypilot_config: None,
//!     }
//! }
//!
//! // ── Step 3: instantiate the suite ─────────────────────────────────────────
//!
//! integration_contract_tests!(MyContract);
//! ```

pub mod checks;
mod contract;

pub use contract::{Contract, OptionVariant, PartialFailure, Phase};

pub use consortium_integration::exec::{Executor, ScriptedExecutor};
pub use consortium_integration::fleet::FleetConfig;
pub use consortium_integration::report::IntegrationReport;

/// Instantiate the abstract integration contract suite for one [`Contract`]
/// implementation.
///
/// Expands to seven `#[test]` functions with uniform names, each delegating
/// to a generic check in [`checks`]:
///
/// ```text
/// #[test] fn contract_missing_config_errors() { checks::missing_config_errors::<C>() }
/// #[test] fn contract_plan_has_no_side_effects() { checks::plan_has_no_side_effects::<C>() }
/// #[test] fn contract_happy_path() { checks::happy_path::<C>() }
/// #[test] fn contract_first_phase_failure_aborts_pipeline() { ... }
/// #[test] fn contract_mid_pipeline_failure_cancels_dependents() { ... }
/// #[test] fn contract_partial_host_failure_continues_independents() { ... }
/// #[test] fn contract_option_variants_skip_declared_phases() { ... }
/// ```
///
/// Invoke once per integration, at the top level of its `tests/contract.rs`:
///
/// ```rust,ignore
/// integration_contract_tests!(MyContract);
/// ```
///
/// (The names are fixed on purpose: every integration reports the same
/// suite, so a green `contract_*` run means the same thing everywhere.)
#[macro_export]
macro_rules! integration_contract_tests {
    ($contract:ty) => {
        #[test]
        fn contract_missing_config_errors() {
            $crate::checks::missing_config_errors::<$contract>();
        }

        #[test]
        fn contract_plan_has_no_side_effects() {
            $crate::checks::plan_has_no_side_effects::<$contract>();
        }

        #[test]
        fn contract_happy_path() {
            $crate::checks::happy_path::<$contract>();
        }

        #[test]
        fn contract_first_phase_failure_aborts_pipeline() {
            $crate::checks::first_phase_failure_aborts_pipeline::<$contract>();
        }

        #[test]
        fn contract_mid_pipeline_failure_cancels_dependents() {
            $crate::checks::mid_pipeline_failure_cancels_dependents::<$contract>();
        }

        #[test]
        fn contract_partial_host_failure_continues_independents() {
            $crate::checks::partial_host_failure_continues_independents::<$contract>();
        }

        #[test]
        fn contract_option_variants_skip_declared_phases() {
            $crate::checks::option_variants_skip_declared_phases::<$contract>();
        }
    };
}
