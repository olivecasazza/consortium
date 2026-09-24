//! The integration-side hook: the [`Contract`] trait plus the fixture types
//! it returns ([`Phase`], [`OptionVariant`], [`PartialFailure`]).
//!
//! Read the crate-level documentation for the full 3-step usage guide. This
//! module documents the *semantics* the harness pins down; getting these
//! right is what makes the generated suite pass.

use std::fmt::Debug;
use std::sync::Arc;

use consortium_integration::exec::{Executor, ScriptedExecutor};
use consortium_integration::fleet::FleetConfig;
use consortium_integration::report::IntegrationReport;

/// A named stage of an integration's command pipeline.
///
/// # Marker discipline (read carefully)
///
/// `marker` is a plain substring matched against the *rendered* command line
/// (`CommandSpec::render()`), which is what
/// [`ScriptedExecutor`](consortium_integration::exec::ScriptedExecutor) rules
/// match and what the harness asserts on. Markers must satisfy three rules:
///
/// 1. **Complete** — every command the integration issues for this phase
///    contains the marker.
/// 2. **Exclusive** — no command belonging to a *different* phase contains
///    the marker. The harness enforces the static part of this (no two
///    declared markers may be substrings of each other); the dynamic part
///    (actual command lines) is the fixture's job.
/// 3. **Quoting-safe** — ssh commands render with every remote token
///    single-quoted: `ssh ... -l root host 'sbatch' '--job-name=x'`. A marker
///    containing a space (e.g. `"sbatch --job-name"`) will *not* match that
///    rendering. Use single tokens without spaces (`"sbatch"`) for ssh-wrapped
///    phases. Space-containing markers (e.g. `"nix copy"`) are fine for
///    phases that only ever run locally, where the rendered line is the raw
///    argv joined by spaces.
///
/// `after` names phases that must precede this one. Phases must be declared
/// in topological order: every `after` entry must name a phase declared
/// *earlier* in the [`Contract::phases`] vector (the harness panics with a
/// descriptive message otherwise).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Phase {
    /// Human-readable phase name, unique within [`Contract::phases`].
    pub name: &'static str,
    /// Substring present in every rendered command line of this phase.
    pub marker: &'static str,
    /// Names of earlier phases that must complete before this one starts.
    pub after: &'static [&'static str],
}

impl Phase {
    /// Convenience constructor.
    pub const fn new(
        name: &'static str,
        marker: &'static str,
        after: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            marker,
            after,
        }
    }
}

/// One non-default option configuration the contract suite exercises.
///
/// The harness runs the integration with `options` on top of
/// [`Contract::happy_executor`] and asserts:
///
/// - the run succeeds;
/// - every phase named in `skipped_phases` was **not** invoked (matched by
///   phase name against [`Contract::phases`]; unknown names fail the test);
/// - every phase *not* listed was invoked.
#[derive(Debug)]
pub struct OptionVariant<Options> {
    /// Variant name, used in failure messages.
    pub name: &'static str,
    /// Option values for this variant.
    pub options: Options,
    /// Names (not markers) of phases this variant intentionally skips.
    pub skipped_phases: Vec<&'static str>,
}

impl<Options> OptionVariant<Options> {
    /// Convenience constructor.
    pub fn new(name: &'static str, options: Options, skipped_phases: Vec<&'static str>) -> Self {
        Self {
            name,
            options,
            skipped_phases,
        }
    }
}

/// Fixture for per-host partial failure: one host dies mid-pipeline while
/// another must run to completion.
///
/// Semantics the harness asserts (when [`Contract::partial_failure`] returns
/// `Some`):
///
/// - the outcome shows failure (either convention — see [`Contract::run`]);
/// - for **every** declared phase, at least one invocation contains both the
///   phase marker and `surviving_host_marker` (the surviving host completes
///   its full chain);
/// - no invocation contains both `blocked_phase_marker` and
///   `failed_host_marker` (the failed host never reaches the blocked phase);
/// - when the run returns `Ok(report)`, `report.success_count() >= 1 &&
///   report.failure_count() >= 1`.
///
/// # Fixture requirements
///
/// - `executor` fails commands containing `failed_host_marker` at/after some
///   point in the pipeline and lets everything else succeed. Put the failing
///   [`Rule`](consortium_integration::exec::Rule) **before** the catch-all
///   success rules — `ScriptedExecutor` is first-match-wins.
/// - Every phase must issue at least one command per host that carries the
///   host marker in its rendered line (make hostnames/addresses the host
///   markers, and make sure they appear in every per-host command). If your
///   integration has host-independent phases (e.g. one local build shared by
///   all hosts), either give those commands a per-host form or return `None`
///   and cover partial failure in integration-specific tests instead.
/// - The integration must run with a continue-independent error policy for
///   this scenario: the failed host's dependents are cancelled, the
///   surviving host's chain runs to completion.
pub struct PartialFailure {
    /// Executor scripting the partial failure.
    pub executor: ScriptedExecutor,
    /// Substring identifying commands aimed at the host that fails.
    pub failed_host_marker: &'static str,
    /// Substring identifying commands aimed at the host that must survive.
    pub surviving_host_marker: &'static str,
    /// Phase marker that must never run for the failed host.
    pub blocked_phase_marker: &'static str,
}

impl PartialFailure {
    /// Convenience constructor.
    pub fn new(
        executor: ScriptedExecutor,
        failed_host_marker: &'static str,
        surviving_host_marker: &'static str,
        blocked_phase_marker: &'static str,
    ) -> Self {
        Self {
            executor,
            failed_host_marker,
            surviving_host_marker,
            blocked_phase_marker,
        }
    }
}

/// The hook an integration implements to enroll in the abstract contract
/// suite.
///
/// Implement this trait once per integration (typically in
/// `tests/contract.rs`), then instantiate
/// [`integration_contract_tests!`](crate::integration_contract_tests) to
/// generate the seven uniform semantic tests. See the crate-level docs for a
/// complete copy-pasteable example.
///
/// # Pinned-down semantics
///
/// - **Failure reporting conventions.** The harness tolerates both styles:
///   `run` may return `Err(..)`, or it may return `Ok(report)` with
///   `report.is_success() == false` and `report.failure_count() >= 1`. Pick
///   one per situation and stay consistent; happy paths and option variants
///   must always return `Ok` with a successful report.
/// - **Missing configuration is an error, before any side effect.**
///   `run(config_missing(), ..)` must return `Err` and must not have issued
///   a single command.
/// - **Planning is side-effect free.** [`Contract::task_descriptions`]
///   receives an executor only so task construction can capture it; it must
///   never call `exec` on it (the harness asserts zero invocations).
/// - **`executor_failing_phase` receives a phase marker**, not a phase name.
///   The returned executor must fail every command whose rendered line
///   contains that marker (non-zero exit status with stderr is the realistic
///   choice; [`ExecError`](consortium_integration::exec::ExecError)-style
///   rule failures also work because the harness tolerates both outcome
///   conventions) and let every other command succeed. Failing rules must be
///   inserted before the catch-all success rules (first match wins).
pub trait Contract {
    /// The integration's run report.
    type Report: IntegrationReport;
    /// The integration's error type.
    type Error: Debug;
    /// Options modifying a run. Use `()` when the integration has no
    /// variants (the harness only needs [`Default`], which `()` provides).
    type Options: Default;

    /// Integration name, used in every failure message (e.g. `"slurm"`).
    fn name() -> &'static str;

    /// A fleet config **missing this integration's sub-config** (e.g.
    /// `slurm_config: None` for the slurm integration) but otherwise valid.
    fn config_missing() -> FleetConfig;

    /// A complete, valid fleet config with this integration's sub-config
    /// present and at least one node.
    fn config_complete() -> FleetConfig;

    /// An executor where every command the integration issues succeeds with
    /// realistic output (e.g. sbatch → `"Submitted batch job 12345\n"`).
    /// A catch-all `.on("", ExecOutput::ok(..))` as the last rule guards
    /// against unscripted commands failing the happy path spuriously.
    fn happy_executor() -> ScriptedExecutor;

    /// An executor where commands containing `phase_marker` fail (non-zero
    /// exit + stderr recommended) and every other command succeeds.
    /// `phase_marker` is always one of the markers from [`Contract::phases`].
    fn executor_failing_phase(phase_marker: &str) -> ScriptedExecutor;

    /// Invoke the integration's entry point.
    fn run(
        config: &FleetConfig,
        exec: Arc<dyn Executor>,
        opts: &Self::Options,
    ) -> Result<Self::Report, Self::Error>;

    /// The integration's pipeline phases, in topological order. See
    /// [`Phase`] for the marker discipline the harness relies on.
    fn phases() -> Vec<Phase>;

    /// Non-default option variants to exercise. Default: none (the
    /// option-variant test then passes trivially).
    fn option_variants() -> Vec<OptionVariant<Self::Options>> {
        Vec::new()
    }

    /// Per-host partial-failure fixture. Default: `None` (the harness skips
    /// that test with a note). See [`PartialFailure`] for the semantics.
    fn partial_failure() -> Option<PartialFailure> {
        None
    }

    /// Construct the run's tasks for a representative small input and return
    /// their human-readable descriptions, **without executing anything**.
    ///
    /// The `exec` parameter exists so integrations whose DAG tasks capture
    /// the executor at construction time can build them; the harness asserts
    /// afterwards that it recorded zero invocations. Descriptions must be
    /// non-empty, single-line (no control characters) strings.
    fn task_descriptions(config: &FleetConfig, exec: Arc<dyn Executor>) -> Vec<String>;
}
