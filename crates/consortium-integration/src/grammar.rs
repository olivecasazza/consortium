//! Declarative integration parameters and side-effect-free pipeline planning.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;

use crate::fleet::FleetConfig;
use crate::{Executor, IntegrationReport};
use consortium::dag::{DagBuilder, DagContext, DagTask, ErrorPolicy, TaskOutcome};

/// Parameter values accepted by both the CLI and runtime boundary.
#[derive(Debug, Clone, Copy)]
pub enum ParameterKind {
    Text,
    Boolean,
    PositiveInteger,
    PositiveDuration,
    Choice(&'static [&'static str]),
}

/// Parse positive seconds without floating-point rounding or truncation.
pub fn parse_duration(value: &str) -> Result<std::time::Duration, IntegrationError> {
    let invalid = || IntegrationError(format!("invalid positive duration in seconds: {value:?}"));
    let (seconds, fraction) = value
        .split_once('.')
        .map_or((value, None), |(seconds, fraction)| {
            (seconds, Some(fraction))
        });
    if seconds.is_empty() || !seconds.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let seconds = seconds.parse::<u64>().map_err(|_| invalid())?;
    let nanos = match fraction {
        None => 0,
        Some(fraction)
            if !fraction.is_empty()
                && fraction.len() <= 9
                && fraction.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            fraction.parse::<u32>().map_err(|_| invalid())? * 10u32.pow(9 - fraction.len() as u32)
        }
        Some(_) => return Err(invalid()),
    };
    let duration = std::time::Duration::new(seconds, nanos);
    if duration.is_zero() {
        Err(invalid())
    } else {
        Ok(duration)
    }
}

/// Decimal seconds preserving all of a duration's nanoseconds.
pub fn duration_text(duration: std::time::Duration) -> String {
    if duration.subsec_nanos() == 0 {
        return duration.as_secs().to_string();
    }
    let fraction = format!("{:09}", duration.subsec_nanos());
    format!("{}.{}", duration.as_secs(), fraction.trim_end_matches('0'))
}

/// One supported parameter; absent defaults make a parameter optional.
#[derive(Debug, Clone, Copy)]
pub struct Parameter {
    pub name: &'static str,
    pub help: &'static str,
    pub kind: ParameterKind,
    pub required: bool,
    pub default: Option<&'static str>,
}

impl Parameter {
    pub const fn required(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            kind: ParameterKind::Text,
            required: true,
            default: None,
        }
    }

    pub const fn optional(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            kind: ParameterKind::Text,
            required: false,
            default: None,
        }
    }

    pub const fn typed(mut self, kind: ParameterKind) -> Self {
        self.kind = kind;
        self
    }

    pub const fn defaulted(
        name: &'static str,
        help: &'static str,
        kind: ParameterKind,
        value: &'static str,
    ) -> Self {
        Self {
            name,
            help,
            kind,
            required: false,
            default: Some(value),
        }
    }
}

/// Condition selecting an optional phase from validated parameter values.
#[derive(Debug, Clone, Copy)]
pub enum Condition {
    Always,
    Equals(&'static str, &'static str),
    NotEquals(&'static str, &'static str),
    Present(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PhaseKind {
    Preflight,
    Evaluate,
    Build,
    Copy,
    Execute,
    Submit,
    Wait,
    Collect,
    Acquire,
    Release,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PhaseScope {
    Shared,
    Target,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EffectKind {
    Process,
    RemoteProcess,
    TemporaryFile,
    Clock,
    Sleep,
    Diagnostic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailurePolicy {
    Stop,
    ContinueIndependent,
}

/// Release is dependent on a confirmed successful acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ReleasePolicy {
    AfterSuccessfulAcquisition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DependencyPolicy {
    Success,
    Settled,
}

/// A phase selects tasks, dependency scope, and allowed effects.
#[derive(Debug, Clone, Copy)]
pub struct Phase {
    pub name: &'static str,
    pub condition: Condition,
    pub kind: PhaseKind,
    pub scope: PhaseScope,
    pub effects: &'static [EffectKind],
    pub release_policy: Option<ReleasePolicy>,
    pub dependency_policy: DependencyPolicy,
    pub requires: &'static [&'static str],
    /// Upper bound on the phase's concurrency group; the `fanout` parameter
    /// cannot raise it. `None` leaves the group at the plan's fanout.
    pub limit_cap: Option<usize>,
}

impl Phase {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            condition: Condition::Always,
            kind: PhaseKind::Execute,
            scope: PhaseScope::Shared,
            effects: &[EffectKind::Process],
            release_policy: None,
            dependency_policy: DependencyPolicy::Success,
            requires: &[],
            limit_cap: None,
        }
    }

    pub const fn when(name: &'static str, condition: Condition) -> Self {
        Self {
            condition,
            ..Self::new(name)
        }
    }
    pub const fn requiring(mut self, phases: &'static [&'static str]) -> Self {
        self.requires = phases;
        self
    }

    pub const fn after_settled(mut self) -> Self {
        self.dependency_policy = DependencyPolicy::Settled;
        self
    }

    /// Bound this phase's concurrency to at most `limit` tasks.
    pub const fn capped(mut self, limit: usize) -> Self {
        self.limit_cap = Some(limit);
        self
    }

    pub const fn behavior(
        mut self,
        kind: PhaseKind,
        scope: PhaseScope,
        effects: &'static [EffectKind],
    ) -> Self {
        self.release_policy = if matches!(kind, PhaseKind::Release) {
            Some(ReleasePolicy::AfterSuccessfulAcquisition)
        } else {
            None
        };
        self.kind = kind;
        self.scope = scope;
        self.effects = effects;
        self
    }
}

/// The vocabulary an adapter implements. CLI arguments are generated from it.
#[derive(Debug)]
pub struct Grammar {
    pub name: &'static str,
    pub about: &'static str,
    pub targets: bool,
    pub parameters: &'static [Parameter],
    pub phases: &'static [Phase],
    pub failure_policy: FailurePolicy,
}

/// One invocation, before grammar validation and default expansion.
#[derive(Debug, Clone, Default)]
pub struct Request {
    pub targets: Vec<String>,
    pub parameters: BTreeMap<String, String>,
}

impl Request {
    pub fn with(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(name.into(), value.into());
        self
    }
}

/// Validated parameters and the logical phases selected by their values.
#[derive(Debug, Clone, Serialize)]
pub struct Plan {
    pub integration: String,
    pub targets: Vec<String>,
    pub phases: Vec<String>,
    pub parameters: BTreeMap<String, String>,
    #[serde(skip)]
    config_binding: serde_json::Value,
    #[serde(skip)]
    invocation_binding: serde_json::Value,
}

impl Plan {
    pub fn value(&self, name: &str) -> &str {
        self.parameters.get(name).map(String::as_str).unwrap_or("")
    }

    pub fn flag(&self, name: &str) -> bool {
        self.value(name) == "true"
    }

    pub fn duration(&self, name: &str) -> Result<std::time::Duration, IntegrationError> {
        parse_duration(self.value(name))
            .map_err(|error| IntegrationError(format!("{}: --{name}: {error}", self.integration)))
    }

    pub fn positive_integer(&self, name: &str) -> Result<usize, IntegrationError> {
        self.value(name)
            .parse::<usize>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| {
                IntegrationError(format!(
                    "{}: invalid positive integer --{name}",
                    self.integration
                ))
            })
    }
}

/// Stable accounting for consumers that do not need an adapter-specific report.
#[derive(Debug, Clone, Serialize)]
pub struct RunReport {
    pub integration: String,
    pub success: bool,
    pub succeeded: usize,
    pub failed: usize,
    pub resources: Vec<ResourceOutcome>,
    pub phases: Vec<PhaseOutcome>,
    pub cleanup_errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResourceState {
    Unknown,
    Retained,
    Released,
    CleanupFailed,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceOutcome {
    pub name: String,
    pub state: ResourceState,
}

#[derive(Debug, Clone, Serialize)]
pub struct PhaseOutcome {
    pub phase: String,
    pub task: String,
    pub success: bool,
    pub effects: Vec<EffectKind>,
    pub error: Option<String>,
}

impl RunReport {
    pub fn from_report(name: &str, report: &impl IntegrationReport) -> Self {
        Self {
            integration: name.into(),
            success: report.is_success(),
            succeeded: report.success_count(),
            failed: report.failure_count(),
            resources: Vec::new(),
            phases: Vec::new(),
            cleanup_errors: Vec::new(),
        }
    }
}

impl IntegrationReport for RunReport {
    fn is_success(&self) -> bool {
        self.success
    }
    fn failure_count(&self) -> usize {
        self.failed
    }
    fn success_count(&self) -> usize {
        self.succeeded
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct IntegrationError(pub String);

/// Normalized outcome and the established task-level DAG report.
#[derive(Debug)]
pub struct ExecutionResult {
    pub report: RunReport,
    pub dag: consortium::dag::DagReport,
}

/// Object-safe adapter contract shared by CLI dispatch and parameterized tests.
pub trait Integration: Send + Sync {
    fn grammar(&self) -> &'static Grammar;

    /// Validate adapter-specific configuration without performing I/O.
    fn validate_config(&self, config: &FleetConfig) -> Result<(), IntegrationError>;

    /// Compile one declared phase into an existing DAG task without I/O.
    fn task(
        &self,
        config: &FleetConfig,
        plan: &Plan,
        phase: &Phase,
        resource: &str,
    ) -> Result<PhaseTask, IntegrationError>;

    /// Configure typed task context without I/O.
    fn configure(
        &self,
        _config: &FleetConfig,
        _plan: &Plan,
        _context: &DagContext,
    ) -> Result<(), IntegrationError> {
        Ok(())
    }

    /// Release invocation-scoped leases, including after failed DAG execution.
    fn cleanup(&self, _context: &DagContext) -> Result<(), IntegrationError> {
        Ok(())
    }

    /// Execute a plan and retain its normalized outcome.
    fn execute(
        &self,
        exec: Arc<dyn Executor>,
        config: &FleetConfig,
        plan: &Plan,
    ) -> Result<RunReport, IntegrationError> {
        self.execute_detailed(exec, config, plan)
            .map(|result| result.report)
    }

    /// Execute only the phase selection validated and bound to this configuration.
    fn execute_detailed(
        &self,
        exec: Arc<dyn Executor>,
        config: &FleetConfig,
        plan: &Plan,
    ) -> Result<ExecutionResult, IntegrationError> {
        let canonical = self.plan(
            config,
            &Request {
                targets: plan.targets.clone(),
                parameters: plan.parameters.clone(),
            },
        )?;
        if serde_json::to_value(plan).map_err(|e| IntegrationError(e.to_string()))?
            != plan.invocation_binding
            || plan.integration != canonical.integration
            || plan.phases != canonical.phases
            || plan.targets != canonical.targets
            || plan.parameters != canonical.parameters
            || plan.config_binding != canonical.config_binding
        {
            return Err(IntegrationError(
                "altered, foreign, or stale integration plan".into(),
            ));
        }
        let context = DagContext::new();
        context.set_state("fleet_config", config.clone());
        self.configure(config, plan, &context)?;
        let mut dag = DagBuilder::new();
        let mut previous: Vec<(String, String)> = Vec::new();
        let mut phase_tasks: BTreeMap<&str, Vec<(String, String)>> = BTreeMap::new();
        let mut lifecycle = Vec::new();
        let mut task_ids = std::collections::HashSet::new();
        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        for phase in self
            .grammar()
            .phases
            .iter()
            .filter(|phase| plan.phases.iter().any(|name| name == phase.name))
        {
            let resources = match phase.scope {
                PhaseScope::Shared => vec![String::new()],
                PhaseScope::Target => plan.targets.clone(),
            };
            let limit = if phase.kind == PhaseKind::Evaluate {
                1
            } else if plan.parameters.contains_key("fanout") {
                plan.positive_integer("fanout")?
            } else {
                1
            };
            dag.concurrency_group(
                phase.name,
                phase.limit_cap.map_or(limit, |cap| limit.min(cap)),
            );
            let mut current = Vec::new();
            for resource in resources {
                let mut task = self.task(config, plan, phase, &resource)?;
                task.phase = phase.name;
                task.allowed = phase.effects;
                task.observations = observations.clone();
                let id = task.id.clone();
                if id.is_empty() || id == "integration:cleanup" || !task_ids.insert(id.clone()) {
                    return Err(IntegrationError(format!(
                        "duplicate or empty compiled task id: {id:?}"
                    )));
                }
                if matches!(phase.kind, PhaseKind::Acquire | PhaseKind::Release) {
                    lifecycle.push((phase.kind, id.clone()));
                }
                dag.add_task(&id, task);
                for (previous_resource, previous_id) in &previous {
                    if resource.is_empty()
                        || previous_resource.is_empty()
                        || resource == *previous_resource
                    {
                        match phase.dependency_policy {
                            DependencyPolicy::Success => {
                                dag.add_dep(&id, previous_id);
                            }
                            DependencyPolicy::Settled => {
                                dag.add_completion_dep(&id, previous_id);
                            }
                        }
                    }
                }
                for required in phase.requires {
                    let dependencies = phase_tasks.get(required).ok_or_else(|| {
                        IntegrationError(format!("missing required phase: {required}"))
                    })?;
                    for (required_resource, required_id) in dependencies {
                        if resource.is_empty()
                            || required_resource.is_empty()
                            || resource == *required_resource
                        {
                            dag.add_dep(&id, required_id);
                        }
                    }
                }
                dag.assign_group(&id, phase.name);
                current.push((resource, id));
            }
            phase_tasks.insert(phase.name, current.clone());
            previous = current;
        }
        // Executor capabilities enter the context only after compilation succeeds.
        context.set_state("executor", exec);
        dag.context(context.clone());
        dag.error_policy(match self.grammar().failure_policy {
            FailurePolicy::Stop => ErrorPolicy::FailFast,
            FailurePolicy::ContinueIndependent => ErrorPolicy::ContinueIndependent,
        });
        let outcome = dag
            .build()
            .map_err(|e| IntegrationError(e.to_string()))
            .and_then(|dag| dag.run().map_err(|e| IntegrationError(e.to_string())));
        let cleanup = self.cleanup(&context);
        let mut report = match outcome {
            Ok(report) => report,
            Err(error) => {
                return Err(match cleanup {
                    Ok(()) => error,
                    Err(cleanup) => IntegrationError(format!("{error}; cleanup failed: {cleanup}")),
                })
            }
        };
        let cleanup_error = cleanup.err().map(|error| error.to_string());
        if let Some(error) = &cleanup_error {
            report.failed.insert(
                consortium::dag::TaskId("integration:cleanup".into()),
                error.clone(),
            );
        }
        let mut result = RunReport::from_report(self.grammar().name, &report);
        if let Some(error) = cleanup_error {
            result.cleanup_errors.push(error);
        }
        result.phases = observations.lock().unwrap().clone();
        result.phases.sort_by(|a, b| a.task.cmp(&b.task));
        for (_, acquire_id) in lifecycle
            .iter()
            .filter(|(kind, _)| *kind == PhaseKind::Acquire)
        {
            let acquired = report
                .completed
                .contains(&consortium::dag::TaskId(acquire_id.clone()));
            let failed = report
                .failed
                .contains_key(&consortium::dag::TaskId(acquire_id.clone()));
            if !acquired && !failed {
                continue;
            }
            let release = lifecycle
                .iter()
                .find(|(kind, _)| *kind == PhaseKind::Release);
            let state = if !acquired {
                ResourceState::Unknown
            } else if release.is_some_and(|(_, id)| {
                report
                    .completed
                    .contains(&consortium::dag::TaskId(id.clone()))
            }) {
                ResourceState::Released
            } else if release.is_some_and(|(_, id)| {
                report
                    .failed
                    .contains_key(&consortium::dag::TaskId(id.clone()))
            }) {
                ResourceState::CleanupFailed
            } else {
                ResourceState::Retained
            };
            result.resources.push(ResourceOutcome {
                name: plan.value("name").into(),
                state,
            });
        }
        Ok(ExecutionResult {
            report: result,
            dag: report,
        })
    }

    /// Validate relationships between already typed parameters.
    fn validate_plan(&self, _config: &FleetConfig, _plan: &Plan) -> Result<(), IntegrationError> {
        Ok(())
    }

    /// Planning never receives an executor and cannot invoke external commands.
    fn plan(&self, config: &FleetConfig, request: &Request) -> Result<Plan, IntegrationError> {
        self.validate_config(config)?;
        let plan = self.grammar().plan(config, request)?;
        self.validate_plan(config, &plan)?;
        Ok(plan)
    }

    /// Validate the entire request before the first executor invocation.
    fn run(
        &self,
        exec: Arc<dyn Executor>,
        config: &FleetConfig,
        request: &Request,
    ) -> Result<RunReport, IntegrationError> {
        self.run_detailed(exec, config, request)
            .map(|result| result.report)
    }

    fn run_detailed(
        &self,
        exec: Arc<dyn Executor>,
        config: &FleetConfig,
        request: &Request,
    ) -> Result<ExecutionResult, IntegrationError> {
        let plan = self.plan(config, request)?;
        self.execute_detailed(exec, config, &plan)
    }
}

impl Grammar {
    pub fn plan(&self, config: &FleetConfig, request: &Request) -> Result<Plan, IntegrationError> {
        let error = |message: String| IntegrationError(format!("{}: {message}", self.name));
        let mut parameter_names = std::collections::HashSet::new();
        for parameter in self.parameters {
            if parameter.name.is_empty() || !parameter_names.insert(parameter.name) {
                return Err(error(format!(
                    "duplicate or empty parameter: {:?}",
                    parameter.name
                )));
            }
        }
        let mut phase_names = std::collections::HashSet::new();
        let mut acquisition = false;
        let mut previous_kind = None;
        let mut release_seen = false;
        for phase in self.phases {
            if phase.name.is_empty() || !phase_names.insert(phase.name) {
                return Err(error(format!("duplicate or empty phase: {:?}", phase.name)));
            }
            for required in phase.requires {
                if *required == phase.name || !phase_names.contains(required) {
                    return Err(error(format!(
                        "phase {} requires an earlier declared phase: {required}",
                        phase.name
                    )));
                }
            }
            if phase.scope == PhaseScope::Target && !self.targets {
                return Err(error(format!(
                    "phase {} requires unsupported targets",
                    phase.name
                )));
            }
            let parameter = match phase.condition {
                Condition::Always => None,
                Condition::Equals(key, _)
                | Condition::NotEquals(key, _)
                | Condition::Present(key) => Some(key),
            };
            if parameter.is_some_and(|key| !parameter_names.contains(key)) {
                return Err(error(format!(
                    "phase {} references an unknown parameter",
                    phase.name
                )));
            }
            if phase.kind == PhaseKind::Release && (!acquisition || phase.release_policy.is_none())
            {
                return Err(error(format!(
                    "release phase {} requires a prior acquisition and release policy",
                    phase.name
                )));
            }
            if phase.kind == PhaseKind::Acquire
                && (acquisition || phase.scope != PhaseScope::Shared)
            {
                return Err(error("only one shared acquisition is supported".into()));
            }
            if phase.kind == PhaseKind::Release {
                if phase.dependency_policy != DependencyPolicy::Success
                    || release_seen
                    || previous_kind != Some(PhaseKind::Acquire)
                    || phase.scope != PhaseScope::Shared
                {
                    return Err(error(
                        "release must immediately follow the single shared acquisition".into(),
                    ));
                }
                release_seen = true;
            }
            acquisition |= phase.kind == PhaseKind::Acquire;
            previous_kind = Some(phase.kind);
        }

        for name in request.parameters.keys() {
            if !self.parameters.iter().any(|p| p.name == name) {
                return Err(error(format!("unsupported parameter --{name}")));
            }
        }
        let mut parameters = request.parameters.clone();
        for parameter in self.parameters {
            if let Some(default) = parameter.default {
                parameters
                    .entry(parameter.name.into())
                    .or_insert_with(|| default.into());
            }
            let Some(value) = parameters.get(parameter.name) else {
                if parameter.required {
                    return Err(error(format!("missing --{}", parameter.name)));
                }
                continue;
            };
            let valid = match parameter.kind {
                ParameterKind::Text => !value.trim().is_empty(),
                ParameterKind::Boolean => matches!(value.as_str(), "true" | "false"),
                ParameterKind::PositiveInteger => value.parse::<usize>().is_ok_and(|n| n > 0),
                ParameterKind::PositiveDuration => parse_duration(value).is_ok(),
                ParameterKind::Choice(choices) => choices.contains(&value.as_str()),
            };
            if !valid {
                return Err(error(format!(
                    "invalid value for --{}: {value:?}",
                    parameter.name
                )));
            }
        }
        let mut targets = request.targets.clone();
        if !self.targets && !targets.is_empty() {
            return Err(error("host selection is unsupported".into()));
        }
        if self.targets {
            if targets.is_empty() {
                targets = config.node_names();
            }
            targets.sort();
            targets.dedup();
            if targets.is_empty() {
                return Err(error("no target nodes".into()));
            }
            for target in &targets {
                if !config.nodes.contains_key(target) {
                    return Err(error(format!("unknown host: {target}")));
                }
            }
        }
        let phases: Vec<String> = self
            .phases
            .iter()
            .filter(|phase| match phase.condition {
                Condition::Always => true,
                Condition::Equals(key, expected) => {
                    parameters.get(key).is_some_and(|v| v == expected)
                }
                Condition::NotEquals(key, expected) => {
                    parameters.get(key).is_none_or(|v| v != expected)
                }
                Condition::Present(key) => parameters.contains_key(key),
            })
            .map(|phase| phase.name.into())
            .collect();
        let selected: Vec<_> = self
            .phases
            .iter()
            .filter(|phase| phases.iter().any(|name| name == phase.name))
            .collect();
        for (index, phase) in selected.iter().enumerate() {
            for required in phase.requires {
                if !selected[..index]
                    .iter()
                    .any(|earlier| earlier.name == *required)
                {
                    return Err(error(format!(
                        "selected phase {} requires selected phase {required}",
                        phase.name
                    )));
                }
            }

            if phase.kind == PhaseKind::Release
                && (index == 0 || selected[index - 1].kind != PhaseKind::Acquire)
            {
                return Err(error(
                    "selected release requires its selected acquisition".into(),
                ));
            }
        }
        let mut plan = Plan {
            integration: self.name.into(),
            targets,
            phases,
            parameters,
            config_binding: serde_json::to_value(config).map_err(|e| error(e.to_string()))?,
            invocation_binding: serde_json::Value::Null,
        };
        plan.invocation_binding = serde_json::to_value(&plan).map_err(|e| error(e.to_string()))?;
        Ok(plan)
    }
}

/// Adapter task retaining the task's established output identifiers.
pub struct PhaseTask {
    pub id: String,
    task: Box<dyn DagTask>,
    phase: &'static str,
    allowed: &'static [EffectKind],
    observations: Arc<std::sync::Mutex<Vec<PhaseOutcome>>>,
}
impl PhaseTask {
    pub fn new(id: impl Into<String>, task: impl DagTask) -> Self {
        Self {
            id: id.into(),
            task: Box::new(task),
            phase: "",
            allowed: &[],
            observations: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}
impl DagTask for PhaseTask {
    fn execute(&self, context: &DagContext) -> TaskOutcome {
        let Some(executor) = context.get_state::<Arc<dyn Executor>>("executor") else {
            return TaskOutcome::Failed("executor not in context".into());
        };
        let guard = Arc::new(PhaseExecutor {
            inner: executor,
            allowed: self.allowed,
            observed: std::sync::Mutex::new(Vec::new()),
            denied: std::sync::atomic::AtomicBool::new(false),
        });
        let scoped = context.with_state_override("executor", guard.clone() as Arc<dyn Executor>);
        let mut outcome = self.task.execute(&scoped);
        if guard.denied.load(std::sync::atomic::Ordering::Relaxed) {
            outcome = TaskOutcome::Failed(format!("{}: undeclared effect rejected", self.phase));
        }
        self.observations.lock().unwrap().push(PhaseOutcome {
            phase: self.phase.into(),
            task: self.id.clone(),
            success: outcome.is_success(),
            error: match &outcome {
                TaskOutcome::Failed(error) => Some(error.clone()),
                _ => None,
            },
            effects: guard.observed.lock().unwrap().clone(),
        });
        outcome
    }
    fn describe(&self) -> String {
        self.task.describe()
    }
    fn resource_tag(&self) -> Option<&str> {
        self.task.resource_tag()
    }
}

struct PhaseExecutor {
    inner: Arc<dyn Executor>,
    allowed: &'static [EffectKind],
    observed: std::sync::Mutex<Vec<EffectKind>>,
    denied: std::sync::atomic::AtomicBool,
}
impl PhaseExecutor {
    fn permit(&self, effect: EffectKind) -> std::io::Result<()> {
        if self.denied.load(std::sync::atomic::Ordering::Relaxed) || !self.allowed.contains(&effect)
        {
            self.denied
                .store(true, std::sync::atomic::Ordering::Relaxed);
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("undeclared integration effect: {effect:?}"),
            ));
        }
        self.observed.lock().unwrap().push(effect);
        Ok(())
    }
    fn command_permission(&self, spec: &crate::CommandSpec) -> Result<(), crate::ExecError> {
        self.permit(if spec.ssh.is_some() {
            EffectKind::RemoteProcess
        } else {
            EffectKind::Process
        })
        .map_err(|source| crate::ExecError::Io {
            program: spec.program.clone(),
            source,
        })
    }
}
impl Executor for PhaseExecutor {
    fn exec(&self, spec: &crate::CommandSpec) -> Result<crate::ExecOutput, crate::ExecError> {
        self.command_permission(spec)?;
        self.inner.exec(spec)
    }
    fn exec_timeout(
        &self,
        spec: &crate::CommandSpec,
        limit: std::time::Duration,
    ) -> Result<crate::ExecOutput, crate::ExecError> {
        self.command_permission(spec)?;
        self.inner.exec_timeout(spec, limit)
    }
    fn temporary_file(
        &self,
        prefix: &str,
        contents: &[u8],
    ) -> std::io::Result<crate::TemporaryFile> {
        self.permit(EffectKind::TemporaryFile)?;
        self.inner.temporary_file(prefix, contents)
    }
    fn monotonic_now(&self) -> std::time::Duration {
        if self.permit(EffectKind::Clock).is_ok() {
            self.inner.monotonic_now()
        } else {
            std::time::Duration::ZERO
        }
    }
    fn wall_now(&self) -> std::time::SystemTime {
        if self.permit(EffectKind::Clock).is_ok() {
            self.inner.wall_now()
        } else {
            std::time::SystemTime::UNIX_EPOCH
        }
    }
    fn sleep(&self, duration: std::time::Duration) -> std::io::Result<()> {
        self.permit(EffectKind::Sleep)?;
        self.inner.sleep(duration)
    }
    fn event(&self, topic: &str, payload: &serde_json::Value) {
        if self.permit(EffectKind::Diagnostic).is_ok() {
            self.inner.event(topic, payload);
        }
    }
    fn event_with_native(
        &self,
        topic: &str,
        payload: &serde_json::Value,
        native: &(dyn std::any::Any + Send + Sync),
    ) {
        if self.permit(EffectKind::Diagnostic).is_ok() {
            self.inner.event_with_native(topic, payload, native);
        }
    }
    fn diagnostic(&self, message: &str) {
        if self.permit(EffectKind::Diagnostic).is_ok() {
            self.inner.diagnostic(message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandSpec, ExecError, ExecOutput, SshTarget, TemporaryFile};
    use consortium::dag::FnTask;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Barrier, Mutex,
    };
    use std::time::Duration;

    #[derive(Default)]
    struct AuditExecutor {
        calls: AtomicUsize,
    }
    impl AuditExecutor {
        fn called(&self) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Executor for AuditExecutor {
        fn exec(&self, _: &CommandSpec) -> Result<ExecOutput, ExecError> {
            self.called();
            Ok(ExecOutput::ok(""))
        }
        fn exec_timeout(&self, _: &CommandSpec, _: Duration) -> Result<ExecOutput, ExecError> {
            self.called();
            Ok(ExecOutput::ok(""))
        }
        fn temporary_file(&self, _: &str, _: &[u8]) -> std::io::Result<TemporaryFile> {
            self.called();
            Ok(TemporaryFile::new("/synthetic/file".into(), |_| Ok(())))
        }
        fn monotonic_now(&self) -> Duration {
            self.called();
            Duration::from_secs(1)
        }
        fn wall_now(&self) -> std::time::SystemTime {
            self.called();
            std::time::SystemTime::UNIX_EPOCH
        }
        fn sleep(&self, _: Duration) -> std::io::Result<()> {
            self.called();
            Ok(())
        }
        fn event(&self, _: &str, _: &serde_json::Value) {
            self.called();
        }
        fn diagnostic(&self, _: &str) {
            self.called();
        }
    }

    fn attempt(exec: &dyn Executor, effect: EffectKind, bounded: bool) {
        match effect {
            EffectKind::Process | EffectKind::RemoteProcess => {
                let spec = if effect == EffectKind::RemoteProcess {
                    CommandSpec::new("true").ssh(SshTarget::new("user", "host"))
                } else {
                    CommandSpec::new("true")
                };
                let _ = if bounded {
                    exec.exec_timeout(&spec, Duration::from_secs(1))
                } else {
                    exec.exec(&spec)
                };
            }
            EffectKind::TemporaryFile => {
                let _ = exec.temporary_file("audit", b"data");
            }
            EffectKind::Clock => {
                let _ = exec.monotonic_now();
            }
            EffectKind::Sleep => {
                let _ = exec.sleep(Duration::from_secs(1));
            }
            EffectKind::Diagnostic => exec.diagnostic("audit"),
        }
    }

    #[test]
    fn all_undeclared_effects_fail_even_when_task_ignores_rejection() {
        for effect in [
            EffectKind::Process,
            EffectKind::RemoteProcess,
            EffectKind::TemporaryFile,
            EffectKind::Clock,
            EffectKind::Sleep,
            EffectKind::Diagnostic,
        ] {
            for bounded in [false, true] {
                let audit = Arc::new(AuditExecutor::default());
                let context = DagContext::new();
                context.set_state("executor", audit.clone() as Arc<dyn Executor>);
                let task = PhaseTask::new(
                    "malicious",
                    FnTask::new("ignore errors", move |ctx| {
                        let exec: Arc<dyn Executor> = ctx.get_state("executor").unwrap();
                        attempt(&*exec, effect, bounded);
                        TaskOutcome::Success
                    }),
                );
                assert!(
                    matches!(task.execute(&context), TaskOutcome::Failed(message) if message.contains("undeclared effect")),
                    "{effect:?} bounded={bounded}"
                );
                assert_eq!(audit.calls.load(Ordering::SeqCst), 0, "{effect:?}");
            }
        }
    }

    #[test]
    fn parallel_tasks_keep_executor_capabilities_isolated_and_share_outputs() {
        let audit = Arc::new(AuditExecutor::default());
        let context = DagContext::new();
        context.set_state("executor", audit.clone() as Arc<dyn Executor>);
        let barrier = Arc::new(Barrier::new(2));
        let observations = Arc::new(Mutex::new(Vec::new()));
        let handles: Vec<_> = [false, true]
            .into_iter()
            .map(|allowed| {
                let barrier = barrier.clone();
                let context = context.clone();
                let mut task = PhaseTask::new(
                    if allowed { "allowed" } else { "denied" },
                    FnTask::new("concurrent access", move |ctx| {
                        let exec: Arc<dyn Executor> = ctx.get_state("executor").unwrap();
                        barrier.wait();
                        let _ = exec.exec(&CommandSpec::new("true"));
                        ctx.set_output(if allowed { "allowed" } else { "denied" }.into(), allowed);
                        TaskOutcome::Success
                    }),
                );
                task.phase = "execute";
                task.allowed = if allowed { &[EffectKind::Process] } else { &[] };
                task.observations = observations.clone();
                std::thread::spawn(move || task.execute(&context))
            })
            .collect();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert!(matches!(outcomes[0], TaskOutcome::Failed(_)));
        assert!(matches!(outcomes[1], TaskOutcome::Success));
        assert_eq!(audit.calls.load(Ordering::SeqCst), 1);
        assert_eq!(context.get_output::<bool>(&"allowed".into()), Some(true));
        assert_eq!(context.get_output::<bool>(&"denied".into()), Some(false));
        let observed = observations.lock().unwrap();
        assert_eq!(
            observed
                .iter()
                .find(|o| o.task == "allowed")
                .unwrap()
                .effects,
            [EffectKind::Process]
        );
        assert!(observed
            .iter()
            .find(|o| o.task == "denied")
            .unwrap()
            .effects
            .is_empty());
        drop(observed);
        let original: Arc<dyn Executor> = context.get_state("executor").unwrap();
        assert!(Arc::ptr_eq(&original, &(audit as Arc<dyn Executor>)));
    }

    fn config() -> FleetConfig {
        FleetConfig {
            nodes: Default::default(),
            builders: Default::default(),
            flake_uri: ".".into(),
            ansible_config: None,
            slurm_config: None,
            ray_config: None,
            skypilot_config: None,
        }
    }
    struct Adapter {
        grammar: &'static Grammar,
        duplicate_ids: bool,
        unsupported_phase: bool,
    }
    impl Integration for Adapter {
        fn grammar(&self) -> &'static Grammar {
            self.grammar
        }
        fn validate_config(&self, _: &FleetConfig) -> Result<(), IntegrationError> {
            Ok(())
        }
        fn task(
            &self,
            _: &FleetConfig,
            _: &Plan,
            phase: &Phase,
            _: &str,
        ) -> Result<PhaseTask, IntegrationError> {
            if self.unsupported_phase && phase.name == "second" {
                return Err(IntegrationError("undeclared task implementation".into()));
            }
            Ok(PhaseTask::new(
                if self.duplicate_ids {
                    "same"
                } else {
                    phase.name
                },
                FnTask::new("command", |ctx| {
                    let exec: Arc<dyn Executor> = ctx.get_state("executor").unwrap();
                    match exec.exec(&CommandSpec::new("true")) {
                        Ok(_) => TaskOutcome::Success,
                        Err(error) => TaskOutcome::Failed(error.to_string()),
                    }
                }),
            ))
        }
    }
    const COMMAND: Phase =
        Phase::new("first").behavior(PhaseKind::Build, PhaseScope::Shared, &[EffectKind::Process]);
    static TWO_PHASES: Grammar = Grammar {
        name: "audit",
        about: "audit",
        targets: false,
        parameters: &[],
        phases: &[COMMAND, Phase::new("second")],
        failure_policy: FailurePolicy::Stop,
    };

    #[test]
    fn compilation_errors_reject_the_entire_workflow_before_effects() {
        for (duplicate_ids, unsupported_phase) in [(true, false), (false, true)] {
            let adapter = Adapter {
                grammar: &TWO_PHASES,
                duplicate_ids,
                unsupported_phase,
            };
            let audit = Arc::new(AuditExecutor::default());
            assert!(adapter
                .run(audit.clone(), &config(), &Request::default())
                .is_err());
            assert_eq!(audit.calls.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn invalid_grammar_declarations_fail_before_task_execution() {
        let invalid: Vec<Grammar> = vec![
            Grammar {
                phases: &[COMMAND, COMMAND],
                ..TWO_PHASES
            },
            Grammar {
                phases: Box::leak(Box::new([Phase::when(
                    "first",
                    Condition::Present("missing"),
                )])),
                ..TWO_PHASES
            },
            Grammar {
                phases: Box::leak(Box::new([Phase::new("first").behavior(
                    PhaseKind::Execute,
                    PhaseScope::Target,
                    &[],
                )])),
                ..TWO_PHASES
            },
            Grammar {
                phases: Box::leak(Box::new([Phase::new("release").behavior(
                    PhaseKind::Release,
                    PhaseScope::Shared,
                    &[],
                )])),
                ..TWO_PHASES
            },
        ];
        for grammar in invalid {
            let adapter = Adapter {
                grammar: Box::leak(Box::new(grammar)),
                duplicate_ids: false,
                unsupported_phase: false,
            };
            let audit = Arc::new(AuditExecutor::default());
            assert!(adapter
                .run(audit.clone(), &config(), &Request::default())
                .is_err());
            assert_eq!(audit.calls.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn phase_conditions_select_actual_effects_for_each_truth_case() {
        for (condition, value, selected) in [
            (Condition::Always, None, true),
            (Condition::Equals("option", "yes"), Some("yes"), true),
            (Condition::Equals("option", "yes"), Some("no"), false),
            (Condition::Equals("option", "yes"), None, false),
            (Condition::NotEquals("option", "yes"), Some("yes"), false),
            (Condition::NotEquals("option", "yes"), Some("no"), true),
            (Condition::NotEquals("option", "yes"), None, true),
            (Condition::Present("option"), Some("yes"), true),
            (Condition::Present("option"), None, false),
        ] {
            let phases = Box::leak(Box::new([COMMAND, Phase::when("second", condition)]));
            let grammar = Box::leak(Box::new(Grammar {
                parameters: Box::leak(Box::new([Parameter::optional("option", "option")])),
                phases,
                ..TWO_PHASES
            }));
            let adapter = Adapter {
                grammar,
                duplicate_ids: false,
                unsupported_phase: false,
            };
            let audit = Arc::new(AuditExecutor::default());
            let request = value.map_or_else(Request::default, |value| {
                Request::default().with("option", value)
            });
            let report = adapter.run(audit.clone(), &config(), &request).unwrap();
            assert!(report.success);
            assert_eq!(
                audit.calls.load(Ordering::SeqCst),
                if selected { 2 } else { 1 }
            );
            assert_eq!(
                report.phases.iter().any(|phase| phase.phase == "second"),
                selected
            );
        }
    }

    struct Sleepers {
        grammar: &'static Grammar,
        inflight: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }
    impl Integration for Sleepers {
        fn grammar(&self) -> &'static Grammar {
            self.grammar
        }
        fn validate_config(&self, _: &FleetConfig) -> Result<(), IntegrationError> {
            Ok(())
        }
        fn task(
            &self,
            _: &FleetConfig,
            _: &Plan,
            _: &Phase,
            resource: &str,
        ) -> Result<PhaseTask, IntegrationError> {
            let inflight = self.inflight.clone();
            let peak = self.peak.clone();
            Ok(PhaseTask::new(
                format!("execute:{resource}"),
                FnTask::new("hold", move |_| {
                    let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(200));
                    inflight.fetch_sub(1, Ordering::SeqCst);
                    TaskOutcome::Success
                }),
            ))
        }
    }
    static UNCAPPED_EXECUTE: Grammar = Grammar {
        name: "uncapped",
        about: "uncapped",
        targets: true,
        parameters: &[Parameter::defaulted(
            "fanout",
            "fanout",
            ParameterKind::PositiveInteger,
            "1",
        )],
        phases: &[Phase::new("execute").behavior(
            PhaseKind::Execute,
            PhaseScope::Target,
            &[EffectKind::Process],
        )],
        failure_policy: FailurePolicy::ContinueIndependent,
    };
    static CAPPED_EXECUTE: Grammar = Grammar {
        name: "capped",
        phases: &[Phase::new("execute")
            .behavior(
                PhaseKind::Execute,
                PhaseScope::Target,
                &[EffectKind::Process],
            )
            .capped(4)],
        ..UNCAPPED_EXECUTE
    };

    fn peak_concurrency(grammar: &'static Grammar, fanout: &str) -> usize {
        let mut fleet = config();
        for index in 0..16 {
            let name = format!("host{index:02}");
            fleet.nodes.insert(
                name.clone(),
                crate::fleet::DeploymentNode {
                    name,
                    target_host: "127.0.0.1".into(),
                    target_user: "root".into(),
                    target_port: None,
                    system: "x86_64-linux".into(),
                    profile_type: crate::fleet::ProfileType::Nixos,
                    build_on_target: false,
                    tags: vec![],
                    drv_path: None,
                    toplevel: None,
                },
            );
        }
        let peak = Arc::new(AtomicUsize::new(0));
        let adapter = Sleepers {
            grammar,
            inflight: Arc::new(AtomicUsize::new(0)),
            peak: peak.clone(),
        };
        let report = adapter
            .run(
                Arc::new(AuditExecutor::default()),
                &fleet,
                &Request::default().with("fanout", fanout),
            )
            .unwrap();
        assert!(report.success);
        peak.load(Ordering::SeqCst)
    }

    #[test]
    fn execute_phase_fanout_is_not_capped_unless_the_phase_declares_a_cap() {
        assert!(peak_concurrency(&UNCAPPED_EXECUTE, "16") > 4);
    }

    #[test]
    fn phase_cap_bounds_the_fanout_it_is_declared_on() {
        assert!(peak_concurrency(&CAPPED_EXECUTE, "16") <= 4);
    }
    #[test]
    fn resource_release_requires_selected_adjacent_single_acquisition() {
        let acquire = Phase::when("acquire", Condition::Present("option")).behavior(
            PhaseKind::Acquire,
            PhaseScope::Shared,
            &[EffectKind::Process],
        );
        let release = Phase::new("release").behavior(
            PhaseKind::Release,
            PhaseScope::Shared,
            &[EffectKind::Process],
        );
        let duplicate_acquire = Phase::new("acquire-again").behavior(
            PhaseKind::Acquire,
            PhaseScope::Shared,
            &[EffectKind::Process],
        );
        let duplicate_release = Phase::new("release-again").behavior(
            PhaseKind::Release,
            PhaseScope::Shared,
            &[EffectKind::Process],
        );
        for phases in [
            vec![acquire, release],
            vec![acquire, COMMAND, release],
            vec![acquire, duplicate_acquire, release],
            vec![acquire, release, duplicate_release],
        ] {
            let grammar = Box::leak(Box::new(Grammar {
                parameters: Box::leak(Box::new([Parameter::optional("option", "option")])),
                phases: Box::leak(phases.into_boxed_slice()),
                ..TWO_PHASES
            }));
            let adapter = Adapter {
                grammar,
                duplicate_ids: false,
                unsupported_phase: false,
            };
            let audit = Arc::new(AuditExecutor::default());
            assert!(adapter
                .run(audit.clone(), &config(), &Request::default())
                .is_err());
            assert_eq!(audit.calls.load(Ordering::SeqCst), 0);
        }
        let grammar = Box::leak(Box::new(Grammar {
            parameters: Box::leak(Box::new([Parameter::optional("option", "option")])),
            phases: Box::leak(Box::new([acquire, release])),
            ..TWO_PHASES
        }));
        let adapter = Adapter {
            grammar,
            duplicate_ids: false,
            unsupported_phase: false,
        };
        let audit = Arc::new(AuditExecutor::default());
        let report = adapter
            .run(
                audit.clone(),
                &config(),
                &Request::default().with("option", "yes"),
            )
            .unwrap();
        assert!(report.success);
        assert_eq!(report.resources[0].state, ResourceState::Released);
        assert_eq!(audit.calls.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn rejected_effect_revokes_remaining_task_capabilities() {
        let audit = Arc::new(AuditExecutor::default());
        let context = DagContext::new();
        context.set_state("executor", audit.clone() as Arc<dyn Executor>);
        let mut task = PhaseTask::new(
            "ignore-and-continue",
            FnTask::new("ignore denial", |ctx| {
                let exec: Arc<dyn Executor> = ctx.get_state("executor").unwrap();
                let _ = exec.temporary_file("undeclared", b"");
                let _ = exec.exec(&CommandSpec::new("true"));
                TaskOutcome::Success
            }),
        );
        task.allowed = &[EffectKind::Process];
        assert!(matches!(task.execute(&context), TaskOutcome::Failed(_)));
        assert_eq!(audit.calls.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn undeclared_wall_clock_is_rejected_even_when_task_ignores_it() {
        let audit = Arc::new(AuditExecutor::default());
        let context = DagContext::new();
        context.set_state("executor", audit.clone() as Arc<dyn Executor>);
        let task = PhaseTask::new(
            "wall-clock",
            FnTask::new("ignore wall clock denial", |ctx| {
                let exec: Arc<dyn Executor> = ctx.get_state("executor").unwrap();
                let _ = exec.wall_now();
                TaskOutcome::Success
            }),
        );
        assert!(matches!(task.execute(&context), TaskOutcome::Failed(_)));
        assert_eq!(audit.calls.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn phase_reports_preserve_failure_reason_and_effects() {
        for failure in [None, Some("remote activation rejected: permission denied")] {
            let audit = Arc::new(AuditExecutor::default());
            let context = DagContext::new();
            context.set_state("executor", audit.clone() as Arc<dyn Executor>);
            let mut task = PhaseTask::new(
                "activate:node",
                FnTask::new("activate", move |ctx| {
                    let exec: Arc<dyn Executor> = ctx.get_state("executor").unwrap();
                    exec.exec(&CommandSpec::new("true")).unwrap();
                    match failure {
                        Some(error) => TaskOutcome::Failed(error.into()),
                        None => TaskOutcome::Success,
                    }
                }),
            );
            task.allowed = &[EffectKind::Process];
            task.execute(&context);
            let outcomes = task.observations.lock().unwrap();
            assert_eq!(outcomes[0].error.as_deref(), failure);
            assert_eq!(outcomes[0].effects, [EffectKind::Process]);
            assert_eq!(outcomes[0].success, failure.is_none());
            assert_eq!(
                serde_json::to_value(&outcomes[0]).unwrap()["error"],
                serde_json::to_value(failure).unwrap()
            );
        }
    }
    #[test]
    fn decimal_duration_round_trips_without_losing_nanoseconds() {
        for duration in [
            Duration::from_nanos(1),
            Duration::from_nanos(999_999_999),
            Duration::from_micros(1),
            Duration::from_millis(1),
            Duration::from_secs(10),
            Duration::new(1, 1),
            Duration::new(u64::MAX, 999_999_999),
        ] {
            let text = duration_text(duration);
            assert_eq!(parse_duration(&text).unwrap(), duration, "{text}");
        }
        for invalid in [
            "",
            "0",
            "0.0",
            "-1",
            "+1",
            ".1",
            "1.",
            "1.0000000001",
            "1e3",
            " 1",
            "1 ",
            "NaN",
            "1.1.1",
            "18446744073709551616",
        ] {
            assert!(parse_duration(invalid).is_err(), "{invalid}");
        }
        assert_eq!(duration_text(Duration::from_millis(250)), "0.25");
        assert_eq!(duration_text(Duration::ZERO), "0");
    }

    #[test]
    fn detailed_execution_preserves_raw_tasks_and_cleanup_failures() {
        struct CleanupAdapter;
        impl Integration for CleanupAdapter {
            fn grammar(&self) -> &'static Grammar {
                &TWO_PHASES
            }
            fn validate_config(&self, _: &FleetConfig) -> Result<(), IntegrationError> {
                Ok(())
            }
            fn task(
                &self,
                config: &FleetConfig,
                plan: &Plan,
                phase: &Phase,
                resource: &str,
            ) -> Result<PhaseTask, IntegrationError> {
                Adapter {
                    grammar: &TWO_PHASES,
                    duplicate_ids: false,
                    unsupported_phase: false,
                }
                .task(config, plan, phase, resource)
            }
            fn cleanup(&self, _: &DagContext) -> Result<(), IntegrationError> {
                Err(IntegrationError("cannot remove invocation lease".into()))
            }
        }
        let result = CleanupAdapter
            .run_detailed(
                Arc::new(AuditExecutor::default()),
                &config(),
                &Request::default(),
            )
            .unwrap();
        assert!(!result.report.success);
        assert!(!result.dag.is_success());
        assert_eq!(result.report.failed, 1);
        assert_eq!(
            result.report.cleanup_errors,
            ["cannot remove invocation lease"]
        );
        assert_eq!(
            result.dag.failed[&"integration:cleanup".into()],
            "cannot remove invocation lease"
        );
        assert_eq!(result.dag.completed.len(), 2);
        assert_eq!(result.report.phases.len(), 2);
    }
    #[test]
    fn shared_settled_phase_executes_after_failed_target_phase() {
        struct SettledAdapter;
        static GRAMMAR: Grammar = Grammar {
            name: "settled",
            about: "barrier",
            targets: false,
            parameters: &[],
            phases: &[Phase::new("build"), Phase::new("barrier").after_settled()],
            failure_policy: FailurePolicy::ContinueIndependent,
        };
        impl Integration for SettledAdapter {
            fn grammar(&self) -> &'static Grammar {
                &GRAMMAR
            }
            fn validate_config(&self, _: &FleetConfig) -> Result<(), IntegrationError> {
                Ok(())
            }
            fn task(
                &self,
                _: &FleetConfig,
                _: &Plan,
                phase: &Phase,
                _: &str,
            ) -> Result<PhaseTask, IntegrationError> {
                let fail = phase.name == "build";
                Ok(PhaseTask::new(
                    phase.name,
                    FnTask::new("phase", move |ctx| {
                        let exec: Arc<dyn Executor> = ctx.get_state("executor").unwrap();
                        exec.exec(&CommandSpec::new("true")).unwrap();
                        if fail {
                            TaskOutcome::Failed("build failed".into())
                        } else {
                            TaskOutcome::Success
                        }
                    }),
                ))
            }
        }
        let audit = Arc::new(AuditExecutor::default());
        let result = SettledAdapter
            .run_detailed(audit.clone(), &config(), &Request::default())
            .unwrap();
        assert!(!result.report.success);
        assert!(result.dag.completed.contains(&"barrier".into()));
        assert!(result.dag.failed.contains_key(&"build".into()));
        assert_eq!(audit.calls.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn required_target_success_survives_a_shared_settlement_barrier() {
        struct TargetAdapter;
        static GRAMMAR: Grammar = Grammar {
            name: "targets",
            about: "barrier",
            targets: true,
            parameters: &[],
            phases: &[
                Phase::new("build").behavior(PhaseKind::Build, PhaseScope::Target, &[]),
                Phase::new("barrier").after_settled(),
                Phase::new("copy")
                    .behavior(PhaseKind::Copy, PhaseScope::Target, &[])
                    .requiring(&["build"]),
            ],
            failure_policy: FailurePolicy::ContinueIndependent,
        };
        impl Integration for TargetAdapter {
            fn grammar(&self) -> &'static Grammar {
                &GRAMMAR
            }
            fn validate_config(&self, _: &FleetConfig) -> Result<(), IntegrationError> {
                Ok(())
            }
            fn task(
                &self,
                _: &FleetConfig,
                _: &Plan,
                phase: &Phase,
                resource: &str,
            ) -> Result<PhaseTask, IntegrationError> {
                let phase_name = phase.name;
                let host = resource.to_string();
                Ok(PhaseTask::new(
                    format!("{}:{resource}", phase.name),
                    FnTask::new("phase", move |ctx| match (phase_name, host.as_str()) {
                        ("build", "bad") => TaskOutcome::Failed("build failure".into()),
                        ("build", "good") => {
                            ctx.set_state("good-built", true);
                            TaskOutcome::Success
                        }
                        ("barrier", _) => {
                            if ctx.get_state::<bool>("good-built") == Some(true) {
                                TaskOutcome::Success
                            } else {
                                TaskOutcome::Failed("barrier ran before healthy predecessor".into())
                            }
                        }
                        ("copy", "bad") => TaskOutcome::Failed(
                            "copy ran without its required successful build".into(),
                        ),
                        _ => TaskOutcome::Success,
                    }),
                ))
            }
        }
        let mut fleet = config();
        for name in ["good", "bad"] {
            fleet.nodes.insert(
                name.into(),
                crate::fleet::DeploymentNode {
                    name: name.into(),
                    target_host: name.into(),
                    target_user: "root".into(),
                    target_port: None,
                    system: "x86_64-linux".into(),
                    profile_type: crate::fleet::ProfileType::Nixos,
                    build_on_target: false,
                    tags: vec![],
                    drv_path: None,
                    toplevel: None,
                },
            );
        }
        let result = TargetAdapter
            .run_detailed(
                Arc::new(AuditExecutor::default()),
                &fleet,
                &Request::default(),
            )
            .unwrap();
        assert_eq!(result.dag.failed.len(), 1);
        assert!(result.dag.failed.contains_key(&"build:bad".into()));
        assert!(result.dag.completed.contains(&"barrier:".into()));
        assert!(result.dag.completed.contains(&"copy:good".into()));
        assert!(result.dag.cancelled.contains(&"copy:bad".into()));
    }
    #[test]
    fn structured_events_require_diagnostic_capability() {
        for (allowed, native) in [(false, false), (false, true), (true, false), (true, true)] {
            let audit = Arc::new(AuditExecutor::default());
            let context = DagContext::new();
            context.set_state("executor", audit.clone() as Arc<dyn Executor>);
            let mut task = PhaseTask::new(
                "event",
                FnTask::new("emit event", move |ctx| {
                    let exec: Arc<dyn Executor> = ctx.get_state("executor").unwrap();
                    let payload = serde_json::json!({"completed": 1});
                    if native {
                        exec.event_with_native("progress", &payload, &1u64);
                    } else {
                        exec.event("progress", &payload);
                    }
                    TaskOutcome::Success
                }),
            );
            task.allowed = if allowed {
                &[EffectKind::Diagnostic]
            } else {
                &[]
            };
            assert_eq!(task.execute(&context).is_success(), allowed);
            assert_eq!(audit.calls.load(Ordering::SeqCst), usize::from(allowed));
            assert_eq!(
                task.observations.lock().unwrap()[0].effects,
                if allowed {
                    vec![EffectKind::Diagnostic]
                } else {
                    vec![]
                }
            );
        }
    }
}
