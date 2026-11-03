//! Adapter for the shared integration grammar.

use consortium::dag::DagContext;
use consortium_integration::fleet::FleetConfig;
use consortium_integration::grammar::{
    duration_text, Condition, EffectKind, FailurePolicy, Grammar, Integration, IntegrationError,
    Parameter, ParameterKind, Phase, PhaseKind, PhaseScope, PhaseTask, Plan, Request,
};

/// NixOS and nix-darwin system deployment through the uniform integration contract.
pub struct NixIntegration;

static GRAMMAR: Grammar = Grammar {
    name: "nix",
    about: "NixOS and nix-darwin system deployment",
    targets: true,
    failure_policy: FailurePolicy::ContinueIndependent,
    parameters: &[
        Parameter::defaulted(
            "action",
            "System activation action",
            ParameterKind::Choice(&["switch", "boot", "test", "dry-activate", "build"]),
            "switch",
        ),
        Parameter::defaulted(
            "fanout",
            "Maximum parallel operations",
            ParameterKind::PositiveInteger,
            "4",
        ),
        Parameter::defaulted(
            "builders",
            "Use fleet distributed builders",
            ParameterKind::Boolean,
            "false",
        ),
        Parameter::optional("local", "Comma-separated selected hosts activated locally"),
        Parameter::defaulted(
            "cascade",
            "Distribute closures through the fleet",
            ParameterKind::Boolean,
            "false",
        ),
        Parameter::defaulted(
            "cascade-fanout",
            "Maximum children per cascade source",
            ParameterKind::PositiveInteger,
            "2",
        ),
        Parameter::optional("seed", "Cascade seed address"),
        Parameter::optional("activate-timeout", "Activation timeout in seconds")
            .typed(ParameterKind::PositiveDuration),
        Parameter::optional(
            "nixos-nix-args",
            "Extra NixOS eval/build arguments (words or JSON array)",
        ),
        Parameter::optional(
            "darwin-nix-args",
            "Extra nix-darwin eval/build arguments (words or JSON array)",
        ),
    ],
    phases: &[
        Phase::when("builders", Condition::Equals("builders", "true")).behavior(
            PhaseKind::Preflight,
            PhaseScope::Shared,
            &[
                EffectKind::Process,
                EffectKind::RemoteProcess,
                EffectKind::TemporaryFile,
                EffectKind::Clock,
                EffectKind::Diagnostic,
            ],
        ),
        Phase::new("eval").behavior(
            PhaseKind::Evaluate,
            PhaseScope::Target,
            &[EffectKind::Process],
        ),
        Phase::new("build").behavior(PhaseKind::Build, PhaseScope::Target, &[EffectKind::Process]),
        Phase::when("cascade", Condition::Equals("cascade", "true"))
            .behavior(
                PhaseKind::Copy,
                PhaseScope::Shared,
                &[
                    EffectKind::Process,
                    EffectKind::RemoteProcess,
                    EffectKind::Clock,
                    EffectKind::Diagnostic,
                ],
            )
            .after_settled(),
        Phase::when("copy", Condition::NotEquals("action", "build"))
            .behavior(
                PhaseKind::Copy,
                PhaseScope::Target,
                &[
                    EffectKind::Process,
                    EffectKind::RemoteProcess,
                    EffectKind::Clock,
                    EffectKind::Diagnostic,
                ],
            )
            .requiring(&["build"]),
        Phase::when("activate", Condition::NotEquals("action", "build")).behavior(
            PhaseKind::Execute,
            PhaseScope::Target,
            &[
                EffectKind::Process,
                EffectKind::RemoteProcess,
                EffectKind::Clock,
                EffectKind::Diagnostic,
            ],
        ),
    ],
};

impl Integration for NixIntegration {
    fn grammar(&self) -> &'static Grammar {
        &GRAMMAR
    }

    fn validate_config(&self, _config: &FleetConfig) -> Result<(), IntegrationError> {
        Ok(())
    }

    fn validate_plan(&self, config: &FleetConfig, plan: &Plan) -> Result<(), IntegrationError> {
        for target in &plan.targets {
            if config.nodes[target].target_port == Some(0) {
                return Err(IntegrationError(format!("invalid SSH port for {target}")));
            }
        }
        if plan.flag("cascade") {
            if plan.value("action") == "build" || plan.value("seed").trim().is_empty() {
                return Err(IntegrationError(
                    "cascade requires a seed and a deployment action".into(),
                ));
            }
            u32::try_from(plan.positive_integer("cascade-fanout")?)
                .map_err(|_| IntegrationError("cascade-fanout exceeds u32".into()))?;
        }
        for key in ["nixos-nix-args", "darwin-nix-args"] {
            argument_words(plan.value(key))?;
        }
        if let Some(local) = plan.parameters.get("local") {
            for host in local_words(local)? {
                if !plan.targets.iter().any(|target| target == &host) {
                    return Err(IntegrationError(format!(
                        "nix: local host {host:?} is not a selected target"
                    )));
                }
            }
        }
        Ok(())
    }

    fn configure(
        &self,
        _config: &FleetConfig,
        plan: &Plan,
        context: &DagContext,
    ) -> Result<(), IntegrationError> {
        let action: crate::config::DeployAction = plan
            .value("action")
            .parse()
            .map_err(|e: crate::config::ConfigError| IntegrationError(e.to_string()))?;
        let mut options = crate::DeployOptions::new().nix_args(crate::NixArgs {
            nixos: argument_words(plan.value("nixos-nix-args"))?,
            darwin: argument_words(plan.value("darwin-nix-args"))?,
        });
        if let Some(local) = plan.parameters.get("local") {
            options = options.local_hosts(local_words(local)?);
        }
        if plan.parameters.contains_key("activate-timeout") {
            options = options.activate_timeout(plan.duration("activate-timeout")?);
        }
        context.set_state("integration_plan", plan.clone());
        context.set_state(
            "machines_file_lease",
            std::sync::Arc::new(std::sync::Mutex::new(
                None::<consortium_integration::TemporaryFile>,
            )),
        );
        context.set_state("action", action);
        context.set_state("deploy_options", options);
        Ok(())
    }
    fn cleanup(&self, context: &DagContext) -> Result<(), IntegrationError> {
        let lease: std::sync::Arc<std::sync::Mutex<Option<consortium_integration::TemporaryFile>>> =
            context
                .get_state("machines_file_lease")
                .ok_or_else(|| IntegrationError("missing machines file lease".into()))?;
        if let Some(file) = lease.lock().unwrap().take() {
            file.close()
                .map_err(|e| IntegrationError(format!("machines file cleanup failed: {e}")))?;
        }
        Ok(())
    }
    fn task(
        &self,
        _config: &FleetConfig,
        plan: &Plan,
        phase: &Phase,
        resource: &str,
    ) -> Result<PhaseTask, IntegrationError> {
        Ok(match phase.name {
            "builders" => PhaseTask::new(
                "builders",
                consortium::dag::FnTask::new("prepare distributed builders", |context| {
                    let config: FleetConfig = context.get_state("fleet_config").unwrap();
                    let exec: std::sync::Arc<dyn consortium_integration::Executor> =
                        context.get_state("executor").unwrap();
                    match crate::prepare_builders(&*exec, &config, true) {
                        Err(error) => {
                            return consortium::dag::TaskOutcome::Failed(error.to_string())
                        }
                        Ok(None) => {}
                        Ok(Some(path)) => {
                            context.set_state(
                                "machines_file",
                                path.path().to_string_lossy().into_owned(),
                            );
                            let lease: std::sync::Arc<
                                std::sync::Mutex<Option<consortium_integration::TemporaryFile>>,
                            > = context.get_state("machines_file_lease").unwrap();
                            *lease.lock().unwrap() = Some(path);
                        }
                    }
                    consortium::dag::TaskOutcome::Success
                }),
            ),
            "eval" => PhaseTask::new(
                format!("eval:{resource}"),
                crate::tasks::NixEvalTask::new(resource),
            ),
            "build" => PhaseTask::new(
                format!("build:{resource}"),
                crate::tasks::NixBuildTask::new(resource),
            ),
            "cascade" => PhaseTask::new("cascade", CascadeTask),
            "copy" if plan.flag("cascade") => PhaseTask::new(
                format!("copy:{resource}"),
                CascadeCopyOutcome {
                    host: resource.into(),
                },
            ),
            "copy" => PhaseTask::new(
                format!("copy:{resource}"),
                crate::tasks::NixCopyTask::new(resource),
            ),
            "activate" => PhaseTask::new(
                format!("activate:{resource}"),
                crate::tasks::NixActivateTask::new(resource),
            ),
            _ => return Err(IntegrationError("unsupported nix phase".into())),
        })
    }
}

fn argument_words(value: &str) -> Result<Vec<String>, IntegrationError> {
    if value.trim_start().starts_with('[') {
        serde_json::from_str(value)
            .map_err(|error| IntegrationError(format!("invalid argument array: {error}")))
    } else {
        Ok(value.split_whitespace().map(str::to_owned).collect())
    }
}

fn local_words(value: &str) -> Result<Vec<String>, IntegrationError> {
    if value.trim_start().starts_with('[') {
        serde_json::from_str(value)
            .map_err(|error| IntegrationError(format!("invalid host array: {error}")))
    } else {
        Ok(value.split(',').map(str::to_owned).collect())
    }
}

pub(crate) fn deployment_request(
    targets: &[String],
    action: crate::DeployAction,
    fanout: usize,
    builders: bool,
    options: &crate::DeployOptions,
) -> crate::Result<Request> {
    if targets.is_empty() {
        return Err(crate::NixError::General("no target nodes selected".into()));
    }
    let mut request = Request::default()
        .with("action", action.to_string())
        .with("fanout", fanout.to_string())
        .with("builders", builders.to_string());
    request.targets = targets.to_vec();
    for (key, words) in [
        ("nixos-nix-args", &options.nix_args.nixos),
        ("darwin-nix-args", &options.nix_args.darwin),
        ("local", &options.local_hosts),
    ] {
        if !words.is_empty() {
            request = request.with(
                key,
                serde_json::to_string(words)
                    .map_err(|error| crate::NixError::General(error.to_string()))?,
            );
        }
    }
    if let Some(timeout) = options.activate_timeout {
        request = request.with("activate-timeout", duration_text(timeout));
    }
    Ok(request)
}

pub(crate) fn run_deployment(
    exec: std::sync::Arc<dyn consortium_integration::Executor>,
    config: &FleetConfig,
    request: &Request,
    action: crate::DeployAction,
) -> crate::Result<crate::DeployReport> {
    crate::validate_targets(config, &request.targets)?;
    let plan = NixIntegration
        .plan(config, request)
        .map_err(|error| crate::NixError::General(error.to_string()))?;
    let execution = NixIntegration
        .execute_detailed(exec, config, &plan)
        .map_err(|error| crate::NixError::General(error.to_string()))?;
    let mut report = crate::DeployReport::from_dag_report(&execution.dag, &plan.targets, action);
    for (id, error) in &execution.dag.failed {
        if !["eval:", "build:", "copy:", "activate:"]
            .iter()
            .any(|prefix| id.0.starts_with(prefix))
        {
            report.build_failures.push((id.0.clone(), error.clone()));
        }
    }
    Ok(report)
}

struct CascadeTask;
impl consortium::dag::DagTask for CascadeTask {
    fn execute(&self, context: &DagContext) -> consortium::dag::TaskOutcome {
        use consortium::dag::{TaskId, TaskOutcome};
        use std::sync::Arc;
        let config: FleetConfig = context.get_state("fleet_config").unwrap();
        let plan: Plan = context.get_state("integration_plan").unwrap();
        let options: crate::DeployOptions = context.get_state("deploy_options").unwrap();
        let executor: Arc<dyn consortium_integration::Executor> =
            context.get_state("executor").unwrap();
        let mut targets = Vec::new();
        for host in &plan.targets {
            let Some(toplevel) = context.get_output::<String>(&TaskId(format!("build:{host}")))
            else {
                continue;
            };
            let node = &config.nodes[host];
            if options.is_local(host) || node.build_on_target {
                context.set_output(TaskId(format!("copy:{host}")), toplevel);
            } else {
                targets.push(crate::cascade_integration::CascadeCopyTarget {
                    host_name: host.clone(),
                    ssh_addr: crate::endpoint::ssh_address(node),
                    toplevel_path: toplevel,
                });
            }
        }
        let sink = ExecutorEvents {
            executor: executor.clone(),
        };
        let config =
            crate::cascade_integration::CascadeCopyConfig::new(plan.value("seed"), targets.clone())
                .fanout(plan.positive_integer("cascade-fanout").unwrap() as u32)
                .events(&sink);
        let result = crate::cascade_integration::cascade_copy_grouped(&executor, config);
        for target in &targets {
            if result.copied.contains(&target.host_name) {
                context.set_output(
                    TaskId(format!("copy:{}", target.host_name)),
                    target.toplevel_path.clone(),
                );
            }
        }
        context.set_state("cascade_failures", result.failed);
        TaskOutcome::Success
    }
    fn describe(&self) -> String {
        "distribute built closures through cascade".into()
    }
}

struct CascadeCopyOutcome {
    host: String,
}
impl consortium::dag::DagTask for CascadeCopyOutcome {
    fn execute(&self, context: &DagContext) -> consortium::dag::TaskOutcome {
        if context.has_output(&consortium::dag::TaskId(format!("copy:{}", self.host))) {
            return consortium::dag::TaskOutcome::Success;
        }
        let failures = context
            .get_state::<std::collections::HashMap<String, String>>("cascade_failures")
            .unwrap_or_default();
        consortium::dag::TaskOutcome::Failed(
            failures
                .get(&self.host)
                .cloned()
                .unwrap_or_else(|| "cascade produced no copy output".into()),
        )
    }
    fn describe(&self) -> String {
        format!("check cascade delivery to {}", self.host)
    }
}

struct ExecutorEvents {
    executor: std::sync::Arc<dyn consortium_integration::Executor>,
}
impl crate::cascade_events::EventSink for ExecutorEvents {
    fn emit(&self, event: &crate::cascade_events::CascadeEvent) {
        self.executor.event_with_native(
            "nix.cascade",
            &serde_json::to_value(event).expect("cascade event serialization"),
            event,
        );
    }
}

struct EventForwarder {
    inner: std::sync::Arc<dyn consortium_integration::Executor>,
    events: std::sync::mpsc::Sender<crate::cascade_events::CascadeEvent>,
}
impl consortium_integration::Executor for EventForwarder {
    fn exec(
        &self,
        spec: &consortium_integration::CommandSpec,
    ) -> Result<consortium_integration::ExecOutput, consortium_integration::ExecError> {
        self.inner.exec(spec)
    }
    fn exec_timeout(
        &self,
        spec: &consortium_integration::CommandSpec,
        limit: std::time::Duration,
    ) -> Result<consortium_integration::ExecOutput, consortium_integration::ExecError> {
        self.inner.exec_timeout(spec, limit)
    }
    fn temporary_file(
        &self,
        prefix: &str,
        contents: &[u8],
    ) -> std::io::Result<consortium_integration::TemporaryFile> {
        self.inner.temporary_file(prefix, contents)
    }
    fn monotonic_now(&self) -> std::time::Duration {
        self.inner.monotonic_now()
    }
    fn wall_now(&self) -> std::time::SystemTime {
        self.inner.wall_now()
    }
    fn sleep(&self, duration: std::time::Duration) -> std::io::Result<()> {
        self.inner.sleep(duration)
    }
    fn diagnostic(&self, message: &str) {
        self.inner.diagnostic(message);
    }
    fn event(&self, topic: &str, payload: &serde_json::Value) {
        self.inner.event(topic, payload);
    }
    fn event_with_native(
        &self,
        topic: &str,
        payload: &serde_json::Value,
        native: &(dyn std::any::Any + Send + Sync),
    ) {
        self.inner.event_with_native(topic, payload, native);
        if topic == "nix.cascade" {
            if let Some(event) = native.downcast_ref::<crate::cascade_events::CascadeEvent>() {
                let _ = self.events.send(event.clone());
            }
        }
    }
}

pub(crate) fn run_deployment_with_events(
    exec: std::sync::Arc<dyn consortium_integration::Executor>,
    config: &FleetConfig,
    request: &Request,
    action: crate::DeployAction,
    sink: Option<&dyn crate::cascade_events::EventSink>,
) -> crate::Result<crate::DeployReport> {
    let Some(sink) = sink else {
        return run_deployment(exec, config, request, action);
    };
    std::thread::scope(|scope| {
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = scope.spawn(move || {
            run_deployment(
                std::sync::Arc::new(EventForwarder {
                    inner: exec,
                    events: sender,
                }),
                config,
                request,
                action,
            )
        });
        for event in receiver {
            sink.emit(&event);
        }
        worker
            .join()
            .map_err(|_| crate::NixError::General("deployment execution panicked".into()))?
    })
}

#[cfg(test)]
mod typed_workflow_tests {
    use super::*;
    use consortium_integration::{Effect, ExecOutput, ScriptedExecutor};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn config() -> FleetConfig {
        serde_json::from_str(include_str!("../../../examples/inventories/fleet.json")).unwrap()
    }
    fn executor() -> ScriptedExecutor {
        ScriptedExecutor::new()
            .on("nix eval", ExecOutput::ok("/nix/store/test-system\n"))
            .on("nix build", ExecOutput::ok("/nix/store/test-system\n"))
            .on("", ExecOutput::ok(""))
    }

    #[test]
    fn typed_options_preserve_argument_boundaries_and_nanosecond_timeout() {
        for profile in [crate::ProfileType::Nixos, crate::ProfileType::NixDarwin] {
            let mut config = config();
            for node in config.nodes.values_mut() {
                node.profile_type = profile.clone();
            }
            let exec = Arc::new(executor());
            let words = vec![
                "--option".into(),
                "extra-substituters".into(),
                "https://one.invalid https://two.invalid".into(),
            ];
            let options = crate::DeployOptions::new()
                .activate_timeout(Duration::from_nanos(17))
                .nix_args(crate::NixArgs {
                    nixos: words.clone(),
                    darwin: words.clone(),
                });
            let report = crate::deploy_with_options(
                exec.clone(),
                &config,
                &["node01".into()],
                crate::DeployAction::Switch,
                2,
                false,
                &options,
            )
            .unwrap();
            assert!(report.is_success());
            let effects = exec.effects();
            for effect in &effects {
                if let Effect::Command { spec, timeout } = effect {
                    if spec.program == "nix"
                        && matches!(
                            spec.args.first().map(String::as_str),
                            Some("eval" | "build")
                        )
                    {
                        assert_eq!(&spec.args[spec.args.len() - words.len()..], words);
                    }
                    if spec.render().contains("activate")
                        || spec.render().contains("switch-to-configuration")
                    {
                        assert_eq!(*timeout, Some(Duration::from_nanos(17)));
                    }
                }
            }
        }
    }

    struct Sink(Mutex<Vec<crate::cascade_events::CascadeEvent>>);
    impl crate::cascade_events::EventSink for Sink {
        fn emit(&self, event: &crate::cascade_events::CascadeEvent) {
            self.0.lock().unwrap().push(event.clone());
        }
    }

    #[test]
    fn cascade_and_direct_typed_apis_share_phase_failure_guards() {
        for bad_host in [None, Some("node01")] {
            let config = config();
            let scripted = match bad_host {
                Some(host) => {
                    ScriptedExecutor::new().rule(consortium_integration::Rule::containing_all(
                        ["nix build", &format!("Configurations.{host}.")],
                        ExecOutput::new(1, "", "bad build"),
                    ))
                }
                None => ScriptedExecutor::new(),
            };
            let exec = Arc::new(
                scripted
                    .on("nix eval", ExecOutput::ok("/nix/store/test-system\n"))
                    .on("nix build", ExecOutput::ok("/nix/store/test-system\n"))
                    .on("", ExecOutput::ok("")),
            );
            let sink = Sink(Mutex::new(Vec::new()));
            let report = crate::deploy_with_cascade(
                exec.clone(),
                &config,
                &["node01".into(), "node02".into()],
                crate::DeployAction::Switch,
                2,
                false,
                2,
                "root@seed",
                Some(&sink),
            )
            .unwrap();
            assert_eq!(report.is_success(), bad_host.is_none(), "{report:?}");
            assert!(report.activated.contains(&"node02".into()), "{report:?}");
            if let Some(host) = bad_host {
                assert!(!report.activated.contains(&host.into()));
                assert_eq!(report.build_failures.len(), 1);
                assert!(report.copy_failures.is_empty());
                assert!(report.activation_failures.is_empty());
            }
            let events = sink.0.lock().unwrap();
            assert!(events
                .iter()
                .any(|e| matches!(e, crate::cascade_events::CascadeEvent::Started { .. })));
            assert!(events
                .iter()
                .any(|e| matches!(e, crate::cascade_events::CascadeEvent::Finished { .. })));
            assert!(exec
                .effects()
                .iter()
                .any(|e| matches!(e, Effect::Event {topic, ..} if topic == "nix.cascade")));
        }
    }

    #[test]
    fn typed_empty_selection_and_invalid_cascade_fail_before_effects() {
        let config = config();
        let exec = Arc::new(executor());
        assert!(crate::deploy(
            exec.clone(),
            &config,
            &[],
            crate::DeployAction::Switch,
            2,
            false
        )
        .is_err());
        assert!(exec.effects().is_empty());
        let request = Request::default().with("cascade", "true");
        assert!(NixIntegration.run(exec.clone(), &config, &request).is_err());
        assert!(exec.effects().is_empty());
    }

    #[test]
    fn typed_report_counts_each_selected_host_once() {
        let config = config();
        for cascade in [false, true] {
            let exec = Arc::new(executor());
            let targets = vec!["node01".into(), "node01".into()];
            let report = if cascade {
                crate::deploy_with_cascade(
                    exec,
                    &config,
                    &targets,
                    crate::DeployAction::Switch,
                    2,
                    false,
                    2,
                    "root@seed",
                    None,
                )
            } else {
                crate::deploy(
                    exec,
                    &config,
                    &targets,
                    crate::DeployAction::Switch,
                    2,
                    false,
                )
            }
            .unwrap();
            assert_eq!(report.built, ["node01"]);
            assert_eq!(report.copied, ["node01"]);
            assert_eq!(report.activated, ["node01"]);
            assert_eq!(report.success_count(), 1);
        }
    }
    fn assert_same_native_error(
        expected: &crate::cascade::CascadeError,
        actual: &crate::cascade::CascadeError,
    ) {
        use crate::cascade::CascadeError;
        match (expected, actual) {
            (
                CascadeError::Copy {
                    node: left,
                    stderr: left_message,
                },
                CascadeError::Copy {
                    node: right,
                    stderr: right_message,
                },
            ) => {
                assert_eq!(left, right);
                assert_eq!(left_message, right_message);
            }
            (
                CascadeError::SshHandshake {
                    node: left,
                    parent: left_parent,
                },
                CascadeError::SshHandshake {
                    node: right,
                    parent: right_parent,
                },
            ) => {
                assert_eq!(left, right);
                assert_eq!(left_parent, right_parent);
            }
            (
                CascadeError::Activation {
                    node: left,
                    stage: left_stage,
                },
                CascadeError::Activation {
                    node: right,
                    stage: right_stage,
                },
            ) => {
                assert_eq!(left, right);
                assert_eq!(left_stage, right_stage);
                assert!(std::ptr::eq(*left_stage, *right_stage));
            }
            (
                CascadeError::Partitioned {
                    src: left_src,
                    tgt: left_tgt,
                },
                CascadeError::Partitioned {
                    src: right_src,
                    tgt: right_tgt,
                },
            ) => {
                assert_eq!(left_src, right_src);
                assert_eq!(left_tgt, right_tgt);
            }
            (
                CascadeError::SubtreeAggregate {
                    node: left,
                    errors: left_errors,
                },
                CascadeError::SubtreeAggregate {
                    node: right,
                    errors: right_errors,
                },
            ) => {
                assert_eq!(left, right);
                assert_eq!(left_errors.len(), right_errors.len());
                for (expected, actual) in left_errors.iter().zip(right_errors) {
                    assert_same_native_error(expected, actual);
                }
            }
            _ => panic!("native error variant changed: {expected:?} -> {actual:?}"),
        }
    }

    #[test]
    fn live_event_bridge_preserves_all_native_error_variants_and_replay_schema() {
        use crate::cascade::{CascadeError, NodeId};
        use crate::cascade_events::{CascadeEvent, EventSink};
        let exec = Arc::new(ScriptedExecutor::new());
        let (sender, receiver) = std::sync::mpsc::channel();
        let bridge = ExecutorEvents {
            executor: Arc::new(EventForwarder {
                inner: exec.clone(),
                events: sender,
            }),
        };
        let cases = vec![
            CascadeError::Copy {
                node: NodeId(71),
                stderr: "copy failed".into(),
            },
            CascadeError::SshHandshake {
                node: NodeId(72),
                parent: NodeId(42),
            },
            CascadeError::Activation {
                node: NodeId(73),
                stage: "switch",
            },
            CascadeError::Partitioned {
                src: NodeId(74),
                tgt: NodeId(75),
            },
            CascadeError::SubtreeAggregate {
                node: NodeId(76),
                errors: vec![
                    CascadeError::Activation {
                        node: NodeId(77),
                        stage: "profile",
                    },
                    CascadeError::Partitioned {
                        src: NodeId(78),
                        tgt: NodeId(79),
                    },
                ],
            },
        ];
        for error in cases {
            let event = CascadeEvent::EdgeFailed {
                round: 7,
                src: NodeId(3),
                tgt: NodeId(9),
                error: error.clone(),
            };
            bridge.emit(&event);
            let actual = receiver.try_recv().unwrap();
            let CascadeEvent::EdgeFailed {
                round,
                src,
                tgt,
                error: actual_error,
            } = &actual
            else {
                panic!("wrong event variant")
            };
            assert_eq!((*round, *src, *tgt), (7, NodeId(3), NodeId(9)));
            assert_same_native_error(&error, actual_error);
            assert_eq!(error.affected_nodes(), actual_error.affected_nodes());
            let payload = serde_json::to_value(&event).unwrap();
            assert!(payload["error"].is_string());
            assert_eq!(serde_json::to_value(&actual).unwrap(), payload);
            assert!(
                matches!(exec.effects().last(), Some(Effect::Event {topic, payload: recorded}) if topic == "nix.cascade" && recorded == &payload)
            );
        }
        assert_eq!(exec.effects().len(), 5);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn borrowed_live_sink_receives_the_original_ssh_failure_type() {
        use crate::cascade::{CascadeError, NodeId};
        use crate::cascade_events::CascadeEvent;
        let exec = Arc::new(
            ScriptedExecutor::new()
                .on(
                    "nix copy",
                    ExecOutput::new(255, "", "Permission denied (publickey)."),
                )
                .on("nix eval", ExecOutput::ok("/nix/store/test-system"))
                .on("nix build", ExecOutput::ok("/nix/store/test-system")),
        );
        let sink = Sink(Mutex::new(Vec::new()));
        let report = crate::deploy_with_cascade(
            exec,
            &config(),
            &["node01".into()],
            crate::DeployAction::Switch,
            2,
            false,
            2,
            "root@seed",
            Some(&sink),
        )
        .unwrap();
        assert_eq!(report.copy_failures.len(), 1);
        let events = sink.0.lock().unwrap();
        assert!(events.iter().any(|event| matches!(event, CascadeEvent::EdgeFailed {error: CascadeError::SshHandshake {node, parent}, ..} if *node != NodeId(u32::MAX) && *parent == NodeId(0))));
        assert!(!events.iter().any(|event| matches!(event, CascadeEvent::EdgeFailed {error: CascadeError::Copy {node, ..}, ..} if *node == NodeId(u32::MAX))));
    }
}
