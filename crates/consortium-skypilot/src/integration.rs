//! Adapter for the shared integration grammar.

use consortium_integration::fleet::FleetConfig;
use consortium_integration::grammar::{
    Condition, EffectKind, FailurePolicy, Grammar, Integration, IntegrationError, Parameter,
    ParameterKind, Phase, PhaseKind, PhaseScope, PhaseTask, Plan,
};

/// SkyPilot cluster task launch through the uniform integration contract.
pub struct SkypilotIntegration;

static GRAMMAR: Grammar = Grammar {
    name: "skypilot",
    about: "SkyPilot cluster task launch",
    targets: false,
    failure_policy: FailurePolicy::Stop,
    parameters: &[
        Parameter::required("name", "Cluster and Nix environment name"),
        Parameter::required("task", "SkyPilot task YAML content"),
        Parameter::defaulted(
            "teardown",
            "Tear down the cluster after launch",
            ParameterKind::Boolean,
            "true",
        ),
    ],
    phases: &[
        Phase::new("build").behavior(PhaseKind::Build, PhaseScope::Shared, &[EffectKind::Process]),
        Phase::new("launch").behavior(
            PhaseKind::Acquire,
            PhaseScope::Shared,
            &[EffectKind::Process, EffectKind::TemporaryFile],
        ),
        Phase::when("teardown", Condition::Equals("teardown", "true")).behavior(
            PhaseKind::Release,
            PhaseScope::Shared,
            &[EffectKind::Process],
        ),
    ],
};

impl Integration for SkypilotIntegration {
    fn cleanup(&self, context: &consortium::dag::DagContext) -> Result<(), IntegrationError> {
        if let Some(name) = context.get_state::<String>("sky-cluster-name") {
            crate::tasks::cleanup_task_yaml(context, &name).map_err(|error| {
                IntegrationError(format!("temporary file cleanup failed: {error}"))
            })?;
        }
        Ok(())
    }

    fn configure(
        &self,
        _config: &FleetConfig,
        plan: &Plan,
        context: &consortium::dag::DagContext,
    ) -> Result<(), IntegrationError> {
        context.set_state("sky-cluster-name", plan.value("name").to_owned());
        Ok(())
    }

    fn grammar(&self) -> &'static Grammar {
        &GRAMMAR
    }

    fn validate_config(&self, config: &FleetConfig) -> Result<(), IntegrationError> {
        if config.skypilot_config.is_none() {
            return Err(IntegrationError(
                "skypilot: missing fleet configuration".into(),
            ));
        }
        Ok(())
    }

    fn task(
        &self,
        config: &FleetConfig,
        plan: &Plan,
        phase: &Phase,
        _resource: &str,
    ) -> Result<PhaseTask, IntegrationError> {
        let c = config
            .skypilot_config
            .as_ref()
            .ok_or_else(|| IntegrationError("skypilot: missing configuration".into()))?;
        let name = plan.value("name");
        Ok(match phase.name {
            "build" => PhaseTask::new(
                format!("build-sky-env:{name}"),
                crate::tasks::NixBuildSkyEnvTask::new(name, &config.flake_uri),
            ),
            "launch" => PhaseTask::new(
                format!("sky-launch:{name}"),
                crate::tasks::SkyLaunchTask {
                    cluster_name: name.into(),
                    task_yaml: plan.value("task").into(),
                    cloud: Some(c.cloud.clone()),
                    region: c.region.clone(),
                },
            ),
            "teardown" => PhaseTask::new(
                format!("sky-down:{name}"),
                crate::tasks::SkyDownTask {
                    cluster_name: name.into(),
                },
            ),
            _ => return Err(IntegrationError("unsupported skypilot phase".into())),
        })
    }
}
