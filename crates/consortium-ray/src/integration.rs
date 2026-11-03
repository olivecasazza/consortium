//! Adapter for the shared integration grammar.

use consortium_integration::fleet::FleetConfig;
use consortium_integration::grammar::{
    Condition, EffectKind, FailurePolicy, Grammar, Integration, IntegrationError, Parameter,
    ParameterKind, Phase, PhaseKind, PhaseScope, PhaseTask, Plan,
};

/// Ray job submission through the uniform integration contract.
pub struct RayIntegration;

static GRAMMAR: Grammar = Grammar {
    name: "ray",
    about: "Ray job submission",
    targets: false,
    failure_policy: FailurePolicy::Stop,
    parameters: &[
        Parameter::required("name", "Job and Nix environment name"),
        Parameter::required("entrypoint", "Job entrypoint command"),
        Parameter::defaulted(
            "wait",
            "Wait for job completion",
            ParameterKind::Boolean,
            "true",
        ),
        Parameter::defaulted(
            "poll-seconds",
            "Status poll interval in seconds",
            ParameterKind::PositiveDuration,
            "10",
        ),
        Parameter::optional("timeout", "Job wait timeout in seconds")
            .typed(ParameterKind::PositiveDuration),
    ],
    phases: &[
        Phase::new("build").behavior(PhaseKind::Build, PhaseScope::Shared, &[EffectKind::Process]),
        Phase::new("submit").behavior(
            PhaseKind::Submit,
            PhaseScope::Shared,
            &[EffectKind::Process],
        ),
        Phase::when("wait", Condition::Equals("wait", "true")).behavior(
            PhaseKind::Wait,
            PhaseScope::Shared,
            &[EffectKind::Process, EffectKind::Clock, EffectKind::Sleep],
        ),
    ],
};

impl Integration for RayIntegration {
    fn grammar(&self) -> &'static Grammar {
        &GRAMMAR
    }

    fn validate_config(&self, config: &FleetConfig) -> Result<(), IntegrationError> {
        if config.ray_config.is_none() {
            return Err(IntegrationError("ray: missing fleet configuration".into()));
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
            .ray_config
            .as_ref()
            .ok_or_else(|| IntegrationError("ray: missing configuration".into()))?;
        let name = plan.value("name");
        Ok(match phase.name {
            "build" => PhaseTask::new(
                format!("build-ray-env:{name}"),
                crate::tasks::NixBuildRayEnvTask::new(name, &config.flake_uri),
            ),
            "submit" => PhaseTask::new(
                format!("ray-submit:{name}"),
                crate::tasks::RaySubmitTask {
                    job_name: name.into(),
                    entrypoint: plan.value("entrypoint").into(),
                    head_address: c.head_address.clone(),
                    dashboard_port: c.dashboard_port,
                    working_dir: None,
                },
            ),
            "wait" => PhaseTask::new(
                format!("ray-wait:{name}"),
                crate::tasks::RayWaitTask {
                    job_name: name.into(),
                    head_address: c.head_address.clone(),
                    dashboard_port: c.dashboard_port,
                    poll_interval: plan.duration("poll-seconds")?,
                    timeout: if plan.parameters.contains_key("timeout") {
                        Some(plan.duration("timeout")?)
                    } else {
                        None
                    },
                },
            ),
            _ => return Err(IntegrationError("unsupported ray phase".into())),
        })
    }
}
