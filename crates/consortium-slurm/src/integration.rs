//! Adapter for the shared integration grammar.

use consortium_integration::fleet::FleetConfig;
use consortium_integration::grammar::{
    Condition, EffectKind, FailurePolicy, Grammar, Integration, IntegrationError, Parameter,
    ParameterKind, Phase, PhaseKind, PhaseScope, PhaseTask, Plan,
};

/// Slurm job submission through the uniform integration contract.
pub struct SlurmIntegration;

static GRAMMAR: Grammar = Grammar {
    name: "slurm",
    about: "Slurm job submission",
    targets: false,
    failure_policy: FailurePolicy::Stop,
    parameters: &[
        Parameter::required("name", "Job and Nix environment name"),
        Parameter::required("script", "Shell script submitted to Slurm"),
        Parameter::optional("timeout", "Job wait timeout in seconds")
            .typed(ParameterKind::PositiveDuration),
        Parameter::optional("partition", "Slurm partition"),
        Parameter::optional("collect", "Literal output file path to collect"),
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
    ],
    phases: &[
        Phase::new("build").behavior(PhaseKind::Build, PhaseScope::Shared, &[EffectKind::Process]),
        Phase::new("copy").behavior(
            PhaseKind::Copy,
            PhaseScope::Shared,
            &[EffectKind::Process, EffectKind::RemoteProcess],
        ),
        Phase::new("submit").behavior(
            PhaseKind::Submit,
            PhaseScope::Shared,
            &[EffectKind::Process, EffectKind::RemoteProcess],
        ),
        Phase::when("wait", Condition::Equals("wait", "true")).behavior(
            PhaseKind::Wait,
            PhaseScope::Shared,
            &[
                EffectKind::Process,
                EffectKind::RemoteProcess,
                EffectKind::Clock,
                EffectKind::Sleep,
            ],
        ),
        Phase::when("collect", Condition::Present("collect")).behavior(
            PhaseKind::Collect,
            PhaseScope::Shared,
            &[EffectKind::Process, EffectKind::RemoteProcess],
        ),
    ],
};

impl Integration for SlurmIntegration {
    fn grammar(&self) -> &'static Grammar {
        &GRAMMAR
    }

    fn validate_config(&self, config: &FleetConfig) -> Result<(), IntegrationError> {
        if config.slurm_config.is_none() {
            return Err(IntegrationError(
                "slurm: missing fleet configuration".into(),
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
            .slurm_config
            .as_ref()
            .ok_or_else(|| IntegrationError("slurm: missing configuration".into()))?;
        let name = plan.value("name");
        Ok(match phase.name {
            "build" => PhaseTask::new(
                format!("build-job-env:{name}"),
                crate::tasks::NixBuildJobEnvTask::new(name, &config.flake_uri),
            ),
            "copy" => PhaseTask::new(
                format!("copy-job-env:{name}"),
                crate::tasks::NixCopyToSubmitTask {
                    job_name: name.into(),
                    submit_host: c.submit_node.clone(),
                    submit_user: c.submit_user.clone(),
                },
            ),
            "submit" => PhaseTask::new(
                format!("slurm-submit:{name}"),
                crate::tasks::SlurmSubmitTask {
                    job_name: name.into(),
                    script: plan.value("script").into(),
                    partition: plan.parameters.get("partition").cloned(),
                    submit_host: c.submit_node.clone(),
                    submit_user: c.submit_user.clone(),
                },
            ),
            "wait" => {
                let mut task =
                    crate::tasks::SlurmWaitTask::new(name, &c.submit_node, &c.submit_user)
                        .with_poll_interval(plan.duration("poll-seconds")?);
                if plan.parameters.contains_key("timeout") {
                    task.timeout = Some(plan.duration("timeout")?);
                }
                PhaseTask::new(format!("slurm-wait:{name}"), task)
            }
            "collect" => PhaseTask::new(
                format!("slurm-collect:{name}"),
                crate::tasks::SlurmCollectTask {
                    job_name: name.into(),
                    output_pattern: plan.value("collect").into(),
                    submit_host: c.submit_node.clone(),
                    submit_user: c.submit_user.clone(),
                },
            ),
            _ => return Err(IntegrationError("unsupported slurm phase".into())),
        })
    }
}
