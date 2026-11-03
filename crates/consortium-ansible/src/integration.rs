//! Adapter for the shared integration grammar.

use consortium_integration::fleet::FleetConfig;
use consortium_integration::grammar::{
    EffectKind, FailurePolicy, Grammar, Integration, IntegrationError, Parameter, ParameterKind,
    Phase, PhaseKind, PhaseScope, PhaseTask, Plan,
};

/// Ansible playbook execution through the uniform integration contract.
pub struct AnsibleIntegration;

static GRAMMAR: Grammar = Grammar {
    name: "ansible",
    about: "Ansible playbook execution",
    targets: true,
    failure_policy: FailurePolicy::ContinueIndependent,
    parameters: &[
        Parameter::required("playbook", "Playbook path on the control node"),
        Parameter::required("environment", "Nix environment attribute name"),
        Parameter::defaulted(
            "check",
            "Run Ansible check mode",
            ParameterKind::Boolean,
            "false",
        ),
        Parameter::defaulted(
            "fanout",
            "Maximum parallel playbook runs",
            ParameterKind::PositiveInteger,
            "4",
        ),
    ],
    phases: &[
        Phase::new("build").behavior(PhaseKind::Build, PhaseScope::Shared, &[EffectKind::Process]),
        Phase::new("copy").behavior(
            PhaseKind::Copy,
            PhaseScope::Shared,
            &[EffectKind::Process, EffectKind::RemoteProcess],
        ),
        Phase::new("execute").behavior(
            PhaseKind::Execute,
            PhaseScope::Target,
            &[EffectKind::Process, EffectKind::RemoteProcess],
        ),
    ],
};

impl Integration for AnsibleIntegration {
    fn grammar(&self) -> &'static Grammar {
        &GRAMMAR
    }

    fn validate_config(&self, config: &FleetConfig) -> Result<(), IntegrationError> {
        if config.ansible_config.is_none() {
            return Err(IntegrationError(
                "ansible: missing fleet configuration".into(),
            ));
        }
        Ok(())
    }

    fn task(
        &self,
        config: &FleetConfig,
        plan: &Plan,
        phase: &Phase,
        resource: &str,
    ) -> Result<PhaseTask, IntegrationError> {
        let c = config
            .ansible_config
            .as_ref()
            .ok_or_else(|| IntegrationError("ansible: missing configuration".into()))?;
        let env = plan.value("environment");
        Ok(match phase.name {
            "build" => PhaseTask::new(
                format!("build-ansible-env:{env}"),
                crate::tasks::NixBuildAnsibleEnvTask::new(env, &config.flake_uri),
            ),
            "copy" => PhaseTask::new(
                format!("copy-ansible-env:{env}"),
                crate::tasks::NixCopyAnsibleEnvTask {
                    env_name: env.into(),
                    target_host: c.control_node.clone(),
                    target_user: "root".into(),
                },
            ),
            "execute" => PhaseTask::new(
                format!("run-playbook:{resource}"),
                crate::tasks::AnsiblePlaybookTask::new(
                    resource,
                    plan.value("playbook"),
                    env,
                    &c.control_node,
                    "root",
                )
                .with_check(plan.flag("check")),
            ),
            _ => return Err(IntegrationError("unsupported ansible phase".into())),
        })
    }
}
