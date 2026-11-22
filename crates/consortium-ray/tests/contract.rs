//! Contract-suite enrollment for consortium-ray.
//!
//! The ray pipeline is a single-job linear chain: `nix build` of the ray
//! environment → `ray job submit` against the head node → `ray job status`
//! polling until a terminal state. All commands run locally through the
//! injected executor, so the fixtures below script plain argv renderings.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use consortium::dag::{DagReport, DagTask};
use consortium_integration::exec::{
    CommandSpec, ExecError, ExecOutput, Executor, Rule, ScriptedExecutor,
};
use consortium_integration::fleet::{DeploymentNode, FleetConfig, ProfileType, RayFleetConfig};
use consortium_integration::grammar::{Integration, Request};
use consortium_integration::submission::SubmissionReceipt;
use consortium_integration_testkit::{
    integration_contract_tests, Contract, OptionVariant, PartialFailure, Phase,
};
use consortium_ray::tasks::{NixBuildRayEnvTask, RaySubmitTask, RayWaitTask};
use consortium_ray::{submit_job, RayError, RayIntegration, RayOptions};

// ── Shared fixtures ─────────────────────────────────────────────────────────

const JOB_NAME: &str = "train";
const ENTRYPOINT: &str = "python train.py";
const SUBMIT_ACK: &str = "Job 'raysubmit_abc123' submitted successfully\n";

fn fleet_config(with_ray: bool) -> FleetConfig {
    let mut nodes = HashMap::new();
    nodes.insert(
        "node01".to_string(),
        DeploymentNode {
            name: "node01".into(),
            target_host: "node01".into(),
            target_user: "root".into(),
            target_port: None,
            system: "x86_64-linux".into(),
            profile_type: ProfileType::Nixos,
            build_on_target: false,
            tags: vec![],
            drv_path: None,
            toplevel: None,
        },
    );
    FleetConfig {
        nodes,
        builders: HashMap::new(),
        flake_uri: ".".into(),
        ansible_config: None,
        slurm_config: None,
        ray_config: with_ray.then(|| RayFleetConfig {
            head_address: "ray-head.local".into(),
            dashboard_port: 8265,
            kubernetes: false,
            worker_groups: HashMap::new(),
        }),
        skypilot_config: None,
    }
}

fn happy() -> ScriptedExecutor {
    ScriptedExecutor::new()
        .on("nix build", ExecOutput::ok("/nix/store/ray-env\n"))
        .on("job submit", ExecOutput::ok(SUBMIT_ACK))
        .on("job status", ExecOutput::ok("Status: SUCCEEDED\n"))
}

fn failing(marker: &str) -> ScriptedExecutor {
    // The failing rule comes FIRST: ScriptedExecutor is first-match-wins.
    ScriptedExecutor::new()
        .rule(Rule::containing(marker, ExecOutput::new(1, "", "boom")))
        .on("nix build", ExecOutput::ok("/nix/store/ray-env\n"))
        .on("job submit", ExecOutput::ok(SUBMIT_ACK))
        .on("job status", ExecOutput::ok("Status: SUCCEEDED\n"))
}

// ── The contract under test ─────────────────────────────────────────────────

struct RayContract;

impl Contract for RayContract {
    type Report = DagReport;
    type Error = RayError;
    type Options = RayOptions;

    fn name() -> &'static str {
        "ray"
    }

    fn run(
        config: &FleetConfig,
        exec: Arc<dyn Executor>,
        opts: &Self::Options,
    ) -> Result<Self::Report, Self::Error> {
        // Contract fixtures poll instantly; production keeps the 10s default.
        let opts = RayOptions {
            poll_interval: Duration::from_millis(1),
            ..*opts
        };
        submit_job(exec, config, JOB_NAME, ENTRYPOINT, &opts)
    }

    fn phases() -> Vec<Phase> {
        // All phases run locally, so multi-word markers match the raw argv
        // renderings. Markers are mutually exclusive: "job submit" and
        // "job status" share the `ray job` prefix but neither contains the
        // other, and neither appears in a `nix build` line.
        vec![
            Phase::new("build", "nix build", &[]),
            Phase::new("submit", "job submit", &["build"]),
            Phase::new("wait", "job status", &["submit"]),
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
            "no-wait",
            RayOptions {
                wait: false,
                ..Default::default()
            },
            vec!["wait"],
        )]
    }

    fn partial_failure() -> Option<PartialFailure> {
        // Single-job linear pipeline: no independent per-host branches.
        None
    }

    fn task_descriptions(config: &FleetConfig, _exec: Arc<dyn Executor>) -> Vec<String> {
        let Some(ray_config) = config.ray_config.as_ref() else {
            return Vec::new();
        };
        vec![
            NixBuildRayEnvTask::new(JOB_NAME, &config.flake_uri).describe(),
            RaySubmitTask {
                job_name: JOB_NAME.to_string(),
                entrypoint: ENTRYPOINT.to_string(),
                head_address: ray_config.head_address.clone(),
                dashboard_port: ray_config.dashboard_port,
                working_dir: None,
            }
            .describe(),
            RayWaitTask {
                job_name: JOB_NAME.to_string(),
                head_address: ray_config.head_address.clone(),
                dashboard_port: ray_config.dashboard_port,
                poll_interval: Duration::from_secs(10),
                timeout: None,
            }
            .describe(),
        ]
    }
}

integration_contract_tests!(RayContract);

// ── Submission receipts ─────────────────────────────────────────────────────

fn receipt_plan(config: &FleetConfig) -> consortium_integration::grammar::Plan {
    let request = Request::default()
        .with("name", JOB_NAME)
        .with("entrypoint", ENTRYPOINT)
        .with("wait", "true");
    RayIntegration.plan(config, &request).unwrap()
}

/// An executor whose `ray` cannot be spawned at all; every other command is
/// delegated to the scripted executor.
struct SubmitUnspawnable {
    scripted: Arc<ScriptedExecutor>,
}

impl Executor for SubmitUnspawnable {
    fn exec(&self, spec: &CommandSpec) -> Result<ExecOutput, ExecError> {
        if spec.render().contains("job submit") {
            return Err(ExecError::Spawn {
                program: spec.program.clone(),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "no ray binary on PATH"),
            });
        }
        self.scripted.exec(spec)
    }
}

#[test]
fn post_spawn_submit_errors_leave_the_receipt_unknown() {
    let exec = Arc::new(
        ScriptedExecutor::new()
            .on("nix build", ExecOutput::ok("/nix/store/ray-env\n"))
            .on_error("job submit", "connection reset")
            .on("job status", ExecOutput::ok("Status: SUCCEEDED\n")),
    );
    let config = fleet_config(true);
    let plan = receipt_plan(&config);
    let result = RayIntegration
        .execute_with_receipt(exec.clone(), &config, &plan)
        .unwrap();
    assert_eq!(result.submission, Some(SubmissionReceipt::Unknown));
    let error = result
        .execution
        .report
        .phases
        .iter()
        .find(|phase| phase.phase == "submit")
        .and_then(|phase| phase.error.as_deref())
        .expect("submit phase must fail");
    assert!(error.contains("connection reset"), "{error}");
    exec.assert_invoked_containing("job submit");
    // Wait must never poll a job whose ID was never established.
    exec.assert_not_invoked_containing("job status");
}

#[test]
fn spawn_failures_are_not_submitted_and_wait_never_polls() {
    let scripted =
        Arc::new(ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/ray-env\n")));
    let config = fleet_config(true);
    let plan = receipt_plan(&config);
    let result = RayIntegration
        .execute_with_receipt(
            Arc::new(SubmitUnspawnable {
                scripted: scripted.clone(),
            }),
            &config,
            &plan,
        )
        .unwrap();
    assert_eq!(result.submission, Some(SubmissionReceipt::NotSubmitted));
    // The delegated build ran; a poll would have been delegated and recorded.
    scripted.assert_invoked_containing("nix build");
    scripted.assert_not_invoked_containing("job status");
}
