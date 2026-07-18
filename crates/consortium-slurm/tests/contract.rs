//! Contract-suite enrollment for the slurm integration.
//!
//! Implements [`Contract`] against the real `submit_job` entry point and
//! instantiates the generated suite; integration-specific tests for the
//! (non-default) collect wiring live at the bottom of this file.

use std::collections::HashMap;
use std::sync::Arc;

use consortium::dag::{DagContext, DagReport, DagTask, TaskId, TaskOutcome};
use consortium_integration::exec::{ExecOutput, Executor, Rule, ScriptedExecutor};
use consortium_integration::fleet::{DeploymentNode, FleetConfig, ProfileType, SlurmFleetConfig};
use consortium_integration_testkit::{
    integration_contract_tests, Contract, OptionVariant, PartialFailure, Phase,
};
use consortium_slurm::error::SlurmError;
use consortium_slurm::tasks::{
    NixBuildJobEnvTask, NixCopyToSubmitTask, SlurmCollectTask, SlurmSubmitTask, SlurmWaitTask,
};
use consortium_slurm::{submit_job, SubmitOptions};

// ── Shared fixtures ─────────────────────────────────────────────────────────

fn fleet_config(with_slurm: bool) -> FleetConfig {
    let mut nodes = HashMap::new();
    nodes.insert(
        "node01".to_string(),
        DeploymentNode {
            name: "node01".to_string(),
            target_host: "node01".to_string(),
            target_user: "root".to_string(),
            target_port: None,
            system: "x86_64-linux".to_string(),
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
    // nix rules come before ssh-wrapped rules (broad ssh substrings would
    // also match the `ssh-ng://` copy rendering); every rule here is
    // phase-specific, so order only matters against the failing-rule prefix
    // in `failing()` below.
    ScriptedExecutor::new()
        .on("nix build", ExecOutput::ok("/nix/store/env\n"))
        .on("nix copy", ExecOutput::ok(""))
        .on("sbatch", ExecOutput::ok("Submitted batch job 12345\n"))
        .on("sacct", ExecOutput::ok("COMPLETED\n"))
}

fn failing(marker: &str) -> ScriptedExecutor {
    // The failing rule comes FIRST: ScriptedExecutor is first-match-wins.
    ScriptedExecutor::new()
        .rule(Rule::containing(marker, ExecOutput::new(1, "", "boom")))
        .on("nix build", ExecOutput::ok("/nix/store/env\n"))
        .on("nix copy", ExecOutput::ok(""))
        .on("sbatch", ExecOutput::ok("Submitted batch job 12345\n"))
        .on("sacct", ExecOutput::ok("COMPLETED\n"))
}

// ── The contract under test ─────────────────────────────────────────────────

struct SlurmContract;

impl Contract for SlurmContract {
    type Report = DagReport;
    type Error = SlurmError;
    type Options = SubmitOptions;

    fn name() -> &'static str {
        "slurm"
    }

    fn run(
        config: &FleetConfig,
        exec: Arc<dyn Executor>,
        opts: &Self::Options,
    ) -> Result<Self::Report, Self::Error> {
        submit_job(exec, config, "train", "train.sh", Some("gpu"), opts)
    }

    fn phases() -> Vec<Phase> {
        // Single-token markers for ssh-wrapped phases (rendered lines
        // single-quote every remote token), space markers for local phases.
        vec![
            Phase::new("build", "nix build", &[]),
            Phase::new("copy", "nix copy", &["build"]),
            Phase::new("submit", "sbatch", &["copy"]),
            Phase::new("wait", "sacct", &["submit"]),
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

    fn executor_failing_phase(phase_marker: &str) -> ScriptedExecutor {
        failing(phase_marker)
    }

    fn option_variants() -> Vec<OptionVariant<Self::Options>> {
        vec![OptionVariant::new(
            "no-wait",
            SubmitOptions {
                wait: false,
                ..Default::default()
            },
            vec!["wait"],
        )]
    }

    fn partial_failure() -> Option<PartialFailure> {
        // Slurm submission is a single-job linear pipeline, not a per-host
        // fanout — per-host partial failure does not apply.
        None
    }

    fn task_descriptions(config: &FleetConfig, _exec: Arc<dyn Executor>) -> Vec<String> {
        let Some(slurm) = config.slurm_config.as_ref() else {
            return Vec::new();
        };
        vec![
            NixBuildJobEnvTask::new("train", &config.flake_uri).describe(),
            NixCopyToSubmitTask {
                job_name: "train".to_string(),
                submit_host: slurm.submit_node.clone(),
                submit_user: slurm.submit_user.clone(),
            }
            .describe(),
            SlurmSubmitTask {
                job_name: "train".to_string(),
                script: "train.sh".to_string(),
                partition: Some("gpu".to_string()),
                submit_host: slurm.submit_node.clone(),
                submit_user: slurm.submit_user.clone(),
            }
            .describe(),
            SlurmWaitTask::new("train", &slurm.submit_node, &slurm.submit_user).describe(),
        ]
    }
}

// The full generated suite runs against the slurm integration.
integration_contract_tests!(SlurmContract);

// ── Integration-specific tests: collect wiring ──────────────────────────────
//
// `collect` is not a declared contract phase (the default `SubmitOptions`
// has `collect: None`), so its DAG wiring is pinned down here instead.

/// With `collect = Some(pattern)`, a `cat <pattern>` runs on the submit node
/// after the wait phase and the pipeline succeeds end to end.
#[test]
fn collect_option_runs_cat_after_wait() {
    let exec = Arc::new(happy().on("cat", ExecOutput::ok("results-blob\n")));
    let opts = SubmitOptions {
        collect: Some("slurm-12345.out".to_string()),
        ..Default::default()
    };
    let report = submit_job(
        exec.clone(),
        &fleet_config(true),
        "train",
        "train.sh",
        Some("gpu"),
        &opts,
    )
    .expect("collect pipeline should succeed");
    assert!(report.is_success());
    // NB: every ssh rendering contains "cat" (PasswordAuthentication), so
    // assert on the unique output pattern instead.
    exec.assert_invoked_containing("slurm-12345.out");
    let sacct_idx = exec.invocation_index_containing("sacct").unwrap();
    let collect_idx = exec.invocation_index_containing("slurm-12345.out").unwrap();
    assert!(sacct_idx < collect_idx, "collect must run after wait");
}

/// With `wait = false` and `collect = Some(..)`, collect is wired directly
/// after submit: no sacct poll happens, and the cat runs after sbatch.
#[test]
fn collect_without_wait_runs_cat_after_submit() {
    let exec = Arc::new(happy().on("cat", ExecOutput::ok("results-blob\n")));
    let opts = SubmitOptions {
        wait: false,
        collect: Some("slurm-12345.out".to_string()),
        ..Default::default()
    };
    let report = submit_job(
        exec.clone(),
        &fleet_config(true),
        "train",
        "train.sh",
        None,
        &opts,
    )
    .expect("collect pipeline should succeed");
    assert!(report.is_success());
    exec.assert_not_invoked_containing("sacct");
    let sbatch_idx = exec.invocation_index_containing("sbatch").unwrap();
    let collect_idx = exec.invocation_index_containing("slurm-12345.out").unwrap();
    assert!(sbatch_idx < collect_idx, "collect must run after submit");
}

/// The collect task stores the catted content in the DAG context under
/// `slurm-collect:{job}`, reading the job id from whichever predecessor ran.
#[test]
fn collect_task_stores_output_in_context() {
    let scripted = Arc::new(ScriptedExecutor::new().on("cat", ExecOutput::ok("payload\n")));
    let exec: Arc<dyn Executor> = scripted.clone();
    let ctx = DagContext::new();
    ctx.set_state("executor", exec);
    ctx.set_output(TaskId("slurm-submit:train".to_string()), 12345u64);

    let task = SlurmCollectTask {
        job_name: "train".to_string(),
        output_pattern: "slurm-12345.out".to_string(),
        submit_host: "node01".to_string(),
        submit_user: "root".to_string(),
    };
    let outcome = task.execute(&ctx);
    assert!(matches!(outcome, TaskOutcome::Success));
    let content: Option<String> = ctx.get_output(&TaskId("slurm-collect:train".to_string()));
    assert_eq!(content.as_deref(), Some("payload\n"));
}
