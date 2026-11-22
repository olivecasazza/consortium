//! Exercise the Slurm adapter with provider replies that would be unsafe to infer from status alone.

use std::sync::Arc;
use std::time::Duration;

use consortium_integration::exec::{
    CommandSpec, ExecError, ExecOutput, Executor, ScriptedExecutor,
};
use consortium_integration::grammar::{Integration, Request, RunReport};
use consortium_slurm::SlurmIntegration;
use serde_json::{json, Value};

use super::{fleet_config, pipeline};

const INVALID_PARTITION: &str =
    "sbatch: error: Batch job submission failed: Invalid partition name specified\n";

/// Assert the caller-visible receipt, including the absence of an ID for non-acceptance.
fn assert_receipt(output: ExecOutput, expected: Value) -> RunReport {
    let exec = Arc::new(pipeline(ScriptedExecutor::new().on("sbatch", output)));
    let request = Request::default()
        .with("name", "train")
        .with("script", "train.sh")
        .with("wait", "false");
    let report = assert_receipt_for(exec, request, expected.clone());
    assert_eq!(report.success, expected["state"] == "accepted");
    report
}

/// Run the whole pipeline for `request` and assert the caller-visible receipt.
fn assert_receipt_for(exec: Arc<dyn Executor>, request: Request, expected: Value) -> RunReport {
    let config = fleet_config(true);
    let plan = SlurmIntegration.plan(&config, &request).unwrap();
    let result = SlurmIntegration
        .execute_with_receipt(exec, &config, &plan)
        .unwrap();
    let report = result.execution.report;
    let receipt = serde_json::to_value(&result.submission).unwrap();
    assert_eq!(receipt, expected, "{report:?}");
    report
}

#[test]
fn unterminated_numeric_reply_retains_evidence_without_publishing_a_partial_id() {
    // If the real ID was 12345, losing the last three bytes leaves a plausible but wrong ID.
    let fragment = "Submitted batch job 123";
    let diagnostic = "connection interrupted";
    let report = assert_receipt(
        ExecOutput::new(0, fragment, diagnostic),
        json!({"state": "unknown"}),
    );
    let error = report
        .phases
        .iter()
        .find(|phase| phase.phase == "submit")
        .unwrap()
        .error
        .as_deref()
        .unwrap();
    for evidence in ["train", "root@node01", fragment, diagnostic] {
        assert!(error.contains(evidence), "missing {evidence:?} in {error}");
    }
}

#[test]
fn invalid_or_conflicting_acknowledgments_are_unknown() {
    let ack = "Submitted batch job 42\n";
    for output in [
        ExecOutput::ok("Submitted batch job 0\n"),
        ExecOutput::ok("Submitted batch job +42\n"),
        ExecOutput::ok("Submitted batch job 042\n"),
        ExecOutput::ok("Submitted batch job 4294967296\n"),
        ExecOutput::ok("Submitted batch job 42\nSubmitted batch job 43\n"),
        ExecOutput::new(255, ack, "connection lost"),
        ExecOutput::new(0, ack, INVALID_PARTITION),
        ExecOutput::new(0, ack, "sbatch: error: invalid partition specified: gpu\n"),
        ExecOutput::new(1, ack, INVALID_PARTITION),
    ] {
        assert_receipt(output, json!({"state": "unknown"}));
    }
}

#[test]
fn only_a_complete_provider_rejection_is_conclusive() {
    let detail = "sbatch: error: invalid partition specified: missing\n";
    assert_receipt(
        ExecOutput::new(1, "", format!("{detail}{INVALID_PARTITION}")),
        json!({"state": "rejected"}),
    );
    for output in [
        ExecOutput::new(255, "", INVALID_PARTITION),
        ExecOutput::new(124, "", INVALID_PARTITION),
        ExecOutput::new(-1, "", INVALID_PARTITION),
        ExecOutput::new(1, "", detail),
        ExecOutput::new(1, "", format!("wrapper quoted: {INVALID_PARTITION}")),
        ExecOutput::new(1, "", INVALID_PARTITION.trim_end()),
        ExecOutput::new(
            1,
            "",
            "sbatch: error: Batch job submission failed: Socket timed out on send/recv operation\n",
        ),
    ] {
        assert_receipt(output, json!({"state": "unknown"}));
    }
}

#[test]
fn ssh_warning_does_not_shadow_an_intact_success() {
    assert_receipt(
        ExecOutput::new(
            0,
            "Submitted batch job 42\r\n",
            "Warning: Permanently added 'node01' to the list of known hosts.\n",
        ),
        json!({"state": "accepted", "job_id": "42"}),
    );
}

/// Fabricates `ExecError::Spawn` for the sbatch command only; every other
/// command is scripted, so the test depends on no real binary or PATH entry.
struct SpawnOnSbatch(ScriptedExecutor);

impl Executor for SpawnOnSbatch {
    fn exec(&self, spec: &CommandSpec) -> Result<ExecOutput, ExecError> {
        if spec.render().contains("sbatch") {
            return Err(ExecError::Spawn {
                program: spec.program.clone(),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "sbatch: not found"),
            });
        }
        self.0.exec(spec)
    }

    fn exec_timeout(&self, spec: &CommandSpec, limit: Duration) -> Result<ExecOutput, ExecError> {
        self.0.exec_timeout(spec, limit)
    }
}

#[test]
fn post_spawn_submit_failure_is_unknown_and_skips_wait_and_collect() {
    let exec = Arc::new(pipeline(
        ScriptedExecutor::new().on_error("sbatch", "connection reset"),
    ));
    let request = Request::default()
        .with("name", "train")
        .with("script", "train.sh")
        .with("wait", "true")
        .with("collect", "results.txt");
    let report = assert_receipt_for(exec.clone(), request, json!({"state": "unknown"}));
    let submit = report
        .phases
        .iter()
        .find(|phase| phase.phase == "submit")
        .unwrap();
    assert!(submit.error.is_some(), "{report:?}");
    // NB: ssh renderings mention `cat` outside the collect command, so assert
    // on the unique output pattern instead (see contract.rs).
    exec.assert_not_invoked_containing("sacct");
    exec.assert_not_invoked_containing("results.txt");
}

#[test]
fn spawn_failure_before_dispatch_is_not_submitted() {
    let exec = Arc::new(SpawnOnSbatch(pipeline(ScriptedExecutor::new())));
    let request = Request::default()
        .with("name", "train")
        .with("script", "train.sh")
        .with("wait", "true")
        .with("collect", "results.txt");
    assert_receipt_for(exec, request, json!({"state": "not-submitted"}));
}

#[test]
fn accepted_receipt_survives_a_later_failed_wait_phase() {
    let exec = Arc::new(pipeline(
        ScriptedExecutor::new()
            .on("sbatch", ExecOutput::ok("Submitted batch job 42\n"))
            .on("sacct", ExecOutput::ok("FAILED\n")),
    ));
    let request = Request::default()
        .with("name", "train")
        .with("script", "train.sh")
        .with("wait", "true");
    let report = assert_receipt_for(exec, request, json!({"state": "accepted", "job_id": "42"}));
    assert!(!report.success, "{report:?}");
}

#[test]
fn receipts_do_not_leak_between_invocations() {
    let slurm = SlurmIntegration;
    let config = fleet_config(true);
    let request = Request::default()
        .with("name", "train")
        .with("script", "train.sh")
        .with("wait", "false");
    let plan = slurm.plan(&config, &request).unwrap();

    let first = slurm
        .execute_with_receipt(Arc::new(pipeline(ScriptedExecutor::new())), &config, &plan)
        .unwrap();
    assert_eq!(
        serde_json::to_value(&first.submission).unwrap(),
        json!({"state": "accepted", "job_id": "12345"}),
    );

    let second = slurm
        .execute_with_receipt(
            Arc::new(pipeline(ScriptedExecutor::new().on(
                "sbatch",
                ExecOutput::new(255, "Submitted batch job 42\n", "connection lost"),
            ))),
            &config,
            &plan,
        )
        .unwrap();
    let receipt = serde_json::to_value(&second.submission).unwrap();
    assert_eq!(receipt, json!({"state": "unknown"}));
    assert!(receipt.get("job_id").is_none(), "{receipt}");
}

#[test]
fn ssh_first_contact_notice_does_not_mask_a_scheduler_refusal() {
    let notice = "Warning: Permanently added 'node01' (ED25519) to the list of known hosts.\n";
    assert_receipt(
        ExecOutput::new(1, "", format!("{notice}{INVALID_PARTITION}")),
        json!({"state": "rejected"}),
    );
}

#[test]
fn ssh_first_contact_notice_with_an_unrelated_error_is_unknown() {
    let notice = "Warning: Permanently added 'node01' (ED25519) to the list of known hosts.\n";
    assert_receipt(
        ExecOutput::new(
            1,
            "",
            format!("{notice}sbatch: error: Batch job submission failed: Invalid QOS specified\n"),
        ),
        json!({"state": "unknown"}),
    );
}
