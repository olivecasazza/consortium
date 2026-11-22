//! Exercise the Slurm adapter with provider replies that would be unsafe to infer from status alone.

use std::sync::Arc;

use consortium_integration::exec::{ExecOutput, ScriptedExecutor};
use consortium_integration::grammar::{Integration, Request, RunReport};
use consortium_slurm::SlurmIntegration;
use serde_json::{json, Value};

use super::{fleet_config, pipeline};

const INVALID_PARTITION: &str =
    "sbatch: error: Batch job submission failed: Invalid partition name specified\n";

/// Assert the caller-visible receipt, including the absence of an ID for non-acceptance.
fn assert_receipt(output: ExecOutput, expected: Value) -> RunReport {
    let exec = pipeline(ScriptedExecutor::new().on("sbatch", output));
    let config = fleet_config(true);
    let request = Request::default()
        .with("name", "train")
        .with("script", "train.sh")
        .with("wait", "false");
    let plan = SlurmIntegration.plan(&config, &request).unwrap();
    let result = SlurmIntegration
        .execute_with_receipt(Arc::new(exec), &config, &plan)
        .unwrap();
    let report = result.execution.report;
    let receipt = serde_json::to_value(&result.submission).unwrap();
    assert_eq!(receipt, expected, "{report:?}");
    assert_eq!(report.success, expected["state"] == "accepted");
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
