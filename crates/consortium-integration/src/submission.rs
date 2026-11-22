//! Scheduler submission receipts are independent of the job's eventual outcome.

use std::fmt::Display;

use consortium::dag::{DagContext, TaskOutcome};
use serde::Serialize;

const RECEIPT_KEY: &str = "consortium_integration.submission";

/// What a single submit task can establish about its request to the scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum SubmissionReceipt {
    /// The scheduler acknowledged this job ID.
    #[non_exhaustive]
    Accepted { job_id: String },
    /// Dispatch may have occurred, but no definitive response was available.
    Unknown,
    /// The scheduler explicitly refused the request.
    Rejected,
    /// The request demonstrably did not leave the client.
    NotSubmitted,
}

/// Read the receipt, if a submit task actually ran in this invocation.
///
/// The reader for custom `DagBuilder` pipelines that run `SlurmSubmitTask`
/// or `RaySubmitTask` directly; the integration path instead returns the
/// receipt through `Integration::execute_with_receipt`.
pub fn receipt(ctx: &DagContext) -> Option<SubmissionReceipt> {
    ctx.get_state(RECEIPT_KEY)
}

/// Record an acknowledged ID; the task still provides its own typed DAG output.
pub fn accepted(ctx: &DagContext, job_id: impl ToString) -> TaskOutcome {
    ctx.set_state(
        RECEIPT_KEY,
        SubmissionReceipt::Accepted {
            job_id: job_id.to_string(),
        },
    );
    TaskOutcome::Success
}

/// Stop dependent phases when acknowledgment was lost. Include the scheduler,
/// job name, and available evidence in `detail` so the caller can investigate.
pub fn unknown(ctx: &DagContext, detail: impl Display) -> TaskOutcome {
    ctx.set_state(RECEIPT_KEY, SubmissionReceipt::Unknown);
    TaskOutcome::Failed(format!(
        "submission unknown: a job may exist; check the scheduler before resubmitting. {detail}"
    ))
}

/// Record an explicit scheduler rejection, not merely a nonzero exit status.
pub fn rejected(ctx: &DagContext, detail: impl Display) -> TaskOutcome {
    ctx.set_state(RECEIPT_KEY, SubmissionReceipt::Rejected);
    TaskOutcome::Failed(format!("submission rejected: {detail}"))
}

/// Record a failure known to precede dispatch to the scheduler.
pub fn not_submitted(ctx: &DagContext, detail: impl Display) -> TaskOutcome {
    ctx.set_state(RECEIPT_KEY, SubmissionReceipt::NotSubmitted);
    TaskOutcome::Failed(format!("not submitted: {detail}"))
}
