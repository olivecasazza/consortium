//! Error types for DAG execution.

use crate::dag::types::TaskId;

/// Errors from DAG construction and execution.
#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("cycle detected in DAG involving task {0}")]
    CycleDetected(TaskId),

    #[error("unknown task referenced in dependency: {0}")]
    UnknownTask(TaskId),

    #[error("duplicate task ID: {0}")]
    DuplicateTask(TaskId),

    #[error("task failed: {0} — {1}")]
    TaskFailed(TaskId, String),

    #[error("DAG is empty — no tasks to execute")]
    EmptyDag,

    #[error("executor channel closed unexpectedly")]
    ChannelClosed,

    #[error("{0}")]
    General(String),
}

pub type Result<T> = std::result::Result<T, DagError>;
