//! Error types for Ray orchestration.

use consortium_integration::exec::ExecError;
use consortium_integration::staging::StagingError;

#[derive(Debug, thiserror::Error)]
pub enum RayError {
    #[error("ray environment build failed: {0}")]
    EnvBuildFailed(String),

    #[error("ray job submission failed: {0}")]
    SubmitFailed(String),

    #[error("ray job {job_id} failed: {message}")]
    JobFailed { job_id: String, message: String },

    #[error("no ray config in fleet configuration")]
    NoConfig,

    #[error("staging error: {0}")]
    Staging(#[from] StagingError),

    #[error("exec error: {0}")]
    Exec(#[from] ExecError),

    #[error("dag error: {0}")]
    Dag(String),

    #[error("{0}")]
    General(String),
}

pub type Result<T> = std::result::Result<T, RayError>;
