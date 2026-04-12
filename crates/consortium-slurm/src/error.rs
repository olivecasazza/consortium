//! Error types for Slurm orchestration.

#[derive(Debug, thiserror::Error)]
pub enum SlurmError {
    #[error("job environment build failed: {0}")]
    EnvBuildFailed(String),

    #[error("job submission failed for {job}: {message}")]
    SubmitFailed { job: String, message: String },

    #[error("job {job_id} failed: {message}")]
    JobFailed { job_id: u64, message: String },

    #[error("job {job_id} timed out after {elapsed_secs}s")]
    JobTimeout { job_id: u64, elapsed_secs: u64 },

    #[error("result collection failed for {job}: {message}")]
    CollectFailed { job: String, message: String },

    #[error("no slurm config in fleet configuration")]
    NoConfig,

    #[error("pipeline parse error: {0}")]
    PipelineParse(String),

    #[error("nix error: {0}")]
    Nix(#[from] consortium_nix::NixError),

    #[error("dag error: {0}")]
    Dag(String),

    #[error("{0}")]
    General(String),
}

pub type Result<T> = std::result::Result<T, SlurmError>;
