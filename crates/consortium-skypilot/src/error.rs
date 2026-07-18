//! Error types for SkyPilot orchestration.

use consortium_integration::staging::StagingError;
use consortium_integration::ExecError;

#[derive(Debug, thiserror::Error)]
pub enum SkypilotError {
    #[error("no skypilot config in fleet configuration")]
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

pub type Result<T> = std::result::Result<T, SkypilotError>;
