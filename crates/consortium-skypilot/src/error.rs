//! Error types for SkyPilot orchestration.

#[derive(Debug, thiserror::Error)]
pub enum SkypilotError {
    #[error("sky environment build failed: {0}")]
    EnvBuildFailed(String),

    #[error("sky launch failed for {cluster}: {message}")]
    LaunchFailed { cluster: String, message: String },

    #[error("sky exec failed on {cluster}: {message}")]
    ExecFailed { cluster: String, message: String },

    #[error("no skypilot config in fleet configuration")]
    NoConfig,

    #[error("nix error: {0}")]
    Nix(#[from] consortium_nix::NixError),

    #[error("dag error: {0}")]
    Dag(String),

    #[error("{0}")]
    General(String),
}

pub type Result<T> = std::result::Result<T, SkypilotError>;
