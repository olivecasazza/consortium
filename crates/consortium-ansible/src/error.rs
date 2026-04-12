//! Error types for Ansible orchestration.

#[derive(Debug, thiserror::Error)]
pub enum AnsibleError {
    #[error("ansible environment build failed: {0}")]
    EnvBuildFailed(String),

    #[error("playbook failed on {host}: {message}")]
    PlaybookFailed { host: String, message: String },

    #[error("verification failed on {host}: {message}")]
    VerifyFailed { host: String, message: String },

    #[error("inventory generation failed: {0}")]
    InventoryFailed(String),

    #[error("no ansible config in fleet configuration")]
    NoConfig,

    #[error("nix error: {0}")]
    Nix(#[from] consortium_nix::NixError),

    #[error("dag error: {0}")]
    Dag(String),

    #[error("{0}")]
    General(String),
}

pub type Result<T> = std::result::Result<T, AnsibleError>;
