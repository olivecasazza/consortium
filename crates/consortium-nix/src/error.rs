//! Error types for NixOS deployment operations.

use std::path::PathBuf;

/// Errors from NixOS deployment operations.
#[derive(Debug, thiserror::Error)]
pub enum NixError {
    #[error("nix evaluation failed for {host}: {message}")]
    EvalFailed { host: String, message: String },

    #[error("nix build failed for {host}: {message}")]
    BuildFailed { host: String, message: String },

    #[error("closure copy failed to {host}: {message}")]
    CopyFailed { host: String, message: String },

    #[error("activation failed on {host}: {message}")]
    ActivationFailed { host: String, message: String },

    #[error("SSH connection failed to {host}: {message}")]
    SshFailed { host: String, message: String },

    #[error("builder {host} is unhealthy: {message}")]
    UnhealthyBuilder { host: String, message: String },

    #[error("no healthy builders available")]
    NoHealthyBuilders,

    #[error("configuration error: {0}")]
    Config(#[from] crate::config::ConfigError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("failed to write machines file to {path}: {source}")]
    MachinesFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("{0}")]
    General(String),
}

pub type Result<T> = std::result::Result<T, NixError>;
