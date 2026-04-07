//! Configuration defaults.
//!
//! Rust implementation of `ClusterShell.Defaults`.

use std::path::PathBuf;

/// Runtime configuration with sane defaults.
#[derive(Debug, Clone)]
pub struct Defaults {
    pub fanout: u32,
    pub connect_timeout: f64,
    pub command_timeout: f64,
    pub color: bool,
    pub fd_max: Option<u32>,
    pub local_workername: String,
    pub distant_workername: String,
}

impl Defaults {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load defaults from config file paths.
    pub fn from_config(paths: &[PathBuf]) -> Self {
        let _ = paths;
        todo!("Defaults::from_config")
    }
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            fanout: 64,
            connect_timeout: 30.0,
            command_timeout: 0.0,
            color: false,
            fd_max: None,
            local_workername: "exec".to_string(),
            distant_workername: "ssh".to_string(),
        }
    }
}

/// Return the standard config file search paths.
pub fn config_paths(name: &str) -> Vec<PathBuf> {
    let _ = name;
    todo!("config_paths")
}
