//! Task orchestration.
//!
//! Rust implementation of `ClusterShell.Task`.

/// A Task manages a set of workers and their I/O engine.
pub struct Task;

impl Task {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Task {
    fn default() -> Self {
        Self::new()
    }
}
