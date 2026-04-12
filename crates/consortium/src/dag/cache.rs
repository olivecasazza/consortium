//! Cache strategy trait for skipping already-completed tasks.

use std::any::Any;

use crate::dag::context::DagContext;
use crate::dag::types::TaskId;

/// Trait for determining whether a task can be skipped.
///
/// Implementors check external state (nix store, file system, etc.)
/// and return the cached output if available.
pub trait CacheStrategy: Send + Sync + 'static {
    /// Check if the task's output already exists.
    /// Returns `Some(output)` if cached (task will be skipped),
    /// `None` if the task must execute.
    fn check(&self, task_id: &TaskId, ctx: &DagContext) -> Option<Box<dyn Any + Send>>;
}

/// Never cache — always execute tasks.
pub struct NoCache;

impl CacheStrategy for NoCache {
    fn check(&self, _task_id: &TaskId, _ctx: &DagContext) -> Option<Box<dyn Any + Send>> {
        None
    }
}
