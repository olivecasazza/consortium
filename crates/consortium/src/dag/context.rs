//! Shared context for DAG task execution.
//!
//! `DagContext` provides type-safe output passing between dependent tasks
//! and arbitrary user state accessible to all tasks.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::dag::types::TaskId;

/// Shared state accessible to all tasks during DAG execution.
///
/// Tasks use `DagContext` to:
/// - Read outputs from predecessor tasks (`get_output`)
/// - Write their own outputs for dependent tasks (`set_output`)
/// - Access user-provided state like configuration (`get_state`/`set_state`)
#[derive(Clone)]
pub struct DagContext {
    outputs: Arc<Mutex<HashMap<TaskId, Box<dyn Any + Send>>>>,
    user_state: Arc<Mutex<HashMap<String, Box<dyn Any + Send>>>>,
}

impl DagContext {
    /// Create a new empty context.
    pub fn new() -> Self {
        Self {
            outputs: Arc::new(Mutex::new(HashMap::new())),
            user_state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get a task's output, downcasting to the expected type.
    pub fn get_output<T: Clone + 'static>(&self, id: &TaskId) -> Option<T> {
        let map = self.outputs.lock().unwrap();
        map.get(id)?.downcast_ref::<T>().cloned()
    }

    /// Store a task's output.
    pub fn set_output<T: Send + 'static>(&self, id: TaskId, value: T) {
        self.outputs.lock().unwrap().insert(id, Box::new(value));
    }

    /// Check if a task has produced output.
    pub fn has_output(&self, id: &TaskId) -> bool {
        self.outputs.lock().unwrap().contains_key(id)
    }

    /// Get user-provided state by key.
    pub fn get_state<T: Clone + 'static>(&self, key: &str) -> Option<T> {
        let map = self.user_state.lock().unwrap();
        map.get(key)?.downcast_ref::<T>().cloned()
    }

    /// Set user-provided state.
    pub fn set_state<T: Send + 'static>(&self, key: &str, value: T) {
        self.user_state
            .lock()
            .unwrap()
            .insert(key.to_string(), Box::new(value));
    }
}

impl Default for DagContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_roundtrip() {
        let ctx = DagContext::new();
        let id = TaskId::from("build:hp01");
        ctx.set_output(id.clone(), "/nix/store/abc".to_string());
        let val: String = ctx.get_output(&id).unwrap();
        assert_eq!(val, "/nix/store/abc");
    }

    #[test]
    fn test_state_roundtrip() {
        let ctx = DagContext::new();
        ctx.set_state("fanout", 64usize);
        let val: usize = ctx.get_state("fanout").unwrap();
        assert_eq!(val, 64);
    }

    #[test]
    fn test_missing_output() {
        let ctx = DagContext::new();
        let val: Option<String> = ctx.get_output(&TaskId::from("nonexistent"));
        assert!(val.is_none());
    }

    #[test]
    fn test_context_clone_shares_state() {
        let ctx1 = DagContext::new();
        let ctx2 = ctx1.clone();
        ctx1.set_state("shared", true);
        let val: bool = ctx2.get_state("shared").unwrap();
        assert!(val);
    }
}
