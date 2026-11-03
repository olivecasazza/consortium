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
    state_override: Option<Arc<StateOverride>>,
}

struct StateOverride {
    key: String,
    value: Mutex<Box<dyn Any + Send>>,
    parent: Option<Arc<StateOverride>>,
}

impl DagContext {
    /// Create a new empty context.
    pub fn new() -> Self {
        Self {
            outputs: Arc::new(Mutex::new(HashMap::new())),
            user_state: Arc::new(Mutex::new(HashMap::new())),
            state_override: None,
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
        let mut current = self.state_override.as_ref();
        while let Some(state) = current {
            if state.key == key {
                return state.value.lock().unwrap().downcast_ref::<T>().cloned();
            }
            current = state.parent.as_ref();
        }
        let map = self.user_state.lock().unwrap();
        map.get(key)?.downcast_ref::<T>().cloned()
    }

    /// Override one read in a task-local view; outputs and ordinary state writes
    /// remain shared. Existing overrides are inherited without mutating siblings.
    pub fn with_state_override<T: Send + 'static>(&self, key: &str, value: T) -> Self {
        Self {
            outputs: Arc::clone(&self.outputs),
            user_state: Arc::clone(&self.user_state),
            state_override: Some(Arc::new(StateOverride {
                key: key.into(),
                value: Mutex::new(Box::new(value)),
                parent: self.state_override.clone(),
            })),
        }
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

    #[test]
    fn task_overrides_isolate_reads_and_share_pipeline_outputs() {
        let base = DagContext::new();
        base.set_state("executor", "base".to_owned());
        let first = base.with_state_override("executor", "first".to_owned());
        let second = base.with_state_override("executor", "second".to_owned());
        let nested = first.with_state_override("other", 5_u8);
        assert_eq!(nested.get_state::<String>("executor").unwrap(), "first");
        assert_eq!(second.get_state::<String>("executor").unwrap(), "second");
        assert_eq!(base.get_state::<String>("executor").unwrap(), "base");
        assert_eq!(nested.get_state::<u8>("other"), Some(5));
        assert_eq!(first.get_state::<u8>("other"), None);
        std::thread::scope(|scope| {
            for (index, view) in [first, second].into_iter().enumerate() {
                scope.spawn(move || {
                    view.set_output(TaskId::from(format!("out:{index}")), index);
                    view.set_state(&format!("state:{index}"), index);
                });
            }
        });
        for index in 0..2 {
            assert_eq!(
                base.get_output::<usize>(&TaskId::from(format!("out:{index}"))),
                Some(index)
            );
            assert_eq!(
                base.get_state::<usize>(&format!("state:{index}")),
                Some(index)
            );
        }
    }
}
