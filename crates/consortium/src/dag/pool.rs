//! Worker pool management for DAG execution.
//!
//! Pools control how many tasks can execute concurrently, optionally
//! with per-resource-tag slot management.

use std::collections::HashMap;
use std::sync::Mutex;

/// Trait for managing available execution slots.
///
/// The executor calls `acquire` before dispatching a task and
/// `release` when the task completes.
pub trait WorkerPool: Send + Sync {
    /// Try to acquire a slot for a task. Returns true if available.
    fn acquire(&self, resource_tag: Option<&str>) -> bool;

    /// Release a slot after task completion.
    fn release(&self, resource_tag: Option<&str>);

    /// Add a resource to the pool (for dynamic pools).
    fn add_resource(&self, _tag: &str) {}

    /// Remove a resource from the pool (for dynamic pools).
    fn remove_resource(&self, _tag: &str) {}
}

/// Fixed-size pool with a global concurrency limit.
pub struct FixedPool {
    max: usize,
    active: Mutex<usize>,
}

impl FixedPool {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max: max_concurrent,
            active: Mutex::new(0),
        }
    }
}

impl WorkerPool for FixedPool {
    fn acquire(&self, _resource_tag: Option<&str>) -> bool {
        let mut active = self.active.lock().unwrap();
        if *active < self.max {
            *active += 1;
            true
        } else {
            false
        }
    }

    fn release(&self, _resource_tag: Option<&str>) {
        let mut active = self.active.lock().unwrap();
        *active = active.saturating_sub(1);
    }
}

/// Unlimited pool — no concurrency restriction.
pub struct UnlimitedPool;

impl WorkerPool for UnlimitedPool {
    fn acquire(&self, _resource_tag: Option<&str>) -> bool {
        true
    }

    fn release(&self, _resource_tag: Option<&str>) {}
}

/// Dynamic pool where resources can be added/removed during execution.
///
/// Tracks both a global limit and per-tag slot counts. A task with a
/// resource tag requires both a global slot AND a tag-specific slot.
pub struct DynamicPool {
    global_max: usize,
    global_active: Mutex<usize>,
    tag_slots: Mutex<HashMap<String, (usize, usize)>>, // tag -> (max, active)
}

impl DynamicPool {
    pub fn new(global_max: usize) -> Self {
        Self {
            global_max,
            global_active: Mutex::new(0),
            tag_slots: Mutex::new(HashMap::new()),
        }
    }

    /// Set the maximum slots for a specific resource tag.
    pub fn set_tag_limit(&self, tag: &str, max: usize) {
        let mut slots = self.tag_slots.lock().unwrap();
        let entry = slots.entry(tag.to_string()).or_insert((0, 0));
        entry.0 = max;
    }
}

impl WorkerPool for DynamicPool {
    fn acquire(&self, resource_tag: Option<&str>) -> bool {
        let mut global = self.global_active.lock().unwrap();
        if *global >= self.global_max {
            return false;
        }

        if let Some(tag) = resource_tag {
            let mut slots = self.tag_slots.lock().unwrap();
            if let Some((max, active)) = slots.get_mut(tag) {
                if *active >= *max {
                    return false;
                }
                *active += 1;
            }
            // No limit defined for this tag — allow
        }

        *global += 1;
        true
    }

    fn release(&self, resource_tag: Option<&str>) {
        let mut global = self.global_active.lock().unwrap();
        *global = global.saturating_sub(1);

        if let Some(tag) = resource_tag {
            let mut slots = self.tag_slots.lock().unwrap();
            if let Some((_, active)) = slots.get_mut(tag) {
                *active = active.saturating_sub(1);
            }
        }
    }

    fn add_resource(&self, tag: &str) {
        let mut slots = self.tag_slots.lock().unwrap();
        let entry = slots.entry(tag.to_string()).or_insert((0, 0));
        entry.0 += 1;
    }

    fn remove_resource(&self, tag: &str) {
        let mut slots = self.tag_slots.lock().unwrap();
        if let Some((max, _)) = slots.get_mut(tag) {
            *max = max.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_pool() {
        let pool = FixedPool::new(2);
        assert!(pool.acquire(None));
        assert!(pool.acquire(None));
        assert!(!pool.acquire(None)); // full
        pool.release(None);
        assert!(pool.acquire(None)); // slot freed
    }

    #[test]
    fn test_unlimited_pool() {
        let pool = UnlimitedPool;
        for _ in 0..1000 {
            assert!(pool.acquire(None));
        }
    }

    #[test]
    fn test_dynamic_pool_global() {
        let pool = DynamicPool::new(2);
        assert!(pool.acquire(None));
        assert!(pool.acquire(None));
        assert!(!pool.acquire(None));
    }

    #[test]
    fn test_dynamic_pool_tags() {
        let pool = DynamicPool::new(10);
        pool.set_tag_limit("gpu", 1);

        assert!(pool.acquire(Some("gpu")));
        assert!(!pool.acquire(Some("gpu"))); // tag limit reached
        assert!(pool.acquire(Some("cpu"))); // no limit on cpu
        pool.release(Some("gpu"));
        assert!(pool.acquire(Some("gpu"))); // slot freed
    }

    #[test]
    fn test_dynamic_pool_add_remove() {
        let pool = DynamicPool::new(10);
        pool.set_tag_limit("builder", 0);
        assert!(!pool.acquire(Some("builder"))); // no slots

        pool.add_resource("builder"); // now 1 slot
        assert!(pool.acquire(Some("builder")));
        assert!(!pool.acquire(Some("builder")));

        pool.add_resource("builder"); // now 2 slots (1 active)
        assert!(pool.acquire(Some("builder")));
    }
}
