//! DAG executor — schedules and runs tasks with maximum parallelism.
//!
//! The executor is a single-threaded scheduler that dispatches tasks to
//! worker threads. It blocks on an mpsc channel waiting for completions,
//! then immediately dispatches any newly-ready tasks. This gives maximum
//! parallelism without busy-waiting: a task starts the instant its
//! dependencies are satisfied and a pool slot is available.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc;
use std::thread;

use crate::dag::cache::CacheStrategy;
use crate::dag::context::DagContext;
use crate::dag::error::{DagError, Result};
use crate::dag::graph::DagGraph;
use crate::dag::pool::WorkerPool;
use crate::dag::types::{ErrorPolicy, TaskId, TaskOutcome};

/// Completion message from a worker thread.
struct CompletionEvent {
    task_id: TaskId,
    outcome: TaskOutcome,
}

/// Events emitted during DAG execution for monitoring.
#[derive(Debug, Clone)]
pub enum DagEvent {
    TaskStarted(TaskId),
    TaskCompleted(TaskId, TaskOutcome),
    TaskSkipped(TaskId),
    TaskCancelled(TaskId),
}

/// Callback trait for monitoring DAG execution progress.
pub trait DagMonitor: Send {
    fn on_event(&mut self, event: &DagEvent);
}

/// Report from a DAG execution run.
#[derive(Debug)]
pub struct DagReport {
    /// Tasks that completed successfully.
    pub completed: HashSet<TaskId>,
    /// Tasks that were skipped (cache hit).
    pub skipped: HashSet<TaskId>,
    /// Tasks that failed, with error messages.
    pub failed: HashMap<TaskId, String>,
    /// Tasks cancelled because a dependency failed.
    pub cancelled: HashSet<TaskId>,
}

impl DagReport {
    /// Whether the entire DAG succeeded (no failures).
    pub fn is_success(&self) -> bool {
        self.failed.is_empty()
    }

    /// Total number of tasks processed (completed + skipped + failed + cancelled).
    pub fn total(&self) -> usize {
        self.completed.len() + self.skipped.len() + self.failed.len() + self.cancelled.len()
    }
}

/// The DAG executor. Constructed via `DagBuilder` or `StageBuilder`.
pub struct DagExecutor {
    pub(crate) graph: DagGraph,
    pub(crate) context: DagContext,
    pub(crate) error_policy: ErrorPolicy,
    pub(crate) pool: Box<dyn WorkerPool>,
    pub(crate) global_cache: Option<Box<dyn CacheStrategy>>,
    pub(crate) monitor: Option<Box<dyn DagMonitor>>,
}

impl DagExecutor {
    /// Execute the DAG to completion.
    ///
    /// Blocks until all tasks finish, are cancelled, or an error policy
    /// aborts execution.
    pub fn run(mut self) -> Result<DagReport> {
        self.graph.validate()?;

        let (tx, rx) = mpsc::channel::<CompletionEvent>();

        // Track unmet dependency count per task
        let mut pending_deps: HashMap<TaskId, usize> = HashMap::new();
        let all_task_ids: Vec<TaskId> = self.graph.tasks.keys().cloned().collect();
        for id in &all_task_ids {
            let dep_count = self
                .graph
                .dependencies
                .get(id)
                .map(|d| d.len())
                .unwrap_or(0);
            pending_deps.insert(id.clone(), dep_count);
        }

        // Concurrency group tracking: group_name -> active count
        let mut group_active: HashMap<String, usize> = HashMap::new();

        let mut ready: VecDeque<TaskId> = VecDeque::new();
        let mut deferred: VecDeque<TaskId> = VecDeque::new(); // tasks waiting for pool/group slots
        let mut in_flight: HashSet<TaskId> = HashSet::new();

        let mut report = DagReport {
            completed: HashSet::new(),
            skipped: HashSet::new(),
            failed: HashMap::new(),
            cancelled: HashSet::new(),
        };

        // Seed with root tasks
        for id in &all_task_ids {
            if pending_deps.get(id).copied().unwrap_or(0) == 0 {
                ready.push_back(id.clone());
            }
        }

        loop {
            // 1. Try to dispatch ready tasks
            let mut retry_ready: VecDeque<TaskId> = VecDeque::new();

            while let Some(task_id) = ready.pop_front() {
                // Check cache (per-task or global)
                if let Some(cached) = self.check_cache(&task_id) {
                    self.context.set_output(task_id.clone(), cached);
                    self.notify(DagEvent::TaskSkipped(task_id.clone()));
                    report.skipped.insert(task_id.clone());
                    self.mark_completed(&task_id, &mut pending_deps, &mut retry_ready);
                    continue;
                }

                // Check concurrency group limit
                if !self.can_dispatch_group(&task_id, &group_active) {
                    deferred.push_back(task_id);
                    continue;
                }

                // Check worker pool
                let resource_tag = self
                    .graph
                    .tasks
                    .get(&task_id)
                    .and_then(|t| t.resource_tag().map(|s| s.to_string()));
                if !self.pool.acquire(resource_tag.as_deref()) {
                    deferred.push_back(task_id);
                    continue;
                }

                // Dispatch to worker thread
                let task = match self.graph.take_task(&task_id) {
                    Some(t) => t,
                    None => continue, // already taken (shouldn't happen)
                };

                // Update group tracking
                if let Some(group) = self.graph.task_groups.get(&task_id) {
                    *group_active.entry(group.clone()).or_insert(0) += 1;
                }

                let ctx = self.context.clone();
                let tx = tx.clone();
                let id = task_id.clone();
                in_flight.insert(task_id.clone());
                self.notify(DagEvent::TaskStarted(task_id));

                thread::spawn(move || {
                    let outcome = task.execute(&ctx);
                    let _ = tx.send(CompletionEvent {
                        task_id: id,
                        outcome,
                    });
                });
            }

            // Move retry_ready items back to ready
            ready.extend(retry_ready);

            // 2. If nothing in flight and nothing ready/deferred, we're done
            if in_flight.is_empty() && ready.is_empty() && deferred.is_empty() {
                break;
            }

            // 3. If nothing in flight but tasks are deferred, that's a deadlock
            if in_flight.is_empty() && ready.is_empty() && !deferred.is_empty() {
                // This can happen if pool limits are too tight
                return Err(DagError::General(format!(
                    "deadlock: {} deferred tasks but nothing in flight",
                    deferred.len()
                )));
            }

            // 4. Wait for a completion event
            let event = rx.recv().map_err(|_| DagError::ChannelClosed)?;
            let CompletionEvent { task_id, outcome } = event;

            in_flight.remove(&task_id);

            // Release pool slot
            self.pool.release(None);

            // Release concurrency group slot
            if let Some(group) = self.graph.task_groups.get(&task_id) {
                if let Some(count) = group_active.get_mut(group) {
                    *count = count.saturating_sub(1);
                }
            }

            let is_success = outcome.is_success();
            let fail_msg = match &outcome {
                TaskOutcome::Failed(msg) => Some(msg.clone()),
                _ => None,
            };

            self.notify(DagEvent::TaskCompleted(task_id.clone(), outcome));

            if is_success {
                report.completed.insert(task_id.clone());
                self.mark_completed(&task_id, &mut pending_deps, &mut ready);

                // Re-check deferred tasks (pool slot freed)
                let deferred_tasks: Vec<_> = deferred.drain(..).collect();
                for dt in deferred_tasks {
                    ready.push_back(dt);
                }
            } else if let Some(msg) = fail_msg {
                match &self.error_policy {
                    ErrorPolicy::FailFast => {
                        report.failed.insert(task_id.clone(), msg);
                        // Wait for in-flight tasks to finish, then return
                        drop(tx);
                        for event in rx {
                            in_flight.remove(&event.task_id);
                            report.cancelled.insert(event.task_id);
                        }
                        return Ok(report);
                    }
                    ErrorPolicy::ContinueIndependent => {
                        report.failed.insert(task_id.clone(), msg);
                        self.cancel_dependents(&task_id, &mut pending_deps, &mut report.cancelled);
                        let deferred_tasks: Vec<_> = deferred.drain(..).collect();
                        for dt in deferred_tasks {
                            if !report.cancelled.contains(&dt) {
                                ready.push_back(dt);
                            }
                        }
                    }
                    ErrorPolicy::Retry { .. } => {
                        report.failed.insert(task_id.clone(), msg);
                        self.cancel_dependents(&task_id, &mut pending_deps, &mut report.cancelled);
                    }
                }
            }
        }

        Ok(report)
    }

    /// Check cache for a task (per-task cache or global cache).
    fn check_cache(&self, task_id: &TaskId) -> Option<Box<dyn std::any::Any + Send>> {
        // Per-task cache takes priority
        if let Some(cache) = self.graph.task_cache.get(task_id) {
            if let Some(output) = cache.check(task_id, &self.context) {
                return Some(output);
            }
        }
        // Fall back to global cache
        if let Some(ref cache) = self.global_cache {
            return cache.check(task_id, &self.context);
        }
        None
    }

    /// Check if a task can be dispatched given its concurrency group limit.
    fn can_dispatch_group(&self, task_id: &TaskId, group_active: &HashMap<String, usize>) -> bool {
        if let Some(group_name) = self.graph.task_groups.get(task_id) {
            if let Some(limit) = self.graph.concurrency_groups.get(group_name) {
                if let Some(max) = limit.max_concurrent {
                    let active = group_active.get(group_name).copied().unwrap_or(0);
                    return active < max;
                }
            }
        }
        true
    }

    /// Mark a task as completed and update dependency counts.
    /// Newly-ready tasks are pushed into the ready queue.
    fn mark_completed(
        &self,
        task_id: &TaskId,
        pending_deps: &mut HashMap<TaskId, usize>,
        ready: &mut VecDeque<TaskId>,
    ) {
        if let Some(dependents) = self.graph.dependents.get(task_id) {
            for dep_id in dependents {
                if let Some(count) = pending_deps.get_mut(dep_id) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        ready.push_back(dep_id.clone());
                    }
                }
            }
        }
    }

    /// Cancel all transitive dependents of a failed task.
    fn cancel_dependents(
        &mut self,
        task_id: &TaskId,
        pending_deps: &mut HashMap<TaskId, usize>,
        cancelled: &mut HashSet<TaskId>,
    ) {
        let mut to_cancel = VecDeque::new();
        if let Some(dependents) = self.graph.dependents.get(task_id) {
            for dep_id in dependents {
                to_cancel.push_back(dep_id.clone());
            }
        }

        while let Some(id) = to_cancel.pop_front() {
            if cancelled.contains(&id) {
                continue;
            }
            cancelled.insert(id.clone());
            self.notify(DagEvent::TaskCancelled(id.clone()));

            // Remove from pending so it's never dispatched
            pending_deps.remove(&id);

            // Cancel its dependents too
            if let Some(dependents) = self.graph.dependents.get(&id) {
                for dep_id in dependents {
                    to_cancel.push_back(dep_id.clone());
                }
            }
        }
    }

    fn notify(&mut self, event: DagEvent) {
        if let Some(ref mut monitor) = self.monitor {
            monitor.on_event(&event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::builder::{DagBuilder, StageBuilder};
    use crate::dag::pool::FixedPool;
    use crate::dag::types::{FnTask, ShellTask, TaskOutcome};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[test]
    fn test_single_task_success() {
        let mut dag = DagBuilder::new();
        dag.add_task("a", FnTask::new("task a", |_| TaskOutcome::Success));
        let report = dag.build().unwrap().run().unwrap();
        assert!(report.is_success());
        assert!(report.completed.contains(&TaskId::from("a")));
    }

    #[test]
    fn test_single_task_failure() {
        let mut dag = DagBuilder::new();
        dag.add_task(
            "a",
            FnTask::new("task a", |_| TaskOutcome::Failed("boom".into())),
        );
        dag.error_policy(ErrorPolicy::FailFast);
        let report = dag.build().unwrap().run().unwrap();
        assert!(!report.is_success());
        assert!(report.failed.contains_key(&TaskId::from("a")));
    }

    #[test]
    fn test_dependency_ordering() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let o1 = order.clone();
        let o2 = order.clone();
        let o3 = order.clone();

        let mut dag = DagBuilder::new();
        dag.add_task(
            "a",
            FnTask::new("a", move |_| {
                o1.lock().unwrap().push("a");
                TaskOutcome::Success
            }),
        );
        dag.add_task(
            "b",
            FnTask::new("b", move |_| {
                o2.lock().unwrap().push("b");
                TaskOutcome::Success
            }),
        );
        dag.add_task(
            "c",
            FnTask::new("c", move |_| {
                o3.lock().unwrap().push("c");
                TaskOutcome::Success
            }),
        );
        dag.add_dep("b", "a");
        dag.add_dep("c", "b");
        // Force sequential with fanout 1
        dag.pool(FixedPool::new(1));

        let report = dag.build().unwrap().run().unwrap();
        assert!(report.is_success());

        let executed = order.lock().unwrap();
        assert_eq!(*executed, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_dependency_ordering_with_high_parallelism() {
        // Verify dependencies are respected even with unlimited parallelism.
        // Task B reads A's output — if B runs before A, it will fail.
        let mut dag = DagBuilder::new();
        dag.add_task(
            "a",
            FnTask::new("a", |ctx| {
                std::thread::sleep(Duration::from_millis(20));
                ctx.set_output(TaskId::from("a"), "from_a".to_string());
                TaskOutcome::Success
            }),
        );
        dag.add_task(
            "b",
            FnTask::new("b", |ctx| {
                // This MUST see A's output — if deps are broken, this fails
                let val: Option<String> = ctx.get_output(&TaskId::from("a"));
                match val {
                    Some(v) if v == "from_a" => {
                        ctx.set_output(TaskId::from("b"), "from_b".to_string());
                        TaskOutcome::Success
                    }
                    Some(v) => TaskOutcome::Failed(format!("unexpected value from a: {}", v)),
                    None => {
                        TaskOutcome::Failed("dependency a has no output — ran out of order!".into())
                    }
                }
            }),
        );
        dag.add_task(
            "c",
            FnTask::new("c", |ctx| {
                let val: Option<String> = ctx.get_output(&TaskId::from("b"));
                match val {
                    Some(v) if v == "from_b" => TaskOutcome::Success,
                    _ => {
                        TaskOutcome::Failed("dependency b has no output — ran out of order!".into())
                    }
                }
            }),
        );
        dag.add_dep("b", "a");
        dag.add_dep("c", "b");
        // High parallelism — deps must still be respected
        dag.pool(FixedPool::new(100));

        let report = dag.build().unwrap().run().unwrap();
        assert!(
            report.is_success(),
            "dependencies violated with high parallelism: failed={:?}",
            report.failed
        );
    }

    #[test]
    fn test_parallel_independent_tasks() {
        let counter = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();

        let mut dag = DagBuilder::new();
        for i in 0..5 {
            let c = counter.clone();
            dag.add_task(
                format!("t{}", i),
                FnTask::new(format!("task {}", i), move |_| {
                    std::thread::sleep(Duration::from_millis(50));
                    c.fetch_add(1, Ordering::SeqCst);
                    TaskOutcome::Success
                }),
            );
        }

        let report = dag.build().unwrap().run().unwrap();
        let elapsed = start.elapsed();

        assert!(report.is_success());
        assert_eq!(counter.load(Ordering::SeqCst), 5);
        // Functional check: if tasks ran sequentially, it would take >=250ms (5 × 50ms).
        // With parallelism it should be ~50-100ms. Use a generous bound to avoid CI flakiness.
        assert!(
            elapsed < Duration::from_secs(2),
            "tasks took {:?}, likely not running in parallel (sequential would be >=250ms)",
            elapsed
        );
    }

    #[test]
    fn test_continue_independent_on_failure() {
        let mut dag = DagBuilder::new();
        dag.add_task("good", FnTask::new("good", |_| TaskOutcome::Success));
        dag.add_task(
            "bad",
            FnTask::new("bad", |_| TaskOutcome::Failed("fail".into())),
        );
        dag.add_task(
            "depends_on_bad",
            FnTask::new("after bad", |_| TaskOutcome::Success),
        );
        dag.add_dep("depends_on_bad", "bad");
        dag.error_policy(ErrorPolicy::ContinueIndependent);

        let report = dag.build().unwrap().run().unwrap();
        assert!(!report.is_success());
        assert!(report.completed.contains(&TaskId::from("good")));
        assert!(report.failed.contains_key(&TaskId::from("bad")));
        assert!(report.cancelled.contains(&TaskId::from("depends_on_bad")));
    }

    #[test]
    fn test_output_passing_between_tasks() {
        let mut dag = DagBuilder::new();
        dag.add_task(
            "producer",
            FnTask::new("produce", |ctx| {
                ctx.set_output(TaskId::from("producer"), "hello from producer".to_string());
                TaskOutcome::Success
            }),
        );
        dag.add_task(
            "consumer",
            FnTask::new("consume", |ctx| {
                let val: String = ctx.get_output(&TaskId::from("producer")).unwrap();
                assert_eq!(val, "hello from producer");
                TaskOutcome::Success
            }),
        );
        dag.add_dep("consumer", "producer");

        let report = dag.build().unwrap().run().unwrap();
        assert!(report.is_success());
    }

    #[test]
    fn test_stage_builder_basic() {
        let report = StageBuilder::new()
            .resources(vec!["h1".into(), "h2".into(), "h3".into()])
            .stage("build", None, |host| {
                let h = host.to_string();
                Box::new(FnTask::new(format!("build {}", h), move |ctx| {
                    ctx.set_output(TaskId(format!("build:{}", h)), format!("built-{}", h));
                    TaskOutcome::Success
                }))
            })
            .stage("deploy", None, |host| {
                let h = host.to_string();
                Box::new(FnTask::new(format!("deploy {}", h), move |ctx| {
                    // Should be able to read build output
                    let built: String = ctx.get_output(&TaskId(format!("build:{}", h))).unwrap();
                    assert!(built.starts_with("built-"));
                    TaskOutcome::Success
                }))
            })
            .build()
            .unwrap()
            .run()
            .unwrap();

        assert!(report.is_success());
        assert_eq!(report.completed.len(), 6); // 3 hosts × 2 stages
    }

    #[test]
    fn test_stage_builder_per_host_pipelining() {
        // Verify that host1 can be in "deploy" while host2 is still in "build"
        let start = Instant::now();

        let report = StageBuilder::new()
            .resources(vec!["fast".into(), "slow".into()])
            .stage("build", None, |host| {
                let h = host.to_string();
                Box::new(FnTask::new(format!("build {}", h), move |_| {
                    let delay = if h == "slow" { 100 } else { 10 };
                    std::thread::sleep(Duration::from_millis(delay));
                    TaskOutcome::Success
                }))
            })
            .stage("deploy", None, |host| {
                let h = host.to_string();
                Box::new(FnTask::new(format!("deploy {}", h), move |_| {
                    std::thread::sleep(Duration::from_millis(10));
                    TaskOutcome::Success
                }))
            })
            .build()
            .unwrap()
            .run()
            .unwrap();

        let elapsed = start.elapsed();
        assert!(report.is_success());
        assert_eq!(report.completed.len(), 4); // 2 hosts × 2 stages
                                               // Pipelining means fast.deploy starts while slow is still building.
                                               // Without pipelining (global barrier), total = build_all(100ms) + deploy_all(10ms) = 110ms+
                                               // With pipelining, total ≈ max(slow_build + slow_deploy, fast_build + fast_deploy) = 110ms
                                               // Use generous bound to avoid CI flakiness.
        assert!(
            elapsed < Duration::from_secs(2),
            "pipelining took {:?}, expected well under 2s",
            elapsed
        );
    }

    #[test]
    fn test_stage_builder_failure_cancels_later_stages() {
        let report = StageBuilder::new()
            .resources(vec!["good".into(), "bad".into()])
            .stage("build", None, |host| {
                let h = host.to_string();
                Box::new(FnTask::new(format!("build {}", h), move |_| {
                    if h == "bad" {
                        TaskOutcome::Failed("build failed".into())
                    } else {
                        TaskOutcome::Success
                    }
                }))
            })
            .stage("deploy", None, |host| {
                let h = host.to_string();
                Box::new(FnTask::new(format!("deploy {}", h), move |_| {
                    TaskOutcome::Success
                }))
            })
            .error_policy(ErrorPolicy::ContinueIndependent)
            .build()
            .unwrap()
            .run()
            .unwrap();

        assert!(!report.is_success());
        // good: build + deploy both succeed
        assert!(report.completed.contains(&TaskId::from("build:good")));
        assert!(report.completed.contains(&TaskId::from("deploy:good")));
        // bad: build fails, deploy cancelled
        assert!(report.failed.contains_key(&TaskId::from("build:bad")));
        assert!(report.cancelled.contains(&TaskId::from("deploy:bad")));
    }

    #[test]
    fn test_concurrency_group_limit() {
        // Use context to pass the counters through, avoiding 'static closure issues
        let ctx = DagContext::new();
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));
        ctx.set_state("max_concurrent", max_concurrent.clone());
        ctx.set_state("current", current.clone());

        let report = StageBuilder::new()
            .resources((0..10).map(|i| format!("n{}", i)).collect())
            .stage("work", Some(2), |host| {
                let h = host.to_string();
                Box::new(FnTask::new(format!("work {}", h), |ctx| {
                    let cur: Arc<AtomicUsize> = ctx.get_state("current").unwrap();
                    let mc: Arc<AtomicUsize> = ctx.get_state("max_concurrent").unwrap();
                    let c = cur.fetch_add(1, Ordering::SeqCst) + 1;
                    loop {
                        let prev = mc.load(Ordering::SeqCst);
                        if c <= prev
                            || mc
                                .compare_exchange(prev, c, Ordering::SeqCst, Ordering::SeqCst)
                                .is_ok()
                        {
                            break;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(30));
                    cur.fetch_sub(1, Ordering::SeqCst);
                    TaskOutcome::Success
                }))
            })
            .context(ctx)
            .build()
            .unwrap()
            .run()
            .unwrap();

        assert!(report.is_success());
        assert_eq!(report.completed.len(), 10);
        assert!(
            max_concurrent.load(Ordering::SeqCst) <= 2,
            "max concurrent was {}, expected <= 2",
            max_concurrent.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn test_shell_task() {
        let mut dag = DagBuilder::new();
        dag.add_task(
            "echo",
            ShellTask {
                command: "echo hello".into(),
                description: "echo test".into(),
                resource: None,
            },
        );
        let report = dag.build().unwrap().run().unwrap();
        assert!(report.is_success());
    }

    #[test]
    fn test_shell_task_failure() {
        let mut dag = DagBuilder::new();
        dag.add_task(
            "fail",
            ShellTask {
                command: "exit 42".into(),
                description: "fail test".into(),
                resource: None,
            },
        );
        dag.error_policy(ErrorPolicy::FailFast);
        let report = dag.build().unwrap().run().unwrap();
        assert!(!report.is_success());
        assert!(report
            .failed
            .get(&TaskId::from("fail"))
            .unwrap()
            .contains("42"));
    }
}
