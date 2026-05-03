//! DAG executor microbench — the canonical autoresearch metric.
//!
//! Exercises `crates/consortium/src/dag/executor.rs` in isolation, with
//! synthetic in-memory `FnTask`s that do nothing but return Success. NO
//! Docker, NO SSH, NO subprocess — every nanosecond we measure here is
//! `executor.rs` doing its job.
//!
//! This replaces a Docker-based wall-time metric whose noise floor was
//! ~98% Docker startup + SSH handshake (verified by an empirical audit
//! 2026-05-03). At 33 nodes the inner DAG work was 0.2s out of 11.3s
//! total iteration cost — a 50% executor speedup would have moved the
//! metric by 0.0035% wall-clock, well below jitter.
//!
//! Three scenarios, each parameterised by node count:
//!
//! - `flat_<N>`: N independent tasks, no dependencies. Stresses the
//!   ready-queue / dispatch loop / completion handler. Primary
//!   autoresearch signal — small enough to iterate on every commit,
//!   dominated by code in `executor.rs`.
//!
//! - `chain_<N>`: N tasks where each depends on the previous. Stresses
//!   the dependency-tracking + mark_completed paths. Critical-path
//!   sensitive: edits to the deferred-queue draining logic show up here.
//!
//! - `groups_<N>`: 3 concurrency groups of N/3 tasks each, group caps
//!   at N/6. Stresses the group-throttling check inside the dispatch
//!   loop. Anything touching `concurrency_groups.get()` lives here.
//!
//! Run: `cargo bench -p consortium-crate --bench dag_executor`.
//! Result: criterion writes `target/criterion/<bench>/new/estimates.json`
//! containing mean / median / stddev in nanoseconds.

use consortium::dag::{DagBuilder, FnTask, TaskOutcome};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn flat_dag(n: usize) {
    let mut b = DagBuilder::new();
    for i in 0..n {
        b.add_task(
            format!("task-{i}"),
            FnTask::new(format!("flat #{i}"), |_ctx| TaskOutcome::Success),
        );
    }
    let dag = b.build().expect("build");
    let report = dag.run().expect("run");
    debug_assert_eq!(report.completed.len(), n);
    debug_assert!(report.failed.is_empty());
}

fn chain_dag(n: usize) {
    let mut b = DagBuilder::new();
    for i in 0..n {
        b.add_task(
            format!("task-{i}"),
            FnTask::new(format!("chain #{i}"), |_ctx| TaskOutcome::Success),
        );
        if i > 0 {
            b.add_dep(format!("task-{i}"), format!("task-{}", i - 1));
        }
    }
    let dag = b.build().expect("build");
    let report = dag.run().expect("run");
    debug_assert_eq!(report.completed.len(), n);
}

fn groups_dag(n: usize) {
    let group_count = 3;
    let per_group = n / group_count;
    let cap = (per_group / 2).max(1);

    let mut b = DagBuilder::new();
    for g in 0..group_count {
        let group_name = format!("g{g}");
        b.concurrency_group(&group_name, cap);
        for i in 0..per_group {
            let id = format!("g{g}-task-{i}");
            b.add_task(
                &id,
                FnTask::new(format!("g{g} #{i}"), |_ctx| TaskOutcome::Success),
            );
            b.assign_group(&id, &group_name);
        }
    }
    let dag = b.build().expect("build");
    let report = dag.run().expect("run");
    debug_assert_eq!(report.completed.len(), per_group * group_count);
}

fn bench_dag(c: &mut Criterion) {
    let mut g = c.benchmark_group("dag_executor");
    // Bound each measurement so an autoresearch iteration completes in
    // well under the per-iter timeout. Criterion re-derives sample count;
    // default ~100 samples is fine.
    g.measurement_time(std::time::Duration::from_secs(5));

    for &n in &[33usize, 100, 300] {
        g.bench_with_input(BenchmarkId::new("flat", n), &n, |b, &n: &usize| {
            b.iter(|| flat_dag(n));
        });
        g.bench_with_input(BenchmarkId::new("chain", n), &n, |b, &n: &usize| {
            b.iter(|| chain_dag(n));
        });
        g.bench_with_input(BenchmarkId::new("groups", n), &n, |b, &n: &usize| {
            b.iter(|| groups_dag(n));
        });
    }

    g.finish();
}

criterion_group!(benches, bench_dag);
criterion_main!(benches);
