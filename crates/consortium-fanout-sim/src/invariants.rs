//! Reusable cascade invariant assertions.
//!
//! Extracted from `tests/fuzz.rs` so the fuzz harness, the curated
//! corpus (`tests/corpus.rs`), and later simulator stages all assert
//! the same contract with the same messages. Every assertion panics
//! with the strategy name and scenario seed so a failure can be
//! replayed without digging through logs.
//!
//! These are plain `assert!`-based helpers (not proptest's
//! `prop_assert!`): a violated invariant is a hard failure either way,
//! and panics keep them usable from both `proptest!` blocks and plain
//! `#[test]` corpus tests.

use std::collections::HashSet;

use consortium_nix::cascade::{CascadeResult, CascadeStrategy, NodeId};

use crate::scenario::{Scenario, ScenarioConfig};

/// Every node converged and the run reported success.
pub fn assert_converged_all(strategy: &str, seed: u64, result: &CascadeResult, n_nodes: u32) {
    assert!(
        result.is_success(),
        "[{strategy}] seed={seed:#x} failed unexpectedly: {:?}",
        result.failed,
    );
    assert_eq!(
        result.converged.len() as u32,
        n_nodes,
        "[{strategy}] seed={seed:#x} not all nodes converged ({} of {n_nodes})",
        result.converged.len(),
    );
}

/// The converged list contains no duplicates.
pub fn assert_no_duplicate_converged(strategy: &str, seed: u64, result: &CascadeResult) {
    let converged_set: HashSet<NodeId> = result.converged.iter().copied().collect();
    assert_eq!(
        converged_set.len(),
        result.converged.len(),
        "[{strategy}] seed={seed:#x} duplicates in converged list",
    );
}

/// Round count stays within the coordinator's `max_rounds` bound and
/// `round_durations` has exactly one entry per round.
///
/// The bound is deliberately `max_rounds`, not `n_nodes`: since the
/// coordinator stopped marking targets permanently failed on transient
/// edge failures, an unlucky failure pattern can legitimately exceed
/// `n_nodes` rounds while the strategy retries alternate sources.
pub fn assert_round_bounds(strategy: &str, seed: u64, result: &CascadeResult, max_rounds: u32) {
    assert!(
        result.rounds <= max_rounds,
        "[{strategy}] seed={seed:#x} rounds {} > max_rounds {max_rounds}",
        result.rounds,
    );
    assert_eq!(
        result.round_durations.len() as u32,
        result.rounds,
        "[{strategy}] seed={seed:#x} round_durations len mismatch",
    );
}

/// Seed-aware semantics for [`crate::fixtures::FailureSchedule::KillNodeAtRound`]
/// injected at round 0.
///
/// The kill schedule fails edges whose TARGET is the killed node
/// (receive-side death). Whether the killed node must fail therefore
/// depends on whether it was pre-seeded:
///
/// - `killed ∈ seeded`: the node starts converged and never needs an
///   inbound copy, so the kill can never fire. It MUST remain in
///   `converged` (and keeps serving as a source), and no failure is
///   required in the result.
/// - `killed ∉ seeded`: every attempt to deliver the closure to it
///   fails from round 0. It MUST NOT appear in `converged`, and
///   `result.failed` MUST name it in `affected_nodes()`.
///
/// The first branch is what the pre-2025 fuzz oracle got wrong: it
/// asserted "killed ∉ converged" unconditionally, which is
/// wrong-by-construction whenever the draw picks the pre-seeded node
/// (`NodeId(0)` under `SeedDistribution::Single`) — the root cause of
/// the historical "killed node still appears in converged set" flake.
pub fn assert_killed_node_semantics(
    strategy: &str,
    seed: u64,
    result: &CascadeResult,
    killed: NodeId,
    seeded: &HashSet<NodeId>,
) {
    if seeded.contains(&killed) {
        assert!(
            result.converged.contains(&killed),
            "[{strategy}] seed={seed:#x} pre-seeded node {killed:?} lost the closure",
        );
    } else {
        assert!(
            !result.converged.contains(&killed),
            "[{strategy}] seed={seed:#x} killed node {killed:?} still appears in converged \
             set: {:?}",
            result.converged,
        );
        let err = result.failed.as_ref().unwrap_or_else(|| {
            panic!(
                "[{strategy}] seed={seed:#x} killed node injected at round 0 but \
                 result.failed is None"
            )
        });
        assert!(
            err.affected_nodes().contains(&killed),
            "[{strategy}] seed={seed:#x} killed node {killed:?} missing from affected set",
        );
    }
}

/// Running the same scenario twice with the same strategy must produce
/// identical round counts, per-round durations, and converged sets.
///
/// Compares full converged SETS, not just counts: same count with a
/// different set would mean the cascade makes non-deterministic edge
/// choices that a `len ==` check would never notice.
pub fn assert_deterministic(cfg: &ScenarioConfig, strategy: &dyn CascadeStrategy) {
    let r1 = Scenario::new(cfg.clone()).run(strategy);
    let r2 = Scenario::new(cfg.clone()).run(strategy);
    assert_eq!(
        r1.rounds, r2.rounds,
        "[{}] seed={:#x} round count differs between identical runs",
        strategy.name(),
        cfg.seed,
    );
    assert_eq!(
        r1.round_durations, r2.round_durations,
        "[{}] seed={:#x} round durations differ between identical runs",
        strategy.name(),
        cfg.seed,
    );
    let s1: HashSet<NodeId> = r1.converged.iter().copied().collect();
    let s2: HashSet<NodeId> = r2.converged.iter().copied().collect();
    assert_eq!(
        s1, s2,
        "[{}] seed={:#x} converged sets diverge between identical-seed runs",
        strategy.name(),
        cfg.seed,
    );
}
