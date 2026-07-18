//! Property-based fuzz tests for cascade strategies.
//!
//! Generates random scenarios across the cross-product of
//! `(n_nodes, seed_fraction, bandwidth_dist, failures, strategy)` and
//! asserts universal invariants. Each generated case is reproducible
//! from its proptest seed.
//!
//! Run with `PROPTEST_CASES=N` to control case count (default 256).
//! CI should set `PROPTEST_CASES=32` for ~2 min runs; local
//! exploration can use 1024+.
//!
//! ## Regression discipline
//!
//! `fuzz.proptest-regressions` (next to this file) is CHECKED IN to
//! git. Proptest replays every seed line in it before generating new
//! cases, so historical failures are re-exercised on every run. When a
//! NEW failure is found:
//!
//! 1. Let proptest shrink it, then commit the updated regressions
//!    file so the failure replays everywhere.
//! 2. Minimize the case by hand and convert it into an explicit
//!    deterministic `#[test]` in `tests/corpus.rs` (with the
//!    seed/parameters in a comment), so the case is documented and
//!    runs without proptest.
//!
//! ## Oracle note (the 2025 flake)
//!
//! Assertions here are SEED-AWARE. With `seed_fraction: 0.0`
//! (`SeedDistribution::Single`), `NodeId(0)` is always pre-seeded — it
//! starts converged and never needs an inbound copy, so
//! `KillNodeAtRound` can never fire on it. The old unconditional
//! "killed ∉ converged" assertion was wrong-by-construction for that
//! draw and caused the intermittent
//! `killed node NodeId(0) still appears in converged set` failure.
//! See [`invariants::assert_killed_node_semantics`] and
//! `tests/corpus.rs` for the pinned historical repros.

use std::collections::HashSet;

use consortium_fanout_sim::{
    fixtures::{BandwidthDistribution, FailureSchedule},
    invariants,
    scenario::{Scenario, ScenarioConfig},
};
use consortium_nix::cascade::{CascadeStrategy, Log2FanOut, NodeId};
use consortium_nix::cascade_strategies::{MaxBottleneckSpanning, SteinerGreedy};
use proptest::prelude::*;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

fn pick_strategy(idx: u8) -> &'static dyn CascadeStrategy {
    match idx % 3 {
        0 => &Log2FanOut,
        1 => &MaxBottleneckSpanning,
        _ => &SteinerGreedy,
    }
}

fn bandwidth_strategy() -> impl Strategy<Value = BandwidthDistribution> {
    prop_oneof![
        // Uniform: bandwidth in [10 MB/s, 1 GB/s]
        (10u64 * 1024 * 1024..1024 * 1024 * 1024).prop_map(BandwidthDistribution::Uniform),
        // Bimodal: slow << fast, fast_fraction varies
        (
            1024u64 * 1024..50 * 1024 * 1024,
            100u64 * 1024 * 1024..2 * 1024 * 1024 * 1024,
            0.05f64..0.95,
        )
            .prop_map(
                |(slow, fast, fast_fraction)| BandwidthDistribution::Bimodal {
                    slow,
                    fast,
                    fast_fraction,
                }
            ),
    ]
}

proptest! {
    // Aggressive but reasonable bound — cases should run in <100ms
    // each since the sim is deterministic + in-process. The case count
    // is deliberately NOT set here: the default (256) honors
    // PROPTEST_CASES, and hardcoding `cases` would silently ignore the
    // env var (ProptestConfig::default() is what reads it).
    #![proptest_config(ProptestConfig {
        max_shrink_iters: 32,
        .. ProptestConfig::default()
    })]

    #[test]
    fn cascade_universal_invariants_hold(
        seed in 0u64..u64::MAX,
        n_nodes in 8u32..=128,
        seed_fraction in 0.0f64..=0.5,
        closure_mb in 1u64..=200,
        bandwidth in bandwidth_strategy(),
        strategy_idx in 0u8..=2,
    ) {
        let strategy = pick_strategy(strategy_idx);
        let cfg = ScenarioConfig {
            seed,
            n_nodes,
            seed_fraction,
            closure_bytes: closure_mb * 1024 * 1024,
            bandwidth,
            uplinks: None,
            failures: FailureSchedule::None,
            max_rounds: 64,
        };
        let result = Scenario::new(cfg.clone()).run(strategy);

        // Universal invariants under no-failure scenarios.
        invariants::assert_converged_all(strategy.name(), seed, &result, cfg.n_nodes);
        invariants::assert_no_duplicate_converged(strategy.name(), seed, &result);
        invariants::assert_round_bounds(strategy.name(), seed, &result, cfg.max_rounds);
    }

    #[test]
    fn cascade_invariants_with_failures(
        seed in 0u64..u64::MAX,
        n_nodes in 16u32..=64,
        bandwidth in bandwidth_strategy(),
        strategy_idx in 0u8..=2,
        failure_seed in 0u64..u64::MAX,
    ) {
        let strategy = pick_strategy(strategy_idx);
        let mut frng = ChaCha8Rng::seed_from_u64(failure_seed);
        // Sample a failure deterministically from this case's seed —
        // proptest's strategy combinators don't compose with our n_nodes
        // dependency cleanly, so we sample manually.
        //
        // Kill candidates are drawn from ALL nodes, including the seed:
        // with seed_fraction = 0.0 (SeedDistribution::Single) NodeId(0)
        // is always pre-seeded, and drawing it exercises the seed-aware
        // oracle's "killing a pre-seeded node is a no-op" branch.
        // Coverage whose intent is strictly "the killed node must fail"
        // uses NON-SEED candidates instead — a pre-seeded node can
        // never fail, which is exactly what made the old unconditional
        // oracle wrong-by-construction (the historical
        // "[max-bottleneck-spanning] killed node NodeId(0) still
        // appears in converged set" flake). See
        // tests/corpus.rs::kill_nonseed_at_round_zero_*.
        let failure_kind: u8 = frng.gen_range(0u8..=2);
        let killed_node = match failure_kind {
            1 => Some(NodeId(frng.gen_range(0..n_nodes))),
            _ => None,
        };
        let failures = match failure_kind {
            0 => FailureSchedule::None,
            1 => FailureSchedule::KillNodeAtRound {
                node: killed_node.unwrap(),
                round: frng.gen_range(0..6),
            },
            _ => {
                let s = frng.gen_range(0..n_nodes);
                let mut t = frng.gen_range(0..n_nodes);
                if t == s {
                    t = (t + 1) % n_nodes;
                }
                FailureSchedule::PartitionAtRound {
                    src: NodeId(s),
                    tgt: NodeId(t),
                    round: frng.gen_range(0..6),
                }
            }
        };

        let cfg = ScenarioConfig {
            seed,
            n_nodes,
            seed_fraction: 0.0,
            closure_bytes: 10 * 1024 * 1024,
            bandwidth,
            uplinks: None,
            failures,
            max_rounds: 64,
        };
        let result = Scenario::new(cfg.clone()).run(strategy);

        // Even with failures, sanity bounds must hold.
        invariants::assert_no_duplicate_converged(strategy.name(), seed, &result);
        invariants::assert_round_bounds(strategy.name(), seed, &result, cfg.max_rounds);
        // If failed Some, every affected node id must be valid.
        if let Some(err) = &result.failed {
            for nid in err.affected_nodes() {
                prop_assert!(
                    nid.0 < cfg.n_nodes,
                    "[{}] error references invalid node {}",
                    strategy.name(),
                    nid
                );
            }
        }

        // Tightened and seed-aware: when KillNodeAtRound was injected
        // with round=0, the kill's effect is strategy-independent, so
        // assert exact semantics — a PRE-SEEDED killed node stays
        // converged (the kill can never fire on it); a NON-SEED killed
        // node must fail and must be named in the error tree. Only
        // round 0 is checked: round > 0 may not fire if the cascade
        // halts before then (which is valid for Steiner on uniform).
        if let Some(killed) = killed_node {
            if let FailureSchedule::KillNodeAtRound { round: 0, .. } = cfg.failures {
                // seed_fraction = 0.0 → SeedDistribution::Single → the
                // seeded set is always exactly {NodeId(0)}.
                let seeded: HashSet<NodeId> = std::iter::once(NodeId(0)).collect();
                invariants::assert_killed_node_semantics(
                    strategy.name(),
                    seed,
                    &result,
                    killed,
                    &seeded,
                );
            }
        }
    }

    #[test]
    fn scenario_is_deterministic_in_seed(
        seed in 0u64..u64::MAX,
        n_nodes in 8u32..=64,
    ) {
        let cfg = ScenarioConfig {
            seed,
            n_nodes,
            seed_fraction: 0.0,
            closure_bytes: 10 * 1024 * 1024,
            bandwidth: BandwidthDistribution::Bimodal {
                slow: 5 * 1024 * 1024,
                fast: 500 * 1024 * 1024,
                fast_fraction: 0.4,
            },
            uplinks: None,
            failures: FailureSchedule::None,
            max_rounds: 32,
        };
        // Tightened: full set equality, not just len(). Catches the case
        // where determinism produces the same COUNT of converged nodes
        // but a different SET — which would mean the cascade is making
        // non-deterministic edge choices we'd never notice with `len ==`.
        invariants::assert_deterministic(&cfg, &MaxBottleneckSpanning);
    }
}
