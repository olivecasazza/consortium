//! Curated deterministic corpus — explicit scenarios pinned from
//! historical fuzz failures and from edge-case semantics that deserve
//! per-strategy coverage. No proptest here: every test is a plain
//! `#[test]`, so it runs identically on every machine, every time.
//!
//! Discipline: when a fuzz run finds a NEW failure, commit the updated
//! `fuzz.proptest-regressions` line AND minimize the case into an
//! explicit test here (seed/parameters in a comment).

use std::collections::HashSet;

use consortium_fanout_sim::{
    fixtures::{BandwidthDistribution, FailureSchedule},
    invariants,
    scenario::{Scenario, ScenarioConfig},
};
use consortium_nix::cascade::{CascadeStrategy, Log2FanOut, NodeId};
use consortium_nix::cascade_strategies::{MaxBottleneckSpanning, SteinerGreedy};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

fn strategies() -> Vec<&'static dyn CascadeStrategy> {
    vec![&Log2FanOut, &MaxBottleneckSpanning, &SteinerGreedy]
}

/// Uniform 100 MB/s, no uplinks, 10 MB closure — the boring baseline
/// every corpus case starts from.
fn base_cfg(seed: u64, n_nodes: u32) -> ScenarioConfig {
    ScenarioConfig {
        seed,
        n_nodes,
        seed_fraction: 0.0,
        closure_bytes: 10 * 1024 * 1024,
        bandwidth: BandwidthDistribution::Uniform(100 * 1024 * 1024),
        uplinks: None,
        failures: FailureSchedule::None,
        max_rounds: 64,
    }
}

/// seed_fraction = 0.0 → SeedDistribution::Single → seeded = {NodeId(0)}.
fn single_seed() -> HashSet<NodeId> {
    std::iter::once(NodeId(0)).collect()
}

// ============================================================================
// Historical flake repros (the "killed node NodeId(0)" incident)
// ============================================================================
//
// Recorded failing draw: proptest drew n_nodes=40,
// bandwidth=Uniform(332786153), failure_seed=14146545083572743443,
// and failed under max-bottleneck-spanning (a steiner-greedy variant
// was also recorded). A `*.proptest-regressions` file captured seed
// 3185599403 but was deleted before being committed.
//
// Root cause was the ORACLE, not the cascade: the draw killed
// NodeId(0), which — with seed_fraction=0.0 — is always the pre-seeded
// node. It starts converged and never needs an inbound copy, so
// KillNodeAtRound can never fire on it. These tests pin the draw and
// assert the CORRECT (seed-aware) semantics, so the historical case
// now runs as a PASSING regression.
//
// The incident's scenario `seed` was not recorded; with
// seed_fraction=0.0 and Uniform bandwidth the scenario rng is never
// consumed, so the case is seed-independent.

const HIST_N_NODES: u32 = 40;
const HIST_BANDWIDTH: u64 = 332786153;
const HIST_FAILURE_SEED: u64 = 14146545083572743443;

/// Replay the incident's failure draw exactly the way
/// `fuzz.rs::cascade_invariants_with_failures` computes it, and pin
/// the result: kill NodeId(0) — the pre-seeded node — from round 0.
fn historical_failure_draw() -> (NodeId, u32) {
    let mut frng = ChaCha8Rng::seed_from_u64(HIST_FAILURE_SEED);
    let failure_kind: u8 = frng.gen_range(0u8..=2);
    assert_eq!(failure_kind, 1, "incident draw pinned: a node-kill failure");
    let killed = NodeId(frng.gen_range(0..HIST_N_NODES));
    let round = frng.gen_range(0..6);
    assert_eq!(
        (killed, round),
        (NodeId(0), 0),
        "incident draw pinned: killed the pre-seeded node at round 0"
    );
    (killed, round)
}

fn run_historical_case(strategy: &dyn CascadeStrategy) {
    let (killed, round) = historical_failure_draw();
    let cfg = ScenarioConfig {
        bandwidth: BandwidthDistribution::Uniform(HIST_BANDWIDTH),
        failures: FailureSchedule::KillNodeAtRound { node: killed, round },
        ..base_cfg(0, HIST_N_NODES)
    };
    let result = Scenario::new(cfg.clone()).run(strategy);
    // Seed-aware semantics: the killed node IS the seed — it stays
    // converged, and the whole fleet converges with no failure.
    invariants::assert_killed_node_semantics(
        strategy.name(),
        cfg.seed,
        &result,
        killed,
        &single_seed(),
    );
    invariants::assert_converged_all(strategy.name(), cfg.seed, &result, HIST_N_NODES);
    invariants::assert_no_duplicate_converged(strategy.name(), cfg.seed, &result);
    invariants::assert_round_bounds(strategy.name(), cfg.seed, &result, cfg.max_rounds);
    invariants::assert_deterministic(&cfg, strategy);
}

#[test]
fn historical_repro_seed_kill_is_noop_max_bottleneck() {
    run_historical_case(&MaxBottleneckSpanning);
}

#[test]
fn historical_repro_seed_kill_is_noop_steiner() {
    // Same failure draw as the max-bottleneck repro: the recorded
    // incident's steiner-greedy variant is the same mechanism — a
    // seed-kill is strategy-independent because the seed never needs
    // an inbound copy under any strategy.
    run_historical_case(&SteinerGreedy);
}

// ============================================================================
// Seed-kill semantics (explicit contract test)
// ============================================================================

/// Killing the pre-seeded node at round 0 is a no-op: it remains
/// converged and keeps serving as the source; the whole fleet
/// converges.
///
/// KillNodeAtRound fails only INBOUND edges (receive-side death). See
/// the variant doc in fixtures.rs for why outbound edges deliberately
/// keep succeeding — failing them with `Activation` errors would make
/// the coordinator dead-letter innocent targets.
#[test]
fn seed_kill_at_round_zero_still_converges_fleet() {
    for s in strategies() {
        let cfg = ScenarioConfig {
            failures: FailureSchedule::KillNodeAtRound {
                node: NodeId(0),
                round: 0,
            },
            ..base_cfg(0x5eed, 32)
        };
        let result = Scenario::new(cfg.clone()).run(s);
        invariants::assert_killed_node_semantics(
            s.name(),
            cfg.seed,
            &result,
            NodeId(0),
            &single_seed(),
        );
        invariants::assert_converged_all(s.name(), cfg.seed, &result, 32);
        invariants::assert_no_duplicate_converged(s.name(), cfg.seed, &result);
        invariants::assert_round_bounds(s.name(), cfg.seed, &result, cfg.max_rounds);
    }
}

// ============================================================================
// Kill a NON-SEED node at round 0 — one test per strategy
// ============================================================================

/// Shared body: killing NodeId(15) (never seeded) at round 0 must
/// dead-letter exactly that node — it never converges, it is the only
/// unique node named in the error tree, and the other 31 nodes
/// converge. This is the strict "node must fail" intent the fuzz
/// oracle exercises on non-seed draws.
fn assert_kill_nonseed_at_round_zero(strategy: &dyn CascadeStrategy) {
    let killed = NodeId(15);
    let cfg = ScenarioConfig {
        failures: FailureSchedule::KillNodeAtRound { node: killed, round: 0 },
        ..base_cfg(4, 32)
    };
    let result = Scenario::new(cfg.clone()).run(strategy);
    invariants::assert_killed_node_semantics(
        strategy.name(),
        cfg.seed,
        &result,
        killed,
        &single_seed(),
    );
    assert_eq!(
        result.converged.len() as u32,
        31,
        "[{}] only the killed node should fail to converge; got {} of 32",
        strategy.name(),
        result.converged.len(),
    );
    let err = result
        .failed
        .as_ref()
        .expect("killed non-seed node must produce a failure");
    // Retries from alternate sources may name the killed node several
    // times; the UNIQUE affected set must be exactly {killed}.
    let unique: HashSet<NodeId> = err.affected_nodes().into_iter().collect();
    let expected: HashSet<NodeId> = std::iter::once(killed).collect();
    assert_eq!(
        unique,
        expected,
        "[{}] affected set must contain exactly the killed node",
        strategy.name(),
    );
    invariants::assert_no_duplicate_converged(strategy.name(), cfg.seed, &result);
    invariants::assert_round_bounds(strategy.name(), cfg.seed, &result, cfg.max_rounds);
}

#[test]
fn kill_nonseed_at_round_zero_log2() {
    assert_kill_nonseed_at_round_zero(&Log2FanOut);
}

#[test]
fn kill_nonseed_at_round_zero_max_bottleneck() {
    assert_kill_nonseed_at_round_zero(&MaxBottleneckSpanning);
}

#[test]
fn kill_nonseed_at_round_zero_steiner() {
    assert_kill_nonseed_at_round_zero(&SteinerGreedy);
}

// ============================================================================
// Partition at round 0
// ============================================================================

/// PartitionAtRound{0 -> 1, round 0}: the (0,1) edge fails with a
/// Partitioned error whenever it is attempted. Partitioned is
/// non-transient, so IF (0,1) is attempted before node 1 converges the
/// coordinator permanently dead-letters node 1; if the strategy never
/// attempts (0,1), node 1 converges via another source. Either way at
/// most ONE node may be lost, and any loss must name node 1.
#[test]
fn partition_at_round_zero_blocks_at_most_one_node() {
    for s in strategies() {
        let cfg = ScenarioConfig {
            failures: FailureSchedule::PartitionAtRound {
                src: NodeId(0),
                tgt: NodeId(1),
                round: 0,
            },
            ..base_cfg(0x9a17, 32)
        };
        let result = Scenario::new(cfg.clone()).run(s);
        invariants::assert_no_duplicate_converged(s.name(), cfg.seed, &result);
        invariants::assert_round_bounds(s.name(), cfg.seed, &result, cfg.max_rounds);
        assert!(
            result.converged.len() as u32 >= 31,
            "[{}] one partitioned edge may lose at most its target; got {} of 32 converged",
            s.name(),
            result.converged.len(),
        );
        if result.converged.contains(&NodeId(1)) {
            assert!(
                result.is_success(),
                "[{}] node 1 converged, so no edge should have failed: {:?}",
                s.name(),
                result.failed,
            );
        } else {
            assert_eq!(
                result.converged.len() as u32,
                31,
                "[{}] only node 1 may be missing; got {} converged",
                s.name(),
                result.converged.len(),
            );
            let err = result
                .failed
                .as_ref()
                .expect("node 1 lost without a recorded failure");
            assert!(
                err.affected_nodes().contains(&NodeId(1)),
                "[{}] lost node 1 must be named in the error tree",
                s.name(),
            );
        }
        invariants::assert_deterministic(&cfg, s);
    }
}
