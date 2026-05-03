//! Universal correctness invariants for the cascade primitive.
//!
//! These hold across every [`CascadeStrategy`]. If any strategy
//! violates one, either the strategy is wrong OR the cascade
//! coordinator is — but the contract is on the coordinator side, so
//! these tests live with the simulator.

use std::collections::HashSet;

use consortium_fanout_sim::{
    fixtures::{BandwidthDistribution, FailureSchedule},
    scenario::{Scenario, ScenarioConfig},
};
use consortium_nix::cascade::{CascadeError, CascadeResult, CascadeStrategy, Log2FanOut, NodeId};
use consortium_nix::cascade_strategies::{MaxBottleneckSpanning, SteinerGreedy};

fn all_strategies() -> Vec<&'static dyn CascadeStrategy> {
    vec![&Log2FanOut, &MaxBottleneckSpanning, &SteinerGreedy]
}

fn assert_universal_invariants(strategy_name: &str, cfg: &ScenarioConfig, result: &CascadeResult) {
    // 1. Every converged node really has the closure (vacuous, but
    //    ensures the count == set len).
    let converged_set: HashSet<NodeId> = result.converged.iter().copied().collect();
    assert_eq!(
        converged_set.len(),
        result.converged.len(),
        "[{strategy_name}] converged list contains duplicates"
    );

    // 2. round_durations.len() == rounds (one duration per round).
    assert_eq!(
        result.round_durations.len() as u32,
        result.rounds,
        "[{strategy_name}] round_durations.len() != rounds"
    );

    // 3. Sanity: rounds <= n_nodes (way looser than log2 but unforgeable).
    assert!(
        result.rounds <= cfg.n_nodes,
        "[{strategy_name}] rounds ({}) exceeded n_nodes ({})",
        result.rounds,
        cfg.n_nodes
    );

    // 4. If we had no failures injected, everyone should converge.
    if matches!(cfg.failures, FailureSchedule::None) {
        assert_eq!(
            result.converged.len() as u32,
            cfg.n_nodes,
            "[{strategy_name}] all should converge with no failures (got {} of {})",
            result.converged.len(),
            cfg.n_nodes,
        );
        assert!(
            result.is_success(),
            "[{strategy_name}] no failures injected yet result.failed is Some: {:?}",
            result.failed
        );
    }

    // 5. If failed is Some, every affected node id is a real node id.
    if let Some(err) = &result.failed {
        for nid in err.affected_nodes() {
            assert!(
                nid.0 < cfg.n_nodes,
                "[{strategy_name}] error references node id {nid} outside [0, {})",
                cfg.n_nodes
            );
        }
    }
}

#[test]
fn invariants_uniform_no_failures() {
    let cfg = ScenarioConfig {
        seed: 1,
        n_nodes: 64,
        seed_fraction: 0.0,
        closure_bytes: 50 * 1024 * 1024,
        bandwidth: BandwidthDistribution::Uniform(100 * 1024 * 1024),
        uplinks: None,
        failures: FailureSchedule::None,
        max_rounds: 32,
    };
    for s in all_strategies() {
        let result = Scenario::new(cfg.clone()).run(s);
        assert_universal_invariants(s.name(), &cfg, &result);
    }
}

#[test]
fn invariants_skewed_no_failures() {
    let cfg = ScenarioConfig {
        seed: 2,
        n_nodes: 128,
        seed_fraction: 0.0,
        closure_bytes: 100 * 1024 * 1024,
        bandwidth: BandwidthDistribution::Bimodal {
            slow: 1024 * 1024,        // 1 MB/s
            fast: 1024 * 1024 * 1024, // 1 GB/s
            fast_fraction: 0.3,
        },
        uplinks: None,
        failures: FailureSchedule::None,
        max_rounds: 32,
    };
    for s in all_strategies() {
        let result = Scenario::new(cfg.clone()).run(s);
        assert_universal_invariants(s.name(), &cfg, &result);
    }
}

#[test]
fn invariants_with_pre_seeded_subset() {
    let cfg = ScenarioConfig {
        seed: 3,
        n_nodes: 100,
        seed_fraction: 0.2,
        closure_bytes: 50 * 1024 * 1024,
        bandwidth: BandwidthDistribution::Uniform(100 * 1024 * 1024),
        uplinks: None,
        failures: FailureSchedule::None,
        max_rounds: 16,
    };
    for s in all_strategies() {
        let result = Scenario::new(cfg.clone()).run(s);
        assert_universal_invariants(s.name(), &cfg, &result);
    }
}

#[test]
fn error_tree_shape_under_killed_node() {
    // Kill one specific node from round 0. Other nodes still converge.
    // (Round 0 because SteinerGreedy can converge before round 2 on
    // uniform topology — picking round 0 makes the test fire under
    // every strategy's actual round count.)
    let cfg = ScenarioConfig {
        seed: 4,
        n_nodes: 32,
        seed_fraction: 0.0,
        closure_bytes: 10 * 1024 * 1024,
        bandwidth: BandwidthDistribution::Uniform(100 * 1024 * 1024),
        uplinks: None,
        failures: FailureSchedule::KillNodeAtRound {
            node: NodeId(15),
            round: 0,
        },
        max_rounds: 32,
    };
    for s in all_strategies() {
        let result = Scenario::new(cfg.clone()).run(s);

        // EXACTLY 31 of 32 should converge — the killed node never
        // receives, every other node has a path through some other
        // source. `>=30` was too loose and would have hidden a real
        // regression where 2+ nodes silently fail to converge.
        assert_eq!(
            result.converged.len() as u32,
            cfg.n_nodes - 1,
            "[{}] only the killed node should fail to converge; got {} of {} converged",
            s.name(),
            result.converged.len(),
            cfg.n_nodes,
        );

        let err = result.failed.as_ref().unwrap_or_else(|| {
            panic!(
                "[{}] expected failure, got is_success=true (converged={})",
                s.name(),
                result.converged.len()
            )
        });
        let affected = err.affected_nodes();

        // EXACTLY one affected node, and it's the one we killed.
        // Tightened from `contains(&15)` so we'd catch cascading
        // failures that drag in extra nodes (none should — strategies
        // route around `failed_nodes`).
        assert_eq!(
            affected.len(),
            1,
            "[{}] expected exactly 1 affected node (only the killed one); got {:?}",
            s.name(),
            affected
        );
        assert_eq!(
            affected[0],
            NodeId(15),
            "[{}] expected affected = [NodeId(15)]; got {:?}",
            s.name(),
            affected
        );

        assert_universal_invariants(s.name(), &cfg, &result);
    }
}

#[test]
fn error_walk_yields_leaves_in_depth_order() {
    let cfg = ScenarioConfig {
        seed: 5,
        n_nodes: 16,
        seed_fraction: 0.0,
        closure_bytes: 10 * 1024 * 1024,
        bandwidth: BandwidthDistribution::Uniform(100 * 1024 * 1024),
        uplinks: None,
        failures: FailureSchedule::KillNodeAtRound {
            node: NodeId(7),
            round: 1,
        },
        max_rounds: 16,
    };
    let result = Scenario::new(cfg.clone()).run(&Log2FanOut);
    let err = result.failed.expect("expected failure");
    let mut leaves: Vec<(usize, NodeId)> = Vec::new();
    err.walk_leaves(|depth, e| match e {
        CascadeError::Copy { node, .. }
        | CascadeError::SshHandshake { node, .. }
        | CascadeError::Activation { node, .. } => leaves.push((depth, *node)),
        CascadeError::Partitioned { tgt, .. } => leaves.push((depth, *tgt)),
        _ => {}
    });
    assert_eq!(
        leaves.len(),
        1,
        "expected exactly 1 leaf for single-node failure: {leaves:?}"
    );
    let (depth, node) = leaves[0];
    assert_eq!(
        node,
        NodeId(7),
        "leaf should be the killed node, got {node:?}"
    );
    // Tightened: a single failed-target failure should bubble at depth
    // <= 2 — coordinator either emits the naked leaf (depth 0) or wraps
    // it in 1-2 SubtreeAggregates (depth 1-2 for the parent and
    // grandparent buckets). The previous `<= n_nodes` bound was 16 —
    // would have admitted catastrophic over-nesting.
    assert!(
        depth <= 2,
        "single-failure leaf depth should be <= 2 (coordinator wraps in 0-2 SubtreeAggregates), got {depth}",
    );
}
