//! Scale smoke tests: verify cascade convergence bounds at 256 and 1024 nodes.
//!
//! Both tests use MaxBottleneckSpanning with a bimodal bandwidth distribution
//! and a pinned seed for reproducibility.

use std::collections::HashSet;

use consortium_fanout_sim::{
    fixtures::{rng_from_seed, BandwidthDistribution, FailureSchedule, UplinkDistribution},
    DeterministicExecutor,
};
use consortium_nix::cascade::{Cascade, CascadeNode, NetworkProfile, NodeId, NodeIdAlloc};
use consortium_nix::cascade_strategies::MaxBottleneckSpanning;

// ============================================================================
// Helpers
// ============================================================================

fn build_run(n_nodes: u32, seed: u64) -> consortium_nix::cascade::CascadeResult {
    let mut rng = rng_from_seed(seed);
    let mut alloc = NodeIdAlloc::new();

    let nodes: Vec<CascadeNode> = (0..n_nodes)
        .map(|_| {
            let id = alloc.alloc();
            CascadeNode::new(id, format!("user@host-{}", id.0))
        })
        .collect();

    let mut seeded = HashSet::new();
    seeded.insert(NodeId(0));

    let mut net = NetworkProfile::default();
    BandwidthDistribution::Bimodal {
        slow: 1024 * 1024,        // 1 MB/s
        fast: 1024 * 1024 * 1024, // 1 GB/s
        fast_fraction: 0.3,
    }
    .populate(&mut rng, &mut net, n_nodes);

    UplinkDistribution::Bimodal {
        slow: 1024 * 1024,
        fast: 1024 * 1024 * 1024,
        fast_fraction: 0.3,
    }
    .populate(&mut rng, &mut net, n_nodes);

    let exec = DeterministicExecutor::new(100 * 1024 * 1024, FailureSchedule::None);

    Cascade::new()
        .nodes(nodes)
        .seeded(seeded)
        .network(net)
        .strategy(&MaxBottleneckSpanning)
        .executor(&exec)
        .max_rounds(64)
        .run()
}

// ============================================================================
// Tests
// ============================================================================

/// 256-node cascade must converge in <= ceil(log2(256)) + 2 = 10 rounds.
///
/// ceil(log2(256)) = 8, +2 headroom for bimodal bandwidth heterogeneity.
#[test]
fn cascade_at_256_nodes_converges_under_log2_plus_2_rounds() {
    // ceil(log2(256)) = 8; +2 = 10
    const N: u32 = 256;
    const SEED: u64 = 0x_dead_beef_256;
    const MAX_ROUNDS: u32 = 10; // ceil(log2(256)) + 2

    let result = build_run(N, SEED);

    assert!(
        result.is_success(),
        "cascade failed unexpectedly: {:?}",
        result.failed
    );
    assert_eq!(
        result.converged.len() as u32,
        N,
        "expected all {} nodes to converge, got {}",
        N,
        result.converged.len()
    );
    assert!(
        result.rounds <= MAX_ROUNDS,
        "expected convergence in <= {} rounds (ceil(log2(256))+2), but got {} rounds",
        MAX_ROUNDS,
        result.rounds
    );
    assert_eq!(
        result.round_durations.len() as u32,
        result.rounds,
        "round_durations.len() != rounds"
    );
}

/// 1024-node cascade must converge in <= ceil(log2(1024)) + 3 = 13 rounds.
///
/// ceil(log2(1024)) = 10, +3 headroom for bimodal bandwidth heterogeneity.
#[test]
fn cascade_at_1024_nodes_converges_under_log2_plus_3_rounds() {
    // ceil(log2(1024)) = 10; +3 = 13
    const N: u32 = 1024;
    const SEED: u64 = 0x_dead_beef_1024;
    const MAX_ROUNDS: u32 = 13; // ceil(log2(1024)) + 3

    let result = build_run(N, SEED);

    assert!(
        result.is_success(),
        "cascade failed unexpectedly: {:?}",
        result.failed
    );
    assert_eq!(
        result.converged.len() as u32,
        N,
        "expected all {} nodes to converge, got {}",
        N,
        result.converged.len()
    );
    assert!(
        result.rounds <= MAX_ROUNDS,
        "expected convergence in <= {} rounds (ceil(log2(1024))+3), but got {} rounds",
        MAX_ROUNDS,
        result.rounds
    );
    assert_eq!(
        result.round_durations.len() as u32,
        result.rounds,
        "round_durations.len() != rounds"
    );
}
