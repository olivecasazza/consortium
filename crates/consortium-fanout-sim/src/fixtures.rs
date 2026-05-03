//! Reproducible generators for cascade scenarios.
//!
//! All generators take an `&mut StdRng` (or `ChaCha8Rng`) seeded from
//! the scenario's master seed, so a given `(seed, descriptor)` pair
//! produces deterministic output.
//!
//! ## Knobs
//!
//! - [`SeedDistribution`] — which subset of nodes starts with the closure
//! - [`BandwidthDistribution`] — how per-edge bandwidth varies
//! - [`FailureSchedule`] — when and where to inject failures during the run
//!
//! Each is a sum-type so callers can mix and match without baking a
//! specific distribution into the simulator.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use consortium_nix::cascade::{CascadeError, NetworkProfile, NodeId};
use rand::distributions::{Distribution, WeightedIndex};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

// ============================================================================
// Seed distribution — which nodes start with the closure
// ============================================================================

/// How to choose the initial set of nodes that have the closure at
/// cascade start.
#[derive(Debug, Clone)]
pub enum SeedDistribution {
    /// Exactly one node (the build host) starts with the closure.
    /// Always picks `NodeId(0)` for reproducibility.
    Single,
    /// `fraction` of all nodes are pre-seeded, chosen uniformly at random.
    /// `fraction` clamped to `[0, 1]`.
    Random { fraction: f64 },
    /// Exactly `count` nodes pre-seeded (chosen uniformly at random).
    Count(usize),
    /// Caller-specified set of nodes (escape hatch for hand-crafted scenarios).
    Explicit(HashSet<NodeId>),
}

impl SeedDistribution {
    pub fn sample(&self, rng: &mut ChaCha8Rng, n_nodes: u32) -> HashSet<NodeId> {
        match self {
            SeedDistribution::Single => {
                let mut s = HashSet::new();
                if n_nodes > 0 {
                    s.insert(NodeId(0));
                }
                s
            }
            SeedDistribution::Random { fraction } => {
                let f = fraction.clamp(0.0, 1.0);
                let count = ((n_nodes as f64) * f).round() as usize;
                Self::Count(count.max(1).min(n_nodes as usize)).sample(rng, n_nodes)
            }
            SeedDistribution::Count(c) => {
                let count = (*c).min(n_nodes as usize);
                let mut ids: Vec<NodeId> = (0..n_nodes).map(NodeId).collect();
                ids.shuffle(rng);
                ids.into_iter().take(count).collect()
            }
            SeedDistribution::Explicit(s) => s.clone(),
        }
    }
}

// ============================================================================
// Bandwidth distribution
// ============================================================================

/// How per-edge bandwidth varies. Generators populate the
/// [`NetworkProfile::bandwidth`] map; edges absent from the map fall
/// through to the executor's default.
#[derive(Debug, Clone)]
pub enum BandwidthDistribution {
    /// Every edge gets the same bandwidth (bytes/sec).
    Uniform(u64),
    /// Each edge picks from `{slow, fast}` with `fast_fraction` of
    /// edges getting `fast` bandwidth, the rest `slow`.
    Bimodal {
        slow: u64,
        fast: u64,
        fast_fraction: f64,
    },
    /// Each edge sampled from a discrete distribution: `(weight, bytes/sec)`.
    /// Weights are relative.
    Discrete(Vec<(u32, u64)>),
}

impl BandwidthDistribution {
    pub fn populate(&self, rng: &mut ChaCha8Rng, net: &mut NetworkProfile, n_nodes: u32) {
        match self {
            BandwidthDistribution::Uniform(bw) => {
                for src in 0..n_nodes {
                    for tgt in 0..n_nodes {
                        if src == tgt {
                            continue;
                        }
                        net.bandwidth.insert((NodeId(src), NodeId(tgt)), *bw);
                    }
                }
            }
            BandwidthDistribution::Bimodal {
                slow,
                fast,
                fast_fraction,
            } => {
                let f = fast_fraction.clamp(0.0, 1.0);
                for src in 0..n_nodes {
                    for tgt in 0..n_nodes {
                        if src == tgt {
                            continue;
                        }
                        let bw = if rng.gen::<f64>() < f { *fast } else { *slow };
                        net.bandwidth.insert((NodeId(src), NodeId(tgt)), bw);
                    }
                }
            }
            BandwidthDistribution::Discrete(weights) => {
                let dist = WeightedIndex::new(weights.iter().map(|(w, _)| *w))
                    .expect("BandwidthDistribution::Discrete needs at least one weight > 0");
                for src in 0..n_nodes {
                    for tgt in 0..n_nodes {
                        if src == tgt {
                            continue;
                        }
                        let idx = dist.sample(rng);
                        let bw = weights[idx].1;
                        net.bandwidth.insert((NodeId(src), NodeId(tgt)), bw);
                    }
                }
            }
        }
    }
}

// ============================================================================
// Failure schedule
// ============================================================================

/// Injects deterministic failures into the cascade. The
/// `DeterministicExecutor` calls `failure_for(round, src, tgt)` for
/// every edge it dispatches; non-`None` returns become the edge's
/// outcome.
#[derive(Debug, Clone, Default)]
pub enum FailureSchedule {
    /// No failures.
    #[default]
    None,
    /// Fail every edge whose target is `node`, starting at `round`.
    KillNodeAtRound { node: NodeId, round: u32 },
    /// Fail the specific `(src, tgt)` edge at `round`.
    PartitionAtRound {
        src: NodeId,
        tgt: NodeId,
        round: u32,
    },
    /// Fail edges per a hand-built table: `(round, src, tgt) -> Error`.
    Explicit(HashMap<(u32, NodeId, NodeId), CascadeError>),
    /// Multiple failure schedules composed (any one matching = fail).
    All(Vec<FailureSchedule>),
}

impl FailureSchedule {
    pub fn failure_for(&self, round: u32, src: NodeId, tgt: NodeId) -> Option<CascadeError> {
        match self {
            FailureSchedule::None => None,
            FailureSchedule::KillNodeAtRound { node, round: r } => {
                if round >= *r && tgt == *node {
                    Some(CascadeError::Copy {
                        node: tgt,
                        stderr: format!("node {tgt} killed at round {r} (current round {round})"),
                    })
                } else {
                    None
                }
            }
            FailureSchedule::PartitionAtRound {
                src: s,
                tgt: t,
                round: r,
            } => {
                if round >= *r && src == *s && tgt == *t {
                    Some(CascadeError::Partitioned { src, tgt })
                } else {
                    None
                }
            }
            FailureSchedule::Explicit(map) => map.get(&(round, src, tgt)).cloned(),
            FailureSchedule::All(schedules) => schedules
                .iter()
                .find_map(|s| s.failure_for(round, src, tgt)),
        }
    }
}

// ============================================================================
// Latency distribution (helper, not first-class)
// ============================================================================

/// Sprinkle uniform latency across all edges. Useful when bandwidth
/// alone isn't enough to differentiate strategies (latency dominates
/// for tiny closures).
pub fn populate_uniform_latency(net: &mut NetworkProfile, latency: Duration, n_nodes: u32) {
    for src in 0..n_nodes {
        for tgt in 0..n_nodes {
            if src == tgt {
                continue;
            }
            net.latency.insert((NodeId(src), NodeId(tgt)), latency);
        }
    }
}

// ============================================================================
// RNG helper
// ============================================================================

/// Build a deterministic ChaCha8Rng from a u64 seed.
pub fn rng_from_seed(seed: u64) -> ChaCha8Rng {
    ChaCha8Rng::seed_from_u64(seed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_distribution_single_returns_node_zero() {
        let mut rng = rng_from_seed(1);
        let s = SeedDistribution::Single.sample(&mut rng, 10);
        assert_eq!(s.len(), 1);
        assert!(s.contains(&NodeId(0)));
    }

    #[test]
    fn seed_distribution_count_clamps_to_n() {
        let mut rng = rng_from_seed(1);
        let s = SeedDistribution::Count(100).sample(&mut rng, 10);
        assert_eq!(s.len(), 10);
    }

    #[test]
    fn seed_distribution_random_is_reproducible() {
        let dist = SeedDistribution::Random { fraction: 0.3 };
        let s1 = dist.sample(&mut rng_from_seed(42), 100);
        let s2 = dist.sample(&mut rng_from_seed(42), 100);
        assert_eq!(s1, s2);
    }

    #[test]
    fn bimodal_bandwidth_respects_fraction() {
        let mut net = NetworkProfile::default();
        let dist = BandwidthDistribution::Bimodal {
            slow: 1_000_000,
            fast: 1_000_000_000,
            fast_fraction: 0.5,
        };
        let mut rng = rng_from_seed(1);
        dist.populate(&mut rng, &mut net, 50);
        let fast_count = net
            .bandwidth
            .values()
            .filter(|&&v| v == 1_000_000_000)
            .count();
        let total = net.bandwidth.len();
        let frac = fast_count as f64 / total as f64;
        assert!((frac - 0.5).abs() < 0.05, "expected ~0.5, got {frac}");
    }

    #[test]
    fn kill_node_failure_fires_only_at_or_after_round() {
        let s = FailureSchedule::KillNodeAtRound {
            node: NodeId(5),
            round: 2,
        };
        assert!(s.failure_for(0, NodeId(0), NodeId(5)).is_none());
        assert!(s.failure_for(1, NodeId(0), NodeId(5)).is_none());
        assert!(s.failure_for(2, NodeId(0), NodeId(5)).is_some());
        assert!(s.failure_for(3, NodeId(1), NodeId(5)).is_some());
        // wrong target
        assert!(s.failure_for(2, NodeId(0), NodeId(6)).is_none());
    }
}
