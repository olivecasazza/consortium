//! Cost-aware cascade strategies built on petgraph.
//!
//! [`Log2FanOut`](crate::cascade::Log2FanOut) ignores the network — it
//! pairs sources to targets in id order. That's correct under uniform
//! topology but pessimal when bandwidth varies: the round wall-time is
//! the slowest edge in the round, so a single (fast-source, slow-target)
//! pairing stalls the whole round.
//!
//! These strategies pick edges *with* knowledge of the network:
//!
//! - [`MaxBottleneckSpanning`]: per round, build a bipartite graph of
//!   (sources × targets) weighted by `bandwidth(src, tgt)`. Greedily
//!   pick the highest-bandwidth edges until either every source is
//!   used once or no targets remain. Maximizes the *slowest* edge in
//!   the round (since round time is gated by it).
//!
//! - [`SteinerGreedy`]: same idea but doesn't constrain each source to
//!   one edge per round — keeps picking the next highest-bandwidth
//!   (src-with-closure → target-without-closure) edge until no more
//!   progress. Fires more edges per round; trades off latency vs
//!   bandwidth contention on the source side.

use std::collections::HashSet;

use crate::cascade::{CascadePlan, CascadeState, CascadeStrategy, NetworkProfile, NodeId};

// Default bandwidth used when an edge has no entry in NetworkProfile.
// 100 MB/s is a reasonable LAN baseline — strategies with no bandwidth
// data degenerate to the Log2FanOut ordering.
const DEFAULT_BW_BYTES_SEC: u64 = 100 * 1024 * 1024;

// ============================================================================
// MaxBottleneckSpanning
// ============================================================================

/// Each round: pick edges that maximize the slowest edge in the round.
///
/// Algorithm: enumerate all candidate `(src, tgt, bandwidth(src,tgt))`
/// triples, sort descending by bandwidth, greedily pick edges where
/// neither src nor tgt has been used yet. Each source serves at most
/// one target per round, so the round's wall-time is gated by the
/// slowest assigned edge — and the greedy max-weight pick *maximizes*
/// that bottleneck.
///
/// This is the *max-weight bipartite matching* the cascade wants per
/// round. We use greedy rather than Hungarian/Hopcroft-Karp because
/// (a) greedy is within a 2-approximation of optimal, (b) per-round
/// graphs are small (matching size = O(N) where N is the cascade
/// size), and (c) each round's matching cost amortizes across log(N)
/// rounds — so even O(N²) greedy is cheap relative to the actual
/// `nix copy` work it schedules.
///
/// Note: this is NOT a spanning tree algorithm despite the name — it's
/// a max-weight matching applied iteratively per round. The
/// *aggregate* deployment topology that emerges across all rounds IS
/// a spanning tree (each non-seeded node has exactly one parent),
/// implicitly built up round-by-round.
pub struct MaxBottleneckSpanning;

impl CascadeStrategy for MaxBottleneckSpanning {
    fn name(&self) -> &'static str {
        "max-bottleneck-spanning"
    }

    fn next_round(&self, state: &CascadeState, net: &NetworkProfile) -> CascadePlan {
        let sources: Vec<NodeId> = state
            .nodes
            .iter()
            .filter(|n| state.has_closure.contains(&n.id))
            .filter(|n| !state.failed_nodes.contains(&n.id))
            .map(|n| n.id)
            .collect();
        let targets: Vec<NodeId> = state
            .nodes
            .iter()
            .filter(|n| !state.has_closure.contains(&n.id))
            .filter(|n| !state.failed_nodes.contains(&n.id))
            .map(|n| n.id)
            .collect();

        if sources.is_empty() || targets.is_empty() {
            return CascadePlan::default();
        }

        // Enumerate all candidate edges, sort descending by bandwidth.
        let mut candidates: Vec<(u64, NodeId, NodeId)> = Vec::new();
        for &src in &sources {
            for &tgt in &targets {
                if net.is_partitioned(src, tgt) {
                    continue;
                }
                if state.attempted.contains(&(src, tgt)) {
                    continue;
                }
                let bw = net.bandwidth_of(src, tgt, DEFAULT_BW_BYTES_SEC);
                candidates.push((bw, src, tgt));
            }
        }
        candidates.sort_by(|a, b| b.0.cmp(&a.0));

        // Greedy max-weight matching: each src and tgt used at most once.
        let mut used_src: HashSet<NodeId> = HashSet::new();
        let mut used_tgt: HashSet<NodeId> = HashSet::new();
        let mut assignments = Vec::new();
        for (_bw, src, tgt) in candidates {
            if used_src.contains(&src) || used_tgt.contains(&tgt) {
                continue;
            }
            assignments.push((src, tgt));
            used_src.insert(src);
            used_tgt.insert(tgt);
        }

        CascadePlan {
            round: state.round,
            assignments,
        }
    }
}

// ============================================================================
// SteinerGreedy
// ============================================================================

/// Greedy max-bandwidth assignment without the one-edge-per-source cap.
///
/// At each round, pick the highest-bandwidth (source, target) edge
/// where the source has the closure and the target doesn't. Repeat
/// until no edges remain that match those criteria.
///
/// This fires *more edges per round* than `MaxBottleneckSpanning` (a
/// fast source can serve multiple targets in one round) but is gated
/// by the source's outbound bandwidth in practice. Use when network
/// is heavily skewed and a few sources dominate capacity.
pub struct SteinerGreedy;

impl CascadeStrategy for SteinerGreedy {
    fn name(&self) -> &'static str {
        "steiner-greedy"
    }

    fn next_round(&self, state: &CascadeState, net: &NetworkProfile) -> CascadePlan {
        let sources: Vec<NodeId> = state
            .nodes
            .iter()
            .filter(|n| state.has_closure.contains(&n.id))
            .filter(|n| !state.failed_nodes.contains(&n.id))
            .map(|n| n.id)
            .collect();
        let targets: Vec<NodeId> = state
            .nodes
            .iter()
            .filter(|n| !state.has_closure.contains(&n.id))
            .filter(|n| !state.failed_nodes.contains(&n.id))
            .map(|n| n.id)
            .collect();

        if sources.is_empty() || targets.is_empty() {
            return CascadePlan::default();
        }

        // Build all candidate (src, tgt, bw) triples, sort descending by bw.
        let mut candidates: Vec<(u64, NodeId, NodeId)> = Vec::new();
        for &src in &sources {
            for &tgt in &targets {
                if net.is_partitioned(src, tgt) {
                    continue;
                }
                if state.attempted.contains(&(src, tgt)) {
                    continue;
                }
                let bw = net.bandwidth_of(src, tgt, DEFAULT_BW_BYTES_SEC);
                candidates.push((bw, src, tgt));
            }
        }
        candidates.sort_by(|a, b| b.0.cmp(&a.0));

        // Greedy pick: each src may serve N targets, each tgt receives once.
        let mut used_tgt: HashSet<NodeId> = HashSet::new();
        let mut assignments = Vec::new();
        for (_bw, src, tgt) in candidates {
            if used_tgt.contains(&tgt) {
                continue;
            }
            assignments.push((src, tgt));
            used_tgt.insert(tgt);
        }

        CascadePlan {
            round: state.round,
            assignments,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascade::{
        run_cascade, CascadeNode, NetworkProfile, NodeId, NodeIdAlloc, RoundExecutor,
    };
    use std::collections::HashMap;
    use std::time::Duration;

    /// Test executor: edge duration = closure_size / bandwidth.
    /// Used so we can compare strategies on total wall-time.
    struct BandwidthSimExecutor {
        closure_bytes: u64,
        default_bw: u64,
    }

    impl RoundExecutor for BandwidthSimExecutor {
        fn dispatch(
            &self,
            _nodes: &[CascadeNode],
            edges: &[(NodeId, NodeId)],
            net: &NetworkProfile,
        ) -> HashMap<(NodeId, NodeId), Result<Duration, crate::cascade::CascadeError>> {
            edges
                .iter()
                .map(|(src, tgt)| {
                    let bw = net.bandwidth_of(*src, *tgt, self.default_bw);
                    let secs = self.closure_bytes as f64 / bw as f64;
                    let dur =
                        Duration::from_secs_f64(secs) + net.latency_of(*src, *tgt, Duration::ZERO);
                    ((*src, *tgt), Ok(dur))
                })
                .collect()
        }
    }

    fn make_nodes(n: u32) -> Vec<CascadeNode> {
        let mut alloc = NodeIdAlloc::new();
        (0..n)
            .map(|_| {
                let id = alloc.alloc();
                CascadeNode::new(id, format!("user@host-{}", id.0))
            })
            .collect()
    }

    /// Skewed network: half the edges are slow (1 MB/s), half are fast (1 GB/s).
    fn skewed_network(n: u32) -> NetworkProfile {
        let mut net = NetworkProfile::default();
        for src in 0..n {
            for tgt in 0..n {
                if src == tgt {
                    continue;
                }
                let bw = if (src + tgt) % 2 == 0 {
                    1024 * 1024 // 1 MB/s slow
                } else {
                    1024 * 1024 * 1024 // 1 GB/s fast
                };
                net.bandwidth.insert((NodeId(src), NodeId(tgt)), bw);
            }
        }
        net
    }

    #[test]
    fn max_bottleneck_correct_on_uniform() {
        // Uniform network → should converge in same round count as Log2FanOut.
        let nodes = make_nodes(16);
        let mut seeded = std::collections::HashSet::new();
        seeded.insert(NodeId(0));
        let exec = BandwidthSimExecutor {
            closure_bytes: 1024 * 1024 * 100, // 100 MB
            default_bw: 100 * 1024 * 1024,
        };
        let result = run_cascade(
            nodes,
            seeded,
            NetworkProfile::default(),
            &MaxBottleneckSpanning,
            &exec,
            32,
            None,
        );
        assert!(result.is_success(), "failed: {:?}", result.failed);
        assert_eq!(result.converged.len(), 16);
        assert_eq!(result.rounds, 4);
    }

    #[test]
    fn steiner_greedy_converges_in_one_round_on_uniform() {
        // Steiner is uncapped per source. With 1 seed, no contention, no
        // per-edge bandwidth heterogeneity, the seed serves all N-1
        // targets in round 0 and the cascade halts. This test pins that
        // strategy-design behavior — if someone "fixes" Steiner to add a
        // per-source cap, the test fails loudly so the rationale gets
        // re-examined rather than silently changed.
        let nodes = make_nodes(16);
        let mut seeded = std::collections::HashSet::new();
        seeded.insert(NodeId(0));
        let exec = BandwidthSimExecutor {
            closure_bytes: 1024 * 1024 * 100,
            default_bw: 100 * 1024 * 1024,
        };
        let result = run_cascade(
            nodes,
            seeded,
            NetworkProfile::default(),
            &SteinerGreedy,
            &exec,
            32,
            None,
        );
        assert!(result.is_success(), "failed: {:?}", result.failed);
        assert_eq!(result.converged.len(), 16);
        assert_eq!(
            result.rounds, 1,
            "Steiner should converge in EXACTLY 1 round on uniform topology with 1 seed (it's uncapped per source). Got {} rounds — strategy semantics changed.",
            result.rounds
        );
    }

    #[test]
    fn max_bottleneck_converges_in_log2_rounds_on_uniform() {
        // MaxBottleneck caps each source to 1 outgoing edge per round, so
        // on uniform topology it converges in EXACTLY ⌈log₂(N)⌉ rounds —
        // same shape as Log2FanOut. Pins the per-source cap behavior; if
        // it regresses, this test names the value that broke.
        let nodes = make_nodes(16);
        let mut seeded = std::collections::HashSet::new();
        seeded.insert(NodeId(0));
        let exec = BandwidthSimExecutor {
            closure_bytes: 1024 * 1024 * 100,
            default_bw: 100 * 1024 * 1024,
        };
        let result = run_cascade(
            nodes,
            seeded,
            NetworkProfile::default(),
            &MaxBottleneckSpanning,
            &exec,
            32,
            None,
        );
        assert!(result.is_success(), "failed: {:?}", result.failed);
        assert_eq!(result.converged.len(), 16);
        assert_eq!(
            result.rounds, 4,
            "MaxBottleneck should converge in ⌈log₂(16)⌉=4 rounds on uniform topology (capped at 1 edge/source/round). Got {}",
            result.rounds
        );
    }

    #[test]
    fn max_bottleneck_beats_log2_on_skewed() {
        use crate::cascade::Log2FanOut;
        let n = 16;
        let net = skewed_network(n);
        let exec = BandwidthSimExecutor {
            closure_bytes: 1024 * 1024 * 50, // 50 MB
            default_bw: 100 * 1024 * 1024,
        };
        let mut seeded = std::collections::HashSet::new();
        seeded.insert(NodeId(0));

        let log2 = run_cascade(
            make_nodes(n),
            seeded.clone(),
            net.clone(),
            &Log2FanOut,
            &exec,
            32,
            None,
        );
        let mb = run_cascade(
            make_nodes(n),
            seeded,
            net,
            &MaxBottleneckSpanning,
            &exec,
            32,
            None,
        );

        let log2_total: Duration = log2.round_durations.iter().sum();
        let mb_total: Duration = mb.round_durations.iter().sum();
        assert!(log2.is_success());
        assert!(mb.is_success());
        // Tightened: require MaxBottleneck to be measurably better, not
        // just "no worse". 90% threshold catches the case where the
        // bandwidth-aware strategy degenerates and matches Log2's
        // network-blind choice (the previous `<=` admitted this).
        assert!(
            mb_total.as_secs_f64() <= log2_total.as_secs_f64() * 0.90,
            "MaxBottleneck ({:?}) should be at least 10% faster than Log2FanOut ({:?}) on skewed network — bandwidth-aware pairing isn't paying off",
            mb_total,
            log2_total,
        );
    }
}
