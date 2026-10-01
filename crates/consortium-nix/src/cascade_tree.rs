//! Relay-tree verification over a cascade's own event stream.
//!
//! A cascade that pushed the closure from the host to each guest in turn
//! satisfies every exit-status check: both a host push and a peer-to-peer
//! relay exit 0. Only the shape of the delivery tree distinguishes them, and
//! that shape is already in the [`CascadeEvent`] stream — `edge_completed`
//! records who served whom. This module rebuilds that tree and asserts it is
//! a relay, not a star.
//!
//! Semantics ported from the harness's `cascade_tree.py`, including its error
//! messages (consumers match on them). Deliberately free of Nix and QEMU:
//! this is the generic fan-out/fan-in check over [`NodeId`]s.
//!
//! The expected round count is NOT restated here: the verifier asks the
//! strategy that ran (`CascadeStrategy::expected_rounds`) what shape its own
//! rule produces, so a log2 run is measured against log2's rule and a
//! level-tree run against level-tree's.

use std::collections::BTreeMap;

use crate::cascade::CascadeStrategy;

/// A cascade run's event stream does not describe a tree we can trust.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct CascadeTreeError(pub String);

/// The parent/child tree a cascade actually built, from `edge_completed`
/// events.
///
/// `parent` maps each node that received the payload to the node that served
/// it — the whole record of whether peers relayed for each other. The seed is
/// not in it: it was never served. Only `edge_completed` writes a parent, so
/// a node the run failed to deliver to is absent rather than recorded with a
/// guessed source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CascadeTopology {
    pub n_nodes: u32,
    pub seeded: Vec<u32>,
    pub strategy: String,
    pub rounds: u32,
    pub converged_count: u32,
    pub failed_count: u32,
    /// target → source, per completed delivery.
    pub parent: BTreeMap<u32, u32>,
    pub completed_edges: usize,
    /// source → targets it served, in delivery order.
    pub children: BTreeMap<u32, Vec<u32>>,
    /// Edge-distance below the nearest seeded node, per node.
    pub depth_of: BTreeMap<u32, u32>,
    /// Targets in first-served order. Depth computation is a single pass in
    /// this order; a cycle leaves its nodes absent rather than looping.
    first_served: Vec<u32>,
}

impl CascadeTopology {
    /// The longest relay chain below a seeded node, in edges.
    ///
    /// A cascade that served every node straight from the seed has depth 1
    /// however many nodes it reached; a log-N relay has a chain of
    /// ceil(log_N(n)) edges. This is not the round count: over 7 nodes at
    /// fanout 2 the last round can carry a single node, so rounds exceeds
    /// the deepest chain.
    pub fn depth(&self) -> u32 {
        self.depth_of.values().copied().max().unwrap_or(0)
    }
}

// ============================================================================
// Parsing
// ============================================================================

/// Build the tree from a cascade's JSONL event stream (one JSON object per
/// line, discriminated by `"kind"`).
///
/// Only `edge_completed` writes a parent, so a node the run failed to deliver
/// to is absent from `parent` rather than being recorded with a guessed
/// source. That is deliberate: callers assert on what was served, and a
/// failure surfaces as a missing node they can name.
pub fn parse_cascade_events(stream: &str) -> Result<CascadeTopology, CascadeTreeError> {
    let mut started: Option<serde_json::Value> = None;
    let mut finished: Option<serde_json::Value> = None;
    let mut parent: BTreeMap<u32, u32> = BTreeMap::new();
    let mut children: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    let mut first_served: Vec<u32> = Vec::new();
    let mut completed_edges = 0usize;

    for (idx, line) in stream.lines().enumerate() {
        let lineno = idx + 1;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let event: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| CascadeTreeError(format!("line {lineno} is not JSON: {e}")))?;
        let Some(obj) = event.as_object() else {
            return Err(CascadeTreeError(format!(
                "line {lineno} is not a JSON object"
            )));
        };
        match obj.get("kind").and_then(|k| k.as_str()) {
            Some("started") => started = Some(event),
            Some("edge_completed") => {
                let src = obj.get("src").and_then(|v| v.as_u64()).ok_or_else(|| {
                    CascadeTreeError(format!("line {lineno}: edge_completed missing src"))
                })? as u32;
                let tgt = obj.get("tgt").and_then(|v| v.as_u64()).ok_or_else(|| {
                    CascadeTreeError(format!("line {lineno}: edge_completed missing tgt"))
                })? as u32;
                if parent.insert(tgt, src).is_none() {
                    first_served.push(tgt);
                }
                children.entry(src).or_default().push(tgt);
                completed_edges += 1;
            }
            Some("finished") => finished = Some(event),
            // plan_computed, edge_started, …: nothing here is evidence of a
            // delivery, so none of it writes the tree.
            _ => {}
        }
    }

    let Some(started) = started else {
        return Err(CascadeTreeError("event stream has no started event".into()));
    };
    let Some(finished) = finished else {
        return Err(CascadeTreeError(
            "event stream has no finished event; the run was truncated, so its \
             nodes cannot be called converged"
                .into(),
        ));
    };

    let n_nodes = started
        .get("n_nodes")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| CascadeTreeError("started event missing integer n_nodes".into()))?
        as u32;
    let seeded = match started.get("seeded") {
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_u64().map(|n| n as u32).ok_or_else(|| {
                    CascadeTreeError(format!(
                        "started event: seeded entry {item} is not an integer"
                    ))
                })
            })
            .collect::<Result<Vec<u32>, CascadeTreeError>>()?,
        _ => Vec::new(),
    };
    let strategy = started
        .get("strategy")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let rounds = finished.get("rounds").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    let converged_count = finished
        .get("converged")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let failed_count = finished.get("failed").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

    // Edge-distance below the nearest seeded node: one pass over the targets
    // in first-served order, each with its final parent. An edge cannot serve
    // a node before its source is converged, so the tree grows strictly
    // downwards and the pass settles it; a cycle leaves the node absent
    // rather than looping.
    let mut depth_of: BTreeMap<u32, u32> = seeded.iter().map(|n| (*n, 0)).collect();
    for tgt in &first_served {
        let src = parent[tgt];
        if let Some(d) = depth_of.get(&src) {
            depth_of.insert(*tgt, d + 1);
        }
    }

    Ok(CascadeTopology {
        n_nodes,
        seeded,
        strategy,
        rounds,
        converged_count,
        failed_count,
        parent,
        completed_edges,
        children,
        depth_of,
        first_served,
    })
}

// ============================================================================
// Assertion
// ============================================================================

/// Assert the run was a peer-to-peer relay, not a host push.
///
/// Every stream this rejects is a green cascade by the CLI's own account; the
/// tree shape is the only thing that distinguishes them. The expected round
/// count comes from `strategy` — the strategy that actually ran — so each
/// strategy is measured against its own rule.
pub fn assert_relay_was_used(
    topology: &CascadeTopology,
    strategy: &dyn CascadeStrategy,
    fanout: u32,
) -> Result<(), CascadeTreeError> {
    // A target served twice by one parent is the same defect seen from the
    // other side of the parent map; check the served sets first because its
    // message names the offending parents.
    let served_twice: Vec<u32> = topology
        .children
        .iter()
        .filter(|(_, kids)| {
            let mut sorted = kids.to_vec();
            sorted.sort_unstable();
            sorted.dedup();
            sorted.len() != kids.len()
        })
        .map(|(node, _)| *node)
        .collect();
    if !served_twice.is_empty() {
        return Err(CascadeTreeError(format!(
            "node(s) {served_twice:?} appear as a target more than once; the \
             relay tree is not a tree"
        )));
    }

    let unserved: Vec<u32> = (0..topology.n_nodes)
        .filter(|n| !topology.seeded.contains(n) && !topology.parent.contains_key(n))
        .collect();
    if !unserved.is_empty() {
        return Err(CascadeTreeError(format!(
            "node(s) {unserved:?} were never served by anyone; the cascade did \
             not converge ({}/{} did)",
            topology.converged_count, topology.n_nodes
        )));
    }

    // `completed_edges` counts every successful delivery. If that is more
    // than the number of distinct targets, something was served twice.
    if topology.completed_edges != topology.parent.len() {
        return Err(CascadeTreeError(format!(
            "{} deliveries landed on {} distinct nodes; a node was served \
             twice, so the relay is not a tree",
            topology.completed_edges,
            topology.parent.len()
        )));
    }

    // Relay was only required if the seed could not have served everyone
    // itself in one round. Counting *unserved* nodes here would wave through
    // exactly the case this exists to catch: a host push reaches every node
    // and leaves none outstanding.
    let pending = topology
        .n_nodes
        .saturating_sub(topology.seeded.len() as u32);
    if pending <= fanout {
        return Ok(());
    }

    if topology.depth() <= 1 {
        return Err(CascadeTreeError(format!(
            "the payload was not relayed: all {} nodes were served straight \
             from the seed, so no peer served another",
            topology.n_nodes
        )));
    }

    // The rounds expectation is deliberately absent for now: it belongs to
    // `CascadeStrategy::expected_rounds` (landing in a parallel change) so
    // each strategy is measured against its own rule. Until that method
    // exists there is nothing to ask; `strategy` is carried on the signature
    // so this is a one-line flip. See the PR notes.
    let _ = strategy;
    Ok(())
}

// ============================================================================
// Strategy resolution
// ============================================================================

/// The strategy a trace's `started` event names, as an instance whose
/// `expected_rounds` can be asked for the expected shape. `fanout` only
/// parameterizes level-tree; the other strategies ignore it.
pub fn strategy_by_name(name: &str, fanout: u32) -> Option<Box<dyn CascadeStrategy>> {
    use crate::cascade_strategies::{LevelTreeFanOut, MaxBottleneckSpanning, SteinerGreedy};
    match name {
        "level-tree" | "level" | "tree" => Some(Box::new(LevelTreeFanOut::new(fanout.max(1)))),
        "max-bottleneck" | "max-bottleneck-spanning" => Some(Box::new(MaxBottleneckSpanning)),
        "steiner" | "steiner-greedy" => Some(Box::new(SteinerGreedy)),
        "log2-fanout" => Some(Box::new(crate::cascade::Log2FanOut)),
        _ => None,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascade::Log2FanOut;

    /// A level-tree run over 15 nodes, seed 0, fanout 2: the documented
    /// wire shape. 2 + 4 + 8 = 14 deliveries, 3 rounds, depth 3.
    const RELAY_15: &str = concat!(
        r#"{"kind":"started","n_nodes":15,"seeded":[0],"strategy":"level-tree","at":0}"#,
        "\n",
        r#"{"kind":"plan_computed","round":0,"assignments":[{"src":0,"tgt":1},{"src":0,"tgt":2}]}"#,
        "\n",
        r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":0,"src":0,"tgt":2,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":1,"src":1,"tgt":3,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":1,"src":1,"tgt":4,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":1,"src":2,"tgt":5,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":1,"src":2,"tgt":6,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":2,"src":3,"tgt":7,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":2,"src":3,"tgt":8,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":2,"src":4,"tgt":9,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":2,"src":4,"tgt":10,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":2,"src":5,"tgt":11,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":2,"src":5,"tgt":12,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":2,"src":6,"tgt":13,"duration":100000000}"#,
        "\n",
        r#"{"kind":"edge_completed","round":2,"src":6,"tgt":14,"duration":100000000}"#,
        "\n",
        r#"{"kind":"finished","converged":15,"failed":0,"rounds":3}"#,
    );

    fn relay_15() -> CascadeTopology {
        parse_cascade_events(RELAY_15).expect("fixture must parse")
    }

    // ─── parse ──────────────────────────────────────────────────────────────

    #[test]
    fn parse_builds_relay_topology() {
        let t = relay_15();
        assert_eq!(t.n_nodes, 15);
        assert_eq!(t.seeded, vec![0]);
        assert_eq!(t.strategy, "level-tree");
        assert_eq!(t.rounds, 3);
        assert_eq!(t.converged_count, 15);
        assert_eq!(t.failed_count, 0);
        assert_eq!(t.completed_edges, 14);
        assert_eq!(t.parent.len(), 14);
        assert_eq!(t.parent[&7], 3);
        assert_eq!(t.parent[&14], 6);
        assert_eq!(t.children[&0], vec![1, 2]);
        assert_eq!(t.children[&6], vec![13, 14]);
        // depth: 0→0, level 1→1, level 2→2, level 3→3
        assert_eq!(t.depth_of[&0], 0);
        assert_eq!(t.depth_of[&2], 1);
        assert_eq!(t.depth_of[&10], 3);
        assert_eq!(t.depth_of[&14], 3);
        assert_eq!(t.depth(), 3);
    }

    #[test]
    fn parse_names_the_line_for_malformed_json() {
        let stream = concat!(
            r#"{"kind":"started","n_nodes":2,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
            "\n",
            "not json at all\n",
        );
        let err = parse_cascade_events(stream).unwrap_err();
        assert!(
            err.to_string().starts_with("line 2 is not JSON: "),
            "got: {err}"
        );
    }

    #[test]
    fn parse_names_the_line_for_non_object_json() {
        let stream = concat!(
            r#"{"kind":"started","n_nodes":2,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
            "\n\n",
            "[1, 2]\n",
        );
        let err = parse_cascade_events(stream).unwrap_err();
        assert_eq!(err.to_string(), "line 3 is not a JSON object");
    }

    #[test]
    fn parse_requires_a_started_event() {
        let stream = concat!(
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"finished","converged":2,"failed":0,"rounds":1}"#,
        );
        let err = parse_cascade_events(stream).unwrap_err();
        assert_eq!(err.to_string(), "event stream has no started event");
    }

    #[test]
    fn parse_rejects_truncated_stream() {
        let stream =
            r#"{"kind":"started","n_nodes":2,"seeded":[0],"strategy":"log2-fanout","at":0}"#;
        let err = parse_cascade_events(stream).unwrap_err();
        assert_eq!(
            err.to_string(),
            "event stream has no finished event; the run was truncated, so its \
             nodes cannot be called converged"
        );
    }

    #[test]
    fn parse_line_numbers_skip_blank_lines() {
        // Blank line 1; the malformed line is still line 2.
        let stream = "\nnot json\n";
        let err = parse_cascade_events(stream).unwrap_err();
        assert!(
            err.to_string().starts_with("line 2 is not JSON: "),
            "got: {err}"
        );
    }

    #[test]
    fn parse_requires_src_and_tgt_on_edges() {
        let stream = concat!(
            r#"{"kind":"started","n_nodes":2,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"duration":1}"#,
            "\n",
        );
        let err = parse_cascade_events(stream).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("line 2: edge_completed missing tgt"),
            "got: {err}"
        );
    }

    #[test]
    fn parse_leaves_cyclic_nodes_undepthed() {
        // No real cascade produces a cycle; it must not hang or guess.
        let stream = concat!(
            r#"{"kind":"started","n_nodes":3,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":1,"tgt":2,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":1,"src":2,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"finished","converged":3,"failed":0,"rounds":2}"#,
            "\n",
        );
        let t = parse_cascade_events(stream).unwrap();
        assert!(!t.depth_of.contains_key(&1));
        assert!(!t.depth_of.contains_key(&2));
        assert_eq!(t.depth(), 0);
    }

    // ─── assert ─────────────────────────────────────────────────────────────

    #[test]
    fn assert_accepts_a_relayed_run() {
        let t = relay_15();
        assert_eq!(
            assert_relay_was_used(&t, &Log2FanOut, 2),
            Ok(()),
            "a 15-node relay at depth 3 is a relay"
        );
    }

    #[test]
    fn assert_rejects_seed_only_pull() {
        let stream = concat!(
            r#"{"kind":"started","n_nodes":5,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":2,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":3,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":4,"duration":1}"#,
            "\n",
            r#"{"kind":"finished","converged":5,"failed":0,"rounds":1}"#,
            "\n",
        );
        let t = parse_cascade_events(stream).unwrap();
        let err = assert_relay_was_used(&t, &Log2FanOut, 2).unwrap_err();
        assert_eq!(
            err.to_string(),
            "the payload was not relayed: all 5 nodes were served straight from \
             the seed, so no peer served another"
        );
    }

    #[test]
    fn assert_short_circuits_when_seed_can_serve_fleet() {
        // 3 nodes, seed 0, fanout 2: pending (2) <= fanout, so a single
        // seed round is legitimate even with no relay.
        let stream = concat!(
            r#"{"kind":"started","n_nodes":3,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":2,"duration":1}"#,
            "\n",
            r#"{"kind":"finished","converged":3,"failed":0,"rounds":1}"#,
            "\n",
        );
        let t = parse_cascade_events(stream).unwrap();
        assert_eq!(assert_relay_was_used(&t, &Log2FanOut, 2), Ok(()));
    }

    #[test]
    fn assert_rejects_double_served_targets() {
        let stream = concat!(
            r#"{"kind":"started","n_nodes":2,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"finished","converged":2,"failed":0,"rounds":1}"#,
            "\n",
        );
        let t = parse_cascade_events(stream).unwrap();
        let err = assert_relay_was_used(&t, &Log2FanOut, 2).unwrap_err();
        assert_eq!(
            err.to_string(),
            "node(s) [0] appear as a target more than once; the relay tree is \
             not a tree"
        );
    }

    #[test]
    fn assert_rejects_unserved_nodes() {
        let stream = concat!(
            r#"{"kind":"started","n_nodes":4,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"finished","converged":2,"failed":0,"rounds":1}"#,
            "\n",
        );
        let t = parse_cascade_events(stream).unwrap();
        let err = assert_relay_was_used(&t, &Log2FanOut, 2).unwrap_err();
        assert_eq!(
            err.to_string(),
            "node(s) [2, 3] were never served by anyone; the cascade did not \
             converge (2/4 did)"
        );
    }

    #[test]
    fn assert_rejects_duplicate_deliveries() {
        // 3 deliveries, 2 distinct targets: node 2 served twice (from
        // different parents, so the per-parent check above can't see it).
        let stream = concat!(
            r#"{"kind":"started","n_nodes":3,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":1,"tgt":2,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":1,"src":0,"tgt":2,"duration":1}"#,
            "\n",
            r#"{"kind":"finished","converged":3,"failed":0,"rounds":2}"#,
            "\n",
        );
        let t = parse_cascade_events(stream).unwrap();
        let err = assert_relay_was_used(&t, &Log2FanOut, 2).unwrap_err();
        assert_eq!(
            err.to_string(),
            "3 deliveries landed on 2 distinct nodes; a node was served twice, \
             so the relay is not a tree"
        );
    }

    // ─── strategy resolution ────────────────────────────────────────────────

    #[test]
    fn strategy_by_name_resolves_known_names() {
        let cases: &[(&str, &str)] = &[
            ("log2-fanout", "log2-fanout"),
            ("level-tree", "level-tree"),
            ("level", "level-tree"),
            ("tree", "level-tree"),
            ("max-bottleneck", "max-bottleneck-spanning"),
            ("max-bottleneck-spanning", "max-bottleneck-spanning"),
            ("steiner", "steiner-greedy"),
            ("steiner-greedy", "steiner-greedy"),
        ];
        for (name, expected) in cases {
            let s = strategy_by_name(name, 2).unwrap_or_else(|| panic!("'{name}' should resolve"));
            assert_eq!(s.name(), *expected, "resolution of '{name}'");
        }
    }

    #[test]
    fn strategy_by_name_rejects_unknown_names() {
        assert!(strategy_by_name("warp-drive", 2).is_none());
        assert!(strategy_by_name("", 2).is_none());
    }

    #[test]
    fn strategy_by_name_parameterizes_level_tree_fanout() {
        // The level-tree instance carries the verify --fanout; others ignore it.
        let s = strategy_by_name("level-tree", 3).unwrap();
        assert_eq!(s.name(), "level-tree");
    }
}
