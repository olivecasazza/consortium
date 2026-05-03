//! Scale smoke: 1024-node cascade with MaxBottleneckSpanning.
//!
//! Run with:
//!   cargo run --example scale_1024 -p consortium-fanout-sim

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use consortium_fanout_sim::{
    fixtures::{rng_from_seed, BandwidthDistribution, FailureSchedule, UplinkDistribution},
    DeterministicExecutor,
};
use consortium_nix::cascade::{Cascade, CascadeNode, NetworkProfile, NodeId, NodeIdAlloc};
use consortium_nix::cascade_events::{CascadeEvent, EventSink};
use consortium_nix::cascade_strategies::MaxBottleneckSpanning;

use consortium_cli::tree::{render, NodeStatus, OutputFormat, TreeNode};

// ============================================================================
// Inline EventCollector
// ============================================================================

struct EventCollector {
    events: Mutex<Vec<CascadeEvent>>,
}

impl EventCollector {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    fn take(&self) -> Vec<CascadeEvent> {
        self.events.lock().unwrap().drain(..).collect()
    }
}

impl EventSink for EventCollector {
    fn emit(&self, event: &CascadeEvent) {
        self.events.lock().unwrap().push(event.clone());
    }
}

// ============================================================================
// OwnedTreeNode
// ============================================================================

struct OwnedTreeNode {
    label: String,
    status: Option<NodeStatus>,
    meta: Vec<(String, String)>,
    children: Vec<OwnedTreeNode>,
}

impl OwnedTreeNode {
    fn leaf(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            status: None,
            meta: Vec::new(),
            children: Vec::new(),
        }
    }

    fn with_status(mut self, s: NodeStatus) -> Self {
        self.status = Some(s);
        self
    }

    fn with_meta(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.meta.push((k.into(), v.into()));
        self
    }

    fn push_child(&mut self, child: OwnedTreeNode) {
        self.children.push(child);
    }
}

impl TreeNode for OwnedTreeNode {
    fn label(&self) -> String {
        self.label.clone()
    }
    fn status(&self) -> Option<NodeStatus> {
        self.status.clone()
    }
    fn metadata(&self) -> Vec<(String, String)> {
        self.meta.clone()
    }
    fn children(&self) -> Vec<&dyn TreeNode> {
        self.children.iter().map(|c| c as &dyn TreeNode).collect()
    }
}

// ============================================================================
// Build tree from events
// ============================================================================

fn topology_from_events(
    events: &[CascadeEvent],
) -> (HashMap<u32, u32>, HashMap<u32, u32>, HashSet<u32>) {
    let mut parent_map: HashMap<u32, u32> = HashMap::new();
    let mut round_map: HashMap<u32, u32> = HashMap::new();
    let mut converged: HashSet<u32> = HashSet::new();
    let mut current_round = 0u32;

    for ev in events {
        match ev {
            CascadeEvent::Started { seeded, .. } => {
                for s in seeded {
                    converged.insert(s.0);
                }
            }
            CascadeEvent::PlanComputed { round, .. } => {
                current_round = *round;
            }
            CascadeEvent::EdgeCompleted { src, tgt, .. } => {
                parent_map.insert(tgt.0, src.0);
                round_map.insert(tgt.0, current_round);
                converged.insert(tgt.0);
                converged.insert(src.0);
            }
            CascadeEvent::RoundCompleted { has_closure, .. } => {
                for id in has_closure {
                    converged.insert(id.0);
                }
            }
            _ => {}
        }
    }

    (parent_map, round_map, converged)
}

fn build_tree(
    events: &[CascadeEvent],
    n_nodes: u32,
    total_rounds: u32,
    label_prefix: &str,
) -> OwnedTreeNode {
    let (parent_map, round_map, converged) = topology_from_events(events);

    let mut children_map: HashMap<u32, Vec<u32>> = HashMap::new();
    for (&child, &parent) in &parent_map {
        children_map.entry(parent).or_default().push(child);
    }
    for v in children_map.values_mut() {
        v.sort_unstable();
    }

    let child_set: HashSet<u32> = parent_map.keys().copied().collect();
    let mut roots: Vec<u32> = converged
        .iter()
        .copied()
        .filter(|id| !child_set.contains(id))
        .collect();
    roots.sort_unstable();

    let unconverged = n_nodes as usize - converged.len();

    fn build_node(
        id: u32,
        parent: Option<u32>,
        round_map: &HashMap<u32, u32>,
        children_map: &HashMap<u32, Vec<u32>>,
        is_seed: bool,
    ) -> OwnedTreeNode {
        let label = if is_seed {
            format!("n{} (seed)", id)
        } else {
            format!("n{}", id)
        };
        let mut node = OwnedTreeNode::leaf(label).with_status(NodeStatus::Ok);

        if let (Some(_p), Some(r)) = (parent, round_map.get(&id)) {
            node = node.with_meta("r", r.to_string());
        }

        if let Some(kids) = children_map.get(&id) {
            for &kid in kids {
                let child_node = build_node(kid, Some(id), round_map, children_map, false);
                node.push_child(child_node);
            }
        }
        node
    }

    let mut root = OwnedTreeNode::leaf(format!(
        "{} [{} rounds, {}/{} converged{}]",
        label_prefix,
        total_rounds,
        converged.len(),
        n_nodes,
        if unconverged > 0 {
            format!(", {} failed", unconverged)
        } else {
            String::new()
        }
    ));

    for seed_id in roots {
        let subtree = build_node(seed_id, None, &round_map, &children_map, true);
        root.push_child(subtree);
    }

    root
}

fn convergence_histogram(events: &[CascadeEvent]) -> Vec<(u32, usize, usize)> {
    let mut histogram: Vec<(u32, usize, usize)> = Vec::new();
    let mut prev_count = 0usize;

    for ev in events {
        if let CascadeEvent::RoundCompleted {
            round, has_closure, ..
        } = ev
        {
            let after = has_closure.len();
            histogram.push((*round, prev_count, after));
            prev_count = after;
        }
    }

    histogram
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    const N: u32 = 1024;
    const SEED: u64 = 0x_cafe_babe_1024;
    // ceil(log2(1024)) + 2 = 10 + 2 = 12
    const MAX_EXPECTED_ROUNDS: u32 = 12;

    println!("=== 1024-node cascade, MaxBottleneckSpanning ===");
    println!("Building network profile for {} nodes...", N);

    let mut rng = rng_from_seed(SEED);
    let mut alloc = NodeIdAlloc::new();

    let nodes: Vec<CascadeNode> = (0..N)
        .map(|_| {
            let id = alloc.alloc();
            CascadeNode::new(id, format!("user@host-{}", id.0))
        })
        .collect();

    let mut seeded = HashSet::new();
    seeded.insert(NodeId(0));

    let mut net = NetworkProfile::default();
    BandwidthDistribution::Bimodal {
        slow: 1024 * 1024,
        fast: 1024 * 1024 * 1024,
        fast_fraction: 0.3,
    }
    .populate(&mut rng, &mut net, N);

    UplinkDistribution::Bimodal {
        slow: 1024 * 1024,
        fast: 1024 * 1024 * 1024,
        fast_fraction: 0.3,
    }
    .populate(&mut rng, &mut net, N);

    let exec = DeterministicExecutor::new(100 * 1024 * 1024, FailureSchedule::None);
    let collector = EventCollector::new();

    println!("Running cascade...");
    let result = Cascade::new()
        .nodes(nodes)
        .seeded(seeded)
        .network(net)
        .strategy(&MaxBottleneckSpanning)
        .executor(&exec)
        .events(&collector)
        .max_rounds(64)
        .run();

    let events = collector.take();

    // Summary
    let total_wall: Duration = result.round_durations.iter().sum();
    println!();
    println!("  rounds:    {}", result.rounds);
    println!("  converged: {}/{} nodes", result.converged.len(), N);
    println!(
        "  wall-time: {:.3}s (sum of round durations)",
        total_wall.as_secs_f64()
    );
    println!("  success:   {}", result.is_success());

    // Convergence assertion
    assert!(
        result.rounds <= MAX_EXPECTED_ROUNDS,
        "Expected convergence in <= {} rounds (ceil(log2(1024))+2), but got {} rounds",
        MAX_EXPECTED_ROUNDS,
        result.rounds
    );
    println!(
        "  assertion: converged in {} rounds <= {} (ceil(log2(1024))+2) [PASS]",
        result.rounds, MAX_EXPECTED_ROUNDS
    );

    // Per-round convergence histogram
    println!();
    println!("--- Per-round convergence ---");
    let histogram = convergence_histogram(&events);
    for (round, before, after) in &histogram {
        println!(
            "  round {:2}: {:5} → {:5} (+{})",
            round,
            before,
            after,
            after - before
        );
    }

    // Tree at depth 3 only (deeper is unreadable at 1024 nodes)
    println!();
    println!("--- Cascade tree (depth 3) ---");
    let tree = build_tree(
        &events,
        N,
        result.rounds,
        "1024-node cascade, MaxBottleneckSpanning",
    );
    let tree3 = render(
        &tree,
        &OutputFormat::Tree {
            max_depth: Some(3),
            color: false,
        },
    );
    println!("{}", tree3);
}
