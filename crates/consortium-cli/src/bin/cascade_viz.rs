//! cascade-viz — visualize cascade event streams.
//!
//! Subcommands:
//!   replay <TRACE_FILE>   Replay a JSONL trace file and render
//!   live                  Run a fresh scenario and render
//!
//! NOTE: Phase 2A's `event_render` module is not yet landed. This binary uses
//! inline `EventCollector` / `JsonlWriter` sinks and a hand-rolled tree fold
//! instead of `event_render::render_events` and `event_render::SnapshotAccumulator`.
//! Once Phase 2A merges, replace the inline impls with imports from
//! `consortium_cli::event_render`.
//!
//! TODO: enable once Phase 2A merges:
//!   use consortium_cli::event_render::{render_events, EventCollector, SnapshotAccumulator};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::sync::Mutex;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use is_terminal::IsTerminal;

use consortium_cli::tree::{render, NodeStatus, OutputFormat, TreeNode};
use consortium_fanout_sim::fixtures::{
    rng_from_seed, BandwidthDistribution, FailureSchedule, SeedDistribution, UplinkDistribution,
};
use consortium_nix::cascade::{
    Cascade, CascadeNode, Log2FanOut, NetworkProfile, NodeId, NodeIdAlloc,
};
use consortium_nix::cascade_events::{CascadeEvent, EventSink};
use consortium_nix::cascade_strategies::{MaxBottleneckSpanning, SteinerGreedy};

// ============================================================================
// CLI definition
// ============================================================================

#[derive(Debug, Parser)]
#[command(
    name = "cascade-viz",
    about = "Visualize cascade event streams (replay JSONL traces or run live scenarios)"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Output format: tree (default), json, yaml, toml, jsonl
    #[arg(short = 'f', long = "format", global = true, default_value = "tree")]
    format: String,

    /// Limit tree depth (tree format only)
    #[arg(short = 'L', long = "max-depth", global = true)]
    max_depth: Option<usize>,

    /// Disable ANSI colors
    #[arg(long = "no-color", global = true)]
    no_color: bool,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Replay a JSONL trace file and render
    Replay(ReplayArgs),

    /// Run a fresh scenario and render
    Live(LiveArgs),
}

#[derive(Debug, Args)]
struct ReplayArgs {
    /// Path to a JSONL trace file (one CascadeEvent per line)
    trace_file: String,
}

#[derive(Debug, Clone, Args)]
struct LiveArgs {
    /// Number of nodes (default 32)
    #[arg(short = 'n', long = "nodes", default_value_t = 32)]
    nodes: u32,

    /// Strategy: log2-fanout (default), max-bottleneck, steiner
    #[arg(short = 's', long = "strategy", default_value = "log2-fanout")]
    strategy: String,

    /// Fraction of nodes pre-seeded (default 0.0)
    #[arg(long = "seed-fraction", default_value_t = 0.0)]
    seed_fraction: f64,

    /// Closure size in MB (default 50)
    #[arg(long = "closure-mb", default_value_t = 50)]
    closure_mb: u64,

    /// Bandwidth style: uniform | bimodal (default uniform)
    #[arg(long = "bandwidth", default_value = "uniform")]
    bandwidth: String,

    /// Per-node uplink in bytes/sec (default unset = no contention)
    #[arg(long = "uplinks")]
    uplinks: Option<u64>,

    /// RNG seed (default 0)
    #[arg(long = "seed", default_value_t = 0)]
    seed: u64,
}

// ============================================================================
// Inline EventSink impls
// (Phase 2A territory; replace imports when event_render.rs lands)
// ============================================================================

/// Accumulates all events into a Vec for batch rendering.
/// TODO: replace with `consortium_cli::event_render::EventCollector` once Phase 2A merges.
struct EventCollector {
    events: Mutex<Vec<CascadeEvent>>,
}

impl EventCollector {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    fn take(self) -> Vec<CascadeEvent> {
        self.events.into_inner().unwrap()
    }
}

impl EventSink for EventCollector {
    fn emit(&self, event: &CascadeEvent) {
        self.events.lock().unwrap().push(event.clone());
    }
}

/// Streams events to stdout as JSONL.
/// TODO: replace with `consortium_cli::event_render::JsonlWriter` once Phase 2A merges.
struct JsonlWriter {
    writer: Mutex<Box<dyn Write + Send>>,
}

impl JsonlWriter {
    fn stdout() -> Self {
        Self {
            writer: Mutex::new(Box::new(io::stdout())),
        }
    }
}

impl EventSink for JsonlWriter {
    fn emit(&self, event: &CascadeEvent) {
        if let Ok(line) = serde_json::to_string(event) {
            let mut w = self.writer.lock().unwrap();
            let _ = writeln!(w, "{line}");
        }
    }
}

// ============================================================================
// Tree fold from events
// ============================================================================

/// State extracted from a Vec<CascadeEvent>.
struct FoldedEvents {
    n_nodes: u32,
    strategy: String,
    rounds: u32,
    converged: usize,
    seeded_set: HashSet<NodeId>,
    /// parent[tgt] = src — set from EdgeCompleted events.
    parent: HashMap<NodeId, NodeId>,
    /// Nodes that received at least one EdgeFailed.
    failed: HashSet<NodeId>,
    /// children_map[src] = [tgt, ...] — derived from parent map.
    children: HashMap<NodeId, Vec<NodeId>>,
}

/// Fold a `Vec<CascadeEvent>` into `FoldedEvents`.
///
/// TODO: replace with `event_render::SnapshotAccumulator` once Phase 2A merges.
fn fold_events(events: &[CascadeEvent]) -> FoldedEvents {
    let mut n_nodes: u32 = 0;
    let mut seeded: Vec<NodeId> = Vec::new();
    let mut strategy = String::from("log2-fanout");
    let mut parent: HashMap<NodeId, NodeId> = HashMap::new();
    let mut failed: HashSet<NodeId> = HashSet::new();
    let mut rounds: u32 = 0;
    let mut converged: usize = 0;

    for ev in events {
        match ev {
            CascadeEvent::Started {
                n_nodes: n,
                seeded: s,
                strategy: strat,
                ..
            } => {
                n_nodes = *n;
                seeded = s.clone();
                strategy = strat.clone();
            }
            CascadeEvent::EdgeCompleted { src, tgt, .. } => {
                parent.insert(*tgt, *src);
            }
            CascadeEvent::EdgeFailed { tgt, .. } => {
                failed.insert(*tgt);
            }
            CascadeEvent::Finished {
                converged: c,
                rounds: r,
                ..
            } => {
                rounds = *r;
                converged = *c;
            }
            _ => {}
        }
    }

    let seeded_set: HashSet<NodeId> = seeded.iter().copied().collect();

    // Build children map from parent map.
    let mut children: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for i in 0..n_nodes {
        children.entry(NodeId(i)).or_default();
    }
    for (&tgt, &src) in &parent {
        children.entry(src).or_default().push(tgt);
    }
    // Sort children for stable output.
    for kids in children.values_mut() {
        kids.sort();
    }

    FoldedEvents {
        n_nodes,
        strategy,
        rounds,
        converged,
        seeded_set,
        parent,
        failed,
        children,
    }
}

// ============================================================================
// TreeNode impl
// ============================================================================

struct CascadeTreeNode {
    id: NodeId,
    status: NodeStatus,
    child_nodes: Vec<CascadeTreeNode>,
}

impl CascadeTreeNode {
    fn build(
        id: NodeId,
        seeded: &HashSet<NodeId>,
        parent: &HashMap<NodeId, NodeId>,
        failed: &HashSet<NodeId>,
        children_map: &HashMap<NodeId, Vec<NodeId>>,
        depth: usize,
        max_depth: Option<usize>,
    ) -> Self {
        let status = if failed.contains(&id) {
            NodeStatus::Failed
        } else if seeded.contains(&id) || parent.contains_key(&id) {
            NodeStatus::Ok
        } else {
            NodeStatus::Pending
        };

        // Let the tree renderer handle truncation markers; stop building here.
        let child_nodes = if max_depth.map_or(true, |m| depth < m) {
            children_map
                .get(&id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|cid| {
                    Self::build(
                        cid,
                        seeded,
                        parent,
                        failed,
                        children_map,
                        depth + 1,
                        max_depth,
                    )
                })
                .collect()
        } else {
            Vec::new()
        };

        Self {
            id,
            status,
            child_nodes,
        }
    }
}

impl TreeNode for CascadeTreeNode {
    fn label(&self) -> String {
        format!("host-{}", self.id.0)
    }

    fn status(&self) -> Option<NodeStatus> {
        Some(self.status.clone())
    }

    fn children(&self) -> Vec<&dyn TreeNode> {
        self.child_nodes
            .iter()
            .map(|c| c as &dyn TreeNode)
            .collect()
    }
}

/// Synthetic root that wraps the whole cascade.
struct SyntheticRoot {
    label: String,
    status: NodeStatus,
    kids: Vec<CascadeTreeNode>,
}

impl TreeNode for SyntheticRoot {
    fn label(&self) -> String {
        self.label.clone()
    }

    fn status(&self) -> Option<NodeStatus> {
        Some(self.status.clone())
    }

    fn metadata(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    fn children(&self) -> Vec<&dyn TreeNode> {
        self.kids.iter().map(|c| c as &dyn TreeNode).collect()
    }
}

// ============================================================================
// Rendering
// ============================================================================

fn resolve_format(
    format_str: &str,
    no_color: bool,
    max_depth: Option<usize>,
) -> Result<OutputFormat> {
    let mut fmt = OutputFormat::parse(format_str).map_err(|e| anyhow::anyhow!(e))?;

    // Apply --no-color and --max-depth overrides after parse.
    if let OutputFormat::Tree {
        ref mut color,
        max_depth: ref mut fmt_max_depth,
    } = fmt
    {
        let is_tty = io::stdout().is_terminal();
        *color = is_tty && !no_color;
        if let Some(d) = max_depth {
            *fmt_max_depth = Some(d);
        }
    }

    Ok(fmt)
}

fn render_events_vec(events: Vec<CascadeEvent>, cli: &Cli) -> Result<()> {
    let fmt_str = cli.format.as_str();

    // jsonl: re-serialize each event line.
    if fmt_str == "jsonl" {
        for ev in &events {
            println!("{}", serde_json::to_string(ev)?);
        }
        return Ok(());
    }

    // json / yaml / toml: serialize the raw event list.
    if matches!(fmt_str, "json" | "yaml" | "yml" | "toml") {
        let value = serde_json::to_value(&events)?;
        match fmt_str {
            "json" => println!("{}", serde_json::to_string_pretty(&value)?),
            "yaml" | "yml" => print!("{}", serde_yaml::to_string(&value)?),
            "toml" => {
                let mut wrapper = BTreeMap::new();
                wrapper.insert("events".to_string(), value);
                println!(
                    "{}",
                    toml::to_string(&wrapper).unwrap_or_else(|e| format!("# toml error: {e}\n"))
                );
            }
            _ => unreachable!(),
        }
        return Ok(());
    }

    // tree format: fold events → tree → render.
    let fmt = resolve_format(fmt_str, cli.no_color, cli.max_depth)?;
    let max_depth_for_build = if let OutputFormat::Tree { max_depth, .. } = &fmt {
        *max_depth
    } else {
        None
    };

    let folded = fold_events(&events);

    // Root nodes: seeded nodes with no parent.
    let mut root_ids: Vec<NodeId> = folded
        .seeded_set
        .iter()
        .copied()
        .filter(|n| !folded.parent.contains_key(n))
        .collect();
    root_ids.sort();

    // Gather the set of all reachable node ids from roots.
    fn gather_reachable(
        id: NodeId,
        children: &HashMap<NodeId, Vec<NodeId>>,
        acc: &mut HashSet<NodeId>,
    ) {
        acc.insert(id);
        if let Some(kids) = children.get(&id) {
            for &k in kids {
                gather_reachable(k, children, acc);
            }
        }
    }
    let mut reachable: HashSet<NodeId> = HashSet::new();
    for &r in &root_ids {
        gather_reachable(r, &folded.children, &mut reachable);
    }

    // Any node not reachable and not in parent map is an orphan; attach it to root too.
    let orphan_ids: Vec<NodeId> = (0..folded.n_nodes)
        .map(NodeId)
        .filter(|id| !reachable.contains(id) && !folded.parent.contains_key(id))
        .collect();

    let build_kids = |ids: Vec<NodeId>| -> Vec<CascadeTreeNode> {
        ids.into_iter()
            .map(|id| {
                CascadeTreeNode::build(
                    id,
                    &folded.seeded_set,
                    &folded.parent,
                    &folded.failed,
                    &folded.children,
                    1,
                    max_depth_for_build,
                )
            })
            .collect()
    };

    let mut all_root_kids = build_kids(root_ids);
    all_root_kids.extend(build_kids(orphan_ids));

    let overall_status = if folded.failed.is_empty() {
        NodeStatus::Ok
    } else {
        NodeStatus::Failed
    };

    let root = SyntheticRoot {
        label: format!(
            "cascade [strategy={} nodes={} rounds={} converged={}]",
            folded.strategy, folded.n_nodes, folded.rounds, folded.converged
        ),
        status: overall_status,
        kids: all_root_kids,
    };

    print!("{}", render(&root, &fmt));
    Ok(())
}

// ============================================================================
// Subcommand handlers
// ============================================================================

fn run_replay(args: &ReplayArgs, cli: &Cli) -> Result<()> {
    let file = File::open(&args.trace_file)
        .with_context(|| format!("failed to open trace file: {}", args.trace_file))?;

    // jsonl: stream directly without accumulating.
    if cli.format == "jsonl" {
        let reader = BufReader::new(file);
        for (i, line) in reader.lines().enumerate() {
            let line = line.with_context(|| format!("failed to read line {}", i + 1))?;
            if line.trim().is_empty() {
                continue;
            }
            let ev: CascadeEvent = serde_json::from_str(&line)
                .with_context(|| format!("invalid event JSON on line {}: {line}", i + 1))?;
            println!("{}", serde_json::to_string(&ev)?);
        }
        return Ok(());
    }

    let reader = BufReader::new(file);
    let mut events: Vec<CascadeEvent> = Vec::new();
    for (i, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read line {}", i + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let ev: CascadeEvent = serde_json::from_str(&line)
            .with_context(|| format!("invalid event JSON on line {}: {line}", i + 1))?;
        events.push(ev);
    }

    render_events_vec(events, cli)
}

fn run_live(args: &LiveArgs, cli: &Cli) -> Result<()> {
    let bandwidth = match args.bandwidth.as_str() {
        "bimodal" => BandwidthDistribution::Bimodal {
            slow: 10 * 1024 * 1024,
            fast: 1024 * 1024 * 1024,
            fast_fraction: 0.3,
        },
        _ => BandwidthDistribution::Uniform(100 * 1024 * 1024),
    };

    let uplinks = args.uplinks.map(UplinkDistribution::Uniform);
    let closure_bytes = args.closure_mb * 1024 * 1024;

    // jsonl: stream directly via JsonlWriter.
    if cli.format == "jsonl" {
        let sink = JsonlWriter::stdout();
        build_and_run(args, closure_bytes, bandwidth, uplinks, &sink)?;
        return Ok(());
    }

    // All other formats: accumulate then render.
    let collector = EventCollector::new();
    build_and_run(args, closure_bytes, bandwidth, uplinks, &collector)?;
    let events = collector.take();
    render_events_vec(events, cli)
}

fn build_and_run<S: EventSink>(
    args: &LiveArgs,
    closure_bytes: u64,
    bandwidth: BandwidthDistribution,
    uplinks: Option<UplinkDistribution>,
    sink: &S,
) -> Result<()> {
    let n_nodes = args.nodes;

    // Nodes.
    let mut alloc = NodeIdAlloc::new();
    let nodes: Vec<CascadeNode> = (0..n_nodes)
        .map(|_| {
            let id = alloc.alloc();
            CascadeNode::new(id, format!("user@host-{}", id.0))
        })
        .collect();

    // Seeded set.
    let seeded: HashSet<NodeId> = if args.seed_fraction > 0.0 {
        let mut rng = rng_from_seed(args.seed);
        SeedDistribution::Random {
            fraction: args.seed_fraction,
        }
        .sample(&mut rng, n_nodes)
    } else {
        let mut s = HashSet::new();
        if n_nodes > 0 {
            s.insert(NodeId(0));
        }
        s
    };

    // Network.
    let net: NetworkProfile = {
        let mut rng = rng_from_seed(args.seed);
        let mut profile = NetworkProfile::default();
        bandwidth.populate(&mut rng, &mut profile, n_nodes);
        if let Some(ref up) = uplinks {
            up.populate(&mut rng, &mut profile, n_nodes);
        }
        profile
    };

    // Executor.
    let exec =
        consortium_fanout_sim::DeterministicExecutor::new(closure_bytes, FailureSchedule::None);

    // Run with the chosen strategy.
    match args.strategy.as_str() {
        "max-bottleneck" | "max-bottleneck-spanning" => {
            Cascade::new()
                .nodes(nodes)
                .seeded(seeded)
                .network(net)
                .strategy(&MaxBottleneckSpanning)
                .executor(&exec)
                .events(sink)
                .run();
        }
        "steiner" | "steiner-greedy" => {
            Cascade::new()
                .nodes(nodes)
                .seeded(seeded)
                .network(net)
                .strategy(&SteinerGreedy)
                .executor(&exec)
                .events(sink)
                .run();
        }
        _ => {
            // default: log2-fanout
            Cascade::new()
                .nodes(nodes)
                .seeded(seeded)
                .network(net)
                .strategy(&Log2FanOut)
                .executor(&exec)
                .events(sink)
                .run();
        }
    }

    Ok(())
}

// ============================================================================
// main
// ============================================================================

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Replay(args) => run_replay(args, &cli),
        Commands::Live(args) => {
            let args = args.clone();
            run_live(&args, &cli)
        }
    }
}
