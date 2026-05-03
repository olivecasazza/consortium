//! cascade-viz — visualize cascade event streams.
//!
//! Subcommands:
//!   replay <TRACE_FILE>   Replay a JSONL trace file and render
//!   live                  Run a fresh scenario and render
//!
//! Renderers + sinks come from `consortium_cli::event_render`. The
//! binary is the thin shell that picks an `EventSink` (collector or
//! JsonlWriter) for the live path, parses JSONL for the replay path,
//! and delegates everything else to `event_render::render_events`.

use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use is_terminal::IsTerminal;

use consortium_cli::event_render::{
    render_events, DelayingExecutor, EventCollector, JsonlWriter, LiveTreeRenderer,
};
use consortium_cli::tree::OutputFormat;
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

    /// Strategy: level-tree (default — pre-shaped F-ary tree, each
    /// round populates one level, matches nh's level-by-level
    /// reveal), log2-fanout, max-bottleneck, or steiner.
    #[arg(short = 's', long = "strategy", default_value = "level-tree")]
    strategy: String,

    /// Fanout for the level-tree strategy (children per node).
    /// 2 → balanced binary tree, 3 → ternary, etc.
    #[arg(long = "fanout", default_value_t = 2)]
    fanout: u32,

    /// Number of pre-seeded nodes (multiple build hosts). Round 0
    /// will have this many parallel deploys instead of just one.
    /// E.g. `--seeds 8` with 64 nodes = 8 parallel spinners in
    /// round 0.
    #[arg(long = "seeds", default_value_t = 1)]
    seeds: u32,

    /// Fraction of nodes pre-seeded (default 0.0). Overrides --seeds
    /// when > 0.0.
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

    /// Disable live re-rendering (collect all events first, render once
    /// at end). Live mode is the default when stdout is a TTY + tree
    /// format; pipes always batch since ANSI cursor codes don't make
    /// sense in a captured stream.
    #[arg(long = "no-watch")]
    no_watch: bool,

    /// Inject artificial wall-time delay between rounds, in milliseconds
    /// (e.g. `--per-round-delay 200`). The deterministic sim runs in
    /// microseconds — without this, the live re-render fires faster
    /// than humans can perceive. Use for demos / visual debugging only;
    /// it does NOT affect the `round_durations` reported in traces (those
    /// stay sim-time).
    #[arg(long = "per-round-delay")]
    per_round_delay_ms: Option<u64>,
}

// ============================================================================
// Format resolution
// ============================================================================

fn resolve_format(
    format_str: &str,
    no_color: bool,
    max_depth: Option<usize>,
) -> Result<OutputFormat> {
    let mut fmt = OutputFormat::parse(format_str).map_err(|e| anyhow::anyhow!(e))?;
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

/// Render an in-memory event slice using either the requested format
/// (delegated to `event_render::render_events`) or stream JSONL one
/// event per line if the format is "jsonl".
fn print_events(events: &[CascadeEvent], cli: &Cli) -> Result<()> {
    if cli.format == "jsonl" {
        for ev in events {
            println!("{}", serde_json::to_string(ev)?);
        }
        return Ok(());
    }
    let fmt = resolve_format(&cli.format, cli.no_color, cli.max_depth)?;
    print!("{}", render_events(events, &fmt));
    Ok(())
}

// ============================================================================
// Subcommand handlers
// ============================================================================

fn run_replay(args: &ReplayArgs, cli: &Cli) -> Result<()> {
    let file = File::open(&args.trace_file)
        .with_context(|| format!("failed to open trace file: {}", args.trace_file))?;
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
    print_events(&events, cli)
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

    // jsonl: stream live to stdout via JsonlWriter sink — events appear
    // as they're emitted, no buffering through a Vec.
    if cli.format == "jsonl" {
        let sink = JsonlWriter::new(Box::new(io::stdout()));
        run_scenario(args, closure_bytes, bandwidth, uplinks, &sink, None);
        return Ok(());
    }

    // Live tree re-render is the default when:
    // - format is `tree` (the only format that has a tree to redraw)
    // - stdout is a TTY (ANSI escapes need a real terminal)
    // - --no-watch wasn't passed
    // Otherwise fall through to batch: collect all events, render once.
    let live_eligible = cli.format == "tree" && io::stdout().is_terminal() && !args.no_watch;
    if live_eligible {
        let color = !cli.no_color;
        let renderer = LiveTreeRenderer::new(color, cli.max_depth);
        let delay = args
            .per_round_delay_ms
            .map(std::time::Duration::from_millis);
        run_scenario(args, closure_bytes, bandwidth, uplinks, &renderer, delay);
        // The renderer prints the final frame on `Finished`; nothing more
        // for us to flush.
        return Ok(());
    }

    // Batch path: accumulate, then delegate to print_events.
    let collector = EventCollector::new();
    run_scenario(args, closure_bytes, bandwidth, uplinks, &collector, None);
    let events = collector.events();
    print_events(&events, cli)
}

/// Build a Cascade from `args` + run it through the given `EventSink`.
/// All scenario wiring (nodes, seeded set, network, executor, strategy)
/// lives here; the sink is the only consumer-specific bit.
fn run_scenario<S: EventSink>(
    args: &LiveArgs,
    closure_bytes: u64,
    bandwidth: BandwidthDistribution,
    uplinks: Option<UplinkDistribution>,
    sink: &S,
    per_round_delay: Option<std::time::Duration>,
) {
    let n_nodes = args.nodes;

    let mut alloc = NodeIdAlloc::new();
    let nodes: Vec<CascadeNode> = (0..n_nodes)
        .map(|_| {
            let id = alloc.alloc();
            CascadeNode::new(id, format!("user@host-{}", id.0))
        })
        .collect();

    let seeded: HashSet<NodeId> = if args.seed_fraction > 0.0 {
        let mut rng = rng_from_seed(args.seed);
        SeedDistribution::Random {
            fraction: args.seed_fraction,
        }
        .sample(&mut rng, n_nodes)
    } else {
        // Multi-seed: NodeIds 0..seeds. Round 0 has `seeds` parallel
        // deploys. Capped to n_nodes, minimum 1.
        let count = args.seeds.min(n_nodes).max(1);
        (0..count).map(NodeId).collect()
    };

    let net: NetworkProfile = {
        let mut rng = rng_from_seed(args.seed);
        let mut profile = NetworkProfile::default();
        bandwidth.populate(&mut rng, &mut profile, n_nodes);
        if let Some(ref up) = uplinks {
            up.populate(&mut rng, &mut profile, n_nodes);
        }
        profile
    };

    let base_exec =
        consortium_fanout_sim::DeterministicExecutor::new(closure_bytes, FailureSchedule::None);

    // If --per-round-delay set, wrap the deterministic executor in a
    // DelayingExecutor so each dispatch() sleeps for the configured
    // duration. The sleep happens BETWEEN PlanComputed (renderer paints
    // ⏵ spinners) and EdgeCompleted (renderer paints ✔), so the spinner
    // frame is actually visible.
    let delayed_exec = per_round_delay.map(|delay| DelayingExecutor {
        inner: &base_exec as &dyn consortium_nix::cascade::RoundExecutor,
        delay,
    });
    let exec: &dyn consortium_nix::cascade::RoundExecutor = delayed_exec
        .as_ref()
        .map(|d| d as &dyn consortium_nix::cascade::RoundExecutor)
        .unwrap_or(&base_exec);

    let level_tree = consortium_nix::cascade_strategies::LevelTreeFanOut::new(args.fanout.max(1));
    match args.strategy.as_str() {
        "level-tree" | "level" | "tree" => {
            Cascade::new()
                .nodes(nodes)
                .seeded(seeded)
                .network(net)
                .strategy(&level_tree)
                .executor(exec)
                .events(sink)
                .run();
        }
        "max-bottleneck" | "max-bottleneck-spanning" => {
            Cascade::new()
                .nodes(nodes)
                .seeded(seeded)
                .network(net)
                .strategy(&MaxBottleneckSpanning)
                .executor(exec)
                .events(sink)
                .run();
        }
        "steiner" | "steiner-greedy" => {
            Cascade::new()
                .nodes(nodes)
                .seeded(seeded)
                .network(net)
                .strategy(&SteinerGreedy)
                .executor(exec)
                .events(sink)
                .run();
        }
        _ => {
            Cascade::new()
                .nodes(nodes)
                .seeded(seeded)
                .network(net)
                .strategy(&Log2FanOut)
                .executor(exec)
                .events(sink)
                .run();
        }
    }
}

// ============================================================================
// main
// ============================================================================

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Replay(args) => run_replay(args, &cli),
        Commands::Live(args) => run_live(args, &cli),
    }
}
