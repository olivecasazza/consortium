//! `cast cascade` — visualize cascade event streams, and verify they relayed.
//!
//! Subcommands:
//! - `tree <TRACE_FILE>`  replay a JSONL trace file and render it
//! - `verify <TRACE_FILE>` assert a trace is a real peer-to-peer relay
//! - `live`               run a fresh scenario and render it
//!
//! Renderers + sinks come from [`crate::event_render`]; the relay assertion
//! comes from [`consortium_nix::cascade_tree`], next to the `CascadeEvent`
//! type that produces the stream. This module is the library half of the old
//! `cascade-viz` binary, so it stays testable without spawning a process.

use std::fs::File;
use std::io::{self, BufRead, BufReader};

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use is_terminal::IsTerminal;

use crate::event_render::{
    render_events, DelayingExecutor, EventCollector, JsonlWriter, LiveTreeRenderer,
};
use crate::tree::OutputFormat;
use consortium_fanout_sim::fixtures::{
    rng_from_seed, BandwidthDistribution, FailureSchedule, SeedDistribution, UplinkDistribution,
};
use consortium_nix::cascade::{
    Cascade, CascadeNode, FanoutOrder, NetworkProfile, NodeIdAlloc, OrderedFanout,
};
use consortium_nix::cascade_events::{CascadeEvent, EventSink};
use consortium_nix::cascade_strategies::{
    find_strategy, parse_strategy, strategy_catalog, strategy_takes_traversal_order,
};
use consortium_nix::cascade_tree::{assert_relay_shape, parse_cascade_events};

// ============================================================================
// CLI definition
// ============================================================================

/// Flags shared by the subcommands that render a tree.
#[derive(Debug, Clone, Args)]
pub struct RenderArgs {
    /// Output format: tree (default), json, yaml, toml, jsonl
    #[arg(short = 'f', long = "format", default_value = "tree")]
    pub format: String,

    /// Limit tree depth (tree format only)
    #[arg(short = 'L', long = "max-depth")]
    pub max_depth: Option<usize>,

    /// Disable ANSI colors
    #[arg(long = "no-color")]
    pub no_color: bool,
}

#[derive(Debug, Args)]
pub struct TreeArgs {
    /// Path to a JSONL trace file (one CascadeEvent per line)
    pub trace_file: String,

    #[command(flatten)]
    pub render: RenderArgs,
}

#[derive(Debug, Args)]
pub struct VerifyArgs {
    /// Path to a JSONL trace file (one CascadeEvent per line)
    pub trace_file: String,

    /// Fanout the cascade was run with. A fleet the seed could serve in one
    /// round is exempt from the relay check.
    #[arg(long)]
    pub fanout: u32,

    /// Expected fleet size; fails if the trace's started event disagrees.
    #[arg(long)]
    pub nodes: Option<u32>,
}

#[derive(Debug, Clone, Args)]
pub struct LiveArgs {
    /// Number of nodes (default 32)
    #[arg(short = 'n', long = "nodes", default_value_t = 32)]
    pub nodes: u32,

    /// Strategy (default `level-tree`). Resolved through the registry:
    /// every strategy has a canonical name plus short aliases — see
    /// them all with `cast cascade strategies`.
    #[arg(short = 's', long = "strategy", default_value = "level-tree")]
    pub strategy: String,

    /// Fanout for the level-tree strategy (children per node).
    /// 2 → balanced binary tree, 3 → ternary, etc.
    #[arg(long = "fanout", default_value_t = 2)]
    pub fanout: u32,

    /// Number of pre-seeded nodes (multiple build hosts). Round 0
    /// will have this many parallel deploys instead of just one.
    #[arg(long = "seeds", default_value_t = 1)]
    pub seeds: u32,

    /// Fraction of nodes pre-seeded (default 0.0). Overrides --seeds
    /// when > 0.0.
    #[arg(long = "seed-fraction", default_value_t = 0.0)]
    pub seed_fraction: f64,

    /// Closure size in MB (default 50)
    #[arg(long = "closure-mb", default_value_t = 50)]
    pub closure_mb: u64,

    /// Bandwidth style: uniform | bimodal (default uniform)
    #[arg(long = "bandwidth", default_value = "uniform")]
    pub bandwidth: String,

    /// Per-node uplink in bytes/sec (default unset = no contention)
    #[arg(long = "uplinks")]
    pub uplinks: Option<u64>,

    /// RNG seed (default 0)
    #[arg(long = "seed", default_value_t = 0)]
    pub seed: u64,

    /// Random per-edge failure rate (0.0 to 1.0), reproducible from
    /// --failure-seed. Use to demo orphan re-routing.
    #[arg(long = "failure-rate", default_value_t = 0.0)]
    pub failure_rate: f64,

    /// Seed for the random failure RNG. Same seed + same scenario
    /// reproduces the exact same fail/succeed sequence.
    #[arg(long = "failure-seed", default_value_t = 0)]
    pub failure_seed: u64,

    /// Disable live re-rendering (collect all events first, render once
    /// at end).
    #[arg(long = "no-watch")]
    pub no_watch: bool,

    /// Inject artificial wall-time delay between rounds, in milliseconds.
    /// Demos / visual debugging only; does not affect reported durations.
    #[arg(long = "per-round-delay")]
    pub per_round_delay_ms: Option<u64>,
}

/// The `cast cascade` subcommands.
#[derive(Debug, Subcommand)]
pub enum CascadeCommands {
    /// Replay a JSONL trace file and render
    Tree(TreeArgs),

    /// Verify a trace relayed peer-to-peer: tree shape, depth, and rounds
    /// must come from a relay, not a host push. Prints the topology summary
    /// as JSON on success; exits non-zero with a specific diagnostic on
    /// failure.
    Verify(VerifyArgs),

    /// Run a fresh scenario and render
    Live {
        #[command(flatten)]
        live: LiveArgs,

        #[command(flatten)]
        render: RenderArgs,
    },
}

// ============================================================================
// Dispatch
// ============================================================================

/// Run one `cast cascade` subcommand. Render output goes to stdout; errors
/// bubble up so the caller can print `error: …` and exit non-zero.
pub fn dispatch(command: CascadeCommands) -> Result<()> {
    match command {
        CascadeCommands::Tree(args) => {
            print!("{}", render_trace_file(&args.trace_file, &args.render)?);
        }
        CascadeCommands::Verify(args) => {
            println!(
                "{}",
                verify_trace_file(&args.trace_file, args.fanout, args.nodes)?
            );
        }
        CascadeCommands::Live { live, render } => run_live(&live, &render)?,
    }
    Ok(())
}

// ============================================================================
// Format resolution + rendering
// ============================================================================

fn resolve_format(render: &RenderArgs) -> Result<OutputFormat> {
    let mut fmt = OutputFormat::parse(&render.format).map_err(|e| anyhow::anyhow!(e))?;
    if let OutputFormat::Tree {
        color,
        max_depth: fmt_max_depth,
    } = &mut fmt
    {
        let is_tty = io::stdout().is_terminal();
        *color = is_tty && !render.no_color;
        if let Some(d) = render.max_depth {
            *fmt_max_depth = Some(d);
        }
    }
    Ok(fmt)
}

/// Replay a JSONL trace file and render it. The `jsonl` format streams the
/// events back one per line; every other format delegates to
/// [`render_events`].
pub fn render_trace_file(path: &str, render: &RenderArgs) -> Result<String> {
    let file = File::open(path).with_context(|| format!("failed to open trace file: {path}"))?;
    let mut events: Vec<CascadeEvent> = Vec::new();
    for (i, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("failed to read line {}", i + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let ev: CascadeEvent = serde_json::from_str(&line)
            .with_context(|| format!("invalid event JSON on line {}: {line}", i + 1))?;
        events.push(ev);
    }
    if render.format == "jsonl" {
        let mut out = String::new();
        for ev in &events {
            out.push_str(&serde_json::to_string(ev)?);
            out.push('\n');
        }
        return Ok(out);
    }
    let fmt = resolve_format(render)?;
    Ok(render_events(&events, &fmt))
}

// ============================================================================
// Verification
// ============================================================================

/// Verify a trace relayed peer-to-peer and return the topology summary as
/// JSON — the same `{"nodes", "rounds", "relay_depth", "relayed_nodes"}` the
/// harness's `verify_cascade_relay` produced. Every failure — unreadable
/// file, malformed line, truncated stream, star-shaped tree, wrong fleet
/// size — exits non-zero with a specific diagnostic.
pub fn verify_trace_file(path: &str, fanout: u32, nodes: Option<u32>) -> Result<String> {
    if fanout < 1 {
        anyhow::bail!("fanout must be at least 1, got {fanout}");
    }
    let stream = std::fs::read_to_string(path)
        .with_context(|| format!("failed to open trace file: {path}"))?;
    let topology = parse_cascade_events(&stream)
        .map_err(|e| anyhow::anyhow!("cascade relay check failed: {e}"))?;
    let strategy = find_strategy(&topology.strategy).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown strategy '{}' in started event; available: {}",
            topology.strategy,
            strategy_catalog()
        )
    })?;
    // The strategy's own round rule — its inherent expected_rounds via the
    // registry — is what this trace is measured against.
    let expected_rounds =
        (strategy.rounds_rule)(topology.n_nodes as usize, topology.seeded.len(), fanout);
    assert_relay_shape(&topology, strategy.canonical, expected_rounds, fanout)
        .map_err(|e| anyhow::anyhow!("cascade relay check failed: {e}"))?;
    // The relay assertion passes on any fleet size; the harness additionally
    // pins how many nodes the run covered.
    if let Some(count) = nodes {
        if topology.n_nodes != count {
            anyhow::bail!(
                "cascade covered {} nodes, expected {}",
                topology.n_nodes,
                count
            );
        }
    }
    Ok(serde_json::json!({
        "nodes": topology.n_nodes,
        "rounds": topology.rounds,
        "relay_depth": topology.depth(),
        "relayed_nodes": topology.parent.len(),
    })
    .to_string())
}

// ============================================================================
// Live scenarios
// ============================================================================

/// Run a fresh scenario and render it. Live tree re-rendering is the default
/// when the format is `tree`, stdout is a TTY, and --no-watch wasn't passed;
/// everything else batches.
pub fn run_live(args: &LiveArgs, render: &RenderArgs) -> Result<()> {
    run_live_with_order(args, render, FanoutOrder::Default)
}

/// [`run_live`] under an explicit fanout traversal order. Crate-internal
/// routing for the binary's `--order` flag: the flag cannot live on the
/// public [`LiveArgs`] — that struct is exhaustively constructible
/// downstream, so a new field is a semver-major break — so the private
/// CLI enum carries it and hands the parsed value here.
pub(crate) fn run_live_with_order(
    args: &LiveArgs,
    render: &RenderArgs,
    order: FanoutOrder,
) -> Result<()> {
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
    if render.format == "jsonl" {
        let sink = JsonlWriter::new(Box::new(io::stdout()));
        run_scenario(args, closure_bytes, bandwidth, uplinks, &sink, None, order)?;
        return Ok(());
    }

    // Live tree re-render is the default when:
    // - format is `tree` (the only format that has a tree to redraw)
    // - stdout is a TTY (ANSI escapes need a real terminal)
    // - --no-watch wasn't passed
    // Otherwise fall through to batch: collect all events, render once.
    let live_eligible = render.format == "tree" && io::stdout().is_terminal() && !args.no_watch;
    if live_eligible {
        let color = !render.no_color;
        // Compose nom-style header lines. Identity always shows; scenario
        // knobs only when non-default; failure / timing config only when
        // explicitly enabled.
        let mut header_lines: Vec<String> = Vec::new();

        header_lines.push(format!(
            "Strategy: {} || Nodes: {}",
            args.strategy, args.nodes
        ));

        let mut scenario: Vec<String> = Vec::new();
        if args.fanout != 2 {
            scenario.push(format!("Fanout: {}", args.fanout));
        }
        if args.seeds != 1 {
            scenario.push(format!("Seeds: {}", args.seeds));
        }
        if args.closure_mb != 50 {
            scenario.push(format!("Closure: {}MB", args.closure_mb));
        }
        if args.bandwidth != "uniform" {
            scenario.push(format!("Bandwidth: {}", args.bandwidth));
        }
        if let Some(uplink) = args.uplinks {
            scenario.push(format!("Uplinks: {}B/s", uplink));
        }
        if !scenario.is_empty() {
            header_lines.push(scenario.join(" || "));
        }

        let mut runtime: Vec<String> = Vec::new();
        if args.failure_rate > 0.0 {
            runtime.push(format!(
                "Failures: {:.0}% (seed={})",
                args.failure_rate * 100.0,
                args.failure_seed
            ));
        }
        if let Some(delay_ms) = args.per_round_delay_ms {
            runtime.push(format!("Delay: {}ms/round", delay_ms));
        }
        if !runtime.is_empty() {
            header_lines.push(runtime.join(" || "));
        }

        let renderer =
            LiveTreeRenderer::new(color, render.max_depth).with_header_lines(header_lines);
        let delay = args
            .per_round_delay_ms
            .map(std::time::Duration::from_millis);
        run_scenario(
            args,
            closure_bytes,
            bandwidth,
            uplinks,
            &renderer,
            delay,
            order,
        )?;
        // The renderer prints the final frame on `Finished`; nothing more
        // for us to flush.
        return Ok(());
    }

    // Batch path: accumulate, then delegate to render_events.
    let collector = EventCollector::new();
    run_scenario(
        args,
        closure_bytes,
        bandwidth,
        uplinks,
        &collector,
        None,
        order,
    )?;
    let events = collector.events();
    let fmt = resolve_format(render)?;
    print!("{}", render_events(&events, &fmt));
    Ok(())
}

/// Build a `Cascade` from `args` + run it through the given `EventSink`.
/// All scenario wiring (nodes, seeded set, network, executor, strategy)
/// lives here; the sink is the only consumer-specific bit.
fn run_scenario<S: EventSink>(
    args: &LiveArgs,
    closure_bytes: u64,
    bandwidth: BandwidthDistribution,
    uplinks: Option<UplinkDistribution>,
    sink: &S,
    per_round_delay: Option<std::time::Duration>,
    order: FanoutOrder,
) -> Result<()> {
    let n_nodes = args.nodes;

    let mut alloc = NodeIdAlloc::new();
    let nodes: Vec<CascadeNode> = (0..n_nodes)
        .map(|_| {
            let id = alloc.alloc();
            CascadeNode::new(id, format!("user@host-{}", id.0))
        })
        .collect();

    let seeded: std::collections::HashSet<consortium_nix::cascade::NodeId> =
        if args.seed_fraction > 0.0 {
            let mut rng = rng_from_seed(args.seed);
            SeedDistribution::Random {
                fraction: args.seed_fraction,
            }
            .sample(&mut rng, n_nodes)
        } else {
            // Multi-seed: NodeIds 0..seeds. Round 0 has `seeds` parallel
            // deploys. Capped to n_nodes, minimum 1.
            let count = args.seeds.min(n_nodes).max(1);
            (0..count).map(consortium_nix::cascade::NodeId).collect()
        };

    let net: NetworkProfile = {
        let mut rng = rng_from_seed(args.seed);
        let mut profile = NetworkProfile::default();
        bandwidth.populate(&mut rng, &mut profile, n_nodes);
        if let Some(up) = &uplinks {
            up.populate(&mut rng, &mut profile, n_nodes);
        }
        profile
    };

    let failures = if args.failure_rate > 0.0 {
        FailureSchedule::Random {
            fraction: args.failure_rate,
            seed: args.failure_seed,
        }
    } else {
        FailureSchedule::None
    };
    let base_exec = consortium_fanout_sim::DeterministicExecutor::new(closure_bytes, failures);

    // If --per-round-delay set, wrap the deterministic executor in a
    // DelayingExecutor so each dispatch() sleeps for the configured
    // duration, making the spinner frame visible.
    let delayed_exec = per_round_delay.map(|delay| DelayingExecutor {
        inner: &base_exec as &dyn consortium_nix::cascade::RoundExecutor,
        delay,
    });
    let exec: &dyn consortium_nix::cascade::RoundExecutor = delayed_exec
        .as_ref()
        .map(|d| d as &dyn consortium_nix::cascade::RoundExecutor)
        .unwrap_or(&base_exec);

    // One strategy, resolved through the registry — the same table the
    // copy binary and the verifier read. Unknown names are errors that
    // list what IS available; there is no silent fallback.
    let mut strategy = parse_strategy(&args.strategy, args.fanout)?;
    // bfs/dfs parameterize the fanout pairing itself: only the log2
    // fanout consumes them. Any other combination is an error, never a
    // silent no-op.
    if order != FanoutOrder::Default {
        if !strategy_takes_traversal_order(&args.strategy) {
            let order_name = consortium_nix::cascade::FANOUT_ORDERS
                .iter()
                .find(|spec| spec.order == order)
                .map(|spec| spec.canonical)
                .unwrap_or("non-default");
            anyhow::bail!(
                "traversal order '{}' parameterizes the log2 fanout pairing; \
                 strategy '{}' does not consume it",
                order_name,
                args.strategy
            );
        }
        strategy = Box::new(OrderedFanout::new(order));
    }
    Cascade::new()
        .nodes(nodes)
        .seeded(seeded)
        .network(net)
        .strategy(strategy.as_ref())
        .executor(exec)
        .events(sink)
        .run();
    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serde_json::json;

    /// Minimal parser harness so the subcommand's own surface is testable
    /// without constructing all of `cast`'s `Args`.
    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        cmd: CascadeCommands,
    }

    fn parse(args: &[&str]) -> CascadeCommands {
        let cli = TestCli::parse_from(args);
        cli.cmd
    }

    /// The 15-node level-tree relay fixture (see consortium-nix
    /// `cascade_tree::tests`), as JSONL text.
    const RELAY_15: &str = concat!(
        r#"{"kind":"started","n_nodes":15,"seeded":[0],"strategy":"level-tree","at":0}"#,
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
        "\n",
    );

    /// A 2-node trace in the documented wire shape.
    const PAIR: &str = concat!(
        r#"{"kind":"started","n_nodes":2,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
        "\n",
        r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":100000000}"#,
        "\n",
        r#"{"kind":"finished","converged":2,"failed":0,"rounds":1}"#,
        "\n",
    );

    fn write_trace(dir: &tempfile::TempDir, name: &str, contents: &str) -> String {
        let path = dir.path().join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_str().unwrap().to_string()
    }

    fn render_args(format: &str) -> RenderArgs {
        RenderArgs {
            format: format.to_string(),
            max_depth: None,
            no_color: true,
        }
    }

    // ─── tree (replay) ──────────────────────────────────────────────────────

    #[test]
    fn tree_names_a_missing_file() {
        let err = render_trace_file("/nonexistent/trace.jsonl", &render_args("tree")).unwrap_err();
        assert!(
            err.to_string().contains("failed to open trace file"),
            "got: {err}"
        );
    }

    #[test]
    fn tree_renders_a_trace() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "trace.jsonl", PAIR);
        let out = render_trace_file(&path, &render_args("tree")).unwrap();
        assert!(out.contains("n0"), "expected n0 in: {out}");
        assert!(out.contains("✔"), "expected converged glyph in: {out}");
    }

    #[test]
    fn tree_json_output_is_an_event_array() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "trace.jsonl", PAIR);
        let out = render_trace_file(&path, &render_args("json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        let arr = parsed.as_array().expect("JSON array of events");
        assert!(arr.len() >= 2, "expected at least Started + Finished");
        assert_eq!(arr[0]["kind"], "started");
        assert_eq!(arr.last().unwrap()["kind"], "finished");
    }

    #[test]
    fn tree_jsonl_passes_events_through() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "trace.jsonl", PAIR);
        let out = render_trace_file(&path, &render_args("jsonl")).unwrap();
        let lines: Vec<&str> = out.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), 3, "one line per event: {out}");
        for line in lines {
            serde_json::from_str::<serde_json::Value>(line)
                .unwrap_or_else(|e| panic!("invalid JSON line: {e}\n  {line}"));
        }
    }

    #[test]
    fn tree_names_invalid_event_json_by_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "trace.jsonl", "{\"kind\":\"started\",\"n_nodes\":2,\"seeded\":[0],\"strategy\":\"log2-fanout\",\"at\":0}\ngarbage\n");
        let err = render_trace_file(&path, &render_args("tree")).unwrap_err();
        assert!(
            err.to_string().contains("invalid event JSON on line 2"),
            "got: {err}"
        );
    }

    // ─── verify ─────────────────────────────────────────────────────────────

    #[test]
    fn verify_accepts_a_relayed_trace() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "relay.jsonl", RELAY_15);
        let out = verify_trace_file(&path, 2, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            parsed,
            json!({
                "nodes": 15,
                "rounds": 3,
                "relay_depth": 3,
                "relayed_nodes": 14,
            })
        );
    }

    #[test]
    fn verify_rejects_a_round_count_the_strategy_could_not_produce() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(
            &dir,
            "too-fast.jsonl",
            &RELAY_15.replace(r#""rounds":3"#, r#""rounds":2"#),
        );
        let err = verify_trace_file(&path, 2, None).unwrap_err();
        assert!(
            err.to_string().contains("2 rounds"),
            "the diagnostic must name the offending round count, got: {err}"
        );
    }

    #[test]
    fn verify_rejects_a_pull_only_trace() {
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
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "pull.jsonl", stream);
        let err = verify_trace_file(&path, 2, None).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("cascade relay check failed: the payload was not relayed:"),
            "got: {err}"
        );
    }

    /// A swarm trace: legal, converged, tree-shaped — and a star. The
    /// negative control: verify must reject it by the star rule, not
    /// with "unknown strategy".
    #[test]
    fn verify_rejects_a_swarm_trace_by_the_star_rule() {
        let stream = concat!(
            r#"{"kind":"started","n_nodes":8,"seeded":[0],"strategy":"swarm","at":0}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":2,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":3,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":4,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":5,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":6,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":7,"duration":1}"#,
            "\n",
            r#"{"kind":"finished","converged":8,"failed":0,"rounds":1}"#,
            "\n",
        );
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "swarm.jsonl", stream);
        let err = verify_trace_file(&path, 2, None).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("cascade relay check failed: the payload was not relayed:"),
            "swarm must trip the star rule, got: {err}"
        );
    }

    /// The live path resolves strategies through the same registry as
    /// copy and verify: an unknown name is an error naming what IS
    /// available, never a silent fallback.
    #[test]
    fn live_rejects_an_unknown_strategy() {
        let args = LiveArgs {
            nodes: 4,
            strategy: "warp-drive".into(),
            fanout: 2,
            seeds: 1,
            seed_fraction: 0.0,
            closure_mb: 1,
            bandwidth: "uniform".into(),
            uplinks: None,
            seed: 0,
            failure_rate: 0.0,
            failure_seed: 0,
            no_watch: true,
            per_round_delay_ms: None,
        };
        let err = run_live(&args, &render_args("json")).unwrap_err();
        assert!(
            err.to_string().contains("unknown strategy 'warp-drive'"),
            "got: {err}"
        );
        assert!(
            err.to_string().contains("level-tree"),
            "the error must list what is available, got: {err}"
        );
    }

    #[test]
    fn verify_names_a_malformed_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "bad.jsonl", "not json\n");
        let err = verify_trace_file(&path, 2, None).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("cascade relay check failed: line 1 is not JSON:"),
            "got: {err}"
        );
    }

    #[test]
    fn verify_requires_a_started_event() {
        let stream = concat!(
            r#"{"kind":"finished","converged":2,"failed":0,"rounds":1}"#,
            "\n",
        );
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "nostart.jsonl", stream);
        let err = verify_trace_file(&path, 2, None).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("cascade relay check failed: event stream has no started event"),
            "got: {err}"
        );
    }

    #[test]
    fn verify_checks_the_requested_node_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "relay.jsonl", RELAY_15);
        let err = verify_trace_file(&path, 2, Some(64)).unwrap_err();
        assert_eq!(err.to_string(), "cascade covered 15 nodes, expected 64");
    }

    #[test]
    fn verify_rejects_a_zero_fanout() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "relay.jsonl", RELAY_15);
        let err = verify_trace_file(&path, 0, None).unwrap_err();
        assert_eq!(err.to_string(), "fanout must be at least 1, got 0");
    }

    #[test]
    fn verify_rejects_an_unknown_strategy() {
        let stream = concat!(
            r#"{"kind":"started","n_nodes":4,"seeded":[0],"strategy":"warp-drive","at":0}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":2,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":3,"duration":1}"#,
            "\n",
            r#"{"kind":"finished","converged":4,"failed":0,"rounds":1}"#,
            "\n",
        );
        let dir = tempfile::tempdir().unwrap();
        let path = write_trace(&dir, "warp.jsonl", stream);
        let err = verify_trace_file(&path, 2, None).unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown strategy 'warp-drive' in started event"),
            "got: {err}"
        );
    }

    // ─── CLI surface ────────────────────────────────────────────────────────

    #[test]
    fn tree_parses_the_contract_flags() {
        let CascadeCommands::Tree(args) = parse(&[
            "cast",
            "tree",
            "t.jsonl",
            "--format",
            "json",
            "--max-depth",
            "3",
            "--no-color",
        ]) else {
            panic!("expected Tree");
        };
        assert_eq!(args.trace_file, "t.jsonl");
        assert_eq!(args.render.format, "json");
        assert_eq!(args.render.max_depth, Some(3));
        assert!(args.render.no_color);
    }

    #[test]
    fn verify_requires_a_fanout() {
        let missing = TestCli::try_parse_from(["cast", "verify", "t.jsonl"]);
        assert!(missing.is_err(), "--fanout is required");
        let CascadeCommands::Verify(args) = parse(&[
            "cast", "verify", "t.jsonl", "--fanout", "2", "--nodes", "64",
        ]) else {
            panic!("expected Verify");
        };
        assert_eq!(args.fanout, 2);
        assert_eq!(args.nodes, Some(64));
        assert_eq!(args.trace_file, "t.jsonl");
    }

    #[test]
    fn live_preserves_the_viz_flags() {
        let CascadeCommands::Live { live: args, render } = parse(&[
            "cast",
            "live",
            "-n",
            "8",
            "-s",
            "steiner",
            "--fanout",
            "3",
            "--seed",
            "7",
            "--no-watch",
            "--format",
            "jsonl",
        ]) else {
            panic!("expected Live");
        };
        assert_eq!(args.nodes, 8);
        assert_eq!(args.strategy, "steiner");
        assert_eq!(args.fanout, 3);
        assert_eq!(args.seed, 7);
        assert!(args.no_watch);
        assert_eq!(render.format, "jsonl");
    }
}
