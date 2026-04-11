//! pinch — node set operations (replaces cluset/nodeset).
//!
//! Usage:
//!   pinch -e node[1-5]          # expand: node1 node2 node3 node4 node5
//!   pinch -c node[1-5]          # count: 5
//!   pinch -f node1 node2 node3  # fold: node[1-3]
//!   pinch -i node[1-5] node[3-7]  # intersection: node[3-5]
//!   pinch -x node[3-4] node[1-5]  # difference: node[1-2,5]

use std::io::{self, BufRead, Write};
use std::process;

use clap::Parser;

use consortium::node_set::NodeSet;
use consortium::range_set::RangeSet;

/// pinch — node set operations for consortium.
///
/// Manipulate cluster node sets: fold, expand, count, and perform
/// set algebra (union, intersection, difference, symmetric difference).
#[derive(Parser)]
#[command(name = "pinch", version, about)]
struct Args {
    // ── Operations (mutually exclusive) ────────────────────────────────
    /// Count nodes in the resulting set.
    #[arg(short = 'c', long = "count", group = "operation")]
    count: bool,

    /// Expand node set into individual node names.
    #[arg(short = 'e', long = "expand", group = "operation")]
    expand: bool,

    /// Fold node names into compact bracket notation (default).
    #[arg(short = 'f', long = "fold", group = "operation")]
    fold: bool,

    /// List node groups. Repeat for more detail (-l, -ll, -lll).
    #[arg(short = 'l', long = "list", action = clap::ArgAction::Count, group = "operation")]
    list: u8,

    /// Regroup nodes using group definitions.
    #[arg(short = 'r', long = "regroup", group = "operation")]
    regroup: bool,

    /// List available group sources.
    #[arg(long = "groupsources", group = "operation")]
    groupsources: bool,

    // ── Set modifiers ──────────────────────────────────────────────────
    /// Exclude nodes (set difference).
    #[arg(short = 'x', long = "exclude")]
    exclude: Vec<String>,

    /// Intersect with node set.
    #[arg(short = 'i', long = "intersection")]
    intersection: Vec<String>,

    /// Symmetric difference (XOR) with node set.
    #[arg(short = 'X', long = "xor")]
    xor: Vec<String>,

    // ── Node sources ───────────────────────────────────────────────────
    /// Use all nodes from default group.
    #[arg(short = 'a', long = "all")]
    all: bool,

    /// Use nodes from named group.
    #[arg(short = 'g', long = "group")]
    group: Vec<String>,

    // ── Display options ────────────────────────────────────────────────
    /// Output separator (default: space for expand, newline for list).
    #[arg(short = 'S', long = "separator")]
    separator: Option<String>,

    /// Operate on RangeSet instead of NodeSet.
    #[arg(short = 'R', long = "rangeset")]
    rangeset: bool,

    /// Enable autostep folding (e.g., 2 → a-b/2).
    #[arg(long = "autostep")]
    autostep: Option<String>,

    /// Split result into N subsets.
    #[arg(long = "split")]
    split: Option<usize>,

    /// Split into contiguous subsets.
    #[arg(long = "contiguous")]
    contiguous: bool,

    /// Pick N random nodes from result.
    #[arg(long = "pick")]
    pick: Option<usize>,

    // ── Positional ─────────────────────────────────────────────────────
    /// Input node sets or node names. Reads from stdin if none given.
    nodesets: Vec<String>,
}

fn main() {
    let args = Args::parse();
    if let Err(e) = run(args) {
        eprintln!("pinch: {e}");
        process::exit(1);
    }
}

fn run(args: Args) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut out = stdout.lock();

    if args.rangeset {
        return run_rangeset(&args, &mut out);
    }

    // ── Build the base node set ────────────────────────────────────────
    let mut ns = gather_input(&args)?;

    // ── Apply set modifiers ────────────────────────────────────────────
    for pat in &args.exclude {
        let other = NodeSet::parse(pat)?;
        ns.difference_update(&other);
    }
    for pat in &args.intersection {
        let other = NodeSet::parse(pat)?;
        ns.intersection_update(&other);
    }
    for pat in &args.xor {
        let other = NodeSet::parse(pat)?;
        ns.symmetric_difference_update(&other);
    }

    // ── Pick random subset ─────────────────────────────────────────────
    if let Some(n) = args.pick {
        ns = pick_random(&ns, n);
    }

    // ── Output ─────────────────────────────────────────────────────────
    let sep = args.separator.as_deref().unwrap_or(" ");

    if args.count {
        writeln!(out, "{}", ns.len())?;
    } else if args.expand {
        let nodes: Vec<String> = ns.iter().collect();
        writeln!(out, "{}", nodes.join(sep))?;
    } else if let Some(n) = args.split {
        let subsets = ns.split(n);
        for subset in subsets {
            writeln!(out, "{subset}")?;
        }
    } else if args.contiguous {
        // Split into contiguous ranges — each pattern separately
        let subsets = ns.split(ns.len().max(1));
        for subset in subsets {
            if !subset.is_empty() {
                writeln!(out, "{subset}")?;
            }
        }
    } else {
        // Default: fold
        writeln!(out, "{ns}")?;
    }

    Ok(())
}

/// Collect input nodesets from args + stdin.
fn gather_input(args: &Args) -> anyhow::Result<NodeSet> {
    let mut ns = NodeSet::new();

    // From positional args
    for pat in &args.nodesets {
        let parsed = NodeSet::parse(pat)?;
        ns.update(&parsed);
    }

    // From stdin if no positional args and not a tty
    if args.nodesets.is_empty() && !is_terminal::is_terminal(io::stdin()) {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line?;
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                let parsed = NodeSet::parse(trimmed)?;
                ns.update(&parsed);
            }
        }
    }

    Ok(ns)
}

/// RangeSet mode — operates on numeric ranges, not node names.
fn run_rangeset(args: &Args, out: &mut impl Write) -> anyhow::Result<()> {
    let autostep = parse_autostep(&args.autostep);

    let mut rs = RangeSet::new();

    // From positional args
    for pat in &args.nodesets {
        let parsed = RangeSet::parse(pat, autostep)?;
        rs.update(&parsed);
    }

    // From stdin
    if args.nodesets.is_empty() && !is_terminal::is_terminal(io::stdin()) {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line?;
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                let parsed = RangeSet::parse(trimmed, autostep)?;
                rs.update(&parsed);
            }
        }
    }

    // Apply set modifiers
    for pat in &args.exclude {
        let other = RangeSet::parse(pat, autostep)?;
        rs.difference_update(&other);
    }
    for pat in &args.intersection {
        let other = RangeSet::parse(pat, autostep)?;
        rs.intersection_update(&other);
    }
    for pat in &args.xor {
        let other = RangeSet::parse(pat, autostep)?;
        rs.symmetric_difference_update(&other);
    }

    let sep = args.separator.as_deref().unwrap_or(" ");

    if args.count {
        writeln!(out, "{}", rs.len())?;
    } else if args.expand {
        let items: Vec<String> = rs.striter().collect();
        writeln!(out, "{}", items.join(sep))?;
    } else {
        // fold
        writeln!(out, "{rs}")?;
    }

    Ok(())
}

/// Parse autostep argument: a number, "auto", or "%N".
fn parse_autostep(s: &Option<String>) -> Option<u32> {
    let s = s.as_deref()?;
    match s {
        "auto" => Some(0), // 0 means auto-detect
        _ => s.parse::<u32>().ok(),
    }
}

/// Pick N random nodes from a NodeSet.
fn pick_random(ns: &NodeSet, n: usize) -> NodeSet {
    use std::collections::HashSet;

    let all: Vec<String> = ns.iter().collect();
    if n >= all.len() {
        return ns.clone();
    }

    // Simple Fisher-Yates-ish random pick using system entropy
    let mut indices: Vec<usize> = (0..all.len()).collect();
    // Use a simple LCG seeded from process id + time for shuffling
    let mut seed: u64 = std::process::id() as u64
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

    for i in (1..indices.len()).rev() {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (seed >> 33) as usize % (i + 1);
        indices.swap(i, j);
    }

    let picked: HashSet<&str> = indices[..n].iter().map(|&i| all[i].as_str()).collect();
    let mut result = NodeSet::new();
    for node in picked {
        let _ = result.update_str(node);
    }
    result
}
