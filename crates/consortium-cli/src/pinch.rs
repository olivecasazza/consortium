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
use consortium_cli::output::{CliOutput, OutputArgs};

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

    /// Pick node(s) at the given index position(s) (reverse of --index).
    #[arg(short = 'I', long = "slice", value_name = "RANGESET")]
    slice: Option<String>,

    /// Output the index of NODE in the nodeset.
    #[arg(long = "index", value_name = "NODE")]
    index: Option<String>,

    /// Output format string (%-style, applied to --index result).
    #[arg(short = 'O', long = "output-format", value_name = "FORMAT")]
    output_format: Option<String>,

    // ── Positional ─────────────────────────────────────────────────────
    /// Input node sets or node names. Reads from stdin if none given.
    nodesets: Vec<String>,

    #[command(flatten)]
    output: OutputArgs,
}

fn main() {
    let args = Args::parse();
    let _out = CliOutput::from_args(&args.output);
    if let Err(e) = run(args) {
        eprintln!("pinch: {e}");
        process::exit(1);
    }
}

fn run(args: Args) -> anyhow::Result<()> {
    // ── Command validation (mirrors CLI/Nodeset.py parser.error paths) ──
    // --index counts as a command; combined with another command upstream
    // reports "Multiple commands not allowed." and exits 2.
    let cmdcount = args.count as u32
        + args.expand as u32
        + args.fold as u32
        + u32::from(args.list > 0)
        + args.regroup as u32
        + args.groupsources as u32
        + u32::from(args.index.is_some());
    if cmdcount > 1 {
        eprintln!("Multiple commands not allowed.");
        process::exit(2);
    }
    if args.index.is_some() && args.pick.is_some() {
        eprintln!("--index cannot be combined with --pick");
        process::exit(2);
    }

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

    // ── Slice transform (-I), applied before --pick and --index ────────
    if let Some(ref slice_pat) = args.slice {
        ns = slice_nodeset(&ns, slice_pat)?;
    }

    // ── Pick random subset ─────────────────────────────────────────────
    if let Some(n) = args.pick {
        ns = pick_random(&ns, n);
    }

    let fmt = args.output_format.as_deref().unwrap_or("%s");

    // ── --index: position of a node in the set (reverse of -I/--slice) ──
    if let Some(ref node_arg) = args.index {
        // a single node is required (like list.index())
        match NodeSet::parse(node_arg) {
            Ok(parsed) if parsed.len() == 1 => {}
            Ok(_) => {
                eprintln!("ERROR: index() argument must be a single node");
                process::exit(1);
            }
            Err(e) => return Err(e.into()),
        }
        return match ns.index(node_arg) {
            Some(i) => {
                writeln!(out, "{}", apply_format(fmt, &i.to_string()))?;
                Ok(())
            }
            None => {
                eprintln!("ERROR: '{node_arg}' is not in nodeset");
                process::exit(1);
            }
        };
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

    // Slice transform (-I), applied before --index like upstream
    if let Some(ref slice_pat) = args.slice {
        rs = slice_rangeset(&rs, slice_pat)?;
    }

    let fmt = args.output_format.as_deref().unwrap_or("%s");

    // --index: position of an element in the rangeset (padding significant)
    if let Some(ref elem_arg) = args.index {
        return match rs.index_str(elem_arg) {
            Some(i) => {
                writeln!(out, "{}", apply_format(fmt, &i.to_string()))?;
                Ok(())
            }
            None => {
                eprintln!("ERROR: {elem_arg} is not in RangeSet");
                process::exit(1);
            }
        };
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

/// Apply the `-I/--slice` transform: keep the elements at the positions
/// selected by the slice rangeset (upstream `xset[sli]` for each slice of
/// `RangeSet(options.slice_rangeset).slices()`; out-of-range positions are
/// clamped away like Python slicing).
fn slice_nodeset(ns: &NodeSet, slice_pat: &str) -> anyhow::Result<NodeSet> {
    let positions = RangeSet::parse(slice_pat, None)?;
    let elems: Vec<String> = ns.iter().collect();
    let mut sliced = NodeSet::new();
    for pos in positions.intiter() {
        if pos >= 0 && (pos as usize) < elems.len() {
            sliced.update_str(&elems[pos as usize])?;
        }
    }
    Ok(sliced)
}

/// RangeSet mode equivalent of [`slice_nodeset`].
fn slice_rangeset(rs: &RangeSet, slice_pat: &str) -> anyhow::Result<RangeSet> {
    let positions = RangeSet::parse(slice_pat, None)?;
    let elems = rs.sorted();
    let mut sliced = RangeSet::new();
    for pos in positions.intiter() {
        if pos >= 0 && (pos as usize) < elems.len() {
            sliced.add_str(&elems[pos as usize]);
        }
    }
    Ok(sliced)
}

/// Apply a Python `%`-style output format string to a single value
/// (`%s`/`%d`/`%i` substitute the value, `%%` is a literal percent).
fn apply_format(fmt: &str, value: &str) -> String {
    let mut out = String::new();
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            match chars.next() {
                Some('s') | Some('d') | Some('i') => out.push_str(value),
                Some('%') => out.push('%'),
                Some(other) => {
                    out.push('%');
                    out.push(other);
                }
                None => out.push('%'),
            }
        } else {
            out.push(c);
        }
    }
    out
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
