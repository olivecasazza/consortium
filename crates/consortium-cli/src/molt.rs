//! molt — aggregate cluster output (replaces clubak).
//!
//! Reads "node:output" lines from stdin or files and aggregates them.
//!
//! Usage:
//!   some_command | molt -b        # gather nodes with identical output
//!   some_command | molt -L        # line mode, ordered by node
//!   molt -b < output.txt         # from file

use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::process;

use clap::{Parser, ValueEnum};

use consortium::node_set::NodeSet;
use consortium_cli::display;
use consortium_cli::fold;
use consortium_cli::output::{CliOutput, OutputArgs};

/// Whether input keys are interpreted as nodesets (upstream clubak
/// `--interpret-keys`; THREE_CHOICES).
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
enum InterpretKeys {
    /// Keys are used verbatim, never parsed as nodesets.
    Never,
    /// Keys must parse as nodesets; a parse error is fatal.
    Always,
    /// Keys are parsed as nodesets until one fails, then never again.
    #[default]
    Auto,
}

/// molt — aggregate and display cluster command output.
///
/// Reads "key: output" lines from stdin (like dsh/pdsh output) and
/// groups nodes that produced identical output together.
#[derive(Parser)]
#[command(name = "molt", version, about)]
struct Args {
    /// Gather nodes with identical output (dshbak mode).
    #[arg(short = 'b', long = "dshbak")]
    dshbak: bool,

    /// Line mode — disable header, order by node name.
    #[arg(short = 'L', long = "line")]
    line: bool,

    /// Key separator (default: ':').
    #[arg(short = 'S', long = "separator", default_value = ":")]
    separator: String,

    /// Message tree trace mode.
    #[arg(short = 'T', long = "tree")]
    tree: bool,

    /// No group source prefix in regroup output.
    #[arg(short = 'G', long = "groupbase")]
    groupbase: bool,

    /// Fold along these axis only (axis 1..n for nD nodeset).
    #[arg(long = "axis", value_name = "RANGESET")]
    axis: Option<String>,

    /// Whether to interpret keys (never, always or auto).
    #[arg(long = "interpret-keys", value_name = "WHEN", default_value = "auto")]
    interpret_keys: InterpretKeys,

    /// Input files (reads stdin if none given).
    files: Vec<String>,

    #[command(flatten)]
    output: OutputArgs,
}

fn main() {
    let args = Args::parse();
    let _out = CliOutput::from_args(&args.output);
    if let Err(e) = run(args) {
        eprintln!("molt: {e}");
        process::exit(1);
    }
}

fn run(args: Args) -> anyhow::Result<()> {
    // User-specified nD-nodeset fold axis for output display (#356).
    // An empty axis list behaves like Python's empty DEFAULTS.fold_axis
    // tuple (falsy → fold along all axes).
    let fold_axes = match &args.axis {
        Some(axis) => {
            Some(fold::parse_fold_axis(axis).map_err(|e| anyhow::anyhow!("Parse error: {e}"))?)
        }
        None => None,
    };
    let fold_axis: Option<&[i64]> = fold_axes.as_deref().filter(|v| !v.is_empty());

    let stdout = io::stdout();
    let mut out = stdout.lock();

    // Read all input lines
    let lines = read_input(&args)?;

    if args.dshbak {
        run_dshbak(&lines, &args, fold_axis, &mut out)?;
    } else if args.line {
        run_line_mode(&lines, &args, &mut out)?;
    } else {
        // Default: pass through with node labels
        run_passthrough(&lines, &mut out)?;
    }

    Ok(())
}

/// Read input from files or stdin.
fn read_input(args: &Args) -> anyhow::Result<Vec<String>> {
    let mut lines = Vec::new();

    if args.files.is_empty() {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            lines.push(line?);
        }
    } else {
        for path in &args.files {
            let file = std::fs::File::open(path)?;
            let reader = io::BufReader::new(file);
            for line in reader.lines() {
                lines.push(line?);
            }
        }
    }

    Ok(lines)
}

/// Parse a line into (key, message) using the separator.
///
/// Like upstream clubak, the key is stripped but the content is kept
/// verbatim (`line.split(separator, 1)`), so a leading space after the
/// separator is preserved in gathered output.
fn parse_line<'a>(line: &'a str, separator: &str) -> Option<(&'a str, &'a str)> {
    let idx = line.find(separator)?;
    let key = line[..idx].trim();
    let msg = &line[idx + separator.len()..];
    if key.is_empty() {
        return None;
    }
    Some((key, msg))
}

/// Expand an input key into node names according to `--interpret-keys`.
///
/// Ports clubak's key handling: in `always`/`auto` mode the key is parsed
/// as a NodeSet (nodeset keys expand to several nodes); in `auto` mode a
/// parse failure permanently switches interpretation off (`enable_nodeset_key
/// = False` upstream), here modeled by flipping the mode in place.
fn expand_key(key: &str, mode: &mut InterpretKeys) -> anyhow::Result<Vec<String>> {
    if *mode == InterpretKeys::Never {
        return Ok(vec![key.to_string()]);
    }
    match NodeSet::parse(key) {
        Ok(ns) => {
            let nodes: Vec<String> = ns.iter().collect();
            if nodes.is_empty() {
                Ok(vec![key.to_string()])
            } else {
                Ok(nodes)
            }
        }
        Err(e) => {
            if *mode == InterpretKeys::Always {
                return Err(anyhow::anyhow!("Parse error: {e}"));
            }
            // auto => switch off
            *mode = InterpretKeys::Never;
            Ok(vec![key.to_string()])
        }
    }
}

/// dshbak mode: group nodes with identical output.
///
/// One header block per distinct full output buffer, like upstream
/// `print_gather(nodeset, tree[node])`. Groups sort like upstream
/// (`nodeset_cmpkey`): larger groups first, then by first node name.
/// Headers fold with the nD-aware folder honoring the `--axis` constraint
/// (#356) and carry the ` (N)` node count like upstream
/// `Display.format_header()`.
fn run_dshbak(
    lines: &[String],
    args: &Args,
    fold_axis: Option<&[i64]>,
    out: &mut impl Write,
) -> anyhow::Result<()> {
    let mut interpret = args.interpret_keys;

    // Aggregate per-node message buffers (joined with '\n' like MsgTree).
    let mut node_bufs: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
    for line in lines {
        if let Some((key, msg)) = parse_line(line, &args.separator) {
            for node in expand_key(key, &mut interpret)? {
                node_bufs
                    .entry(node)
                    .or_default()
                    .push(msg.as_bytes().to_vec());
            }
        }
    }

    // Group nodes by identical full output buffer.
    let mut groups: HashMap<Vec<u8>, Vec<String>> = HashMap::new();
    for (node, msgs) in node_bufs {
        groups.entry(msgs.join(&b"\n"[..])).or_default().push(node);
    }

    let mut groups: Vec<(Vec<u8>, Vec<String>)> = groups.into_iter().collect();
    for (_buf, names) in groups.iter_mut() {
        names.sort();
    }
    // larger nodeset first, then sorted by first node index
    groups.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.1[0].cmp(&b.1[0])));

    for (message, names) in &groups {
        let folded = if interpret == InterpretKeys::Never {
            // raw keys: no nodeset folding, --axis is a no-op
            names.join(",")
        } else {
            fold::fold_nodes(names, fold_axis)
        };
        display::print_gathered_header(&folded, names.len(), out)?;
        out.write_all(message)?;
        writeln!(out)?;
    }

    Ok(())
}

/// Line mode: output ordered by node name.
fn run_line_mode(lines: &[String], args: &Args, out: &mut impl Write) -> anyhow::Result<()> {
    // Collect per-node output, then sort by node name
    let mut node_output: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();

    let mut interpret = args.interpret_keys;
    for line in lines {
        if let Some((key, msg)) = parse_line(line, &args.separator) {
            for node in expand_key(key, &mut interpret)? {
                node_output.entry(node).or_default().push(msg.to_string());
            }
        }
    }

    for (node, msgs) in &node_output {
        for msg in msgs {
            display::print_line_with_label(node, msg, out)?;
        }
    }

    Ok(())
}

/// Passthrough: just print lines as-is.
fn run_passthrough(lines: &[String], out: &mut impl Write) -> anyhow::Result<()> {
    for line in lines {
        writeln!(out, "{line}")?;
    }
    Ok(())
}
