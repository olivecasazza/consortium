//! Shared output formatting for consortium CLI tools.

use std::io::{self, Write};

/// Print gathered output with a node header.
///
/// ```text
/// ---------------
/// node[1-3]
/// ---------------
/// hello world
/// ```
pub fn print_gathered_header(nodes: &str, out: &mut impl Write) -> io::Result<()> {
    let sep = "-".repeat(15);
    writeln!(out, "{sep}")?;
    writeln!(out, "{nodes}")?;
    writeln!(out, "{sep}")?;
    Ok(())
}

/// Print a single line with node prefix: `node1: output`
pub fn print_line_with_label(node: &str, line: &str, out: &mut impl Write) -> io::Result<()> {
    writeln!(out, "{node}: {line}")
}
