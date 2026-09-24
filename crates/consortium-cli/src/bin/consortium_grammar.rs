//! Strict grammar CLI: introspected command-surface dump and strict
//! baseline check over `rule bin subcommand arg` keys.
//!
//! `check` compares the live violation keys against either an explicit
//! `--baseline` file (which must exist) or the baseline embedded at
//! build time from `src/grammar/baseline.json`, so an installed binary
//! works outside the source tree. New, stale, or duplicate keys exit
//! nonzero; the baseline can only shrink.
use std::path::PathBuf;

use clap::Parser;
use consortium_cli::grammar;

/// Baseline embedded at build time from the committed
/// `src/grammar/baseline.json`.
const EMBEDDED_BASELINE: &str = include_str!("../grammar/baseline.json");

#[derive(Parser)]
#[command(
    name = "consortium-grammar",
    about = "CLI grammar dump and strict baseline check"
)]
enum Cli {
    /// Pretty-print the introspected `Row` array.
    Dump,
    /// Compare live violations with the baseline; exits nonzero on new,
    /// stale, or duplicate keys.
    Check {
        /// Explicit baseline file (must exist); defaults to the embedded
        /// committed baseline.
        #[arg(long)]
        baseline: Option<PathBuf>,
    },
}

fn main() -> anyhow::Result<()> {
    match Cli::parse() {
        Cli::Dump => {
            println!("{}", serde_json::to_string_pretty(&grammar::dump())?);
            Ok(())
        }
        Cli::Check { baseline } => {
            let report = match baseline {
                Some(path) => grammar::check(&path)?,
                None => {
                    let base: Vec<String> = serde_json::from_str(EMBEDDED_BASELINE)?;
                    let live = grammar::violations();
                    let keys: Vec<String> = live.iter().map(grammar::key_of).collect();
                    let count = grammar::check_keys(&keys, &base)?;
                    grammar::CheckReport {
                        ok: true,
                        baseline: base.len(),
                        violations: count,
                    }
                }
            };
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
    }
}
