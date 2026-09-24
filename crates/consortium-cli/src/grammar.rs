//! CLI grammar: one introspected source of truth for the command surface.
//!
//! Ports the lazaret `nu/grammar.nu` idea (INTERFACE / dump / violations /
//! check) onto clap: every signature fact comes from each bin's
//! `Args::command()` via `clap::CommandFactory` — never a hand-maintained
//! list. `check` fails on any new violation and on any baseline entry that
//! no longer reproduces, so the baseline can only shrink.
//!
//! Rules enforced by [`violations`]:
//!
//! - `global-flags` — introspected bins expose the shared `OutputArgs`
//!   vocabulary (`verbose`, `color`, `format`) at the root.
//! - `selector` — selector-taking commands expose exactly the selector
//!   arguments declared in [`SELECTORS`]; `molt` (stdin aggregator) offers
//!   none.
//! - `registry` — every bin maps to an existing source file and ships a
//!   `tests/<bin>_tests.rs`.
//! - `compat` — claw/molt/pinch short flags equal the frozen upstream
//!   ClusterShell sets (parity is pinned by the `consortium-tests` oracle).
//! - `typed` — every introspected argument carries help text.

use std::collections::BTreeSet;
use std::path::Path;

use clap::{Arg, Command, CommandFactory};
use serde::{Deserialize, Serialize};

/// Module kinds. `legacy`: upstream ClusterShell parity surface (frozen);
/// `orchestration`: new surface, free to evolve. The two cascade bins are
/// registry-checked but not introspected (their `Args` stay private in
/// `src/bin/`).
pub const KINDS: &[(&str, &[&str])] = &[
    ("legacy", &["claw", "molt", "pinch"]),
    ("orchestration", &["cast", "cascade_viz", "cascade_copy"]),
];

/// Shared output vocabulary every introspected bin must expose at its root
/// (from `output::OutputArgs`).
pub const SHARED_FLAGS: &[&str] = &["verbose", "color", "format"];

/// Each selector-taking command declares its selector arguments here;
/// [`dump`] exposes this metadata and [`violations`] checks it.
pub const SELECTORS: &[(&str, &[&str])] = &[
    (
        "claw",
        &[
            "nodes",
            "exclude",
            "group",
            "exclude_group",
            "pick",
            "hostfile",
        ],
    ),
    (
        "pinch",
        &["count", "expand", "fold", "list", "regroup", "groupsources"],
    ),
    ("cast", &["on", "tag"]),
];

/// Frozen upstream short flags (clush/clubak/cluset compat). The shared
/// `OutputArgs` shorts (`-v`, `-F`) are consortium additions and are
/// excluded from this comparison; long-only upstream flags are unaffected.
pub const PINNED_SHORT_FLAGS: &[(&str, &str)] = &[
    ("claw", "wxagXfltuRobLNqS"),
    ("molt", "bLSTG"),
    ("pinch", "ceflrxiXagSRIO"),
];

/// Bin → source-file map for the registry rule (relative to the crate root).
const BIN_SOURCES: &[(&str, &str)] = &[
    ("claw", "claw.rs"),
    ("molt", "molt.rs"),
    ("pinch", "pinch.rs"),
    ("cast", "cast.rs"),
    ("cascade_viz", "bin/cascade_viz.rs"),
    ("cascade_copy", "bin/cascade_copy.rs"),
];

/// One introspected argument (or a `registry` placeholder row for the
/// non-introspected cascade bins).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub bin: String,
    pub subcommand: String,
    pub arg: String,
    /// `positional` | `flag` | `switch` | `registry`
    pub kind: String,
    pub short: Option<char>,
    pub r#type: String,
    pub default: String,
    pub help: String,
}

/// One rule violation, addressable by `rule bin arg`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    pub rule: String,
    pub bin: String,
    pub arg: String,
    pub detail: String,
}

/// Stable join key for baseline comparison.
pub fn key_of(v: &Violation) -> String {
    format!("{} {} {}", v.rule, v.bin, v.arg)
}

fn arg_kind(a: &Arg) -> &'static str {
    if a.is_positional() {
        "positional"
    } else if a.get_num_args().map(|n| n.takes_values()).unwrap_or(false) {
        "flag"
    } else {
        "switch"
    }
}

fn walk(cmd: &Command, bin: &str, subcommand: &str, rows: &mut Vec<Row>) {
    for a in cmd.get_arguments() {
        let r#type = a
            .get_value_names()
            .and_then(|n| n.first())
            .map(|n| n.to_string())
            .unwrap_or_default();
        let default = a
            .get_default_values()
            .iter()
            .map(|v| v.to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join(" ");
        rows.push(Row {
            bin: bin.to_string(),
            subcommand: subcommand.to_string(),
            arg: a.get_id().to_string(),
            kind: arg_kind(a).to_string(),
            short: a.get_short(),
            r#type,
            default,
            help: a.get_help().map(|h| h.to_string()).unwrap_or_default(),
        });
    }
    for sc in cmd.get_subcommands() {
        walk(sc, bin, sc.get_name(), rows);
    }
}

/// One row per exported argument across all bins, straight from clap —
/// the same data a human reads off `--help`, as a table.
pub fn dump() -> Vec<Row> {
    let mut rows = Vec::new();
    for (bin, cmd) in [
        ("claw", crate::claw::Args::command()),
        ("molt", crate::molt::Args::command()),
        ("pinch", crate::pinch::Args::command()),
        ("cast", crate::cast::Args::command()),
    ] {
        walk(&cmd, bin, "", &mut rows);
    }
    for bin in ["cascade_viz", "cascade_copy"] {
        rows.push(Row {
            bin: (*bin).to_string(),
            subcommand: String::new(),
            arg: String::new(),
            kind: "registry".to_string(),
            short: None,
            r#type: String::new(),
            default: String::new(),
            help: "registered bin (not introspected)".to_string(),
        });
    }
    rows
}

/// Interface rules evaluated against the live grammar.
pub fn violations() -> Vec<Violation> {
    let rows = dump();
    let mut v = Vec::new();

    for bin in ["claw", "molt", "pinch", "cast"] {
        for flag in SHARED_FLAGS {
            if !rows
                .iter()
                .any(|r| r.bin == bin && r.subcommand.is_empty() && r.arg == *flag)
            {
                v.push(Violation {
                    rule: "global-flags".to_string(),
                    bin: bin.to_string(),
                    arg: (*flag).to_string(),
                    detail: "shared OutputArgs flag missing at the root".to_string(),
                });
            }
        }
    }

    for (bin, names) in SELECTORS {
        for n in *names {
            if !rows.iter().any(|r| r.bin == *bin && r.arg == *n) {
                v.push(Violation {
                    rule: "selector".to_string(),
                    bin: (*bin).to_string(),
                    arg: (*n).to_string(),
                    detail: "declared selector missing from the live grammar".to_string(),
                });
            }
        }
    }

    // molt is a stdin aggregator: no node-selection surface, ever.
    for bad in ["nodes", "all", "group", "exclude", "hostfile", "pick"] {
        if rows.iter().any(|r| r.bin == "molt" && r.arg == bad) {
            v.push(Violation {
                rule: "selector".to_string(),
                bin: "molt".to_string(),
                arg: bad.to_string(),
                detail: "aggregator reads stdin; node selection not allowed".to_string(),
            });
        }
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (bin, file) in BIN_SOURCES {
        if !root.join("src").join(file).exists() {
            v.push(Violation {
                rule: "registry".to_string(),
                bin: (*bin).to_string(),
                arg: String::new(),
                detail: format!("missing src/{file}"),
            });
        }
        let tests = format!("{bin}_tests.rs");
        if !root.join("tests").join(&tests).exists() {
            v.push(Violation {
                rule: "registry".to_string(),
                bin: (*bin).to_string(),
                arg: String::new(),
                detail: format!("missing tests/{tests}"),
            });
        }
    }

    for (bin, pinned) in PINNED_SHORT_FLAGS {
        let got: BTreeSet<char> = rows
            .iter()
            .filter(|r| r.bin == *bin && !SHARED_FLAGS.contains(&r.arg.as_str()))
            .filter_map(|r| r.short)
            .collect();
        let want: BTreeSet<char> = pinned.chars().collect();
        if got != want {
            v.push(Violation {
                rule: "compat".to_string(),
                bin: (*bin).to_string(),
                arg: "short-flags".to_string(),
                detail: format!("frozen upstream set drift: got {got:?} want {want:?}"),
            });
        }
    }

    for r in &rows {
        if r.kind != "registry" && r.help.is_empty() && r.arg != "help" && r.arg != "version" {
            v.push(Violation {
                rule: "typed".to_string(),
                bin: r.bin.clone(),
                arg: r.arg.clone(),
                detail: "argument has no help text".to_string(),
            });
        }
    }

    v
}

/// Pure new-vs-stale comparison over baseline keys: errors when the live
/// set has keys the baseline lacks (new violations) or the baseline has
/// keys that no longer reproduce (stale entries — the baseline can only
/// shrink). Returns the live violation count on success.
pub fn check_keys(live: &[String], baseline: &[String]) -> anyhow::Result<usize> {
    let new: Vec<&String> = live.iter().filter(|k| !baseline.contains(k)).collect();
    let stale: Vec<&String> = baseline.iter().filter(|k| !live.contains(k)).collect();
    if !new.is_empty() || !stale.is_empty() {
        return Err(anyhow::anyhow!(
            "grammar check failed: {new_count} new, {stale_count} stale\nnew: {new:?}\nstale: {stale:?}",
            new_count = new.len(),
            stale_count = stale.len()
        ));
    }
    Ok(live.len())
}

/// Rewrite the baseline from the live set (shrink-only discipline is the
/// caller's responsibility, same as lazaret's `--write-baseline`).
pub fn write_baseline(path: &Path, live: &[Violation]) -> anyhow::Result<()> {
    let keys: Vec<String> = live.iter().map(key_of).collect();
    std::fs::write(path, serde_json::to_string_pretty(&keys)?)?;
    Ok(())
}

/// Result of comparing the live grammar against a baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckReport {
    pub ok: bool,
    pub baseline: usize,
    pub violations: usize,
}

/// Compare live violations with the baseline file (JSON array of
/// `rule bin arg` keys). Errors on new or stale entries.
pub fn check(baseline: &Path) -> anyhow::Result<CheckReport> {
    let live = violations();
    let keys: Vec<String> = live.iter().map(key_of).collect();
    let base: Vec<String> = if baseline.exists() {
        serde_json::from_str(&std::fs::read_to_string(baseline)?)?
    } else {
        Vec::new()
    };
    let count = check_keys(&keys, &base)?;
    Ok(CheckReport {
        ok: true,
        baseline: base.len(),
        violations: count,
    })
}
