//! Grammar-module tests: introspected dump, pinned legacy flag sets, and
//! shrink-only baseline semantics.
//!
//! These import `consortium_cli::grammar`. Until that module exists this
//! file fails to compile — that compile failure is the TDD red state.

use std::collections::BTreeSet;

use consortium_cli::grammar;

/// claw/molt/pinch short flags are frozen upstream ClusterShell surface
/// (clush/clubak/cluset parity is pinned by the `consortium-tests` oracle).
/// Shared `OutputArgs` shorts (-v, -F) are consortium additions and are
/// excluded; if a refactor renames or drops an upstream short, this fails.
#[test]
fn legacy_flag_sets_pinned() {
    let pinned: &[(&str, &str)] = &[
        ("claw", "wxagXfltuRobLNqS"),
        ("molt", "bLSTG"),
        ("pinch", "ceflrxiXagSRIO"),
    ];
    for (bin, frozen) in pinned {
        let got: BTreeSet<char> = grammar::dump()
            .into_iter()
            .filter(|r| r.bin == *bin && !["verbose", "color", "format"].contains(&r.arg.as_str()))
            .filter_map(|r| r.short)
            .collect();
        let want: BTreeSet<char> = frozen.chars().collect();
        assert_eq!(
            got, want,
            "bin '{bin}' short flags drifted from the frozen upstream set"
        );
    }
}

/// dump() covers every classified bin (no unclassified binary) and exposes
/// the shared flag vocabulary plus the declared selectors.
#[test]
fn dump_is_total() {
    let rows = grammar::dump();
    let bins: BTreeSet<String> = rows.iter().map(|r| r.bin.clone()).collect();
    let want: BTreeSet<String> = [
        "claw",
        "molt",
        "pinch",
        "cast",
        "cascade_viz",
        "cascade_copy",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(bins, want, "dump must cover every classified bin");

    for bin in ["claw", "molt", "pinch", "cast"] {
        for flag in ["verbose", "color", "format"] {
            assert!(
                rows.iter().any(|r| r.bin == bin && r.arg == flag),
                "dump missing {bin} shared flag '{flag}'"
            );
        }
    }
    assert!(
        rows.iter().any(|r| r.bin == "claw" && r.arg == "nodes"),
        "dump missing claw selector 'nodes'"
    );
    assert!(
        !rows.iter().any(|r| r.bin == "molt" && r.arg == "nodes"),
        "molt must not expose a 'nodes' selector"
    );
    assert!(
        rows.iter()
            .filter(|r| r.kind != "registry")
            .all(|r| !r.help.is_empty()),
        "typed rule: every introspected argument carries help text"
    );
}

/// Baseline semantics: a stale baseline entry errors, a new live violation
/// errors, and an exactly-matching baseline passes.
#[test]
fn baseline_stale_entry_errors() {
    let live = vec!["registry cascade_copy ".to_string()];
    let stale = vec![
        "compat molt short-flags".to_string(),
        "registry cascade_copy ".to_string(),
    ];
    assert!(
        grammar::check_keys(&live, &stale).is_err(),
        "stale baseline entry must error"
    );
    let matching = vec!["registry cascade_copy ".to_string()];
    assert!(
        grammar::check_keys(&live, &matching).is_ok(),
        "exact baseline must pass"
    );
    let empty: Vec<String> = vec![];
    assert!(
        grammar::check_keys(&live, &empty).is_err(),
        "new live violation must error"
    );
}

/// write_baseline followed by check on the current tree: the live set is
/// empty once `tests/cascade_copy_tests.rs` exists, so an empty baseline
/// round-trips clean.
#[test]
fn write_and_check_roundtrip() {
    let dir = std::env::temp_dir().join(format!("consortium-grammar-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("baseline.json");
    grammar::write_baseline(&path, &[]).unwrap();
    let report = grammar::check(&path).unwrap();
    assert!(report.ok);
    assert_eq!(report.violations, 0, "current tree must be violation-free");
    std::fs::remove_file(&path).ok();
}
