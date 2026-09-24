//! Grammar-module tests: introspected dump, pinned legacy flag sets, and
//! shrink-only baseline semantics.

use std::collections::BTreeSet;

use consortium_cli::grammar;

/// The manifest is the source of truth: a `[[bin]]` absent from KINDS and
/// shipping no `tests/<name>_tests.rs` must be flagged by the registry rule.
#[test]
fn registry_flags_unclassified_bin_without_tests() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("src/bin")).unwrap();
    std::fs::write(dir.path().join("src/bin/zeta.rs"), "fn main() {}").unwrap();
    std::fs::write(
        dir.path().join("Cargo.toml"),
        "[[bin]]\nname = \"zeta\"\npath = \"src/bin/zeta.rs\"\n",
    )
    .unwrap();
    let violations = grammar::registry_violations(&dir.path().join("Cargo.toml"));
    let keys: BTreeSet<String> = violations.iter().map(grammar::key_of).collect();
    assert!(
        keys.contains("registry zeta (root) kind"),
        "unclassified bin must be flagged: {keys:?}"
    );
    assert!(
        keys.contains("registry zeta (root) tests"),
        "missing tests file must be flagged: {keys:?}"
    );
    assert!(
        !keys.iter().any(|k| k.contains(" zeta (root) source")),
        "declared source exists, no source violation expected: {keys:?}"
    );
}

/// An entry with a declared source path that does not exist must not pass
/// merely because its per-bin test file is present.
#[test]
fn registry_flags_missing_declared_source() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("tests")).unwrap();
    std::fs::write(dir.path().join("tests/zeta_tests.rs"), "").unwrap();
    std::fs::write(
        dir.path().join("Cargo.toml"),
        "[[bin]]\nname = \"zeta\"\npath = \"src/bin/missing.rs\"\n",
    )
    .unwrap();
    let keys: BTreeSet<String> = grammar::registry_violations(&dir.path().join("Cargo.toml"))
        .iter()
        .map(grammar::key_of)
        .collect();
    assert!(
        keys.contains("registry zeta (root) source"),
        "missing source: {keys:?}"
    );
    assert!(
        !keys.contains("registry zeta (root) tests"),
        "existing test file must not be misclassified: {keys:?}"
    );
}

/// Reverse direction: a KINDS bin with no matching `[[bin]]` in the
/// manifest must be flagged (the manifest cannot silently drop a bin).
#[test]
fn registry_flags_kinds_bin_missing_from_manifest() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("Cargo.toml"), "").unwrap();
    let violations = grammar::registry_violations(&dir.path().join("Cargo.toml"));
    let keys: BTreeSet<String> = violations.iter().map(grammar::key_of).collect();
    for bin in [
        "claw",
        "molt",
        "pinch",
        "cast",
        "cascade_viz",
        "cascade_copy",
        "consortium_grammar",
    ] {
        assert!(
            keys.contains(&format!("registry {bin} (root) manifest")),
            "KINDS bin '{bin}' missing from manifest must be flagged: {keys:?}"
        );
    }
}

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
        "consortium_grammar",
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

/// A stale baseline entry and a new live violation both fail the comparison.
#[test]
fn baseline_stale_entry_errors() {
    let live = vec!["registry cascade_copy (root) tests".to_string()];
    let stale = vec![
        "compat molt (root) short-flags".to_string(),
        "registry cascade_copy (root) tests".to_string(),
    ];
    assert!(
        grammar::check_keys(&live, &stale).is_err(),
        "stale baseline entry must error"
    );
    let matching = vec!["registry cascade_copy (root) tests".to_string()];
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

#[test]
fn repeated_cast_arg_ids_have_distinct_subcommand_keys() {
    let mut missing_help = grammar::Violation {
        rule: "typed".to_string(),
        bin: "cast".to_string(),
        subcommand: "build".to_string(),
        arg: "on".to_string(),
        detail: "argument has no help text".to_string(),
    };
    let build = grammar::key_of(&missing_help);
    missing_help.subcommand = "deploy".to_string();
    let deploy = grammar::key_of(&missing_help);
    assert_eq!(build, "typed cast build on");
    assert_eq!(deploy, "typed cast deploy on");
    assert_ne!(build, deploy);
    assert!(grammar::check_keys(&[build.clone(), deploy.clone()], &[build.clone()]).is_err());
    assert!(grammar::check_keys(&[build.clone()], &[build.clone(), deploy.clone()]).is_err());
    assert_eq!(
        grammar::check_keys(&[build.clone(), deploy.clone()], &[build, deploy]).unwrap(),
        2
    );
}

#[test]
fn duplicate_keys_cannot_hide_distinct_violations() {
    let key = "typed cast build on".to_string();
    assert!(grammar::check_keys(&[key.clone(), key.clone()], &[key.clone()]).is_err());
    assert!(grammar::check_keys(&[key.clone()], &[key.clone(), key]).is_err());
}
