//! Grammar discipline tests: structural invariants over the CLI binaries.
//!
//! TDD red-first: `registry_covers_every_bin` fails until
//! `tests/cascade_copy_tests.rs` exists. The remaining tests guard the
//! shared `OutputArgs` vocabulary and molt's stdin-only grammar end-to-end
//! through the real binaries (assert_cmd).

use assert_cmd::Command;

/// Every `[[bin]]` in Cargo.toml maps to an existing declared source file
/// AND has a `tests/<bin>_tests.rs` (the lazaret `registry` rule), derived
/// from the manifest itself — no hand-maintained bin list here.
#[test]
fn registry_covers_every_bin() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let violations = consortium_cli::grammar::registry_violations(&manifest);
    assert!(violations.is_empty(), "registry violations: {violations:?}");
}

/// Shared output vocabulary from `output::OutputArgs` (-v/--verbose,
/// --color, -F/--format). claw/molt/pinch/cast flatten it and cascade-copy
/// repeats the trio inline; cascade-viz is format-only (verified against
/// the live binaries). Fails if any of those bins drops its vocabulary.
#[test]
fn global_flag_vocabulary() {
    for bin in ["claw", "molt", "pinch", "cast", "cascade-copy"] {
        let output = Command::cargo_bin(bin)
            .unwrap_or_else(|e| panic!("bin '{bin}': {e}"))
            .arg("--help")
            .output()
            .unwrap_or_else(|e| panic!("bin '{bin}' --help: {e}"));
        let help = String::from_utf8_lossy(&output.stdout);
        for flag in ["--verbose", "--color", "--format"] {
            assert!(
                help.contains(flag),
                "bin '{bin}' --help missing shared flag '{flag}'\n---\n{help}"
            );
        }
    }
    let output = Command::cargo_bin("cascade-viz")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(
        help.contains("--format"),
        "cascade-viz --help missing --format\n---\n{help}"
    );
}

/// molt aggregates stdin; it must not offer node selection the way claw
/// and pinch do (`-w/--nodes`, `-a/--all`, `-g/--group`, `--hostfile`,
/// `--pick`). Long flags are matched with their short prefix to avoid
/// substring collisions (`--group` would otherwise match molt's
/// `--groupbase`).
#[test]
fn molt_takes_no_node_selection() {
    let output = Command::cargo_bin("molt")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();
    let help = String::from_utf8_lossy(&output.stdout);
    for forbidden in [
        "-w, --nodes",
        "--all",
        "-g, --group ",
        "-x, --exclude",
        "--hostfile",
        "--pick",
    ] {
        assert!(
            !help.contains(forbidden),
            "molt --help must not offer node selection '{forbidden}'\n---\n{help}"
        );
    }
}
