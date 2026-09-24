//! Grammar discipline tests: structural invariants over the CLI binaries.
//!
//! TDD red-first: `registry_covers_every_bin` fails until
//! `tests/cascade_copy_tests.rs` exists. The remaining tests guard the
//! shared `OutputArgs` vocabulary and molt's stdin-only grammar end-to-end
//! through the real binaries (assert_cmd).

use assert_cmd::Command;

const ALL_BINS: &[&str] = &[
    "claw",
    "molt",
    "pinch",
    "cast",
    "cascade-viz",
    "cascade-copy",
];

/// Every `[[bin]]` in Cargo.toml maps to an existing src file AND has a
/// `tests/<bin>_tests.rs` (the lazaret `registry` rule).
#[test]
fn registry_covers_every_bin() {
    let root = env!("CARGO_MANIFEST_DIR");
    for bin in ALL_BINS {
        let src = if bin.starts_with("cascade-") {
            format!("{root}/src/bin/{}.rs", bin.replace('-', "_"))
        } else {
            format!("{root}/src/{bin}.rs")
        };
        assert!(
            std::path::Path::new(&src).exists(),
            "bin '{bin}': missing source file {src}"
        );
        let test_file = format!("{root}/tests/{}_tests.rs", bin.replace('-', "_"));
        assert!(
            std::path::Path::new(&test_file).exists(),
            "bin '{bin}': missing test file {test_file}"
        );
    }
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
