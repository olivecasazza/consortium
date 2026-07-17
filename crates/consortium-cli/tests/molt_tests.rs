//! Integration tests for the `molt` CLI binary.
//!
//! Mirrors the new `test_010_axis` method from upstream
//! `tests/CLIClubakTest.py` (commit 6ed71ac): clubak `--axis` folds
//! gathered output along the selected axis of an nD nodeset.

use std::io::Write;
use std::process::{Command, Stdio};

fn molt() -> Command {
    Command::new(env!("CARGO_BIN_EXE_molt"))
}

/// Run molt with args and stdin, returning (stdout, stderr, code).
fn run_molt(args: &[&str], stdin: &str) -> (String, String, i32) {
    let mut child = molt()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn molt");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
        out.status.code().unwrap(),
    )
}

/// Mirror of `CLIClubakTest._clubak_t` (exact stdout match).
fn check(args: &[&str], stdin: &str, expected_stdout: &str) {
    let (stdout, stderr, rc) = run_molt(args, stdin);
    assert_eq!(rc, 0, "molt failed for args {args:?}: {stderr}");
    assert_eq!(stdout, expected_stdout, "stdout mismatch for args {args:?}");
}

/// Mirror of `CLIClubakTest.test_010_axis`.
#[test]
fn test_010_axis() {
    // 2D node keys with identical content gather into a single block
    // whose header folds along the requested axis.
    let nd = "foo1-1: bar\nfoo1-2: bar\nfoo2-1: bar\nfoo2-2: bar\n";
    // no --axis: fold along all axis (default)
    check(
        &["-b"],
        nd,
        "---------------\nfoo[1-2]-[1-2] (4)\n---------------\n bar\n",
    );
    // --axis=1: fold first dimension only
    check(
        &["--axis=1", "-b"],
        nd,
        "---------------\nfoo[1-2]-1,foo[1-2]-2 (4)\n---------------\n bar\n",
    );
    // --axis=2: fold second dimension only
    check(
        &["--axis=2", "-b"],
        nd,
        "---------------\nfoo1-[1-2],foo2-[1-2] (4)\n---------------\n bar\n",
    );
    // --axis=-1: fold last dimension only
    check(
        &["--axis=-1", "-b"],
        nd,
        "---------------\nfoo1-[1-2],foo2-[1-2] (4)\n---------------\n bar\n",
    );
    // interpret-keys=never: raw key, --axis is a no-op
    check(
        &["--axis=1", "--interpret-keys=never", "-b"],
        "foo1-1: bar\n",
        "---------------\nfoo1-1\n---------------\n bar\n",
    );
}

/// Supplementary: `--axis` with a 1D nodeset still folds normally, and a
/// bad `--axis` rangeset is a parse error (exit 1, like upstream
/// `RangeSetParseError` handling).
#[test]
fn test_010_axis_edge_cases() {
    let one_d = "node1: x\nnode2: x\nnode3: x\nnode4: x\n";
    check(
        &["--axis=1", "-b"],
        one_d,
        "---------------\nnode[1-4] (4)\n---------------\n x\n",
    );

    let (_stdout, stderr, rc) = run_molt(&["--axis=abc", "-b"], "node1: x\n");
    assert_eq!(rc, 1);
    assert!(
        stderr.contains("Parse error"),
        "expected parse error, got {stderr:?}"
    );
}
