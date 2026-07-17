//! Integration tests for the `claw` CLI binary.
//!
//! Mirrors the two new methods from upstream `tests/CLIClushTest.py`:
//! - `test_045_pipe_line_buffering` (commit 65e5433, GH#597): stdout lines
//!   must reach a piped consumer as they are produced, not in one block at
//!   command exit.
//! - `test_046_axis` (commit 6ed71ac): clush `--axis` folds gathered output
//!   along the selected axis of an nD nodeset.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn claw() -> Command {
    Command::new(env!("CARGO_BIN_EXE_claw"))
}

/// Run claw with args and empty stdin, returning (stdout, stderr, code).
fn run_claw(args: &[&str]) -> (String, String, i32) {
    let out = claw()
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn claw");
    (
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
        out.status.code().unwrap(),
    )
}

/// Mirror of `CLIClushTest.test_045_pipe_line_buffering` (GH#597).
///
/// The remote command emits 3 lines with a sleep between each. With the
/// fix, lines arrive ~0.5s apart through the pipe; block-buffered (or
/// end-of-run batched) output would deliver all 3 together at exit.
///
/// Deviation from the Python mirror: upstream passes `-q`, but Rust claw's
/// `-q` suppresses command output entirely, so the timing assertion runs in
/// the default (labeled) mode; the buffering semantics under test are
/// identical.
#[test]
fn test_045_pipe_line_buffering() {
    let mut child = claw()
        .args([
            "-w",
            "localhost",
            "--worker=exec",
            "for i in A B C; do echo $i; sleep 0.5; done",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn claw");

    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);
    let t0 = Instant::now();
    let mut timestamps = Vec::new();
    let mut lines = Vec::new();
    for line in reader.lines() {
        lines.push(line.unwrap());
        timestamps.push(t0.elapsed());
    }
    let status = child.wait().unwrap();
    assert!(status.success());

    assert_eq!(
        timestamps.len(),
        3,
        "expected 3 lines, got {}: {lines:?}",
        timestamps.len()
    );
    let spread = timestamps[2] - timestamps[0];
    // Bug signature: spread ~0 (lines released together at claw exit).
    // Fix signature: spread ~1s. 0.5s threshold = midpoint, equally far
    // from both signals — generous for CI jitter.
    assert!(
        spread > Duration::from_millis(500),
        "lines arrived together (spread={spread:.3?}); claw stdout looks block-buffered"
    );
}

/// Mirror of `CLIClushTest.test_046_axis`.
#[test]
fn test_046_axis() {
    // 2D nodeset gathered locally via exec worker; all hosts emit the
    // same output, so the folded header reflects the requested axis.
    let base = ["-R", "exec", "-w", "foo[1-2]-[1-2]", "-b", "echo test"];

    let check = |args: &[&str], expected: &str| {
        let (stdout, stderr, rc) = run_claw(args);
        assert_eq!(rc, 0, "claw failed for args {args:?}: {stderr}");
        assert_eq!(stdout, expected, "stdout mismatch for args {args:?}");
    };

    // no --axis: fold along all axis (default)
    check(
        &base,
        "---------------\nfoo[1-2]-[1-2] (4)\n---------------\ntest\n",
    );
    // --axis=1: fold first dimension only
    check(
        &[&["--axis=1"], &base[..]].concat(),
        "---------------\nfoo[1-2]-1,foo[1-2]-2 (4)\n---------------\ntest\n",
    );
    // --axis=2: fold second dimension only
    check(
        &[&["--axis=2"], &base[..]].concat(),
        "---------------\nfoo1-[1-2],foo2-[1-2] (4)\n---------------\ntest\n",
    );
    // --axis=-1: fold last dimension only
    check(
        &[&["--axis=-1"], &base[..]].concat(),
        "---------------\nfoo1-[1-2],foo2-[1-2] (4)\n---------------\ntest\n",
    );
    // 1D nodeset still folds normally with --axis=1
    check(
        &["--axis=1", "-R", "exec", "-w", "node[1-4]", "-b", "echo test"],
        "---------------\nnode[1-4] (4)\n---------------\ntest\n",
    );
}
