//! Integration tests for the `pinch` CLI binary.
//!
//! Mirrors the three new `--index` test methods from upstream
//! `tests/CLINodesetTest.py` (commit 8755d96): `test_026_index`,
//! `test_027_index_rangeset` and `test_028_index_nd`. Expected outputs and
//! exit codes are copied from the Python tests; stderr is compared with an
//! `ends_with` check like `TLib.CLI_main` (messages may be prefixed).

use std::io::Write;
use std::process::{Command, Stdio};

fn pinch() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pinch"))
}

/// Run pinch with args and optional stdin, returning (stdout, stderr, code).
fn run_pinch(args: &[&str], stdin: Option<&str>) -> (String, String, i32) {
    let mut cmd = pinch();
    cmd.args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        });
    let mut child = cmd.spawn().expect("failed to spawn pinch");
    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    }
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
        out.status.code().unwrap(),
    )
}

/// Mirror of `CLINodesetTestBase._nodeset_t`.
fn check(args: &[&str], stdin: Option<&str>, expected_stdout: Option<&str>, expected_rc: i32, expected_stderr: Option<&str>) {
    let (stdout, stderr, rc) = run_pinch(args, stdin);
    if let Some(expected) = expected_stdout {
        assert_eq!(stdout, expected, "stdout mismatch for args {args:?}");
    }
    assert_eq!(rc, expected_rc, "exit code mismatch for args {args:?}");
    if let Some(expected) = expected_stderr {
        assert!(
            stderr.ends_with(expected),
            "stderr {stderr:?} does not end with {expected:?} for args {args:?}"
        );
    }
}

/// Mirror of `CLINodesetTest.test_026_index`.
#[test]
fn test_026_index() {
    check(&["--index", "node0", "node[0-9]"], None, Some("0\n"), 0, None);
    check(&["--index", "node5", "node[0-9]"], None, Some("5\n"), 0, None);
    check(&["--index", "node9", "node[0-9]"], None, Some("9\n"), 0, None);
    // --index is the reverse of -I/--slice
    check(&["--index", "bar34", "bar[34-68,89-90]"], None, Some("0\n"), 0, None);
    check(&["--index", "bar42", "bar[34-68,89-90]"], None, Some("8\n"), 0, None);
    check(&["--index", "bar89", "bar[34-68,89-90]"], None, Some("35\n"), 0, None);
    check(&["--index", "bar90", "bar[34-68,89-90]"], None, Some("36\n"), 0, None);
    // several patterns: sorted alphabetically by name, not input order
    check(&["--index", "bmc10", "node[1-4],bmc[10-20]"], None, Some("0\n"), 0, None);
    check(&["--index", "node1", "node[1-4],bmc[10-20]"], None, Some("11\n"), 0, None);
    // nD nodeset
    check(&["--index", "da34p1", "da[30,34-51]p[1-2]"], None, Some("2\n"), 0, None);
    // zero-padding is significant
    check(&["--index", "prune003", "prune[003-034]"], None, Some("0\n"), 0, None);
    check(&["--index", "prune034", "prune[003-034]"], None, Some("31\n"), 0, None);
    check(&["--index", "prune3", "prune[003-034]"], None, Some(""), 1, Some("ERROR: 'prune3' is not in nodeset\n"));
    // read the set from stdin
    check(&["--index", "node5"], Some("node[0-9]\n"), Some("5\n"), 0, None);
    // honor -O output format
    check(&["--index", "node5", "-O", "idx=%s", "node[0-9]"], None, Some("idx=5\n"), 0, None);
    // set operations are applied before the lookup
    check(&["--index", "node5", "node[0-9]", "-x", "node0"], None, Some("4\n"), 0, None);
    // -I/--slice is a transform like any other: --index sees the sliced set
    check(&["--index", "node3", "-I", "0-4", "node[0-9]"], None, Some("3\n"), 0, None);
    check(&["--index", "node7", "-I", "0-4", "node[0-9]"], None, Some(""), 1, Some("ERROR: 'node7' is not in nodeset\n"));
    // missing node: ValueError -> exit 1 (like list.index())
    check(&["--index", "node42", "node[0-9]"], None, Some(""), 1, Some("ERROR: 'node42' is not in nodeset\n"));
    // a single node is required
    check(&["--index", "node[1-2]", "node[0-9]"], None, Some(""), 1, Some("ERROR: index() argument must be a single node\n"));
    // --index is a command: incompatible with other commands and --pick
    check(&["--index", "node5", "-c", "node[0-9]"], None, None, 2, Some("Multiple commands not allowed.\n"));
    check(&["--index", "node5", "--pick", "2", "node[0-9]"], None, None, 2, Some("--index cannot be combined with --pick\n"));
}

/// Mirror of `CLINodesetTest.test_027_index_rangeset`.
#[test]
fn test_027_index_rangeset() {
    check(&["-R", "--index", "5", "0-9"], None, Some("5\n"), 0, None);
    check(&["-R", "--index", "200", "0-100,200"], None, Some("101\n"), 0, None);
    check(&["-R", "--index", "18", "1,5,18-31"], None, Some("2\n"), 0, None);
    // zero-padding is significant
    check(&["-R", "--index", "09", "08-10"], None, Some("1\n"), 0, None);
    // read the set from stdin
    check(&["-R", "--index", "5"], Some("0-9\n"), Some("5\n"), 0, None);
    // missing element: ValueError -> exit 1
    check(&["-R", "--index", "50", "0-9"], None, Some(""), 1, Some("ERROR: 50 is not in RangeSet\n"));
}

/// Mirror of `CLINodesetTest.test_028_index_nd`.
#[test]
fn test_028_index_nd() {
    // nD flattening is a cartesian product where the LAST dimension
    // varies fastest; --index round-trips with -e/expand for every node.
    // 2-D: da[1-2]p[1-3] expands da1p1 da1p2 da1p3 da2p1 da2p2 da2p3
    let nd2 = "da[1-2]p[1-3]";
    for (idx, node) in ["da1p1", "da1p2", "da1p3", "da2p1", "da2p2", "da2p3"]
        .iter()
        .enumerate()
    {
        check(&["--index", node, nd2], None, Some(&format!("{idx}\n")), 0, None);
    }
    // 3-D: a[1-2]b[1-2]c[1-2] expands a1b1c1 a1b1c2 a1b2c1 a1b2c2
    //      a2b1c1 a2b1c2 a2b2c1 a2b2c2 (c least significant, a most)
    let nd3 = "a[1-2]b[1-2]c[1-2]";
    for (idx, node) in [
        "a1b1c1", "a1b1c2", "a1b2c1", "a1b2c2", "a2b1c1", "a2b1c2", "a2b2c1", "a2b2c2",
    ]
    .iter()
    .enumerate()
    {
        check(&["--index", node, nd3], None, Some(&format!("{idx}\n")), 0, None);
    }
    // rightmost axis varies fastest: p cycles fully before da increments
    check(&["--index", "da1p1", nd2], None, Some("0\n"), 0, None);
    check(&["--index", "da1p3", nd2], None, Some("2\n"), 0, None);
    check(&["--index", "da2p1", nd2], None, Some("3\n"), 0, None);
    check(&["--index", "da2p3", nd2], None, Some("5\n"), 0, None);
    // last axis is least significant (a1b1c1=0 .. a2b2c2=7)
    check(&["--index", "a1b1c2", nd3], None, Some("1\n"), 0, None);
    check(&["--index", "a1b2c1", nd3], None, Some("2\n"), 0, None);
    check(&["--index", "a2b1c1", nd3], None, Some("4\n"), 0, None);
    check(&["--index", "a2b2c2", nd3], None, Some("7\n"), 0, None);
    // several vectors in one pattern: larger vector emitted first.
    // da[1-2]p[1-3] (6 elts) precedes da[5-6]p[1-2] (4 elts)
    let nd2v = "da[1-2]p[1-3],da[5-6]p[1-2]";
    for (idx, node) in [
        "da1p1", "da1p2", "da1p3", "da2p1", "da2p2", "da2p3", "da5p1", "da5p2", "da6p1", "da6p2",
    ]
    .iter()
    .enumerate()
    {
        check(&["--index", node, nd2v], None, Some(&format!("{idx}\n")), 0, None);
    }
    check(&["--index", "da5p1", nd2v], None, Some("6\n"), 0, None);
    check(&["--index", "da6p2", nd2v], None, Some("9\n"), 0, None);
}
