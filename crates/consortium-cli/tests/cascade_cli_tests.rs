//! Integration tests for the `cast cascade` CLI surface (the former
//! `cascade-viz` binary: `tree` replays traces, `live` runs scenarios,
//! `verify` asserts a relay actually happened).

use assert_cmd::Command;
use predicates::prelude::*;

fn cast_cascade() -> Command {
    let mut cmd = Command::cargo_bin("cast").unwrap();
    cmd.arg("cascade");
    cmd
}

// ─── live tree rendering ──────────────────────────────────────────────────────

#[test]
fn cast_cascade_live_tree_renders() {
    cast_cascade()
        .args(["live", "-n", "16", "-L", "2", "--no-color"])
        .assert()
        .success()
        // host-N labels from CascadeTreeNode::label()
        // event_render uses NodeId's Display ("nN"); richer labels pending
        // an event_render constructor that takes a NodeId → addr map.
        .stdout(predicate::str::contains("n0"))
        // box-drawing connectors from tree::render
        .stdout(predicate::str::contains("├──"))
        // ✔ glyph for converged nodes (NodeStatus::Ok)
        .stdout(predicate::str::contains("✔"));
}

// ─── live json output ─────────────────────────────────────────────────────────

#[test]
fn cast_cascade_live_json_is_valid() {
    let output = cast_cascade()
        .args(["live", "-n", "8", "--format", "json"])
        .output()
        .expect("failed to run cast cascade");

    assert!(output.status.success(), "exit status: {:?}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    // The JSON output is an array of CascadeEvent objects.
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must be valid JSON");
    assert!(parsed.is_array(), "expected JSON array of events");
    let arr = parsed.as_array().unwrap();
    // Must have at least Started and Finished.
    assert!(
        arr.len() >= 2,
        "expected at least 2 events, got {}",
        arr.len()
    );
    assert_eq!(arr[0]["kind"], "started");
    assert_eq!(arr.last().unwrap()["kind"], "finished");
}

// ─── live jsonl output ───────────────────────────────────────────────────────

#[test]
fn cast_cascade_live_jsonl_each_line_is_valid_json() {
    let output = cast_cascade()
        .args(["live", "-n", "8", "--format", "jsonl"])
        .output()
        .expect("failed to run cast cascade");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for (i, line) in stdout.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let _: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("line {i} invalid JSON: {e}\n  line: {line}"));
    }
}

// ─── strategy flag ────────────────────────────────────────────────────────────

#[test]
fn cast_cascade_live_max_bottleneck_strategy() {
    cast_cascade()
        .args(["live", "-n", "8", "-s", "max-bottleneck", "--no-color"])
        .assert()
        .success()
        // event_render uses NodeId's Display ("nN"); richer labels pending
        // an event_render constructor that takes a NodeId → addr map.
        .stdout(predicate::str::contains("n0"));
}

#[test]
fn cast_cascade_live_steiner_strategy() {
    cast_cascade()
        .args(["live", "-n", "8", "-s", "steiner", "--no-color"])
        .assert()
        .success()
        // event_render uses NodeId's Display ("nN"); richer labels pending
        // an event_render constructor that takes a NodeId → addr map.
        .stdout(predicate::str::contains("n0"));
}

// ─── tree subcommand (trace replay) ──────────────────────────────────────────

#[test]
fn cast_cascade_tree_renders_trace() {
    use std::io::Write;

    // Write a minimal JSONL trace.
    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("trace.jsonl");
    let mut f = std::fs::File::create(&trace_path).unwrap();

    // Three events: Started (node 0 seeded) + EdgeCompleted(0→1) + Finished.
    writeln!(
        f,
        r#"{{"kind":"started","n_nodes":2,"seeded":[0],"strategy":"log2-fanout","at":0}}"#
    )
    .unwrap();
    writeln!(
        f,
        r#"{{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":100000000}}"#
    )
    .unwrap();
    writeln!(
        f,
        r#"{{"kind":"finished","converged":2,"failed":0,"rounds":1}}"#
    )
    .unwrap();
    drop(f);

    cast_cascade()
        .args(["tree", trace_path.to_str().unwrap(), "--no-color"])
        .assert()
        .success()
        // event_render uses NodeId's Display ("nN"); richer labels pending
        // an event_render constructor that takes a NodeId → addr map.
        .stdout(predicate::str::contains("n0"))
        .stdout(predicate::str::contains("✔"));
}

#[test]
fn cast_cascade_tree_bad_file_errors() {
    cast_cascade()
        .args(["tree", "/nonexistent/trace.jsonl", "--no-color"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("failed to open trace file"));
}

// ─── all 4 nodes converge ────────────────────────────────────────────────────

#[test]
fn cast_cascade_live_renders_all_requested_nodes() {
    let output = cast_cascade()
        .args(["live", "-n", "4", "--no-color"])
        .output()
        .expect("failed to run cast cascade");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // event_render's SnapshotAccumulator builds the tree rooted at
    // each seeded node; with 4 nodes + single seed, all 4 NodeId
    // displays should appear.
    for n in 0..4 {
        assert!(
            stdout.contains(&format!("n{n}")),
            "expected n{n} in output: {stdout}"
        );
    }
}

// ─── depth limit ─────────────────────────────────────────────────────────────

#[test]
fn cast_cascade_max_depth_zero_shows_only_root() {
    let output = cast_cascade()
        .args(["live", "-n", "16", "-L", "0", "--no-color"])
        .output()
        .expect("failed to run cast cascade");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Seed node (n0) is the root at depth 0.
    assert!(stdout.contains("n0"), "missing seed root: {stdout}");
    // At depth 0 all children are hidden behind a truncation marker.
    assert!(
        stdout.contains("more)"),
        "expected depth-limit truncation marker: {stdout}"
    );
}

// ─── verify subcommand ───────────────────────────────────────────────────────

/// The 15-node level-tree relay: depth 3, 14 relayed nodes, 3 rounds.
fn relay_15_trace(dir: &tempfile::TempDir) -> String {
    let mut lines = vec![
        r#"{"kind":"started","n_nodes":15,"seeded":[0],"strategy":"level-tree","at":0}"#
            .to_string(),
    ];
    let round_of = |src: u32, tgts: &[u32], round: u32, lines: &mut Vec<String>| {
        for tgt in tgts {
            lines.push(format!(
                r#"{{"kind":"edge_completed","round":{round},"src":{src},"tgt":{tgt},"duration":100000000}}"#
            ));
        }
    };
    round_of(0, &[1, 2], 0, &mut lines);
    round_of(1, &[3, 4], 1, &mut lines);
    round_of(2, &[5, 6], 1, &mut lines);
    round_of(3, &[7, 8], 2, &mut lines);
    round_of(4, &[9, 10], 2, &mut lines);
    round_of(5, &[11, 12], 2, &mut lines);
    round_of(6, &[13, 14], 2, &mut lines);
    lines.push(r#"{"kind":"finished","converged":15,"failed":0,"rounds":3}"#.to_string());
    let path = dir.path().join("relay.jsonl");
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();
    path.to_str().unwrap().to_string()
}

#[test]
fn cast_cascade_verify_accepts_a_relayed_trace() {
    let dir = tempfile::tempdir().unwrap();
    let trace = relay_15_trace(&dir);
    let output = cast_cascade()
        .args(["verify", &trace, "--fanout", "2"])
        .output()
        .expect("failed to run cast cascade");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout must be the summary JSON");
    assert_eq!(parsed["nodes"], 15);
    assert_eq!(parsed["rounds"], 3);
    assert_eq!(parsed["relay_depth"], 3);
    assert_eq!(parsed["relayed_nodes"], 14);
}

#[test]
fn cast_cascade_verify_rejects_a_pull_only_trace() {
    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("pull.jsonl");
    std::fs::write(
        &trace_path,
        concat!(
            r#"{"kind":"started","n_nodes":5,"seeded":[0],"strategy":"log2-fanout","at":0}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":1,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":2,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":3,"duration":1}"#,
            "\n",
            r#"{"kind":"edge_completed","round":0,"src":0,"tgt":4,"duration":1}"#,
            "\n",
            r#"{"kind":"finished","converged":5,"failed":0,"rounds":1}"#,
            "\n",
        ),
    )
    .unwrap();

    cast_cascade()
        .args(["verify", trace_path.to_str().unwrap(), "--fanout", "2"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "cascade relay check failed: the payload was not relayed",
        ));
}

#[test]
fn cast_cascade_verify_names_a_malformed_line() {
    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("bad.jsonl");
    std::fs::write(&trace_path, "not json\n").unwrap();

    cast_cascade()
        .args(["verify", trace_path.to_str().unwrap(), "--fanout", "2"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "cascade relay check failed: line 1 is not JSON:",
        ));
}

#[test]
fn cast_cascade_verify_requires_a_started_event() {
    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("nostart.jsonl");
    std::fs::write(
        &trace_path,
        concat!(
            r#"{"kind":"finished","converged":2,"failed":0,"rounds":1}"#,
            "\n",
        ),
    )
    .unwrap();

    cast_cascade()
        .args(["verify", trace_path.to_str().unwrap(), "--fanout", "2"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "cascade relay check failed: event stream has no started event",
        ));
}

#[test]
fn cast_cascade_verify_fanout_is_required() {
    cast_cascade()
        .args(["verify", "/nonexistent/trace.jsonl"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--fanout"));
}

// ─── strategies subcommand (discovery) ──────────────────────────────────────

/// `cast cascade strategies` is generated from the same registry the
/// parser and the error messages read — this test reads the registry
/// itself, so the command cannot drift from it.
#[test]
fn cast_cascade_strategies_lists_the_registry() {
    use consortium_nix::cascade_strategies::STRATEGIES;

    let output = cast_cascade()
        .args(["strategies"])
        .output()
        .expect("failed to run cast cascade strategies");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    for spec in STRATEGIES {
        assert!(
            stdout.contains(spec.canonical),
            "strategies output must list '{}': {stdout}",
            spec.canonical
        );
        for alias in spec.aliases {
            assert!(
                stdout.contains(alias),
                "strategies output must list alias '{alias}': {stdout}"
            );
        }
    }
}

// ─── swarm end to end (negative control) ────────────────────────────────────

/// A real swarm run produces a green trace that verify MUST reject by
/// the star rule — this is the relay assertion's negative control,
/// exercised through the actual binaries.
#[test]
fn cast_cascade_verify_rejects_a_real_swarm_run() {
    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("swarm.jsonl");
    let output = cast_cascade()
        .args([
            "live",
            "-n",
            "15",
            "-s",
            "swarm",
            "--format",
            "jsonl",
            "--no-watch",
        ])
        .output()
        .expect("swarm live run must execute");
    assert!(output.status.success(), "swarm live run must succeed");
    std::fs::write(&trace_path, &output.stdout).unwrap();

    cast_cascade()
        .args(["verify", trace_path.to_str().unwrap(), "--fanout", "2"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "cascade relay check failed: the payload was not relayed",
        ));
}

/// The positive control over the same binaries: a real log2 run is
/// accepted, and the success JSON is exactly the four documented keys.
#[test]
fn cast_cascade_verify_accepts_a_real_log2_run() {
    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("log2.jsonl");
    let output = cast_cascade()
        .args([
            "live",
            "-n",
            "15",
            "-s",
            "log2",
            "--format",
            "jsonl",
            "--no-watch",
        ])
        .output()
        .expect("log2 live run must execute");
    assert!(output.status.success(), "log2 live run must succeed");
    std::fs::write(&trace_path, &output.stdout).unwrap();

    let verified = cast_cascade()
        .args(["verify", trace_path.to_str().unwrap(), "--fanout", "2"])
        .output()
        .expect("verify must execute");
    assert!(
        verified.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&verified.stderr)
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&verified.stdout).expect("stdout must be the summary JSON");
    let mut keys: Vec<String> = parsed
        .as_object()
        .expect("summary must be a JSON object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        vec!["nodes", "relay_depth", "relayed_nodes", "rounds"],
        "the success JSON is a frozen consumer contract"
    );
    assert_eq!(parsed["nodes"], 15);
}

/// The live path resolves through the registry: an unknown strategy is
/// an error naming the available set, never a silent log2 fallback.
#[test]
fn cast_cascade_live_unknown_strategy_errors() {
    cast_cascade()
        .args(["live", "-n", "4", "-s", "warp-drive", "--no-watch"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown strategy 'warp-drive'"))
        .stderr(predicate::str::contains("level-tree"));
}
