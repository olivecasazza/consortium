//! Integration tests for the `cascade-viz` binary.

use assert_cmd::Command;
use predicates::prelude::*;

fn cascade_viz() -> Command {
    Command::cargo_bin("cascade-viz").unwrap()
}

// ─── live tree rendering ──────────────────────────────────────────────────────

#[test]
fn cascade_viz_live_tree_renders() {
    cascade_viz()
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
fn cascade_viz_live_json_is_valid() {
    let output = cascade_viz()
        .args(["--format", "json", "live", "-n", "8"])
        .output()
        .expect("failed to run cascade-viz");

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
fn cascade_viz_live_jsonl_each_line_is_valid_json() {
    let output = cascade_viz()
        .args(["--format", "jsonl", "live", "-n", "8"])
        .output()
        .expect("failed to run cascade-viz");

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
fn cascade_viz_live_max_bottleneck_strategy() {
    cascade_viz()
        .args(["live", "-n", "8", "-s", "max-bottleneck", "--no-color"])
        .assert()
        .success()
        // event_render uses NodeId's Display ("nN"); richer labels pending
        // an event_render constructor that takes a NodeId → addr map.
        .stdout(predicate::str::contains("n0"));
}

#[test]
fn cascade_viz_live_steiner_strategy() {
    cascade_viz()
        .args(["live", "-n", "8", "-s", "steiner", "--no-color"])
        .assert()
        .success()
        // event_render uses NodeId's Display ("nN"); richer labels pending
        // an event_render constructor that takes a NodeId → addr map.
        .stdout(predicate::str::contains("n0"));
}

// ─── replay subcommand ───────────────────────────────────────────────────────

#[test]
fn cascade_viz_replay_renders_tree() {
    use std::io::Write;

    // Write a minimal JSONL trace.
    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("trace.jsonl");
    let mut f = std::fs::File::create(&trace_path).unwrap();

    // Two events: Started (node 0 seeded) + EdgeCompleted(0→1) + Finished.
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

    cascade_viz()
        .args(["replay", trace_path.to_str().unwrap(), "--no-color"])
        .assert()
        .success()
        // event_render uses NodeId's Display ("nN"); richer labels pending
        // an event_render constructor that takes a NodeId → addr map.
        .stdout(predicate::str::contains("n0"))
        .stdout(predicate::str::contains("✔"));
}

#[test]
fn cascade_viz_replay_bad_file_errors() {
    cascade_viz()
        .args(["replay", "/nonexistent/trace.jsonl", "--no-color"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("failed to open trace file"));
}

// ─── all 4 nodes converge ────────────────────────────────────────────────────

#[test]
fn cascade_viz_live_renders_all_requested_nodes() {
    let output = cascade_viz()
        .args(["live", "-n", "4", "--no-color"])
        .output()
        .expect("failed to run cascade-viz");

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
fn cascade_viz_max_depth_zero_shows_only_root() {
    let output = cascade_viz()
        .args(["live", "-n", "16", "-L", "0", "--no-color"])
        .output()
        .expect("failed to run cascade-viz");

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
