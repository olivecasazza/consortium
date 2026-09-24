//! End-to-end tests for the `consortium-grammar` binary: default check
//! against the embedded baseline, dump contents, and fail-closed
//! baseline handling.

use assert_cmd::Command;

fn grammar_cmd() -> Command {
    Command::cargo_bin("consortium-grammar").unwrap()
}

/// Default check must pass against the embedded committed baseline with
/// zero live violations.
#[test]
fn default_check_is_clean() {
    let out = grammar_cmd().arg("check").output().unwrap();
    assert!(
        out.status.success(),
        "default check must pass: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(report["ok"], serde_json::Value::Bool(true));
    assert_eq!(report["violations"], serde_json::json!(0));
}

/// Dump is the pretty Row array: it must include a cast subcommand row
/// and a registry placeholder row.
#[test]
fn dump_includes_cast_subcommand_and_registry_placeholder() {
    let out = grammar_cmd().arg("dump").output().unwrap();
    assert!(out.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let rows = rows.as_array().unwrap();
    assert!(
        rows.iter()
            .any(|r| r["bin"] == "cast" && r["subcommand"] == "eval" && r["arg"] == "on"),
        "dump must include cast eval --on: {} rows",
        rows.len()
    );
    assert!(
        rows.iter()
            .any(|r| r["bin"] == "cascade_viz" && r["kind"] == "registry"),
        "dump must include registry placeholders"
    );
}

/// A stale baseline (key that no longer reproduces) must fail the check.
#[test]
fn stale_baseline_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stale.json");
    std::fs::write(&path, r#"["typed cast build on"]"#).unwrap();
    let out = grammar_cmd()
        .arg("check")
        .arg("--baseline")
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "stale baseline entry must fail the check"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("grammar check failed"),
        "failure must explain the drift: {stderr}"
    );
}

/// An explicit --baseline pointing at a missing file must fail closed.
#[test]
fn missing_baseline_override_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.json");
    let out = grammar_cmd()
        .arg("check")
        .arg("--baseline")
        .arg(&missing)
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "missing explicit baseline must fail closed"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("cannot read baseline") && stderr.contains("missing.json"),
        "failure must name the missing baseline: {stderr}"
    );
}
