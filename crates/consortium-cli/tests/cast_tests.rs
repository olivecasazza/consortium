//! Integration tests for the `cast` CLI binary.

use assert_cmd::Command;
use predicates::prelude::*;
use std::io::Write;

fn cast() -> Command {
    Command::cargo_bin("cast").unwrap()
}

fn write_fleet_config(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("fleet.json");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(
        br#"{
        "nodes": {
            "host1": {
                "name": "host1",
                "targetHost": "127.0.0.1",
                "targetUser": "root",
                "targetPort": null,
                "system": "x86_64-linux",
                "profileType": "nixos",
                "buildOnTarget": false,
                "tags": ["web", "prod"]
            },
            "host2": {
                "name": "host2",
                "targetHost": "127.0.0.2",
                "targetUser": "root",
                "targetPort": null,
                "system": "x86_64-linux",
                "profileType": "nixos",
                "buildOnTarget": false,
                "tags": ["db"]
            }
        },
        "builders": {},
        "flakeUri": "."
    }"#,
    )
    .unwrap();
    path
}

// ─── Config loading ──────────────────────────────────────────────────────────

#[test]
fn test_missing_config_file() {
    cast()
        .args(["--config", "/nonexistent/fleet.json", "eval"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("failed to read config file"))
        .stderr(predicate::str::contains("No such file or directory"));
}

#[test]
fn test_invalid_json_config() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.json");
    std::fs::write(&path, "not json").unwrap();

    cast()
        .args(["--config", path.to_str().unwrap(), "eval"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("failed to parse config file"));
}

// ─── Node selection ──────────────────────────────────────────────────────────

#[test]
fn test_eval_all_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_fleet_config(dir.path());

    cast()
        .args(["--config", config.to_str().unwrap(), "eval"])
        .assert()
        .success()
        .stdout(predicate::str::contains("host1"))
        .stdout(predicate::str::contains("host2"))
        .stdout(predicate::str::contains("Evaluating 2 host(s)"));
}

#[test]
fn test_eval_specific_node() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_fleet_config(dir.path());

    cast()
        .args([
            "--config",
            config.to_str().unwrap(),
            "eval",
            "--on",
            "host1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("host1"))
        .stdout(predicate::str::contains("Evaluating 1 host(s)"));
}

#[test]
fn test_eval_by_tag() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_fleet_config(dir.path());

    cast()
        .args(["--config", config.to_str().unwrap(), "eval", "-g", "web"])
        .assert()
        .success()
        .stdout(predicate::str::contains("host1"))
        .stdout(predicate::str::contains("Evaluating 1 host(s)"));
}

#[test]
fn test_unknown_node() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_fleet_config(dir.path());

    cast()
        .args([
            "--config",
            config.to_str().unwrap(),
            "eval",
            "--on",
            "doesnotexist",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown node 'doesnotexist'"))
        .stderr(predicate::str::contains("host1, host2"));
}

#[test]
fn test_malformed_bracket_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_fleet_config(dir.path());

    cast()
        .args(["--config", config.to_str().unwrap(), "eval", "--on", "bad["])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid node pattern"));
}

// ─── Deploy action validation ────────────────────────────────────────────────

#[test]
fn test_invalid_deploy_action() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_fleet_config(dir.path());

    cast()
        .args([
            "--config",
            config.to_str().unwrap(),
            "deploy",
            "--on",
            "host1",
            "reboot",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid action 'reboot'"))
        .stderr(predicate::str::contains(
            "switch, boot, test, dry-activate, build",
        ));
}

// ─── Health command ──────────────────────────────────────────────────────────

#[test]
fn test_health_no_builders() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_fleet_config(dir.path());

    cast()
        .args(["--config", config.to_str().unwrap(), "health"])
        .assert()
        .success()
        .stdout(predicate::str::contains("No builders configured"));
}

// ─── Eval output format ─────────────────────────────────────────────────────

#[test]
fn test_eval_shows_target_details() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_fleet_config(dir.path());

    cast()
        .args([
            "--config",
            config.to_str().unwrap(),
            "eval",
            "--on",
            "host1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("127.0.0.1"))
        .stdout(predicate::str::contains("x86_64-linux"))
        .stdout(predicate::str::contains("web, prod"));
}
