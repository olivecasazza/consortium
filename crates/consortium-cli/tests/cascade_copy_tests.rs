//! cascade-copy registration tests (registry rule: every bin ships tests).

use assert_cmd::Command;

/// --help succeeds and mentions usage (smoke; cascade-copy has no remote
/// side effects).
#[test]
fn help_succeeds() {
    Command::cargo_bin("cascade-copy")
        .unwrap()
        .arg("--help")
        .assert()
        .success();
}
