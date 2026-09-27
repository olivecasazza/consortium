//! Behavioural tests for the sandbox policy core.
//!
//! These run with no hypervisor and no /dev/kvm: the point of `SandboxSpec`
//! being data is that the interesting logic is testable everywhere.

use std::time::Duration;

use consortium_cli::sandbox::{
    DirectSandbox, NetworkPolicy, Sandbox, SandboxCommand, SandboxError, SandboxSpec, SpecError,
};

#[test]
fn deny_all_grants_nothing() {
    let spec = SandboxSpec::deny_all();
    assert_eq!(spec.network, NetworkPolicy::Deny);
    assert!(spec.writable_paths.is_empty());
    assert!(spec.read_only_paths.is_empty());
    assert!(spec.timeout.is_none());
    assert_eq!(spec.validate(), Ok(()));
}

#[test]
fn conflicting_path_is_rejected() {
    let spec = SandboxSpec::deny_all()
        .with_read_only("/srv/data")
        .with_writable("/srv/data");
    let err = spec.validate().expect_err("must reject an ambiguous path");
    assert_eq!(
        err,
        SpecError::ConflictingPath {
            path: "/srv/data".into()
        }
    );
    // The message names the path, so an operator can fix the policy.
    assert!(err.to_string().contains("/srv/data"));
}

#[test]
fn distinct_paths_validate() {
    let spec = SandboxSpec::deny_all()
        .with_read_only("/usr")
        .with_writable("/tmp/scratch");
    assert_eq!(spec.validate(), Ok(()));
}

#[test]
fn direct_backend_reports_it_is_not_isolated() {
    // A degraded run must be visible, not assumed.
    assert!(!DirectSandbox.is_isolated());
    assert_eq!(DirectSandbox.name(), "direct");
}

#[test]
fn direct_backend_refuses_a_policy_it_cannot_enforce() {
    // DirectSandbox cannot deny network, so it must say so rather than run anyway.
    let spec = SandboxSpec::deny_all();
    let command = SandboxCommand::new("/bin/true");
    let err = DirectSandbox
        .exec(&spec, &command)
        .expect_err("must refuse a network-denying policy");
    assert!(matches!(err, SandboxError::Unsupported(_)));
    assert!(err.to_string().contains("network") || err.to_string().contains("policy"));
}

#[test]
fn direct_backend_runs_when_the_policy_is_satisfiable() {
    let spec = SandboxSpec::deny_all().with_network(NetworkPolicy::Allow);
    let command = SandboxCommand::new("/bin/sh").arg("-c").arg("exit 7");
    let out = DirectSandbox
        .exec(&spec, &command)
        .expect("shell should run");
    assert_eq!(out.status, 7);
    assert!(!out.success());
}

#[test]
fn direct_backend_captures_stdout() {
    let spec = SandboxSpec::deny_all().with_network(NetworkPolicy::Allow);
    let command = SandboxCommand::new("/bin/sh").arg("-c").arg("printf hello");
    let out = DirectSandbox.exec(&spec, &command).expect("should run");
    assert!(out.success());
    assert_eq!(out.stdout, b"hello");
}

#[test]
fn spec_serializes_to_json_for_review() {
    // A policy is meant to be reviewable in a PR, so it must round-trip.
    let spec = SandboxSpec::deny_all()
        .with_read_only("/usr")
        .with_writable("/tmp")
        .with_timeout(Duration::from_secs(30));
    let json = serde_json::to_string(&spec).expect("serialize");
    let back: SandboxSpec = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(spec, back);
}

#[test]
fn conflicting_path_survives_a_json_round_trip_then_fails_validation() {
    // Validation must not be bypassable by loading a policy from disk.
    let spec = SandboxSpec::deny_all()
        .with_read_only("/srv/data")
        .with_writable("/srv/data");
    let json = serde_json::to_string(&spec).expect("serialize");
    let back: SandboxSpec = serde_json::from_str(&json).expect("deserialize");
    assert!(back.validate().is_err());
}
