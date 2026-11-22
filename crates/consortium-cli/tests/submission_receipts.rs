//! Black-box acknowledgement-loss tests. The scheduler fixture records admission before
//! returning to the transport fixture; neither its ledger nor the transport's reply is a live cluster.
#![cfg(unix)]

use serde_json::Value;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

const JOB_NAME: &str = "receipt-control";
// Ray's Jobs SDK reports the dashboard's request-validation failure as a nested traceback.
const RAY_REJECTION: &str = r#"Traceback (most recent call last):
  File "/venv/ray/dashboard/modules/job/sdk.py", line 250, in submit_job
    self._raise_error(r)
  File "/venv/ray/dashboard/modules/dashboard_sdk.py", line 277, in _raise_error
    raise RuntimeError(
RuntimeError: Request failed with status code 400: Traceback (most recent call last):
  File "/venv/ray/dashboard/modules/job/utils.py", line 135, in parse_and_validate_request
    return validate_request_type(json_data, request_type)
  File "/venv/ray/dashboard/modules/job/common.py", line 389, in validate_request_type
    return request_type(**json_data)
TypeError: JobSubmitRequest.__init__() got an unexpected keyword argument 'unknown_field'
."#;
const SLURM_REJECTION: &str =
    "sbatch: error: Batch job submission failed: Invalid partition name specified";

const TOOLS: &str = r#"set -eu
tool=${0##*/}
case "$tool" in
    nix) case "$1" in build) printf '/nix/store/fixture-env\n';; copy) :;; *) exit 64;; esac; exit 0;;
    sbatch|jobs-api)
        if [ "$REPLY_MODE" = rejected ]; then printf '%s\n' "$REJECTION" >&2; exit 1; fi
        case "$tool" in
            sbatch) id=$$; ack="Submitted batch job $id";;
            jobs-api) id=raysubmit_$$; ack="Job '$id' submitted successfully";;
        esac
        printf '%s\n' "$id" >> jobs
        printf '%s\n' "$ack"; exit 0;;
    sacct) printf 'COMPLETED\n'; exit 0;;
    cat) printf 'fixture output\n'; exit 0;;
esac
# Record all client commands, separately from admitted jobs (one fresh process/ID per admission).
printf '%s %s\n' "$tool" "$*" >> calls
case "$tool" in
    ssh) for remote do :; done
        set -- "$POSIX_SH" -c "$remote"
        case "$remote" in *sbatch*) :;; *) exec "$@";; esac;;
    ray) case "$1 $2" in
        'job submit') set -- jobs-api "$@";;
        'job status') printf 'Status: SUCCEEDED\n'; exit 0;;
        *) exit 64;; esac;;
    *) exit 64;;
esac
case "$REPLY_MODE" in intact|rejected) exec "$@";; esac
# The real CLI never sees this file. Losing the response cannot undo the preceding admission.
"$@" > reply
IFS= read -r ack < reply
case "$REPLY_MODE" in
    empty) exit 0;;
    nonzero) exit 1;;
    signal) kill -TERM "$$";;
    transport) case "$tool" in
        ssh) printf 'Connection reset by peer\n' >&2; exit 255;;
        ray) printf 'RuntimeError: Request failed with status code 503: Service Unavailable.\n' >&2; exit 1;; esac;;
    truncated) case "$tool" in ssh) printf '%s' "${ack%?}";; ray) printf '%s' "${ack% successfully}";; esac;;
    conflicting) printf '%s\n' "$ack"; case "$tool" in
        ssh) printf 'Submitted batch job 3999999999\n';;
        ray) printf "Job 'raysubmit_another' submitted successfully\n";; esac;;
    *) exit 64;;
esac
"#;

/// Install only the named executables. Resolve POSIX sh before isolating PATH (also works in Nix).
fn fixture(dir: &Path, client: &str, mode: &str) -> std::path::PathBuf {
    let shell = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|dir| dir.join("sh"))
        .find(|p| p.is_absolute() && p.is_file())
        .unwrap_or_else(|| "/bin/sh".into());
    let bin = dir.join("bin");
    fs::create_dir(&bin).unwrap();
    let script = dir.join("fixture");
    fs::write(&script, format!("#!{}\n{TOOLS}", shell.display())).unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["nix", "sbatch", "jobs-api", "sacct", "cat", client] {
        if name != client || mode != "missing" {
            fs::hard_link(&script, bin.join(name)).unwrap();
        }
    }
    for ledger in ["calls", "jobs"] {
        fs::write(dir.join(ledger), "").unwrap();
    }
    fs::write(dir.join("job.sh"), "#!/bin/sh\nexit 0\n").unwrap();
    fs::write(
        dir.join("fleet.json"),
        include_bytes!("../../../examples/inventories/fleet.json"),
    )
    .unwrap();
    shell
}

/// Observe `cast` as its caller would, and compare its report with the fixture's independent ledgers.
fn exercise(adapter: &str, mode: &str, state: &str) {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    let (client, args, endpoint, rejection): (_, &[&str], _, _) = match adapter {
        "slurm" => (
            "ssh",
            &["--script", "job.sh", "--collect", "result.txt"],
            "node01",
            SLURM_REJECTION,
        ),
        "ray" => (
            "ray",
            &["--entrypoint", "true"],
            "192.168.1.11:8265",
            RAY_REJECTION,
        ),
        _ => unreachable!(),
    };
    let shell = fixture(dir, client, mode);
    let mut cast = assert_cmd::Command::new(env!("CARGO_BIN_EXE_cast"));
    cast.current_dir(dir)
        .env_clear()
        .env("PATH", dir.join("bin"))
        .env("HOME", dir)
        .env("POSIX_SH", shell)
        .env("REPLY_MODE", mode)
        .env("REJECTION", rejection)
        .args(["--config", "fleet.json", "--format", "json"])
        .args(["integration", adapter, "--name", JOB_NAME])
        .args(args)
        .args(["--wait", "true", "--timeout", "5"])
        .timeout(Duration::from_secs(15));
    if adapter == "slurm" && mode == "rejected" {
        cast.args(["--partition", "no-such-partition"]);
    }
    let output = cast.output().unwrap();
    let calls = fs::read_to_string(dir.join("calls")).unwrap();
    let jobs = fs::read_to_string(dir.join("jobs")).unwrap();
    check_ledgers(adapter, mode, state, &calls, &jobs);

    let accepted = state == "accepted";
    let stderr = String::from_utf8_lossy(&output.stderr);
    let exit = if accepted { 0 } else { 1 };
    assert_eq!(output.status.code(), Some(exit), "{stderr}");
    let report: Value =
        serde_json::from_slice(&output.stdout).unwrap_or_else(|e| panic!("{e}: {output:?}"));
    assert_eq!(report["integration"], adapter);
    assert_eq!(report["success"], accepted, "{report}");
    check_phases(&report, adapter, state, endpoint, mode);
    let receipt = &report["submission"];
    assert_eq!(receipt["state"], state, "{report}");
    if accepted {
        assert_eq!(receipt["job_id"].as_str(), Some(jobs.trim()), "{receipt}");
    } else {
        assert!(receipt.get("job_id").is_none(), "{receipt}");
    }
}

/// Count attempts independently of admissions, including any downstream commands using an ID.
fn check_ledgers(adapter: &str, mode: &str, state: &str, calls: &str, jobs: &str) {
    let (submit, wait) = if adapter == "slurm" {
        ("sbatch", "sacct")
    } else {
        ("job submit", "job status")
    };
    let attempts = usize::from(mode != "missing");
    let submissions = calls.lines().filter(|line| line.contains(submit)).count();
    assert_eq!(submissions, attempts, "{calls}");
    let admitted = matches!(state, "accepted" | "unknown");
    assert_eq!(jobs.lines().count(), usize::from(admitted), "jobs: {jobs}");

    let mut commands = calls.lines().skip(attempts);
    if state == "accepted" {
        let poll = commands.next().expect("missing poll");
        assert!(poll.contains(wait), "{poll}");
        assert!(
            poll.split([' ', '\'']).any(|arg| arg == jobs.trim()),
            "{poll}"
        );
        if adapter == "slurm" {
            let collect = commands.next().expect("missing collect");
            assert!(collect.contains("result.txt"), "{collect}");
        }
    }
    assert!(
        commands.next().is_none(),
        "unexpected client calls: {calls}"
    );
}

/// A failed submit must be the last observed phase, not merely a nonzero CLI exit.
fn check_phases(report: &Value, adapter: &str, state: &str, endpoint: &str, mode: &str) {
    let phases = report["phases"].as_array().unwrap();
    let phase = |name: &str| phases.iter().find(|p| p["phase"] == name);
    let accepted = state == "accepted";
    assert_eq!(phase("build").unwrap()["success"], true);
    if adapter == "slurm" {
        assert_eq!(phase("copy").unwrap()["success"], true);
        assert_eq!(
            phase("collect").map(|p| &p["success"]),
            accepted.then_some(&Value::Bool(true))
        );
    }
    assert_eq!(
        phase("wait").map(|p| &p["success"]),
        accepted.then_some(&Value::Bool(true))
    );
    let submit = phase("submit").expect("submit phase did not run");
    assert_eq!(submit["success"], accepted, "{submit}");
    if state == "unknown" {
        let error = submit["error"].as_str().unwrap();
        assert!(
            error.contains(JOB_NAME) && error.contains(endpoint),
            "{error}"
        );
        if mode == "transport" {
            let evidence = if adapter == "slurm" {
                "Connection reset"
            } else {
                "503"
            };
            assert!(error.contains(evidence), "{error}");
        }
    }
}

macro_rules! receipt_case {
    ($test:ident, $adapter:literal, $mode:literal, $state:literal) => {
        #[test]
        fn $test() {
            exercise($adapter, $mode, $state);
        }
    };
}

receipt_case!(slurm_intact, "slurm", "intact", "accepted");
receipt_case!(slurm_empty, "slurm", "empty", "unknown");
receipt_case!(slurm_nondiagnostic_nonzero, "slurm", "nonzero", "unknown");
receipt_case!(slurm_ssh_255, "slurm", "transport", "unknown");
receipt_case!(slurm_signal, "slurm", "signal", "unknown");
receipt_case!(slurm_partial_id, "slurm", "truncated", "unknown");
receipt_case!(slurm_conflicting_ids, "slurm", "conflicting", "unknown");
receipt_case!(slurm_rejection, "slurm", "rejected", "rejected");
receipt_case!(slurm_missing_ssh, "slurm", "missing", "not-submitted");
receipt_case!(ray_intact, "ray", "intact", "accepted");
receipt_case!(ray_empty, "ray", "empty", "unknown");
receipt_case!(ray_nondiagnostic_nonzero, "ray", "nonzero", "unknown");
receipt_case!(ray_service_unavailable, "ray", "transport", "unknown");
receipt_case!(ray_signal, "ray", "signal", "unknown");
receipt_case!(ray_partial_banner, "ray", "truncated", "unknown");
receipt_case!(ray_conflicting_ids, "ray", "conflicting", "unknown");
receipt_case!(ray_rejection, "ray", "rejected", "rejected");
receipt_case!(ray_missing_cli, "ray", "missing", "not-submitted");
