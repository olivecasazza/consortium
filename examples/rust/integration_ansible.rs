//! Integration: Ansible playbook runs (consortium-ansible) without infrastructure.
//!
//! `consortium_ansible::run_playbook` stages a hermetic ansible environment
//! with nix, copies it to the fleet's control node, then runs the playbook
//! once per target host over ssh:
//!
//!   build-env (local, once) -> copy-env (to control node, once)
//!     -> run-playbook:{host} per target (over ssh on the control node)
//!
//! The DAG uses ErrorPolicy::ContinueIndependent, so one unreachable host
//! does not stop the others. Requires `ansibleConfig` in the fleet file —
//! without it, `run_playbook` returns `AnsibleError::NoConfig` before
//! issuing any command.
//!
//! Prerequisites: none — fully offline (ScriptedExecutor).
//!
//! Run with:
//!   cargo run -p consortium-examples --example integration_ansible

use std::path::Path;
use std::sync::Arc;

use consortium_ansible::{run_playbook, AnsibleOptions};
use consortium_integration::exec::{ExecOutput, Executor, ScriptedExecutor};
use consortium_integration::fleet::FleetConfig;

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("Load fleet configuration");
    let fleet_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("inventories/fleet.json");
    let config = FleetConfig::from_file(&fleet_path)?;
    let ansible = config.ansible_config.as_ref().expect("fleet.json has ansibleConfig");
    println!("control node: {} (ansible {})", ansible.control_node, ansible.ansible_version.as_deref().unwrap_or("unpinned"));

    banner("Script the executor");
    let scripted = Arc::new(
        ScriptedExecutor::new()
            .on("nix build", ExecOutput::ok("/nix/store/ansible-env\n"))
            .on("nix copy", ExecOutput::ok(""))
            .on(
                "ansible-playbook",
                ExecOutput::ok("PLAY RECAP *** ok=3 changed=1 unreachable=0 failed=0\n"),
            ),
    );
    let exec: Arc<dyn Executor> = scripted.clone();

    banner("run_playbook — site.yml on node01,node02");
    let targets = vec!["node01".to_string(), "node02".to_string()];
    // AnsibleOptions::default(): check_mode=false, max_parallel=4.
    let report = run_playbook(exec, &config, &targets, "site.yml", "default", &AnsibleOptions::default())?;

    banner("DagReport");
    println!("is_success: {}", report.is_success());
    let mut completed: Vec<_> = report.completed.iter().map(|t| t.0.clone()).collect();
    completed.sort();
    println!("completed : {completed:?}");
    // Per-host tasks are keyed run-playbook:{host}.
    for host in &targets {
        let id = format!("run-playbook:{host}");
        if report.completed.iter().any(|t| t.0 == id) {
            println!("{id}: ok");
        } else if let Some(err) = report.failed.get(&consortium::dag::TaskId::from(id.as_str())) {
            println!("{id}: FAILED — {err}");
        } else {
            println!("{id}: not run");
        }
    }

    banner("Commands that would have run (recorded invocations)");
    for cmd in scripted.invocations() {
        println!("  {cmd}");
    }
    scripted.assert_invoked_containing("ansible-playbook");

    Ok(())
}
