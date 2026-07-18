//! Integration: NixOS fleet deployment (consortium-nix) without infrastructure.
//!
//! `consortium_nix::deploy` runs the full eval -> build -> copy -> activate
//! pipeline for each target host as a staged DAG. Every external command
//! goes through the `Executor` trait, so a `ScriptedExecutor` can play back
//! canned outputs and record what *would* have run — no nix, no ssh, no
//! fleet required. Swap in `ProcessExecutor::new()` to run for real.
//!
//! ScriptedExecutor matching is FIRST-MATCH-WINS on the rendered command
//! line, so specific rules come before broad ones: activation runs over
//! ssh, and the "switch-to-configuration" marker must precede any broad
//! "ssh" rule (a broad "ssh" rule would also swallow `nix copy` lines,
//! which contain `ssh-ng://` target URLs).
//!
//! Prerequisites: none — fully offline (ScriptedExecutor).
//!
//! Run with:
//!   cargo run -p consortium-examples --example integration_nix_deploy

use std::path::Path;
use std::sync::Arc;

use consortium_integration::exec::{ExecOutput, Executor, ScriptedExecutor};
use consortium_integration::fleet::FleetConfig;
use consortium_nix::{deploy, DeployAction};

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("Load fleet configuration");
    let fleet_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("inventories/fleet.json");
    let config = FleetConfig::from_file(&fleet_path)?;
    println!("loaded {} — nodes: {}", fleet_path.display(), config.node_names().join(", "));

    banner("Script the executor (first-match-wins on rendered cmdline)");
    let scripted = Arc::new(
        ScriptedExecutor::new()
            // Activation marker first: it runs over ssh, and a broad "ssh"
            // rule placed before "nix copy" would also match copy lines
            // (they contain ssh-ng:// URLs).
            .on("switch-to-configuration", ExecOutput::ok("activating the configuration...\n"))
            .on("nix eval", ExecOutput::ok("/nix/store/abc-toplevel\n"))
            .on("nix build", ExecOutput::ok("/nix/store/abc-toplevel\n"))
            .on("nix copy", ExecOutput::ok(""))
            // Catch-all LAST: any unanticipated command succeeds silently.
            .on("", ExecOutput::ok("")),
    );
    let exec: Arc<dyn Executor> = scripted.clone();

    banner("deploy — Switch action, 2 targets");
    let targets = vec!["node01".to_string(), "node02".to_string()];
    let report = deploy(exec, &config, &targets, DeployAction::Switch, 4, false)?;

    banner("DeployReport");
    println!("is_success        : {}", report.is_success());
    println!("success_count     : {}", report.success_count());
    println!("failure_count     : {}", report.failure_count());
    println!("built             : {:?}", report.built);
    println!("copied            : {:?}", report.copied);
    println!("activated         : {:?}", report.activated);
    println!("eval_failures     : {:?}", report.eval_failures);
    println!("build_failures    : {:?}", report.build_failures);
    println!("copy_failures     : {:?}", report.copy_failures);
    println!("activation_failures: {:?}", report.activation_failures);

    banner("Commands that would have run (recorded invocations)");
    for cmd in scripted.invocations() {
        println!("  {cmd}");
    }
    scripted.assert_invoked_containing("switch-to-configuration switch");

    // ── Cascade variant ──────────────────────────────────────────────────
    // For larger fleets, replace the per-host `nix copy` fan-out with a
    // peer-to-peer cascade: hosts that already have the closure serve the
    // next round's targets, so copy time drops from O(N) to O(log N).
    //
    //     use consortium_nix::deploy_with_cascade;
    //     let report = deploy_with_cascade(
    //         exec, &config, &targets, DeployAction::Switch,
    //         4,     // max_parallel
    //         false, // use_builders
    //         2,     // cascade_fanout — children per node (2 = binary tree)
    //         "root@192.168.1.11", // seed_addr — host that has the closure
    //         None,  // event_sink — Option<&dyn EventSink> for live UI
    //     )?;
    //
    // When NOT to use it: 1–2 targets (cascade overhead > parallel direct
    // copy), or DeployAction::Build (nothing to copy). See also the
    // `cascade-copy` and `cascade-viz` CLIs (examples/cli/README.md).

    Ok(())
}
