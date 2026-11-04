//! SSH fan-out: run one command on many remote hosts in parallel.
//!
//! `Task::shell` always uses the local exec worker; for real remote
//! execution you schedule an `SshWorker` explicitly. The worker wraps the
//! command in `ssh <opts> <host> <cmd>` per node and drives them with the
//! same fan-out engine as the local case, so result collection
//! (`iter_retcodes`, `node_buffer`, `max_retcode`) works identically.
//!
//! Usage:
//!   cargo run -p consortium-examples --example fanout_ssh -- [NODES] [CMD]
//!
//!   NODES  nodeset pattern          (default: node[1-3])
//!   CMD    remote command           (default: uname -r)
//!
//! Example:
//!   cargo run -p consortium-examples --example fanout_ssh -- 'web[01-04]' 'uptime'
//!
//! Requires hosts reachable over non-interactive SSH as `$SSH_USER` (default:
//! the ssh client's default user). Exits non-zero unless every host returns 0.
//! `examples/ssh/walkthrough.sh` runs it in CI against loopback sshd.

use std::time::Duration;

use consortium::node_set::NodeSet;
use consortium::task::Task;
use consortium::worker::ssh::{SshOptions, SshWorker};

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pattern = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "node[1-3]".to_string());
    let command = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "uname -r".to_string());

    banner("SSH fan-out");
    println!("targets : {pattern}");
    println!("command : {command}");

    let nodes: Vec<String> = NodeSet::parse(&pattern)?.iter().collect();

    banner("Schedule SshWorker");
    let ssh_opts = SshOptions {
        user: std::env::var("SSH_USER").ok(),
        connect_timeout: Some(5),
        strict_host_key_checking: false,
        ..Default::default()
    };
    // fanout 64, per-worker overall timeout 10s
    let worker = SshWorker::new(nodes, command, 64, Some(Duration::from_secs(10)), ssh_opts);

    let mut task = Task::new();
    task.schedule(Box::new(worker), None, false);

    banner("Run");
    task.run(None)?;

    banner("Results");
    for (rc, nodes) in task.iter_retcodes(None) {
        println!("rc {rc}: {}", nodes.join(","));
    }
    let all: Vec<String> = NodeSet::parse(&pattern)?.iter().collect();
    for node in &all {
        if let Ok(buf) = task.node_buffer(node) {
            let out = String::from_utf8_lossy(&buf);
            if !out.trim().is_empty() {
                println!("{node}: {}", out.trim_end());
            }
        }
    }
    println!("max_retcode: {:?}", task.max_retcode());
    println!("num_timeout: {}", task.num_timeout());

    match task.max_retcode() {
        Some(0) if task.num_timeout() == 0 => Ok(()),
        rc => Err(format!("not every host succeeded (max_retcode {rc:?})").into()),
    }
}
