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
//! *** NOT offline *** — requires SSH-reachable hosts (key auth, resolvable
//! names). Against the default `node[1-3]` pattern it will simply fail to
//! connect; pass real hosts to see it work. Compile-checked only in CI.

use std::time::Duration;

use consortium::node_set::NodeSet;
use consortium::task::Task;
use consortium::worker::ssh::{SshOptions, SshWorker};

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pattern = std::env::args().nth(1).unwrap_or_else(|| "node[1-3]".to_string());
    let command = std::env::args().nth(2).unwrap_or_else(|| "uname -r".to_string());

    banner("SSH fan-out — requires SSH-reachable hosts");
    println!("targets : {pattern}");
    println!("command : {command}");
    println!("(default targets node[1-3] are placeholders; pass real hosts as argv[1])");

    let nodes: Vec<String> = NodeSet::parse(&pattern)?.iter().collect();

    banner("Schedule SshWorker");
    let ssh_opts = SshOptions {
        user: Some("root".to_string()),
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

    Ok(())
}
