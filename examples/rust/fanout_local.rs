//! Local fan-out: run one command per node with the Exec worker.
//!
//! `Task::shell` with a nodeset schedules an `ExecWorker`: the command runs
//! once per node **locally**, with `%h` substituted for the node name. This
//! is the same fan-out engine `claw` uses; with the exec worker no SSH is
//! involved, so the example is fully offline.
//!
//! After `task.run()` the example reads results back through the task:
//! per-node return codes (`iter_retcodes`, `node_retcode`), merged stdout
//! per node (`node_buffer`), and the worst exit status (`max_retcode`).
//!
//! Prerequisites: none — fully offline.
//!
//! Run with:
//!   cargo run -p consortium-examples --example fanout_local

use consortium::task::Task;

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("Schedule a per-node local command");
    let mut task = Task::new();
    // %h expands to the node name in each per-node invocation.
    let worker_id = task.shell("echo hello from %h", Some("node[1-4]"), None);
    println!("scheduled worker {worker_id} on node[1-4]");

    banner("Run (blocks until all nodes finish)");
    task.run(None)?; // Option<Duration> overall timeout — None = no limit
    println!("done");

    banner("Per-node stdout (node_buffer)");
    for node in ["node1", "node2", "node3", "node4"] {
        let buf = task.node_buffer(node)?;
        println!("{node}: {}", String::from_utf8_lossy(&buf).trim_end());
    }

    banner("Return codes");
    // iter_retcodes groups node lists by shared exit code, rc -> nodes.
    for (rc, nodes) in task.iter_retcodes(None) {
        println!("rc {rc}: {}", nodes.join(","));
    }
    println!("node_retcode(\"node1\"): {:?}", task.node_retcode("node1"));
    println!("max_retcode           : {:?}", task.max_retcode());
    println!("num_timeout           : {}", task.num_timeout());

    banner("Mixed exit codes (node3 fails)");
    let mut task2 = Task::new();
    task2.shell(
        "if [ %h = node3 ]; then echo oops >&2; exit 1; else echo ok from %h; fi",
        Some("node[1-4]"),
        None,
    );
    task2.run(None)?;
    for (rc, nodes) in task2.iter_retcodes(None) {
        println!("rc {rc}: {}", nodes.join(","));
    }
    println!("max_retcode: {:?}", task2.max_retcode());

    Ok(())
}
