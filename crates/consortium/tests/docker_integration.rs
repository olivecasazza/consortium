//! Docker-based integration tests for consortium.
//!
//! These tests require Docker to be running and are gated behind
//! the `docker-tests` feature flag:
//!
//!   cargo test -p consortium --features docker-tests --test docker_integration

#![cfg(feature = "docker-tests")]

use std::sync::LazyLock;
use std::time::Duration;

use consortium::dag::*;
use consortium::worker::exec::ExecWorker;
use consortium::worker::Worker;
use consortium_test_harness::DockerCluster;

/// Shared cluster — started once, used by all tests in this file.
static CLUSTER: LazyLock<DockerCluster> = LazyLock::new(|| {
    DockerCluster::start_small().expect(
        "Failed to start Docker cluster. Is Docker running? \
         Run with: cargo test -p consortium --features docker-tests --test docker_integration",
    )
});

// ─── SSH Worker Tests ────────────────────────────────────────────────────────

#[test]
fn test_ssh_hostname_single_node() {
    let cluster = &*CLUSTER;
    let nodes = cluster.nodes_with_prefix("compute");
    let node = &nodes[0];
    let port = cluster.port_for(node).unwrap();
    let key = cluster.ssh_key_path().to_string_lossy().to_string();

    // Use ExecWorker directly with explicit SSH command (since SshWorker
    // doesn't support per-node port mapping — all nodes get the same port).
    let cmd = format!(
        "ssh -oStrictHostKeyChecking=no -oPasswordAuthentication=no -i {} -p {} root@127.0.0.1 hostname",
        key, port
    );
    let mut worker = ExecWorker::new(vec![node.clone()], cmd, 1, Some(Duration::from_secs(10)))
        .with_stderr(true);

    worker.start().unwrap();

    let start = std::time::Instant::now();
    while !worker.is_done() && start.elapsed() < Duration::from_secs(10) {
        let fds = worker.read_fds();
        for fd in &fds {
            let _ = worker.handle_read(*fd);
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(worker.is_done(), "worker should complete");
    let retcodes = worker.retcodes();
    assert_eq!(
        retcodes.get(node.as_str()).copied(),
        Some(0),
        "SSH hostname should succeed on {}, retcodes={:?}",
        node,
        retcodes
    );
}

#[test]
fn test_ssh_fanout_multiple_nodes() {
    let cluster = &*CLUSTER;
    let key = cluster.ssh_key_path().to_string_lossy().to_string();

    // Run `echo hello` on all compute nodes via ExecWorker (since each has a different port)
    let compute_nodes = cluster.nodes_with_prefix("compute");
    let mut nodes = Vec::new();
    let mut commands = Vec::new();

    for node in &compute_nodes {
        let port = cluster.port_for(node).unwrap();
        nodes.push(node.clone());
        commands.push(format!(
            "ssh -oStrictHostKeyChecking=no -oPasswordAuthentication=no -i {} -p {} root@127.0.0.1 'echo hello_from_$(hostname)'",
            key, port
        ));
    }

    // Use the case-dispatch pattern from the DAG executor
    let case_body: String = nodes
        .iter()
        .zip(commands.iter())
        .map(|(node, cmd)| format!("  '{}') {};;\n", node, cmd))
        .collect();

    let dispatch = format!(
        "case \"%h\" in\n{}  *) echo unknown >&2; exit 1;;\nesac",
        case_body
    );

    let mut worker = ExecWorker::new(nodes.clone(), dispatch, 64, Some(Duration::from_secs(30)))
        .with_stderr(true);
    worker.start().unwrap();

    let start = std::time::Instant::now();
    while !worker.is_done() && start.elapsed() < Duration::from_secs(30) {
        let fds = worker.read_fds();
        for fd in &fds {
            let _ = worker.handle_read(*fd);
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(worker.is_done(), "worker should complete");
    let retcodes = worker.retcodes();
    for node in &nodes {
        assert_eq!(
            retcodes.get(node).copied(),
            Some(0),
            "node {} should succeed (rc={:?})",
            node,
            retcodes.get(node)
        );
    }
    assert_eq!(retcodes.len(), nodes.len());
}

// ─── DAG Executor Tests ─────────────────────────────────────────────────────

#[test]
fn test_dag_ssh_across_cluster() {
    let cluster = &*CLUSTER;
    let key = cluster.ssh_key_path().to_string_lossy().to_string();
    let compute_nodes = cluster.nodes_with_prefix("compute");

    let mut dag = DagBuilder::new();

    for node in &compute_nodes {
        let port = cluster.port_for(node).unwrap();
        let k = key.clone();
        let n = node.clone();
        dag.add_task(
            node.as_str(),
            ShellTask {
                command: format!(
                    "ssh -oStrictHostKeyChecking=no -oPasswordAuthentication=no -i {} -p {} root@127.0.0.1 'echo ok_from_{}'",
                    k, port, n
                ),
                description: format!("check {}", n),
                resource: None,
            },
        );
    }

    let report = dag.build().unwrap().run().unwrap();
    assert!(
        report.is_success(),
        "DAG across cluster failed: {:?}",
        report.failed
    );
    assert_eq!(report.completed.len(), compute_nodes.len());
}

#[test]
fn test_stage_builder_across_cluster() {
    let cluster = &*CLUSTER;
    let key = cluster.ssh_key_path().to_string_lossy().to_string();
    let compute_nodes = cluster.nodes_with_prefix("compute");
    let fleet = cluster.fleet_config();

    let ctx = DagContext::new();
    ctx.set_state("fleet", fleet.clone());
    ctx.set_state("ssh_key", key.clone());

    let report = StageBuilder::new()
        .resources(compute_nodes.clone())
        .stage("probe", Some(5), |host| {
            let h = host.to_string();
            Box::new(FnTask::new(format!("probe {}", h), move |ctx| {
                let fleet: consortium_nix::FleetConfig = ctx.get_state("fleet").unwrap();
                let key: String = ctx.get_state("ssh_key").unwrap();
                let node = &fleet.nodes[&h];
                let port = node.target_port.unwrap_or(22);

                let output = std::process::Command::new("ssh")
                    .args([
                        "-oStrictHostKeyChecking=no",
                        "-oPasswordAuthentication=no",
                        "-oConnectTimeout=5",
                        "-i",
                        &key,
                        "-p",
                        &port.to_string(),
                        &format!("{}@{}", node.target_user, node.target_host),
                        "uname -n",
                    ])
                    .output();

                match output {
                    Ok(o) if o.status.success() => {
                        let hostname = String::from_utf8_lossy(&o.stdout).trim().to_string();
                        ctx.set_output(TaskId(format!("probe:{}", h)), hostname);
                        TaskOutcome::Success
                    }
                    Ok(o) => {
                        let err = String::from_utf8_lossy(&o.stderr);
                        TaskOutcome::Failed(format!("probe {} failed: {}", h, err.trim()))
                    }
                    Err(e) => TaskOutcome::Failed(format!("probe {} error: {}", h, e)),
                }
            }))
        })
        .stage("verify", Some(5), |host| {
            let h = host.to_string();
            Box::new(FnTask::new(format!("verify {}", h), move |ctx| {
                // Verify the probe stage produced output
                let hostname: Option<String> = ctx.get_output(&TaskId(format!("probe:{}", h)));
                match hostname {
                    Some(name) if !name.is_empty() => TaskOutcome::Success,
                    _ => TaskOutcome::Failed(format!("no probe output for {}", h)),
                }
            }))
        })
        .error_policy(ErrorPolicy::ContinueIndependent)
        .context(ctx)
        .build()
        .unwrap()
        .run()
        .unwrap();

    assert!(
        report.is_success(),
        "StageBuilder across cluster failed: {:?}",
        report.failed
    );
    // 2 stages × N compute nodes
    assert_eq!(report.completed.len(), compute_nodes.len() * 2);
}

#[test]
fn test_dag_with_concurrency_limit_across_cluster() {
    let cluster = &*CLUSTER;
    let key = cluster.ssh_key_path().to_string_lossy().to_string();
    let compute_nodes = cluster.nodes_with_prefix("compute");
    let fleet = cluster.fleet_config();

    let ctx = DagContext::new();
    ctx.set_state("fleet", fleet.clone());
    ctx.set_state("ssh_key", key.clone());

    // Run with concurrency limit of 2 across all nodes
    let report = StageBuilder::new()
        .resources(compute_nodes.clone())
        .stage("work", Some(2), |host| {
            let h = host.to_string();
            Box::new(FnTask::new(format!("work {}", h), move |ctx| {
                let fleet: consortium_nix::FleetConfig = ctx.get_state("fleet").unwrap();
                let key: String = ctx.get_state("ssh_key").unwrap();
                let node = &fleet.nodes[&h];
                let port = node.target_port.unwrap_or(22);

                let output = std::process::Command::new("ssh")
                    .args([
                        "-oStrictHostKeyChecking=no",
                        "-oPasswordAuthentication=no",
                        "-i",
                        &key,
                        "-p",
                        &port.to_string(),
                        &format!("{}@{}", node.target_user, node.target_host),
                        "echo done",
                    ])
                    .output();

                match output {
                    Ok(o) if o.status.success() => TaskOutcome::Success,
                    _ => TaskOutcome::Failed(format!("work {} failed", h)),
                }
            }))
        })
        .error_policy(ErrorPolicy::ContinueIndependent)
        .context(ctx)
        .build()
        .unwrap()
        .run()
        .unwrap();

    assert!(
        report.is_success(),
        "concurrency-limited DAG failed: {:?}",
        report.failed
    );
    assert_eq!(report.completed.len(), compute_nodes.len());
}
