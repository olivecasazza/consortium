//! Builder health checking — probe builders for SSH connectivity and Nix store access.

use std::time::Instant;

use consortium_integration::exec::{CommandSpec, Executor, ProcessExecutor, SshTarget};

use crate::config::{Builder, FleetConfig};
use crate::error::{NixError, Result};

/// Health status for a single builder.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// The builder configuration.
    pub builder: Builder,
    /// Whether the builder is healthy (SSH + nix store reachable).
    pub healthy: bool,
    /// Round-trip latency in milliseconds (if healthy).
    pub latency_ms: Option<u64>,
    /// Error message (if unhealthy).
    pub error: Option<String>,
}

/// Probe all builders in the fleet and return their health status.
///
/// Convenience wrapper around [`check_builders_with`] using a local
/// [`ProcessExecutor`].
pub fn check_builders(config: &FleetConfig) -> Vec<HealthStatus> {
    check_builders_with(&ProcessExecutor::new(), config)
}

/// Probe all builders in the fleet via an [`Executor`].
pub fn check_builders_with(exec: &dyn Executor, config: &FleetConfig) -> Vec<HealthStatus> {
    // TODO: parallelize with consortium's SshWorker + fanout
    config
        .builders
        .values()
        .map(|builder| check_builder_with(exec, builder))
        .collect()
}

/// Probe a single builder for health via an [`Executor`].
pub fn check_builder_with(exec: &dyn Executor, builder: &Builder) -> HealthStatus {
    let start = Instant::now();

    // First check SSH connectivity
    let ssh_spec = CommandSpec::new("true")
        .ssh(SshTarget::new(&builder.user, &builder.host).extra_opt("-oConnectTimeout=5"));
    match exec.exec(&ssh_spec) {
        Err(e) => HealthStatus {
            builder: builder.clone(),
            healthy: false,
            latency_ms: None,
            error: Some(format!("SSH exec failed: {}", e)),
        },
        Ok(output) if !output.success() => HealthStatus {
            builder: builder.clone(),
            healthy: false,
            latency_ms: None,
            error: Some(format!("SSH connection failed: {}", output.stderr.trim())),
        },
        Ok(_) => {
            let ssh_latency = start.elapsed().as_millis() as u64;

            // Now check nix store accessibility
            let store_uri = format!("{}://{}@{}", builder.protocol, builder.user, builder.host);
            let ping_spec =
                CommandSpec::new("nix").args(["store", "ping", "--store", store_uri.as_str()]);
            match exec.exec(&ping_spec) {
                Err(e) => HealthStatus {
                    builder: builder.clone(),
                    healthy: false,
                    latency_ms: Some(ssh_latency),
                    error: Some(format!("nix store ping failed: {}", e)),
                },
                Ok(output) if !output.success() => HealthStatus {
                    builder: builder.clone(),
                    healthy: false,
                    latency_ms: Some(ssh_latency),
                    error: Some(format!(
                        "nix store unreachable: {}",
                        output.stderr.trim()
                    )),
                },
                Ok(_) => HealthStatus {
                    builder: builder.clone(),
                    healthy: true,
                    latency_ms: Some(start.elapsed().as_millis() as u64),
                    error: None,
                },
            }
        }
    }
}

/// Get only healthy builders, sorted by speed factor (highest first).
pub fn healthy_builders(statuses: &[HealthStatus]) -> Vec<&HealthStatus> {
    let mut healthy: Vec<_> = statuses.iter().filter(|s| s.healthy).collect();
    healthy.sort_by_key(|s| std::cmp::Reverse(s.builder.speed_factor));
    healthy
}

/// Pre-warm SSH connections to builders by establishing ControlMaster sockets.
pub fn warm_connections(
    exec: &dyn Executor,
    builders: &[&HealthStatus],
    control_path: &str,
) -> Result<()> {
    for status in builders {
        let b = &status.builder;
        // `-fN` backgrounds the connection without running a remote command;
        // `true` is only a placeholder for the composed ssh argv.
        let spec = CommandSpec::new("true").ssh(SshTarget::new(&b.user, &b.host).extra_opts([
            "-oControlMaster=auto".to_string(),
            format!("-oControlPath={}", control_path),
            "-oControlPersist=10m".to_string(),
            "-fN".to_string(),
        ]));
        let output = exec.exec(&spec).map_err(|e| NixError::SshFailed {
            host: b.host.clone(),
            message: format!("failed to warm connection: {}", e),
        })?;

        if !output.success() {
            // Non-fatal: just log and continue
            eprintln!(
                "warning: failed to warm SSH connection to {}: {}",
                b.host,
                output.stderr.trim()
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};
    use std::collections::HashMap;

    fn test_builder(host: &str) -> Builder {
        Builder {
            host: host.to_string(),
            user: "root".to_string(),
            max_jobs: 4,
            speed_factor: 1,
            systems: vec!["x86_64-linux".to_string()],
            features: vec![],
            ssh_key: None,
            protocol: "ssh-ng".to_string(),
        }
    }

    #[test]
    fn test_healthy_builder_passes_both_probes() {
        let exec = ScriptedExecutor::new()
            .on("ssh", ExecOutput::ok(""))
            .on("nix store ping", ExecOutput::ok(""));
        let status = check_builder_with(&exec, &test_builder("b1"));
        assert!(status.healthy);
        assert!(status.error.is_none());
        exec.assert_invoked_containing("-oConnectTimeout=5");
        exec.assert_invoked_containing("nix store ping --store ssh-ng://root@b1");
    }

    #[test]
    fn test_unhealthy_when_ssh_probe_fails() {
        let exec = ScriptedExecutor::new().on(
            "ssh",
            ExecOutput::new(255, "", "ssh: connect to host b1 port 22: Connection refused"),
        );
        let status = check_builder_with(&exec, &test_builder("b1"));
        assert!(!status.healthy);
        let error = status.error.unwrap();
        assert!(error.contains("SSH connection failed"), "{}", error);
        assert!(error.contains("Connection refused"), "{}", error);
        // The nix store ping must not run after a failed ssh probe.
        exec.assert_not_invoked_containing("nix store ping");
    }

    #[test]
    fn test_unhealthy_when_store_ping_fails() {
        // NB: the ping's store URI ("ssh-ng://…") contains "ssh", so the
        // ssh-probe rule must match the composed ssh line more specifically.
        let exec = ScriptedExecutor::new()
            .on("ssh -oStrictHostKeyChecking=no", ExecOutput::ok(""))
            .on("nix store ping", ExecOutput::new(1, "", "cannot open connection"));
        let status = check_builder_with(&exec, &test_builder("b1"));
        assert!(!status.healthy);
        let error = status.error.unwrap();
        assert!(error.contains("nix store unreachable"), "{}", error);
    }

    #[test]
    fn test_check_builders_with_covers_fleet() {
        let mut builders = HashMap::new();
        builders.insert("b1".to_string(), test_builder("b1"));
        builders.insert("b2".to_string(), test_builder("b2"));
        let config = FleetConfig {
            nodes: HashMap::new(),
            builders,
            flake_uri: ".".to_string(),
            ansible_config: None,
            slurm_config: None,
            ray_config: None,
            skypilot_config: None,
        };
        let exec = ScriptedExecutor::new().on_error("ssh", "connection refused");
        let statuses = check_builders_with(&exec, &config);
        assert_eq!(statuses.len(), 2);
        assert!(statuses.iter().all(|s| !s.healthy));
        assert!(statuses
            .iter()
            .all(|s| s.error.as_ref().unwrap().contains("SSH exec failed")));
    }
}
