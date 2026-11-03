//! # consortium-ray
//!
//! Ray job orchestration with nix-built environments.
//!
//! Supports both KubeRay (Kubernetes-native) and bare-metal Ray clusters.
//! Uses nix to build hermetic job environments and the DAG executor for
//! pipeline orchestration.
//!
//! All external commands run through the [`Executor`] abstraction, so the
//! pipeline can be driven by a `ProcessExecutor` in production or a
//! `ScriptedExecutor` in tests.

pub mod integration;
pub use integration::RayIntegration;

pub mod error;
pub mod tasks;

pub use error::{RayError, Result};

use std::sync::Arc;
use std::time::Duration;

use consortium::dag::DagReport;
use consortium_integration::exec::Executor;
use consortium_integration::fleet::FleetConfig;
use consortium_integration::grammar::{duration_text, Integration, Request};

/// Options controlling a ray job submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RayOptions {
    /// Whether to wait for the job to reach a terminal state before
    /// returning. Defaults to `true`.
    pub wait: bool,

    /// Interval between `ray job status` polls while waiting. Defaults to
    /// 10 seconds. Positive subsecond intervals are supported.
    pub poll_interval: Duration,

    /// Maximum time to wait for the job before failing the wait task.
    /// `None` waits indefinitely.
    pub timeout: Option<Duration>,
}

impl Default for RayOptions {
    fn default() -> Self {
        Self {
            wait: true,
            poll_interval: Duration::from_secs(10),
            timeout: None,
        }
    }
}

/// Submit a ray job with a nix-built environment.
///
/// Builds the DAG `NixBuildRayEnvTask → RaySubmitTask (→ RayWaitTask when
/// `opts.wait`)` and runs it to completion, with all commands executed
/// through `exec`.
pub fn submit_job(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    job_name: &str,
    entrypoint: &str,
    opts: &RayOptions,
) -> Result<DagReport> {
    config.ray_config.as_ref().ok_or(RayError::NoConfig)?;
    let mut request = Request::default()
        .with("name", job_name)
        .with("entrypoint", entrypoint)
        .with("wait", opts.wait.to_string());
    if opts.wait {
        request = request.with("poll-seconds", duration_text(opts.poll_interval));
        if let Some(timeout) = opts.timeout {
            request = request.with("timeout", duration_text(timeout));
        }
    }
    RayIntegration
        .run_detailed(exec, config, &request)
        .map(|result| result.dag)
        .map_err(|error| RayError::Dag(error.to_string()))
}

#[cfg(test)]
mod typed_contract_tests {
    use super::*;
    use consortium_integration::{Effect, ExecOutput, ScriptedExecutor};

    fn config() -> FleetConfig {
        FleetConfig {
            nodes: Default::default(),
            builders: Default::default(),
            flake_uri: ".".into(),
            ansible_config: None,
            skypilot_config: None,
            ray_config: Some(consortium_integration::fleet::RayFleetConfig {
                head_address: "head".into(),
                dashboard_port: 8265,
                kubernetes: false,
                worker_groups: Default::default(),
            }),
            slurm_config: None,
        }
    }
    fn running() -> Arc<ScriptedExecutor> {
        Arc::new(
            ScriptedExecutor::new()
                .on("nix build", ExecOutput::ok("/nix/store/env"))
                .on("job submit", ExecOutput::ok("raysubmit_test"))
                .on("job status", ExecOutput::ok("Status: RUNNING")),
        )
    }

    #[test]
    fn typed_wait_preserves_subsecond_duration_precision() {
        for (poll, timeout) in [
            (Duration::from_nanos(1), Duration::from_nanos(3)),
            (
                Duration::from_nanos(1_234_567),
                Duration::from_nanos(2_345_678),
            ),
            (Duration::from_millis(250), Duration::from_millis(600)),
        ] {
            let exec = running();
            let report = submit_job(
                exec.clone(),
                &config(),
                "test",
                "run",
                &RayOptions {
                    wait: true,
                    poll_interval: poll,
                    timeout: Some(timeout),
                },
            )
            .unwrap();
            assert_eq!(report.failed.len(), 1);
            assert!(report
                .failed
                .values()
                .any(|message| message.contains("timed out")));
            assert_eq!(exec.monotonic_now(), timeout);
            let sleeps: Vec<_> = exec
                .effects()
                .into_iter()
                .filter_map(|effect| match effect {
                    Effect::Sleep { duration } => Some(duration),
                    _ => None,
                })
                .collect();
            assert_eq!(sleeps.iter().copied().sum::<Duration>(), timeout);
            assert!(sleeps
                .iter()
                .all(|duration| !duration.is_zero() && *duration <= poll));
            let limits: Vec<_> = exec
                .effects()
                .into_iter()
                .filter_map(|effect| match effect {
                    Effect::Command {
                        timeout: Some(limit),
                        ..
                    } => Some(limit),
                    _ => None,
                })
                .collect();
            assert_eq!(limits.first(), Some(&timeout));
            assert!(limits.iter().all(|limit| !limit.is_zero()));
        }
    }

    #[test]
    fn no_wait_ignores_unused_zero_poll_and_timeout_options() {
        let exec = running();
        let report = submit_job(
            exec.clone(),
            &config(),
            "test",
            "run",
            &RayOptions {
                wait: false,
                poll_interval: Duration::ZERO,
                timeout: Some(Duration::ZERO),
            },
        )
        .unwrap();
        assert!(report.failed.is_empty());
        assert!(!exec
            .effects()
            .iter()
            .any(|effect| matches!(effect, Effect::Sleep { .. })));
        assert_eq!(exec.monotonic_now(), Duration::ZERO);
    }
}
