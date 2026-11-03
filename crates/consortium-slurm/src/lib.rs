//! # consortium-slurm
//!
//! Slurm job orchestration with nix-built environments.
//!
//! Uses nix to build hermetic job environments, copies them to the submit
//! node, then submits jobs with slurm. Supports arbitrary DAG pipelines
//! (e.g., RNA-seq bioinformatics workflows) via DagBuilder.
//!
//! All external commands run through an injected
//! `consortium_integration::exec::Executor`: production callers pass a
//! `ProcessExecutor`, tests pass a `ScriptedExecutor`.

pub mod integration;
pub use integration::SlurmIntegration;

pub mod error;
pub mod tasks;

pub use error::{Result, SlurmError};

use std::sync::Arc;
use std::time::Duration;

use consortium::dag::DagReport;
use consortium_integration::exec::Executor;
use consortium_integration::fleet::FleetConfig;
use consortium_integration::grammar::{duration_text, Integration, Request};

/// Options for [`submit_job`].
///
/// The default exercises the full pipeline — build → copy → submit → wait —
/// with no result collection.
#[derive(Debug, Clone)]
pub struct SubmitOptions {
    /// Wait for the job to reach a terminal state (polls sacct).
    /// Default: `true`.
    pub wait: bool,
    /// Literal output file path to `cat` on the submit node once the job
    /// has finished (e.g. `"slurm-12345.out"`). Default: `None` (no
    /// collection).
    pub collect: Option<String>,
    /// sacct poll interval used while waiting. Default: 10s.
    pub poll_interval: Duration,
}

impl Default for SubmitOptions {
    fn default() -> Self {
        Self {
            wait: true,
            collect: None,
            poll_interval: Duration::from_secs(10),
        }
    }
}

/// Submit a single slurm job with a nix-built environment.
///
/// Runs the pipeline `build-job-env → copy-to-submit → submit → wait →
/// collect` as a DAG: the wait stage is included when
/// [`SubmitOptions::wait`] is set, and the collect stage when
/// [`SubmitOptions::collect`] is a literal file path — wired after `slurm-wait` when
/// waiting is enabled, directly after `slurm-submit` otherwise. All commands
/// run through `exec`.
///
/// Returns [`SlurmError::NoConfig`] before any command is issued when the
/// fleet config has no slurm sub-config.
pub fn submit_job(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    job_name: &str,
    script: &str,
    partition: Option<&str>,
    opts: &SubmitOptions,
) -> Result<DagReport> {
    submit_job_with_timeout(exec, config, job_name, script, partition, opts, None)
}

/// Submit a job with an optional overall status-wait deadline.
#[allow(clippy::too_many_arguments)]
pub fn submit_job_with_timeout(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    job_name: &str,
    script: &str,
    partition: Option<&str>,
    opts: &SubmitOptions,
    timeout: Option<Duration>,
) -> Result<DagReport> {
    config.slurm_config.as_ref().ok_or(SlurmError::NoConfig)?;
    let mut request = Request::default()
        .with("name", job_name)
        .with("script", script)
        .with("wait", opts.wait.to_string());
    if let Some(partition) = partition {
        request = request.with("partition", partition);
    }
    if let Some(collect) = &opts.collect {
        request = request.with("collect", collect);
    }
    if opts.wait {
        request = request.with("poll-seconds", duration_text(opts.poll_interval));
        if let Some(timeout) = timeout {
            request = request.with("timeout", duration_text(timeout));
        }
    }
    SlurmIntegration
        .run_detailed(exec, config, &request)
        .map(|result| result.dag)
        .map_err(|error| SlurmError::Dag(error.to_string()))
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
            slurm_config: Some(consortium_integration::fleet::SlurmFleetConfig {
                submit_node: "submit".into(),
                submit_user: "root".into(),
                control_node: "submit".into(),
                partitions: Default::default(),
            }),
            ray_config: None,
        }
    }
    fn running() -> Arc<ScriptedExecutor> {
        Arc::new(
            ScriptedExecutor::new()
                .on("nix build", ExecOutput::ok("/nix/store/env"))
                .on("nix copy", ExecOutput::ok(""))
                .on("sbatch", ExecOutput::ok("Submitted batch job 17"))
                .on("sacct", ExecOutput::ok("RUNNING")),
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
            let report = submit_job_with_timeout(
                exec.clone(),
                &config(),
                "test",
                "run",
                None,
                &SubmitOptions {
                    wait: true,
                    collect: None,
                    poll_interval: poll,
                },
                Some(timeout),
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
        let report = submit_job_with_timeout(
            exec.clone(),
            &config(),
            "test",
            "run",
            None,
            &SubmitOptions {
                wait: false,
                collect: None,
                poll_interval: Duration::ZERO,
            },
            Some(Duration::ZERO),
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
