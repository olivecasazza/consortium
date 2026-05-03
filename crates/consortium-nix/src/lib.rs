//! # consortium-nix
//!
//! NixOS deployment orchestration for consortium.
//!
//! This crate provides the deployment pipeline for NixOS and nix-darwin systems,
//! replacing colmena. It consumes fleet configuration (produced by the Nix library)
//! and orchestrates the evaluate → build → copy → activate pipeline using
//! consortium's DAG executor for maximum parallelism with per-host pipelining.
//!
//! ## Architecture
//!
//! The deployment pipeline has four stages, executed as a DAG:
//!
//! ```text
//! For each host:
//!   eval(host) → build(host) → copy(host) → activate(host)
//! ```
//!
//! Stages run in parallel across hosts (up to concurrency limits), and each
//! host can advance independently — host A can be copying while host B is
//! still building.
//!
//! Builder health checking ([`health`]) validates remote builders before use.

pub mod activate;
pub mod build;
pub mod cascade;
pub mod cascade_events;
pub mod cascade_strategies;
pub mod cascade_trace;
pub mod config;
pub mod copy;
pub mod error;
pub mod eval;
pub mod health;
pub mod tasks;

pub use config::{DeployAction, DeploymentNode, DeploymentPlan, FleetConfig, ProfileType};
pub use error::{NixError, Result};

use consortium::dag::{DagContext, DagReport, ErrorPolicy, StageBuilder};

/// Run the full deployment pipeline using the DAG executor.
///
/// Each host progresses independently through eval → build → copy → activate,
/// with per-stage concurrency limits. A host that fails at any stage is
/// cancelled for subsequent stages without blocking other hosts.
pub fn deploy(
    config: &FleetConfig,
    target_nodes: &[String],
    action: DeployAction,
    max_parallel: usize,
    use_builders: bool,
) -> Result<DeployReport> {
    // Phase 0: Health check builders and prepare machines file
    let machines_file: Option<String> = if use_builders && !config.builders.is_empty() {
        let statuses = health::check_builders(config);
        let healthy: Vec<_> = statuses.iter().filter(|s| s.healthy).cloned().collect();
        if healthy.is_empty() {
            eprintln!("warning: no healthy builders available, building locally");
            None
        } else {
            match build::generate_machines_file_from_healthy(&healthy) {
                Ok(path) => Some(path),
                Err(e) => {
                    eprintln!("warning: failed to generate machines file: {}", e);
                    None
                }
            }
        }
    } else {
        None
    };

    // Set up shared context
    let ctx = DagContext::new();
    ctx.set_state("fleet_config", config.clone());
    ctx.set_state("action", action);
    if let Some(ref path) = machines_file {
        ctx.set_state("machines_file", path.clone());
    }

    // Determine stage concurrency limits
    // eval: limited to 1 (nix evaluation is memory-heavy)
    // build: up to max_parallel (nix distributes across builders internally)
    // copy: up to max_parallel (IO-bound, can be aggressive)
    // activate: limited to avoid overwhelming the fleet
    let eval_limit = 1;
    let build_limit = max_parallel;
    let copy_limit = max_parallel;
    let activate_limit = max_parallel.min(4);

    // Build the deployment DAG
    let mut builder = StageBuilder::new()
        .resources(target_nodes.to_vec())
        .stage("eval", Some(eval_limit), |host| {
            Box::new(tasks::NixEvalTask::new(host))
        })
        .stage("build", Some(build_limit), |host| {
            Box::new(tasks::NixBuildTask::new(host))
        })
        .error_policy(ErrorPolicy::ContinueIndependent)
        .context(ctx);

    // Only add copy + activate stages if not build-only
    if action != DeployAction::Build {
        builder = builder
            .stage("copy", Some(copy_limit), |host| {
                Box::new(tasks::NixCopyTask::new(host))
            })
            .stage("activate", Some(activate_limit), |host| {
                Box::new(tasks::NixActivateTask::new(host))
            });
    }

    let dag_report = builder
        .build()
        .map_err(|e| NixError::General(e.to_string()))?
        .run()
        .map_err(|e| NixError::General(e.to_string()))?;

    Ok(DeployReport::from_dag_report(
        &dag_report,
        target_nodes,
        action,
    ))
}

/// Summary of a deployment run.
#[derive(Debug)]
pub struct DeployReport {
    /// Hosts whose closures were built successfully.
    pub built: Vec<String>,
    /// Hosts whose closures were copied successfully.
    pub copied: Vec<String>,
    /// Hosts that were activated successfully.
    pub activated: Vec<String>,
    /// Hosts that failed to build (name, error message).
    pub build_failures: Vec<(String, String)>,
    /// Hosts that failed closure copy (name, error message).
    pub copy_failures: Vec<(String, String)>,
    /// Hosts that failed activation (name, error message).
    pub activation_failures: Vec<(String, String)>,
}

impl DeployReport {
    /// Whether the deployment was fully successful (no failures).
    pub fn is_success(&self) -> bool {
        self.build_failures.is_empty()
            && self.copy_failures.is_empty()
            && self.activation_failures.is_empty()
    }

    /// Total number of failures across all phases.
    pub fn failure_count(&self) -> usize {
        self.build_failures.len() + self.copy_failures.len() + self.activation_failures.len()
    }

    /// Number of hosts that completed successfully (all phases).
    pub fn success_count(&self) -> usize {
        self.activated.len()
    }

    /// Build a DeployReport from a DagReport by inspecting task IDs.
    fn from_dag_report(report: &DagReport, target_nodes: &[String], action: DeployAction) -> Self {
        let mut built = Vec::new();
        let mut copied = Vec::new();
        let mut activated = Vec::new();
        let mut build_failures = Vec::new();
        let mut copy_failures = Vec::new();
        let mut activation_failures = Vec::new();

        for host in target_nodes {
            let build_id = consortium::dag::TaskId(format!("build:{}", host));
            let copy_id = consortium::dag::TaskId(format!("copy:{}", host));
            let activate_id = consortium::dag::TaskId(format!("activate:{}", host));

            // Check build
            if report.completed.contains(&build_id) || report.skipped.contains(&build_id) {
                built.push(host.clone());
            } else if let Some(err) = report.failed.get(&build_id) {
                build_failures.push((host.clone(), err.clone()));
            }
            // cancelled builds are not reported as failures — they're implied by an earlier failure

            if action == DeployAction::Build {
                continue;
            }

            // Check copy
            if report.completed.contains(&copy_id) || report.skipped.contains(&copy_id) {
                copied.push(host.clone());
            } else if let Some(err) = report.failed.get(&copy_id) {
                copy_failures.push((host.clone(), err.clone()));
            }

            // Check activate
            if report.completed.contains(&activate_id) || report.skipped.contains(&activate_id) {
                activated.push(host.clone());
            } else if let Some(err) = report.failed.get(&activate_id) {
                activation_failures.push((host.clone(), err.clone()));
            }
        }

        Self {
            built,
            copied,
            activated,
            build_failures,
            copy_failures,
            activation_failures,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium::dag::{DagReport, TaskId};
    use std::collections::HashSet;

    fn make_report(completed: &[&str], failed: &[(&str, &str)], cancelled: &[&str]) -> DagReport {
        DagReport {
            completed: completed.iter().map(|s| TaskId(s.to_string())).collect(),
            skipped: HashSet::new(),
            failed: failed
                .iter()
                .map(|(k, v)| (TaskId(k.to_string()), v.to_string()))
                .collect(),
            cancelled: cancelled.iter().map(|s| TaskId(s.to_string())).collect(),
        }
    }

    #[test]
    fn test_deploy_report_all_success() {
        let dag_report = make_report(
            &[
                "eval:hp01",
                "build:hp01",
                "copy:hp01",
                "activate:hp01",
                "eval:hp02",
                "build:hp02",
                "copy:hp02",
                "activate:hp02",
            ],
            &[],
            &[],
        );
        let targets = vec!["hp01".to_string(), "hp02".to_string()];
        let report = DeployReport::from_dag_report(&dag_report, &targets, DeployAction::Switch);

        assert!(report.is_success());
        assert_eq!(report.built, vec!["hp01", "hp02"]);
        assert_eq!(report.copied, vec!["hp01", "hp02"]);
        assert_eq!(report.activated, vec!["hp01", "hp02"]);
        assert_eq!(report.failure_count(), 0);
    }

    #[test]
    fn test_deploy_report_build_failure() {
        let dag_report = make_report(
            &[
                "eval:hp01",
                "eval:hp02",
                "build:hp01",
                "copy:hp01",
                "activate:hp01",
            ],
            &[("build:hp02", "nix build failed")],
            &["copy:hp02", "activate:hp02"],
        );
        let targets = vec!["hp01".to_string(), "hp02".to_string()];
        let report = DeployReport::from_dag_report(&dag_report, &targets, DeployAction::Switch);

        assert!(!report.is_success());
        assert_eq!(report.built, vec!["hp01"]);
        assert_eq!(report.activated, vec!["hp01"]);
        assert_eq!(
            report.build_failures,
            vec![("hp02".to_string(), "nix build failed".to_string())]
        );
        // copy and activate for hp02 are cancelled, not failed
        assert!(report.copy_failures.is_empty());
        assert!(report.activation_failures.is_empty());
    }

    #[test]
    fn test_deploy_report_copy_failure() {
        let dag_report = make_report(
            &["eval:hp01", "build:hp01", "eval:hp02", "build:hp02"],
            &[("copy:hp01", "ssh connection refused")],
            &["activate:hp01"],
        );
        let targets = vec!["hp01".to_string(), "hp02".to_string()];
        let report = DeployReport::from_dag_report(&dag_report, &targets, DeployAction::Switch);

        assert!(!report.is_success());
        assert_eq!(report.built, vec!["hp01", "hp02"]); // both built
        assert_eq!(
            report.copy_failures,
            vec![("hp01".to_string(), "ssh connection refused".to_string())]
        );
    }

    #[test]
    fn test_deploy_report_build_only() {
        let dag_report = make_report(
            &["eval:hp01", "build:hp01", "eval:hp02", "build:hp02"],
            &[],
            &[],
        );
        let targets = vec!["hp01".to_string(), "hp02".to_string()];
        let report = DeployReport::from_dag_report(&dag_report, &targets, DeployAction::Build);

        assert!(report.is_success());
        assert_eq!(report.built, vec!["hp01", "hp02"]);
        assert!(report.copied.is_empty());
        assert!(report.activated.is_empty());
    }

    #[test]
    fn test_deploy_action_display_roundtrip() {
        for action in &["switch", "boot", "test", "dry-activate", "build"] {
            let parsed: DeployAction = action.parse().unwrap();
            assert_eq!(parsed.to_string(), *action);
        }
    }

    #[test]
    fn test_deploy_action_invalid() {
        assert!("reboot".parse::<DeployAction>().is_err());
        assert!("".parse::<DeployAction>().is_err());
        assert!("SWITCH".parse::<DeployAction>().is_err());
    }
}
