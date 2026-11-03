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

pub mod integration;
pub use integration::NixIntegration;

pub mod activate;
pub mod build;
pub mod cascade;
pub mod cascade_events;
pub mod cascade_executor;
pub mod cascade_integration;
pub mod cascade_strategies;
pub mod cascade_trace;
pub mod cascade_tree;
pub mod config;
pub mod copy;
pub mod endpoint;
pub mod error;
pub mod eval;
pub mod fleet_source;
pub mod health;
pub mod options;
pub mod tasks;

pub use config::{DeployAction, DeploymentNode, DeploymentPlan, FleetConfig, ProfileType};
pub use error::{NixError, Result};
pub use options::{DeployOptions, NixArgs};

use std::sync::Arc;

use consortium::dag::DagReport;
use consortium_integration::exec::Executor;
use consortium_integration::report::IntegrationReport;

use crate::cascade_events::EventSink;

/// Run the full deployment pipeline using the DAG executor.
///
/// Each host progresses independently through eval → build → copy → activate,
/// with per-stage concurrency limits. A host that fails at any stage is
/// cancelled for subsequent stages without blocking other hosts.
///
/// Every external command (`nix eval` / `nix build` / `nix copy` / ssh
/// activation) runs through `exec`, which is also shared with the DAG tasks
/// via the `"executor"` context state key.
///
/// # Example
///
/// ```
/// use std::collections::HashMap;
/// use std::sync::Arc;
///
/// use consortium_integration::exec::{ExecOutput, Executor, ScriptedExecutor};
/// use consortium_nix::config::{DeploymentNode, FleetConfig, ProfileType};
/// use consortium_nix::{deploy, DeployAction};
///
/// let mut nodes = HashMap::new();
/// nodes.insert(
///     "web1".to_string(),
///     DeploymentNode {
///         name: "web1".to_string(),
///         target_host: "10.0.0.1".to_string(),
///         target_user: "root".to_string(),
///         target_port: None,
///         system: "x86_64-linux".to_string(),
///         profile_type: ProfileType::Nixos,
///         build_on_target: false,
///         tags: vec![],
///         drv_path: None,
///         toplevel: None,
///     },
/// );
/// let config = FleetConfig {
///     nodes,
///     builders: HashMap::new(),
///     flake_uri: ".".to_string(),
///     ansible_config: None,
///     slurm_config: None,
///     ray_config: None,
///     skypilot_config: None,
/// };
///
/// let scripted = Arc::new(
///     ScriptedExecutor::new()
///         .on("nix eval", ExecOutput::ok("/nix/store/abc-toplevel\n"))
///         .on("nix build", ExecOutput::ok("/nix/store/abc-toplevel\n"))
///         .on("nix copy", ExecOutput::ok(""))
///         .on("ssh", ExecOutput::ok("")),
/// );
/// let exec: Arc<dyn Executor> = scripted.clone();
///
/// let report = deploy(
///     exec,
///     &config,
///     &["web1".to_string()],
///     DeployAction::Switch,
///     4,
///     false,
/// )?;
/// assert!(report.is_success());
/// scripted.assert_invoked_containing(
///     "nix build .#nixosConfigurations.web1.config.system.build.toplevel",
/// );
/// scripted.assert_invoked_containing("switch-to-configuration' 'switch'");
/// # Ok::<(), consortium_nix::NixError>(())
/// ```
pub fn deploy(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    target_nodes: &[String],
    action: DeployAction,
    max_parallel: usize,
    use_builders: bool,
) -> Result<DeployReport> {
    deploy_with_options(
        exec,
        config,
        target_nodes,
        action,
        max_parallel,
        use_builders,
        &DeployOptions::default(),
    )
}

/// [`deploy`] with [`DeployOptions`]: per-platform extra nix arguments and
/// hosts activated locally (no copy, `sudo` activation, no ssh).
#[allow(clippy::too_many_arguments)]
pub fn deploy_with_options(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    target_nodes: &[String],
    action: DeployAction,
    max_parallel: usize,
    use_builders: bool,
    options: &DeployOptions,
) -> Result<DeployReport> {
    let request =
        integration::deployment_request(target_nodes, action, max_parallel, use_builders, options)?;
    integration::run_deployment(exec, config, &request, action)
}

/// Probe the configured builders when `use_builders` is set and write a
/// machines file for the healthy ones. `None` means build locally.
pub(crate) fn prepare_builders(
    exec: &dyn Executor,
    config: &FleetConfig,
    use_builders: bool,
) -> Result<Option<consortium_integration::exec::TemporaryFile>> {
    if !use_builders || config.builders.is_empty() {
        return Ok(None);
    }
    let statuses = health::check_builders_with(exec, config);
    let healthy: Vec<_> = statuses.iter().filter(|s| s.healthy).cloned().collect();
    if healthy.is_empty() {
        exec.diagnostic("warning: no healthy builders available, building locally");
        return Ok(None);
    }
    build::generate_machines_file_from_healthy(exec, &healthy).map(Some)
}

/// Cascade-driven deploy: same eval/build/activate as [`deploy`], but
/// the per-host `nix copy` stage is replaced by a single whole-fleet
/// cascade that distributes each toplevel peer-to-peer.
///
/// ## Why
///
/// `deploy()` runs N parallel `nix copy` subprocesses, each pulling
/// from the build host. The build host's uplink is the bottleneck —
/// at scale the deploy serializes on it.
///
/// `deploy_with_cascade()` lets nodes that have already received the
/// closure serve the next round's targets. With `fanout=2` and N hosts
/// sharing one toplevel, copy time drops from `N * single_copy_duration`
/// to `ceil(log2(N+1)) * single_copy_duration` in the best case.
///
/// ## How
///
/// 1. **DAG phase 1**: per-host eval + build (same as `deploy()`).
/// 2. **Cascade phase**: collect built toplevels, group by toplevel,
///    run one `cascade_copy_grouped()` per group. Live UI via
///    `event_sink` if provided.
/// 3. **DAG phase 2**: per-host activate (only for hosts whose copy
///    succeeded; failed-copy hosts are reported as copy_failures).
///
/// ## When NOT to use
///
/// - 1-2 targets: cascade overhead > parallel direct copy. Use `deploy()`.
/// - `action == DeployAction::Build`: nothing to copy. Use `deploy()`.
/// - First-time use against an untrusted fleet: prefer `deploy()` first
///   to validate SSH + signing trust path, then switch.
#[allow(clippy::too_many_arguments)]
pub fn deploy_with_cascade(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    target_nodes: &[String],
    action: DeployAction,
    max_parallel: usize,
    use_builders: bool,
    cascade_fanout: u32,
    seed_addr: &str,
    event_sink: Option<&dyn EventSink>,
) -> Result<DeployReport> {
    deploy_with_cascade_options(
        exec,
        config,
        target_nodes,
        action,
        max_parallel,
        use_builders,
        cascade_fanout,
        seed_addr,
        event_sink,
        &DeployOptions::default(),
    )
}

/// [`deploy_with_cascade`] with [`DeployOptions`]. Local hosts never enter
/// the cascade: their closure is already here, so they go straight to
/// local activation alongside the hosts the cascade reached.
#[allow(clippy::too_many_arguments)]
pub fn deploy_with_cascade_options(
    exec: Arc<dyn Executor>,
    config: &FleetConfig,
    target_nodes: &[String],
    action: DeployAction,
    max_parallel: usize,
    use_builders: bool,
    cascade_fanout: u32,
    seed_addr: &str,
    event_sink: Option<&dyn EventSink>,
    options: &DeployOptions,
) -> Result<DeployReport> {
    if action == DeployAction::Build {
        return deploy_with_options(
            exec,
            config,
            target_nodes,
            action,
            max_parallel,
            use_builders,
            options,
        );
    }
    let request =
        integration::deployment_request(target_nodes, action, max_parallel, use_builders, options)?
            .with("cascade", "true")
            .with("cascade-fanout", cascade_fanout.max(1).to_string())
            .with("seed", seed_addr);
    integration::run_deployment_with_events(exec, config, &request, action, event_sink)
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
    /// Hosts that failed nix evaluation (name, error message).
    pub eval_failures: Vec<(String, String)>,
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
        self.eval_failures.is_empty()
            && self.build_failures.is_empty()
            && self.copy_failures.is_empty()
            && self.activation_failures.is_empty()
    }

    /// Total number of failures across all phases.
    pub fn failure_count(&self) -> usize {
        self.eval_failures.len()
            + self.build_failures.len()
            + self.copy_failures.len()
            + self.activation_failures.len()
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
        let mut eval_failures = Vec::new();
        let mut build_failures = Vec::new();
        let mut copy_failures = Vec::new();
        let mut activation_failures = Vec::new();

        for host in target_nodes {
            let eval_id = consortium::dag::TaskId(format!("eval:{}", host));
            let build_id = consortium::dag::TaskId(format!("build:{}", host));
            let copy_id = consortium::dag::TaskId(format!("copy:{}", host));
            let activate_id = consortium::dag::TaskId(format!("activate:{}", host));

            // Check eval. An eval failure cancels build/copy/activate for
            // the host, so without this check the report would silently
            // show success with zero work done.
            if let Some(err) = report.failed.get(&eval_id) {
                eval_failures.push((host.clone(), err.clone()));
            }

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
            eval_failures,
            build_failures,
            copy_failures,
            activation_failures,
        }
    }
}

impl IntegrationReport for DeployReport {
    fn is_success(&self) -> bool {
        DeployReport::is_success(self)
    }

    fn failure_count(&self) -> usize {
        DeployReport::failure_count(self)
    }

    fn success_count(&self) -> usize {
        DeployReport::success_count(self)
    }
}

/// Validate that every requested target node exists in the fleet config.
///
/// This runs before any command is issued: an unknown target is a
/// configuration error, not a runtime failure, so it must not be
/// discovered halfway through the pipeline (or worse, silently turned
/// into per-task failures after side effects have already happened).
fn validate_targets(config: &FleetConfig, target_nodes: &[String]) -> Result<()> {
    for name in target_nodes {
        if !config.nodes.contains_key(name) {
            return Err(NixError::General(format!(
                "target node '{}' is not present in the fleet config",
                name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium::dag::{DagReport, TaskId};
    use std::collections::{HashMap, HashSet};

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
        assert!(report.eval_failures.is_empty());
    }

    #[test]
    fn test_deploy_report_eval_failure() {
        // eval fails for hp02: build/copy/activate for hp02 are cancelled
        // (not failed). The eval failure must still be attributed, or the
        // report would wrongly show is_success() == true.
        let dag_report = make_report(
            &["eval:hp01", "build:hp01", "copy:hp01", "activate:hp01"],
            &[("eval:hp02", "attribute 'nixosConfigurations.hp02' missing")],
            &["build:hp02", "copy:hp02", "activate:hp02"],
        );
        let targets = vec!["hp01".to_string(), "hp02".to_string()];
        let report = DeployReport::from_dag_report(&dag_report, &targets, DeployAction::Switch);

        assert!(!report.is_success());
        assert_eq!(report.failure_count(), 1);
        assert_eq!(
            report.eval_failures,
            vec![(
                "hp02".to_string(),
                "attribute 'nixosConfigurations.hp02' missing".to_string()
            )]
        );
        assert!(report.build_failures.is_empty());
        assert!(report.copy_failures.is_empty());
        assert!(report.activation_failures.is_empty());
        assert_eq!(report.built, vec!["hp01"]);
        assert_eq!(report.activated, vec!["hp01"]);
        assert_eq!(report.success_count(), 1);
    }

    #[test]
    fn test_deploy_report_eval_failure_build_only() {
        // Same attribution requirement in build-only mode.
        let dag_report = make_report(
            &["eval:hp01", "build:hp01"],
            &[("eval:hp02", "eval boom")],
            &["build:hp02"],
        );
        let targets = vec!["hp01".to_string(), "hp02".to_string()];
        let report = DeployReport::from_dag_report(&dag_report, &targets, DeployAction::Build);

        assert!(!report.is_success());
        assert_eq!(report.failure_count(), 1);
        assert_eq!(
            report.eval_failures,
            vec![("hp02".to_string(), "eval boom".to_string())]
        );
        assert_eq!(report.built, vec!["hp01"]);
    }

    #[test]
    fn test_deploy_report_eval_failure_all_hosts() {
        // First-phase failure for every host: nothing may count as success.
        let dag_report = make_report(
            &[],
            &[("eval:hp01", "boom"), ("eval:hp02", "boom")],
            &[
                "build:hp01",
                "copy:hp01",
                "activate:hp01",
                "build:hp02",
                "copy:hp02",
                "activate:hp02",
            ],
        );
        let targets = vec!["hp01".to_string(), "hp02".to_string()];
        let report = DeployReport::from_dag_report(&dag_report, &targets, DeployAction::Switch);

        assert!(!report.is_success());
        assert_eq!(report.failure_count(), 2);
        assert_eq!(report.success_count(), 0);
        assert!(report.built.is_empty());
        assert!(report.activated.is_empty());
    }

    #[test]
    fn test_deploy_report_integration_report_delegates() {
        let report = DeployReport {
            built: vec!["hp01".to_string()],
            copied: vec!["hp01".to_string()],
            activated: vec!["hp01".to_string()],
            eval_failures: vec![("hp02".to_string(), "boom".to_string())],
            build_failures: vec![],
            copy_failures: vec![],
            activation_failures: vec![],
        };
        let trait_view: &dyn IntegrationReport = &report;
        assert_eq!(trait_view.is_success(), report.is_success());
        assert_eq!(trait_view.failure_count(), report.failure_count());
        assert_eq!(trait_view.success_count(), report.success_count());
        assert!(!trait_view.is_success());
        assert_eq!(trait_view.failure_count(), 1);
        assert_eq!(trait_view.success_count(), 1);
    }

    #[test]
    fn test_validate_targets_rejects_unknown_node() {
        let config = FleetConfig {
            nodes: HashMap::new(),
            builders: HashMap::new(),
            flake_uri: ".".to_string(),
            ansible_config: None,
            slurm_config: None,
            ray_config: None,
            skypilot_config: None,
        };
        let targets = vec!["node01".to_string()];
        let err = validate_targets(&config, &targets).unwrap_err();
        assert!(err.to_string().contains("node01"), "{}", err);
        assert!(validate_targets(&config, &[]).is_ok());
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
        assert!(report.eval_failures.is_empty());
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
        assert!(report.eval_failures.is_empty());
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
        assert!(report.eval_failures.is_empty());
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
