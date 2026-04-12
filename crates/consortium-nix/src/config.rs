//! Configuration types for NixOS deployment.
//!
//! These types map to the JSON produced by the Nix library's `mkFleet` function.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Profile type for a deployment target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProfileType {
    Nixos,
    NixDarwin,
}

/// A single deployment target node.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentNode {
    /// Node name (matches nixosConfigurations key).
    pub name: String,
    /// Target host (hostname or IP).
    pub target_host: String,
    /// SSH user for deployment.
    pub target_user: String,
    /// SSH port (None = default 22).
    pub target_port: Option<u16>,
    /// System architecture (e.g. "x86_64-linux").
    pub system: String,
    /// Profile type.
    pub profile_type: ProfileType,
    /// Whether to build on the target itself.
    pub build_on_target: bool,
    /// Tags for group-based selection.
    pub tags: Vec<String>,
    /// Derivation path for the system toplevel.
    #[serde(default)]
    pub drv_path: Option<String>,
    /// Store path for the system toplevel (after build).
    #[serde(default)]
    pub toplevel: Option<String>,
}

/// A remote builder machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Builder {
    /// Builder hostname or IP.
    pub host: String,
    /// SSH user for builder access.
    pub user: String,
    /// Maximum concurrent build jobs.
    pub max_jobs: u32,
    /// Speed factor (higher = preferred).
    pub speed_factor: u32,
    /// Supported system types.
    pub systems: Vec<String>,
    /// Supported build features.
    pub features: Vec<String>,
    /// Path to SSH identity file for builder access.
    pub ssh_key: Option<String>,
    /// SSH protocol (e.g. "ssh-ng").
    #[serde(default = "default_protocol")]
    pub protocol: String,
}

fn default_protocol() -> String {
    "ssh-ng".to_string()
}

/// Complete fleet configuration produced by Nix evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetConfig {
    /// Deployment nodes keyed by hostname.
    pub nodes: HashMap<String, DeploymentNode>,
    /// Available remote builders keyed by hostname.
    #[serde(default)]
    pub builders: HashMap<String, Builder>,
    /// Flake URI for builds (e.g. "." or "github:user/repo").
    #[serde(default = "default_flake_uri")]
    pub flake_uri: String,
}

fn default_flake_uri() -> String {
    ".".to_string()
}

impl FleetConfig {
    /// Load fleet configuration from a JSON file.
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let content =
            std::fs::read_to_string(path).map_err(|e| ConfigError::Io(path.to_path_buf(), e))?;
        serde_json::from_str(&content).map_err(|e| ConfigError::Parse(path.to_path_buf(), e))
    }

    /// Load fleet configuration from a JSON string.
    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        serde_json::from_str(json).map_err(|e| ConfigError::Parse(PathBuf::from("<string>"), e))
    }

    /// Get nodes matching a set of tags (any match).
    pub fn nodes_by_tags(&self, tags: &[String]) -> Vec<&DeploymentNode> {
        self.nodes
            .values()
            .filter(|n| n.tags.iter().any(|t| tags.contains(t)))
            .collect()
    }

    /// Get nodes matching a list of names (supports consortium NodeSet patterns).
    pub fn nodes_by_names(&self, names: &[String]) -> Vec<&DeploymentNode> {
        self.nodes
            .values()
            .filter(|n| names.contains(&n.name))
            .collect()
    }

    /// Get all node names as a sorted vector.
    pub fn node_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.nodes.keys().cloned().collect();
        names.sort();
        names
    }

    /// Get all builder names as a sorted vector.
    pub fn builder_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.builders.keys().cloned().collect();
        names.sort();
        names
    }

    /// Generate a Nix machines file string from the builder pool.
    pub fn machines_file(&self) -> String {
        self.builders
            .values()
            .map(|b| {
                let key = b.ssh_key.as_deref().unwrap_or("-");
                let features = b.features.join(",");
                let systems = b.systems.join(",");
                format!(
                    "{}://{}@{} {} {} {} {} {}",
                    b.protocol, b.user, b.host, systems, key, b.max_jobs, b.speed_factor, features
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Deployment action to perform on targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeployAction {
    /// Activate and set as boot default.
    Switch,
    /// Set as boot default without activating.
    Boot,
    /// Activate without setting as boot default.
    Test,
    /// Check what would change without activating.
    DryActivate,
    /// Only build, don't deploy.
    Build,
}

impl std::fmt::Display for DeployAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeployAction::Switch => write!(f, "switch"),
            DeployAction::Boot => write!(f, "boot"),
            DeployAction::Test => write!(f, "test"),
            DeployAction::DryActivate => write!(f, "dry-activate"),
            DeployAction::Build => write!(f, "build"),
        }
    }
}

impl std::str::FromStr for DeployAction {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "switch" => Ok(DeployAction::Switch),
            "boot" => Ok(DeployAction::Boot),
            "test" => Ok(DeployAction::Test),
            "dry-activate" => Ok(DeployAction::DryActivate),
            "build" => Ok(DeployAction::Build),
            _ => Err(ConfigError::InvalidAction(s.to_string())),
        }
    }
}

/// A single target in a deployment plan.
#[derive(Debug, Clone)]
pub struct DeploymentTarget {
    /// The node to deploy to.
    pub node: DeploymentNode,
    /// Built toplevel store path.
    pub toplevel_path: String,
    /// Current system path on the target (if known).
    pub current_system: Option<String>,
    /// Whether the closure needs building.
    pub needs_build: bool,
    /// Whether the closure needs copying to the target.
    pub needs_copy: bool,
}

/// A deployment plan describing what to do.
#[derive(Debug, Clone)]
pub struct DeploymentPlan {
    /// Targets to deploy.
    pub targets: Vec<DeploymentTarget>,
    /// Action to perform.
    pub action: DeployAction,
    /// Maximum parallel operations (fanout).
    pub max_parallel: usize,
}

impl DeploymentPlan {
    /// Create a new empty deployment plan.
    pub fn new(action: DeployAction, max_parallel: usize) -> Self {
        Self {
            targets: Vec::new(),
            action,
            max_parallel,
        }
    }

    /// Number of targets that need building.
    pub fn build_count(&self) -> usize {
        self.targets.iter().filter(|t| t.needs_build).count()
    }

    /// Number of targets that need closure copying.
    pub fn copy_count(&self) -> usize {
        self.targets.iter().filter(|t| t.needs_copy).count()
    }

    /// Total number of targets.
    pub fn target_count(&self) -> usize {
        self.targets.len()
    }
}

/// Errors from configuration loading.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {0}: {1}")]
    Io(PathBuf, std::io::Error),
    #[error("failed to parse config file {0}: {1}")]
    Parse(PathBuf, serde_json::Error),
    #[error("invalid deploy action: {0}")]
    InvalidAction(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config_json() -> &'static str {
        r#"{
            "nodes": {
                "hp01": {
                    "name": "hp01",
                    "targetHost": "192.168.1.121",
                    "targetUser": "root",
                    "targetPort": null,
                    "system": "x86_64-linux",
                    "profileType": "nixos",
                    "buildOnTarget": false,
                    "tags": ["build-host", "hpe"]
                },
                "mm01": {
                    "name": "mm01",
                    "targetHost": "192.168.1.111",
                    "targetUser": "root",
                    "targetPort": null,
                    "system": "x86_64-linux",
                    "profileType": "nixos",
                    "buildOnTarget": false,
                    "tags": ["ray"]
                }
            },
            "builders": {
                "hp01": {
                    "host": "192.168.1.121",
                    "user": "root",
                    "maxJobs": 16,
                    "speedFactor": 2,
                    "systems": ["x86_64-linux"],
                    "features": ["big-parallel", "kvm"],
                    "sshKey": null,
                    "protocol": "ssh-ng"
                }
            },
            "flakeUri": "."
        }"#
    }

    #[test]
    fn test_parse_fleet_config() {
        let config = FleetConfig::from_json(sample_config_json()).unwrap();
        assert_eq!(config.nodes.len(), 2);
        assert_eq!(config.builders.len(), 1);
        assert_eq!(config.flake_uri, ".");
    }

    #[test]
    fn test_node_fields() {
        let config = FleetConfig::from_json(sample_config_json()).unwrap();
        let hp01 = &config.nodes["hp01"];
        assert_eq!(hp01.target_host, "192.168.1.121");
        assert_eq!(hp01.target_user, "root");
        assert_eq!(hp01.profile_type, ProfileType::Nixos);
        assert!(!hp01.build_on_target);
        assert_eq!(hp01.tags, vec!["build-host", "hpe"]);
    }

    #[test]
    fn test_nodes_by_tags() {
        let config = FleetConfig::from_json(sample_config_json()).unwrap();
        let build_hosts = config.nodes_by_tags(&["build-host".to_string()]);
        assert_eq!(build_hosts.len(), 1);
        assert_eq!(build_hosts[0].name, "hp01");
    }

    #[test]
    fn test_node_names_sorted() {
        let config = FleetConfig::from_json(sample_config_json()).unwrap();
        let names = config.node_names();
        assert_eq!(names, vec!["hp01", "mm01"]);
    }

    #[test]
    fn test_machines_file() {
        let config = FleetConfig::from_json(sample_config_json()).unwrap();
        let machines = config.machines_file();
        assert!(machines.contains("ssh-ng://root@192.168.1.121"));
        assert!(machines.contains("x86_64-linux"));
        assert!(machines.contains("16"));
        assert!(machines.contains("big-parallel,kvm"));
    }

    #[test]
    fn test_deploy_action_display() {
        assert_eq!(DeployAction::Switch.to_string(), "switch");
        assert_eq!(DeployAction::DryActivate.to_string(), "dry-activate");
    }

    #[test]
    fn test_deploy_action_parse() {
        assert_eq!(
            "switch".parse::<DeployAction>().unwrap(),
            DeployAction::Switch
        );
        assert_eq!(
            "dry-activate".parse::<DeployAction>().unwrap(),
            DeployAction::DryActivate
        );
        assert!("invalid".parse::<DeployAction>().is_err());
    }

    #[test]
    fn test_deployment_plan() {
        let mut plan = DeploymentPlan::new(DeployAction::Switch, 4);
        plan.targets.push(DeploymentTarget {
            node: DeploymentNode {
                name: "hp01".to_string(),
                target_host: "192.168.1.121".to_string(),
                target_user: "root".to_string(),
                target_port: None,
                system: "x86_64-linux".to_string(),
                profile_type: ProfileType::Nixos,
                build_on_target: false,
                tags: vec![],
                drv_path: None,
                toplevel: None,
            },
            toplevel_path: "/nix/store/abc-nixos-system".to_string(),
            current_system: Some("/nix/store/old-nixos-system".to_string()),
            needs_build: true,
            needs_copy: true,
        });
        assert_eq!(plan.build_count(), 1);
        assert_eq!(plan.copy_count(), 1);
        assert_eq!(plan.target_count(), 1);
    }
}
