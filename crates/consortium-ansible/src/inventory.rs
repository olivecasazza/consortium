//! Generate ansible inventory from FleetConfig.

use std::collections::HashMap;

use consortium_nix::config::FleetConfig;

/// Generate an ansible inventory YAML string from fleet config.
pub fn generate_inventory(config: &FleetConfig) -> String {
    let ansible = config.ansible_config.as_ref();

    let mut groups: HashMap<String, Vec<String>> = HashMap::new();

    // Group by tags
    for (name, node) in &config.nodes {
        for tag in &node.tags {
            groups.entry(tag.clone()).or_default().push(name.clone());
        }
        // All hosts go in the "all" group
        groups
            .entry("all".to_string())
            .or_default()
            .push(name.clone());
    }

    // Add custom host groups from ansible config
    if let Some(ac) = ansible {
        for (group, hosts) in &ac.host_groups {
            groups
                .entry(group.clone())
                .or_default()
                .extend(hosts.clone());
        }
    }

    // Build YAML
    let mut yaml = String::from("all:\n  children:\n");
    for (group, hosts) in &groups {
        if group == "all" {
            continue;
        }
        yaml.push_str(&format!("    {}:\n      hosts:\n", group));
        for host in hosts {
            if let Some(node) = config.nodes.get(host) {
                yaml.push_str(&format!(
                    "        {}:\n          ansible_host: {}\n          ansible_user: {}\n",
                    host, node.target_host, node.target_user
                ));
            }
        }
    }

    yaml
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_nix::config::{AnsibleFleetConfig, DeploymentNode, ProfileType};

    fn make_fleet(
        nodes: Vec<(&str, &str, &str, Vec<&str>)>,
        ansible: Option<AnsibleFleetConfig>,
    ) -> FleetConfig {
        let mut node_map = HashMap::new();
        for (name, host, user, tags) in nodes {
            node_map.insert(
                name.to_string(),
                DeploymentNode {
                    name: name.to_string(),
                    target_host: host.to_string(),
                    target_user: user.to_string(),
                    target_port: None,
                    system: "x86_64-linux".to_string(),
                    profile_type: ProfileType::Nixos,
                    build_on_target: false,
                    tags: tags.into_iter().map(|t| t.to_string()).collect(),
                    drv_path: None,
                    toplevel: None,
                },
            );
        }
        FleetConfig {
            nodes: node_map,
            builders: HashMap::new(),
            flake_uri: ".".to_string(),
            ansible_config: ansible,
            slurm_config: None,
            ray_config: None,
            skypilot_config: None,
        }
    }

    #[test]
    fn test_generate_inventory_basic() {
        let config = make_fleet(
            vec![
                ("web1", "10.0.0.1", "root", vec!["web"]),
                ("db1", "10.0.0.2", "admin", vec!["db"]),
            ],
            None,
        );
        let inv = generate_inventory(&config);
        assert!(inv.contains("web:"));
        assert!(inv.contains("db:"));
        assert!(inv.contains("ansible_host: 10.0.0.1"));
        assert!(inv.contains("ansible_user: admin"));
    }

    #[test]
    fn test_generate_inventory_with_host_groups() {
        let config = make_fleet(
            vec![
                ("n1", "10.0.0.1", "root", vec![]),
                ("n2", "10.0.0.2", "root", vec![]),
            ],
            Some(AnsibleFleetConfig {
                control_node: "n1".to_string(),
                ansible_version: None,
                collections: vec![],
                playbook_dir: None,
                host_groups: {
                    let mut g = HashMap::new();
                    g.insert(
                        "custom".to_string(),
                        vec!["n1".to_string(), "n2".to_string()],
                    );
                    g
                },
            }),
        );
        let inv = generate_inventory(&config);
        assert!(inv.contains("custom:"));
    }

    #[test]
    fn test_generate_inventory_empty_fleet() {
        let config = make_fleet(vec![], None);
        let inv = generate_inventory(&config);
        assert!(inv.contains("all:"));
        assert!(inv.contains("children:"));
    }

    #[test]
    fn test_generate_inventory_multi_tag_host() {
        let config = make_fleet(
            vec![("gpu1", "10.0.0.1", "root", vec!["gpu", "compute", "hpc"])],
            None,
        );
        let inv = generate_inventory(&config);
        assert!(inv.contains("gpu:"));
        assert!(inv.contains("compute:"));
        assert!(inv.contains("hpc:"));
        // Host should appear under each tag group
        let gpu1_count = inv.matches("gpu1:").count();
        assert!(
            gpu1_count >= 3,
            "gpu1 should appear in 3 groups, found {}",
            gpu1_count
        );
    }
}
