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
