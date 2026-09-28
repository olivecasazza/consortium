//! ClusterShell node-group lookup shared by `claw` and `cast`.
//!
//! Two sources, in order:
//!
//! 1. `groups.conf` (`$XDG_CONFIG_HOME/clustershell/groups.conf`, default
//!    `~/.config/clustershell/groups.conf`, then `/etc/clustershell/groups.conf`)
//!    through [`GroupResolverConfig`] — the same upcall-driven resolver
//!    `claw -g` uses.
//! 2. Flat `groups.d` files (`$XDG_CONFIG_HOME/clustershell/groups.d/*`,
//!    then `/etc/clustershell/groups.d/*`; YAML files are skipped) with one
//!    `group: nodeset` per line, where the nodeset is a whitespace- or
//!    comma-separated list of node patterns.
//!
//! `@group` and `@source:group` spellings are accepted; with flat files the
//! source selects the file stem inside `groups.d`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use consortium::node_set::NodeSet;
use consortium::node_utils::GroupResolverConfig;

/// The user's ClusterShell config directory:
/// `$XDG_CONFIG_HOME/clustershell` or `~/.config/clustershell`.
pub fn user_config_dir() -> Option<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(xdg).join("clustershell"));
    }
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config/clustershell"))
}

/// Find groups.conf config files in standard locations.
pub fn find_groups_conf() -> Vec<PathBuf> {
    let mut paths = Vec::new();

    if let Some(dir) = user_config_dir() {
        let user_conf = dir.join("groups.conf");
        if user_conf.exists() {
            paths.push(user_conf);
        }
    }

    // System config: /etc/clustershell/groups.conf
    let sys_conf = PathBuf::from("/etc/clustershell/groups.conf");
    if sys_conf.exists() {
        paths.push(sys_conf);
    }

    paths
}

/// Load a GroupResolverConfig from standard config file locations.
pub fn load_group_resolver() -> anyhow::Result<GroupResolverConfig> {
    let paths = find_groups_conf();
    if paths.is_empty() {
        anyhow::bail!(
            "no groups.conf found (checked ~/.config/clustershell/ and /etc/clustershell/)"
        );
    }
    Ok(GroupResolverConfig::new(paths, HashSet::new()))
}

/// The `groups.d` directories holding flat group files, user first.
pub fn groups_d_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Some(dir) = user_config_dir() {
        dirs.push(dir.join("groups.d"));
    }
    dirs.push(PathBuf::from("/etc/clustershell/groups.d"));
    dirs
}

/// `(group, nodeset)` pairs from a flat `group: nodeset` file. Blank lines
/// and `#` comments are ignored.
pub fn parse_flat_groups(content: &str) -> Vec<(String, String)> {
    content
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            let (name, nodes) = l.split_once(':')?;
            let name = name.trim();
            if name.is_empty() {
                return None;
            }
            Some((name.to_string(), nodes.trim().to_string()))
        })
        .collect()
}

/// Look `group` up in the flat files under `dirs`. `source` restricts the
/// search to files whose stem equals it. The first match wins.
pub fn lookup_flat_group(dirs: &[PathBuf], source: Option<&str>, group: &str) -> Option<String> {
    for dir in dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        let mut files: Vec<PathBuf> = entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file() && !is_yaml(p))
            .filter(|p| {
                source.map_or(true, |s| {
                    p.file_stem().and_then(|st| st.to_str()) == Some(s)
                })
            })
            .collect();
        files.sort();
        for file in files {
            let Ok(content) = std::fs::read_to_string(&file) else {
                continue;
            };
            if let Some((_, nodes)) = parse_flat_groups(&content)
                .into_iter()
                .find(|(g, _)| g == group)
            {
                return Some(nodes);
            }
        }
    }
    None
}

fn is_yaml(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("yaml") | Some("yml")
    )
}

/// Resolve a group spelled `group` or `source:group` (without the leading
/// `@`) to node patterns: groups.conf first, then flat `groups.d` files.
pub fn group_nodes(spec: &str) -> anyhow::Result<Vec<String>> {
    let (source, group) = match spec.split_once(':') {
        Some((s, g)) => (Some(s), g),
        None => (None, spec),
    };
    if group.is_empty() {
        anyhow::bail!("empty group name in '@{}'", spec);
    }

    if let Ok(mut config) = load_group_resolver() {
        if let Ok(resolver) = config.resolver() {
            if let Ok(nodes) = resolver.group_nodes(group, source) {
                if !nodes.is_empty() {
                    return Ok(nodes);
                }
            }
        }
    }

    let dirs = groups_d_dirs();
    if let Some(nodes) = lookup_flat_group(&dirs, source, group) {
        return Ok(split_patterns(&nodes));
    }

    let mut checked: Vec<String> = find_groups_conf()
        .iter()
        .map(|p| p.display().to_string())
        .collect();
    checked.extend(dirs.iter().map(|d| format!("{}/*", d.display())));
    anyhow::bail!(
        "group '@{}' not found (checked: {})",
        spec,
        checked.join(", ")
    )
}

/// Split a flat-file nodeset value into individual patterns.
fn split_patterns(nodes: &str) -> Vec<String> {
    nodes
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Expand a `--on` pattern whose top-level comma-separated tokens may be
/// `@group` references, resolving each through `lookup`, into a
/// [`NodeSet`]. Plain tokens keep full bracket/range syntax.
pub fn expand_pattern(
    pattern: &str,
    lookup: &dyn Fn(&str) -> anyhow::Result<Vec<String>>,
) -> anyhow::Result<NodeSet> {
    let mut ns = NodeSet::new();
    let mut plain: Vec<&str> = Vec::new();
    for token in split_top_level(pattern) {
        if let Some(group) = token.strip_prefix('@') {
            for node_pattern in lookup(group)? {
                let parsed = NodeSet::parse(&node_pattern).map_err(|e| {
                    anyhow::anyhow!(
                        "invalid node pattern '{}' in group '@{}': {}",
                        node_pattern,
                        group,
                        e
                    )
                })?;
                ns.update(&parsed);
            }
        } else if !token.is_empty() {
            plain.push(token);
        }
    }
    if !plain.is_empty() {
        let joined = plain.join(",");
        let parsed = NodeSet::parse(&joined)
            .map_err(|e| anyhow::anyhow!("invalid node pattern '{}': {}", joined, e))?;
        ns.update(&parsed);
    }
    Ok(ns)
}

/// Split on commas outside `[...]`.
fn split_top_level(pattern: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, c) in pattern.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                out.push(pattern[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(pattern[start..].trim());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_flat_group_lines_and_skips_comments() {
        let groups = parse_flat_groups(
            "# fleet\n\ndarwin: mac01.local mac02.local\nlinux: box[01-03],gpu01\nbad line\n",
        );
        assert_eq!(
            groups,
            vec![
                ("darwin".to_string(), "mac01.local mac02.local".to_string()),
                ("linux".to_string(), "box[01-03],gpu01".to_string()),
            ]
        );
    }

    #[test]
    fn flat_lookup_prefers_first_dir_and_honours_source_stem() {
        let user = tempfile::tempdir().unwrap();
        let sys = tempfile::tempdir().unwrap();
        std::fs::write(user.path().join("cluster.cfg"), "web: u[1-2]\n").unwrap();
        std::fs::write(user.path().join("lab.cfg"), "web: lab1\ndb: lab-db\n").unwrap();
        std::fs::write(user.path().join("cluster.yaml"), "web: yaml-only\n").unwrap();
        std::fs::write(sys.path().join("cluster.cfg"), "web: s[1-2]\nsys: s9\n").unwrap();
        let dirs = vec![user.path().to_path_buf(), sys.path().to_path_buf()];

        assert_eq!(
            lookup_flat_group(&dirs, None, "web").as_deref(),
            Some("u[1-2]")
        );
        assert_eq!(
            lookup_flat_group(&dirs, Some("lab"), "web").as_deref(),
            Some("lab1")
        );
        assert_eq!(
            lookup_flat_group(&dirs, None, "db").as_deref(),
            Some("lab-db")
        );
        assert_eq!(lookup_flat_group(&dirs, None, "sys").as_deref(), Some("s9"));
        assert_eq!(lookup_flat_group(&dirs, None, "nope"), None);
    }

    #[test]
    fn expand_pattern_mixes_groups_and_bracket_ranges() {
        let lookup = |g: &str| -> anyhow::Result<Vec<String>> {
            match g {
                "darwin" => Ok(vec!["mac01.local".into(), "mac02.local".into()]),
                "gpu" => Ok(vec!["gpu[1-2]".into()]),
                other => anyhow::bail!("group '@{}' not found", other),
            }
        };
        let ns = expand_pattern("@darwin,box[01-02],@gpu", &lookup).unwrap();
        let mut nodes: Vec<String> = ns.iter().collect();
        nodes.sort();
        assert_eq!(
            nodes,
            vec![
                "box01",
                "box02",
                "gpu1",
                "gpu2",
                "mac01.local",
                "mac02.local"
            ]
        );

        let err = expand_pattern("@nope", &lookup).unwrap_err();
        assert!(err.to_string().contains("'@nope' not found"), "{err}");
    }

    #[test]
    fn split_top_level_keeps_bracket_commas() {
        assert_eq!(split_top_level("a[1,3],@g, b"), vec!["a[1,3]", "@g", "b"]);
    }
}
