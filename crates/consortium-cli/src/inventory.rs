//! TOML inventory file format for cascade-copy / cascade deploy bins.
//!
//! Schema:
//!
//! ```toml
//! # The seed host — `nix copy` from here runs locally (no SSH wrap).
//! # Typically the build host.
//! seed = "olive@seir"
//!
//! # All other targets the cascade should distribute to. SSH addresses;
//! # `user@host` form. The cascade strategy decides which become
//! # intermediate sources for further fan-out.
//! nodes = [
//!     "root@hp01",
//!     "root@hp02",
//!     "root@hp03",
//!     "olive@mm01",
//!     "olive@mm02",
//!     "olive@mm03",
//!     "olive@mm04",
//!     "olive@mm05",
//! ]
//! ```
//!
//! Loaded into a `(Vec<CascadeNode>, NodeId)` pair where:
//! - `nodes[0]` is the seed at `NodeId(0)`
//! - `nodes[1..]` are the targets at `NodeId(1)`, `NodeId(2)`, etc.
//! - The returned `NodeId` is the seed (always `NodeId(0)`)

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use consortium_nix::cascade::{CascadeNode, NodeId, NodeIdAlloc};

#[derive(Debug, Deserialize)]
struct InventoryFile {
    /// SSH address of the seed host (the build machine).
    seed: String,
    /// SSH addresses of every other target.
    nodes: Vec<String>,
}

/// Load a TOML inventory file. Returns `(nodes, seed_id, addr_map)`:
///
/// - `nodes`: the cascade's node list, seed at NodeId(0), targets following
/// - `seed_id`: always `NodeId(0)`, returned for clarity at call sites
/// - `addr_map`: NodeId → SSH address, ready to hand to `NixCopyExecutor`
pub fn load_inventory<P: AsRef<Path>>(
    path: P,
) -> Result<(Vec<CascadeNode>, NodeId, HashMap<NodeId, String>)> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read inventory file: {}", path.display()))?;
    let inv: InventoryFile = toml::from_str(&raw)
        .with_context(|| format!("failed to parse inventory file as TOML: {}", path.display()))?;

    if inv.seed.trim().is_empty() {
        anyhow::bail!("inventory `seed` is empty");
    }

    let mut alloc = NodeIdAlloc::new();
    let mut nodes = Vec::with_capacity(1 + inv.nodes.len());
    let mut addrs = HashMap::with_capacity(1 + inv.nodes.len());

    let seed_id = alloc.alloc();
    nodes.push(CascadeNode::new(seed_id, inv.seed.clone()));
    addrs.insert(seed_id, inv.seed);

    for addr in inv.nodes {
        if addr.trim().is_empty() {
            continue;
        }
        let id = alloc.alloc();
        nodes.push(CascadeNode::new(id, addr.clone()));
        addrs.insert(id, addr);
    }

    Ok((nodes, seed_id, addrs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn loads_basic_inventory() {
        let f = write_temp(
            r#"
seed = "olive@seir"
nodes = ["root@hp01", "root@hp02", "olive@mm01"]
"#,
        );
        let (nodes, seed, addrs) = load_inventory(f.path()).unwrap();
        assert_eq!(seed, NodeId(0));
        assert_eq!(nodes.len(), 4);
        assert_eq!(nodes[0].addr, "olive@seir");
        assert_eq!(nodes[1].addr, "root@hp01");
        assert_eq!(
            addrs.get(&NodeId(0)).map(String::as_str),
            Some("olive@seir")
        );
        assert_eq!(addrs.get(&NodeId(2)).map(String::as_str), Some("root@hp02"));
    }

    #[test]
    fn rejects_empty_seed() {
        let f = write_temp(r#"seed = "" \nnodes = []"#);
        assert!(load_inventory(f.path()).is_err());
    }

    #[test]
    fn skips_empty_node_entries() {
        let f = write_temp(
            r#"
seed = "olive@seir"
nodes = ["root@hp01", "", "root@hp02"]
"#,
        );
        let (nodes, _, _) = load_inventory(f.path()).unwrap();
        assert_eq!(nodes.len(), 3); // seed + 2 valid (skipped the empty)
    }
}
