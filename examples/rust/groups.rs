//! Group resolution: named node groups via StaticGroupSource + GroupResolver.
//!
//! NodeSet itself does not resolve `@group` syntax — groups are a separate
//! layer in `consortium::node_utils` (mirroring ClusterShell.NodeUtils).
//! This example shows:
//!
//!   - defining groups in memory with `StaticGroupSource`
//!   - resolving a group with `GroupResolver` (returns the group's pattern
//!     parts; feed them through `NodeSet` to expand bracket notation)
//!   - listing groups and resolving the special "all" set
//!   - composing groups with NodeSet set algebra (how `@group`-style
//!     selection combines with plain bracket notation)
//!
//! Prerequisites: none — fully offline.
//!
//! Run with:
//!   cargo run -p consortium-examples --example groups

use std::collections::HashMap;

use consortium::node_set::NodeSet;
use consortium::node_utils::{GroupResolver, StaticGroupSource};

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

/// Resolve a group and expand its pattern parts into a NodeSet.
fn resolve_nodeset(
    resolver: &GroupResolver,
    group: &str,
) -> Result<NodeSet, Box<dyn std::error::Error>> {
    Ok(NodeSet::parse(
        &resolver.group_nodes(group, None)?.join(","),
    )?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("Define a static group source");
    // group name -> nodeset pattern
    let mut map = HashMap::new();
    map.insert("compute".to_string(), "node[01-04]".to_string());
    map.insert("io".to_string(), "io[1-2]".to_string());
    map.insert("login".to_string(), "login1".to_string());

    // with_all / with_list provide the "all" set and the group listing;
    // without them the corresponding resolver calls return an error.
    let src = StaticGroupSource::new(map)
        .with_all("node[01-04],io[1-2],login1")
        .with_list(vec![
            "compute".to_string(),
            "io".to_string(),
            "login".to_string(),
        ]);

    let mut resolver = GroupResolver::new();
    resolver.add_source("local", Box::new(src));
    resolver.set_default("local");
    println!("registered source 'local' with groups: compute, io, login");

    banner("Resolve groups");
    // group_nodes returns the pattern parts (e.g. "node[01-04]"); expanding
    // through NodeSet yields the individual nodes.
    let compute_parts = resolver.group_nodes("compute", None)?;
    println!("@compute pattern : {compute_parts:?}");
    let compute_ns = resolve_nodeset(&resolver, "compute")?;
    println!(
        "@compute nodes   : {:?}",
        compute_ns.iter().collect::<Vec<_>>()
    );
    let io_ns = resolve_nodeset(&resolver, "io")?;
    println!("@io nodes        : {:?}", io_ns.iter().collect::<Vec<_>>());
    println!("grouplist        : {:?}", resolver.grouplist(None)?);
    let all = NodeSet::parse(&resolver.all_nodes(None)?.join(","))?;
    println!("all_nodes        : {all}");

    banner("Compose groups with NodeSet algebra");
    // This is how @group-style selection composes with plain bracket
    // notation: resolve the group, then use NodeSet set operations.
    let head = NodeSet::parse("login1,node01")?;

    println!("@compute | @io   : {}", compute_ns.union(&io_ns));
    println!("@compute & head  : {}", compute_ns.intersection(&head));
    println!(
        "@compute - node01: {}",
        compute_ns.difference(&NodeSet::parse("node01")?)
    );

    // Reverse direction: intersect an arbitrary selection with the "all"
    // set to see which fleet members it covers (and which it misses).
    let selection = NodeSet::parse("node02,io1,spare9")?;
    println!("selection ∩ all  : {}", selection.intersection(&all));
    println!("selection outside: {}", selection.difference(&all));

    Ok(())
}
