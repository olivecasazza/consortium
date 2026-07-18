//! Output gathering: group identical per-node outputs with MsgTree.
//!
//! `MsgTree` is the primitive behind dshbak/clubak (and `claw -b` / `molt`):
//! feed it `(node, output-bytes)` pairs, then `walk()` yields one entry per
//! *distinct* output together with the list of nodes that produced it.
//!
//! This example simulates a fleet where most nodes agree on their kernel
//! version, a couple differ, and shows the gathered view — plus folding the
//! node keys back into bracket notation with NodeSet.
//!
//! Prerequisites: none — fully offline.
//!
//! Run with:
//!   cargo run -p consortium-examples --example gather_outputs

use consortium::msg_tree::{MsgTree, MsgTreeMode};
use consortium::node_set::NodeSet;

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

/// Fold node keys into bracket notation when possible.
fn fold_keys(keys: &[String]) -> String {
    NodeSet::parse(&keys.join(","))
        .map(|ns| ns.to_string())
        .unwrap_or_else(|_| keys.join(","))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("Feed per-node outputs into a MsgTree");
    // Simulated `uname -r` output collected from an eight-node fan-out.
    let outputs: Vec<(&str, &str)> = vec![
        ("node1", "5.15.0-91-generic"),
        ("node2", "5.15.0-91-generic"),
        ("node3", "5.15.0-91-generic"),
        ("node4", "6.1.0-18-amd64"),
        ("node5", "5.15.0-91-generic"),
        ("node6", "6.1.0-18-amd64"),
        ("node7", "5.15.0-91-generic"),
        ("node8", "5.10.0-27-amd64"),
    ];

    // Defer mode: messages are indexed lazily at walk() time.
    let mut tree: MsgTree<String> = MsgTree::new(MsgTreeMode::Defer);
    for (node, out) in &outputs {
        tree.add(node.to_string(), format!("{out}\n").into_bytes());
    }
    println!("fed {} node outputs", outputs.len());

    banner("walk() — one entry per distinct output");
    for (msg, keys) in tree.walk(None) {
        println!("{} ({} nodes):", fold_keys(&keys), keys.len());
        print!("{}", String::from_utf8_lossy(&msg));
    }

    banner("walk() with a key filter");
    // The filter restricts which keys participate in the walk.
    let even = |k: &String| {
        k.strip_prefix("node")
            .and_then(|n| n.parse::<u32>().ok())
            .is_some_and(|n| n % 2 == 0)
    };
    println!("(even-numbered nodes only)");
    for (msg, keys) in tree.walk(Some(&even)) {
        println!("{}: {}", fold_keys(&keys), String::from_utf8_lossy(&msg).trim_end());
    }

    Ok(())
}
