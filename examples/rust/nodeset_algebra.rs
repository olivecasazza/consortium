//! NodeSet algebra: parsing, folding, expansion, and set operations.
//!
//! Demonstrates the core `consortium::node_set` API — the Rust counterpart
//! of `ClusterShell.NodeSet`:
//!
//!   - `NodeSet::parse` / Display (fold back to bracket notation)
//!   - `expand` / `fold` free functions
//!   - union / intersection / difference / symmetric_difference
//!   - `update_str`, `split`, `slice`, `contains`, `index`, `get`
//!
//! Prerequisites: none — fully offline.
//!
//! Run with:
//!   cargo run -p consortium-examples --example nodeset_algebra

use consortium::node_set::{expand, fold, NodeSet};

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("Parse and fold");
    // Parse bracket notation; Display folds the set back to compact form.
    let ns = NodeSet::parse("node[01-05],login1")?;
    println!("parsed  : node[01-05],login1");
    println!("len     : {}", ns.len());
    println!("folded  : {ns}");

    banner("Expand / fold free functions");
    let nodes = expand("node[1-3]")?;
    println!("expand(\"node[1-3]\") -> {nodes:?}");
    println!("fold(\"node1,node2,node3\") -> {}", fold("node1,node2,node3")?);

    banner("Membership and indexing");
    println!("contains(\"node03\") : {}", ns.contains("node03"));
    println!("contains(\"node99\") : {}", ns.contains("node99"));
    println!("index(\"node03\")    : {:?}", ns.index("node03"));
    println!("get(0)             : {:?}", ns.get(0));
    println!("iter               : {:?}", ns.iter().collect::<Vec<_>>());

    banner("Set algebra");
    let a = NodeSet::parse("node[01-05]")?;
    let b = NodeSet::parse("node[04-08]")?;
    println!("a                   : {a}");
    println!("b                   : {b}");
    println!("union               : {}", a.union(&b));
    println!("intersection        : {}", a.intersection(&b));
    println!("difference (a - b)  : {}", a.difference(&b));
    println!("symmetric_difference: {}", a.symmetric_difference(&b));

    banner("update_str (in-place union with a pattern)");
    let mut growing = NodeSet::parse("node[01-05]")?;
    growing.update_str("node[06-07]")?;
    growing.update_str("login2")?;
    println!("after updates: {growing}");

    banner("split into N sub-nodesets");
    for (i, part) in NodeSet::parse("node[01-06]")?.split(2).iter().enumerate() {
        println!("split 2 [{i}]: {part}");
    }

    banner("slice (Python-style start/stop/step over sorted nodes)");
    let sliced = NodeSet::parse("node[01-10]")?.slice(None, None, 2)?;
    println!("node[01-10][::2] : {sliced}");
    let tail = NodeSet::parse("node[01-10]")?.slice(Some(-3), None, 1)?;
    println!("node[01-10][-3:] : {tail}");

    Ok(())
}
