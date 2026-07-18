//! RangeSet basics: numeric range sets with folding and autostep.
//!
//! Demonstrates `consortium::range_set::RangeSet` — the Rust counterpart of
//! `ClusterShell.RangeSet`, and the engine underneath NodeSet's bracket
//! notation:
//!
//!   - `RangeSet::parse` with an optional autostep threshold
//!   - `add_range` (half-open `[start, stop)` with step and zero-padding)
//!   - union / intersection / difference / symmetric_difference
//!   - iteration via `striter` / `intiter` / `sorted`
//!   - autostep folding (`0-8/2`-style step detection)
//!
//! Note: the Python bindings expose `RangeSet.contiguous()` (iterate over
//! contiguous sub-sets); the Rust core does not — use `striter()`/`sorted()`
//! here.
//!
//! Prerequisites: none — fully offline.
//!
//! Run with:
//!   cargo run -p consortium-examples --example rangeset_basics

use consortium::range_set::RangeSet;

fn banner(title: &str) {
    println!("\n=== {title} ===");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    banner("Parse and display (folded form)");
    // The second argument is the autostep threshold: None disables step folding.
    let rs = RangeSet::parse("1-4,6,8-12", None)?;
    println!("parsed  : 1-4,6,8-12");
    println!("len     : {}", rs.len());
    println!("folded  : {rs}");
    println!("sorted  : {:?}", rs.sorted());

    banner("add_range — half-open [start, stop) with step and padding");
    let mut built = RangeSet::new();
    built.add_range(0, 10, 2, 0); // 0,2,4,6,8
    println!("add_range(0, 10, 2, 0) : {built}");
    let mut padded = RangeSet::new();
    padded.add_range(1, 4, 1, 3); // 001,002,003
    println!("add_range(1, 4, 1, 3) : {padded}");

    banner("Set algebra");
    let a = RangeSet::parse("1-10", None)?;
    let b = RangeSet::parse("5-15", None)?;
    println!("a                   : {a}");
    println!("b                   : {b}");
    println!("union               : {}", a.union(&b));
    println!("intersection        : {}", a.intersection(&b));
    println!("difference (a - b)  : {}", a.difference(&b));
    println!("symmetric_difference: {}", a.symmetric_difference(&b));

    banner("Iteration — strings, integers, membership");
    let rs = RangeSet::parse("1-5", None)?;
    println!("striter        : {:?}", rs.striter().collect::<Vec<_>>());
    println!("intiter        : {:?}", rs.intiter().collect::<Vec<_>>());
    println!("contains_str(3): {}", rs.contains_str("3"));
    println!("contains_int(9): {}", rs.contains_int(9));

    banner("Autostep — fold 0,2,4,...,16 as 0-16/2");
    // autostep = Some(3): fold stepped ranges of 3+ elements.
    let stepped = RangeSet::parse("0,2,4,6,8,10,12,14,16", Some(3))?;
    println!("with autostep=3 : {stepped}");
    let plain = RangeSet::parse("0,2,4,6,8,10,12,14,16", None)?;
    println!("without autostep: {plain}");

    Ok(())
}
