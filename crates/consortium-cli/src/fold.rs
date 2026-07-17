//! nD-aware nodeset folding honoring a fold-axis constraint (`--axis`).
//!
//! Ports the ClusterShell pieces needed by upstream commit 6ed71ac (GH#356):
//!
//! - [`parse_fold_axis`] — port of `CLI/Utils.py::parse_fold_axis`, translating
//!   a 1-indexed command-line `--axis` value into 0-indexed axes suitable for
//!   `NodeSet.fold_axis` / `Defaults.fold_axis`.
//! - [`fold_nodes`] — port of `NodeSetBase.__str__` + `_iter_nd_pat` rendering
//!   on top of `RangeSetND` vector folding (`_fold_univariate` /
//!   `_fold_multivariate_merge`). The core `NodeSet` eagerly expands
//!   multidimensional patterns, so the nD-aware folding needed for gathered
//!   output display is reimplemented here from the raw node-name list.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use consortium::range_set::RangeSet;

/// Port of `CLI/Utils.py::parse_fold_axis`.
///
/// Translates a 1-indexed `--axis` value to 0-indexed axes:
///
/// - `"1"` / `"1-2"` / `"1,3"` → rangeset axes, `x - 1` for each `x > 0`
///   (axis `0` is ignored, like upstream).
/// - `"-1"` → negative axis index (only a single number supported),
///   resolved modulo the dimension count at fold time.
///
/// Returns the raw axis list; callers treat an *empty* list like Python's
/// empty tuple assigned to `DEFAULTS.fold_axis` (falsy → fold along all
/// axes).
///
/// # Errors
///
/// Returns a `RangeSet` parse error string for malformed rangeset input, or
/// an `invalid literal` style error for malformed negative input, mirroring
/// the upstream `RangeSetParseError` / `ValueError` split.
pub fn parse_fold_axis(axis: &str) -> Result<Vec<i64>, String> {
    if !axis.starts_with('-') {
        // axis are 1-indexed on the command line (0 ignored)
        let rs = RangeSet::parse(axis, None).map_err(|e| e.to_string())?;
        Ok(rs.intiter().filter(|x| *x > 0).map(|x| x - 1).collect())
    } else {
        // negative axis index (only single number supported)
        axis.parse::<i64>()
            .map(|v| vec![v])
            .map_err(|_| format!("invalid literal for int() with base 10: '{axis}'"))
    }
}

/// One parsed node name: a `%s`-placeholder template plus one `(value, pad)`
/// pair per digit run, mirroring `node_set.rs::parse_node_indices`.
fn scan_node(name: &str) -> (String, Vec<(i64, u32)>) {
    let mut template = String::new();
    let mut indices = Vec::new();

    let chars: Vec<char> = name.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let prefix_start = i;
        while i < len && !chars[i].is_ascii_digit() {
            i += 1;
        }
        template.push_str(&name[prefix_start..i]);

        let digit_start = i;
        while i < len && chars[i].is_ascii_digit() {
            i += 1;
        }
        let digits = &name[digit_start..i];

        if !digits.is_empty() {
            template.push_str("%s");
            let value: i64 = digits.parse().unwrap_or(0);
            let pad = if value != 0 {
                let stripped = digits.trim_start_matches('0');
                if digits.len() > stripped.len() {
                    digits.len() as u32
                } else {
                    0
                }
            } else if digits.len() > 1 {
                digits.len() as u32
            } else {
                0
            };
            indices.push((value, pad));
        }
    }

    (template, indices)
}

/// Fold a list of node names into compact bracket notation, honoring a
/// fold-axis constraint.
///
/// `fold_axis` is `None` to fold along all axes (upstream default), or
/// `Some(axes)` with 0-indexed axes (negative values resolved modulo the
/// dimension count), mirroring `NodeSet.fold_axis` semantics:
///
/// - 1D patterns fold only when `fold_axis` is `None` or contains an axis
///   `x` with `-1 <= x < 1`; otherwise they expand.
/// - nD patterns fold each axis in the resolved set and expand the others.
///
/// Patterns are emitted sorted alphabetically; elements within a pattern
/// follow `RangeSet` ordering, like the Python oracle.
pub fn fold_nodes<S: AsRef<str>>(names: &[S], fold_axis: Option<&[i64]>) -> String {
    // Group index vectors by template (patterns sort alphabetically).
    let mut groups: BTreeMap<String, BTreeSet<Vec<(i64, u32)>>> = BTreeMap::new();
    for name in names {
        let (template, indices) = scan_node(name.as_ref());
        groups.entry(template).or_default().insert(indices);
    }

    let mut results: Vec<String> = Vec::new();
    for (template, vectors) in &groups {
        let dim = vectors.iter().next().map_or(0, Vec::len);
        if dim == 0 {
            // non-indexed node name
            results.push(template.clone());
        } else if dim == 1 {
            results.extend(fold_1d(template, vectors, fold_axis));
        } else {
            results.extend(fold_nd(template, vectors, fold_axis));
        }
    }
    results.join(",")
}

/// Build a `RangeSet` from `(value, pad)` pairs.
fn rangeset_of<I: IntoIterator<Item = (i64, u32)>>(items: I) -> RangeSet {
    let mut rs = RangeSet::new();
    for (value, pad) in items {
        rs.add_int(value, pad);
    }
    rs
}

/// 1D fold rule, port of the `rset.dim() == 1` branch of
/// `NodeSetBase.__str__`.
fn fold_1d(
    template: &str,
    vectors: &BTreeSet<Vec<(i64, u32)>>,
    fold_axis: Option<&[i64]>,
) -> Vec<String> {
    let rs = rangeset_of(vectors.iter().map(|v| v[0]));

    // fold if fold_axis is None or contains x with -1 <= x < 1
    let may_fold = fold_axis.is_none()
        || fold_axis
            .map(|axes| axes.iter().any(|x| (-1..1).contains(x)))
            .unwrap_or(false);

    if may_fold {
        if rs.len() > 1 {
            vec![template.replacen("%s", &format!("[{rs}]"), 1)]
        } else {
            vec![template.replacen("%s", &rs.to_string(), 1)]
        }
    } else {
        rs.striter()
            .map(|s| template.replacen("%s", &s, 1))
            .collect()
    }
}

/// nD fold rule, port of `NodeSetBase._iter_nd_pat` on top of a folded
/// `RangeSetND` veclist.
fn fold_nd(
    template: &str,
    vectors: &BTreeSet<Vec<(i64, u32)>>,
    fold_axis: Option<&[i64]>,
) -> Vec<String> {
    let dim = vectors.iter().next().map_or(0, Vec::len);

    // Build the veclist: one vector of singleton RangeSets per node.
    let mut veclist: Vec<Vec<RangeSet>> = Vec::new();
    for ivec in vectors {
        let singletons: Vec<RangeSet> = ivec.iter().map(|&(v, p)| rangeset_of([(v, p)])).collect();
        if !veclist.contains(&singletons) {
            veclist.push(singletons);
        }
    }

    fold_veclist(&mut veclist);

    // Resolved fold set: [int(x) % dimcnt for x in fold_axis
    //                     if -dimcnt <= int(x) < dimcnt]
    let dimcnt = dim as i64;
    let fold_set: HashSet<usize> = match fold_axis {
        None => (0..dim).collect(),
        Some(axes) => axes
            .iter()
            .filter(|x| **x >= -dimcnt && **x < dimcnt)
            .map(|x| x.rem_euclid(dimcnt) as usize)
            .collect(),
    };

    let mut results = Vec::new();
    for rgvec in &veclist {
        // Per-axis string lists, then cross-product (last axis aggregated
        // slowest, like the Python oracle).
        let mut combos: Vec<Vec<String>> = vec![Vec::new()];
        for (axis, rs) in rgvec.iter().enumerate() {
            let strings: Vec<String> = if rs.len() > 1 {
                if fold_set.contains(&axis) {
                    vec![format!("[{rs}]")]
                } else {
                    rs.striter().collect()
                }
            } else {
                vec![rs.to_string()]
            };
            let mut next = Vec::new();
            for s in &strings {
                for combo in &combos {
                    let mut extended = combo.clone();
                    extended.push(s.clone());
                    next.push(extended);
                }
            }
            combos = next;
        }
        for combo in combos {
            let mut rendered = template.to_string();
            for part in &combo {
                rendered = rendered.replacen("%s", part, 1);
            }
            results.push(rendered);
        }
    }
    results
}

/// In-place nD folding, port of `RangeSetND._fold`: univariate fast path,
/// then the multivariate greedy merge.
fn fold_veclist(veclist: &mut Vec<Vec<RangeSet>>) {
    if veclist.len() > 1 && !fold_univariate(veclist) {
        fold_multivariate_merge(veclist);
    } else {
        sort_veclist(veclist);
    }
}

/// Univariate nD folding. Returns true when only one dimension varies and
/// the veclist was merged along it (port of `RangeSetND._fold_univariate`).
fn fold_univariate(veclist: &mut Vec<Vec<RangeSet>>) -> bool {
    let dim = veclist[0].len();
    let mut vardim = 0usize;
    let mut dimdiff = 0;
    if dim > 1 {
        for i in 0..dim {
            // Are all rangesets on this dimension the same?
            if veclist.iter().any(|vec| vec[i] != veclist[0][i]) {
                dimdiff += 1;
                if dimdiff > 1 {
                    break;
                }
                vardim = i;
            }
        }
    }
    let univar = dim == 1 || dimdiff == 1;
    if univar {
        for k in 1..veclist.len() {
            let rs = veclist[k][vardim].clone();
            veclist[0][vardim].update(&rs);
        }
        veclist.truncate(1);
    }
    univar
}

/// Multivariate nD folding, merge phase (port of
/// `RangeSetND._fold_multivariate_merge`): easy O(n) passes first, then one
/// full O(n^2) pass, merging vectors that differ in at most one position.
fn fold_multivariate_merge(veclist: &mut Vec<Vec<RangeSet>>) {
    let mut full = false; // try easy O(n) passes first
    let mut chg = true; // new pass (eg. after change on veclist)
    while chg {
        chg = false;
        sort_veclist(veclist); // sort veclist before new pass
        let mut index1 = 0;
        while index1 + 1 < veclist.len() {
            // compare items by couples, the idea being to merge vectors if
            // they differ only by one item
            let mut index2 = index1 + 1;
            while index2 < veclist.len() {
                match try_merge_pair(&veclist[index1], &veclist[index2]) {
                    Some(new_item) => {
                        // one change has been done: use this new item to
                        // compare with other
                        chg = true;
                        veclist[index1] = new_item;
                        veclist.remove(index2);
                    }
                    None => {
                        index2 += 1;
                        if !full {
                            // easy pass so break to avoid scanning all
                            // index2; advance with next index1 for now
                            break;
                        }
                    }
                }
            }
            index1 += 1;
        }
        if !chg && !full {
            // if no change was done during the last normal pass, we do a
            // full O(n^2) pass. This pass is done only at the end in the
            // hope that most vectors have already been merged by easy
            // O(n) passes.
            chg = true;
            full = true;
        }
    }
}

/// Try to merge two rangeset vectors into one, following
/// `_fold_multivariate_merge` per-position rules: equal positions are kept,
/// disjoint ranges are unioned, a fully contained range yields the largest
/// one, and a partial overlap aborts the merge. At most one position may
/// differ.
fn try_merge_pair(item1: &[RangeSet], item2: &[RangeSet]) -> Option<Vec<RangeSet>> {
    let mut new_item: Vec<RangeSet> = Vec::with_capacity(item1.len());
    let mut nb_diff = 0;
    for (rg1, rg2) in item1.iter().zip(item2.iter()) {
        if rg1 == rg2 {
            new_item.push(rg1.clone());
        } else if rg1.intersection(rg2).is_empty() {
            // merge on disjoint ranges
            nb_diff += 1;
            if nb_diff > 1 {
                return None;
            }
            new_item.push(rg1.union(rg2));
        } else if strict_superset(rg1, rg2) {
            // if fully contained, keep the largest one
            nb_diff += 1;
            if nb_diff > 1 {
                return None;
            }
            new_item.push(rg1.clone());
        } else if strict_superset(rg2, rg1) {
            nb_diff += 1;
            if nb_diff > 1 {
                return None;
            }
            new_item.push(rg2.clone());
        } else {
            // intersection but do nothing
            return None;
        }
    }
    Some(new_item)
}

/// Python's `rg1 > rg2` rangeset comparison: strict superset by length and
/// containment.
fn strict_superset(rg1: &RangeSet, rg2: &RangeSet) -> bool {
    rg1.len() > rg2.len() && rg1.intersection(rg2).len() == rg2.len()
}

/// nD sorting (port of `RangeSetND._sort`):
/// (1) larger vector first (#elements)
/// (2) larger dim first (#elements)
/// (3) lower first index first
/// (4) lower last index first
fn sort_veclist(veclist: &mut [Vec<RangeSet>]) {
    veclist.sort_by_key(|rgvec| rgvec_key(rgvec));
}

type RgvecKey = (
    std::cmp::Reverse<u128>,
    Vec<(std::cmp::Reverse<usize>, i64, i64)>,
);

fn rgvec_key(rgvec: &[RangeSet]) -> RgvecKey {
    let product: u128 = rgvec.iter().map(|rg| rg.len() as u128).product();
    let dims = rgvec
        .iter()
        .map(|rg| {
            let first = rg.intiter().next().unwrap_or(0);
            let last = rg.intiter().last().unwrap_or(first);
            (std::cmp::Reverse(rg.len()), first, last)
        })
        .collect();
    (std::cmp::Reverse(product), dims)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fold(names: &[&str], fold_axis: Option<&[i64]>) -> String {
        fold_nodes(names, fold_axis)
    }

    // ── parse_fold_axis (CLI/Utils.py port) ─────────────────────────────

    #[test]
    fn parse_axis_single() {
        assert_eq!(parse_fold_axis("1").unwrap(), vec![0]);
        assert_eq!(parse_fold_axis("2").unwrap(), vec![1]);
    }

    #[test]
    fn parse_axis_rangeset() {
        assert_eq!(parse_fold_axis("1-2").unwrap(), vec![0, 1]);
        assert_eq!(parse_fold_axis("1,3").unwrap(), vec![0, 2]);
    }

    #[test]
    fn parse_axis_zero_ignored() {
        // axis 0 is filtered out like upstream (x > 0)
        assert_eq!(parse_fold_axis("0").unwrap(), Vec::<i64>::new());
        assert_eq!(parse_fold_axis("0,2").unwrap(), vec![1]);
    }

    #[test]
    fn parse_axis_negative() {
        assert_eq!(parse_fold_axis("-1").unwrap(), vec![-1]);
        assert_eq!(parse_fold_axis("-2").unwrap(), vec![-2]);
    }

    #[test]
    fn parse_axis_errors() {
        assert!(parse_fold_axis("abc").is_err());
        assert!(parse_fold_axis("-x").is_err());
        assert!(parse_fold_axis("-").is_err());
    }

    // ── 1D folding ───────────────────────────────────────────────────────

    #[test]
    fn fold_1d_default() {
        assert_eq!(
            fold(&["node1", "node2", "node3", "node4"], None),
            "node[1-4]"
        );
    }

    #[test]
    fn fold_1d_axis1_folds() {
        // axis 1 → 0-indexed 0 → 1D pattern may fold
        assert_eq!(
            fold(&["node1", "node2", "node3", "node4"], Some(&[0])),
            "node[1-4]"
        );
    }

    #[test]
    fn fold_1d_axis_minus1_folds() {
        // -1 <= x < 1 → 1D pattern may fold
        assert_eq!(
            fold(&["node1", "node2", "node3", "node4"], Some(&[-1])),
            "node[1-4]"
        );
    }

    #[test]
    fn fold_1d_axis2_expands() {
        // axis 2 → 0-indexed 1 → no x in {-1, 0} → expand
        assert_eq!(
            fold(&["node1", "node2", "node3", "node4"], Some(&[1])),
            "node1,node2,node3,node4"
        );
    }

    #[test]
    fn fold_1d_single_element_no_brackets() {
        assert_eq!(fold(&["node3"], None), "node3");
    }

    #[test]
    fn fold_1d_padding_preserved() {
        assert_eq!(fold(&["prune003", "prune004"], None), "prune[003-004]");
    }

    #[test]
    fn fold_plain_nodes() {
        assert_eq!(fold(&["switch"], None), "switch");
        assert_eq!(fold(&["node1", "switch"], None), "node1,switch");
    }

    // ── nD folding (GH#356 / upstream --axis tests) ──────────────────────

    #[test]
    fn fold_nd_default_folds_all_axes() {
        let names = ["foo1-1", "foo1-2", "foo2-1", "foo2-2"];
        assert_eq!(fold(&names, None), "foo[1-2]-[1-2]");
    }

    #[test]
    fn fold_nd_axis1() {
        let names = ["foo1-1", "foo1-2", "foo2-1", "foo2-2"];
        assert_eq!(fold(&names, Some(&[0])), "foo[1-2]-1,foo[1-2]-2");
    }

    #[test]
    fn fold_nd_axis2() {
        let names = ["foo1-1", "foo1-2", "foo2-1", "foo2-2"];
        assert_eq!(fold(&names, Some(&[1])), "foo1-[1-2],foo2-[1-2]");
    }

    #[test]
    fn fold_nd_axis_minus1() {
        // -1 % 2 == 1 → same as axis=2
        let names = ["foo1-1", "foo1-2", "foo2-1", "foo2-2"];
        assert_eq!(fold(&names, Some(&[-1])), "foo1-[1-2],foo2-[1-2]");
    }

    #[test]
    fn fold_nd_axis_minus2() {
        // -2 % 2 == 0 → same as axis=1
        let names = ["foo1-1", "foo1-2", "foo2-1", "foo2-2"];
        assert_eq!(fold(&names, Some(&[-2])), "foo[1-2]-1,foo[1-2]-2");
    }

    #[test]
    fn fold_nd_out_of_range_axis_expands_everything() {
        // axis 3 on a 2D nodeset: filtered out (3 >= dimcnt) → no fold axis
        // (expansion order verified against the Python oracle)
        let names = ["foo1-1", "foo1-2", "foo2-1", "foo2-2"];
        assert_eq!(fold(&names, Some(&[2])), "foo1-1,foo2-1,foo1-2,foo2-2");
    }

    #[test]
    fn fold_nd_multivariate_keeps_larger_vector_first() {
        // da[1-2]p[1-3] (6 elts) precedes da[5-6]p[1-2] (4 elts)
        let names = [
            "da1p1", "da1p2", "da1p3", "da2p1", "da2p2", "da2p3", "da5p1", "da5p2", "da6p1",
            "da6p2",
        ];
        assert_eq!(fold(&names, None), "da[1-2]p[1-3],da[5-6]p[1-2]");
    }

    #[test]
    fn fold_nd_univariate_merges_along_varying_dim() {
        // only axis 1 varies → merge into a single vector
        let names = ["r1n1", "r1n2", "r1n3"];
        assert_eq!(fold(&names, None), "r1n[1-3]");
    }

    #[test]
    fn fold_nd_partial_overlap_does_not_merge() {
        // {1-2} x {1} and {2-3} x {2}: overlap on axis 0 without containment
        let names = ["a1b1", "a2b1", "a2b2", "a3b2"];
        assert_eq!(fold(&names, None), "a[1-2]b1,a[2-3]b2");
    }

    #[test]
    fn fold_nd_contained_range_keeps_largest() {
        // {1} x {1-2} and {2} x {1}: mergeable on neither axis without
        // a second difference (verified against the Python oracle)
        let names = ["a1b1", "a1b2", "a2b1"];
        assert_eq!(fold(&names, None), "a1b[1-2],a2b1");
    }

    #[test]
    fn fold_nd_3d_full_cartesian() {
        let names = [
            "a1b1c1", "a1b1c2", "a1b2c1", "a1b2c2", "a2b1c1", "a2b1c2", "a2b2c1", "a2b2c2",
        ];
        assert_eq!(fold(&names, None), "a[1-2]b[1-2]c[1-2]");
        // expansion order verified against the Python oracle
        assert_eq!(
            fold(&names, Some(&[2])),
            "a1b1c[1-2],a2b1c[1-2],a1b2c[1-2],a2b2c[1-2]"
        );
    }

    #[test]
    fn fold_nd_padding_preserved() {
        let names = ["n01-1", "n01-2", "n02-1", "n02-2"];
        assert_eq!(fold(&names, None), "n[01-02]-[1-2]");
    }

    #[test]
    fn fold_mixed_templates_sorted() {
        let names = ["node1", "bmc10", "node2", "bmc11"];
        assert_eq!(fold(&names, None), "bmc[10-11],node[1-2]");
    }
}
