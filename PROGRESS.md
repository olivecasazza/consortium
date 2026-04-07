# Consortium Migration Progress

## Summary

| Metric                | Count |
|-----------------------|-------|
| Total Python tests    |   698 |
| Rust unit tests       |    43 |
| Modules in progress   |     1 |
| Progress              |  ~8%  |

## Per-Module Status

| Module         | Rust impl | PyO3 binding | Python tests | Rust tests | Status      |
|----------------|-----------|--------------|--------------|------------|-------------|
| RangeSet       | done      | partial      | 57           | 41         | in progress |
| RangeSetND     | stub      | -            | 23           | 0          | not started |
| NodeSet        | partial   | partial      | 125          | 1          | in progress |
| NodeSetGroup   | -         | -            | 76           | 0          | not started |
| MsgTree        | stub      | -            | 11           | 1          | not started |
| Defaults       | stub      | -            | 9            | 0          | not started |
| Event          | stub      | -            | 20           | 0          | not started |
| Topology       | stub      | -            | 25           | 0          | not started |
| Task           | stub      | -            | 46           | 0          | not started |
| Worker         | -         | -            | 85           | 0          | not started |
| Gateway        | stub      | -            | 34           | 0          | not started |
| CLI            | -         | -            | 135          | 0          | not started |
| Misc           | -         | -            | 4            | 0          | not started |

## RangeSet Detailed Coverage

The RangeSet Rust implementation covers the core functionality:

### Implemented
- **Parsing**: Full pattern parsing with comma-separated subranges, `start-stop/step` syntax
- **Padding**: Zero-padded ranges (`001-099`), mixed-width padding, padding mismatch errors
- **Display/Fold**: Canonical string output with autostep-based folding (step detection)
- **Set operations**: union, intersection, difference, symmetric_difference (+ `_update` variants)
- **Element operations**: add, remove, discard, contains, len, sorted, intiter, add_range
- **Properties**: autostep (get/set), padding

### 41 Rust tests mapping to Python RangeSetTest.py:
- test_simple, test_step_simple, test_step_advanced, test_step_advanced_more, test_step_more_complex
- test_bad_syntax (error cases)
- test_padding, test_padding_display, test_padding_property, test_padding_mismatch_errors
- test_mixed_padding, test_mixed_padding_with_different_widths
- test_folding (autostep-based step detection)
- test_intersection, test_intersection_step, test_intersection_complex, test_intersection_length
- test_union, test_union_complex, test_update_complex
- test_difference, test_difference_bounds, test_diff_step, test_diff_step_complex
- test_symmetric_difference, test_symmetric_difference_complex
- test_ior, test_iand, test_ixor, test_isub
- test_add_remove, test_remove_and_discard, test_remove_missing_panics
- test_contains, test_intiter, test_empty, test_large_range
- test_copy, test_autostep_property
- test_add_range_api, test_add_range_bounds

### Not yet ported from Python
- Indexing / slicing (`__getitem__`, `__setitem__`)
- `split()`, `contiguous()`, `dim()`, `slices()`
- `fromlist()`, `fromone()` constructors
- Iterator protocol (`__iter__`, `striter()`)
- Comparison operators (`__gt__`, `__lt__`, `issubset`, `issuperset`)
- Pickle/unpickle support
- `__hash__`

## Architecture

```
consortium/
  crates/
    consortium/          # Core Rust library
      src/
        range_set.rs     # RangeSet (1250+ lines, 41 tests) ← ACTIVE
        node_set.rs      # NodeSet (partial)
        msg_tree.rs      # MsgTree (stub)
        lib.rs           # Module exports
        ...              # Other stubs
    consortium-py/       # PyO3 bindings
      src/
        range_set.rs     # Python-facing RangeSet wrapper
        node_set.rs      # Python-facing NodeSet wrapper
      ClusterShell/      # Python compatibility shims
  tests/                 # Original Python test suite (oracle)
  flake.nix              # Nix flake (flake-parts + Crane)
```

## Legend

- Rust impl: `done` | `partial` | `stub` | `-`
- PyO3 binding: `done` | `partial` | `-`
- Status: `not started` | `in progress` | `done`

## Next Steps

1. Port remaining Python RangeSet tests (slicing, contiguous, split, iterators)
2. Wire up PyO3 bindings so original Python tests pass against Rust backend
3. Begin NodeSet implementation (depends on complete RangeSet)
4. MsgTree implementation (independent, small)
