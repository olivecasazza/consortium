# Consortium Migration Progress

## Migration DAG

![Migration DAG](migration-dag.svg)

## Summary

| Metric                | Count |
|-----------------------|-------|
| Total Python tests    |   582 |
| Mapped to Rust        |    37 |
| Rust unit tests       |    43 |
| Extra Rust tests      |    27 |
| Modules complete      |     1 |
| Modules remaining     |    13 |

## Per-Module Status

| Module         | Tier | Deps                                      | Rust impl | PyO3 | Status      |
|----------------|------|-------------------------------------------|-----------|------|-------------|
| RangeSet       | 0    | —                                         | done      | done | ✅ complete |
| MsgTree        | 1    | —                                         | stub      | —    | not started |
| Event          | 1    | —                                         | stub      | —    | not started |
| Defaults       | 1    | —                                         | stub      | —    | not started |
| NodeUtils      | 1    | —                                         | stub      | —    | not started |
| Engine         | 1    | —                                         | stub      | —    | not started |
| NodeSet        | 2    | RangeSet, Defaults, NodeUtils             | stub      | —    | not started |
| Communication  | 2    | Event                                     | stub      | —    | not started |
| Worker         | 2    | Engine                                    | stub      | —    | not started |
| Topology       | 3    | NodeSet                                   | stub      | —    | not started |
| Propagation    | 3    | Communication, Defaults, NodeSet, Topology| stub      | —    | not started |
| Task           | 4    | most modules                              | stub      | —    | not started |
| Gateway        | 4    | Comm, Event, NodeSet, Task, Engine, Worker| stub      | —    | not started |
| CLI            | 5    | Task, Gateway, NodeSet                    | —         | —    | not started |

## Tier Execution Order

Modules within the same tier can be worked in parallel.

- **Tier 0** — RangeSet ✅
- **Tier 1** — MsgTree, Event, Defaults, NodeUtils, Engine (all independent, 0 deps)
- **Tier 2** — NodeSet, Communication, Worker (depend on tier 0+1)
- **Tier 3** — Topology, Propagation (depend on tier 2)
- **Tier 4** — Task, Gateway (depend on most modules)
- **Tier 5** — CLI (final layer)

## RangeSet (Complete)

- 1241 lines Rust, 43 unit tests, 57 Python parity tests at 100%
- PyO3 bindings working via `maturin develop`
- Backend switching via `CONSORTIUM_BACKEND=rust|python`

### Implemented
- Parsing, padding, display/fold, autostep
- Set operations: union, intersection, difference, symmetric_difference
- Element operations: add, remove, discard, contains, len, sorted, intiter, add_range

### Not yet ported
- Indexing / slicing (`__getitem__`, `__setitem__`)
- `split()`, `contiguous()`, `dim()`, `slices()`
- `fromlist()`, `fromone()` constructors
- Comparison operators (`__gt__`, `__lt__`, `issubset`, `issuperset`)
- Pickle/unpickle, `__hash__`

## Architecture

```
consortium/
  crates/
    consortium/          # Core Rust library
      src/
        range_set.rs     # RangeSet (1241 lines, 43 tests) ✅
        node_set.rs      # NodeSet (stub)
        msg_tree.rs      # MsgTree (stub)
        ...              # Other stubs
    consortium-py/       # PyO3 bindings
      src/
        range_set.rs     # Python-facing RangeSet wrapper
      ClusterShell/      # Python compatibility shims (backend switching)
  harness/               # Migration test harness
    generate_test_mapping.py
    run_comparison.py
    render_summary.py
    cargo_to_junit.py
    sync_upstream_tests.sh
  tests/                 # Original Python test suite (oracle)
  TEST_MAPPING.toml      # 582 Python tests tracked
  flake.nix              # Nix flake (flake-parts + Crane)
```

## Branch Strategy

Stacked feature branches merging back to master:

```
master
  └── feat/pyo3-binding-fixes
        └── feat/migration-harness
              └── feat/<next-module> ...
```
