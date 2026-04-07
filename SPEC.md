# Consortium - Spec

A Rust rewrite of [ClusterShell](https://github.com/cea-hpc/clustershell) with
Python bindings (PyO3) for drop-in compatibility.

## Goals

1. Rewrite ClusterShell's core library in Rust
2. Expose Python bindings via PyO3/maturin so the original test suite runs unchanged
3. Measure migration progress by counting passing Python tests against Rust-backed modules
4. Ship both a Rust crate and a Python wheel

## Architecture

```
consortium/
├── flake.nix                    # Nix flake (flake-parts + snowfall + crane)
├── flake.lock
├── Cargo.toml                   # Workspace root
├── Cargo.lock
├── SPEC.md                      # This file
├── PROGRESS.md                  # Migration progress tracker
│
├── crates/
│   ├── consortium/              # Core Rust library
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── range_set.rs     # ← ClusterShell.RangeSet
│   │       ├── node_set.rs      # ← ClusterShell.NodeSet
│   │       ├── node_utils.rs    # ← ClusterShell.NodeUtils
│   │       ├── msg_tree.rs      # ← ClusterShell.MsgTree
│   │       ├── defaults.rs      # ← ClusterShell.Defaults
│   │       ├── event.rs         # ← ClusterShell.Event
│   │       ├── task.rs          # ← ClusterShell.Task
│   │       ├── topology.rs      # ← ClusterShell.Topology
│   │       ├── communication.rs # ← ClusterShell.Communication
│   │       ├── propagation.rs   # ← ClusterShell.Propagation
│   │       ├── gateway.rs       # ← ClusterShell.Gateway
│   │       └── engine/          # ← ClusterShell.Engine.*
│   │           ├── mod.rs
│   │           └── ...
│   │
│   └── consortium-py/           # PyO3 bindings crate
│       ├── Cargo.toml
│       ├── pyproject.toml       # maturin config
│       └── src/
│           ├── lib.rs           # #[pymodule] entry point
│           ├── range_set.rs     # PyO3 wrappers for RangeSet
│           ├── node_set.rs      # PyO3 wrappers for NodeSet
│           └── ...
│
├── lib/ClusterShell/            # Original Python source (reference)
├── tests/                       # Original Python tests (acceptance criteria)
│
├── conf/                        # Original config files
└── doc/                         # Original documentation
```

## Module Dependency Order (bottom-up)

This is the implementation order -- each module only depends on those above it:

```
Layer 0 (no deps):     RangeSet, MsgTree, Event, Defaults
Layer 1 (RangeSet):    NodeUtils, NodeSet (depends on RangeSet, Defaults, NodeUtils)
Layer 2 (NodeSet):     Topology (depends on NodeSet)
Layer 3:               Communication (depends on Event)
Layer 4:               Propagation (depends on NodeSet, Communication, Topology, Defaults)
Layer 5:               Engine (depends on Event)
Layer 6:               Worker (depends on Engine, Event)
Layer 7:               Task (depends on everything)
Layer 8:               Gateway (depends on everything)
Layer 9:               CLI tools (clush, clubak, cluset/nodeset)
```

## Test Strategy: Side-by-Side

### Two test suites running in parallel:

**Rust tests** (`cargo test`)
- Unit tests written TDD-style during development
- Live inside each crate under `#[cfg(test)]`
- Fast, run on every change
- These are the "development" tests

**Python tests** (`pytest tests/`)
- The original ClusterShell test suite, UNCHANGED
- Run against the PyO3 bindings (`consortium-py`)
- These are the "acceptance" tests
- A Python test passing = verified behavioral parity with ClusterShell

### Progress measurement:

```
PROGRESS = (passing python tests) / (total python tests) * 100
```

Per-module breakdown tracked in PROGRESS.md.

### Test files → module mapping:

| Python Test File          | Tests | Rust Module    | Priority |
|---------------------------|-------|----------------|----------|
| RangeSetTest.py           |    57 | range_set      | 1        |
| RangeSetNDTest.py         |    23 | range_set (ND) | 2        |
| NodeSetTest.py            |   125 | node_set       | 3        |
| NodeSetGroupTest.py       |    76 | node_set       | 4        |
| MsgTreeTest.py            |    11 | msg_tree       | 5        |
| DefaultsTest.py           |     9 | defaults       | 6        |
| TreeTopologyTest.py       |    25 | topology       | 7        |
| TaskEventTest.py          |    20 | task/event     | 8        |
| TaskTimerTest.py          |    22 | task           | 9        |
| TaskMsgTreeTest.py        |    10 | task           | 10       |
| TaskLocalTest.py          |     * | task           | 11       |
| TaskPortTest.py           |     4 | task           | 12       |
| TaskRLimitsTest.py        |     6 | task           | 13       |
| TaskTimeoutTest.py        |     1 | task           | 14       |
| TaskThreadJoinTest.py     |     7 | task           | 15       |
| TaskThreadSuspendTest.py  |     2 | task           | 16       |
| WorkerExecTest.py         |    15 | worker         | 17       |
| StreamWorkerTest.py       |    10 | worker         | 18       |
| TreeWorkerTest.py         |    60 | worker/tree    | 19       |
| TreeGatewayTest.py        |    34 | gateway        | 20       |
| TreeTaskTest.py           |     3 | task/tree      | 21       |
| TaskDistantTest.py        |     * | task           | 22       |
| TaskDistantPdshTest.py    |     * | task           | 23       |
| CLIClushTest.py           |    49 | cli            | 24       |
| CLIClubakTest.py          |    11 | cli            | 25       |
| CLINodesetTest.py         |    51 | cli            | 26       |
| CLIConfigTest.py          |    13 | cli            | 27       |
| CLIDisplayTest.py         |     7 | cli            | 28       |
| CLIOptionParserTest.py    |     4 | cli            | 29       |
| MisusageTest.py           |     4 | misc           | 30       |

(* = test count via mixins, not directly in file)

## Nix Build System

### Inputs:
- `nixpkgs` (nixpkgs-unstable)
- `flake-parts` (modular flake composition)
- `snowfall-lib` (project structure convention)
- `crane` (Rust/Nix integration)
- `rust-overlay` (Rust toolchain management)
- `git-hooks-nix` (pre-commit hooks)

### Outputs:
- `packages.consortium` -- the Rust library (static/dynamic)
- `packages.consortium-py` -- the Python wheel (maturin + PyO3)
- `packages.default` -- consortium-py (the user-facing artifact)
- `checks.cargo-test` -- `cargo test` (Rust unit tests)
- `checks.python-test` -- `pytest tests/` against Rust bindings
- `checks.cargo-clippy` -- linting
- `checks.cargo-fmt` -- formatting
- `devShells.default` -- dev environment with Rust toolchain, Python, pytest

### Dev shell provides:
- Rust nightly (for PyO3)
- Python 3.12+ with pytest, PyYAML
- maturin (build PyO3 wheels)
- cargo-watch, cargo-nextest
- The original ClusterShell Python source on PYTHONPATH (for reference)

## PyO3 Binding Strategy

The `consortium-py` crate produces a Python module named `ClusterShell` so the
original test imports work unchanged:

```python
# This import in the tests:
from ClusterShell.RangeSet import RangeSet
# ... resolves to our Rust-backed PyO3 module
```

### Incremental binding approach:

1. Start with stubs that raise `NotImplementedError`
2. As each Rust module is implemented, wire up the real binding
3. Track which tests go from `NotImplementedError` → PASS

### PyO3 module structure:

```python
ClusterShell/              # Python package (built by maturin)
├── __init__.py
├── RangeSet.py            # Re-exports from _consortium_rs
├── NodeSet.py
├── MsgTree.py
├── ...
└── _consortium_rs.so      # The compiled Rust extension
```

Alternatively, a single `_consortium_rs` extension module with submodules,
plus thin Python wrapper files that re-export for API compatibility.

## Development Workflow

### TDD Cycle (per module):

1. Read the Python source to understand behavior
2. Read the Python tests to understand expected behavior
3. Write a failing Rust test
4. Implement minimal Rust code to pass
5. Refactor
6. Wire up PyO3 binding
7. Run Python tests against binding
8. Update PROGRESS.md

### Commands:

```bash
# Rust TDD cycle
cargo test -p consortium                    # all rust tests
cargo test -p consortium range_set          # just range_set tests
cargo watch -x 'test -p consortium'         # watch mode

# Python acceptance tests
maturin develop -m crates/consortium-py/Cargo.toml
pytest tests/RangeSetTest.py -v             # specific module
pytest tests/ -v                            # full suite

# Nix checks (CI)
nix flake check                             # runs everything
nix build .#consortium-py                   # build the wheel
```

## Non-Goals (for now)

- CLI tools (clush, clubak, cluset) -- these come last
- Performance benchmarks -- correctness first
- Async runtime -- match ClusterShell's threading model first
- Windows support

## Open Questions

- Should we use Rust nightly or stable? (PyO3 works with stable now)
- Thread safety: ClusterShell uses threading -- do we want Send+Sync on core types?
- Error types: map Python exceptions 1:1 or design idiomatic Rust errors?
