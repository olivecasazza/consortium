# Test Harness Spec: Side-by-Side Python↔Rust Comparison

## Goals

1. **Upstream sync**: Pull latest tests from `cea-hpc/clustershell` without manual copy
2. **Side-by-side**: Run each Python test method against both backends (original Python, Rust via PyO3)
3. **Rust parity**: Run pure Rust tests that mirror the Python tests
4. **Single report**: GitHub Actions summary showing a matrix of test × backend → pass/fail/skip
5. **Regenerable**: A script scans all Python test methods and generates/updates the Rust test mapping

## Architecture

```
consortium/
├── .github/
│   └── workflows/
│       └── migration-scorecard.yml    ← runs on push/PR + manual
│
├── tests/                             ← git subtree or CI-fetched from upstream
│   ├── RangeSetTest.py
│   ├── NodeSetTest.py
│   └── ...
│
├── crates/
│   ├── consortium/                    ← pure Rust library
│   │   └── src/
│   │       └── range_set.rs           ← #[cfg(test)] mod tests { ... }
│   │
│   └── consortium-py/                 ← PyO3 bindings (maturin)
│       ├── ClusterShell/              ← drop-in Python shim
│       │   ├── __init__.py
│       │   ├── RangeSet.py            ← imports from _consortium (Rust)
│       │   └── _py/                   ← original pure-Python ClusterShell
│       │       ├── __init__.py
│       │       └── RangeSet.py        ← original Python impl
│       └── src/
│           └── range_set.rs           ← PyO3 wrapper
│
├── harness/
│   ├── sync_upstream_tests.sh         ← fetch tests from cea-hpc/clustershell
│   ├── generate_test_mapping.py       ← scan Python tests → TEST_MAPPING.toml
│   ├── run_comparison.py              ← run both backends, emit JUnit XML
│   └── render_summary.py             ← JUnit XML → GitHub Actions summary markdown
│
├── TEST_MAPPING.toml                  ← generated: Python test → Rust test(s) mapping
└── PROGRESS.md                        ← generated from mapping + results
```

## 1. Upstream Test Sync

`harness/sync_upstream_tests.sh`:
- Fetches a pinned tag/commit from `cea-hpc/clustershell`
- Copies `tests/` and `lib/ClusterShell/` into our repo
- The pinned ref is stored in `UPSTREAM_REF` file at repo root
- CI runs this at the start of the workflow so tests are always fresh
- We do NOT commit upstream tests — they're fetched at CI time
  (or we could use git subtree; TBD based on preference)

Wait — we already have tests/ committed from the original fork. Better approach:

- Keep tests/ committed (they came with the fork)
- `sync_upstream_tests.sh` updates them from upstream on demand
- A separate `UPSTREAM_REF` file tracks what we're pinned to
- CI can optionally run with `--latest` to test against upstream HEAD

## 2. Backend Switching

`ClusterShell/__init__.py` checks `CONSORTIUM_BACKEND` env var:

```python
import os
_backend = os.environ.get("CONSORTIUM_BACKEND", "rust")

if _backend == "python":
    # Use original pure-Python implementation
    import ClusterShell._py as _impl
elif _backend == "rust":
    # Use Rust via PyO3
    import ClusterShell._consortium as _impl
```

Each submodule (RangeSet.py, NodeSet.py, etc.) re-exports from the
selected backend.

## 3. Test Mapping Generation

`harness/generate_test_mapping.py`:
- Walks `tests/*Test.py`, uses `ast` module to extract every `def test*` method
- Groups by class (module)
- Outputs `TEST_MAPPING.toml`:

```toml
[upstream]
repo = "cea-hpc/clustershell"
ref = "v1.9.2"

[RangeSetTest]
# Python method = [list of Rust test names, or empty if not yet ported]
testSimple = ["range_set::tests::test_simple"]
testStepSimple = ["range_set::tests::test_step_simple"]
testGetItem = []  # not yet implemented
testGetSlice = []  # not yet implemented
```

- Also scans Rust test names from `cargo test -- --list` output
- Flags Rust tests that don't map to any Python test (extra coverage, fine)
- Flags Python tests with empty mapping (gaps)

## 4. Comparison Runner

`harness/run_comparison.py`:
- Runs pytest twice:
  1. `CONSORTIUM_BACKEND=python pytest tests/ --junit-xml=results-python.xml`
  2. `CONSORTIUM_BACKEND=rust pytest tests/ --junit-xml=results-rust.xml`
- Runs `cargo test -- --format=junit` (or cargo-nextest with JUnit output)
  → `results-cargo.xml`
- All three XML files are passed to the summary renderer

## 5. GitHub Actions Summary

`harness/render_summary.py`:
- Parses the three JUnit XML files
- Cross-references with TEST_MAPPING.toml
- Renders a markdown table to `$GITHUB_STEP_SUMMARY`:

```markdown
## Migration Scorecard

| Module | Python Tests | Python ✅ | Rust-backed ✅ | Pure Rust ✅ | Parity |
|--------|-------------|-----------|---------------|-------------|--------|
| RangeSet | 57 | 57 | 34 | 41 | 60% |
| NodeSet | 125 | 125 | 0 | 1 | 0% |
| MsgTree | 11 | 11 | 0 | 1 | 0% |
| **Total** | **698** | **698** | **34** | **43** | **~5%** |

<details>
<summary>RangeSet detailed results</summary>

| Python Test | Python (original) | Python (rust-backed) | Rust Unit Test |
|------------|-------------------|---------------------|----------------|
| testSimple | ✅ | ✅ | ✅ range_set::tests::test_simple |
| testGetItem | ✅ | ❌ NotImplemented | ⬜ not mapped |
| testGetSlice | ✅ | ❌ NotImplemented | ⬜ not mapped |

</details>
```

## 6. GitHub Actions Workflow

```yaml
name: Migration Scorecard
on:
  push:
    branches: [master]
  pull_request:
  workflow_dispatch:
    inputs:
      upstream_ref:
        description: 'Upstream ref to test against (default: pinned)'
        required: false

jobs:
  scorecard:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable

      - name: Install Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install deps
        run: pip install pytest pytest-timeout maturin

      - name: Sync upstream tests (optional)
        run: bash harness/sync_upstream_tests.sh

      - name: Build PyO3 bindings
        run: cd crates/consortium-py && maturin develop

      - name: Run Rust tests
        run: cargo test -- --format=junit > results-cargo.xml 2>&1 || true

      - name: Run Python tests (original backend)
        run: CONSORTIUM_BACKEND=python pytest tests/ -v --junit-xml=results-python.xml || true

      - name: Run Python tests (Rust backend)
        run: CONSORTIUM_BACKEND=rust pytest tests/ -v --junit-xml=results-rust.xml || true

      - name: Generate scorecard
        run: python harness/render_summary.py >> $GITHUB_STEP_SUMMARY
```

## Open Questions

1. Should we commit tests/ or fetch at CI time?
   → Recommend: keep committed (fork history), sync script updates them
2. Git subtree vs manual copy for upstream sync?
   → Recommend: simple curl/tar of tests/ dir from a pinned ref
3. Should the workflow fail if parity regresses?
   → Yes — track "minimum parity %" in a config, fail if it drops
