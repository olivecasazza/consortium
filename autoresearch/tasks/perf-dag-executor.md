# Task template: perf-dag-executor

The agent edits `crates/consortium/src/dag/*.rs` to make the DAG
executor faster. Measured by the `dag_executor/flat/33` criterion bench
landed in [olivecasazza/consortium#4].

This task type is the consortium-autoresearch port of karpathy's
`autoresearch/program.md` discipline: a single quantitative scalar
(`flat_33_ns`, lower=better), a fixed-time eval (criterion `--quick`,
2-15 s), and a strict keep/discard gate. The score gate (`score.sh`)
reads `flat_33_ns` from `target/criterion/dag_executor/flat/33/new/estimates.json`
and compares against the baseline in `autoresearch/.baseline.json`.

## Frontmatter (filled per task)

```yaml
type: perf-dag-executor
target_file: crates/consortium/src/dag/<X>.rs
target_line: <N>            # optional — point at the hot path
description: <one-line summary of the proposed optimization>
acceptance:
  - flat_33_ns improves by >= 5% vs baseline (current: ~360_000 ns on
    seir; baseline file: autoresearch/.baseline.json)
  - chain_33_ns does not regress > 10%
  - cargo nextest run --workspace passes (no test count regressions)
  - cargo clippy clean (no new warnings vs baseline)
  - cargo fmt --check passes
needs:
  - read crates/consortium/src/dag/executor.rs first
  - read crates/consortium/src/dag/{graph,builder,types,pool}.rs as needed
  - benches/dag_executor.rs IS THE SCORING HARNESS — do not modify
```

## Body

The DAG executor (`crates/consortium/src/dag/executor.rs`, 1069 lines)
schedules `DagTask` instances respecting a dependency graph + concurrency
groups. The flat/33 microbench measures the dispatch loop's overhead
with no real work (`FnTask`s that just return Success).

The plausible wins live in:

- **The dispatch loop** (executor.rs lines 80-257): ready-queue
  management, deferred-task draining on completion, group-throttling
  HashMap lookups inside the hot loop.
- **Data-structure choice**: `VecDeque<TaskId>` for the ready queue;
  `HashMap<&str, usize>` for concurrency-group active counts. Both have
  cheap alternatives (e.g. SmallVec for the ready queue at small N,
  cached group-cap pointers).
- **Allocation**: any per-task `String` formatting / cloning in hot
  paths.

Don't propose:
- Adding new dependencies to `crates/consortium/Cargo.toml`. Use what's
  there (`slotmap`, `thiserror`, std).
- Architectural rewrites (replacing the executor with rayon, tokio,
  etc.). The bench will probably stay flat or regress; the gate's
  test-count check will likely break.
- Edits to `crates/consortium/benches/dag_executor.rs` — that's the
  scoring harness; modifying it is invalid (the score gate will refuse).
- Edits to `crates/consortium/tests/*.rs` — tests are correctness
  signals; if a test breaks, your edit is wrong, not the test.

## Scoring contract

`score.sh` runs (in addition to the standard fmt/clippy/test gates):

```bash
timeout 60 cargo bench -p consortium-crate --bench dag_executor -- \
  flat/33 chain/33 --quick > "$TMP/bench.log" 2>&1
```

Then it reads:

```
target/criterion/dag_executor/flat/33/new/estimates.json
target/criterion/dag_executor/chain/33/new/estimates.json
```

via `jq -r '.mean.point_estimate'`. The gate passes iff:

- `flat_33_ns(branch) <= flat_33_ns(baseline) * 0.95`
  (i.e., at least 5% faster — within criterion's ~5% CV is noise,
  doesn't count)
- `chain_33_ns(branch) <= chain_33_ns(baseline) * 1.10`
  (chain didn't regress more than 10%)

The simplicity criterion still applies: trivial (1-2 ns) wins via
unsafe / ugly code → discard. Wins via deletion → keep regardless of
magnitude.

## Hint files to read before editing

- `crates/consortium/src/dag/executor.rs` (the file you'll edit)
- `crates/consortium/src/dag/types.rs` — `TaskId`, `DagTask`, `FnTask`
- `crates/consortium/src/dag/pool.rs` — `WorkerPool` resource gating
- `crates/consortium/benches/dag_executor.rs` — what's being measured
  (DO NOT modify)

## Why this is autoresearch-shaped

karpathy's program.md says: ONE editable file (`train.py`), ONE eval
function (`evaluate_bpb`), ONE scalar (`val_bpb`), fixed time budget,
keep/discard, NEVER STOP. We have:

| karpathy | here |
|---|---|
| `train.py` | `crates/consortium/src/dag/*.rs` |
| `prepare.py` (off-limits) | `crates/consortium/benches/dag_executor.rs` + the test suite |
| `val_bpb` | `flat_33_ns` |
| 5 min training | 2-15 s `--quick` bench |
| `results.tsv` | `autoresearch/results.tsv` (this repo's existing log) |
| `autoresearch/<tag>` branch | `agent/<id>/perf-dag-<task-id>` (this repo's standard worktree convention) |

The single difference: the existing task-queue model means each task is
ONE ITERATION of karpathy's loop, not the whole loop. The accountant +
mayor handle the "next iteration" decision via current-recommendations
+ task seeding.
