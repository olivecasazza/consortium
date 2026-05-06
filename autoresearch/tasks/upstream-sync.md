# Task template: upstream-sync

Used for porting one cea-hpc/clustershell upstream commit (or short range)
into both the Python `lib/` and, where applicable, the Rust port.

## Frontmatter

```yaml
type: upstream-sync
upstream_sha: <sha>
upstream_subject: <commit subject from upstream>
files_touched: [<list of lib/ paths the upstream commit changed>]
acceptance:
  - python lib/ files updated to match upstream semantics
  - pytest tests/ -v passes
  - if Rust port has equivalent code: ported (and `cargo nextest run` passes)
  - if Rust port differs structurally: a follow-up task is filed at
    autoresearch/queue/pending/parity-<sha>.task.toml
  - UPSTREAM_REF bumped only when the full range is integrated (multi-task)
```

## Body

The harness pre-fetches `upstream/master` (cea-hpc/clustershell) into the
worktree. To see the diff:

```sh
git show <upstream_sha> -- lib/
```

Steps:
1. Apply the patch to `lib/` — prefer `git cherry-pick -n -X theirs
   <upstream_sha>` if the touched paths haven't diverged, else apply
   manually.
2. Run `pytest tests/`. Fix or skip mismatched tests one at a time.
   Don't blanket-skip.
3. For each `lib/` file changed, grep `crates/` for an analogous Rust
   module:
   - `lib/ClusterShell/RangeSet.py` ↔ `crates/consortium-crate/src/rangeset.rs`
   - `lib/ClusterShell/NodeSet.py`  ↔ `crates/consortium-crate/src/nodeset.rs`
   - `lib/ClusterShell/Task.py`     ↔ `crates/consortium-crate/src/task.rs`
   - `lib/ClusterShell/Worker/*.py` ↔ `crates/consortium-crate/src/worker*.rs`
   - `lib/ClusterShell/Gateway.py`  ↔ `crates/consortium-crate/src/gateway.rs`
4. If the Rust analogue exists and the upstream change is straightforward
   (bug fix, new edge case), port it. If structurally different (new
   feature, large refactor), file a follow-up parity task and stop.

Do **not** bump `UPSTREAM_REF` in this task unless the queue is empty
of all `upstream-sync` tasks — that's a separate concern.
