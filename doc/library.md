# Using the Rust library

The runnable examples live in [`examples/rust/`](../examples/rust/), a
workspace member (`consortium-examples`, never published) that CI compiles
with every build:

```sh
cargo run -p consortium-examples --example <name> [-- ARGS]
```

## Fleet execution

| example | demonstrates | runs against |
|---|---|---|
| `fanout_ssh` | `SshWorker` on a `Task`: parallel SSH, per-host return codes and output | SSH hosts given as `NODES`; CI runs it via [`examples/ssh/walkthrough.sh`](../examples/ssh/walkthrough.sh) |
| `fanout_local` | `Task` fan-out with the exec worker: one local command per node, `%h` substitution | this machine |
| `gather_outputs` | `MsgTree`, the clubak/dshbak primitive: group hosts by identical output | in-process |
| `dag_pipeline` | `DagBuilder` and `StageBuilder` (stages × resources), pools, `ErrorPolicy` partial failure | this machine |

## Node sets and groups

| example | demonstrates |
|---|---|
| `nodeset_algebra` | `NodeSet` parse/fold/expand, union/intersection/difference/xor, `split`, `slice` |
| `rangeset_basics` | `RangeSet` parse, `add_range`, set algebra |
| `groups` | `StaticGroupSource` + `GroupResolver`: named groups composed with `NodeSet` algebra |

## Integrations

Each integration example runs its full pipeline against a `ScriptedExecutor`,
which plays back canned command output and prints every command that would
have run. They load [`examples/inventories/fleet.json`](../examples/inventories/fleet.json).
Swap `ScriptedExecutor` for `ProcessExecutor::new()` to run against real
infrastructure.

| example | integration | pipeline |
|---|---|---|
| `integration_nix_deploy` | `consortium-nix` | eval → build → copy → activate per host (`ContinueIndependent`), plus the cascade variant |
| `integration_slurm` | `consortium-slurm` | build-job-env → copy-job-env → sbatch → sacct wait → collect |
| `integration_ansible` | `consortium-ansible` | build-env → copy-env → ansible-playbook per host |
| `integration_skypilot` | `consortium-skypilot` | build-sky-env → sky launch → sky down |
| `integration_ray` | `consortium-ray` | build-ray-env → ray job submit → ray job status wait |

The `ScriptedExecutor` / `Rule` API is the one the contract suite
(`crates/consortium-integration-testkit`) uses; see any integration's
`tests/contract.rs` and [Integrations](integrations.md).

API reference: `cargo doc --workspace --no-deps`, published to GitHub Pages
by the Docs workflow.
