# Examples

Practical, runnable examples for consortium — from core nodeset algebra to
full infrastructure-integration pipelines. Everything marked **offline** runs
without SSH hosts, a nix store, a scheduler, or a cloud account.

## Layout

| path | what it is |
|---|---|
| `rust/` | Rust library examples, compile-checked as cargo examples |
| `python/` | Python binding examples (`ClusterShell` package, drop-in API) |
| `cli/` | Walkthrough of the six `consortium-cli` binaries |
| `inventories/` | Sample fleet/inventory files shared by the examples |

## Running the Rust examples

The `examples/` directory is itself a workspace member
(`consortium-examples`, never published), so every Rust example is
compile-checked by `cargo build --examples` and runnable by name:

```
cargo build --examples -p consortium-examples
cargo run -p consortium-examples --example <name>
```

### Regular usage

| example | demonstrates | offline? |
|---|---|---|
| `nodeset_algebra` | `NodeSet` parse/fold/expand, union/intersection/difference/xor, `split`, `slice` | yes |
| `rangeset_basics` | `RangeSet` parse, `add_range`, set algebra, autostep folding | yes |
| `groups` | `StaticGroupSource` + `GroupResolver`: named groups composed with `NodeSet` algebra | yes |
| `fanout_local` | `Task` fan-out with the exec worker — one local command per node, `%h` substitution, retcode/output collection | yes |
| `fanout_ssh` | `SshWorker` scheduled on a `Task` — real parallel SSH with `SshOptions` | no — needs SSH hosts |
| `gather_outputs` | `MsgTree` — the dshbak/clubak primitive: group nodes by identical output | yes |
| `dag_pipeline` | `DagBuilder` (explicit graph) and `StageBuilder` (stages x resources), pools, `ErrorPolicy` partial-failure semantics | yes |

### Integrations

Each integration example runs the **full pipeline** against a
`ScriptedExecutor` — canned command outputs are played back and every command
that *would* have run is printed at the end. They all load
[`inventories/fleet.json`](inventories/fleet.json). Swap
`ScriptedExecutor` for `ProcessExecutor::new()` to run against real
infrastructure.

| example | integration | pipeline | offline? |
|---|---|---|---|
| `integration_nix_deploy` | `consortium-nix` | eval → build → copy → activate (per host, `ContinueIndependent`) + cascade variant | yes (scripted) |
| `integration_slurm` | `consortium-slurm` | build-job-env → copy-job-env → sbatch → sacct wait → collect | yes (scripted) |
| `integration_ansible` | `consortium-ansible` | build-env → copy-env → ansible-playbook per host | yes (scripted) |
| `integration_skypilot` | `consortium-skypilot` | build-sky-env → sky launch → sky down | yes (scripted) |
| `integration_ray` | `consortium-ray` | build-ray-env → ray job submit → ray job status wait | yes (scripted) |

The `ScriptedExecutor` / `Rule` API shown here is the same one used by the
contract test suite (`crates/consortium-integration-testkit`) — see any
integration's `tests/contract.rs` for the macro-generated semantic tests
every integration must pass.

## Python examples

The bindings in `crates/consortium-py` are imported as the `ClusterShell`
package (a drop-in replacement for upstream ClusterShell):

```
pip install crates/consortium-py        # or: cd crates/consortium-py && maturin develop
python3 examples/python/nodeset_demo.py
python3 examples/python/rangeset_demo.py
```

## CLI examples

See [cli/README.md](cli/README.md) for a full walkthrough of `claw` (clush),
`pinch` (nodeset), `molt` (clubak/dshbak), `cast` (fleet deployment),
`cascade-copy`, and `cascade-viz` — including which ones work offline.

## Inventories

- [`inventories/fleet.json`](inventories/fleet.json) — sample `FleetConfig`
  (3 NixOS nodes, 1 builder, plus `ansibleConfig` / `slurmConfig` /
  `rayConfig` / `skypilotConfig` sub-configs). Shared by all integration
  examples above and usable with `cast -c`.
- [`inventories/nixlab-safe.toml`](inventories/nixlab-safe.toml) — sample
  `cascade-copy` inventory (seed + nodes) for a real fleet.
