# Integrations

Consortium replaces ClusterShell and adds scheduler and infrastructure
integrations on top. Every integration is built on the DAG executor and a
shared command-execution abstraction (`consortium-integration::Executor`), so
each one is testable without real infrastructure — tests script the executor
instead of touching live clusters, clouds, or a Nix store.

| Crate | Purpose | Pipeline phases | Entry point |
| ----- | ------- | --------------- | ----------- |
| `consortium-nix` | NixOS/nix-darwin fleet deployment (colmena replacement) | eval → build → copy → activate (+ cascade P2P copy) | `deploy` / `deploy_with_cascade` |
| `consortium-slurm` | Slurm job submission with nix-built hermetic envs | build → copy → submit (sbatch) → wait (sacct) [→ collect] | `submit_job` |
| `consortium-ansible` | Playbook runs with a nix-built ansible env | build-env → copy-env → run-playbook | `run_playbook` |
| `consortium-skypilot` | Multi-cloud clusters via SkyPilot | build → launch [→ down] | `launch_task` |
| `consortium-ray` | Ray job submission | build → submit [→ wait] | `submit_job` |

## Shared integration grammar

`cast integration <adapter>` exposes the Rust adapter contract through one
CLI vocabulary. Run `cast integration --help` for the registry and
`cast integration <adapter> --help` for supported parameters.

```sh
cast --config fleet.json --format json integration nix --plan --on node[01-02]
cast --config fleet.json integration nix --action build --fanout 4
cast --config fleet.json integration ansible --playbook site.yml --environment ansible --check true
cast --config fleet.json integration slurm --name batch --script 'hostname' --wait false
cast --config fleet.json integration ray --name train --entrypoint 'python train.py' --timeout 300
cast --config fleet.json integration skypilot --name training --task 'run: hostname' --teardown false
```

Each adapter implements `consortium_integration::grammar::Integration`.
The CLI registry is generated from its manifest metadata; the Nix enrollment
gate requires every integration adapter to be registered.
Its grammar generates CLI arguments and validates programmatic `Request`
values. `plan` returns selected hosts, defaulted parameters, and phases without
receiving an executor. `run` compiles those phases into the existing DAG
scheduler, enforces each phase's declared effects, and reports task and resource
outcomes. Altered plans and changed fleet configurations fail before execution.
Temporary files, clocks, sleeps, diagnostics, and commands use the injected
executor; scripted execution keeps files and time virtual. Cleanup errors remain
visible in the report.

Host selectors (`--on`, `--tag`) apply to Nix and Ansible. Job and cluster
adapters reject them. Boolean parameters take explicit `true` or `false`;
unsupported options fail rather than being silently ignored. Existing
`cast` deployment commands and typed Rust entry points use the same grammar
execution path, including Nix cascade distribution. Durations accept exact
decimal seconds. Nix arguments accept whitespace-separated words or JSON arrays
when individual arguments contain spaces.

The parameterized grammar suite lives in consortium-tests alongside the
upstream parity and external integration suites. The design and acceptance
criteria are in [.specs/draft/integration-grammar.md](../.specs/draft/integration-grammar.md).

## Contract test suite

Every integration implements `consortium_integration_testkit::Contract` and
invokes the `integration_contract_tests!` macro in its `tests/contract.rs`.
The macro generates seven identical semantic tests per integration:

- `contract_missing_config_errors`
- `contract_plan_has_no_side_effects`
- `contract_happy_path`
- `contract_first_phase_failure_aborts_pipeline`
- `contract_mid_pipeline_failure_cancels_dependents`
- `contract_partial_host_failure_continues_independents`
- `contract_option_variants_skip_declared_phases`

To add a new integration:

1. Build it on `consortium-integration`'s `Executor` and `staging` helpers.
2. Implement `Contract` with fixtures driven by `ScriptedExecutor` rules.
3. Invoke `integration_contract_tests!` in its `tests/contract.rs`.

See `crates/consortium-integration-testkit/tests/dummy.rs` for the canonical
example and `crates/consortium-nix/tests/contract.rs` for the fullest real
one.

## Simulation test harness

`consortium-fanout-sim` provides seed-reproducible network/failure simulation
for the cascade primitive AND `SimExecutor`, an `Executor` that runs any
integration's pipeline under simulated bandwidth/latency/partitions/node-kills
with a virtual clock (no real waiting) and order-independent determinism
assertions. Every integration ships a `tests/sim.rs` suite on top of it:

| Suite | What it proves |
| ----- | -------------- |
| `consortium-nix` | `healthy_baseline_converges` (per-host copy + activation edges, virtual clock), `killed_target_fails_copy_but_fleet_continues` (round-0 kill fails one copy, fleet converges), `build_action_ignores_network_kill` (build-only deploy is all-local), `unhealthy_builder_falls_back_to_local_build` (ssh probe failure → local fallback), `deterministic_under_threads` |
| `consortium-slurm` | `happy_path_full_pipeline` (build → copy → sbatch → sacct → collect), `slow_submit_uplink_still_succeeds` (100 MiB at 1 MiB/s on the virtual clock), `killed_submit_node_aborts_before_sbatch` (FailFast cancels downstream), `sacct_reports_failed_state` (terminal job state fails the wait), `deterministic_under_threads` |
| `consortium-ansible` | `happy_path_all_hosts_configured` (shared control edge: per-host copies + playbooks), `killed_control_node_aborts_playbooks` (no playbook attempted against a dead control node), `one_target_playbook_fails_others_continue` (per-command scripted failure, ContinueIndependent), `check_mode_still_runs_all_phases`, `deterministic_under_threads`, `deterministic_single_target_native_equivalence` |
| `consortium-skypilot` | `happy_path_launch_and_teardown` (all-local pipeline, command ordering), `no_teardown_leaves_cluster_up`, `launch_failure_skips_teardown`, `env_build_failure_aborts_before_any_sky_command`, `deterministic_runs` (unique temp yaml paths scrubbed via `.normalize_command_line(..)`) |
| `consortium-ray` | `happy_path_submit_and_succeeded` (submit → status SUCCEEDED), `no_wait_submits_and_returns`, `submit_failure_skips_wait`, `job_failed_terminal_state`, `status_endpoint_flapping_then_timeout` (retried until timeout), `deterministic_runs` |

Writing a sim test: build a `SimExecutor` mirroring the fleet, wrap it in an
`Arc`, pass it as the integration's executor (`Arc<dyn Executor>`), run the
pipeline, and assert on the report plus edge/kind-filtered views of
`invocation_log()` — never on log order — then run the identical scenario
twice and check `assert_deterministic_equivalence`. The canonical example is
the `crates/consortium-fanout-sim/src/simexec.rs` module documentation; the
fullest real suite is `crates/consortium-nix/tests/sim.rs`.

Fuzz discipline: `crates/consortium-fanout-sim/tests/fuzz.proptest-regressions`
is checked in. When a fuzz run finds a new failing seed, commit it there and
minimize the scenario into `crates/consortium-fanout-sim/tests/corpus.rs`.
