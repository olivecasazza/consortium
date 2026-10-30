# Consortium

Consortium is a Rust cluster administration toolkit with Nix packaging,
parallel command execution, node-set algebra, and infrastructure integrations.
It reimplements functionality from [ClusterShell](https://github.com/cea-hpc/clustershell).

## Development

This repository uses **conventional commits** and **semantic versioning** for automated releases.

Design documents, task specs, and the planning roadmap live in [`.specs/`](.specs/).

### Commit Message Format

All commits must follow the [conventional commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <subject>
```

**Types:**
- `feat`: New feature (bumps MINOR version)
- `fix`: Bug fix (bumps PATCH version)
- `perf`: Performance improvement (bumps PATCH version)
- `refactor`: Code refactoring (bumps PATCH version)
- `docs`: Documentation changes (no version bump)
- `style`: Code style changes (no code change)
- `test`: Adding or updating tests (no version bump)
- `chore`: Maintenance tasks (no version bump)
- `ci`: CI/CD changes (no version bump)
- `build`: Build system changes
- `revert`: Revert previous commit

**Breaking Changes:**
```
feat: remove deprecated API

BREAKING CHANGE: The old API has been removed. Use new API instead.
```

### Development Requirements

- **Rust stable** (see `rust-toolchain.toml`)
- **Nix** (for reproducible packages and development tools)
- **OpenSSH** (for remote execution)

### CLI grammar gate

The `consortium-grammar` binary introspects the clap signatures of `claw`,
`molt`, `pinch`, and `cast`, and checks the declared binary registry, reviewed
root long options and selectors, legacy short flags, and argument help:

```sh
nix develop --command cargo run -p consortium-cli --bin consortium-grammar -- check
nix develop --command cargo run -p consortium-cli --bin consortium-grammar -- dump
```

`check` compares live violation keys with the committed
`crates/consortium-cli/src/grammar/baseline.json` embedded at build time.
The Cargo binary registry and source/test presence facts are embedded too,
so the Nix-installed checker works without the build source tree.
New, stale, or duplicate keys fail the check; CI runs it as a strict step.
`check --baseline PATH` audits a different JSON key array and fails if the
file is missing. `dump` prints the introspected rows as JSON, including
registry placeholders for binaries without exported clap arguments. Review
rule changes rather than adding new violations to the baseline.

### cast: push deploys for NixOS / nix-darwin fleets

`cast` (in `packages.<system>.consortium-cli`, the flake's default package)
deploys a flake from one workstation: every target's system closure is
evaluated and built locally, `nix copy`'d to its host, then activated, with
the four stages pipelined per host by the DAG executor (`--cascade` swaps the
copy stage for peer-to-peer fan-out).

```sh
cast --flake . eval                              # list targets, no nix build
cast --flake . build --on box[01-03]             # build only
cast --flake . deploy --on @darwin switch        # a ClusterShell group
cast --flake github:me/cfg deploy --on mac01,@gpu switch
cast --flake . --nixos-nix-args '--override-input secrets path:./stub' deploy
cast --flake . deploy --on @darwin --activate-timeout 600 switch  # fail hosts stuck >10 min
```

**Fleet source.** In order of precedence: `--config FILE`; `./fleet.json`
when it exists; the flake's `fleet` output (an `mkFleet` result — its
`configJson` is read directly, or a derivation producing the JSON file is
built); and finally a minimal fleet derived from the flake's
`darwinConfigurations` / `nixosConfigurations` attribute names (profile type
by output, `targetHost` = attribute name, tag `nix-darwin` / `nixos`, ssh
user `$USER` unless `--user` is given). Only attribute names are evaluated in
the last case, so it is cheap. `--flake` defaults to the current directory
and overrides the fleet's `flakeUri`; `--user` overrides every `targetUser`.

**Targets.** `--on` takes bracket notation and `@group` / `@source:group`
references, resolved through ClusterShell `groups.conf`
(`$XDG_CONFIG_HOME/clustershell`, `~/.config/clustershell`,
`/etc/clustershell`) exactly as `claw -g` does, then flat
`groups.d/*` files with `group: nodeset` lines. Group entries written as
`host.local` or `host.example.org` map to the fleet node `host`. `--tag`
selects by fleet tag; with neither flag every node is targeted.

**Endpoint resolution.** Before copy and activation, each remote target's
ssh endpoint is resolved live: the fleet's `targetHost`, then `<name>.local`,
then `<name>`, then the IPv4 addresses those resolve to, deduplicated; the
first one accepting a non-interactive `ssh … true` is used for both `nix copy`
and activation. Hosts that cannot be reached are reported with every endpoint
tried and skipped (the run exits non-zero). A target whose name is this
machine's hostname is not copied to and is activated locally through `sudo`.

**Activation.** NixOS: `nix-env --set` (switch/boot) then
`switch-to-configuration <action>`. nix-darwin: `nix-env --set` (switch/boot),
the legacy `activate-user` only when it exists and is not nix-darwin's
deprecated stub, then `<toplevel>/activate` — all through `sudo` unless the
ssh user is `root`.

**Extra nix arguments.** `--nix-args WORDS` (every host),
`--darwin-nix-args WORDS` and `--nixos-nix-args WORDS` (per platform) append
whitespace-split words to each `nix eval` / `nix build`; repeat the flag to
append more. Typical uses: `--override-input` to stub an input one platform
must not fetch, or `--option builders ''` to keep a deploy off the fleet's
own distributed builders.

## Integrations

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

### Contract test suite

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

### Simulation test harness

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

## Testing

Run `cargo test --workspace` for Rust tests and `nix flake check` for the
Nix build and contract checks. Build the library documentation with
`cargo doc --workspace --no-deps`. Rust and CLI examples live in [examples/](examples/README.md).

The companion [consortium-tests](https://github.com/olivecasazza/consortium-tests)
repository owns the upstream ClusterShell oracle, Python parity suite,
PyO3 compatibility bridge, comparison tooling, and Docker integration tests.
Check it out at `../consortium-tests` and follow its README to run both
the upstream Python and Rust-backed parity tests. Consortium itself has
no Python package or Python build dependency.

## License and attribution

Consortium is distributed under the GNU Lesser General Public License,
version 2.1 or later. The original ClusterShell license text is preserved
in [COPYING.LGPLv2.1](COPYING.LGPLv2.1).

ClusterShell is copyright CEA/DAM, Aurelien Degremont, Stephane Thiell,
and its other contributors. Consortium derives from the ClusterShell
project; the upstream implementation and compatibility suite are maintained
in consortium-tests. Existing copyright notices remain with their sources.
