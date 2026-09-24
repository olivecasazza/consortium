ClusterShell Python Library and Tools
 =====================================

ClusterShell is an event-driven open source Python library, designed to run
local or distant commands in parallel on server farms or on large Linux
clusters. It will take care of common issues encountered on HPC clusters, such
as operating on groups of nodes, running distributed commands using optimized
execution algorithms, as well as gathering results and merging identical
outputs, or retrieving return codes. ClusterShell takes advantage of existing
remote shell facilities already installed on your systems, like SSH.

ClusterShell's primary goal is to improve the administration of high-
performance clusters by providing a lightweight but scalable Python API for
developers. It also provides clush, clubak and cluset/nodeset, convenient
command-line tools that allow traditional shell scripts to benefit from some
of the library features.

## Development

This repository uses **conventional commits** and **semantic versioning** for automated releases.

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

For full details, see [CONVENTIONAL_COMMITS.md](./CONVENTIONAL_COMMITS.md).

### Development Requirements

- **Node.js 20+** (for commitlint and semantic-release)
- **Rust stable** (for Rust crates)
- **Python 3.7+** (for Python library)

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

Requirements
------------

 * GNU/Linux, BSD, Mac OS X
 * OpenSSH (ssh/scp) or rsh
 * Python 2.x (x >= 7) or Python 3.x (x >= 6)
 * PyYAML

License
-------

ClusterShell is distributed under the GNU Lesser General Public License version
2.1 or later (LGPL v2.1+). Read the file `COPYING.LGPLv2.1` for details.

Documentation
-------------

Online documentation is available here:

    http://clustershell.readthedocs.org/

The Sphinx documentation source is available under the doc/sphinx directory.
Type 'make' to see all available formats (you need Sphinx installed and
sphinx_rtd_theme to build the documentation). For example, to generate html
docs, just type:

    make html BUILDDIR=/dest/path

For local library API documentation, just type:

    $ pydoc ClusterShell

The following man pages are also provided:

    clush(1), clubak(1), nodeset(1), clush.conf(5), groups.conf(5)

Test Suite
----------

The test infrastructure — the upstream ClusterShell Python parity suite
(`tests/`, `lib/` oracle), the comparison harness, and the Docker-based
integration tests — lives in the companion repo:
[consortium-tests](https://github.com/olivecasazza/consortium-tests).
Check it out as a sibling of this repo (`../consortium-tests`) and see its
README for how to run the parity suite and Docker integration tests.

Rust unit tests run right here:

    $ cargo test --workspace

Python code (simple example)
----------------------------

```python
>>> from ClusterShell.Task import task_self
>>> from ClusterShell.NodeSet import NodeSet
>>> task = task_self()
>>> task.run("/bin/uname -r", nodes="linux[4-6,32-39]")
<ClusterShell.Worker.Ssh.WorkerSsh object at 0x20a5e90>
>>> for buf, key in task.iter_buffers():
...     print NodeSet.fromlist(key), buf
... 
linux[32-39] 2.6.40.6-0.fc15.x86_64

linux[4-6] 2.6.32-71.el6.x86_64
```

Links
-----

Web site:

    http://cea-hpc.github.com/clustershell/

Online documentation:

    http://clustershell.readthedocs.org/

Github source repository:

    https://github.com/cea-hpc/clustershell

Github Wiki:

    https://github.com/cea-hpc/clustershell/wiki

Github Issue tracking system:

    https://github.com/cea-hpc/clustershell/issues

Python Package Index (PyPI) links:

    https://pypi.org/project/ClusterShell/

    http://pypi.python.org/pypi/ClusterShell

ClusterShell was born along with Shine, a scalable Lustre FS admin tool:

    https://github.com/cea-hpc/shine

Core developers/reviewers
-------------------------

* Stephane Thiell
* Aurelien Degremont
* Henri Doreau
* Dominique Martinet

CEA/DAM 2010, 2011, 2012, 2013, 2014, 2015 - http://www-hpc.cea.fr
