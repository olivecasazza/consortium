# Integration grammar

## Requirements

All adapters implement `consortium_integration::grammar::Integration`.
The CLI manifest registers adapter types under
`package.metadata.consortium.integrations`; its build script generates the
registry. The Nix enrollment gate requires every adapter to be registered
and rejects registry entries that are not adapters.

Each declarative `Grammar` owns parameter names, types, defaults, required
values, target-selection capability, phase selection, dependency scope,
failure policy, resource release policy, and permitted effects. Both the
Rust request boundary and `cast integration <adapter>` enforce that grammar.
Existing typed APIs and `cast` deployment commands use this execution path; typed entry points do not assemble separate DAGs. Detailed execution preserves the native DAG report alongside normalized outcomes.

Planning validates the fleet and request, expands defaults and selected hosts,
and returns selected phases without an executor or external commands.
Execution rejects altered, foreign, and configuration-stale plans, then
compiles exactly those phases into the existing DAG scheduler. Compilation
and task-factory errors fail before effects. Shared phases execute once;
target phases execute for each selected host. The grammar chooses fail-fast
or independent-host progress.

Every phase receives a scoped executor enforcing its declared process,
remote process, temporary-file, clock, sleep, diagnostic, and structured event capabilities.
A denied effect fails the task and revokes its remaining capabilities, even
when task code ignores the returned error. Reports expose actual phase
outcomes and observed effect kinds. This is a library effect contract, not
an operating-system sandbox for arbitrary third-party Rust code.

Temporary files use invocation-scoped leases with explicit cleanup errors
and drop fallback. Production files are uniquely allocated with private
permissions; scripted files and clocks remain virtual. Unsupported bounded
execution fails before process launch. Durations preserve nanosecond precision through typed and CLI requests. Status polling deadlines include
command execution and sleeps; Unix process timeouts terminate the local
process group. Remote job cancellation requires the integration's own
resource protocol and is not implied by killing an SSH client.

Nix supports both NixOS and nix-darwin profiles, all existing deployment
actions, distributed builders, concurrency, explicit local activation,
activation timeouts, and platform-specific Nix arguments. Build-only mode
omits copy and activation. Cascade distribution is a selected shared grammar phase. It waits for all host builds to settle, distributes successful closures, and preserves per-host build prerequisites before copy and activation. Existing cascade entry points preserve streamed events through the injected event boundary.

Ansible supports host selection, environment, playbook, check mode, and
concurrency. Slurm and Ray submit named jobs with optional waits. Slurm can
select a partition and collect a literal output file path; both can bound
their waits. Ray submission returns before job completion so the selected
wait phase controls polling. Malformed submission IDs and ambiguous status
responses fail closed. SkyPilot
launches a named cluster from task YAML with optional teardown. Job and
cluster integrations reject host selectors rather than ignoring them.

Resource release requires a selected, confirmed acquisition immediately
before it. Ambiguous or multi-resource lifecycle declarations are rejected.
SkyPilot reports released, retained, uncertain, and cleanup-failed resource
states. A failed launch can leave partial external allocation; it is reported
as uncertain and is not blindly torn down because the named cluster may
pre-exist. Local YAML cleanup runs after resource release, and a cleanup
failure cannot hide successful acquisition or suppress teardown.

Nix endpoint configuration preserves custom SSH ports and IPv6 addresses across builds, direct and cascade copies, and activation. Explicit empty target sets in typed host APIs fail before effects rather than expanding to the fleet.

## Acceptance and validation

- CLI arguments are generated from the same grammar used by runtime validation.
- Unsupported parameters, missing required values, malformed booleans, invalid
  actions, zero/negative/overflowing concurrency and timing values, and unknown
  targets fail before executor invocation.
- Planning with an explicit fleet works without Nix, SSH, scheduler, or cloud tools.
- A parameterized suite visits the complete CLI adapter registry and checks
  valid plans, input/config failures, success and failure reporting, optional
  phases, and both Nix platform profiles across deployment actions.
- Existing semantic contract, CLI grammar, simulation, and compatibility suites
  remain in use; the new suite lives in consortium-tests.

CLI tests cover parameter value classes and numeric boundaries, unsupported
cross-adapter flags, parser/runtime default agreement, output-format round
trips, selector combinations, and configuration failures. Executable tool
fixtures exercise child-process dispatch, reports, and exit status without
contacting a fleet.

The consortium-tests registry-driven suite checks phase order, failure at each
enabled phase, independent-host progress, selected-host isolation, optional
phase combinations, terminal job states, numeric decoding, and report trait
consumers. Nix cases cover NixOS and nix-darwin actions, local activation,
platform arguments, target-side builds, and timeout forwarding.

The registry-driven tests use synthetic fleets and scripted command results.
Separate scoped Docker suites verify real SSH transport, Nix evaluation,
builds, store transfers, and fixture activation for both Nix profiles with
direct and cascade distribution. They also exercise real Ansible tooling.
Fixture activation does not verify operating-system activation; scheduler
and cloud behavior still require their external deployment environments.
Mutation analysis applies only to the selected mutation sites.
