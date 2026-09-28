# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Breaking Changes

- Bumped the workspace to 0.3.0: published 0.2.0 Rust APIs were already
  incompatible with the current source, so the next release declares a
  pre-1.0 breaking change rather than claiming 0.2.0 compatibility.

### Features

- `cast deploy` prints a start line and a finish line (with duration) per
  host for the copy and activate stages, so a host that blocks mid-stage
  names itself. `--activate-timeout SECONDS` kills any single activation
  command that runs longer and fails only that host; others continue.
  Library: `DeployOptions::activate_timeout`, `activate_host_bounded`,
  `activate_local_bounded`, and `Executor::exec_timeout` (a provided method;
  `ProcessExecutor` kills the child, `ScriptedExecutor` simulates it via
  `Rule::slow_containing_all`).
- Added a strict, introspected CLI grammar gate for `claw`, `molt`, `pinch`,
  and `cast`, with an embedded zero-violation baseline, drift checks, and
  a dedicated CI invocation.
- `cast` discovers its fleet without a hand-built `fleet.json`: precedence is
  `--config FILE`, `./fleet.json` when present, the flake's `fleet` output
  (an `mkFleet` result or a derivation producing the JSON), then a minimal
  fleet derived from the flake's `darwinConfigurations` /
  `nixosConfigurations` attribute names (tagged `nix-darwin` / `nixos`).
  `--flake` defaults to the current directory; `--user` overrides every
  target's ssh user.
- `cast --on` expands ClusterShell `@group` / `@source:group` references via
  `groups.conf` (as `claw -g` does) and flat `groups.d/*` `group: nodeset`
  files; `host.local`-style entries map to the fleet node `host`. The
  groups.conf lookup now honours `$XDG_CONFIG_HOME` for `claw` too.
- `cast deploy` resolves each remote target's ssh endpoint live before copy
  and activation (`targetHost`, `<name>.local`, `<name>`, then their IPv4
  addresses; first to accept `ssh … true` wins), reports unreachable hosts
  with every endpoint tried, and activates a target that is the local
  machine through `sudo` without copying or ssh.
- `cast` evaluates and builds `darwinConfigurations.<name>` for nix-darwin
  nodes; extra per-platform nix words are passed with `--nix-args`,
  `--darwin-nix-args`, `--nixos-nix-args` (`consortium_nix::NixArgs` /
  `DeployOptions`, `deploy_with_options`, `deploy_with_cascade_options`).
- nix-darwin activation runs `sudo <toplevel>/activate` and only runs the
  legacy `activate-user` when it exists and is not nix-darwin's
  `# nix-darwin: deprecated` stub; privileged activation steps use `sudo`
  unless the ssh user is `root`.
- The interim bash `cast-on` tool (`nix/cast-on.nix`,
  `packages.<system>.cast-on`) is replaced by the above in the `cast` binary.
  Its automatic `git commit && git push` of the user's flake was
  intentionally not carried over: committing on the user's behalf is a
  surprising side effect for a deploy tool, and remote hosts never fetch
  the flake (closures are built locally and copied).

### Bug Fixes

- Parse `mkFleet`'s camelCase JSON fields, including non-default `flakeUri`,
  correctly in the Rust fleet consumer.

### Performance

### Refactor

- Migrated all test infrastructure to the new `consortium-tests` repo:
  the upstream ClusterShell Python parity suite (`tests/`), the Python
  oracle (`lib/`), the comparison harness (`harness/`), `conf/`,
  `packaging/`, `doc/legacy/`, `TEST_MAPPING.toml`, `UPSTREAM_REF`, the
  `consortium-test-harness` crate, and the Docker integration tests.
  Check out `consortium-tests` as a sibling of this repo to run them.

### Documentation

### Chores

### CI/CD

- Run the upstream Python parity suite without pytest stream capture so clush
  receives a real stdin file descriptor, matching ClusterShell's test runner.
- Gate Rust workspace tests, Nix fleet/adapter contracts, and the native Python
  API in CI; enforce Rust API compatibility against the Git base commit and
  release versioning against crates.io outside the Nix build sandbox.
- Configure the parity runner's nested loopback SSH sessions with the
  ClusterShell hostname fixture and quiet host-key handling, so tree copy,
  reverse-copy, and abort tests exercise simulated node identities reliably.
- Give the parity runner's gateway login shells a working Rust backend: the
  venv's `python` on PATH and `LIB_CLUSTERSHELL`, which upstream does not
  forward to gateways. Without them every tree gateway failed to import
  `ClusterShell.Gateway` and surfaced as `No route available`.
