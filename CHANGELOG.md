# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Breaking Changes

- Bumped the workspace to 0.3.0: published 0.2.0 Rust APIs were already
  incompatible with the current source, so the next release declares a
  pre-1.0 breaking change rather than claiming 0.2.0 compatibility.

### Features

- Added a strict, introspected CLI grammar gate for `claw`, `molt`, `pinch`,
  and `cast`, with an embedded zero-violation baseline, drift checks, and
  a dedicated CI invocation.
- Added `packages.<system>.cast-on`: a push-based NixOS / nix-darwin fleet
  deploy tool (build locally, `nix copy`, activate concurrently) whose
  `@group` targets are expanded from a ClusterShell `groups.d` file via
  `pinch`. Flake, default targets, groups file, SSH user, and per-platform
  extra nix args are configurable via flags/environment.

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
