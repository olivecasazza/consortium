---
type: concept
title: Nix side — flake, fleet library, microVM fleet
description: The hand-rolled flake-parts flake at the repo root plus nix/lib and nix/vms.
resource: https://github.com/olivecasazza/consortium
tags: [structure, nix, flake-parts, microvm]
generated:
  by: opencode/claude-code
  at: "2026-09-28T19:43:12-07:00"
status: draft
stale_after: "2026-12-28T00:00:00-07:00"
---

# Nix side — flake, fleet library, microVM fleet

## Purpose

All Nix functionality: a single flake at the repository root, a hand-written
fleet-configuration library exported as `flake.lib`, and a declarative microVM
test fleet.

## Language / build system

Nix, using [flake-parts](https://github.com/hercules-ci/flake-parts).

> **This is not Snowfall Lib.** `flake.nix:35` calls
> `flake-parts.lib.mkFlake { inherit inputs; } { ... }`, and the complete input
> set is declared inline at `flake.nix:4-19`:
> `nixpkgs`, `flake-parts`, `git-hooks-nix`, `crane`, `rust-overlay`,
> `microvm-nix`. There is no `snowfall-lib` input. The string `snowfall` occurs
> exactly once in the tracked tree — `nix/lib/fleet.nix:18` — and it is a
> comment noting that `mkFleet` can consume configurations "from flake outputs
> or Snowfall Lib", not a dependency on it.

## flake.nix — repo root, 440 lines

| Lines | Content |
| --- | --- |
| `1-19` | `description` and the six declared inputs |
| `21-34` | `outputs` wrapper; imports `./nix/vms` into a `vms` let-binding |
| `35-36` | `flake-parts.lib.mkFlake`, plus `git-hooks-nix.flakeModule` |
| `38-43` | `systems`: x86_64/aarch64 × linux/darwin |
| `46-53` | Non-per-system `flake.lib` = `import ./nix/lib` |
| `55-80` | microVM fleet `nixosConfigurations` |
| `82-93` | `perSystem`; `rust-overlay` applied on the pkgs side |
| `157-166` | `consortiumLib`, `fleetFixture` from `fleet-contract-fixture.nix` |
| `171-213` | `cargo-semver-checks` app/derivation against Git and crates.io baselines |
| `247-262` | `checks.cargo-test`, `checks.cargo-clippy` via Crane |
| `283-290` | `checks.integration-enrollment` via `nix/lib/enrollment.nix` |
| `335-375` | `packages` + per-VM qemu runners |
| `378-389` | `apps.semver-check` |
| `392-…` | `devShells.default` |

## nix/lib/ — hand-written fleet-config library

Four files, all authored here. **Not vendored.**

```
nix/lib/default.nix                 entrypoint; exports mkFleet only
nix/lib/fleet.nix                   mkFleet implementation
nix/lib/enrollment.nix              eval-time test-enrollment gate
nix/lib/fleet-contract-fixture.nix  fixture consumed by checks.fleet-contract
```

- `nix/lib/default.nix:8-15` takes `{ lib, writeText }` and re-exports
  `mkFleet` from `fleet.nix` (`:11`, `:14`).
- `nix/lib/fleet.nix:1-13` documents the intent: it replaces colmena's "hive"
  concept by turning `nixosConfigurations` / `darwinConfigurations` into a fleet
  config that the `consortium-nix` CLI consumes as JSON. Arguments start at
  `nix/lib/fleet.nix:16-30` (`nixosConfigurations`, `darwinConfigurations`,
  `builders`, `defaultSystem`, `getHostTags`, `hostOverrides`).
- `nix/lib/enrollment.nix` is imported only at `flake.nix:287` to build
  `checks.integration-enrollment`, which fails at **eval** time when suites are
  unenrolled (`flake.nix:283-284`).

### Referenced by

| Consumer | Citation |
| --- | --- |
| flake `lib` output | `flake.nix:48-53` |
| `checks.fleet-contract` fixture | `flake.nix:166` |
| `checks.integration-enrollment` | `flake.nix:287` |
| Rust contract test that parses the generated JSON | `crates/consortium-integration/tests/nix_fleet_contract.rs:1-20` |
| Documented consumer snippet | `nix/lib/default.nix:5-7` |

## nix/vms/ — declarative microVM test fleet

Ten files, all authored here. **Not vendored.**

```
nix/vms/default.nix     mkVm constructor + node table
nix/vms/base.nix        shared minimal NixOS base node
nix/vms/plugins/{nix,slurm,ansible,ray,skypilot}.nix
nix/vms/keys/id_test    TEST-ONLY ssh key
nix/vms/README.md       runbook
nix/vms/keys/README.md  key provenance
```

- `nix/vms/default.nix:1-12` documents the export surface: `mkVm`,
  `nodeNumbers`, `nodeModules`, `configs`. `:14-20` lists the six nodes
  (`vm-base` 10.99.0.10 through `vm-skypilot` .15, all x86_64-linux qemu).
- `:22-25` gives the runbook; microVMs cannot run on macOS, only a Linux KVM host.
- Imported at `flake.nix:33`; runners are exposed as packages at `flake.nix:373-375`,
  restricted to `x86_64-linux` via `lib.optionalAttrs`.
- Prose docs: `doc/testing-microvims.md` (note the misspelling in the filename;
  it is referenced from `flake.nix:32`).

## nix/*.nix — flat flake-parts modules

| File | Purpose | Citation |
| --- | --- | --- |
| `nix/pre-commit.nix` | git-hooks.nix hook settings: rustfmt, clippy (excludes `consortium-py`), commitlint | `nix/pre-commit.nix:12-20` |
| `nix/rust.nix` | Rust toolchain package used by the hook entries | consumed by `nix/pre-commit.nix:15`, `:19` |
| `nix/python.nix` | Python package set for the dev shell; **not imported by `flake.nix`** (no `nix/python.nix` reference exists) — the dev shell instead builds a venv inline at `flake.nix:412-417` | `flake.nix:412-417` |

## Generated file, do not hand-edit

`.pre-commit-config.yaml` is produced by git-hooks.nix and marked as such at
`.pre-commit-config.yaml:1-2`; the hook list is defined in `nix/pre-commit.nix`.

## Related

- [workspace-crates](/workspace-crates.md) — what the flake builds
- [ci-tooling](/ci-tooling.md) — the Nix gates run in CI
