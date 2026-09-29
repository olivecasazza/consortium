---
type: concept
title: Workspace and crates layout
description: Root Cargo workspace, the 11 crates under crates/, and the examples member.
resource: https://github.com/olivecasazza/consortium
tags: [structure, rust, cargo, workspace]
generated:
  by: opencode/claude-code
  at: "2026-09-28T19:43:12-07:00"
status: draft
stale_after: "2026-12-28T00:00:00-07:00"
---

# Workspace and crates layout

## Purpose

The Rust workspace definition and every published or internal crate. This is
the project proper — the Rust rewrite of ClusterShell.

## Language / build system

Cargo workspace, Rust edition 2021, resolver 2 (`Cargo.toml:1-2`, `Cargo.toml:23`).
Toolchain is pinned to `stable` with `rust-src`, `rust-analyzer`, `clippy`,
`rustfmt` (`rust-toolchain.toml:2-3`).

## Workspace members

`Cargo.toml:3-16` lists **12** members: 11 crates plus `examples`.

| Member directory | Package name | Notes |
| --- | --- | --- |
| `crates/consortium` | `consortium-crate` | Core library: DAG executor, NodeSet, SSH workers, MsgTree (`docs-index/index.html:38`) |
| `crates/consortium-py` | `consortium-py` | PyO3 bindings; **excluded** from the pre-commit clippy hook (`nix/pre-commit.nix:19`) |
| `crates/consortium-cli` | `consortium-cli` | `claw`, `molt`, `pinch`, `cast`; the flake `default` package (`flake.nix:338`) |
| `crates/consortium-nix` | `consortium-nix` | NixOS deploy; consumed by the fleet contract test |
| `crates/consortium-ansible` | `consortium-ansible` | |
| `crates/consortium-slurm` | `consortium-slurm` | |
| `crates/consortium-ray` | `consortium-ray` | |
| `crates/consortium-skypilot` | `consortium-skypilot` | |
| `crates/consortium-fanout-sim` | `consortium-fanout-sim` | |
| `crates/consortium-integration` | `consortium-integration` | Owns `tests/nix_fleet_contract.rs` |
| `crates/consortium-integration-testkit` | `consortium-integration-testkit` | Test harness; not published (`release-plz.toml:5`) |
| `examples` | `consortium-examples` | 12th member, `publish = false` |

`crates/` contains exactly 11 directories, matching the 11 `crates/*` entries
in `Cargo.toml:4-14`. The directory name `consortium` and the package name
`consortium-crate` deliberately differ; the workspace alias is declared at
`Cargo.toml:29`.

## Shared configuration

`Cargo.toml:21-26` sets workspace-wide `version = "0.3.0"`, edition, license
`LGPL-2.1-or-later`, repository, and description. `Cargo.toml:28-39` declares
path dependencies for all internal crates plus `pyo3` 0.22 and `thiserror` 2, so
member manifests inherit rather than re-pin.

## Referenced by

| Consumer | Citation |
| --- | --- |
| Nix flake (Crane build, overlays, packages, dev shell) | `flake.nix:8`, `flake.nix:92-93`, `flake.nix:337`, `flake.nix:392-393` |
| CI unit tests | `.github/workflows/ci.yml:39-42` |
| CI clippy / fmt / rustdoc | `.github/workflows/ci.yml:53`, `:57`, `:65` |
| Release publishing set | `release-plz.toml:6-28` |
| Docs index crate grid | `docs-index/index.html:36-68` |

## examples/

`examples/Cargo.toml` is a real workspace member, not a loose folder. It sets
`autoexamples`/`autotests`/`autobenches = false` (`examples/Cargo.toml:9-11`) and
declares each example explicitly as a `[[example]]` target
(`examples/Cargo.toml:24-26` onward). Subdirectories:

```
examples/rust/          standalone Rust example sources
examples/python/        Python usage samples (via consortium-py)
examples/cli/           CLI invocation fixtures
examples/inventories/   static inventory files for integration examples
```

Docs: `examples/README.md`, `examples/cli/README.md` (neither currently carries
OKF frontmatter — see [ci-tooling](/ci-tooling.md) for the repo-wide audit).

## Not vendored

Nothing in this area is vendored from upstream. `crates/`, `examples/`,
`Cargo.toml`, and `rust-toolchain.toml` are all authored here.

## Related

- [nix](/nix.md) — how the workspace is built and gated by Nix
- [ci-tooling](/ci-tooling.md) — test runner configuration for these crates
