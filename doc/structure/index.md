---
okf_version: "0.2"
---

# consortium — repository structure

Structural map of the `consortium` Rust workspace. Every claim below is cited as
`file:line` against the tree on branch `chore/repo-structure-docs` at
`49746e39` (docs baseline).

> This bundle lives inside the vendored ClusterShell documentation tree
> (`doc/`). See [documentation](/documentation.md) for why that matters and for
> the provenance of the surrounding files. Only `doc/structure/` is authored
> here; every sibling directory is upstream.

## Concepts

| Concept | Covers |
| --- | --- |
| [workspace-crates](/workspace-crates.md) | `Cargo.toml` workspace, the 11 crates under `crates/`, `examples/`, toolchain pin |
| [nix](/nix.md) | root `flake.nix` (flake-parts, not Snowfall Lib), `nix/lib/`, `nix/vms/`, `nix/*.nix` modules |
| [documentation](/documentation.md) | vendored `doc/` ClusterShell sphinx tree, `docs-index/`, readthedocs wiring, rustdoc publishing |
| [ci-tooling](/ci-tooling.md) | `.github/workflows/`, `.config/nextest.toml`, pre-commit, commitlint, release-plz, `scripts/`, `skills/`, `.memories/` |

## Top-level map

| Path | Language / build system | Role |
| --- | --- | --- |
| `flake.nix` | Nix (flake-parts) | Single 440-line flake at the repo root; the entire Nix entrypoint |
| `Cargo.toml`, `Cargo.lock` | Cargo | Workspace manifest and lockfile |
| `crates/` | Rust / Cargo | 11 library and binary crates (`Cargo.toml:3-16`) |
| `examples/` | Rust, Python, CLI fixtures | 12th workspace member, `publish = false` (`examples/Cargo.toml:8`) |
| `nix/` | Nix | `lib/` (fleet config library), `vms/` (microVM fleet), 3 module files |
| `doc/` | Sphinx / reStructuredText | **Vendored** upstream ClusterShell docs + this bundle |
| `docs-index/` | HTML | Single `index.html` that fetches GitHub tags at runtime |
| `.github/workflows/` | GitHub Actions YAML | 4 workflows: CI, Docs, Migration Scorecard, Release |
| `.config/` | TOML | cargo-nextest `ci` profile, consumed by `ci.yml:39-42` |
| `skills/` | Markdown | Agent skill catalog, packaged by `flake.nix:352-367` |
| `scripts/` | Bash, Python | Commit-message verifier, test-report generator |
| `.memories/` | Markdown | Single agent memory note, untracked tooling state |
| `rust-toolchain.toml` | TOML | Pins `stable` + clippy/rustfmt/rust-src/rust-analyzer |

## Things that are NOT here

- No `snowfall-lib` input. The flake is hand-rolled flake-parts; see [nix](/nix.md).
- No `nix/`-level Snowfall structure — `nix/` holds only `lib/` and `vms/` plus
  three flat module files.
- `results/` and `test-reports/` are **untracked** local artifacts
  (`git ls-files results test-reports` returns nothing), not repository structure.
