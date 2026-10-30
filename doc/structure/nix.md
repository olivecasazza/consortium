# Nix builds and fleet configuration

`flake.nix` uses flake-parts, Crane, and rust-overlay. It exports the Rust
library, command-line package (the default), Nix adapter, and skill catalog.
The development shell supplies Rust and Nix tooling without a Python environment.

`nix/lib/default.nix` exports `mkFleet`; `fleet.nix` generates fleet JSON.
`fleet-contract-fixture.nix` supplies the fixture for the Rust fleet parser
check. `enrollment.nix` requires each integration to enroll in the shared
contract suite.

The flake checks cover Cargo tests and clippy, fleet JSON, integration
enrollment, repository source policy, and installed CLI grammar.
The `semver-check` app compares public Rust APIs with Git and crates.io baselines.

`nix/vms/` contains the declarative integration fleet and its
[runbook](../../nix/vms/README.md). VM tool environments can include Python
as a dependency of Ansible, Ray, or SkyPilot; they contain no ClusterShell
implementation or consortium Python package.

`nix/rust.nix` and `nix/pre-commit.nix` are optional flake-parts modules;
the root flake defines its active builds and hooks inline.
