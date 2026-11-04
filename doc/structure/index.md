# Repository structure

Consortium is a Rust workspace with Nix packaging. Its reference documentation
lives under `doc/`; requirements and planning live under `.specs/`.

## Categories

- Structure: use this category for repository, build, and tooling references.
  [Workspace](workspace-crates.md), [Nix](nix.md),
  [CI](ci-tooling.md), and [documentation](documentation.md).
- Usage: use this category for operator references:
  [SSH fan-out](../ssh.md), [`cast` deploys](../cast.md),
  [the Rust library](../library.md), and [integrations](../integrations.md).
- Architecture: use this category for runtime designs, including
  [scatter/gather builds](../scatter-gather-build.md).

The companion consortium-tests repository owns Python compatibility code,
upstream ClusterShell sources, parity tooling, and external integration suites.
