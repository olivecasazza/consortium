# Rust workspace

`Cargo.toml` is the authority for workspace membership and shared dependency
versions. `Cargo.lock` is tracked for reproducible Cargo and Crane builds.

The core library is `crates/consortium` (package `consortium-crate`).
`consortium-cli` provides the command-line tools. The Nix, Slurm, Ansible,
Ray, and SkyPilot adapters share `consortium-integration` execution, fleet,
staging, and report types. `consortium-integration-testkit` supplies the
shared semantic contract suite; `consortium-fanout-sim` supplies simulation.

`examples` is an unpublished Cargo workspace member with Rust examples,
CLI walkthroughs, and inventory fixtures. See [examples](../../examples/README.md).

Python bindings, compatibility shims, and Python examples belong to
consortium-tests. Its `crates/consortium-py` depends on this repository's
Rust core through the sibling-checkout layout.
