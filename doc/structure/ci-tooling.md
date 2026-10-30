# CI and tooling

`.github/workflows/ci.yml` runs Rust tests, formatting, clippy, the CLI grammar
gate, documentation, Docker integration tests, and Nix checks.
`.config/nextest.toml` defines the CI test profile and JUnit output.

The migration scorecard checks out consortium-tests, builds its PyO3 bridge,
and runs the upstream suite against the Python oracle and Rust backend.
Both repositories expect a sibling-checkout layout; the workflow provides
that layout for Cargo path dependencies. The Python API smoke test runs
with the bridge build in both repositories' parity workflows.

`docs.yml` builds and publishes rustdoc; `release.yml` handles releases.
`release-plz.toml` selects published crates; `cliff.toml` configures changelogs.
Commit conventions are checked by commitlint and `scripts/verify-commits.sh`.

The development shell installs hooks through git-hooks.nix. Its generated
`.pre-commit-config.yaml` is ignored. `skills/` supplies the skill catalog;
`.agents/` contains ignored local discovery links.
