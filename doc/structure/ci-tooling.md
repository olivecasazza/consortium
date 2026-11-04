# CI and tooling

`.github/workflows/ci.yml` runs Rust tests, formatting, clippy, the CLI grammar
gate, documentation, Docker integration tests, and the SemVer gate; Buildbot
builds every root Nix check.
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

## Commit messages

Releases are automated from [conventional commits](https://www.conventionalcommits.org/)
and semantic versioning.

All commits must follow the [conventional commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <subject>
```

**Types:**
- `feat`: New feature (bumps MINOR version)
- `fix`: Bug fix (bumps PATCH version)
- `perf`: Performance improvement (bumps PATCH version)
- `refactor`: Code refactoring (bumps PATCH version)
- `docs`: Documentation changes (no version bump)
- `style`: Code style changes (no code change)
- `test`: Adding or updating tests (no version bump)
- `chore`: Maintenance tasks (no version bump)
- `ci`: CI/CD changes (no version bump)
- `build`: Build system changes
- `revert`: Revert previous commit

**Breaking Changes:**
```
feat: remove deprecated API

BREAKING CHANGE: The old API has been removed. Use new API instead.
```

## CLI grammar gate

The `consortium-grammar` binary introspects the clap signatures of `claw`,
`molt`, `pinch`, and `cast`, and checks the declared binary registry, reviewed
root long options and selectors, legacy short flags, and argument help:

```sh
nix develop --command cargo run -p consortium-cli --bin consortium-grammar -- check
nix develop --command cargo run -p consortium-cli --bin consortium-grammar -- dump
```

`check` compares live violation keys with the committed
`crates/consortium-cli/src/grammar/baseline.json` embedded at build time.
The Cargo binary registry and source/test presence facts are embedded too,
so the Nix-installed checker works without the build source tree.
New, stale, or duplicate keys fail the check; CI runs it as a strict step.
`check --baseline PATH` audits a different JSON key array and fails if the
file is missing. `dump` prints the introspected rows as JSON, including
registry placeholders for binaries without exported clap arguments. Review
rule changes rather than adding new violations to the baseline.
