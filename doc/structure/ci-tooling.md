---
type: concept
title: CI and tooling
description: GitHub Actions workflows, nextest profile, hooks, commit/release tooling, scripts, skills, and the repo-wide markdown audit.
resource: https://github.com/olivecasazza/consortium
tags: [structure, ci, github-actions, tooling, markdown-audit]
generated:
  by: opencode/claude-code
  at: "2026-09-28T19:43:12-07:00"
status: draft
stale_after: "2026-12-28T00:00:00-07:00"
---

# CI and tooling

## Purpose

Everything that runs, lints, releases, or documents the project outside the
compiler, plus the two authored non-doc directories `skills/` and `.memories/`.

## Language / build system

GitHub Actions YAML, TOML, Bash, Python, and Markdown.

## .github/workflows/ — 4 workflows

| File | Trigger | Job summary |
| --- | --- | --- |
| `ci.yml` | push/PR on `main`,`master`; ignores `doc/**`, `README.md`, `.gitignore` (`:6-9`) | 4 jobs |
| `docs.yml` | push on `master`; `workflow_dispatch` (`:3-6`) | Build + deploy rustdoc and `docs-index/` |
| `migration-scorecard.yml` | push/PR on `main`,`master`,`develop`; ignores `doc/**` (`:6-10`) | Python→Rust migration scorecard |
| `release.yml` | release + release-PR | `release-plz` publishing, docs deploy (`:32-43`) |

### ci.yml job layers

| Job | Lines | Notes |
| --- | --- | --- |
| `unit` | `ci.yml:16-65` | nextest, clippy, fmt, CLI grammar gate, rustdoc |
| `docker-integration` | `ci.yml:68-138` | checks out sibling `consortium-tests` (`:79`), builds an SSH node image |
| `tool-integration` | `ci.yml:141-236` | same sibling layout, specialized images per tool |
| `nix-gates` | `ci.yml:238-…` | self-hosted; builds Nix integration checks (`:253`) and runs the SemVer check (`:261`) |

The `unit` job's key lines:

```yaml
cargo nextest run --workspace --profile ci --no-fail-fast   # ci.yml:39-41
  2>&1 | tee target/nextest/ci/junit.xml                    # ci.yml:42
```

Note the two integration jobs and the microVM work depend on the **sibling
`consortium-tests` repository**, not on anything in this tree. The dev shell
also references it at `flake.nix:419-420`.

## .config/ — genuinely used

`nextest.toml` is the only file in `.config/`, and it is load-bearing:

| Key | Value | Line |
| --- | --- | --- |
| `[profile.ci]` | profile definition | `nextest.toml:1` |
| `junit.path` | `junit.xml` for GH Actions reporting | `nextest.toml:3` |
| `fail-fast` | `false`, so all tests run | `nextest.toml:6` |
| `retries` | `2`, for the known-flaky `defaults::test_config_paths_with_cfgdir` | `nextest.toml:9` |
| `slow-timeout` | period `30s`, terminate after `10` | `nextest.toml:12` |

Consumer: `.github/workflows/ci.yml:40` passes `--profile ci`, and
`ci.yml:48` reads back the `junit.xml` that `nextest.toml:3` produces.

## Hooks and commit conventions

| File | Role | Citation |
| --- | --- | --- |
| `.pre-commit-config.yaml` | **Generated** by git-hooks.nix — do not hand-edit | `.pre-commit-config.yaml:1-2`; hook source at `nix/pre-commit.nix:12-20` |
| `.pre-commit-config-commitlint.yaml` | commitlint hook config | repo root |
| `.commitlint-pre-commit.yaml` | commitlint staged-file wiring | repo root |
| `.commitlintrc.yml` | Conventional Commits rules | repo root |
| `scripts/verify-commits.sh` | standalone conventional-commit checker, `verify-commits.sh [from] [to]`, defaults `HEAD~1..HEAD` | `scripts/verify-commits.sh:2-6` |
| `scripts/test-report.py` | turns JUnit XML into reports | repo root |
| `release-plz.toml` | crates.io publish set; `consortium-integration-testkit` and `consortium-py` are deliberately excluded | `release-plz.toml:5`, `:6-28` |
| `cliff.toml` | git-cliff changelog generator, wired via `changelog_config` | `release-plz.toml:2-3` |

## skills/ — agent skill catalog

```
skills/consortium/SKILL.md             skill manifest
skills/consortium/references/cli-surface.md
```

Packaged for downstream consumption by `flake.nix:352-367`: the derivation
walks `${./skills}/*/SKILL.md`, reads the `name:` frontmatter value
(`flake.nix:356`), asserts it is non-empty (`:357`), and copies the file plus any
of `agents assets examples references scripts tests` resource directories
(`:361-365`). Consumers are `local.skills.sources` in `nixos-config` and the
olive-skills catalog, both merging with `cp -rL` (`flake.nix:344-346`).

`SKILL.md` already has a YAML frontmatter block (`:1-8`: `name`, `description`,
`version`, `license`, `compatibility`, `tags`) but **no `type:` key** — the one
frontmatter gap in the repo.

## .memories/ — single agent note

`.memories/cli-grammar-gate.md`, 1.3 KB. Not referenced by any tracked file or
workflow; it is tooling state for agents working in this repo. Note that
`.gitignore` does not exclude `.memories/`, and the file is tracked.

## Repo-wide markdown audit (OKF v0.2)

Every tracked `.md` file (`git ls-files '*.md'` → **22 files**) and its OKF
conformance status. Audited read-only; nothing outside `doc/structure/` was
modified.

### Conforming

None. **Zero of 22** tracked markdown files satisfy OKF v0.2.

### Violating

| # | File | Failure |
| --- | --- | --- |
| 1 | `README.md` | no frontmatter |
| 2 | `CHANGELOG.md` | no frontmatter |
| 3 | `.memories/cli-grammar-gate.md` | no frontmatter |
| 4 | `doc/testing-microvims.md` | no frontmatter |
| 5 | `examples/README.md` | no frontmatter |
| 6 | `examples/cli/README.md` | no frontmatter |
| 7 | `nix/vms/README.md` | no frontmatter |
| 8 | `nix/vms/keys/README.md` | no frontmatter |
| 9 | `skills/consortium/references/cli-surface.md` | no frontmatter |
| 10 | `crates/consortium-py/PLAN.md` | no frontmatter |
| 11 | `crates/consortium/CHANGELOG.md` | no frontmatter |
| 12 | `crates/consortium-ansible/CHANGELOG.md` | no frontmatter |
| 13 | `crates/consortium-cli/CHANGELOG.md` | no frontmatter |
| 14 | `crates/consortium-fanout-sim/CHANGELOG.md` | no frontmatter |
| 15 | `crates/consortium-integration/CHANGELOG.md` | no frontmatter |
| 16 | `crates/consortium-integration-testkit/CHANGELOG.md` | no frontmatter |
| 17 | `crates/consortium-nix/CHANGELOG.md` | no frontmatter |
| 18 | `crates/consortium-py/CHANGELOG.md` | no frontmatter |
| 19 | `crates/consortium-ray/CHANGELOG.md` | no frontmatter |
| 20 | `crates/consortium-skypilot/CHANGELOG.md` | no frontmatter |
| 21 | `crates/consortium-slurm/CHANGELOG.md` | no frontmatter |
| 22 | `skills/consortium/SKILL.md` | frontmatter present, but **no non-empty `type:` key** |

Notes for whoever remediates:

- No file is named `index.md` or `log.md`, so the reserved-name rule is not
  triggered anywhere. Adding `doc/structure/index.md` introduces the only
  reserved name in the tree, and it correctly carries `okf_version: "0.2"`.
- The nine per-crate `CHANGELOG.md` files and the root `CHANGELOG.md` are
  **generated by git-cliff** (`release-plz.toml:2-3`, `cliff.toml:29-30`
  sets the `# Changelog` header). Adding frontmatter to them would be undone by
  the next release; the generator config, not the files, is the fix.
- `skills/consortium/SKILL.md` needs only a `type:` line added to its existing
  block. Note `flake.nix:356` parses its frontmatter with
  `sed -n 's/^name:[[:space:]]*//p'`, so the `name:` key must stay top-level
  and unindented.
- The vendored `doc/sphinx/**` tree contains no `.md` files (it is
  reStructuredText), so the vendored subtree contributes **zero** entries to
  this audit.

## Related

- [documentation](/documentation.md) — the docs workflows in detail
- [workspace-crates](/workspace-crates.md) — what CI builds and tests
- [nix](/nix.md) — the `nix-gates` job's subject
