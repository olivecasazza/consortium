---
type: concept
title: Documentation side — vendored ClusterShell tree and publishing
description: doc/ is an upstream ClusterShell sphinx tree, not project documentation; docs-index/ is the rustdoc version landing page.
resource: https://github.com/olivecasazza/consortium
tags: [structure, documentation, sphinx, vendored, clustershell]
generated:
  by: opencode/claude-code
  at: "2026-09-28T19:43:12-07:00"
status: draft
stale_after: "2026-12-28T00:00:00-07:00"
---

# Documentation side — vendored ClusterShell tree and publishing

## Purpose

Two unrelated things live under "documentation":

1. `doc/` — the **vendored upstream ClusterShell Sphinx tree**, which documents
   the *Python library being replaced*, not this Rust project.
2. `docs-index/` — a single static `index.html` that is the landing page for
   the **rustdoc** output this project actually publishes.

## Language / build system

Sphinx + reStructuredText for `doc/`; plain HTML/JavaScript for `docs-index/`.

## doc/ — vendored from ClusterShell (cea-hpc/clustershell)

63 tracked files (`git ls-files 'doc/*' | wc -l` → 63).

```
doc/sphinx/    conf.py, index.rst, guide/, api/, requirements.txt, Makefile, _static/
doc/txt/       clustershell.rst and friends
doc/man/       manpage sources
doc/epydoc/    generated API stubs
doc/extras/    auxiliary material
doc/examples/  example material
doc/testing-microvims.md   ← NOT upstream; authored here
doc/structure/             ← this bundle; authored here
```

### How to tell it is vendored

- `doc/sphinx/conf.py:3-4` is the stock sphinx-quickstart banner:
  `# clustershell documentation build configuration file, created by sphinx-quickstart on Mon Jul 13 20:46:35 2015.`
- `doc/sphinx/conf.py:19` inserts `../../lib` onto `sys.path` for the upstream
  Python package — a path that does not exist in this repo.
- Git history carries upstream ClusterShell commits verbatim: `2d951cbe
  doc/txt: fix clustershell.rst for pypi (#577)`, `5c3dccca Bump pillow from
  5.4.1 to 10.3.0 in /doc/sphinx (#576)`, `5c4e508e Release 1.9.3 (#574)`.
- `README.md:168` points readers at it as "The Sphinx documentation source",
  alongside the upstream project links at `README.md:240-262`.

### How it is synced

**There is no automated sync. The tree is fetched by hand from a configured
`upstream` remote and merged as ordinary commits.**

- The repository has an `upstream` remote pointing at
  `https://github.com/cea-hpc/clustershell.git` (alongside
  `origin` → `olivecasazza/consortium`). That is how the vendored tree is
  brought forward: `git fetch upstream` and merge or copy the relevant paths.
- There is **no** `.gitmodules` and no git-subtree registration, so
  `doc/sphinx` is plain tracked files — not a link that git can rebase onto
  upstream. Nothing in CI performs the fetch.
- No script or workflow re-imports it. The vendored tree is also excluded from
  the two code workflows: `.github/workflows/ci.yml:6-9` and
  `.github/workflows/migration-scorecard.yml:6-10` both list `doc/**` under
  `paths-ignore`. `.readthedocs.yaml` is its only automated consumer.
- The re-imports so far were manual and lossy. Commit `5bdad294` ("chore:
  relocate ClusterShell fork packaging files to packaging/ and doc/legacy/")
  and `30cd168c` ("chore: remove legacy ClusterShell fork files migrated to
  consortium-tests") pruned upstream files as the rewrite progressed, so the
  copy no longer matches upstream one-to-one.

Practical consequence: `doc/sphinx/**` is hand-maintained on top of a drifting
upstream copy, and no test covers it. Treat the subtree as read-only history.
Project documentation is the markdown at the repo root, `nix/vms/README.md`,
and the rustdoc published from the crates.

### Wired into readthedocs

```yaml
# .readthedocs.yaml
version: 2                                    # :1
build: { os: ubuntu-24.04, tools: { python: "3.12" } }   # :2-5
sphinx:
  configuration: doc/sphinx/conf.py           # :8
  fail_on_warning: true                        # :9
python:
  install:
  - requirements: doc/sphinx/requirements.txt  # :13
```

`fail_on_warning: true` at `.readthedocs.yaml:9` means any upstream doc warning
now breaks the RTD build. This is the only automated consumer of the vendored
tree.

## docs-index/ — rustdoc version landing page

One file, `docs-index/index.html` (86 lines).

- Static HTML with inline CSS (`:6-23`) and a hardcoded crate grid
  (`:36-68`) linking to `latest/<crate>/` rustdoc pages.
- `:31` seeds a single `latest` entry for master.
- `:70-84` runs at page load: `fetch('https://api.github.com/repos/olivecasazza/consortium/tags')`,
  filters names starting with `v` (`:73`), takes the 20 most recent (`:74`),
  and appends one list item per tag (`:75-79`). Failures are swallowed
  (`:83`, `.catch(() => {})`).
- Deployed as the gh-pages **root** by `.github/workflows/docs.yml:59-65`
  (`publish_dir: ./docs-index`, `destination_dir: .`).

Consequence: the version list is populated at read time from the GitHub API, so
it degrades to the single hardcoded `latest` row when rate-limited or offline.

## Rustdoc publishing

`.github/workflows/docs.yml` builds and deploys versioned rustdoc:

| Step | Citation |
| --- | --- |
| `cargo doc --workspace --no-deps` with `-D warnings` | `.github/workflows/docs.yml:24-27` |
| Redirect shim at `target/doc/index.html` | `.github/workflows/docs.yml:28-29` |
| Version resolution (`release` event vs `latest`) | `.github/workflows/docs.yml:31-40` |
| Publish to `/<version>/` | `.github/workflows/docs.yml:42-48` |
| Also update `/latest/` on release | `.github/workflows/docs.yml:50-57` |
| Publish `docs-index/` at root | `.github/workflows/docs.yml:59-65` |

Release events deploy docs directly from `.github/workflows/release.yml:32-43`
because `GITHUB_TOKEN` pushes do not trigger other workflows
(`.github/workflows/docs.yml:7-8`).

## Spelling

`.spelling-dict` at the repo root is a word list for prose checks. It applies to
the authored markdown, not to the vendored `doc/` tree.

## Related

- [ci-tooling](/ci-tooling.md) — the workflow set in detail
- [workspace-crates](/workspace-crates.md) — the crates whose rustdoc is published
