# Conventional Commits and Semantic Versioning Guide

This guide explains how to use conventional commits and semantic versioning for automated releases in the consortium repository.

## Overview

The consortium repository uses:
- **Conventional Commits** to enforce standardized commit messages
- **Semantic Versioning (SemVer)** for automatic version bumping
- **GitHub Actions** for CI/CD pipelines
- **semantic-release** for automated releases

## Commit Message Format

All commits must follow the [conventional commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <subject>
```

### Types

| Type | Description | Version Bump |
|------|-------------|--------------|
| `feat` | New feature | MINOR (x.Y.z) |
| `fix` | Bug fix | PATCH (x.y.Z) |
| `perf` | Performance improvement | PATCH (x.y.Z) |
| `refactor` | Code refactoring | PATCH (x.y.Z) |
| `docs` | Documentation changes | None |
| `style` | Code style changes (formatting) | None |
| `test` | Adding or updating tests | None |
| `chore` | Maintenance tasks | None |
| `ci` | CI/CD changes | None |
| `build` | Build system changes | Depends |
| `revert` | Revert previous commit | Depends |

### Breaking Changes

Breaking changes must be indicated with `BREAKING CHANGE:` in the commit footer:

```bash
git commit -m "feat: remove deprecated API" -m "BREAKING CHANGE: The old API has been removed. Use new API instead."
```

This will bump the MAJOR version (X.y.z).

### Examples

```bash
# New feature
git commit -m "feat: add new clustering algorithm"

# Bug fix
git commit -m "fix: resolve memory leak in cluster handling"

# Performance improvement
git commit -m "perf: optimize node grouping algorithm"

# Documentation
git commit -m "docs: update API documentation"

# Breaking change
git commit -m "feat: remove deprecated config format" -m "BREAKING CHANGE: The old config format is no longer supported. Use the new YAML format."

# Revert
git commit -m "revert: revert 'feat: add experimental feature'"
```

## Version Bumping Rules

The release workflow automatically determines the version bump based on commit messages:

1. **MAJOR** (X.0.0): If any commit contains `BREAKING CHANGE:`
2. **MINOR** (x.Y.0): If any commit is `feat:`
3. **PATCH** (x.y.Z): If any commit is `fix:`, `perf:`, or `refactor:`
4. **No release**: If no relevant commits are found

## CI/CD Workflows

### 1. Commit Linting (`.github/workflows/release.yml`)

All commits are linted against conventional commits format in the CI pipeline.

### 2. Release Workflow

The release workflow is triggered when commits are pushed to `main` branch:

1. **Lint commits** - Validate conventional commit format
2. **Test and version check** - Run tests and determine version bump
3. **Build Rust** - Build and test Rust crate
4. **Release** - Create GitHub release and publish to crates.io

### 3. Version Detection

The workflow analyzes commits since the last tag to determine the version bump:

- `BREAKING CHANGE:` → MAJOR version
- `feat:` → MINOR version
- `fix:`, `perf:`, `refactor:` → PATCH version
- No relevant commits → No release

## Setup Instructions

### For Developers

1. **Ensure Node.js 20+ is installed** (required for commitlint):

```bash
# Check Node.js version
node --version

# If using nvm
nvm install 20
nvm use 20
```

2. **Install commitizen** (optional, for guided commits):

```bash
npm install -g commitizen
```

3. **Use conventional commit messages**:

```bash
# Manual format (recommended)
git commit -m "feat: add new feature"
git commit -m "fix: resolve bug in module"

# Or use commitizen (if installed)
git cz

# Or use the verify script
./scripts/verify-commits.sh
```

4. **Push to main** - When merged, the release workflow will automatically:
   - Validate commit messages
   - Run all tests
   - Determine version bump
   - Create GitHub release with changelog
   - Publish to crates.io

### Required Secrets

The following secrets must be configured in GitHub repository settings:

1. **CRATES_IO_TOKEN** - API token for publishing to crates.io
2. **GITHUB_TOKEN** - Automatically provided by GitHub Actions

## Configuration Files

- `.commitlintrc.yml` - Commitlint configuration
- `.github/workflows/release.yml` - Release workflow
- `.github/workflows/nosetests.yml` - Python tests
- `.github/workflows/migration-scorecard.yml` - Scorecard workflow
- `package.json` - Node.js dependencies and semantic-release config
- `CONVENTIONAL_COMMITS.md` - Detailed documentation

## Changelog Generation

The changelog is automatically generated from commit messages using semantic-release. It includes:

- Breaking changes (MAJOR)
- New features (MINOR)
- Bug fixes and improvements (PATCH)

The changelog is stored in `CHANGELOG.md`.

## Troubleshooting

### Commit Linting Fails

If commitlint fails, check your commit message format:

```bash
# Check recent commits
git log --oneline -5

# Fix commit message
git commit --amend -m "feat: correct message format"

# Or use the verify script
./scripts/verify-commits.sh
```

### No Release Created

If no release is created, verify:

1. Commits follow conventional format
2. Branch is `main` or `master`
3. Tests passed successfully
4. Version bump is detected (feat, fix, perf, refactor, or breaking change)

### Build Fails

If the build fails, ensure:

1. Rust is up to date: `rustup update`
2. Node.js 20+ is installed: `node --version`
3. All tests pass locally

## Best Practices

1. **Write clear commit messages**: Be specific about what changed
2. **Use scopes**: Include the module/component in parentheses: `feat(cluster): add new feature`
3. **Break changes into multiple commits**: Don't combine multiple features in one commit
4. **Update documentation**: Document breaking changes in the commit message
5. **Test before merging**: Ensure all tests pass before merging to main

## References

- [Conventional Commits](https://www.conventionalcommits.org/)
- [Semantic Versioning](https://semver.org/)
- [semantic-release](https://semantic-release.gitbook.io/semantic-release/)
- [commitlint](https://commitlint.js.org/)
- [Angular Commit Message Guidelines](https://github.com/angular/angular/blob/main/contributing-docs/commit-message-guidelines.md)
