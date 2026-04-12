# Conventional Commits and Semantic Versioning Setup

This document describes the CI/CD workflow for managing releases using conventional commits and semantic versioning.

## Overview

The consortium repository uses:
- **Conventional Commits** to enforce standardized commit messages
- **Semantic Versioning (SemVer)** for automatic version bumping
- **GitHub Actions** for CI/CD pipelines
- **semantic-release** for automated releases to GitHub and crates.io

## Commit Message Format

All commits must follow the [conventional commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <subject>
```

### Types

- `feat`: New feature (bumps MINOR version)
- `fix`: Bug fix (bumps PATCH version)
- `perf`: Performance improvement (bumps PATCH version)
- `refactor`: Code refactoring (bumps PATCH version)
- `docs`: Documentation changes (no version bump)
- `style`: Code style changes (formatting, etc., no code change)
- `test`: Adding or updating tests (no version bump)
- `chore`: Maintenance tasks (no version bump)
- `ci`: CI/CD changes (no version bump)
- `build`: Build system changes
- `revert`: Revert previous commit

### Breaking Changes

Breaking changes must be indicated with `BREAKING CHANGE:` in the commit footer:

```
feat: remove deprecated API

BREAKING CHANGE: The old API has been removed. Use new API instead.
```

This will bump the MAJOR version.

## Version Bumping Rules

| Commit Type | Version Bump |
|-------------|--------------|
| `feat` | MINOR (x.Y.z) |
| `fix` | PATCH (x.y.Z) |
| `perf` | PATCH (x.y.Z) |
| `refactor` | PATCH (x.y.Z) |
| Breaking change | MAJOR (X.y.z) |
| `docs`, `style`, `test`, `chore`, `ci` | No bump |

## CI/CD Workflows

### 1. Commit Linting (`.github/workflows/release.yml`)

All commits are linted against conventional commits format:

```yaml
jobs:
  lint-commits:
    name: Lint Commit Messages
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Node.js
        uses: actions/setup-node@v4
      - name: Install commitlint
        run: npm install --save-dev @commitlint/config-conventional @commitlint/cli
      - name: Lint commit messages
        run: npx commitlint --from HEAD~1 --to HEAD --exit-code
```

### 2. Release Workflow

The release workflow is triggered on pushes to `main` branch:

1. **Lint commits** - Validate conventional commit format
2. **Test and version check** - Run tests and determine version bump
3. **Build Rust** - Build and test Rust crate
4. **Release** - Create GitHub release and publish to crates.io

### 3. Version Detection

The workflow analyzes commits since the last tag to determine the version bump:

- `BREAKING CHANGE:` → MAJOR version
- `feat:` → MINOR version
- `fix:` → PATCH version
- No relevant commits → No release

## Setup Instructions

### For Developers

1. **Install commitizen** (optional, for guided commits):

```bash
npm install -g commitizen
```

2. **Use conventional commit messages**:

```bash
# Manual format
git commit -m "feat: add new feature"
git commit -m "fix: resolve bug in module"

# Or use commitizen
git cz
```

3. **Push to main** - When merged, the release workflow will automatically:
   - Create a GitHub release
   - Generate release notes
   - Create a Git tag (vX.Y.Z)
   - Publish to crates.io

### Required Secrets

The following secrets must be configured in GitHub repository settings:

1. **CRATES_IO_TOKEN** - API token for publishing to crates.io
2. **GITHUB_TOKEN** - Automatically provided by GitHub Actions

## Configuration Files

- `.commitlintrc.yml` - Commitlint configuration
- `.github/workflows/release.yml` - Release workflow
- `.github/workflows/nosetests.yml` - Python tests (updated)
- `.github/workflows/migration-scorecard.yml` - Scorecard workflow (updated)
- `.nvmrc` - Node.js version (20)

## Release Process

1. Developer creates a branch and makes commits with conventional messages
2. PR is opened and reviewed
3. PR is merged to `main` branch
4. Release workflow automatically:
   - Validates commit messages
   - Runs all tests
   - Determines version bump
   - Creates GitHub release with changelog
   - Publishes to crates.io

## Changelog Generation

The changelog is automatically generated from commit messages using semantic-release. It includes:

- Breaking changes (MAJOR)
- New features (MINOR)
- Bug fixes (PATCH)

## Troubleshooting

### Commit Linting Fails

If commitlint fails, check your commit message format:

```bash
# Check recent commits
git log --oneline -5

# Fix commit message
git commit --amend -m "feat: correct message format"
```

### No Release Created

If no release is created, verify:

1. Commits follow conventional format
2. Branch is `main` or `master`
3. Tests passed successfully
4. Version bump is detected (feat, fix, perf, refactor, or breaking change)

## References

- [Conventional Commits](https://www.conventionalcommits.org/)
- [Semantic Versioning](https://semver.org/)
- [semantic-release](https://semantic-release.gitbook.io/semantic-release/)
- [commitlint](https://commitlint.js.org/)
