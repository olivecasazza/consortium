#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: semver-check.sh <base-commit>" >&2
  exit 2
fi
if [ ! -f Cargo.toml ]; then
  echo "semver-check: run from the consortium workspace root" >&2
  exit 1
fi

baseline_rev="$1"
mapfile -t published_libs < <(
  cargo metadata --no-deps --format-version 1 |
    jq -r '.packages[] | select(.manifest_path | contains("/crates/")) | select(.name != "consortium-py") | .name'
)
test "${#published_libs[@]}" -gt 0

status=0
for package in "${published_libs[@]}"; do
  echo "=== cargo semver-checks -p $package (git baseline: $baseline_rev; API release type: minor)"
  if ! cargo semver-checks -p "$package" --baseline-rev "$baseline_rev" --release-type minor; then
    echo "semver-check: $package FAILED API compatibility against $baseline_rev" >&2
    status=1
  fi
  echo "=== cargo semver-checks -p $package (baseline: crates.io)"
  if ! cargo semver-checks -p "$package"; then
    echo "semver-check: $package FAILED crates.io version check" >&2
    status=1
  fi
done
exit "$status"
