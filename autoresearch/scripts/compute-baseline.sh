#!/usr/bin/env bash
# compute-baseline.sh — record master's test/clippy counts for differential scoring.
#
# Usage: bash compute-baseline.sh [--force]
#
# Writes autoresearch/.baseline.json with:
#   - master_sha: the sha we measured against
#   - tests_passing: count of passing tests on master
#   - clippy_errors: count of clippy errors at -D warnings on master
#   - measured_at: ISO timestamp
#
# Runs cargo nextest + cargo clippy against master in a temp worktree so
# the main checkout isn't disturbed. Skip if .baseline.json already exists
# and master_sha matches HEAD, unless --force.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

OUT="$REPO_ROOT/autoresearch/.baseline.json"
FORCE=0
[[ "${1:-}" == "--force" ]] && FORCE=1

# Use the consortium remote's master if it exists (so we measure against
# the upstream-of-fork, not whatever master is locally). Falls back to
# local master.
BASE_REMOTE=consortium
git remote get-url "$BASE_REMOTE" >/dev/null 2>&1 || BASE_REMOTE=origin
BASE_REF="$BASE_REMOTE/master"
git rev-parse --verify "$BASE_REF" >/dev/null 2>&1 || BASE_REF=master
BASE_SHA=$(git rev-parse "$BASE_REF")

if [[ -f "$OUT" && $FORCE -eq 0 ]]; then
    PREV_SHA=$(awk -F'"' '/"master_sha"/{print $4; exit}' "$OUT")
    if [[ "$PREV_SHA" == "$BASE_SHA" ]]; then
        echo "baseline current (master_sha=$BASE_SHA), skipping"
        cat "$OUT"
        exit 0
    fi
fi

WORKDIR=$(mktemp -d -t ar-baseline.XXXXXX)
trap 'rm -rf "$WORKDIR"; git worktree prune --quiet 2>/dev/null || true' EXIT

git worktree add --detach "$WORKDIR" "$BASE_SHA" >/dev/null
cd "$WORKDIR"

echo "computing baseline at $BASE_SHA in $WORKDIR..."

# Test count. nextest exit code is 0 even if some tests pass — match
# "<N> passed" line in summary. Use --no-fail-fast and || true so the
# whole script doesn't abort on test failures (we want the count anyway).
TEST_LOG=$(mktemp)
cargo nextest run --workspace --no-fail-fast --status-level pass 2>&1 \
    | tee "$TEST_LOG" \
    | tail -5 || true
TESTS_PASSING=$(grep -oE '[0-9]+ passed' "$TEST_LOG" | awk '{s+=$1} END {print s+0}')
rm -f "$TEST_LOG"

# Clippy error count.
CLIPPY_LOG=$(mktemp)
cargo clippy --workspace --all-targets -- -D warnings >"$CLIPPY_LOG" 2>&1 || true
CLIPPY_ERRORS=$(grep -cE '^error(\[|: )' "$CLIPPY_LOG" 2>/dev/null || true)
CLIPPY_ERRORS=${CLIPPY_ERRORS:-0}
rm -f "$CLIPPY_LOG"

cd "$REPO_ROOT"
{
    printf '{\n'
    printf '  "master_sha": "%s",\n' "$BASE_SHA"
    printf '  "tests_passing": %d,\n' "$TESTS_PASSING"
    printf '  "clippy_errors": %d,\n' "$CLIPPY_ERRORS"
    printf '  "measured_at": "%s"\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '}\n'
} > "$OUT"

cat "$OUT"
