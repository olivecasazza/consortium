#!/usr/bin/env bash
# score.sh — differential fitness gate for an autoresearch task.
#
# Usage: bash score.sh [<worktree>]
# Default worktree is $PWD.
#
# A run passes iff, relative to the master baseline in
# autoresearch/.baseline.json:
#   - cargo fmt --check passes (absolute)
#   - tests_passing(branch) >= tests_passing(master)
#   - clippy_errors(branch) <= clippy_errors(master)
#   - if diff touches lib/ or tests/*.py: pytest -x passes (absolute)
#
# This means agents are not penalized for pre-existing clippy noise, but
# they cannot regress test counts or add new clippy errors.
#
# Exit 0 on pass, non-zero otherwise. Stdout: human summary. Stderr:
# tail of any failing tool's output.
set -uo pipefail

WORKTREE="${1:-$PWD}"
cd "$WORKTREE" || { echo "score: cannot cd to $WORKTREE" >&2; exit 2; }

# Find the main repo (worktrees share one .git dir; the baseline file
# lives in the main checkout).
MAIN_REPO="$(git worktree list --porcelain | head -1 | awk '{print $2}')"
BASELINE="$MAIN_REPO/autoresearch/.baseline.json"
if [[ ! -f "$BASELINE" ]]; then
    echo "score: $BASELINE missing — run autoresearch/scripts/compute-baseline.sh first" >&2
    exit 9
fi

BASE_TESTS=$(awk -F'[:,]' '/"tests_passing"/{gsub(/[[:space:]]/,"",$2); print $2; exit}' "$BASELINE")
BASE_CLIPPY=$(awk -F'[:,]' '/"clippy_errors"/{gsub(/[[:space:]]/,"",$2); print $2; exit}' "$BASELINE")
: "${BASE_TESTS:=0}"
: "${BASE_CLIPPY:=999}"

TMP=$(mktemp -d -t ar-score.XXXXXX)
trap 'rm -rf "$TMP"' EXIT
FAIL=0
SUMMARY=""

# Gate 1 (absolute): cargo fmt --check
if cargo fmt --all -- --check >"$TMP/fmt.log" 2>&1; then
    SUMMARY+="PASS  fmt"$'\n'
else
    SUMMARY+="FAIL  fmt"$'\n'
    echo "=== FAIL: fmt ===" >&2
    tail -n 50 "$TMP/fmt.log" >&2
    FAIL=$((FAIL + 1))
fi

# Gate 2 (differential): cargo clippy. We don't error on warnings — we
# count them and compare to baseline.
cargo clippy --workspace --all-targets -- -D warnings >"$TMP/clippy.log" 2>&1 || true
BRANCH_CLIPPY=$(grep -cE '^error(\[|: )' "$TMP/clippy.log" 2>/dev/null || true)
BRANCH_CLIPPY=${BRANCH_CLIPPY:-0}
if [[ "$BRANCH_CLIPPY" -le "$BASE_CLIPPY" ]]; then
    SUMMARY+="PASS  clippy ($BRANCH_CLIPPY <= $BASE_CLIPPY baseline)"$'\n'
else
    SUMMARY+="FAIL  clippy ($BRANCH_CLIPPY > $BASE_CLIPPY baseline)"$'\n'
    echo "=== FAIL: clippy regression ($BRANCH_CLIPPY new vs $BASE_CLIPPY baseline) ===" >&2
    grep -E '^error' "$TMP/clippy.log" | tail -n 20 >&2
    FAIL=$((FAIL + 1))
fi

# Gate 3 (differential): cargo nextest test count.
cargo nextest run --workspace --no-fail-fast --status-level pass >"$TMP/test.log" 2>&1 || true
BRANCH_TESTS=$(grep -oE '[0-9]+ passed' "$TMP/test.log" | awk '{s+=$1} END {print s+0}')
if [[ "$BRANCH_TESTS" -ge "$BASE_TESTS" ]]; then
    SUMMARY+="PASS  test ($BRANCH_TESTS >= $BASE_TESTS baseline)"$'\n'
else
    SUMMARY+="FAIL  test regression ($BRANCH_TESTS < $BASE_TESTS baseline)"$'\n'
    echo "=== FAIL: test regression ===" >&2
    tail -n 50 "$TMP/test.log" >&2
    FAIL=$((FAIL + 1))
fi

# Gate 4 (absolute, conditional): pytest if Python paths touched.
BASE_REMOTE=consortium
git -C "$WORKTREE" remote get-url "$BASE_REMOTE" >/dev/null 2>&1 || BASE_REMOTE=origin
BASE_REF="$BASE_REMOTE/master"
git -C "$WORKTREE" rev-parse --verify "$BASE_REF" >/dev/null 2>&1 || BASE_REF=master

if git diff --name-only "$BASE_REF"...HEAD 2>/dev/null | grep -qE '^(lib/|tests/.*\.py$)'; then
    if command -v pytest >/dev/null 2>&1; then
        if pytest tests/ -v --timeout=30 -x >"$TMP/pytest.log" 2>&1; then
            SUMMARY+="PASS  pytest"$'\n'
        else
            SUMMARY+="FAIL  pytest"$'\n'
            echo "=== FAIL: pytest ===" >&2
            tail -n 50 "$TMP/pytest.log" >&2
            FAIL=$((FAIL + 1))
        fi
    else
        SUMMARY+="SKIP  pytest (not installed)"$'\n'
    fi
fi

echo "$SUMMARY"
echo "tests_passed=$BRANCH_TESTS clippy_errors=$BRANCH_CLIPPY"
exit "$FAIL"
