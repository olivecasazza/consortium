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

# Measure against this fork's origin/master — the same ref agents fork
# from. Falls back to consortium/master then local master.
BASE_REMOTE=origin
git remote get-url "$BASE_REMOTE" >/dev/null 2>&1 || BASE_REMOTE=consortium
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

# Optional perf baselines:
#  - perf.{flat_33_ns,chain_33_ns}                    — dag_executor microbench
#  - perf_cascade.{uniform,bimodal}_256.<strategy_ns> — cascade strategies bench
# Each is conditional on its bench file being present. When absent we emit
# null and the corresponding score gate refuses to PASS (avoids no-signal
# false-positives).
PERF_JSON=null
PERF_CASCADE_JSON=null

if [[ -f "$WORKDIR/crates/consortium/benches/dag_executor.rs" ]]; then
    echo "computing dag_executor baseline (cargo bench --quick)..."
    BENCH_LOG=$(mktemp)
    if timeout 120 cargo bench -p consortium-crate --bench dag_executor -- \
            '^dag_executor/(flat|chain)/33$' --quick >"$BENCH_LOG" 2>&1; then
        FLAT_EST="$WORKDIR/target/criterion/dag_executor/flat/33/new/estimates.json"
        CHAIN_EST="$WORKDIR/target/criterion/dag_executor/chain/33/new/estimates.json"
        if [[ -f "$FLAT_EST" && -f "$CHAIN_EST" ]] && command -v jq >/dev/null 2>&1; then
            FLAT_NS=$(jq -r '.mean.point_estimate' "$FLAT_EST")
            CHAIN_NS=$(jq -r '.mean.point_estimate' "$CHAIN_EST")
            PERF_JSON=$(printf '{ "flat_33_ns": %.0f, "chain_33_ns": %.0f }' "$FLAT_NS" "$CHAIN_NS")
            echo "perf baseline: flat=${FLAT_NS}ns chain=${CHAIN_NS}ns"
        else
            echo "WARN: bench ran but estimates.json/jq missing — perf baseline left null" >&2
            tail -n 20 "$BENCH_LOG" >&2
        fi
    else
        echo "WARN: cargo bench (dag_executor) failed — perf baseline left null" >&2
        tail -n 20 "$BENCH_LOG" >&2
    fi
    rm -f "$BENCH_LOG"
else
    echo "perf baseline: dag_executor bench file not present — leaving perf=null"
fi

if [[ -f "$WORKDIR/crates/consortium-fanout-sim/benches/cascade_strategies.rs" ]]; then
    echo "computing cascade_strategies baseline (cargo bench --quick)..."
    BENCH_LOG=$(mktemp)
    if timeout 240 cargo bench -p consortium-fanout-sim --bench cascade_strategies -- \
            '^cascade_strategies/(uniform|bimodal)/256/' --quick >"$BENCH_LOG" 2>&1; then
        if command -v jq >/dev/null 2>&1; then
            # Collect per-(topo, strategy) means.
            tmp_json=$(mktemp)
            printf '{ "uniform_256": {' > "$tmp_json"
            first=1
            for strat in log2-fanout max-bottleneck-spanning steiner-greedy; do
                est="$WORKDIR/target/criterion/cascade_strategies/uniform/256/$strat/new/estimates.json"
                if [[ -f "$est" ]]; then
                    val=$(jq -r '.mean.point_estimate' "$est")
                    [[ $first -eq 0 ]] && printf ', ' >> "$tmp_json"
                    printf '"%s": %.0f' "$strat" "$val" >> "$tmp_json"
                    first=0
                fi
            done
            printf '}, "bimodal_256": {' >> "$tmp_json"
            first=1
            for strat in log2-fanout max-bottleneck-spanning steiner-greedy; do
                est="$WORKDIR/target/criterion/cascade_strategies/bimodal/256/$strat/new/estimates.json"
                if [[ -f "$est" ]]; then
                    val=$(jq -r '.mean.point_estimate' "$est")
                    [[ $first -eq 0 ]] && printf ', ' >> "$tmp_json"
                    printf '"%s": %.0f' "$strat" "$val" >> "$tmp_json"
                    first=0
                fi
            done
            printf '} }' >> "$tmp_json"
            PERF_CASCADE_JSON=$(cat "$tmp_json")
            rm -f "$tmp_json"
            echo "perf_cascade baseline: $PERF_CASCADE_JSON"
        else
            echo "WARN: jq missing — perf_cascade baseline left null" >&2
        fi
    else
        echo "WARN: cargo bench (cascade_strategies) failed — perf_cascade baseline left null" >&2
        tail -n 20 "$BENCH_LOG" >&2
    fi
    rm -f "$BENCH_LOG"
else
    echo "perf_cascade baseline: cascade_strategies bench file not present — leaving null"
fi

cd "$REPO_ROOT"
{
    printf '{\n'
    printf '  "master_sha": "%s",\n' "$BASE_SHA"
    printf '  "tests_passing": %d,\n' "$TESTS_PASSING"
    printf '  "clippy_errors": %d,\n' "$CLIPPY_ERRORS"
    printf '  "perf": %s,\n' "$PERF_JSON"
    printf '  "perf_cascade": %s,\n' "$PERF_CASCADE_JSON"
    printf '  "measured_at": "%s"\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '}\n'
} > "$OUT"

cat "$OUT"
