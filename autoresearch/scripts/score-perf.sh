#!/usr/bin/env bash
# score-perf.sh — perf-dag-executor differential gate.
#
# Sourced (or invoked) by score.sh when AR_TASK_TYPE=perf-dag-executor.
# Standalone usage: bash score-perf.sh [<worktree>]
#
# Pass iff:
#   - the dag_executor bench compiles + runs (--quick)
#   - flat_33_ns(branch) <= flat_33_ns(baseline) * 0.95   (>=5% faster)
#   - chain_33_ns(branch) <= chain_33_ns(baseline) * 1.10 (no >10% regression)
#
# Stdout: "PASS perf (flat: 360123 -> 320456 ns, -11.0% | chain: ...)"
# Stderr: tail of bench log + jq parse errors on FAIL.
#
# Bench output layout (criterion default):
#   target/criterion/dag_executor/flat/33/new/estimates.json
#   target/criterion/dag_executor/chain/33/new/estimates.json
#
# Baseline source: autoresearch/.baseline.json (written by compute-baseline.sh).
# Required keys: .perf.flat_33_ns, .perf.chain_33_ns. Missing → SKIP+exit 0
# (the bench has not landed in the fork yet — gracefully no-op until upstream
# consortium PR #4 syncs in).

set -uo pipefail

WORKTREE="${1:-$PWD}"
cd "$WORKTREE" || { echo "score-perf: cannot cd to $WORKTREE" >&2; exit 2; }

MAIN_REPO="$(git worktree list --porcelain | head -1 | awk '{print $2}')"
BASELINE="$MAIN_REPO/autoresearch/.baseline.json"

if [[ ! -f "$BASELINE" ]]; then
    echo "FAIL  perf (no baseline; run autoresearch/scripts/compute-baseline.sh --force)" >&2
    exit 5
fi

# jq is in the consortium-autoresearch nix devshell. score.sh self-activates
# the devshell before invoking us, so jq should always be present.
if ! command -v jq >/dev/null 2>&1; then
    echo "FAIL  perf (jq missing — should not happen inside nix develop)" >&2
    exit 5
fi

BASE_FLAT=$(jq -er '.perf.flat_33_ns // empty' "$BASELINE" 2>/dev/null || true)
BASE_CHAIN=$(jq -er '.perf.chain_33_ns // empty' "$BASELINE" 2>/dev/null || true)

if [[ -z "$BASE_FLAT" || -z "$BASE_CHAIN" ]]; then
    # Baseline lacks .perf — either the bench file isn't present in the fork
    # yet (compute-baseline.sh emits perf=null when missing — this fork's
    # current state pending upstream consortium PR #3 + #4 sync) or the
    # baseline pre-dates the perf wiring. Refuse to PASS — a missing perf
    # signal on a perf-dag-executor task means we have no fitness signal,
    # and finalizing on a no-signal would silently false-positive.
    # Recovery: sync upstream, then run compute-baseline.sh --force.
    echo "FAIL  perf (baseline lacks .perf — sync upstream consortium PR #3+#4, then compute-baseline.sh --force)" >&2
    exit 5
fi

# Confirm the bench actually exists in this worktree before invoking cargo.
if [[ ! -f "$WORKTREE/crates/consortium/benches/dag_executor.rs" ]]; then
    echo "FAIL  perf (benches/dag_executor.rs missing in worktree but baseline has .perf — fork is mid-sync)" >&2
    exit 3
fi

TMP=$(mktemp -d -t ar-perf.XXXXXX)
trap 'rm -rf "$TMP"' EXIT

echo "score-perf: running cargo bench --quick (180s budget — first run after worktree fork includes a full crate build)..." >&2
if ! timeout 180 cargo bench -p consortium-crate --bench dag_executor -- \
        '^dag_executor/(flat|chain)/33$' --quick >"$TMP/bench.log" 2>&1; then
    EXIT=$?
    echo "FAIL  perf (cargo bench exit $EXIT)" >&2
    tail -n 50 "$TMP/bench.log" >&2
    exit "$EXIT"
fi

FLAT_EST="$WORKTREE/target/criterion/dag_executor/flat/33/new/estimates.json"
CHAIN_EST="$WORKTREE/target/criterion/dag_executor/chain/33/new/estimates.json"

if [[ ! -f "$FLAT_EST" || ! -f "$CHAIN_EST" ]]; then
    echo "FAIL  perf (bench exit 0 but estimates.json missing)" >&2
    echo "  expected: $FLAT_EST" >&2
    echo "  expected: $CHAIN_EST" >&2
    tail -n 30 "$TMP/bench.log" >&2
    exit 4
fi

BRANCH_FLAT=$(jq -r '.mean.point_estimate' "$FLAT_EST")
BRANCH_CHAIN=$(jq -r '.mean.point_estimate' "$CHAIN_EST")

# Awk for the float comparison — bash can't, and the consortium-autoresearch
# devshell doesn't necessarily have bc.
PASS=$(awk -v bf="$BRANCH_FLAT" -v base_f="$BASE_FLAT" \
           -v bc="$BRANCH_CHAIN" -v base_c="$BASE_CHAIN" '
    BEGIN {
        flat_thresh  = base_f * 0.95
        chain_thresh = base_c * 1.10
        flat_ok  = (bf <= flat_thresh)
        chain_ok = (bc <= chain_thresh)
        flat_pct  = (bf  - base_f) / base_f * 100
        chain_pct = (bc - base_c) / base_c * 100
        printf "%s|%.0f|%.0f|%.1f|%.1f|%s|%s\n",
            (flat_ok && chain_ok ? "PASS" : "FAIL"),
            bf, bc, flat_pct, chain_pct,
            (flat_ok ? "+" : "-"), (chain_ok ? "+" : "-")
    }
')

VERDICT=${PASS%%|*}
REST=${PASS#*|}
IFS='|' read -r FLAT_OUT CHAIN_OUT FLAT_PCT CHAIN_PCT FLAT_OK CHAIN_OK <<< "$REST"

MSG="$VERDICT  perf (flat: ${BASE_FLAT%.*} -> ${FLAT_OUT} ns, ${FLAT_PCT}%; chain: ${BASE_CHAIN%.*} -> ${CHAIN_OUT} ns, ${CHAIN_PCT}%)"
if [[ "$VERDICT" == "PASS" ]]; then
    echo "$MSG"
    exit 0
else
    echo "$MSG" >&2
    [[ "$FLAT_OK"  == "-" ]] && echo "  flat regressed (need <= ${BASE_FLAT} * 0.95 = $(awk -v x="$BASE_FLAT" 'BEGIN{printf "%.0f", x*0.95}'))" >&2
    [[ "$CHAIN_OK" == "-" ]] && echo "  chain regressed > 10% (need <= ${BASE_CHAIN} * 1.10 = $(awk -v x="$BASE_CHAIN" 'BEGIN{printf "%.0f", x*1.10}'))" >&2
    exit 1
fi
