#!/usr/bin/env bash
# score-perf-cascade.sh — perf-cascade-strategy differential gate.
#
# Dispatched by score.sh when AR_TASK_TYPE=perf-cascade-strategy.
# Standalone usage: bash score-perf-cascade.sh [<worktree>]
#
# Pass iff:
#   - the cascade_strategies bench compiles + runs (--quick)
#   - the strategy under optimization improves bimodal/256 wall-time
#     by >= 5% vs baseline AND uniform/256 doesn't regress > 10%.
#
# The "strategy under optimization" defaults to max-bottleneck-spanning
# (the most-tuneable strategy). Override via AR_CASCADE_STRATEGY env.

set -uo pipefail

WORKTREE="${1:-$PWD}"
cd "$WORKTREE" || { echo "score-perf-cascade: cannot cd to $WORKTREE" >&2; exit 2; }

MAIN_REPO="$(git worktree list --porcelain | head -1 | awk '{print $2}')"
BASELINE="$MAIN_REPO/autoresearch/.baseline.json"

if [[ ! -f "$BASELINE" ]]; then
    echo "FAIL  perf-cascade (no baseline; run autoresearch/scripts/compute-baseline.sh --force)" >&2
    exit 5
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "FAIL  perf-cascade (jq missing)" >&2
    exit 5
fi

STRATEGY="${AR_CASCADE_STRATEGY:-max-bottleneck-spanning}"
# jq path: .perf_cascade.bimodal_256["<strategy>"]
BASE_BIMODAL=$(jq -er ".perf_cascade.bimodal_256[\"$STRATEGY\"] // empty" "$BASELINE" 2>/dev/null || true)
BASE_UNIFORM=$(jq -er ".perf_cascade.uniform_256[\"$STRATEGY\"] // empty" "$BASELINE" 2>/dev/null || true)

if [[ -z "$BASE_BIMODAL" || -z "$BASE_UNIFORM" ]]; then
    echo "FAIL  perf-cascade (baseline lacks .perf_cascade.{bimodal,uniform}_256.$STRATEGY — run compute-baseline.sh --force)" >&2
    exit 5
fi

if [[ ! -f "$WORKTREE/crates/consortium-fanout-sim/benches/cascade_strategies.rs" ]]; then
    echo "FAIL  perf-cascade (cascade_strategies bench missing)" >&2
    exit 3
fi

TMP=$(mktemp -d -t ar-perf-cascade.XXXXXX)
trap 'rm -rf "$TMP"' EXIT

echo "score-perf-cascade: running cargo bench --quick (300s budget — first run includes full sim+bench build)..." >&2
if ! timeout 300 cargo bench -p consortium-fanout-sim --bench cascade_strategies -- \
        "^cascade_strategies/(uniform|bimodal)/256/$STRATEGY\$" --quick \
        >"$TMP/bench.log" 2>&1; then
    EXIT=$?
    echo "FAIL  perf-cascade (cargo bench exit $EXIT)" >&2
    tail -n 50 "$TMP/bench.log" >&2
    exit "$EXIT"
fi

# Criterion writes estimates per (group, parameter): the strategy name
# in this case is the parameter, and group is "cascade_strategies/<topo>/<n>".
BIMODAL_EST="$WORKTREE/target/criterion/cascade_strategies/bimodal/256/$STRATEGY/new/estimates.json"
UNIFORM_EST="$WORKTREE/target/criterion/cascade_strategies/uniform/256/$STRATEGY/new/estimates.json"

if [[ ! -f "$BIMODAL_EST" || ! -f "$UNIFORM_EST" ]]; then
    echo "FAIL  perf-cascade (bench exit 0 but estimates.json missing)" >&2
    echo "  expected: $BIMODAL_EST" >&2
    echo "  expected: $UNIFORM_EST" >&2
    tail -n 30 "$TMP/bench.log" >&2
    exit 4
fi

BRANCH_BIMODAL=$(jq -r '.mean.point_estimate' "$BIMODAL_EST")
BRANCH_UNIFORM=$(jq -r '.mean.point_estimate' "$UNIFORM_EST")

PASS=$(awk -v bb="$BRANCH_BIMODAL" -v base_b="$BASE_BIMODAL" \
           -v bu="$BRANCH_UNIFORM" -v base_u="$BASE_UNIFORM" '
    BEGIN {
        bimodal_thresh = base_b * 0.95
        uniform_thresh = base_u * 1.10
        bimodal_ok = (bb <= bimodal_thresh)
        uniform_ok = (bu <= uniform_thresh)
        bimodal_pct = (bb - base_b) / base_b * 100
        uniform_pct = (bu - base_u) / base_u * 100
        printf "%s|%.0f|%.0f|%.1f|%.1f|%s|%s\n",
            (bimodal_ok && uniform_ok ? "PASS" : "FAIL"),
            bb, bu, bimodal_pct, uniform_pct,
            (bimodal_ok ? "+" : "-"), (uniform_ok ? "+" : "-")
    }
')

VERDICT=${PASS%%|*}
REST=${PASS#*|}
IFS='|' read -r BIMODAL_OUT UNIFORM_OUT BIMODAL_PCT UNIFORM_PCT BIMODAL_OK UNIFORM_OK <<< "$REST"

MSG="$VERDICT  perf-cascade [$STRATEGY] (bimodal: ${BASE_BIMODAL%.*} -> ${BIMODAL_OUT} ns, ${BIMODAL_PCT}%; uniform: ${BASE_UNIFORM%.*} -> ${UNIFORM_OUT} ns, ${UNIFORM_PCT}%)"
if [[ "$VERDICT" == "PASS" ]]; then
    echo "$MSG"
    exit 0
else
    echo "$MSG" >&2
    [[ "$BIMODAL_OK" == "-" ]] && echo "  bimodal didn't improve >=5% (need <= ${BASE_BIMODAL} * 0.95)" >&2
    [[ "$UNIFORM_OK" == "-" ]] && echo "  uniform regressed > 10% (need <= ${BASE_UNIFORM} * 1.10)" >&2
    exit 1
fi
