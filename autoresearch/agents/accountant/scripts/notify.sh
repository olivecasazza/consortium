#!/usr/bin/env bash
# notify.sh — write a receipt to the accountant after every agent run.
#
# Called by run-once.sh from both the abandon and finalize paths.
# Creates a bd decision bead (the accountant's persistent memory) and
# updates the running outcomes tally on the ledger row for the model used.
#
# Usage:
#   notify.sh <task_type> <model> <outcome> <agent_id> [<score>] [<reason>]
#
# Args:
#   task_type   nix-parallelize, pyfix, port-python-test, ...
#   model       AR_MODEL the run actually used (e.g. minimax-m2.5-free)
#   outcome     finalized | abandoned-no-diff | abandoned-score-fail | needs-architect
#   agent_id    short hex id from run-once
#   score       optional — pass on finalize ("PASS" or score string)
#   reason      optional — abandon reason (free text)
#
# All effects are local-only (bd database + ledger.toml); we never call
# external services from here. The accountant patrol order will roll up
# remote spend/usage on its own 6h cadence.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACCT_DIR="$(cd "$HERE/.." && pwd)"
REPO_ROOT="$(cd "$HERE/../../../.." && pwd)"
LEDGER="$ACCT_DIR/ledger.toml"
RECOMMENDATIONS="$ACCT_DIR/current-recommendations.toml"
RECEIPTS="$ACCT_DIR/receipts.tsv"

[[ $# -ge 4 ]] || { echo "usage: $0 <task_type> <model> <outcome> <agent_id> [<score>] [<reason>]" >&2; exit 2; }

TASK_TYPE="$1"
MODEL="$2"
OUTCOME="$3"
AGENT_ID="$4"
SCORE="${5:-}"
REASON="${6:-}"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TODAY="$(date -u +%Y-%m-%d)"

# Header (once).
if [[ ! -f "$RECEIPTS" ]]; then
    printf 'timestamp\ttask_type\tmodel\toutcome\tagent_id\tscore\treason\n' > "$RECEIPTS"
fi
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$NOW" "$TASK_TYPE" "$MODEL" "$OUTCOME" "$AGENT_ID" "$SCORE" "$REASON" \
    >> "$RECEIPTS"

# Create a bd decision bead — the accountant's durable memory.
# `bd` lives in gascity-env; only call if available. Failure is non-fatal
# (the receipts.tsv above is the fallback).
if command -v bd >/dev/null 2>&1; then
    cd "$REPO_ROOT/autoresearch"
    BEAD_TITLE="${TASK_TYPE} -> ${MODEL} (${OUTCOME})"
    BEAD_BODY="agent_id=${AGENT_ID} score=${SCORE} reason=${REASON} timestamp=${NOW}"
    bd create --type decision \
        --title "$BEAD_TITLE" \
        --description "$BEAD_BODY" \
        --priority p3 \
        >/dev/null 2>&1 || true
    cd - >/dev/null
fi

# Update model's last_seen_working on a finalize.
if [[ "$OUTCOME" == "finalized" ]]; then
    # Locate the [[model]] block whose `id = "$MODEL"` line precedes
    # last_seen_working, and bump that field's date in place.
    awk -v model="$MODEL" -v today="$TODAY" '
        BEGIN { in_block = 0 }
        /^\[\[model\]\]/  { in_block = 1; matched = 0 }
        in_block && $0 ~ "^id = \"" model "\"$" { matched = 1 }
        in_block && matched && /^last_seen_working = / {
            print "last_seen_working = \"" today "\""
            in_block = 0; matched = 0
            next
        }
        /^$/ { in_block = 0; matched = 0 }
        { print }
    ' "$LEDGER" > "$LEDGER.tmp" && mv "$LEDGER.tmp" "$LEDGER"
fi

# After abandons on a model+task_type, advise the accountant agent (via a
# trailer added to current-recommendations.toml) that escalation may be
# warranted. The accountant agent reads these trailers on its next
# consulting pass.
TRAILER_FILE="$ACCT_DIR/abandon-counts.tsv"
if [[ "$OUTCOME" == abandoned-* || "$OUTCOME" == "needs-architect" ]]; then
    [[ -f "$TRAILER_FILE" ]] || printf 'task_type\tmodel\tcount\tlast_seen\n' > "$TRAILER_FILE"
    # Increment (task_type, model) row, recreate file with merged counts.
    awk -F'\t' -v tt="$TASK_TYPE" -v m="$MODEL" -v t="$NOW" '
        BEGIN { OFS="\t" }
        NR == 1 { print; next }
        $1 == tt && $2 == m { $3 = $3 + 1; $4 = t; touched = 1 }
        { print }
        END { if (!touched) print tt, m, 1, t }
    ' "$TRAILER_FILE" > "$TRAILER_FILE.tmp" && mv "$TRAILER_FILE.tmp" "$TRAILER_FILE"
fi

echo "notify-accountant: receipt recorded ($TASK_TYPE / $MODEL / $OUTCOME)"
