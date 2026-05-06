#!/usr/bin/env bash
# run-loop.sh — overnight driver.
#
# Usage: bash run-loop.sh [--max-tasks N] [--no-agent] [--agent <cmd>]
#
# Runs run-once.sh repeatedly until queue/pending/ is empty or
# --max-tasks is reached. Sleeps briefly between iterations to be nice
# to the cluster.
set -uo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
SCRIPTS="$REPO_ROOT/autoresearch/scripts"

MAX_TASKS=999
PASSTHROUGH=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-tasks) MAX_TASKS="$2"; shift 2 ;;
        *) PASSTHROUGH+=("$1"); shift ;;
    esac
done

count=0
while [[ $count -lt $MAX_TASKS ]]; do
    if ! find "$REPO_ROOT/autoresearch/queue/pending" -maxdepth 1 -name '*.task.toml' -type f \
            | grep -q .; then
        echo "queue empty — exiting loop"
        break
    fi
    count=$((count + 1))
    echo
    echo "##### run-loop iteration $count #####"
    bash "$SCRIPTS/run-once.sh" "${PASSTHROUGH[@]}" || true
    sleep 3
done

echo
echo "run-loop done. processed $count task(s)."
echo "results: $REPO_ROOT/autoresearch/results.tsv"
