#!/usr/bin/env bash
# pick-task.sh — atomically claim one task from queue/pending/.
#
# Usage: bash pick-task.sh
# Prints the absolute path to the claimed task file (now in
# queue/in-progress/) on stdout. Exits 1 if no tasks are available.
#
# Atomicity comes from flock(1) on a lockfile under the queue dir, plus
# an atomic mv. Multiple agents calling this concurrently each get a
# distinct task or "no tasks" — never the same task twice.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
QUEUE="$REPO_ROOT/autoresearch/queue"
LOCK="$QUEUE/.lock"
mkdir -p "$QUEUE/pending" "$QUEUE/in-progress"
touch "$LOCK"

(
    flock -x 9
    # Pick the lexicographically first .task.toml in pending/.
    # Sort gives stable order; tasks can be prefixed 001-, 002- to prioritize.
    next=$(find "$QUEUE/pending" -maxdepth 1 -name '*.task.toml' -type f | sort | head -n 1)
    if [[ -z "$next" ]]; then
        echo "no tasks pending" >&2
        exit 1
    fi
    base=$(basename "$next")
    target="$QUEUE/in-progress/$base"
    mv "$next" "$target"
    echo "$target"
) 9>"$LOCK"
