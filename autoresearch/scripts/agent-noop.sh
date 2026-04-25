#!/usr/bin/env bash
# agent-noop.sh — deterministic validation stub.
#
# Reads the task's target_file and target_line from $AR_TASK_FILE and
# deletes that one line. This is NOT a real agent — it exists only to
# validate the orchestrator (pick → worktree → edit → score → finalize)
# without LLM variability.
#
# Use:  AR_TASK_FILE=... AR_WORKTREE=... bash agent-noop.sh
# Or:   bash run-once.sh --agent autoresearch/scripts/agent-noop.sh
set -euo pipefail

: "${AR_TASK_FILE:?required}"
: "${AR_WORKTREE:?required}"

cd "$AR_WORKTREE"

target_file=$(awk -F'"' '/^target_file/{print $2; exit}' "$AR_TASK_FILE")
target_line=$(awk '/^target_line/{print $NF; exit}' "$AR_TASK_FILE")

if [[ -z "$target_file" || -z "$target_line" ]]; then
    echo "agent-noop: task missing target_file/target_line; nothing to do" >&2
    exit 0
fi

if [[ ! -f "$target_file" ]]; then
    echo "agent-noop: $target_file not found in worktree" >&2
    exit 0
fi

# Sanity: does the line look like a TODO/FIXME comment?
line_content=$(sed -n "${target_line}p" "$target_file")
if ! grep -qE '(TODO|FIXME|XXX)' <<< "$line_content"; then
    echo "agent-noop: $target_file:$target_line doesn't look like a TODO/FIXME — skipping" >&2
    echo "  line content: $line_content" >&2
    exit 0
fi

echo "agent-noop: deleting $target_file:$target_line" >&2
echo "  content: $line_content" >&2
sed -i "${target_line}d" "$target_file"
