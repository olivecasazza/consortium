#!/usr/bin/env bash
# run-once.sh — drive a single task end-to-end.
#
# Usage: bash run-once.sh [--no-agent] [--agent <cmd>]
#
# 1. pick-task.sh           → claims a task
# 2. new-worktree.sh        → fresh worktree on its own branch
# 3. invoke the agent       → opencode (or whatever --agent points to)
# 4. score.sh               → fitness gate
# 5. finalize.sh on green, otherwise move task to abandoned/ + log
#
# --no-agent skips step 3 (useful for smoke-testing 1, 2, 4, 5).
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
SCRIPTS="$REPO_ROOT/autoresearch/scripts"
LOGS="$REPO_ROOT/autoresearch/logs"
RESULTS="$REPO_ROOT/autoresearch/results.tsv"
mkdir -p "$LOGS"

NO_AGENT=0
AGENT_CMD=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-agent) NO_AGENT=1; shift ;;
        --agent) AGENT_CMD="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# 1. claim a task
TASK_FILE=$(bash "$SCRIPTS/pick-task.sh") || {
    echo "no task to run"; exit 0
}
TASK_ID=$(basename "$TASK_FILE" .task.toml)
TASK_TYPE=$(awk -F'"' '/^type[[:space:]]*=/{print $2; exit}' "$TASK_FILE")
TARGET_FILE=$(awk -F'"' '/^target_file[[:space:]]*=/{print $2; exit}' "$TASK_FILE")
# Export AR_TASK_TYPE once, here. Both the agent invocation (--no-agent
# flag may skip it) and the score.sh call below need it visible; setting
# it once at task-pick time guarantees the env propagates to every child.
export AR_TASK_TYPE="$TASK_TYPE"

# Accountant-driven model selection: read current-recommendations.toml,
# look up [TASK_TYPE] then [default]. Falls through to whatever AR_MODEL
# is already in the environment (set via .env). The accountant agent
# rewrites this file based on bd decision-log + ledger.toml; we just
# obey it here.
RECS="$REPO_ROOT/autoresearch/agents/accountant/current-recommendations.toml"
if [[ -f "$RECS" ]]; then
    pick_model_for() {
        local tt="$1"
        awk -v tt="[$tt]" '
            $0 == tt { inb = 1; next }
            /^\[/    { inb = 0 }
            inb && /^model = "/ {
                sub(/^model = "/, ""); sub(/"$/, ""); print; exit
            }
        ' "$RECS"
    }
    chosen=$(pick_model_for "$TASK_TYPE")
    [[ -z "$chosen" ]] && chosen=$(pick_model_for "default")
    if [[ -n "$chosen" ]]; then
        echo "accountant chose model: $chosen (task_type=$TASK_TYPE)" | tee -a /dev/stderr
        export AR_MODEL="$chosen"
    fi
fi

AGENT_ID=$(openssl rand -hex 4 2>/dev/null || head -c 4 /dev/urandom | xxd -p)
TOPIC=$(echo "$TASK_ID" | tr '_' '-')
LOGFILE="$LOGS/$AGENT_ID-$TASK_ID.log"

echo "=== run-once: agent=$AGENT_ID task=$TASK_ID type=$TASK_TYPE ===" | tee -a "$LOGFILE"

# 2. worktree
eval "$(bash "$SCRIPTS/new-worktree.sh" "$AGENT_ID" "$TOPIC")"
echo "WORKTREE=$WORKTREE BRANCH=$BRANCH" | tee -a "$LOGFILE"

# 3. agent
notify_accountant() {
    # Args: outcome (finalized|abandoned-no-diff|abandoned-score-fail|needs-architect) [reason]
    local outcome="$1"
    local reason="${2:-}"
    local model="${AR_MODEL:-unknown}"
    bash "$REPO_ROOT/autoresearch/agents/accountant/scripts/notify.sh" \
        "$TASK_TYPE" "$model" "$outcome" "$AGENT_ID" "" "$reason" \
        2>&1 | tee -a "$LOGFILE" || true
}

abandon() {
    local reason="$1"
    local outcome="abandoned-other"
    case "$reason" in
        no-diff*) outcome="abandoned-no-diff" ;;
        score-fail*) outcome="abandoned-score-fail" ;;
        needs-architect*) outcome="needs-architect" ;;
    esac
    echo "ABANDON: $reason" | tee -a "$LOGFILE"
    {
        printf '\n# abandoned %s\n# reason: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$reason"
    } >> "$TASK_FILE"
    mkdir -p "$REPO_ROOT/autoresearch/queue/abandoned"
    mv "$TASK_FILE" "$REPO_ROOT/autoresearch/queue/abandoned/"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$TASK_ID" "$BRANCH" "abandoned" "" "" "" "$reason" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        >> "$RESULTS"
    notify_accountant "$outcome" "$reason"
    exit 1
}

if [[ $NO_AGENT -eq 0 ]]; then
    : "${AGENT_CMD:=$REPO_ROOT/autoresearch/scripts/agent-opencode.sh}"
    if [[ ! -x "$AGENT_CMD" && ! -f "$AGENT_CMD" ]]; then
        abandon "agent-cmd-missing: $AGENT_CMD"
    fi
    export AR_TASK_FILE="$TASK_FILE"
    export AR_TASK_TYPE="$TASK_TYPE"
    export AR_WORKTREE="$WORKTREE"
    export AR_BRANCH="$BRANCH"
    export AR_SCORE="$SCRIPTS/score.sh"
    export AR_PROGRAM="$REPO_ROOT/autoresearch/program.md"
    timeout 30m bash "$AGENT_CMD" 2>&1 | tee -a "$LOGFILE" || true
fi

# 4. score (AR_TASK_TYPE export persists into the score.sh subshell so
#    Gate 5 dispatches when the task type calls for it)
cd "$WORKTREE"
export AR_TASK_TYPE="$TASK_TYPE"
if bash "$SCRIPTS/score.sh" "$WORKTREE" 2>&1 | tee -a "$LOGFILE"; then
    SCORE=pass
else
    SCORE=fail
fi

# 5. finalize or abandon
if [[ "$SCORE" == "pass" ]]; then
    # "no-diff" means: nothing in this branch differs from master, after
    # the agent has run. Per program.md the agent commits its own work
    # before exiting, so the right comparison is master..HEAD (committed
    # diff) plus the working-tree/staged areas (anything the agent left
    # uncommitted). All three empty → agent really did nothing.
    if git diff --quiet master..HEAD 2>/dev/null \
        && git diff --quiet 2>/dev/null \
        && git diff --cached --quiet 2>/dev/null; then
        abandon "no-diff: agent did not modify any files"
    fi
    SUBJECT="$(printf '%s(%s): resolve %s' \
        "$([[ $TASK_TYPE == port-python-test ]] && echo test || echo fix)" \
        "$(echo "$TARGET_FILE" | awk -F/ '{print $2}')" \
        "$TASK_ID")"
    bash "$SCRIPTS/finalize.sh" "$WORKTREE" "$BRANCH" "$TASK_FILE" "$SUBJECT" 2>&1 | tee -a "$LOGFILE"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$TASK_ID" "$BRANCH" "done" "$SCORE" "" "" "" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        >> "$RESULTS"
    notify_accountant "finalized" ""
else
    abandon "score-fail"
fi
