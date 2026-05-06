#!/usr/bin/env bash
# new-worktree.sh — create a fresh git worktree for an agent.
#
# Usage: bash new-worktree.sh <agent-id> <topic>
# Prints two lines on success:
#   WORKTREE=<absolute path>
#   BRANCH=<branch name>
#
# Per CLAUDE.md: worktree path is .claude/worktrees/agent-<id>/, branch
# is agent/<id>/<topic>. Branches off origin/master after a fetch+rebase.
# Never deletes existing worktrees — that's the orchestrator's job, after
# user review.
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: new-worktree.sh <agent-id> <topic>" >&2
    exit 2
fi

AGENT_ID="$1"
TOPIC="$2"
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

WORKTREE="$REPO_ROOT/.claude/worktrees/agent-$AGENT_ID"
BRANCH="agent/$AGENT_ID/$TOPIC"

if [[ -e "$WORKTREE" ]]; then
    echo "worktree already exists: $WORKTREE" >&2
    exit 3
fi

# Fork from origin/master (this fork's master), not consortium/master, so
# the worktree has the autoresearch/ harness in it and PR diffs only
# contain the agent's changes — not the entire harness as an addition.
BASE_REMOTE=origin
git remote get-url "$BASE_REMOTE" >/dev/null 2>&1 || BASE_REMOTE=consortium
git fetch "$BASE_REMOTE" master --quiet 2>/dev/null || true
BASE_REF="$BASE_REMOTE/master"
git rev-parse --verify "$BASE_REF" >/dev/null 2>&1 || BASE_REF=master

mkdir -p "$REPO_ROOT/.claude/worktrees"
git worktree add -b "$BRANCH" "$WORKTREE" "$BASE_REF" >/dev/null

echo "WORKTREE=$WORKTREE"
echo "BRANCH=$BRANCH"
