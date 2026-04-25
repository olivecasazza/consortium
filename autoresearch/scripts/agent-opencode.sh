#!/usr/bin/env bash
# agent-opencode.sh — invoke opencode against the LiteLLM proxy.
#
# Reads $AR_TASK_FILE, $AR_WORKTREE, $AR_BRANCH, $AR_SCORE, $AR_PROGRAM
# from the orchestrator. Constructs a single prompt that is program.md +
# the task file, and runs opencode in headless mode pointed at LiteLLM.
#
# Env vars expected from .env or shell:
#   LITELLM_BASE_URL   default http://localhost:4000  (port-forward target)
#   LITELLM_API_KEY    LiteLLM master key (treat as secret)
#   AR_MODEL           default local/qwen3-8b
set -euo pipefail

: "${AR_TASK_FILE:?required}"
: "${AR_WORKTREE:?required}"
: "${AR_PROGRAM:?required}"

LITELLM_BASE_URL="${LITELLM_BASE_URL:-http://localhost:4000}"
LITELLM_API_KEY="${LITELLM_API_KEY:-}"
AR_MODEL="${AR_MODEL:-local/qwen3-8b}"

if [[ -z "$LITELLM_API_KEY" ]]; then
    echo "agent-opencode: LITELLM_API_KEY not set" >&2
    exit 7
fi

if ! command -v opencode >/dev/null 2>&1; then
    echo "agent-opencode: opencode CLI not in PATH — install from https://github.com/sst/opencode" >&2
    exit 8
fi

cd "$AR_WORKTREE"

PROMPT=$(mktemp)
trap 'rm -f "$PROMPT"' EXIT
{
    cat "$AR_PROGRAM"
    printf '\n\n---\n\n## Your task\n\n'
    cat "$AR_TASK_FILE"
} > "$PROMPT"

# opencode honors OPENAI_BASE_URL / OPENAI_API_KEY for OpenAI-compatible
# providers. LiteLLM presents that interface natively.
export OPENAI_BASE_URL="$LITELLM_BASE_URL"
export OPENAI_API_KEY="$LITELLM_API_KEY"

# Headless run: read the prompt, exit when the agent stops.
opencode run \
    --model "$AR_MODEL" \
    --prompt-file "$PROMPT" \
    --cwd "$AR_WORKTREE"
