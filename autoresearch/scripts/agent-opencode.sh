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
#   AR_MODEL           default traitor/qwen3-8b
#                      Routes to vllm-rocm on traitor's RX 7900 XTX (gfx1100).
#                      LiteLLM fallback chain falls over to seir's vllm-hermes
#                      then OpenRouter free tier if traitor is unavailable.
set -euo pipefail

: "${AR_TASK_FILE:?required}"
: "${AR_WORKTREE:?required}"
: "${AR_PROGRAM:?required}"

LITELLM_BASE_URL="${LITELLM_BASE_URL:-http://localhost:4000}"
LITELLM_API_KEY="${LITELLM_API_KEY:-}"
AR_MODEL="${AR_MODEL:-traitor/qwen3-8b}"

# Prefer the newer downloaded binary over nixpkgs's older system one — system
# opencode 1.1.14 doesn't have --dir / --dangerously-skip-permissions and uses
# a prompt-file rather than positional message, so a PATH lookup that resolves
# to /run/current-system/sw/bin/opencode silently no-ops every drain-queue.
# Override with OPENCODE_BIN if you need a different binary.
OPENCODE_BIN="${OPENCODE_BIN:-$HOME/.local/bin/opencode}"

if [[ -z "$LITELLM_API_KEY" ]]; then
    echo "agent-opencode: LITELLM_API_KEY not set" >&2
    exit 7
fi

if [[ ! -x "$OPENCODE_BIN" ]]; then
    echo "agent-opencode: $OPENCODE_BIN not executable" >&2
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
# opencode 1.14+ takes the prompt as a positional message and uses --dir
# (renamed from --cwd). --dangerously-skip-permissions is required so the
# agent can edit files unattended.
#
# Use an isolated XDG_CONFIG_HOME so we don't pick up the user's personal
# opencode hooks/plugins. Declare a "litellm" custom provider that proxies
# our OpenAI-compatible LiteLLM endpoint, then ask opencode to use it.
OC_CFG=$(mktemp -d)
trap 'rm -f "$PROMPT"; rm -rf "$OC_CFG"' EXIT
mkdir -p "$OC_CFG/opencode"
# opencode 1.14+ reads $XDG_CONFIG_HOME/opencode/config.json (not opencode.json).
# Writing the wrong filename causes opencode to fall back to the global
# ~/.config/opencode/config.json — which targets a different provider.
cat > "$OC_CFG/opencode/config.json" <<JSON
{
  "\$schema": "https://opencode.ai/config.json",
  "provider": {
    "litellm": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "LiteLLM",
      "options": {
        "baseURL": "$LITELLM_BASE_URL",
        "apiKey": "$LITELLM_API_KEY"
      },
      "models": {
        "$AR_MODEL": {}
      }
    }
  },
  "small_model": "litellm/$AR_MODEL",
  "compaction": {
    "auto": true,
    "prune": true,
    "tail_turns": 2,
    "preserve_recent_tokens": 6000,
    "reserved": 2000
  }
}
JSON
XDG_CONFIG_HOME="$OC_CFG" "$OPENCODE_BIN" run \
    --model "litellm/$AR_MODEL" \
    --dir "$AR_WORKTREE" \
    --dangerously-skip-permissions \
    "$(cat "$PROMPT")"
