#!/usr/bin/env bash
# patrol.sh — accountant's periodic data refresh.
#
# Run by `mol-accountant-patrol` order on a 6h cron.
# Pulls live data from OpenRouter and LiteLLM, writes snapshot files the
# accountant agent reads when allocating models.
#
# Outputs (all alongside this script):
#   ../usage-snapshot.toml         — current OpenRouter balance, today's spend
#   ../openrouter-models.json      — raw catalog snapshot (for ledger refresh)
#   ../litellm-spend.json          — LiteLLM per-key spend snapshot
#   ../patrol.log                  — append-only log of patrol runs
#
# Reads env (sourced from $REPO_ROOT/autoresearch/.env if present):
#   LITELLM_BASE_URL    default http://localhost:4000
#   LITELLM_API_KEY     LiteLLM master key
#   OPENROUTER_API_KEY  optional — without this, /key endpoint is skipped
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACCT_DIR="$(cd "$HERE/.." && pwd)"
# Derive REPO_ROOT from script location (don't trust cwd or git toplevel —
# script may run from anywhere via the supervisor):
#   <REPO_ROOT>/autoresearch/agents/accountant/scripts/patrol.sh
REPO_ROOT="$(cd "$HERE/../../../.." && pwd)"
ENV_FILE="$REPO_ROOT/autoresearch/.env"
LOG="$ACCT_DIR/patrol.log"

log()  { printf '%s patrol: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG"; }
die()  { log "FATAL: $*"; exit 1; }

[[ -f "$ENV_FILE" ]] && { set -a; . "$ENV_FILE"; set +a; }
LITELLM_BASE_URL="${LITELLM_BASE_URL:-http://localhost:4000}"
: "${LITELLM_API_KEY:?required (set in $ENV_FILE)}"

log "starting patrol"

# ── 1. OpenRouter catalog: snapshot all :free models for ledger refresh.
OR_CATALOG="$ACCT_DIR/openrouter-models.json"
if curl -fsSL --max-time 30 https://openrouter.ai/api/v1/models -o "$OR_CATALOG.tmp"; then
    mv "$OR_CATALOG.tmp" "$OR_CATALOG"
    free_count=$(jq '[.data[] | select(.pricing.prompt == "0" and .pricing.completion == "0")] | length' "$OR_CATALOG")
    paid_count=$(jq '[.data[] | select(.pricing.prompt != "0")] | length' "$OR_CATALOG")
    log "openrouter catalog: $free_count free, $paid_count paid models"
else
    log "WARN: openrouter catalog fetch failed"
fi

# ── 2. OpenRouter key info: balance + usage.
OR_KEY_FILE="$ACCT_DIR/openrouter-key.json"
if [[ -n "${OPENROUTER_API_KEY:-}" ]]; then
    if curl -fsSL --max-time 30 -H "Authorization: Bearer $OPENROUTER_API_KEY" \
        https://openrouter.ai/api/v1/auth/key -o "$OR_KEY_FILE.tmp"; then
        mv "$OR_KEY_FILE.tmp" "$OR_KEY_FILE"
        # OpenRouter's /auth/key returns cumulative `usage` (USD spent ever) and
        # null limit/limit_remaining unless the user set a hard cap on the key.
        # Actual remaining balance is shown only on the dashboard
        # (https://openrouter.ai/credits) — we surface what the API does give.
        usage_total=$(jq -r '.data.usage // "n/a"' "$OR_KEY_FILE")
        usage_daily=$(jq -r '.data.usage_daily // "n/a"' "$OR_KEY_FILE")
        usage_weekly=$(jq -r '.data.usage_weekly // "n/a"' "$OR_KEY_FILE")
        usage_monthly=$(jq -r '.data.usage_monthly // "n/a"' "$OR_KEY_FILE")
        limit=$(jq -r '.data.limit // "null"' "$OR_KEY_FILE")
        limit_remaining=$(jq -r '.data.limit_remaining // "null"' "$OR_KEY_FILE")
        is_free=$(jq -r '.data.is_free_tier // "n/a"' "$OR_KEY_FILE")
        log "openrouter key: usage_total=\$$usage_total usage_today=\$$usage_daily limit=$limit"
    else
        log "WARN: openrouter /auth/key fetch failed"
        usage_total="n/a"; usage_daily="n/a"; usage_weekly="n/a"; usage_monthly="n/a"
        limit="n/a"; limit_remaining="n/a"; is_free="n/a"
    fi
else
    log "OPENROUTER_API_KEY unset — skipping /auth/key"
    usage_total="unset"; usage_daily="unset"; usage_weekly="unset"; usage_monthly="unset"
    limit="unset"; limit_remaining="unset"; is_free="unset"
fi

# ── 3. LiteLLM spend (note: no /v1/ prefix on these admin endpoints).
LL_SPEND_FILE="$ACCT_DIR/litellm-spend.json"
if curl -fsSL --max-time 30 -H "Authorization: Bearer $LITELLM_API_KEY" \
    "$LITELLM_BASE_URL/spend/users" -o "$LL_SPEND_FILE.tmp"; then
    mv "$LL_SPEND_FILE.tmp" "$LL_SPEND_FILE"
    total_spend=$(jq -r '[.[] | .spend // 0] | add // 0' "$LL_SPEND_FILE" 2>/dev/null || echo 0)
    log "litellm spend: total_users_spend=\$$total_spend"
else
    log "WARN: litellm /spend/users fetch failed"
    total_spend=0
fi

# ── 4. LiteLLM global spend snapshot (cumulative since deploy).
GLOBAL_SPEND_FILE="$ACCT_DIR/litellm-global-spend.json"
if curl -fsSL --max-time 30 -H "Authorization: Bearer $LITELLM_API_KEY" \
    "$LITELLM_BASE_URL/global/spend" -o "$GLOBAL_SPEND_FILE.tmp"; then
    mv "$GLOBAL_SPEND_FILE.tmp" "$GLOBAL_SPEND_FILE"
    global_spend=$(jq -r '.spend // .data.spend // 0' "$GLOBAL_SPEND_FILE" 2>/dev/null || echo 0)
    log "litellm global spend: \$$global_spend"
else
    global_spend=0
fi

# ── 5. Per-key spend (since each model can be keyed in litellm-config).
LL_KEYS_FILE="$ACCT_DIR/litellm-keys-spend.json"
curl -sf --max-time 30 -H "Authorization: Bearer $LITELLM_API_KEY" \
    "$LITELLM_BASE_URL/spend/keys" -o "$LL_KEYS_FILE" 2>/dev/null || true

# ── 5. Write the human-readable usage snapshot consumed by the accountant agent.
SNAPSHOT="$ACCT_DIR/usage-snapshot.toml"
cat > "$SNAPSHOT" <<TOML
# Autogenerated by patrol.sh — DO NOT HAND-EDIT.
# Refresh: bash autoresearch/agents/accountant/scripts/patrol.sh
# Or wait for the mol-accountant-patrol order (every 6h).

generated_at = "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

[openrouter]
# usage_total is cumulative USD spent ever on this key.
# usage_today/week/month are rolling-window spend.
# limit fields are non-null only if a hard cap was set on the key.
# Actual remaining balance lives only on https://openrouter.ai/credits —
# infer it as: (last known credit purchase) - usage_total.
usage_total_usd = "$usage_total"
usage_today_usd = "$usage_daily"
usage_week_usd = "$usage_weekly"
usage_month_usd = "$usage_monthly"
limit_usd = "$limit"
limit_remaining_usd = "$limit_remaining"
is_free_tier = "$is_free"
catalog_free_count = ${free_count:-0}
catalog_paid_count = ${paid_count:-0}

[litellm]
total_users_spend_usd = $total_spend
global_spend_usd = $global_spend
TOML

log "snapshot written: $SNAPSHOT"

# ── 6. Rule-based auto-escalation: read abandon-counts.tsv, escalate any
#    (task_type, model) pair with count >= 2 by rewriting that task_type's
#    block in current-recommendations.toml to the next tier up. Pure
#    bash — no LLM call. The LLM accountant agent does richer judgment
#    on its consulting passes; this is the cheap autopilot.
RECS="$ACCT_DIR/current-recommendations.toml"
COUNTS="$ACCT_DIR/abandon-counts.tsv"
LEDGER="$ACCT_DIR/ledger.toml"

# Tier escalation order. Each tier maps to a recommended model that's
# the strongest tool-use member of the next-up tier.
escalate_to() {
    # Tier escalation order biases to local first wherever possible.
    # on-prem fails -> try a different on-prem (handled by router_settings
    # fallbacks in nixlab/litellm-config). If all on-prem broken, jump to
    # the strongest tool-use free row. Free fails -> paid-cheap. Etc.
    case "$1" in
        on-prem)     echo "minimax-m2.5-free:free" ;;
        free)        echo "claude-haiku-4-5:paid-cheap" ;;
        paid-cheap)  echo "claude-sonnet-4-6:paid" ;;
        paid)        echo "claude-sonnet-4-6:paid" ;;  # already top
        *)           echo "" ;;
    esac
}

if [[ -f "$COUNTS" ]] && [[ -f "$RECS" ]]; then
    escalations=0
    # Skip header, group by task_type, find pairs with count>=2 that match
    # the current recommendation.
    while IFS=$'\t' read -r task_type model count last_seen; do
        [[ "$task_type" == "task_type" ]] && continue
        [[ "${count:-0}" -lt 2 ]] && continue

        current=$(awk -v tt="[$task_type]" '
            $0 == tt { inb = 1; next }
            /^\[/ { inb = 0 }
            inb && /^model = "/ { sub(/^model = "/, ""); sub(/"$/, ""); print; exit }
        ' "$RECS")
        [[ "$current" != "$model" ]] && continue  # already escalated past this

        current_tier=$(awk -v tt="[$task_type]" '
            $0 == tt { inb = 1; next }
            /^\[/ { inb = 0 }
            inb && /^tier = "/ { sub(/^tier = "/, ""); sub(/"$/, ""); print; exit }
        ' "$RECS")

        next=$(escalate_to "$current_tier")
        [[ -z "$next" ]] && continue
        next_model="${next%:*}"
        next_tier="${next#*:}"
        [[ "$next_model" == "$current" ]] && continue  # nothing to escalate to

        # Verify next_model is in the ledger (sanity).
        grep -q "^id = \"$next_model\"" "$LEDGER" || {
            log "WARN: would escalate $task_type to $next_model but it's not in ledger.toml"
            continue
        }

        log "escalating [$task_type]: $current ($current_tier) -> $next_model ($next_tier) — $count abandons"

        # Rewrite the [task_type] block in-place. Replace model/tier/reason/chosen_at.
        awk -v tt="[$task_type]" -v nm="$next_model" -v nt="$next_tier" -v c="$count" -v ls="$last_seen" -v now="$(date -u +%Y-%m-%dT%H:%M:%SZ)" '
            $0 == tt { inb = 1; print; next }
            /^\[/ && $0 != tt { inb = 0 }
            inb && /^model = / { print "model = \"" nm "\""; next }
            inb && /^tier = / { print "tier = \"" nt "\""; next }
            inb && /^reason = / { print "reason = \"Auto-escalated by patrol " now ": " c " abandons on prior model (last seen " ls ").\""; next }
            inb && /^chosen_at = / { print "chosen_at = \"" now "\""; next }
            { print }
        ' "$RECS" > "$RECS.tmp" && mv "$RECS.tmp" "$RECS"

        escalations=$((escalations + 1))

        # Decision bead — accountant memory.
        if command -v bd >/dev/null 2>&1; then
            (cd "$REPO_ROOT/autoresearch" && bd create --type decision \
                --title "auto-escalate $task_type: $current -> $next_model" \
                --description "patrol $now escalated after $count abandons; last_seen=$ls" \
                --priority p2 >/dev/null 2>&1 || true)
        fi
    done < "$COUNTS"
    [[ $escalations -gt 0 ]] && log "auto-escalated $escalations task type(s)"
fi

log "patrol complete"
