#!/usr/bin/env bash
# upstream-diff.sh — fetch cea-hpc/clustershell and emit one upstream-sync
# task per upstream commit not yet integrated.
#
# Usage: bash upstream-diff.sh [--limit N] [--dry-run]
#
# UPSTREAM_REF (file at repo root) holds the last-integrated tag/sha.
# We diff UPSTREAM_REF..upstream/master, restrict to lib/ paths, and
# create one task per commit. Multi-task commits are kept atomic — the
# agent will either port the whole commit or abandon it.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

LIMIT=20
DRY_RUN=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --limit) LIMIT="$2"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

UPSTREAM_REF_FILE="$REPO_ROOT/UPSTREAM_REF"
if [[ ! -f "$UPSTREAM_REF_FILE" ]]; then
    echo "UPSTREAM_REF file missing — cannot compute diff" >&2
    exit 3
fi
LAST=$(tr -d '[:space:]' < "$UPSTREAM_REF_FILE")

# Make sure the upstream remote exists (added at clone time).
git remote get-url upstream >/dev/null 2>&1 || {
    echo "remote 'upstream' missing — re-add: git remote add upstream https://github.com/cea-hpc/clustershell.git" >&2
    exit 4
}

git fetch upstream --tags --quiet

# Resolve LAST to a sha we can diff from. If it's a tag, use refs/tags/<X>.
BASE_SHA=$(git rev-parse "refs/tags/$LAST" 2>/dev/null \
    || git rev-parse "$LAST" 2>/dev/null \
    || true)
if [[ -z "$BASE_SHA" ]]; then
    echo "cannot resolve UPSTREAM_REF=$LAST in upstream/" >&2
    exit 5
fi

QUEUE="$REPO_ROOT/autoresearch/queue/pending"
mkdir -p "$QUEUE"

KNOWN_IDS_FILE=$(mktemp)
trap 'rm -f "$KNOWN_IDS_FILE"' EXIT
find "$REPO_ROOT/autoresearch/queue" -maxdepth 2 -name '*.task.toml' -printf '%f\n' \
    | sed 's/\.task\.toml$//' > "$KNOWN_IDS_FILE"

count=0
while IFS= read -r sha; do
    [[ -z "$sha" ]] && continue
    id="upstream-${sha:0:8}"
    if grep -Fxq "$id" "$KNOWN_IDS_FILE"; then continue; fi

    subject=$(git log -1 --format='%s' "$sha")
    files=$(git show --name-only --format= "$sha" -- 'lib/*' 2>/dev/null | grep -v '^$' | head -20)
    [[ -z "$files" ]] && continue  # commit didn't touch lib/

    out="$QUEUE/$id.task.toml"
    if [[ $DRY_RUN -eq 1 ]]; then
        echo "DRY: $out  ($subject)"
    else
        {
            printf 'type = "upstream-sync"\n'
            printf 'upstream_sha = "%s"\n' "$sha"
            printf 'upstream_subject = %s\n' "$(printf '%s' "$subject" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read().rstrip()))' 2>/dev/null || printf '"%s"' "${subject//\"/\\\"}")"
            printf 'files_touched = [\n'
            while IFS= read -r f; do
                printf '  "%s",\n' "$f"
            done <<< "$files"
            printf ']\n'
            printf 'created = "%s"\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        } > "$out"
        echo "wrote $out"
    fi
    count=$((count + 1))
    [[ $count -ge $LIMIT ]] && break
done < <(git log --format='%H' --reverse "$BASE_SHA..upstream/master" -- 'lib/*' 2>/dev/null)

echo
echo "emitted $count upstream-sync tasks (LIMIT=$LIMIT)"
