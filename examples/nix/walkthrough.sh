#!/usr/bin/env bash
# Runs the commands in doc/cast.md's walkthrough against real SSH hosts and
# checks the output. `deploy --dry-run` resolves every target's SSH endpoint
# but builds, copies, and activates nothing.
#   NODES      node set reachable over non-interactive SSH (default: loopback)
#   SSH_USER   remote user (default: $USER)
set -euo pipefail

nodes="${NODES:-127.0.0.[1-3]}"
user="${SSH_USER:-$USER}"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

step() { printf '\n== %s\n' "$*"; }

# One fleet node per host, named host1..hostN, tagged by position.
i=0
{
    printf '{"nodes": {'
    for host in $(pinch -e "$nodes"); do
        i=$((i + 1))
        [ "$i" -gt 1 ] && printf ','
        tags='"web"'
        [ "$i" -gt 1 ] && tags='"web", "compute"'
        printf '"host%d": {"name": "host%d", "targetHost": "%s", "targetUser": "%s", "system": "x86_64-linux", "profileType": "nixos", "targetPort": null, "buildOnTarget": false, "tags": [%s]}' \
            "$i" "$i" "$host" "$user" "$tags"
    done
    printf '}, "builders": {}, "flakeUri": "."}\n'
} >"$work/fleet.json"
count=$i

step "cast hosts / tags: read the fleet"
cast -c "$work/fleet.json" hosts | tee "$work/hosts.out"
test "$(grep -c '^ *host' "$work/hosts.out")" = "$count"
cast -c "$work/fleet.json" tags | grep -qx web

step "cast deploy --dry-run: resolve SSH endpoints, plan, no changes"
cast -c "$work/fleet.json" deploy --dry-run --tag web switch 2>&1 | tee "$work/plan.out"
grep -q "Deploying $count host(s)" "$work/plan.out"
if grep -q skipping "$work/plan.out"; then
    echo "a target was unreachable over SSH" >&2
    exit 1
fi

step "cast cascade live: simulate peer-to-peer closure fan-out"
cast cascade live -n 16 --fanout 2 --format json >"$work/cascade.json"
grep -q '"converged": 16' "$work/cascade.json"

echo
echo "nix walkthrough: ok ($count nodes)"
