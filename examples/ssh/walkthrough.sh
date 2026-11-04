#!/usr/bin/env bash
# Runs every command in doc/ssh.md against real SSH hosts and checks the output.
# Needs claw, pinch, molt, and the fanout_ssh example on PATH.
#   NODES      node set reachable over non-interactive SSH (default: loopback)
#   SSH_USER   remote user (default: $USER)
set -euo pipefail

nodes="${NODES:-127.0.0.[1-3]}"
user="${SSH_USER:-$USER}"
ssh_opts="-o BatchMode=yes -o StrictHostKeyChecking=accept-new"
count=$(pinch -c "$nodes")
first=$(pinch -e "$nodes" | cut -d' ' -f1)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

step() { printf '\n== %s\n' "$*"; }

step "pinch: fold, expand, count, set algebra"
test "$(pinch -f node1 node2 node3)" = "node[1-3]"
test "$(pinch -e 'node[1-3]')" = "node1 node2 node3"
test "$(pinch -c 'node[1-100]')" = "100"
test "$(pinch 'node[1-10]' -i 'node[5-15]')" = "node[5-10]"
test "$(pinch 'node[1-10]' -x 'node[8-99]')" = "node[1-7]"
test "$(pinch 'node[1-5]' 'node[3-8]')" = "node[1-8]"
test "$(pinch --split 2 'node[1-6]' | tr '\n' ' ')" = "node[1-3] node[4-6] "

step "claw: run a command on every node"
claw -R ssh -l "$user" --options="$ssh_opts" -w "$nodes" -- uname -n | tee "$work/hostname.out"
test "$(grep -c ': ' "$work/hostname.out")" = "$count"

step "claw -b: group identical output"
claw -R ssh -l "$user" --options="$ssh_opts" -b -w "$nodes" -- uname -s | tee "$work/uname.out"
grep -q "($count)" "$work/uname.out" || test "$count" = 1

step "claw -S: worst return code for scripting"
if claw -R ssh -l "$user" --options="$ssh_opts" -S -q -w "$nodes" -- false; then
    echo "expected a non-zero return code" >&2
    exit 1
fi

step "claw --copy: push a file to every node"
echo "consortium walkthrough" >"$work/app.conf"
claw -R ssh -l "$user" --options="$ssh_opts" -w "$nodes" -- mkdir -p "$work/remote"
claw -R ssh -l "$user" --options="$ssh_opts" -w "$nodes" --copy "$work/app.conf" --dest "$work/remote/"
claw -R ssh -l "$user" --options="$ssh_opts" -w "$first" -- cat "$work/remote/app.conf" | grep -q "consortium walkthrough"

step "claw --rcopy: pull a file back from every node as <name>.<node>"
mkdir -p "$work/pulled"
claw -R ssh -l "$user" --options="$ssh_opts" -w "$nodes" --rcopy "$work/remote/app.conf" --dest "$work/pulled"
test "$(find "$work/pulled" -name 'app.conf.*' | wc -l | tr -d ' ')" = "$count"

step "molt: gather output collected elsewhere"
claw -R ssh -l "$user" --options="$ssh_opts" -w "$nodes" -- uname -s | molt -b | tee "$work/molt.out"
grep -q "$(uname -s)" "$work/molt.out"

step "library: the same fan-out from Rust (examples/rust/fanout_ssh.rs)"
SSH_USER="$user" fanout_ssh "$nodes" 'uname -s' | tee "$work/lib.out"
grep -q "rc 0: " "$work/lib.out"

echo
echo "ssh walkthrough: ok ($count nodes)"
