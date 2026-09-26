---
name: consortium
description: Run one command across many cluster nodes in parallel, move files, and roll out nixOS or nix-darwin across a fleet. Use it when a task targets a set of hosts at once — fanning a command out over a node set, gathering or aggregating the results, distributing a file or store path, or deploying a nixOS system to many machines.
version: 0.3.0
license: LGPL-2.1-or-later
compatibility: consortium-cli >= 0.3.0. Remote runs need ssh on PATH; group selection (-g, -X, -a, and pinch -l) needs a groups.conf under ~/.config/clustershell/ or /etc/clustershell/. cast and cascade-copy need nix with ssh-ng and the nix store.
tags:
  - cluster
  - ssh
  - parallel-execution
  - hpc
  - nixos
  - fleet
  - clustershell
---

# consortium

consortium is a Rust rewrite of ClusterShell plus a NixOS fleet-deploy layer. One
package, `consortium-cli`, ships six binaries. `claw`, `molt`, and `pinch` keep the
frozen upstream ClusterShell short-flag sets (`clush`, `clubak`, and
`cluset`/`nodeset` respectively), so an existing muscle-memory command usually
transfers unchanged.

## When to use which binary

| Binary | Use it for |
| --- | --- |
| `claw` | Running one command on many nodes in parallel and collecting the results. Also carries `--copy` and `--rcopy` for single-file transfers. |
| `molt` | Aggregating output that something else already produced. It reads `key: output` lines on stdin and gathers, orders, or draws them as a message tree. It takes no node selection — the input decides the nodes. |
| `pinch` | Node-set questions rather than remote work: count, expand, fold, list groups, regroup, and set algebra (`-x`, `-i`, `-X`). Use it to check what a selector resolves to before fanning out. |
| `cast` | nixOS and nix-darwin fleet deploys. Subcommands: `eval`, `build`, `deploy`, `health`, `status`. |
| `cascade-copy` | Distributing one nix store path from a seed host across a fleet with a cascade tree, so each host that receives the path becomes a source for the next round. |
| `cascade-viz` | Replaying or simulating cascade event streams as a live tree. Use it to inspect copy behavior, not to copy anything. |

## claw flag surface

| Flag | Meaning |
| --- | --- |
| `-w, --nodes NODESET` | Target nodes, bracket notation (`node[1-5]`). One value; it accepts comma lists and set operators, so it does not need repeating. |
| `-x, --exclude NODESET` | Remove these nodes from the target set. |
| `-a, --all` | Use every node from the default group. |
| `-g, --group GROUP` | Use the nodes of a named group. Repeatable. |
| `-X, --exclude-group GROUP` | Drop the nodes of a named group. Only takes effect alongside `-g` or `-a`. |
| `--pick N` | Keep only N nodes from the resolved set. |
| `--hostfile FILE` | Read nodes from a file, one per line; blank lines and `#` comments are skipped. |
| `-f, --fanout N` | Maximum concurrent connections. Default 64. |
| `-l, --user USER` | Remote user for the ssh connections. |
| `-t, --connect-timeout SECONDS` | Passed through as ssh `ConnectTimeout`. |
| `-u, --command-timeout SECONDS` | Wall-clock limit on the remote command. |
| `-R, --worker exec\|ssh` | Force the execution backend. By default claw uses `exec` only when every target is `localhost`, `127.0.0.1`, or `::1`; otherwise it uses ssh. |
| `-o, --options OPTS` | Verbatim ssh option string, inserted into the command line as written. Include the leading `-o` yourself. |
| `--remote PATH` | The binary that stands in for ssh. Default `ssh`. |
| `--topology FILE` | Topology file for tree mode. |
| `-b, --dshbak` | Gather nodes whose output is byte-identical under one header. |
| `-L, --line` | Line mode: no output gathering. |
| `-N, --label` | Turn node labels off. The flag reads as "label" but it disables them. |
| `-q, --quiet` | Print nothing; report only through the exit code. |
| `-S, --maxrc` | Exit with the highest return code seen, instead of 0/1. |
| `--axis RANGESET` | Fold output headers along these axes only. |
| `--copy FILE` | Copy a file to every target node. |
| `--rcopy FILE` | Copy a file back from the nodes to the local machine. |
| `--dest DIR` | Destination directory for `--copy` and `--rcopy`. |
| `COMMAND ...` | The command to run. When no positional argument is given and stdin is not a TTY, claw reads the command from stdin. |

## Node selection syntax

The `-w`, `-x`, and `-X` values use the ClusterShell nodeset language:

| Form | Resolves to |
| --- | --- |
| `node7` | the single node `node7` |
| `node[1-5]` | `node1` … `node5` |
| `node[1-5,10]` | a range plus a single index |
| `node[001-003]` | zero-padded, so `node001` … `node003` |
| `node1,node2,node3` | an explicit comma list |
| `web[1-3],db[1-2]` | a union of two families |
| `node[1-10]!node[5-7]` | set difference (`!`) |
| `node[1-10]&node[5-15]` | intersection (`&`) |
| `node[1-5]^node[3-7]` | symmetric difference (`^`) |

`claw` resolves its sources in a fixed order, and the order decides which
subtraction wins:

1. `-w` nodeset.
2. `-a` and `-g` groups, then `-X` exclusions.
3. `--hostfile` entries.
4. `-x` exclusions.
5. `--pick N` trim.

The sources union together. `-X` is only consulted when `-g` or `-a` is also
present, so `claw -w 'node[1-8]' -X gpu` removes nothing. Use `-x` for a plain
subtraction. If the set ends up empty, claw exits with
`no target nodes specified (use -w, -a, -g, or --hostfile)`.

## Output modes: `-b`, `-L`, and `-q`

| Mode | Behavior |
| default | Streams each line as it completes, prefixed with its node label unless `-N` is set. A progress bar appears on stderr when stderr is a TTY and more than one node is targeted. |
| `-b, --dshbak` | Buffers instead of streaming, then prints one header per group of nodes that produced identical output, largest group first, with a folded bracket header and a node count. |
| `-L, --line` | Line mode, no output gathering. |
| `-q, --quiet` | No stdout, no progress bar, no per-node return-code lines. The exit code is the only signal. |

Non-zero return codes and per-node timeouts are reported on stderr in every mode
except `-q`, one folded node set per message.

## How claw builds the remote command

For every target node, claw assembles one line and hands it to `sh -c`:

```
<remote> <options> -o ConnectTimeout=<t> [<user>@]%h '<shell-escaped command>'
```

`%h` is substituted with the node name before the shell sees the line, and the
command is single-quoted unless it needs no quoting. With `claw -w node1 uptime`
and no other flags that expands to `ssh node1 uptime`.

Two flags control the transport.

### `--remote` swaps the transport binary

`--remote` replaces the `ssh` binary itself. The replacement still receives the
same `-o … [user@]%h 'cmd'` arguments, so a short wrapper can add site policy
without touching ssh configuration:

```bash
#!/bin/sh
exec /usr/bin/ssh -o ProxyCommand="ssh -W %h:%p jump-host.example.com" "$@"
```

```bash
claw -w 'node[1-8]' -f 16 -t 10 --remote /usr/local/bin/cluster-ssh uptime
```

Because the wrapper hides the `%h` from claw, ssh expands it itself. Either
route lands on the same destination.

### `--options` passes ssh options verbatim

`--options` is inserted into the command line exactly as given, so the value must
carry the leading `-o` itself:

```bash
claw -w 'node[1-8]' -f 16 -t 10 --options='-o BatchMode=yes' uptime
```

### The `--options=-F…` gotcha

A value that starts with a hyphen must be attached with `=`. Written
space-separated, clap reads the next token as another set of short flags:
`claw --options -F/etc/ssh/cluster_config` sets claw's own `-F` output format and
never passes the config file. The same applies to any hyphen-leading value,
including a `ProxyCommand` naming a flag-like path.

```bash
claw -w 'node[1-4]' --options=-F/etc/ssh/cluster_config hostname
```

### Multi-hop with `ProxyCommand` and `%h`

Because `%h` is substituted with the node name before the shell runs, a `%h`
inside a `ProxyCommand` value resolves to the node being targeted. Quote the
inner value so the shell hands ssh one argument:

```bash
claw -w 'node[1-8]' -f 16 -t 10 \
  --options='-o ProxyCommand="ssh -W %h:%p jump-host.example.com"' \
  systemctl is-system-running
```

For `node1` that assembles
`ssh -o ProxyCommand="ssh -W node1:22 jump-host.example.com" -o ConnectTimeout=10 node1 'systemctl is-system-running'`.

The same pattern belongs in `~/.ssh/config` when the jump host is permanent; see
`references/cli-surface.md` for a two-block snippet and the `ssh -G` check that
proves the hop does not recurse through itself.

## Groups and groups.conf

`-g`, `-X`, `-a`, and every `pinch` group query read `groups.conf`, loaded from
`~/.config/clustershell/groups.conf` then `/etc/clustershell/groups.conf`. Both
are read when both exist, and a section defined in both resolves from the one
read last, the system file. Every section other than `[Main]` becomes a group
source and needs a `map` or `mapall` upcall or the load fails. With neither file
present, group-based selection fails with
`no groups.conf found (checked ~/.config/clustershell/ and /etc/clustershell/)`;
pure `-w` and `--hostfile` runs need no config file at all.


## Exit codes

| Situation | Exit code |
| --- | --- |
| Every node returned 0 | 0 |
| Any node returned non-zero, default mode | 1 |
| Any node returned non-zero, with `-S` | the highest return code seen |
| claw could not start or resolve its targets | 1, with `claw: <error>` on stderr |

Use `-S` in scripts that must distinguish, say, a missing binary (127) from a
failed health check (1). Use the default in `if claw …; then` guards, where any
non-zero is a failure. `-q` and `-S` compose cleanly for a status sweep that
prints nothing and still reports the worst status.

## Reference

`references/cli-surface.md` carries the per-binary argument reference and a
copy-pasteable jump-host `ssh_config` snippet.

