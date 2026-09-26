# consortium CLI surface

Argument reference for the binaries in the `consortium-cli` package, plus a
jump-host `ssh_config` snippet. Node-set syntax (`node[1-5]`, comma lists, the
`!` `&` `^` operators) is described in `../SKILL.md`.

Every binary accepts the same shared output flags: `-v`/`--verbose` (repeatable,
`-v` info, `-vv` debug, `-vvv` trace), `--color auto|always|never` (default
`auto`, which honors `NO_COLOR` and TTY detection), and `-F`/`--format`
(default `tree`).

## claw

Parallel command execution. Command construction, output modes, and exit codes
are covered in `../SKILL.md`.

| Flag | Type | Default | Meaning |
| --- | --- | --- | --- |
| `-w, --nodes` | nodeset | — | Target nodes. |
| `-x, --exclude` | nodeset | — | Remove nodes from the set. Repeatable. |
| `-a, --all` | flag | false | Every node from the default group. |
| `-g, --group` | name | — | Nodes of a named group. Repeatable. |
| `-X, --exclude-group` | name | — | Drop a named group's nodes; needs `-g` or `-a`. |
| `--pick` | N | — | Keep N nodes from the resolved set. |
| `--hostfile` | path | — | Node list, one per line; `#` comments skipped. |
| `-f, --fanout` | N | 64 | Maximum concurrent connections. |
| `-l, --user` | name | — | Remote ssh user. |
| `-t, --connect-timeout` | seconds | — | Becomes ssh `ConnectTimeout`. |
| `-u, --command-timeout` | seconds | — | Wall-clock limit on the remote command. |
| `-R, --worker` | `exec`\|`ssh` | auto | Force the backend; auto picks `exec` only for an all-localhost set. |
| `-o, --options` | string | — | Verbatim ssh option string, leading `-o` included. |
| `--remote` | path | `ssh` | Binary that stands in for ssh. |
| `--topology` | path | — | Topology file for tree mode. |
| `-b, --dshbak` | flag | false | Gather identical output under one header. |
| `-L, --line` | flag | false | Line mode, no gathering. |
| `-N, --label` | flag | false | Disable node labels. |
| `-q, --quiet` | flag | false | No output; exit code only. |
| `-S, --maxrc` | flag | false | Exit with the highest node return code. |
| `--axis` | rangeset | all axes | Constrain header folding. |
| `--copy` | path | — | Copy a file to every target node. |
| `--rcopy` | path | — | Copy a file back from the nodes. |
| `--dest` | dir | — | Destination for `--copy` and `--rcopy`. |
| `COMMAND ...` | positional | stdin when not a TTY | The remote command. |

## molt

Aggregates `key: output` lines from stdin (or from files) and groups nodes with
identical output. It exposes no node-selection flags; the input determines the
node set.

| Flag | Type | Default | Meaning |
| --- | --- | --- | --- |
| `-b, --dshbak` | flag | false | Gather nodes with identical output. |
| `-L, --line` | flag | false | Line mode: no header, ordered by node name. |
| `-S, --separator` | string | `:` | Separator between key and message. |
| `-T, --tree` | flag | false | Message-tree trace mode. |
| `-G, --groupbase` | flag | false | Drop the group-source prefix in regroup output. |
| `--axis` | rangeset | all axes | Constrain header folding. |
| `--interpret-keys` | `never`\|`always`\|`auto` | `auto` | Parse keys as nodesets. In `auto`, one parse failure switches interpretation off for the rest of the run. |
| `FILES ...` | positional | stdin | Input files. |

```bash
claw -w 'node[1-8]' -L hostname | molt -b
```

## pinch

Node-set operations. No remote execution. Reads node sets from positional
arguments, or from stdin when none are given.

| Flag | Type | Default | Meaning |
| --- | --- | --- | --- |
| `-c, --count` | flag | false | Count nodes in the result. |
| `-e, --expand` | flag | false | Expand to individual node names. |
| `-f, --fold` | flag | default op | Fold node names into bracket notation. |
| `-l, --list` | count | 0 | List groups; repeat for detail (`-l`, `-ll`, `-lll`). |
| `-r, --regroup` | flag | false | Fold nodes using the group definitions. |
| `--groupsources` | flag | false | List the active group sources. |
| `-x, --exclude` | nodeset | — | Set difference. |
| `-i, --intersection` | nodeset | — | Set intersection. |
| `-X, --xor` | nodeset | — | Symmetric difference. |
| `-a, --all` | flag | false | All nodes from the default group. |
| `-g, --group` | name | — | Nodes of a named group. Repeatable. |
| `-S, --separator` | string | space for `-e`, newline for `-l` | Output separator. |
| `-R, --rangeset` | flag | false | Operate on a RangeSet instead of a NodeSet. |
| `--autostep` | number, `auto`, or `%N` | — | Autostep folding, e.g. `2` → `a-b/2`. |
| `--split` | N | — | Split the result into N subsets. |
| `--contiguous` | flag | false | Make the split subsets contiguous. |
| `--pick` | N | — | Pick N random nodes. |
| `-I, --slice` | rangeset | — | Keep the elements at the given positions. |
| `--index` | node | — | Print the index of a node in the set. |
| `-O, --output-format` | `%`-style | — | Format string for the `--index` result. |
| `NODESETS ...` | positional | stdin | Input node sets. |

The six operation flags are mutually exclusive; passing two of them is an error.

```bash
pinch -g all -e | head
pinch -c 'node[1-1000]'
pinch -l -ll
```

## cast

nixOS and nix-darwin fleet deployment, driven by a fleet configuration file.
Global: `-c, --config` (default `fleet.json`) and `--flake URI` to override the
flake URI from the config.

| Subcommand | Flags | Meaning |
| --- | --- | --- |
| `eval` | `-w, --on`, `-g, --tag` | Evaluate which hosts need a deployment. |
| `build` | `-w, --on`, `-g, --tag`, `--builders`, `-f, --fanout` (default 4) | Build the system closures. |
| `deploy [ACTION]` | `-w, --on`, `-g, --tag`, `--builders`, `-f, --fanout` (default 4), `--cascade`, `--cascade-fanout` (default 2) | Build, copy, and activate. ACTION defaults to `switch`. `--cascade` swaps per-host serial copy for the peer-to-peer cascade, cutting copy time from O(N) to O(log N) for hosts sharing a toplevel. |
| `health` | — | Probe builder health. |
| `status` | `-w, --on`, `-g, --tag` | Show current system versions on the targets. |

```bash
cast --config fleet.json eval -w 'hp[01-03]'
cast deploy -g all --cascade switch
```

## cascade-copy

Distributes one nix store path from a seed host across a fleet using a cascade
tree. Each host that receives the path joins the pool for the next round.

| Argument | Type | Default | Meaning |
| --- | --- | --- | --- |
| `STORE_PATH` | positional | — | The nix store path to distribute; must already exist on the seed host. |
| `-i, --inventory` | path | — | TOML file holding `seed` and `nodes` SSH addresses. Needs at least one seed and one target. |
| `-s, --strategy` | `level-tree`\|`log2-fanout`\|`max-bottleneck`\|`steiner` | `level-tree` | Cascade strategy. |
| `--fanout` | N | 2 | Children per node for level-tree. |
| `--timeout` | seconds | 300 | Per-edge `nix copy` subprocess timeout. |
| `--max-rounds` | N | 64 | Give up if the cascade has not converged by this round. |
| `--no-watch` | flag | false | Disable live re-rendering, for pipes and CI. |
| `-L, --max-depth` | N | — | Limit the rendered tree depth. |

```bash
STORE_PATH=$(nix build --no-link --print-out-paths .#hello)
cascade-copy "$STORE_PATH" -i inventory.toml -s level-tree --fanout 8
```

## cascade-viz

Replays JSONL traces or runs live cascade scenarios as a tree.

| Flag | Type | Default | Meaning |
| --- | --- | --- | --- |
| `-n, --nodes` | N | 32 | Nodes in the simulated fleet. |
| `-s, --strategy` | string | `level-tree` | Cascade strategy. |
| `--fanout` | N | 2 | Children per node. |
| `--seeds` | N | 1 | Number of seed hosts. |
| `--seed-fraction` | fraction | 0.0 | Fraction of nodes that start seeded. |
| `--closure-mb` | N | 50 | Simulated closure size. |
| `--bandwidth` | string | `uniform` | Bandwidth model. |
| `--uplinks` | list | — | Per-node uplink constraints. |
| `--seed` | N | 0 | Random seed. |
| `--failure-rate` | fraction | 0.0 | Edge failure rate. |
| `--failure-seed` | N | 0 | Random seed for failures. |
| `--per-round-delay` | duration | — | Artificial delay per round. |
| `-f, --format` | string | `tree` | Output format. |
| `-L, --max-depth` | N | — | Rendered tree depth. |
| `--no-color` | flag | false | Disable color. |
| `--no-watch` | flag | false | Disable live re-rendering. |

## ssh_config jump host

Copy this into `~/.ssh/config`, replacing the placeholders. ssh expands `%h` to
the destination host and `%p` to its port, so one `Host` block covers every node
in the set.

```
# The jump host itself. ForwardAgent stays off so the jump session cannot use
# your agent; the per-host block below is what forwards the connection.
Host jump-host.example.com
    ForwardAgent no

# Every cluster node is reached through the jump host.
Host node*
    User my-account
    ProxyCommand ssh -W %h:%p jump-host.example.com
    ConnectTimeout 10
    ServerAliveInterval 30
```

The two blocks must not overlap. Check the result with `ssh -G -F
~/.ssh/config node1`: the node resolves to a `proxycommand` line, and
`jump-host.example.com` resolves to none, so the hop connects straight out
instead of through itself.

The same route can live on the command line instead, which keeps the setting out
of the file entirely:

```bash
claw -w 'node[1-8]' -f 16 -t 10 \
  --options='-o ProxyCommand="ssh -W %h:%p jump-host.example.com"' \
  uptime
```

Or in a separate file selected with the equals-sign form:

```bash
claw -w 'node[1-8]' --options=-F/path/to/cluster_ssh_config uptime
```
