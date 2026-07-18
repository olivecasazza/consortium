# CLI walkthrough

Six binaries ship from `crates/consortium-cli`. Run any of them from the
repository root via cargo:

```
cargo run -p consortium-cli --bin <name> -- [ARGS]
```

(or `cargo build --release -p consortium-cli` once and call the binaries
directly from `target/release/`.)

| binary | upstream equivalent | needs infrastructure? |
|---|---|---|
| `claw` | `clush` | yes — SSH hosts (except local exec) |
| `pinch` | `nodeset` | no — pure set algebra |
| `molt` | `clubak` / `dshbak` | no — reads stdin |
| `cast` | (nixos deployment) | yes — nix + SSH fleet |
| `cascade-copy` | — | yes — nix + SSH fleet |
| `cascade-viz` | — | no (offline simulation) |

## pinch — nodeset algebra (offline)

Fold, expand, count, and set operations on node sets:

```
$ cargo run -p consortium-cli --bin pinch -- node[1-5]
node[1-5]

$ cargo run -p consortium-cli --bin pinch -- -e node[1-3]
node1 node2 node3

$ cargo run -p consortium-cli --bin pinch -- -f node1 node2 node3
node[1-3]

$ cargo run -p consortium-cli --bin pinch -- -c node[1-100]
100

# set algebra: intersection, exclusion, xor, multiple inputs (union)
$ cargo run -p consortium-cli --bin pinch -- node[1-10] -i node[5-15]
node[5-10]
$ cargo run -p consortium-cli --bin pinch -- node[1-10] -x node[8-99]
node[1-7]
$ cargo run -p consortium-cli --bin pinch -- node[1-5] node[3-8]
node[1-8]

# RangeSet mode, autostep folding (fold stepped ranges of >= N elements),
# splitting
$ cargo run -p consortium-cli --bin pinch -- -R --autostep 3 0,2,4,6,8,10,12,14,16
0-16/2
$ cargo run -p consortium-cli --bin pinch -- --split 2 node[1-6]
node[1-3]
node[4-6]
```

## molt — gather identical outputs (offline)

Reads `node: output` lines from stdin (e.g. piped from `claw` or dsh) and
groups nodes that produced identical output — the clubak/dshbak primitive:

```
$ printf 'node1: 5.15.0\nnode2: 5.15.0\nnode3: 6.1.0\n' | cargo run -p consortium-cli --bin molt -- -b
---------------
node[1-2] (2)
---------------
 5.15.0
---------------
node3
---------------
 6.1.0
```

`-L` gives line mode, `-S` changes the key separator, `-T` traces the
message tree.

## claw — parallel remote execution (needs SSH hosts)

Fan a command out to many hosts over SSH, with `%h`-style per-node output
collection:

```
# run on a nodeset, gathered per-node output
$ cargo run -p consortium-cli --bin claw -- -w node[1-5] uptime

# dshbak mode: group nodes with identical output under one header
$ cargo run -p consortium-cli --bin claw -- -b -w node[1-5] uname -r

# remote user, fanout limit, ssh options
$ cargo run -p consortium-cli --bin claw -- -l root -f 32 -w web[01-16] systemctl restart nginx

# copy a file to all targets (reverse with --rcopy, --dest DIR)
$ cargo run -p consortium-cli --bin claw -- -w node[1-5] --copy ./app.conf --dest /etc/app/

# report only the max return code (scripting)
$ cargo run -p consortium-cli --bin claw -- -Sq -w node[1-5] 'false'; echo $?
```

`-R exec` forces the local exec worker (runs the command locally once per
node with `%h` substituted — see `examples/rust/fanout_local.rs` for the
library equivalent).

## cast — fleet deployment (needs nix + SSH fleet)

NixOS deployment orchestration driven by a fleet JSON — try it against
`examples/inventories/fleet.json` (subcommands: `eval`, `build`, `deploy`,
`health`, `status`; fleet file via `-c/--config`, defaults to `./fleet.json`):

```
# which hosts would deploy?
$ cargo run -p consortium-cli --bin cast -- -c examples/inventories/fleet.json eval

# build closures for two hosts (no copy/activate)
$ cargo run -p consortium-cli --bin cast -- -c examples/inventories/fleet.json build --on 'node[01-02]'

# full deploy: build + copy + activate, by tag; action is positional
# (switch|boot|test|dry-activate|build, default switch)
$ cargo run -p consortium-cli --bin cast -- -c examples/inventories/fleet.json deploy --tag web switch
$ cargo run -p consortium-cli --bin cast -- -c examples/inventories/fleet.json deploy --on 'node[01-03]' --cascade --cascade-fanout 2

# probe remote builders, show deployed system versions
$ cargo run -p consortium-cli --bin cast -- -c examples/inventories/fleet.json health
$ cargo run -p consortium-cli --bin cast -- -c examples/inventories/fleet.json status --on 'node[01-03]'
```

## cascade-copy — P2P store-path distribution (needs nix + SSH fleet)

Distribute one nix store path to a fleet peer-to-peer: each host that
receives the closure joins the source pool for the next round, so total
copy time drops from O(N) to O(log N). Inventory is a TOML file with a
`seed` and `nodes` — see `examples/inventories/nixlab-safe.toml`:

```
$ cargo run -p consortium-cli --bin cascade-copy -- /nix/store/…-toplevel \
    -i examples/inventories/nixlab-safe.toml --fanout 2
```

The store path must already exist on the seed host. Strategies:
`level-tree` (default), `log2-fanout`, `max-bottleneck`, `steiner`.

## cascade-viz — replay or simulate a cascade (offline)

Render a cascade as a tree — either replaying a recorded JSONL trace or
running a fresh simulated scenario (deterministic, no infrastructure):

```
# live simulation: 32 nodes, binary level-tree, rendered round by round
$ cargo run -p consortium-cli --bin cascade-viz -- live -n 32

# other strategies and shapes
$ cargo run -p consortium-cli --bin cascade-viz -- live -n 64 -s max-bottleneck --fanout 3
$ cargo run -p consortium-cli --bin cascade-viz -- live -n 16 --seeds 4 --closure-mb 200

# inject random edge failures (deterministic per seed) to watch re-routing
$ cargo run -p consortium-cli --bin cascade-viz -- live -n 32 --failure-rate 0.2 --failure-seed 7

# machine-readable output
$ cargo run -p consortium-cli --bin cascade-viz -- -f json live -n 16
```
