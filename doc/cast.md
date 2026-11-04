# cast: push deploys for NixOS / nix-darwin fleets

`cast` (in `packages.<system>.consortium-cli`, the flake's default package)
deploys a flake from one workstation: every target's system closure is
evaluated and built locally, `nix copy`'d to its host over SSH, then
activated, with the four stages pipelined per host by the DAG executor
(`--cascade` swaps the copy stage for peer-to-peer fan-out). Targets need
the same key-based SSH as [`claw`](ssh.md).

```sh
cast --flake . eval                              # list targets, no nix build
cast --flake . build --on box[01-03]             # build only
cast --flake . deploy --on @darwin switch        # a ClusterShell group
cast --flake github:me/cfg deploy --on mac01,@gpu switch
cast --flake . --nixos-nix-args '--override-input secrets path:./stub' deploy
cast --flake . deploy --on @darwin --activate-timeout 600 switch  # fail hosts stuck >10 min
cast --flake . deploy --on 'node[01-03]' --cascade --cascade-fanout 2 switch
cast health                                      # probe distributed builders
cast status --on 'node[01-03]'                   # deployed system per host
cast hosts                                       # fleet nodes and tags
```

## Try it against your hosts

[`examples/nix/walkthrough.sh`](../examples/nix/walkthrough.sh) writes a
fleet file for `NODES`, reads it back with `cast hosts` / `cast tags`, and runs
`cast deploy --dry-run`, which resolves every target's SSH endpoint and
prints the plan without building, copying, or activating. CI runs it against
real `sshd` hosts:

```sh
NODES='web[01-04]' SSH_USER=root examples/nix/walkthrough.sh
```

[`examples/inventories/fleet.json`](../examples/inventories/fleet.json) is a
fuller fleet (builders plus Ansible, Slurm, Ray, and SkyPilot sections) used
by the [library examples](library.md).

## How a deploy resolves fleet, targets, and hosts

**Fleet source.** In order of precedence: `--config FILE`; `./fleet.json`
when it exists; the flake's `fleet` output (an `mkFleet` result — its
`configJson` is read directly, or a derivation producing the JSON file is
built); and finally a minimal fleet derived from the flake's
`darwinConfigurations` / `nixosConfigurations` attribute names (profile type
by output, `targetHost` = attribute name, tag `nix-darwin` / `nixos`, ssh
user `$USER` unless `--user` is given). Only attribute names are evaluated in
the last case, so it is cheap. `--flake` defaults to the current directory
and overrides the fleet's `flakeUri`; `--user` overrides every `targetUser`.

**Targets.** `--on` takes bracket notation and `@group` / `@source:group`
references, resolved through ClusterShell `groups.conf`
(`$XDG_CONFIG_HOME/clustershell`, `~/.config/clustershell`,
`/etc/clustershell`) exactly as `claw -g` does, then flat
`groups.d/*` files with `group: nodeset` lines. Group entries written as
`host.local` or `host.example.org` map to the fleet node `host`. `--tag`
selects by fleet tag; with neither flag every node is targeted.

**Endpoint resolution.** Before copy and activation, each remote target's
ssh endpoint is resolved live: the fleet's `targetHost`, then `<name>.local`,
then `<name>`, then the IPv4 addresses those resolve to, deduplicated; the
first one accepting a non-interactive `ssh … true` is used for both `nix copy`
and activation. Hosts that cannot be reached are reported with every endpoint
tried and skipped (the run exits non-zero). A target whose name is this
machine's hostname is not copied to and is activated locally through `sudo`.

**Activation.** NixOS: `nix-env --set` (switch/boot) then
`switch-to-configuration <action>`. nix-darwin: `nix-env --set` (switch/boot),
the legacy `activate-user` only when it exists and is not nix-darwin's
deprecated stub, then `<toplevel>/activate` — all through `sudo` unless the
ssh user is `root`.

**Extra nix arguments.** `--nix-args WORDS` (every host),
`--darwin-nix-args WORDS` and `--nixos-nix-args WORDS` (per platform) append
whitespace-split words to each `nix eval` / `nix build`; repeat the flag to
append more. Typical uses: `--override-input` to stub an input one platform
must not fetch, or `--option builders ''` to keep a deploy off the fleet's
own distributed builders.

## Peer-to-peer copy: `cascade-copy` and `cast cascade`

`cascade-copy` distributes one store path to a fleet peer-to-peer: each host
that receives the closure becomes a source for the next round, so copy time
grows with log N instead of N. The inventory is a TOML file with a `seed` and
`nodes` ([example](../examples/inventories/nixlab-safe.toml)); the store path
must already exist on the seed.

```sh
cascade-copy /nix/store/…-toplevel -i examples/inventories/nixlab-safe.toml --fanout 2
```

Strategies: `level-tree` (default), `log2-fanout`, `max-bottleneck`,
`steiner`. `cast cascade` renders and checks cascades without touching hosts:

```sh
cast cascade live -n 32                                  # simulate, round by round
cast cascade live -n 64 -s max-bottleneck --fanout 3
cast cascade live -n 32 --failure-rate 0.2 --failure-seed 7   # watch re-routing
cast cascade live -n 16 --format json
cast cascade tree trace.jsonl                            # replay a recorded trace
cast cascade verify trace.jsonl --fanout 2 --nodes 64    # non-zero if not P2P
```
