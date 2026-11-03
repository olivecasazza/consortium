# cast: push deploys for NixOS / nix-darwin fleets

`cast` (in `packages.<system>.consortium-cli`, the flake's default package)
deploys a flake from one workstation: every target's system closure is
evaluated and built locally, `nix copy`'d to its host, then activated, with
the four stages pipelined per host by the DAG executor (`--cascade` swaps the
copy stage for peer-to-peer fan-out).

```sh
cast --flake . eval                              # list targets, no nix build
cast --flake . build --on box[01-03]             # build only
cast --flake . deploy --on @darwin switch        # a ClusterShell group
cast --flake github:me/cfg deploy --on mac01,@gpu switch
cast --flake . --nixos-nix-args '--override-input secrets path:./stub' deploy
cast --flake . deploy --on @darwin --activate-timeout 600 switch  # fail hosts stuck >10 min
```

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
