# Consortium

Run commands across many hosts, and deploy NixOS / nix-darwin fleets, from
one Rust toolkit. Consortium is a Rust reimplementation of
[ClusterShell](https://github.com/cea-hpc/clustershell) with Nix packaging and
scheduler / infrastructure integrations.

```sh
nix shell .   # claw, pinch, molt, cast, cascade-copy on PATH
```

## Start here

| You want to… | Read |
| --- | --- |
| Run a command on many hosts, fold node sets, gather output (`claw`, `pinch`, `molt`) | [CLI walkthrough](examples/cli/README.md) |
| Deploy a flake to NixOS / nix-darwin machines (`cast`) | [cast](doc/cast.md) |
| Drive Nix, Slurm, Ansible, Ray, or SkyPilot through one grammar | [Integrations](doc/integrations.md) |
| Use the Rust library | [Rust examples](examples/README.md), `cargo doc --workspace --no-deps` |
| Add an integration | [Integrations → contract tests](doc/integrations.md#contract-test-suite), [grammar design](.specs/draft/integration-grammar.md) |
| Find your way around the code | [Repository structure](doc/structure/index.md) |
| Contribute | [CI, commits, grammar gate](doc/structure/ci-tooling.md), [specs and roadmap](.specs/index.md) |
| Check ClusterShell parity or use Python bindings | [consortium-tests](https://github.com/olivecasazza/consortium-tests) |
| Give an agent the CLI | [skills/consortium](skills/consortium/SKILL.md) |

## Binaries

| Binary | Replaces | Does |
| --- | --- | --- |
| `claw` | `clush` | Parallel command execution over SSH |
| `pinch` | `nodeset` | Node-set algebra |
| `molt` | `clubak` / `dshbak` | Groups identical output |
| `cast` | colmena | Fleet deploys and `cast integration <adapter>` |
| `cascade-copy` | — | Peer-to-peer store-path fan-out |

## Develop

```sh
nix develop
cargo test --workspace
nix flake check
```

Python compatibility, the upstream ClusterShell oracle, and Docker
integration suites live in the sibling
[consortium-tests](https://github.com/olivecasazza/consortium-tests) checkout
(`../consortium-tests`). This repository is Rust and Nix only.

## License

LGPL-2.1-or-later. Consortium derives from ClusterShell, copyright CEA/DAM,
Aurelien Degremont, Stephane Thiell, and contributors; the original license
is preserved in [COPYING.LGPLv2.1](COPYING.LGPLv2.1).
