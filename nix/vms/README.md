# nix/vms — declarative microVM test fleet

One unified flake design: every test node is a microVM built with
[microvm.nix](https://github.com/microvm-nix/microvm.nix) (formerly
`astro/microvm.nix`). Each consortium integration is a pure NixOS module
layered on a shared, minimal base node.

> **Status:** this README documents the design contract. See
> [`doc/testing-microvms.md`](../../doc/testing-microvms.md) for the full
> operator runbook (build, run, test loop, troubleshooting).

## Layout

| Path | Contents |
|---|---|
| `base.nix` | Base test node NixOS module: microvm.nix **qemu** microVM, `x86_64-linux`, 512 MiB RAM, 1 vCPU, host-`/nix/store` share via virtiofs or 9p (no disk image), `sshd` with root login via the baked test key, static test IP on the tap interface, per-node hostname. |
| `plugins/nix.nix` | nix daemon in the guest: sandbox off, `experimental-features = nix-command flakes`. |
| `plugins/slurm.nix` | Single-node Slurm: `slurmctld` + `slurmd` + `munge`, all-in-one. |
| `plugins/ansible.nix` | `python3` + `ansible-core` on `PATH`. |
| `plugins/ray.nix` | `python3` with `ray`. May be a minimal ray-head-only variant if the closure would exceed ~1.5 GiB (see comments in the module). |
| `plugins/skypilot.nix` | `python3` with `skypilot`. Same size discipline as ray. |
| `default.nix` | `mkVm : name -> extraModules -> nixosConfiguration`, composing `base.nix` + plugin modules; exports the node attrset below. |
| `keys/` | **TEST-ONLY** ssh keypair (`id_test`, `id_test.pub`). See warning below. |

## The fleet

| Node | Plugin modules |
|---|---|
| `vm-base` | — (bare base node, smoke tests) |
| `vm-nix` | `plugins/nix.nix` |
| `vm-slurm` | `plugins/slurm.nix` |
| `vm-ansible` | `plugins/ansible.nix` |
| `vm-ray` | `plugins/ray.nix` |
| `vm-skypilot` | `plugins/skypilot.nix` |

Flake outputs (all `x86_64-linux`):

- `nixosConfigurations.vm-<name>` — full NixOS configuration per node.
- `packages.x86_64-linux.vm-<name>` — the microvm qemu runner for each node
  (microvm.nix `config.microvm.runner.qemu` / `declaredRunner`; the package's
  main program is `microvm-run`).
- A colmena-compatible hive output (`colmenaHive` / `colmena`) describing the
  same fleet for deployment to a KVM host.

## ⚠️ Test-only ssh keypair

`keys/id_test` is a **private key committed to this repo on purpose**. It
exists solely so that throwaway test VMs can accept root ssh without any
per-operator setup.

- **Never** use this keypair for anything outside these microVMs.
- **Never** add `id_test.pub` to any real machine, service, or account.
- Root login is enabled in the guests **only** with this key, only for the
  test fleet.

## Where to go next

- Build / run / E2E test loop / troubleshooting:
  [`doc/testing-microvms.md`](../../doc/testing-microvms.md)
- microvm.nix handbook: <https://microvm-nix.github.io/microvm.nix/>
