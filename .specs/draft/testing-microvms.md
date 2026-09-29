# Testing with microVMs — operator runbook

The consortium test environment is **one declarative Nix setup**: every test
node is a microVM built with [microvm.nix](https://github.com/microvm-nix/microvm.nix).
There is no ad-hoc VM tooling — each integration (nix, slurm, ansible, ray,
skypilot) is a pure NixOS module layered onto a shared 512 MiB base node, all
composed in one flake.

> **Status:** this is design/runbook documentation written against the fixed
> flake contract in `nix/vms/`. Commands below are the intended operator
> workflow; they have **not** been verified end-to-end yet. Where a command
> depends on details defined in the Nix modules (IP addresses, tap interface
> names), the doc says so explicitly — check `nix/vms/base.nix`.

## The fleet

| Node | What it runs | Plugin module |
|---|---|---|
| `vm-base` | bare base node (smoke tests) | — |
| `vm-nix` | nix daemon, sandbox off, `nix-command flakes` | `nix/vms/plugins/nix.nix` |
| `vm-slurm` | single-node `slurmctld` + `slurmd` + `munge` | `nix/vms/plugins/slurm.nix` |
| `vm-ansible` | `python3` + `ansible-core` on `PATH` | `nix/vms/plugins/ansible.nix` |
| `vm-ray` | `python3` + `ray` (possibly minimal head-only variant) | `nix/vms/plugins/ray.nix` |
| `vm-skypilot` | `python3` + `skypilot` (possibly minimal variant) | `nix/vms/plugins/skypilot.nix` |

Every node is a qemu microVM on `x86_64-linux`: 512 MiB RAM, 1 vCPU, the
host's `/nix/store` mounted read-only into the guest via virtiofs or 9p (no
disk image, no duplicated store), `sshd` with root login via the **test-only**
keypair in `nix/vms/keys/`, and a static test IP on a tap interface.

Flake outputs:

- `nixosConfigurations.vm-{base,nix,slurm,ansible,ray,skypilot}`
- `packages.x86_64-linux.vm-<name>` — microvm qemu runner per node
- colmena hive output (`colmenaHive` / `colmena`) for the same fleet

## Prerequisites

- **This Mac** (aarch64-darwin, no KVM): builds only. Any `x86_64-linux`
  derivation is automatically delegated to the remote builder
  `ssh://root@pdx-nxst-001.schrodinger.com` (configured in
  `/etc/nix/machines`, features `big-parallel,kvm,nixos-test`).
- **A Linux+KVM host to run the VMs.** The runbook target is
  `pdx-nxst-001.schrodinger.com` — the same machine that builds also carries
  the `kvm` feature, so it doubles as the runtime host. microVMs **cannot**
  run on the Mac; do not try.
- The test ssh key at `nix/vms/keys/id_test` (already in the repo).

## 1. Build (on the Mac)

Build one node's runner — the example below uses `vm-slurm`; substitute any
node name:

```bash
nix build .#packages.x86_64-linux.vm-slurm -L
```

Because the platform is `x86_64-linux`, Nix ships the build to
`pdx-nxst-001` automatically and copies the result back. The `./result`
symlink now points at the runner package, whose main program is
`microvm-run`:

```bash
ls -l result/bin/        # expect: microvm-run (+ possibly microvm-shutdown etc.)
```

Cheap eval-only sanity checks (no build, no remote builder needed):

```bash
# Does the flake expose the expected outputs?
nix eval --apply builtins.attrNames .#packages.x86_64-linux
nix eval --apply builtins.attrNames .#nixosConfigurations

# Evaluate a node config without building it:
nix eval .#nixosConfigurations.vm-slurm.config.networking.hostName
```

Build the whole fleet if you need all nodes (remember: closures land in your
local `/nix` too — see §5):

```bash
nix build .#packages.x86_64-linux.vm-{base,nix,slurm,ansible,ray,skypilot} -L
```

## 2. Run (on the KVM host)

microVMs need Linux + KVM. The runner does **not** create tap interfaces or
start `virtiofsd` itself (upstream microvm.nix behavior, verified from its
source: qemu is invoked with `script=no downscript=no`, and virtiofs shares
expect an existing socket). Two ways to run:

### 2a. Quick path: build/copy the runner to the host and start it

**Step 1 — get the closure onto the KVM host.** Since Mac-initiated builds
already execute on `pdx-nxst-001`, the store paths are usually already there;
`nix copy` is then a no-op. From the Mac:

```bash
STORE_PATH=$(readlink ./result)   # after building vm-slurm above
nix copy --to ssh://root@pdx-nxst-001.schrodinger.com "$STORE_PATH"
```

Alternative: clone/copy this repo on the host and build there directly:

```bash
ssh root@pdx-nxst-001.schrodinger.com
cd /path/to/consortium
nix build .#packages.x86_64-linux.vm-slurm -L
```

**Step 2 — host preparation (tap interface + host-side address).** The tap
interface id and the guest's static IP/subnet are defined in
`nix/vms/base.nix` (`microvm.interfaces` / `networking.interfaces`). Look
them up, then create the tap on the host — example, assuming id `vm-slurm`
and subnet `10.0.0.0/24` with the host at `10.0.0.1` (**adjust to whatever
`base.nix` actually declares**):

```bash
# on pdx-nxst-001, as root
ip tuntap add dev vm-slurm mode tap
ip addr add 10.0.0.1/24 dev vm-slurm   # host end of the guest's subnet
ip link set vm-slurm up
```

If the base node uses a **virtiofs** share for `/nix/store` (rather than 9p),
`virtiofsd` must be running on the host before the VM starts. The systemd
integration in step 2b handles this automatically; for the manual path,
either start `virtiofsd` for the share's socket (see
`result/share/microvm/virtiofs/`) or confirm `base.nix` chose the 9p variant,
which needs no host-side daemon.

**Step 3 — start the VM.** The serial console is on stdio, so run it in a
terminal you can keep open:

```bash
ssh root@pdx-nxst-001.schrodinger.com "$STORE_PATH/bin/microvm-run"
```

Or detach it under systemd on the host:

```bash
ssh root@pdx-nxst-001.schrodinger.com \
  "systemd-run --unit=vm-slurm --collect $STORE_PATH/bin/microvm-run"
```

If the exact entrypoint ever differs, check `result/bin/` — microvm.nix sets
`meta.mainProgram = "microvm-run"` on its runner packages.

**Step 4 — ssh into the guest.** From the KVM host (or via ProxyJump from
the Mac if the VM subnet isn't routable from your desk):

```bash
# on the host
ssh -i nix/vms/keys/id_test -o StrictHostKeyChecking=accept-new root@<vm-ip>

# from the Mac, jumping through the host
ssh -i nix/vms/keys/id_test -o StrictHostKeyChecking=accept-new \
    -J root@pdx-nxst-001.schrodinger.com root@<vm-ip>
```

`<vm-ip>` is the static test IP assigned per node — discover it with:

```bash
grep -n "ipv4" nix/vms/*.nix
# or from the evaluated config:
nix eval --json .#nixosConfigurations.vm-slurm.config.microvm.interfaces
```

### 2b. Managed path: declare the VMs on the host

For repeated use, microvm.nix's host-side NixOS integration
(`microvm.vms.<name>` on the host's own configuration, or the imperative
`microvm` command) manages tap interfaces, `virtiofsd`, and systemd services
for you. See the [microvm.nix handbook](https://microvm-nix.github.io/microvm.nix/)
("Host with virtual machines" / "microvm command" chapters). This is the
recommended shape if the fleet becomes long-lived.

## 3. Deploy/manage with colmena

The flake also exposes a colmena-compatible hive (`colmenaHive` / `colmena`)
describing the same fleet. colmena is **not** installed on the Mac — run it
from nixpkgs:

```bash
# list what the hive thinks the nodes are
nix run nixpkgs#colmena -- eval -E '{ nodes, ... }: builtins.attrNames nodes'

# build all node closures (delegates to pdx-nxst-001 like any x86_64-linux build)
nix run nixpkgs#colmena -- build --on 'vm-*'

# deploy + activate on the running nodes (they must be up and ssh-reachable)
nix run nixpkgs#colmena -- apply --on 'vm-*'

# run a command across the fleet
nix run nixpkgs#colmena -- exec --on 'vm-*' -- hostname
```

Notes:

- Quote `'vm-*'` so your shell doesn't glob.
- colmena drives ssh as the operator user; make the test key available to
  your agent first:

  ```bash
  eval "$(ssh-agent -s)"
  ssh-add nix/vms/keys/id_test
  ```

## 4. E2E test loop (consortium CLI against the fleet)

The intended loop: the consortium CLIs (`claw`, `molt`, `pinch`, `cast`) run
on the operator machine and reach the microVM nodes over ssh. If the VM
subnet is only reachable from `pdx-nxst-001`, run the CLI there or use
ssh's `ProxyJump` (`-J`) as in §2.

Smoke-test shape once two nodes are up (mirrors the `node01`/`node02` shape
in `examples/inventories/fleet.json` — point the same CLIs at the microVM
IPs instead of the example's `192.168.1.1x` addresses):

```bash
# parallel command across both nodes — clush-style
claw -w root@<vm01-ip>,root@<vm02-ip> -l root \
    -o "-i nix/vms/keys/id_test -o StrictHostKeyChecking=accept-new" \
    hostname

# gather identical outputs — dshbak-style
claw -b -w root@<vm01-ip>,root@<vm02-ip> -l root \
    -o "-i nix/vms/keys/id_test" uname -a
```

Integration-shaped checks per node:

```bash
# vm-nix: flakes work, sandbox off
ssh -i nix/vms/keys/id_test root@<vm-nix-ip> \
  'nix --extra-experimental-features "nix-command flakes" eval --impure --expr builtins.currentSystem'

# vm-slurm: single-node cluster answers
ssh -i nix/vms/keys/id_test root@<vm-slurm-ip> 'sinfo && srun -N1 hostname'

# vm-ansible: ansible-core on PATH
ssh -i nix/vms/keys/id_test root@<vm-ansible-ip> 'ansible --version'

# vm-ray / vm-skypilot: interpreter + library import
ssh -i nix/vms/keys/id_test root@<vm-ray-ip> 'python3 -c "import ray; print(ray.__version__)"'
ssh -i nix/vms/keys/id_test root@<vm-skypilot-ip> 'python3 -c "import sky; print(sky.__version__)"'
```

For deployment-oriented E2E, `cast` consumes the fleet JSON format in
`examples/inventories/fleet.json`; a test variant of that file pointing
`targetHost` at the microVM IPs (`targetUser: root`, ssh via the test key)
exercises the NixOS deployment path against throwaway nodes.

## 5. Size & speed discipline

- The base node is deliberately lean: 512 MiB / 1 vCPU, host `/nix/store`
  share instead of a disk image, no docs, no packages beyond sshd + a shell.
  Keep it that way — plugin modules are where extras belong.
- `ray` and `skypilot` pull large Python closures. If a module would exceed
  ~1.5 GiB, it is intentionally reduced to a minimal variant (ray-head-only,
  etc.) — check the comments in `nix/vms/plugins/ray.nix` /
  `skypilot.nix` before assuming full functionality.
- `/nix` on the Mac is ~88% full. Build one node at a time, and measure
  before adding anything to `base.nix`:

  ```bash
  nix path-info -Sh ./result        # closure size of what you just built
  df -h /nix
  ```

- First-time builds are slow (fresh NixOS closures on the remote builder);
  subsequent builds reuse the builder's store. Do **not** run
  `nix-store --gc` / `nix-collect-garbage` to reclaim space — coordinate
  cleanup out of band instead.

## 6. Troubleshooting

**Build fails with ssh/connection errors.** The remote builder is
unreachable or its host key changed. Verify:

```bash
ssh root@pdx-nxst-001.schrodinger.com true
cat /etc/nix/machines
```

`x86_64-linux` derivations can *only* build on a Linux builder — there is no
local fallback on the Mac. Eval-only commands (`nix eval ...`) still work
offline of the builder.

**"it evaluates but doesn't build" is expected.** Eval happens locally and
needs no KVM, no builder, no network; building realizes the closure on the
remote builder; running needs KVM on the host. Three different machines,
three different failure modes — check which stage you're actually in.

**Where is the runner?** After `nix build`, `./result` is the runner
package: `./result/bin/microvm-run` (microvm.nix sets
`meta.mainProgram = "microvm-run"`, and ships helper scripts plus metadata
under `result/share/microvm/`). If that path ever changes upstream,
`ls result/bin/` is the source of truth.

**`Could not access KVM kernel module` on the host.** `/dev/kvm` missing or
permission-denied — run as root or add the user to the `kvm` group; on
`pdx-nxst-001` confirm the module is loaded (`lsmod | grep kvm`).

**qemu can't bring up the network / tap not found.** The runner does not
create tap interfaces (upstream behavior). Create the tap on the host first
(§2, step 2), matching the interface id declared in `nix/vms/base.nix`.

**Guest fails to mount `/nix/store`.** If `base.nix` uses virtiofs,
`virtiofsd` isn't running on the host — start it (or use the host systemd
integration, §2b). 9p shares need no host daemon.

**ssh to the VM works from the host but not from the Mac.** The tap subnet
is host-local. Use ProxyJump (`ssh -J root@pdx-nxst-001.schrodinger.com ...`)
or run the test loop on the host.

**Flake input naming.** Upstream moved: `github:astro/microvm.nix` now
redirects to `github:microvm-nix/microvm.nix`. Either URL works in the
flake input, but expect the `flake.lock` entry to show the new owner.

## Security note

`nix/vms/keys/id_test` is a **test-only private key committed to this repo**.
It grants root on the microVM nodes — which are throwaway by design. Never
reuse it anywhere else; never authorize it on real infrastructure.
