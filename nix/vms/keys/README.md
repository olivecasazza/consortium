# ⚠️ TEST-ONLY SSH KEYPAIR — DO NOT REUSE

`id_test` / `id_test.pub` are a **private/public keypair committed to this
repo on purpose**. The public key is baked into every consortium test
microVM (see `nix/vms/base.nix`) as root's *only* authorized key, so the
test suite can ssh into throwaway VMs with zero per-operator setup.

Rules:

- **Never** use this keypair for anything outside these test microVMs.
- **Never** add `id_test.pub` to any real machine, service, CI secret, or
  account's `authorized_keys`.
- Anyone with repo access can log in as **root** to any VM built from
  `nix/vms/`. That is acceptable *only* because those VMs are ephemeral
  test nodes on the isolated `10.99.0.0/24` tap subnet with no secrets.

Usage (from the KVM host, or any driver with L2 access to the tap subnet):

```sh
ssh -i nix/vms/keys/id_test \
    -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    root@10.99.0.10   # vm-base; see nix/vms/default.nix for the IP map
```

Regenerate (only if rotating the fleet key deliberately):

```sh
ssh-keygen -t ed25519 -f nix/vms/keys/id_test -N '' -C 'consortium-test-vm-root-TEST-ONLY'
```
