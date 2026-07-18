# nix/vms/default.nix — the consortium microVM test fleet.
#
# mkVm : name -> extraModules -> nixosConfiguration
#
# Composes base.nix + per-node settings (hostname, node number) + any
# plugin modules from ./plugins/<name>.nix into a nixosConfiguration.
#
# Exports:
#   mkVm        — the constructor (also used by the /tmp stub checks)
#   nodeNumbers — node name -> last octet of its static 10.99.0.0/24 IP
#   nodeModules — node name -> full module list (used for the colmena hive)
#   configs     — node name -> nixosConfiguration (flake nixosConfigurations)
#
# The fleet (all x86_64-linux qemu microVMs, IPs 10.99.0.<n>):
#   vm-base      10   bare base node (smoke tests)
#   vm-nix       11   + plugins/nix.nix
#   vm-slurm     12   + plugins/slurm.nix
#   vm-ansible   13   + plugins/ansible.nix
#   vm-ray       14   + plugins/ray.nix
#   vm-skypilot  15   + plugins/skypilot.nix
#
# Runbook (from a linux KVM host — microVMs cannot run on macOS):
#   nix build .#packages.x86_64-linux.vm-base        # build the qemu runner
#   nix run  .#packages.x86_64-linux.vm-base         # boot vm-base (as root, for tap)
#   ssh -i nix/vms/keys/id_test root@10.99.0.10      # log in (TEST-ONLY key)
# Full operator runbook: doc/testing-microvms.md
{ inputs }:

let
  lib = inputs.nixpkgs.lib;

  # Node name -> last octet of its static test IP (10.99.0.<n>/24).
  nodeNumbers = {
    vm-base = 10;
    vm-nix = 11;
    vm-slurm = 12;
    vm-ansible = 13;
    vm-ray = 14;
    vm-skypilot = 15;
  };

  # Plugin modules layered onto base.nix per node. vm-base is the bare
  # base node; every other node layers exactly one plugin module.
  pluginModules = {
    vm-base = [ ];
    vm-nix = [ ./plugins/nix.nix ];
    vm-slurm = [ ./plugins/slurm.nix ];
    vm-ansible = [ ./plugins/ansible.nix ];
    vm-ray = [ ./plugins/ray.nix ];
    vm-skypilot = [ ./plugins/skypilot.nix ];
  };

  # Per-node identity: hostname + fleet node number (drives IP/MAC in
  # base.nix via the consortium.test.nodeNumber option).
  perNode = name: {
    networking.hostName = name;
    consortium.test.nodeNumber = nodeNumbers.${name};
  };

  # Full module list for a node: microvm.nix guest module + base + identity
  # + plugins. Also what colmena imports for the same node.
  nodeModuleList =
    name: extraModules:
    [
      inputs.microvm-nix.nixosModules.microvm
      ./base.nix
      (perNode name)
    ]
    ++ extraModules;

  mkVm =
    name: extraModules:
    lib.nixosSystem {
      system = "x86_64-linux";
      specialArgs = { inherit inputs; };
      modules = nodeModuleList name extraModules;
    };
in
{
  inherit mkVm nodeNumbers;

  nodeModules = lib.mapAttrs nodeModuleList pluginModules;
  configs = lib.mapAttrs mkVm pluginModules;
}
