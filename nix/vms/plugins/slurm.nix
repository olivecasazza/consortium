# Plugin: slurm — single-node Slurm cluster (slurmctld + slurmd + munge).
#
# Runs controller, compute daemon and munge auth all on this one node so the
# consortium tool can drive sinfo/srun/sbatch against a real scheduler
# without an external cluster. The node's own hostname (set per-node by
# ../base.nix) is both the control machine and the only compute node, sized
# to the base node's 1 vCPU.
#
# Size tier: small — measured ~228 MiB nar for slurm + munge runtime
# closures (from cache.nixos.org metadata at module verification time).
#
# TEST-ONLY: the munge key below is a fixed, world-readable, trivially weak
# key baked into the image for deterministic tests (same pattern as the
# nixpkgs Slurm NixOS test). NEVER reuse outside this ephemeral test fleet.
{ config, ... }:
let
  node = config.networking.hostName;
in
{
  services.slurm = {
    server.enable = true; # slurmctld
    client.enable = true; # slurmd
    clusterName = "consortium-test";
    controlMachine = node;
    nodeName = [ "${node} CPUs=1 State=UNKNOWN" ];
    partitionName = [ "debug Nodes=${node} Default=YES MaxTime=INFINITE State=UP" ];
  };

  # services.slurm enables services.munge automatically, but the stock munge
  # module does not create the key file munged needs — provide the fixed
  # test-only key (see header).
  systemd.tmpfiles.rules = [
    "f /etc/munge/munge.key 0400 munge munge - mungeverryweakkeybuteasytointegratoinatest"
  ];

  # Slurm resolves the control/compute node by hostname; the test node has no
  # DNS, so map its hostname to loopback.
  networking.hosts."127.0.0.1" = [ node ];

  # Allow slurmctld (6818) and slurmd (6817) traffic from the host/other test
  # nodes on the microvm tap network. Merged with whatever ../base.nix sets;
  # harmless if the base disables the firewall entirely.
  networking.firewall.allowedTCPPorts = [
    6817
    6818
  ];
}
