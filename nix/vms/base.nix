# nix/vms/base.nix — base NixOS module for the consortium test fleet.
#
# Every test node is a microvm.nix qemu microVM (x86_64-linux):
#
#   * 1 vCPU / 512 MiB RAM — tiny on purpose; these are protocol test targets
#   * the host /nix/store is shared read-only via virtiofs, so the guest has
#     NO disk image and NO store of its own (fast build, fast boot)
#   * static test IP 10.99.0.<nodeNumber>/24 on a tap interface
#     (the KVM host owns 10.99.0.1/24 on the tap side and NATs)
#   * sshd with root login restricted to the TEST-ONLY key in ./keys/
#   * hostname assigned per node by mkVm in ./default.nix
#
# ⚠️  The ssh keypair in ./keys/ is committed to the repo ON PURPOSE and is
# TEST-ONLY. Never reuse it anywhere else. See ./keys/README.md.
{ config, lib, ... }:

let
  cfg = config.consortium.test;

  # nodeNumber (2–254) -> two-digit lowercase hex byte, e.g. 10 -> "0a"
  hexByte =
    n:
    let
      digits = "0123456789abcdef";
    in
    "${builtins.substring (builtins.div n 16) 1 digits}${builtins.substring (lib.mod n 16) 1 digits}";

  mac = "02:00:00:99:00:${hexByte cfg.nodeNumber}";
in
{
  options.consortium.test = {
    nodeNumber = lib.mkOption {
      type = lib.types.ints.between 2 254;
      default = 10;
      example = 11;
      description = ''
        Fleet node index, assigned per node in nix/vms/default.nix.
        Becomes the last octet of the static test IP
        (10.99.0.<nodeNumber>/24) and the last byte of the tap MAC.
      '';
    };
  };

  config = {
    # ── microvm.nix: qemu, tiny, nix store shared from the host ─────────
    microvm = {
      hypervisor = "qemu";
      vcpu = 1;
      mem = 512;

      # Read-only host-store share; microvm.nix overlays this onto the
      # guest /nix/store, so no disk image / own store is needed.
      shares = [
        {
          proto = "virtiofs";
          tag = "ro-store";
          source = "/nix/store";
          mountPoint = "/nix/.ro-store";
        }
      ];

      # One tap interface per node. The KVM host must own 10.99.0.1/24 on
      # the tap side (microvm.nix `nixosModules.host` + systemd-networkd,
      # or the equivalent by hand). See doc/testing-microvms.md.
      interfaces = [
        {
          type = "tap";
          id = "tap-${config.networking.hostName}";
          inherit mac;
        }
      ];
    };

    # ── static guest networking ─────────────────────────────────────────
    networking = {
      # hostName is set per node by mkVm in nix/vms/default.nix
      useDHCP = false;
      useNetworkd = true;
      # Throwaway test VMs on an isolated tap subnet — no firewall.
      firewall.enable = false;
    };

    systemd.network.networks."10-tap" = {
      matchConfig.MACAddress = mac;
      address = [ "10.99.0.${toString cfg.nodeNumber}/24" ];
      routes = [ { Gateway = "10.99.0.1"; } ];
      dns = [
        "1.1.1.1"
        "9.9.9.9"
      ];
    };

    # ── sshd: root login, TEST-ONLY baked key only ──────────────────────
    services.openssh = {
      enable = true;
      settings = {
        PermitRootLogin = "prohibit-password";
        PasswordAuthentication = false;
        KbdInteractiveAuthentication = false;
      };
    };
    users.users.root.openssh.authorizedKeys.keyFiles = [ ./keys/id_test.pub ];

    # ── keep the closure tiny: no docs, no default extras ───────────────
    documentation.enable = false;
    environment.defaultPackages = lib.mkForce [ ];
    # nix itself stays enabled on purpose: colmena pushes closures to the
    # guest over ssh (needs the nix daemon). The vm-nix plugin only tweaks
    # daemon settings on top of this.
    nix.channel.enable = false;

    system.stateVersion = "25.05";
  };
}
