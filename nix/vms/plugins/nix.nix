# Plugin: nix — Nix daemon + CLI inside the test microVM.
#
# Provides a working `nix` command (flakes enabled) so the consortium tool's
# nix integration can be exercised against this node. The build sandbox is
# disabled: these nodes share the host nix store (virtiofs, wired up by
# ../base.nix) and run nested/unprivileged workloads where the sandbox cannot
# function. This is a throwaway test node, not a build farm.
#
# Size tier: small — the nix package is already part of any stock NixOS
# system closure (standalone runtime closure ~156 MiB nar, measured from
# cache.nixos.org metadata); this module only changes daemon configuration.
{ ... }:
{
  nix = {
    enable = true;
    settings = {
      experimental-features = [
        "nix-command"
        "flakes"
      ];
      # Sandboxed builds cannot work inside this shared-store test microVM.
      sandbox = false;
    };
  };
}
