# consortium Nix library — deployment configuration for NixOS/nix-darwin.
#
# Import this in your flake to get mkFleet and related helpers.
#
# Example:
#   consortium.lib = import "${consortium}/nix/lib" { inherit (nixpkgs) lib; inherit (pkgs) writeText; };
#   fleet = consortium.lib.mkFleet { inherit nixosConfigurations; };
{ lib, writeText }:

let
  fleetLib = import ./fleet.nix { inherit lib writeText; };
in
{
  inherit (fleetLib) mkFleet;
}
