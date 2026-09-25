# Fleet contract fixture — the mkFleet argument set consumed by
# crates/consortium-integration/tests/nix_fleet_contract.rs via
# checks.fleet-contract (env CONSORTIUM_FLEET_CONFIG).
#
# Deliberately inexpensive: one stub NixOS configuration exposing exactly
# what mkFleet reads (platform system + a toplevel drvPath that is only
# read, never built, and is stripped from the emitted JSON), a non-default
# flakeUri, one host override, tags, and all four optional integration
# configs — so the fixture exercises the real JSON producer seam end to
# end.
{
  nixosConfigurations = {
    "contract-01" = {
      config = {
        nixpkgs.hostPlatform.system = "x86_64-linux";
        system.build.toplevel.drvPath = "/nix/store/gate.drv";
      };
    };
  };
  flakeUri = "github:example/fleet-contract";
  hostOverrides."contract-01".targetHost = "10.99.0.42";
  getHostTags = _: [ "contract" ];
  ansibleConfig.controlNode = "contract-ctl.example.internal";
  slurmConfig.submitNode = "contract-submit.example.internal";
  slurmConfig.submitUser = "root";
  slurmConfig.controlNode = "contract-ctl.example.internal";
  rayConfig.headAddress = "contract-head.example.internal:8787";
  skypilotConfig.cloud = "aws";
}
