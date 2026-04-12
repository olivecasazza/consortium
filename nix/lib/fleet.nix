# mkFleet — Build deployment metadata from nixosConfigurations.
#
# This is the core Nix library interface for consortium, replacing
# colmena's "hive" concept. It takes NixOS/Darwin configurations and
# produces a fleet config that the consortium-nix CLI consumes as JSON.
#
# Usage in a flake:
#   fleet = consortium.lib.mkFleet {
#     inherit nixosConfigurations;
#     builders = { ... };
#     getHostTags = hostname: [ ... ];
#   };
{ lib, writeText }:

{
  mkFleet =
    {
      # NixOS system configurations (from flake outputs or Snowfall Lib).
      nixosConfigurations ? { },
      # nix-darwin system configurations.
      darwinConfigurations ? { },
      # Remote builder definitions.
      builders ? { },
      # Default system architecture.
      defaultSystem ? "x86_64-linux",
      # Function to compute tags from hostname.
      getHostTags ? (_: [ ]),
      # Per-host overrides for deployment settings.
      # { hostname = { targetHost = "1.2.3.4"; targetUser = "admin"; ... }; }
      hostOverrides ? { },
      # Flake URI for builds (e.g. "." or "github:user/repo").
      flakeUri ? ".",
    }:
    let
      # Merge NixOS and Darwin configurations
      allConfigs = nixosConfigurations // darwinConfigurations;

      # Build node metadata for each configuration
      mkNode = name: config:
        let
          system =
            config.config.nixpkgs.hostPlatform.system or
              (config.config.nixpkgs.localSystem.system or defaultSystem);
          isDarwin = lib.strings.hasInfix "darwin" system;
          overrides = hostOverrides.${name} or { };
        in
        {
          inherit name system;
          profileType = if isDarwin then "nix-darwin" else "nixos";
          targetHost = overrides.targetHost or name;
          targetUser = overrides.targetUser or (if isDarwin then "admin" else "root");
          targetPort = overrides.targetPort or null;
          buildOnTarget = overrides.buildOnTarget or false;
          tags = getHostTags name;
          drvPath = config.config.system.build.toplevel.drvPath;
        };

      nodes = lib.mapAttrs mkNode allConfigs;

      # Normalize builder definitions to the expected schema
      mkBuilder = name: b: {
        host = b.host or name;
        user = b.user or "root";
        maxJobs = b.maxJobs or 1;
        speedFactor = b.speedFactor or 1;
        systems = b.systems or [ defaultSystem ];
        features = b.features or [ ];
        sshKey = b.sshKey or null;
        protocol = b.protocol or "ssh-ng";
      };

      normalizedBuilders = lib.mapAttrs mkBuilder builders;

      # Strip derivation references from nodes for JSON serialization.
      # drvPath is a Nix store derivation that can't be embedded in toJSON.
      jsonNodes = lib.mapAttrs (
        _: node: builtins.removeAttrs node [ "drvPath" ]
      ) nodes;

      # JSON config for the consortium-nix CLI
      configData = {
        nodes = jsonNodes;
        inherit flakeUri;
        builders = normalizedBuilders;
      };

      configJson = builtins.toJSON configData;
    in
    {
      inherit nodes;
      inherit normalizedBuilders;

      # The JSON config file as a derivation
      configFile = writeText "consortium-fleet.json" configJson;

      # Raw JSON string (for embedding in other outputs)
      inherit configJson;

      # Helper: get node names as a list
      nodeNames = lib.attrNames nodes;

      # Helper: get nodes matching tags
      nodesByTags =
        tags:
        lib.filterAttrs (
          _: node: lib.any (t: lib.elem t tags) node.tags
        ) nodes;

      # Helper: get all unique tags
      allTags = lib.unique (lib.concatMap (n: n.tags) (lib.attrValues nodes));

      # Helper: get the toplevel derivation for a host
      toplevel = name: allConfigs.${name}.config.system.build.toplevel;

      # Helper: build all toplevels as a linkFarm (for CI)
      allToplevels = lib.mapAttrs (_: config: config.config.system.build.toplevel) allConfigs;
    };
}
