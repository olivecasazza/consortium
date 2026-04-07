# Rust builds via Crane + rust-overlay
{ inputs, ... }:
{
  perSystem =
    { system, pkgs, ... }:
    let
      # Apply rust-overlay to get a pinned toolchain
      pkgsWithOverlay = import inputs.nixpkgs {
        inherit system;
        overlays = [ (import inputs.rust-overlay) ];
      };

      # Use the stable toolchain from rust-toolchain.toml if present,
      # otherwise default to stable with the components we need
      rustToolchain = pkgsWithOverlay.rust-bin.stable.latest.default.override {
        extensions = [
          "rust-src"
          "rust-analyzer"
          "clippy"
        ];
      };

      craneLib = (inputs.crane.mkLib pkgs).overrideToolchain rustToolchain;

      # Common args shared by deps-only and full builds
      commonArgs = {
        src = craneLib.cleanCargoSource (craneLib.path ../.);
        strictDeps = true;

        nativeBuildInputs = [ pkgs.pkg-config ];
        buildInputs =
          [ ]
          ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [
            pkgs.darwin.apple_sdk.frameworks.Security
            pkgs.libiconv
          ];
      };

      # Build only workspace deps (cache layer)
      cargoArtifacts = craneLib.buildDepsOnly commonArgs;

      # Build the full workspace
      consortium = craneLib.buildPackage (
        commonArgs
        // {
          inherit cargoArtifacts;
          # Skip the cdylib crate in the main build — it needs Python
          cargoExtraArgs = "--workspace --exclude consortium-py";
        }
      );

      # Run clippy on the workspace
      consortiumClippy = craneLib.cargoClippy (
        commonArgs
        // {
          inherit cargoArtifacts;
          cargoClippyExtraArgs = "--workspace --exclude consortium-py -- --deny warnings";
        }
      );

      # Run cargo test on the core crate
      consortiumTests = craneLib.cargoTest (
        commonArgs
        // {
          inherit cargoArtifacts;
          cargoTestExtraArgs = "--workspace --exclude consortium-py";
        }
      );

      # cargo fmt check
      consortiumFmt = craneLib.cargoFmt {
        src = craneLib.cleanCargoSource (craneLib.path ../.);
      };
    in
    {
      # Expose the toolchain for devShells
      _module.args.rustToolchain = rustToolchain;
      _module.args.craneLib = craneLib;
      _module.args.cargoArtifacts = cargoArtifacts;
      _module.args.commonCraneArgs = commonArgs;

      packages = {
        default = consortium;
        inherit consortium;
      };

      checks = {
        inherit consortium consortiumClippy consortiumTests consortiumFmt;
      };
    };
}
