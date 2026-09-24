{
  description = "consortium — Rust rewrite of ClusterShell with Python bindings";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    git-hooks-nix.url = "github:cachix/git-hooks.nix";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    # microVM test fleet (nix/vms/): declarative NixOS microVMs for testing
    # the nix/slurm/ansible/ray/skypilot integrations.
    microvm-nix = {
      url = "github:astro/microvm.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      nixpkgs,
      flake-parts,
      crane,
      rust-overlay,
      git-hooks-nix,
      ...
    }:
    let
      # Declarative microVM test fleet (all nodes x86_64-linux).
      # See nix/vms/ and doc/testing-microvms.md.
      vms = import ./nix/vms { inherit inputs; };
    in
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [ git-hooks-nix.flakeModule ];

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];

      # Non-per-system outputs
      flake = {
        # Nix library for fleet configuration (system-independent)
        lib = import ./nix/lib {
          inherit (nixpkgs) lib;
          writeText =
            name: text:
            builtins.toFile name text;
        };

        # ── microVM test fleet (nix/vms/) ──────────────────────────────
        # nixosConfigurations.vm-{base,nix,slurm,ansible,ray,skypilot}
        nixosConfigurations = vms.configs;

        # colmena-compatible hive for deploying the same fleet to a KVM
        # host (guests are pushed over ssh to their 10.99.0.x tap IPs):
        #   colmena apply --on vm-base
        # Deployment builds happen wherever colmena runs; use a linux box.
        colmena = {
          meta = {
            nixpkgs = import nixpkgs { system = "x86_64-linux"; };
          };
        }
        // builtins.mapAttrs (
          name: modules:
          { ... }:
          {
            deployment = {
              targetHost = "10.99.0.${toString vms.nodeNumbers.${name}}";
              targetUser = "root";
              tags = [ "consortium-test" ];
            };
            imports = modules;
          }
        ) vms.nodeModules;
      };

      perSystem =
        {
          config,
          system,
          pkgs,
          lib,
          ...
        }:
        let
          # ── Rust toolchain ─────────────────────────────────────────────
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs { inherit system overlays; };

          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
              "clippy"
            ];
          };

          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

          # ── Source filtering ───────────────────────────────────────────
          src = lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              (craneLib.filterCargoSources path type)
              || (builtins.match ".*\\.py$" path != null)
              # The grammar gate embeds this reviewed JSON baseline at compile time.
              || (lib.hasSuffix "/crates/consortium-cli/src/grammar/baseline.json" path);
          };

          # ── Common Cargo args ──────────────────────────────────────────
          commonArgs = {
            inherit src;
            strictDeps = true;
            # PyO3 needs a Python interpreter at build time
            nativeBuildInputs = [ python ];
            # Tell pyo3-build-config where Python is
            PYO3_PYTHON = "${python}/bin/python3";
          };

          # ── Build artifacts (deps only, for caching) ───────────────────
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          # ── The Rust library crate ─────────────────────────────────────
          consortium = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "-p consortium-crate";
            }
          );

          # ── CLI binaries (claw, molt, pinch, cast) ─────────────────────────
          consortium-cli = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "-p consortium-cli";
            }
          );

          # ── NixOS deployment library ────────────────────────────────
          consortium-nix = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "-p consortium-nix";
            }
          );

          # ── Nix library for fleet configuration ────────────────────
          consortiumLib = import ./nix/lib {
            inherit lib;
            inherit (pkgs) writeText;
          };

          # ── Python environment ─────────────────────────────────────────
          python = pkgs.python312;
          pythonEnv = python.withPackages (
            ps: with ps; [
              pytest
              pytest-timeout
              pytest-xdist
              pyyaml
            ]
          );

        in
        {
          # ── Pre-commit hooks ─────────────────────────────────────────
          pre-commit.settings.hooks = {
            rustfmt = {
              enable = true;
              packageOverrides.cargo = rustToolchain;
              packageOverrides.rustfmt = rustToolchain;
            };
            # clippy runs via craneLib.cargoClippy in checks (with vendored deps);
            # the pre-commit hook can't vendor deps in --offline sandbox mode.
          };

          # ── Checks ─────────────────────────────────────────────────────
          checks = {
            # Rust unit tests
            cargo-test = craneLib.cargoTest (
              commonArgs
              // {
                inherit cargoArtifacts;
              }
            );

            # Clippy lints
            cargo-clippy = craneLib.cargoClippy (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoClippyExtraArgs = "--all-targets -- -D warnings";
              }
            );

            # Format check
            cargo-fmt = craneLib.cargoFmt {
              inherit src;
            };

            # Build the library
            inherit consortium;
          };

          # ── Packages ───────────────────────────────────────────────────
          packages =
            {
              inherit consortium consortium-cli consortium-nix;
              default = consortium-cli;
            }
            # microVM test-fleet qemu runners (x86_64-linux only; they can
            # only RUN on a linux KVM host, but build from anywhere):
            #   nix build .#packages.x86_64-linux.vm-base
            #   nix run   .#packages.x86_64-linux.vm-base   # on the KVM host
            // lib.optionalAttrs (system == "x86_64-linux") (
              builtins.mapAttrs (_: cfg: cfg.config.microvm.runner.qemu) vms.configs
            );

          # ── Dev shell ──────────────────────────────────────────────────
          devShells.default = pkgs.mkShell {
            inputsFrom = [ consortium ];

            nativeBuildInputs = [
              rustToolchain
              pythonEnv

              # Dev tools
              pkgs.cargo-watch
              pkgs.cargo-nextest
              pkgs.maturin

              # Nix tools
              pkgs.statix
              pkgs.deadnix
            ];

            shellHook = ''
              export RUST_BACKTRACE=1

              # Create a venv if one doesn't exist (maturin develop needs it)
              if [ ! -d .venv ]; then
                echo "  Creating .venv for maturin..."
                python3 -m venv .venv --system-site-packages
              fi
              source .venv/bin/activate

              # Python oracle/parity suite lives in the sibling consortium-tests repo
              if [ -d ../consortium-tests/lib ]; then
                export PYTHONPATH="$PWD/../consortium-tests/lib:$PYTHONPATH"
              fi
              ${config.pre-commit.installationScript}

              echo ""
              echo "  consortium dev shell"
              echo "  ─────────────────────────────────────────"
              echo "  cargo test                    — Rust tests"
              echo "  cargo watch -x test           — Rust TDD"
              echo "  maturin develop               — build Rust→Python bindings"
              echo "  (cd ../consortium-tests && CONSORTIUM_BACKEND=python pytest tests/ -v)"
              echo "                                — Python parity tests (original ClusterShell)"
              echo "  (cd ../consortium-tests && CONSORTIUM_BACKEND=rust pytest tests/ -v)"
              echo "                                — Python parity tests (Rust-backed)"
              echo ""
            '';
          };
        };
    };
}
