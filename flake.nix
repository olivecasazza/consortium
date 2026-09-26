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

          # ── cast-on: push deploys for NixOS / nix-darwin fleets ────────
          # Shell tool around `pinch` + `nix build/copy`; see nix/cast-on.nix.
          cast-on = pkgs.callPackage ./nix/cast-on.nix { inherit consortium-cli; };

          # ── Fleet contract fixture ─────────────────────────────────────
          # Real mkFleet JSON over an inexpensive stub node; consumed by
          # crates/consortium-integration/tests/nix_fleet_contract.rs.
          # See nix/lib/fleet-contract-fixture.nix for the exact values.
          fleetFixture = consortiumLib.mkFleet (import ./nix/lib/fleet-contract-fixture.nix);

          # ── Published library crates (SemVer gate) ─────────────────────
          # Every crates/ workspace member except consortium-py: it is
          # published, but as a cdylib Python extension with no meaningful
          # Rust public API for cargo-semver-checks to diff. The app checks
          # the premerge Git baseline and the crates.io release baseline at
          # runtime, never in a Nix sandbox.
          publishedLibs =
            let
              workspace = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace;
              pkgName = member:
                (builtins.fromTOML (builtins.readFile (./. + "/${member}/Cargo.toml"))).package.name;
            in
            map pkgName (
              lib.filter (member: pkgName member != "consortium-py")
                (lib.filter (lib.strings.hasPrefix "crates/") workspace.members)
            );

          # Git baseline enforces API compatibility even while 0.3.0 is an
          # unpublished breaking release; crates.io checks the version bump.
          # Both run outside the Nix sandbox. CI passes the exact PR base
          # (or pre-push tip), rather than relying on the checkout depth.
          semverCheck = pkgs.writeShellApplication {
            name = "semver-check";
            runtimeInputs = [
              pkgs.cargo-semver-checks
              rustToolchain
            ];
            text = ''
              if [ "$#" -ne 1 ]; then
                echo "usage: nix run .#semver-check -- <base-commit>" >&2
                exit 2
              fi
              if [ ! -f Cargo.toml ]; then
                echo "semver-check: run from the consortium workspace root" >&2
                exit 1
              fi
              baseline_rev="$1"
              status=0
              for pkg in ${lib.escapeShellArgs publishedLibs}; do
                echo "=== cargo semver-checks -p $pkg (git baseline: $baseline_rev; API release type: minor)"
                if ! cargo semver-checks -p "$pkg" --baseline-rev "$baseline_rev" --release-type minor; then
                  echo "semver-check: $pkg FAILED API compatibility against $baseline_rev" >&2
                  status=1
                fi
                echo "=== cargo semver-checks -p $pkg (baseline: crates.io)"
                if ! cargo semver-checks -p "$pkg"; then
                  echo "semver-check: $pkg FAILED crates.io version check" >&2
                  status=1
                fi
              done
              exit "$status"
            '';
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

            # Fleet JSON contract: real mkFleet fixture against the real
            # FleetConfig parser. The integration test is `#[ignore]`d in
            # the suite because it needs a store fixture path, so plain
            # `cargo test` skips it — this check runs it explicitly with
            # CONSORTIUM_FLEET_CONFIG pointing at the generated JSON.
            fleet-contract = craneLib.cargoTest (commonArgs // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "-p consortium-integration --test nix_fleet_contract -- --ignored";
              CONSORTIUM_FLEET_CONFIG = fleetFixture.configFile;
            });

            # Integration adapters must be enrolled in the generated
            # contract suite. Pure manifest gate (nix/lib/enrollment.nix);
            # violations fail at EVAL time, never pass by accident. The
            # enrolled suites themselves execute via cargo-test above.
            integration-enrollment =
              let
                enrollment = import ./nix/lib/enrollment.nix { inherit lib; } ./.;
              in
              pkgs.runCommand "consortium-integration-enrollment"
                {
                  adapters = lib.concatStringsSep " " enrollment.adapters;
                }
                ''
                  echo "enrolled contract-suite adapters: $adapters" > $out
                '';

            # Python bindings gate: builds the real PyO3 extension and runs
            # crates/consortium-py/tests/nix_smoke.py against it plus the
            # in-repo ClusterShell shims. Hermetic: no oracle repo, no
            # network, no maturin/venv. The cdylib is installed as
            # ClusterShell/_consortium.so (CPython's loader requires the
            # .so suffix, also on darwin).
            python-api =
              let
                pySrc = ./crates/consortium-py;
                extension = craneLib.buildPackage (commonArgs // {
                  inherit cargoArtifacts;
                  cargoExtraArgs = "-p consortium-py";
                  # cdylib only; nothing to test inside this derivation.
                  doCheck = false;
                });
              in
              pkgs.runCommand "consortium-python-api"
                {
                  nativeBuildInputs = [ pythonEnv ];
                }
                ''
                  export PYTHONDONTWRITEBYTECODE=1

                  mkdir -p $out
                  cp -r ${pySrc}/ClusterShell $out/ClusterShell
                  chmod -R u+w $out/ClusterShell

                  cp ${extension}/lib/lib_consortium.* $out/ClusterShell/_consortium.so

                  PYTHONPATH="$out" python ${pySrc}/tests/nix_smoke.py
                  touch $out
                '';

            # Build the library
            inherit consortium;
          };

          # ── Packages ───────────────────────────────────────────────────
          packages =
            {
              inherit
                consortium
                consortium-cli
                consortium-nix
                cast-on
                ;
              default = consortium-cli;

              # Skill catalog for downstream consumers. `$out` is a directory
              # of skill directories whose names equal their SKILL.md `name:`
              # frontmatter value (never the source directory name), each
              # holding SKILL.md plus its `agents assets examples references
              # scripts tests` resource dirs. Consumed by `local.skills.sources`
              # in nixos-config and by the olive-skills catalog, both of which
              # merge it with `cp -rL $skills/. $out/`.
              #
              # Assembly loop mirrors olive-skills' own `packages.default`, so
              # a consumer of either catalog sees an identical layout. The
              # `test -n` guard fails the build rather than silently shipping
              # a skill under the literal name "".
              skills = pkgs.runCommand "consortium-skills" { nativeBuildInputs = [ pkgs.findutils ]; } ''
                mkdir -p $out
                for skill_file in ${./skills}/*/SKILL.md; do
                  source_dir="$(dirname "$skill_file")"
                  skill_name="$(sed -n 's/^name:[[:space:]]*//p' "$skill_file" | head -n1)"
                  test -n "$skill_name"
                  destination="$out/$skill_name"
                  mkdir -p "$destination"
                  find "$source_dir" -maxdepth 1 -type f -exec cp {} "$destination/" \;
                  for resource in agents assets examples references scripts tests; do
                    if test -d "$source_dir/$resource"; then
                      cp -rL "$source_dir/$resource" "$destination/$resource"
                    fi
                  done
                done
              '';
            }
            # microVM test-fleet qemu runners (x86_64-linux only; they can
            # only RUN on a linux KVM host, but build from anywhere):
            #   nix build .#packages.x86_64-linux.vm-base
            #   nix run   .#packages.x86_64-linux.vm-base   # on the KVM host
            // lib.optionalAttrs (system == "x86_64-linux") (
              builtins.mapAttrs (_: cfg: cfg.config.microvm.runner.qemu) vms.configs
            );

          # ── Apps ───────────────────────────────────────────────────────
          apps = {
            # `nix run .#semver-check` from the workspace root: real
            # cargo-semver-checks against the crates.io baseline for every
            # published library crate (see publishedLibs above). Exits
            # nonzero on substantive SemVer breakage or baseline lookup
            # errors. Network use happens here at runtime, never inside a
            # Nix derivation.
            semver-check = {
              type = "app";
              program = lib.getExe semverCheck;
            };
          };

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
