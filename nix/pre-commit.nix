# Pre-commit hooks via git-hooks.nix
_:
{
  perSystem =
    {
      pkgs,
      config,
      rustToolchain,
      ...
    }:
    {
      pre-commit.settings.hooks = {
        rustfmt = {
          enable = true;
          entry = "${rustToolchain}/bin/cargo fmt -- --check";
        };
        clippy = {
          enable = true;
          entry = "${rustToolchain}/bin/cargo clippy --workspace --exclude consortium-py -- -D warnings";
        };
        nixfmt-rfc-style.enable = true;
      };

      devShells.default = pkgs.mkShell {
        nativeBuildInputs = [
          rustToolchain
          pkgs.pkg-config
          pkgs.python3
          pkgs.python3Packages.maturin
          pkgs.python3Packages.pytest
          pkgs.cargo-watch
          pkgs.rust-analyzer
          pkgs.nixfmt-rfc-style
        ] ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [
          pkgs.darwin.apple_sdk.frameworks.Security
          pkgs.libiconv
        ];

        shellHook = ''
          export EDITOR=nvim
          ${config.pre-commit.installationScript}
          echo "consortium dev shell ready"
          echo "  cargo test          — run Rust tests"
          echo "  cargo watch -x test — TDD loop"
          echo "  pytest tests/       — run Python acceptance tests"
        '';
      };
    };
}
