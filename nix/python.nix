# Python bindings build (maturin + PyO3) and test harness
{ inputs, ... }:
{
  perSystem =
    {
      system,
      pkgs,
      craneLib,
      cargoArtifacts,
      commonCraneArgs,
      ...
    }:
    let
      python = pkgs.python3;

      # Build the Python extension module with maturin
      consortiumPy = python.pkgs.buildPythonPackage {
        pname = "consortium-py";
        version = "0.1.0";
        format = "pyproject";

        src = ../.;

        nativeBuildInputs = with python.pkgs; [
          maturin
          pip
        ] ++ commonCraneArgs.nativeBuildInputs;

        buildInputs = commonCraneArgs.buildInputs;

        # maturin needs the Rust toolchain
        cargoDeps = pkgs.rustPlatform.importCargoLock {
          lockFile = ../Cargo.lock;
        };

        buildPhase = ''
          # Build only the PyO3 crate
          maturin build \
            --release \
            --manifest-path crates/consortium-py/Cargo.toml \
            --out dist
        '';

        installPhase = ''
          pip install dist/*.whl --prefix=$out --no-deps
        '';
      };

      # Python environment with the Rust extension + test deps
      pythonTestEnv = python.withPackages (ps: [
        consortiumPy
        ps.pytest
      ]);

      # Run the original ClusterShell Python tests against Rust bindings
      pythonTests = pkgs.runCommand "consortium-python-tests"
        {
          nativeBuildInputs = [ pythonTestEnv ];
          src = ../.;
        }
        ''
          cd $src
          # Run tests — the import shim in consortium-py makes
          # "from ClusterShell.RangeSet import RangeSet" resolve to Rust
          python -m pytest tests/ -v --tb=short 2>&1 | tee $out
        '';
    in
    {
      packages.consortium-py = consortiumPy;

      checks = {
        inherit pythonTests;
      };

      devShells.python = pkgs.mkShell {
        packages = [
          pythonTestEnv
          python.pkgs.maturin
        ];
      };
    };
}
