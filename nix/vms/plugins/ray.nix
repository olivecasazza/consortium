# Plugin: ray — minimal ray-head-only variant.
#
# Installs the ray core runtime (the PyPI wheel already bundles the compiled
# head-node servers: raylet, GCS, plasma store) PLUS ray's "default" extra,
# which is what `ray start --head`, the dashboard, the `ray` client protocol
# (grpcio) and `ray status`/`ray job submit` actually import at runtime.
#
# Deliberately EXCLUDED to keep the closure well under the ~1.5 GiB fleet
# budget: ray's data/serve/tune/train/rllib/llm extras (pyarrow, pandas,
# fastapi, scipy, cupy, vllm, ... — vllm alone is multi-GiB). Tests that need
# those must build a fatter node; this variant covers head-node control-plane
# workflows only.
#
# Size tier: medium — measured ~602 MiB nar for ray + the "default" extras
# (union of runtime closures from cache.nixos.org metadata at module
# verification time), well under the ~1.5 GiB budget.
{ pkgs, ... }:
{
  environment.systemPackages = [
    (pkgs.python3.withPackages (
      ps:
      # ps.ray plus its "default" optional dependencies = head-node essentials.
      [ ps.ray ]
      ++ ps.ray.optional-dependencies.default
    ))
  ];
}
