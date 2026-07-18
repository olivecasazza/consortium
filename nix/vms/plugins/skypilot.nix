# Plugin: skypilot — SkyPilot CLI (minimal variant).
#
# Installs `pkgs.skypilot`, the SkyPilot CLI application (`sky` on PATH) with
# its bundled python3 environment. This nixpkgs build is ALREADY the minimal
# variant: upstream's pip extras for cloud providers (aws/boto3,
# gcp/google-api-python-client, azure, kubernetes, ...) are not packaged
# here, so no cloud SDKs bloat the closure. What remains is the framework
# core: CLI, optimizer, provisioner abstraction (pandas, networkx, pulp,
# rich, cryptography, ...).
#
# Consequence: on this node `sky` can parse task YAMLs, run `sky check`
# against locally configured credentials and drive the core flows the
# consortium skypilot integration exercises, but it cannot talk to real
# clouds — which is correct for an offline microVM test fleet.
#
# Size tier: medium — measured ~639 MiB nar (python3 + declared propagated
# dependencies + source tree as proxy for the uncached package itself, from
# cache.nixos.org metadata at module verification time). Well under the
# ~1.5 GiB budget. Note: skypilot is NOT substitutable from cache.nixos.org
# (not hydra-built), so first build of a skypilot node compiles this one
# pure-python package from source (~minutes); all its deps are cached.
{ pkgs, ... }:
let
  # nixpkgs bug workaround: skypilot 0.8.1's wheel metadata pins
  # `wheel<0.46.0` but this nixpkgs ships wheel 0.46.1, so
  # pythonRuntimeDepsCheck fails and the whole node closure won't build.
  # Relax just that one pin; runtime behavior is unaffected.
  skypilot-fixed = pkgs.skypilot.overridePythonAttrs (old: {
    pythonRelaxDeps = (old.pythonRelaxDeps or [ ]) ++ [ "wheel" ];
  });
in
{
  environment.systemPackages = [ skypilot-fixed ];
}
