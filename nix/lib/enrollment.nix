# Integration contract-suite enrollment gate.
#
# Pure manifest gate, evaluated entirely in the flake: builtins.fromTOML on
# the workspace and member manifests — no shell text parsing, no
# text-based source assertions. Returns the enrolled adapter list and
# THROWS AT EVAL TIME when enrollment is violated, so `nix flake check`
# and the `integration-enrollment` check fail hard (never pass by
# accident):
#
#   * adapter        = a `crates/` workspace member with a NORMAL
#                      dependency on `consortium-integration`, other than
#                      infrastructure consumers: CLI aggregation, the
#                      contract testkit, and the fanout simulator;
#   * every adapter  MUST dev-depend on `consortium-integration-testkit`
#                      AND ship `tests/contract.rs` (the generated contract
#                      suite — the suites execute via the cargo-test check);
#   * any crate      dev-depending on the harness MUST ship
#                      `tests/contract.rs`;
#   * at least one   adapter must exist, so an emptied workspace cannot
#                      trivially pass.
#
# A new integration added without enrolling it fails evaluation with a
# precise message.
{ lib }:
workspaceRoot:
let
  workspace = (builtins.fromTOML (builtins.readFile (workspaceRoot + "/Cargo.toml"))).workspace;

  manifestOf =
    member:
    builtins.fromTOML (builtins.readFile (workspaceRoot + "/${member}/Cargo.toml"));

  hasNormalDep =
    manifest: crate:
    builtins.hasAttr crate (manifest.dependencies or { });

  hasDevDep =
    manifest: crate:
    builtins.hasAttr crate (manifest.dev-dependencies or { });

  hasContractSuite = member: builtins.pathExists (workspaceRoot + "/${member}/tests/contract.rs");

  # Infrastructure consumers of consortium-integration that are not
  # integration adapters.
  infraCrates = [
    "consortium-cli"
    "consortium-integration-testkit"
    "consortium-fanout-sim"
  ];

  crateMembers = lib.filter (lib.strings.hasPrefix "crates/") workspace.members;

  adapters = lib.filter (
    member:
    let
      m = manifestOf member;
    in
    hasNormalDep m "consortium-integration" && !lib.elem m.package.name infraCrates
  ) crateMembers;

  harnessDevs = lib.filter (member: hasDevDep (manifestOf member) "consortium-integration-testkit") crateMembers;

  assertions =
    [
      (lib.assertMsg (adapters != [ ])
        "integration-enrollment: no integration adapters found — expected at least one crates/ member with a normal dependency on consortium-integration")
    ]
    ++ map (
      member:
      let
        m = manifestOf member;
      in
      lib.assertMsg
        ((hasDevDep m "consortium-integration-testkit") && hasContractSuite member)
        "integration-enrollment: adapter `${m.package.name}` (${member}) depends on consortium-integration but is not enrolled in the contract suite; add a dev-dependency on consortium-integration-testkit and tests/contract.rs (see crates/consortium-slurm/tests/contract.rs), or add it to infraCrates in nix/lib/enrollment.nix if it is infrastructure"
    ) adapters
    ++ map (
      member:
      let
        m = manifestOf member;
      in
      lib.assertMsg (hasContractSuite member)
        "integration-enrollment: `${m.package.name}` (${member}) dev-depends on consortium-integration-testkit but has no tests/contract.rs contract suite"
    ) harnessDevs;
in
assert lib.all lib.id assertions;
{
  inherit adapters harnessDevs;
}
