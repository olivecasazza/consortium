# Plugin: ansible — ansible-core CLI + python3 on PATH.
#
# Gives the node `ansible`, `ansible-playbook`, `ansible-galaxy`, ... so the
# consortium tool's ansible integration can target or run from this node.
#
# Size discipline: in the pinned nixpkgs revision the stock ansible-core
# expression inverts the upstream dependency and pulls the ~550 MiB community
# `ansible` collections bundle into its closure (see
# pkgs/development/python-modules/ansible/core.nix: "depend on ansible
# instead of the other way around"). The fleet contract asks for ansible-
# CORE, so we strip that one dependency back out; only ansible.builtin.*
# modules ship. Tests needing community collections must install them at
# test time (ansible-galaxy collection install ...).
#
# Size tier: small — measured ~173 MiB nar for python3 + stripped
# ansible-core runtime closures (cache.nixos.org metadata), plus the core
# package itself (~23 MiB installed). The stripped variant is a local rebuild
# (not substitutable), but ansible-core is pure python, so it is quick.
{ pkgs, ... }:
let
  ansibleCore = pkgs.python3Packages.ansible-core.overridePythonAttrs (old: {
    dependencies = builtins.filter (p: (p.pname or "") != "ansible") old.dependencies;
  });
in
{
  environment.systemPackages = [
    pkgs.python3 # interpreter ansible modules/playbooks run under
    ansibleCore # bin/ansible, bin/ansible-playbook, ... (core only)
  ];
}
