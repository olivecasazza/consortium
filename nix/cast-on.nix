# nix/cast-on.nix — cast-on: push-based NixOS / nix-darwin deploys to one or
# more hosts in parallel, driven from a single workstation.
#
# The name is a knitting nod: "cast on" is how you start a knitting project —
# analogously, this is how you start a deploy.
#
#   nix run .#cast-on                       # deploy $CAST_ON_DEFAULT_TARGETS
#   nix run .#cast-on -- node01             # one host
#   nix run .#cast-on -- @darwin            # a group from the groups file
#   nix run .#cast-on -- --dry-run node01   # build + print closures only
#
# Pipeline (all builds happen on the driving workstation, never on targets):
#   1. optionally commit + push the flake checkout so targets see a real ref
#   2. classify each target: darwinConfigurations.<attr> or nixosConfigurations.<attr>
#   3. `nix build` every closure locally
#   4. `nix copy` every closure to its host concurrently
#   5. activate every host concurrently:
#        darwin -> `sudo <closure>/activate`
#        nixos  -> `sudo <closure>/bin/switch-to-configuration switch`
#
# `@group` targets are expanded from a ClusterShell groups.d file
# (`group: nodeset` lines) via `pinch -e`, so bracket ranges and comma lists
# work as well as plain space-separated host lists.
#
# Configuration (flag, then env, then default):
#   --flake REF / CAST_ON_FLAKE            flake to deploy; default: git toplevel of $PWD
#   CAST_ON_DEFAULT_TARGETS                targets when none given; default "@all-deploy"
#   CAST_ON_GROUPS_FILE                    default ${XDG_CONFIG_HOME:-~/.config}/clustershell/groups.d/cluster.cfg
#   CAST_ON_SSH_USER                       remote user; default $USER
#   CAST_ON_SSH_CONNECT_TIMEOUT            seconds; default 8
#   CAST_ON_TMPDIR                         scratch dir; default $TMPDIR/cast-on
#   CAST_ON_DARWIN_NIX_ARGS                extra words for darwin eval/build (e.g. --override-input ...)
#   CAST_ON_NIXOS_NIX_ARGS                 extra words for nixos eval/build
#   CAST_ON_USE_DISTRIBUTED_BUILDERS=1     keep the nix.conf builders (off by default: the
#                                          builders are usually the hosts being deployed)
{
  writeShellApplication,
  consortium-cli,
  coreutils,
  gawk,
  git,
  gnugrep,
  gnused,
  nix,
  openssh,
}:

writeShellApplication {
  name = "cast-on";
  runtimeInputs = [
    consortium-cli
    coreutils
    gawk
    git
    gnugrep
    gnused
    nix
    openssh
  ];

  text = ''
    DRY_RUN=0
    SKIP_GIT=0
    FLAKE="''${CAST_ON_FLAKE:-}"
    TARGETS=()

    while [[ $# -gt 0 ]]; do
      case "$1" in
        --dry-run|-n) DRY_RUN=1; shift ;;
        --skip-git)   SKIP_GIT=1; shift ;;
        --flake)      FLAKE="$2"; shift 2 ;;
        --flake=*)    FLAKE="''${1#--flake=}"; shift ;;
        --help|-h)
          cat <<'EOF'
    cast-on — parallel NixOS / nix-darwin deploys (build locally, copy, activate)

    Usage: cast-on [OPTIONS] [TARGET...]

    Options:
      -n, --dry-run     Build closures, but skip copy and activation
          --flake REF   Flake to deploy (default: $CAST_ON_FLAKE, else the git
                        toplevel of the current directory)
          --skip-git    Don't auto-commit/push uncommitted changes in a local flake
      -h, --help        Show this help

    Targets:
      Hostnames, or @group entries from the ClusterShell groups file
      ($CAST_ON_GROUPS_FILE, default ~/.config/clustershell/groups.d/cluster.cfg).
      With no targets, $CAST_ON_DEFAULT_TARGETS is used (default: @all-deploy).
      The attribute deployed to a host is its hostname up to the first dot, looked
      up in darwinConfigurations then nixosConfigurations.

    Examples:
      cast-on                        # every host in the default targets
      cast-on node01                 # just node01
      cast-on @darwin                # the darwin group
      cast-on @gpu --dry-run         # build for the gpu group, don't deploy
      cast-on --flake github:me/cfg node01

    Environment:
      CAST_ON_FLAKE, CAST_ON_DEFAULT_TARGETS, CAST_ON_GROUPS_FILE, CAST_ON_SSH_USER,
      CAST_ON_SSH_CONNECT_TIMEOUT, CAST_ON_TMPDIR, CAST_ON_DARWIN_NIX_ARGS,
      CAST_ON_NIXOS_NIX_ARGS, CAST_ON_USE_DISTRIBUTED_BUILDERS
    EOF
          exit 0
          ;;
        --) shift; TARGETS+=("$@"); break ;;
        -*)
          echo "cast-on: unknown option: $1" >&2
          exit 64
          ;;
        *) TARGETS+=("$1"); shift ;;
      esac
    done

    if [[ ''${#TARGETS[@]} -eq 0 ]]; then
      read -r -a TARGETS <<<"''${CAST_ON_DEFAULT_TARGETS:-@all-deploy}"
    fi

    # ── Flake to deploy ───────────────────────────────────────────────
    # A local directory is deployed as `.#<attr>` from inside it (so the git
    # step and relative overrides behave); anything else is a plain flake
    # ref passed straight to nix.
    if [[ -z $FLAKE ]]; then
      if git -C "$PWD" rev-parse --show-toplevel &>/dev/null; then
        FLAKE="$(git -C "$PWD" rev-parse --show-toplevel)"
      else
        echo "cast-on: not inside a git checkout; pass --flake <ref> or set CAST_ON_FLAKE" >&2
        exit 1
      fi
    fi
    if [[ -d $FLAKE ]]; then
      cd "$FLAKE"
      FLAKE_IS_LOCAL=1
      FLAKE_REF="."
    else
      FLAKE_IS_LOCAL=0
      FLAKE_REF="$FLAKE"
    fi

    LOCAL_HOSTNAME="$(hostname -s)"
    SSH_USER="''${CAST_ON_SSH_USER:-''${USER:-$(id -un)}}"
    GROUPS_FILE="''${CAST_ON_GROUPS_FILE:-''${XDG_CONFIG_HOME:-$HOME/.config}/clustershell/groups.d/cluster.cfg}"
    CAST_ON_TMPDIR="''${CAST_ON_TMPDIR:-''${TMPDIR:-/tmp}/cast-on}"
    mkdir -p "$CAST_ON_TMPDIR"

    # Deploy builds must not inherit the fleet's distributed builders by
    # default. Those builders are typically the same hosts being switched;
    # their network state is exactly what the deploy may be trying to
    # repair, and using them can deadlock or fail before copy/activation.
    # Set CAST_ON_USE_DISTRIBUTED_BUILDERS=1 to opt back in explicitly.
    NIX_LOCAL_ARGS=()
    if [[ ''${CAST_ON_USE_DISTRIBUTED_BUILDERS:-0} != 1 ]]; then
      NIX_LOCAL_ARGS+=(--option builders "")
    fi

    # Extra nix eval/build words per platform, e.g. `--override-input foo
    # path:./stubs/empty` to stub inputs a NixOS host must not fetch.
    DARWIN_NIX_ARGS=()
    NIXOS_NIX_ARGS=()
    [[ -z ''${CAST_ON_DARWIN_NIX_ARGS:-} ]] || read -r -a DARWIN_NIX_ARGS <<<"$CAST_ON_DARWIN_NIX_ARGS"
    [[ -z ''${CAST_ON_NIXOS_NIX_ARGS:-} ]]  || read -r -a NIXOS_NIX_ARGS  <<<"$CAST_ON_NIXOS_NIX_ARGS"

    # Expand `@group` targets to a flat host list. Groups come from a
    # ClusterShell groups.d file (`group: nodeset`); the value is run through
    # `pinch -e` so `node[1-4],gpu01` and plain space-separated lists both work.
    resolve_targets() {
      local out=""
      for pat in "$@"; do
        if [[ $pat == @* ]]; then
          local group="''${pat#@}"
          local hosts
          hosts=$(grep "^''${group}:" "$GROUPS_FILE" 2>/dev/null \
                  | head -1 \
                  | cut -d: -f2- || true)
          if [[ -z ''${hosts// /} ]]; then
            echo "cast-on: group '$group' not found in $GROUPS_FILE" >&2
            exit 1
          fi
          # shellcheck disable=SC2086
          out+=" $(pinch -e -S ' ' $hosts)"
        else
          out+=" $pat"
        fi
      done
      # shellcheck disable=SC2001
      echo "$out" | sed 's/^ *//;s/ *$//' | tr -s ' '
    }

    RESOLVED="$(resolve_targets "''${TARGETS[@]}")"
    read -r -a HOSTS <<<"$RESOLVED"

    echo "==> Targets: ''${HOSTS[*]}"

    # ── 1. Commit/push if dirty (so remotes can pull a real ref) ──────
    if [[ $SKIP_GIT -eq 0 && $FLAKE_IS_LOCAL -eq 1 ]] && git rev-parse --is-inside-work-tree &>/dev/null; then
      if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "==> Committing local changes..."
        git add -A
        git commit -m "chore: cast-on deploy $(date '+%Y-%m-%d %H:%M')"
      fi
      if git remote get-url origin &>/dev/null; then
        echo "==> Pushing..."
        git push
      fi
    fi

    # ── 2. Per-host classify (darwin vs nixos) ─────────────────────────
    # Strip the domain (.local, .example.com, ...) to get the bare hostname
    # used as the configuration attribute name.
    bare_attr() {
      local h="$1"
      h="''${h%%.*}"
      echo "$h"
    }

    resolve_host_ips() {
      local name="$1"
      if command -v dscacheutil >/dev/null 2>&1; then
        dscacheutil -q host -a name "$name" 2>/dev/null | awk '/ip_address/ { print $2 }'
      fi
      if command -v getent >/dev/null 2>&1; then
        getent hosts "$name" 2>/dev/null | awk '{ print $1 }'
      fi
    }

    # Resolve a target entry to the SSH endpoint that is live *right now*.
    # Laptop DHCP leases move and cached IPs go stale, so never bake
    # addresses into deploy logic. Try the entry as given, <attr>.local,
    # <attr>, then the IPs those names resolve to, and pick the first
    # endpoint accepting our SSH key.
    resolve_ssh_target() {
      local original="$1"
      local attr="$2"
      local candidates=()
      local c ip

      candidates+=("$original" "$attr.local" "$attr")
      while IFS= read -r ip; do candidates+=("$ip"); done < <(resolve_host_ips "$original")
      while IFS= read -r ip; do candidates+=("$ip"); done < <(resolve_host_ips "$attr.local")
      while IFS= read -r ip; do candidates+=("$ip"); done < <(resolve_host_ips "$attr")

      local seen=" "
      for c in "''${candidates[@]}"; do
        [[ -n "$c" ]] || continue
        [[ "$seen" == *" $c "* ]] && continue
        seen+="$c "
        if ssh \
          -o ConnectTimeout="''${CAST_ON_SSH_CONNECT_TIMEOUT:-8}" \
          -o BatchMode=yes \
          -o StrictHostKeyChecking=accept-new \
          "$SSH_USER@$c" 'true' >/dev/null 2>&1; then
          echo "$c"
          return 0
        fi
      done

      echo "cast-on: cannot resolve a reachable SSH target for $original (attr '$attr')" >&2
      echo "cast-on: tried:$seen" >&2
      return 1
    }

    is_darwin_attr() {
      nix eval "''${NIX_LOCAL_ARGS[@]}" --raw "$FLAKE_REF#darwinConfigurations.\"$1\".config.system.build.toplevel.outPath" \
        "''${DARWIN_NIX_ARGS[@]}" \
        &>/dev/null
    }
    is_nixos_attr() {
      nix eval "''${NIX_LOCAL_ARGS[@]}" --raw "$FLAKE_REF#nixosConfigurations.\"$1\".config.system.build.toplevel.outPath" \
        "''${NIXOS_NIX_ARGS[@]}" \
        &>/dev/null
    }

    # ── 3. Build all closures locally first ────────────────────────────

    echo "==> Building closures"
    for h in "''${HOSTS[@]}"; do
      attr="$(bare_attr "$h")"
      if is_darwin_attr "$attr"; then
        echo "  build darwin: $attr"
        nix build "''${NIX_LOCAL_ARGS[@]}" --no-link "$FLAKE_REF#darwinConfigurations.''${attr}.system" \
          "''${DARWIN_NIX_ARGS[@]}" \
          --print-out-paths > "$CAST_ON_TMPDIR/''${attr}.out"
      elif is_nixos_attr "$attr"; then
        echo "  build nixos:  $attr"
        nix build "''${NIX_LOCAL_ARGS[@]}" --no-link "$FLAKE_REF#nixosConfigurations.''${attr}.config.system.build.toplevel" \
          "''${NIXOS_NIX_ARGS[@]}" \
          --print-out-paths > "$CAST_ON_TMPDIR/''${attr}.out"
      else
        echo "cast-on: $h (attr '$attr') has neither a darwin nor nixos configuration in $FLAKE" >&2
        exit 1
      fi
    done

    if [[ $DRY_RUN -eq 1 ]]; then
      echo "==> Dry run: skipping copy + activation"
      for h in "''${HOSTS[@]}"; do
        attr="$(bare_attr "$h")"
        echo "  $h -> $(cat "$CAST_ON_TMPDIR/''${attr}.out")"
      done
      exit 0
    fi

    # ── 4. Copy closures + activate per-host ──────────────────────────
    # Resolve each host at execution time. DHCP/mDNS state moves around on
    # laptops, so group entries are treated as names/patterns, not stable
    # transport endpoints.
    REMOTE_HOSTS=()
    LOCAL_HOSTS=()
    for h in "''${HOSTS[@]}"; do
      bare="$(bare_attr "$h")"
      if [[ "$bare" == "$LOCAL_HOSTNAME" ]]; then
        LOCAL_HOSTS+=("$h")
      else
        REMOTE_HOSTS+=("$h")
      fi
    done

    activate_local() {
      local attr="$1"
      local out
      out="$(cat "$CAST_ON_TMPDIR/''${attr}.out")"
      if is_darwin_attr "$attr"; then
        echo "==> Activating local darwin ($attr)"
        # `<closure>/activate` is the script the closure ships; it
        # handles secrets, launchd, etc. Calling
        # `<closure>/sw/bin/darwin-rebuild activate` instead breaks on
        # newer darwin-rebuild because the inner script self-locates a
        # sibling `activate` that doesn't exist. The bare closure
        # script has no such ambiguity.
        sudo "$out/activate"
      else
        echo "==> Activating local nixos ($attr)"
        sudo nixos-rebuild switch --flake "$FLAKE_REF#''${attr}"
      fi
    }

    for h in "''${LOCAL_HOSTS[@]}"; do
      activate_local "$(bare_attr "$h")"
    done

    if [[ ''${#REMOTE_HOSTS[@]} -gt 0 ]]; then
      # Copy every host's closure concurrently — each copy is independent, so
      # serialising them just adds wall-clock. Output is captured per host and
      # replayed in order afterwards so the logs stay readable.
      # --no-check-sigs: closures built locally don't carry a binary cache
      # signature, so the receiving host would reject them by default.
      # Intra-fleet copies are trusted (auth is already gated by the SSH key
      # just used to connect), so skip the sig check on the receive side.
      echo "==> Copying closures to remote hosts (parallel)"
      cp_hosts=()
      cp_pids=()
      for h in "''${REMOTE_HOSTS[@]}"; do
        attr="$(bare_attr "$h")"
        out="$(cat "$CAST_ON_TMPDIR/''${attr}.out")"
        target="$(resolve_ssh_target "$h" "$attr")"
        echo "  $attr -> $target"
        (
          if ! nix copy --no-check-sigs --to "ssh-ng://''${SSH_USER}@$target" "$out"; then
            echo "nix copy over ssh-ng failed for $target; falling back to nix-store export/import"
            nix-store -qR "$out" \
              | nix-store --export \
              | ssh "$SSH_USER@$target" '/nix/var/nix/profiles/default/bin/nix-store --import' >/dev/null
          fi
        ) > "$CAST_ON_TMPDIR/''${attr}.copy.log" 2>&1 &
        cp_hosts+=("$h")
        cp_pids+=("$!")
      done
      cp_fail=0
      for i in "''${!cp_pids[@]}"; do
        if ! wait "''${cp_pids[$i]}"; then cp_fail=1; echo "==> copy FAILED: ''${cp_hosts[$i]}"; fi
        cat "$CAST_ON_TMPDIR/$(bare_attr "''${cp_hosts[$i]}").copy.log" 2>/dev/null || true
      done
      [[ $cp_fail -eq 0 ]] || { echo "==> one or more copies failed; aborting before activation"; exit 1; }

      # Activate every host concurrently — each host's darwin/nixos activation
      # is independent, so wall-clock is now the slowest single host instead of
      # the sum of all of them. Per-host output is captured and replayed in
      # order, and any host's failure fails the whole run (after all finish).
      echo "==> Activating remote hosts (parallel)"
      act_hosts=()
      act_pids=()
      for h in "''${REMOTE_HOSTS[@]}"; do
        attr="$(bare_attr "$h")"
        out="$(cat "$CAST_ON_TMPDIR/''${attr}.out")"
        if is_darwin_attr "$attr"; then
          # See activate_local() above for why we call $out/activate directly
          # instead of $out/sw/bin/darwin-rebuild activate.
          cmd="sudo $out/activate"
        else
          # NixOS: switch-to-configuration is the right entry point when the
          # system closure is already on-disk (we just copied it).
          cmd="sudo $out/bin/switch-to-configuration switch"
        fi
        target="$(resolve_ssh_target "$h" "$attr")"
        echo "  $h -> $target: $cmd"
        ssh -o ConnectTimeout="''${CAST_ON_SSH_CONNECT_TIMEOUT:-8}" "$SSH_USER@$target" "$cmd" \
          > "$CAST_ON_TMPDIR/''${attr}.activate.log" 2>&1 &
        act_hosts+=("$h")
        act_pids+=("$!")
      done
      act_fail=0
      for i in "''${!act_pids[@]}"; do
        if wait "''${act_pids[$i]}"; then status="ok"; else status="FAILED"; act_fail=1; fi
        echo "----- ''${act_hosts[$i]} activation ($status) -----"
        cat "$CAST_ON_TMPDIR/$(bare_attr "''${act_hosts[$i]}").activate.log" 2>/dev/null || true
      done
      [[ $act_fail -eq 0 ]] || { echo "==> one or more activations failed"; exit 1; }
    fi

    echo "==> cast-on complete"
  '';

  meta = {
    description = "Parallel NixOS/nix-darwin deploys: build locally, nix copy, activate";
    mainProgram = "cast-on";
  };
}
