# Scatter–gather Nix builds over a consortium pool — design spec

Fan a large Nix build out across every node in a pool, let the nodes exchange
intermediate artifacts **with each other**, and gather back to the requesting
host only what it actually asked for. The dual of the existing
[`cascade`](../crates/consortium-nix/src/cascade.rs) primitive: cascade moves
one closure *out* to many nodes (log-N broadcast); scatter–gather moves one
derivation graph *out* for building and collects the results *in* (reduce).

> **Status:** design only. Nothing here is implemented. Motivating numbers in
> §1 were measured on a real fleet build on 2026-09-26; claims about Nix's
> native behavior cite the Nix manual and nixbuild.net's documentation. The
> performance claims in §7 are hypotheses the simulator (§8) must confirm
> before any production code is written.

---

## 1. Why

A single `darwinConfigurations.<host>.system` build of a real fleet config
(Snowfall flake, ~30 inputs, several Rust tools built with per-crate
derivations) planned:

| | count |
|---|---|
| derivations to build | 1036 |
| paths to fetch from caches | 1222 (2.9 GiB) |
| Rust vendoring: `crate-*.tar.gz` fetches + one unpack derivation each | 366 (183 + 183) |
| heavy compiles: independent Rust package builds (`codex`, `herdr` + plugins, `memex`, `agentctx`, `bingbong`, `opencode-voice`, …) | ~12 |
| config glue (HM files, launchd plists, activation) | 168 |
| still-missing package outputs (210) found on any reachable cache | 3 |

The six other Macs in the fleet were unused (and, at the time, unreachable),
so one laptop did all of it. The shape matters for the design:

- The **wide** part (366 vendoring derivations) is network-bound and nearly
  free in CPU. Spreading it across nodes buys little; what it needs is for the
  crate tarballs to be fetched once, not once per requester.
- The **heavy** part is about a dozen `cargo build` derivations that do **not**
  depend on each other. Each compiles its whole crate graph inside one
  derivation, so none of them parallelizes *internally* across machines — but
  a dozen independent heavy jobs on a dozen nodes is exactly the fan-out win.
- The config glue is tiny; shipping it anywhere costs more than building it.

It all ran on one machine because:

## 2. What Nix does today, and where it stops

Nix already has distributed builds (`builders`, `/etc/nix/machines`). Its
scheduler is the build hook inside the requesting daemon. Per the Nix manual
([Remote Builds](https://nix.dev/manual/nix/2.34/advanced-topics/distributed-builds.html))
and nixbuild.net's analysis
([Running Remote Builds](https://docs.nixbuild.net/cloud/remote-builds/)):

1. **The requester is a hub for every byte.** "Before [building remotely], all
   required input paths must exist in the local store … Nix might fetch or
   build inputs in the local store that *never are needed locally*." Every
   intermediate output is then copied back: "Nix will fetch the build result
   and its transitive run-time closure from the remote builder to the local
   store."
2. **Builders are not substituters.** "Nix will not query them for the output
   paths", so work a builder already did is redone or re-transferred.
3. **Scheduling is client-local and greedy.** "The build scheduling on remote
   builders is simplistic and entirely directed from the Nix client"; multiple
   clients "will not be aware of each other".
4. **No builder-to-builder transfer.** If builder A produces an input that
   builder B needs, it travels A → requester → B.

Existing tools that route around (1):

- **Remote store builds** (`nix build --store ssh-ng://host --eval-store auto`)
  build entirely on one remote host; intermediates never touch the requester.
- **[nix-fast-build](https://github.com/Mic92/nix-fast-build) `--remote`**
  uploads the flake and performs "all evaluation/build operations on the
  remote end", downloading only finished builds.

Both use **one** remote machine. Neither partitions a graph across a pool.
consortium's own `build.rs` today renders a machines file from healthy
builders and hands scheduling to the Nix hook, inheriting 1–4.

## 3. The idea

```text
             requester R
   plan:  eval → DAG → drop substitutable → place (strategy)
                 │ ship .drv files (small)
     ┌───────────┼───────────┬───────────┐
     ▼           ▼           ▼           ▼
   node A      node B      node C      node D     scatter: each node builds
   subtree     subtree     subtree     subtree    its assigned sub-DAG
     │  ◀─peer copy─▶ │  ◀─peer copy─▶ │          inputs move node↔node,
     └──────┬────┘           └─────┬─────┘        never via R
            ▼                      ▼
      merge (dedupe)         merge (dedupe)       gather: only the requested
            └─────────┬────────────┘              roots' RUNTIME closure,
                      ▼                           reduced up a tree
                 requester R   (or: straight to deploy targets via cascade)
```

"Merge sort" is the right intuition: leaves are independent runs built in
parallel; merges happen where a derivation needs outputs from several runs; the
final merge at the root is a set union of store paths, which deduplicates
(runtime closures overlap heavily).

## 4. Model

Reuse cascade's vocabulary and seams so the simulator and error machinery carry
over unchanged.

- **`BuildGraph`** — nodes are derivations: `drv_path`, `system`,
  `required_features`, `outputs`, `est_build_secs`, `est_output_bytes`,
  `runtime_refs`. Edges are input-derivation dependencies. Built from
  `nix derivation show -r` (or `nix-eval-jobs` for many roots), then pruned of
  everything already valid on the requester **or on any pool node** (fixes §2.2:
  query peers as stores during planning) **or** on a configured substituter.
- **`Pool`** — `CascadeNode`-like entries: supported systems/features, build
  slots (`max-jobs`), a **location map** of which valid paths each node holds,
  and links from the existing `NetworkProfile` (bandwidth/latency/partition).
- **`PlacementStrategy`** — the pluggable decision surface, the analog of
  `CascadeStrategy::next_round`: given the ready set, the location map and the
  network profile, choose `(drv → node)` assignments and the transfers they
  imply. Round-based like cascade, so it is simulable.
- **`BuildExecutor`** — analog of `RoundExecutor`: production shells out to
  Nix over SSH; the simulator advances a virtual clock from
  `est_build_secs` and `bytes / bandwidth + latency`.
- **Errors** — reuse `CascadeError::SubtreeAggregate` semantics: failures
  aggregate by DAG subtree, so the report names every failed derivation with
  the path that needed it, not "first error wins".

## 5. Placement strategies

Ship at least three so the simulator can compare them:

1. **`HubBaseline`** — emulates the Nix build hook (all inputs via R, all
   outputs back to R). Exists only as the comparison baseline.
2. **`SubtreePartition`** — cut the DAG at heavy nodes and assign whole
   subtrees to one worker (every crate of one Rust package goes to the same
   node). Cheap, needs no cost model, maximizes locality. Cross-subtree edges
   become peer copies.
3. **`Heft`** — Heterogeneous Earliest Finish Time list scheduling (Topcuoglu,
   Hariri & Wu, *IEEE TPDS* 2002): rank derivations by upward rank
   (`cost + max(comm + rank(successor))`), place each on the node with the
   earliest finish time including transfer cost from wherever its inputs
   currently live. Handles heterogeneous pools (an M5 Max next to an Air).

All strategies are subject to hard constraints: `system` and
`requiredSystemFeatures` must match the node; `preferLocalBuild` derivations
stay on R (they exist because moving them costs more than building them).

Estimates will be wrong, so execution is **static plan + work stealing**: an
idle node may take a ready derivation whose inputs are cheap for it to fetch.

## 6. Execution protocol (per node, over SSH)

1. **Ship derivations**: `nix copy --to ssh-ng://node /nix/store/<…>.drv`.
   A bare `.drv` store path names the derivation itself (outputs would be
   `<…>.drv^*`), but copying it transfers its whole **derivation closure** —
   every input `.drv` plus source files and patches. Measured: one crate-unpack
   derivation pulled 798 paths. Most are already present on a pool node that
   has built this flake before (`nix copy` skips valid paths), but a cold node
   pays it once; ship per subtree, not per derivation.
2. **Inputs**, in order of preference: already in the node's store → the
   node's own substituters → **a peer** that holds it per the location map,
   fetched *by the node* (`nix copy --from ssh-ng://peer`), never relayed
   through R.
3. **Build**: realise the assigned derivations on the node with its own
   builders disabled (`--builders ''`), so no node recursively re-dispatches.
4. **Report**: on completion the scheduler records the outputs in the location
   map; dependents become ready.
5. **Gather**: only for the requested roots, copy their **runtime** closure to
   the destination from the nearest holder. When R's link is the bottleneck,
   reduce up a tree (the mirror image of cascade's log-N broadcast) so shared
   paths cross R's link once.
6. **Deploy mode** (`--gather deploy:@targets`): skip R entirely and seed a
   cascade from the nodes that hold the outputs straight to the target hosts —
   *build where it lands*. This is the `cast-on` path: today it builds on the
   workstation and pushes every closure out from there.

**Trust.** Moving unsigned paths between stores requires `trusted-users` on
each node. The durable option is a pool signing key: nodes sign what they
build (`nix store sign`), and every pool member plus R trusts the pool key, as
a Hydra fleet does. Input-addressed outputs are not content-verified on
receipt; a compromised pool node can poison results. Content-addressed
derivations would make that verifiable and are out of scope here.

## 7. Expected behavior (hypotheses for the simulator)

- **Wins**: many independent **heavy** derivations (the dozen Rust package
  builds in §1), a slow or saturated requester link, heterogeneous pools, and
  deploys where the requester never needs the result.
- **No win**: the critical path. A chain of N dependent derivations takes
  their summed build time whatever the pool size; the planner should report
  that lower bound so users know when adding nodes cannot help.
- **Can lose**: tiny derivations where SSH round-trips dominate (the 168
  config-glue derivations in §1 should stay on R); network-bound fan-out such
  as the 366 vendoring derivations, where scattering multiplies downloads
  unless nodes share a cache; and graphs whose intermediates are huge relative
  to their build time (move-cost dominated).
- **Within one derivation, never**: a `cargo build` compiles its entire crate
  graph as one derivation. Splitting *that* needs per-crate derivations
  (crate2nix-style `buildRustCrate`), a packaging change outside this design.

## 8. Simulation first

Extend `consortium-fanout-sim` with a build scenario before writing the
executor:

- **Fixtures**: seeded DAG generators (wide-shallow vendoring layers, a few
  heavy independent roots, deep chains, diamonds, mixed) with seeded build-time and output-size distributions, plus
  **one real fixture**: the §1 graph exported from its `.drv` files
  (1036 nodes), with sizes from `nix path-info -S`.
- **Metrics**: makespan; bytes over R's link; total bytes moved; per-node
  utilization; ratio to the critical-path lower bound.
- **Invariants** (reuse the `invariants` helpers): every derivation built
  exactly once; no derivation starts before all its inputs are on its node;
  same seed ⇒ identical schedule, byte counts and error tree.
- **Acceptance for moving past design**: on the real fixture, `SubtreePartition`
  or `Heft` beats `HubBaseline` on makespan AND on bytes over R's link, across
  a range of seeded network profiles.

## 9. CLI sketch

```text
consortium build <flake>#<attr> --pool @builders
    [--strategy heft|subtree|hub]
    [--gather root|none|deploy:@targets]
    [--dry-run]   # print placement, predicted makespan, bytes via R,
                  # critical-path lower bound — from the same model as the sim
```

## 10. Phasing

- **P0 — Nix does the heavy lifting.** `SubtreePartition` + one remote-store
  build per subtree root (`nix build --store ssh-ng://node --eval-store auto`)
  + pool nodes configured as each other's substituters + gather the final
  closure. Little new code; validates the win on real hardware.
- **P1 — derivation-level scheduler.** `Heft`, the location map, peer copies
  driven by the consortium executor, per-subtree error aggregation.
- **P2 — gather tree + deploy mode.** Reduce-tree gather; build-where-it-lands
  via cascade; `cast-on` gains `--pool`.
- **P3 — scale.** Work stealing, a persisted cost model (build seconds and NAR
  size per derivation name from past runs), and a shared scheduler so several
  requesters stop colliding on the same builders (§2.3).

## 11. Open questions

- Parallel evaluation: `nix-eval-jobs` for many roots, and whether evaluation
  itself should move to a pool node for very large flakes.
- IFD: builds during evaluation must not be dispatched before the plan exists.
- Where the cost model lives (per-requester file vs. shared service).
- macOS nodes: `__noChroot` derivations and sandbox differences can make a
  build host-sensitive; failures should be retried elsewhere only when the
  failure is plausibly environmental, never for deterministic build errors.
- Minimum trust model for a homelab pool: `trusted-users` over SSH first, pool
  signing key later?
