# Task template: perf-cascade-strategy

The agent edits one of the cascade strategies in
`crates/consortium-nix/src/cascade_strategies.rs` to make a target
strategy converge faster on bandwidth-skewed scenarios. Measured by
the `cascade_strategies/bimodal/256/<strategy>` criterion bench in
`crates/consortium-fanout-sim/benches/cascade_strategies.rs`.

The default optimization target is `max-bottleneck-spanning` (set
`AR_CASCADE_STRATEGY=<name>` to target a different strategy).

## Substrate (read before editing)

The cascade primitive (`crates/consortium-nix/src/cascade.rs`) runs a
log-N peer-to-peer closure distribution: each round, the strategy
picks `(source, target)` edges; the executor measures simulated wall
time as `closure_size / bandwidth + latency`; the cascade halts when
all nodes converge.

Three strategies ship out-of-the-box:

| Strategy | Algorithm | Per-source cap |
|---|---|---|
| `Log2FanOut` | id-order pairing, network-blind | 1 edge / round |
| `MaxBottleneckSpanning` | greedy max-weight matching by bandwidth | 1 edge / round |
| `SteinerGreedy` | greedy max-weight assignment | unlimited |

The bench runs all three against:

- `cascade_strategies/uniform/256` — all edges 100 MB/s, 256 nodes
- `cascade_strategies/bimodal/256` — bimodal bandwidth (1 MB/s slow,
  1 GB/s fast at 30%-fast), 256 nodes
- `cascade_strategies/bimodal/512` — same shape, 512 nodes

Score gate (`autoresearch/scripts/score-perf-cascade.sh`) reads
`bimodal/256/<target_strategy>` and `uniform/256/<target_strategy>`
mean ns from criterion's `estimates.json` and compares to baseline.

## Frontmatter

```yaml
type: perf-cascade-strategy
target_file: crates/consortium-nix/src/cascade_strategies.rs
target_line: <line>           # point at the function to optimize
description: <one-line summary of the proposed optimization>
acceptance:
  - bimodal_256_<strategy>_ns improves by >= 5% vs baseline
  - uniform_256_<strategy>_ns does not regress > 10%
  - cargo nextest run -p consortium-fanout-sim passes (correctness +
    fuzz invariants must still hold)
  - cargo clippy clean
  - cargo fmt --check passes
needs:
  - read crates/consortium-nix/src/cascade.rs first to understand
    the CascadeStrategy trait + state shape
  - read crates/consortium-fanout-sim/benches/cascade_strategies.rs
    (the scoring harness — DO NOT modify)
  - read crates/consortium-fanout-sim/tests/{correctness,fuzz}.rs
    (universal invariants — your edit must keep these passing)
```

## Body

The plausible wins live in:

- **Smarter matching**: greedy max-weight is a 2-approximation. A
  proper Hungarian / Hopcroft-Karp bipartite matching could pick
  globally better assignments per round (~50-100 lines, no new deps
  needed since the round is small enough for O(N³) Hungarian).
- **Source-cap heuristics**: SteinerGreedy currently uncaps; capping
  at `floor(sources / 2)` per source might balance load and reduce
  contention on sources that span both fast and slow targets.
- **Cross-round lookahead**: today the strategy plans round-at-a-time.
  Greedily picking the highest-bandwidth edges per round can leave
  fast sources matched to fast targets that already have many
  alternatives, while slow targets compete for the few sources that
  can reach them at speed. A 2-round lookahead is cheap.
- **Targeted graph algorithms**: for the bimodal scenario, partitioning
  the graph into "fast cluster" and "slow cluster" first, then
  cascading within clusters before bridging, may converge faster
  overall.

Don't propose:
- Adding new dependencies to `crates/consortium-nix/Cargo.toml` unless
  necessary (petgraph is already on the deferred-list — re-adding it
  is fine when the algorithm needs it).
- Edits to `crates/consortium-fanout-sim/benches/cascade_strategies.rs`
  — that's the scoring harness; modifying invalidates the score.
- Edits to `crates/consortium-nix/src/cascade.rs` (the coordinator).
  The strategy interface is the only edit surface; if a feature
  requires changing the trait, file an architect task instead.
- Edits to `crates/consortium-fanout-sim/tests/{correctness,fuzz}.rs`
  — those are correctness signals; if a fuzz case breaks, your edit
  is wrong, not the test.

## Scoring contract

`score.sh` runs (in addition to fmt/clippy/nextest gates) when
`AR_TASK_TYPE=perf-cascade-strategy`:

```bash
timeout 300 cargo bench -p consortium-fanout-sim --bench cascade_strategies -- \
  "^cascade_strategies/(uniform|bimodal)/256/$STRATEGY\$" --quick
```

Then it reads:

```
target/criterion/cascade_strategies/bimodal/256/<strategy>/new/estimates.json
target/criterion/cascade_strategies/uniform/256/<strategy>/new/estimates.json
```

via `jq -r '.mean.point_estimate'`. The gate passes iff:

- `bimodal_<strategy>_ns(branch) <= baseline.perf_cascade.bimodal_256.<strategy> * 0.95`
- `uniform_<strategy>_ns(branch) <= baseline.perf_cascade.uniform_256.<strategy> * 1.10`

The simplicity criterion still applies: trivial wins via unsafe
or contorted code → discard. Wins via deletion or clearer algorithms
→ keep regardless of magnitude.

## How this connects to the cascade primitive

The cascade primitive (`run_cascade` in cascade.rs) is the
coordinator loop. It calls `strategy.next_round(&state, &net)`,
dispatches the returned edges via `RoundExecutor`, records outcomes,
and repeats. Strategies are pure: given the same `(state, net)`,
they must return the same `CascadePlan`. They can be `Send + Sync`
because they hold no mutable state — the cascade owns all state.

This means optimizing a strategy is purely about decision quality
(what edges to pick) and CPU efficiency (how fast you compute the
answer). You cannot optimize by changing the protocol, the failure
semantics, or the convergence definition — those are owned by the
coordinator.

## Hint files to read before editing

- `crates/consortium-nix/src/cascade.rs` — `CascadeStrategy` trait,
  `CascadeState`, `NetworkProfile`
- `crates/consortium-nix/src/cascade_strategies.rs` — the file you'll
  edit (`Log2FanOut`, `MaxBottleneckSpanning`, `SteinerGreedy`)
- `crates/consortium-fanout-sim/src/scenario.rs` — `Scenario::run` is
  what the bench calls
- `crates/consortium-fanout-sim/benches/cascade_strategies.rs` — what
  is being measured (DO NOT modify)
- `crates/consortium-fanout-sim/tests/correctness.rs` — invariants
  your edit must preserve
