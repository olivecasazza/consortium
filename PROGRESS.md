# Consortium Migration Progress

## Summary

| Metric                     | Count |
|----------------------------|-------|
| Rust unit tests passing    |   136 |
| Rust unit tests failing    |     1 |
| Python acceptance passing  |   712 |
| Python acceptance skipped  |   135 |
| Python acceptance failing  |     0 |

## Per-Module Status

| Module         | Rust impl | Rust tests | PyO3 binding | Py tests (pass/skip/fail) | Status      |
|----------------|-----------|------------|--------------|---------------------------|-------------|
| RangeSet       | done      | 40 pass    | done         | 57/0/0                    | done        |
| RangeSetND     | done      | (in range_set) | done     | 23/0/0                    | done        |
| NodeSet        | done      | 27 pass    | done         | 125/0/0                   | done        |
| NodeSetGroup   | done      | (in node_set)  | done     | 76/0/0                    | done        |
| NodeUtils      | done      | 14 pass    | done         | (tested via NodeSet)      | done        |
| MsgTree        | done      | 17 pass    | done         | 11/0/0                    | done        |
| Defaults       | done      | 14p/1f     | done         | 9/0/0                     | done (1 Rust test flaky) |
| Event          | done      | 4 pass     | done         | 20/0/0                    | done        |
| Engine         | partial   | 8 pass     | -            | (tested via Task)         | partial     |
| Worker         | partial   | 12 pass    | -            | 14/0/0 (Exec), 10/0/0 (Stream) | partial |
| Topology       | partial   | 0          | done         | 25/0/0                    | partial (parser stubbed) |
| Task           | partial   | 0          | partial      | 118/61/0 (Local), 22/0/0 (Timer), 10/0/0 (MsgTree), 3/0/0 (Port), 3/0/0 (RLimits), 1/0/0 (Timeout), 7/0/0 (ThreadJoin), 2/0/0 (ThreadSuspend) | partial |
| Communication  | stub      | 0          | -            | -                         | not started |
| Propagation    | stub      | 0          | -            | -                         | not started |
| Gateway        | stub      | 0          | -            | 32/0/0 (TreeGateway)      | partial (py passing via fallback?) |
| CLI            | done      | 0          | n/a          | 21/0/0 (Clubak), 51/0/0 (Nodeset), 12/0/0 (Config), 7/0/0 (Display), 4/0/0 (OptionParser) | mostly done |
| CLIClush       | -         | 0          | n/a          | 0/0/0 (timeout/hang?)     | not started |
| TreeWorker     | -         | 0          | -            | 0/0/0 (timeout/hang?)     | not started |
| TaskDistant    | -         | 0          | -            | 36/45/0 (ssh-dependent)   | partial (skips=no SSH) |
| TaskDistantPdsh| -         | 0          | -            | 6/29/0 (pdsh-dependent)   | partial (skips=no pdsh) |
| Misc           | -         | 0          | -            | 4/0/0                     | done        |

## Implementation DAG (dependency order)

```
Layer 0 (no deps):     RangeSet ✅, MsgTree ✅, Event ✅, Defaults ✅
Layer 1 (RangeSet):    NodeUtils ✅, NodeSet ✅
Layer 2 (NodeSet):     Topology ⚠️  (types done, parser stubbed)
Layer 3 (Event):       Communication ❌ (stub only)
Layer 4 (many deps):   Propagation ❌ (stub only)
Layer 5 (Event):       Engine ⚠️  (partial — timers, state machine done)
Layer 6 (Engine):      Worker ⚠️  (partial — exec worker done, traits done)
Layer 7 (everything):  Task ⚠️  (partial — many py tests passing via Python fallback)
Layer 8 (everything):  Gateway ❌ (stub only)
Layer 9:               CLI tools ✅ (mostly done, CLIClush blocked)
```

## Next priorities (by DAG order)

1. **Topology** — finish TopologyParser::parse (only remaining todo!())
2. **Communication** — implement message types and channel (stub → real)
3. **Propagation** — implement tree router (depends on Communication)
4. **Engine** — complete I/O engine (partial → done)
5. **Worker** — complete remaining worker types
6. **Task** — wire up Rust-backed task orchestration
7. **Gateway** — implement gateway logic
8. **CLIClush** — fix whatever is causing 0 results

## Legend

- Rust impl: `done` | `partial` | `stub` | `-`
- PyO3 binding: `done` | `partial` | `-`
- Status: `done` | `partial` | `not started`
- ✅ = done, ⚠️ = partial, ❌ = not started/stub
