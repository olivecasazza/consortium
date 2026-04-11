# Consortium Architecture Guide

Lead-designer reference for sub-agents implementing individual modules.
Each section is self-contained: a sub-agent should be able to implement
a module by reading ONLY its section plus Tier 0 (RangeSet) as context.

Last updated: 2026-04-07

---

## Table of Contents

1. [Migration Overview](#migration-overview)
2. [Codebase Layout](#codebase-layout)
3. [Cross-Cutting Conventions](#cross-cutting-conventions)
4. [Tier 0 — RangeSet (COMPLETE)](#tier-0--rangeset-complete)
5. [Tier 1 — Leaf Modules (0 deps)](#tier-1--leaf-modules)
   - [MsgTree](#msgtree)
   - [Event](#event)
   - [Defaults](#defaults)
   - [NodeUtils](#nodeutils)
   - [Engine](#engine)
6. [Tier 2 — First Dependencies](#tier-2--first-dependencies)
   - [NodeSet](#nodeset)
   - [Worker](#worker)
   - [Communication](#communication)
7. [Tier 3 — Topology Layer](#tier-3--topology-layer)
   - [Topology](#topology)
   - [Propagation](#propagation)
8. [Tier 4 — Orchestration](#tier-4--orchestration)
   - [Task](#task)
   - [Gateway](#gateway)
9. [Tier 5 — CLI](#tier-5--cli)
10. [Test Mapping Strategy](#test-mapping-strategy)
11. [Dependency Graph](#dependency-graph)

---

## Migration Overview

We are porting CEA's ClusterShell (Python) to Rust, crate name `consortium`.
The Python source in `lib/ClusterShell/` is the oracle — the Rust
implementation must produce identical behavior (verified by parity tests).

**Strategy:**
- Pure Rust core in `crates/consortium/`
- PyO3 bindings in `crates/consortium-py/` so Python users get a drop-in
  replacement with `CONSORTIUM_BACKEND=rust`
- Stacked git branches per module, merging to master
- TEST_MAPPING.toml tracks Python→Rust test parity (582 total Python tests)

**Current status:** 1 of 14 modules complete (RangeSet).

---

## Codebase Layout

```
consortium/
├── crates/
│   ├── consortium/              # Pure Rust library
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── range_set.rs     # ✅ 1241 lines, 43 tests
│   │       ├── msg_tree.rs      # stub (59 lines)
│   │       ├── event.rs         # stub (20 lines)
│   │       ├── defaults.rs      # stub (49 lines)
│   │       ├── node_utils.rs    # stub (51 lines)
│   │       ├── engine/mod.rs    # stub (11 lines)
│   │       ├── node_set.rs      # stub (104 lines)
│   │       ├── topology.rs      # stub (78 lines)
│   │       ├── communication.rs # stub (24 lines)
│   │       ├── propagation.rs   # stub (9 lines)
│   │       ├── task.rs          # stub (18 lines)
│   │       └── gateway.rs       # stub (7 lines)
│   └── consortium-py/           # PyO3 bindings
│       ├── src/
│       │   ├── lib.rs
│       │   ├── range_set.rs     # ✅ Python-facing RangeSet
│       │   └── node_set.rs      # stub
│       └── ClusterShell/        # Python shims (backend switching)
├── lib/ClusterShell/            # ORIGINAL Python source (oracle)
├── tests/                       # Original Python test suite
├── harness/                     # Migration test infrastructure
├── TEST_MAPPING.toml            # 582 tests tracked
└── flake.nix                    # Nix dev environment
```

---

## Cross-Cutting Conventions

### Rust Patterns
- Use `thiserror` for error enums (already in deps)
- Expose `pub` API matching Python's public interface
- Use `#[cfg(test)] mod tests` for unit tests
- Bytes-oriented: Python ClusterShell operates on `bytes` for messages,
  use `Vec<u8>` / `&[u8]` in Rust
- All public types derive `Debug`; derive `Clone` where sensible
- Use `std::collections::HashMap` / `BTreeMap` as needed

### PyO3 Binding Pattern (established in RangeSet)
- Wrapper struct in `consortium-py/src/<module>.rs`
- `#[pyclass]` + `#[pymethods]` implementing Python dunder methods
- Backend switching: `ClusterShell/<Module>.py` checks
  `CONSORTIUM_BACKEND` env var, imports from `consortium` (Rust) or
  falls back to Python impl

### Testing Pattern
- Rust unit tests: `cargo test -p consortium`
- Python parity tests: `python -m pytest tests/ -k <module>`
- Migration harness: `python harness/run_comparison.py`

---

## Tier 0 — RangeSet (COMPLETE)

**Status:** ✅ Done — 1241 Rust lines, 43 unit tests, 57 parity tests at 100%

RangeSet is the foundational numeric-range set used by NodeSet for bracket
ranges (e.g., `1-100`, `001-050/2`). Sub-agents implementing NodeSet MUST
use the Rust RangeSet API.

**Key Rust API available:**
- `RangeSet::new()`, `RangeSet::from_str()`, `RangeSet::from_list()`
- `.add()`, `.add_range()`, `.remove()`, `.contains()`, `.len()`
- `.union()`, `.intersection()`, `.difference()`, `.symmetric_difference()`
- `.iter()`, `.display()` with folding/padding/autostep
- Supports zero-padding width tracking

**Not yet ported in RangeSet** (needed for full NodeSet support):
- `__getitem__` / `__setitem__` (indexing/slicing)
- `split()`, `contiguous()`, `dim()`
- `fromlist()`, `fromone()` constructors
- Comparison operators (`issubset`, `issuperset`, `__gt__`, `__lt__`)
- `__hash__`, pickle

---

## Tier 1 — Leaf Modules

These modules have ZERO internal ClusterShell dependencies and can be
implemented in any order, in parallel.

---

### MsgTree

**File:** `lib/ClusterShell/MsgTree.py` (362 lines)
**Test:** `tests/MsgTreeTest.py` (330 lines, 11 test methods)
**Rust stub:** `crates/consortium/src/msg_tree.rs` (59 lines)
**Deps:** NONE

**Purpose:** Shared message tree for aggregating output lines from
multiple sources (nodes). Memory-efficient — identical message lines
from different nodes share tree nodes. This is the core data structure
behind `task.iter_buffers()`.

**Architecture:**

Two classes:

1. **MsgTreeElem** — A node in the tree
   - `parent: Option<&MsgTreeElem>` (links up to root)
   - `children: HashMap<Vec<u8>, MsgTreeElem>` (msgline → child)
   - `msgline: Option<Vec<u8>>` (this node's line content)
   - `keys: Option<HashSet<K>>` (source keys, e.g. node names)
   - Methods: `append(msgline, key)`, `lines()`, `message()`
   - Walking up the tree via parent links assembles the full message

2. **MsgTree** — The container
   - `mode: Mode` (DEFER=0, SHIFT=1, TRACE=2)
   - `_root: MsgTreeElem` (sentinel root, msgline=None)
   - `_keys: HashMap<K, &MsgTreeElem>` (key → current tree position)
   - Methods: `add(key, msgline)`, `walk()`, `walk_trace()`, `remove(match)`,
     `keys()`, `messages()`, `items()`, `clear()`, `__len__`, `__getitem__`

**Key Algorithm — The Tree Structure:**
```
When add(key="node1", msgline=b"hello") then add(key="node1", msgline=b"world"):
  root
  └── "hello" (children["hello"])
      └── "world" (children["world"], keys={"node1"})

When add(key="node2", msgline=b"hello") then add(key="node2", msgline=b"world"):
  root
  └── "hello" (children["hello"])          ← shared!
      └── "world" (children["world"], keys={"node1", "node2"})

When add(key="node3", msgline=b"hello") then add(key="node3", msgline=b"different"):
  root
  └── "hello"
      ├── "world" (keys={"node1", "node2"})
      └── "different" (keys={"node3"})
```

**Three Modes:**
- `MODE_DEFER` (default): Messages stored immediately, keys deferred.
  Keys assigned on first `walk()`, then mode upgrades to SHIFT.
  Most efficient for bulk loading.
- `MODE_SHIFT`: Keys tracked at each `add()`. When a key moves to a
  child, it's removed from parent (shift semantics).
- `MODE_TRACE`: Keys tracked and KEPT at every level (no removal).
  Enables `walk_trace()` which yields `(msgline, keys, depth, nchildren)`.

**Critical Details:**
- All message data is `bytes` (`Vec<u8>` in Rust), NOT strings
- `MsgTreeElem.__bytes__()` / `message()` joins lines with `b'\n'`
- `walk()` yields `(MsgTreeElem, Vec<K>)` — the elem + associated keys
- `walk()` supports `match` (key filter fn) and `mapper` (key transform fn)
- `remove(match)` removes keys matching predicate from tree AND `_keys` dict
- Thread safety: NOT thread-safe in Python; Rust impl should also be
  single-threaded (owned by Task)

**Rust Implementation Notes:**
- The parent-linked tree is tricky in Rust. Options:
  (a) Arena allocation with indices (recommended)
  (b) `Rc<RefCell<>>` (simpler but slower)
  (c) Unsafe raw pointers (avoid)
- Recommend `slotmap` or `generational-arena` crate for arena approach
- Key type should be generic `K: Eq + Hash + Clone`

---

### Event

**File:** `lib/ClusterShell/Event.py` (178 lines)
**Tests:** Tested via `tests/TaskEventTest.py` (568 lines, 20 tests)
**Rust stub:** `crates/consortium/src/event.rs` (20 lines)
**Deps:** NONE

**Purpose:** Defines the `EventHandler` trait — a callback interface for
Worker, EngineTimer, and EnginePort lifecycle events.

**Architecture:**

Single class `EventHandler` with all-stub methods (do nothing by default).
Subclasses override the methods they care about.

**Full Method Signatures:**

```rust
pub trait EventHandler {
    // Worker events
    fn ev_start(&mut self, worker: &Worker) {}
    fn ev_pickup(&mut self, worker: &Worker, node: &str) {}
    fn ev_read(&mut self, worker: &Worker, node: &str, sname: &str, msg: &[u8]) {}
    fn ev_error(&mut self, worker: &Worker) {}  // DEPRECATED, use ev_read + sname=="stderr"
    fn ev_written(&mut self, worker: &Worker, node: &str, sname: &str, size: usize) {}
    fn ev_hup(&mut self, worker: &Worker, node: &str, rc: Option<i32>) {}
    fn ev_close(&mut self, worker: &Worker, timedout: bool) {}

    // EnginePort events
    fn ev_port_start(&mut self, port: &EnginePort) {}
    fn ev_msg(&mut self, port: &EnginePort, msg: &dyn Any) {}

    // EngineTimer events
    fn ev_timer(&mut self, timer: &EngineTimer) {}

    // Private routing event
    fn _ev_routing(&mut self, worker: &Worker, arg: &dyn Any) {}
}
```

**Critical Details:**
- Every method has a default no-op implementation
- `ev_read` is the primary data event: `sname` is `"stdout"` or `"stderr"`
- `ev_hup` fires per-node when command finishes; `ev_close` fires once when
  entire Worker finishes
- `ev_close(timedout=true)` replaces the old `ev_timeout()` (removed in 1.9)
- `ev_error()` is DEPRECATED but still supported for backward compat
- Python uses duck typing for EventHandler; Rust should use a trait with
  default method impls
- The `_ev_routing` method is "private" but used by TreeWorker internals

**Rust Implementation Notes:**
- This is mostly a trait definition — very small module
- The trait will be used as `Box<dyn EventHandler>` in Engine/Worker
- Consider splitting Worker events and Port/Timer events into separate
  traits if cleaner, but Python uses a single class

---

### Defaults

**File:** `lib/ClusterShell/Defaults.py` (333 lines)
**Test:** `tests/DefaultsTest.py` (274 lines, 9 tests)
**Rust stub:** `crates/consortium/src/defaults.rs` (49 lines)
**Deps:** NONE (explicitly designed to not import other ClusterShell modules)

**Purpose:** Global configuration singleton (`DEFAULTS`) loaded from
`defaults.conf` INI files. Controls task defaults, engine settings, and
NodeSet fold behavior.

**Architecture:**

One class `Defaults` + free functions + module-level `DEFAULTS` singleton.

**Configuration Sections & Keys:**

```
[task.default]
stderr = false              # bool
stdin = true                # bool
stdout_msgtree = true       # bool
stderr_msgtree = true       # bool
engine = "auto"             # string
port_qlimit = 100           # int (1.8 compat, also in [engine])
auto_tree = true            # bool
local_workername = "exec"   # string
distant_workername = "ssh"  # string

[task.info]
debug = false               # bool
fanout = 64                 # int
grooming_delay = 0.25       # float (seconds)
connect_timeout = 10.0      # float (seconds)
command_timeout = 0.0       # float (0 = no timeout)

[nodeset]
fold_axis = ()              # tuple of ints (comma-separated)

[engine]
port_qlimit = 100           # int
```

**Config File Search Order** (`config_paths(name)`):
1. `/etc/clustershell/<name>`
2. `~/.local/etc/clustershell/<name>`
3. `<sys.prefix>/etc/clustershell/<name>` (for venv)
4. `$XDG_CONFIG_HOME/clustershell/<name>` (default: `~/.config`)
5. `$CLUSTERSHELL_CFGDIR/<name>` (highest priority, if set)

**Key Behaviors:**
- `__getattr__` dispatches lookups across 4 internal dicts in order:
  engine → task_default → task_info → nodeset
- `__setattr__` similarly dispatches writes
- `port_qlimit` has 1.8 compat: checks engine section first, falls back
  to task_default
- `_TASK_INFO_PKEYS_BL` = `['engine', 'print_debug']` — keys that cannot
  be propagated in tree mode
- `print_debug` is a function callback, NOT configurable via file
- `_load_workerclass(name)` dynamically imports worker modules by name
  (e.g., "ssh" → `ClusterShell.Worker.Ssh`)

**Free Functions to Port:**
- `config_paths(config_name) -> Vec<PathBuf>` — search paths
- `_load_workerclass(name)` — not needed in Rust (static dispatch)
- `_task_print_debug(task, line)` — default debug printer

**Rust Implementation Notes:**
- Use `configparser` or `ini` crate for INI parsing
- The singleton pattern: either `lazy_static!` / `once_cell::Lazy` or
  construct at Task creation time
- Worker class loading doesn't apply in Rust (compile-time dispatch)
- The `print_debug` callback becomes a `fn(&Task, &str)` or
  `Box<dyn Fn(&Task, &str)>`
- Keep the 4-dict structure for backward-compatible attribute lookup

---

### NodeUtils

**File:** `lib/ClusterShell/NodeUtils.py` (689 lines)
**Tests:** `tests/GroupResolverYAMLTest.py`, `tests/GroupSourceTest.py`,
  `tests/NodeSetGroupTest.py` (76 tests use group resolution)
**Rust stub:** `crates/consortium/src/node_utils.rs` (51 lines)
**Deps:** NONE (uses stdlib only: configparser, subprocess, yaml)

**Purpose:** Group resolution framework. Maps group names to node lists
via pluggable backends (files, YAML, external scripts). Used by NodeSet
for `@group` syntax.

**Architecture:**

**Error types (6):**
- `GroupSourceError` (base, has `.group_source` field)
- `GroupSourceNoUpcall` (upcall/method not available)
- `GroupSourceQueryFailed` (query returned non-zero)
- `GroupResolverError` (base resolver error)
- `GroupResolverSourceError` (source not found)
- `GroupResolverIllegalCharError` (bad group chars)
- `GroupResolverConfigError` (config file problem)

**Classes (6):**

1. **GroupSource** — Base class, in-memory groups
   - `name: String`
   - `groups: HashMap<String, String>` (group_name → node_list_str)
   - `allgroups: Option<String>`
   - `has_reverse: bool`
   - Methods: `resolv_map(group)`, `resolv_list()`, `resolv_all()`,
     `resolv_reverse(node)` (raises NoUpcall)

2. **FileGroupSource** — Uses a YAMLGroupLoader for lazy/cached access
   - Properties `groups` and `allgroups` delegate to loader
   - `resolv_all()` uses the special `"all"` group

3. **UpcallGroupSource** — External command execution
   - Upcalls: `map` (required), `all`, `list`, `reverse` (optional)
   - Template substitution: `$GROUP`, `$NODE`, `$CFGDIR`, `$SOURCE`
   - Result caching with configurable TTL (default: 3600s)
   - `cache_time` controls expiry; `clear_cache()` resets
   - Executes via `subprocess.Popen(shell=True, cwd=cfgdir)`

4. **YAMLGroupLoader** — YAML file loader with cache expiry
   - Loads multi-source YAML: top-level keys are source names,
     values are dicts of group_name → node_string
   - Creates `FileGroupSource` objects for each source
   - Auto-reloads when cache expires
   - `null` YAML values → empty string (GH#533)

5. **GroupResolver** — Multi-source resolution coordinator
   - `_sources: HashMap<String, GroupSource>`
   - `_default_source: Option<GroupSource>`
   - `illegal_chars: HashSet<char>` (for sanity checking group names)
   - Lazy initialization via `_late_init()`
   - Methods: `group_nodes(group, ns)`, `all_nodes(ns)`,
     `grouplist(ns)`, `node_groups(node, ns)`, `has_node_groups(ns)`

6. **GroupResolverConfig** — Config-file-driven resolver
   - Reads `groups.conf` INI file (inherits GroupResolver)
   - Reads groups from `confdir`/`groupsdir` `.conf` files
   - Reads YAML auto-sources from `autodir` `.yaml` files
   - `$CFGDIR` template var → dir of last parsed config
   - Section names in conf = source names (comma-separated for aliases)
   - `[Main]` section: `default`, `confdir`/`groupsdir`, `autodir`

**Config File Structure (`groups.conf`):**
```ini
[Main]
default=local
confdir=/etc/clustershell/groups.conf.d
autodir=/etc/clustershell/groups.d

[local]
map=sed -n 's/^$GROUP:\(.*\)/\1/p' /etc/clustershell/groups
all=sed -n 's/^all:\(.*\)/\1/p' /etc/clustershell/groups
list=sed -n 's/^\([a-zA-Z0-9_]*\):.*/\1/p' /etc/clustershell/groups
```

**Rust Implementation Notes:**
- UpcallGroupSource runs shell commands — use `std::process::Command`
- YAMLGroupLoader needs `serde_yaml` crate
- GroupResolverConfig needs INI parsing — same `ini` crate as Defaults
- The `@init` decorator = lazy init pattern; use `Once` or check flag
- Template substitution (`$GROUP` etc) — simple string replace
- This module is complex but well-isolated; good candidate for early work

---

### Engine

**Files:** `lib/ClusterShell/Engine/` directory
- `Engine.py` (786 lines) — base class + timer infrastructure
- `Poll.py` (200 lines) — `select.poll()` backend
- `EPoll.py` (198 lines) — `select.epoll()` backend
- `Select.py` (180 lines) — `select.select()` backend
- `Factory.py` (72 lines) — `PreferredEngine` factory
**Tests:** No dedicated tests; tested via Task tests
**Rust stub:** `crates/consortium/src/engine/mod.rs` (11 lines)
**Deps:** NONE (pure stdlib)

**Purpose:** I/O event loop abstraction. Manages file descriptors,
timers, client registration, and the main `runloop()`. Each Task owns
one Engine.

**Architecture:**

**Constants:**
- `E_READ = 0x1`, `E_WRITE = 0x2` — event interest bits
- `EPSILON = 1e-3` — time comparison epsilon
- `FANOUT_UNLIMITED = -1`, `FANOUT_DEFAULT = None`

**Exception hierarchy:**
- `EngineException` (base)
- `EngineAbortException(kill: bool)` — user abort
- `EngineTimeoutException` — timeout
- `EngineIllegalOperationError` — bad operation
- `EngineAlreadyRunningError` — double run
- `EngineNotSupportedError(engineid)` — backend not available

**Classes:**

1. **EngineBaseTimer** — Abstract timer
   - `fire_delay: f64`, `interval: f64` (-1 = no repeat),
     `autoclose: bool`
   - `_engine: Option<Engine>` (back-reference)
   - `_nextfire: f64` (absolute fire time)
   - Methods: `invalidate()`, `is_valid()`, `set_nextfire(delay, interval)`
   - Abstract: `_fire()`

2. **EngineTimer(EngineBaseTimer)** — Concrete timer with EventHandler
   - `handler: EventHandler`
   - `_fire()` calls `handler.ev_timer(self)`

3. **_EngineTimerQ** — Priority queue of timers (min-heap by fire time)
   - Uses `heapq` (Rust: `BinaryHeap` with `Reverse`)
   - `_EngineTimerCase` wrapper for comparison by fire time
   - `schedule()`, `reschedule()`, `invalidate()`, `fire_expired()`,
     `nextfire_delay()`, `clear()`
   - Lazy cleanup of disarmed timers on dequeue

4. **Engine** (base class, 786 lines — the big one)
   - `_clients: dict` (fd → EngineClient)
   - `_ports: list` (EnginePort objects)
   - `_timerq: _EngineTimerQ`
   - `_info: dict` (from Task.info — fanout, grooming_delay, etc)
   - `_running, _exited: bool`
   - Fanout tracking: `_reg_stats` dict tracks registered clients per source

   **Core loop (`run(timeout)`):**
   1. `start_ports()` — fire ev_port_start for all ports
   2. `start_clients()` — register clients up to fanout limit
   3. `runloop(timeout)` — abstract, impl by backend
   4. `clear()` — cleanup on exit

   **Client lifecycle:**
   - `add(client)` → queues client
   - `register(client)` → associates fd with poll/epoll/select
   - `remove(client)` → unregisters, calls `_close()`
   - `remove_stream(client, stream)` → partial unregister

   **Abstract methods** (implemented by backends):
   - `_register_specific(fd, event)`
   - `_unregister_specific(fd, ev_is_set)`
   - `_modify_specific(fd, event, setvalue)`
   - `runloop(timeout)` — the actual poll/select loop

5. **Backend Classes** (Poll, EPoll, Select)
   - Each implements the 4 abstract methods
   - `runloop()` calls the OS-specific multiplexer, then dispatches
     `_handle_read()` / `_handle_write()` on matching clients
   - EPoll.`release()` closes the epoll fd

6. **PreferredEngine** (Factory.py)
   - Tries EPoll → Poll → Select in order
   - `__new__(cls, hint, info)` — `hint` can force a specific backend
   - Returns an Engine instance

**Rust Implementation Notes:**
- **Strongly recommend `mio`** crate instead of porting all 3 backends.
  `mio` wraps epoll/kqueue/IOCP cross-platform.
- Alternative: `polling` crate (simpler API than mio)
- Timer queue: use `BinaryHeap<Reverse<TimerEntry>>`
- Fanout management is important — limits concurrent subprocess fds
- The Engine is NOT thread-safe (runs in Task's thread)
- `EngineClient` (from Worker module) is the thing being registered —
  Engine and Worker are tightly coupled. Implement them together or
  define clear trait boundaries.

---

## Tier 2 — First Dependencies

---

### NodeSet

**File:** `lib/ClusterShell/NodeSet.py` (1591 lines — LARGEST module)
**Tests:** `tests/NodeSetTest.py` (125 tests), `tests/NodeSetGroupTest.py`
  (76 tests), plus NodeSetGroup2GSTest, NodeSetRegroupTest
**Rust stub:** `crates/consortium/src/node_set.rs` (104 lines)
**Deps:** `RangeSet`, `Defaults` (config_paths, DEFAULTS), `NodeUtils`

**Purpose:** The flagship data structure — represents sets of cluster
nodes with bracket notation: `node[1-100]`, `rack[1-4]sw[1-48]`.
Supports set operations, group resolution (`@group`), regrouping, and
N-dimensional patterns.

**Architecture:**

**Key classes (3):**

1. **NodeSetBase** (abstract, 650+ lines)
   - Internal: `dict` mapping `pattern_str → RangeSet`
     e.g., `{"node[": RangeSet("1-100"), "sw[": RangeSet("1-48")}`
   - Actually stored as `_patterns: dict[str, RangeSet]` where the
     key is the "prefix" part of the node pattern
   - `autostep` property (inherits from RangeSet)

   **Iteration:**
   - `__iter__()` → iterates individual node names (e.g., "node1", "node2")
   - `nsiter()` → iterates NodeSetBase sub-patterns
   - `contiguous()` → iterates contiguous NodeSet chunks

   **String representation:**
   - `__str__()` → folded string with brackets, e.g., "node[1-4,8]"
   - N-dimensional support via `fold_axis` (from Defaults)

   **Set operations** (same as RangeSet pattern — return new NodeSetBase):
   - `union`, `intersection`, `difference`, `symmetric_difference`
   - `update`, `intersection_update`, `difference_update`, etc.
   - `add`, `remove`, `discard`
   - `issubset`, `issuperset`, `__eq__`, `__lt__`, `__gt__`

   **Indexing:**
   - `__getitem__(index)` — supports int index and slice
   - `_extractslice(index)` — helper for slicing

2. **ParsingEngine** (400 lines)
   - Parses node set strings including extended operations:
     `"node[1-10] ! node[5-8]"` (difference)
   - Operators: `,` (union), `!` (difference), `&` (intersection),
     `^` (symmetric difference)
   - Group resolution: `@group` → calls `GroupResolver.group_nodes()`
   - `@*` → all nodes, `@source:group` → namespaced groups
   - Handles nested brackets and N-dimensional patterns
   - `_scan_string()` is the core parser (120 lines, recursive descent)

3. **NodeSet(NodeSetBase)** (380 lines)
   - Adds parsing (via ParsingEngine), serialization, group awareness
   - `resolver` parameter (default: `std_group_resolver()`)
   - `fromlist(nodelist)`, `fromall(groupsource)` constructors
   - `groups(groupsource)` → find matching groups for this nodeset
   - `regroup(groupsource)` → express nodeset using group notation
   - `split(n)` → split into n roughly equal NodeSets
   - `__getstate__` / `__setstate__` for pickle

**Module-level functions:**
- `expand(pat) → list[str]` — expand pattern to node list
- `fold(pat) → str` — fold node list into pattern
- `grouplist(namespace)` — list available groups
- `std_group_resolver()` → returns module-level resolver singleton
- `set_std_group_resolver(resolver)` — replace the global resolver
- `set_std_group_resolver_config(groupsconf)` — configure from file

**Module-level state:**
- `_STD_GROUP_RESOLVER` — singleton GroupResolverConfig
- `RESOLVER_NOGROUP` — sentinel for "no group resolution"
- `RESOLVER_STD_GROUP` — sentinel for "use default resolver"

**Parsing Grammar (informal):**
```
nodeset_expr = term ((',' | '!' | '&' | '^') term)*
term = group_ref | node_pattern | '(' nodeset_expr ')'
group_ref = '@' [source ':'] group_name
node_pattern = prefix '[' rangeset ']' [suffix] | literal_node
```

**Rust Implementation Notes:**
- This is the most complex module — plan 2000+ lines of Rust
- Internal storage: `BTreeMap<String, RangeSet>` (sorted for deterministic output)
- The ParsingEngine is essentially a recursive descent parser —
  Rust is excellent for this
- Group resolution needs NodeUtils → must implement or stub NodeUtils first
- Consider making `NodeSetBase` a concrete struct (Rust doesn't need the
  base/derived split as much)
- The N-dimensional folding (`fold_axis`) is complex but rarely used —
  can defer to a later pass
- Pickle support → implement `serde::Serialize` / `Deserialize`

---

### Worker

**Files:** `lib/ClusterShell/Worker/` directory (9 files, ~3000 total lines)
- `Worker.py` (684 lines) — base classes
- `EngineClient.py` (570 lines) — fd management + EnginePort
- `Exec.py` (387 lines) — local command execution
- `Tree.py` (598 lines) — tree/gateway-based execution
- `Ssh.py` (164 lines) — SSH command builder
- `Rsh.py` (160 lines) — RSH command builder
- `Pdsh.py` (270 lines) — PDSH (parallel distributed shell)
- `Popen.py` (118 lines) — simple subprocess
- `fastsubprocess.py` (491 lines) — optimized Popen replacement
**Tests:** StreamWorkerTest (10), WorkerExecTest (15), TreeWorkerTest (60)
**Deps:** Engine, NodeSet, Event

**Purpose:** Execute commands on nodes. Workers manage subprocesses,
I/O streams, and event dispatch. This is the execution layer.

**Class Hierarchy:**

```
Worker (base)
├── DistantWorker (adds per-node tracking)
│   ├── ExecWorker (local exec, ExecClient per node)
│   │   ├── WorkerSsh (SSH command builder)
│   │   ├── WorkerRsh (RSH command builder)
│   │   └── WorkerPdsh (parallel RSH)
│   └── TreeWorker (gateway-based tree execution)
└── StreamWorker (generic stream I/O)
    └── WorkerSimple (DEPRECATED compat wrapper)
        └── WorkerPopen (simple subprocess)

EngineBaseTimer (from Engine)
└── EngineClient (fd lifecycle management)
    ├── StreamClient (read/write on streams)
    ├── ExecClient (subprocess management)
    │   ├── SshClient
    │   ├── RshClient
    │   ├── PdshClient
    │   └── CopyClient (scp/rcp)
    └── EnginePort (inter-task messaging)
```

**Key Concepts:**

- **Worker** = user-facing API (submit command, iterate results)
- **EngineClient** = internal, registered with Engine for I/O events
- One Worker may own multiple EngineClients (one per node in ExecWorker)
- StreamClient handles buffered line-based reading with `_readlines()`
- `fastsubprocess.py` is a custom `Popen` with non-blocking I/O

**Worker base class key methods:**
- `_set_task(task)`, `_engine_clients()` → list of EngineClients
- `_on_start(key)`, `_on_close(key, rc)`, `_on_written(key, bytes, sname)`
- `read(node, sname)`, `abort()`, `flush_buffers()`, `flush_errors()`

**DistantWorker additions:**
- Per-node message trees: `_msgtrees[sname]` (MsgTree per stream)
- Per-node return codes: `_rc` dict
- `iter_buffers()`, `iter_errors()`, `iter_retcodes()`
- `node_buffer(node)`, `node_error(node)`, `node_retcode(node)`

**EngineClient lifecycle:**
1. Created by Worker
2. `_start()` — spawn subprocess, set up fd streams
3. Registered with Engine (fd → poll/epoll)
4. Engine calls `_handle_read(sname)` / `_handle_write(sname)`
5. `_close(abort, timeout)` — cleanup, collect exit code

**fastsubprocess.py:**
- Custom `Popen` optimized for non-blocking I/O
- `set_nonblock_flag(fd)` — makes fd non-blocking
- Avoids Python GIL issues with many concurrent subprocesses
- In Rust, use `std::process::Command` + `mio` for non-blocking I/O

**Rust Implementation Notes:**
- This is the second-largest subsystem after NodeSet
- Rust naturally handles non-blocking I/O better — `fastsubprocess` not needed
- Use `std::process::Command` for spawning
- Consider `mio` or `tokio` (if going async) for I/O multiplexing
- The Worker/EngineClient split may not be needed in Rust — consider
  merging or simplifying
- TreeWorker is complex (depends on Propagation, Topology) — defer to Tier 3
- Start with ExecWorker + ExecClient for basic functionality

---

### Communication

**File:** `lib/ClusterShell/Communication.py` (480 lines)
**Tests:** No dedicated tests; tested via TreeWorkerTest, TreeGatewayTest
**Rust stub:** `crates/consortium/src/communication.rs` (24 lines)
**Deps:** `Event` (EventHandler)

**Purpose:** XML-based messaging protocol for inter-gateway communication
in tree mode. Defines message types and the Channel abstraction.

**Architecture:**

1. **XMLReader** (ContentHandler) — SAX parser for incoming messages
   - Parses `<message>` XML elements with type/srcid/etc attributes
   - Reconstructs Message objects from XML
   - Handles base64-encoded data payloads

2. **Channel(EventHandler)** — Bidirectional message channel
   - Wraps a Worker's stdin/stdout as a message transport
   - `initiator: bool` — which side initiated the connection
   - `setup: bool` — handshake complete flag
   - `_recvq: list` — received message queue
   - `_sendq: list` — outgoing message queue
   - `ev_read()` feeds data to XMLReader, dispatches to `recv()`
   - `send(msg)` serializes message as XML to worker's stdin
   - `start()` and `recv(msg)` are abstract — subclassed

3. **Message** hierarchy (10 classes):
   ```
   Message (base)
   ├── ConfigurationMessage     (gateway="...")
   ├── RoutedMessageBase        (srcid=N)  [abstract]
   │   ├── ControlMessage       (action, target, ...)
   │   ├── StdOutMessage        (nodes, output)
   │   ├── StdErrMessage        (inherits StdOutMessage)
   │   ├── RetcodeMessage       (nodes, retcode)
   │   ├── TimeoutMessage       (nodes)
   │   └── RoutingMessage       (event, gateway, targets)
   ├── ACKMessage               (ackid=N)
   ├── ErrorMessage             (err="...")
   ├── StartMessage             (no extra fields)
   └── EndMessage               (no extra fields)
   ```

**Message XML format:**
```xml
<message type="CTL" srcid="1" action="shell" target="node[1-100]">
  <data>base64_encoded_command</data>
</message>
```

**Message.data_encode(inst)** — base64-encodes + pickles Python objects
**Message.data_decode()** — reverse

**Rust Implementation Notes:**
- Replace XML SAX parsing with `quick-xml` crate (serde-based)
- Replace pickle serialization with `serde` + msgpack or bincode
  (BUT must remain wire-compatible if talking to Python gateways)
- If wire compat with Python gateways is needed, must keep XML + pickle
- Channel wraps stdin/stdout of a gateway subprocess
- This module is only used in tree mode (multi-hop execution)

---

## Tier 3 — Topology Layer

---

### Topology

**File:** `lib/ClusterShell/Topology.py` (484 lines)
**Tests:** `tests/TreeTopologyTest.py` (557 lines, 25 tests)
**Rust stub:** `crates/consortium/src/topology.rs` (78 lines)
**Deps:** `NodeSet`

**Purpose:** Defines the network topology of the cluster — which nodes
are gateways, what's behind them. Used to build routing trees for
tree-mode execution.

**Architecture:**

1. **TopologyNodeGroup** — A node in the topology tree
   - `nodeset: NodeSet` (the nodes in this group)
   - `children: list[TopologyNodeGroup]`
   - `parent: Option<TopologyNodeGroup>`
   - `add_child()`, `clear_child()`, `clear_children()`
   - `printable_subtree(prefix)` — ASCII tree visualization

2. **TopologyTree** — Tree of TopologyNodeGroups
   - `root: TopologyNodeGroup`
   - `load(rootnode)` — build from root
   - `find_nodegroup(node)` — find group containing node
   - `inner_node_count()`, `leaf_node_count()`
   - Iterable (depth-first traversal via `TreeIterator`)

3. **TopologyRoute** — A directed edge: source → destination
   - `src_ns: NodeSet`, `dst_ns: NodeSet`
   - `dest(nodeset)` — filter destinations matching nodeset

4. **TopologyRoutingTable** — Collection of routes with validation
   - `add_route(route)` — validates no circular refs or convergent paths
   - `connected(src_ns)` — find routes from source
   - `_introduce_circular_reference()` — checks for loops
   - `_introduce_convergent_paths()` — checks for diamond deps

5. **TopologyGraph** — Directed graph of NodeSet edges
   - `add_route(src_ns, dst_ns)` — add edge
   - `to_tree(root)` — convert to TopologyTree via BFS
   - `_validate(root)` — check all nodes reachable from root

6. **TopologyParser(ConfigParser)** — Reads topology.conf
   - INI format: section = source node pattern, key:value = route entries
   - `tree(root)` — builds TopologyTree from root node
   - Caches built graph

**Config format (topology.conf):**
```ini
[Main]
admin: gw[1-4]
gw1: node[001-064]
gw2: node[065-128]
gw3: node[129-192]
gw4: node[193-256]
```

**Rust Implementation Notes:**
- Straightforward graph/tree algorithms
- Use NodeSet for all node representations
- INI parsing for config (same as Defaults/NodeUtils)
- BFS tree construction from graph is simple
- Validation (no cycles, no convergent paths) is important
- Good candidate for early Tier 3 work (simpler than Propagation)

---

### Propagation

**File:** `lib/ClusterShell/Propagation.py` (424 lines)
**Tests:** No dedicated tests; tested via TreeWorkerTest (60 tests)
**Rust stub:** `crates/consortium/src/propagation.rs` (9 lines)
**Deps:** `Defaults`, `NodeSet`, `Communication`, `Topology`

**Purpose:** Routes commands through the topology tree. Manages
gateway channels and dispatches messages.

**Architecture:**

1. **PropagationTreeRouter** — Route calculator
   - `root: str` (root node name)
   - `fanout: int`
   - `nodes_fanout: dict[NodeSet, int]` — per-destination fanout
   - `table: TopologyRoutingTable`
   - `table_generate(root, topology)` — builds routing table
   - `dispatch(dst)` — assigns nodes to next-hop gateways
   - `next_hop(dst)` — find gateway for destination
   - `mark_unreachable(dst)` — mark nodes as failed
   - `_best_next_hop(candidates)` — picks gateway with lowest load

2. **PropagationChannel(Channel)** — Gateway channel implementation
   - Manages queued commands to a gateway
   - `shell(nodes, command, worker, timeout, stderr, gw_invoke_cmd, remote)`
   - `write(nodes, buf, worker)`, `set_write_eof(nodes, worker)`
   - `recv()` dispatches incoming messages:
     - `recv_cfg(msg)` — configuration handshake
     - `recv_ctl(msg)` — control messages (StdOut, StdErr, Retcode, Timeout)
   - Queuing: `send_queued(ctl)`, `send_dequeue()`
   - Error handling: `ev_hup()`, `ev_close()`

**Dispatch Algorithm:**
Given a target NodeSet, the router walks the topology tree to find
the next-hop gateway. It distributes nodes across gateways respecting
the fanout limit. If a gateway is overloaded, excess nodes queue.

**Rust Implementation Notes:**
- Heavy dependency on other modules (Tier 3)
- The routing algorithm is the core value — focus on correctness
- Channel subclass needs Communication + Worker interaction
- Wire protocol must be compatible if mixed Python/Rust gateways

---

## Tier 4 — Orchestration

---

### Task

**File:** `lib/ClusterShell/Task.py` (1464 lines)
**Tests:** 8 test files with ~100+ test methods total:
  TaskEventTest (20), TaskTimerTest (22), TaskMsgTreeTest (8),
  TaskPortTest (6), TaskRLimitsTest (4), TaskThreadJoinTest (8),
  TaskThreadSuspendTest (5), TaskTimeoutTest (2), TaskLocalMixin (63)
**Rust stub:** `crates/consortium/src/task.rs` (18 lines)
**Deps:** ALL other modules (Defaults, Engine, Event, MsgTree, NodeSet,
  Topology, Propagation, Worker)

**Purpose:** The central orchestrator. A Task owns an Engine, manages
Workers, collects results, and provides the user-facing API.

**Architecture:**

Thread-singleton pattern: one Task per thread (stored in thread-local).

**Key state:**
- `_engine: Engine`
- `_default: dict` (task defaults from Defaults)
- `_info: dict` (runtime info — fanout, timeouts, debug)
- `_msgtrees: dict[sname, MsgTree]` (per-stream message trees)
- `_d_rc: dict[Worker, dict[int, NodeSet]]` (return codes per worker)
- `_d_timeout: dict[Worker, NodeSet]` (timed-out nodes per worker)
- `_topology: TopologyTree` (optional, for tree mode)

**User API (the methods users call):**
```python
task = task_self()
task.shell("uname -r", nodes="node[1-100]")
task.run()
for buf, nodes in task.iter_buffers():
    print(NodeSet.fromlist(nodes), buf)
```

**Key methods:**
- `shell(command, nodes, handler, timeout, ...)` → creates Worker
- `copy(source, dest, nodes)` → file copy Worker
- `run(command, nodes, timeout)` → shell() + resume()
- `resume(timeout)` → enters Engine.run() loop
- `suspend()` → pauses the Engine loop (for interactive use)
- `abort(kill)` → stops all Workers
- `join()` → wait for Workers from other threads
- `timer(delay, handler, interval)` → creates EngineTimer
- `port(handler)` → creates EnginePort for inter-task messaging

**Result accessors:**
- `iter_buffers(match_keys)` → `(msg_bytes, node_list)` from stdout MsgTree
- `iter_errors(match_keys)` → same for stderr
- `iter_retcodes(match_keys)` → `(retcode, node_list)`
- `key_buffer(key)`, `key_error(key)`, `key_retcode(key)`
- `max_retcode()` → highest return code
- `num_timeout()`, `iter_keys_timeout()`

**Tree mode (multi-hop):**
- `_default_tree_is_enabled()` — checks if topology is configured
- `load_topology(topology_file)` — loads TopologyParser
- `_pchannel(gateway, metaworker)` — gets/creates PropagationChannel
- `_pchannel_release()`, `_pchannel_close()` — channel lifecycle

**Threading:**
- `Task.__new__()` implements thread-singleton (one Task per thread)
- `_SuspendCondition` for coordinating suspend/resume across threads
- `wait(from_thread)` — class method to wait for task from another thread

**Defaults integration:**
- `task.default(key)` reads from `_default` dict
- `task.info(key)` reads from `_info` dict
- `task.set_default(key, value)`, `task.set_info(key, value)`
- Special defaults: `local_worker`, `distant_worker` → Worker class refs

**Rust Implementation Notes:**
- This is the integration point — hardest module to port
- Consider implementing a simplified version first (no tree mode)
- Thread-singleton → `thread_local!` macro
- The run loop is just `engine.run(timeout)` — Engine does the work
- Result collection via MsgTree is straightforward
- Tree mode can be deferred to a later milestone

---

### Gateway

**File:** `lib/ClusterShell/Gateway.py` (390 lines)
**Tests:** `tests/TreeGatewayTest.py` (484 lines), `tests/TreeTaskTest.py` (63 lines)
**Rust stub:** `crates/consortium/src/gateway.rs` (7 lines)
**Deps:** Event, NodeSet, Task, Engine, Worker, Communication

**Purpose:** Gateway process entry point. When ClusterShell executes in
tree mode, intermediate nodes run `gateway_main()` which reads commands
from stdin, executes locally, and sends results back via stdout.

**Architecture:**

1. **TreeWorkerResponder(EventHandler)** — Handles local worker events
   - Receives ev_read/ev_hup from local workers
   - Forwards results as StdOut/StdErr/Retcode Messages via GatewayChannel
   - Manages timeout detection and reporting

2. **GatewayChannel(Channel)** — Gateway's communication channel
   - `recv_cfg(msg)` — receives configuration (topology, fanout, etc)
   - `recv_ctl(msg)` — receives control messages:
     - Shell commands → spawns local ExecWorker or sub-gateways
     - Write data → forwards to workers
     - Write EOF → closes input
   - Manages sub-gateway channels for further propagation
   - ACK protocol for flow control

3. **gateway_main()** — Entry point (called as subprocess)
   - Sets up Task, GatewayChannel on stdin/stdout
   - Enters Task.run() loop
   - Custom exception hook for error reporting

**Rust Implementation Notes:**
- This is a standalone binary entry point, not just a library module
- It's the last module needed before tree mode works end-to-end
- Depends on almost everything else
- For initial Rust port, tree mode can be skipped entirely —
  ExecWorker handles direct SSH fine without gateways

---

## Tier 5 — CLI

**Files:** `lib/ClusterShell/CLI/` directory (7 files, ~2900 total lines)
- `Clush.py` (1197 lines) — `clush` command
- `Nodeset.py` (365 lines) — `nodeset` command
- `Clubak.py` (191 lines) — `clubak` command (buffer aggregation)
- `OptionParser.py` (347 lines) — shared CLI option parsing
- `Config.py` (339 lines) — `clush.conf` configuration
- `Display.py` (313 lines) — output formatting
- `Error.py` (122 lines) — error handling/display
- `Utils.py` (51 lines) — utility functions
**Tests:** CLIClushTest (49), CLINodesetTest (51), CLIClubakTest (10+),
  CLIDisplayTest (10+), CLIOptionParserTest (5)
**Deps:** Everything

**Purpose:** Three command-line tools:
- `clush` — parallel shell (the main tool)
- `nodeset` — node set manipulation
- `clubak` — cluster output aggregation

**Not detailed here** — CLI is the last layer and will be ported after
all library modules are complete. It's mostly argument parsing, display
formatting, and wiring to Task/NodeSet.

**Rust Implementation Notes:**
- Use `clap` for argument parsing
- `nodeset` CLI can be ported early (only needs NodeSet)
- `clush` needs Task + full Worker stack
- `clubak` needs MsgTree + NodeSet

---

## Test Mapping Strategy

Total Python tests: 582 (tracked in TEST_MAPPING.toml)

| Module          | Test Files                              | Test Count |
|-----------------|-----------------------------------------|------------|
| RangeSet        | RangeSetTest                            | 57 ✅      |
| MsgTree         | MsgTreeTest                             | 11         |
| Defaults        | DefaultsTest                            | 9          |
| NodeSet         | NodeSetTest, NodeSetGroupTest, etc      | 201+       |
| Topology        | TreeTopologyTest                        | 25         |
| Event           | TaskEventTest (subset)                  | ~10        |
| Engine          | (tested via Task tests)                 | ~30        |
| Worker          | StreamWorkerTest, WorkerExecTest, etc   | ~85        |
| Task            | TaskEventTest, TaskTimerTest, etc       | ~100       |
| Tree            | TreeWorkerTest, TreeGatewayTest         | ~60        |
| CLI             | CLIClushTest, CLINodesetTest, etc       | ~115       |

---

## Dependency Graph

```
          ┌──────────┐
          │   CLI    │ Tier 5
          └────┬─────┘
               │
     ┌─────────┼──────────┐
     ▼         ▼          ▼
 ┌──────┐  ┌───────┐  ┌─────────┐
 │ Task │  │Gateway│  │         │ Tier 4
 └──┬───┘  └───┬───┘  │         │
    │          │      │         │
    ├──────────┤      │         │
    ▼          ▼      ▼         │
┌──────────┐ ┌────────────┐    │
│Propagation│ │   Worker   │    │ Tier 3
│          │ │  (Tree)    │    │
└──┬───────┘ └────┬───────┘    │
   │              │            │
   ├──────┬───────┤            │
   ▼      ▼       ▼            │
┌────┐ ┌──────┐ ┌──────────┐  │
│Topo│ │Comm  │ │Worker    │  │ Tier 2
│logy│ │      │ │(Exec/Ssh)│  │
└──┬─┘ └──┬───┘ └────┬─────┘  │
   │       │          │        │
   ▼       ▼          ▼        │
┌───────┐ ┌─────┐ ┌───────┐   │
│NodeSet│ │Event│ │Engine │   │ Tier 1-2
└───┬───┘ └─────┘ └───────┘   │
    │                          │
    ├──────────────────────────┘
    ▼
┌──────────┐ ┌────────┐ ┌─────────┐
│ RangeSet │ │Defaults│ │NodeUtils│  Tier 0-1
└──────────┘ └────────┘ └─────────┘
```

**Recommended implementation order:**
1. MsgTree (small, well-tested, needed by Task)
2. Event (trivial — just a trait)
3. Defaults (config loading, needed by NodeSet and Task)
4. NodeUtils (group resolution, needed by NodeSet)
5. Engine (I/O loop, needed by Worker and Task)
6. NodeSet (biggest single module, but well-tested)
7. Worker/Exec (basic execution — no tree mode)
8. Topology (routing tree)
9. Communication (wire protocol)
10. Task (orchestration — simplified, no tree mode first)
11. Propagation + TreeWorker + Gateway (tree mode)
12. CLI (final layer)
