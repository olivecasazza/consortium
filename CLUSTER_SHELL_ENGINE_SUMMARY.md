# ClusterShell Engine Abstraction Layer Summary

## 1. Engine Base Class API (Engine.py)

### Class: Engine (Base Class)

**Core Attributes:**
- `info` - Dictionary of configuration info
- `_clients` - Set of registered client objects
- `_ports` - Set of port clients (non-delayable)
- `reg_clifds` - Dict mapping fd -> (EngineClient, EngineClientStream)
- `_reg_stats` - Per-worker registration statistics (for fanout control)
- `_current_loopcnt` - Loop iteration counter for FIFO management
- `_current_stream` - Currently processed stream
- `timerq` - _EngineTimerQ timer queue instance
- `evlooprefcnt` - Event loop reference count (registered clients + timers without autoclose)
- `_prev_fanout` - Cached previous fanout value
- `running` - Boolean running state
- `_exited` - Runloop exit flag

### Core Methods:

| Method | Purpose |
|--------|---------|
| `__init__(self, info)` | Initialize engine with info dict, sets identifier, initializes internal state |
| `release()` | Release engine-specific resources (no-op in base, override in subclasses) |
| `clients()` | Return copy of registered clients set |
| `ports()` | Return copy of ports set |
| `add(client)` | Add client to engine, auto-register if running and within fanout limits |
| `remove(client, abort=False, did_timeout=False)` | Remove client, close its streams, triggers start_clients() |
| `remove_stream(client, stream)` | Flush and unregister a specific stream from a client |
| `clear(did_timeout=False, clear_ports=False)` | Remove all clients/ports |
| `register(client)` | Register a client, set its fd events in the event loop, schedule timeout |
| `unregister(client)` | Unregister client, remove from event loop |
| `unregister_stream(client, stream)` | Internal stream unregistration |
| `modify(client, sname, setmask, clearmask)` | Set interest events (set/clear bits) for a stream |
| `set_events(client, stream)` | Apply modified events to the event loop |
| `set_reading(client, sname)` | Set interest for read events |
| `set_writing(client, sname)` | Set interest for write events |
| `add_timer(timer)` | Schedule a timer |
| `remove_timer(timer)` | Invalidate/remove a timer |
| `fire_timers()` | Fire expired timers |
| `start_ports()` | Start/register port clients |
| `start_clients()` | Start/register clients respecting fanout limits |
| `run(timeout)` | Main entry point: setup -> runloop -> cleanup |
| `runloop(timeout)` | MUST be overridden; drives event loop |
| `snoop_ports()` | Non-blocking read from ports to detect pending messages |
| `abort(kill)` | Abort runloop, clear all clients |
| `exited()` | Returns True if engine has exited runloop |
| `_debug(s)` | Debug logging hook |

### Internal Methods (Subclass Hooks):
- `_fd2client(fd)` - Resolve fd to client (handles fd reuse)
- `_can_register(client)` - Check if client can be registered (fanout check)
- `_update_reg_stats(client, offset)` - Update fanout stats

### Timer System:
- `_EngineTimerQ` - Implements a priority queue of timers using heapq
- `EngineTimer` - Concrete timer class with fire_delay (relative seconds), interval (optional repeat), handler
- Timer operations: schedule, reschedule, invalidate, fire_expired, nextfire_delay, clear

---

## 2. How Select, Poll, and EPoll Engines Differ

### EngineSelect (Platform-agnostic)

**OS Features:** Uses `select()` system call
- Available on almost all UNIX-like systems
- Limits to 1024 file descriptors (FD_SETSIZE default)

**Runloop Mechanism:**
- Maintains two lists: `_fds_r` (read), `_fds_w` (write)
- Calls `select.select(r, w, timeout)` directly
- Processes returned ready FDs in a simple loop

**Event Mask Conversion:**
- Simple: E_READ (0x1) / E_WRITE (0x2) maps directly to interest in r/w lists
- No complex event bits

---

### EnginePoll (Linux/BSD)

**OS Features:** Uses `poll()` system call
- Available on Linux and BSD
- Limited by kernel's max_pollfd (often 10236 or similar)
- No FD limit like select

**Runloop Mechanism:**
- Initializes: `self.polling = select.poll()`
- Calls `polling.poll(timeo * 1000)` (converts seconds to ms)
- Gets list of `(fd, event)` tuples

**Special Event Handling:**
- `POLLIN` - Data available for read
- `POLLOUT` - Writable
- `POLLERR` - Error condition (removes stream)
- `POLLHUP` - Hang-up (removes stream)
- `POLLNVAL` - Invalid fd (raises exception)

**Key differences from select:**
- Handles POLLHUP explicitly (remove_stream)
- Handles POLLERR explicitly (remove_stream)
- POLLNVAL checks before processing

---

### EngineEPoll (Linux)

**OS Features:** Uses `epoll()` system call
- Linux 2.6+
- Scales to millions of FDs (O(1) interest modification)
- Python 2.6+ support

**Runloop Mechanism:**
- Initializes: `self.epolling = select.epoll()`
- Calls `epolling.poll(poll_timeo)` (seconds, not ms)
- Uses `epoll.modify()` semantics

**Special Event Handling:**
- `EPOLLIN` - Data available for read
- `EPOLLOUT` - Writable
- `EPOLLERR` - Error condition (removes stream)
- `EPOLLHUP` - Hang-up (removes stream)

**Key advantage:**
- Supports `EPOLLET` (edge-triggered) if desired
- More efficient for large FD sets

---

### Comparison Summary Table

| Feature | Select | Poll | EPoll |
|---------|--------|------|-------|
| Max FDs | ~1024 | Kernel limit (+) | Millions (+) |
| Complexity | Simple | Moderate | Complex |
| Edge-triggered | Yes | Limited | Yes (+) |
| Level-triggered | Yes | Yes | Yes |
| Interest mod | O(n) | O(1) | O(1) |
| Error/hangup | Manual | Explicit | Explicit |
| OS availability | All Unix | Linux/BSD | Linux |

---

## 3. How the Factory Picks Which Engine to Use

### File: Factory.py

**PreferredEngine metaclass:**

```python
engines = {
    EngineEPoll.identifier: EngineEPoll,   # "epoll"
    EnginePoll.identifier: EnginePoll,     # "poll"
    EngineSelect.identifier: EngineSelect   # "select"
}
```

**Selection Logic:**

1. **Default ("auto") mode:**
   - Tries engines in order: [EPoll, Poll, Select]
   - Attempts to instantiate each engine(info)
   - Returns first one that doesn't raise `EngineNotSupportedError`
   - If all fail: `RuntimeError("FATAL: No supported ClusterShell.Engine found")`

2. **Explicit hint mode:**
   - User specifies engine ID (e.g., "epoll", "poll", "select")
   - Tries to use that specific engine first
   - Falls back through remaining engines if that one is unsupported
   - Uses dict.pop() for hint, dict.popitem() for fallback

3. **Exception:**
   - `EngineNotSupportedError` raised if specific engine requested is unavailable
   - Falls back to next preferred engine on error

**Engine NotSupportedError scenarios:**
- EPoll: AttributeError (epoll object not available), or underlying kernel lacks epoll
- Poll: AttributeError (poll() unavailable)
- Select: Should rarely fail (available nearly everywhere)

---

## 4. OS-Specific Features Each Engine Relies On

### EngineSelect
- **Syscall:** `select()`
- **Constants:** E_READ (0x1), E_WRITE (0x2)
- **Constraints:** FD_SETSIZE limit (~1024)
- **No OS-specific features beyond standard POSIX select**

### EnginePoll
- **Syscall:** `poll()`
- **Constants:** 
  - POLLIN (0x0001)
  - POLLOUT (0x0004)
  - POLLERR (0x0008)
  - POLLHUP (0x0010)
  - POLLNVAL (0x2000)
- **OS availability:** Linux, BSD (not macOS or AIX without poll)
- **No epoll support**

### EngineEPoll
- **Syscall:** `epoll_create/epoll_ctl/epoll_wait` (via Python select.epoll)
- **Constants:**
  - EPOLLIN (0x001)
  - EPOLLOUT (0x004)
  - EPOLLERR (0x008)
  - EPOLLHUP (0x010)
  - EPOLLNVAL (0x2000)
- **OS availability:** Linux 2.6+ (not macOS)
- **Edge-triggered:** Supports epoll edge/level trigger modes

### Python Availability Checks

```python
try:
    self.epolling = select.epoll()
except AttributeError:
    raise EngineNotSupportedError(EngineEPoll.identifier)

try:
    self.polling = select.poll()
except AttributeError:
    raise EngineNotSupportedError(EnginePoll.identifier)
```

---

## 5. Events/Callbacks Supported

### Event Types:

1. **IO Events:**
   - `E_READ` (0x1) / `E_READ` bit in stream.evmask
   - `E_WRITE` (0x2) / `E_WRITE` bit in stream.evmask

2. **Special Poll Events (Poll/EPoll only):**
   - `POLLIN` / `EPOLLIN` - Data available for read
   - `POLLOUT` / `EPOLLOUT` - Socket writable
   - `POLLERR` / `EPOLLERR` - Error (triggers stream removal)
   - `POLLHUP` / `EPOLLHUP` - Hangup (triggers stream removal)
   - `POLLNVAL` / `EPOLLNVAL` - Invalid fd (exception)

### Timer/Timeout Events:

- `EngineTimeoutException` - Task-level timeout exceeded
- Timer callbacks via `ev_timer(timer)` - When timer fires
- Timer queue: `_EngineTimerQ.fire_expired()` processes expired timers
- Each timer has optional interval for autorepeat

### Other Events:

- **Port messages:** Port clients generate internal messages (handled in ports set)
- **Client startup:** Auto-start when added to engine while running
- **Client removal:** Triggers flush/close, then calls start_clients()

### Event Processing Flow:

```
runloop() loop:
  1. Wait for events on interested FDs or timer expiry
  2. Process each ready FD:
     - Handle read/write/hangup/error as appropriate
     - Call client._handle_read() or client._handle_write()
     - On EOF exception: remove_stream()
  3. Check task timeout: raise EngineTimeoutException
  4. Fire timers
  5. Loop until evlooprefcnt == 0
```

### Client Callback Interface:

- `client._handle_read(sname)` - Called when data available
- `client._handle_write(sname)` - Called when writable
- `ev_timer(timer)` - Timer fire (via EngineTimer)

### Stream State Management:

- `stream.events` - Current interest events
- `stream.new_events` - Pending modifications (staged for next apply)
- `stream.evmask` - Allowed event masks
- `client.streams.active_readers/writers()` - Streams of interest

---

## Design Notes for Rust Port

### Core Abstraction Patterns:

1. **Event Loop:** Implement `runloop()` driving event processing
2. **Client registration:** Manage FD->client mapping across FD reuse
3. **Timer queue:** Heap-based priority queue with schedule/reschedule/invalidate
4. **Fanout:** Track registrations per-worker, enforce limits
5. **Reference counting:** evlooprefCnt tracks active registrations

### OS Abstraction Strategy:

1. **Preferred engine hierarchy:** epoll > poll > select
2. **Fallback pattern:** Try best first, fall through to simpler mechanisms
3. **Platform detection:** Check for underlying syscall availability at runtime

### Key Rust Implementation Concerns:

1. `poll()` vs `epoll()`: Prefer epoll for performance, poll for fallback
2. Edge-triggered mode: Enable for EPoll, handle carefully in Poll/Select
3. FD reuse handling: Track reg_epoch to detect when FDs are reassigned
4. Timer integration: Single queue handling both FD timeouts and timers
5. Error handling: PollNVAL, POLLERR, POLLHUP all need explicit handling

### Event Representation:

- Use bitmask enums for event types (READ=1, WRITE=2, ERR, HUP, NVAL)
- Event mask stored per-stream
- Separate pending/new_events tracking for batched updates
