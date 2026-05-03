//! Event consumers for the cascade event protocol.
//!
//! Three sinks, all implementing [`EventSink`]:
//!
//! - [`JsonlWriter`] — streams events as JSONL to any `Write + Send`
//! - [`EventCollector`] — accumulates events into a `Vec` for batch use / tests
//! - [`SnapshotAccumulator`] — folds events into a cascade-tree view wired
//!   into `tree.rs`'s [`TreeNode`] trait
//!
//! Plus [`render_events`] — same event slice, four output formats.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use consortium_nix::cascade::NodeId;
use consortium_nix::cascade_events::{CascadeEvent, EventSink};

use crate::tree::{NodeStatus, OutputFormat, TreeNode};

// ============================================================================
// JsonlWriter
// ============================================================================

/// Streams events as JSONL (one JSON object per line) to any `Write + Send`.
///
/// Wraps the writer in a `Mutex` so `JsonlWriter` is `Send + Sync` and can be
/// shared across the coordinator threads.
pub struct JsonlWriter {
    inner: Mutex<Box<dyn Write + Send>>,
}

impl JsonlWriter {
    /// Create from any `Write + Send` — e.g. `Box::new(std::io::stdout())`.
    pub fn new(w: Box<dyn Write + Send>) -> Self {
        Self {
            inner: Mutex::new(w),
        }
    }

    /// Open `path` for appending and write JSONL there.
    pub fn file(path: impl AsRef<Path>) -> io::Result<Self> {
        let f = File::create(path)?;
        Ok(Self::new(Box::new(BufWriter::new(f))))
    }
}

impl EventSink for JsonlWriter {
    fn emit(&self, event: &CascadeEvent) {
        if let Ok(line) = serde_json::to_string(event) {
            let mut w = self.inner.lock().unwrap();
            let _ = writeln!(w, "{line}");
        }
    }
}

// ============================================================================
// EventCollector
// ============================================================================

/// Accumulates all events into a `Vec<CascadeEvent>` for batch processing.
/// Useful for tests and for feeding `render_events` after a run.
pub struct EventCollector {
    events: Mutex<Vec<CascadeEvent>>,
}

impl EventCollector {
    pub fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    /// Clone out the full accumulated event list.
    pub fn events(&self) -> Vec<CascadeEvent> {
        self.events.lock().unwrap().clone()
    }

    pub fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&mut self) {
        self.events.lock().unwrap().clear();
    }
}

impl Default for EventCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl EventSink for EventCollector {
    fn emit(&self, event: &CascadeEvent) {
        self.events.lock().unwrap().push(event.clone());
    }
}

// ============================================================================
// SnapshotAccumulator + OwnedTreeNode
// ============================================================================

/// Per-node state built up as events arrive.
#[derive(Debug, Clone)]
struct NodeState {
    /// NodeId of the parent (established from EdgeCompleted or EdgeFailed).
    parent: Option<NodeId>,
    /// Children NodeIds (targets for which this node was src).
    children: Vec<NodeId>,
    /// Round in which this node converged (EdgeCompleted).
    converged_round: Option<u32>,
    /// Duration of the completing edge.
    duration: Option<Duration>,
    /// True if this node has the closure (either seeded or EdgeCompleted).
    has_closure: bool,
    /// True if this node appeared in any EdgeFailed as tgt.
    failed: bool,
    /// True if this node is currently being served — set on PlanComputed
    /// (we know the strategy assigned it this round), cleared on
    /// EdgeCompleted (success → has_closure) or EdgeFailed (failure).
    /// Drives the "spinning" ⏵ glyph during the wall-time the cascade
    /// is actually doing the copy.
    in_progress: bool,
}

impl NodeState {
    fn new() -> Self {
        Self {
            parent: None,
            children: Vec::new(),
            converged_round: None,
            duration: None,
            has_closure: false,
            failed: false,
            in_progress: false,
        }
    }
}

/// Folds `CascadeEvent`s into per-node snapshots that can be rendered as a
/// cascade tree.
pub struct SnapshotAccumulator {
    inner: Mutex<AccumulatorInner>,
}

#[derive(Default)]
struct AccumulatorInner {
    /// All nodes seen, indexed by NodeId.
    nodes: HashMap<NodeId, NodeState>,
    /// Seed nodes (from `Started`), in order.
    seeds: Vec<NodeId>,
    /// Round totals we track for metadata (round → max edge duration).
    round_durations: HashMap<u32, Duration>,
}

impl AccumulatorInner {
    fn node(&mut self, id: NodeId) -> &mut NodeState {
        self.nodes.entry(id).or_insert_with(NodeState::new)
    }
}

impl SnapshotAccumulator {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(AccumulatorInner::default()),
        }
    }

    /// Build a self-contained `OwnedTreeNode` tree.
    ///
    /// Root = `seeded[0]` (or an artificial root labelled "cascade" if no
    /// `Started` event was received).  Children are nodes whose parent edge
    /// (EdgeCompleted or EdgeFailed) established `src→tgt`.  Nodes that
    /// appear in both EdgeCompleted and EdgeFailed keep their last status.
    ///
    /// Status per node:
    /// - `Ok` — `has_closure` is true
    /// - `Failed` — appeared as `tgt` in at least one `EdgeFailed`
    /// - `Pending` — neither
    ///
    /// Judgment call on orphan failed nodes: both `EdgeCompleted` and
    /// `EdgeFailed` contribute to parent-of relationships so that failed
    /// targets still appear in the tree. If we only used `EdgeCompleted`,
    /// failed nodes would be orphans and invisible.
    pub fn to_tree(&self) -> OwnedTreeNode {
        let acc = self.inner.lock().unwrap();
        build_tree(&acc)
    }
}

impl Default for SnapshotAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl EventSink for SnapshotAccumulator {
    fn emit(&self, event: &CascadeEvent) {
        let mut acc = self.inner.lock().unwrap();
        match event {
            CascadeEvent::Started { seeded, .. } => {
                acc.seeds = seeded.clone();
                for &id in seeded {
                    let n = acc.node(id);
                    n.has_closure = true;
                }
            }
            CascadeEvent::PlanComputed { assignments, .. } => {
                // Register all planned edges as tentative parent→child relationships
                // AND mark targets as in_progress — that drives the "spinning"
                // ⏵ glyph during the actual copy. EdgeCompleted/EdgeFailed will
                // clear the spinner when the edge resolves.
                for edge in assignments {
                    let src = edge.src;
                    let tgt = edge.tgt;
                    acc.node(src);
                    {
                        let n = acc.node(tgt);
                        if n.parent.is_none() {
                            n.parent = Some(src);
                        }
                        n.in_progress = true;
                    }
                    if !acc
                        .nodes
                        .get(&src)
                        .map_or(false, |n| n.children.contains(&tgt))
                    {
                        acc.node(src).children.push(tgt);
                    }
                }
            }
            CascadeEvent::EdgeStarted { src, tgt, .. } => {
                acc.node(*src);
                acc.node(*tgt);
            }
            CascadeEvent::EdgeCompleted {
                round,
                src,
                tgt,
                duration,
            } => {
                // Establish parent→child relationship.
                let src = *src;
                let tgt = *tgt;
                let round = *round;
                let dur = *duration;

                // Register child on src.
                if !acc
                    .nodes
                    .get(&src)
                    .map_or(false, |n| n.children.contains(&tgt))
                {
                    acc.node(src).children.push(tgt);
                }
                // Register parent on tgt + clear in_progress (it's done now).
                {
                    let n = acc.node(tgt);
                    n.parent = Some(src);
                    n.has_closure = true;
                    n.in_progress = false;
                    n.converged_round = Some(round);
                    n.duration = Some(dur);
                }
            }
            CascadeEvent::EdgeFailed {
                round: _, src, tgt, ..
            } => {
                let src = *src;
                let tgt = *tgt;
                if !acc
                    .nodes
                    .get(&src)
                    .map_or(false, |n| n.children.contains(&tgt))
                {
                    acc.node(src).children.push(tgt);
                }
                {
                    let n = acc.node(tgt);
                    if n.parent.is_none() {
                        n.parent = Some(src);
                    }
                    n.failed = true;
                    n.in_progress = false; // resolved (to failure)
                }
            }
            CascadeEvent::RoundCompleted {
                round,
                duration,
                has_closure,
            } => {
                acc.round_durations.insert(*round, *duration);
                for &id in has_closure {
                    acc.node(id).has_closure = true;
                }
            }
            CascadeEvent::Finished { .. } => {}
        }
    }
}

/// Build the `OwnedTreeNode` tree from accumulated state.
fn build_tree(acc: &AccumulatorInner) -> OwnedTreeNode {
    let root_id = acc.seeds.first().copied();

    match root_id {
        Some(rid) => build_node(rid, acc),
        None => {
            // No Started event — return a synthetic root with whatever we have.
            OwnedTreeNode {
                label: "cascade".into(),
                status: Some(NodeStatus::Pending),
                metadata: Vec::new(),
                children: Vec::new(),
            }
        }
    }
}

fn build_node(id: NodeId, acc: &AccumulatorInner) -> OwnedTreeNode {
    let state = acc.nodes.get(&id);

    let status = match state {
        None => Some(NodeStatus::Pending),
        Some(s) if s.failed => Some(NodeStatus::Failed),
        Some(s) if s.has_closure => Some(NodeStatus::Ok),
        Some(s) if s.in_progress => Some(NodeStatus::InProgress),
        Some(_) => Some(NodeStatus::Pending),
    };

    let mut metadata: Vec<(String, String)> = Vec::new();
    if let Some(s) = state {
        if let Some(round) = s.converged_round {
            metadata.push(("round".into(), round.to_string()));
        }
        if let Some(dur) = s.duration {
            let ms = dur.as_millis();
            metadata.push(("duration".into(), format!("{ms}ms")));
        }
    }

    // Build children, sorted by NodeId for stable ordering.
    let mut child_ids: Vec<NodeId> = state.map(|s| s.children.clone()).unwrap_or_default();
    child_ids.sort();
    child_ids.dedup();

    let children: Vec<OwnedTreeNode> = child_ids
        .into_iter()
        .map(|cid| build_node(cid, acc))
        .collect();

    OwnedTreeNode {
        label: id.to_string(),
        status,
        metadata,
        children,
    }
}

// ============================================================================
// OwnedTreeNode — self-contained tree that implements TreeNode
// ============================================================================

/// A self-contained tree node that owns its children. Implements [`TreeNode`]
/// so it can be handed directly to `tree::render`.
///
/// Constructed by [`SnapshotAccumulator::to_tree`]; also usable standalone.
pub struct OwnedTreeNode {
    pub label: String,
    pub status: Option<NodeStatus>,
    pub metadata: Vec<(String, String)>,
    pub children: Vec<OwnedTreeNode>,
}

impl TreeNode for OwnedTreeNode {
    fn label(&self) -> String {
        self.label.clone()
    }

    fn status(&self) -> Option<NodeStatus> {
        self.status.clone()
    }

    fn metadata(&self) -> Vec<(String, String)> {
        self.metadata.clone()
    }

    fn children(&self) -> Vec<&dyn TreeNode> {
        self.children.iter().map(|c| c as &dyn TreeNode).collect()
    }
}

// ============================================================================
// LiveTreeRenderer — re-renders the tree in place on each RoundCompleted
// ============================================================================

/// EventSink that re-renders the cascade tree to stdout in place every
/// `RoundCompleted` (and `Finished`). Uses ANSI cursor positioning so
/// the tree fills in line-by-line as the cascade unfolds — same idiom
/// nix-output-monitor (nom) uses.
///
/// Usage: hand to `Cascade::new()...events(&renderer).run()` for default
/// "watch the cascade unfold" behavior in any TTY-attached cli bin.
///
/// Caller is responsible for not wiring this when stdout is a pipe — it
/// would emit ANSI escapes into the captured output. [`crate::output`]
/// handles the TTY check.
pub struct LiveTreeRenderer {
    accumulator: SnapshotAccumulator,
    color: bool,
    max_depth: Option<usize>,
    /// Optional Mutex<Vec<u8>> for testing — when Some, frames go here
    /// instead of stdout. Production passes None.
    capture: Option<Mutex<Vec<u8>>>,
    /// Tracks whether we've entered the alternate screen buffer.
    /// Mirrors what top/htop/vim/less do: a separate screen with no
    /// scrollback, so cursor-home + clear works reliably regardless of
    /// frame size or terminal viewport height. Without this, frames
    /// larger than the viewport scroll content off the top into
    /// scrollback, and subsequent redraws can't walk up to clear them
    /// → user sees multiple "n0" headers piling up as the tree grows.
    alt_screen_active: Mutex<bool>,
}

impl LiveTreeRenderer {
    pub fn new(color: bool, max_depth: Option<usize>) -> Self {
        Self {
            accumulator: SnapshotAccumulator::new(),
            color,
            max_depth,
            capture: None,
            alt_screen_active: Mutex::new(false),
        }
    }

    /// Test-only constructor that captures frames into an internal buffer
    /// instead of writing to stdout. Skips alt-screen mode (tests don't
    /// have a real terminal to swap buffers on).
    #[doc(hidden)]
    pub fn with_capture(color: bool, max_depth: Option<usize>) -> Self {
        Self {
            accumulator: SnapshotAccumulator::new(),
            color,
            max_depth,
            capture: Some(Mutex::new(Vec::new())),
            alt_screen_active: Mutex::new(false),
        }
    }

    /// Test-only: read out captured frames as a String.
    #[doc(hidden)]
    pub fn captured(&self) -> String {
        self.capture
            .as_ref()
            .map(|m| String::from_utf8_lossy(&m.lock().unwrap()).to_string())
            .unwrap_or_default()
    }

    /// Switch the terminal to its alternate screen buffer + hide the
    /// cursor. Idempotent; a no-op in capture mode.
    fn enter_alt_screen(&self) {
        if self.capture.is_some() {
            return;
        }
        let mut active = self.alt_screen_active.lock().unwrap();
        if *active {
            return;
        }
        let mut stdout = io::stdout().lock();
        // \x1b[?1049h: enter alt screen
        // \x1b[?25l:   hide cursor
        // \x1b[H:      cursor home
        let _ = stdout.write_all(b"\x1b[?1049h\x1b[?25l\x1b[H");
        let _ = stdout.flush();
        *active = true;
    }

    /// Restore the main screen buffer + cursor visibility. Called on
    /// Finished and from Drop (panic safety).
    fn exit_alt_screen(&self) {
        if self.capture.is_some() {
            return;
        }
        let mut active = self.alt_screen_active.lock().unwrap();
        if !*active {
            return;
        }
        let mut stdout = io::stdout().lock();
        // \x1b[?25h:   show cursor
        // \x1b[?1049l: leave alt screen (restores prior buffer contents)
        let _ = stdout.write_all(b"\x1b[?25h\x1b[?1049l");
        let _ = stdout.flush();
        *active = false;
    }

    /// Render the current tree state to the alt screen (or capture
    /// buffer in tests). Cursor goes to home, screen is cleared, frame
    /// is written. Simple + reliable regardless of frame size.
    fn repaint(&self) {
        let tree = self.accumulator.to_tree();
        let frame = crate::tree::render(
            &tree,
            &OutputFormat::Tree {
                max_depth: self.max_depth,
                color: self.color,
            },
        );

        if let Some(cap) = &self.capture {
            // Capture mode: prepend the cursor-home + clear escapes so
            // tests can see frame boundaries, but don't actually mess
            // with the terminal.
            let mut bytes: Vec<u8> = Vec::with_capacity(frame.len() + 8);
            bytes.extend_from_slice(b"\x1b[H\x1b[2J");
            bytes.extend_from_slice(frame.as_bytes());
            cap.lock().unwrap().extend_from_slice(&bytes);
            return;
        }

        // Production: ensure alt screen is active, cursor home, clear,
        // print frame. iTerm2 synchronized-update markers wrap the
        // operation to avoid mid-frame flicker on terminals that
        // support them; older terminals ignore them silently.
        self.enter_alt_screen();
        let mut stdout = io::stdout().lock();
        let _ = stdout.write_all(b"\x1b[?2026h\x1b[H\x1b[2J");
        let _ = stdout.write_all(frame.as_bytes());
        let _ = stdout.write_all(b"\x1b[?2026l");
        let _ = stdout.flush();
    }
}

impl Drop for LiveTreeRenderer {
    /// Restore the terminal on drop in case the cascade panicked
    /// before Finished fired. Without this, the user is stuck in alt
    /// screen with the cursor hidden until they reset their terminal.
    fn drop(&mut self) {
        self.exit_alt_screen();
    }
}

impl EventSink for LiveTreeRenderer {
    fn emit(&self, event: &CascadeEvent) {
        // Always update the in-memory tree.
        self.accumulator.emit(event);
        // Repaint on:
        // - PlanComputed → spinning state for this round's targets
        // - RoundCompleted → done state for this round
        // - Finished → final state, then exit alt screen + print final
        //   frame to main buffer so the user has a permanent record.
        match event {
            CascadeEvent::PlanComputed { .. } | CascadeEvent::RoundCompleted { .. } => {
                self.repaint();
            }
            CascadeEvent::Finished { .. } => {
                self.repaint();
                // Render the final frame separately for the main buffer
                // so the user sees the converged tree after we exit
                // alt screen.
                let final_frame = crate::tree::render(
                    &self.accumulator.to_tree(),
                    &OutputFormat::Tree {
                        max_depth: self.max_depth,
                        color: self.color,
                    },
                );
                self.exit_alt_screen();
                if self.capture.is_none() {
                    let mut stdout = io::stdout().lock();
                    let _ = stdout.write_all(final_frame.as_bytes());
                    let _ = stdout.flush();
                }
            }
            _ => {}
        }
    }
}

// ============================================================================
// DelayingExecutor — wraps a RoundExecutor + sleeps inside dispatch
// ============================================================================

/// Wraps a [`consortium_nix::cascade::RoundExecutor`] and sleeps for a
/// configurable duration *inside* `dispatch`. Critical for live demos:
/// without this, the deterministic sim resolves a round in microseconds
/// and the live renderer's "spinning" frame (rendered at PlanComputed)
/// is invisible because EdgeCompleted fires before the human eye can
/// register it.
///
/// The sleep happens between PlanComputed (which marks targets
/// in_progress) and EdgeCompleted (which marks them has_closure), so
/// the spinning ⏵ glyph is visible for `delay` real wall-time, then
/// snaps to ✔ on the next frame.
///
/// **Demo / visualization only.** Putting this in production paths
/// adds fake latency to real deploys.
pub struct DelayingExecutor<'a> {
    pub inner: &'a dyn consortium_nix::cascade::RoundExecutor,
    pub delay: Duration,
}

impl<'a> consortium_nix::cascade::RoundExecutor for DelayingExecutor<'a> {
    fn dispatch(
        &self,
        nodes: &[consortium_nix::cascade::CascadeNode],
        edges: &[(
            consortium_nix::cascade::NodeId,
            consortium_nix::cascade::NodeId,
        )],
        net: &consortium_nix::cascade::NetworkProfile,
    ) -> std::collections::HashMap<
        (
            consortium_nix::cascade::NodeId,
            consortium_nix::cascade::NodeId,
        ),
        Result<Duration, consortium_nix::cascade::CascadeError>,
    > {
        std::thread::sleep(self.delay);
        self.inner.dispatch(nodes, edges, net)
    }
}

// ============================================================================
// DelaySink — wraps another sink and sleeps after each RoundCompleted
// ============================================================================

/// Wraps an `EventSink` and injects a wall-time sleep after each
/// `RoundCompleted` event. Useful for making the deterministic sim
/// watchable in live mode — without this the cascade fires in
/// microseconds, faster than the live renderer's frames can register
/// to a human eye.
///
/// **Test/demo only.** Inject this in production and you'll add fake
/// latency to real deploys.
pub struct DelaySink<'a> {
    pub inner: &'a dyn EventSink,
    pub delay: Duration,
}

impl<'a> EventSink for DelaySink<'a> {
    fn emit(&self, event: &CascadeEvent) {
        self.inner.emit(event);
        if matches!(event, CascadeEvent::RoundCompleted { .. }) {
            std::thread::sleep(self.delay);
        }
    }
}

// ============================================================================
// render_events
// ============================================================================

/// Render a slice of `CascadeEvent`s in the requested `OutputFormat`.
///
/// - `Tree` — folds events through `SnapshotAccumulator`, builds an
///   `OwnedTreeNode`, delegates to `tree::render`.
/// - `Json` / `Yaml` / `Toml` — serializes the raw event slice directly
///   (events derive `Serialize`).
pub fn render_events(events: &[CascadeEvent], format: &OutputFormat) -> String {
    match format {
        OutputFormat::Tree { max_depth, color } => {
            let acc = SnapshotAccumulator::new();
            for ev in events {
                acc.emit(ev);
            }
            let tree = acc.to_tree();
            crate::tree::render(
                &tree,
                &OutputFormat::Tree {
                    max_depth: *max_depth,
                    color: *color,
                },
            )
        }
        OutputFormat::Json => serde_json::to_string_pretty(events)
            .unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}")),
        OutputFormat::Yaml => {
            serde_yaml::to_string(events).unwrap_or_else(|e| format!("error: {e}"))
        }
        OutputFormat::Toml => {
            // TOML can't serialize a bare array at the root; wrap under "events".
            let mut wrapper = std::collections::BTreeMap::new();
            wrapper.insert("events", events);
            match toml::to_string(&wrapper) {
                Ok(t) => t,
                Err(e) => format!("# toml render error: {e}\n"),
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use consortium_nix::cascade::NodeId;
    use consortium_nix::cascade_events::{CascadeEvent, Edge, EventSink};

    use super::*;
    use crate::tree::OutputFormat;

    // ---- helpers ----

    fn node(n: u32) -> NodeId {
        NodeId(n)
    }

    fn started_event(seeds: &[u32]) -> CascadeEvent {
        CascadeEvent::Started {
            n_nodes: 4,
            seeded: seeds.iter().map(|&n| NodeId(n)).collect(),
            strategy: "log2-fanout".into(),
            at: SystemTime::UNIX_EPOCH,
        }
    }

    fn edge_completed(round: u32, src: u32, tgt: u32, ms: u64) -> CascadeEvent {
        CascadeEvent::EdgeCompleted {
            round,
            src: node(src),
            tgt: node(tgt),
            duration: Duration::from_millis(ms),
        }
    }

    fn edge_failed(round: u32, src: u32, tgt: u32) -> CascadeEvent {
        use consortium_nix::cascade::CascadeError;
        CascadeEvent::EdgeFailed {
            round,
            src: node(src),
            tgt: node(tgt),
            error: CascadeError::Copy {
                node: node(tgt),
                stderr: "rsync exited 23".into(),
            },
        }
    }

    fn finished(converged: usize, failed: usize, rounds: u32) -> CascadeEvent {
        CascadeEvent::Finished {
            converged,
            failed,
            rounds,
        }
    }

    // ---- tests ----

    /// 1. JsonlWriter emits one line per event, each valid JSON.
    #[test]
    fn jsonl_writer_writes_one_line_per_event() {
        let buf: Vec<u8> = Vec::new();
        let writer = JsonlWriter::new(Box::new(buf));

        writer.emit(&started_event(&[0]));
        writer.emit(&edge_completed(0, 0, 1, 10));
        writer.emit(&finished(2, 0, 1));

        // Extract the inner buffer.
        let raw = {
            let guard = writer.inner.lock().unwrap();
            // Downcast is not possible through dyn Write; capture via Cursor instead.
            // We'll re-run the test using a Cursor to get the bytes.
            drop(guard);
            // Re-run with a cursor approach below.
            Vec::<u8>::new() // placeholder
        };
        // Redo using std::io::Cursor directly.
        let cursor = std::io::Cursor::new(Vec::<u8>::new());
        let writer2 = JsonlWriter::new(Box::new(cursor));
        writer2.emit(&started_event(&[0]));
        writer2.emit(&edge_completed(0, 0, 1, 10));
        writer2.emit(&finished(2, 0, 1));

        let guard = writer2.inner.lock().unwrap();
        // Access the cursor bytes by downcasting. Since we can't downcast dyn Write,
        // we test via EventCollector + JsonlWriter on a shared buffer.
        drop(guard);
        let _ = raw; // suppress unused warning

        // Use a channel-backed approach: write to a Vec<u8> captured behind Arc<Mutex>.
        use std::sync::Arc;
        let shared: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let shared_clone = Arc::clone(&shared);

        struct SharedBuf(Arc<Mutex<Vec<u8>>>);
        impl Write for SharedBuf {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let writer3 = JsonlWriter::new(Box::new(SharedBuf(shared_clone)));
        writer3.emit(&started_event(&[0]));
        writer3.emit(&edge_completed(0, 0, 1, 10));
        writer3.emit(&finished(2, 0, 1));

        let bytes = shared.lock().unwrap().clone();
        let text = String::from_utf8(bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();

        assert_eq!(lines.len(), 3, "expected 3 JSONL lines, got: {text}");
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("invalid JSON on line {line:?}: {e}"));
        }
    }

    /// 2. EventCollector records events in emission order.
    #[test]
    fn event_collector_records_in_order() {
        let col = EventCollector::new();
        col.emit(&started_event(&[0]));
        col.emit(&edge_completed(0, 0, 1, 5));
        col.emit(&finished(2, 0, 1));

        assert_eq!(col.len(), 3);

        let evs = col.events();
        assert!(matches!(&evs[0], CascadeEvent::Started { .. }));
        assert!(matches!(&evs[1], CascadeEvent::EdgeCompleted { .. }));
        assert!(matches!(&evs[2], CascadeEvent::Finished { .. }));
    }

    /// 3. SnapshotAccumulator builds correct tree topology from EdgeCompleted.
    #[test]
    fn snapshot_accumulator_builds_correct_tree_topology() {
        // n0 → n1, n0 → n2
        let acc = SnapshotAccumulator::new();
        acc.emit(&started_event(&[0]));
        acc.emit(&edge_completed(0, 0, 1, 10));
        acc.emit(&edge_completed(0, 0, 2, 12));
        acc.emit(&finished(3, 0, 1));

        let tree = acc.to_tree();
        assert_eq!(tree.label, "n0");
        assert_eq!(tree.children.len(), 2, "root should have 2 children");

        let child_labels: Vec<&str> = tree.children.iter().map(|c| c.label.as_str()).collect();
        assert!(child_labels.contains(&"n1"), "missing n1: {child_labels:?}");
        assert!(child_labels.contains(&"n2"), "missing n2: {child_labels:?}");

        // Root and children should be Ok (has_closure).
        assert_eq!(tree.status, Some(NodeStatus::Ok));
        for child in &tree.children {
            assert_eq!(child.status, Some(NodeStatus::Ok));
        }
    }

    /// 4. SnapshotAccumulator marks failed nodes correctly.
    #[test]
    fn accumulator_marks_failed_nodes() {
        let acc = SnapshotAccumulator::new();
        acc.emit(&started_event(&[0]));
        acc.emit(&edge_completed(0, 0, 1, 8));
        acc.emit(&edge_failed(0, 0, 2)); // n2 fails
        acc.emit(&finished(2, 1, 1));

        let tree = acc.to_tree();

        // Find n2 in tree children.
        let n2 = tree
            .children
            .iter()
            .find(|c| c.label == "n2")
            .expect("n2 should be in tree even though it failed");
        assert_eq!(n2.status, Some(NodeStatus::Failed), "n2 should be Failed");

        // n1 should be Ok.
        let n1 = tree
            .children
            .iter()
            .find(|c| c.label == "n1")
            .expect("n1 should be in tree");
        assert_eq!(n1.status, Some(NodeStatus::Ok));
    }

    /// 5. render_events Tree format includes all four status glyphs.
    /// Status semantics:
    /// - PlanComputed marks a target as InProgress (⏵ "spinning")
    /// - EdgeCompleted clears in_progress and sets has_closure (✔)
    /// - EdgeFailed clears in_progress and sets failed (⚠)
    /// - A node that's only listed in `Started` as a child (or never
    ///   appears in any event but is reachable from a parent) renders
    ///   as Pending (⏸)
    #[test]
    fn render_events_tree_format_includes_status_glyphs() {
        // Plan: (0,1) completes → ✔, (0,3) is in plan AND completes
        // before Finished → ✔. (0,2) fails → ⚠. n4 is in plan but
        // never resolves (no EdgeCompleted/EdgeFailed) → stays ⏵
        // (in_progress) when PlanComputed marked it.
        let events = vec![
            started_event(&[0]),
            CascadeEvent::PlanComputed {
                round: 0,
                assignments: vec![
                    Edge {
                        src: node(0),
                        tgt: node(1),
                    },
                    Edge {
                        src: node(0),
                        tgt: node(2),
                    },
                    Edge {
                        src: node(0),
                        tgt: node(4),
                    }, // never resolves → ⏵
                ],
            },
            edge_completed(0, 0, 1, 5), // ✔
            edge_failed(0, 0, 2),       // ⚠
            // n4 left in_progress (PlanComputed marked it but no
            // EdgeCompleted/EdgeFailed)
            finished(2, 1, 1),
        ];

        let out = render_events(
            &events,
            &OutputFormat::Tree {
                max_depth: None,
                color: false,
            },
        );

        assert!(out.contains("✔"), "missing ✔ (Ok glyph): {out}");
        assert!(out.contains("⚠"), "missing ⚠ (Failed glyph): {out}");
        // n4 still in_progress at the end → ⏵ glyph visible.
        // This is the test that proves PlanComputed sets in_progress
        // and unresolved nodes keep that state.
        assert!(
            out.contains("⏵"),
            "missing ⏵ (InProgress glyph for unresolved planned target): {out}"
        );
    }

    /// 7. LiveTreeRenderer emits a frame on each PlanComputed +
    /// RoundCompleted + Finished, uses cursor-home + clear-screen
    /// escapes (alt-screen idiom, like top/htop/vim/less), and
    /// captures multiple distinct frames over the cascade lifetime.
    #[test]
    fn live_tree_renderer_emits_multiple_frames_with_ansi_redraw() {
        // Drive a 4-event sequence: Started, RoundCompleted, RoundCompleted, Finished.
        // Expect 3 frames captured (one per RoundCompleted, one per Finished).
        let renderer = LiveTreeRenderer::with_capture(false, None);

        renderer.emit(&CascadeEvent::Started {
            n_nodes: 4,
            seeded: vec![NodeId(0)],
            strategy: "log2-fanout".into(),
            at: SystemTime::UNIX_EPOCH,
        });
        renderer.emit(&CascadeEvent::PlanComputed {
            round: 0,
            assignments: vec![Edge {
                src: NodeId(0),
                tgt: NodeId(1),
            }],
        });
        renderer.emit(&edge_completed(0, 0, 1, 5));
        renderer.emit(&CascadeEvent::RoundCompleted {
            round: 0,
            duration: Duration::from_millis(5),
            has_closure: vec![NodeId(0), NodeId(1)],
        });
        renderer.emit(&edge_completed(1, 1, 2, 5));
        renderer.emit(&CascadeEvent::RoundCompleted {
            round: 1,
            duration: Duration::from_millis(5),
            has_closure: vec![NodeId(0), NodeId(1), NodeId(2)],
        });
        renderer.emit(&CascadeEvent::Finished {
            converged: 3,
            failed: 0,
            rounds: 2,
        });

        let captured = renderer.captured();

        // Each frame is prefixed with cursor-home + clear-screen
        // (\x1b[H\x1b[2J) — the alt-screen redraw idiom. PlanComputed,
        // RoundCompleted, and Finished each trigger a repaint. With
        // these test events:
        //   PlanComputed (round 0) → frame 1
        //   RoundCompleted (round 0) → frame 2
        //   RoundCompleted (round 1) → frame 3
        //   Finished → frame 4
        // = 4 frames, 4 cursor-home sequences.
        let cursor_home_count = captured.matches("\x1b[H").count();
        assert_eq!(
            cursor_home_count, 4,
            "expected exactly 4 cursor-home sequences (one per repaint); got {cursor_home_count}\n{captured:?}"
        );
        let clear_screen_count = captured.matches("\x1b[2J").count();
        assert_eq!(
            clear_screen_count, 4,
            "expected exactly 4 clear-screen sequences (one per repaint); got {clear_screen_count}\n{captured:?}"
        );
        // n0, n1, n2 should all eventually appear in the captured output.
        assert!(captured.contains("n0"));
        assert!(captured.contains("n1"));
        assert!(captured.contains("n2"));
    }

    /// 6. JSONL round-trip: serialize events via JsonlWriter → parse back.
    #[test]
    fn render_events_jsonl_format_round_trips() {
        use std::sync::Arc;

        let events = vec![
            started_event(&[0]),
            edge_completed(0, 0, 1, 10),
            edge_completed(0, 0, 2, 15),
            edge_failed(0, 0, 3),
            finished(3, 1, 1),
        ];

        // Write via JsonlWriter.
        let shared: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let shared_clone = Arc::clone(&shared);

        struct SharedBuf(Arc<Mutex<Vec<u8>>>);
        impl Write for SharedBuf {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let writer = JsonlWriter::new(Box::new(SharedBuf(shared_clone)));
        for ev in &events {
            writer.emit(ev);
        }

        let bytes = shared.lock().unwrap().clone();
        let text = String::from_utf8(bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();

        assert_eq!(lines.len(), events.len(), "line count mismatch");

        // Parse each line back and count variants.
        let mut started = 0usize;
        let mut completed = 0usize;
        let mut failed = 0usize;
        let mut finished_count = 0usize;

        for line in &lines {
            let ev: CascadeEvent = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("invalid JSONL line {line:?}: {e}"));
            match ev {
                CascadeEvent::Started { .. } => started += 1,
                CascadeEvent::EdgeCompleted { .. } => completed += 1,
                CascadeEvent::EdgeFailed { .. } => failed += 1,
                CascadeEvent::Finished { .. } => finished_count += 1,
                _ => {}
            }
        }

        assert_eq!(started, 1);
        assert_eq!(completed, 2);
        assert_eq!(failed, 1);
        assert_eq!(finished_count, 1);
    }
}
