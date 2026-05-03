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
                // Register all planned edges as tentative parent→child relationships.
                // If an edge later completes or fails, those events will refine the
                // state. This ensures planned-but-unresolved nodes appear in the tree
                // as Pending rather than being invisible orphans.
                for edge in assignments {
                    let src = edge.src;
                    let tgt = edge.tgt;
                    acc.node(src); // ensure exists
                    {
                        // Only set tentative parent if no parent assigned yet.
                        let n = acc.node(tgt);
                        if n.parent.is_none() {
                            n.parent = Some(src);
                        }
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
                // Register parent on tgt.
                {
                    let n = acc.node(tgt);
                    n.parent = Some(src);
                    n.has_closure = true;
                    n.converged_round = Some(round);
                    n.duration = Some(dur);
                }
            }
            CascadeEvent::EdgeFailed {
                round: _, src, tgt, ..
            } => {
                let src = *src;
                let tgt = *tgt;
                // Still establish topology so failed nodes appear in tree.
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

    /// 5. render_events Tree format includes status glyphs.
    #[test]
    fn render_events_tree_format_includes_status_glyphs() {
        // n3 is in plan but never completed → Pending ⏸
        // n2 → ⚠ (EdgeFailed adds it even without PlanComputed)
        let events_with_pending = vec![
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
                        tgt: node(3),
                    },
                ],
            },
            edge_completed(0, 0, 1, 5), // n1 → Ok ✔
            edge_failed(0, 0, 2),       // n2 → ⚠
            finished(2, 1, 1),
        ];

        let out = render_events(
            &events_with_pending,
            &OutputFormat::Tree {
                max_depth: None,
                color: false,
            },
        );

        assert!(out.contains("✔"), "missing ✔ (Ok glyph): {out}");
        assert!(out.contains("⚠"), "missing ⚠ (Failed glyph): {out}");
        assert!(out.contains("⏸"), "missing ⏸ (Pending glyph): {out}");
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
