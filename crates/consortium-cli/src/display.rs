//! Shared output formatting for consortium CLI tools.
//!
//! Provides nh-inspired progress reporting and output formatting.

use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use console::style;
use indicatif::{ProgressBar, ProgressStyle};

use consortium::worker::EventHandler;

// ── Progress tracking ───────────────────────────────────────────────────────

/// Tracks completed/failed node counts. Shared between ProgressHandler and caller.
#[derive(Clone)]
pub struct ProgressState {
    completed: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
    total: usize,
}

impl ProgressState {
    pub fn new(total: usize) -> Self {
        Self {
            completed: Arc::new(AtomicUsize::new(0)),
            failed: Arc::new(AtomicUsize::new(0)),
            total,
        }
    }

    pub fn completed(&self) -> usize {
        self.completed.load(Ordering::Relaxed)
    }

    pub fn failed(&self) -> usize {
        self.failed.load(Ordering::Relaxed)
    }

    pub fn total(&self) -> usize {
        self.total
    }
}

/// EventHandler that drives an indicatif ProgressBar on each node completion.
///
/// Pass as `user_handler` to `task.schedule()`. The TaskGatheringHandler
/// chains `on_close`/`on_timeout` to us — no core changes needed.
///
/// ProgressBar is internally Arc'd, so updates from the poll thread are
/// immediately visible to the steady-tick render thread.
pub struct ProgressHandler {
    state: ProgressState,
    bar: ProgressBar,
}

impl ProgressHandler {
    pub fn new(state: ProgressState, bar: ProgressBar) -> Self {
        Self { state, bar }
    }
}

impl EventHandler for ProgressHandler {
    fn on_close(&mut self, _node: &str, rc: i32) {
        if rc != 0 {
            self.state.failed.fetch_add(1, Ordering::Relaxed);
        }
        let done = self.state.completed.fetch_add(1, Ordering::Relaxed) + 1;
        self.bar.set_position(done as u64);

        let failed = self.state.failed();
        if failed > 0 {
            self.bar
                .set_message(format!("({} failed)", style(failed).red()));
        }
    }

    fn on_timeout(&mut self, _node: &str) {
        self.state.failed.fetch_add(1, Ordering::Relaxed);
        let done = self.state.completed.fetch_add(1, Ordering::Relaxed) + 1;
        self.bar.set_position(done as u64);
        self.bar
            .set_message(format!("({} failed)", style(self.state.failed()).red()));
    }
}

/// Create an nh-style progress bar + state for cluster operations.
///
/// Returns (ProgressBar, ProgressState, ProgressHandler). Pass the handler
/// to `task.schedule(..., Some(Box::new(handler)), ...)`.
pub fn create_progress(total: usize) -> (ProgressBar, ProgressState, ProgressHandler) {
    let state = ProgressState::new(total);
    let bar = ProgressBar::new(total as u64);

    bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{bar:30.cyan/dim}] {pos}/{len} nodes {msg} ({elapsed})",
        )
        .unwrap()
        .progress_chars("━╸─"),
    );
    bar.enable_steady_tick(Duration::from_millis(80));

    let handler = ProgressHandler::new(state.clone(), bar.clone());
    (bar, state, handler)
}

/// Finish the progress bar with a summary line.
pub fn finish_progress(bar: &ProgressBar, state: &ProgressState, elapsed: Duration) {
    let done = state.completed();
    let failed = state.failed();
    let ok = done - failed;
    let total = state.total();

    bar.finish_and_clear();

    let elapsed_str = format!("{:.1}s", elapsed.as_secs_f64());

    if failed == 0 {
        eprintln!(
            "{} {}/{} nodes completed in {}",
            style("✓").green().bold(),
            ok,
            total,
            style(elapsed_str).dim()
        );
    } else {
        eprintln!(
            "{} {}/{} ok, {} failed in {}",
            style("✗").red().bold(),
            ok,
            total,
            style(failed).red().bold(),
            style(elapsed_str).dim()
        );
    }
}

// ── Output formatting ───────────────────────────────────────────────────────

/// Print gathered output with a node header.
///
/// Mirrors `Display.format_header()` from the Python oracle: the node count
/// is appended as ` (N)` only when more than one node is gathered
/// (`node_count` at standard verbosity).
///
/// ```text
/// ---------------
/// node[1-3] (3)
/// ---------------
/// hello world
/// ```
pub fn print_gathered_header(nodes: &str, count: usize, out: &mut impl Write) -> io::Result<()> {
    let sep = "-".repeat(15);
    writeln!(out, "{sep}")?;
    if count > 1 {
        writeln!(out, "{nodes} ({count})")?;
    } else {
        writeln!(out, "{nodes}")?;
    }
    writeln!(out, "{sep}")?;
    Ok(())
}

/// Print a single line with node prefix: `node1: output`
pub fn print_line_with_label(node: &str, line: &str, out: &mut impl Write) -> io::Result<()> {
    writeln!(out, "{node}: {line}")
}

// ── Streaming line output (GH#528/GH#597 port) ──────────────────────────────

/// Streaming per-line output handler: prints each completed output line as
/// soon as it arrives instead of buffering everything until command exit.
///
/// Ports the upstream line-buffering fix (ClusterShell commits 65e5433 /
/// GH#528/GH#597): in Python, `Display` reconfigures `sys.stdout` /
/// `sys.stderr` with `line_buffering=True` so that piped consumers observe
/// lines in real time. Rust's `io::Stdout` is already a `LineWriter`, and
/// this handler additionally flushes after every completed line, giving the
/// same observable behavior: lines arrive as they are produced, not in one
/// block at process exit.
///
/// Partial lines are buffered per node and flushed on `on_close`, so no
/// output is lost even when a node's final line lacks a trailing newline.
/// Lines print to stdout with the usual `node: ` label unless disabled.
///
/// Note: the core `ExecWorker` currently reports raw pipe fds in `on_read`
/// (never normalized to 1/2), so stderr chunks cannot be told apart from
/// stdout here — consistent with `TaskGatheringHandler`, which also folds
/// them into the stdout buffers. Once the core normalizes fds, a `fd == 2`
/// branch can route stderr lines to stderr like Python's `print_line_error`.
///
/// An optional inner handler (e.g. [`ProgressHandler`]) is chained after
/// every callback.
pub struct LineStreamHandler {
    label: bool,
    partial: HashMap<String, Vec<u8>>,
    inner: Option<Box<dyn EventHandler>>,
}

impl LineStreamHandler {
    /// Create a handler; `label` controls the `node: ` prefix on lines.
    pub fn new(label: bool) -> Self {
        Self {
            label,
            partial: HashMap::new(),
            inner: None,
        }
    }

    /// Chain another handler (called after this one on every event).
    pub fn with_inner(mut self, inner: Box<dyn EventHandler>) -> Self {
        self.inner = Some(inner);
        self
    }

    /// Print one completed line to stdout and flush.
    fn emit_line(&self, node: &str, bytes: &[u8]) {
        let text = String::from_utf8_lossy(bytes);
        let mut out = io::stdout().lock();
        if self.label {
            let _ = writeln!(out, "{node}: {text}");
        } else {
            let _ = writeln!(out, "{text}");
        }
        // Line-buffering semantics: make each line visible to piped
        // consumers immediately (Stdout is a LineWriter; flush anyway).
        let _ = out.flush();
    }

    /// Append a chunk to the node's partial buffer and emit completed lines.
    fn feed(&self, node: &str, buf: &mut Vec<u8>, chunk: &[u8]) {
        buf.extend_from_slice(chunk);
        while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = buf.drain(..pos).collect();
            let _ = buf.drain(..1); // drop the newline
            self.emit_line(node, &line);
        }
    }

    /// Flush any remaining partial line for a node (called on close).
    fn flush_partial(&mut self, node: &str) {
        if let Some(buf) = self.partial.remove(node) {
            if !buf.is_empty() {
                self.emit_line(node, &buf);
            }
        }
    }
}

impl EventHandler for LineStreamHandler {
    fn on_start(&mut self, worker: &dyn consortium::worker::Worker) {
        if let Some(ref mut h) = self.inner {
            h.on_start(worker);
        }
    }

    fn on_read(&mut self, node: &str, fd: std::os::unix::io::RawFd, msg: &[u8]) {
        let _ = fd; // see struct note: raw pipe fds, not normalized to 1/2
        let mut buf = self.partial.remove(node).unwrap_or_default();
        self.feed(node, &mut buf, msg);
        self.partial.insert(node.to_string(), buf);
        if let Some(ref mut h) = self.inner {
            h.on_read(node, fd, msg);
        }
    }

    fn on_close(&mut self, node: &str, rc: i32) {
        self.flush_partial(node);
        if let Some(ref mut h) = self.inner {
            h.on_close(node, rc);
        }
    }

    fn on_timeout(&mut self, node: &str) {
        self.flush_partial(node);
        if let Some(ref mut h) = self.inner {
            h.on_timeout(node);
        }
    }

    fn on_error(&mut self, node: &str, error: &consortium::worker::WorkerError) {
        if let Some(ref mut h) = self.inner {
            h.on_error(node, error);
        }
    }
}
