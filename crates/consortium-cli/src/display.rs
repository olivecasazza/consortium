//! Shared output formatting for consortium CLI tools.
//!
//! Provides nh-inspired progress reporting and output formatting.

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
/// ```text
/// ---------------
/// node[1-3]
/// ---------------
/// hello world
/// ```
pub fn print_gathered_header(nodes: &str, out: &mut impl Write) -> io::Result<()> {
    let sep = "-".repeat(15);
    writeln!(out, "{sep}")?;
    writeln!(out, "{nodes}")?;
    writeln!(out, "{sep}")?;
    Ok(())
}

/// Print a single line with node prefix: `node1: output`
pub fn print_line_with_label(node: &str, line: &str, out: &mut impl Write) -> io::Result<()> {
    writeln!(out, "{node}: {line}")
}
