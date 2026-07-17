//! Worker module for executing commands across nodes.
//!
//! This module provides the abstraction for executing commands
//! across nodes, similar to ClusterShell's Worker.
//!
//! ## Submodules
//!
//! - [`exec`] — Local command execution worker (ExecWorker)
//! - [`ssh`] — SSH-based remote execution worker (SshWorker, ScpWorker)
//! - [`tree`] — Tree-based propagation worker (TreeWorker)

pub mod exec;
pub mod ssh;
pub mod tree;

use std::collections::{HashMap, HashSet};
use std::os::unix::io::RawFd;
use thiserror::Error;

// ============================================================================
// StreamWriters — named output streams with EngineClient EPIPE semantics
// ============================================================================

/// Outcome of a single stream flush attempt.
///
/// Mirrors the paths of ClusterShell's `EngineClient._handle_write`
/// (Worker/EngineClient.py), including commit cfe0448 ("remove stream on
/// broken pipe").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamWriteOutcome {
    /// All buffered data was written; the stream stays registered.
    Flushed,
    /// The fd would block (EAGAIN): data stays buffered and the stream stays
    /// registered to retry later (upstream calls `_set_writing` and returns).
    Pending,
    /// Broken pipe (EPIPE): the stream was removed at the point of error and
    /// must NOT be retried (cfe0448 — engines without POLLERR, like select,
    /// would otherwise never remove it and the run loop would not return).
    BrokenPipe,
    /// EOF was requested and the buffer just drained: the stream was removed.
    Closed,
    /// The named stream is unknown or was already removed.
    Unknown,
}

/// State of a single named write stream (upstream `EngineStream` with
/// `E_WRITE` events).
#[derive(Debug)]
struct WriteStream {
    /// File descriptor to write to (owned by the caller).
    fd: RawFd,
    /// Pending output bytes (upstream `wfile.wbuf`).
    wbuf: Vec<u8>,
    /// Whether EOF was requested (upstream `wfile.eof`): once the buffer
    /// drains, the stream is removed.
    eof: bool,
}

/// Registry of named output streams with ClusterShell `EngineClient` write
/// semantics.
///
/// Ported behavior (commit cfe0448):
/// - on EPIPE the stream is removed/closed cleanly at the point of error
///   instead of erroring repeatedly;
/// - writes (and EOF requests) to an unknown or removed stream are dropped
///   silently, since the stream may have been removed while an event handler
///   is still running.
///
/// File descriptors are borrowed: removal never closes the fd; ownership
/// stays with the caller.
#[derive(Debug, Default)]
pub struct StreamWriters {
    streams: HashMap<String, WriteStream>,
}

impl StreamWriters {
    /// Create an empty stream registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a named write stream over `fd` (mirrors `set_writer`).
    /// Registering an existing name resets its buffer and EOF flag.
    pub fn set_writer(&mut self, sname: &str, fd: RawFd) {
        self.streams.insert(
            sname.to_string(),
            WriteStream {
                fd,
                wbuf: Vec::new(),
                eof: false,
            },
        );
    }

    /// Remove a stream (mirrors `Engine.remove_stream`). Returns whether a
    /// stream was actually removed.
    pub fn remove(&mut self, sname: &str) -> bool {
        self.streams.remove(sname).is_some()
    }

    /// Whether a stream with this name is currently registered.
    pub fn contains(&self, sname: &str) -> bool {
        self.streams.contains_key(sname)
    }

    /// Number of registered streams.
    pub fn len(&self) -> usize {
        self.streams.len()
    }

    /// Whether there are no registered streams.
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    /// Add data to be written to the stream (mirrors `EngineClient._write`).
    ///
    /// Writes to an unknown or already-removed stream are dropped silently
    /// (cfe0448): the stream may have been removed on EPIPE while an event
    /// handler was still running.
    pub fn write(&mut self, sname: &str, buf: &[u8]) {
        if let Some(stream) = self.streams.get_mut(sname) {
            stream.wbuf.extend_from_slice(buf);
        }
        // else: dropped, like upstream's "dropping write to unknown or
        // closed stream" debug log.
    }

    /// Request EOF once the buffer drains (mirrors `_set_write_eof`).
    /// Unknown streams are ignored, like upstream.
    pub fn set_write_eof(&mut self, sname: &str) {
        if let Some(stream) = self.streams.get_mut(sname) {
            stream.eof = true;
        }
    }

    /// Bytes still buffered for this stream (0 for unknown streams).
    pub fn buffered_len(&self, sname: &str) -> usize {
        self.streams.get(sname).map_or(0, |s| s.wbuf.len())
    }

    /// Fds with pending data or pending EOF — the write-interest set.
    pub fn pending_fds(&self) -> Vec<RawFd> {
        self.streams
            .values()
            .filter(|s| !s.wbuf.is_empty() || s.eof)
            .map(|s| s.fd)
            .collect()
    }

    /// Flush a stream's buffer to its fd (mirrors
    /// `EngineClient._handle_write`).
    ///
    /// - empty buffer + EOF requested → stream removed, [`StreamWriteOutcome::Closed`]
    /// - EAGAIN → buffer kept, [`StreamWriteOutcome::Pending`] (retry later)
    /// - EPIPE → stream removed at the point of error,
    ///   [`StreamWriteOutcome::BrokenPipe`]; NOT an error (cfe0448)
    /// - other I/O errors are propagated
    /// - full/partial write → [`StreamWriteOutcome::Flushed`] /
    ///   [`StreamWriteOutcome::Pending`]; if EOF was requested and the
    ///   buffer drained, the stream is removed ([`StreamWriteOutcome::Closed`])
    pub fn handle_write(&mut self, sname: &str) -> std::io::Result<StreamWriteOutcome> {
        let Some(stream) = self.streams.get_mut(sname) else {
            return Ok(StreamWriteOutcome::Unknown);
        };

        if stream.wbuf.is_empty() {
            if stream.eof {
                // nothing left to write and EOF requested: remove stream
                self.streams.remove(sname);
                return Ok(StreamWriteOutcome::Closed);
            }
            return Ok(StreamWriteOutcome::Flushed);
        }

        let fd = stream.fd;
        let written = unsafe {
            libc::write(
                fd,
                stream.wbuf.as_ptr() as *const libc::c_void,
                stream.wbuf.len(),
            )
        };

        if written < 0 {
            let err = std::io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EAGAIN) => {
                    // try again later; stream stays registered
                    return Ok(StreamWriteOutcome::Pending);
                }
                Some(libc::EPIPE) => {
                    // broken pipe: remove stream now and do NOT retry
                    // (cfe0448 — engines without POLLERR never saw the
                    // stream go away and the run loop would not return)
                    self.streams.remove(sname);
                    return Ok(StreamWriteOutcome::BrokenPipe);
                }
                _ => return Err(err),
            }
        }

        let wcnt = written as usize;
        if wcnt == 0 {
            return Ok(StreamWriteOutcome::Pending);
        }

        let stream = self
            .streams
            .get_mut(sname)
            .expect("stream removed during write");
        stream.wbuf.drain(..wcnt);

        if stream.wbuf.is_empty() && stream.eof {
            self.streams.remove(sname);
            return Ok(StreamWriteOutcome::Closed);
        }
        if stream.wbuf.is_empty() {
            Ok(StreamWriteOutcome::Flushed)
        } else {
            Ok(StreamWriteOutcome::Pending)
        }
    }
}

/// Error types for the worker module.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// I/O error wrapper.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// General worker error.
    #[error("worker error: {0}")]
    General(String),
    /// Operation timed out.
    #[error("timeout")]
    Timeout,
}

/// Result type alias for worker operations.
pub type Result<T> = std::result::Result<T, WorkerError>;

/// Current state of a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    /// Worker is created but not started yet.
    Pending,
    /// Worker is currently running.
    Running,
    /// Worker has completed.
    Done,
    /// Worker was aborted.
    Aborted,
}

/// Event handler trait for worker events.
///
/// Implement this trait to receive notifications about worker events.
pub trait EventHandler: Send {
    /// Called when the worker starts.
    fn on_start(&mut self, worker: &dyn Worker) {
        let _ = worker;
    }

    /// Called when data is available for reading from a node.
    fn on_read(&mut self, node: &str, fd: RawFd, msg: &[u8]) {
        let _ = (node, fd, msg);
    }

    /// Called when a connection to a node is closed.
    fn on_close(&mut self, node: &str, rc: i32) {
        let _ = (node, rc);
    }

    /// Called when a node operation times out.
    fn on_timeout(&mut self, node: &str) {
        let _ = node;
    }

    /// Called when an error occurs for a node.
    fn on_error(&mut self, node: &str, error: &WorkerError) {
        let _ = (node, error);
    }

    /// Extract buffered stdout/stderr data from this handler.
    /// Returns (stdout, stderr, timeouts) where stdout/stderr are node -> `Vec<chunks>`.
    /// Default returns empty maps. Used by Task to collect results after execution.
    fn take_buffers(
        &mut self,
    ) -> (
        HashMap<String, Vec<Vec<u8>>>,
        HashMap<String, Vec<Vec<u8>>>,
        HashSet<String>,
    ) {
        (HashMap::new(), HashMap::new(), HashSet::new())
    }
}

/// Trait for worker implementations.
///
/// A worker executes commands across multiple nodes, handling I/O
/// events and managing child processes.
pub trait Worker: Send {
    /// Start the worker.
    fn start(&mut self) -> Result<()>;

    /// Abort the worker, optionally killing child processes.
    fn abort(&mut self, kill: bool);

    /// Get the worker's current state.
    fn state(&self) -> WorkerState;

    /// Set the event handler for this worker.
    fn set_handler(&mut self, handler: Box<dyn EventHandler>);

    /// Get file descriptors for read interest.
    fn read_fds(&self) -> Vec<RawFd>;

    /// Get file descriptors for write interest.
    fn write_fds(&self) -> Vec<RawFd>;

    /// Handle a read event on the given file descriptor.
    fn handle_read(&mut self, fd: RawFd) -> Result<()>;

    /// Handle a write event on the given file descriptor.
    fn handle_write(&mut self, fd: RawFd) -> Result<()>;

    /// Check if the worker has completed.
    fn is_done(&self) -> bool;

    /// Get the return codes map: node -> return_code.
    fn retcodes(&self) -> &HashMap<String, i32>;

    /// Get the number of nodes.
    fn num_nodes(&self) -> usize;

    /// Take the event handler out of the worker (used by Task to extract gathered data).
    fn take_handler(&mut self) -> Option<Box<dyn EventHandler>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Test WorkerState enum variants
    #[test]
    fn test_worker_state_variants() {
        assert_eq!(WorkerState::Pending, WorkerState::Pending);
        assert_eq!(WorkerState::Running, WorkerState::Running);
        assert_eq!(WorkerState::Done, WorkerState::Done);
        assert_eq!(WorkerState::Aborted, WorkerState::Aborted);
    }

    /// Test WorkerState is Debug
    #[test]
    fn test_worker_state_debug() {
        assert_eq!(format!("{:?}", WorkerState::Pending), "Pending");
        assert_eq!(format!("{:?}", WorkerState::Running), "Running");
        assert_eq!(format!("{:?}", WorkerState::Done), "Done");
        assert_eq!(format!("{:?}", WorkerState::Aborted), "Aborted");
    }

    /// Test WorkerState is Clone
    #[test]
    fn test_worker_state_clone() {
        let state = WorkerState::Pending;
        let cloned = state.clone();
        assert_eq!(state, cloned);
    }

    /// Test WorkerState is Copy
    #[test]
    fn test_worker_state_copy() {
        fn take_copy<T: Copy>(_: T) {}
        take_copy(WorkerState::Pending);
    }

    /// Test WorkerState is PartialEq
    #[test]
    fn test_worker_state_partial_eq() {
        assert!(WorkerState::Pending == WorkerState::Pending);
        assert!(WorkerState::Running != WorkerState::Pending);
    }

    /// Test WorkerError variants
    #[test]
    fn test_worker_error_variants() {
        let io_err = WorkerError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not found",
        ));
        let gen_err = WorkerError::General("something went wrong".to_string());
        let timeout_err = WorkerError::Timeout;

        matches!(io_err, WorkerError::Io(_));
        matches!(gen_err, WorkerError::General(_));
        matches!(timeout_err, WorkerError::Timeout);
    }

    /// Test WorkerError implements Error trait
    #[test]
    fn test_worker_error_impls_error() {
        use std::error::Error;

        let gen_err = WorkerError::General("test".to_string());
        assert!(gen_err.source().is_none());

        let timeout_err = WorkerError::Timeout;
        assert!(timeout_err.source().is_none());
    }

    /// Test WorkerError implements Display trait
    #[test]
    fn test_worker_error_impls_display() {
        let io_err = WorkerError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file.txt",
        ));
        assert_eq!(io_err.to_string(), "io error: file.txt");

        let gen_err = WorkerError::General("test error".to_string());
        assert_eq!(gen_err.to_string(), "worker error: test error");

        let timeout_err = WorkerError::Timeout;
        assert_eq!(timeout_err.to_string(), "timeout");
    }

    /// Test EventHandler default methods compile
    #[test]
    fn test_event_handler_default_methods() {
        struct TestHandler;

        impl EventHandler for TestHandler {}

        let handler = TestHandler;

        // These should compile with default implementations (no-op tests)
        // We just verify the methods exist and can be called
        let _ = handler;
    }

    /// Test that EventHandler is object-safe
    #[test]
    fn test_event_handler_object_safe() {
        struct TestHandler;

        impl EventHandler for TestHandler {}

        let _handler: Box<dyn EventHandler> = Box::new(TestHandler);
    }

    /// Test EventHandler is Send
    #[test]
    fn test_event_handler_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Box<dyn EventHandler>>();
    }

    /// Test WorkerError Result type alias
    #[test]
    fn test_result_type_alias() {
        let ok: Result<()> = Ok(());
        let err: Result<()> = Err(WorkerError::Timeout);

        assert!(ok.is_ok());
        assert!(err.is_err());
    }

    // -- StreamWriters tests (cfe0448 port) ----------------------------------

    /// Create a pipe, returning (read_fd, write_fd).
    ///
    /// Both ends are marked FD_CLOEXEC: tests run multi-threaded in one
    /// process, and child processes spawned by other tests must not inherit
    /// these fds (an inherited read end would keep the pipe open and mask
    /// the EPIPE we are testing for).
    fn make_pipe() -> (RawFd, RawFd) {
        let mut fds = [0; 2];
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(rc, 0, "pipe() failed");
        for fd in fds {
            unsafe {
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
        }
        (fds[0], fds[1])
    }

    /// Read up to `buf.len()` bytes from fd (blocking).
    fn read_fd(fd: RawFd, buf: &mut [u8]) -> usize {
        unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) as usize }
    }

    fn close_fd(fd: RawFd) {
        unsafe {
            libc::close(fd);
        }
    }

    /// Mirror of StreamWorkerTest::test_011_broken_pipe_on_write_twice.
    ///
    /// After a broken pipe, the stream is removed at the point of error
    /// (cfe0448) and subsequent writes/EOF are safely dropped instead of
    /// erroring repeatedly. Logic-level: the upstream `check_written == 1`
    /// corresponds to the first successful flush, and `check_hup == 1` to
    /// the stream disappearing (removal) rather than raising.
    #[test]
    fn test_stream_writer_broken_pipe_on_write_twice() {
        let (rfd, wfd) = make_pipe();
        let mut writers = StreamWriters::new();
        writers.set_writer("test", wfd);

        // initial write goes through
        writers.write("test", b"initial");
        assert_eq!(writers.handle_write("test").unwrap(), StreamWriteOutcome::Flushed);
        let mut buf = [0u8; 16];
        let n = read_fd(rfd, &mut buf);
        assert_eq!(&buf[..n], b"initial");

        // close the reader: the pipe is now broken
        close_fd(rfd);

        // EPIPE removes the stream (no retry). Concurrently spawned child
        // processes from other tests may transiently hold the read end
        // (fork/exec window before FD_CLOEXEC applies), so retry the write
        // until the kernel reports EPIPE, bounded by a deadline.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            writers.write("test", b"final");
            match writers.handle_write("test").unwrap() {
                StreamWriteOutcome::BrokenPipe => break,
                _ => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "pipe never reported EPIPE"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
        assert!(!writers.contains("test"));
        assert!(writers.is_empty());

        // subsequent writes to the removed stream are dropped without error
        writers.write("test", b"more");
        assert_eq!(writers.buffered_len("test"), 0);
        writers.set_write_eof("test");
        assert_eq!(writers.handle_write("test").unwrap(), StreamWriteOutcome::Unknown);

        close_fd(wfd);
    }

    /// EOF requested with pending data: the stream is removed once the
    /// buffer has drained (mirrors upstream's eof + empty wbuf path).
    #[test]
    fn test_stream_writer_eof_closes_after_drain() {
        let (rfd, wfd) = make_pipe();
        let mut writers = StreamWriters::new();
        writers.set_writer("stdin", wfd);

        writers.write("stdin", b"payload");
        writers.set_write_eof("stdin");
        assert_eq!(writers.handle_write("stdin").unwrap(), StreamWriteOutcome::Closed);
        assert!(!writers.contains("stdin"));

        let mut buf = [0u8; 16];
        let n = read_fd(rfd, &mut buf);
        assert_eq!(&buf[..n], b"payload");

        close_fd(rfd);
        close_fd(wfd);
    }

    /// EOF requested with an empty buffer removes the stream immediately.
    #[test]
    fn test_stream_writer_eof_empty_buffer_closes() {
        let (rfd, wfd) = make_pipe();
        let mut writers = StreamWriters::new();
        writers.set_writer("test", wfd);
        writers.set_write_eof("test");
        assert_eq!(writers.handle_write("test").unwrap(), StreamWriteOutcome::Closed);
        assert!(!writers.contains("test"));
        close_fd(rfd);
        close_fd(wfd);
    }

    /// EAGAIN keeps the data buffered and the stream registered (mirrors
    /// upstream's `_set_writing` retry-later path).
    #[test]
    fn test_stream_writer_eagain_keeps_buffer() {
        let (rfd, wfd) = make_pipe();
        // non-blocking write end
        unsafe {
            let flags = libc::fcntl(wfd, libc::F_GETFL);
            libc::fcntl(wfd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        let mut writers = StreamWriters::new();
        writers.set_writer("test", wfd);

        // Fill the pipe beyond capacity without reading: eventually EAGAIN.
        let chunk = [b'x'; 65536];
        let mut saw_pending = false;
        for _ in 0..256 {
            writers.write("test", &chunk);
            match writers.handle_write("test").unwrap() {
                StreamWriteOutcome::Pending => {
                    saw_pending = true;
                    break;
                }
                StreamWriteOutcome::Flushed => continue,
                other => panic!("unexpected outcome {:?}", other),
            }
        }
        assert!(saw_pending, "pipe should fill up and report EAGAIN");
        assert!(writers.contains("test"), "stream must stay registered on EAGAIN");
        assert!(writers.buffered_len("test") > 0, "unwritten data stays buffered");

        close_fd(rfd);
        close_fd(wfd);
    }

    /// Operations on a never-registered stream are dropped/ignored (cfe0448:
    /// "Drop writes to an unknown or closed stream, like set_write_eof()
    /// already does").
    #[test]
    fn test_stream_writer_unknown_stream_ops_dropped() {
        let mut writers = StreamWriters::new();
        writers.write("nope", b"data");
        writers.set_write_eof("nope");
        assert_eq!(writers.buffered_len("nope"), 0);
        assert!(!writers.contains("nope"));
        assert_eq!(writers.handle_write("nope").unwrap(), StreamWriteOutcome::Unknown);
    }

    /// Explicit removal mirrors Engine.remove_stream; later writes drop.
    #[test]
    fn test_stream_writer_remove() {
        let (rfd, wfd) = make_pipe();
        let mut writers = StreamWriters::new();
        writers.set_writer("test", wfd);
        writers.write("test", b"queued");
        assert!(writers.remove("test"));
        assert!(!writers.remove("test"));
        writers.write("test", b"more");
        assert_eq!(writers.buffered_len("test"), 0);
        close_fd(rfd);
        close_fd(wfd);
    }

    /// Pending fds reflect buffered data / requested EOF (write interest).
    #[test]
    fn test_stream_writer_pending_fds() {
        let (rfd, wfd) = make_pipe();
        let mut writers = StreamWriters::new();
        writers.set_writer("test", wfd);
        assert!(writers.pending_fds().is_empty());
        writers.write("test", b"x");
        assert_eq!(writers.pending_fds(), vec![wfd]);
        assert_eq!(writers.handle_write("test").unwrap(), StreamWriteOutcome::Flushed);
        assert!(writers.pending_fds().is_empty());
        close_fd(rfd);
        close_fd(wfd);
    }
}
