//! I/O engine abstraction.
//!
//! Rust implementation of `ClusterShell.Engine`.
//!
//! The Python version has EPoll, Poll, and Select backends.
//! In Rust we will likely use a single backend (mio or polling crate).

/// Engine trait — the core event loop abstraction.
pub trait Engine: Send {
    fn run(&mut self) -> std::io::Result<()>;
}
