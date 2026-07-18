//! # consortium-integration
//!
//! The shared contract for consortium's tool integrations (nix, slurm,
//! ansible, skypilot, ray).
//!
//! Integrations all do the same three things: run external commands (locally
//! or over ssh), read the fleet topology, and report success/failure. This
//! crate provides each piece once, so integration crates stay thin and —
//! crucially — testable without real infrastructure:
//!
//! - [`exec`] — the [`exec::Executor`] abstraction. Integrations build a
//!   [`exec::CommandSpec`] and hand it to an executor instead of touching
//!   [`std::process::Command`] directly. [`exec::ProcessExecutor`] is the
//!   production implementation (and the only place ssh argv is composed);
//!   [`exec::ScriptedExecutor`] is the test fake that scripts outputs and
//!   records invocations.
//! - [`fleet`] — fleet configuration types ([`fleet::FleetConfig`] and the
//!   per-integration sub-configs) produced by the Nix library's `mkFleet`.
//! - [`staging`] — executor-based nix staging helpers
//!   ([`staging::build_flake_attr`], [`staging::copy_closure`]) used to place
//!   hermetic environments on remote hosts before running integration
//!   payloads.
//! - [`report`] — the [`report::IntegrationReport`] trait every integration's
//!   report type satisfies.
//!
//! # Testing integrations
//!
//! ```
//! use consortium_integration::exec::{CommandSpec, ExecOutput, Executor, ScriptedExecutor};
//!
//! // Script the commands your integration is expected to run.
//! let exec = ScriptedExecutor::new()
//!     .on("nix build", ExecOutput::ok("/nix/store/abc-env\n"))
//!     .on_error("sbatch", "socket timed out");
//!
//! // ... pass &exec into integration code, then assert on what it did:
//! # let _ = exec.exec(&CommandSpec::new("nix").args(["build", ".#x"]));
//! exec.assert_invoked_containing("nix build");
//! ```

pub mod exec;
pub mod fleet;
pub mod report;
pub mod staging;

pub use exec::{
    shell_quote, ssh_argv, CommandSpec, ExecError, ExecOutput, Executor, ProcessExecutor, Rule,
    ScriptedExecutor, SshTarget, DEFAULT_SSH_OPTS,
};
pub use report::IntegrationReport;
pub use staging::StagingError;
