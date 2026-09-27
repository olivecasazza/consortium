//! Isolation policy for running untrusted commands on fleet nodes.
//!
//! [`SandboxSpec`] is deliberately *data*, not behaviour: a side-effect-free
//! description of what a command may touch. It serializes to JSON so a policy
//! is reviewable in a pull request and testable with no hypervisor, no
//! `/dev/kvm`, and no VM to boot.
//!
//! [`Sandbox`] is the execution seam. A backend turns a spec plus a command
//! into an outcome; [`Sandbox::is_isolated`] reports honestly whether the
//! backend actually enforced anything, so a degraded run is visible rather
//! than silently unprotected.
//!
//! No hypervisor backend is wired up yet. The libkrun one cannot be built or
//! tested on this machine - libkrun is Linux-only in nixpkgs - so it lives
//! behind a feature gate rather than shipping unexercised FFI.

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Whether a sandboxed command may reach the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkPolicy {
    /// No network access from inside the sandbox.
    #[default]
    Deny,
    /// The command may open outbound connections.
    Allow,
}

/// What a sandboxed command is permitted to touch.
///
/// Construct with [`SandboxSpec::deny_all`], then relax. Validation lives in
/// [`SandboxSpec::validate`] so an impossible policy is rejected before any
/// process is spawned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct SandboxSpec {
    /// Paths mounted read-only inside the sandbox.
    pub read_only_paths: Vec<PathBuf>,
    /// Paths the command may write.
    pub writable_paths: Vec<PathBuf>,
    /// Network reachability.
    pub network: NetworkPolicy,
    /// Environment handed to the command, applied after the backend default.
    pub env: Vec<(String, String)>,
    /// Wall-clock limit; `None` means the backend default applies.
    pub timeout: Option<Duration>,
}

impl SandboxSpec {
    /// A policy that grants nothing: no writable paths, no network.
    pub fn deny_all() -> Self {
        Self::default()
    }

    /// Grant a read-only path.
    #[must_use]
    pub fn with_read_only(mut self, path: impl Into<PathBuf>) -> Self {
        self.read_only_paths.push(path.into());
        self
    }

    /// Grant a writable path.
    #[must_use]
    pub fn with_writable(mut self, path: impl Into<PathBuf>) -> Self {
        self.writable_paths.push(path.into());
        self
    }

    /// Set network reachability.
    #[must_use]
    pub fn with_network(mut self, network: NetworkPolicy) -> Self {
        self.network = network;
        self
    }

    /// Set a wall-clock limit.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Reject a policy that cannot be enforced.
    ///
    /// A path granted both read-only and writable is ambiguous: whether a write
    /// lands depends on backend and mount order, so it is refused rather than
    /// silently resolved.
    pub fn validate(&self) -> Result<(), SpecError> {
        for path in &self.writable_paths {
            if self.read_only_paths.contains(path) {
                return Err(SpecError::ConflictingPath { path: path.clone() });
            }
        }
        Ok(())
    }
}

/// A command to run inside a sandbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SandboxCommand {
    /// Program to execute; not passed through a shell.
    pub program: String,
    /// Arguments, passed verbatim.
    pub args: Vec<String>,
    /// Working directory inside the sandbox.
    pub cwd: Option<PathBuf>,
}

impl SandboxCommand {
    /// A command with no arguments and no working directory.
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            cwd: None,
        }
    }

    /// Append an argument.
    #[must_use]
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Set the working directory.
    #[must_use]
    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }
}

/// What a sandboxed command produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxOutput {
    /// Exit status, or `-1` when terminated by a signal.
    pub status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl SandboxOutput {
    /// Whether the command exited zero.
    pub fn success(&self) -> bool {
        self.status == 0
    }
}

/// Why a sandbox could not run a command.
#[derive(Debug)]
pub enum SandboxError {
    /// The policy is internally inconsistent.
    Spec(SpecError),
    /// Spawning the command failed.
    Spawn(io::Error),
    /// The backend does not implement the requested capability.
    Unsupported(&'static str),
}

impl std::fmt::Display for SandboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spec(err) => write!(f, "invalid sandbox policy: {err}"),
            Self::Spawn(err) => write!(f, "sandbox spawn failed: {err}"),
            Self::Unsupported(what) => write!(f, "sandbox backend does not support {what}"),
        }
    }
}

impl std::error::Error for SandboxError {}

impl From<SpecError> for SandboxError {
    fn from(err: SpecError) -> Self {
        Self::Spec(err)
    }
}

impl From<io::Error> for SandboxError {
    fn from(err: io::Error) -> Self {
        Self::Spawn(err)
    }
}

/// Why a [`SandboxSpec`] is not enforceable.
#[derive(Debug, PartialEq, Eq)]
pub enum SpecError {
    /// The same path was granted read-only and writable.
    ConflictingPath { path: PathBuf },
}

impl std::fmt::Display for SpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConflictingPath { path } => {
                write!(f, "{} is both read-only and writable", path.display())
            }
        }
    }
}

impl std::error::Error for SpecError {}

/// Executes commands under an isolation policy.
///
/// Implementations are `Send + Sync` so a backend can be shared across the
/// CLI's worker threads, matching how `consortium_integration::exec::Executor`
/// is threaded.
pub trait Sandbox: Send + Sync {
    /// Backend name, for diagnostics.
    fn name(&self) -> &'static str;

    /// Whether this backend actually enforces isolation.
    ///
    /// A backend that cannot honour the policy must return `false` here rather
    /// than pretend. Callers decide whether a degraded run is acceptable; the
    /// backend never decides it silently.
    fn is_isolated(&self) -> bool;

    /// Run `command` under `spec`.
    fn exec(
        &self,
        spec: &SandboxSpec,
        command: &SandboxCommand,
    ) -> Result<SandboxOutput, SandboxError>;
}

/// A backend that enforces nothing; the default when no hypervisor is
/// available.
///
/// It exists so the trait has a working implementation and so an unprotected
/// run is explicitly reported by [`Sandbox::is_isolated`] rather than assumed.
#[derive(Debug, Clone, Copy, Default)]
pub struct DirectSandbox;

impl Sandbox for DirectSandbox {
    fn name(&self) -> &'static str {
        "direct"
    }

    fn is_isolated(&self) -> bool {
        false
    }

    fn exec(
        &self,
        spec: &SandboxSpec,
        command: &SandboxCommand,
    ) -> Result<SandboxOutput, SandboxError> {
        spec.validate()?;
        if spec.network == NetworkPolicy::Deny {
            return Err(SandboxError::Unsupported("a network-denying policy"));
        }

        let mut cmd = std::process::Command::new(&command.program);
        cmd.args(&command.args);
        for (key, value) in &spec.env {
            cmd.env(key, value);
        }
        if let Some(cwd) = &command.cwd {
            cmd.current_dir(cwd);
        }
        cmd.stdin(std::process::Stdio::null());

        let output = cmd.output()?;
        Ok(SandboxOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

// A hypervisor backend lands here behind `#[cfg(feature = "libkrun")]`
// once one exists that builds on this platform. libkrun itself supports
// HVF on macOS/ARM64, but nixpkgs packages it for Linux only
// (meta.platforms = x86_64-linux, aarch64-linux, riscv64-linux), so a
// Nix-built backend cannot be exercised on darwin. On macOS,
// nixpkgs ships `vfkit` (aarch64-darwin) as the Virtualization.framework
// equivalent. Either way the feature stays off until a backend is
// compiled and run on the host that will use it.
