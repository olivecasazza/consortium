//! Command execution abstraction.
//!
//! Every integration crate (nix, slurm, ansible, skypilot, ray) ultimately
//! runs external commands — `nix build`, `sbatch` over ssh, `ansible-playbook`,
//! and so on. This module is the single choke point for that:
//!
//! - [`CommandSpec`] describes *what* to run (program, args, env, cwd, and an
//!   optional [`SshTarget`] for remote execution).
//! - [`Executor`] is the trait integrations code against.
//! - [`ProcessExecutor`] is the production implementation. It runs commands
//!   locally via [`std::process::Command`], or over ssh when
//!   [`CommandSpec::ssh`] is set. It is the **only** place ssh argv is
//!   composed (see [`ssh_argv`]).
//! - [`ScriptedExecutor`] is a test fake: it matches each invocation against
//!   an ordered list of [`Rule`]s, returns the scripted output, and records
//!   every invocation for later assertions.
//!
//! # Example
//!
//! ```
//! use consortium_integration::exec::{ExecOutput, Executor, ScriptedExecutor};
//!
//! let exec = ScriptedExecutor::new()
//!     .on("nix build", ExecOutput::ok("/nix/store/abc-hello\n"))
//!     .on_error("nix copy", "ssh: connection refused");
//!
//! let out = exec.exec(&consortium_integration::exec::CommandSpec::new("nix")
//!     .args(["build", ".#hello", "--no-link"]))?;
//! assert!(out.success());
//!
//! exec.assert_invoked_containing("nix build .#hello");
//! exec.assert_not_invoked_containing("nix copy");
//! # Ok::<(), consortium_integration::exec::ExecError>(())
//! ```

use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

/// Default ssh options applied to every remote invocation.
///
/// `BatchMode` + `PasswordAuthentication=no` guarantee non-interactive
/// failure instead of hanging on a password prompt; `StrictHostKeyChecking=no`
/// matches the behavior the integrations relied on before this abstraction
/// existed (ephemeral fleets, trust-on-first-use by convention).
pub const DEFAULT_SSH_OPTS: [&str; 3] = [
    "-oStrictHostKeyChecking=no",
    "-oPasswordAuthentication=no",
    "-oBatchMode=yes",
];

/// An ssh endpoint for remote command execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshTarget {
    /// Remote login user.
    pub user: String,
    /// Remote host (hostname or IP).
    pub host: String,
    /// SSH port (`None` = ssh default, 22).
    pub port: Option<u16>,
    /// Extra ssh options (e.g. `"-i"`, `"/path/to/key"`, `"-oProxyJump=..."`),
    /// appended after the hardened defaults so they may override them.
    pub extra_opts: Vec<String>,
}

impl SshTarget {
    /// A target with no custom port and no extra options.
    pub fn new(user: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            host: host.into(),
            port: None,
            extra_opts: Vec::new(),
        }
    }

    /// Set a non-default ssh port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Append one extra ssh option.
    pub fn extra_opt(mut self, opt: impl Into<String>) -> Self {
        self.extra_opts.push(opt.into());
        self
    }

    /// Append several extra ssh options.
    pub fn extra_opts<I, S>(mut self, opts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extra_opts.extend(opts.into_iter().map(Into::into));
        self
    }
}

/// A fully-specified command to execute.
///
/// Construct with [`CommandSpec::new`] and chain builder helpers:
///
/// ```
/// use consortium_integration::exec::{CommandSpec, SshTarget};
///
/// let spec = CommandSpec::new("sbatch")
///     .args(["--job-name=train", "train.sh"])
///     .env("PATH", "/nix/store/abc-env/bin:/usr/bin")
///     .ssh(SshTarget::new("root", "submit01").port(2222));
/// ```
#[derive(Debug, Clone, Default)]
pub struct CommandSpec {
    /// Program to run (resolved via `PATH` locally, or on the remote host).
    pub program: String,
    /// Arguments, in order.
    pub args: Vec<String>,
    /// Environment variables to set for the command.
    ///
    /// Locally these are applied via [`std::process::Command::env`]. Over ssh
    /// they are prepended to the remote shell command as `VAR='value'`
    /// assignments.
    pub env: Vec<(String, String)>,
    /// Working directory. Locally applied via
    /// [`std::process::Command::current_dir`]; over ssh the remote command is
    /// wrapped as `cd '<dir>' && <command>`.
    pub cwd: Option<String>,
    /// When set, the command runs on this host via ssh instead of locally.
    pub ssh: Option<SshTarget>,
}

impl CommandSpec {
    /// A spec for `program` with no args, env, cwd, or ssh.
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            ..Default::default()
        }
    }

    /// Append one argument.
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Append several arguments.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Set one environment variable.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Set the working directory.
    pub fn cwd(mut self, cwd: impl Into<String>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// Run this command on `target` over ssh.
    pub fn ssh(mut self, target: SshTarget) -> Self {
        self.ssh = Some(target);
        self
    }

    /// The remote shell command string: env assignments, optional `cd`
    /// wrapper, then the shell-quoted program and arguments.
    ///
    /// Every token is single-quoted with `'\''` escaping, so the remote shell
    /// sees exactly the argv given here.
    pub fn remote_command(&self) -> String {
        if self.validate_environment().is_err() {
            return "false".to_owned();
        }
        let mut parts: Vec<String> = self
            .env
            .iter()
            .map(|(k, v)| format!("{}={}", k, shell_quote(v)))
            .collect();
        parts.push(shell_quote(&self.program));
        parts.extend(self.args.iter().map(|a| shell_quote(a)));
        let cmd = parts.join(" ");
        match &self.cwd {
            Some(dir) => format!("cd {} && {}", shell_quote(dir), cmd),
            None => cmd,
        }
    }

    /// Validate environment names for consistent local and POSIX shell execution.
    pub fn validate_environment(&self) -> std::io::Result<()> {
        for (key, _) in &self.env {
            let mut bytes = key.bytes();
            let first_valid = bytes
                .next()
                .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_');
            if !first_valid || !bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "environment variable name must be a POSIX identifier",
                ));
            }
        }
        Ok(())
    }

    /// The effective argv: [`ssh_argv`] output when ssh is set, otherwise
    /// `[program, args...]`.
    pub fn argv(&self) -> Vec<String> {
        match &self.ssh {
            Some(target) => ssh_argv(target, &self.remote_command()),
            None => std::iter::once(self.program.clone())
                .chain(self.args.iter().cloned())
                .collect(),
        }
    }

    /// The rendered command line used for rule matching and invocation
    /// recording in [`ScriptedExecutor`].
    ///
    /// This is the effective argv joined with single spaces — display and
    /// matching only; argv elements that contain spaces are *not* re-quoted
    /// here (the authoritative form is [`CommandSpec::argv`]). `env` and
    /// `cwd` are omitted for local commands but are visible in the remote
    /// command string for ssh commands.
    pub fn render(&self) -> String {
        self.argv().join(" ")
    }
}

/// Quote `s` for POSIX shells: wrap in single quotes, escaping embedded
/// single quotes as `'\''`.
pub fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Compose the full ssh argv for running `remote_command` on `target`.
///
/// Layout: `ssh <DEFAULT_SSH_OPTS> -l <user> [-p <port>] <extra_opts...>
/// <host> <remote_command>`. Extra options come after the hardened defaults
/// (and after `-l`/`-p`), so callers may override defaults when they must.
///
/// This is the only place ssh argv is composed in the workspace.
pub fn ssh_argv(target: &SshTarget, remote_command: &str) -> Vec<String> {
    let mut argv: Vec<String> = vec!["ssh".to_string()];
    argv.extend(DEFAULT_SSH_OPTS.iter().map(|s| s.to_string()));
    argv.push("-l".to_string());
    argv.push(target.user.clone());
    if let Some(port) = target.port {
        argv.push("-p".to_string());
        argv.push(port.to_string());
    }
    argv.extend(target.extra_opts.iter().cloned());
    argv.push("--".to_string());
    argv.push(target.host.clone());
    argv.push(remote_command.to_string());
    argv
}

/// Captured result of a finished command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecOutput {
    /// Process exit code (`-1` when terminated by a signal).
    pub status: i32,
    /// Captured stdout (lossy UTF-8).
    pub stdout: String,
    /// Captured stderr (lossy UTF-8).
    pub stderr: String,
}

impl ExecOutput {
    /// Full constructor.
    pub fn new(status: i32, stdout: impl Into<String>, stderr: impl Into<String>) -> Self {
        Self {
            status,
            stdout: stdout.into(),
            stderr: stderr.into(),
        }
    }

    /// Successful (exit 0) output with empty stderr.
    pub fn ok(stdout: impl Into<String>) -> Self {
        Self::new(0, stdout, "")
    }

    /// Whether the command exited with status 0.
    pub fn success(&self) -> bool {
        self.status == 0
    }
}

/// Errors from command execution.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    /// The process could not be spawned at all (missing binary, resource
    /// exhaustion, ...).
    #[error("failed to spawn {program}: {source}")]
    Spawn {
        /// Program that failed to spawn.
        program: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// I/O error while waiting on or reading from a spawned process.
    #[error("I/O error while executing {program}: {source}")]
    Io {
        /// Program that was being executed.
        program: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// [`ScriptedExecutor`] received a command no rule matched. The message
    /// contains the full rendered command line.
    #[error("unexpected command (no scripted rule matched): {0}")]
    Unexpected(String),
    /// A [`ScriptedExecutor`] rule was configured to fail with this message.
    #[error("scripted failure: {0}")]
    Scripted(String),
}

type FileCleanup = dyn Fn(&std::path::Path) -> std::io::Result<()> + Send + Sync;

/// A scoped temporary file. Explicit close reports cleanup failures; drop retries cleanup.
pub struct TemporaryFile {
    path: std::path::PathBuf,
    cleanup: Option<Box<FileCleanup>>,
}

impl TemporaryFile {
    /// Construct a file lease owned by an executor implementation.
    pub fn new(
        path: std::path::PathBuf,
        cleanup: impl Fn(&std::path::Path) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            path,
            cleanup: Some(Box::new(cleanup)),
        }
    }

    /// Path available for commands during this lease.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Remove the file, returning cleanup errors to the caller.
    pub fn close(mut self) -> std::io::Result<()> {
        if let Some(cleanup) = &self.cleanup {
            cleanup(&self.path)?;
        }
        self.cleanup = None;
        Ok(())
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if let Some(cleanup) = &self.cleanup {
            let _ = cleanup(&self.path);
        }
    }
}

/// An observable integration effect, recorded without performing host I/O by scripted executors.
#[derive(Debug, Clone)]
pub enum Effect {
    Command {
        spec: CommandSpec,
        timeout: Option<Duration>,
    },
    TemporaryFileCreated {
        path: std::path::PathBuf,
        contents: Vec<u8>,
    },
    TemporaryFileRemoved {
        path: std::path::PathBuf,
    },
    Sleep {
        duration: Duration,
    },
    Diagnostic {
        message: String,
    },
    Event {
        topic: String,
        payload: serde_json::Value,
    },
}

fn unsupported_effect() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "executor does not implement this effect",
    )
}

/// The boundary for integration commands, scoped files, clocks, delays, and diagnostics.
///
/// Object-safe and `Send + Sync` so a single `&dyn Executor` (or
/// `Arc<dyn Executor>`) can be threaded through DAG tasks.
pub trait Executor: Send + Sync {
    /// Allocate a scoped file. The default performs no filesystem I/O.
    fn temporary_file(&self, _prefix: &str, _contents: &[u8]) -> std::io::Result<TemporaryFile> {
        Err(unsupported_effect())
    }

    /// Elapsed monotonic time in this executor's clock domain.
    fn monotonic_now(&self) -> Duration {
        Duration::ZERO
    }

    /// Wall-clock timestamp for observable events; the default is deterministic.
    fn wall_now(&self) -> std::time::SystemTime {
        std::time::SystemTime::UNIX_EPOCH
    }

    /// Advance or wait on this executor's clock. The default never blocks.
    fn sleep(&self, _duration: Duration) -> std::io::Result<()> {
        Err(unsupported_effect())
    }

    /// Emit a diagnostic through the executor's configured output sink.
    fn diagnostic(&self, _message: &str) {}

    /// Emit a structured integration event to the executor's configured sink.
    fn event(&self, _topic: &str, _payload: &serde_json::Value) {}

    /// Emit one event with its replay payload and an optional in-process typed view.
    /// The default preserves existing JSON event sinks without retaining the borrowed value.
    fn event_with_native(
        &self,
        topic: &str,
        payload: &serde_json::Value,
        _native: &(dyn std::any::Any + Send + Sync),
    ) {
        self.event(topic, payload);
    }

    /// Run `spec` to completion and capture its output.
    fn exec(&self, spec: &CommandSpec) -> Result<ExecOutput, ExecError>;

    /// Run `spec`, killing it once it has run for `limit`. A killed command
    /// fails with [`ExecError::Io`] whose source has kind
    /// [`std::io::ErrorKind::TimedOut`].
    ///
    /// Executors without bounded execution fail before running the command.
    fn exec_timeout(&self, spec: &CommandSpec, _limit: Duration) -> Result<ExecOutput, ExecError> {
        Err(ExecError::Io {
            program: spec.program.clone(),
            source: unsupported_effect(),
        })
    }
}

/// The [`ExecError`] for a command killed at its time limit.
fn timed_out(program: String, limit: Duration) -> ExecError {
    ExecError::Io {
        program,
        source: std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("killed after {}s time limit", limit.as_secs_f64()),
        ),
    }
}

/// Production [`Executor`]: runs commands with [`std::process::Command`].
///
/// Local specs run directly; specs with [`CommandSpec::ssh`] set are wrapped
/// in the composed ssh invocation from [`ssh_argv`].
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessExecutor;

impl ProcessExecutor {
    /// Create a new `ProcessExecutor`.
    pub fn new() -> Self {
        Self
    }
}

impl ProcessExecutor {
    /// Spawn `spec` with stdin closed and stdout/stderr piped, as
    /// `Command::output()` does; `spawn()` alone inherits the parent's streams.
    fn spawn(spec: &CommandSpec, bounded: bool) -> Result<(String, Child), ExecError> {
        spec.validate_environment()
            .map_err(|source| ExecError::Io {
                program: spec.program.clone(),
                source,
            })?;
        let argv = spec.argv();
        let program = argv[0].clone();
        let mut cmd = Command::new(&program);
        cmd.args(&argv[1..]);
        #[cfg(unix)]
        if bounded {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        #[cfg(not(unix))]
        let _ = bounded;
        // For ssh specs the env/cwd are already folded into the remote
        // command string; applying them locally too would be wrong.
        if spec.ssh.is_none() {
            for (key, value) in &spec.env {
                cmd.env(key, value);
            }
            if let Some(cwd) = &spec.cwd {
                cmd.current_dir(cwd);
            }
        }
        let child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| ExecError::Spawn {
                program: program.clone(),
                source: e,
            })?;
        Ok((program, child))
    }
}

#[cfg(unix)]
fn nonblocking<R: std::os::fd::AsRawFd>(pipe: &Option<R>) -> std::io::Result<()> {
    if let Some(pipe) = pipe {
        let fd = pipe.as_raw_fd();
        // The descriptor belongs to this executor and remains open for both fcntl calls.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
        {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn read_available<R: Read>(pipe: &mut Option<R>, output: &mut Vec<u8>) -> std::io::Result<bool> {
    let Some(pipe) = pipe else { return Ok(true) };
    let mut bytes = [0; 8192];
    // Bound each pass so continuous output cannot starve the deadline check.
    for _ in 0..16 {
        match pipe.read(&mut bytes) {
            Ok(0) => return Ok(true),
            Ok(count) => output.extend_from_slice(&bytes[..count]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

impl Executor for ProcessExecutor {
    fn temporary_file(&self, prefix: &str, contents: &[u8]) -> std::io::Result<TemporaryFile> {
        use std::io::Write;
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        if prefix.is_empty()
            || !prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid temporary file prefix",
            ));
        }
        loop {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("{prefix}-{}-{id}", std::process::id()));
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = match options.open(&path) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            };
            let lease = TemporaryFile::new(path, |path| std::fs::remove_file(path));
            file.write_all(contents)?;
            file.flush()?;
            return Ok(lease);
        }
    }

    fn monotonic_now(&self) -> Duration {
        static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        START.get_or_init(std::time::Instant::now).elapsed()
    }

    fn wall_now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }

    fn sleep(&self, duration: Duration) -> std::io::Result<()> {
        std::thread::sleep(duration);
        Ok(())
    }

    fn diagnostic(&self, message: &str) {
        eprintln!("{message}");
    }

    fn exec(&self, spec: &CommandSpec) -> Result<ExecOutput, ExecError> {
        let (program, child) = Self::spawn(spec, false)?;
        let output = child
            .wait_with_output()
            .map_err(|e| ExecError::Io { program, source: e })?;
        Ok(ExecOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }

    fn exec_timeout(&self, spec: &CommandSpec, limit: Duration) -> Result<ExecOutput, ExecError> {
        #[cfg(not(unix))]
        {
            let _ = limit;
            return Err(ExecError::Io {
                program: spec.program.clone(),
                source: unsupported_effect(),
            });
        }
        #[cfg(unix)]
        {
            let started = Instant::now();
            let (program, mut child) = Self::spawn(spec, true)?;
            let group = child.id() as libc::pid_t;
            let mut stdout = child.stdout.take();
            let mut stderr = child.stderr.take();
            let mut out = Vec::new();
            let mut err = Vec::new();
            const POLL: Duration = Duration::from_millis(10);
            let result = (|| {
                nonblocking(&stdout)
                    .and_then(|_| nonblocking(&stderr))
                    .map_err(|source| ExecError::Io {
                        program: program.clone(),
                        source,
                    })?;
                let mut status = None;
                let mut out_eof = false;
                let mut err_eof = false;
                loop {
                    if !out_eof {
                        out_eof = read_available(&mut stdout, &mut out).map_err(|source| {
                            ExecError::Io {
                                program: program.clone(),
                                source,
                            }
                        })?;
                    }
                    if !err_eof {
                        err_eof = read_available(&mut stderr, &mut err).map_err(|source| {
                            ExecError::Io {
                                program: program.clone(),
                                source,
                            }
                        })?;
                    }
                    if status.is_none() {
                        status = child.try_wait().map_err(|source| ExecError::Io {
                            program: program.clone(),
                            source,
                        })?;
                    }
                    if let Some(exit) = status.filter(|_| out_eof && err_eof) {
                        return Ok(exit);
                    }
                    let elapsed = started.elapsed();
                    if elapsed >= limit {
                        return Err(timed_out(program.clone(), limit));
                    }
                    thread::sleep(POLL.min(limit - elapsed));
                }
            })();
            // The local process group owns subprocesses; remote job lifetimes require adapter cleanup.
            // The child starts in its own process group; a negative ID selects that group.
            let killed = unsafe { libc::kill(-group, libc::SIGKILL) };
            if killed != 0 {
                let source = std::io::Error::last_os_error();
                if source.raw_os_error() != Some(libc::ESRCH) {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(ExecError::Io { program, source });
                }
            }
            let _ = child.wait();
            let status = result?;
            Ok(ExecOutput {
                status: status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&out).into_owned(),
                stderr: String::from_utf8_lossy(&err).into_owned(),
            })
        }
    }
}

/// What a [`Rule`] matches against: the rendered command line
/// ([`CommandSpec::render`]).
enum Matcher {
    /// Every listed substring must appear in the rendered command.
    All(Vec<String>),
    /// Arbitrary predicate over the rendered command.
    Pred(Box<dyn Fn(&str) -> bool + Send + Sync>),
}

impl Matcher {
    fn matches(&self, rendered: &str) -> bool {
        match self {
            Matcher::All(subs) => subs.iter().all(|s| rendered.contains(s)),
            Matcher::Pred(f) => f(rendered),
        }
    }
}

/// What a matching [`Rule`] produces.
enum Outcome {
    Output(ExecOutput),
    Fail(String),
    /// Simulates a command that runs for `duration` and then yields `output`;
    /// no wall clock is spent.
    Slow {
        duration: Duration,
        output: ExecOutput,
    },
}

/// One scripted behavior for [`ScriptedExecutor`].
///
/// Rules are evaluated in order; the first match wins. Construct with the
/// `Rule::*` constructors or the [`ScriptedExecutor`] `on*` helpers.
pub struct Rule {
    matcher: Matcher,
    outcome: Outcome,
}

impl Rule {
    /// Match when the rendered command contains `substring`, returning `output`.
    pub fn containing(substring: &str, output: ExecOutput) -> Self {
        Self::containing_all([substring], output)
    }

    /// Match when the rendered command contains *all* of `substrings`.
    pub fn containing_all<I, S>(substrings: I, output: ExecOutput) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            matcher: Matcher::All(substrings.into_iter().map(Into::into).collect()),
            outcome: Outcome::Output(output),
        }
    }

    /// Match when `predicate` accepts the rendered command, returning `output`.
    pub fn matching<F>(predicate: F, output: ExecOutput) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        Self {
            matcher: Matcher::Pred(Box::new(predicate)),
            outcome: Outcome::Output(output),
        }
    }

    /// Match when the rendered command contains `substring`, failing with
    /// [`ExecError::Scripted`] carrying `message`.
    pub fn failing_containing(substring: &str, message: &str) -> Self {
        Self {
            matcher: Matcher::All(vec![substring.to_string()]),
            outcome: Outcome::Fail(message.to_string()),
        }
    }

    /// Match when the rendered command contains *all* of `substrings`,
    /// simulating a command that runs for `duration` before returning
    /// `output`. Under [`Executor::exec_timeout`] with a shorter limit it
    /// times out instead, as [`ProcessExecutor`] would. No wall clock is
    /// spent either way.
    pub fn slow_containing_all<I, S>(substrings: I, duration: Duration, output: ExecOutput) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            matcher: Matcher::All(substrings.into_iter().map(Into::into).collect()),
            outcome: Outcome::Slow { duration, output },
        }
    }

    /// Match when `predicate` accepts the rendered command, failing with
    /// [`ExecError::Scripted`] carrying `message`.
    pub fn failing_matching<F>(predicate: F, message: &str) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        Self {
            matcher: Matcher::Pred(Box::new(predicate)),
            outcome: Outcome::Fail(message.to_string()),
        }
    }
}

/// Test fake [`Executor`]: scripts outputs per command and records every
/// invocation.
///
/// # Matching
///
/// Each invocation is reduced to its rendered command line
/// ([`CommandSpec::render`]). Rules are checked in insertion order and the
/// first match wins — put specific rules before broad ones. An invocation
/// with no matching rule fails with [`ExecError::Unexpected`], which embeds
/// the rendered command so the test failure names the offending call.
///
/// # Recording
///
/// Every invocation (matched or not) is recorded under a `Mutex`, so a
/// `ScriptedExecutor` can be shared across threads (e.g. DAG tasks running
/// on a worker pool) and asserted on afterwards.
pub struct ScriptedExecutor {
    rules: Vec<Rule>,
    invocations: Mutex<Vec<String>>,
    effects: std::sync::Arc<Mutex<Vec<Effect>>>,
    files: std::sync::Arc<Mutex<std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>>>,
    clock: Mutex<Duration>,
    next_file: std::sync::atomic::AtomicU64,
}

impl Default for ScriptedExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptedExecutor {
    /// An executor with no rules — every invocation returns
    /// [`ExecError::Unexpected`].
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            invocations: Mutex::new(Vec::new()),
            effects: Default::default(),
            files: Default::default(),
            clock: Mutex::new(Duration::ZERO),
            next_file: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// An executor pre-loaded with `rules`.
    pub fn with_rules(rules: Vec<Rule>) -> Self {
        Self {
            rules,
            invocations: Mutex::new(Vec::new()),
            effects: Default::default(),
            files: Default::default(),
            clock: Mutex::new(Duration::ZERO),
            next_file: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Append a rule (builder style).
    pub fn rule(mut self, rule: Rule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Sugar for `.rule(Rule::containing(substring, output))`.
    pub fn on(self, substring: &str, output: ExecOutput) -> Self {
        self.rule(Rule::containing(substring, output))
    }

    /// Sugar for `.rule(Rule::failing_containing(substring, message))`.
    pub fn on_error(self, substring: &str, message: &str) -> Self {
        self.rule(Rule::failing_containing(substring, message))
    }

    /// Sugar for `.rule(Rule::matching(predicate, output))`.
    pub fn on_pred<F>(self, predicate: F, output: ExecOutput) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.rule(Rule::matching(predicate, output))
    }

    /// Typed effects in observation order, including temporary file cleanup.
    pub fn effects(&self) -> Vec<Effect> {
        self.effects.lock().unwrap().clone()
    }

    /// Contents of an active virtual temporary file.
    pub fn temporary_contents(&self, path: &std::path::Path) -> Option<Vec<u8>> {
        self.files.lock().unwrap().get(path).cloned()
    }

    /// All rendered command lines, in invocation order.
    pub fn invocations(&self) -> Vec<String> {
        self.invocations.lock().unwrap().clone()
    }

    /// Number of recorded invocations.
    pub fn invocation_count(&self) -> usize {
        self.invocations.lock().unwrap().len()
    }

    /// Index of the first invocation containing `substring`, if any.
    ///
    /// Use for ordering assertions, e.g. build must precede copy:
    ///
    /// ```
    /// # use consortium_integration::exec::*;
    /// # let exec = ScriptedExecutor::new().on("", ExecOutput::ok(""));
    /// # exec.exec(&CommandSpec::new("build")).unwrap();
    /// # exec.exec(&CommandSpec::new("copy")).unwrap();
    /// assert!(
    ///     exec.invocation_index_containing("build").unwrap()
    ///         < exec.invocation_index_containing("copy").unwrap()
    /// );
    /// ```
    pub fn invocation_index_containing(&self, substring: &str) -> Option<usize> {
        self.invocations
            .lock()
            .unwrap()
            .iter()
            .position(|cmd| cmd.contains(substring))
    }

    /// Panic unless at least one invocation contained `substring`.
    pub fn assert_invoked_containing(&self, substring: &str) {
        let invocations = self.invocations();
        assert!(
            invocations.iter().any(|cmd| cmd.contains(substring)),
            "expected an invocation containing {:?}, got: {:?}",
            substring,
            invocations
        );
    }

    /// Panic if any invocation contained `substring`.
    pub fn assert_not_invoked_containing(&self, substring: &str) {
        let invocations = self.invocations();
        assert!(
            !invocations.iter().any(|cmd| cmd.contains(substring)),
            "expected no invocation containing {:?}, got: {:?}",
            substring,
            invocations
        );
    }
}

impl ScriptedExecutor {
    fn run(&self, spec: &CommandSpec, limit: Option<Duration>) -> Result<ExecOutput, ExecError> {
        spec.validate_environment()
            .map_err(|source| ExecError::Io {
                program: spec.program.clone(),
                source,
            })?;
        self.effects.lock().unwrap().push(Effect::Command {
            spec: spec.clone(),
            timeout: limit,
        });
        let rendered = spec.render();
        self.invocations.lock().unwrap().push(rendered.clone());
        for rule in &self.rules {
            if rule.matcher.matches(&rendered) {
                return match &rule.outcome {
                    Outcome::Output(output) => Ok(output.clone()),
                    Outcome::Fail(message) => Err(ExecError::Scripted(message.clone())),
                    Outcome::Slow { duration, output } => {
                        let elapsed = limit.map_or(*duration, |limit| limit.min(*duration));
                        let mut clock = self.clock.lock().unwrap();
                        *clock = clock.saturating_add(elapsed);
                        match limit {
                            Some(limit) if *duration > limit => Err(timed_out(rendered, limit)),
                            _ => Ok(output.clone()),
                        }
                    }
                };
            }
        }
        Err(ExecError::Unexpected(rendered))
    }
}

impl Executor for ScriptedExecutor {
    fn temporary_file(&self, prefix: &str, contents: &[u8]) -> std::io::Result<TemporaryFile> {
        if prefix.is_empty()
            || !prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid temporary file prefix",
            ));
        }
        let id = self
            .next_file
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::path::PathBuf::from(format!("/virtual/{prefix}-{id}"));
        self.files
            .lock()
            .unwrap()
            .insert(path.clone(), contents.to_vec());
        self.effects
            .lock()
            .unwrap()
            .push(Effect::TemporaryFileCreated {
                path: path.clone(),
                contents: contents.to_vec(),
            });
        let files = self.files.clone();
        let effects = self.effects.clone();
        Ok(TemporaryFile::new(path, move |path| {
            files.lock().unwrap().remove(path);
            effects.lock().unwrap().push(Effect::TemporaryFileRemoved {
                path: path.to_owned(),
            });
            Ok(())
        }))
    }

    fn monotonic_now(&self) -> Duration {
        *self.clock.lock().unwrap()
    }

    fn wall_now(&self) -> std::time::SystemTime {
        std::time::SystemTime::UNIX_EPOCH
            .checked_add(self.monotonic_now())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
    }

    fn sleep(&self, duration: Duration) -> std::io::Result<()> {
        let mut clock = self.clock.lock().unwrap();
        *clock = clock.saturating_add(duration);
        self.effects
            .lock()
            .unwrap()
            .push(Effect::Sleep { duration });
        Ok(())
    }

    fn diagnostic(&self, message: &str) {
        self.effects.lock().unwrap().push(Effect::Diagnostic {
            message: message.to_owned(),
        });
    }

    fn event(&self, topic: &str, payload: &serde_json::Value) {
        self.effects.lock().unwrap().push(Effect::Event {
            topic: topic.to_owned(),
            payload: payload.clone(),
        });
    }

    fn exec(&self, spec: &CommandSpec) -> Result<ExecOutput, ExecError> {
        self.run(spec, None)
    }

    fn exec_timeout(&self, spec: &CommandSpec, limit: Duration) -> Result<ExecOutput, ExecError> {
        self.run(spec, Some(limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- shell_quote ----------

    #[test]
    fn shell_quote_simple() {
        assert_eq!(shell_quote("nix"), "'nix'");
    }

    #[test]
    fn shell_quote_spaces() {
        assert_eq!(shell_quote("hello world"), "'hello world'");
    }

    #[test]
    fn shell_quote_single_quote() {
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn shell_quote_empty() {
        assert_eq!(shell_quote(""), "''");
    }

    // ---------- ssh_argv composition ----------

    #[test]
    fn ssh_argv_defaults() {
        let argv = ssh_argv(&SshTarget::new("root", "node01"), "'uptime'");
        assert_eq!(
            argv,
            vec![
                "ssh",
                "-oStrictHostKeyChecking=no",
                "-oPasswordAuthentication=no",
                "-oBatchMode=yes",
                "-l",
                "root",
                "--",
                "node01",
                "'uptime'",
            ]
        );
    }

    #[test]
    fn ssh_argv_with_port_and_extra_opts() {
        let target = SshTarget::new("admin", "10.0.0.5").port(2222).extra_opts([
            "-i",
            "/keys/id_ed25519",
            "-oProxyJump=bastion",
        ]);
        let argv = ssh_argv(&target, "'systemctl' 'status'");
        assert_eq!(
            argv,
            vec![
                "ssh",
                "-oStrictHostKeyChecking=no",
                "-oPasswordAuthentication=no",
                "-oBatchMode=yes",
                "-l",
                "admin",
                "-p",
                "2222",
                "-i",
                "/keys/id_ed25519",
                "-oProxyJump=bastion",
                "--",
                "10.0.0.5",
                "'systemctl' 'status'",
            ]
        );
        // Extra opts come after defaults, so they can override them.
        let override_target = SshTarget::new("root", "h").extra_opt("-oBatchMode=no");
        let argv = ssh_argv(&override_target, "'true'");
        let batch_mode_positions: Vec<usize> = argv
            .iter()
            .enumerate()
            .filter(|(_, a)| a.starts_with("-oBatchMode"))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(batch_mode_positions.len(), 2);
        assert!(argv[batch_mode_positions[1]] == "-oBatchMode=no");
    }

    #[test]
    fn remote_command_quotes_args() {
        let spec = CommandSpec::new("sbatch")
            .args(["--job-name=my job", "run.sh"])
            .ssh(SshTarget::new("root", "submit01"));
        assert_eq!(
            spec.remote_command(),
            "'sbatch' '--job-name=my job' 'run.sh'"
        );
        let argv = spec.argv();
        assert_eq!(argv.first().unwrap(), "ssh");
        assert_eq!(
            argv.last().unwrap(),
            "'sbatch' '--job-name=my job' 'run.sh'"
        );
    }

    #[test]
    fn remote_command_env_and_cwd() {
        let spec = CommandSpec::new("make")
            .env("CFLAGS", "-O2 -Wall")
            .cwd("/srv/build dir")
            .ssh(SshTarget::new("root", "b1"));
        assert_eq!(
            spec.remote_command(),
            "cd '/srv/build dir' && CFLAGS='-O2 -Wall' 'make'"
        );
    }

    #[test]
    fn render_local_is_program_plus_args() {
        let spec = CommandSpec::new("nix").args(["build", ".#pkg", "--no-link"]);
        assert_eq!(spec.render(), "nix build .#pkg --no-link");
    }

    #[test]
    fn render_ssh_shows_full_invocation() {
        let spec = CommandSpec::new("uptime").ssh(SshTarget::new("root", "n1").port(2222));
        let rendered = spec.render();
        assert!(rendered.starts_with("ssh -oStrictHostKeyChecking=no"));
        assert!(rendered.contains("-l root"));
        assert!(rendered.contains("-p 2222"));
        assert!(rendered.contains("n1"));
        assert!(rendered.contains("'uptime'"));
    }

    // ---------- ProcessExecutor (real spawns, unix only) ----------

    #[cfg(unix)]
    #[test]
    fn process_executor_success() {
        let out = ProcessExecutor::new()
            .exec(&CommandSpec::new("sh").args(["-c", "echo out; echo err >&2"]))
            .unwrap();
        assert!(out.success());
        assert_eq!(out.stdout.trim(), "out");
        assert_eq!(out.stderr.trim(), "err");
    }

    #[cfg(unix)]
    #[test]
    fn process_executor_nonzero_exit() {
        let out = ProcessExecutor::new()
            .exec(&CommandSpec::new("sh").args(["-c", "exit 42"]))
            .unwrap();
        assert_eq!(out.status, 42);
        assert!(!out.success());
    }

    #[cfg(unix)]
    #[test]
    fn process_executor_env_and_cwd() {
        let out = ProcessExecutor::new()
            .exec(
                &CommandSpec::new("sh")
                    .args(["-c", "echo $CONSORTIUM_TEST_VAR; pwd"])
                    .env("CONSORTIUM_TEST_VAR", "hello")
                    .cwd("/tmp"),
            )
            .unwrap();
        assert!(out.success());
        let lines: Vec<&str> = out.stdout.trim().lines().collect();
        assert_eq!(lines[0], "hello");
        assert!(lines[1].ends_with("tmp"));
    }

    #[test]
    fn process_executor_spawn_failure() {
        let err = ProcessExecutor::new()
            .exec(&CommandSpec::new("/definitely/not/a/real/binary"))
            .unwrap_err();
        match err {
            ExecError::Spawn { program, .. } => {
                assert_eq!(program, "/definitely/not/a/real/binary")
            }
            other => panic!("expected Spawn error, got: {}", other),
        }
    }

    // ---------- ScriptedExecutor ----------

    fn spec(cmdline: &str) -> CommandSpec {
        let mut parts = cmdline.split(' ');
        CommandSpec::new(parts.next().unwrap()).args(parts)
    }

    #[test]
    fn scripted_matches_substring() {
        let exec = ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/abc\n"));
        let out = exec.exec(&spec("nix build .#hello --no-link")).unwrap();
        assert_eq!(out.stdout, "/nix/store/abc\n");
    }

    #[test]
    fn scripted_first_match_wins() {
        // Broad rule first would shadow the specific one — order matters.
        let exec = ScriptedExecutor::new()
            .on("nix build .#hello", ExecOutput::ok("specific"))
            .on("nix build", ExecOutput::ok("broad"));
        let out = exec.exec(&spec("nix build .#hello")).unwrap();
        assert_eq!(out.stdout, "specific");
    }

    #[test]
    fn scripted_containing_all_requires_every_substring() {
        let exec = ScriptedExecutor::new().rule(Rule::containing_all(
            ["nix", "copy", "ssh-ng://root@node01"],
            ExecOutput::ok(""),
        ));
        assert!(exec
            .exec(&spec("nix copy --to ssh-ng://root@node02 /nix/store/x"))
            .is_err());
        assert!(exec
            .exec(&spec("nix copy --to ssh-ng://root@node01 /nix/store/x"))
            .is_ok());
    }

    #[test]
    fn scripted_predicate_rule() {
        let exec = ScriptedExecutor::new().on_pred(
            |cmd| cmd.contains("sbatch") && cmd.contains("--job-name=train"),
            ExecOutput::ok("Submitted batch job 12345\n"),
        );
        let out = exec
            .exec(&spec("sbatch --job-name=train train.sh"))
            .unwrap();
        assert!(out.stdout.contains("12345"));
    }

    #[test]
    fn scripted_error_rule() {
        let exec = ScriptedExecutor::new().on_error("ssh", "connection refused");
        let err = exec.exec(&spec("ssh node01 uptime")).unwrap_err();
        match err {
            ExecError::Scripted(msg) => assert_eq!(msg, "connection refused"),
            other => panic!("expected Scripted, got: {}", other),
        }
    }

    #[test]
    fn scripted_unmatched_is_obvious() {
        let exec = ScriptedExecutor::new().on("nix build", ExecOutput::ok(""));
        let err = exec.exec(&spec("rm -rf /tmp/x")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unexpected command"), "{}", msg);
        assert!(msg.contains("rm -rf /tmp/x"), "{}", msg);
    }

    #[test]
    fn scripted_records_all_invocations_in_order() {
        let exec = ScriptedExecutor::new().on("", ExecOutput::ok(""));
        exec.exec(&spec("eval host1")).ok();
        exec.exec(&spec("build host1")).ok();
        exec.exec(&spec("copy host1")).ok();
        assert_eq!(
            exec.invocations(),
            vec!["eval host1", "build host1", "copy host1"]
        );
        assert_eq!(exec.invocation_count(), 3);
    }

    #[test]
    fn scripted_records_unmatched_invocations_too() {
        let exec = ScriptedExecutor::new();
        exec.exec(&spec("surprise command")).ok();
        assert_eq!(exec.invocations(), vec!["surprise command"]);
    }

    #[test]
    fn scripted_invocation_index_for_ordering() {
        let exec = ScriptedExecutor::new().on("", ExecOutput::ok(""));
        exec.exec(&spec("build h1")).ok();
        exec.exec(&spec("copy h1")).ok();
        let build_idx = exec.invocation_index_containing("build").unwrap();
        let copy_idx = exec.invocation_index_containing("copy").unwrap();
        assert!(build_idx < copy_idx);
        assert_eq!(exec.invocation_index_containing("activate"), None);
    }

    #[test]
    fn scripted_assert_helpers_pass() {
        let exec = ScriptedExecutor::new().on("", ExecOutput::ok(""));
        exec.exec(&spec("nix build .#a")).ok();
        exec.assert_invoked_containing("nix build");
        exec.assert_not_invoked_containing("nix copy");
    }

    #[test]
    #[should_panic(expected = "expected an invocation containing")]
    fn scripted_assert_invoked_fails() {
        let exec = ScriptedExecutor::new();
        exec.assert_invoked_containing("nix build");
    }

    #[test]
    #[should_panic(expected = "expected no invocation containing")]
    fn scripted_assert_not_invoked_fails() {
        let exec = ScriptedExecutor::new().on("", ExecOutput::ok(""));
        exec.exec(&spec("nix copy --to x y")).ok();
        exec.assert_not_invoked_containing("nix copy");
    }

    #[test]
    fn scripted_shared_across_threads() {
        use std::sync::Arc;
        let exec = Arc::new(ScriptedExecutor::new().on("", ExecOutput::ok("")));
        std::thread::scope(|s| {
            for i in 0..4 {
                let exec = Arc::clone(&exec);
                s.spawn(move || {
                    exec.exec(&CommandSpec::new(format!("worker-{}", i))).ok();
                });
            }
        });
        assert_eq!(exec.invocation_count(), 4);
        for i in 0..4 {
            exec.assert_invoked_containing(&format!("worker-{}", i));
        }
    }

    #[test]
    fn scripted_matches_ssh_rendered_line() {
        let exec = ScriptedExecutor::new().on("sbatch", ExecOutput::ok("Submitted batch job 7\n"));
        let spec = CommandSpec::new("sbatch")
            .args(["--job-name=x", "x.sh"])
            .ssh(SshTarget::new("root", "submit01"));
        exec.exec(&spec).unwrap();
        exec.assert_invoked_containing("ssh -oStrictHostKeyChecking=no");
        exec.assert_invoked_containing("root");
        exec.assert_invoked_containing("submit01");
        exec.assert_invoked_containing("sbatch");
    }

    // ---------- exec_timeout ----------

    fn is_timeout(err: &ExecError) -> bool {
        matches!(err, ExecError::Io { source, .. } if source.kind() == std::io::ErrorKind::TimedOut)
    }

    #[test]
    fn process_exec_timeout_kills_a_command_past_its_limit() {
        let spec = CommandSpec::new("sh").args(["-c", "sleep 30"]);
        let started = Instant::now();
        let err = ProcessExecutor::new()
            .exec_timeout(&spec, Duration::from_millis(300))
            .unwrap_err();
        assert!(is_timeout(&err), "expected a timeout, got: {err}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "kill landed late"
        );
    }

    #[test]
    fn process_exec_timeout_returns_full_output_beyond_the_pipe_buffer() {
        // 1 MiB on stdout: a parent that polls without draining would leave
        // the child blocked on a full pipe and kill it at the limit.
        let spec = CommandSpec::new("sh").args([
            "-c",
            "head -c 1048576 /dev/zero | tr '\\0' x; echo err >&2; exit 3",
        ]);
        let out = ProcessExecutor::new()
            .exec_timeout(&spec, Duration::from_secs(30))
            .unwrap();
        assert_eq!(out.status, 3);
        assert_eq!(out.stdout.len(), 1 << 20);
        assert_eq!(out.stderr, "err\n");
    }

    #[test]
    fn scripted_slow_rule_times_out_only_under_a_shorter_limit() {
        let exec = ScriptedExecutor::new().rule(Rule::slow_containing_all(
            ["activate"],
            Duration::from_secs(60),
            ExecOutput::ok("done"),
        ));
        let spec = CommandSpec::new("activate");
        assert_eq!(exec.exec(&spec).unwrap().stdout, "done");
        assert_eq!(
            exec.exec_timeout(&spec, Duration::from_secs(120))
                .unwrap()
                .stdout,
            "done"
        );
        let err = exec
            .exec_timeout(&spec, Duration::from_secs(10))
            .unwrap_err();
        assert!(is_timeout(&err), "expected a timeout, got: {err}");
    }
}

#[cfg(test)]
mod effect_tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct CommandsOnly;
    impl Executor for CommandsOnly {
        fn exec(&self, _: &CommandSpec) -> Result<ExecOutput, ExecError> {
            panic!("default effect methods must never execute a command")
        }
    }

    #[test]
    fn unsupported_effect_defaults_never_touch_the_host() {
        let exec = CommandsOnly;
        assert_eq!(exec.monotonic_now(), Duration::ZERO);
        assert_eq!(
            exec.sleep(Duration::from_secs(100)).unwrap_err().kind(),
            std::io::ErrorKind::Unsupported
        );
        assert_eq!(
            exec.temporary_file("fixture", b"private")
                .err()
                .unwrap()
                .kind(),
            std::io::ErrorKind::Unsupported
        );
        exec.diagnostic("ignored");
        let error = exec
            .exec_timeout(&CommandSpec::new("must-not-run"), Duration::from_secs(1))
            .unwrap_err();
        assert!(
            matches!(error, ExecError::Io { source, .. } if source.kind() == std::io::ErrorKind::Unsupported)
        );
    }

    #[test]
    fn scripted_files_are_virtual_and_scoped() {
        let exec = ScriptedExecutor::new();
        let file = exec.temporary_file("fixture", b"private\0bytes").unwrap();
        let path = file.path().to_owned();
        assert_eq!(exec.temporary_contents(&path).unwrap(), b"private\0bytes");
        assert!(!path.exists());
        drop(file);
        assert!(exec.temporary_contents(&path).is_none());
        assert!(
            matches!(&exec.effects()[..], [Effect::TemporaryFileCreated {contents, ..}, Effect::TemporaryFileRemoved {path: removed}] if contents == b"private\0bytes" && removed == &path)
        );
    }

    #[test]
    fn explicit_close_removes_once_and_does_not_double_clean_on_drop() {
        let count = Arc::new(AtomicUsize::new(0));
        let cloned = count.clone();
        TemporaryFile::new("/virtual/close".into(), move |_| {
            cloned.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .close()
        .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cleanup_error_is_reported_and_drop_retries() {
        let count = Arc::new(AtomicUsize::new(0));
        let cloned = count.clone();
        let file = TemporaryFile::new("/virtual/retry".into(), move |_| {
            if cloned.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected cleanup failure",
                ))
            } else {
                Ok(())
            }
        });
        assert_eq!(
            file.close().unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn unwinding_cleans_temporary_resources() {
        let exec = ScriptedExecutor::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _file = exec.temporary_file("fixture", b"data").unwrap();
            panic!("injected command panic");
        }));
        assert!(result.is_err());
        assert!(matches!(
            exec.effects().last(),
            Some(Effect::TemporaryFileRemoved { .. })
        ));
    }

    #[test]
    fn scripted_effect_order_and_virtual_clock_are_deterministic() {
        let exec = ScriptedExecutor::new().on("echo", ExecOutput::ok("ok"));
        exec.diagnostic("start");
        exec.sleep(Duration::from_secs(7)).unwrap();
        exec.exec_timeout(&CommandSpec::new("echo").arg("ok"), Duration::from_secs(3))
            .unwrap();
        assert_eq!(exec.monotonic_now(), Duration::from_secs(7));
        assert!(
            matches!(&exec.effects()[..], [Effect::Diagnostic{message}, Effect::Sleep{duration}, Effect::Command{spec,timeout}] if message == "start" && *duration == Duration::from_secs(7) && spec.program == "echo" && *timeout == Some(Duration::from_secs(3)))
        );
    }

    #[test]
    fn temporary_paths_are_unique_and_contents_are_independent() {
        for exec in [&ProcessExecutor as &dyn Executor, &ScriptedExecutor::new()] {
            let first = exec
                .temporary_file("consortium-effect-test", b"first")
                .unwrap();
            let second = exec
                .temporary_file("consortium-effect-test", b"second")
                .unwrap();
            assert_ne!(first.path(), second.path());
            first.close().unwrap();
            second.close().unwrap();
        }
    }

    #[test]
    fn process_temporary_file_is_private_and_removed_after_use() {
        let file = ProcessExecutor
            .temporary_file("consortium-effect-test", b"payload")
            .unwrap();
        let path = file.path().to_owned();
        assert_eq!(std::fs::read(&path).unwrap(), b"payload");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        file.close().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn temporary_prefix_rejects_path_traversal_for_both_backends() {
        for exec in [&ProcessExecutor as &dyn Executor, &ScriptedExecutor::new()] {
            for prefix in ["", "../escape", "a/b", "a\\b", "has space", "é"] {
                assert_eq!(
                    exec.temporary_file(prefix, b"secret").err().unwrap().kind(),
                    std::io::ErrorKind::InvalidInput
                );
            }
        }
    }
}

#[cfg(all(test, unix))]
mod process_group_tests {
    use super::*;

    fn verify_descendant_is_killed(shell_exits: bool, redirected: bool) {
        let file = ProcessExecutor
            .temporary_file("consortium-descendant-test", b"")
            .unwrap();
        let path = file.path().to_owned();
        let command = format!(
            "(sleep 1; printf escaped > {}) {} & {}",
            shell_quote(path.to_str().unwrap()),
            if redirected { ">/dev/null 2>&1" } else { "" },
            if shell_exits { "exit 0" } else { "wait" }
        );
        let started = Instant::now();
        let result = ProcessExecutor.exec_timeout(
            &CommandSpec::new("sh").args(["-c", command.as_str()]),
            Duration::from_millis(400),
        );
        if !shell_exits || !redirected {
            assert!(
                matches!(result, Err(ExecError::Io { source, .. }) if source.kind() == std::io::ErrorKind::TimedOut)
            );
        } else {
            assert!(result.unwrap().success());
        }
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "descendant pipe escaped the deadline"
        );
        std::thread::sleep(Duration::from_millis(900));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"",
            "descendant survived its process group"
        );
        file.close().unwrap();
    }

    #[test]
    fn timed_out_shell_cannot_leave_running_descendant() {
        verify_descendant_is_killed(false, false);
    }

    #[test]
    fn exited_shell_with_inherited_pipes_cannot_bypass_deadline() {
        verify_descendant_is_killed(true, false);
    }

    #[test]
    fn successful_foreground_also_cleans_background_processes() {
        verify_descendant_is_killed(true, true);
    }
}

#[cfg(test)]
mod environment_clock_tests {
    use super::*;

    #[test]
    fn invalid_environment_keys_are_rejected_before_any_local_or_remote_process() {
        let marker = ProcessExecutor
            .temporary_file("consortium-env-test", b"")
            .unwrap();
        let malicious = format!(
            "X=1; printf injected > {}; #",
            shell_quote(marker.path().to_str().unwrap())
        );
        for key in [
            "",
            "1NAME",
            "BAD-NAME",
            "A=B",
            "A B",
            "A\nB",
            "é",
            "A\0B",
            malicious.as_str(),
        ] {
            for remote in [false, true] {
                let mut spec = CommandSpec::new("sh")
                    .args(["-c", "exit 0"])
                    .env(key, "value");
                if remote {
                    spec = spec.ssh(SshTarget::new("root", "must-not-contact.invalid"));
                }
                assert!(
                    matches!(ProcessExecutor.exec(&spec), Err(ExecError::Io {source, ..}) if source.kind() == std::io::ErrorKind::InvalidInput)
                );
                assert!(
                    matches!(ProcessExecutor.exec_timeout(&spec, Duration::from_secs(1)), Err(ExecError::Io {source, ..}) if source.kind() == std::io::ErrorKind::InvalidInput)
                );
                let scripted = ScriptedExecutor::new().on("", ExecOutput::ok(""));
                assert!(scripted.exec(&spec).is_err());
                assert!(scripted.effects().is_empty());
                assert!(scripted.invocations().is_empty());
                assert_eq!(spec.remote_command(), "false");
            }
        }
        assert_eq!(std::fs::read(marker.path()).unwrap(), b"");
        marker.close().unwrap();
    }

    #[test]
    fn valid_identifiers_and_quoted_values_keep_their_contents() {
        for key in ["A", "_", "_lower_1", "aBC123"] {
            let value = "space ' quote ; $(touch never)";
            let spec = CommandSpec::new("true").env(key, value);
            assert!(spec.validate_environment().is_ok());
            assert_eq!(
                spec.remote_command(),
                format!("{key}={} 'true'", shell_quote(value))
            );
        }
        let spec = CommandSpec::new("sh")
            .args(["-c", "printf '%s' \"$TEST_value1\""])
            .env("TEST_value1", "literal ; ' $(no-command)");
        assert_eq!(
            ProcessExecutor.exec(&spec).unwrap().stdout,
            "literal ; ' $(no-command)"
        );
    }

    #[test]
    fn wall_clock_defaults_are_deterministic_and_scripted_time_advances_them() {
        struct NoEffects;
        impl Executor for NoEffects {
            fn exec(&self, _: &CommandSpec) -> Result<ExecOutput, ExecError> {
                panic!("no command expected")
            }
        }
        assert_eq!(NoEffects.wall_now(), std::time::SystemTime::UNIX_EPOCH);
        let exec = ScriptedExecutor::new();
        assert_eq!(exec.wall_now(), std::time::SystemTime::UNIX_EPOCH);
        exec.sleep(Duration::from_secs(7)).unwrap();
        assert_eq!(
            exec.wall_now(),
            std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(7)
        );
        let before = std::time::SystemTime::now();
        let observed = ProcessExecutor.wall_now();
        assert!(observed >= before && observed <= std::time::SystemTime::now());
    }
}

#[cfg(all(test, unix))]
mod escaped_pipe_tests {
    use super::*;

    #[test]
    #[ignore = "subprocess fixture for escaped_session_cannot_extend_pipe_deadline"]
    fn escaped_pipe_holder_helper() {
        let path = std::env::var("CONSORTIUM_ESCAPE_PID_PATH").unwrap();
        let mut ready = [0; 2];
        // The fixture child calls only async-signal-safe libc functions after fork.
        unsafe {
            assert_eq!(libc::pipe(ready.as_mut_ptr()), 0);
            let pid = libc::fork();
            assert!(pid >= 0);
            if pid == 0 {
                libc::close(ready[0]);
                if libc::setsid() == -1 {
                    libc::_exit(2);
                }
                let byte = [1u8];
                libc::write(ready[1], byte.as_ptr().cast(), 1);
                libc::close(ready[1]);
                libc::sleep(3);
                libc::_exit(0);
            }
            libc::close(ready[1]);
            let mut byte = [0u8];
            assert_eq!(libc::read(ready[0], byte.as_mut_ptr().cast(), 1), 1);
            libc::close(ready[0]);
            assert_eq!(byte[0], 1);
            std::fs::write(path, pid.to_string()).unwrap();
        }
    }

    #[test]
    fn escaped_session_cannot_extend_pipe_deadline() {
        struct KillEscaped(std::path::PathBuf);
        impl Drop for KillEscaped {
            fn drop(&mut self) {
                if let Ok(contents) = std::fs::read_to_string(&self.0) {
                    if let Ok(pid) = contents.parse::<libc::pid_t>() {
                        // This PID was explicitly recorded by the owned fixture process.
                        unsafe {
                            libc::kill(pid, libc::SIGKILL);
                        }
                    }
                }
            }
        }
        let file = ProcessExecutor
            .temporary_file("consortium-escape-test", b"")
            .unwrap();
        let guard = KillEscaped(file.path().to_owned());
        let spec = CommandSpec::new(std::env::current_exe().unwrap().to_string_lossy())
            .args([
                "--exact",
                "exec::escaped_pipe_tests::escaped_pipe_holder_helper",
                "--ignored",
                "--nocapture",
            ])
            .env("CONSORTIUM_ESCAPE_PID_PATH", file.path().to_str().unwrap());
        let started = Instant::now();
        let result = ProcessExecutor.exec_timeout(&spec, Duration::from_millis(300));
        let elapsed = started.elapsed();
        let pid = std::fs::read_to_string(file.path())
            .unwrap()
            .parse::<libc::pid_t>()
            .unwrap();
        assert!(pid > 1);
        drop(guard);
        let cleanup_started = Instant::now();
        loop {
            // This PID was recorded by the owned fixture before it escaped the local group.
            if unsafe { libc::kill(pid, 0) } == -1 {
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::ESRCH)
                );
                break;
            }
            assert!(
                cleanup_started.elapsed() < Duration::from_secs(2),
                "escaped fixture PID was not reaped"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            matches!(result, Err(ExecError::Io {source, ..}) if source.kind() == std::io::ErrorKind::TimedOut)
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "escaped pipe holder bypassed the deadline: {elapsed:?}"
        );
        file.close().unwrap();
    }
}

#[cfg(test)]
mod event_tests {
    use super::*;

    #[test]
    fn scripted_events_capture_topic_and_payload_at_emission() {
        let exec = ScriptedExecutor::new();
        let mut payload = serde_json::json!({"node": "a", "values": [1, 2]});
        exec.event("cascade", &payload);
        payload["node"] = serde_json::json!("b");
        assert!(
            matches!(&exec.effects()[..], [Effect::Event {topic, payload}] if topic == "cascade" && payload["node"] == "a" && payload["values"] == serde_json::json!([1, 2]))
        );
    }
}
