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

use std::process::Command;
use std::sync::Mutex;

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

/// The contract every integration uses to run external commands.
///
/// Object-safe and `Send + Sync` so a single `&dyn Executor` (or
/// `Arc<dyn Executor>`) can be threaded through DAG tasks.
pub trait Executor: Send + Sync {
    /// Run `spec` to completion and capture its output.
    fn exec(&self, spec: &CommandSpec) -> Result<ExecOutput, ExecError>;
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

impl Executor for ProcessExecutor {
    fn exec(&self, spec: &CommandSpec) -> Result<ExecOutput, ExecError> {
        let argv = spec.argv();
        let program = argv[0].clone();
        let mut cmd = Command::new(&program);
        cmd.args(&argv[1..]);
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

        // Mirror what `Command::output()` sets up so `wait_with_output` can
        // actually capture: spawn() alone inherits the parent's streams.
        let child = cmd
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| ExecError::Spawn {
                program: program.clone(),
                source: e,
            })?;
        let output = child.wait_with_output().map_err(|e| ExecError::Io {
            program: program.clone(),
            source: e,
        })?;

        Ok(ExecOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
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
        }
    }

    /// An executor pre-loaded with `rules`.
    pub fn with_rules(rules: Vec<Rule>) -> Self {
        Self {
            rules,
            invocations: Mutex::new(Vec::new()),
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

impl Executor for ScriptedExecutor {
    fn exec(&self, spec: &CommandSpec) -> Result<ExecOutput, ExecError> {
        let rendered = spec.render();
        self.invocations.lock().unwrap().push(rendered.clone());
        for rule in &self.rules {
            if rule.matcher.matches(&rendered) {
                return match &rule.outcome {
                    Outcome::Output(output) => Ok(output.clone()),
                    Outcome::Fail(message) => Err(ExecError::Scripted(message.clone())),
                };
            }
        }
        Err(ExecError::Unexpected(rendered))
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
                "node01",
                "'uptime'",
            ]
        );
    }

    #[test]
    fn ssh_argv_with_port_and_extra_opts() {
        let target = SshTarget::new("admin", "10.0.0.5")
            .port(2222)
            .extra_opts(["-i", "/keys/id_ed25519", "-oProxyJump=bastion"]);
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
        assert_eq!(argv.last().unwrap(), "'sbatch' '--job-name=my job' 'run.sh'");
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
        assert!(exec.exec(&spec("nix copy --to ssh-ng://root@node02 /nix/store/x")).is_err());
        assert!(exec.exec(&spec("nix copy --to ssh-ng://root@node01 /nix/store/x")).is_ok());
    }

    #[test]
    fn scripted_predicate_rule() {
        let exec = ScriptedExecutor::new().on_pred(
            |cmd| cmd.contains("sbatch") && cmd.contains("--job-name=train"),
            ExecOutput::ok("Submitted batch job 12345\n"),
        );
        let out = exec.exec(&spec("sbatch --job-name=train train.sh")).unwrap();
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
        assert_eq!(exec.invocations(), vec!["eval host1", "build host1", "copy host1"]);
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
        let exec =
            ScriptedExecutor::new().on("sbatch", ExecOutput::ok("Submitted batch job 7\n"));
        let spec = CommandSpec::new("sbatch")
            .args(["--job-name=x", "x.sh"])
            .ssh(SshTarget::new("root", "submit01"));
        exec.exec(&spec).unwrap();
        exec.assert_invoked_containing("ssh -oStrictHostKeyChecking=no");
        exec.assert_invoked_containing("root");
        exec.assert_invoked_containing("submit01");
        exec.assert_invoked_containing("sbatch");
    }
}
