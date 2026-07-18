//! Nix store staging helpers, ported onto the [`Executor`] abstraction.
//!
//! These are the generic primitives every tool integration needs before it
//! can run its own payload: build a flake attribute into a store path, and
//! copy that path to a remote store. They are behavior-identical ports of
//! `consortium_nix::build::build_flake_attr` and
//! `consortium_nix::copy::copy_closure`, but take an [`Executor`] instead of
//! shelling out directly — so integrations can be tested against
//! [`crate::exec::ScriptedExecutor`] with no nix installation present.
//!
//! # Example
//!
//! ```
//! use consortium_integration::exec::{ExecOutput, ScriptedExecutor};
//! use consortium_integration::staging;
//!
//! let exec = ScriptedExecutor::new()
//!     .on("nix build", ExecOutput::ok("/nix/store/abc-env\n"))
//!     .on("nix copy", ExecOutput::ok(""));
//!
//! let path = staging::build_flake_attr(&exec, ".#slurmEnvs.train", None)?;
//! staging::copy_closure(&exec, &path, "ssh-ng://root@submit01")?;
//!
//! exec.assert_invoked_containing("nix build .#slurmEnvs.train --no-link --print-out-paths");
//! exec.assert_invoked_containing("--to ssh-ng://root@submit01");
//! # Ok::<(), staging::StagingError>(())
//! ```

use crate::exec::{CommandSpec, ExecError, Executor};

/// Errors from staging operations.
#[derive(Debug, thiserror::Error)]
pub enum StagingError {
    /// The underlying command could not be executed at all.
    #[error("failed to execute nix: {0}")]
    Exec(#[from] ExecError),

    /// `nix build` exited non-zero; `message` carries the captured stderr.
    #[error("nix build failed for {attr}: {message}")]
    BuildFailed {
        /// Flake attribute that was being built.
        attr: String,
        /// Captured stderr (or a note when stderr was empty).
        message: String,
    },

    /// `nix build` succeeded but printed no store path.
    #[error("nix build for {attr} returned empty path")]
    EmptyPath {
        /// Flake attribute that was built.
        attr: String,
    },

    /// `nix copy` exited non-zero; `message` carries the captured stderr.
    #[error("nix copy to {store_uri} failed: {message}")]
    CopyFailed {
        /// Destination store URI.
        store_uri: String,
        /// Captured stderr (or a note when stderr was empty).
        message: String,
    },
}

/// Build any flake attribute and return its store path.
///
/// Runs `nix build <flake_attr> --no-link --print-out-paths`, adding
/// `--builders @<machines_file>` when a machines file is given (Nix's native
/// distributed build mechanism).
///
/// This is the generic build primitive — consortium-ansible uses it for
/// `ansibleEnvs.{name}`, consortium-slurm for `slurmEnvs.{name}`, etc.
pub fn build_flake_attr(
    exec: &dyn Executor,
    flake_attr: &str,
    machines_file: Option<&str>,
) -> Result<String, StagingError> {
    let mut spec =
        CommandSpec::new("nix").args(["build", flake_attr, "--no-link", "--print-out-paths"]);
    if let Some(path) = machines_file {
        spec = spec.args(["--builders".to_string(), format!("@{}", path)]);
    }

    let output = exec.exec(&spec)?;

    if !output.success() {
        return Err(StagingError::BuildFailed {
            attr: flake_attr.to_string(),
            message: stderr_or_note(&output.stderr),
        });
    }

    let path = output.stdout.trim().to_string();
    if path.is_empty() {
        return Err(StagingError::EmptyPath {
            attr: flake_attr.to_string(),
        });
    }

    Ok(path)
}

/// Copy a single closure to a remote store.
///
/// Uses `--no-check-sigs` because locally-built closures aren't signed
/// by a key the remote trusts. We're deploying as root over SSH, so
/// the trust boundary is the SSH connection itself.
pub fn copy_closure(
    exec: &dyn Executor,
    store_path: &str,
    store_uri: &str,
) -> Result<(), StagingError> {
    let spec =
        CommandSpec::new("nix").args(["copy", "--no-check-sigs", "--to", store_uri, store_path]);

    let output = exec.exec(&spec)?;

    if !output.success() {
        return Err(StagingError::CopyFailed {
            store_uri: store_uri.to_string(),
            message: stderr_or_note(&output.stderr),
        });
    }

    Ok(())
}

/// Use stderr verbatim when present, otherwise note its absence so the
/// error still says something actionable.
fn stderr_or_note(stderr: &str) -> String {
    let trimmed = stderr.trim();
    if trimmed.is_empty() {
        "(no stderr output)".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::ExecOutput;
    use crate::exec::ScriptedExecutor;

    // ---------- build_flake_attr ----------

    #[test]
    fn build_happy_path() {
        let exec = ScriptedExecutor::new()
            .on("nix build", ExecOutput::ok("/nix/store/abc123-slurm-env-train\n"));
        let path = build_flake_attr(&exec, ".#slurmEnvs.train", None).unwrap();
        assert_eq!(path, "/nix/store/abc123-slurm-env-train");
        exec.assert_invoked_containing("nix build .#slurmEnvs.train --no-link --print-out-paths");
        assert_eq!(exec.invocation_count(), 1);
    }

    #[test]
    fn build_with_machines_file() {
        let exec = ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/x\n"));
        build_flake_attr(&exec, ".#pkg", Some("/tmp/machines")).unwrap();
        exec.assert_invoked_containing("--builders @/tmp/machines");
    }

    #[test]
    fn build_nonzero_exit_contains_stderr() {
        let exec = ScriptedExecutor::new().on(
            "nix build",
            ExecOutput::new(1, "", "error: attribute 'slurmEnvs.nope' missing"),
        );
        let err = build_flake_attr(&exec, ".#slurmEnvs.nope", None).unwrap_err();
        match err {
            StagingError::BuildFailed { attr, message } => {
                assert_eq!(attr, ".#slurmEnvs.nope");
                assert!(
                    message.contains("attribute 'slurmEnvs.nope' missing"),
                    "stderr not propagated: {}",
                    message
                );
            }
            other => panic!("expected BuildFailed, got: {}", other),
        }
    }

    #[test]
    fn build_empty_path_is_error() {
        let exec = ScriptedExecutor::new().on("nix build", ExecOutput::ok("  \n"));
        let err = build_flake_attr(&exec, ".#pkg", None).unwrap_err();
        assert!(matches!(err, StagingError::EmptyPath { .. }), "{}", err);
    }

    #[test]
    fn build_exec_error_propagates() {
        let exec = ScriptedExecutor::new().on_error("nix", "spawn failed: nix not found");
        let err = build_flake_attr(&exec, ".#pkg", None).unwrap_err();
        match err {
            StagingError::Exec(ExecError::Scripted(msg)) => {
                assert!(msg.contains("nix not found"), "{}", msg)
            }
            other => panic!("expected Exec(Scripted), got: {}", other),
        }
    }

    // ---------- copy_closure ----------

    #[test]
    fn copy_happy_path() {
        let exec = ScriptedExecutor::new().on("nix copy", ExecOutput::ok(""));
        copy_closure(&exec, "/nix/store/abc-env", "ssh-ng://root@submit01").unwrap();
        exec.assert_invoked_containing(
            "nix copy --no-check-sigs --to ssh-ng://root@submit01 /nix/store/abc-env",
        );
        assert_eq!(exec.invocation_count(), 1);
    }

    #[test]
    fn copy_nonzero_exit_contains_stderr() {
        let exec = ScriptedExecutor::new().on(
            "nix copy",
            ExecOutput::new(1, "", "ssh: connect to host submit01 port 22: Connection refused"),
        );
        let err = copy_closure(&exec, "/nix/store/abc-env", "ssh-ng://root@submit01").unwrap_err();
        match err {
            StagingError::CopyFailed { store_uri, message } => {
                assert_eq!(store_uri, "ssh-ng://root@submit01");
                assert!(
                    message.contains("Connection refused"),
                    "stderr not propagated: {}",
                    message
                );
            }
            other => panic!("expected CopyFailed, got: {}", other),
        }
    }

    #[test]
    fn copy_exec_error_propagates() {
        // No rules at all: any invocation is Unexpected.
        let exec = ScriptedExecutor::new();
        let err = copy_closure(&exec, "/nix/store/p", "ssh-ng://u@h").unwrap_err();
        match err {
            StagingError::Exec(ExecError::Unexpected(rendered)) => {
                assert!(rendered.contains("nix copy"), "{}", rendered)
            }
            other => panic!("expected Exec(Unexpected), got: {}", other),
        }
    }

    // ---------- build → copy pipeline ordering ----------

    #[test]
    fn build_then_copy_pipeline() {
        let exec = ScriptedExecutor::new()
            .on("nix build", ExecOutput::ok("/nix/store/abc-env\n"))
            .on("nix copy", ExecOutput::ok(""));
        let path = build_flake_attr(&exec, ".#ansibleEnvs.default", None).unwrap();
        copy_closure(&exec, &path, "ssh-ng://root@ctrl").unwrap();
        let build_idx = exec.invocation_index_containing("nix build").unwrap();
        let copy_idx = exec.invocation_index_containing("nix copy").unwrap();
        assert!(build_idx < copy_idx);
    }
}
