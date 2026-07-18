//! Closure copying — transfer built closures to deployment targets.

use std::collections::HashMap;

use consortium_integration::exec::{Executor, ProcessExecutor};
use consortium_integration::staging::{self, StagingError};

use crate::config::DeploymentPlan;
use crate::error::{NixError, Result};

/// Copy results keyed by hostname.
pub struct CopyResults {
    /// Hosts that were successfully copied to.
    pub succeeded: Vec<String>,
    /// Map of hostname -> copy error.
    pub errors: HashMap<String, NixError>,
}

/// Copy closures to all targets in the deployment plan.
pub fn copy_closures(exec: &dyn Executor, plan: &DeploymentPlan) -> Result<CopyResults> {
    let mut results = CopyResults {
        succeeded: Vec::new(),
        errors: HashMap::new(),
    };

    // TODO: parallelize with consortium's Task/Worker fanout
    for target in &plan.targets {
        if !target.needs_copy {
            results.succeeded.push(target.node.name.clone());
            continue;
        }

        let store_uri = format!(
            "ssh-ng://{}@{}",
            target.node.target_user, target.node.target_host
        );

        match copy_closure_with(exec, &target.toplevel_path, &store_uri) {
            Ok(()) => {
                results.succeeded.push(target.node.name.clone());
            }
            Err(e) => {
                results.errors.insert(target.node.name.clone(), e);
            }
        }
    }

    Ok(results)
}

/// Copy a single closure to a remote store, via an [`Executor`].
///
/// Uses `--no-check-sigs` because locally-built closures aren't signed
/// by a key the remote trusts. We're deploying as root over SSH, so
/// the trust boundary is the SSH connection itself.
///
/// Thin adapter over [`staging::copy_closure`] mapping errors into
/// [`NixError`].
pub fn copy_closure_with(exec: &dyn Executor, store_path: &str, store_uri: &str) -> Result<()> {
    staging::copy_closure(exec, store_path, store_uri).map_err(|e| match e {
        StagingError::CopyFailed { message, .. } => NixError::CopyFailed {
            host: store_uri.to_string(),
            message,
        },
        StagingError::Exec(source) => NixError::CopyFailed {
            host: store_uri.to_string(),
            message: format!("failed to run nix copy: {}", source),
        },
        // Unreachable from `nix copy`, mapped for exhaustiveness.
        other => NixError::CopyFailed {
            host: store_uri.to_string(),
            message: other.to_string(),
        },
    })
}

/// Copy a single closure to a remote store.
///
/// Runs the copy on a local [`ProcessExecutor`].
#[deprecated(note = "use the Executor-based variant")]
pub fn copy_closure(store_path: &str, store_uri: &str) -> Result<()> {
    copy_closure_with(&ProcessExecutor::new(), store_path, store_uri)
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    #[test]
    fn test_copy_closure_records_nix_copy_command() {
        let exec = ScriptedExecutor::new().on("nix copy", ExecOutput::ok(""));
        copy_closure_with(&exec, "/nix/store/abc-env", "ssh-ng://root@hp01").unwrap();
        exec.assert_invoked_containing(
            "nix copy --no-check-sigs --to ssh-ng://root@hp01 /nix/store/abc-env",
        );
        assert_eq!(exec.invocation_count(), 1);
    }

    #[test]
    fn test_copy_closure_failure_surfaces_stderr() {
        let exec = ScriptedExecutor::new().on(
            "nix copy",
            ExecOutput::new(1, "", "ssh: connect to host hp01 port 22: Connection refused"),
        );
        let err =
            copy_closure_with(&exec, "/nix/store/abc-env", "ssh-ng://root@hp01").unwrap_err();
        match err {
            NixError::CopyFailed { host, message } => {
                assert_eq!(host, "ssh-ng://root@hp01");
                assert!(message.contains("Connection refused"), "{}", message);
            }
            other => panic!("expected CopyFailed, got: {}", other),
        }
    }
}
