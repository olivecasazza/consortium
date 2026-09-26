//! Nix build orchestration — build closures locally or with distributed builders.

use std::collections::HashMap;
use std::io::Write as IoWrite;
use std::sync::{Arc, Mutex};

use consortium::dag::types::{FnTask, TaskOutcome};
use consortium::dag::DagBuilder;
use consortium_integration::exec::Executor;
use consortium_integration::staging::{self, StagingError};

use crate::config::{DeploymentPlan, ProfileType};
use crate::error::{NixError, Result};
use crate::eval;
use crate::health::HealthStatus;

/// Build results keyed by hostname.
pub struct BuildResults {
    /// Map of hostname -> built store path.
    pub paths: HashMap<String, String>,
    /// Map of hostname -> build error.
    pub errors: HashMap<String, NixError>,
}

/// Build all closures in a deployment plan.
///
/// If healthy builders are provided, generates a temporary machines file
/// and uses Nix's native distributed build mechanism.
///
/// Parallelizes build operations via consortium's DAG executor, respecting
/// the plan's max_parallel concurrency limit. All `nix build` invocations
/// run through `exec`.
pub fn build_closures(
    exec: Arc<dyn Executor>,
    plan: &DeploymentPlan,
    flake_uri: &str,
    healthy_builders: Option<&[HealthStatus]>,
) -> Result<BuildResults> {
    // Generate temporary machines file if we have healthy builders
    let machines_file = healthy_builders
        .map(generate_machines_file_from_healthy)
        .transpose()?;

    // Shared results collected by DAG tasks
    let build_paths: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let build_errors: Arc<Mutex<HashMap<String, NixError>>> = Arc::new(Mutex::new(HashMap::new()));

    // Separate skipped targets (no build needed) from those requiring builds
    let mut dag_builder = DagBuilder::new();
    let flake_uri_owned = flake_uri.to_string();

    for target in &plan.targets {
        if !target.needs_build {
            // Record skipped targets immediately
            build_paths
                .lock()
                .unwrap()
                .insert(target.node.name.clone(), target.toplevel_path.clone());
            continue;
        }

        let hostname = target.node.name.clone();
        let flake_uri_clone = flake_uri_owned.clone();
        let machines_file_clone = machines_file.clone();
        let exec_clone = Arc::clone(&exec);
        let paths = Arc::clone(&build_paths);
        let errors = Arc::clone(&build_errors);

        // Create a task ID based on the hostname
        let task_id = format!("build:{}", hostname);

        // Create a closure that captures the build parameters
        let task = FnTask::new(format!("build {}", hostname), move |_ctx| match build_host(
            &*exec_clone,
            &flake_uri_clone,
            &hostname,
            machines_file_clone.as_deref(),
        ) {
            Ok(path) => {
                paths.lock().unwrap().insert(hostname.clone(), path);
                TaskOutcome::Success
            }
            Err(e) => {
                errors.lock().unwrap().insert(hostname.clone(), e);
                TaskOutcome::Failed(format!("build failed for {}", hostname))
            }
        });

        dag_builder.add_task(task_id, task);
    }

    // Set concurrency limit if specified
    if plan.max_parallel > 0 {
        dag_builder.concurrency_group("builds", plan.max_parallel);
        for target in &plan.targets {
            if target.needs_build {
                let task_id = format!("build:{}", target.node.name);
                dag_builder.assign_group(task_id, "builds");
            }
        }
    }

    // Execute the DAG
    let executor = dag_builder.build()?;
    let _report = executor.run()?;

    // Extract results from Arc<Mutex<_>>
    let final_paths = build_paths.lock().unwrap().clone();
    let final_errors = build_errors.lock().unwrap().clone();

    let results = BuildResults {
        paths: final_paths,
        errors: final_errors,
    };

    Ok(results)
}

/// Build any flake attribute and return its store path, via an [`Executor`].
///
/// This is the generic build primitive — consortium-ansible uses it for
/// `ansibleEnvs.{name}`, consortium-slurm for `slurmEnvs.{name}`, etc.
/// Thin adapter over [`staging::build_flake_attr`] mapping errors into
/// [`NixError`].
pub fn build_flake_attr_with(
    exec: &dyn Executor,
    flake_attr: &str,
    machines_file: Option<&str>,
) -> Result<String> {
    build_flake_attr_with_args(exec, flake_attr, machines_file, &[])
}

/// [`build_flake_attr_with`] with extra words appended to `nix build`
/// (e.g. `--override-input foo path:./stub`).
pub fn build_flake_attr_with_args(
    exec: &dyn Executor,
    flake_attr: &str,
    machines_file: Option<&str>,
    extra_args: &[String],
) -> Result<String> {
    staging::build_flake_attr_with_args(exec, flake_attr, machines_file, extra_args).map_err(|e| {
        match e {
            StagingError::BuildFailed { message, .. } => NixError::BuildFailed {
                host: flake_attr.to_string(),
                message,
            },
            StagingError::EmptyPath { .. } => NixError::BuildFailed {
                host: flake_attr.to_string(),
                message: "nix build returned empty path".to_string(),
            },
            StagingError::Exec(source) => NixError::BuildFailed {
                host: flake_attr.to_string(),
                message: format!("failed to run nix build: {}", source),
            },
            // Unreachable from `nix build`, mapped for exhaustiveness.
            StagingError::CopyFailed { message, .. } => NixError::BuildFailed {
                host: flake_attr.to_string(),
                message,
            },
        }
    })
}

/// Build the system closure for a single NixOS host. Use
/// [`build_system_toplevel`] for nix-darwin hosts or extra nix arguments.
pub fn build_host(
    exec: &dyn Executor,
    flake_uri: &str,
    hostname: &str,
    machines_file: Option<&str>,
) -> Result<String> {
    build_system_toplevel(
        exec,
        flake_uri,
        hostname,
        &ProfileType::Nixos,
        machines_file,
        &[],
    )
}

/// Build the system closure for a host of either profile type, appending
/// `extra_args` to the `nix build` command line.
pub fn build_system_toplevel(
    exec: &dyn Executor,
    flake_uri: &str,
    hostname: &str,
    profile_type: &ProfileType,
    machines_file: Option<&str>,
    extra_args: &[String],
) -> Result<String> {
    let attr = eval::toplevel_attr(flake_uri, hostname, profile_type);
    build_flake_attr_with_args(exec, &attr, machines_file, extra_args)
}

/// Generate a temporary machines file from healthy builders.
/// Public so the deploy pipeline can call it before building the DAG.
pub fn generate_machines_file_from_healthy(builders: &[HealthStatus]) -> Result<String> {
    let content: String = builders
        .iter()
        .filter(|b| b.healthy)
        .map(|b| {
            let key = b.builder.ssh_key.as_deref().unwrap_or("-");
            let features = b.builder.features.join(",");
            let systems = b.builder.systems.join(",");
            format!(
                "{}://{}@{} {} {} {} {} {}",
                b.builder.protocol,
                b.builder.user,
                b.builder.host,
                systems,
                key,
                b.builder.max_jobs,
                b.builder.speed_factor,
                features
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let dir = std::env::temp_dir().join("consortium-nix");
    std::fs::create_dir_all(&dir).map_err(|e| NixError::MachinesFile {
        path: dir.clone(),
        source: e,
    })?;

    let path = dir.join("machines");
    let mut file = std::fs::File::create(&path).map_err(|e| NixError::MachinesFile {
        path: path.clone(),
        source: e,
    })?;
    file.write_all(content.as_bytes())
        .map_err(|e| NixError::MachinesFile {
            path: path.clone(),
            source: e,
        })?;

    Ok(path.to_string_lossy().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Builder;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};

    #[test]
    fn test_generate_machines_file() {
        let builders = vec![HealthStatus {
            builder: Builder {
                host: "192.168.1.121".to_string(),
                user: "root".to_string(),
                max_jobs: 16,
                speed_factor: 2,
                systems: vec!["x86_64-linux".to_string()],
                features: vec!["big-parallel".to_string(), "kvm".to_string()],
                ssh_key: None,
                protocol: "ssh-ng".to_string(),
            },
            healthy: true,
            latency_ms: Some(5),
            error: None,
        }];

        let path = generate_machines_file_from_healthy(&builders).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("ssh-ng://root@192.168.1.121"));
        assert!(content.contains("x86_64-linux"));
        assert!(content.contains("big-parallel,kvm"));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_build_host_records_nix_build() {
        let exec = ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/abc-top\n"));
        let path = build_host(&exec, ".", "hp01", Some("/tmp/machines")).unwrap();
        assert_eq!(path, "/nix/store/abc-top");
        exec.assert_invoked_containing(
            "nix build .#nixosConfigurations.hp01.config.system.build.toplevel \
             --no-link --print-out-paths",
        );
        exec.assert_invoked_containing("--builders @/tmp/machines");
    }

    #[test]
    fn test_build_system_toplevel_darwin_with_extra_args() {
        let exec = ScriptedExecutor::new().on("nix build", ExecOutput::ok("/nix/store/abc-mac\n"));
        let extra = vec![
            "--option".to_string(),
            "builders".to_string(),
            "".to_string(),
        ];
        let path = build_system_toplevel(
            &exec,
            ".",
            "mac01",
            &ProfileType::NixDarwin,
            Some("/tmp/machines"),
            &extra,
        )
        .unwrap();
        assert_eq!(path, "/nix/store/abc-mac");
        exec.assert_invoked_containing(
            "nix build .#darwinConfigurations.mac01.config.system.build.toplevel \
             --no-link --print-out-paths --builders @/tmp/machines --option builders ",
        );
    }

    #[test]
    fn test_build_host_failure_surfaces_stderr() {
        let exec =
            ScriptedExecutor::new().on("nix build", ExecOutput::new(1, "", "error: builder busy"));
        let err = build_host(&exec, ".", "hp01", None).unwrap_err();
        match err {
            NixError::BuildFailed { message, .. } => {
                assert!(message.contains("builder busy"), "{}", message)
            }
            other => panic!("expected BuildFailed, got: {}", other),
        }
    }
}
