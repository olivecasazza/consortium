// Suppress PyO3 0.22 macro-generated cfg warnings (fixed in PyO3 0.23+).
#![allow(unexpected_cfgs)]

//! PyO3 bindings for consortium.
//!
//! This crate exposes the Rust implementation through a Python module named
//! `ClusterShell._consortium`. Thin Python wrapper modules (ClusterShell/RangeSet.py
//! etc.) re-export types from here so the original test imports work unchanged.

use std::path::Path;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

mod node_set;
mod range_set;

const GATEWAY_PYTHON_ENV: &str = "CLUSTERSHELL_GW_PYTHON_EXECUTABLE";
const GATEWAY_MODULE_ARGS: &str = " -m ClusterShell.Gateway -Bu";

fn rewrite_gateway_python(
    command: &str,
    default_executable: &str,
    importing_executable: &str,
) -> Result<String, &'static str> {
    if importing_executable.is_empty() {
        return Err("the importing Python executable is empty");
    }

    let command = command
        .strip_suffix(GATEWAY_MODULE_ARGS)
        .ok_or("the inherited TreeWorker gateway command has an unexpected shape")?;
    let prefix = command
        .strip_suffix(default_executable)
        .ok_or("the inherited TreeWorker gateway executable has an unexpected shape")?;
    let mut rewritten = String::with_capacity(
        prefix.len() + importing_executable.len() + GATEWAY_MODULE_ARGS.len(),
    );
    rewritten.push_str(prefix);
    rewritten.push_str(importing_executable);
    rewritten.push_str(GATEWAY_MODULE_ARGS);
    Ok(rewritten)
}

/// Make an inherited Python TreeWorker launch its remote gateway with the
/// interpreter that loaded this extension, while preserving explicit
/// deployment overrides.
#[pyfunction]
fn _use_importing_python_for_gateway(worker: &Bound<'_, PyAny>) -> PyResult<()> {
    let py = worker.py();
    let environ = py.import_bound("os")?.getattr("environ")?;
    let configured = environ.contains(GATEWAY_PYTHON_ENV)?;
    if configured {
        return Ok(());
    }
    let executable: Option<String> = py.import_bound("sys")?.getattr("executable")?.extract()?;
    let Some(executable) = executable.filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let default_executable = Path::new(&executable)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(&executable);
    let quoted_executable: String = py
        .import_bound("shlex")?
        .call_method1("quote", (&executable,))?
        .extract()?;
    let command: String = worker.getattr("invoke_gateway")?.extract()?;

    let rewritten = rewrite_gateway_python(&command, default_executable, &quoted_executable)
        .map_err(PyRuntimeError::new_err)?;
    worker.setattr("invoke_gateway", rewritten)?;
    Ok(())
}

/// The native extension module, importable as `ClusterShell._consortium`.
#[pymodule]
fn _consortium(m: &Bound<'_, PyModule>) -> PyResult<()> {
    range_set::register(m)?;
    node_set::register(m)?;
    m.add_function(wrap_pyfunction!(_use_importing_python_for_gateway, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_python_uses_importing_interpreter() {
        assert_eq!(
            rewrite_gateway_python(
                "PYTHONPATH=/src python -m ClusterShell.Gateway -Bu",
                "python",
                "/venv/bin/python",
            ),
            Ok("PYTHONPATH=/src /venv/bin/python -m ClusterShell.Gateway -Bu".to_owned())
        );
    }

    #[test]
    fn gateway_python_only_rewrites_gateway_executable() {
        assert!(rewrite_gateway_python(
            "python -m some.other.module",
            "python",
            "/venv/bin/python",
        )
        .is_err());
    }
}
