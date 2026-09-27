// Suppress PyO3 0.22 macro-generated cfg warnings (fixed in PyO3 0.23+).
#![allow(unexpected_cfgs)]

//! PyO3 bindings for consortium.
//!
//! This crate exposes the Rust implementation through a Python module named
//! `ClusterShell._consortium`. Thin Python wrapper modules (ClusterShell/RangeSet.py
//! etc.) re-export types from here so the original test imports work unchanged.

use pyo3::prelude::*;

mod node_set;
mod range_set;

const GATEWAY_PYTHON_ENV: &str = "CLUSTERSHELL_GW_PYTHON_EXECUTABLE";

fn gateway_python_fallback<T>(configured: bool, executable: Option<T>) -> Option<T> {
    if configured {
        None
    } else {
        executable
    }
}

/// Keep SSH-launched tree gateways on the interpreter that loaded this
/// compiled extension. Upstream falls back to `basename(sys.executable)`,
/// which loses an active virtual environment in the remote login shell.
/// An explicit deployment override remains authoritative.
fn configure_gateway_python(py: Python<'_>) -> PyResult<()> {
    let environ = py.import_bound("os")?.getattr("environ")?;
    let executable = py.import_bound("sys")?.getattr("executable")?;
    let executable = executable.is_truthy()?.then_some(executable);

    if let Some(selected) =
        gateway_python_fallback(environ.contains(GATEWAY_PYTHON_ENV)?, executable)
    {
        environ.set_item(GATEWAY_PYTHON_ENV, selected)?;
    }
    Ok(())
}

/// The native extension module, importable as `ClusterShell._consortium`.
#[pymodule]
fn _consortium(m: &Bound<'_, PyModule>) -> PyResult<()> {
    range_set::register(m)?;
    node_set::register(m)?;
    configure_gateway_python(m.py())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_python_defaults_to_importing_interpreter() {
        assert_eq!(
            gateway_python_fallback(false, Some("/venv/bin/python")),
            Some("/venv/bin/python")
        );
        assert_eq!(gateway_python_fallback::<&str>(false, None), None);
    }

    #[test]
    fn gateway_python_preserves_explicit_override() {
        assert_eq!(
            gateway_python_fallback(true, Some("/venv/bin/python")),
            None
        );
    }
}
