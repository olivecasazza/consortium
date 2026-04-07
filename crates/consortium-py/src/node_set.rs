//! PyO3 wrappers for consortium::node_set.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Python-visible NodeSet class.
#[pyclass(name = "NodeSet")]
#[derive(Debug, Clone)]
pub struct PyNodeSet {
    inner: consortium::node_set::NodeSet,
}

pyo3::create_exception!(ClusterShell.NodeSet, NodeSetParseError, PyValueError);

#[pymethods]
impl PyNodeSet {
    #[new]
    #[pyo3(signature = (pattern=None))]
    fn new(pattern: Option<&str>) -> PyResult<Self> {
        match pattern {
            Some(p) => {
                let inner = consortium::node_set::NodeSet::parse(p)
                    .map_err(|e| NodeSetParseError::new_err(e.to_string()))?;
                Ok(Self { inner })
            }
            None => Ok(Self {
                inner: consortium::node_set::NodeSet::new(),
            }),
        }
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("NodeSet(\"{}\")", self.inner)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __contains__(&self, node: &str) -> bool {
        self.inner.contains(node)
    }

    fn __or__(&self, other: &PyNodeSet) -> Self {
        Self {
            inner: self.inner.union(&other.inner),
        }
    }

    fn __and__(&self, other: &PyNodeSet) -> Self {
        Self {
            inner: self.inner.intersection(&other.inner),
        }
    }

    fn __sub__(&self, other: &PyNodeSet) -> Self {
        Self {
            inner: self.inner.difference(&other.inner),
        }
    }

    fn __xor__(&self, other: &PyNodeSet) -> Self {
        Self {
            inner: self.inner.symmetric_difference(&other.inner),
        }
    }
}

/// Register node_set types into the parent module.
pub fn register(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<PyNodeSet>()?;
    Ok(())
}
