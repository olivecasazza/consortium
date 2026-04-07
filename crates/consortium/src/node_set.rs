//! Node name sets with bracket-based range expansion.
//!
//! Rust implementation of `ClusterShell.NodeSet`.

use crate::range_set::RangeSet;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NodeSetError {
    #[error("parse error: {msg} (in \"{part}\")")]
    ParseError { part: String, msg: String },

    #[error("range error: {0}")]
    RangeError(#[from] crate::range_set::RangeSetError),

    #[error("external error: {0}")]
    ExternalError(String),
}

pub type Result<T> = std::result::Result<T, NodeSetError>;

/// A set of cluster node names supporting range notation like `node[1-100]`.
#[derive(Debug, Clone)]
pub struct NodeSet {
    /// Internal mapping from pattern prefix/suffix → RangeSet.
    patterns: Vec<(String, Option<RangeSet>, String)>,
    _autostep: f64,
}

impl NodeSet {
    pub fn new() -> Self {
        Self {
            patterns: Vec::new(),
            _autostep: crate::range_set::AUTOSTEP_DISABLED,
        }
    }

    /// Parse a node set from a string like `"node[1-10,15],other[001-050]"`.
    pub fn parse(pattern: &str) -> Result<Self> {
        let _ = pattern;
        todo!("NodeSet::parse")
    }

    pub fn len(&self) -> usize {
        todo!("NodeSet::len")
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    pub fn contains(&self, node: &str) -> bool {
        let _ = node;
        todo!("NodeSet::contains")
    }

    pub fn union(&self, other: &NodeSet) -> NodeSet {
        let _ = other;
        todo!("NodeSet::union")
    }

    pub fn intersection(&self, other: &NodeSet) -> NodeSet {
        let _ = other;
        todo!("NodeSet::intersection")
    }

    pub fn difference(&self, other: &NodeSet) -> NodeSet {
        let _ = other;
        todo!("NodeSet::difference")
    }

    pub fn symmetric_difference(&self, other: &NodeSet) -> NodeSet {
        let _ = other;
        todo!("NodeSet::symmetric_difference")
    }

    pub fn iter(&self) -> impl Iterator<Item = String> + '_ {
        std::iter::empty() // TODO
    }
}

impl Default for NodeSet {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TODO")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_is_empty() {
        let ns = NodeSet::new();
        assert!(ns.is_empty());
    }
}
