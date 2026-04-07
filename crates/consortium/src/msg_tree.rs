//! Message aggregation tree.
//!
//! Rust implementation of `ClusterShell.MsgTree`.

use std::collections::HashMap;

/// Aggregates output lines into a tree structure for deduplication.
#[derive(Debug)]
pub struct MsgTree {
    root: MsgTreeNode,
    _mode: MsgTreeMode,
}

#[derive(Debug, Clone, Copy)]
pub enum MsgTreeMode {
    /// Defer message processing until walk.
    Defer,
    /// Process messages immediately.
    Immediate,
}

#[derive(Debug, Default)]
struct MsgTreeNode {
    children: HashMap<String, MsgTreeNode>,
    keys: Vec<String>,
}

impl MsgTree {
    pub fn new(mode: MsgTreeMode) -> Self {
        Self {
            root: MsgTreeNode::default(),
            _mode: mode,
        }
    }

    pub fn add(&mut self, key: &str, message: &str) {
        let _ = (key, message);
        todo!("MsgTree::add")
    }

    pub fn len(&self) -> usize {
        todo!("MsgTree::len")
    }

    pub fn is_empty(&self) -> bool {
        self.root.children.is_empty() && self.root.keys.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_is_empty() {
        let tree = MsgTree::new(MsgTreeMode::Defer);
        assert!(tree.is_empty());
    }
}
