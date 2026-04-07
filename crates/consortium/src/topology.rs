//! Cluster topology and routing tree.
//!
//! Rust implementation of `ClusterShell.Topology`.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TopologyError {
    #[error("topology error: {0}")]
    General(String),

    #[error("topology parse error: {0}")]
    ParseError(String),
}

pub type Result<T> = std::result::Result<T, TopologyError>;

/// A node group in the topology tree.
#[derive(Debug, Clone)]
pub struct TopologyNodeGroup {
    pub nodes: crate::node_set::NodeSet,
    pub children: Vec<TopologyNodeGroup>,
}

/// Parsed topology tree.
#[derive(Debug)]
pub struct TopologyTree {
    _root: Option<TopologyNodeGroup>,
}

impl TopologyTree {
    pub fn new() -> Self {
        Self { _root: None }
    }
}

impl Default for TopologyTree {
    fn default() -> Self {
        Self::new()
    }
}

/// A route from source nodes to destination nodes.
#[derive(Debug, Clone)]
pub struct TopologyRoute {
    pub src: crate::node_set::NodeSet,
    pub dst: crate::node_set::NodeSet,
}

/// Routing table built from topology.
#[derive(Debug)]
pub struct TopologyRoutingTable {
    _routes: Vec<TopologyRoute>,
}

impl TopologyRoutingTable {
    pub fn new() -> Self {
        Self {
            _routes: Vec::new(),
        }
    }
}

impl Default for TopologyRoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Parser for topology configuration files.
pub struct TopologyParser;

impl TopologyParser {
    pub fn parse(filename: &str) -> Result<TopologyTree> {
        let _ = filename;
        todo!("TopologyParser::parse")
    }
}
