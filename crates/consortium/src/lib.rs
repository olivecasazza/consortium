//! # Consortium
//!
//! A Rust implementation of [ClusterShell](https://github.com/cea-hpc/clustershell).
//!
//! ## Module hierarchy (mirrors ClusterShell)
//!
//! - [`range_set`] — Numeric range sets with folding/autostep (ClusterShell.RangeSet)
//! - [`node_set`] — Node name sets with bracket expansion (ClusterShell.NodeSet)
//! - [`node_utils`] — Group resolution backends (ClusterShell.NodeUtils)
//! - [`msg_tree`] — Message aggregation tree (ClusterShell.MsgTree)
//! - [`defaults`] — Configuration defaults (ClusterShell.Defaults)
//! - [`event`] — Event handler traits (ClusterShell.Event)
//! - [`topology`] — Cluster topology/tree (ClusterShell.Topology)
//! - [`communication`] — Inter-node messaging (ClusterShell.Communication)
//! - [`propagation`] — Tree propagation engine (ClusterShell.Propagation)
//! - [`task`] — Task orchestration (ClusterShell.Task)
//! - [`gateway`] — Gateway node logic (ClusterShell.Gateway)
//! - [`engine`] — I/O engine abstraction (ClusterShell.Engine)
//! - [`worker`] — Worker abstraction for node execution (ClusterShell.Worker)

pub mod communication;
pub mod dag;
pub mod defaults;
pub mod engine;
pub mod event;
pub mod gateway;
pub mod msg_tree;
pub mod node_set;
pub mod node_utils;
pub mod propagation;
pub mod range_set;
pub mod task;
pub mod topology;
pub mod worker;
