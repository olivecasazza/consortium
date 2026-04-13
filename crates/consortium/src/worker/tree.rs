//! TreeWorker implementation for tree-based command propagation.
//!
//! Provides TreeWorker, which distributes command execution across a cluster
//! topology using gateway nodes and propagation channels. Mirrors
//! ClusterShell's TreeWorker class.
//!
//! The TreeWorker uses:
//! - [`PropagationTreeRouter`] to determine next-hop gateways
//! - [`PropagationChannel`] to communicate with gateway processes
//! - Direct `SshWorker`/`ExecWorker` for leaf nodes reachable without gateways

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::node_set::NodeSet;
use crate::propagation::{PropagationChannel, PropagationResult, PropagationTreeRouter};
use crate::topology::TopologyTree;
use crate::worker::{WorkerError, WorkerState};

/// Build a NodeSet from a collection of node name strings.
fn nodeset_from_strings(names: &[String]) -> String {
    names.join(",")
}

/// Configuration for tree-based execution.
#[derive(Debug, Clone)]
pub struct TreeWorkerConfig {
    /// Maximum concurrent operations per gateway.
    pub fanout: usize,
    /// Command timeout per node.
    pub timeout: Option<Duration>,
    /// Whether to capture stderr.
    pub stderr: bool,
    /// Whether targets are remote (SSH) or local.
    pub remote: bool,
    /// Whether this is a copy operation.
    pub source: Option<String>,
    /// Destination for copy operations.
    pub dest: Option<String>,
    /// Whether this is a reverse copy (remote -> local).
    pub reverse: bool,
    /// Whether to preserve file attributes in copy.
    pub preserve: bool,
}

impl Default for TreeWorkerConfig {
    fn default() -> Self {
        Self {
            fanout: 64,
            timeout: None,
            stderr: false,
            remote: true,
            source: None,
            dest: None,
            reverse: false,
            preserve: false,
        }
    }
}

/// Tracks the state of a gateway connection.
#[derive(Debug)]
struct GatewayState {
    /// The propagation channel for this gateway.
    channel: PropagationChannel,
    /// Active target nodes being processed through this gateway.
    active_targets: HashSet<String>,
    /// Whether the gateway channel has been started.
    started: bool,
}

/// Result from a remote node received through the tree.
#[derive(Debug, Clone)]
pub enum TreeWorkerEvent {
    /// Node produced stdout output.
    StdOut {
        node: String,
        gateway: String,
        data: Vec<u8>,
    },
    /// Node produced stderr output.
    StdErr {
        node: String,
        gateway: String,
        data: Vec<u8>,
    },
    /// Node completed with a return code.
    Close {
        node: String,
        gateway: String,
        rc: i32,
    },
    /// Node timed out.
    Timeout { node: String, gateway: String },
    /// Routing event (reroute, unreachable, etc.)
    Routing {
        event: String,
        gateway: String,
        targets: String,
    },
}

/// Worker that distributes command execution across a cluster topology.
///
/// TreeWorker uses [`PropagationTreeRouter`] to determine routing through
/// gateway nodes, and [`PropagationChannel`] to communicate with gateways.
/// Nodes directly reachable (same hop as root) are executed via direct
/// workers (SSH or local).
///
/// Mirrors ClusterShell's `TreeWorker` class.
pub struct TreeWorker {
    /// Target nodes for this worker.
    nodes: NodeSet,
    /// Command to execute (mutually exclusive with source).
    command: Option<String>,
    /// Worker configuration.
    config: TreeWorkerConfig,
    /// Current worker state.
    state: WorkerState,
    /// Router for determining next-hop gateways.
    router: Option<PropagationTreeRouter>,
    /// Gateway states indexed by gateway name.
    gateways: HashMap<String, GatewayState>,
    /// Return codes collected per node.
    retcodes: HashMap<String, i32>,
    /// Events collected from remote nodes.
    events: Vec<TreeWorkerEvent>,
    /// Count of closed/completed targets.
    close_count: usize,
    /// Total target count (for completion tracking).
    target_count: usize,
    /// Whether any node has timed out.
    has_timeout: bool,
    /// Unique worker ID for channel communication.
    worker_id: u64,
    /// Nodes that timed out.
    timeout_nodes: HashSet<String>,
}

impl TreeWorker {
    /// Create a new TreeWorker for command execution.
    ///
    /// # Arguments
    /// * `nodes` - Target nodes to execute on.
    /// * `command` - Command to execute on each node.
    /// * `config` - Tree worker configuration.
    pub fn new(nodes: NodeSet, command: String, config: TreeWorkerConfig) -> Self {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

        Self {
            nodes,
            command: Some(command),
            config,
            state: WorkerState::Pending,
            router: None,
            gateways: HashMap::new(),
            retcodes: HashMap::new(),
            events: Vec::new(),
            close_count: 0,
            target_count: 0,
            has_timeout: false,
            worker_id: NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            timeout_nodes: HashSet::new(),
        }
    }

    /// Set the topology router for this worker.
    ///
    /// Must be called before `start()`. The router determines how commands
    /// are distributed across gateway nodes.
    pub fn set_router(&mut self, router: PropagationTreeRouter) {
        self.router = Some(router);
    }

    /// Create a router from a topology tree.
    ///
    /// # Arguments
    /// * `root` - The root node name (typically the local hostname).
    /// * `topology` - The cluster topology tree.
    ///
    /// # Errors
    /// Returns `WorkerError` if the root node is not found in the topology.
    pub fn set_topology(&mut self, root: &str, topology: &TopologyTree) -> Result<(), WorkerError> {
        let router = PropagationTreeRouter::new(root, topology, self.config.fanout)
            .map_err(|e| WorkerError::General(format!("topology error: {}", e)))?;
        self.router = Some(router);
        Ok(())
    }

    /// Get the target nodes.
    pub fn nodes(&self) -> &NodeSet {
        &self.nodes
    }

    /// Get the command being executed.
    pub fn command(&self) -> Option<&str> {
        self.command.as_deref()
    }

    /// Get the current state.
    pub fn state(&self) -> WorkerState {
        self.state
    }

    /// Get collected return codes.
    pub fn retcodes(&self) -> &HashMap<String, i32> {
        &self.retcodes
    }

    /// Get collected events from remote nodes.
    pub fn events(&self) -> &[TreeWorkerEvent] {
        &self.events
    }

    /// Drain collected events.
    pub fn drain_events(&mut self) -> Vec<TreeWorkerEvent> {
        std::mem::take(&mut self.events)
    }

    /// Get the number of timed out nodes.
    pub fn num_timeout(&self) -> usize {
        self.timeout_nodes.len()
    }

    /// Get timed out nodes.
    pub fn timeout_nodes(&self) -> &HashSet<String> {
        &self.timeout_nodes
    }

    /// Whether any node has timed out.
    pub fn has_timeout(&self) -> bool {
        self.has_timeout
    }

    /// Check if the worker is done (all targets completed or timed out).
    pub fn is_done(&self) -> bool {
        self.state == WorkerState::Done || self.state == WorkerState::Aborted
    }

    /// Distribute target nodes across next-hop gateways.
    ///
    /// Returns a list of (gateway, targets) pairs. When gateway == targets,
    /// the nodes are directly reachable (no gateway needed).
    fn distribute(&self) -> Result<Vec<(NodeSet, NodeSet)>, WorkerError> {
        let router = self.router.as_ref().ok_or_else(|| {
            WorkerError::General("router not set; call set_router() or set_topology()".to_string())
        })?;

        let mut distribution: HashMap<String, NodeSet> = HashMap::new();

        for (gw, dstset) in router.dispatch(&self.nodes) {
            let gw_str = gw.to_string();
            distribution
                .entry(gw_str)
                .and_modify(|ns| ns.update(&dstset))
                .or_insert(dstset);
        }

        Ok(distribution
            .into_iter()
            .map(|(k, v)| {
                let gw = NodeSet::parse(&k).unwrap_or_default();
                (gw, v)
            })
            .collect())
    }

    /// Start the tree worker — distribute work across gateways.
    ///
    /// This sets up propagation channels for gateway nodes and prepares
    /// direct execution for leaf nodes.
    pub fn start(&mut self) -> Result<Vec<(NodeSet, NodeSet)>, WorkerError> {
        if self.state != WorkerState::Pending {
            return Err(WorkerError::General(
                "worker already started or aborted".to_string(),
            ));
        }

        self.state = WorkerState::Running;

        let next_hops = self.distribute()?;

        for (gw, targets) in &next_hops {
            let target_count = targets.len();
            self.target_count += target_count;

            let gw_str = gw.to_string();
            let targets_str = targets.to_string();

            // Check if gateway == targets (direct execution, no gateway needed)
            if gw_str == targets_str {
                // Direct targets — caller should spawn SSH/local workers for these
                continue;
            }

            // Gateway-mediated execution: set up propagation channel
            let channel = PropagationChannel::new(&gw_str);
            let gw_state = GatewayState {
                channel,
                active_targets: targets.iter().collect(),
                started: false,
            };
            self.gateways.insert(gw_str, gw_state);
        }

        Ok(next_hops)
    }

    /// Start a gateway's propagation channel with topology data.
    ///
    /// # Arguments
    /// * `gateway` - The gateway node name.
    /// * `topology_bytes` - Serialized topology data to send to gateway.
    pub fn start_gateway(
        &mut self,
        gateway: &str,
        topology_bytes: &[u8],
    ) -> Result<(), WorkerError> {
        let gw_state = self
            .gateways
            .get_mut(gateway)
            .ok_or_else(|| WorkerError::General(format!("unknown gateway: {}", gateway)))?;

        if gw_state.started {
            return Err(WorkerError::General(format!(
                "gateway {} already started",
                gateway
            )));
        }

        gw_state.channel.start(topology_bytes);
        gw_state.started = true;

        // Send the shell command if this is a command execution
        if let Some(ref cmd) = self.command {
            let targets: Vec<String> = gw_state.active_targets.iter().cloned().collect();
            let targets_str = nodeset_from_strings(&targets);
            gw_state
                .channel
                .shell(&targets_str, cmd.as_bytes(), self.worker_id);
        }

        Ok(())
    }

    /// Process a propagation result from a gateway.
    ///
    /// Call this when the gateway channel receives results.
    /// Returns true if the worker is now complete.
    pub fn process_result(
        &mut self,
        gateway: &str,
        result: PropagationResult,
    ) -> Result<bool, WorkerError> {
        match result {
            PropagationResult::StdOut { ref node, ref data } => {
                self.events.push(TreeWorkerEvent::StdOut {
                    node: node.clone(),
                    gateway: gateway.to_string(),
                    data: data.clone(),
                });
            }
            PropagationResult::StdErr { ref node, ref data } => {
                self.events.push(TreeWorkerEvent::StdErr {
                    node: node.clone(),
                    gateway: gateway.to_string(),
                    data: data.clone(),
                });
            }
            PropagationResult::Retcode { ref node, rc } => {
                self.retcodes.insert(node.clone(), rc);
                self.close_count += 1;

                // Remove from active targets
                if let Some(gw_state) = self.gateways.get_mut(gateway) {
                    gw_state.active_targets.remove(node);
                }

                self.events.push(TreeWorkerEvent::Close {
                    node: node.clone(),
                    gateway: gateway.to_string(),
                    rc,
                });
            }
            PropagationResult::Timeout { ref node } => {
                self.timeout_nodes.insert(node.clone());
                self.has_timeout = true;
                self.close_count += 1;

                if let Some(gw_state) = self.gateways.get_mut(gateway) {
                    gw_state.active_targets.remove(node);
                }

                self.events.push(TreeWorkerEvent::Timeout {
                    node: node.clone(),
                    gateway: gateway.to_string(),
                });
            }
            PropagationResult::Routing {
                ref event,
                ref gateway,
                ref targets,
            } => {
                self.events.push(TreeWorkerEvent::Routing {
                    event: event.clone(),
                    gateway: gateway.clone(),
                    targets: targets.clone(),
                });
            }
        }

        // Check if we're done
        let done = self.close_count >= self.target_count && self.target_count > 0;
        if done {
            self.state = WorkerState::Done;
        }

        // Check if gateway has no more active targets
        self.check_gateway_fini(gateway);

        Ok(done)
    }

    /// Record completion of a direct (non-gateway) node.
    ///
    /// Called when a direct SSH/local worker reports node completion.
    pub fn record_direct_close(&mut self, node: &str, rc: i32) {
        self.retcodes.insert(node.to_string(), rc);
        self.close_count += 1;

        if self.close_count >= self.target_count && self.target_count > 0 {
            self.state = WorkerState::Done;
        }
    }

    /// Record a direct node timeout.
    pub fn record_direct_timeout(&mut self, node: &str) {
        self.timeout_nodes.insert(node.to_string());
        self.has_timeout = true;
        self.close_count += 1;

        if self.close_count >= self.target_count && self.target_count > 0 {
            self.state = WorkerState::Done;
        }
    }

    /// Check if a gateway has finished (no more active targets).
    fn check_gateway_fini(&mut self, gateway: &str) {
        if let Some(gw_state) = self.gateways.get(gateway) {
            if gw_state.active_targets.is_empty() {
                // Gateway is done — could trigger cleanup
                // The actual channel close is handled by the caller
            }
        }
    }

    /// Write data to all active gateway channels.
    pub fn write_remote(&mut self, buf: &[u8]) {
        let worker_id = self.worker_id;
        for (_, gw_state) in self.gateways.iter_mut() {
            if !gw_state.active_targets.is_empty() {
                let targets: Vec<String> = gw_state.active_targets.iter().cloned().collect();
                let targets_str = nodeset_from_strings(&targets);
                gw_state.channel.write(&targets_str, buf, worker_id);
            }
        }
    }

    /// Send write EOF to all active gateway channels.
    pub fn set_write_eof_remote(&mut self) {
        let worker_id = self.worker_id;
        for (_, gw_state) in self.gateways.iter_mut() {
            if !gw_state.active_targets.is_empty() {
                let targets: Vec<String> = gw_state.active_targets.iter().cloned().collect();
                let targets_str = nodeset_from_strings(&targets);
                gw_state.channel.set_write_eof(&targets_str, worker_id);
            }
        }
    }

    /// Abort the tree worker.
    ///
    /// Marks all remaining gateway targets as failed with EX_PROTOCOL (76).
    pub fn abort(&mut self) {
        if self.is_done() {
            return;
        }

        // Close all gateway channels
        for (gateway, gw_state) in self.gateways.iter_mut() {
            let remaining: Vec<String> = gw_state.active_targets.drain().collect();
            for node in remaining {
                self.retcodes.insert(node.clone(), 76); // EX_PROTOCOL
                self.close_count += 1;
                self.events.push(TreeWorkerEvent::Close {
                    node,
                    gateway: gateway.clone(),
                    rc: 76,
                });
            }
        }

        self.state = WorkerState::Aborted;
    }

    /// Get the number of active gateways.
    pub fn active_gateway_count(&self) -> usize {
        self.gateways
            .values()
            .filter(|gs| !gs.active_targets.is_empty())
            .count()
    }

    /// Get active gateway names.
    pub fn active_gateways(&self) -> Vec<&str> {
        self.gateways
            .iter()
            .filter(|(_, gs)| !gs.active_targets.is_empty())
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Get the total number of target nodes.
    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    /// Get the worker ID.
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_nodeset(s: &str) -> NodeSet {
        NodeSet::parse(s).unwrap()
    }

    #[test]
    fn test_tree_worker_config_default() {
        let config = TreeWorkerConfig::default();
        assert_eq!(config.fanout, 64);
        assert!(config.timeout.is_none());
        assert!(!config.stderr);
        assert!(config.remote);
        assert!(config.source.is_none());
        assert!(config.dest.is_none());
        assert!(!config.reverse);
        assert!(!config.preserve);
    }

    #[test]
    fn test_tree_worker_creation() {
        let nodes = make_nodeset("node[1-10]");
        let config = TreeWorkerConfig::default();
        let worker = TreeWorker::new(nodes.clone(), "hostname".to_string(), config);

        assert_eq!(worker.state(), WorkerState::Pending);
        assert_eq!(worker.num_nodes(), 10);
        assert_eq!(worker.command(), Some("hostname"));
        assert!(worker.retcodes().is_empty());
        assert!(worker.events().is_empty());
        assert!(!worker.has_timeout());
        assert_eq!(worker.num_timeout(), 0);
    }

    #[test]
    fn test_tree_worker_start_without_router() {
        let nodes = make_nodeset("node[1-3]");
        let config = TreeWorkerConfig::default();
        let mut worker = TreeWorker::new(nodes, "echo hi".to_string(), config);

        let result = worker.start();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("router not set"));
    }

    #[test]
    fn test_tree_worker_abort() {
        let nodes = make_nodeset("node[1-5]");
        let config = TreeWorkerConfig::default();
        let mut worker = TreeWorker::new(nodes, "echo hi".to_string(), config);

        worker.abort();
        assert_eq!(worker.state(), WorkerState::Aborted);
        assert!(worker.is_done());
    }

    #[test]
    fn test_tree_worker_record_direct_close() {
        let nodes = make_nodeset("node[1-3]");
        let config = TreeWorkerConfig::default();
        let mut worker = TreeWorker::new(nodes, "echo hi".to_string(), config);

        // Simulate that 3 direct targets were started
        worker.state = WorkerState::Running;
        worker.target_count = 3;

        worker.record_direct_close("node1", 0);
        assert_eq!(worker.retcodes().get("node1"), Some(&0));
        assert!(!worker.is_done());

        worker.record_direct_close("node2", 0);
        assert!(!worker.is_done());

        worker.record_direct_close("node3", 1);
        assert!(worker.is_done());
        assert_eq!(worker.retcodes().get("node3"), Some(&1));
    }

    #[test]
    fn test_tree_worker_record_direct_timeout() {
        let nodes = make_nodeset("node[1-2]");
        let config = TreeWorkerConfig::default();
        let mut worker = TreeWorker::new(nodes, "sleep 100".to_string(), config);

        worker.state = WorkerState::Running;
        worker.target_count = 2;

        worker.record_direct_timeout("node1");
        assert!(worker.has_timeout());
        assert_eq!(worker.num_timeout(), 1);
        assert!(worker.timeout_nodes().contains("node1"));
        assert!(!worker.is_done());

        worker.record_direct_close("node2", 0);
        assert!(worker.is_done());
    }

    #[test]
    fn test_tree_worker_process_result_stdout() {
        let nodes = make_nodeset("node[1-2]");
        let config = TreeWorkerConfig::default();
        let mut worker = TreeWorker::new(nodes, "echo hi".to_string(), config);

        worker.state = WorkerState::Running;
        worker.target_count = 2;

        let result = PropagationResult::StdOut {
            node: "node1".to_string(),
            data: b"hello world".to_vec(),
        };

        let done = worker.process_result("gw1", result).unwrap();
        assert!(!done);
        assert_eq!(worker.events().len(), 1);

        match &worker.events()[0] {
            TreeWorkerEvent::StdOut { node, data, .. } => {
                assert_eq!(node, "node1");
                assert_eq!(data, b"hello world");
            }
            _ => panic!("expected StdOut event"),
        }
    }

    #[test]
    fn test_tree_worker_process_result_retcode() {
        let nodes = make_nodeset("node[1-2]");
        let config = TreeWorkerConfig::default();
        let mut worker = TreeWorker::new(nodes, "echo hi".to_string(), config);

        worker.state = WorkerState::Running;
        worker.target_count = 2;

        // First node completes
        let result1 = PropagationResult::Retcode {
            node: "node1".to_string(),
            rc: 0,
        };
        let done = worker.process_result("gw1", result1).unwrap();
        assert!(!done);

        // Second node completes
        let result2 = PropagationResult::Retcode {
            node: "node2".to_string(),
            rc: 42,
        };
        let done = worker.process_result("gw1", result2).unwrap();
        assert!(done);
        assert_eq!(worker.state(), WorkerState::Done);
        assert_eq!(worker.retcodes().get("node2"), Some(&42));
    }

    #[test]
    fn test_tree_worker_process_result_timeout() {
        let nodes = make_nodeset("node1");
        let config = TreeWorkerConfig::default();
        let mut worker = TreeWorker::new(nodes, "sleep 999".to_string(), config);

        worker.state = WorkerState::Running;
        worker.target_count = 1;

        let result = PropagationResult::Timeout {
            node: "node1".to_string(),
        };
        let done = worker.process_result("gw1", result).unwrap();
        assert!(done);
        assert!(worker.has_timeout());
        assert!(worker.timeout_nodes().contains("node1"));
    }

    #[test]
    fn test_tree_worker_drain_events() {
        let nodes = make_nodeset("node1");
        let config = TreeWorkerConfig::default();
        let mut worker = TreeWorker::new(nodes, "echo hi".to_string(), config);

        worker.state = WorkerState::Running;
        worker.target_count = 1;

        let result = PropagationResult::StdOut {
            node: "node1".to_string(),
            data: b"data".to_vec(),
        };
        worker.process_result("gw1", result).unwrap();

        assert_eq!(worker.events().len(), 1);
        let drained = worker.drain_events();
        assert_eq!(drained.len(), 1);
        assert!(worker.events().is_empty());
    }

    #[test]
    fn test_tree_worker_unique_ids() {
        let nodes = make_nodeset("node1");
        let config1 = TreeWorkerConfig::default();
        let config2 = TreeWorkerConfig::default();
        let w1 = TreeWorker::new(nodes.clone(), "a".to_string(), config1);
        let w2 = TreeWorker::new(nodes, "b".to_string(), config2);
        assert_ne!(w1.worker_id(), w2.worker_id());
    }

    #[test]
    fn test_tree_worker_active_gateways_empty() {
        let nodes = make_nodeset("node[1-3]");
        let config = TreeWorkerConfig::default();
        let worker = TreeWorker::new(nodes, "echo hi".to_string(), config);

        assert_eq!(worker.active_gateway_count(), 0);
        assert!(worker.active_gateways().is_empty());
    }

    #[test]
    fn test_tree_worker_routing_event() {
        let nodes = make_nodeset("node1");
        let config = TreeWorkerConfig::default();
        let mut worker = TreeWorker::new(nodes, "echo".to_string(), config);

        worker.state = WorkerState::Running;
        worker.target_count = 1;

        let result = PropagationResult::Routing {
            event: "reroute".to_string(),
            gateway: "gw1".to_string(),
            targets: "node1".to_string(),
        };
        worker.process_result("gw1", result).unwrap();

        assert_eq!(worker.events().len(), 1);
        match &worker.events()[0] {
            TreeWorkerEvent::Routing {
                event,
                gateway,
                targets,
            } => {
                assert_eq!(event, "reroute");
                assert_eq!(gateway, "gw1");
                assert_eq!(targets, "node1");
            }
            _ => panic!("expected Routing event"),
        }
    }
}
