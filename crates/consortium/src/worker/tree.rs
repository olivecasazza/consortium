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

use crate::communication::MessageProcessingError;
use crate::node_set::NodeSet;
use crate::propagation::{PropagationChannel, PropagationResult, PropagationTreeRouter};
use crate::topology::TopologyTree;
use crate::worker::{WorkerError, WorkerState};

/// Build a NodeSet from a collection of node name strings.
fn nodeset_from_strings(names: &[String]) -> String {
    names.join(",")
}

/// `SNAME_STDERR` equivalent: stream name attributed to stderr data.
pub const SNAME_STDERR: &str = "stderr";
/// `SNAME_STDOUT` equivalent: stream name attributed to stdout data.
pub const SNAME_STDOUT: &str = "stdout";

/// Remote untar command format for forward tree copy (UNTAR_CMD_FMT).
///
/// Single quotes are essential (mirrors ClusterShell); use
/// [`shell_escape_single_quoted`] on the interpolated path.
pub const UNTAR_CMD_FMT: &str = "tar -xf - -C '%s'";

/// Remote tar|base64 command format for tree rcopy (TAR_CMD_FMT).
///
/// Args are (srcdir, srcbase). The remote side archives from `srcdir`,
/// renames the top-level entry to `<name>.<short-hostname>/` and emits
/// base64 with 64 KiB lines on stdout.
pub const TAR_CMD_FMT: &str = "tar -cf - -C '%s' \
--transform \"s,^\\([^/]*\\)[/]*,\\1.$(hostname -s)/,\" \
'%s' | base64 -w 65536";

/// Escape a string for inclusion between single quotes in a shell command.
///
/// Mirrors the `'`-within-`''` escaping used by ClusterShell's
/// `_copy_remote`: each `'` becomes `'"'"'`.
pub fn shell_escape_single_quoted(value: &str) -> String {
    value.replace('\'', "'\"'\"'")
}

/// Build the remote untar command used for forward tree copy.
pub fn untar_command(dest: &str) -> String {
    UNTAR_CMD_FMT.replacen("%s", &shell_escape_single_quoted(dest), 1)
}

/// Build the remote tar|base64 command used for tree rcopy.
pub fn tar_command(srcdir: &str, srcbase: &str) -> String {
    TAR_CMD_FMT
        .replacen("%s", &shell_escape_single_quoted(srcdir), 1)
        .replacen("%s", &shell_escape_single_quoted(srcbase), 1)
}

/// POSIX-style dirname (upstream Python os.path.dirname semantics for
/// the inputs ClusterShell produces).
fn posix_dirname(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        // path was only slashes: os.path.dirname('/') == '/'
        return if path.starts_with('/') { "/" } else { "" };
    }
    match trimmed.rfind('/') {
        Some(0) => "/",
        Some(pos) => &trimmed[..pos],
        None => "",
    }
}

/// POSIX-style basename of a normalized path (os.path.basename).
fn posix_basename(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(pos) => &trimmed[pos + 1..],
        None => trimmed,
    }
}

/// Minimal os.path.normpath for absolute/relative POSIX paths: collapses
/// duplicate slashes, resolves `.` and `..` components, strips any
/// trailing slash. Does not resolve symlinks (like os.path.normpath).
fn posix_normpath(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for comp in path.split('/') {
        match comp {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|last| *last != "..") {
                    out.pop();
                } else if !absolute {
                    out.push("..");
                }
            }
            _ => out.push(comp),
        }
    }
    let joined = out.join("/");
    match (absolute, joined.is_empty()) {
        (true, true) => "/".to_string(),
        (true, false) => format!("/{}", joined),
        (false, true) => ".".to_string(),
        (false, false) => joined,
    }
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
    /// Targets whose shell control message has been handed to the
    /// channel but not yet confirmed sent on the wire.
    ///
    /// `ev_pickup` for a node fires only once its control message has
    /// actually left the local process — i.e. when it can no longer be
    /// rerouted (upstream ClusterShell #594): a control message queued
    /// behind an un-ACKed configuration message must not trigger it.
    ctl_unsent_targets: Vec<String>,
}

/// Result from a remote node received through the tree.
#[derive(Debug, Clone)]
pub enum TreeWorkerEvent {
    /// The meta worker has started (`ev_start` equivalent). Always
    /// emitted before any [`TreeWorkerEvent::Pickup`] event.
    Started,
    /// A node's command can no longer be rerouted (`ev_pickup`
    /// equivalent). Fires at most once per node, only after the control
    /// message carrying its command has actually been sent (upstream
    /// ClusterShell #594).
    Pickup {
        /// Node that was picked up.
        node: String,
    },
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

/// Per-node rcopy reassembly state (upstream `TreeWorker._rcopy_bufs` /
/// `TreeWorker._rcopy_tars`).
#[derive(Debug, Default)]
struct RcopyState {
    /// Leftover base64 characters not yet decodable (fewer than 4).
    encoded_tail: Vec<u8>,
    /// Decoded tar bytes accumulated so far.
    tar_bytes: Vec<u8>,
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
    /// `ev_start` has fired (upstream `TreeWorker._initialized`).
    initialized: bool,
    /// Worker has been aborted (upstream `TreeWorker._aborted`); gates
    /// all future pickup emission (#594).
    aborted: bool,
    /// Pickup events buffered before `ev_start` fires, flushed by
    /// [`TreeWorker::check_ini`] (upstream `TreeWorker._pending_pickups`).
    pending_pickups: Vec<String>,
    /// Number of direct child workers that have started (upstream
    /// `TreeWorker._start_count`).
    start_count: usize,
    /// Number of direct child workers expected (upstream
    /// `TreeWorker._child_count`): one per directly reachable target
    /// group. Gateway-mediated targets do not count.
    child_count: usize,
    /// Per-node rcopy reassembly state (reverse copy only).
    rcopy_states: HashMap<String, RcopyState>,
}

impl TreeWorker {
    /// Create a new TreeWorker for command execution.
    ///
    /// # Arguments
    /// * `nodes` - Target nodes to execute on.
    /// * `command` - Command to execute on each node.
    /// * `config` - Tree worker configuration.
    pub fn new(nodes: NodeSet, command: String, config: TreeWorkerConfig) -> Self {
        Self::with_command(nodes, Some(command), config)
            .expect("command is always provided to TreeWorker::new")
    }

    /// Create a new TreeWorker for a copy or reverse-copy (rcopy)
    /// operation.
    ///
    /// `config.source` must be set (mirrors the upstream `ValueError`
    /// raised when neither command nor source is provided).
    ///
    /// # Errors
    /// Returns `WorkerError::General` if `config.source` is `None`.
    pub fn new_copy(nodes: NodeSet, config: TreeWorkerConfig) -> Result<Self, WorkerError> {
        Self::with_command(nodes, None, config)
    }

    /// Shared constructor (upstream `TreeWorker.__init__`).
    fn with_command(
        nodes: NodeSet,
        command: Option<String>,
        mut config: TreeWorkerConfig,
    ) -> Result<Self, WorkerError> {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

        if command.is_none() && config.source.is_none() {
            return Err(WorkerError::General(
                "missing command or source parameter in TreeWorker constructor".to_string(),
            ));
        }

        // copy/rcopy: keep internal tar/scp errors on a separate stderr
        // stream instead of merging them into stdout (which also carries
        // rcopy data) — upstream ClusterShell #622 (commit 5ed4c34).
        if config.source.is_some() {
            config.stderr = true;
        }

        Ok(Self {
            nodes,
            command,
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
            initialized: false,
            aborted: false,
            pending_pickups: Vec::new(),
            start_count: 0,
            child_count: 0,
            rcopy_states: HashMap::new(),
        })
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
        self.distribute_nodes(&self.nodes)
    }

    /// Distribute an arbitrary nodeset across next-hop gateways
    /// (upstream `TreeWorker._distribute`).
    fn distribute_nodes(&self, nodes: &NodeSet) -> Result<Vec<(NodeSet, NodeSet)>, WorkerError> {
        let router = self.router.as_ref().ok_or_else(|| {
            WorkerError::General("router not set; call set_router() or set_topology()".to_string())
        })?;

        let mut distribution: HashMap<String, NodeSet> = HashMap::new();

        for (gw, dstset) in router.dispatch(nodes) {
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

    /// The command to run on remote targets through a gateway, if any.
    ///
    /// Mirrors upstream `_execute_remote` (command mode) and
    /// `_copy_remote` (copy/rcopy mode): copy operations send a tar
    /// pipeline command to the targets instead of a user command.
    fn remote_shell_command(&self) -> Option<String> {
        if let Some(ref cmd) = self.command {
            return Some(cmd.clone());
        }
        let source = self.config.source.as_ref()?;
        if self.config.reverse {
            // rcopy: archive remotely, transfer base64 tar on stdout
            let srcdir = posix_dirname(source);
            let normed = posix_normpath(source);
            let srcbase = posix_basename(&normed);
            Some(tar_command(srcdir, srcbase))
        } else {
            // forward copy: untar from stdin at the destination
            let dest = self.config.dest.as_deref().unwrap_or("");
            Some(untar_command(dest))
        }
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
                // Direct targets — caller should spawn SSH/local workers for
                // these; each spawned child worker counts towards _child_count
                // (upstream TreeWorker._launch).
                self.child_count += 1;
                continue;
            }

            // Gateway-mediated execution: set up propagation channel
            let channel = PropagationChannel::new(&gw_str);
            let gw_state = GatewayState {
                channel,
                active_targets: targets.iter().collect(),
                started: false,
                ctl_unsent_targets: Vec::new(),
            };
            self.gateways.insert(gw_str, gw_state);
        }

        // With no direct children this fires ev_start immediately, like
        // upstream's `_start()`: `_launch()` then `_check_ini()`.
        self.check_ini();

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
        let command = self.remote_shell_command();
        let worker_id = self.worker_id;
        {
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

            // Send the shell command (command mode) or tar pipeline (copy mode).
            if let Some(ref cmd) = command {
                let mut targets: Vec<String> = gw_state.active_targets.iter().cloned().collect();
                targets.sort();
                let targets_str = nodeset_from_strings(&targets);
                gw_state.channel.shell(&targets_str, cmd.as_bytes(), worker_id);
                // ev_pickup is fired by sync_gateway_pickups() once the control
                // message has actually been sent on the wire (#594).
                gw_state.ctl_unsent_targets.extend(targets);
            }
        }

        self.sync_gateway_pickups(gateway);

        Ok(())
    }

    /// Emit pickups for gateway targets whose control message has
    /// actually been sent on the wire.
    ///
    /// Mirrors `PropagationChannel._send_ctl` firing `_emit_pickup`
    /// after `channel.send` (upstream ClusterShell #594): once channel
    /// setup is complete, a queued control message has left the local
    /// process and its targets can no longer be rerouted. Call this
    /// after the channel processes the configuration ACK (and after
    /// [`TreeWorker::start_gateway`]); it is a no-op while the control
    /// message is still queued behind an un-ACKed configuration.
    pub fn sync_gateway_pickups(&mut self, gateway: &str) {
        let Some(gw_state) = self.gateways.get_mut(gateway) else {
            return;
        };
        if !gw_state.channel.channel.setup || gw_state.ctl_unsent_targets.is_empty() {
            return;
        }
        let targets = std::mem::take(&mut gw_state.ctl_unsent_targets);
        for node in targets {
            self.emit_pickup(&node);
        }
    }

    /// Fire `ev_pickup` for a node, or queue it until `ev_start` fires.
    ///
    /// Mirrors upstream `TreeWorker._emit_pickup` (#594): direct child
    /// workers route pickups through `MetaWorkerEventHandler.ev_pickup`;
    /// gateway-routed targets route them through the propagation channel
    /// once the control message leaves the local process (see
    /// [`TreeWorker::sync_gateway_pickups`]). Emission is skipped once
    /// the worker has been aborted.
    pub fn emit_pickup(&mut self, node: &str) {
        if self.aborted {
            return;
        }
        if self.initialized {
            self.events.push(TreeWorkerEvent::Pickup {
                node: node.to_string(),
            });
        } else {
            // ev_start has not fired yet; defer to preserve ordering
            self.pending_pickups.push(node.to_string());
        }
    }

    /// Notify the meta worker that a direct child worker has started.
    ///
    /// `MetaWorkerEventHandler.ev_start` equivalent: bumps the started
    /// count and re-evaluates initialization.
    pub fn notify_child_start(&mut self) {
        self.start_count += 1;
        self.check_ini();
    }

    /// Fire `ev_start` once all direct child workers have started, then
    /// flush pickup events buffered during launch.
    ///
    /// Mirrors upstream `TreeWorker._check_ini` (#594): this part runs
    /// once. If the worker is aborted on (or before) `ev_start`, the
    /// flush stops at the first aborted check and no pickups are
    /// emitted.
    pub fn check_ini(&mut self) {
        if !self.initialized && self.start_count >= self.child_count {
            self.initialized = true;
            self.events.push(TreeWorkerEvent::Started);
            let pending = std::mem::take(&mut self.pending_pickups);
            for node in pending {
                if self.aborted {
                    break;
                }
                self.events.push(TreeWorkerEvent::Pickup { node });
            }
        }
    }

    /// Mark a node as unreachable in the router, so later dispatches and
    /// relaunches avoid it (upstream `PropagationTreeRouter
    /// .mark_unreachable` pass-through used before `_relaunch`).
    pub fn mark_unreachable(&mut self, node: &str) {
        if let Some(ref mut router) = self.router {
            router.mark_unreachable(node);
        }
    }

    /// Redistribute and relaunch targets that were running on a failed
    /// gateway (upstream `TreeWorker._relaunch`).
    ///
    /// Emits a `reroute` routing event BEFORE re-dispatching, so a
    /// rerouted target's `ev_pickup` can only fire afterwards — and,
    /// because pickup requires the control message to have been sent,
    /// exactly once (#594). Upstream only reroutes targets whose control
    /// message never left the failed channel; callers should do the same
    /// (a target already picked up on the failed gateway must not be
    /// relaunched).
    ///
    /// Returns the new (gateway, targets) distribution; entries with
    /// gateway == targets are directly reachable and must be handled by
    /// the caller like any direct target.
    pub fn relaunch(
        &mut self,
        previous_gateway: &str,
    ) -> Result<Vec<(NodeSet, NodeSet)>, WorkerError> {
        let gw_state = self.gateways.remove(previous_gateway).ok_or_else(|| {
            WorkerError::General(format!("unknown gateway: {}", previous_gateway))
        })?;

        let mut targets: Vec<String> = gw_state.active_targets.into_iter().collect();
        targets.sort();
        let target_count = targets.len();
        // Any control message queued but never sent on the failed channel
        // dies with it: no pickup was ever emitted for these targets (#594).
        // (gw_state is dropped here, closing the channel state.)

        self.target_count = self.target_count.saturating_sub(target_count);

        // Routing event fires before re-dispatch (upstream _ev_routing).
        self.events.push(TreeWorkerEvent::Routing {
            event: "reroute".to_string(),
            gateway: previous_gateway.to_string(),
            targets: targets.join(","),
        });

        // Re-dispatch the orphaned targets (upstream `_launch(targets)`).
        let targets_ns = NodeSet::parse(&targets.join(","))
            .map_err(|e| WorkerError::General(format!("invalid targets: {}", e)))?;
        let next_hops = self.distribute_nodes(&targets_ns)?;

        for (gw, hop_targets) in &next_hops {
            self.target_count += hop_targets.len();
            let gw_str = gw.to_string();
            if gw_str == hop_targets.to_string() {
                // became directly reachable — caller spawns a child worker
                self.child_count += 1;
                continue;
            }
            let new_targets: HashSet<String> = hop_targets.iter().collect();
            match self.gateways.get_mut(&gw_str) {
                Some(existing) => {
                    // Channel to this gateway already exists (it serves other
                    // targets): send the command for the relaunched targets on
                    // it, like upstream `_execute_remote` reusing the pchannel.
                    let mut ts: Vec<String> = new_targets.iter().cloned().collect();
                    ts.sort();
                    existing.active_targets.extend(new_targets);
                    self.shell_on_channel(&gw_str, &ts)?;
                }
                None => {
                    let channel = PropagationChannel::new(&gw_str);
                    self.gateways.insert(
                        gw_str,
                        GatewayState {
                            channel,
                            active_targets: new_targets,
                            started: false,
                            ctl_unsent_targets: Vec::new(),
                        },
                    );
                }
            }
        }

        self.check_ini();

        Ok(next_hops)
    }

    /// Send this worker's command to `targets` over an existing gateway
    /// channel (used when a relaunch lands on an already-started gateway).
    /// Pickups fire via [`TreeWorker::sync_gateway_pickups`] once the
    /// control message is actually sent.
    fn shell_on_channel(&mut self, gateway: &str, targets: &[String]) -> Result<(), WorkerError> {
        let command = self.remote_shell_command();
        let worker_id = self.worker_id;
        {
            let gw_state = self
                .gateways
                .get_mut(gateway)
                .ok_or_else(|| WorkerError::General(format!("unknown gateway: {}", gateway)))?;

            if let Some(ref cmd) = command {
                let targets_str = nodeset_from_strings(targets);
                gw_state.channel.shell(&targets_str, cmd.as_bytes(), worker_id);
                gw_state
                    .ctl_unsent_targets
                    .extend(targets.iter().cloned());
            }
        }
        self.sync_gateway_pickups(gateway);
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
        // Results only arrive after the channel is set up, so any queued
        // control message has been sent by now (#594).
        self.sync_gateway_pickups(gateway);

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
    ///
    /// Like upstream `TreeWorker._write_remote`, this never fires
    /// `ev_pickup`: write traffic on an already-picked-up node must not
    /// double-fire (#594).
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
    ///
    /// Like upstream `TreeWorker._set_write_eof_remote`, this never
    /// fires `ev_pickup` (#594).
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
    /// Also stops pending and future pickup events (#594): buffered
    /// pickups are dropped and [`TreeWorker::emit_pickup`] becomes a no-op.
    pub fn abort(&mut self) {
        // Stop pending and future pickup events (#594). Upstream sets
        // _aborted unconditionally, even for an already-finished worker.
        self.aborted = true;
        self.pending_pickups.clear();

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

    /// Whether the worker has been aborted (upstream `_aborted`).
    pub fn is_aborted(&self) -> bool {
        self.aborted
    }

    /// Whether `ev_start` has fired (upstream `_initialized`).
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Number of pickup events currently buffered before `ev_start`.
    pub fn pending_pickup_count(&self) -> usize {
        self.pending_pickups.len()
    }

    /// Get the worker configuration.
    pub fn config(&self) -> &TreeWorkerConfig {
        &self.config
    }

    /// Whether stderr is captured separately from stdout.
    ///
    /// Always true for copy/rcopy workers: internal tar/scp errors are
    /// kept off stdout (upstream ClusterShell #622).
    pub fn stderr(&self) -> bool {
        self.config.stderr
    }

    /// Report a gateway-originated error buffer as stderr lines
    /// attributed to `nodes`, as seen from `gateway`.
    ///
    /// Mirrors `PropagationChannel._report_stderr` delivering to this
    /// meta worker (upstream ClusterShell 90d3195): a trailing newline
    /// is appended first so an empty buffer still yields one (empty)
    /// line — `splitlines()` would drop it entirely (#249).
    pub fn report_gateway_stderr(&mut self, gateway: &str, nodes: &NodeSet, buf: &[u8]) {
        let mut data = buf.to_vec();
        data.push(b'\n');
        // split() on the trailing newline always leaves a final empty
        // piece; drop it to match str.splitlines(). A trailing \r per
        // line is stripped as well (\r\n line endings).
        let mut lines: Vec<&[u8]> = data.split(|b| *b == b'\n').collect();
        lines.pop();
        for line in lines {
            let line = match line.last() {
                Some(b'\r') => &line[..line.len() - 1],
                _ => line,
            };
            for node in nodes.iter() {
                self.events.push(TreeWorkerEvent::StdErr {
                    node,
                    gateway: gateway.to_string(),
                    data: line.to_vec(),
                });
            }
        }
    }

    /// Report an error message received from a gateway before channel
    /// setup completed (e.g. the gateway rejected the configuration
    /// payload).
    ///
    /// Mirrors upstream 90d3195: such errors used to be logged and
    /// dropped; they are now reported as stderr of the gateway node
    /// itself, so they reach `ev_read`/`ev_error` on this worker
    /// instead of vanishing.
    pub fn report_gateway_error(&mut self, gateway: &str, reason: &str) {
        let gateway_ns = NodeSet::parse(gateway).unwrap_or_default();
        self.report_gateway_stderr(gateway, &gateway_ns, reason.as_bytes());
    }

    /// Handle a [`MessageProcessingError`] raised while parsing traffic
    /// from `gateway` (e.g. a malformed or unknown gateway message).
    ///
    /// Mirrors the upstream initiator-side `Channel._do_read` path
    /// (commit a027c3e): the error is fed back as a stderr message from
    /// the gateway node instead of crashing the worker. The worker
    /// state is left untouched; closing the failed channel is the
    /// driver's responsibility.
    pub fn process_channel_error(&mut self, gateway: &str, err: &MessageProcessingError) {
        self.report_gateway_error(gateway, &err.to_string());
    }

    /// Process a message line received from a remote node through a
    /// gateway (upstream `TreeWorker._on_remote_node_msgline`).
    ///
    /// For rcopy workers, `stdout` carries the base64-encoded tar stream
    /// and is accumulated for later extraction instead of being reported
    /// (partial base64 decoding requires a multiple of 4 characters;
    /// trailing chars are kept for the next message). Everything else is
    /// recorded as a stdout/stderr event.
    pub fn on_remote_node_msgline(&mut self, node: &str, msg: &[u8], sname: &str, gateway: &str) {
        let is_rcopy_data =
            self.config.source.is_some() && self.config.reverse && sname == SNAME_STDOUT;
        if !is_rcopy_data {
            let event = if sname == SNAME_STDERR {
                TreeWorkerEvent::StdErr {
                    node: node.to_string(),
                    gateway: gateway.to_string(),
                    data: msg.to_vec(),
                }
            } else {
                TreeWorkerEvent::StdOut {
                    node: node.to_string(),
                    gateway: gateway.to_string(),
                    data: msg.to_vec(),
                }
            };
            self.events.push(event);
            return;
        }

        // rcopy only: we expect base64 encoded tar content on stdout
        let state = self.rcopy_states.entry(node.to_string()).or_default();
        state.encoded_tail.extend_from_slice(msg);
        // partial base64 decoding requires a multiple of 4 characters
        let decodable = state.encoded_tail.len() / 4 * 4;
        let chunk: Vec<u8> = state.encoded_tail.drain(..decodable).collect();
        if let Ok(text) = std::str::from_utf8(&chunk) {
            // base64_decode is RFC 4648 relaxed (ignores newlines), like
            // Python's base64.b64decode discarding non-alphabet chars.
            if let Ok(decoded) = crate::communication::base64_decode(text) {
                state.tar_bytes.extend_from_slice(&decoded);
            }
            // Undecodable chunks are dropped here; the resulting corrupt
            // tar stream is reported at finalize time (upstream 973870b).
        }
    }

    /// Finalize rcopy for a node whose remote side has closed: extract
    /// the accumulated tar stream into the destination directory.
    ///
    /// Mirrors the reverse-copy branch of upstream
    /// `TreeWorker._on_remote_node_close`. Extraction uses
    /// [`TarExtractFilter::FullyTrusted`] semantics (PEP 706, upstream
    /// d2fcd16). Any failure — corrupt archive, I/O error — is reported
    /// as a stderr event attributed to the node carrying the gateway's
    /// message (upstream 973870b), never panicked or silently dropped.
    ///
    /// Returns the number of extracted bytes on success; errors are
    /// reported through events, so this only fails on worker misuse
    /// (missing destination).
    pub fn finalize_rcopy_node(&mut self, node: &str, gateway: &str) -> Result<usize, WorkerError> {
        if !(self.config.source.is_some() && self.config.reverse) {
            return Ok(0);
        }
        let Some(state) = self.rcopy_states.remove(node) else {
            // no rcopy buffer received from this node
            return Ok(0);
        };

        let dest = self.config.dest.clone().ok_or_else(|| {
            WorkerError::General("missing dest parameter for rcopy".to_string())
        })?;

        // Upstream flushes the raw leftover base64 tail into the archive
        // before extracting; mirror that (it is empty for well-formed
        // streams).
        let mut tar_bytes = state.tar_bytes;
        tar_bytes.extend_from_slice(&state.encoded_tail);

        match extract_tar(
            std::path::Path::new(&dest),
            &tar_bytes,
            TarExtractFilter::FullyTrusted,
        ) {
            Ok(written) => Ok(written),
            Err(err) => {
                // Report the extraction error as stderr bytes (as bytes,
                // like any other message — upstream 973870b).
                self.events.push(TreeWorkerEvent::StdErr {
                    node: node.to_string(),
                    gateway: gateway.to_string(),
                    data: err.message.into_bytes(),
                });
                Ok(0)
            }
        }
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

// ============================================================================
// rcopy tar extraction (PEP 706 port)
// ============================================================================

/// Explicit trust level for tar extraction (PEP 706 `filter=` port).
///
/// Upstream ClusterShell passes `filter='fully_trusted'` to
/// `tarfile.extractall()` (commit d2fcd16): rcopy tarballs are built by
/// trusted ClusterShell gateways, and the `tar`/`data` filters would
/// strip `S_IWGRP`/`S_IWOTH`, downgrading modes like 0664 to 0644.
/// Naming the trust decision in the type keeps it explicit at the call
/// site instead of silently legacy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TarExtractFilter {
    /// Preserve legacy extraction behavior for trusted archives:
    /// stored permission bits are honored as-is (no umask, no
    /// stripping) and member paths are not sanitized. Only use with
    /// archives from trusted sources (ClusterShell gateways).
    FullyTrusted,
}

/// Error raised when rcopy tar extraction fails.
///
/// The Display message mirrors what upstream reports as the node's
/// stderr (`str(ex).encode()` in `_on_remote_node_close`).
#[derive(Debug, Clone)]
pub struct TarExtractError {
    /// Human-readable failure reason (I/O error, corrupt archive, ...).
    pub message: String,
}

impl std::fmt::Display for TarExtractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TarExtractError {}

impl From<std::io::Error> for TarExtractError {
    fn from(err: std::io::Error) -> Self {
        Self {
            message: err.to_string(),
        }
    }
}

impl TarExtractError {
    fn corrupt(reason: &str) -> Self {
        Self {
            message: reason.to_string(),
        }
    }
}

/// Size of a tar header/data block.
const TAR_BLOCK: usize = 512;

/// Parsed header fields needed for extraction.
struct TarHeader {
    /// Member path (prefix + name, GNU longname or pax path applied).
    name: String,
    /// Permission bits (octal mode field).
    mode: u32,
    /// Typeflag byte.
    typeflag: u8,
    /// Link target for symlink/hardlink members.
    linkname: String,
}

/// Parse an octal-or-base256 numeric tar header field.
fn parse_tar_number(field: &[u8]) -> Option<u64> {
    if field.is_empty() {
        return None;
    }
    // base-256 (GNU large values): high bit of first byte set
    if field[0] & 0x80 != 0 {
        let mut value: u64 = (field[0] & 0x7F) as u64;
        for byte in &field[1..] {
            value = value.checked_shl(8)? | u64::from(*byte);
        }
        return Some(value);
    }
    // octal ASCII, terminated by NUL/space, possibly space-padded
    let text: String = field
        .iter()
        .take_while(|b| **b != 0 && **b != b' ')
        .map(|b| *b as char)
        .collect();
    if text.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(&text, 8).ok()
}

/// Extract a ustar archive into `dest` with explicit trust semantics.
///
/// Returns the total number of payload bytes written. Failures mirror
/// the exception kinds upstream reports as node stderr (commit 973870b):
/// corrupt archives (Python `tarfile.TarError`) and I/O failures
/// (`IOError`/`OSError`) both surface as [`TarExtractError`].
///
/// With [`TarExtractFilter::FullyTrusted`], stored permission bits are
/// applied verbatim (`mode & 0o7777`, ignoring umask) and member paths
/// are used unsanitized — matching PEP 706 `fully_trusted`, which
/// preserves the pre-3.12 behavior for trusted ClusterShell gateways.
pub fn extract_tar(
    dest: &std::path::Path,
    bytes: &[u8],
    _filter: TarExtractFilter,
) -> Result<usize, TarExtractError> {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    if bytes.is_empty() {
        // tarfile.ReadError("empty file") on a zero-byte archive
        return Err(TarExtractError::corrupt("empty file"));
    }

    let mut written = 0usize;
    let mut offset = 0usize;
    // GNU longname/longlink carried over to the next header
    let mut pending_name: Option<String> = None;
    let mut pending_linkname: Option<String> = None;
    // directory modes are applied after all members are extracted, like
    // tarfile delaying directory chmod (dirs may be read-only)
    let mut dir_modes: Vec<(std::path::PathBuf, u32)> = Vec::new();

    let mut terminated = false;
    while offset + TAR_BLOCK <= bytes.len() {
        let block = &bytes[offset..offset + TAR_BLOCK];

        // a zero block terminates the archive
        if block.iter().all(|b| *b == 0) {
            terminated = true;
            break;
        }

        // verify header checksum: chksum field counts as 8 spaces
        let stored = parse_tar_number(&block[148..156])
            .ok_or_else(|| TarExtractError::corrupt("bad checksum field"))?;
        let computed: u64 = block[..148]
            .iter()
            .chain(std::iter::repeat(&b' ').take(8))
            .chain(block[156..].iter())
            .map(|b| u64::from(*b))
            .sum();
        if stored != computed {
            return Err(TarExtractError::corrupt("bad checksum"));
        }

        let mut name_bytes: Vec<u8> = block[0..100]
            .iter()
            .take_while(|b| **b != 0)
            .cloned()
            .collect();
        // ustar prefix (ignored for old v7 headers without magic)
        if &block[257..262] == b"ustar" {
            let prefix: Vec<u8> = block[345..500]
                .iter()
                .take_while(|b| **b != 0)
                .cloned()
                .collect();
            if !prefix.is_empty() {
                let mut full = prefix;
                full.push(b'/');
                full.extend_from_slice(&name_bytes);
                name_bytes = full;
            }
        }
        let name = String::from_utf8_lossy(&name_bytes).into_owned();
        let mode = parse_tar_number(&block[100..108]).unwrap_or(0) as u32;
        let size = parse_tar_number(&block[124..136])
            .ok_or_else(|| TarExtractError::corrupt("bad size field"))?;
        let typeflag = block[156];
        let linkname = String::from_utf8_lossy(
            &block[157..257]
                .iter()
                .take_while(|b| **b != 0)
                .cloned()
                .collect::<Vec<u8>>(),
        )
        .into_owned();

        offset += TAR_BLOCK;
        let data_blocks = (size as usize).div_ceil(TAR_BLOCK);
        let data_len = data_blocks * TAR_BLOCK;
        if offset + data_len > bytes.len() {
            return Err(TarExtractError::corrupt("truncated tar archive"));
        }
        let payload = &bytes[offset..offset + size as usize];
        offset += data_len;

        // GNU/pax name overrides apply to the next real header
        match typeflag {
            b'L' => {
                pending_name = Some(
                    String::from_utf8_lossy(payload)
                        .trim_end_matches('\0')
                        .to_string(),
                );
                continue;
            }
            b'K' => {
                pending_linkname = Some(
                    String::from_utf8_lossy(payload)
                        .trim_end_matches('\0')
                        .to_string(),
                );
                continue;
            }
            b'x' | b'g' => {
                // pax extended header: honor path=/linkpath= records
                for (key, value) in parse_pax_records(payload) {
                    if typeflag == b'x' {
                        match key.as_str() {
                            "path" => pending_name = Some(value),
                            "linkpath" => pending_linkname = Some(value),
                            _ => {}
                        }
                    }
                }
                continue;
            }
            _ => {}
        }

        let header = TarHeader {
            name: pending_name.take().unwrap_or(name),
            mode,
            typeflag,
            linkname: pending_linkname.take().unwrap_or(linkname),
        };

        match header.typeflag {
            // regular file (also '\0' and contiguous '7')
            b'0' | 0 | b'7' => {
                let path = dest.join(&header.name);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut file = std::fs::File::create(&path)?;
                file.write_all(payload)?;
                // fully_trusted: honor stored mode bits verbatim
                file.set_permissions(std::fs::Permissions::from_mode(header.mode & 0o7777))?;
                written += payload.len();
            }
            b'5' => {
                let path = dest.join(&header.name);
                std::fs::create_dir_all(&path)?;
                dir_modes.push((path, header.mode));
            }
            b'2' => {
                let path = dest.join(&header.name);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::os::unix::fs::symlink(&header.linkname, &path)?;
            }
            b'1' => {
                let path = dest.join(&header.name);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::hard_link(dest.join(&header.linkname), &path)?;
            }
            other => {
                return Err(TarExtractError::corrupt(&format!(
                    "unsupported tar member type '{}' for '{}'",
                    other as char, header.name
                )));
            }
        }
    }

    // Without a terminating zero block, non-zero trailing bytes mean a
    // truncated final header (tarfile.ReadError). Content AFTER a zero
    // block is ignored like tarfile does — this is what makes upstream's
    // raw leftover-tail flush at finalize time harmless (#594-era quirk
    // preserved in TreeWorker::finalize_rcopy_node).
    if !terminated && bytes[offset..].iter().any(|b| *b != 0) {
        return Err(TarExtractError::corrupt("truncated tar header"));
    }

    // apply delayed directory modes (deepest first)
    dir_modes.sort_by_key(|(path, _)| std::cmp::Reverse(path.components().count()));
    for (path, mode) in dir_modes {
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode & 0o7777))?;
    }

    Ok(written)
}

/// Parse pax extended-header records (`LEN KEY=VALUE\n`).
fn parse_pax_records(payload: &[u8]) -> Vec<(String, String)> {
    let mut records = Vec::new();
    let text = String::from_utf8_lossy(payload);
    let mut rest = text.as_ref();
    while !rest.is_empty() {
        // record length is the decimal prefix up to the first space
        let Some(space) = rest.find(' ') else { break };
        let Ok(len) = rest[..space].parse::<usize>() else {
            break;
        };
        if len == 0 || len > rest.len() {
            break;
        }
        let record = &rest[space + 1..len];
        if let Some(eq) = record.find('=') {
            let key = record[..eq].to_string();
            let value = record[eq + 1..].trim_end_matches('\n').to_string();
            records.push((key, value));
        }
        rest = &rest[len..];
    }
    records
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

    // ====================================================================
    // Upstream TreeWorkerTest mirrors (1.10 changes, #594/#622/PEP 706)
    //
    // The upstream tests need live SSH gateways; these are logic-level
    // equivalents around message handling (see mission notes).
    // ====================================================================

    use crate::communication::Message;
    use crate::topology::TopologyGraph;

    /// admin -> gw[1-2] -> node[1-2]: two gateways reaching the same
    /// targets, like the upstream GW2F1F test topology.
    fn build_gw2_topology() -> TopologyTree {
        let mut graph = TopologyGraph::new();
        graph
            .add_route(make_nodeset("admin"), make_nodeset("gw[1-2]"))
            .unwrap();
        graph
            .add_route(make_nodeset("gw[1-2]"), make_nodeset("node[1-2]"))
            .unwrap();
        graph.to_tree("admin").unwrap()
    }

    /// ACK a started gateway channel's configuration, which completes
    /// setup and dequeues the queued shell control message.
    fn ack_gateway(worker: &mut TreeWorker, gateway: &str) {
        worker
            .gateways
            .get_mut(gateway)
            .unwrap()
            .channel
            .recv(Message::ack(0))
            .unwrap();
    }

    /// Nodes that fired a Pickup event, in emission order (one entry per
    /// ev_pickup call, not deduplicated — like upstream's pickup_nodes).
    fn pickup_nodes(worker: &TreeWorker) -> Vec<String> {
        worker
            .events()
            .iter()
            .filter_map(|e| match e {
                TreeWorkerEvent::Pickup { node } => Some(node.clone()),
                _ => None,
            })
            .collect()
    }

    /// Build a ustar header block with a correct checksum.
    fn tar_header_block(name: &str, mode: u32, size: usize, typeflag: u8) -> Vec<u8> {
        assert!(name.len() <= 100);
        let mut h = vec![0u8; TAR_BLOCK];
        h[..name.len()].copy_from_slice(name.as_bytes());
        let mode_s = format!("{:o}", mode);
        h[100..100 + mode_s.len()].copy_from_slice(mode_s.as_bytes());
        let size_s = format!("{:o}", size);
        h[124..124 + size_s.len()].copy_from_slice(size_s.as_bytes());
        h[156] = typeflag;
        h[257..262].copy_from_slice(b"ustar");
        for b in h.iter_mut().take(156).skip(148) {
            *b = b' ';
        }
        let sum: u64 = h.iter().map(|b| u64::from(*b)).sum();
        let chk = format!("{:o}\0 ", sum);
        h[148..148 + chk.len()].copy_from_slice(chk.as_bytes());
        h
    }

    /// Build a minimal ustar archive from directory and file members.
    fn make_tar(dirs: &[(&str, u32)], files: &[(&str, u32, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        for (name, mode) in dirs {
            out.extend(tar_header_block(name, *mode, 0, b'5'));
        }
        for (name, mode, data) in files {
            out.extend(tar_header_block(name, *mode, data.len(), b'0'));
            out.extend_from_slice(data);
            let pad = (TAR_BLOCK - data.len() % TAR_BLOCK) % TAR_BLOCK;
            out.extend(std::iter::repeat(0u8).take(pad));
        }
        // two terminating zero blocks
        out.extend(std::iter::repeat(0u8).take(2 * TAR_BLOCK));
        out
    }

    /// rcopy worker configuration over a temporary destination dir.
    fn rcopy_config(dest: &std::path::Path) -> TreeWorkerConfig {
        TreeWorkerConfig {
            source: Some("/remote/src".to_string()),
            dest: Some(dest.to_string_lossy().into_owned()),
            reverse: true,
            ..TreeWorkerConfig::default()
        }
    }

    // -- ev_pickup semantics (#594, upstream commit 9e688cc) --------------

    #[test]
    fn test_pickup_buffered_until_check_ini_flushes_in_order() {
        // Pickups emitted before ev_start are deferred, then flushed in
        // order right after Started (upstream _pending_pickups/_check_ini).
        let nodes = make_nodeset("node[1-2]");
        let mut worker = TreeWorker::new(nodes, "echo".to_string(), TreeWorkerConfig::default());

        worker.emit_pickup("node1");
        worker.emit_pickup("node2");
        assert!(worker.events().is_empty());
        assert_eq!(worker.pending_pickup_count(), 2);

        worker.check_ini();
        assert!(worker.is_initialized());
        assert_eq!(worker.pending_pickup_count(), 0);
        assert!(matches!(worker.events()[0], TreeWorkerEvent::Started));
        assert!(
            matches!(&worker.events()[1], TreeWorkerEvent::Pickup { node } if node == "node1")
        );
        assert!(
            matches!(&worker.events()[2], TreeWorkerEvent::Pickup { node } if node == "node2")
        );

        // this part is called once (upstream _initialized gate)
        worker.check_ini();
        assert_eq!(worker.events().len(), 3);
    }

    #[test]
    fn test_ev_start_waits_for_direct_children() {
        // Direct child workers delay ev_start until they have all started
        // (upstream _start_count >= _child_count); a direct pickup
        // emitted meanwhile is buffered and flushed after Started.
        let mut graph = TopologyGraph::new();
        graph
            .add_route(make_nodeset("admin"), make_nodeset("gw1"))
            .unwrap();
        graph
            .add_route(make_nodeset("gw1"), make_nodeset("node2"))
            .unwrap();
        let tree = graph.to_tree("admin").unwrap();

        // node2 is behind gw1; node1 is not in any route -> direct
        let mut worker =
            TreeWorker::new(make_nodeset("node[1-2]"), "echo".to_string(), TreeWorkerConfig::default());
        worker.set_topology("admin", &tree).unwrap();
        let hops = worker.start().unwrap();
        assert_eq!(hops.len(), 2);
        assert!(!worker.is_initialized());

        // direct child's ev_pickup arrives before its ev_start: buffered
        worker.emit_pickup("node1");
        assert!(worker.events().is_empty());

        worker.notify_child_start();
        assert!(worker.is_initialized());
        assert!(matches!(worker.events()[0], TreeWorkerEvent::Started));
        assert!(
            matches!(&worker.events()[1], TreeWorkerEvent::Pickup { node } if node == "node1")
        );
    }

    #[test]
    fn test_pickup_not_fired_while_ctl_queued_before_setup() {
        // Core #594 semantics: ev_pickup fires only when the command can
        // no longer be rerouted — a control message queued behind an
        // un-ACKed configuration must not trigger it.
        let tree = build_gw2_topology();
        let mut worker = TreeWorker::new(
            make_nodeset("node[1-2]"),
            "echo Lorem Ipsum".to_string(),
            TreeWorkerConfig::default(),
        );
        worker.set_topology("admin", &tree).unwrap();
        let hops = worker.start().unwrap();
        assert_eq!(hops.len(), 1);
        let gw = hops[0].0.to_string();

        worker.start_gateway(&gw, b"topo").unwrap();
        worker.sync_gateway_pickups(&gw);
        assert!(
            pickup_nodes(&worker).is_empty(),
            "queued-but-unsent CTL must not fire ev_pickup"
        );

        // After the configuration ACK the CTL leaves the process...
        ack_gateway(&mut worker, &gw);
        worker.sync_gateway_pickups(&gw);
        let mut pickups = pickup_nodes(&worker);
        pickups.sort();
        assert_eq!(pickups, vec!["node1".to_string(), "node2".to_string()]);

        // ...and never twice (1:1 ev_pickup-per-node invariant)
        worker.sync_gateway_pickups(&gw);
        assert_eq!(pickup_nodes(&worker).len(), 2);
    }

    #[test]
    fn test_tree_run_abort_on_start_no_pickups() {
        // Mirror of upstream test_tree_run_abort_on_start (#594):
        // aborted on ev_start -> ev_start fires, ev_pickup does not.
        let nodes = make_nodeset("node1");
        let mut worker = TreeWorker::new(nodes, "echo".to_string(), TreeWorkerConfig::default());

        worker.emit_pickup("node1"); // buffered before ev_start
        worker.abort(); // handler aborts during ev_start upstream
        worker.check_ini();

        assert!(worker.is_aborted());
        assert_eq!(worker.state(), WorkerState::Aborted);
        assert_eq!(
            worker
                .events()
                .iter()
                .filter(|e| matches!(e, TreeWorkerEvent::Started))
                .count(),
            1,
            "ev_start fires even when the worker is aborted"
        );
        assert!(pickup_nodes(&worker).is_empty());
    }

    #[test]
    fn test_tree_run_abort_on_start_multi_no_pickups() {
        // Mirror of upstream test_tree_run_abort_on_start_multi (#594):
        // buffered pickups for >1 node are all cleared; gateway targets
        // hang up with EX_PROTOCOL.
        let tree = build_gw2_topology();
        let mut worker = TreeWorker::new(
            make_nodeset("node[1-2]"),
            "echo Lorem Ipsum".to_string(),
            TreeWorkerConfig::default(),
        );
        worker.set_topology("admin", &tree).unwrap();
        worker.start().unwrap();
        worker.start_gateway("gw1", b"topo").unwrap();
        // CTL queued behind un-ACKed CFG: pickups buffered/unsent
        worker.abort();

        assert!(pickup_nodes(&worker).is_empty());
        let protocol_closes = worker
            .events()
            .iter()
            .filter(|e| matches!(e, TreeWorkerEvent::Close { rc: 76, .. }))
            .count();
        assert_eq!(protocol_closes, 2, "gateway targets must get EX_PROTOCOL");
    }

    #[test]
    fn test_no_pickups_after_abort() {
        // _emit_pickup gates on _aborted (#594): post-abort pickups are
        // dropped and any buffered ones are cleared.
        let nodes = make_nodeset("node[1-3]");
        let mut worker = TreeWorker::new(nodes, "echo".to_string(), TreeWorkerConfig::default());

        worker.emit_pickup("node1");
        worker.check_ini();
        assert_eq!(pickup_nodes(&worker), vec!["node1".to_string()]);

        // post-init pickups fire immediately...
        worker.emit_pickup("node2");
        assert_eq!(
            pickup_nodes(&worker),
            vec!["node1".to_string(), "node2".to_string()]
        );

        // ...but after abort they are dropped entirely
        worker.abort();
        worker.emit_pickup("node3");
        assert_eq!(
            pickup_nodes(&worker),
            vec!["node1".to_string(), "node2".to_string()]
        );
    }

    #[test]
    fn test_relaunch_emits_reroute_and_redispatches() {
        // relaunch() mirrors upstream _relaunch: routing event first,
        // then targets re-dispatched away from the failed gateway.
        let tree = build_gw2_topology();
        let mut worker = TreeWorker::new(
            make_nodeset("node[1-2]"),
            "echo".to_string(),
            TreeWorkerConfig::default(),
        );
        worker.set_topology("admin", &tree).unwrap();
        worker.start().unwrap();
        worker.start_gateway("gw1", b"topo").unwrap();
        assert_eq!(worker.target_count, 2);

        worker.mark_unreachable("gw1");
        let hops = worker.relaunch("gw1").unwrap();

        assert_eq!(hops.len(), 1);
        assert_eq!(hops[0].0.to_string(), "gw2");
        assert_eq!(hops[0].1.to_string(), "node[1-2]");
        assert_eq!(worker.target_count, 2, "target count preserved");
        assert!(!worker.gateways.contains_key("gw1"));
        assert!(worker.gateways.contains_key("gw2"));

        let routing = worker
            .events()
            .iter()
            .find_map(|e| match e {
                TreeWorkerEvent::Routing {
                    event,
                    gateway,
                    targets,
                } => Some((event, gateway, targets)),
                _ => None,
            })
            .expect("missing reroute event");
        assert_eq!(routing.0, "reroute");
        assert_eq!(routing.1, "gw1");
        assert_eq!(routing.2, "node1,node2");
    }

    #[test]
    fn test_tree_run_no_double_pickup_under_reroute() {
        // Mirror of upstream test_tree_run_gw2f1_no_double_pickup_under_
        // reroute (#594): a rerouted target fires ev_pickup at most once.
        let tree = build_gw2_topology();
        let mut worker = TreeWorker::new(
            make_nodeset("node[1-2]"),
            "echo Lorem Ipsum".to_string(),
            TreeWorkerConfig::default(),
        );
        worker.set_topology("admin", &tree).unwrap();
        worker.start().unwrap();
        worker.start_gateway("gw1", b"topo").unwrap();

        // gateway fails before setup: its queued CTL never left, so no
        // pickup has fired and the targets can be rerouted
        worker.mark_unreachable("gw1");
        let hops = worker.relaunch("gw1").unwrap();
        let survivor = hops[0].0.to_string();
        assert_eq!(survivor, "gw2");

        worker.start_gateway(&survivor, b"topo").unwrap();
        worker.sync_gateway_pickups(&survivor);
        assert!(pickup_nodes(&worker).is_empty());

        ack_gateway(&mut worker, &survivor);
        worker.sync_gateway_pickups(&survivor);

        // list (not set) comparison catches accidental double-fires
        let mut pickups = pickup_nodes(&worker);
        pickups.sort();
        assert_eq!(pickups, vec!["node1".to_string(), "node2".to_string()]);
    }

    #[test]
    fn test_tree_run_pickup_after_reroute() {
        // Mirror of upstream test_tree_run_gw2f1_pickup_after_reroute
        // (#594): a rerouted target's ev_pickup fires after _ev_routing.
        let tree = build_gw2_topology();
        let mut worker = TreeWorker::new(
            make_nodeset("node[1-2]"),
            "echo Lorem Ipsum".to_string(),
            TreeWorkerConfig::default(),
        );
        worker.set_topology("admin", &tree).unwrap();
        worker.start().unwrap();
        worker.start_gateway("gw1", b"topo").unwrap();

        worker.mark_unreachable("gw1");
        worker.relaunch("gw1").unwrap();
        worker.start_gateway("gw2", b"topo").unwrap();
        ack_gateway(&mut worker, "gw2");
        worker.sync_gateway_pickups("gw2");

        let events = worker.events();
        let routing_idx = events
            .iter()
            .position(|e| matches!(e, TreeWorkerEvent::Routing { event, .. } if event == "reroute"))
            .expect("missing routing event");
        let started_idx = events
            .iter()
            .position(|e| matches!(e, TreeWorkerEvent::Started))
            .expect("missing started event");
        assert!(routing_idx > started_idx, "ev_start must come first");

        for target in ["node1", "node2"] {
            let pickup_idx = events
                .iter()
                .position(|e| matches!(e, TreeWorkerEvent::Pickup { node } if node == target))
                .unwrap_or_else(|| panic!("missing ev_pickup for rerouted target {}", target));
            assert!(
                pickup_idx > routing_idx,
                "ev_pickup for rerouted target {} fired before _ev_routing",
                target
            );
        }
    }

    #[test]
    fn test_relaunch_onto_existing_gateway_fires_pickup_on_send() {
        // When rerouted targets land on a gateway whose channel is
        // already set up (it serves other targets), the command is sent
        // immediately and ev_pickup fires at once (#594).
        let tree = build_gw2_topology();
        let mut worker = TreeWorker::new(
            make_nodeset("node[1-2]"),
            "echo".to_string(),
            TreeWorkerConfig::default(),
        );
        worker.set_topology("admin", &tree).unwrap();
        worker.start().unwrap();
        worker.start_gateway("gw1", b"topo").unwrap();

        // simulate a second, already-established channel to gw2
        let mut channel = PropagationChannel::new("gw2");
        channel.start(b"topo");
        channel.recv(Message::ack(0)).unwrap();
        assert!(channel.channel.setup);
        worker.gateways.insert(
            "gw2".to_string(),
            GatewayState {
                channel,
                active_targets: HashSet::new(),
                started: true,
                ctl_unsent_targets: Vec::new(),
            },
        );

        worker.mark_unreachable("gw1");
        worker.relaunch("gw1").unwrap();

        // the surviving channel was already set up, so the relaunched
        // targets were picked up immediately after the reroute event
        let mut pickups = pickup_nodes(&worker);
        pickups.sort();
        assert_eq!(pickups, vec!["node1".to_string(), "node2".to_string()]);
        assert_eq!(worker.gateways["gw2"].active_targets.len(), 2);
    }

    // -- separate stderr on forward copy (#622, upstream commit 5ed4c34) --

    #[test]
    fn test_tree_copy_forces_separate_stderr() {
        // copy/rcopy: internal tar/scp errors are kept on a separate
        // stderr stream instead of merging into stdout (upstream #622).
        let mut cfg = TreeWorkerConfig::default();
        cfg.source = Some("/tmp/src".to_string());
        cfg.dest = Some("/tmp/dst".to_string());
        let worker = TreeWorker::new_copy(make_nodeset("node1"), cfg).unwrap();
        assert!(worker.stderr(), "forward copy must separate stderr");

        let mut cfg = TreeWorkerConfig::default();
        cfg.source = Some("/tmp/src".to_string());
        cfg.dest = Some("/tmp/dst".to_string());
        cfg.reverse = true;
        let worker = TreeWorker::new_copy(make_nodeset("node1"), cfg).unwrap();
        assert!(worker.stderr(), "rcopy must separate stderr");

        // command workers keep the requested setting
        let worker =
            TreeWorker::new(make_nodeset("node1"), "echo".to_string(), TreeWorkerConfig::default());
        assert!(!worker.stderr());
    }

    #[test]
    fn test_tree_worker_missing_arguments() {
        // Mirror of upstream test_tree_worker_missing_arguments: a copy
        // worker without source is rejected like Python's ValueError.
        let result = TreeWorker::new_copy(make_nodeset("node1"), TreeWorkerConfig::default());
        assert!(result.is_err());
        let err = result.err().expect("expected constructor error");
        assert!(err.to_string().contains("missing command or source"));
    }

    #[test]
    fn test_untar_command_quoting() {
        assert_eq!(untar_command("/tmp/dest"), "tar -xf - -C '/tmp/dest'");
        // single quotes are escaped the upstream way: ' -> '"'"'
        assert_eq!(
            untar_command("/tmp/a'b"),
            "tar -xf - -C '/tmp/a'\"'\"'b'"
        );
    }

    #[test]
    fn test_tar_command_quoting() {
        assert_eq!(
            tar_command("/tmp/src", "base"),
            "tar -cf - -C '/tmp/src' \
--transform \"s,^\\([^/]*\\)[/]*,\\1.$(hostname -s)/,\" \
'base' | base64 -w 65536"
        );
        assert!(tar_command("/tmp/a'b", "c'd").contains("'\"'\"'"));
    }

    #[test]
    fn test_remote_shell_command_modes() {
        // command mode passes the user command through
        let worker =
            TreeWorker::new(make_nodeset("node1"), "uname -a".to_string(), TreeWorkerConfig::default());
        assert_eq!(worker.remote_shell_command().as_deref(), Some("uname -a"));

        // forward copy sends the untar pipeline
        let mut cfg = TreeWorkerConfig::default();
        cfg.source = Some("/tmp/src".to_string());
        cfg.dest = Some("/tmp/dst".to_string());
        let worker = TreeWorker::new_copy(make_nodeset("node1"), cfg).unwrap();
        assert_eq!(
            worker.remote_shell_command().as_deref(),
            Some("tar -xf - -C '/tmp/dst'")
        );

        // rcopy sends the tar|base64 pipeline built from dirname/basename
        let mut cfg = TreeWorkerConfig::default();
        cfg.source = Some("/data/dir".to_string());
        cfg.dest = Some("/tmp/dst".to_string());
        cfg.reverse = true;
        let worker = TreeWorker::new_copy(make_nodeset("node1"), cfg).unwrap();
        assert_eq!(
            worker.remote_shell_command().as_deref(),
            Some(tar_command("/data", "dir").as_str())
        );
    }

    // -- rcopy receive/extract (upstream 973870b, d2fcd16) ----------------

    #[test]
    fn test_rcopy_msgline_accumulates_base64() {
        // Mirror of upstream _on_remote_node_msgline: rcopy stdout is
        // accumulated as base64 tar data (decodable only on 4-char
        // boundaries); other streams pass through as events.
        let destdir = tempfile::tempdir().unwrap();
        let mut worker =
            TreeWorker::new_copy(make_nodeset("node1"), rcopy_config(destdir.path())).unwrap();

        worker.on_remote_node_msgline("node1", b"TWF", SNAME_STDOUT, "gw1");
        assert!(worker
            .rcopy_states
            .get("node1")
            .unwrap()
            .tar_bytes
            .is_empty());
        worker.on_remote_node_msgline("node1", b"u", SNAME_STDOUT, "gw1");
        assert_eq!(worker.rcopy_states.get("node1").unwrap().tar_bytes, b"Man");

        // stderr still passes through as an event
        worker.on_remote_node_msgline("node1", b"oops", SNAME_STDERR, "gw1");
        assert!(worker.events().iter().any(
            |e| matches!(e, TreeWorkerEvent::StdErr { node, data, .. }
                if node == "node1" && data == b"oops")
        ));
        // the tar stream itself is never reported as stdout
        assert!(!worker
            .events()
            .iter()
            .any(|e| matches!(e, TreeWorkerEvent::StdOut { .. })));
    }

    #[test]
    fn test_msgline_passthrough_for_command_worker() {
        let mut worker =
            TreeWorker::new(make_nodeset("node1"), "echo".to_string(), TreeWorkerConfig::default());
        worker.on_remote_node_msgline("node1", b"out", SNAME_STDOUT, "gw1");
        worker.on_remote_node_msgline("node1", b"err", SNAME_STDERR, "gw1");
        assert!(matches!(&worker.events()[0], TreeWorkerEvent::StdOut { data, .. } if data == b"out"));
        assert!(matches!(&worker.events()[1], TreeWorkerEvent::StdErr { data, .. } if data == b"err"));
    }

    #[test]
    fn test_rcopy_finalize_extracts_and_preserves_mode() {
        // Mirror of upstream test_tree_rcopy_preserves_mode (PEP 706,
        // commit d2fcd16): fully_trusted extraction keeps group-write
        // mode bits (0664 is not downgraded to 0644).
        use std::os::unix::fs::PermissionsExt;

        let destdir = tempfile::tempdir().unwrap();
        let mut worker =
            TreeWorker::new_copy(make_nodeset("node1"), rcopy_config(destdir.path())).unwrap();

        let tar = make_tar(&[], &[("file.txt", 0o664, b"mode-check")]);
        let b64 = crate::communication::base64_encode(&tar);
        // awkward chunk sizes exercise the 4-char boundary logic
        for chunk in b64.as_bytes().chunks(7) {
            worker.on_remote_node_msgline("node1", chunk, SNAME_STDOUT, "gw1");
        }

        let written = worker.finalize_rcopy_node("node1", "gw1").unwrap();
        assert_eq!(written, b"mode-check".len());

        let extracted = destdir.path().join("file.txt");
        assert_eq!(std::fs::read(&extracted).unwrap(), b"mode-check");
        assert_eq!(
            std::fs::metadata(&extracted)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o664
        );
        assert!(worker.events().is_empty());
    }

    #[test]
    fn test_rcopy_finalize_corrupt_tar_reported_as_stderr() {
        // Mirror of upstream rcopy extraction error reporting (973870b):
        // a corrupt archive surfaces as a stderr event carrying the error
        // message as bytes — not an exception, not a crash.
        let destdir = tempfile::tempdir().unwrap();
        let mut worker =
            TreeWorker::new_copy(make_nodeset("node1"), rcopy_config(destdir.path())).unwrap();

        let mut tar = make_tar(&[], &[("file.txt", 0o644, b"data")]);
        tar[10] ^= 0xFF; // break the header checksum
        let b64 = crate::communication::base64_encode(&tar);
        worker.on_remote_node_msgline("node1", b64.as_bytes(), SNAME_STDOUT, "gw1");

        worker.finalize_rcopy_node("node1", "gw1").unwrap();

        assert_eq!(worker.events().len(), 1);
        match &worker.events()[0] {
            TreeWorkerEvent::StdErr {
                node,
                gateway,
                data,
            } => {
                assert_eq!(node, "node1");
                assert_eq!(gateway, "gw1");
                assert!(!data.is_empty(), "error reported as bytes");
                assert!(String::from_utf8_lossy(data).contains("checksum"));
            }
            e => panic!("expected StdErr event, got {:?}", e),
        }
        // nothing was extracted
        assert!(std::fs::read_dir(destdir.path()).unwrap().next().is_none());
    }

    #[test]
    fn test_rcopy_finalize_conflict_reported_as_stderr() {
        // Mirror of upstream _tree_rcopy_dir_error: a conflicting regular
        // file at an extract path fails on the I/O path (PEP 3151
        // IOError/OSError branch of 973870b).
        let destdir = tempfile::tempdir().unwrap();
        std::fs::write(destdir.path().join("conf"), b"conflict").unwrap();

        let mut worker =
            TreeWorker::new_copy(make_nodeset("node1"), rcopy_config(destdir.path())).unwrap();

        let tar = make_tar(&[("conf", 0o755)], &[("conf/file.txt", 0o644, b"x")]);
        let b64 = crate::communication::base64_encode(&tar);
        worker.on_remote_node_msgline("node1", b64.as_bytes(), SNAME_STDOUT, "gw1");

        worker.finalize_rcopy_node("node1", "gw1").unwrap();
        assert_eq!(worker.events().len(), 1);
        assert!(
            matches!(&worker.events()[0], TreeWorkerEvent::StdErr { node, data, .. }
                if node == "node1" && !data.is_empty())
        );
    }

    #[test]
    fn test_rcopy_finalize_raw_tail_ignored_like_upstream() {
        // Upstream flushes the raw undecodable base64 tail into the
        // archive before extracting; tarfile stops at the zero blocks so
        // the tail is harmless. Mirror: trailing base64 leftover after a
        // valid archive must not fail extraction.
        let destdir = tempfile::tempdir().unwrap();
        let mut worker =
            TreeWorker::new_copy(make_nodeset("node1"), rcopy_config(destdir.path())).unwrap();

        let tar = make_tar(&[], &[("file.txt", 0o644, b"ok")]);
        let mut b64 = crate::communication::base64_encode(&tar);
        b64.push('A'); // one undecodable leftover char (< 4)
        worker.on_remote_node_msgline("node1", b64.as_bytes(), SNAME_STDOUT, "gw1");

        let written = worker.finalize_rcopy_node("node1", "gw1").unwrap();
        assert_eq!(written, 2);
        assert_eq!(
            std::fs::read(destdir.path().join("file.txt")).unwrap(),
            b"ok"
        );
        assert!(worker.events().is_empty());
    }

    #[test]
    fn test_rcopy_finalize_without_data_is_noop() {
        // upstream logs "no rcopy buffer received from %s" — no error
        let destdir = tempfile::tempdir().unwrap();
        let mut worker =
            TreeWorker::new_copy(make_nodeset("node1"), rcopy_config(destdir.path())).unwrap();
        assert_eq!(worker.finalize_rcopy_node("node1", "gw1").unwrap(), 0);
        assert!(worker.events().is_empty());

        // non-rcopy workers finalize as a no-op too
        let mut worker =
            TreeWorker::new(make_nodeset("node1"), "echo".to_string(), TreeWorkerConfig::default());
        assert_eq!(worker.finalize_rcopy_node("node1", "gw1").unwrap(), 0);
    }

    // -- gateway errors (upstream 90d3195, a027c3e) -----------------------

    #[test]
    fn test_report_gateway_error_before_channel_setup() {
        // Mirror of upstream 90d3195: an ErrorMessage reason received
        // before the channel ACK is reported as the gateway's own stderr
        // instead of being logged and dropped.
        let mut worker =
            TreeWorker::new(make_nodeset("node1"), "echo".to_string(), TreeWorkerConfig::default());
        worker.report_gateway_error("gw1", "invalid configuration data");

        assert_eq!(worker.events().len(), 1);
        match &worker.events()[0] {
            TreeWorkerEvent::StdErr {
                node,
                gateway,
                data,
            } => {
                assert_eq!(node, "gw1");
                assert_eq!(gateway, "gw1");
                assert_eq!(data, b"invalid configuration data");
            }
            e => panic!("expected StdErr event, got {:?}", e),
        }
    }

    #[test]
    fn test_report_gateway_stderr_splits_lines_and_keeps_empty() {
        // _report_stderr line semantics (upstream 90d3195 + #249):
        // per line, per node; an empty buffer still yields one empty line.
        let mut worker =
            TreeWorker::new(make_nodeset("node[1-2]"), "echo".to_string(), TreeWorkerConfig::default());
        let nodes = make_nodeset("node[1-2]");
        worker.report_gateway_stderr("gw1", &nodes, b"line1\nline2");

        let got: Vec<(String, Vec<u8>)> = worker
            .events()
            .iter()
            .filter_map(|e| match e {
                TreeWorkerEvent::StdErr { node, data, .. } => Some((node.clone(), data.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("node1".to_string(), b"line1".to_vec()),
                ("node2".to_string(), b"line1".to_vec()),
                ("node1".to_string(), b"line2".to_vec()),
                ("node2".to_string(), b"line2".to_vec()),
            ]
        );

        worker.drain_events();
        worker.report_gateway_stderr("gw1", &make_nodeset("node1"), b"");
        assert_eq!(worker.events().len(), 1);
        assert!(matches!(&worker.events()[0], TreeWorkerEvent::StdErr { data, .. } if data.is_empty()));
    }

    #[test]
    fn test_process_channel_error_surfaces_message_not_crash() {
        // Mirror of upstream a027c3e: a MessageProcessingError from
        // malformed gateway traffic is reported as gateway stderr; the
        // worker itself survives.
        let mut worker =
            TreeWorker::new(make_nodeset("node1"), "echo".to_string(), TreeWorkerConfig::default());

        worker.process_channel_error("gw1", &MessageProcessingError::UnknownType("ABC".into()));
        worker.process_channel_error("gw1", &MessageProcessingError::NoType);

        assert_eq!(worker.state(), WorkerState::Pending, "worker must not crash");
        assert_eq!(worker.events().len(), 2);
        assert!(
            matches!(&worker.events()[0], TreeWorkerEvent::StdErr { node, data, .. }
                if node == "gw1" && data == b"Unknown message type ABC")
        );
        assert!(
            matches!(&worker.events()[1], TreeWorkerEvent::StdErr { node, data, .. }
                if node == "gw1" && data == b"Unknown message with no type")
        );
    }
}
