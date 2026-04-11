//! Cluster topology and routing tree.
//!
//! Rust implementation of `ClusterShell.Topology`.
//!
//! A topology configuration file uses INI-style syntax:
//!
//! ```ini
//! [routes]
//! admin: gateways[0-10]
//! gateways[0-10]: nodes[0-100]
//! ```

use crate::node_set::NodeSet;
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, Error)]
pub enum TopologyError {
    #[error("topology error: {0}")]
    General(String),

    #[error("topology parse error: {0}")]
    ParseError(String),
}

pub type Result<T> = std::result::Result<T, TopologyError>;

// ============================================================================
// TopologyNodeGroup
// ============================================================================

/// A node group in the topology tree. Contains a nodeset and parent-child
/// relationships with other groups.
#[derive(Debug, Clone)]
pub struct TopologyNodeGroup {
    /// The nodes in this group.
    pub nodeset: NodeSet,
    /// Child groups.
    children: Vec<TopologyNodeGroup>,
    /// Cached union of all children's nodesets.
    children_ns: Option<NodeSet>,
}

impl TopologyNodeGroup {
    /// Create a new node group from a nodeset.
    pub fn new(nodeset: NodeSet) -> Self {
        Self {
            nodeset,
            children: Vec::new(),
            children_ns: None,
        }
    }

    /// Add a child group. Duplicate children (by nodeset string) are ignored.
    pub fn add_child(&mut self, child: TopologyNodeGroup) {
        assert!(!child.nodeset.is_empty(), "empty nodeset in child");

        let child_str = child.nodeset.to_string();
        for existing in &self.children {
            if existing.nodeset.to_string() == child_str {
                return;
            }
        }

        match &mut self.children_ns {
            Some(ns) => ns.update(&child.nodeset),
            None => self.children_ns = Some(child.nodeset.clone()),
        }
        self.children.push(child);
    }

    /// Remove a child group.
    pub fn clear_child(
        &mut self,
        child_nodeset: &NodeSet,
        strict: bool,
    ) -> std::result::Result<(), String> {
        let child_str = child_nodeset.to_string();
        if let Some(pos) = self
            .children
            .iter()
            .position(|c| c.nodeset.to_string() == child_str)
        {
            let removed = self.children.remove(pos);
            if let Some(ref mut ns) = self.children_ns {
                ns.difference_update(&removed.nodeset);
                if ns.is_empty() {
                    self.children_ns = None;
                }
            }
            Ok(())
        } else if strict {
            Err(format!("child {} not found", child_str))
        } else {
            Ok(())
        }
    }

    /// Remove all children.
    pub fn clear_children(&mut self) {
        self.children.clear();
        self.children_ns = None;
    }

    /// Get the children list.
    pub fn children(&self) -> &[TopologyNodeGroup] {
        &self.children
    }

    /// Return the children as a combined nodeset.
    pub fn children_ns(&self) -> Option<&NodeSet> {
        self.children_ns.as_ref()
    }

    /// Returns the total number of nodes across all children.
    pub fn children_len(&self) -> usize {
        self.children_ns.as_ref().map_or(0, |ns| ns.len())
    }

    /// Recursive printable subtree.
    pub fn printable_subtree(
        &self,
        prefix: &str,
        is_root: bool,
        is_first_level: bool,
        is_last: bool,
    ) -> String {
        let mut res = String::new();

        if is_root {
            res.push_str(&format!("{}\n", self.nodeset));
        } else if is_first_level {
            if !is_last {
                res.push_str(&format!("|- {}\n", self.nodeset));
            } else {
                res.push_str(&format!("`- {}\n", self.nodeset));
            }
        } else if !is_last {
            res.push_str(&format!("{}|- {}\n", prefix, self.nodeset));
        } else {
            res.push_str(&format!("{}`- {}\n", prefix, self.nodeset));
        }

        let child_count = self.children.len();
        for (i, child) in self.children.iter().enumerate() {
            let child_is_last = i == child_count - 1;

            let new_prefix = if is_root {
                String::new()
            } else if is_first_level {
                if !is_last {
                    "|  ".to_string()
                } else {
                    "   ".to_string()
                }
            } else {
                let extra = if !is_last { "|  " } else { "   " };
                format!("{}{}", prefix, extra)
            };

            res.push_str(&child.printable_subtree(
                &new_prefix,
                false,
                is_root, // children of root are first level
                child_is_last,
            ));
        }

        res
    }
}

impl fmt::Display for TopologyNodeGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<TopologyNodeGroup ({})>", self.nodeset)
    }
}

// ============================================================================
// TopologyRoute
// ============================================================================

/// A single route between two nodesets.
#[derive(Debug, Clone)]
pub struct TopologyRoute {
    pub src: NodeSet,
    pub dst: NodeSet,
}

impl TopologyRoute {
    /// Create a new route. Errors if src and dst overlap.
    pub fn new(src: NodeSet, dst: NodeSet) -> Result<Self> {
        let overlap = src.intersection(&dst);
        if !overlap.is_empty() {
            return Err(TopologyError::General(
                "Source and destination nodesets overlap".to_string(),
            ));
        }
        Ok(Self { src, dst })
    }

    /// Get the route's destination if the given nodeset is a subset of src.
    pub fn dest(&self, nodeset: Option<&NodeSet>) -> Option<&NodeSet> {
        match nodeset {
            None => Some(&self.dst),
            Some(ns) => {
                if ns.is_subset(&self.src) {
                    Some(&self.dst)
                } else {
                    None
                }
            }
        }
    }
}

impl fmt::Display for TopologyRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> {}", self.src, self.dst)
    }
}

// ============================================================================
// TopologyRoutingTable
// ============================================================================

/// Stores and manages topology routes with validation.
#[derive(Debug)]
pub struct TopologyRoutingTable {
    routes: Vec<TopologyRoute>,
    pub aggregated_src: NodeSet,
    pub aggregated_dst: NodeSet,
}

impl TopologyRoutingTable {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            aggregated_src: NodeSet::new(),
            aggregated_dst: NodeSet::new(),
        }
    }

    /// Add a new route with circular reference and convergent path checks.
    pub fn add_route(&mut self, route: TopologyRoute) -> Result<()> {
        if self.introduces_circular_reference(&route) {
            return Err(TopologyError::General(format!(
                "Loop detected! Cannot add route {}",
                route
            )));
        }
        if self.introduces_convergent_paths(&route) {
            return Err(TopologyError::General(format!(
                "Convergent path detected! Cannot add route {}",
                route
            )));
        }

        self.aggregated_src.update(&route.src);
        self.aggregated_dst.update(&route.dst);
        self.routes.push(route);
        Ok(())
    }

    /// Find the aggregation of directly connected children from src_ns.
    pub fn connected(&self, src_ns: &NodeSet) -> Option<NodeSet> {
        let mut next_hop = NodeSet::new();
        for route in &self.routes {
            if let Some(dst) = route.dest(Some(src_ns)) {
                next_hop.update(dst);
            }
        }
        if next_hop.is_empty() {
            None
        } else {
            Some(next_hop)
        }
    }

    /// Iterate over routes.
    pub fn iter(&self) -> impl Iterator<Item = &TopologyRoute> {
        self.routes.iter()
    }

    /// Check whether adding this route would create a topology loop.
    fn introduces_circular_reference(&self, route: &TopologyRoute) -> bool {
        let mut current_ns = route.dst.clone();
        loop {
            match self.connected(&current_ns) {
                None => return false,
                Some(dest) => {
                    if dest.is_empty() {
                        return false;
                    }
                    let overlap = dest.intersection(&route.src);
                    if !overlap.is_empty() {
                        return true;
                    }
                    current_ns = dest;
                }
            }
        }
    }

    /// Check for undesired convergent paths.
    fn introduces_convergent_paths(&self, route: &TopologyRoute) -> bool {
        for known_route in &self.routes {
            // source cannot be a strict superset of an already known destination
            if route.src.is_superset(&known_route.dst)
                && route.src.to_string() != known_route.dst.to_string()
            {
                return true;
            }
            // destination cannot be a strict subset of a known source
            if route.dst.is_subset(&known_route.src)
                && route.dst.to_string() != known_route.src.to_string()
            {
                return true;
            }
            // two different source groups cannot point to overlapping destinations
            let dst_overlap = route.dst.intersection(&known_route.dst);
            if !dst_overlap.is_empty() && route.src.to_string() != known_route.src.to_string() {
                return true;
            }
        }
        false
    }
}

impl Default for TopologyRoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TopologyRoutingTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let strs: Vec<String> = self.routes.iter().map(|r| r.to_string()).collect();
        write!(f, "{}", strs.join("\n"))
    }
}

// ============================================================================
// TopologyGraph
// ============================================================================

/// Represents a complete network topology by storing "can reach" relations.
#[derive(Debug)]
pub struct TopologyGraph {
    routing: TopologyRoutingTable,
    nodegroups: HashMap<String, TopologyNodeGroup>,
    root: String,
}

impl TopologyGraph {
    pub fn new() -> Self {
        Self {
            routing: TopologyRoutingTable::new(),
            nodegroups: HashMap::new(),
            root: String::new(),
        }
    }

    /// Add a route from src nodeset to dst nodeset.
    pub fn add_route(&mut self, src_ns: NodeSet, dst_ns: NodeSet) -> Result<()> {
        let route = TopologyRoute::new(src_ns, dst_ns)?;
        self.routing.add_route(route)
    }

    /// Return the aggregated destinations for a given nodeset.
    pub fn dest(&self, from_nodeset: &NodeSet) -> Option<NodeSet> {
        self.routing.connected(from_nodeset)
    }

    /// Convert the routing table to a topology tree of nodegroups.
    pub fn to_tree(&mut self, root: &str) -> Result<TopologyTree> {
        self.routes_to_tng();
        self.validate(root)?;

        let mut tree = TopologyTree::new();
        if let Some(root_group) = self.nodegroups.remove(&self.root) {
            tree.load(root_group);
        }
        Ok(tree)
    }

    /// Convert routes to TopologyNodeGroup instances with parent-child links.
    ///
    /// Two-phase approach to handle Rust ownership (Python uses object refs):
    /// 1. Build flat nodegroup map + adjacency list (parent_key -> [child_keys])
    /// 2. Recursively build tree bottom-up so children are complete before cloning
    fn routes_to_tng(&mut self) {
        self.nodegroups.clear();

        let aggregated_src = self.routing.aggregated_src.clone();

        // Phase 1: Create flat nodegroups (no children yet)
        let mut flat: HashMap<String, NodeSet> = HashMap::new();

        for route in self.routing.iter() {
            let src_key = route.src.to_string();
            flat.entry(src_key).or_insert_with(|| route.src.clone());

            // Leaf destinations (not also a source)
            let leaf = route.dst.difference(&aggregated_src);
            if !leaf.is_empty() {
                let leaf_key = leaf.to_string();
                flat.entry(leaf_key).or_insert_with(|| leaf);
            }
        }

        // Phase 1b: Build adjacency list
        let keys: Vec<String> = flat.keys().cloned().collect();
        let mut adj: HashMap<String, Vec<String>> = HashMap::new();

        for parent_key in &keys {
            let parent_ns = &flat[parent_key];
            if let Some(dst_ns) = self.routing.connected(parent_ns) {
                for child_key in &keys {
                    if child_key != parent_key {
                        let child_ns = &flat[child_key];
                        if child_ns.is_subset(&dst_ns) {
                            adj.entry(parent_key.clone())
                                .or_default()
                                .push(child_key.clone());
                        }
                    }
                }
            }
        }

        // Phase 2: Recursively build TopologyNodeGroup tree bottom-up
        fn build_group(
            key: &str,
            flat: &HashMap<String, NodeSet>,
            adj: &HashMap<String, Vec<String>>,
        ) -> TopologyNodeGroup {
            let ns = flat[key].clone();
            let mut group = TopologyNodeGroup::new(ns);
            if let Some(children) = adj.get(key) {
                for child_key in children {
                    let child = build_group(child_key, flat, adj);
                    group.add_child(child);
                }
            }
            group
        }

        for key in &keys {
            let group = build_group(key, &flat, &adj);
            self.nodegroups.insert(key.clone(), group);
        }
    }

    /// Validate the graph for tree conversion.
    fn validate(&mut self, root: &str) -> Result<()> {
        if self.nodegroups.is_empty() {
            return Err(TopologyError::General(
                "No route found in topology definition!".to_string(),
            ));
        }

        // Find the nodegroup containing the root node
        let found_key = self
            .nodegroups
            .iter()
            .find(|(_, v)| v.nodeset.contains(root))
            .map(|(k, _)| k.clone());

        match found_key {
            Some(key) => {
                if key != root {
                    let group = self.nodegroups.remove(&key).unwrap();
                    self.nodegroups.insert(root.to_string(), group);
                }
                self.root = root.to_string();
                Ok(())
            }
            None => Err(TopologyError::General(format!(
                "\"{}\" is not a valid root node!",
                root
            ))),
        }
    }
}

impl Default for TopologyGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TopologyGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "<TopologyGraph>")?;
        for (k, v) in &self.nodegroups {
            writeln!(f, "{}: {}", k, v)?;
        }
        Ok(())
    }
}

// ============================================================================
// TopologyTree
// ============================================================================

/// A simplified network topology represented as a tree of node groups.
#[derive(Debug)]
pub struct TopologyTree {
    root: Option<TopologyNodeGroup>,
    groups: Vec<NodeSet>,
}

impl TopologyTree {
    pub fn new() -> Self {
        Self {
            root: None,
            groups: Vec::new(),
        }
    }

    /// Load a topology tree from a root nodegroup.
    pub fn load(&mut self, root: TopologyNodeGroup) {
        self.groups.clear();
        Self::collect_groups(&root, &mut self.groups);
        self.root = Some(root);
    }

    fn collect_groups(group: &TopologyNodeGroup, out: &mut Vec<NodeSet>) {
        out.push(group.nodeset.clone());
        for child in group.children() {
            Self::collect_groups(child, out);
        }
    }

    /// Find a nodegroup containing the given node.
    pub fn find_nodegroup(&self, node: &str) -> Result<&NodeSet> {
        for group in &self.groups {
            if group.contains(node) {
                return Ok(group);
            }
        }
        Err(TopologyError::General(format!(
            "TopologyNodeGroup not found for node {}",
            node
        )))
    }

    /// Count of nodes in groups that have children (inner nodes: root + gateways).
    pub fn inner_node_count(&self) -> usize {
        fn count_inner(group: &TopologyNodeGroup) -> usize {
            let mut count = if group.children_len() > 0 {
                group.nodeset.len()
            } else {
                0
            };
            for child in group.children() {
                count += count_inner(child);
            }
            count
        }

        self.root.as_ref().map_or(0, count_inner)
    }

    /// Count of nodes in leaf groups (no children).
    pub fn leaf_node_count(&self) -> usize {
        fn count_leaves(group: &TopologyNodeGroup) -> usize {
            let mut count = if group.children_len() == 0 {
                group.nodeset.len()
            } else {
                0
            };
            for child in group.children() {
                count += count_leaves(child);
            }
            count
        }

        self.root.as_ref().map_or(0, count_leaves)
    }

    /// Iterate over all nodegroups in the tree (depth-first, stack-based).
    pub fn iter(&self) -> TopologyTreeIter<'_> {
        TopologyTreeIter {
            stack: self.root.as_ref().map_or_else(Vec::new, |r| vec![r]),
        }
    }

    /// Get a reference to the root group.
    pub fn root(&self) -> Option<&TopologyNodeGroup> {
        self.root.as_ref()
    }
}

impl Default for TopologyTree {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TopologyTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            None => write!(f, "<TopologyTree instance (empty)>"),
            Some(root) => write!(f, "{}", root.printable_subtree("", true, false, false)),
        }
    }
}

/// Stack-based iterator over TopologyNodeGroup references.
pub struct TopologyTreeIter<'a> {
    stack: Vec<&'a TopologyNodeGroup>,
}

impl<'a> Iterator for TopologyTreeIter<'a> {
    type Item = &'a TopologyNodeGroup;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        for child in node.children() {
            self.stack.push(child);
        }
        Some(node)
    }
}

// ============================================================================
// TopologyParser
// ============================================================================

/// Parser for topology configuration files.
///
/// Reads INI-style files with a `[routes]` (or `[Main]`) section where
/// each entry is `src_nodeset: dst_nodeset`.
pub struct TopologyParser {
    graph: Option<TopologyGraph>,
    tree_cache: Option<TopologyTree>,
}

impl TopologyParser {
    /// Create a new empty parser.
    pub fn new() -> Self {
        Self {
            graph: None,
            tree_cache: None,
        }
    }

    /// Create a parser and immediately load a topology file.
    pub fn from_file(filename: &str) -> Result<Self> {
        let mut parser = Self::new();
        parser.load(filename)?;
        Ok(parser)
    }

    /// Load a topology configuration file.
    pub fn load(&mut self, filename: &str) -> Result<()> {
        let content = std::fs::read_to_string(filename).map_err(|e| {
            TopologyError::ParseError(format!("Invalid configuration file: {} ({})", filename, e))
        })?;

        let routes = Self::parse_ini(&content)?;
        self.build_graph(routes)?;
        Ok(())
    }

    /// Parse INI-style content to extract routes.
    fn parse_ini(content: &str) -> Result<Vec<(String, String)>> {
        let mut routes = Vec::new();
        let mut in_section = false;

        for line in content.lines() {
            let trimmed = line.trim();

            // Skip empty lines and comments
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            // Section headers
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                let section = &trimmed[1..trimmed.len() - 1];
                in_section =
                    section.eq_ignore_ascii_case("routes") || section.eq_ignore_ascii_case("main");
                continue;
            }

            if in_section {
                // Parse "src: dst" or "src:dst"
                if let Some(colon_pos) = trimmed.find(':') {
                    let src = trimmed[..colon_pos].trim().to_string();
                    let dst = trimmed[colon_pos + 1..].trim().to_string();
                    if !src.is_empty() && !dst.is_empty() {
                        routes.push((src, dst));
                    }
                }
            }
        }

        Ok(routes)
    }

    /// Build the topology graph from parsed routes.
    fn build_graph(&mut self, routes: Vec<(String, String)>) -> Result<()> {
        let mut graph = TopologyGraph::new();

        for (src_str, dst_str) in routes {
            let src_ns = NodeSet::parse(&src_str).map_err(|e| {
                TopologyError::ParseError(format!(
                    "Failed to parse source nodeset '{}': {}",
                    src_str, e
                ))
            })?;
            let dst_ns = NodeSet::parse(&dst_str).map_err(|e| {
                TopologyError::ParseError(format!(
                    "Failed to parse destination nodeset '{}': {}",
                    dst_str, e
                ))
            })?;

            if !src_ns.is_empty() && !dst_ns.is_empty() {
                graph.add_route(src_ns, dst_ns)?;
            }
        }

        self.graph = Some(graph);
        self.tree_cache = None;
        Ok(())
    }

    /// Get or build the propagation tree for the given root node.
    pub fn tree(&mut self, root: &str) -> Result<&TopologyTree> {
        let graph = self
            .graph
            .as_mut()
            .ok_or_else(|| TopologyError::General("No topology loaded".to_string()))?;

        let tree = graph.to_tree(root)?;
        self.tree_cache = Some(tree);
        Ok(self.tree_cache.as_ref().unwrap())
    }

    /// Parse a topology file and return the tree directly (convenience).
    /// Note: requires a root node, so callers should prefer from_file() + tree().
    #[allow(dead_code)]
    pub fn parse(_filename: &str) -> Result<TopologyTree> {
        Err(TopologyError::General(
            "Use TopologyParser::from_file() and .tree(root) instead".to_string(),
        ))
    }
}

impl Default for TopologyParser {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn ns(s: &str) -> NodeSet {
        NodeSet::parse(s).unwrap()
    }

    // --- TopologyRoute tests ---

    #[test]
    fn test_route_basic() {
        let r = TopologyRoute::new(ns("node[0-5]"), ns("node[6-10]")).unwrap();
        assert_eq!(r.dest(None).unwrap().to_string(), "node[6-10]");
        assert_eq!(
            r.dest(Some(&ns("node[0-5]"))).unwrap().to_string(),
            "node[6-10]"
        );
        assert!(r.dest(Some(&ns("node[100-200]"))).is_none());
    }

    #[test]
    fn test_route_overlap_error() {
        let result = TopologyRoute::new(ns("node[0-5]"), ns("node[3-10]"));
        assert!(result.is_err());
    }

    #[test]
    fn test_route_display() {
        let r = TopologyRoute::new(ns("src[0-9]"), ns("dst[5-8]")).unwrap();
        assert_eq!(r.to_string(), "src[0-9] -> dst[5-8]");
    }

    // --- TopologyRoutingTable tests ---

    #[test]
    fn test_routing_table_basic() {
        let mut rt = TopologyRoutingTable::new();
        let r = TopologyRoute::new(ns("node[0-5]"), ns("node[6-10]")).unwrap();
        rt.add_route(r).unwrap();

        let connected = rt.connected(&ns("node[0-5]")).unwrap();
        assert_eq!(connected.to_string(), "node[6-10]");
    }

    #[test]
    fn test_routing_table_circular_ref() {
        let mut rt = TopologyRoutingTable::new();
        rt.add_route(TopologyRoute::new(ns("a"), ns("b")).unwrap())
            .unwrap();
        rt.add_route(TopologyRoute::new(ns("b"), ns("c")).unwrap())
            .unwrap();
        // c -> a would create a loop
        let result = rt.add_route(TopologyRoute::new(ns("c"), ns("a")).unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Loop detected"));
    }

    #[test]
    fn test_routing_table_convergent_paths() {
        let mut rt = TopologyRoutingTable::new();
        rt.add_route(TopologyRoute::new(ns("a"), ns("c")).unwrap())
            .unwrap();
        // b -> c would create convergent paths (two sources pointing to same dst)
        let result = rt.add_route(TopologyRoute::new(ns("b"), ns("c")).unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Convergent path"));
    }

    #[test]
    fn test_routing_table_display() {
        let mut rt = TopologyRoutingTable::new();
        rt.add_route(TopologyRoute::new(ns("src[0-9]"), ns("dst[5-8]")).unwrap())
            .unwrap();
        rt.add_route(TopologyRoute::new(ns("src[10-19]"), ns("dst[15-18]")).unwrap())
            .unwrap();
        assert_eq!(
            rt.to_string(),
            "src[0-9] -> dst[5-8]\nsrc[10-19] -> dst[15-18]"
        );
    }

    // --- TopologyGraph tests ---

    #[test]
    fn test_graph_basic() {
        let mut g = TopologyGraph::new();
        g.add_route(ns("nodes[0-5]"), ns("nodes[6-10]")).unwrap();
        let dest = g.dest(&ns("nodes[0-5]")).unwrap();
        assert_eq!(dest.to_string(), "nodes[6-10]");
    }

    #[test]
    fn test_graph_multiple_routes() {
        let mut g = TopologyGraph::new();
        let admin = ns("admin");
        let ns0 = ns("nodes[0-9]");
        let ns1 = ns("nodes[10-19]");
        g.add_route(admin.clone(), ns0.clone()).unwrap();
        g.add_route(ns0.clone(), ns1.clone()).unwrap();

        let ns2 = ns("nodes[20-29]");
        g.add_route(ns0.clone(), ns2.clone()).unwrap();
        // Adding same dst again is OK
        g.add_route(ns0.clone(), ns2.clone()).unwrap();

        assert_eq!(g.dest(&admin).unwrap().to_string(), "nodes[0-9]");
        let dest_ns0 = g.dest(&ns0).unwrap();
        assert_eq!(dest_ns0.len(), 20);
    }

    #[test]
    fn test_graph_bad_link() {
        let mut g = TopologyGraph::new();
        g.add_route(ns("admin"), ns("nodes[0-9]")).unwrap();
        g.add_route(ns("nodes[0-9]"), ns("nodes[10-19]")).unwrap();
        let result = g.add_route(ns("nodes[10-19]"), ns("nodes[0-9]"));
        assert!(result.is_err());
    }

    #[test]
    fn test_graph_overlapping_routes() {
        let mut g = TopologyGraph::new();
        // src == dst
        assert!(g.add_route(ns("nodes[0-9]"), ns("nodes[0-9]")).is_err());

        g.add_route(ns("nodes[0-9]"), ns("nodes[10-19]")).unwrap();
        // Overlapping destination
        assert!(g.add_route(ns("nodes[0-9]"), ns("nodes[5-29]")).is_err());
    }

    #[test]
    fn test_graph_to_tree() {
        let mut g = TopologyGraph::new();
        g.add_route(ns("admin"), ns("nodes[0-9]")).unwrap();
        g.add_route(ns("nodes[0-9]"), ns("nodes[10-19]")).unwrap();
        g.add_route(ns("nodes[0-9]"), ns("nodes[20-29]")).unwrap();

        let tree = g.to_tree("admin").unwrap();

        let mut all_nodes = NodeSet::new();
        for group in tree.iter() {
            all_nodes.update(&group.nodeset);
        }

        assert_eq!(all_nodes.len(), ns("admin,nodes[0-29]").len());
    }

    #[test]
    fn test_graph_invalid_root() {
        let mut g = TopologyGraph::new();
        g.add_route(ns("node[0-9]"), ns("node[10-19]")).unwrap();
        let result = g.to_tree("admin1");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not a valid root node"));
    }

    // --- TopologyTree tests ---

    #[test]
    fn test_tree_empty_display() {
        let tree = TopologyTree::new();
        assert_eq!(tree.to_string(), "<TopologyTree instance (empty)>");
    }

    #[test]
    fn test_tree_counts() {
        let mut g = TopologyGraph::new();
        g.add_route(ns("admin"), ns("proxy")).unwrap();
        g.add_route(ns("proxy"), ns("STA[0-1]")).unwrap();
        g.add_route(ns("STA0"), ns("STB[0-1]")).unwrap();
        g.add_route(ns("STB0"), ns("nodes[0-2]")).unwrap();
        g.add_route(ns("STB1"), ns("nodes[3-5]")).unwrap();
        g.add_route(ns("STA1"), ns("STB[2-3]")).unwrap();
        g.add_route(ns("STB2"), ns("nodes[6-7]")).unwrap();
        g.add_route(ns("STB3"), ns("nodes[8-10]")).unwrap();

        let tree = g.to_tree("admin").unwrap();
        assert_eq!(tree.inner_node_count(), 8);
        assert_eq!(tree.leaf_node_count(), 11);
    }

    #[test]
    fn test_tree_iteration() {
        let mut g = TopologyGraph::new();
        g.add_route(ns("admin"), ns("nodes[0-9]")).unwrap();
        g.add_route(ns("nodes[0-9]"), ns("nodes[10-19]")).unwrap();

        let tree = g.to_tree("admin").unwrap();

        let mut all = NodeSet::new();
        for group in tree.iter() {
            all.update(&group.nodeset);
        }
        assert_eq!(all.len(), 21); // admin + 20 nodes
    }

    // --- TopologyNodeGroup tests ---

    #[test]
    fn test_nodegroup_add_child() {
        let mut t0 = TopologyNodeGroup::new(ns("node[0-9]"));
        let t1 = TopologyNodeGroup::new(ns("node[10-19]"));

        t0.add_child(t1.clone());
        assert_eq!(t0.children_ns().unwrap().to_string(), "node[10-19]");
        // Adding same child again is a no-op
        t0.add_child(t1);
        assert_eq!(t0.children_ns().unwrap().to_string(), "node[10-19]");
    }

    #[test]
    fn test_nodegroup_clear_child() {
        let mut t0 = TopologyNodeGroup::new(ns("node[0-9]"));
        let t1 = TopologyNodeGroup::new(ns("node[10-19]"));

        t0.add_child(t1.clone());
        t0.clear_child(&ns("node[10-19]"), false).unwrap();
        assert!(t0.children_ns().is_none());

        // Non-strict removal of nonexistent child is OK
        t0.clear_child(&ns("node[10-19]"), false).unwrap();
        // Strict removal of nonexistent child errors
        assert!(t0.clear_child(&ns("node[10-19]"), true).is_err());
    }

    #[test]
    fn test_nodegroup_clear_children() {
        let mut t0 = TopologyNodeGroup::new(ns("node[0-9]"));
        let t1 = TopologyNodeGroup::new(ns("node[10-19]"));
        let t2 = TopologyNodeGroup::new(ns("node[20-29]"));

        t0.add_child(t1);
        t0.add_child(t2);
        assert_eq!(t0.children_len(), 20);
        t0.clear_children();
        assert!(t0.children_ns().is_none());
        assert_eq!(t0.children_len(), 0);
    }

    #[test]
    fn test_nodegroup_display() {
        let t = TopologyNodeGroup::new(ns("admin0"));
        assert_eq!(t.to_string(), "<TopologyNodeGroup (admin0)>");
    }

    // --- TopologyParser tests ---

    #[test]
    fn test_parser_invalid_file() {
        let result = TopologyParser::from_file("/invalid/path/for/testing");
        assert!(result.is_err());
    }

    #[test]
    fn test_parser_basic() {
        let mut tmpfile = NamedTempFile::new().unwrap();
        writeln!(tmpfile, "# this is a comment").unwrap();
        writeln!(tmpfile, "[routes]").unwrap();
        writeln!(tmpfile, "admin: nodes[0-1]").unwrap();
        writeln!(tmpfile, "nodes[0-1]: nodes[2-5]").unwrap();
        writeln!(tmpfile, "nodes[4-5]: nodes[6-9]").unwrap();
        tmpfile.flush().unwrap();

        let mut parser = TopologyParser::from_file(tmpfile.path().to_str().unwrap()).unwrap();
        let tree = parser.tree("admin").unwrap();

        let mut all = NodeSet::new();
        for group in tree.iter() {
            all.update(&group.nodeset);
        }
        assert_eq!(all.to_string(), ns("admin,nodes[0-9]").to_string());
    }

    #[test]
    fn test_parser_main_compat() {
        let mut tmpfile = NamedTempFile::new().unwrap();
        writeln!(tmpfile, "# comment").unwrap();
        writeln!(tmpfile, "[Main]").unwrap();
        writeln!(tmpfile, "admin: nodes[0-1]").unwrap();
        writeln!(tmpfile, "nodes[0-1]: nodes[2-5]").unwrap();
        writeln!(tmpfile, "nodes[4-5]: nodes[6-9]").unwrap();
        tmpfile.flush().unwrap();

        let mut parser = TopologyParser::from_file(tmpfile.path().to_str().unwrap()).unwrap();
        let tree = parser.tree("admin").unwrap();

        let mut all = NodeSet::new();
        for group in tree.iter() {
            all.update(&group.nodeset);
        }
        assert_eq!(all.to_string(), ns("admin,nodes[0-9]").to_string());
    }

    #[test]
    fn test_parser_short_syntax() {
        let mut tmpfile = NamedTempFile::new().unwrap();
        writeln!(tmpfile, "[routes]").unwrap();
        writeln!(tmpfile, "admin: nodes[0-9]").unwrap();
        writeln!(tmpfile, "nodes[0-3,5]: nodes[10-19]").unwrap();
        writeln!(tmpfile, "nodes[4,6-9]: nodes[30-39]").unwrap();
        tmpfile.flush().unwrap();

        let mut parser = TopologyParser::from_file(tmpfile.path().to_str().unwrap()).unwrap();
        let tree = parser.tree("admin").unwrap();

        let mut all = NodeSet::new();
        for group in tree.iter() {
            all.update(&group.nodeset);
        }
        assert_eq!(all.to_string(), ns("admin,nodes[0-19,30-39]").to_string());
    }

    #[test]
    fn test_parser_long_syntax() {
        let mut tmpfile = NamedTempFile::new().unwrap();
        writeln!(tmpfile, "[routes]").unwrap();
        writeln!(tmpfile, "admin: proxy").unwrap();
        writeln!(tmpfile, "proxy: STA[0-1]").unwrap();
        writeln!(tmpfile, "STA0: STB[0-1]").unwrap();
        writeln!(tmpfile, "STB0: nodes[0-2]").unwrap();
        writeln!(tmpfile, "STB1: nodes[3-5]").unwrap();
        writeln!(tmpfile, "STA1: STB[2-3]").unwrap();
        writeln!(tmpfile, "STB2: nodes[6-7]").unwrap();
        writeln!(tmpfile, "STB3: nodes[8-10]").unwrap();
        tmpfile.flush().unwrap();

        let mut parser = TopologyParser::from_file(tmpfile.path().to_str().unwrap()).unwrap();
        let tree = parser.tree("admin").unwrap();
        assert_eq!(tree.inner_node_count(), 8);
        assert_eq!(tree.leaf_node_count(), 11);

        let mut all = NodeSet::new();
        for group in tree.iter() {
            all.update(&group.nodeset);
        }
        assert_eq!(
            all.to_string(),
            ns("admin,proxy,STA[0-1],STB[0-3],nodes[0-10]").to_string()
        );
    }

    #[test]
    fn test_parser_deep_tree() {
        let mut tmpfile = NamedTempFile::new().unwrap();
        writeln!(tmpfile, "[routes]").unwrap();
        writeln!(tmpfile, "admin: nodes[0-9]").unwrap();

        let levels = 15;
        for i in (0..levels * 10).step_by(10) {
            writeln!(
                tmpfile,
                "nodes[{}-{}]: nodes[{}-{}]",
                i,
                i + 9,
                i + 10,
                i + 19
            )
            .unwrap();
        }
        tmpfile.flush().unwrap();

        let mut parser = TopologyParser::from_file(tmpfile.path().to_str().unwrap()).unwrap();
        let tree = parser.tree("admin").unwrap();
        assert_eq!(tree.inner_node_count(), 151);
        assert_eq!(tree.leaf_node_count(), 10);
    }

    #[test]
    fn test_parser_big_tree() {
        let mut tmpfile = NamedTempFile::new().unwrap();
        writeln!(tmpfile, "[routes]").unwrap();
        writeln!(tmpfile, "admin: ST[0-4]").unwrap();
        writeln!(tmpfile, "ST[0-4]: STA[0-49]").unwrap();
        writeln!(tmpfile, "STA[0-49]: nodes[0-10000]").unwrap();
        tmpfile.flush().unwrap();

        let mut parser = TopologyParser::from_file(tmpfile.path().to_str().unwrap()).unwrap();
        let tree = parser.tree("admin").unwrap();
        assert_eq!(tree.inner_node_count(), 56);
        assert_eq!(tree.leaf_node_count(), 10001);
    }

    #[test]
    fn test_parser_convergent_paths() {
        let mut tmpfile = NamedTempFile::new().unwrap();
        writeln!(tmpfile, "[routes]").unwrap();
        writeln!(tmpfile, "fortoy32: fortoy[33-34]").unwrap();
        writeln!(tmpfile, "fortoy33: fortoy35").unwrap();
        writeln!(tmpfile, "fortoy34: fortoy36").unwrap();
        writeln!(tmpfile, "fortoy[35-36]: fortoy37").unwrap();
        tmpfile.flush().unwrap();

        let mut parser = TopologyParser::new();
        let result = parser.load(tmpfile.path().to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_parser_multiple_admin_groups() {
        let mut tmpfile = NamedTempFile::new().unwrap();
        writeln!(tmpfile, "[routes]").unwrap();
        writeln!(tmpfile, "admin0: nodes[0-1]").unwrap();
        writeln!(tmpfile, "admin2: nodes[2-3]").unwrap();
        writeln!(tmpfile, "nodes[0-1]: nodes[10-19]").unwrap();
        writeln!(tmpfile, "nodes[2-3]: nodes[20-29]").unwrap();
        tmpfile.flush().unwrap();

        let mut parser = TopologyParser::from_file(tmpfile.path().to_str().unwrap()).unwrap();
        let tree = parser.tree("admin2").unwrap();
        assert_eq!(tree.inner_node_count(), 3);
        assert_eq!(tree.leaf_node_count(), 10);
    }

    #[test]
    fn test_tree_printing() {
        let mut g = TopologyGraph::new();
        g.add_route(ns("n0"), ns("n[1-2]")).unwrap();
        g.add_route(ns("n1"), ns("n[10-49]")).unwrap();
        g.add_route(ns("n2"), ns("n[50-89]")).unwrap();

        let tree = g.to_tree("n0").unwrap();
        let display = tree.to_string();

        let ref1 = "n0\n|- n1\n|  `- n[10-49]\n`- n2\n   `- n[50-89]\n";
        let ref2 = "n0\n|- n2\n|  `- n[50-89]\n`- n1\n   `- n[10-49]\n";
        assert!(display == ref1 || display == ref2, "Got: {}", display);
    }

    #[test]
    fn test_node_string_topology() {
        let mut tmpfile = NamedTempFile::new().unwrap();
        writeln!(tmpfile, "[routes]").unwrap();

        let nodes: Vec<String> = (0..=10).map(|i| format!("node{}", i)).collect();
        let mut prev = "admin".to_string();
        for n in &nodes {
            writeln!(tmpfile, "{}: {}", prev, n).unwrap();
            prev = n.clone();
        }
        tmpfile.flush().unwrap();

        let mut parser = TopologyParser::from_file(tmpfile.path().to_str().unwrap()).unwrap();
        let tree = parser.tree("admin").unwrap();

        let mut all = NodeSet::new();
        for group in tree.iter() {
            all.update(&group.nodeset);
        }

        let mut expected = ns("admin");
        expected.update(&ns("node[0-10]"));
        assert_eq!(all.to_string(), expected.to_string());
    }

    #[test]
    fn test_graph_big_groups() {
        let mut g = TopologyGraph::new();
        g.add_route(ns("nodes[0-10000]"), ns("nodes[12000-23000]"))
            .unwrap();
        assert_eq!(
            g.dest(&ns("nodes[0-10000]")).unwrap().to_string(),
            "nodes[12000-23000]"
        );
    }
}
