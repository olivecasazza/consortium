//! Group resolution backends.
//!
//! Rust implementation of `ClusterShell.NodeUtils`.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use configparser::ini::Ini;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GroupSourceError {
    #[error("no upcall configured for group source")]
    GroupSourceNoUpcall,

    #[error("group source query failed: {0}")]
    GroupSourceQueryFailed(String),
}

#[derive(Debug, Error)]
pub enum GroupResolverSourceError {
    #[error("source not found: {0}")]
    SourceNotFound(String),

    #[error("source error: {0}")]
    SourceError(#[from] GroupSourceError),
}

#[derive(Debug, Error)]
pub enum GroupResolverConfigError {
    #[error("config file not found: {0}")]
    FileNotFound(PathBuf),

    #[error("config parsing error: {0}")]
    ParseError(String),
}

#[derive(Debug, Error)]
pub enum GroupError {
    #[error("group source error: {0}")]
    SourceError(#[from] GroupSourceError),

    #[error("group resolver error: {0}")]
    ResolverError(#[from] GroupResolverSourceError),

    #[error("group resolver config error: {0}")]
    ConfigError(#[from] GroupResolverConfigError),
}

pub type Result<T> = std::result::Result<T, GroupError>;

/// A source of node group definitions.
///
/// Group sources can be backed by various backends: static in-memory data,
/// shell scripts, configuration files, etc.
pub trait GroupSource: Send + Sync {
    /// Resolve a group name to a node pattern string.
    ///
    /// Returns a comma-separated list of node names or a node pattern.
    fn resolve_map(&self, group: &str) -> Result<String>;

    /// Get all nodes from all groups.
    ///
    /// Returns a comma-separated list of all nodes.
    fn resolve_all(&self) -> Result<String> {
        Err(GroupSourceError::GroupSourceNoUpcall.into())
    }

    /// List all available group names.
    fn resolve_list(&self) -> Result<Vec<String>> {
        Err(GroupSourceError::GroupSourceNoUpcall.into())
    }

    /// Find all groups that contain a given node.
    fn resolve_reverse(&self, _node: &str) -> Result<Vec<String>> {
        Err(GroupSourceError::GroupSourceNoUpcall.into())
    }

    /// Returns true if this source supports reverse lookups.
    fn has_reverse(&self) -> bool {
        false
    }

    /// Returns the cache time in seconds, if any.
    fn cache_time(&self) -> Option<f64> {
        None
    }
}

/// In-memory group source backed by a HashMap.
///
/// Supports basic resolve_map, and optionally resolve_all, resolve_list,
/// and resolve_reverse if provided.
pub struct StaticGroupSource {
    map: HashMap<String, String>,
    all: Option<String>,
    list: Option<Vec<String>>,
    reverse: Option<HashMap<String, Vec<String>>>,
}

impl StaticGroupSource {
    /// Create a new StaticGroupSource with the given map.
    pub fn new(map: HashMap<String, String>) -> Self {
        Self {
            map,
            all: None,
            list: None,
            reverse: None,
        }
    }

    /// Set the all nodes string.
    pub fn with_all(mut self, all: impl Into<String>) -> Self {
        self.all = Some(all.into());
        self
    }

    /// Set the list of group names.
    pub fn with_list(mut self, list: Vec<String>) -> Self {
        self.list = Some(list);
        self
    }

    /// Set the reverse lookup map (node -> groups containing it).
    pub fn with_reverse(mut self, reverse: HashMap<String, Vec<String>>) -> Self {
        self.reverse = Some(reverse);
        self
    }
}

impl GroupSource for StaticGroupSource {
    fn resolve_map(&self, group: &str) -> Result<String> {
        self.map
            .get(group)
            .cloned()
            .ok_or(GroupSourceError::GroupSourceNoUpcall.into())
    }

    fn resolve_all(&self) -> Result<String> {
        self.all
            .clone()
            .ok_or(GroupSourceError::GroupSourceNoUpcall.into())
    }

    fn resolve_list(&self) -> Result<Vec<String>> {
        self.list
            .clone()
            .ok_or(GroupSourceError::GroupSourceNoUpcall.into())
    }

    fn resolve_reverse(&self, node: &str) -> Result<Vec<String>> {
        self.reverse
            .as_ref()
            .and_then(|r| r.get(node).cloned())
            .ok_or(GroupSourceError::GroupSourceNoUpcall.into())
    }

    fn has_reverse(&self) -> bool {
        self.reverse.is_some()
    }
}

/// Default upcall cache lifetime in seconds (matches Python's
/// `UpcallGroupSource` default of 3600).
const DEFAULT_CACHE_TIME: f64 = 3600.0;

/// Cache for upcall results, mirroring Python `UpcallGroupSource._cache`.
///
/// Only engaged when a `mapall` upcall is configured; without `mapall`
/// every upcall is executed on demand, exactly as before.
#[derive(Debug, Default)]
struct UpcallCache {
    /// group -> (nodes string, expiry), filled from mapall output or by
    /// per-group `map` fallback calls
    map: HashMap<String, (String, Instant)>,
    /// (group names in mapall output order, expiry)
    list: Option<(Vec<String>, Instant)>,
    /// freshness marker of the last successful mapall call
    mapall_expiry: Option<Instant>,
}

/// Shell-based group source that runs commands for lookups.
///
/// Commands can use %s as a placeholder for the group name.
///
/// The optional `mapall` upcall returns all group-to-nodes mappings in a
/// single call (upstream commit 68e5df9). Its output is a list of
/// `group: nodes` lines and is used to serve both `map` and `list`
/// queries from the cache. When a group is missing from the mapall
/// output, resolution falls back to the per-group `map` upcall if one is
/// defined; otherwise the group resolves to an empty node set.
pub struct UpcallGroupSource {
    map_upcall: Option<String>,
    all_upcall: Option<String>,
    list_upcall: Option<String>,
    reverse_upcall: Option<String>,
    mapall_upcall: Option<String>,
    cache_time: f64,
    cache: Mutex<UpcallCache>,
}

impl UpcallGroupSource {
    /// Create a new UpcallGroupSource with the given command templates.
    ///
    /// Command templates can use %s as a placeholder for the group name.
    pub fn new(
        map_upcall: Option<String>,
        all_upcall: Option<String>,
        list_upcall: Option<String>,
        reverse_upcall: Option<String>,
    ) -> Self {
        Self {
            map_upcall,
            all_upcall,
            list_upcall,
            reverse_upcall,
            mapall_upcall: None,
            cache_time: DEFAULT_CACHE_TIME,
            cache: Mutex::new(UpcallCache::default()),
        }
    }

    /// Set the optional `mapall` upcall command template, returning all
    /// group-to-nodes mappings as `group: nodes` lines in one call.
    pub fn with_mapall_upcall(mut self, mapall_upcall: Option<String>) -> Self {
        self.mapall_upcall = mapall_upcall;
        self
    }

    /// Set the upcall cache lifetime in seconds (0 disables caching).
    pub fn with_cache_time(mut self, cache_time: f64) -> Self {
        self.cache_time = cache_time;
        self
    }

    /// Remove all previously cached upcall results whatever their
    /// lifetime is (mirrors Python `UpcallGroupSource.clear_cache()`).
    pub fn clear_cache(&self) {
        let mut cache = self.cache.lock().unwrap();
        *cache = UpcallCache::default();
    }

    fn cache_duration(&self) -> Duration {
        Duration::from_secs_f64(self.cache_time.max(0.0))
    }

    /// Run the optional `mapall` upcall and fill the map and list caches
    /// from its output. No-op if `mapall` is not defined or its last
    /// result is still fresh. Mirrors Python `_populate_from_mapall()`:
    /// the map cache is swapped (not merged), upcall failures and parse
    /// errors are not cached (the next query retries), and duplicate
    /// group lines are unioned.
    fn populate_from_mapall(&self) -> Result<()> {
        let mapall_tpl = match &self.mapall_upcall {
            Some(tpl) => tpl,
            None => return Ok(()),
        };
        {
            let cache = self.cache.lock().unwrap();
            if let Some(expiry) = cache.mapall_expiry {
                if Instant::now() < expiry {
                    return Ok(());
                }
            }
        }

        // Command failure (eg. non-zero exit) propagates and is not
        // cached, so the next query retries the upcall.
        let content = self.run_command(mapall_tpl, "")?;
        let expiry = Instant::now() + self.cache_duration();

        // Parse 'group: nodes' lines, preserving first-seen group order.
        let mut order: Vec<String> = Vec::new();
        let mut values: Vec<String> = Vec::new();
        let mut seen: HashMap<String, usize> = HashMap::new();
        for raw_line in content.lines() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(2, ':');
            let group = parts.next().unwrap_or("").trim();
            let nodes = parts.next().map(|n| n.trim());
            let nodes = match nodes {
                // every line must contain a ':' separator
                Some(nodes) if !group.is_empty() && group.split_whitespace().count() == 1 => nodes,
                _ => {
                    // do not keep unusable output: leave no freshness
                    // marker so the next query retries the upcall
                    return Err(GroupSourceError::GroupSourceQueryFailed(format!(
                        "mapall: invalid line {:?} (expected 'group: nodes')",
                        line
                    ))
                    .into());
                }
            };
            if let Some(&idx) = seen.get(group) {
                // union duplicate group lines, like multi-line map output
                let old = values[idx].clone();
                values[idx] = [old.as_str(), nodes]
                    .into_iter()
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
                    .join(",");
            } else {
                seen.insert(group.to_string(), order.len());
                order.push(group.to_string());
                values.push(nodes.to_string());
            }
        }

        let mut cache = self.cache.lock().unwrap();
        cache.mapall_expiry = Some(expiry);
        // swap the map cache, do not merge: removed groups are gone
        cache.map = order
            .iter()
            .cloned()
            .zip(values)
            .map(|(group, nodes)| (group, (nodes, expiry)))
            .collect();
        cache.list = Some((order, expiry));
        Ok(())
    }

    fn run_command(&self, template: &str, arg: &str) -> Result<String> {
        let command = template.replace("%s", arg);
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(&command)
            .env("GROUP", arg)
            .output()
            .map_err(|e| GroupSourceError::GroupSourceQueryFailed(e.to_string()))?;

        if output.status.success() {
            Ok(String::from_utf8(output.stdout)
                .map_err(|e| GroupSourceError::GroupSourceQueryFailed(e.to_string()))?
                .trim()
                .to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(GroupSourceError::GroupSourceQueryFailed(stderr.to_string()).into())
        }
    }
}

/// Split an upcall output string into individual names (comma or
/// whitespace separated).
fn split_upcall_output(s: &str) -> Vec<String> {
    s.split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

impl GroupSource for UpcallGroupSource {
    fn resolve_map(&self, group: &str) -> Result<String> {
        if self.mapall_upcall.is_some() {
            // mapall configured: serve from the mapall-populated cache
            self.populate_from_mapall()?;
            let mut cache = self.cache.lock().unwrap();
            if self.map_upcall.is_none() {
                // no map fallback: unknown group resolves to an empty
                // node set (freshness only gates the mapall re-run)
                return Ok(cache
                    .map
                    .get(group)
                    .map(|(nodes, _)| nodes.clone())
                    .unwrap_or_default());
            }
            // map fallback: cached entries (from mapall or an earlier
            // fallback call) win while fresh
            if let Some((nodes, expiry)) = cache.map.get(group) {
                if Instant::now() < *expiry {
                    return Ok(nodes.clone());
                }
            }
            let nodes = self.run_command(self.map_upcall.as_ref().unwrap(), group)?;
            cache.map.insert(
                group.to_string(),
                (nodes.clone(), Instant::now() + self.cache_duration()),
            );
            return Ok(nodes);
        }
        match self.map_upcall.as_ref() {
            Some(t) => self.run_command(t, group),
            None => Err(GroupSourceError::GroupSourceNoUpcall.into()),
        }
    }

    fn resolve_all(&self) -> Result<String> {
        // note: 'all' is NOT derived from mapall output
        match self.all_upcall.as_ref() {
            Some(t) => self.run_command(t, ""),
            None => Err(GroupSourceError::GroupSourceNoUpcall.into()),
        }
    }

    fn resolve_list(&self) -> Result<Vec<String>> {
        if self.mapall_upcall.is_some() {
            // mapall configured: its output takes precedence for listing
            self.populate_from_mapall()?;
            let mut cache = self.cache.lock().unwrap();
            if self.list_upcall.is_none() {
                // no list fallback: read mapall result directly (the
                // freshness check only gates the mapall re-run, so this
                // also works with cache_time 0)
                return Ok(cache
                    .list
                    .as_ref()
                    .map(|(groups, _)| groups.clone())
                    .unwrap_or_default());
            }
            // list fallback: a fresh mapall-derived list wins; otherwise
            // the real list upcall runs and is cached
            if let Some((groups, expiry)) = &cache.list {
                if Instant::now() < *expiry {
                    return Ok(groups.clone());
                }
            }
            let s = self.run_command(self.list_upcall.as_ref().unwrap(), "")?;
            let groups = split_upcall_output(&s);
            cache.list = Some((groups.clone(), Instant::now() + self.cache_duration()));
            return Ok(groups);
        }
        match self.list_upcall.as_ref() {
            Some(t) => {
                let s = self.run_command(t, "")?;
                Ok(split_upcall_output(&s))
            }
            None => Err(GroupSourceError::GroupSourceNoUpcall.into()),
        }
    }

    fn resolve_reverse(&self, node: &str) -> Result<Vec<String>> {
        match self.reverse_upcall.as_ref() {
            Some(t) => {
                let s = self.run_command(t, node)?;
                Ok(s.split(|c: char| c == ',' || c.is_whitespace())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect())
            }
            None => Err(GroupSourceError::GroupSourceNoUpcall.into()),
        }
    }

    fn has_reverse(&self) -> bool {
        self.reverse_upcall.is_some()
    }
}

/// Manages named group sources with a default source.
///
/// Resolves group references like `@group` to node sets.
pub struct GroupResolver {
    sources: HashMap<String, Box<dyn GroupSource>>,
    default_source_name: Option<String>,
    illegal_chars: HashSet<char>,
}

impl GroupResolver {
    /// Create a new GroupResolver.
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            default_source_name: None,
            illegal_chars: HashSet::new(),
        }
    }

    /// Add a named source to the resolver.
    pub fn add_source(&mut self, name: impl Into<String>, source: Box<dyn GroupSource>) {
        self.sources.insert(name.into(), source);
    }

    /// Set the default source name.
    pub fn set_default(&mut self, name: impl Into<String>) {
        self.default_source_name = Some(name.into());
    }

    /// Get the source by name, or the default if name is None.
    fn get_source(&self, namespace: Option<&str>) -> Result<&dyn GroupSource> {
        let name = match namespace {
            Some(n) => n,
            None => self.default_source_name.as_ref().ok_or(
                GroupResolverSourceError::SourceNotFound("no default source set".to_string()),
            )?,
        };

        self.sources
            .get(name)
            .map(|s| s.as_ref())
            .ok_or(GroupResolverSourceError::SourceNotFound(name.to_string()).into())
    }

    /// Resolve a group to a list of nodes.
    ///
    /// If namespace is Some, uses that source; otherwise uses the default.
    pub fn group_nodes(&self, group: &str, namespace: Option<&str>) -> Result<Vec<String>> {
        let source = self.get_source(namespace)?;
        let pattern = source.resolve_map(group)?;
        Ok(pattern
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect())
    }

    /// Get all nodes from all groups in the specified namespace.
    pub fn all_nodes(&self, namespace: Option<&str>) -> Result<Vec<String>> {
        let source = self.get_source(namespace)?;
        let all = source.resolve_all()?;
        Ok(all
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect())
    }

    /// List all group names in the specified namespace.
    pub fn grouplist(&self, namespace: Option<&str>) -> Result<Vec<String>> {
        let source = self.get_source(namespace)?;
        source.resolve_list()
    }

    /// Find all groups containing a given node.
    pub fn node_groups(&self, node: &str, namespace: Option<&str>) -> Result<Vec<String>> {
        let source = self.get_source(namespace)?;
        source.resolve_reverse(node)
    }

    /// Check if the namespace supports node group lookups.
    pub fn has_node_groups(&self, namespace: Option<&str>) -> bool {
        self.get_source(namespace)
            .map(|s| s.has_reverse())
            .unwrap_or(false)
    }

    /// Set illegal characters for group names.
    pub fn set_illegal_chars(&mut self, chars: HashSet<char>) {
        self.illegal_chars = chars;
    }

    /// Check if a group name contains illegal characters.
    pub fn is_valid_group_name(&self, name: &str) -> Result<()> {
        for c in name.chars() {
            if self.illegal_chars.contains(&c) {
                return Err(GroupResolverSourceError::SourceError(
                    GroupSourceError::GroupSourceQueryFailed(format!(
                        "group name contains illegal character: '{}'",
                        c
                    )),
                )
                .into());
            }
        }
        Ok(())
    }
}

impl Default for GroupResolver {
    fn default() -> Self {
        Self::new()
    }
}

/// Loads group sources from INI configuration files.
///
/// Supports the groups.conf format with:
/// - `[Main]` section for groupsdir, confdir, autodir, default
/// - Other sections as UpcallGroupSource definitions
pub struct GroupResolverConfig {
    filenames: Vec<PathBuf>,
    illegal_chars: HashSet<char>,
    initialized: bool,
    resolver: Option<GroupResolver>,
}

impl GroupResolverConfig {
    /// Create a new GroupResolverConfig with the given filenames.
    pub fn new(filenames: Vec<PathBuf>, illegal_chars: HashSet<char>) -> Self {
        Self {
            filenames,
            illegal_chars,
            initialized: false,
            resolver: None,
        }
    }

    /// Initialize the config if not already done.
    fn ensure_initialized(&mut self) -> Result<&mut GroupResolver> {
        if !self.initialized {
            self.initialize()?;
            self.initialized = true;
        }
        Ok(self.resolver.as_mut().unwrap())
    }

    /// Initialize the resolver from config files.
    fn initialize(&mut self) -> Result<()> {
        let mut resolver = GroupResolver::new();
        resolver.set_illegal_chars(self.illegal_chars.clone());

        for filename in &self.filenames {
            if !filename.exists() {
                return Err(GroupResolverConfigError::FileNotFound(filename.clone()).into());
            }

            let mut ini = Ini::new();
            // configparser lowercases keys/sections by default
            ini.load(filename.to_string_lossy().as_ref())
                .map_err(|e| GroupResolverConfigError::ParseError(e.to_string()))?;

            // Process [Main] section (lowercased by configparser)
            if let Some(default) = ini.get("main", "default") {
                resolver.set_default(default);
            }

            // Process other sections as UpcallGroupSource
            for section_name in ini.sections() {
                if section_name == "main" || section_name == "default" {
                    continue;
                }

                let map_upcall = ini.get(&section_name, "map");
                let mapall_upcall = ini.get(&section_name, "mapall");
                let all_upcall = ini.get(&section_name, "all");
                let list_upcall = ini.get(&section_name, "list");
                let reverse_upcall = ini.get(&section_name, "reverse");
                let cache_time = ini
                    .get(&section_name, "cache_time")
                    .and_then(|v| v.parse::<f64>().ok());

                // map or mapall is a mandatory upcall (upstream 68e5df9;
                // Python raises GroupResolverConfigError)
                if map_upcall.is_none() && mapall_upcall.is_none() {
                    return Err(GroupResolverConfigError::ParseError(format!(
                        "No option 'map' or 'mapall' in section: {:?}",
                        section_name
                    ))
                    .into());
                }

                let mut source =
                    UpcallGroupSource::new(map_upcall, all_upcall, list_upcall, reverse_upcall)
                        .with_mapall_upcall(mapall_upcall);
                if let Some(ctime) = cache_time {
                    source = source.with_cache_time(ctime);
                }
                resolver.add_source(section_name.clone(), Box::new(source));
            }
        }

        self.resolver = Some(resolver);
        Ok(())
    }

    /// Get the underlying resolver.
    pub fn resolver(&mut self) -> Result<&mut GroupResolver> {
        self.ensure_initialized()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_static_group_source_basic_resolve() {
        let mut map = HashMap::new();
        map.insert("web".to_string(), "web1,web2,web3".to_string());
        map.insert("db".to_string(), "db1,db2".to_string());

        let source = StaticGroupSource::new(map);

        assert_eq!(source.resolve_map("web").unwrap(), "web1,web2,web3");
        assert_eq!(source.resolve_map("db").unwrap(), "db1,db2");
    }

    #[test]
    fn test_static_group_source_missing_group() {
        let source = StaticGroupSource::new(HashMap::new());
        assert!(source.resolve_map("nonexistent").is_err());
    }

    #[test]
    fn test_static_group_source_with_all_and_list() {
        let mut map = HashMap::new();
        map.insert("web".to_string(), "web1,web2".to_string());
        map.insert("db".to_string(), "db1,db2".to_string());

        let source = StaticGroupSource::new(map)
            .with_all("web1,web2,db1,db2".to_string())
            .with_list(vec!["web".to_string(), "db".to_string()]);

        assert_eq!(source.resolve_all().unwrap(), "web1,web2,db1,db2");
        assert_eq!(source.resolve_list().unwrap(), vec!["web", "db"]);
    }

    #[test]
    fn test_static_group_source_reverse_lookup() {
        let mut map = HashMap::new();
        map.insert("web".to_string(), "web1,web2".to_string());
        map.insert("db".to_string(), "db1,db2".to_string());

        let mut reverse = HashMap::new();
        reverse.insert("web1".to_string(), vec!["web".to_string()]);
        reverse.insert("web2".to_string(), vec!["web".to_string()]);
        reverse.insert("db1".to_string(), vec!["db".to_string()]);
        reverse.insert("db2".to_string(), vec!["db".to_string()]);

        let source = StaticGroupSource::new(map).with_reverse(reverse);

        assert!(source.has_reverse());
        assert_eq!(
            source.resolve_reverse("web1").unwrap(),
            vec!["web".to_string()]
        );
        assert_eq!(
            source.resolve_reverse("db2").unwrap(),
            vec!["db".to_string()]
        );
    }

    #[test]
    fn test_group_resolver_with_multiple_sources() {
        let mut resolver = GroupResolver::new();

        let mut map1 = HashMap::new();
        map1.insert("web".to_string(), "web1,web2".to_string());
        resolver.add_source("cluster1", Box::new(StaticGroupSource::new(map1)));

        let mut map2 = HashMap::new();
        map2.insert("db".to_string(), "db1,db2".to_string());
        resolver.add_source("cluster2", Box::new(StaticGroupSource::new(map2)));

        let web_nodes = resolver.group_nodes("web", Some("cluster1")).unwrap();
        assert_eq!(web_nodes, vec!["web1", "web2"]);

        let db_nodes = resolver.group_nodes("db", Some("cluster2")).unwrap();
        assert_eq!(db_nodes, vec!["db1", "db2"]);
    }

    #[test]
    fn test_group_resolver_default_source() {
        let mut resolver = GroupResolver::new();

        let mut map1 = HashMap::new();
        map1.insert("web".to_string(), "web1,web2".to_string());
        resolver.add_source("default", Box::new(StaticGroupSource::new(map1)));

        resolver.set_default("default");

        // Use default source (no namespace)
        let web_nodes = resolver.group_nodes("web", None).unwrap();
        assert_eq!(web_nodes, vec!["web1", "web2"]);
    }

    #[test]
    fn test_group_resolver_missing_source() {
        let resolver = GroupResolver::new();
        assert!(resolver.group_nodes("web", Some("nonexistent")).is_err());
    }

    #[test]
    fn test_group_resolver_missing_default() {
        let resolver = GroupResolver::new();
        assert!(resolver.group_nodes("web", None).is_err());
    }

    #[test]
    fn test_illegal_chars_validation() {
        let mut resolver = GroupResolver::new();
        let mut illegal = HashSet::new();
        illegal.insert('$');
        illegal.insert(' ');

        resolver.set_illegal_chars(illegal);

        assert!(resolver.is_valid_group_name("web").is_ok());
        assert!(resolver.is_valid_group_name("web-group").is_ok());
        assert!(resolver.is_valid_group_name("web_group").is_ok());
        assert!(resolver.is_valid_group_name("web$group").is_err());
        assert!(resolver.is_valid_group_name("web group").is_err());
    }

    #[test]
    fn test_upcall_source_no_upcall() {
        let source = UpcallGroupSource::new(None, None, None, None);
        assert!(source.resolve_map("test").is_err());
        assert!(source.resolve_all().is_err());
        assert!(source.resolve_list().is_err());
        assert!(source.resolve_reverse("node").is_err());
        assert!(!source.has_reverse());
    }

    #[test]
    fn test_upcall_source_with_map() {
        let source = UpcallGroupSource::new(Some("echo web1,web2".to_string()), None, None, None);
        assert_eq!(source.resolve_map("test").unwrap(), "web1,web2");
    }

    #[test]
    fn test_group_resolver_sources_from_ini() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"[Main]
default = cluster1

[cluster1]
map = echo web1,web2

[cluster2]
map = echo db1,db2
list = echo web db
all = echo web1,web2,db1,db2
reverse = echo web
"#
        )
        .unwrap();

        let mut config = GroupResolverConfig::new(vec![file.path().to_path_buf()], HashSet::new());
        let resolver = config.resolver().unwrap();

        let web_nodes = resolver.group_nodes("test", Some("cluster1")).unwrap();
        assert_eq!(web_nodes, vec!["web1", "web2"]);

        let db_nodes = resolver.group_nodes("test", Some("cluster2")).unwrap();
        assert_eq!(db_nodes, vec!["db1", "db2"]);

        let list = resolver.grouplist(Some("cluster2")).unwrap();
        assert_eq!(list, vec!["web", "db"]);
    }

    #[test]
    fn test_group_resolver_config_file_not_found() {
        let mut config = GroupResolverConfig::new(
            vec![PathBuf::from("/nonexistent/path/groups.conf")],
            HashSet::new(),
        );
        assert!(config.resolver().is_err());
    }

    #[test]
    fn test_whitespace_splitting() {
        let mut map = HashMap::new();
        // Test with various whitespace
        map.insert(
            "web".to_string(),
            "web1, web2  ,web3\tweb4\nweb5".to_string(),
        );

        let source = StaticGroupSource::new(map);
        let nodes = source.resolve_map("web").unwrap();

        let resolver = GroupResolver::new();
        let parsed = nodes
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        assert_eq!(parsed, vec!["web1", "web2", "web3", "web4", "web5"]);
        drop(resolver);
    }

    // ── mapall upcall (upstream 68e5df9) ─────────────────────────────────
    // Mirrors tests/NodeSetGroupTest.py::GroupSourceMapallTest. Shell
    // upcalls play the role of StaticGroupSource's fake upcalls.

    /// Build a GroupResolver with `source` as the default source.
    fn resolver_with(source: UpcallGroupSource) -> GroupResolver {
        let mut resolver = GroupResolver::new();
        resolver.add_source("test", Box::new(source));
        resolver.set_default("test");
        resolver
    }

    #[test]
    fn test_mapall_basic() {
        // UpcallGroupSource with only a mapall upcall
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some("printf 'a: foo1\nb: foo2\nc: foo[3-4]\n'".to_string()));
        let res = resolver_with(source);
        assert_eq!(res.group_nodes("a", None).unwrap(), vec!["foo1"]);
        assert_eq!(res.group_nodes("b", None).unwrap(), vec!["foo2"]);
        assert_eq!(res.group_nodes("c", None).unwrap(), vec!["foo[3-4]"]);
        // list is also derived from mapall output
        let mut groups = res.grouplist(None).unwrap();
        groups.sort();
        assert_eq!(groups, vec!["a", "b", "c"]);
        // unknown group resolves to empty (no map upcall available)
        assert!(res.group_nodes("nope", None).unwrap().is_empty());
    }

    #[test]
    fn test_mapall_with_map_fallback() {
        // mapall + map: missing group falls back to per-group map
        let count = tempfile::NamedTempFile::new().unwrap();
        let map_cmd = format!("echo foo2 && echo called >> {}", count.path().display());
        let source = UpcallGroupSource::new(Some(map_cmd), None, None, None)
            .with_mapall_upcall(Some("printf 'a: foo1\n'".to_string()));
        let res = resolver_with(source);
        // mapall output wins over the map upcall for known groups
        assert_eq!(res.group_nodes("a", None).unwrap(), vec!["foo1"]);
        // 'b' not in mapall output: falls back to the map upcall
        assert_eq!(res.group_nodes("b", None).unwrap(), vec!["foo2"]);
        // 'b' is now cached via map: no second map upcall
        assert_eq!(res.group_nodes("b", None).unwrap(), vec!["foo2"]);
        let calls = std::fs::read_to_string(count.path()).unwrap();
        assert_eq!(calls.lines().count(), 1);
    }

    #[test]
    fn test_mapall_overrides_list() {
        // mapall output takes precedence over the list upcall
        let source =
            UpcallGroupSource::new(None, None, Some("echo explicit_only".to_string()), None)
                .with_mapall_upcall(Some("printf 'a: n1\n'".to_string()))
                .with_cache_time(0.2);
        let res = resolver_with(source);
        assert_eq!(res.grouplist(None).unwrap(), vec!["a"]);
        std::thread::sleep(std::time::Duration::from_millis(250));
        // precedence holds across a cache refresh cycle
        assert_eq!(res.grouplist(None).unwrap(), vec!["a"]);
    }

    #[test]
    fn test_mapall_expiry() {
        // mapall is re-run when cache_time expires
        let data = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(data.path(), "a: v1\nold: x1\n").unwrap();
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some(format!("cat {}", data.path().display())))
            .with_cache_time(0.2);
        let res = resolver_with(source);
        assert_eq!(res.group_nodes("a", None).unwrap(), vec!["v1"]);
        assert_eq!(res.group_nodes("old", None).unwrap(), vec!["x1"]);
        // change underlying mapall output, sleep past expiry
        std::fs::write(data.path(), "a: v2\nb: v3\n").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(250));
        assert_eq!(res.group_nodes("a", None).unwrap(), vec!["v2"]);
        assert_eq!(res.group_nodes("b", None).unwrap(), vec!["v3"]);
        // map cache was swapped, not merged: removed group is gone
        assert!(res.group_nodes("old", None).unwrap().is_empty());
        // list cache is also refreshed from mapall output
        let mut groups = res.grouplist(None).unwrap();
        groups.sort();
        assert_eq!(groups, vec!["a", "b"]);
    }

    #[test]
    fn test_mapall_clear_cache() {
        // clear_cache() triggers a new mapall call
        let data = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(data.path(), "a: v1\n").unwrap();
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some(format!("cat {}", data.path().display())));
        assert_eq!(source.resolve_map("a").unwrap(), "v1");
        std::fs::write(data.path(), "a: v2\n").unwrap();
        // still served from cache
        assert_eq!(source.resolve_map("a").unwrap(), "v1");
        source.clear_cache();
        assert_eq!(source.resolve_map("a").unwrap(), "v2");
    }

    #[test]
    fn test_mapall_cache_time_zero() {
        // mapall-only source with cache_time set to 0 (no caching)
        let data = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(data.path(), "a: n1\n").unwrap();
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some(format!("cat {}", data.path().display())))
            .with_cache_time(0.0);
        assert_eq!(source.resolve_list().unwrap(), vec!["a"]);
        assert_eq!(source.resolve_map("a").unwrap(), "n1");
        // no caching: updated mapall output is seen right away
        std::fs::write(data.path(), "a: n2\nb: n3\n").unwrap();
        assert_eq!(source.resolve_map("a").unwrap(), "n2");
        assert_eq!(source.resolve_list().unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn test_mapall_empty_group() {
        // mapall line with no nodes resolves to empty
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some("printf 'empty:\nfilled: n1\n'".to_string()));
        let res = resolver_with(source);
        assert!(res.group_nodes("empty", None).unwrap().is_empty());
        assert_eq!(res.group_nodes("filled", None).unwrap(), vec!["n1"]);
    }

    #[test]
    fn test_mapall_empty_output() {
        // mapall upcall returning no groups at all
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some("printf ''".to_string()));
        let res = resolver_with(source);
        assert!(res.grouplist(None).unwrap().is_empty());
        assert!(res.group_nodes("nope", None).unwrap().is_empty());
    }

    #[test]
    fn test_mapall_blank_lines_ignored() {
        // mapall tolerates blank lines and surrounding whitespace
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some("printf '\n  a: n1 \n\n  b: n2\n\n'".to_string()));
        let res = resolver_with(source);
        assert_eq!(res.group_nodes("a", None).unwrap(), vec!["n1"]);
        assert_eq!(res.group_nodes("b", None).unwrap(), vec!["n2"]);
    }

    #[test]
    fn test_mapall_parse_error() {
        // mapall line without ':' raises GroupSourceQueryFailed
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some("printf 'a: n1\nbroken_no_colon\n'".to_string()));
        let res = resolver_with(source);
        assert!(res.group_nodes("a", None).is_err());
    }

    #[test]
    fn test_mapall_empty_group_name() {
        // mapall line with empty group name raises GroupSourceQueryFailed
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some("printf ': n1\n'".to_string()));
        let res = resolver_with(source);
        assert!(res.group_nodes("a", None).is_err());
    }

    #[test]
    fn test_mapall_whitespace_group_name() {
        // mapall line with multi-word group name raises GroupSourceQueryFailed
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some("printf 'bad group: n1\n'".to_string()));
        let res = resolver_with(source);
        assert!(res.group_nodes("a", None).is_err());
    }

    #[test]
    fn test_mapall_duplicate_groups_union() {
        // duplicate mapall group lines are unioned
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some("printf 'a: n1\nb: n3\na: n2\na:\n'".to_string()));
        let res = resolver_with(source);
        let nodes = res.group_nodes("a", None).unwrap();
        assert_eq!(nodes, vec!["n1", "n2"]);
        // the unioned result folds like a nodeset
        assert_eq!(crate::node_set::fold(&nodes.join(",")).unwrap(), "n[1-2]");
        assert_eq!(res.group_nodes("b", None).unwrap(), vec!["n3"]);
        let mut groups = res.grouplist(None).unwrap();
        groups.sort();
        assert_eq!(groups, vec!["a", "b"]);
    }

    #[test]
    fn test_mapall_all_nodes_single_call() {
        // @*-style expansion (list + map per group) served by one mapall call
        let count = tempfile::NamedTempFile::new().unwrap();
        let cmd = format!(
            "printf 'a: n[1-2]\nb: n[3-4]\n' && echo called >> {}",
            count.path().display()
        );
        let source = UpcallGroupSource::new(None, None, None, None).with_mapall_upcall(Some(cmd));
        let res = resolver_with(source);
        let mut all = Vec::new();
        for group in res.grouplist(None).unwrap() {
            all.extend(res.group_nodes(&group, None).unwrap());
        }
        assert_eq!(all, vec!["n[1-2]", "n[3-4]"]);
        let calls = std::fs::read_to_string(count.path()).unwrap();
        assert_eq!(calls.lines().count(), 1);
    }

    #[test]
    fn test_mapall_parse_error_retry() {
        // mapall output is not cached on parse error
        let data = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(data.path(), "broken_no_colon\n").unwrap();
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some(format!("cat {}", data.path().display())));
        let res = resolver_with(source);
        assert!(res.group_nodes("a", None).is_err());
        // fixed output must be picked up by the next call, not a cached error
        std::fs::write(data.path(), "a: n1\n").unwrap();
        assert_eq!(res.group_nodes("a", None).unwrap(), vec!["n1"]);
    }

    #[test]
    fn test_mapall_all_still_required() {
        // 'all' is not derived from mapall
        let source = UpcallGroupSource::new(None, None, None, None)
            .with_mapall_upcall(Some("printf 'a: n1\nb: n2\n'".to_string()));
        assert!(source.resolve_all().is_err());
    }

    #[test]
    fn test_config_mapall_only() {
        // config with mapall upcall and no map
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            "[Main]\ndefault: bulk\n\n[bulk]\nmapall: printf 'g1: n[1-3]\\ng2: n[4-5]\\n'\n",
        )
        .unwrap();
        let mut config = GroupResolverConfig::new(vec![file.path().to_path_buf()], HashSet::new());
        let resolver = config.resolver().unwrap();
        assert_eq!(resolver.group_nodes("g1", None).unwrap(), vec!["n[1-3]"]);
        assert_eq!(resolver.group_nodes("g2", None).unwrap(), vec!["n[4-5]"]);
        let mut groups = resolver.grouplist(None).unwrap();
        groups.sort();
        assert_eq!(groups, vec!["g1", "g2"]);
    }

    #[test]
    fn test_config_mapall_query_failed() {
        // config with failing mapall upcall (not cached, retried)
        let data = tempfile::NamedTempFile::new().unwrap(); // stays empty for now
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            format!(
                "[Main]\ndefault: bulk\n\n[bulk]\nmapall: test -s {0} && cat {0}\n",
                data.path().display()
            ),
        )
        .unwrap();
        let mut config = GroupResolverConfig::new(vec![file.path().to_path_buf()], HashSet::new());
        let resolver = config.resolver().unwrap();
        // empty data file: mapall upcall exits non-zero
        assert!(resolver.grouplist(None).is_err());
        // upcall failure is not cached: fix the data file and retry
        std::fs::write(data.path(), "a: n1\n").unwrap();
        assert_eq!(resolver.grouplist(None).unwrap(), vec!["a"]);
        assert_eq!(resolver.group_nodes("a", None).unwrap(), vec!["n1"]);
    }

    #[test]
    fn test_config_map_or_mapall_required() {
        // config without map or mapall raises a config error
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            "[Main]\ndefault: nope\n\n[nope]\nlist: echo foo\n",
        )
        .unwrap();
        let mut config = GroupResolverConfig::new(vec![file.path().to_path_buf()], HashSet::new());
        // surfaces at (lazy) resolver initialization, like Python's
        // GroupResolverConfigError at first use
        let err = config.resolver().err().unwrap();
        assert!(format!("{:?}", err).contains("map' or 'mapall'"));
    }
}
