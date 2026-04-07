//! Group resolution backends.
//!
//! Rust implementation of `ClusterShell.NodeUtils`.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum GroupError {
    #[error("group source error: {0}")]
    SourceError(String),

    #[error("group resolver error: {0}")]
    ResolverError(String),

    #[error("group resolver config error: {0}")]
    ConfigError(String),
}

pub type Result<T> = std::result::Result<T, GroupError>;

/// A source of node group definitions.
pub trait GroupSource: Send + Sync {
    /// Resolve a group name to a node pattern string.
    fn resolve_map(&self, group: &str) -> Result<String>;

    /// List all available group names.
    fn list_groups(&self) -> Result<Vec<String>>;
}

/// Resolves `@group` references to node sets.
pub struct GroupResolver {
    sources: Vec<Box<dyn GroupSource>>,
}

impl GroupResolver {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    pub fn add_source(&mut self, source: Box<dyn GroupSource>) {
        self.sources.push(source);
    }
}

impl Default for GroupResolver {
    fn default() -> Self {
        Self::new()
    }
}
