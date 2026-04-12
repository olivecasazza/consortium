//! Generic DAG task executor.
//!
//! Provides a parallel task execution engine with dependency resolution,
//! pluggable caching, dynamic worker pools, and configurable error handling.
//!
//! # DAG Construction
//!
//! Multiple interfaces are available for building a DAG:
//!
//! - [`DagBuilder`] — programmatic API with explicit tasks and dependencies
//! - [`StageBuilder`] — template pattern: "apply N stages to M resources"
//! - [`DagTask`] trait — implement for fully custom task behavior
//!
//! # Example: Stage Builder (deployment pattern)
//!
//! ```rust,no_run
//! use consortium::dag::*;
//!
//! let report = StageBuilder::new()
//!     .resources(vec!["host1".into(), "host2".into()])
//!     .stage("build", Some(4), |host| {
//!         Box::new(ShellTask {
//!             command: format!("echo building {}", host),
//!             description: format!("build {}", host),
//!             resource: None,
//!         })
//!     })
//!     .stage("deploy", Some(2), |host| {
//!         Box::new(ShellTask {
//!             command: format!("echo deploying {}", host),
//!             description: format!("deploy {}", host),
//!             resource: None,
//!         })
//!     })
//!     .error_policy(ErrorPolicy::ContinueIndependent)
//!     .build()
//!     .unwrap()
//!     .run()
//!     .unwrap();
//!
//! assert!(report.is_success());
//! ```

pub mod builder;
pub mod cache;
pub mod context;
pub mod error;
pub mod executor;
pub mod graph;
pub mod pool;
pub mod types;

// Re-export key types at module level
pub use builder::{DagBuilder, StageBuilder};
pub use cache::{CacheStrategy, NoCache};
pub use context::DagContext;
pub use error::{DagError, Result};
pub use executor::{DagEvent, DagExecutor, DagMonitor, DagReport};
pub use graph::DagGraph;
pub use pool::{DynamicPool, FixedPool, UnlimitedPool, WorkerPool};
pub use types::{ConcurrencyLimit, DagTask, ErrorPolicy, FnTask, ShellTask, TaskId, TaskOutcome};
