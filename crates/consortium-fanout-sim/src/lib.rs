//! # consortium-fanout-sim
//!
//! Deterministic in-process simulator for the [cascade
//! primitive](consortium_nix::cascade). Computes per-edge durations
//! from `closure_size / bandwidth + latency`, applies failure
//! schedules, and returns results to the cascade coordinator.
//!
//! No real networking, no async runtime, no SSH. Each `Scenario` is
//! reproducible from a `u64` seed — same seed produces identical
//! `CascadeResult` including round count and error tree shape.
//!
//! ## Layout
//!
//! - [`executor::DeterministicExecutor`] — implements
//!   [`RoundExecutor`](consortium_nix::cascade::RoundExecutor) with
//!   bandwidth/latency-driven timing and an injectable failure schedule.
//! - [`fixtures`] — generators for seed sets, bandwidth distributions,
//!   and failure schedules. Each takes `&mut StdRng` for reproducibility.
//! - [`invariants`] — reusable assertion helpers shared by the fuzz
//!   harness and the curated corpus tests (seed-aware kill semantics,
//!   convergence / round / determinism checks).
//! - [`scenario::Scenario`] — high-level wrapper that builds a
//!   complete cascade run (nodes + seeded set + network + executor +
//!   strategy) from a seed and a few descriptors.
//! - [`simexec::SimExecutor`] — network/failure-aware
//!   [`consortium_integration::exec::Executor`] implementation: the
//!   bridge that runs *integration pipelines* (deploy, submit_job, …)
//!   under simulated network + failure conditions. Includes the "Writing
//!   a sim test for an integration" guide.
//!
//! ## Quick start
//!
//! ```
//! use consortium_fanout_sim::scenario::{Scenario, ScenarioConfig};
//! use consortium_fanout_sim::fixtures::BandwidthDistribution;
//! use consortium_nix::cascade::Log2FanOut;
//!
//! let cfg = ScenarioConfig {
//!     seed: 0xc0ffee,
//!     n_nodes: 64,
//!     seed_fraction: 0.0,
//!     closure_bytes: 100 * 1024 * 1024,
//!     bandwidth: BandwidthDistribution::Uniform(100 * 1024 * 1024),
//!     uplinks: None,
//!     failures: Default::default(),
//!     max_rounds: 32,
//! };
//! let result = Scenario::new(cfg).run(&Log2FanOut);
//! assert!(result.is_success());
//! assert_eq!(result.converged.len(), 64);
//! ```

pub mod executor;
pub mod fixtures;
pub mod invariants;
pub mod scenario;
pub mod simexec;

pub use executor::DeterministicExecutor;
pub use fixtures::{BandwidthDistribution, FailureSchedule, SeedDistribution, UplinkDistribution};
pub use scenario::{Scenario, ScenarioConfig};
pub use simexec::{
    assert_deterministic_equivalence, canonical_log, logs_equivalent, per_edge_outcomes,
    SimCommandKind, SimEvent, SimExecutor, SimExecutorBuilder, SimOutcome,
};
