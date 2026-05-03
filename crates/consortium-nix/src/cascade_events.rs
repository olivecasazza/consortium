//! Cascade event protocol — fine-grained events emitted by the
//! cascade coordinator as the run progresses.
//!
//! Replaces the per-round [`crate::cascade::TraceSink`] batch model
//! with a streaming event-stream model. Each event carries enough
//! data to (a) be the unit of fine-grained tracing/debugging and
//! (b) compose into higher-level views (per-round snapshots,
//! cascade-tree builds, JSONL streams, nom-compatible bridges) at
//! consumer time, not coordinator time.
//!
//! ## Why an event protocol
//!
//! - **Tracing/debugging**: each edge has its own start/complete event
//!   with a timestamp. Failure debugging gets per-edge timing instead
//!   of aggregated round duration.
//! - **Reductions are consumer-side**: parent backtraces, subtree
//!   aggregates, error-tree shape — all derivable from events plus
//!   the cascade state. We don't pre-compute one canonical view.
//! - **Live rendering**: a renderer can repaint on each event without
//!   polling.
//! - **Multi-consumer**: the same event stream feeds the tree
//!   renderer, JSONL writer, prometheus exporter, future nom-compat
//!   bridge — all without coordinator changes.
//! - **Replay**: persist as JSONL, replay later to reproduce any view.
//!
//! ## Wire format
//!
//! Every variant derives `serde::{Serialize, Deserialize}` so the
//! same event types serialize to JSON, YAML, TOML, or (future)
//! protobuf via prost without coordinator changes. The on-disk
//! format for streamed traces is JSONL — one event per line.

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::cascade::{CascadeError, NodeId};

// ============================================================================
// CascadeEvent
// ============================================================================

/// One event in the cascade lifecycle. The `kind` discriminator names
/// the event; per-variant fields carry the event's payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CascadeEvent {
    /// Cascade run begins. Emitted once before any plan / edge events.
    Started {
        n_nodes: u32,
        seeded: Vec<NodeId>,
        strategy: String,
        #[serde(with = "system_time_serde")]
        at: SystemTime,
    },

    /// Strategy returned a plan for `round`. Emitted once per round
    /// before any [`EdgeStarted`](CascadeEvent::EdgeStarted) events.
    PlanComputed { round: u32, assignments: Vec<Edge> },

    /// An edge started. Emitted once per (src, tgt) at round start.
    EdgeStarted {
        round: u32,
        src: NodeId,
        tgt: NodeId,
        #[serde(with = "system_time_serde")]
        at: SystemTime,
    },

    /// An edge completed successfully. Mutually exclusive with
    /// [`EdgeFailed`](CascadeEvent::EdgeFailed) for the same edge.
    EdgeCompleted {
        round: u32,
        src: NodeId,
        tgt: NodeId,
        #[serde(with = "duration_serde")]
        duration: Duration,
    },

    /// An edge failed. Carries the same `CascadeError` shape the round
    /// coordinator will eventually fold into the cascade error tree.
    EdgeFailed {
        round: u32,
        src: NodeId,
        tgt: NodeId,
        #[serde(
            serialize_with = "serialize_error",
            deserialize_with = "deserialize_error"
        )]
        error: CascadeError,
    },

    /// A round finished — all edges in `PlanComputed` have a matching
    /// `EdgeCompleted` or `EdgeFailed`. Carries the round's wall-time
    /// (max edge duration) and the post-round `has_closure` set.
    RoundCompleted {
        round: u32,
        #[serde(with = "duration_serde")]
        duration: Duration,
        has_closure: Vec<NodeId>,
    },

    /// Cascade run ends. Emitted once. `failed` is the count of nodes
    /// that did NOT converge (synthetic root error nodes are excluded).
    Finished {
        converged: usize,
        failed: usize,
        rounds: u32,
    },
}

/// Compact `(src, tgt)` pair for plans + assignments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub src: NodeId,
    pub tgt: NodeId,
}

impl From<(NodeId, NodeId)> for Edge {
    fn from((src, tgt): (NodeId, NodeId)) -> Self {
        Self { src, tgt }
    }
}

// ============================================================================
// EventSink trait
// ============================================================================

/// Receives [`CascadeEvent`]s as the cascade runs. Implementations are
/// called from inside the coordinator loop so they should be cheap;
/// heavy work belongs in downstream consumers (renderers, persistence).
///
/// Default impls live in `consortium-cli::event_render`:
/// - `JsonlWriter` — streams events to a file as JSONL
/// - `EventCollector` — accumulates into a Vec for batch processing
/// - `SnapshotAccumulator` — folds events into per-round snapshots
///   for back-compat with the old `TraceSink` model
pub trait EventSink: Send + Sync {
    fn emit(&self, event: &CascadeEvent);
}

/// No-op sink. Used as the production default — emit calls compile
/// down to no-ops.
pub struct NullSink;

impl EventSink for NullSink {
    fn emit(&self, _event: &CascadeEvent) {}
}

/// Forward events to multiple sinks. Useful when you want both a
/// JSONL persisted trace AND a live tree renderer at the same time.
pub struct MultiSink<'a> {
    pub sinks: Vec<&'a dyn EventSink>,
}

impl<'a> EventSink for MultiSink<'a> {
    fn emit(&self, event: &CascadeEvent) {
        for s in &self.sinks {
            s.emit(event);
        }
    }
}

// ============================================================================
// Serde helpers — CascadeError doesn't derive Serialize, so we
// flatten it to its Display string. This is one-way (events captured
// for tracing/replay; we don't need to round-trip the typed error
// for a consumer — they can match on prefix or just display it).
// ============================================================================

mod system_time_serde {
    use super::*;
    use serde::{Deserializer, Serializer};
    use std::time::UNIX_EPOCH;

    pub fn serialize<S: Serializer>(t: &SystemTime, s: S) -> Result<S::Ok, S::Error> {
        let nanos = t
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        s.serialize_u64(nanos)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<SystemTime, D::Error> {
        use serde::Deserialize;
        let nanos = u64::deserialize(d)?;
        Ok(UNIX_EPOCH + Duration::from_nanos(nanos))
    }
}

mod duration_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_nanos() as u64)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        use serde::Deserialize;
        let nanos = u64::deserialize(d)?;
        Ok(Duration::from_nanos(nanos))
    }
}

fn serialize_error<S: serde::Serializer>(err: &CascadeError, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&err.to_string())
}

fn deserialize_error<'de, D: serde::Deserializer<'de>>(d: D) -> Result<CascadeError, D::Error> {
    use serde::Deserialize;
    let msg = String::deserialize(d)?;
    // We don't reconstruct the typed error from the message — consumers
    // that need the typed error should run the cascade live. For replay
    // purposes we wrap it in a Copy variant carrying the original
    // message. NodeId(u32::MAX) marks "deserialized — original variant
    // unrecoverable from string."
    Ok(CascadeError::Copy {
        node: NodeId(u32::MAX),
        stderr: msg,
    })
}

// ============================================================================
// NodeId serde — NodeId is `pub struct NodeId(pub u32)`. We need
// Serialize + Deserialize on it; it lives in cascade.rs which we
// don't modify. Implement here as remote derive.
// ============================================================================

impl Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u32(self.0)
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let n = u32::deserialize(d)?;
        Ok(NodeId(n))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cascade_event_round_trips_through_json() {
        let ev = CascadeEvent::Started {
            n_nodes: 16,
            seeded: vec![NodeId(0), NodeId(1)],
            strategy: "log2-fanout".into(),
            at: SystemTime::UNIX_EPOCH + Duration::from_secs(1700000000),
        };
        let json = serde_json::to_string(&ev).unwrap();
        let back: CascadeEvent = serde_json::from_str(&json).unwrap();
        match back {
            CascadeEvent::Started {
                n_nodes,
                seeded,
                strategy,
                ..
            } => {
                assert_eq!(n_nodes, 16);
                assert_eq!(seeded, vec![NodeId(0), NodeId(1)]);
                assert_eq!(strategy, "log2-fanout");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn edge_events_round_trip() {
        let started = CascadeEvent::EdgeStarted {
            round: 3,
            src: NodeId(0),
            tgt: NodeId(7),
            at: SystemTime::UNIX_EPOCH,
        };
        let completed = CascadeEvent::EdgeCompleted {
            round: 3,
            src: NodeId(0),
            tgt: NodeId(7),
            duration: Duration::from_millis(120),
        };
        let s1 = serde_json::to_string(&started).unwrap();
        let s2 = serde_json::to_string(&completed).unwrap();
        assert!(s1.contains("\"kind\":\"edge_started\""));
        assert!(s2.contains("\"kind\":\"edge_completed\""));
        let _: CascadeEvent = serde_json::from_str(&s1).unwrap();
        let _: CascadeEvent = serde_json::from_str(&s2).unwrap();
    }

    #[test]
    fn edge_failed_event_carries_error_string() {
        let err = CascadeError::Copy {
            node: NodeId(5),
            stderr: "rsync exited 23".into(),
        };
        let ev = CascadeEvent::EdgeFailed {
            round: 1,
            src: NodeId(2),
            tgt: NodeId(5),
            error: err,
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("rsync exited 23"));
        let back: CascadeEvent = serde_json::from_str(&json).unwrap();
        match back {
            CascadeEvent::EdgeFailed {
                error: CascadeError::Copy { stderr, .. },
                ..
            } => {
                assert!(stderr.contains("rsync exited 23"));
            }
            _ => panic!("wrong variant on round-trip"),
        }
    }

    #[test]
    fn null_sink_compiles_and_is_no_op() {
        let s = NullSink;
        s.emit(&CascadeEvent::Finished {
            converged: 16,
            failed: 0,
            rounds: 4,
        });
    }

    #[test]
    fn multi_sink_forwards_to_all() {
        use std::sync::Mutex;
        struct Counter(Mutex<usize>);
        impl EventSink for Counter {
            fn emit(&self, _: &CascadeEvent) {
                *self.0.lock().unwrap() += 1;
            }
        }
        let a = Counter(Mutex::new(0));
        let b = Counter(Mutex::new(0));
        let multi = MultiSink {
            sinks: vec![&a, &b],
        };
        multi.emit(&CascadeEvent::Finished {
            converged: 1,
            failed: 0,
            rounds: 1,
        });
        assert_eq!(*a.0.lock().unwrap(), 1);
        assert_eq!(*b.0.lock().unwrap(), 1);
    }
}
