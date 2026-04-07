//! Inter-node messaging protocol.
//!
//! Rust implementation of `ClusterShell.Communication`.

/// A message channel between gateway nodes.
pub struct Channel;

/// Configuration message sent during tree setup.
pub struct ConfigurationMessage;

/// Control message for worker lifecycle.
pub struct ControlMessage;

/// Standard output message payload.
pub struct StdOutMessage;

/// Standard error message payload.
pub struct StdErrMessage;

/// Return code message.
pub struct RetcodeMessage;

/// Timeout message.
pub struct TimeoutMessage;
