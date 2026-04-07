//! Event handler traits.
//!
//! Rust implementation of `ClusterShell.Event`.

/// Trait for handling worker lifecycle events.
///
/// Implement any subset of these methods to react to events
/// from workers and tasks.
pub trait EventHandler: Send {
    fn ev_start(&mut self, _worker: &dyn std::any::Any) {}
    fn ev_read(&mut self, _worker: &dyn std::any::Any, _node: &str, _sname: &str, _msg: &str) {}
    fn ev_error(&mut self, _worker: &dyn std::any::Any) {}
    fn ev_written(&mut self, _worker: &dyn std::any::Any, _node: &str, _sname: &str, _size: usize) {
    }
    fn ev_hup(&mut self, _worker: &dyn std::any::Any, _node: &str, _rc: i32) {}
    fn ev_close(&mut self, _worker: &dyn std::any::Any, _timedout: bool) {}
    fn ev_msg(&mut self, _port: u16, _msg: &str) {}
    fn ev_timer(&mut self, _timer: &dyn std::any::Any) {}
    fn ev_pickup(&mut self, _worker: &dyn std::any::Any, _node: &str) {}
}
