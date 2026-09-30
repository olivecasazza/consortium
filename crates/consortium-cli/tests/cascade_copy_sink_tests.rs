//! cascade-copy event-sink selection tests.
//!
//! The benchmark harness reads a real run's cascade as JSONL and asserts the
//! payload was relayed peer-to-peer. That evidence only exists if cascade-copy
//! emits its event stream — and over SSH, stdout is never a TTY, so the
//! historical `live_eligible` check chose `NullSink` and the run emitted
//! nothing at all. The harness could then only record the CLI's exit status,
//! which a host push to every guest also produces.
//!
//! These tests pin the decision that fixed it: asking for jsonl must select the
//! streaming sink regardless of TTY, and the default formats must not.

use consortium_cli::event_render::{sink_for_format, SinkKind};

#[test]
fn jsonl_selects_the_streaming_sink_even_without_a_tty() {
    // live_eligible is false whenever stdout is piped, which is always true
    // over SSH. This is the case that made the harness blind.
    assert_eq!(sink_for_format("jsonl", false), Some(SinkKind::Jsonl));
}

#[test]
fn jsonl_selects_the_streaming_sink_on_a_tty_too() {
    assert_eq!(sink_for_format("jsonl", true), Some(SinkKind::Jsonl));
}

#[test]
fn the_default_tree_format_keeps_its_live_renderer_on_a_tty() {
    // Not a regression: the live tree is the default experience for a human.
    assert_eq!(sink_for_format("tree", true), Some(SinkKind::LiveTree));
}

#[test]
fn a_non_tty_run_emits_nothing_for_the_visual_formats() {
    // This is the pre-existing behaviour the harness worked around, kept
    // explicit so nobody reads it as an accident.
    assert_eq!(sink_for_format("tree", false), None);
    assert_eq!(sink_for_format("yaml", false), None);
}

#[test]
fn the_format_match_is_case_insensitive() {
    assert_eq!(sink_for_format("JSONL", false), Some(SinkKind::Jsonl));
}
