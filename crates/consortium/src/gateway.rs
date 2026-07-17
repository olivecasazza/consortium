//! Gateway node logic.
//!
//! Rust implementation of `ClusterShell.Gateway`.
//!
//! A gateway is a tree node that relays commands between the admin node and
//! the subtree below it. This module contains the target (non-initiator)
//! side of a tree channel:
//!
//! - [`GatewayChannel`]: high-level logic for gateways — accepts the channel
//!   greeting, receives the topology configuration, ACKs it, and converts
//!   processing failures into Error replies followed by channel close
//!   (mirrors `Gateway.GatewayChannel`).
//! - [`TreeWorkerResponder`]: builds routing notifications for the gateway
//!   channel with the corrected `RoutingMessage` construction of upstream
//!   commit 490323d.

use crate::communication::{Channel, Message, MessageProcessingError};

/// Routing event data handed to a [`TreeWorkerResponder`].
///
/// Mirrors the `arg` dict of upstream `TreeWorkerResponder._ev_routing`
/// (`{"event": ..., "gateway": ..., "targets": ...}`). There is deliberately
/// NO `srcid` field: the responder always stamps its own source worker id.
/// Upstream commit 490323d fixed `RoutingMessage(**arg, srcid=self.srcwkr)`
/// (keyword-after-**kwargs is a syntax error before Python 3.5) into
/// `RoutingMessage(srcid=self.srcwkr, **arg)`; modeling the event without a
/// srcid makes that whole bug class unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingEvent {
    /// Routing event name (eg. "connect", "reroute").
    pub event: String,
    /// Gateway the event relates to.
    pub gateway: String,
    /// Target nodes of the event.
    pub targets: String,
}

/// Gateway-side responder for a tree worker.
///
/// Mirrors `Gateway.TreeWorkerResponder`: it relays routing events from the
/// worker running on the gateway to the admin node through the gateway
/// channel.
#[derive(Debug)]
pub struct TreeWorkerResponder {
    /// Source worker id stamped on routing messages (upstream `srcwkr`).
    srcwkr: u64,
}

impl TreeWorkerResponder {
    /// Create a responder for the given source worker id.
    pub fn new(srcwkr: u64) -> Self {
        Self { srcwkr }
    }

    /// Get the source worker id.
    pub fn srcwkr(&self) -> u64 {
        self.srcwkr
    }

    /// Build the routing message to send on the gateway channel.
    ///
    /// Corrected construction (upstream 490323d): `srcid` is always the
    /// responder's own worker id; the event only carries
    /// event/gateway/targets.
    pub fn routing_message(&self, arg: &RoutingEvent) -> Message {
        Message::routing(&arg.event, &arg.gateway, &arg.targets, self.srcwkr)
    }

    /// Relay a routing event onto the gateway channel (mirrors `_ev_routing`
    /// sending the RoutingMessage on `gwchan`).
    pub fn ev_routing(&self, chan: &mut Channel, arg: &RoutingEvent) {
        chan.send(&self.routing_message(arg));
    }
}

/// Short system hostname, used as fallback gateway node name (mirrors
/// upstream `_getshorthostname()`).
fn short_hostname() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_default()
        .split('.')
        .next()
        .unwrap_or_default()
        .to_string()
}

/// High level logic for gateways (mirrors `Gateway.GatewayChannel`).
///
/// This is the TARGET side of a tree channel: it receives the configuration
/// from the admin node, ACKs it, and then accepts control messages. Any
/// processing failure — including undecodable payloads such as a gateway
/// message stamped with an unsupported pickle protocol — is turned into an
/// Error reply sent back to the initiator, after which the channel is
/// closed (fatal channel error).
#[derive(Debug)]
pub struct GatewayChannel {
    /// The underlying communication channel (target end: `initiator=false`).
    pub channel: Channel,
    /// This gateway's node name (from CFG or system hostname fallback).
    pub nodename: Option<String>,
    /// Decoded topology payload received in CFG.
    pub topology: Option<Vec<u8>>,
}

impl Default for GatewayChannel {
    fn default() -> Self {
        Self::new()
    }
}

impl GatewayChannel {
    /// Create a new gateway channel ready to accept communication.
    pub fn new() -> Self {
        Self {
            channel: Channel::new(false), // target side
            nodename: None,
            topology: None,
        }
    }

    /// Close the gateway channel (mirrors `GatewayChannel.close`).
    pub fn close(&mut self) {
        self.channel.close();
    }

    /// Feed raw incoming data to the channel, mirroring the read side of
    /// upstream `Channel.ev_read` for a target: parse errors are reported
    /// back to the initiator as an Error message and close the channel.
    pub fn feed(&mut self, data: &str) {
        match self.channel.feed(data) {
            Ok(messages) => {
                for msg in messages {
                    self.recv(msg);
                }
            }
            Err(err) => {
                // target side: send ErrorMessage back + close (fatal)
                self.channel.handle_channel_error("", &err);
            }
        }
    }

    /// Handle one incoming message (mirrors `GatewayChannel.recv`).
    ///
    /// Dispatch errors are reported back to the initiator as an Error
    /// message, then the channel is closed — including configuration
    /// payload decode failures (90d3195's gateway side: the admin later
    /// reports this reason as gateway stderr).
    pub fn recv(&mut self, msg: Message) {
        if let Err(err) = self.dispatch(&msg) {
            self.channel.send(&Message::error(&err.to_string()));
            self.close();
        }
    }

    /// Message dispatch, mirroring upstream's recv() branching:
    /// END → close; setup → recv_ctl; opened → recv_cfg; CHA → open;
    /// anything else → "unexpected message: %s".
    fn dispatch(&mut self, msg: &Message) -> Result<(), MessageProcessingError> {
        if msg.ident() == "END" {
            self.close();
            return Ok(());
        }
        if self.channel.setup {
            return self.recv_ctl(msg);
        }
        if self.channel.opened {
            return self.recv_cfg(msg);
        }
        if msg.ident() == "CHA" {
            self.channel.opened = true;
            return Ok(());
        }
        Err(MessageProcessingError::UnexpectedMessage(msg.to_string()))
    }

    /// Receive cfg/topology configuration (mirrors `GatewayChannel.recv_cfg`).
    ///
    /// Only a CFG message is acceptable here. The payload is decoded (a
    /// failure — eg. an unsupported payload protocol version — propagates
    /// and becomes the Error reply), the channel is marked set up, and an
    /// ACK of the configuration message id is enqueued.
    fn recv_cfg(&mut self, msg: &Message) -> Result<(), MessageProcessingError> {
        if msg.ident() != "CFG" {
            return Err(MessageProcessingError::UnexpectedMessage(msg.to_string()));
        }

        // gateway node name, with system hostname fallback (upstream warns
        // and uses _getshorthostname() when not provided)
        let gateway = match msg {
            Message::Configuration { gateway, .. } => gateway.clone(),
            _ => String::new(),
        };
        self.nodename = Some(if gateway.is_empty() {
            short_hostname()
        } else {
            gateway
        });

        // topology payload — errors (invalid payload, unsupported protocol)
        // propagate so recv() reports them to the initiator
        self.topology = Some(msg.data_decode()?);

        self.channel.setup = true;
        // ACK the configuration message id
        self.channel.send(&Message::ack(msg.msgid()));
        Ok(())
    }

    /// Receive a control message (mirrors `GatewayChannel.recv_ctl`).
    ///
    /// The execution side of gateway control (spawning the responder
    /// worker) is not part of this port; payload-bearing control messages
    /// are validated so undecodable payloads are still reported back to the
    /// initiator like upstream.
    fn recv_ctl(&mut self, msg: &Message) -> Result<(), MessageProcessingError> {
        if msg.has_payload() && msg.data().is_some() {
            // validate the payload the same way upstream's data_decode does
            let _ = msg.data_decode()?;
        }
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::communication::{base64_encode, GW_PICKLE_PROTOCOL};

    // -- TreeWorkerResponder / RoutingMessage construction (490323d) --------

    /// Mirror of upstream 490323d ("Gateway: fix RoutingMessage construction
    /// error"): the responder's srcid is always stamped on the routing
    /// message; the event data cannot override or collide with it.
    #[test]
    fn test_responder_routing_message_stamps_srcid() {
        let responder = TreeWorkerResponder::new(42);
        let arg = RoutingEvent {
            event: "connect".into(),
            gateway: "gw2".into(),
            targets: "node[10-20]".into(),
        };
        let msg = responder.routing_message(&arg);
        assert_eq!(msg.ident(), "RTR");
        assert_eq!(msg.srcid(), Some(42));
        match msg {
            Message::Routing {
                event,
                gateway,
                targets,
                ..
            } => {
                assert_eq!(event, "connect");
                assert_eq!(gateway, "gw2");
                assert_eq!(targets, "node[10-20]");
            }
            other => panic!("expected Routing, got {:?}", other),
        }
    }

    /// The routing event struct carries no srcid at all (the upstream bug
    /// class — a duplicate keyword argument — is unrepresentable).
    #[test]
    fn test_routing_event_has_no_srcid_field() {
        let arg = RoutingEvent {
            event: "reroute".into(),
            gateway: "gw1".into(),
            targets: "node1".into(),
        };
        let responder = TreeWorkerResponder::new(7);
        let msg = responder.routing_message(&arg);
        // srcid comes from the responder only
        assert_eq!(msg.srcid(), Some(7));
    }

    /// ev_routing enqueues an XML routing message on the channel.
    #[test]
    fn test_responder_ev_routing_sends_on_channel() {
        let responder = TreeWorkerResponder::new(3);
        let mut chan = Channel::new(false);
        let arg = RoutingEvent {
            event: "connect".into(),
            gateway: "gw1".into(),
            targets: "node[1-5]".into(),
        };
        responder.ev_routing(&mut chan, &arg);
        let out = String::from_utf8(chan.take_outgoing().unwrap()).unwrap();
        assert!(out.contains("type=\"RTR\""));
        assert!(out.contains("srcid=\"3\""));
        assert!(out.contains("event=\"connect\""));
        assert!(out.contains("targets=\"node[1-5]\""));
    }

    // -- GatewayChannel state machine ----------------------------------------

    /// Basic channel open/close (mirrors TreeGatewayTest::test_basic_noop).
    #[test]
    fn test_gateway_channel_open_close() {
        let mut gw = GatewayChannel::new();
        assert!(!gw.channel.opened);
        assert!(!gw.channel.setup);

        gw.feed("<channel version=\"1.10.1\">");
        assert!(gw.channel.opened);
        assert!(!gw.channel.setup);

        gw.feed("</channel>");
        assert!(gw.channel.closed);
        assert!(!gw.channel.opened);
    }

    /// A valid CFG is decoded, setup completes, and the msgid is ACKed.
    #[test]
    fn test_gateway_channel_cfg_ack() {
        let mut gw = GatewayChannel::new();
        gw.feed("<channel version=\"1.10.1\">");

        let mut cfg = Message::configuration("n1");
        cfg.data_encode(b"topology bytes");
        let cfg_xml = String::from_utf8(cfg.xml()).unwrap();
        gw.feed(&cfg_xml);

        assert!(gw.channel.setup);
        assert_eq!(gw.nodename.as_deref(), Some("n1"));
        assert_eq!(gw.topology.as_deref(), Some(b"topology bytes".as_ref()));

        let out = String::from_utf8(gw.channel.take_outgoing().unwrap()).unwrap();
        assert!(out.contains("type=\"ACK\""));
        assert!(out.contains(&format!("ack=\"{}\"", cfg.msgid())));
    }

    /// Mirror of TreeGatewayTest::test_channel_err_pickle_proto_pl
    /// (gateway side): a CFG whose payload uses an unknown pickle protocol
    /// is rejected with an Error reply carrying upstream's exact reason,
    /// then the channel closes.
    #[test]
    fn test_gateway_channel_err_pickle_proto_payload() {
        let mut gw = GatewayChannel::new();
        gw.feed("<channel version=\"1.10.1\">");

        // same wire vector as upstream: base64(b'\x80\x07spam') -> protocol 7
        let payload = base64_encode(b"\x80\x07spam");
        gw.feed(&format!(
            "<message msgid=\"14\" type=\"CFG\" gateway=\"n1\">{}</message>",
            payload
        ));

        let out = String::from_utf8(gw.channel.take_outgoing().unwrap()).unwrap();
        assert!(out.contains("type=\"ERR\""));
        assert!(
            out.contains(
                "reason=\"Message CFG has an invalid payload (unsupported pickle protocol: 7)\""
            ),
            "unexpected reason in: {}",
            out
        );
        assert!(gw.channel.closed);
        assert!(!gw.channel.setup);
        // protocol 7 must exceed the pinned gateway protocol
        assert!(7 > GW_PICKLE_PROTOCOL);
    }

    /// Mirror of TreeGatewayTest::test_channel_err_no_type_msg (gateway
    /// side): a message without a type gets "Unknown message with no type"
    /// back and closes the channel.
    #[test]
    fn test_gateway_channel_err_no_type_msg() {
        let mut gw = GatewayChannel::new();
        gw.feed("<channel version=\"1.10.1\">");
        gw.feed("<message msgid=\"24\"></message>");

        let out = String::from_utf8(gw.channel.take_outgoing().unwrap()).unwrap();
        assert!(out.contains("type=\"ERR\""));
        assert!(out.contains("reason=\"Unknown message with no type\""));
        assert!(gw.channel.closed);
    }

    /// Mirror of TreeGatewayTest::test_channel_err_empty_type_msg.
    #[test]
    fn test_gateway_channel_err_empty_type_msg() {
        let mut gw = GatewayChannel::new();
        gw.feed("<channel version=\"1.10.1\">");
        gw.feed("<message msgid=\"24\" type=\"\"></message>");

        let out = String::from_utf8(gw.channel.take_outgoing().unwrap()).unwrap();
        assert!(out.contains("reason=\"Unknown message with no type\""));
        assert!(gw.channel.closed);
    }

    /// Mirror of updated TreeGatewayTest::test_err_unknown_msg: the unknown
    /// type is named in the Error reply.
    #[test]
    fn test_gateway_channel_err_unknown_msg() {
        let mut gw = GatewayChannel::new();
        gw.feed("<channel version=\"1.10.1\">");
        gw.feed("<message msgid=\"24\" type=\"ABC\"></message>");

        let out = String::from_utf8(gw.channel.take_outgoing().unwrap()).unwrap();
        assert!(out.contains("reason=\"Unknown message type ABC\""));
        assert!(gw.channel.closed);
    }

    /// Mirror of TreeGatewayTest::test_channel_err_unknown_tag: an invalid
    /// starting tag is reported back (target side gets the error text).
    #[test]
    fn test_gateway_channel_err_unknown_tag() {
        let mut gw = GatewayChannel::new();
        gw.feed("<channel version=\"1.10.1\">");
        gw.feed("<foo></foo>");

        let out = String::from_utf8(gw.channel.take_outgoing().unwrap()).unwrap();
        assert!(out.contains("type=\"ERR\""));
        assert!(out.contains("reason=\"Invalid starting tag foo\""));
        assert!(gw.channel.closed);
    }

    /// Mirror of TreeGatewayTest::test_err_xml_malformed-style input:
    /// malformed XML yields "Parse error: ..." and closes the channel.
    #[test]
    fn test_gateway_channel_err_malformed_xml() {
        let mut gw = GatewayChannel::new();
        gw.feed("<channel version=\"1.10.1\">");
        gw.feed("<message type=\"ABC\"</message>");

        let out = String::from_utf8(gw.channel.take_outgoing().unwrap()).unwrap();
        assert!(out.contains("type=\"ERR\""));
        assert!(out.contains("reason=\"Parse error: not well-formed (invalid token)\""));
        assert!(gw.channel.closed);
    }

    /// Mirror of TreeGatewayTest::test_channel_err_dup: a second channel
    /// tag while opened is an "unexpected message" error.
    #[test]
    fn test_gateway_channel_err_dup_start() {
        let mut gw = GatewayChannel::new();
        gw.feed("<channel version=\"1.10.1\">");
        assert!(gw.channel.opened);

        // second channel tag: the reader emits another CHA, which dispatch
        // routes to recv_cfg -> unexpected message
        gw.feed("<channel version=\"1.10.1\">");
        let out = String::from_utf8(gw.channel.take_outgoing().unwrap()).unwrap();
        assert!(out.contains("type=\"ERR\""));
        assert!(out.contains("unexpected message: Message CHA"));
        assert!(gw.channel.closed);
    }

    /// A CFG with a missing payload fails decode and is reported
    /// (mirrors TreeGatewayTest::test_channel_err_missing_pl).
    #[test]
    fn test_gateway_channel_err_missing_payload() {
        let mut gw = GatewayChannel::new();
        gw.feed("<channel version=\"1.10.1\">");
        gw.feed("<message msgid=\"14\" type=\"CFG\" gateway=\"n1\"></message>");

        let out = String::from_utf8(gw.channel.take_outgoing().unwrap()).unwrap();
        assert!(out.contains("type=\"ERR\""));
        assert!(out.contains("reason=\"Message CFG has an invalid payload\""));
        assert!(gw.channel.closed);
    }

    /// Empty gateway attribute falls back to the system hostname (mirrors
    /// upstream recv_cfg behavior).
    #[test]
    fn test_gateway_channel_cfg_hostname_fallback() {
        let mut gw = GatewayChannel::new();
        gw.feed("<channel version=\"1.10.1\">");
        let mut cfg = Message::configuration("");
        cfg.data_encode(b"topo");
        gw.feed(&String::from_utf8_lossy(&cfg.xml()));

        assert!(gw.channel.setup);
        // nodename must be exactly the short-hostname fallback
        assert_eq!(gw.nodename.as_deref(), Some(short_hostname().as_str()));
    }
}
