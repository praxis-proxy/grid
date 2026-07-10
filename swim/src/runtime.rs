//! Foca [`Runtime`] implementation for the AI Grid.
//!
//! Bridges foca's callback-based API with Tokio channels for
//! async network I/O and membership event delivery.
//!
//! [`Runtime`]: foca::Runtime

use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::{event::MemberEvent, identity::NodeId};

// ---------------------------------------------------------------------------
// Timer Event
// ---------------------------------------------------------------------------

/// Events that foca schedules for deferred delivery.
#[derive(Clone, Debug)]
pub enum TimerEvent {
    /// A probe should be sent after a delay.
    SendProbe(NodeId),

    /// Probe timeout expired.
    ProbeTimeout(NodeId),

    /// A protocol-level periodic event.
    PeriodicAnnounce,

    /// A protocol-level periodic gossip.
    PeriodicGossip,

    /// A generic foca timer token.
    Token(foca::Timer<NodeId>),
}

// ---------------------------------------------------------------------------
// Accumulated Output
// ---------------------------------------------------------------------------

/// Outbound message to send via UDP.
#[derive(Debug)]
pub struct OutboundMessage {
    /// Destination address.
    pub addr: SocketAddr,

    /// Serialized message bytes.
    pub data: Vec<u8>,
}

/// Timer to schedule via Tokio.
#[derive(Debug)]
pub struct ScheduledTimer {
    /// Delay before firing.
    pub delay: std::time::Duration,

    /// The timer event to deliver.
    pub event: TimerEvent,
}

/// Accumulated output from a foca operation.
///
/// Collects outbound messages, scheduled timers, and
/// membership events during a single foca interaction.
/// The caller drains these after each foca call.
#[derive(Debug, Default)]
pub struct AccumulatedOutput {
    /// Messages to send via UDP.
    pub messages: Vec<OutboundMessage>,

    /// Timers to schedule via Tokio.
    pub timers: Vec<ScheduledTimer>,

    /// Membership events to forward to the operator.
    pub events: Vec<MemberEvent>,
}

impl AccumulatedOutput {
    /// Create an empty output accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return true if there is nothing to process.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.timers.is_empty() && self.events.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Foca Runtime
// ---------------------------------------------------------------------------

/// Foca [`Runtime`] that accumulates output for batch processing.
///
/// Instead of immediately sending UDP packets or spawning timers,
/// this runtime collects all side effects into an
/// [`AccumulatedOutput`] that the caller drains after each foca
/// interaction.
///
/// [`Runtime`]: foca::Runtime
pub struct GridRuntime {
    /// Accumulated output from foca operations.
    output: AccumulatedOutput,

    /// Channel for forwarding membership events to the operator.
    event_tx: mpsc::Sender<MemberEvent>,
}

impl GridRuntime {
    /// Create a new runtime with the given event channel.
    pub fn new(event_tx: mpsc::Sender<MemberEvent>) -> Self {
        Self {
            output: AccumulatedOutput::new(),
            event_tx,
        }
    }

    /// Take the accumulated output, replacing it with an empty one.
    pub fn take_output(&mut self) -> AccumulatedOutput {
        std::mem::take(&mut self.output)
    }
}

impl foca::Runtime<NodeId> for GridRuntime {
    fn notify(&mut self, notification: foca::Notification<'_, NodeId>) {
        let event = match notification {
            foca::Notification::MemberUp(id) => MemberEvent::Joined {
                site_name: id.site_name().to_owned(),
                addr: id.socket_addr(),
            },
            foca::Notification::MemberDown(id) => MemberEvent::Left {
                site_name: id.site_name().to_owned(),
            },
            _ => return,
        };

        self.output.events.push(event.clone());
        drop(self.event_tx.try_send(event));
    }

    fn send_to(&mut self, to: NodeId, data: &[u8]) {
        self.output.messages.push(OutboundMessage {
            addr: to.socket_addr(),
            data: data.to_vec(),
        });
    }

    fn submit_after(&mut self, event: foca::Timer<NodeId>, after: std::time::Duration) {
        self.output.timers.push(ScheduledTimer {
            delay: after,
            event: TimerEvent::Token(event),
        });
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use foca::Runtime as _;

    use super::*;

    #[test]
    fn output_starts_empty() {
        let output = AccumulatedOutput::new();
        assert!(output.is_empty(), "new output should be empty");
    }

    #[test]
    fn send_to_accumulates_messages() {
        let (tx, _rx) = mpsc::channel(16);
        let mut rt = GridRuntime::new(tx);
        let id = test_node("peer");
        rt.send_to(id, b"hello");
        let output = rt.take_output();
        assert_eq!(output.messages.len(), 1, "should have 1 message");
    }

    #[test]
    fn submit_after_accumulates_timers() {
        let (tx, _rx) = mpsc::channel(16);
        let mut rt = GridRuntime::new(tx);
        let timer = foca::Timer::ProbeRandomMember(0);
        rt.submit_after(timer, std::time::Duration::from_secs(5));
        let output = rt.take_output();
        assert_eq!(output.timers.len(), 1, "should have 1 timer");
    }

    #[test]
    fn take_output_resets() {
        let (tx, _rx) = mpsc::channel(16);
        let mut rt = GridRuntime::new(tx);
        rt.send_to(test_node("peer"), b"data");
        let first = rt.take_output();
        assert!(!first.is_empty(), "first take should have data");
        let second = rt.take_output();
        assert!(second.is_empty(), "second take should be empty");
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    /// Create a test node identity.
    fn test_node(name: &str) -> NodeId {
        NodeId::new(
            name.to_owned(),
            "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
        )
    }
}
