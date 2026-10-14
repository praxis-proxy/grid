//! Membership events emitted by the SWIM runtime.

use std::net::SocketAddr;

// ---------------------------------------------------------------------------
// Membership Events
// ---------------------------------------------------------------------------

/// A membership change observed by the SWIM runtime.
///
/// Published in [`AccumulatedOutput::events`] after each foca
/// interaction and consumed by the operator's `drain_output` loop.
///
/// [`AccumulatedOutput::events`]: crate::AccumulatedOutput
#[derive(Clone, Debug)]
pub enum MemberEvent {
    /// A new site has joined the grid.
    Joined {
        /// Site name.
        site_name: String,

        /// Network address.
        addr: SocketAddr,

        /// Process generation of this identity.
        generation: u64,
    },

    /// A site has left the grid (graceful or timeout).
    Left {
        /// Site name.
        site_name: String,

        /// Address of the identity that left.
        addr: SocketAddr,

        /// Process generation of the identity that left.
        generation: u64,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_debug_format() {
        let event = MemberEvent::Joined {
            site_name: "cluster-a".to_owned(),
            addr: "10.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
            generation: 1,
        };
        let debug = format!("{event:?}");
        assert!(debug.contains("cluster-a"), "should contain site name");
    }

    #[test]
    fn event_clone() {
        let event = MemberEvent::Left {
            site_name: "cluster-b".to_owned(),
            addr: "10.0.0.2:7946".parse().unwrap_or_else(|_| std::process::abort()),
            generation: 1,
        };
        let cloned = event.clone();
        assert!(matches!(cloned, MemberEvent::Left { .. }), "should clone correctly");
        assert_eq!(
            format!("{event:?}"),
            format!("{cloned:?}"),
            "clone must be independently equal to the original, not just the same variant shape"
        );
    }
}
