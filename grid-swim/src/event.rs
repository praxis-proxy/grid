//! Membership events emitted by the SWIM runtime.

use std::net::SocketAddr;

// ---------------------------------------------------------------------------
// Membership Events
// ---------------------------------------------------------------------------

/// A membership change observed by the SWIM runtime.
///
/// Sent from the SWIM runtime to the `GridSite` controller
/// via a `tokio::sync::mpsc` channel.
#[derive(Clone, Debug)]
pub enum MemberEvent {
    /// A new site has joined the grid.
    Joined {
        /// Site name.
        site_name: String,

        /// Network address.
        addr: SocketAddr,
    },

    /// A site has left the grid (graceful or timeout).
    Left {
        /// Site name.
        site_name: String,
    },

    /// A site is suspected of being unreachable.
    Suspect {
        /// Site name.
        site_name: String,
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
        };
        let debug = format!("{event:?}");
        assert!(debug.contains("cluster-a"), "should contain site name");
    }

    #[test]
    fn event_clone() {
        let event = MemberEvent::Left {
            site_name: "cluster-b".to_owned(),
        };
        let cloned = event.clone();
        assert!(matches!(cloned, MemberEvent::Left { .. }), "should clone correctly");
    }
}
