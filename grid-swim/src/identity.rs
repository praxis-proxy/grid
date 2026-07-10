//! SWIM node identity for the AI Grid.
//!
//! Each site in the grid is identified by a unique name and
//! a network address. The name is stable across restarts; the
//! address may change if the pod is rescheduled.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Node Identity
// ---------------------------------------------------------------------------

/// Identity for a node in the SWIM membership protocol.
///
/// Wraps a stable site name with a network address. The
/// `generation` field enables automatic rejoin after being
/// declared dead — a higher generation wins address conflicts.
///
/// ```
/// use grid_swim::NodeId;
///
/// let id = NodeId::new("cluster-a".to_owned(), "10.0.0.1:7946".parse().unwrap());
/// assert_eq!(id.site_name(), "cluster-a");
/// ```
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeId {
    /// Monotonically increasing generation for rejoin.
    generation: u16,

    /// Site name (stable across restarts).
    site_name: String,

    /// Network address for SWIM probes.
    addr: SocketAddr,
}

impl NodeId {
    /// Create a new node identity.
    #[must_use]
    pub fn new(site_name: String, addr: SocketAddr) -> Self {
        Self {
            generation: 0,
            site_name,
            addr,
        }
    }

    /// Return the site name.
    #[must_use]
    pub fn site_name(&self) -> &str {
        &self.site_name
    }

    /// Return the network address.
    #[must_use]
    pub fn socket_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl foca::Identity for NodeId {
    type Addr = SocketAddr;

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn renew(&self) -> Option<Self> {
        Some(Self {
            generation: self.generation.wrapping_add(1),
            site_name: self.site_name.clone(),
            addr: self.addr,
        })
    }

    fn win_addr_conflict(&self, other: &Self) -> bool {
        self.generation > other.generation
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use foca::Identity as _;

    use super::*;

    #[test]
    fn new_creates_generation_zero() {
        let id = NodeId::new(
            "test".to_owned(),
            "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
        );
        assert_eq!(id.site_name(), "test", "site name");
        assert_eq!(id.generation, 0, "initial generation");
    }

    #[test]
    fn renew_increments_generation() {
        let id = NodeId::new(
            "test".to_owned(),
            "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
        );
        let renewed = id.renew();
        assert!(renewed.is_some(), "should produce renewed identity");
        let renewed = renewed.unwrap_or_else(|| std::process::abort());
        assert_eq!(renewed.generation, 1, "generation should increment");
        assert_eq!(renewed.site_name(), "test", "name preserved");
    }

    #[test]
    fn higher_generation_wins_conflict() {
        let old = NodeId::new(
            "test".to_owned(),
            "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
        );
        let new = old.renew().unwrap_or_else(|| std::process::abort());
        assert!(new.win_addr_conflict(&old), "newer should win");
        assert!(!old.win_addr_conflict(&new), "older should lose");
    }

    #[test]
    fn addr_returns_socket_addr() {
        let addr: SocketAddr = "10.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort());
        let id = NodeId::new("site".to_owned(), addr);
        assert_eq!(id.addr(), addr, "addr mismatch");
    }
}
