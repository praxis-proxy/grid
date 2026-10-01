//! SWIM node identity for the AI Grid.
//!
//! Each site in the grid is identified by a unique name and
//! a network address. The name is stable across restarts; the
//! address may change if the pod is rescheduled.

use std::{
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

/// Skew tolerated on a peer's clock-seeded generation or revision before it counts as forged.
pub const MAX_LEASE_SKEW: Duration = Duration::from_secs(600);

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
/// use swim::NodeId;
///
/// let id = NodeId::new("cluster-a".to_owned(), "10.0.0.1:7946".parse().unwrap());
/// assert_eq!(id.site_name(), "cluster-a");
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeId {
    /// Restart-scale generation used to supersede an older process identity.
    generation: u64,

    /// Highest generation reserved for this process.
    ///
    /// This is process-local control state, not part of the wire identity.
    #[serde(skip, default = "maximum_generation")]
    generation_limit: u64,

    /// Site name (stable across restarts).
    site_name: String,

    /// Network address for SWIM probes.
    addr: SocketAddr,
}

impl NodeId {
    /// Create a process identity that supersedes an older process at the same
    /// advertised address.
    #[must_use]
    pub fn new(site_name: String, addr: SocketAddr) -> Self {
        Self {
            generation: initial_generation(),
            generation_limit: u64::MAX,
            site_name,
            addr,
        }
    }

    /// Create a process identity with a generation reserved by the caller.
    ///
    /// Grid's operator uses this constructor after durably reserving a
    /// restart-monotonic generation in Kubernetes.
    #[must_use]
    pub fn with_generation(site_name: String, addr: SocketAddr, generation: u64) -> Self {
        Self {
            generation,
            generation_limit: generation,
            site_name,
            addr,
        }
    }

    /// Create a process identity with a durably reserved generation range.
    ///
    /// Foca may renew an identity after the local member is declared down.
    /// Renewal stops at `last_generation`, preventing the process from using a
    /// generation that a replacement process could reserve.
    #[must_use]
    pub fn with_generation_range(
        site_name: String,
        addr: SocketAddr,
        first_generation: u64,
        last_generation: u64,
    ) -> Self {
        Self {
            generation: first_generation,
            generation_limit: last_generation.max(first_generation),
            site_name,
            addr,
        }
    }

    /// Create an address-only seed placeholder.
    ///
    /// The placeholder exists only to address the initial announcement. Its
    /// zero generation ensures that the real peer identity received from that
    /// address wins the first address conflict.
    #[must_use]
    pub fn seed(addr: SocketAddr) -> Self {
        Self {
            generation: 0,
            generation_limit: 0,
            site_name: format!("seed-{addr}"),
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

    /// Return the process generation. A higher one supersedes a lower one.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

impl PartialEq for NodeId {
    fn eq(&self, other: &Self) -> bool {
        self.generation == other.generation && self.site_name == other.site_name && self.addr == other.addr
    }
}

impl Eq for NodeId {}

impl foca::Identity for NodeId {
    type Addr = SocketAddr;

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn renew(&self) -> Option<Self> {
        self.generation
            .checked_add(1)
            .filter(|generation| *generation <= self.generation_limit)
            .map(|generation| Self {
                generation,
                generation_limit: self.generation_limit,
                site_name: self.site_name.clone(),
                addr: self.addr,
            })
    }

    fn win_addr_conflict(&self, other: &Self) -> bool {
        let now = initial_generation();
        match (
            is_future_generation(self.generation, now),
            is_future_generation(other.generation, now),
        ) {
            (false, true) => true,
            (true, false) => false,
            _ => self.generation > other.generation,
        }
    }
}

/// [`MAX_LEASE_SKEW`] in nanoseconds, the unit generations are seeded in.
fn skew_nanos() -> u64 {
    u64::try_from(MAX_LEASE_SKEW.as_nanos()).unwrap_or(u64::MAX)
}

/// Whether `generation` lies past `now_nanos` by more than [`MAX_LEASE_SKEW`].
#[must_use]
pub fn is_future_generation(generation: u64, now_nanos: u64) -> bool {
    generation > now_nanos.saturating_add(skew_nanos())
}

/// Default generation limit for identities decoded from the wire.
const fn maximum_generation() -> u64 {
    u64::MAX
}

/// Seed a standalone process identity from wall-clock nanoseconds.
///
/// A process-local zero value cannot supersede the membership identity retained
/// for a previous process at the same advertised address. Nanosecond-scale
/// generations preserve ordering across ordinary restarts and leave room for
/// foca's in-process renewals. Production operator startup uses
/// [`NodeId::with_generation`] with a durably reserved generation instead.
fn initial_generation() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use foca::Identity as _;

    use super::*;

    #[test]
    fn new_creates_restart_scale_generation() {
        let id = NodeId::new(
            "test".to_owned(),
            "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort()),
        );
        assert_eq!(id.site_name(), "test", "site name");
        assert!(id.generation > 1_000_000_000_000_000_000);
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
        assert_eq!(renewed.generation, id.generation + 1, "generation should increment");
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
    fn a_generation_from_the_future_never_wins_a_conflict() {
        let addr = "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort());
        let live = NodeId::new("test".to_owned(), addr);
        let forged = NodeId::with_generation("test".to_owned(), addr, u64::MAX);
        assert!(
            live.win_addr_conflict(&forged),
            "the live identity beats a forged future one"
        );
        assert!(!forged.win_addr_conflict(&live), "the forged identity loses");
        let near = NodeId::with_generation("test".to_owned(), addr, live.generation + skew_nanos() / 2);
        assert!(near.win_addr_conflict(&live), "within the skew, higher still wins");
    }

    #[test]
    fn process_identity_supersedes_seed_placeholder() {
        let addr = "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort());
        let seed = NodeId::seed(addr);
        let process = NodeId::new("test".to_owned(), addr);

        assert!(process.win_addr_conflict(&seed), "real process identity must win");
        assert!(!seed.win_addr_conflict(&process), "seed placeholder must lose");
    }

    #[test]
    fn addr_returns_socket_addr() {
        let addr: SocketAddr = "10.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort());
        let id = NodeId::new("site".to_owned(), addr);
        assert_eq!(id.addr(), addr, "addr mismatch");
    }

    #[test]
    fn renew_refuses_to_wrap_at_u64_max() {
        let addr: SocketAddr = "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort());
        let id = NodeId {
            generation: u64::MAX,
            generation_limit: u64::MAX,
            site_name: "site".to_owned(),
            addr,
        };
        assert!(id.renew().is_none(), "generation must not wrap");
    }

    #[test]
    fn with_generation_uses_exact_reserved_value() {
        let addr: SocketAddr = "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort());
        let id = NodeId::with_generation("test".to_owned(), addr, 42);
        assert_eq!(id.generation, 42, "generation must use the durable reservation");
    }

    #[test]
    fn reserved_generation_range_bounds_renewal() {
        let addr = "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort());
        let first = NodeId::with_generation_range("test".to_owned(), addr, 41, 42);
        let second = first.renew().unwrap_or_else(|| std::process::abort());
        assert_eq!(second.generation, 42);
        assert!(second.renew().is_none(), "renewal must stop at the durable reservation");
    }

    #[test]
    fn with_generation_accepts_zero_for_seeded_tests() {
        let addr: SocketAddr = "127.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort());
        let id = NodeId::with_generation("test".to_owned(), addr, 0);
        assert_eq!(id.generation, 0);
    }

    #[test]
    fn node_id_serde_round_trip() {
        let addr: SocketAddr = "10.0.0.1:7946".parse().unwrap_or_else(|_| std::process::abort());
        let id = NodeId::new("my-site".to_owned(), addr);
        let json = serde_json::to_string(&id).unwrap_or_else(|_| std::process::abort());
        let restored: NodeId = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            restored.site_name(),
            "my-site",
            "serde round-trip must preserve site_name"
        );
        assert_eq!(restored.addr(), addr, "serde round-trip must preserve addr");
        assert_eq!(
            restored.generation, id.generation,
            "serde round-trip must preserve generation"
        );
    }
}
