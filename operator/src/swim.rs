//! SWIM membership data model and status summarization.
//!
//! This module provides the pure, data-only representation of SWIM-based peer
//! discovery.  It holds no live networking handles, background tasks, or global
//! mutable state.  The live foca-backed UDP runtime lives in
//! [`crate::swim_runtime`] and produces [`MembershipSnapshot`] values consumed
//! by the [`GridNetwork`] controller.
//!
//! [`MembershipSnapshot`]: crate::swim::MembershipSnapshot
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork

use crate::crd::grid_network::GridNetworkPhase;

// ---------------------------------------------------------------------------
// Membership status
// ---------------------------------------------------------------------------

/// Observed status of a single peer in the local SWIM membership view.
///
/// Only `Alive` peers contribute to the connected-site count and to the
/// `Active` phase hint.  `Suspect` peers are within the suspicion window
/// and may recover.  `Dead` peers have been confirmed unreachable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemberStatus {
    /// Peer has been heard from recently and responds to probes.
    Alive,

    /// Peer has not responded to probes but is still within the suspicion window.
    ///
    /// A suspect peer is not counted as connected.  The SWIM runtime promotes
    /// it to [`Dead`] if the suspicion timeout elapses without recovery.
    ///
    /// [`Dead`]: MemberStatus::Dead
    Suspect,

    /// Peer has been confirmed unreachable or explicitly evicted.
    ///
    /// Dead records are not counted as connected and should eventually be pruned
    /// by the runtime once they exceed a tombstone retention window.
    Dead,
}

// ---------------------------------------------------------------------------
// Member record
// ---------------------------------------------------------------------------

/// One entry in the SWIM membership table.
///
/// All fields are provided by the SWIM runtime at snapshot time.  This struct
/// carries no live handles — it is safe to clone, serialize, or pass across
/// thread boundaries.
#[derive(Clone, Debug)]
pub struct MemberRecord {
    /// Opaque site identity.
    ///
    /// Typically matches `GridSite.metadata.name` or a configured advertised
    /// ID from `GridNetwork.spec.gridId`.
    pub site_id: String,

    /// Advertised SWIM listener address (e.g. `"10.0.1.5:7946"`).
    pub endpoint: String,

    /// Incarnation counter from the SWIM protocol.
    ///
    /// Monotonically increasing per peer; prevents old gossip from overriding
    /// state that was set during a later incarnation.
    pub incarnation: u64,

    /// Membership status as observed by this node.
    pub status: MemberStatus,

    /// Seconds since this record was last updated by the SWIM runtime.
    ///
    /// A value of `0` means the record was received in the current gossip
    /// round.  Callers use this field to detect records that have not been
    /// refreshed and may be stale.
    pub age_secs: u64,

    /// Data-plane gateway address advertised by this peer.
    ///
    /// When present, this is the address that should be used for the
    /// `GridSite.spec.egress.address` field instead of the SWIM UDP
    /// endpoint.  `None` when the peer has not configured a gateway address.
    pub gateway_address: Option<String>,

    /// Public site certificate PEM received from this peer via SWIM broadcast.
    ///
    /// Contains only the public certificate — never a private key.
    /// `None` when the peer has not yet broadcast its site certificate.
    pub site_cert_pem: Option<String>,
}

impl MemberRecord {
    /// Returns `true` when this record is at least `threshold_secs` old.
    ///
    /// Stale detection is the responsibility of the snapshot consumer, not the
    /// SWIM runtime.  A typical use: prune `Dead` records older than the
    /// tombstone retention window before passing the snapshot to the controller.
    pub fn is_stale(&self, threshold_secs: u64) -> bool {
        self.age_secs >= threshold_secs
    }
}

// ---------------------------------------------------------------------------
// Membership snapshot
// ---------------------------------------------------------------------------

/// Point-in-time view of the SWIM membership table.
///
/// Produced by polling a SWIM runtime (foca, memberlist, etc.) or by injecting
/// a static test fixture.  Holds no live handles — safe to clone and pass to
/// pure summarization functions.
///
/// # Staleness
///
/// Each [`MemberRecord`] carries an `age_secs` field.  Before summarizing,
/// callers may filter stale entries with [`MemberRecord::is_stale`] to avoid
/// stale records inflating [`connected_count`] or affecting [`phase_hint`].
///
/// [`connected_count`]: MembershipSnapshot::connected_count
/// [`phase_hint`]: MembershipSnapshot::phase_hint
#[derive(Clone, Debug, Default)]
pub struct MembershipSnapshot {
    /// All known member records at the time of the snapshot.
    pub members: Vec<MemberRecord>,
}

impl MembershipSnapshot {
    /// Count the number of members with [`MemberStatus::Alive`] status.
    ///
    /// Only `Alive` peers are counted.  `Suspect` and `Dead` peers do not
    /// contribute to the connected-site count.
    pub fn connected_count(&self) -> u32 {
        u32::try_from(self.members.iter().filter(|m| m.status == MemberStatus::Alive).count()).unwrap_or(u32::MAX)
    }

    /// Derive a [`GridNetworkPhase`] hint from the current membership state.
    ///
    /// | Snapshot state | Hint |
    /// |----------------|------|
    /// | Empty | `None` — caller uses its own phase logic |
    /// | ≥1 `Alive` member | `Some(Active)` — network is operational |
    /// | Members exist, all `Suspect`/`Dead` | `Some(Degraded)` — network is impaired |
    ///
    /// The caller applies the hint on top of its existing phase logic, so an
    /// empty snapshot never overrides `Pending` or `Initializing`.
    pub fn phase_hint(&self) -> Option<GridNetworkPhase> {
        if self.members.is_empty() {
            return None;
        }
        if self.members.iter().any(|m| m.status == MemberStatus::Alive) {
            return Some(GridNetworkPhase::Active);
        }
        Some(GridNetworkPhase::Degraded)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Test utilities
    // -----------------------------------------------------------------------

    /// Build a [`MemberRecord`] with the given status and zero age.
    fn member(site_id: &str, status: MemberStatus) -> MemberRecord {
        MemberRecord {
            site_id: site_id.to_owned(),
            endpoint: "10.0.0.1:7946".to_owned(),
            incarnation: 1,
            status,
            age_secs: 0,
            gateway_address: None,
            site_cert_pem: None,
        }
    }

    fn alive(site_id: &str) -> MemberRecord {
        member(site_id, MemberStatus::Alive)
    }

    fn suspect(site_id: &str) -> MemberRecord {
        member(site_id, MemberStatus::Suspect)
    }

    fn dead(site_id: &str) -> MemberRecord {
        member(site_id, MemberStatus::Dead)
    }

    fn snapshot(members: Vec<MemberRecord>) -> MembershipSnapshot {
        MembershipSnapshot { members }
    }

    // -----------------------------------------------------------------------
    // connected_count
    // -----------------------------------------------------------------------

    #[test]
    fn empty_snapshot_has_zero_connected_count() {
        let s = MembershipSnapshot::default();
        assert_eq!(s.connected_count(), 0, "empty snapshot must have 0 connected sites");
    }

    #[test]
    fn alive_members_are_counted_as_connected() {
        let s = snapshot(vec![alive("site-p"), alive("site-q")]);
        assert_eq!(s.connected_count(), 2, "two Alive members must give count=2");
    }

    #[test]
    fn suspect_members_not_counted_as_connected() {
        let s = snapshot(vec![suspect("site-p"), suspect("site-q")]);
        assert_eq!(s.connected_count(), 0, "Suspect members must not count as connected");
    }

    #[test]
    fn dead_members_not_counted_as_connected() {
        let s = snapshot(vec![dead("site-p")]);
        assert_eq!(s.connected_count(), 0, "Dead members must not count as connected");
    }

    #[test]
    fn mixed_status_only_alive_counted() {
        let s = snapshot(vec![alive("site-a"), suspect("site-b"), dead("site-c")]);
        assert_eq!(s.connected_count(), 1, "only Alive members must be counted");
    }

    // -----------------------------------------------------------------------
    // phase_hint
    // -----------------------------------------------------------------------

    #[test]
    fn empty_snapshot_phase_hint_is_none() {
        let s = MembershipSnapshot::default();
        assert!(s.phase_hint().is_none(), "empty snapshot must not provide a phase hint");
    }

    #[test]
    fn all_alive_phase_hint_is_active() {
        let s = snapshot(vec![alive("site-a"), alive("site-b")]);
        assert_eq!(
            s.phase_hint(),
            Some(GridNetworkPhase::Active),
            "all-Alive snapshot must hint Active"
        );
    }

    #[test]
    fn all_suspect_phase_hint_is_degraded() {
        let s = snapshot(vec![suspect("site-a"), suspect("site-b")]);
        assert_eq!(
            s.phase_hint(),
            Some(GridNetworkPhase::Degraded),
            "all-Suspect snapshot must hint Degraded"
        );
    }

    #[test]
    fn all_dead_phase_hint_is_degraded() {
        let s = snapshot(vec![dead("site-a")]);
        assert_eq!(
            s.phase_hint(),
            Some(GridNetworkPhase::Degraded),
            "all-Dead snapshot must hint Degraded"
        );
    }

    #[test]
    fn mix_with_alive_phase_hint_is_active() {
        // Even one Alive member is enough to keep the phase Active.
        let s = snapshot(vec![alive("site-a"), suspect("site-b"), dead("site-c")]);
        assert_eq!(
            s.phase_hint(),
            Some(GridNetworkPhase::Active),
            "snapshot with at least one Alive member must hint Active"
        );
    }

    // -----------------------------------------------------------------------
    // is_stale
    // -----------------------------------------------------------------------

    #[test]
    fn fresh_record_is_not_stale() {
        let m = alive("site-a");
        assert!(
            !m.is_stale(30),
            "record with age_secs=0 must not be stale at threshold 30"
        );
    }

    #[test]
    fn record_at_threshold_is_stale() {
        let m = MemberRecord {
            age_secs: 30,
            ..alive("site-a")
        };
        assert!(m.is_stale(30), "record with age_secs==threshold must be stale");
    }

    #[test]
    fn old_record_is_stale() {
        let m = MemberRecord {
            age_secs: 120,
            ..alive("site-a")
        };
        assert!(m.is_stale(30), "record with age_secs > threshold must be stale");
    }

    #[test]
    fn stale_dead_records_do_not_inflate_connected_count() {
        // Simulate pruning stale Dead records before summarizing.
        let mut snap = snapshot(vec![
            MemberRecord {
                age_secs: 300,
                ..dead("site-gone")
            },
            alive("site-present"),
        ]);
        snap.members.retain(|m| !m.is_stale(60));
        assert_eq!(
            snap.connected_count(),
            1,
            "after pruning stale Dead, only live Alive member remains"
        );
    }
}
