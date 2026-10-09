//! The `PeerAgesRejected` condition on the `GridNetwork`: which peers' relayed
//! signal ages this site's peer poller is rejecting.
//!
//! A relayed age survives a clock step on every hop, so a rejected one means a
//! peer's stamps ran more than a second past its own `Date`, or more than a day
//! behind it: that peer's clock, or its stamping, is wrong. Routing already
//! treats the dropped rows as absent. The condition says why.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::crd::{grid_network::GridNetwork, inference_provider::Condition};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The condition's type.
pub const PEER_AGES_REJECTED: &str = "PeerAgesRejected";

/// Reason while some polled peer's relayed ages are being rejected.
pub const IMPLAUSIBLE_PEER_AGE: &str = "ImplausiblePeerAge";

/// Reason while no polled peer's are.
pub const PEER_AGES_ACCEPTED: &str = "PeerAgesAccepted";

/// Server-side apply manager for the condition, apart from `GridNetwork`
/// reconciliation, so neither removes what the other wrote.
pub const FIELD_MANAGER: &str = "grid-operator-peer-signals";

/// Peers a message names before it counts the rest.
const MAX_NAMED_PEERS: usize = 10;

// ---------------------------------------------------------------------------
// Peer Ages
// ---------------------------------------------------------------------------

/// Peers whose relayed ages the poller is rejecting, carried from round to round.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerAges {
    /// Peers whose latest answer carried a rejected age.
    rejecting: BTreeSet<String>,
}

impl PeerAges {
    /// Fold in one round: `polled` the peers dialed, and `answered` each answering
    /// peer's count of rejected ages.
    ///
    /// An answer alone judges a peer, so one clean answer clears it. A peer that
    /// did not answer keeps its standing, since silence says nothing about its
    /// clock, and a peer no longer polled is forgotten.
    pub fn observe(&mut self, polled: &[String], answered: &BTreeMap<String, usize>) {
        self.rejecting.retain(|peer| polled.contains(peer));
        for peer in polled {
            match answered.get(peer) {
                Some(0) => {
                    self.rejecting.remove(peer);
                },
                Some(_) => {
                    self.rejecting.insert(peer.clone());
                },
                None => {},
            }
        }
    }

    /// Peers whose relayed ages are being rejected, in name order.
    #[must_use]
    pub const fn rejecting(&self) -> &BTreeSet<String> {
        &self.rejecting
    }

    /// The condition to write over `current`, `None` when it already says this.
    ///
    /// Unlike `Ready`, a change of message alone is written, since the message is
    /// where the peers are named. `lastTransitionTime` moves only with the status.
    #[must_use]
    pub fn condition(&self, current: &[Condition], now_rfc3339: &str, generation: Option<i64>) -> Option<Condition> {
        let held = current.iter().find(|held| held.type_ == PEER_AGES_REJECTED);
        let (status, reason) = if self.rejecting.is_empty() {
            ("False", PEER_AGES_ACCEPTED)
        } else {
            ("True", IMPLAUSIBLE_PEER_AGE)
        };
        let message = self.message();
        if held.is_some_and(|held| {
            held.status == status
                && held.reason == reason
                && held.message == message
                && held.observed_generation == generation
        }) {
            return None;
        }
        let last_transition_time = held
            .filter(|held| held.status == status)
            .map_or_else(|| now_rfc3339.to_owned(), |held| held.last_transition_time.clone());
        Some(Condition {
            type_: PEER_AGES_REJECTED.to_owned(),
            status: status.to_owned(),
            reason: reason.to_owned(),
            message,
            last_transition_time,
            observed_generation: generation,
        })
    }

    /// What the condition says: the peers rejected, by name only, and what to check.
    fn message(&self) -> String {
        if self.rejecting.is_empty() {
            return "no polled peer's relayed signal ages are being rejected".to_owned();
        }
        let named: Vec<&str> = self
            .rejecting
            .iter()
            .take(MAX_NAMED_PEERS)
            .map(String::as_str)
            .collect();
        let more = self.rejecting.len().saturating_sub(named.len());
        let peers = if more == 0 {
            named.join(", ")
        } else {
            format!("{} and {more} more", named.join(", "))
        };
        format!(
            "rejecting relayed signal ages from {peers}: their samples are stamped more than 1s past their \
             Date or more than a day before it; check those sites' clocks"
        )
    }
}

/// The status patch carrying `network`'s `PeerAgesRejected` condition from
/// `ages`, with the name to apply it to, or `None` when the live status already
/// says this.
#[must_use]
pub fn patch<'network>(
    network: &'network GridNetwork,
    ages: &PeerAges,
    now_rfc3339: &str,
) -> Option<(&'network str, Value)> {
    let name = network.metadata.name.as_deref()?;
    let current = network
        .status
        .as_ref()
        .map_or(&[][..], |status| status.conditions.as_slice());
    let condition = ages.condition(current, now_rfc3339, network.metadata.generation)?;
    Some((
        name,
        serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": name },
            "status": { "conditions": [condition] }
        }),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn a_peer_serving_implausible_ages_is_named_in_a_true_condition() {
        let ages = after(&PeerAges::default(), &["east", "west"], &[("east", 2), ("west", 0)]);
        let raised = ages.condition(&[], "t0", Some(3)).expect("absent is written");
        assert_eq!(
            (
                raised.status.as_str(),
                raised.reason.as_str(),
                raised.last_transition_time.as_str(),
                raised.observed_generation
            ),
            ("True", IMPLAUSIBLE_PEER_AGE, "t0", Some(3))
        );
        assert!(
            raised.message.contains("east") && !raised.message.contains("west"),
            "only the rejected peer is named: {}",
            raised.message
        );
    }

    #[test]
    fn a_clean_answer_clears_it_back_to_false() {
        let rejecting = after(&PeerAges::default(), &["east"], &[("east", 1)]);
        let raised = rejecting.condition(&[], "t0", Some(3)).expect("raised");
        let cleared = after(&rejecting, &["east"], &[("east", 0)])
            .condition(std::slice::from_ref(&raised), "t1", Some(3))
            .expect("a clean round is written");
        assert_eq!(
            (
                cleared.status.as_str(),
                cleared.reason.as_str(),
                cleared.last_transition_time.as_str()
            ),
            ("False", PEER_AGES_ACCEPTED, "t1")
        );
    }

    #[test]
    fn a_silent_peer_keeps_its_standing() {
        let rejecting = after(&PeerAges::default(), &["east"], &[("east", 1)]);
        let raised = rejecting.condition(&[], "t0", Some(3)).expect("raised");
        let silent = after(&rejecting, &["east"], &[]);
        assert_eq!(silent, rejecting, "no answer says nothing about the peer's clock");
        assert_eq!(
            silent.condition(std::slice::from_ref(&raised), "t1", Some(3)),
            None,
            "and nothing is written"
        );
    }

    #[test]
    fn a_departed_peer_is_forgotten_and_no_peers_at_all_clears_it() {
        let rejecting = after(&PeerAges::default(), &["east", "west"], &[("east", 1), ("west", 0)]);
        assert!(
            after(&rejecting, &["west"], &[]).rejecting().is_empty(),
            "a peer that left membership is dropped"
        );
        let raised = rejecting.condition(&[], "t0", Some(3)).expect("raised");
        let alone = after(&rejecting, &[], &[]);
        assert_eq!(
            alone
                .condition(std::slice::from_ref(&raised), "t1", Some(3))
                .map(|cleared| cleared.status),
            Some("False".to_owned()),
            "a round with no peer to poll clears it"
        );
    }

    #[test]
    fn a_change_of_peers_alone_is_written() {
        let east = after(&PeerAges::default(), &["east", "west"], &[("east", 1), ("west", 0)]);
        let raised = east.condition(&[], "t0", Some(3)).expect("raised");
        let both = after(&east, &["east", "west"], &[("east", 1), ("west", 1)])
            .condition(std::slice::from_ref(&raised), "t1", Some(3))
            .expect("a new peer set is written though the status holds");
        assert!(both.message.contains("east, west"), "both are named: {}", both.message);
        assert_eq!(
            both.last_transition_time, "t0",
            "the status did not change, so neither does its transition time"
        );
    }

    #[test]
    fn nothing_is_written_when_nothing_changed() {
        let quiet = PeerAges::default();
        let accepted = quiet.condition(&[], "t0", Some(3)).expect("absent is written");
        assert_eq!(accepted.status, "False", "a poller with nothing rejected says so");
        assert_eq!(quiet.condition(std::slice::from_ref(&accepted), "t1", Some(3)), None);
        assert!(
            quiet
                .condition(std::slice::from_ref(&accepted), "t1", Some(4))
                .is_some_and(|regenerated| regenerated.last_transition_time == "t0"),
            "a new generation is written, keeping its transition time"
        );
    }

    #[test]
    fn a_restart_reconciles_against_the_live_status() {
        let before = after(&PeerAges::default(), &["east"], &[("east", 1)]);
        let live = before.condition(&[], "t0", Some(3)).expect("raised before the restart");
        let restarted = after(&PeerAges::default(), &["east"], &[("east", 0)]);
        assert_eq!(
            restarted
                .condition(std::slice::from_ref(&live), "t1", Some(3))
                .map(|cleared| cleared.status),
            Some("False".to_owned()),
            "the first round after a restart corrects what the last process wrote"
        );
        let still = after(&PeerAges::default(), &["east"], &[("east", 1)]);
        assert_eq!(
            still.condition(std::slice::from_ref(&live), "t1", Some(3)),
            None,
            "and leaves what is still true alone"
        );
    }

    #[test]
    fn the_message_names_peers_and_nothing_else() {
        let names: Vec<String> = (0..12).map(|peer| format!("site-{peer:02}")).collect();
        let answered = names.iter().map(|peer| (peer.clone(), 1)).collect();
        let mut many = PeerAges::default();
        many.observe(&names, &answered);
        assert_eq!(
            many.message(),
            "rejecting relayed signal ages from site-00, site-01, site-02, site-03, site-04, site-05, site-06, \
             site-07, site-08, site-09 and 2 more: their samples are stamped more than 1s past their Date or \
             more than a day before it; check those sites' clocks"
        );
    }

    #[test]
    fn the_patch_carries_one_condition_for_the_named_network() {
        let mut network: GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "grid", "generation": 4 },
            "spec": { "seeds": [], "gridId": "test-id" }
        }))
        .expect("a network");
        let rejecting = after(&PeerAges::default(), &["east"], &[("east", 1)]);
        let (name, body) = patch(&network, &rejecting, "t0").expect("written");
        let condition = rejecting.condition(&[], "t0", Some(4)).expect("raised");
        assert_eq!(name, "grid");
        assert_eq!(
            body,
            serde_json::json!({
                "apiVersion": "grid.praxis.fast/v1alpha1",
                "kind": "GridNetwork",
                "metadata": { "name": "grid" },
                "status": { "conditions": [condition] }
            }),
            "only this condition, at the network's generation, so no other manager's entry is claimed"
        );
        network.metadata.name = None;
        assert!(
            patch(&network, &rejecting, "t0").is_none(),
            "an unnamed network is not patched"
        );
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    /// `ages` after one round polling `polled`, where `answered` names each answering
    /// peer's count of rejected ages.
    fn after(ages: &PeerAges, polled: &[&str], answered: &[(&str, usize)]) -> PeerAges {
        let mut next = ages.clone();
        next.observe(
            &polled.iter().map(|peer| (*peer).to_owned()).collect::<Vec<_>>(),
            &answered
                .iter()
                .map(|(peer, rejected)| ((*peer).to_owned(), *rejected))
                .collect(),
        );
        next
    }
}
