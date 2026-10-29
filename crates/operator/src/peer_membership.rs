//! The `PeerMembership` condition on the `GridNetwork`: whether every polled peer
//! learns its members the way this site does.
//!
//! A site running Swim and one running Local still route one way, but a
//! Local site never gossips and a Swim site that only gossips never polls, so
//! the mismatch is reported rather than refused. A peer that sends no mode is an
//! older release and judged by nothing.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::crd::{
    grid_network::GridNetwork,
    inference_provider::{Condition, ConditionStatus},
};

/// The condition's type.
pub const PEER_MEMBERSHIP: &str = "PeerMembership";

/// Reason while some polled peer names a different mode.
pub const PEER_MEMBERSHIP_MISMATCH: &str = "PeerMembershipMismatch";

/// Reason while none does: the type's own name.
pub const PEER_MEMBERSHIP_MATCHES: &str = PEER_MEMBERSHIP;

/// Server-side apply manager, apart from the other `GridNetwork` writers.
pub const FIELD_MANAGER: &str = "grid-operator-peer-membership";

/// Peers a message names before it counts the rest.
const MAX_NAMED_PEERS: usize = 10;

/// Peers naming a different membership mode, carried from round to round.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerMemberships {
    /// Each mismatched peer and the mode it named, in name order for a stable message.
    mismatched: BTreeMap<String, String>,
}

impl PeerMemberships {
    /// Fold in one round: `polled` the peers dialed, `answered` the mode each answering
    /// peer named, and `ours` this site's.
    ///
    /// A peer that did not answer, or answered without a mode, keeps its standing, and a
    /// peer no longer polled is forgotten.
    pub fn observe(&mut self, polled: &[String], answered: &BTreeMap<String, Option<String>>, ours: &str) {
        self.mismatched.retain(|peer, _| polled.contains(peer));
        for (peer, mode) in answered {
            match mode {
                Some(mode) if mode != ours => {
                    self.mismatched.insert(peer.clone(), mode.clone());
                },
                Some(_) => {
                    self.mismatched.remove(peer);
                },
                None => {},
            }
        }
    }

    /// Peers naming a different mode, in name order.
    #[must_use]
    pub const fn mismatched(&self) -> &BTreeMap<String, String> {
        &self.mismatched
    }

    /// The condition to write over `current`, `None` when it already says this.
    #[must_use]
    pub fn condition(
        &self,
        current: &[Condition],
        ours: &str,
        now_rfc3339: &str,
        generation: Option<i64>,
    ) -> Option<Condition> {
        let held = current.iter().find(|held| held.type_ == PEER_MEMBERSHIP);
        let (status, reason) = if self.mismatched.is_empty() {
            (ConditionStatus::True, PEER_MEMBERSHIP_MATCHES)
        } else {
            (ConditionStatus::False, PEER_MEMBERSHIP_MISMATCH)
        };
        let message = self.message(ours);
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
            type_: PEER_MEMBERSHIP.to_owned(),
            status,
            reason: reason.to_owned(),
            message,
            last_transition_time,
            observed_generation: generation,
        })
    }

    /// What the condition says: the mismatched peers and their modes, by name only.
    fn message(&self, ours: &str) -> String {
        if self.mismatched.is_empty() {
            return format!("every polled peer that names a membership mode runs {ours}");
        }
        let named: Vec<String> = self
            .mismatched
            .iter()
            .take(MAX_NAMED_PEERS)
            .map(|(peer, mode)| format!("{peer} ({mode})"))
            .collect();
        let more = self.mismatched.len().saturating_sub(named.len());
        let peers = if more == 0 {
            named.join(", ")
        } else {
            format!("{} and {more} more", named.join(", "))
        };
        format!("this site runs {ours} membership, but {peers} differ; set grid.membership.source alike on every site")
    }
}

/// The status patch carrying `network`'s `PeerMembership` condition, with the name to
/// apply it to, or `None` when the live status already says this.
#[must_use]
pub fn patch<'network>(
    network: &'network GridNetwork,
    memberships: &PeerMemberships,
    ours: &str,
    now_rfc3339: &str,
) -> Option<(&'network str, Value)> {
    let name = network.metadata.name.as_deref()?;
    let current = network
        .status
        .as_ref()
        .map_or(&[][..], |status| status.conditions.as_slice());
    let condition = memberships.condition(current, ours, now_rfc3339, network.metadata.generation)?;
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{PEER_MEMBERSHIP_MATCHES, PEER_MEMBERSHIP_MISMATCH, PeerMemberships};

    fn after(held: &PeerMemberships, polled: &[&str], answered: &[(&str, Option<&str>)]) -> PeerMemberships {
        let mut next = held.clone();
        let polled: Vec<String> = polled.iter().map(|peer| (*peer).to_owned()).collect();
        let answered = answered
            .iter()
            .map(|(peer, mode)| ((*peer).to_owned(), mode.map(str::to_owned)))
            .collect::<BTreeMap<_, _>>();
        next.observe(&polled, &answered, "Local");
        next
    }

    #[test]
    fn a_peer_naming_another_mode_is_a_mismatch_until_it_names_ours() {
        let held = after(
            &PeerMemberships::default(),
            &["east", "west"],
            &[("east", Some("Swim"))],
        );
        assert_eq!(held.mismatched().get("east").map(String::as_str), Some("Swim"));
        let condition = held
            .condition(&[], "Local", "t", None)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(condition.reason, PEER_MEMBERSHIP_MISMATCH);
        assert!(condition.message.contains("east (Swim)"), "{}", condition.message);
        let cleared = after(&held, &["east"], &[("east", Some("Local"))]);
        assert!(cleared.mismatched().is_empty());
        let matched = cleared
            .condition(&[], "Local", "t", None)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(matched.reason, PEER_MEMBERSHIP_MATCHES);
    }

    #[test]
    fn silence_and_an_older_release_keep_a_peers_standing() {
        let held = after(&PeerMemberships::default(), &["east"], &[("east", Some("Swim"))]);
        assert_eq!(after(&held, &["east"], &[]), held, "no answer");
        assert_eq!(after(&held, &["east"], &[("east", None)]), held, "no mode sent");
        assert!(after(&held, &[], &[]).mismatched().is_empty(), "no longer polled");
    }

    #[test]
    fn an_unchanged_condition_is_not_rewritten() {
        let held = after(&PeerMemberships::default(), &["east"], &[("east", Some("Swim"))]);
        let written = held
            .condition(&[], "Local", "t", Some(1))
            .unwrap_or_else(|| std::process::abort());
        assert!(held.condition(&[written], "Local", "later", Some(1)).is_none());
    }
}
