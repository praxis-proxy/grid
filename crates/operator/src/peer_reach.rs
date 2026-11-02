//! Per-peer conditions under Local membership. Each peer's `GridSite` carries this site's
//! view of it: whether its signals endpoint answered (`SignalsReachable`) and whether its
//! catalog was accepted (`CatalogAccepted`), each False reason naming the cause. The
//! `GridNetwork` carries only the counts (`PeerSignalsReachable`, `PeerCatalogsAccepted`).

use std::collections::HashMap;

use serde_json::Value;

use crate::{
    crd::{
        grid_network::GridNetwork,
        grid_site::GridSite,
        inference_provider::{Condition, ConditionStatus},
    },
    signals::Unreachable,
    site_providers::Verdict,
};

/// The condition's type.
pub const PEER_SIGNALS_REACHABLE: &str = "PeerSignalsReachable";

/// Reason while any polled peer's signals endpoint did not answer.
pub const PEERS_UNREACHABLE: &str = "PeersUnreachable";

/// Server-side apply manager, apart from the other `GridNetwork` and `GridSite` writers.
pub const FIELD_MANAGER: &str = "grid-operator-peer-reach";

/// The catalogs condition's type.
pub const PEER_CATALOGS_ACCEPTED: &str = "PeerCatalogsAccepted";

/// Reason while any peer's catalog is refused.
pub const CATALOGS_REFUSED: &str = "CatalogsRefused";

/// A peer `GridSite`'s signals condition type.
pub const SIGNALS_REACHABLE: &str = "SignalsReachable";

/// A peer `GridSite`'s catalog condition type.
pub const CATALOG_ACCEPTED: &str = "CatalogAccepted";

/// Reason for a peer with no signals endpoint to poll: it sends traffic out but this site
/// reads nothing from it, which is a fact about the peer, not a failure.
pub const EGRESS_ONLY: &str = "EgressOnly";

/// Reason for a peer not yet checked.
pub const AWAITING_FIRST_CHECK: &str = "AwaitingFirstCheck";

/// The `GridNetwork` condition saying whether each poll round had time for every peer.
pub const PEER_POLLS_COMPLETE: &str = "PeerPollsComplete";

/// Its False reason: this site's round ran out before it polled some peers.
pub const POLL_ROUND_OVERRUN: &str = "PollRoundOverrun";

/// Its server-side apply manager.
pub const POLLS_FIELD_MANAGER: &str = "grid-operator-peer-polls";

/// Rounds in a row a reachable peer goes unanswered before it reads unreachable.
pub const MISSES_BEFORE_UNREACHABLE: u32 = 3;

/// Rounds in a row each polled peer went unanswered, not counting a round that had no time
/// left to poll it.
#[derive(Debug, Default)]
pub struct Misses(HashMap<String, u32>);

impl Misses {
    /// Record a round over `polled`; a peer not in it is no longer counted.
    pub fn record<Answered: Fn(&str) -> bool, Excused: Fn(&str) -> bool>(
        &mut self,
        polled: &[String],
        answered: Answered,
        excused: Excused,
    ) {
        let counted = polled
            .iter()
            .map(|peer| {
                let before = self.of(peer);
                let misses = if answered(peer) {
                    0
                } else if excused(peer) {
                    before
                } else {
                    before.saturating_add(1)
                };
                (peer.clone(), misses)
            })
            .collect();
        self.0 = counted;
    }

    /// Rounds in a row `peer` went unanswered.
    #[must_use]
    pub fn of(&self, peer: &str) -> u32 {
        self.0.get(peer).copied().unwrap_or(0)
    }

    /// Whether an unanswered `peer` gets the full poll budget this round, every
    /// [`MISSES_BEFORE_UNREACHABLE`] rounds, so a peer slower than the short budget recovers.
    #[must_use]
    pub fn full_budget_due(&self, peer: &str) -> bool {
        self.of(peer).is_multiple_of(MISSES_BEFORE_UNREACHABLE)
    }
}

/// What one condition is called and says, on the `GridNetwork` and on each peer's `GridSite`.
#[derive(Debug, Eq, PartialEq)]
pub struct Kind {
    /// The `GridNetwork` condition's type, and its True reason.
    pub type_: &'static str,
    /// The False reason, whatever the count.
    pub reason: &'static str,
    /// What the True `GridNetwork` condition says.
    pub clear: &'static str,
    /// What the False `GridNetwork` condition counts.
    pub counted: &'static str,
    /// The peer `GridSite` condition's type, and its True reason.
    pub site_type: &'static str,
    /// What the True `GridSite` condition says.
    pub site_clear: &'static str,
    /// Its server-side apply manager, on both kinds.
    pub field_manager: &'static str,
}

/// Peers' signals endpoints that did not answer.
pub const REACH: Kind = Kind {
    type_: PEER_SIGNALS_REACHABLE,
    reason: PEERS_UNREACHABLE,
    clear: "every polled peer's signals endpoint answered",
    counted: "peers unreachable",
    site_type: SIGNALS_REACHABLE,
    site_clear: "its signals endpoint answered",
    field_manager: FIELD_MANAGER,
};

/// Peers' catalogs refused for crowding the overlay or naming clusters they do not own.
pub const CATALOGS: Kind = Kind {
    type_: PEER_CATALOGS_ACCEPTED,
    reason: CATALOGS_REFUSED,
    clear: "every peer catalog read is accepted",
    counted: "peers' catalogs refused",
    site_type: CATALOG_ACCEPTED,
    site_clear: "its catalog is accepted",
    field_manager: "grid-operator-peer-catalogs",
};

/// What a peer's signals endpoint is told about when it does not answer.
const NO_ENDPOINT: &str = "no signals endpoint, so it is not polled: set spec.signalsEndpoint, or a dialable spec.gatewayEndpoint, to poll it";

/// What one peer was judged this round.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Judged {
    /// In the clear.
    Clear,
    /// In the clear, holding or crowding what another peer names: the detail.
    Noted(String),
    /// Not in the clear: the cause as a reason, and the detail.
    Failing(&'static str, String),
    /// Declares no signals endpoint, so it is not polled; counted neither way.
    EgressOnly,
    /// This site's round ran out before polling it: it keeps its last verdict.
    Skipped,
    /// Not yet checked.
    Awaiting,
}

impl Judged {
    /// Whether the peer is in the clear, with or without a note.
    #[must_use]
    pub const fn is_clear(&self) -> bool {
        matches!(self, Self::Clear | Self::Noted(_))
    }
}

/// Each peer a condition judged, as of the last round.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerReach {
    /// What the condition is.
    kind: &'static Kind,
    /// Peer to what it was judged.
    peers: HashMap<String, Judged>,
}

impl Default for PeerReach {
    fn default() -> Self {
        Self::none(&REACH)
    }
}

impl PeerReach {
    /// No peer judged, which removes `kind`'s condition from every `GridSite`.
    #[must_use]
    pub fn none(kind: &'static Kind) -> Self {
        Self {
            kind,
            peers: HashMap::new(),
        }
    }

    /// One round: the `answered` peers answered, the `unanswered` ones did not, with why and
    /// where when a failure was recorded, and the `without_endpoint` ones could not be polled.
    #[must_use]
    pub fn of<'peer, Answered, Unanswered, WithoutEndpoint>(
        answered: Answered,
        unanswered: Unanswered,
        without_endpoint: WithoutEndpoint,
    ) -> Self
    where
        Answered: IntoIterator<Item = &'peer str>,
        Unanswered: IntoIterator<Item = (&'peer str, Option<(Unreachable, &'peer str)>)>,
        WithoutEndpoint: IntoIterator<Item = &'peer str>,
    {
        let mut peers: HashMap<String, Judged> = answered
            .into_iter()
            .map(|peer| (peer.to_owned(), Judged::Clear))
            .collect();
        for (peer, failure) in unanswered {
            let judged = match failure {
                None => Judged::Awaiting,
                Some((Unreachable::RoundDeadline, _)) => Judged::Skipped,
                Some((why, url)) => Judged::Failing(why.reason(), format!("{url}: {}", why.describe())),
            };
            peers.insert(peer.to_owned(), judged);
        }
        for peer in without_endpoint {
            peers.insert(peer.to_owned(), Judged::EgressOnly);
        }
        Self { kind: &REACH, peers }
    }

    /// This round's verdicts, with a peer `previous` judged reachable kept so until it has gone
    /// [`MISSES_BEFORE_UNREACHABLE`] rounds unanswered. A peer the round had no time for keeps
    /// whatever `previous` said of it, else waits for its first check. A peer that lost its
    /// endpoint reads so at once.
    #[must_use]
    pub fn held_over(mut self, previous: &Self, misses: &Misses) -> Self {
        self.peers.iter_mut().for_each(|(peer, judged)| {
            let before = previous.peers.get(peer);
            if *judged == Judged::Skipped {
                *judged = before
                    .filter(|before| **before != Judged::Skipped)
                    .cloned()
                    .unwrap_or(Judged::Awaiting);
                return;
            }
            let recent = misses.of(peer) < MISSES_BEFORE_UNREACHABLE;
            let excused = match judged {
                Judged::Failing(..) | Judged::Awaiting => recent,
                Judged::Clear | Judged::Noted(_) | Judged::EgressOnly | Judged::Skipped => false,
            };
            if excused && before == Some(&Judged::Clear) {
                *judged = Judged::Clear;
            }
        });
        self
    }

    /// How many peers this round had no time to poll, of how many it judged.
    #[must_use]
    pub fn skipped(&self) -> usize {
        self.peers.values().filter(|judged| **judged == Judged::Skipped).count()
    }

    /// The catalogs of the `held` peers, as `verdicts` judged them in the last admission:
    /// refused with its cause, accepted with a note on what it holds, waits or crowds, or clear.
    #[must_use]
    pub fn catalogs<'peer, Held>(held: Held, verdicts: &HashMap<String, Verdict>) -> Self
    where
        Held: IntoIterator<Item = &'peer str>,
    {
        let peers = held
            .into_iter()
            .map(|peer| {
                let judged = match verdicts.get(peer) {
                    None => Judged::Clear,
                    Some(Verdict::Noted(note)) => Judged::Noted(note.clone()),
                    Some(Verdict::Refused { reason, message }) => Judged::Failing(reason, message.clone()),
                };
                (peer.to_owned(), judged)
            })
            .collect();
        Self { kind: &CATALOGS, peers }
    }

    /// What the condition is.
    #[must_use]
    pub const fn kind(&self) -> &'static Kind {
        self.kind
    }

    /// Each peer and what it was judged.
    pub fn judged(&self) -> impl Iterator<Item = (&str, &Judged)> {
        self.peers.iter().map(|(peer, judged)| (peer.as_str(), judged))
    }

    /// The `GridNetwork` condition to write over `current`, `None` when it already says this.
    #[must_use]
    pub fn condition(&self, current: &[Condition], now_rfc3339: &str, generation: Option<i64>) -> Option<Condition> {
        let failing = self
            .peers
            .values()
            .filter(|judged| matches!(judged, Judged::Failing(..)))
            .count();
        let checked = self
            .peers
            .values()
            .filter(|judged| !matches!(judged, Judged::Awaiting | Judged::EgressOnly | Judged::Skipped))
            .count();
        let (status, reason, message) = if failing == 0 {
            (ConditionStatus::True, self.kind.type_, self.kind.clear.to_owned())
        } else {
            (
                ConditionStatus::False,
                self.kind.reason,
                format!("{failing} of {checked} {}", self.kind.counted),
            )
        };
        stamp(
            current,
            (self.kind.type_, status, reason, message),
            now_rfc3339,
            generation,
        )
    }

    /// The condition `site` should carry: `None` when this round did not judge it.
    fn site_condition(&self, site: &str) -> Option<(ConditionStatus, &'static str, String)> {
        Some(match self.peers.get(site)? {
            Judged::Clear => (
                ConditionStatus::True,
                self.kind.site_type,
                self.kind.site_clear.to_owned(),
            ),
            Judged::Noted(note) => (
                ConditionStatus::True,
                self.kind.site_type,
                format!("{}; {note}", self.kind.site_clear),
            ),
            Judged::Failing(reason, message) => (ConditionStatus::False, *reason, message.clone()),
            Judged::EgressOnly => (ConditionStatus::False, EGRESS_ONLY, NO_ENDPOINT.to_owned()),
            Judged::Awaiting | Judged::Skipped => (
                ConditionStatus::Unknown,
                AWAITING_FIRST_CHECK,
                "not yet answered or failed a poll".to_owned(),
            ),
        })
    }

    /// The status apply writing `site`'s condition, or removing it when this round did not
    /// judge the peer, with the name to apply it to. `None` when the live status already says
    /// this.
    #[must_use]
    pub fn site_patch<'site>(&self, site: &'site GridSite, now_rfc3339: &str) -> Option<(&'site str, Value)> {
        let name = site.metadata.name.as_deref()?;
        let current = site
            .status
            .as_ref()
            .map_or(&[][..], |status| status.conditions.as_slice());
        let conditions = match self.site_condition(name) {
            Some((status, reason, message)) => vec![stamp(
                current,
                (self.kind.site_type, status, reason, message),
                now_rfc3339,
                site.metadata.generation,
            )?],
            None if current.iter().any(|held| held.type_ == self.kind.site_type) => Vec::new(),
            None => return None,
        };
        Some((
            name,
            serde_json::json!({
                "apiVersion": "grid.praxis.fast/v1alpha1",
                "kind": "GridSite",
                "metadata": { "name": name },
                "status": { "conditions": conditions }
            }),
        ))
    }
}

/// `type_` as `status`, `reason` and `message` over `current`, `None` when it already says this.
fn stamp(
    current: &[Condition],
    (type_, status, reason, message): (&str, ConditionStatus, &str, String),
    now_rfc3339: &str,
    generation: Option<i64>,
) -> Option<Condition> {
    let held = current.iter().find(|held| held.type_ == type_);
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
        type_: type_.to_owned(),
        status,
        reason: reason.to_owned(),
        message,
        last_transition_time,
        observed_generation: generation,
    })
}

/// The status patch carrying `network`'s condition for `reach`, with the name to
/// apply it to, or `None` when the live status already says this.
#[must_use]
pub fn patch<'network>(
    network: &'network GridNetwork,
    reach: &PeerReach,
    now_rfc3339: &str,
) -> Option<(&'network str, Value)> {
    network_patch(network, |current| {
        reach.condition(current, now_rfc3339, network.metadata.generation)
    })
}

/// `PeerPollsComplete` after a round that had no time for `skipped` of its `judged` peers.
#[must_use]
pub fn polls_condition(
    current: &[Condition],
    (skipped, judged): (usize, usize),
    now_rfc3339: &str,
) -> Option<Condition> {
    let (status, reason, message) = if skipped == 0 {
        (
            ConditionStatus::True,
            PEER_POLLS_COMPLETE,
            "each round polls every peer".to_owned(),
        )
    } else {
        (
            ConditionStatus::False,
            POLL_ROUND_OVERRUN,
            format!(
                "{skipped} of {judged} peers not polled within the round: lengthen the round or raise the dial \
                 concurrency"
            ),
        )
    };
    stamp(
        current,
        (PEER_POLLS_COMPLETE, status, reason, message),
        now_rfc3339,
        None,
    )
}

/// The status apply withdrawing `type_`, which the caller's manager owns alone, from
/// `network`, with its name; `None` when `network` holds none.
#[must_use]
pub fn withdrawal<'network>(network: &'network GridNetwork, type_: &str) -> Option<(&'network str, Value)> {
    let name = network.metadata.name.as_deref()?;
    network
        .status
        .as_ref()
        .is_some_and(|status| status.conditions.iter().any(|held| held.type_ == type_))
        .then(|| {
            (
                name,
                serde_json::json!({
                    "apiVersion": "grid.praxis.fast/v1alpha1",
                    "kind": "GridNetwork",
                    "metadata": { "name": name },
                    "status": { "conditions": [] }
                }),
            )
        })
}

/// The status apply on `network` carrying the condition `build` makes from its live ones, with
/// its name; `None` when there is none to write.
pub fn network_patch<Build>(network: &GridNetwork, build: Build) -> Option<(&str, Value)>
where
    Build: FnOnce(&[Condition]) -> Option<Condition>,
{
    let name = network.metadata.name.as_deref()?;
    let current = network
        .status
        .as_ref()
        .map_or(&[][..], |status| status.conditions.as_slice());
    let condition = build(current)?;
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
    use std::collections::HashMap;

    use super::{
        AWAITING_FIRST_CHECK, CATALOG_ACCEPTED, CATALOGS, CATALOGS_REFUSED, ConditionStatus, EGRESS_ONLY,
        MISSES_BEFORE_UNREACHABLE, Misses, NO_ENDPOINT, PEER_CATALOGS_ACCEPTED, PEER_POLLS_COMPLETE,
        PEER_SIGNALS_REACHABLE, PEERS_UNREACHABLE, POLL_ROUND_OVERRUN, PeerReach, REACH, SIGNALS_REACHABLE,
        polls_condition,
    };
    use crate::{
        crd::{
            grid_site::{GridSite, GridSiteStatus},
            inference_provider::Condition,
        },
        signals::Unreachable,
        site_providers::Verdict,
    };

    fn condition(reach: &PeerReach) -> Condition {
        reach
            .condition(&[], "t", Some(1))
            .unwrap_or_else(|| std::process::abort())
    }

    fn site(name: &str, conditions: Vec<Condition>) -> GridSite {
        let mut site = GridSite::new(
            name,
            serde_json::from_value(serde_json::json!({ "gridNetworkRef": "net" }))
                .unwrap_or_else(|_| std::process::abort()),
        );
        site.metadata.generation = Some(3);
        site.status = Some(GridSiteStatus {
            conditions,
            ..GridSiteStatus::default()
        });
        site
    }

    /// The single condition a site patch applies, `None` for a removal.
    fn applied(reach: &PeerReach, site: &GridSite) -> Option<Condition> {
        let (_, patch) = reach.site_patch(site, "t").unwrap_or_else(|| std::process::abort());
        serde_json::from_value::<Vec<Condition>>(patch.pointer("/status/conditions").cloned().unwrap_or_default())
            .unwrap_or_else(|_| std::process::abort())
            .pop()
    }

    fn mixed() -> PeerReach {
        PeerReach::of(
            ["up"],
            [
                ("east", Some((Unreachable::NotFound, "https://east:9443"))),
                ("west", Some((Unreachable::Timeout, "https://west:9443"))),
                ("north", Some((Unreachable::Tls, "https://north:9443"))),
                ("fresh", None),
            ],
            ["south"],
        )
    }

    #[test]
    fn each_peer_site_names_its_cause_and_where() {
        let reach = mixed();
        for (peer, status, reason, said) in [
            ("up", "True", SIGNALS_REACHABLE, "answered"),
            ("east", "False", "NotFound", "https://east:9443: answered 404"),
            ("west", "False", "Timeout", "https://west:9443: timed out"),
            ("north", "False", "TLSRefused", "TLS"),
            ("south", "False", EGRESS_ONLY, NO_ENDPOINT),
            ("fresh", "Unknown", AWAITING_FIRST_CHECK, "not yet"),
        ] {
            let written = applied(&reach, &site(peer, Vec::new())).unwrap_or_else(|| std::process::abort());
            assert_eq!(
                (written.type_.as_str(), written.status.as_str(), written.reason.as_str()),
                (SIGNALS_REACHABLE, status, reason),
                "{peer}"
            );
            assert!(written.message.contains(said), "{peer}: {}", written.message);
            assert_eq!(written.observed_generation, Some(3), "stamped from the GridSite");
        }
    }

    #[test]
    fn the_network_counts_and_names_no_peer() {
        let written = condition(&mixed());
        assert_eq!(
            (
                written.status.as_str(),
                written.reason.as_str(),
                written.message.as_str()
            ),
            ("False", PEERS_UNREACHABLE, "3 of 4 peers unreachable"),
            "neither the peer not yet checked nor the egress-only one is counted"
        );
        let none = PeerReach::of(
            [],
            [("east", Some((Unreachable::Refused, "https://east:9443")))],
            ["south"],
        );
        assert_eq!(
            (condition(&none).reason.as_str(), condition(&none).message.as_str()),
            (PEERS_UNREACHABLE, "1 of 1 peers unreachable")
        );
    }

    #[test]
    fn an_egress_only_peer_is_named_on_its_grid_site_and_counted_neither_way() {
        let reach = PeerReach::of(["up"], [], ["egress"]);
        let written = condition(&reach);
        assert_eq!(
            (written.status.as_str(), written.reason.as_str()),
            ("True", PEER_SIGNALS_REACHABLE),
            "one egress-only member does not degrade every other site"
        );
        let site = applied(&reach, &site("egress", Vec::new())).unwrap_or_else(|| std::process::abort());
        assert_eq!((site.status.as_str(), site.reason.as_str()), ("False", EGRESS_ONLY));
    }

    #[test]
    fn this_site_is_never_judged_as_its_own_peer_when_its_grid_site_declares_no_endpoint() {
        let identities = crate::signals::PeerIdentities::new();
        identities.set(
            ["hub", "east"]
                .map(|site| (site.to_owned(), crate::signals::PeerRecord::default()))
                .into(),
        );
        let without = identities.without_endpoint("hub");
        assert_eq!(without, ["east"], "the own record is not a peer's");
        let reach = PeerReach::of(["west"], [], without.iter().map(String::as_str));
        assert!(
            reach.site_patch(&site("hub", Vec::new()), "t").is_none(),
            "the own GridSite carries no peer-reach condition"
        );
        assert_eq!(condition(&reach).status, ConditionStatus::True, "and is not counted");
    }

    /// Each peer's reason after one round over `peers`, `answered` the ones that answered.
    fn round(
        previous: &PeerReach,
        misses: &mut Misses,
        unanswered: &[(&str, Unreachable)],
        answered: &[&str],
    ) -> PeerReach {
        let polled: Vec<String> = answered
            .iter()
            .copied()
            .chain(unanswered.iter().map(|(peer, _)| *peer))
            .map(str::to_owned)
            .collect();
        let deadline = |peer: &str| unanswered.contains(&(peer, Unreachable::RoundDeadline));
        misses.record(&polled, |peer| answered.contains(&peer), deadline);
        PeerReach::of(
            answered.iter().copied(),
            unanswered
                .iter()
                .map(|(peer, why)| (*peer, Some((*why, "https://east:9443")))),
            [],
        )
        .held_over(previous, misses)
    }

    fn reason_of(reach: &PeerReach, peer: &str) -> String {
        applied(reach, &site(peer, Vec::new()))
            .unwrap_or_else(|| std::process::abort())
            .reason
    }

    #[test]
    fn a_reachable_peer_reads_unreachable_only_after_three_misses_in_a_row() {
        let mut misses = Misses::default();
        let mut reach = round(&PeerReach::default(), &mut misses, &[], &["east"]);
        for miss in 1..MISSES_BEFORE_UNREACHABLE {
            reach = round(&reach, &mut misses, &[("east", Unreachable::Timeout)], &[]);
            assert_eq!(
                reason_of(&reach, "east"),
                SIGNALS_REACHABLE,
                "miss {miss} changes nothing"
            );
        }
        reach = round(&reach, &mut misses, &[("east", Unreachable::Timeout)], &[]);
        assert_eq!(reason_of(&reach, "east"), "Timeout", "the third miss does");
        reach = round(&reach, &mut misses, &[], &["east"]);
        assert_eq!(reason_of(&reach, "east"), SIGNALS_REACHABLE, "one answer clears it");
    }

    #[test]
    fn a_peer_the_round_had_no_time_for_keeps_its_verdict_and_the_overrun_is_this_sites() {
        let mut misses = Misses::default();
        let mut reach = round(&PeerReach::default(), &mut misses, &[], &["east"]);
        for _ in 0..MISSES_BEFORE_UNREACHABLE * 2 {
            reach = round(&reach, &mut misses, &[("east", Unreachable::RoundDeadline)], &[]);
        }
        assert_eq!(
            reason_of(&reach, "east"),
            SIGNALS_REACHABLE,
            "a reachable peer stays so"
        );
        assert_eq!(misses.of("east"), 0, "and the skip is not a miss");
        let mut failing = Misses::default();
        let mut west = PeerReach::default();
        for _ in 0..MISSES_BEFORE_UNREACHABLE {
            west = round(&west, &mut failing, &[("west", Unreachable::Timeout)], &[]);
        }
        west = round(&west, &mut failing, &[("west", Unreachable::RoundDeadline)], &[]);
        assert_eq!(
            reason_of(&west, "west"),
            "Timeout",
            "a failing peer keeps its own cause"
        );
        let never = round(
            &PeerReach::default(),
            &mut Misses::default(),
            &[("north", Unreachable::RoundDeadline)],
            &[],
        );
        assert_eq!(
            reason_of(&never, "north"),
            AWAITING_FIRST_CHECK,
            "a peer never reached waits for its first check, and is never blamed for this site's round"
        );
    }

    #[test]
    fn a_withdrawal_empties_only_a_network_that_holds_the_condition() {
        let mut network: crate::crd::grid_network::GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "grid" },
            "spec": {},
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            super::withdrawal(&network, PEER_SIGNALS_REACHABLE).is_none(),
            "nothing to withdraw"
        );
        network.status = Some(crate::crd::grid_network::GridNetworkStatus {
            conditions: vec![condition(&PeerReach::of(["up"], [], []))],
            ..Default::default()
        });
        let (name, patch) =
            super::withdrawal(&network, PEER_SIGNALS_REACHABLE).unwrap_or_else(|| std::process::abort());
        assert_eq!(name, "grid");
        assert_eq!(patch.pointer("/status/conditions"), Some(&serde_json::json!([])));
    }

    #[test]
    fn an_overrun_round_reads_on_this_sites_grid_network_with_the_count() {
        let skipped = PeerReach::of([], [("north", Some((Unreachable::RoundDeadline, "")))], []).skipped();
        let overrun = polls_condition(&[], (skipped, 4), "t").unwrap_or_else(|| std::process::abort());
        assert_eq!(
            (
                overrun.status.as_str(),
                overrun.reason.as_str(),
                overrun.message.as_str()
            ),
            (
                "False",
                POLL_ROUND_OVERRUN,
                "1 of 4 peers not polled within the round: lengthen the round or raise the dial concurrency"
            )
        );
        let complete = polls_condition(&[], (0, 4), "t").unwrap_or_else(|| std::process::abort());
        assert_eq!(
            (complete.status.as_str(), complete.reason.as_str()),
            ("True", PEER_POLLS_COMPLETE)
        );
    }

    #[test]
    fn a_peer_slower_than_the_short_budget_gets_the_full_one_within_three_misses() {
        let mut misses = Misses::default();
        let polled = ["slow".to_owned()];
        let mut full = Vec::new();
        for _ in 0..MISSES_BEFORE_UNREACHABLE * 2 {
            misses.record(&polled, |_| false, |_| false);
            full.push(misses.full_budget_due("slow"));
        }
        assert_eq!(
            full,
            [false, false, true, false, false, true],
            "every third miss in a row polls with the full budget"
        );
    }

    #[test]
    fn every_peer_answering_is_reachable_and_unchanged_is_not_rewritten() {
        let reach = PeerReach::of(["a", "b", "c"], [], []);
        let written = condition(&reach);
        assert_eq!(
            (written.status.as_str(), written.reason.as_str()),
            ("True", PEER_SIGNALS_REACHABLE),
            "True names the type"
        );
        assert!(reach.condition(&[written], "later", Some(1)).is_none());
        let up = site("a", Vec::new());
        let held = applied(&reach, &up).unwrap_or_else(|| std::process::abort());
        assert!(reach.site_patch(&site("a", vec![held]), "later").is_none());
    }

    #[test]
    fn a_site_no_longer_judged_loses_the_condition_and_one_never_holding_it_is_not_written() {
        let held = applied(&mixed(), &site("east", Vec::new())).unwrap_or_else(|| std::process::abort());
        let gone = PeerReach::none(&REACH);
        assert_eq!(
            applied(&gone, &site("east", vec![held.clone()])),
            None,
            "an empty apply removes it"
        );
        let other = Condition {
            type_: "Connected".to_owned(),
            ..held
        };
        assert!(
            gone.site_patch(&site("east", vec![other]), "t").is_none(),
            "another type is not ours"
        );
    }

    /// West refused for naming this site's cluster, east accepted and told it leaves west short.
    fn crowded_verdicts() -> HashMap<String, Verdict> {
        HashMap::from([
            (
                "west".to_owned(),
                Verdict::Refused {
                    reason: "OwnCluster",
                    message: "names cluster local-vllm, this site's own".to_owned(),
                },
            ),
            (
                "east".to_owned(),
                Verdict::Noted(
                    "takes 128 candidates over its share of 512 for 2 peers, leaving peer west short".to_owned(),
                ),
            ),
        ])
    }

    #[test]
    fn each_refused_catalog_names_its_cause_on_its_site() {
        let verdicts = crowded_verdicts();
        let catalogs = PeerReach::catalogs(["east", "west", "north"], &verdicts);
        let west = applied(&catalogs, &site("west", Vec::new())).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            (west.type_.as_str(), west.status.as_str(), west.reason.as_str()),
            (CATALOG_ACCEPTED, "False", "OwnCluster")
        );
        assert!(west.message.contains("names cluster local-vllm"), "{}", west.message);
        let east = applied(&catalogs, &site("east", Vec::new())).unwrap_or_else(|| std::process::abort());
        assert_eq!((east.status.as_str(), east.reason.as_str()), ("True", CATALOG_ACCEPTED));
        assert!(
            east.message.contains("leaving peer west short"),
            "the crowding peer is accepted and named: {}",
            east.message
        );
        let north = applied(&catalogs, &site("north", Vec::new())).unwrap_or_else(|| std::process::abort());
        assert_eq!(
            north.message, CATALOGS.site_clear,
            "a peer in nobody's way says only that"
        );
    }

    #[test]
    fn the_network_counts_refused_catalogs() {
        let refused = HashMap::from([(
            "west".to_owned(),
            Verdict::Refused {
                reason: "ClusterClaimed",
                message: "names cluster east, which site east holds by name".to_owned(),
            },
        )]);
        let network = condition(&PeerReach::catalogs(["east", "west"], &refused));
        assert_eq!(
            (
                network.type_.as_str(),
                network.reason.as_str(),
                network.message.as_str()
            ),
            (
                PEER_CATALOGS_ACCEPTED,
                CATALOGS_REFUSED,
                "1 of 2 peers' catalogs refused"
            )
        );
        let clear = condition(&PeerReach::catalogs(["east"], &HashMap::new()));
        assert_eq!(
            (clear.status.as_str(), clear.reason.as_str()),
            ("True", PEER_CATALOGS_ACCEPTED)
        );
        assert_eq!(PeerReach::none(&CATALOGS).kind().site_type, CATALOG_ACCEPTED);
    }

    #[test]
    fn a_404_is_told_apart_from_a_timeout_and_a_tls_failure() {
        use grid_client::RequestOutcome;
        assert_eq!(
            Unreachable::of(RequestOutcome::ClientError, Some(404)),
            Unreachable::NotFound
        );
        assert_eq!(Unreachable::of(RequestOutcome::Timeout, None), Unreachable::Timeout);
        assert_eq!(
            Unreachable::of(RequestOutcome::Deadline, None),
            Unreachable::RoundDeadline
        );
        assert_eq!(Unreachable::of(RequestOutcome::Tls, None), Unreachable::Tls);
        assert_eq!(Unreachable::of(RequestOutcome::Refused, None), Unreachable::Refused);
        assert_eq!(
            Unreachable::of(RequestOutcome::ClientError, Some(403)),
            Unreachable::Failed(RequestOutcome::ClientError)
        );
        assert_eq!(Unreachable::Refused.reason(), "ConnectionRefused");
        assert_eq!(
            Unreachable::Failed(RequestOutcome::ClientError).reason(),
            "RequestFailed"
        );
    }
}
