//! `grid_route_decisions_total`: where `grid_site_route` sent a request, site and cluster, or why it refused.
//!
//! Counter handles are registered off the request path, so recording is one atomic add.

use std::sync::{Arc, LazyLock};

use metrics::Counter;

/// The counter's name.
const METRIC: &str = "grid_route_decisions_total";

/// The `reason` label: a closed set, so the series count is fixed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Sent to a healthy site. How it ranked is in `grid_route_site_score`.
    Routed,
    /// Sent to a demoted site because no healthy one was left.
    Fallback,
    /// Every candidate was excluded. Answered 503.
    NotReady,
    /// An admitted candidate had no route from this gateway. Answered 503.
    NoRoute,
    /// The request named no model, or one no candidate serves. Answered 400 or 404.
    BadRequest,
    /// Every healthy site serving the model was past full. Answered 503.
    Shed,
    /// Every site serving the model lies outside the request's sovereignty zones. Answered 403.
    Fenced,
}

impl Outcome {
    /// Every value, in label order.
    #[cfg(test)]
    pub(crate) const ALL: [Self; 7] = [
        Self::Routed,
        Self::Fallback,
        Self::NotReady,
        Self::NoRoute,
        Self::BadRequest,
        Self::Shed,
        Self::Fenced,
    ];

    /// The `reason` label value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Routed => "routed",
            Self::Fallback => "fallback",
            Self::NotReady => "not_ready",
            Self::NoRoute => "no_route",
            Self::BadRequest => "bad_request",
            Self::Shed => "shed",
            Self::Fenced => "fenced",
        }
    }
}

/// Why a request went nowhere. It picks the status, and its [`Outcome`] is the label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refused {
    /// Every match was excluded, by readiness or admission.
    NotReady,
    /// An admitted match existed but had no route from this gateway.
    NoRoute,
    /// No candidate serves the model.
    UnknownModel,
    /// The request named no model.
    NoModel,
    /// Every healthy site serving the model was past full.
    Shed,
    /// A fence is configured and the request named no valid sovereignty zone.
    ZoneRequired,
    /// No site serving the model lies in the request's zones, or another filter chose the cluster.
    Fenced,
}

impl Refused {
    /// The label this refusal counts under.
    pub(crate) fn outcome(self) -> Outcome {
        match self {
            Self::NotReady => Outcome::NotReady,
            Self::NoRoute => Outcome::NoRoute,
            Self::UnknownModel | Self::NoModel | Self::ZoneRequired => Outcome::BadRequest,
            Self::Shed => Outcome::Shed,
            Self::Fenced => Outcome::Fenced,
        }
    }

    /// Count one refused request, under site "".
    pub(crate) fn record(self) {
        // Built on first use, after the gateway installs the recorder.
        static REFUSED: LazyLock<[Counter; 5]> = LazyLock::new(|| {
            [
                Outcome::NotReady,
                Outcome::NoRoute,
                Outcome::BadRequest,
                Outcome::Shed,
                Outcome::Fenced,
            ]
            .map(|outcome| metrics::counter!(METRIC, "site" => "", "cluster" => "", "reason" => outcome.as_str()))
        });
        let index = match self.outcome() {
            Outcome::NotReady => 0,
            Outcome::NoRoute => 1,
            Outcome::BadRequest | Outcome::Routed | Outcome::Fallback => 2,
            Outcome::Shed => 3,
            Outcome::Fenced => 4,
        };
        if let Some(counter) = REFUSED.get(index) {
            counter.increment(1);
        }
    }
}

/// The counter of requests the sovereignty fence refused, or would have when it only logs.
const FENCED_METRIC: &str = "grid_route_fenced_total";

/// Why the fence refused a request, the `cause` label: a closed set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FenceCause {
    /// The request named no valid zone in one header line.
    ZoneRequired,
    /// No site in the request's zones serves it, or the pick lay outside them.
    OutsideZones,
    /// Another filter chose the cluster, which the fence cannot check.
    EarlierRoute,
}

impl FenceCause {
    /// Every value, in label order.
    const ALL: [Self; 3] = [Self::ZoneRequired, Self::OutsideZones, Self::EarlierRoute];

    /// The `cause` label value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ZoneRequired => "zone_required",
            Self::OutsideZones => "outside_zones",
            Self::EarlierRoute => "earlier_route",
        }
    }

    /// Count one request, `enforced` when it was refused rather than only logged.
    pub(crate) fn record(self, enforced: bool) {
        // Built on first use, after the gateway installs the recorder.
        static FENCED: LazyLock<Vec<Counter>> = LazyLock::new(|| {
            FenceCause::ALL
                .iter()
                .flat_map(|cause| {
                    ["true", "false"].map(
                        |enforced| metrics::counter!(FENCED_METRIC, "cause" => cause.as_str(), "enforced" => enforced),
                    )
                })
                .collect()
        });
        let index = match (self, enforced) {
            (Self::ZoneRequired, true) => 0,
            (Self::ZoneRequired, false) => 1,
            (Self::OutsideZones, true) => 2,
            (Self::OutsideZones, false) => 3,
            (Self::EarlierRoute, true) => 4,
            (Self::EarlierRoute, false) => 5,
        };
        if let Some(counter) = FENCED.get(index) {
            counter.increment(1);
        }
    }
}

/// One site's counters: routed, then fallback.
#[derive(Debug)]
pub struct SiteDecisions([Counter; 2]);

impl SiteDecisions {
    /// Register the counters for `site` and the `cluster` serving it, names from the serving config.
    pub(crate) fn new(site: &Arc<str>, cluster: &Arc<str>) -> Self {
        Self(
            [Outcome::Routed, Outcome::Fallback]
                .map(|outcome| metrics::counter!(METRIC, "site" => Arc::clone(site), "cluster" => Arc::clone(cluster), "reason" => outcome.as_str())),
        )
    }

    /// Count one request sent to this site, as a fallback when no healthy or live site was left.
    pub(crate) fn record(&self, fallback: bool) {
        if let Some(counter) = self.0.get(usize::from(fallback)) {
            counter.increment(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Outcome;

    #[test]
    fn the_fence_cause_label_is_a_closed_set() {
        assert_eq!(
            super::FenceCause::ALL.map(super::FenceCause::as_str),
            ["zone_required", "outside_zones", "earlier_route"],
            "adding a cause is a deliberate change: update the chart README metrics section"
        );
    }

    #[test]
    fn the_reason_label_is_a_closed_set() {
        let labels = Outcome::ALL.map(Outcome::as_str);
        assert_eq!(
            labels,
            [
                "routed",
                "fallback",
                "not_ready",
                "no_route",
                "bad_request",
                "shed",
                "fenced"
            ],
            "adding a reason is a deliberate change: update the chart README metrics section"
        );
    }
}
