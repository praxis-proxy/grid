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
}

impl Outcome {
    /// Every value, in label order.
    #[cfg(test)]
    pub(crate) const ALL: [Self; 6] = [
        Self::Routed,
        Self::Fallback,
        Self::NotReady,
        Self::NoRoute,
        Self::BadRequest,
        Self::Shed,
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
}

impl Refused {
    /// The label this refusal counts under.
    pub(crate) fn outcome(self) -> Outcome {
        match self {
            Self::NotReady => Outcome::NotReady,
            Self::NoRoute => Outcome::NoRoute,
            Self::UnknownModel | Self::NoModel => Outcome::BadRequest,
            Self::Shed => Outcome::Shed,
        }
    }

    /// Count one refused request, under site "".
    pub(crate) fn record(self) {
        // Built on first use, after the gateway installs the recorder.
        static REFUSED: LazyLock<[Counter; 4]> = LazyLock::new(|| {
            [Outcome::NotReady, Outcome::NoRoute, Outcome::BadRequest, Outcome::Shed]
                .map(|outcome| metrics::counter!(METRIC, "site" => "", "cluster" => "", "reason" => outcome.as_str()))
        });
        let index = match self.outcome() {
            Outcome::NotReady => 0,
            Outcome::NoRoute => 1,
            Outcome::BadRequest | Outcome::Routed | Outcome::Fallback => 2,
            Outcome::Shed => 3,
        };
        if let Some(counter) = REFUSED.get(index) {
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

    /// Count one request sent to this site, as a fallback when no healthy site was left.
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
    fn the_reason_label_is_a_closed_set() {
        let labels = Outcome::ALL.map(Outcome::as_str);
        assert_eq!(
            labels,
            ["routed", "fallback", "not_ready", "no_route", "bad_request", "shed"],
            "adding a reason is a deliberate change: update the chart README metrics section"
        );
    }
}
