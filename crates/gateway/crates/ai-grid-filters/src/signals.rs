//! What site selection reads about a site, however the series are named or sourced.
//!
//! Every input comes from an inference provider over the operator's signals endpoint, into
//! the store. The contract here names no series: each provider dialect is a submodule that
//! maps its own series onto a [`SiteReading`].

pub(crate) mod llm_d;

/// One site's inputs to site selection, read once per candidate per refresh.
///
/// A field is absent when the site publishes nothing for it. In-flight and held are counts
/// of requests, the queues are in the units `availability.queue_full` is set in, `sampled_at` is
/// milliseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SiteReading {
    /// Requests the site holds, running and waiting, worst within the window.
    pub in_flight: Option<f64>,
    /// Work waiting at the site as a whole, per serving unit, worst within the window.
    pub queued: Option<f64>,
    /// When the newest recognized queue sample arrived, even after it becomes stale.
    pub queue_sampled_at: Option<i64>,
    /// Work held before scheduling, a count for the whole site, worst within the window.
    pub held: Option<f64>,
    /// The most any one serving unit of the site has waiting, so a scale-out cannot hide a
    /// backlog behind a site-wide average.
    pub deepest_queue: Option<f64>,
    /// The most the site held at an instant when it also had a backlog (waiting at or past
    /// `queue_full` per unit, or anything held before scheduling), worst within the window.
    /// Absent when no instant in the window had one.
    pub congested_in_flight: Option<f64>,
    /// When the newest sample itself had a backlog. Unlike the windowed backlog fields, this
    /// advances only when another backlogged sample arrives, so refreshes cannot age one burst
    /// into a sustained backlog.
    pub backlogged_at: Option<i64>,
    /// Whether the site's last word on readiness was no, with nothing newer since.
    pub unready: bool,
    /// When the site's newest sample arrived, the epoch of a smoothing step.
    pub sampled_at: Option<i64>,
    /// Serving units the site last reported. Not a capacity: it says nothing about how much
    /// one unit carries, only that the site is a different size than it was.
    pub units: Option<f64>,
    /// The EPP's own saturation reading, worst in the window, uncapped; only with flow control on.
    /// After flow control turns off, the last published value can outrank the derived one for up to `window_ms`.
    pub published_saturation: Option<f64>,
    /// The gateway's approximation of that reading from pool averages, worst in the window.
    pub derived_saturation: Option<f64>,
    /// The comparator's reading at instants that also had a backlog, from the same source as
    /// [`Self::comparator`], worst within the window. Absent when no such instant had one.
    pub congested_saturation: Option<f64>,
}

impl SiteReading {
    /// How full the site is by one rule for every site: the EPP's own reading where published.
    #[must_use]
    pub fn comparator(&self) -> Option<f64> {
        self.published_saturation.or(self.derived_saturation)
    }

    /// Which reading `comparator` took.
    #[must_use]
    pub(crate) fn comparator_source(&self) -> Option<ComparatorSource> {
        match (self.published_saturation, self.derived_saturation) {
            (Some(_), _) => Some(ComparatorSource::Published),
            (None, Some(_)) => Some(ComparatorSource::Derived),
            (None, None) => None,
        }
    }
}

/// What a reading covers: the clock, how old a site's newest sample may be, how far behind
/// that sample it looks, and the waiting per unit that counts as a backlog.
#[derive(Clone, Copy, Debug)]
pub struct Over {
    /// Now, milliseconds.
    pub now_ms: i64,
    /// Freshness: a site whose newest sample is older than this is unmeasured, milliseconds.
    pub window_ms: i64,
    /// How far behind the site's newest sample the reading looks, milliseconds.
    pub horizon_ms: i64,
    /// Waiting work per serving unit at which an instant has a backlog.
    pub queue_full: f64,
}

/// Where a site's comparator reading came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ComparatorSource {
    /// The EPP's own detector, published with flow control on.
    Published,
    /// The gateway's approximation from the EPP's pool averages.
    Derived,
}

/// A source of site readings. Dispatch is static: the snapshot is generic over it.
pub trait SiteSignals {
    /// The site's reading `over` the horizon behind its newest fresh sample.
    fn read(&self, site: &str, cluster: &str, over: Over) -> SiteReading;

    /// Publication serial and qualified learning epoch for a snapshot read.
    fn learning_state(&self, site: &str, cluster: &str, now_ms: i64, max_age_ms: i64) -> grid_signals::LearningState;

    /// When this site's own operator last answered, `None` if it never has.
    fn relay_heard_ms(&self) -> Option<i64>;

    /// When the operator's last poll round that tried `site` ended, `None` if it reports none.
    fn round_ms(&self, site: &str) -> Option<i64>;

    /// Requests the site would run at a full cache, for the dashboard only; read once per refresh.
    fn implied_capacity(&self, _site: &str, _cluster: &str, _over: Over) -> Option<f64> {
        None
    }
}
