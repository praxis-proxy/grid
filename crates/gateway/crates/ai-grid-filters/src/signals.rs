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
    /// Work held before scheduling, a count for the whole site, worst within the window.
    pub held: Option<f64>,
    /// The most any one serving unit of the site has waiting, so a scale-out cannot hide a
    /// backlog behind a site-wide average.
    pub deepest_queue: Option<f64>,
    /// The most the site held at an instant when it also had a backlog (waiting at or past
    /// `queue_full` per unit, or anything held before scheduling), worst within the window.
    /// Absent when no instant in the window had one.
    pub congested_in_flight: Option<f64>,
    /// The site's own reading of how full it is, 0 to 1, worst within the window. The
    /// provider divides by its own capacity, so this needs no ceiling and does not depend on
    /// how much the gateway has sent. An llm-d site derives it from its KV-cache mean widened
    /// by the spread across units; a provider that knows its own limit by other means reports
    /// the same joined reading.
    pub saturation: Option<f64>,
    /// The most it held at an instant that had a backlog and reported itself full. One sample
    /// decides both, because every series here is the worst in its window: maximised apart, the
    /// count from one congested sample pairs with the fullness of another, and a limit is
    /// recorded for a state the site was never in.
    pub full_in_flight: Option<f64>,
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
    /// The fullness a site reports at which that reading alone marks a limit.
    pub reported_full: f64,
}

/// A source of site readings. Dispatch is static: the snapshot is generic over it.
pub trait SiteSignals {
    /// The site's reading `over` the horizon behind its newest fresh sample.
    fn read(&self, site: &str, cluster: &str, over: Over) -> SiteReading;

    /// Publication serial and qualified learning epoch for a snapshot read.
    fn learning_state(&self, site: &str, cluster: &str, now_ms: i64, max_age_ms: i64) -> grid_signals::LearningState;
}
