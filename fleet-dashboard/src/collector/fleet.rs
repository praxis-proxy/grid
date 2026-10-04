//! The poll loop: collects every site in parallel, derives health, publishes
//! the snapshot, and keeps a short history for site sparklines.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, PoisonError, RwLock},
    time::{Duration, Instant},
};

use futures::future::join_all;
use time::OffsetDateTime;
use tokio::{sync::watch, time::MissedTickBehavior};

use super::{Metrics, SeriesError, SiteMetrics, SitePoller, series};
use crate::{
    health::{self, Inputs, Verdict},
    metrics::Source,
    model::{self, FleetSnapshot, Health, Hub, Summary},
    queries::{QuerySet, Thresholds},
    registry::{Site, SiteList},
};

/// Polls kept for site sparklines: 20 minutes at the default 15s interval.
const HISTORY_LEN: usize = 80;

/// Where the current time comes from, so tests can pin it.
pub type Clock = Arc<dyn Fn() -> OffsetDateTime + Send + Sync>;

/// Everything a collector needs.
pub struct Options {
    /// The registry's current site list, woken on change.
    pub sites: watch::Receiver<SiteList>,
    /// Where site metrics are read from.
    pub source: Source,
    /// What to ask each site.
    pub queries: QuerySet,
    /// When a site turns yellow or red.
    pub thresholds: Thresholds,
    /// The hub glyph, when configured.
    pub hub: Option<Hub>,
    /// How often every site is polled.
    pub interval: Duration,
    /// Deadline for each query to a site.
    pub site_timeout: Duration,
    /// The dashboard's own metrics.
    pub metrics: Arc<Metrics>,
    /// The current time.
    pub clock: Clock,
}

/// Bookkeeping that outlives a single poll.
#[derive(Default)]
pub(super) struct State {
    /// The latest snapshot, once a poll has completed.
    pub(super) snapshot: Option<Arc<FleetSnapshot>>,
    /// Polls in a row each site has failed.
    failures: BTreeMap<String, u32>,
    /// When each site last answered.
    last_seen: BTreeMap<String, OffsetDateTime>,
    /// The most recent [`HISTORY_LEN`] snapshots, oldest first.
    history: VecDeque<Arc<FleetSnapshot>>,
    /// Fleet series by range, with when each was computed.
    pub(super) series_cache: BTreeMap<String, (OffsetDateTime, model::Series)>,
}

/// Polls the fleet and publishes snapshots.
pub struct Collector {
    /// The registry's current site list.
    pub(super) sites: watch::Receiver<SiteList>,
    /// Where site metrics are read from.
    pub(super) source: Source,
    /// What to ask each site.
    pub(super) queries: QuerySet,
    /// When a site turns yellow or red.
    thresholds: Thresholds,
    /// The hub glyph, when configured.
    hub: Option<Hub>,
    /// How often every site is polled.
    interval: Duration,
    /// Deadline for each query to a site.
    pub(super) site_timeout: Duration,
    /// The dashboard's own metrics.
    metrics: Arc<Metrics>,
    /// The current time.
    pub(super) clock: Clock,
    /// Bookkeeping; never held across an await.
    pub(super) state: RwLock<State>,
    /// The latest snapshot for subscribers, who always see the newest one.
    published: watch::Sender<Option<Arc<FleetSnapshot>>>,
}

impl Collector {
    /// A collector that has not polled yet.
    #[must_use]
    pub fn new(options: Options) -> Self {
        let Options {
            sites,
            source,
            queries,
            thresholds,
            hub,
            interval,
            site_timeout,
            metrics,
            clock,
        } = options;
        let (published, _initial) = watch::channel(None);
        Self {
            sites,
            source,
            queries,
            thresholds,
            hub,
            interval,
            site_timeout,
            metrics,
            clock,
            state: RwLock::default(),
            published,
        }
    }

    /// Polls immediately, then on every tick and on every registry change,
    /// until the future is dropped.
    pub async fn run(&self) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut registry = self.sites.clone();
        registry.mark_unchanged();
        let mut registry_open = true;
        loop {
            tokio::select! {
                _ = ticker.tick() => {},
                update = registry.changed(), if registry_open => {
                    // A closed registry means no more changes will ever come; keep ticking.
                    registry_open = update.is_ok();
                    if !registry_open {
                        continue;
                    }
                },
            }
            self.poll().await;
        }
    }

    /// Collects every site in parallel and publishes the resulting snapshot.
    pub async fn poll(&self) -> Arc<FleetSnapshot> {
        let started = Instant::now();
        let sites = self.sites.borrow().clone();
        let at = (self.clock)();
        let poller = SitePoller {
            source: &self.source,
            queries: &self.queries,
            timeout: self.site_timeout,
            metrics: &self.metrics,
        };
        let collected = join_all(sites.iter().map(|site| poller.collect(site, at))).await;
        let snapshot = self.publish(&sites, collected, at);
        self.metrics.poll_duration.observe(started.elapsed().as_secs_f64());
        snapshot
    }

    /// The latest snapshot, once a poll has completed.
    #[must_use]
    pub fn snapshot(&self) -> Option<Arc<FleetSnapshot>> {
        self.state
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .snapshot
            .clone()
    }

    /// Every new snapshot. A receiver only ever observes the newest one, so a
    /// slow subscriber skips frames rather than falling behind.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Option<Arc<FleetSnapshot>>> {
        self.published.subscribe()
    }

    /// The dashboard's own metrics.
    #[must_use]
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// A fleet-wide series over `range` (`1h`, `6h`, or `24h`), folded from
    /// every site and cached briefly.
    ///
    /// # Errors
    ///
    /// [`SeriesError::BadRange`] for any other range.
    pub async fn fleet_series(&self, range: &str) -> Result<model::Series, SeriesError> {
        series::compute(self, range).await
    }

    /// The site's latest entry plus its sparkline from history, so it costs
    /// no extra Prometheus round trip.
    #[must_use]
    pub fn site_detail(&self, name: &str) -> Option<model::SiteDetail> {
        let (site, points) = detail_of(&self.state.read().unwrap_or_else(PoisonError::into_inner), name)?;
        Some(model::SiteDetail {
            site,
            series: model::Series {
                range: None,
                step: self.interval.as_secs(),
                points,
            },
        })
    }

    /// Folds one poll into the state and hands the snapshot to subscribers.
    fn publish(&self, sites: &[Site], collected: Vec<SiteMetrics>, at: OffsetDateTime) -> Arc<FleetSnapshot> {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        let views: Vec<model::Site> = sites
            .iter()
            .zip(collected)
            .map(|(site, metrics)| self.build_site(&mut state, site, metrics, at))
            .collect();
        self.prune(&mut state, sites);
        let snapshot = Arc::new(FleetSnapshot {
            generated_at: at,
            hub: self.hub.clone(),
            summary: summarize(&views),
            sites: views,
            routes: Vec::new(),
        });
        state.snapshot = Some(Arc::clone(&snapshot));
        state.history.push_back(Arc::clone(&snapshot));
        while state.history.len() > HISTORY_LEN {
            state.history.pop_front();
        }
        drop(state);
        self.published.send_replace(Some(Arc::clone(&snapshot)));
        snapshot
    }

    /// Records reachability, derives health, and assembles the wire view.
    fn build_site(&self, state: &mut State, site: &Site, metrics: SiteMetrics, at: OffsetDateTime) -> model::Site {
        let failures = self.record_reachability(state, site, &metrics, at);
        let verdict = health::derive(&inputs(&metrics, failures), &self.thresholds);
        view(site, metrics, verdict, state.last_seen.get(&site.name).copied())
    }

    /// Updates the failure counter, last-seen time, and gauge; returns the
    /// consecutive failure count after this poll.
    fn record_reachability(&self, state: &mut State, site: &Site, metrics: &SiteMetrics, at: OffsetDateTime) -> u32 {
        let failures = state.failures.entry(site.name.clone()).or_default();
        if metrics.reachable {
            *failures = 0;
            state.last_seen.insert(site.name.clone(), at);
        } else {
            *failures = failures.saturating_add(1);
            tracing::info!(site = %site.name, failures = *failures, error = %metrics.err, "site unreachable");
        }
        let reachable = if metrics.reachable { 1.0 } else { 0.0 };
        self.metrics
            .site_reachable
            .with_label_values(&[&site.name])
            .set(reachable);
        *failures
    }

    /// Forgets sites that left the registry.
    fn prune(&self, state: &mut State, sites: &[Site]) {
        let keep: BTreeSet<&str> = sites.iter().map(|site| site.name.as_str()).collect();
        let gone: Vec<String> = state
            .failures
            .keys()
            .filter(|name| !keep.contains(name.as_str()))
            .cloned()
            .collect();
        for name in gone {
            state.failures.remove(&name);
            state.last_seen.remove(&name);
            self.metrics.forget_site(&name);
        }
    }
}

/// The site's current view and its history points, read under the lock.
fn detail_of(state: &State, name: &str) -> Option<(model::Site, Vec<model::Point>)> {
    let current = state
        .snapshot
        .as_ref()?
        .sites
        .iter()
        .find(|entry| entry.name == name)?
        .clone();
    let points = state
        .history
        .iter()
        .filter_map(|snapshot| point_of(snapshot, name))
        .collect();
    Some((current, points))
}

/// One site's sparkline sample from a past snapshot.
fn point_of(snapshot: &FleetSnapshot, name: &str) -> Option<model::Point> {
    let entry = snapshot.sites.iter().find(|entry| entry.name == name)?;
    Some(model::Point {
        at: snapshot.generated_at,
        gpu_util: entry.gpus.util_pct,
        queue_depth: entry.queue_depth,
        tokens_per_sec: entry.tokens_per_sec,
    })
}

/// What health derivation needs from one poll.
fn inputs(metrics: &SiteMetrics, consecutive_failures: u32) -> Inputs {
    Inputs {
        reachable: metrics.reachable,
        consecutive_failures,
        ready_endpoints: metrics.ready_endpoints,
        gpu_util: metrics.gpu_util,
        queue_depth: metrics.queue_depth,
        replicas_down: metrics.replicas_down,
        p50_latency_ms: metrics.p50_latency_ms,
        missing: metrics.missing.clone(),
    }
}

/// The wire view of one site.
fn view(site: &Site, metrics: SiteMetrics, verdict: Verdict, last_seen: Option<OffsetDateTime>) -> model::Site {
    model::Site {
        name: site.name.clone(),
        display_name: site.display_name.clone(),
        region: site.region.clone(),
        dc: site.dc.clone(),
        lat: site.lat,
        lng: site.lng,
        placed: site.lat.is_some() && site.lng.is_some(),
        address: site.address.clone(),
        health: verdict.health,
        reasons: verdict.reasons,
        gpus: model::Gpus {
            total: metrics.gpu_total.unwrap_or_default(),
            util_pct: metrics.gpu_util,
        },
        models: metrics.models,
        rps: metrics.rps,
        p50_latency_ms: metrics.p50_latency_ms,
        tokens_per_sec: metrics.tokens_per_sec,
        queue_depth: metrics.queue_depth,
        tenants: metrics.tenants,
        last_seen,
        last_error: metrics.err,
    }
}

/// Fleet-wide totals. GPU utilization is weighted by GPU count over the sites
/// that reported one; a site with GPUs but no reading counts toward the total
/// and not the mean.
fn summarize(sites: &[model::Site]) -> Summary {
    let mut summary = Summary::default();
    let (mut weighted, mut weighted_gpus) = (0.0_f64, 0.0_f64);
    let mut models = BTreeSet::new();
    let mut tenants = BTreeSet::new();
    for site in sites {
        summary.gpu_total += site.gpus.total;
        if let Some(util) = site.gpus.util_pct {
            weighted += util * site.gpus.total;
            weighted_gpus += site.gpus.total;
        }
        summary.tokens_per_sec += site.tokens_per_sec.unwrap_or_default();
        summary.rps += site.rps.unwrap_or_default();
        models.extend(site.models.iter().map(|model| model.name.as_str()));
        tenants.extend(site.tenants.iter().map(|tenant| tenant.name.as_str()));
        count_health(&mut summary, site.health);
    }
    if weighted_gpus > 0.0 {
        summary.gpu_util_pct = weighted / weighted_gpus;
    }
    summary.active_models = models.len();
    summary.active_tenants = tenants.len();
    summary
}

/// Adds one site to its health bucket.
fn count_health(summary: &mut Summary, health: Health) {
    match health {
        Health::Green => summary.sites_green = summary.sites_green.saturating_add(1),
        Health::Yellow => summary.sites_yellow = summary.sites_yellow.saturating_add(1),
        Health::Red => summary.sites_red = summary.sites_red.saturating_add(1),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
#[expect(clippy::indexing_slicing, reason = "test assertions on known snapshot shapes")]
#[expect(clippy::float_cmp, reason = "exact values chosen so the arithmetic is exact")]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use tokio::sync::watch;

    use super::Collector;
    use crate::{
        collector::testing::{INTERVAL, TIMEOUT, fleet, spoke},
        metrics::testing::{EMPTY, FakePrometheus, fake_for, healthy_answers, one, refused_base},
        model::{FleetSnapshot, Health},
        queries::QuerySet,
        registry::SiteList,
    };

    #[tokio::test]
    async fn poll_publishes_a_snapshot_with_the_hub_and_no_routes() {
        let (collector, _sites) = two_site_fleet(INTERVAL).await;
        assert!(collector.snapshot().is_none(), "no snapshot before the first poll");
        let snapshot = collector.poll().await;
        let hub = snapshot.hub.as_ref().map(|hub| hub.name.as_str());
        assert_eq!((snapshot.sites.len(), hub), (2, Some("hub")), "{snapshot:?}");
        assert!(snapshot.routes.is_empty(), "routes stay empty until the overlay ships");
        assert!(
            Arc::ptr_eq(&collector.snapshot().unwrap(), &snapshot),
            "snapshot() returns the last poll"
        );
    }

    #[tokio::test]
    async fn poll_derives_each_site_view_from_its_metrics() {
        let (collector, _sites) = two_site_fleet(INTERVAL).await;
        let snapshot = collector.poll().await;
        let ohio = &snapshot.sites[0];
        let shape = (ohio.name.as_str(), ohio.health, ohio.display_name.as_str(), ohio.placed);
        assert_eq!(shape, ("spoke1", Health::Green, "Ohio", true), "{ohio:?}");
        assert!(ohio.last_seen.is_some(), "a reachable site records lastSeen");
        assert_eq!(ohio.gpus.total, 2.0, "gpuTotal is copied");
        let oregon = &snapshot.sites[1];
        assert_eq!(
            (oregon.health, oregon.reasons[0].as_str()),
            (Health::Yellow, "GPU utilization 95% >= 90%")
        );
    }

    #[tokio::test]
    async fn the_summary_totals_and_weights_gpu_utilization_by_gpu_count() {
        let (collector, _sites) = two_site_fleet(INTERVAL).await;
        let summary = collector.poll().await.summary.clone();
        assert_eq!(
            (summary.gpu_total, summary.tokens_per_sec, summary.active_models),
            (6.0, 2930.0, 2),
            "{summary:?}"
        );
        assert_eq!(
            (summary.sites_green, summary.sites_yellow, summary.sites_red),
            (1, 1, 0),
            "{summary:?}"
        );
        let mean = summary.gpu_util_pct;
        assert!((77.1..77.2).contains(&mean), "(41.5*2 + 95*4) / 6 = 77.17, got {mean}");
    }

    #[tokio::test]
    async fn the_summary_excludes_sites_without_utilization_from_the_mean() {
        let no_fallback = BTreeMap::from([("gpuUtilFallback".to_owned(), String::new())]);
        let queries = QuerySet::defaults().unwrap().with(&no_fallback);
        let mut answers = healthy_answers();
        answers.insert("gpuTotal", (200, one(4.0)));
        answers.insert("gpuUtil", (200, EMPTY.to_owned()));
        let spoke1 = spoke(
            "spoke1",
            "Ohio",
            "us-east-2",
            &fake_for(&queries, &healthy_answers()).serve().await,
        );
        let spoke2 = spoke(
            "spoke2",
            "Oregon",
            "us-west-2",
            &fake_for(&queries, &answers).serve().await,
        );
        let (collector, _sites) = fleet(vec![spoke1, spoke2], queries, INTERVAL, TIMEOUT);
        let snapshot = collector.poll().await;
        let summary = &snapshot.summary;
        assert_eq!(
            (summary.gpu_total, summary.gpu_util_pct),
            (6.0, 41.5),
            "GPUs without a reading count toward the total only"
        );
        let oregon = &snapshot.sites[1];
        assert_eq!(
            (oregon.health, oregon.reasons[0].as_str()),
            (Health::Yellow, "no data for gpuUtil")
        );
    }

    #[tokio::test]
    async fn consecutive_failures_escalate_from_yellow_to_red() {
        let (collector, _sites, _healthy) = fleet_with_spoke1_down().await;
        let first = collector.poll().await;
        let spoke1 = &first.sites[0];
        assert_eq!(spoke1.health, Health::Yellow, "one failure is yellow: {spoke1:?}");
        assert!(
            !spoke1.last_error.is_empty() && spoke1.last_seen.is_none(),
            "{spoke1:?}"
        );
        let second = collector.poll().await;
        assert_eq!(
            (second.sites[0].health, second.summary.sites_red),
            (Health::Red, 1),
            "two failures are red"
        );
    }

    #[tokio::test]
    async fn a_recovered_site_turns_green_and_records_last_seen() {
        let (collector, sites, healthy) = fleet_with_spoke1_down().await;
        collector.poll().await;
        collector.poll().await;
        let spoke2 = sites.borrow()[1].clone();
        sites
            .send(Arc::new(vec![spoke("spoke1", "Ohio", "us-east-2", &healthy), spoke2]))
            .unwrap();
        let third = collector.poll().await;
        let spoke1 = &third.sites[0];
        assert_eq!(spoke1.health, Health::Green, "recovery resets the counter: {spoke1:?}");
        assert!(spoke1.last_seen.is_some(), "recovery records lastSeen");
    }

    #[tokio::test]
    async fn run_polls_immediately_and_again_when_the_registry_changes() {
        let (collector, sites) = two_site_fleet(Duration::from_secs(3600)).await;
        let collector = Arc::new(collector);
        let mut updates = collector.subscribe();
        let runner = Arc::clone(&collector);
        let handle = tokio::spawn(async move { runner.run().await });
        let first = next_snapshot(&mut updates).await;
        assert_eq!(
            first.sites.len(),
            2,
            "the first poll happens without waiting for a tick"
        );
        let mut three = (*sites.borrow()).as_ref().clone();
        three.push(spoke("spoke3", "spoke3", "", &refused_base()));
        sites.send(Arc::new(three)).unwrap();
        let second = next_snapshot(&mut updates).await;
        assert_eq!(
            second.sites.len(),
            3,
            "a registry change triggers a poll long before the 1h tick"
        );
        let spoke3 = &second.sites[2];
        assert!(
            !spoke3.placed && spoke3.lat.is_none(),
            "an unknown region stays unplaced: {spoke3:?}"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn run_polls_on_every_tick() {
        let queries = QuerySet::defaults().unwrap();
        let spoke1 = spoke(
            "spoke1",
            "Ohio",
            "us-east-2",
            &fake_for(&queries, &healthy_answers()).serve().await,
        );
        let (collector, _sites) = fleet(vec![spoke1], queries, Duration::from_millis(20), TIMEOUT);
        let collector = Arc::new(collector);
        let mut updates = collector.subscribe();
        let runner = Arc::clone(&collector);
        let handle = tokio::spawn(async move { runner.run().await });
        let first = next_snapshot(&mut updates).await;
        let second = next_snapshot(&mut updates).await;
        assert!(
            second.generated_at > first.generated_at,
            "a later tick produces a newer snapshot"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn site_detail_is_the_latest_entry_plus_its_history() {
        let (collector, _sites) = two_site_fleet(INTERVAL).await;
        for _ in 0..3 {
            collector.poll().await;
        }
        let detail = collector.site_detail("spoke1").unwrap();
        let shape = (
            detail.site.name.as_str(),
            detail.series.step,
            detail.series.points.len(),
        );
        assert_eq!(shape, ("spoke1", 15, 3), "{detail:?}");
        assert!(detail.series.range.is_none(), "a site series carries no range");
        assert!(
            detail.series.points[0].at < detail.series.points[2].at,
            "points are chronological"
        );
        assert_eq!(
            detail.series.points[2].gpu_util,
            Some(41.5),
            "points carry the site's utilization"
        );
        assert!(collector.site_detail("nope").is_none(), "unknown site");
    }

    #[tokio::test]
    async fn history_is_capped_at_eighty_polls() {
        let queries = QuerySet::defaults().unwrap();
        let spoke1 = spoke(
            "spoke1",
            "Ohio",
            "us-east-2",
            &fake_for(&queries, &healthy_answers()).serve().await,
        );
        let (collector, _sites) = fleet(vec![spoke1], queries, INTERVAL, TIMEOUT);
        for _ in 0..81 {
            collector.poll().await;
        }
        let points = collector.site_detail("spoke1").unwrap().series.points.len();
        assert_eq!(points, 80, "history keeps the last 80 polls");
    }

    #[tokio::test]
    async fn poll_is_not_blocked_by_a_slow_site() {
        let queries = QuerySet::defaults().unwrap();
        let spoke1 = spoke(
            "spoke1",
            "Ohio",
            "us-east-2",
            &fake_for(&queries, &healthy_answers()).serve().await,
        );
        let spoke2 = spoke(
            "spoke2",
            "Oregon",
            "us-west-2",
            &FakePrometheus::new().hang().serve().await,
        );
        let (collector, _sites) = fleet(vec![spoke1, spoke2], queries, INTERVAL, Duration::from_millis(500));
        let snapshot = tokio::time::timeout(Duration::from_secs(3), collector.poll())
            .await
            .unwrap();
        assert_eq!(snapshot.sites[0].health, Health::Green, "{:?}", snapshot.sites[0]);
        assert_ne!(snapshot.sites[1].health, Health::Green, "{:?}", snapshot.sites[1]);
    }

    #[tokio::test]
    async fn a_slow_subscriber_sees_only_the_newest_snapshot() {
        let (collector, _sites) = two_site_fleet(INTERVAL).await;
        let mut updates = collector.subscribe();
        let first = collector.poll().await;
        let second = collector.poll().await;
        let seen = updates.borrow_and_update().clone().unwrap();
        assert!(
            Arc::ptr_eq(&seen, &second) && !Arc::ptr_eq(&seen, &first),
            "the stale frame is replaced, not queued"
        );
    }

    #[tokio::test]
    async fn poll_records_the_site_reachable_gauge_and_prunes_removed_sites() {
        let (collector, sites, _healthy) = fleet_with_spoke1_down().await;
        collector.poll().await;
        let gauge = |site: &str| collector.metrics().site_reachable.with_label_values(&[site]).get();
        assert_eq!(
            (gauge("spoke1"), gauge("spoke2")),
            (0.0, 1.0),
            "the gauge reflects the latest poll"
        );
        let spoke2 = sites.borrow()[1].clone();
        sites.send(Arc::new(vec![spoke2])).unwrap();
        let snapshot = collector.poll().await;
        assert_eq!(snapshot.sites.len(), 1, "a removed site leaves the snapshot");
        assert_eq!(
            reachable_labels(&collector),
            ["spoke2"],
            "a removed site leaves the gauge too"
        );
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// Ohio healthy; Oregon with 4 GPUs at 95% and 1000 tokens/s.
    async fn two_site_fleet(interval: Duration) -> (Collector, watch::Sender<SiteList>) {
        let queries = QuerySet::defaults().unwrap();
        let mut oregon = healthy_answers();
        oregon.insert("gpuTotal", (200, one(4.0)));
        oregon.insert("gpuUtil", (200, one(95.0)));
        oregon.insert("tokensPerSec", (200, one(1000.0)));
        let spoke1 = spoke(
            "spoke1",
            "Ohio",
            "us-east-2",
            &fake_for(&queries, &healthy_answers()).serve().await,
        );
        let spoke2 = spoke(
            "spoke2",
            "Oregon",
            "us-west-2",
            &fake_for(&queries, &oregon).serve().await,
        );
        fleet(vec![spoke1, spoke2], queries, interval, TIMEOUT)
    }

    /// Ohio refusing connections, Oregon healthy; also returns the healthy
    /// base URL so a test can bring Ohio back.
    async fn fleet_with_spoke1_down() -> (Collector, watch::Sender<SiteList>, String) {
        let queries = QuerySet::defaults().unwrap();
        let healthy = fake_for(&queries, &healthy_answers()).serve().await;
        let down = spoke("spoke1", "Ohio", "us-east-2", &refused_base());
        let spoke2 = spoke("spoke2", "Oregon", "us-west-2", &healthy);
        let (collector, sites) = fleet(vec![down, spoke2], queries, INTERVAL, TIMEOUT);
        (collector, sites, healthy)
    }

    async fn next_snapshot(updates: &mut watch::Receiver<Option<Arc<FleetSnapshot>>>) -> Arc<FleetSnapshot> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                updates.changed().await.unwrap();
                let latest = updates.borrow_and_update().clone();
                if let Some(snapshot) = latest {
                    return snapshot;
                }
            }
        })
        .await
        .unwrap()
    }

    fn reachable_labels(collector: &Collector) -> Vec<String> {
        collector
            .metrics()
            .registry
            .gather()
            .into_iter()
            .filter(|family| family.name() == "fleet_dashboard_site_reachable")
            .flat_map(|family| {
                family
                    .get_metric()
                    .iter()
                    .map(|metric| metric.get_label()[0].value().to_owned())
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}
