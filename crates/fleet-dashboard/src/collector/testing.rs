#![expect(clippy::unwrap_used, reason = "test support")]
//! Shared builders for collector tests: sites, a deterministic clock, and a
//! collector wired to fake Prometheus servers.

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use time::OffsetDateTime;
use tokio::sync::watch;

use super::{Clock, Collector, Metrics, Options};
use crate::{
    geocode,
    metrics::{
        PerSiteSource, Source,
        testing::{TOKEN, install_provider, secrets},
    },
    model::Hub,
    queries::{QuerySet, Thresholds},
    registry::{Site, SiteList},
};

/// Generous per-query deadline for tests that are not about timeouts.
pub(crate) const TIMEOUT: Duration = Duration::from_secs(5);

/// The production default poll interval.
pub(crate) const INTERVAL: Duration = Duration::from_secs(15);

/// Secrets for `spoke1` through `spoke3`, all with the fake's token and no CA.
const SECRETS: [(&str, &str, &[u8]); 3] = [
    ("site-spoke1", TOKEN, &[]),
    ("site-spoke2", TOKEN, &[]),
    ("site-spoke3", TOKEN, &[]),
];

/// A site whose metrics live at `base`, placed by its region when known.
pub(crate) fn spoke(name: &str, display: &str, region: &str, base: &str) -> Site {
    let placed = geocode::lookup(region);
    Site {
        name: name.to_owned(),
        display_name: display.to_owned(),
        region: region.to_owned(),
        address: format!("gw-{name}"),
        metrics_url: base.to_owned(),
        metrics_secret: format!("site-{name}"),
        cluster_value: name.to_owned(),
        lat: placed.map(|at| at.latitude),
        lng: placed.map(|at| at.longitude),
        ..Site::default()
    }
}

/// Advances 15s on every read, from 2026-09-06T12:00:00Z.
pub(crate) fn ticking_clock() -> Clock {
    let ticks = AtomicI64::new(0);
    Arc::new(move || {
        let tick = ticks.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        OffsetDateTime::from_unix_timestamp(1_788_696_000_i64.saturating_add(tick.saturating_mul(15))).unwrap()
    })
}

/// A collector over `sites` with per-site sources, a hub, default thresholds,
/// fresh metrics, and the ticking clock; returns the registry sender too.
pub(crate) fn fleet(
    sites: Vec<Site>,
    queries: QuerySet,
    interval: Duration,
    site_timeout: Duration,
) -> (Collector, watch::Sender<SiteList>) {
    install_provider();
    let (sender, receiver) = watch::channel(Arc::new(sites));
    let hub = Hub {
        name: "hub".to_owned(),
        region: "us-east-1".to_owned(),
        lat: Some(38.95),
        lng: Some(-77.45),
    };
    let collector = Collector::new(Options {
        sites: receiver,
        source: Source::PerSite(PerSiteSource::new(secrets(&SECRETS), site_timeout)),
        queries,
        thresholds: Thresholds::default(),
        hub: Some(hub),
        interval,
        site_timeout,
        metrics: Arc::new(Metrics::new().unwrap()),
        clock: ticking_clock(),
    });
    (collector, sender)
}
