//! The JSON contract served by the API and consumed by the SPA.
//!
//! Field names are camelCase on the wire. A metric the collector could not
//! read is `None` and serializes as `null`; the SPA relies on the key being
//! present, so nothing here skips absent values.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Traffic-light health of one site.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Health {
    /// Every check passed.
    #[default]
    Green,
    /// At least one warning threshold fired or data is missing.
    Yellow,
    /// The site is unreachable or has no ready endpoints.
    Red,
}

/// The hub glyph drawn on the map; hidden by the SPA when `name` is empty.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hub {
    /// Display name.
    pub name: String,
    /// Cloud region code, used for geocoding when coordinates are absent.
    pub region: String,
    /// Latitude in degrees, when placed.
    pub lat: Option<f64>,
    /// Longitude in degrees, when placed.
    pub lng: Option<f64>,
}

/// GPU inventory and utilization for one site.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Gpus {
    /// Number of GPUs the site reports; zero when unknown.
    pub total: f64,
    /// Mean utilization in percent, when reported.
    pub util_pct: Option<f64>,
}

/// One served model and how many replicas run it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCount {
    /// Model name as labeled by the serving stack.
    pub name: String,
    /// Running replica count.
    pub running: f64,
}

/// One tenant's share of a site's traffic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantShare {
    /// Tenant name.
    pub name: String,
    /// Share of the site's total in percent.
    pub share_pct: f64,
}

/// One registered site with its latest metrics and derived health.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Site {
    /// Registry name; the stable identifier.
    pub name: String,
    /// Human-readable name, falling back to `name`.
    pub display_name: String,
    /// Cloud region code.
    pub region: String,
    /// Data center label.
    pub dc: String,
    /// Latitude in degrees, when placed.
    pub lat: Option<f64>,
    /// Longitude in degrees, when placed.
    pub lng: Option<f64>,
    /// Whether the site has coordinates to draw at.
    pub placed: bool,
    /// Serving address from the registry.
    pub address: String,
    /// Derived traffic-light health.
    pub health: Health,
    /// Every reason that contributed to `health`, in the order it fired.
    pub reasons: Vec<String>,
    /// GPU inventory and utilization.
    pub gpus: Gpus,
    /// Models running at the site, sorted by name.
    pub models: Vec<ModelCount>,
    /// Requests per second, when reported.
    pub rps: Option<f64>,
    /// Median request latency in milliseconds, when reported.
    pub p50_latency_ms: Option<f64>,
    /// Generated tokens per second, when reported.
    pub tokens_per_sec: Option<f64>,
    /// Requests waiting, when reported.
    pub queue_depth: Option<f64>,
    /// Tenant shares, sorted descending.
    pub tenants: Vec<TenantShare>,
    /// When the site last answered a poll; `None` if never.
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_seen: Option<OffsetDateTime>,
    /// Last collection error, empty when the latest poll succeeded.
    pub last_error: String,
}

/// Fleet-wide totals for the summary strip.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    /// Sum of every site's GPU count.
    pub gpu_total: f64,
    /// GPU utilization averaged across sites, weighted by GPU count.
    pub gpu_util_pct: f64,
    /// Sum of tokens per second.
    pub tokens_per_sec: f64,
    /// Sum of requests per second.
    pub rps: f64,
    /// Distinct model names across the fleet.
    pub active_models: usize,
    /// Distinct tenant names across the fleet.
    pub active_tenants: usize,
    /// Sites currently green.
    pub sites_green: usize,
    /// Sites currently yellow.
    pub sites_yellow: usize,
    /// Sites currently red.
    pub sites_red: usize,
}

/// A link between two sites, reserved for the deferred topology overlay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    /// Source site name.
    pub from: String,
    /// Destination site name.
    pub to: String,
}

/// Everything the map needs for one poll.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FleetSnapshot {
    /// When the poll ran.
    #[serde(with = "time::serde::rfc3339")]
    pub generated_at: OffsetDateTime,
    /// The hub glyph, when configured.
    pub hub: Option<Hub>,
    /// Every registered site.
    pub sites: Vec<Site>,
    /// Fleet-wide totals.
    pub summary: Summary,
    /// Topology links; always empty until the overlay ships.
    pub routes: Vec<Route>,
}

/// One sample on a time series.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Point {
    /// Sample time; `t` on the wire.
    #[serde(rename = "t", with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
    /// GPU utilization in percent, when known.
    pub gpu_util: Option<f64>,
    /// Requests waiting, when known.
    pub queue_depth: Option<f64>,
    /// Generated tokens per second, when known.
    pub tokens_per_sec: Option<f64>,
}

/// A time series for the charts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Series {
    /// Requested window (`1h`, `6h`, `24h`); absent for a site's history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    /// Seconds between points.
    pub step: u64,
    /// Samples in ascending time order.
    pub points: Vec<Point>,
}

/// One site's latest entry plus its sparkline from the collector's history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SiteDetail {
    /// The site's current state, flattened to match the Go embedded struct.
    #[serde(flatten)]
    pub site: Site,
    /// Recent history for the detail panel.
    pub series: Series,
}

/// The warning thresholds the collector applies, so the SPA can say
/// "warn at 90%" next to a value instead of guessing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Thresholds {
    /// GPU utilization percent at which a site turns yellow.
    pub gpu_util_warn: f64,
    /// Queue depth at which a site turns yellow.
    pub queue_warn: f64,
    /// Median latency in milliseconds at which a site turns yellow.
    pub latency_warn_ms: f64,
}

/// Static configuration the SPA reads once at load.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    /// The hub glyph, when configured.
    pub hub: Option<Hub>,
    /// How often the collector polls, so the SPA can show data age.
    pub poll_interval_seconds: u64,
    /// Build version string.
    pub version: String,
    /// Warning thresholds in effect.
    pub thresholds: Thresholds,
    /// Identity oauth-proxy forwarded for this request; `None` without a proxy.
    pub user: Option<String>,
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
#[expect(clippy::indexing_slicing, reason = "test assertions on known JSON structure")]
mod tests {
    use time::macros::datetime;

    use super::{FleetSnapshot, Gpus, Health, Series, Site, Summary};

    #[test]
    fn absent_metrics_serialize_as_null() {
        let site = Site {
            name: "s".to_owned(),
            health: Health::Green,
            gpus: Gpus {
                total: 0.0,
                util_pct: None,
            },
            rps: None,
            ..Site::default()
        };
        let json = serde_json::to_value(&site).unwrap();
        assert_eq!(
            json["rps"],
            serde_json::Value::Null,
            "absent rps must be null, not omitted"
        );
        assert_eq!(
            json["gpus"]["utilPct"],
            serde_json::Value::Null,
            "nested absent value must be null"
        );
        assert!(
            json.get("p50LatencyMs").is_some(),
            "every contract field must be present"
        );
    }

    #[test]
    fn timestamps_use_z_and_omit_zero_subseconds() {
        let snapshot = FleetSnapshot {
            generated_at: datetime!(2026-01-01 00:00:00 UTC),
            hub: None,
            sites: Vec::new(),
            summary: Summary::default(),
            routes: Vec::new(),
        };
        let json = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(
            json["generatedAt"], "2026-01-01T00:00:00Z",
            "must match Go's RFC3339 output"
        );
    }

    #[test]
    fn a_series_without_a_range_omits_the_key() {
        let json = serde_json::to_value(Series {
            range: None,
            step: 15,
            points: Vec::new(),
        })
        .unwrap();
        assert!(json.get("range").is_none(), "range mirrors Go's omitempty");
    }
}
