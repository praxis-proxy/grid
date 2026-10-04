//! A synthetic eight-site fleet so the UI can be shown with no spokes
//! attached. Every value is a function of the site name and the query time,
//! with a ten-minute period; Frankfurt always runs hot and Singapore is down
//! for the first two minutes of every ten.

use std::collections::BTreeMap;

use time::OffsetDateTime;

use crate::{
    metrics::{Matrix, MatrixSeries, MetricsError, Sample, SeriesPoint, Vector, Window},
    queries::Query,
    registry::Site,
};

/// What a site reports while the demo takes it down.
const OUTAGE: &str = "dial tcp 10.0.0.1:9091: i/o timeout";

/// One demo site's fixed identity.
struct Metro {
    /// Registry name.
    name: &'static str,
    /// Display name.
    display: &'static str,
    /// Cloud region code.
    region: &'static str,
    /// Data center label.
    dc: &'static str,
    /// Degrees north.
    latitude: f64,
    /// Degrees east.
    longitude: f64,
}

/// The eight demo metros.
const SITES: [Metro; 8] = [
    Metro {
        name: "ohio",
        display: "Ohio",
        region: "us-east-2",
        dc: "aws-us-east-2",
        latitude: 40.09,
        longitude: -82.75,
    },
    Metro {
        name: "oregon",
        display: "Oregon",
        region: "us-west-2",
        dc: "aws-us-west-2",
        latitude: 45.60,
        longitude: -122.68,
    },
    Metro {
        name: "virginia",
        display: "Virginia",
        region: "us-east-1",
        dc: "aws-us-east-1",
        latitude: 38.95,
        longitude: -77.45,
    },
    Metro {
        name: "london",
        display: "London",
        region: "eu-west-2",
        dc: "aws-eu-west-2",
        latitude: 51.51,
        longitude: -0.13,
    },
    Metro {
        name: "frankfurt",
        display: "Frankfurt",
        region: "eu-central-1",
        dc: "aws-eu-central-1",
        latitude: 50.11,
        longitude: 8.68,
    },
    Metro {
        name: "mumbai",
        display: "Mumbai",
        region: "ap-south-1",
        dc: "aws-ap-south-1",
        latitude: 19.08,
        longitude: 72.88,
    },
    Metro {
        name: "singapore",
        display: "Singapore",
        region: "ap-southeast-1",
        dc: "aws-ap-southeast-1",
        latitude: 1.35,
        longitude: 103.82,
    },
    Metro {
        name: "tokyo",
        display: "Tokyo",
        region: "ap-northeast-1",
        dc: "aws-ap-northeast-1",
        latitude: 35.68,
        longitude: 139.69,
    },
];

/// The eight demo sites, one per well-known cloud metro.
#[must_use]
pub fn sites() -> Vec<Site> {
    SITES
        .into_iter()
        .map(|metro| Site {
            name: metro.name.to_owned(),
            display_name: metro.display.to_owned(),
            region: metro.region.to_owned(),
            dc: metro.dc.to_owned(),
            address: format!("{}.demo.internal", metro.name),
            cluster_value: metro.name.to_owned(),
            lat: Some(metro.latitude),
            lng: Some(metro.longitude),
            ..Site::default()
        })
        .collect()
}

/// An instant query at `at`.
///
/// # Errors
///
/// [`MetricsError::Simulated`] while the site is down.
pub fn query(site: &Site, query: &Query, at: OffsetDateTime) -> Result<Vector, MetricsError> {
    if down(site, at) {
        return Err(MetricsError::Simulated(OUTAGE));
    }
    let seeded = |suffix: &str| seed(&format!("{}{suffix}", site.name));
    Ok(match query.key.as_str() {
        "models" => labeled(
            "model_name",
            &[
                ("Qwen2.5-7B-Instruct", (3.0 + 9.0 * seeded("")).floor()),
                ("Llama-3.1-8B-Instruct", (1.0 + 5.0 * seeded("b")).floor()),
            ],
        ),
        "tenants" => labeled(
            "tenant",
            &[
                ("acme", 50.0 + 20.0 * seeded("")),
                ("globex", 30.0),
                ("initech", 10.0 + 10.0 * seeded("c")),
            ],
        ),
        key => value_at(site, key, at.unix_timestamp())
            .map(|value| {
                vec![Sample {
                    labels: BTreeMap::new(),
                    value,
                }]
            })
            .unwrap_or_default(),
    })
}

/// A range query sampled every `window.step` from start to end inclusive.
///
/// # Errors
///
/// [`MetricsError::Simulated`] while the site is down at `window.end`.
pub fn query_range(site: &Site, query: &Query, window: Window) -> Result<Matrix, MetricsError> {
    if down(site, window.end) {
        return Err(MetricsError::Simulated(OUTAGE));
    }
    let step = i64::try_from(window.step.as_secs()).unwrap_or(i64::MAX).max(1);
    let end = window.end.unix_timestamp();
    let mut points = Vec::new();
    let mut unix = window.start.unix_timestamp();
    while unix <= end {
        if let (Some(value), Ok(at)) = (
            value_at(site, &query.key, unix),
            OffsetDateTime::from_unix_timestamp(unix),
        ) {
            points.push(SeriesPoint { at, value });
        }
        unix = unix.saturating_add(step);
    }
    Ok(vec![MatrixSeries {
        labels: BTreeMap::new(),
        points,
    }])
}

/// Singapore is unreachable during minutes 0 and 1 of every ten-minute window.
fn down(site: &Site, at: OffsetDateTime) -> bool {
    site.name == "singapore" && at.minute().rem_euclid(10) < 2
}

/// A stable value in `[0, 1)` for a name, matching the Go demo's FNV-1a seed.
fn seed(name: &str) -> f64 {
    f64::from(fnv1a(name).rem_euclid(1000)) / 1000.0
}

/// 32-bit FNV-1a.
fn fnv1a(text: &str) -> u32 {
    text.bytes().fold(0x811C_9DC5_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    })
}

/// The scalar for `key` at `unix`, or `None` for a key the demo does not serve.
fn value_at(site: &Site, key: &str, unix: i64) -> Option<f64> {
    let phase = seed(&site.name) * 2.0 * std::f64::consts::PI;
    let wave = ((seconds(unix) / 600.0 + phase).sin() + 1.0) / 2.0;
    let gpus = 4.0 + (seed(&site.name) * 12.0).floor();
    let util = if site.name == "frankfurt" {
        92.0 + 6.0 * wave
    } else {
        30.0 + 55.0 * wave
    };
    Some(match key {
        "readyEndpoints" => (gpus / 2.0).floor().max(1.0),
        "gpuTotal" => gpus,
        "gpuUtil" => util,
        "rps" => gpus * (2.0 + 8.0 * wave),
        "p50LatencyMs" => 400.0 + 1800.0 * wave * wave,
        "tokensPerSec" => gpus * (150.0 + 600.0 * wave),
        "queueDepth" => (40.0 * wave * wave).floor(),
        "replicasDown" => 0.0,
        _ => return None,
    })
}

/// Unix seconds as a float, the Go demo's `float64(at.Unix())`.
#[expect(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "the demo wave is a function of seconds as f64"
)]
fn seconds(unix: i64) -> f64 {
    unix as f64
}

/// One sample per `(label value, value)`.
fn labeled(label: &str, samples: &[(&str, f64)]) -> Vector {
    samples
        .iter()
        .map(|(name, value)| Sample {
            labels: BTreeMap::from([(label.to_owned(), (*name).to_owned())]),
            value: *value,
        })
        .collect()
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
#[expect(clippy::indexing_slicing, reason = "test assertions on known vectors")]
mod tests {
    use std::time::Duration;

    use time::macros::datetime;

    use super::{query, query_range, sites};
    use crate::{metrics::Window, queries::QuerySet};

    #[test]
    fn eight_placed_sites() {
        let sites = sites();
        assert_eq!(sites.len(), 8);
        assert!(
            sites.iter().all(|site| site.lat.is_some() && site.lng.is_some()),
            "every demo site is placed"
        );
    }

    #[test]
    fn values_are_positive_models_are_labeled_and_frankfurt_runs_hot() {
        let (queries, at) = (QuerySet::defaults().unwrap(), datetime!(2026-09-06 12:00:00 UTC));
        let ohio = &sites()[0];
        let gpus = query(ohio, &queries.query("gpuTotal"), at).unwrap();
        assert!(gpus[0].value > 0.0, "{gpus:?}");
        let models = query(ohio, &queries.query("models"), at).unwrap();
        assert!(models[0].labels.contains_key("model_name"), "{models:?}");
        let frankfurt = sites().into_iter().find(|site| site.name == "frankfurt").unwrap();
        let hot = query(&frankfurt, &queries.query("gpuUtil"), at).unwrap();
        assert!(hot[0].value >= 90.0, "frankfurt must be hot: {hot:?}");
    }

    #[test]
    fn singapore_is_down_in_the_first_two_minutes_of_every_ten() {
        let queries = QuerySet::defaults().unwrap();
        let singapore = sites().into_iter().find(|site| site.name == "singapore").unwrap();
        let err = query(
            &singapore,
            &queries.query("gpuUtil"),
            datetime!(2026-09-06 12:00:00 UTC),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "dial tcp 10.0.0.1:9091: i/o timeout",
            "the fixture's exact message"
        );
        query(
            &singapore,
            &queries.query("gpuUtil"),
            datetime!(2026-09-06 12:05:00 UTC),
        )
        .unwrap();
    }

    #[test]
    fn a_range_yields_one_point_per_step_inclusive() {
        let queries = QuerySet::defaults().unwrap();
        let end = datetime!(2026-09-06 12:00:00 UTC);
        let window = Window {
            start: end - time::Duration::hours(1),
            end,
            step: Duration::from_secs(30),
        };
        let matrix = query_range(&sites()[0], &queries.query("tokensPerSec"), window).unwrap();
        assert_eq!((matrix.len(), matrix[0].points.len()), (1, 121), "{matrix:?}");
    }
}
