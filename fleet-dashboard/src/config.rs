//! The optional config file the Helm chart renders: thresholds, hub position,
//! and query overrides, merged over the defaults.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::{
    geocode,
    model::{self, Hub},
    queries::Thresholds,
};

/// Why the config file could not be used.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("read config {path}: {source}")]
    Read {
        /// The configured path.
        path: PathBuf,
        /// The I/O failure.
        source: std::io::Error,
    },
    /// The file is not the expected YAML.
    #[error("parse config {path}: {source}")]
    Parse {
        /// The configured path.
        path: PathBuf,
        /// The decode failure.
        source: serde_yaml::Error,
    },
}

/// Thresholds as written; `Option` so an explicit 0 is an override, not an
/// absent key.
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct ThresholdsYaml {
    /// GPU utilization percent at or above which a site is yellow.
    gpu_util_warn: Option<f64>,
    /// Queue depth at or above which a site is yellow.
    queue_warn: Option<f64>,
    /// Median latency in milliseconds at or above which a site is yellow.
    latency_warn_ms: Option<f64>,
    /// Consecutive unreachable polls at or above which a site is red.
    red_after_failures: Option<u32>,
}

/// The hub as written.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct HubYaml {
    /// Display name; an empty name means no hub.
    name: String,
    /// Cloud region code, geocoded when coordinates are absent.
    region: String,
    /// Latitude in degrees.
    lat: Option<f64>,
    /// Longitude in degrees.
    lng: Option<f64>,
}

/// The file's root.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileYaml {
    /// Threshold overrides.
    thresholds: Option<ThresholdsYaml>,
    /// The hub glyph.
    hub: Option<HubYaml>,
    /// PromQL overrides by key.
    queries: BTreeMap<String, String>,
}

/// The effective configuration: file values over defaults.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConfigFile {
    /// When a site turns yellow or red.
    pub thresholds: Thresholds,
    /// The hub glyph, when configured.
    pub hub: Option<Hub>,
    /// PromQL overrides by key.
    pub queries: BTreeMap<String, String>,
}

impl ConfigFile {
    /// The defaults with `path` merged over them, or just the defaults when
    /// there is no path.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] when the file cannot be read or is not valid YAML.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let raw: FileYaml = serde_yaml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source,
        })?;
        let mut config = Self::default();
        if let Some(thresholds) = &raw.thresholds {
            merge_thresholds(&mut config.thresholds, thresholds);
        }
        config.hub = raw.hub.filter(|hub| !hub.name.is_empty()).map(hub_from);
        config.queries = raw.queries;
        Ok(config)
    }

    /// The thresholds the SPA displays. `red_after_failures` stays server-side;
    /// the UI only ever sees its outcome.
    #[must_use]
    pub fn api_thresholds(&self) -> model::Thresholds {
        model::Thresholds {
            gpu_util_warn: self.thresholds.gpu_util_warn,
            queue_warn: self.thresholds.queue_warn,
            latency_warn_ms: self.thresholds.latency_warn_ms,
        }
    }
}

/// Overrides each threshold the file sets.
fn merge_thresholds(into: &mut Thresholds, from: &ThresholdsYaml) {
    if let Some(value) = from.gpu_util_warn {
        into.gpu_util_warn = value;
    }
    if let Some(value) = from.queue_warn {
        into.queue_warn = value;
    }
    if let Some(value) = from.latency_warn_ms {
        into.latency_warn_ms = value;
    }
    if let Some(value) = from.red_after_failures {
        into.red_after_failures = value;
    }
}

/// The hub glyph; a hub missing either coordinate is placed by its region.
fn hub_from(hub: HubYaml) -> Hub {
    let (lat, lng) = match (hub.lat, hub.lng) {
        (Some(lat), Some(lng)) => (Some(lat), Some(lng)),
        (lat, lng) => geocode::lookup(&hub.region).map_or((lat, lng), |at| (Some(at.latitude), Some(at.longitude))),
    };
    Hub {
        name: hub.name,
        region: hub.region,
        lat,
        lng,
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
#[expect(clippy::float_cmp, reason = "exact values read back from the file")]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{ConfigError, ConfigFile};

    #[test]
    fn no_path_gives_the_defaults() {
        let config = ConfigFile::load(None).unwrap();
        assert_eq!(config.thresholds.gpu_util_warn, 90.0);
        assert!(config.hub.is_none() && config.queries.is_empty(), "{config:?}");
    }

    #[test]
    fn a_file_merges_over_the_defaults_and_geocodes_the_hub() {
        let path = temp_file(
            "merge",
            "thresholds:\n  gpuUtilWarn: 80\n  redAfterFailures: 3\nhub:\n  name: aigrid-ds-hub\n  region: us-east-1\nqueries:\n  gpuUtil: avg(custom_util)\n",
        );
        let config = ConfigFile::load(Some(&path)).unwrap();
        let thresholds = &config.thresholds;
        assert_eq!(
            (
                thresholds.gpu_util_warn,
                thresholds.queue_warn,
                thresholds.red_after_failures
            ),
            (80.0, 50.0, 3)
        );
        let hub = config.hub.as_ref().unwrap();
        assert_eq!(
            (hub.name.as_str(), hub.lat),
            ("aigrid-ds-hub", Some(38.95)),
            "a hub with only a region is geocoded"
        );
        assert_eq!(
            config.queries.get("gpuUtil").map(String::as_str),
            Some("avg(custom_util)")
        );
    }

    #[test]
    fn an_explicit_zero_threshold_is_honored() {
        let path = temp_file("zero", "thresholds:\n  redAfterFailures: 0\n  queueWarn: 0\n");
        let thresholds = ConfigFile::load(Some(&path)).unwrap().thresholds;
        assert_eq!(
            (
                thresholds.red_after_failures,
                thresholds.queue_warn,
                thresholds.gpu_util_warn
            ),
            (0, 0.0, 90.0)
        );
    }

    #[test]
    fn explicit_hub_coordinates_win_over_the_region_table() {
        let path = temp_file("hub", "hub:\n  name: hq\n  region: us-east-1\n  lat: 1.5\n  lng: 2.5\n");
        let hub = ConfigFile::load(Some(&path)).unwrap().hub.unwrap();
        assert_eq!((hub.lat, hub.lng), (Some(1.5), Some(2.5)));
    }

    #[test]
    fn a_missing_file_is_an_error() {
        let err = ConfigFile::load(Some(Path::new("/nonexistent/fleet/config.yaml"))).unwrap_err();
        assert!(matches!(err, ConfigError::Read { .. }), "{err}");
    }

    #[test]
    fn api_thresholds_drop_red_after_failures() {
        let api = ConfigFile::load(None).unwrap().api_thresholds();
        assert_eq!(
            (api.gpu_util_warn, api.queue_warn, api.latency_warn_ms),
            (90.0, 50.0, 5000.0)
        );
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    fn temp_file(name: &str, contents: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("fleet-config-{}-{name}.yaml", std::process::id()));
        std::fs::write(&path, contents).unwrap();
        path
    }
}
