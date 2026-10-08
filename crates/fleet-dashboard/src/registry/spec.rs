//! Parses the EPP file-discovery schema and resolves each endpoint to a site.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use crate::geocode::{self, Coordinates};

/// One entry of the EPP file-discovery schema, reduced to the fields the
/// dashboard reads; the rest of the schema is ignored on decode.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct EndpointSpec {
    /// Unique site name.
    pub name: String,
    /// Serving address.
    pub address: String,
    /// Free-form labels; the dashboard reads `displayName`, `region`, `dc`,
    /// `metricsURL`, `metricsSecret`, `clusterLabel`, `lat`, and `lng`.
    pub labels: BTreeMap<String, String>,
}

/// The document root.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Document {
    /// Every registered endpoint.
    endpoints: Vec<EndpointSpec>,
}

/// A registry entry resolved for the dashboard. Coordinates are `Some` for
/// both or neither; an unplaced site is still shown, in the unplaced tray.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Site {
    /// Unique site name.
    pub name: String,
    /// Human-readable name, falling back to `name`.
    pub display_name: String,
    /// Cloud region code.
    pub region: String,
    /// Data center label.
    pub dc: String,
    /// Serving address.
    pub address: String,
    /// Base URL of the site's Prometheus-compatible API.
    pub metrics_url: String,
    /// Name of the Secret holding the bearer token and CA for `metrics_url`.
    pub metrics_secret: String,
    /// What identifies this site in central mode's cluster label; defaults to
    /// `name`, overridable when the site's series use another identifier.
    pub cluster_value: String,
    /// Latitude in degrees, when placed.
    pub lat: Option<f64>,
    /// Longitude in degrees, when placed.
    pub lng: Option<f64>,
}

/// Returns the valid entries and one message per skipped entry. A YAML syntax
/// error yields no entries and a single message.
#[must_use]
pub fn parse_endpoints(data: &[u8]) -> (Vec<EndpointSpec>, Vec<String>) {
    let document: Document = match serde_yaml::from_slice(data) {
        Ok(document) => document,
        Err(err) => return (Vec::new(), vec![format!("parse registry: {err}")]),
    };
    let mut specs = Vec::with_capacity(document.endpoints.len());
    let mut errors = Vec::new();
    let mut seen = BTreeSet::new();
    for (index, spec) in document.endpoints.into_iter().enumerate() {
        match validate(index, &spec, &seen) {
            Ok(()) => {
                seen.insert(spec.name.clone());
                specs.push(spec);
            },
            Err(reason) => errors.push(reason),
        }
    }
    (specs, errors)
}

/// Rejects an entry without a name or address, or whose name was already seen.
fn validate(index: usize, spec: &EndpointSpec, seen: &BTreeSet<String>) -> Result<(), String> {
    let name = &spec.name;
    if name.is_empty() {
        return Err(format!("endpoint index {index}: name is required"));
    }
    if spec.address.is_empty() {
        return Err(format!("endpoint index {index} ({name}): address is required"));
    }
    if seen.contains(name) {
        return Err(format!("endpoint index {index}: duplicate name {name:?}"));
    }
    Ok(())
}

/// Resolves every entry to a site. Explicit `lat`/`lng` labels win, then the
/// region table; a site that resolves neither is kept without coordinates.
#[must_use]
pub fn build(specs: &[EndpointSpec]) -> Vec<Site> {
    specs.iter().map(resolve).collect()
}

/// Resolves one entry.
fn resolve(spec: &EndpointSpec) -> Site {
    let label = |key: &str| spec.labels.get(key).filter(|value| !value.is_empty()).cloned();
    let name = spec.name.clone();
    let region = label("region").unwrap_or_default();
    let coordinates = explicit_coordinates(&spec.labels).or_else(|| geocode::lookup(&region));
    Site {
        display_name: label("displayName").unwrap_or_else(|| name.clone()),
        cluster_value: label("clusterLabel").unwrap_or_else(|| name.clone()),
        dc: label("dc").unwrap_or_default(),
        metrics_url: label("metricsURL").unwrap_or_default(),
        metrics_secret: label("metricsSecret").unwrap_or_default(),
        address: spec.address.clone(),
        lat: coordinates.map(|at| at.latitude),
        lng: coordinates.map(|at| at.longitude),
        name,
        region,
    }
}

/// Coordinates from `lat`/`lng` labels, when both are present and numeric.
fn explicit_coordinates(labels: &BTreeMap<String, String>) -> Option<Coordinates> {
    let latitude = labels.get("lat")?.parse().ok()?;
    let longitude = labels.get("lng")?.parse().ok()?;
    Some(Coordinates { latitude, longitude })
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "test assertions on known document structure")]
mod tests {
    use super::{Site, build, parse_endpoints};

    const SAMPLE: &str = r#"
endpoints:
  - name: spoke1
    namespace: clusters
    address: gw.spoke1.example
    port: "443"
    labels:
      region: us-east-2
      dc: aws-us-east-2
      metricsURL: https://thanos.spoke1.example
      metricsSecret: site-spoke1
      displayName: Ohio
  - name: spoke2
    address: gw.spoke2.example
    port: "443"
    labels:
      region: nowhere-9
      lat: "10.5"
      lng: "-20.25"
      metricsURL: https://thanos.spoke2.example
      metricsSecret: site-spoke2
  - name: spoke3
    address: gw.spoke3.example
    port: "443"
    labels:
      region: nowhere-9
      metricsURL: https://thanos.spoke3.example
      metricsSecret: site-spoke3
  - name: ""
    address: bad.example
  - name: spoke1
    address: dup.example
"#;

    const CLUSTER_LABEL_SAMPLE: &str = "
endpoints:
  - name: spoke1
    address: gw1
    port: '443'
    labels: {region: us-east-2}
  - name: spoke2
    address: gw2
    port: '443'
    labels: {region: us-west-2, clusterLabel: override-value}
";

    #[test]
    fn valid_entries_are_kept_and_each_bad_entry_yields_one_error() {
        let (specs, errors) = parse_endpoints(SAMPLE.as_bytes());
        assert_eq!(specs.len(), 3, "three valid entries: {specs:?}");
        assert_eq!(errors.len(), 2, "one error per skipped entry: {errors:?}");
        assert!(
            errors[0].contains("index 3"),
            "the nameless entry is reported by index: {}",
            errors[0]
        );
        assert!(
            errors[1].contains("duplicate"),
            "the repeated name is reported: {}",
            errors[1]
        );
    }


    #[test]
    fn a_yaml_syntax_error_yields_no_entries_and_one_error() {
        let (specs, errors) = parse_endpoints(b"endpoints: [oops");
        assert!(specs.is_empty(), "no entries from a broken document");
        assert_eq!(errors.len(), 1, "exactly one parse error: {errors:?}");
    }

    #[test]
    fn a_known_region_is_geocoded() {
        let site = &sites()[0];
        assert_eq!((site.lat, site.lng), (Some(40.09), Some(-82.75)), "{site:?}");
        assert_eq!(site.display_name, "Ohio", "displayName label is used");
    }

    #[test]
    fn explicit_coordinates_beat_the_region_table() {
        let site = &sites()[1];
        assert_eq!((site.lat, site.lng), (Some(10.5), Some(-20.25)), "{site:?}");
        assert_eq!(site.display_name, "spoke2", "display name falls back to the name");
    }

    #[test]
    fn an_unknown_region_is_kept_but_unplaced() {
        let site = &sites()[2];
        assert_eq!((site.lat, site.lng), (None, None), "{site:?}");
        assert_eq!(site.display_name, "spoke3", "display name falls back to the name");
    }

    #[test]
    fn labels_and_address_are_copied_onto_the_site() {
        let site = &sites()[0];
        assert_eq!(site.metrics_url, "https://thanos.spoke1.example", "metricsURL");
        assert_eq!(site.metrics_secret, "site-spoke1", "metricsSecret");
        assert_eq!(site.dc, "aws-us-east-2", "dc");
        assert_eq!(site.address, "gw.spoke1.example", "address");
    }

    #[test]
    fn cluster_value_defaults_to_the_name_and_honors_the_override() {
        let sites = build(&parse_endpoints(CLUSTER_LABEL_SAMPLE.as_bytes()).0);
        assert_eq!(sites[0].cluster_value, "spoke1", "default is the site name");
        assert_eq!(sites[1].cluster_value, "override-value", "clusterLabel label overrides");
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    fn sites() -> Vec<Site> {
        build(&parse_endpoints(SAMPLE.as_bytes()).0)
    }
}
