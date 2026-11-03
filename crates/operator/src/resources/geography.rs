//! Site-level geography and load-aware admission for routing overlays.
//!
//! Provides `LocalityTier` (site distance classification),
//! `AdmissionState` (bounded load admission), and helpers to derive
//! both from [`GridSite`] geography and [`BackendMetrics`].
//!
//! All functions are pure: no I/O, no Kubernetes calls.  The overlay
//! renderer in the `routing_overlay` module calls these during candidate
//! enrichment, before the final ordering pass.
//!
//! [`GridSite`]: crate::crd::grid_site::GridSite
//! [`BackendMetrics`]: scoring::BackendMetrics

use serde::{Deserialize, Serialize};

use crate::crd::{
    grid_network::{GatewayRef, REGIONAL_POLICY_MAX_VALUE_LENGTH},
    grid_site::GridSite,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Queue depth above which a provider is considered saturated.
const QUEUE_DEPTH_SATURATION: f64 = 0.85;

/// KV-cache utilisation above which a provider is considered saturated.
const KV_CACHE_SATURATION: f64 = 0.90;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Distance classification between consumer and provider sites.
///
/// Derived from [`GridSite`] `spec.region` and `spec.zone` fields.
/// Declaration order matches the desired sort order (closest first),
/// so the derived [`Ord`] implementation orders correctly.
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
#[expect(unnameable_types, reason = "pub(crate) module restricts reachability")]
pub enum LocalityTier {
    /// Consumer and provider are the same named site.
    SameSite,
    /// Same region **and** same zone.
    SameZone,
    /// Same region, different zone.
    SameRegion,
    /// Different regions.
    CrossRegion,
    /// Geography could not be determined.
    Unknown,
}

/// Bounded admission state derived from provider health and capacity.
///
/// Declaration order matches the desired sort order so the derived
/// [`Ord`] places `NewAndExisting` before `ExistingOnly` before
/// `Excluded`.
///
/// This is threshold-based admission from current metrics only.
/// Hysteresis, hold-down, CAS, and shared active-active state are
/// future work.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
#[expect(unnameable_types, reason = "pub(crate) module restricts reachability")]
pub enum AdmissionState {
    /// Accepts new sessions and established sessions.
    NewAndExisting,
    /// Preserves established sessions only; no new sessions.
    ExistingOnly,
    /// Not eligible for routing.  Serialises as `"none"`.
    #[serde(rename = "none")]
    Excluded,
}

/// Resolution of one trusted regional metadata field.
///
/// The states are intentionally distinct so later policy decisions can fail
/// closed with bounded, machine-readable reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionalPolicyMetadataResolution<'site> {
    /// Every matching `GridSite` declares the same valid exact value.
    Resolved(&'site str),
    /// Matching `GridSite` records exist but all omit the field.
    Missing,
    /// No `GridSite` in the network matches the requested routing site key.
    Unknown,
    /// Matching records agree that the field is present but malformed.
    Malformed,
    /// Matching records disagree on the field's value or presence.
    Conflicting,
}

/// Trusted regional metadata resolved independently for one routing site key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionalPolicySiteMetadata<'site> {
    /// Trusted deployment region resolution.
    pub(crate) region: RegionalPolicyMetadataResolution<'site>,
    /// Trusted sovereignty-zone resolution.
    pub(crate) sovereignty_zone: RegionalPolicyMetadataResolution<'site>,
}

/// Internal observation used while reconciling duplicate site-key matches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegionalPolicyFieldObservation<'site> {
    /// The field is absent.
    Missing,
    /// The field is present but invalid for policy matching.
    Malformed,
    /// The field contains a valid exact value.
    Resolved(&'site str),
}

// ---------------------------------------------------------------------------
// Derivation functions
// ---------------------------------------------------------------------------

/// Classify the distance between consumer and provider sites.
///
/// Compares site names first, then resolves geography from the
/// `&[GridSite]` inventory.  Zone comparison requires a region match
/// because zone names are not globally unique.
pub(crate) fn derive_locality_tier(
    consumer_site: &str,
    provider_site: &str,
    sites: &[GridSite],
    network_name: &str,
) -> LocalityTier {
    if consumer_site == provider_site {
        return LocalityTier::SameSite;
    }
    let (c_region, c_zone) = resolve_site_geography(consumer_site, sites, network_name);
    let (p_region, p_zone) = resolve_site_geography(provider_site, sites, network_name);
    match (c_region, p_region) {
        (Some(cr), Some(pr)) if cr == pr => {
            if c_zone.is_some() && c_zone == p_zone {
                LocalityTier::SameZone
            } else {
                LocalityTier::SameRegion
            }
        },
        (Some(_), Some(_)) => LocalityTier::CrossRegion,
        _ => LocalityTier::Unknown,
    }
}

/// Look up region and zone for a site in a network, keyed as candidates and peers name it:
/// a discovered site by its site id, not its object name.
fn resolve_site_geography<'site>(
    site_name: &str,
    sites: &'site [GridSite],
    network_name: &str,
) -> (Option<&'site str>, Option<&'site str>) {
    sites
        .iter()
        .filter(|s| s.spec.grid_network_ref == network_name)
        .find(|s| crate::controller::grid_network::peer_site_key(s).is_some_and(|(key, _)| key == site_name))
        .map_or((None, None), |s| (s.spec.region.as_deref(), s.spec.zone.as_deref()))
}

/// Resolve trusted region and sovereignty metadata for a routing site key.
///
/// Matching is network-scoped and uses the same routing identity as candidates
/// and peers: a declared site's object name or an auto-discovered site's
/// `site_id`. Duplicate matches resolve only when every record declares the
/// same exact valid value. Missing, malformed, and differing observations are
/// never normalized or selected by inventory order.
pub(crate) fn resolve_regional_policy_site_metadata<'site>(
    site_name: &str,
    sites: &'site [GridSite],
    network_name: &str,
) -> RegionalPolicySiteMetadata<'site> {
    RegionalPolicySiteMetadata {
        region: resolve_regional_policy_field(site_name, sites, network_name, |site| site.spec.region.as_deref()),
        sovereignty_zone: resolve_regional_policy_field(site_name, sites, network_name, |site| {
            site.spec.sovereignty_zone.as_deref()
        }),
    }
}

/// Resolve the trusted metadata used by a gateway's default affinity context.
///
/// The explicit [`GatewayRef::site_name`] wins. When it is absent, the current
/// single-site compatibility fallback uses the [`GridNetwork`] name.
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
pub(crate) fn resolve_gateway_regional_policy_metadata<'site>(
    gateway: &GatewayRef,
    sites: &'site [GridSite],
    network_name: &str,
) -> RegionalPolicySiteMetadata<'site> {
    resolve_regional_policy_site_metadata(
        gateway.site_name.as_deref().unwrap_or(network_name),
        sites,
        network_name,
    )
}

/// Resolve one field across all records matching a routing site key.
fn resolve_regional_policy_field<'site>(
    site_name: &str,
    sites: &'site [GridSite],
    network_name: &str,
    field: impl Fn(&'site GridSite) -> Option<&'site str>,
) -> RegionalPolicyMetadataResolution<'site> {
    let mut sites = sites.iter().filter(|site| {
        site.spec.grid_network_ref == network_name
            && crate::controller::grid_network::peer_site_key(site).is_some_and(|(key, _)| key == site_name)
    });
    let Some(first_site) = sites.next() else {
        return RegionalPolicyMetadataResolution::Unknown;
    };
    let first = observe_regional_policy_field(field(first_site));
    if sites.any(|site| observe_regional_policy_field(field(site)) != first) {
        return RegionalPolicyMetadataResolution::Conflicting;
    }
    match first {
        RegionalPolicyFieldObservation::Missing => RegionalPolicyMetadataResolution::Missing,
        RegionalPolicyFieldObservation::Malformed => RegionalPolicyMetadataResolution::Malformed,
        RegionalPolicyFieldObservation::Resolved(value) => RegionalPolicyMetadataResolution::Resolved(value),
    }
}

/// Classify a declared regional metadata value without normalization.
fn observe_regional_policy_field(value: Option<&str>) -> RegionalPolicyFieldObservation<'_> {
    match value {
        None => RegionalPolicyFieldObservation::Missing,
        Some(value)
            if value.is_empty()
                || value.chars().any(char::is_whitespace)
                || value.chars().count() > REGIONAL_POLICY_MAX_VALUE_LENGTH =>
        {
            RegionalPolicyFieldObservation::Malformed
        },
        Some(value) => RegionalPolicyFieldObservation::Resolved(value),
    }
}

/// Derive admission state from provider health and capacity metrics.
///
/// | Condition | State |
/// |-----------|-------|
/// | No metrics | `NewAndExisting` |
/// | `!healthy` | `Excluded` |
/// | queue\_depth > 0.85 or kv\_cache > 0.90 | `ExistingOnly` |
/// | Otherwise | `NewAndExisting` |
pub(crate) fn derive_admission_state(metrics: Option<&scoring::BackendMetrics>) -> AdmissionState {
    let Some(m) = metrics else {
        return AdmissionState::NewAndExisting;
    };
    if !m.healthy {
        return AdmissionState::Excluded;
    }
    if m.queue_depth > QUEUE_DEPTH_SATURATION || m.kv_cache_utilization > KV_CACHE_SATURATION {
        return AdmissionState::ExistingOnly;
    }
    AdmissionState::NewAndExisting
}

/// Apply administrative drain without overriding a hard exclusion.
///
/// This is deliberately pure: health, staleness, trust, and exclusion are
/// resolved before this cap is applied. Clearing drain returns the supplied
/// health/metrics-derived state unchanged.
pub(crate) fn apply_administrative_drain(state: AdmissionState, drain: bool) -> AdmissionState {
    if drain && state == AdmissionState::NewAndExisting {
        AdmissionState::ExistingOnly
    } else {
        state
    }
}

/// Deterministic stable ID for a routing candidate.
///
/// Computed as `fnv1a_hex8("{kind}/{name}/{site}/{cluster}")`.
/// Suitable for consumer-side session binding keys.
pub(crate) fn compute_stable_id(kind: &str, name: &str, site: &str, cluster: &str) -> String {
    super::routing_overlay::fnv1a_hex8(&format!("{kind}/{name}/{site}/{cluster}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "tests"
)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Test utilities
    // -----------------------------------------------------------------------

    #[test]
    fn a_discovered_site_is_found_by_its_site_id() {
        let mut site = test_site_with_geography("grid-retail", "net", Some("us-east"), Some("a"));
        site.metadata.labels = Some(
            [(
                crate::controller::grid_network::LABEL_AUTO_DISCOVERED.to_owned(),
                "true".to_owned(),
            )]
            .into_iter()
            .collect(),
        );
        site.metadata.annotations = Some(
            [(
                crate::controller::grid_network::ANNOTATION_SITE_ID.to_owned(),
                "retail".to_owned(),
            )]
            .into_iter()
            .collect(),
        );
        let sites = [site];
        assert_eq!(
            resolve_site_geography("retail", &sites, "net"),
            (Some("us-east"), Some("a")),
            "candidates carry the site id"
        );
        assert_eq!(resolve_site_geography("grid-retail", &sites, "net"), (None, None));
    }

    // -----------------------------------------------------------------------
    // Regional policy metadata resolution
    // -----------------------------------------------------------------------

    #[test]
    fn regional_policy_metadata_resolves_region_and_sovereignty_independently() {
        let sites = [test_site_with_metadata(
            "site-a",
            "net",
            Some("us-east"),
            Some("regulated-us"),
            Some("a"),
        )];
        assert_eq!(
            resolve_regional_policy_site_metadata("site-a", &sites, "net"),
            RegionalPolicySiteMetadata {
                region: RegionalPolicyMetadataResolution::Resolved("us-east"),
                sovereignty_zone: RegionalPolicyMetadataResolution::Resolved("regulated-us"),
            }
        );
    }

    #[test]
    fn regional_policy_metadata_distinguishes_unknown_site_from_missing_fields() {
        let sites = [test_site_with_metadata("site-a", "net", None, None, None)];
        assert_eq!(
            resolve_regional_policy_site_metadata("unknown", &sites, "net"),
            RegionalPolicySiteMetadata {
                region: RegionalPolicyMetadataResolution::Unknown,
                sovereignty_zone: RegionalPolicyMetadataResolution::Unknown,
            }
        );
        assert_eq!(
            resolve_regional_policy_site_metadata("site-a", &sites, "net"),
            RegionalPolicySiteMetadata {
                region: RegionalPolicyMetadataResolution::Missing,
                sovereignty_zone: RegionalPolicyMetadataResolution::Missing,
            }
        );
    }

    #[test]
    fn regional_policy_metadata_is_network_scoped() {
        let sites = [
            test_site_with_metadata("site-a", "net-a", Some("east"), Some("eu"), None),
            test_site_with_metadata("site-a", "net-b", Some("west"), Some("us"), None),
        ];
        assert_eq!(
            resolve_regional_policy_site_metadata("site-a", &sites, "net-a"),
            RegionalPolicySiteMetadata {
                region: RegionalPolicyMetadataResolution::Resolved("east"),
                sovereignty_zone: RegionalPolicyMetadataResolution::Resolved("eu"),
            }
        );
    }

    #[test]
    fn regional_policy_metadata_uses_discovered_site_id() {
        let site = with_site_id(
            test_site_with_metadata("grid-retail", "net", Some("us-east"), Some("regulated-us"), None),
            "retail",
        );
        let sites = [site];
        assert_eq!(
            resolve_regional_policy_site_metadata("retail", &sites, "net").region,
            RegionalPolicyMetadataResolution::Resolved("us-east")
        );
        assert_eq!(
            resolve_regional_policy_site_metadata("grid-retail", &sites, "net").region,
            RegionalPolicyMetadataResolution::Unknown
        );
    }

    #[test]
    fn regional_policy_metadata_rejects_malformed_values_without_normalizing() {
        let sites = [test_site_with_metadata(
            "site-a",
            "net",
            Some("us east"),
            Some(&"x".repeat(REGIONAL_POLICY_MAX_VALUE_LENGTH + 1)),
            None,
        )];
        assert_eq!(
            resolve_regional_policy_site_metadata("site-a", &sites, "net"),
            RegionalPolicySiteMetadata {
                region: RegionalPolicyMetadataResolution::Malformed,
                sovereignty_zone: RegionalPolicyMetadataResolution::Malformed,
            }
        );
    }

    #[test]
    fn duplicate_site_keys_resolve_only_when_metadata_agrees() {
        let site_a = with_site_id(
            test_site_with_metadata("grid-a", "net", Some("east"), Some("eu"), None),
            "shared",
        );
        let site_b = with_site_id(
            test_site_with_metadata("grid-b", "net", Some("east"), Some("eu"), None),
            "shared",
        );
        let sites = [site_a, site_b];
        assert_eq!(
            resolve_regional_policy_site_metadata("shared", &sites, "net"),
            RegionalPolicySiteMetadata {
                region: RegionalPolicyMetadataResolution::Resolved("east"),
                sovereignty_zone: RegionalPolicyMetadataResolution::Resolved("eu"),
            }
        );
    }

    #[test]
    fn duplicate_site_keys_report_conflicting_fields_independently() {
        let site_a = with_site_id(
            test_site_with_metadata("grid-a", "net", Some("east"), Some("eu"), None),
            "shared",
        );
        let site_b = with_site_id(
            test_site_with_metadata("grid-b", "net", Some("EAST"), Some("eu"), None),
            "shared",
        );
        let sites = [site_a, site_b];
        assert_eq!(
            resolve_regional_policy_site_metadata("shared", &sites, "net"),
            RegionalPolicySiteMetadata {
                region: RegionalPolicyMetadataResolution::Conflicting,
                sovereignty_zone: RegionalPolicyMetadataResolution::Resolved("eu"),
            },
            "case differences must not be normalized"
        );
    }

    #[test]
    fn duplicate_site_key_presence_disagreement_is_conflicting() {
        let site_a = with_site_id(
            test_site_with_metadata("grid-a", "net", Some("east"), Some("eu"), None),
            "shared",
        );
        let site_b = with_site_id(
            test_site_with_metadata("grid-b", "net", None, Some("eu"), None),
            "shared",
        );
        let sites = [site_a, site_b];
        assert_eq!(
            resolve_regional_policy_site_metadata("shared", &sites, "net").region,
            RegionalPolicyMetadataResolution::Conflicting
        );
    }

    #[test]
    fn duplicate_site_key_valid_and_malformed_values_are_conflicting() {
        let valid = with_site_id(
            test_site_with_metadata("grid-a", "net", Some("east"), Some("eu"), None),
            "shared",
        );
        let malformed = with_site_id(
            test_site_with_metadata("grid-b", "net", Some("east west"), Some("eu"), None),
            "shared",
        );
        for sites in [[valid.clone(), malformed.clone()], [malformed, valid]] {
            assert_eq!(
                resolve_regional_policy_site_metadata("shared", &sites, "net").region,
                RegionalPolicyMetadataResolution::Conflicting,
                "conflict detection must not depend on inventory order"
            );
        }
    }

    #[test]
    fn gateway_metadata_prefers_site_name_and_falls_back_to_network_name() {
        let sites = [
            test_site_with_metadata("net", "net", Some("default"), Some("global"), None),
            test_site_with_metadata("edge-east", "net", Some("east"), Some("eu"), None),
        ];
        let explicit: GatewayRef = serde_json::from_value(serde_json::json!({
            "name": "gw-east",
            "namespace": "grid-system",
            "siteName": "edge-east"
        }))
        .unwrap_or_else(|_| std::process::abort());
        let fallback: GatewayRef = serde_json::from_value(serde_json::json!({
            "name": "gw-default",
            "namespace": "grid-system"
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            resolve_gateway_regional_policy_metadata(&explicit, &sites, "net").region,
            RegionalPolicyMetadataResolution::Resolved("east")
        );
        assert_eq!(
            resolve_gateway_regional_policy_metadata(&fallback, &sites, "net").region,
            RegionalPolicyMetadataResolution::Resolved("default")
        );
    }

    fn test_site_with_geography(name: &str, network: &str, region: Option<&str>, zone: Option<&str>) -> GridSite {
        test_site_with_metadata(name, network, region, None, zone)
    }

    fn test_site_with_metadata(
        name: &str,
        network: &str,
        region: Option<&str>,
        sovereignty_zone: Option<&str>,
        zone: Option<&str>,
    ) -> GridSite {
        let mut spec = serde_json::json!({ "gridNetworkRef": network });
        if let Some(r) = region {
            spec["region"] = serde_json::json!(r);
        }
        if let Some(sovereignty_zone) = sovereignty_zone {
            spec["sovereigntyZone"] = serde_json::json!(sovereignty_zone);
        }
        if let Some(z) = zone {
            spec["zone"] = serde_json::json!(z);
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": name },
            "spec": spec
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn with_site_id(mut site: GridSite, site_id: &str) -> GridSite {
        site.metadata.labels = Some(
            [(
                crate::controller::grid_network::LABEL_AUTO_DISCOVERED.to_owned(),
                "true".to_owned(),
            )]
            .into_iter()
            .collect(),
        );
        site.metadata.annotations = Some(
            [(
                crate::controller::grid_network::ANNOTATION_SITE_ID.to_owned(),
                site_id.to_owned(),
            )]
            .into_iter()
            .collect(),
        );
        site
    }

    // -----------------------------------------------------------------------
    // derive_locality_tier
    // -----------------------------------------------------------------------

    #[test]
    fn locality_same_site() {
        let sites = [test_site_with_geography(
            "site-a",
            "net",
            Some("us-east"),
            Some("us-east-1a"),
        )];
        let tier = derive_locality_tier("site-a", "site-a", &sites, "net");
        assert_eq!(tier, LocalityTier::SameSite, "same site name must be SameSite");
    }

    #[test]
    fn locality_same_zone() {
        let sites = [
            test_site_with_geography("site-a", "net", Some("us-east"), Some("us-east-1a")),
            test_site_with_geography("site-b", "net", Some("us-east"), Some("us-east-1a")),
        ];
        let tier = derive_locality_tier("site-a", "site-b", &sites, "net");
        assert_eq!(tier, LocalityTier::SameZone, "same region and zone must be SameZone");
    }

    #[test]
    fn locality_same_zone_requires_same_region() {
        let sites = [
            test_site_with_geography("site-a", "net", Some("us-east"), Some("az-1")),
            test_site_with_geography("site-b", "net", Some("us-west"), Some("az-1")),
        ];
        let tier = derive_locality_tier("site-a", "site-b", &sites, "net");
        assert_eq!(
            tier,
            LocalityTier::CrossRegion,
            "same zone name but different region must not be SameZone"
        );
    }

    #[test]
    fn locality_same_region() {
        let sites = [
            test_site_with_geography("site-a", "net", Some("us-east"), Some("us-east-1a")),
            test_site_with_geography("site-b", "net", Some("us-east"), Some("us-east-1b")),
        ];
        let tier = derive_locality_tier("site-a", "site-b", &sites, "net");
        assert_eq!(
            tier,
            LocalityTier::SameRegion,
            "same region different zone must be SameRegion"
        );
    }

    #[test]
    fn locality_cross_region() {
        let sites = [
            test_site_with_geography("site-a", "net", Some("us-east"), Some("us-east-1a")),
            test_site_with_geography("site-b", "net", Some("eu-west"), Some("eu-west-1a")),
        ];
        let tier = derive_locality_tier("site-a", "site-b", &sites, "net");
        assert_eq!(tier, LocalityTier::CrossRegion, "different regions must be CrossRegion");
    }

    #[test]
    fn locality_unknown_consumer_missing() {
        let sites = [test_site_with_geography(
            "site-b",
            "net",
            Some("us-east"),
            Some("us-east-1a"),
        )];
        let tier = derive_locality_tier("missing", "site-b", &sites, "net");
        assert_eq!(tier, LocalityTier::Unknown, "missing consumer site must be Unknown");
    }

    #[test]
    fn locality_unknown_no_geography() {
        let sites = [
            test_site_with_geography("site-a", "net", None, None),
            test_site_with_geography("site-b", "net", None, None),
        ];
        let tier = derive_locality_tier("site-a", "site-b", &sites, "net");
        assert_eq!(
            tier,
            LocalityTier::Unknown,
            "no geography on either site must be Unknown"
        );
    }

    // -----------------------------------------------------------------------
    // derive_admission_state
    // -----------------------------------------------------------------------

    #[test]
    fn admission_new_and_existing_healthy() {
        let m = scoring::BackendMetrics::new(0.0, true, 0.5, 100.0, 0.5, 0.3);
        assert_eq!(
            derive_admission_state(Some(&m)),
            AdmissionState::NewAndExisting,
            "healthy with low load must be NewAndExisting"
        );
    }

    #[test]
    fn admission_new_and_existing_no_metrics() {
        assert_eq!(
            derive_admission_state(None),
            AdmissionState::NewAndExisting,
            "absent metrics must default to NewAndExisting"
        );
    }

    #[test]
    fn admission_existing_only_queue() {
        let m = scoring::BackendMetrics::new(0.0, true, 0.5, 100.0, 0.5, 0.9);
        assert_eq!(
            derive_admission_state(Some(&m)),
            AdmissionState::ExistingOnly,
            "queue_depth > 0.85 must be ExistingOnly"
        );
    }

    #[test]
    fn admission_existing_only_kv_cache() {
        let m = scoring::BackendMetrics::new(0.0, true, 0.95, 100.0, 0.5, 0.3);
        assert_eq!(
            derive_admission_state(Some(&m)),
            AdmissionState::ExistingOnly,
            "kv_cache > 0.90 must be ExistingOnly"
        );
    }

    #[test]
    fn admission_excluded_unhealthy() {
        let m = scoring::BackendMetrics::new(0.5, false, 0.5, 100.0, 0.5, 0.3);
        assert_eq!(
            derive_admission_state(Some(&m)),
            AdmissionState::Excluded,
            "unhealthy must be Excluded"
        );
    }

    #[test]
    fn administrative_drain_caps_only_eligible_providers() {
        assert_eq!(
            apply_administrative_drain(AdmissionState::NewAndExisting, true),
            AdmissionState::ExistingOnly
        );
        assert_eq!(
            apply_administrative_drain(AdmissionState::ExistingOnly, true),
            AdmissionState::ExistingOnly
        );
        assert_eq!(
            apply_administrative_drain(AdmissionState::Excluded, true),
            AdmissionState::Excluded
        );
    }

    #[test]
    fn clearing_administrative_drain_preserves_derived_state() {
        assert_eq!(
            apply_administrative_drain(AdmissionState::NewAndExisting, false),
            AdmissionState::NewAndExisting
        );
        assert_eq!(
            apply_administrative_drain(AdmissionState::ExistingOnly, false),
            AdmissionState::ExistingOnly
        );
        assert_eq!(
            apply_administrative_drain(AdmissionState::Excluded, false),
            AdmissionState::Excluded
        );
    }

    // -----------------------------------------------------------------------
    // compute_stable_id
    // -----------------------------------------------------------------------

    #[test]
    fn stable_id_deterministic() {
        let a = compute_stable_id("inference_model", "model-a", "site-x", "cluster-1");
        let b = compute_stable_id("inference_model", "model-a", "site-x", "cluster-1");
        assert_eq!(a, b, "same inputs must produce same hash");
    }

    #[test]
    fn stable_id_differs() {
        let base = compute_stable_id("inference_model", "model-a", "site-x", "cluster-1");
        let changed = compute_stable_id("inference_model", "model-b", "site-x", "cluster-1");
        assert_ne!(base, changed, "different name must produce different hash");
    }

    // -----------------------------------------------------------------------
    // serde round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn locality_tier_serializes_snake_case() {
        let json = serde_json::to_string(&LocalityTier::SameSite).unwrap();
        assert_eq!(json, "\"same_site\"", "SameSite must serialize as same_site");
    }

    #[test]
    fn admission_excluded_serializes_as_none() {
        let json = serde_json::to_string(&AdmissionState::Excluded).unwrap();
        assert_eq!(json, "\"none\"", "Excluded must serialize as none");
    }
}
