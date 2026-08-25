//! Site-level geography and load-aware admission for routing overlays.
//!
//! Provides `LocalityTier` (site distance classification),
//! `AdmissionState` (bounded load admission), and helpers to derive
//! both from [`GridSite`] geography and [`BackendMetrics`].
//!
//! All functions are pure — no I/O, no Kubernetes calls.  The overlay
//! renderer in the `routing_overlay` module calls these during candidate
//! enrichment, before the final ordering pass.
//!
//! [`GridSite`]: crate::crd::grid_site::GridSite
//! [`BackendMetrics`]: scoring::BackendMetrics

use serde::{Deserialize, Serialize};

use crate::crd::grid_site::GridSite;

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
/// Declaration order matches the desired sort order — closest first —
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
    /// Preserves established sessions only — no new sessions.
    ExistingOnly,
    /// Not eligible for routing.  Serialises as `"none"`.
    #[serde(rename = "none")]
    Excluded,
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

/// Look up region and zone for a named site in a network.
fn resolve_site_geography<'site>(
    site_name: &str,
    sites: &'site [GridSite],
    network_name: &str,
) -> (Option<&'site str>, Option<&'site str>) {
    sites
        .iter()
        .find(|s| s.metadata.name.as_deref() == Some(site_name) && s.spec.grid_network_ref == network_name)
        .map_or((None, None), |s| (s.spec.region.as_deref(), s.spec.zone.as_deref()))
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

    fn test_site_with_geography(name: &str, network: &str, region: Option<&str>, zone: Option<&str>) -> GridSite {
        let mut spec = serde_json::json!({ "gridNetworkRef": network });
        if let Some(r) = region {
            spec["region"] = serde_json::json!(r);
        }
        if let Some(z) = zone {
            spec["zone"] = serde_json::json!(z);
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": name },
            "spec": spec
        }))
        .unwrap_or_else(|_| std::process::abort())
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
