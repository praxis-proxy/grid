//! Pure overlay renderer for Praxis `grid_route` routing candidates.
//!
//! Converts [`GridNetwork`], [`GridSite`], and [`InferenceProvider`]
//! CRDs into a `RoutingOverlay` that is serialised into a Kubernetes
//! `ConfigMap`.  Praxis reads this `ConfigMap` to configure its
//! `grid_route` filter with routing candidates.
//!
//! This renderer is **pure**: it accepts already-fetched CRD data and
//! produces structured output.  No Kubernetes API calls are made inside
//! this module.
//!
//! # Phase 1 / OP-01 semantics
//!
//! - [`GridSite`]s are used to resolve per-provider site membership via `spec.siteSelector.matchLabels`.  An empty
//!   selector matches all sites in the same [`GridNetwork`].
//! - Each `(model, site)` pair becomes one `RoutingCandidate`.
//! - `candidate.site` = the [`GridSite`] name (resolved via selector).
//! - `candidate.cluster` = the [`InferenceProvider`] metadata name. This is a **Phase 1 placeholder**.  Praxis uses the
//!   cluster name to look up the corresponding `load_balancer` cluster in its config. Local validation can wire this by
//!   naming the Praxis load-balancer cluster after the provider.  Production will derive this from `spec.endpoint` or
//!   an explicit Praxis cluster reference once OP-04 proves the consumption path.
//! - When no [`GridSite`]s are provided, the provider name is used as both `site` and `cluster` (Phase 1 self-hosted
//!   fallback).
//!
//! # Spec-based vs status-based site derivation
//!
//! The renderer derives `candidate.site` from `spec.siteSelector.matchLabels`
//! against live [`GridSite`] data, **not** from
//! `status.matchingSites`.  `status.matchingSites` is set asynchronously by
//! the OP-02 `InferenceProvider` controller and may be stale if sites or
//! labels changed since the last `InferenceProvider` reconcile.  Re-deriving
//! from spec guarantees freshness relative to the current `GridNetwork`
//! reconcile cycle.  `status.matchingSites` exists for human observability
//! and external tools, not as the overlay's authoritative input.
//!
//! # ConfigMap contract
//!
//! - Name: `grid-overlay-{network}-{gateway}` (≤ 63 chars). Long names receive a deterministic FNV-1a hash suffix to
//!   avoid collisions.
//! - Data key: `grid-config.json`
//! - Serialization failures are returned as errors, not silently defaulted.
//!
//! [`GridNetwork`]: crate::crd::grid_network::GridNetwork
//! [`GridSite`]: crate::crd::grid_site::GridSite
//! [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::core::v1::ConfigMap;
use serde::{Deserialize, Serialize};

use crate::crd::{
    grid_network::GridNetwork,
    grid_site::GridSite,
    inference_provider::{InferenceProvider, ProviderPhase},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum length for a Kubernetes resource name (DNS label limit).
const MAX_K8S_NAME: usize = 63;

/// Maximum prefix length for each component when a hash suffix is needed.
const MAX_COMPONENT_PREFIX: usize = 20;

/// Candidate kind identifier for inference model entries.
const CANDIDATE_KIND: &str = "inference_model";

// ---------------------------------------------------------------------------
// Site resolution
// ---------------------------------------------------------------------------

/// Resolution of matching sites for a single provider.
///
/// This enum distinguishes two semantically different "empty" cases so that
/// the candidate generator cannot accidentally apply the legacy fallback when
/// a real site inventory exists.
///
/// | Variant | Meaning | Candidate action |
/// |---------|---------|-----------------|
/// | `Unavailable` | No [`GridSite`] CRDs in the network | Use provider name as site (Phase 1 fallback) |
/// | `Known(empty)` | CRDs exist but selector matched none | Emit **no** candidates |
/// | `Known(names)` | Selector matched these sites | Emit one candidate per `(model, site)` |
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
enum SiteResolution {
    /// No [`GridSite`] CRDs were supplied to the renderer for this network.
    ///
    /// The provider name is used as the site identity.  This is the Phase 1
    /// self-hosted fallback and should only be used when the cluster has no
    /// site inventory at all.
    ///
    /// [`GridSite`]: crate::crd::grid_site::GridSite
    Unavailable,

    /// Site CRDs are available.  Contains the names matched by the provider's
    /// `siteSelector`.
    ///
    /// An empty `Vec` means the selector matched no sites; the provider
    /// contributes no candidates to the overlay.
    Known(Vec<String>),
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single routing candidate for the Praxis `grid_route` filter.
///
/// Each candidate represents one (model, site) pair offered by a provider.
/// Praxis uses the candidate list to select a backend cluster for each
/// inference request.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct RoutingCandidate {
    /// Candidate kind.  `"inference_model"` for inference providers; other
    /// variants (e.g. `"mcp_tool"`) are defined by Praxis `grid_route`.
    pub kind: String,

    /// Model name as declared in the [`InferenceProvider`] spec.
    ///
    /// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
    pub name: String,

    /// Site name where this model is hosted.
    ///
    /// Resolved via `spec.siteSelector.matchLabels` against [`GridSite`]
    /// metadata labels.  Falls back to the provider metadata name when
    /// no [`GridSite`]s are passed (Phase 1 self-hosted fallback).
    ///
    /// [`GridSite`]: crate::crd::grid_site::GridSite
    pub site: String,

    /// Upstream cluster identifier.
    ///
    /// In Phase 1 (OP-01) this equals the [`InferenceProvider`] metadata
    /// name.  A future Praxis integration will map this to the Praxis
    /// cluster that serves the provider's `spec.endpoint`.
    ///
    /// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
    pub cluster: String,

    /// Whether this candidate's data is considered fresh.
    ///
    /// Always `true` for the static overlay.  Freshness updates arrive
    /// in OP-05 (metrics-to-snapshot loop).
    pub fresh: bool,
}

/// The full routing overlay for a single [`GridNetwork`].
///
/// Serialised as JSON under the `grid-config.json` key of the
/// overlay `ConfigMap`.
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RoutingOverlay {
    /// Name of the [`GridNetwork`] this overlay belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub network: String,

    /// Local site identifier.
    ///
    /// Supplied per gateway by the controller as
    /// `gw_ref.local_site_name.as_deref().unwrap_or(network_name)`.
    /// Each `GatewayRef` may declare its own `localSiteName`, allowing
    /// multi-gateway networks to produce overlays with distinct
    /// `local_site` values.  Falls back to the network name for
    /// single-site networks.
    ///
    /// Praxis uses `local_site` to score candidates on the same site
    /// higher than remote candidates.
    pub local_site: String,

    /// Routing candidates, sorted deterministically by site then name.
    pub candidates: Vec<RoutingCandidate>,
}

// ---------------------------------------------------------------------------
// Renderer
// ---------------------------------------------------------------------------

/// Render a [`RoutingOverlay`] from CRD state.
///
/// Only [`InferenceProvider`]s whose `spec.gridNetworkRef` matches
/// `network.metadata.name` are included.  Each provider's
/// `spec.siteSelector.matchLabels` is matched against the supplied
/// `sites`; an empty selector matches all sites in the network.
///
/// The `local_site` parameter identifies this gateway's own site.
/// Praxis uses it to score candidates running on the local site higher
/// than remote candidates.  The caller is responsible for computing
/// `local_site` per gateway:
/// ```text
/// local_site = gw_ref.local_site_name.as_deref().unwrap_or(network_name)
/// ```
///
/// Candidates are sorted deterministically by `(site, name, cluster)`.
/// Exact duplicates — same `(kind, name, site, cluster)` — are removed.
/// Two providers that serve the same model on the same site but with
/// different cluster identifiers are **not** deduplicated.
///
/// # Errors
///
/// Returns a descriptive `String` if:
/// - The network resource has no metadata name.
/// - Any eligible provider has no metadata name.
/// - Any model name in an eligible provider is blank or whitespace-only.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
pub fn render_routing_overlay(
    network: &GridNetwork,
    sites: &[GridSite],
    providers: &[InferenceProvider],
    local_site: &str,
) -> Result<RoutingOverlay, String> {
    let network_name = network
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| "GridNetwork has no name".to_owned())?;

    let mut candidates = collect_candidates(network_name, sites, providers)?;
    candidates.sort_by(|a, b| {
        a.site
            .cmp(&b.site)
            .then(a.name.cmp(&b.name))
            .then(a.cluster.cmp(&b.cluster))
    });
    candidates.dedup_by(|a, b| a.kind == b.kind && a.name == b.name && a.site == b.site && a.cluster == b.cluster);

    Ok(RoutingOverlay {
        network: network_name.to_owned(),
        local_site: local_site.to_owned(),
        candidates,
    })
}

/// Collect [`RoutingCandidate`]s from providers belonging to `network_name`.
///
/// Providers explicitly marked [`ProviderPhase::Unavailable`] in their status
/// are excluded.  Providers in any other phase (`Pending`, `Available`,
/// `Degraded`, or absent status) are included.  See [`is_explicitly_unavailable`]
/// for the rationale.
fn collect_candidates(
    network_name: &str,
    sites: &[GridSite],
    providers: &[InferenceProvider],
) -> Result<Vec<RoutingCandidate>, String> {
    // Pre-filter sites to those in this network.
    let network_sites: Vec<&GridSite> = sites
        .iter()
        .filter(|s| s.spec.grid_network_ref == network_name)
        .collect();

    let mut all: Vec<RoutingCandidate> = Vec::new();
    for provider in providers {
        if provider.spec.grid_network_ref != network_name {
            continue;
        }
        if is_explicitly_unavailable(provider) {
            continue;
        }
        let resolution = resolve_sites(provider, &network_sites);
        all.extend(candidates_from_provider(provider, &resolution)?);
    }
    Ok(all)
}

/// Resolve matching sites for a provider against the network site inventory.
///
/// Returns [`SiteResolution::Unavailable`] when no site inventory exists,
/// which enables the Phase 1 provider-name fallback.  Returns
/// [`SiteResolution::Known`] otherwise — with an empty `Vec` if the
/// selector matched nothing, which suppresses candidate generation.
fn resolve_sites(provider: &InferenceProvider, network_sites: &[&GridSite]) -> SiteResolution {
    if network_sites.is_empty() {
        return SiteResolution::Unavailable;
    }

    let selector = &provider.spec.site_selector.match_labels;

    let names: Vec<String> = network_sites
        .iter()
        .filter(|site| {
            let site_labels = site.metadata.labels.as_ref();
            selector
                .iter()
                .all(|(k, v)| site_labels.is_some_and(|labels| labels.get(k).is_some_and(|sv| sv == v)))
        })
        .map(|site| site.metadata.name.clone().unwrap_or_else(|| "unknown-site".to_owned()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    SiteResolution::Known(names)
}

/// Build one [`RoutingCandidate`] per `(model, site)` for a single provider.
///
/// The `site_resolution` parameter determines which sites this provider serves:
///
/// - [`SiteResolution::Unavailable`]: no site inventory exists; the provider name is used as the site identity (Phase 1
///   self-hosted fallback).
/// - [`SiteResolution::Known`] with a non-empty list: one candidate per `(model, site)` pair.
/// - [`SiteResolution::Known`] with an empty list: the provider's selector matched no sites; **no candidates are
///   emitted**.  This is distinct from `Unavailable` — it means the inventory exists but excluded this provider.
///
/// # Errors
///
/// Returns an error if the provider has no metadata name or any model
/// name is blank or whitespace-only.
fn candidates_from_provider(
    provider: &InferenceProvider,
    site_resolution: &SiteResolution,
) -> Result<Vec<RoutingCandidate>, String> {
    let provider_name = provider
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| "InferenceProvider has no name".to_owned())?;

    for model in &provider.spec.models {
        if model.name.trim().is_empty() {
            return Err(format!("provider {provider_name} has a blank model name"));
        }
    }

    let sites: Vec<&str> = match &site_resolution {
        // No site inventory at all → Phase 1 fallback: use provider name as site.
        SiteResolution::Unavailable => vec![provider_name],
        // Inventory exists but selector matched nothing → emit no candidates.
        SiteResolution::Known(names) if names.is_empty() => return Ok(Vec::new()),
        // Inventory exists and selector matched these sites.
        SiteResolution::Known(names) => names.iter().map(String::as_str).collect(),
    };

    let mut candidates = Vec::new();
    for model in &provider.spec.models {
        for site in &sites {
            candidates.push(RoutingCandidate {
                kind: CANDIDATE_KIND.to_owned(),
                name: model.name.clone(),
                site: (*site).to_owned(),
                cluster: provider_name.to_owned(),
                fresh: true,
            });
        }
    }
    Ok(candidates)
}

/// Returns `true` only when the provider's status phase is explicitly
/// [`ProviderPhase::Unavailable`].
///
/// Absent status (no [`InferenceProvider`] controller yet), `Pending`,
/// `Available`, and `Degraded` all return `false` — the provider is
/// included in the overlay.  This conservative default ensures that
/// providers are visible before OP-02 populates their status.  The
/// OP-02 `InferenceProvider` controller can tighten this policy once
/// it reliably sets `status.phase`.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
fn is_explicitly_unavailable(provider: &InferenceProvider) -> bool {
    provider
        .status
        .as_ref()
        .is_some_and(|s| s.phase == ProviderPhase::Unavailable)
}

// ---------------------------------------------------------------------------
// ConfigMap Builder
// ---------------------------------------------------------------------------

/// Build a Kubernetes `ConfigMap` for a [`RoutingOverlay`].
///
/// Returns an error if the overlay cannot be serialised to JSON.
/// This prevents applying an empty or invalid config to the cluster.
///
/// The `ConfigMap` name is computed by `overlay_configmap_name` which
/// ensures names are ≤ 63 characters and collision-safe.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the [`RoutingOverlay`] cannot be
/// serialised.  In practice this cannot fail for the current type
/// definition, but the caller must handle it to prevent silently
/// applying an empty config.
pub fn build_overlay_configmap(
    overlay: &RoutingOverlay,
    network_name: &str,
    gateway_name: &str,
    namespace: &str,
) -> Result<ConfigMap, serde_json::Error> {
    let json = serde_json::to_string_pretty(overlay)?;
    let name = overlay_configmap_name(network_name, gateway_name);

    let data = BTreeMap::from([("grid-config.json".to_owned(), json)]);

    Ok(ConfigMap {
        metadata: kube::api::ObjectMeta {
            labels: Some(overlay_labels(network_name, gateway_name)),
            name: Some(name),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    })
}

/// Compute the `ConfigMap` name.
///
/// Returns `grid-overlay-{network}-{gateway}` when the full string
/// fits in [`MAX_K8S_NAME`] (63) characters.
///
/// When the raw name would exceed 63 characters, uses a hash-suffixed
/// form: `grid-overlay-{net_prefix}-{gw_prefix}-{hash8}` where
/// `net_prefix` and `gw_prefix` are each at most [`MAX_COMPONENT_PREFIX`]
/// (20) characters and `hash8` is 8 lowercase hex digits derived from a
/// FNV-1a 32-bit hash of `"{network}/{gateway}"`.
///
/// The total of the hash-suffixed form is always ≤ 63 characters:
/// `"grid-overlay-"` (13) + 20 + `"-"` + 20 + `"-"` + 8 = 63.
fn overlay_configmap_name(network_name: &str, gateway_name: &str) -> String {
    let raw = format!("grid-overlay-{network_name}-{gateway_name}");
    if raw.len() <= MAX_K8S_NAME {
        return raw;
    }

    let hash = fnv1a_hex8(&format!("{network_name}/{gateway_name}"));
    let net_prefix: String = network_name.chars().take(MAX_COMPONENT_PREFIX).collect();
    let gw_prefix: String = gateway_name.chars().take(MAX_COMPONENT_PREFIX).collect();
    format!("grid-overlay-{net_prefix}-{gw_prefix}-{hash}")
}

/// FNV-1a 32-bit hash, returned as 8 lowercase hexadecimal digits.
///
/// Deterministic, dependency-free, and sufficient for name disambiguation.
/// Not cryptographically secure; not used for security-critical purposes.
fn fnv1a_hex8(input: &str) -> String {
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for byte in input.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:08x}")
}

/// Build the standard labels for an overlay `ConfigMap`.
fn overlay_labels(network_name: &str, gateway_name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/managed-by".to_owned(), "grid-operator".to_owned()),
        ("grid.praxis-proxy.io/gateway".to_owned(), gateway_name.to_owned()),
        ("grid.praxis-proxy.io/network".to_owned(), network_name.to_owned()),
    ])
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

    fn test_network(name: &str) -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": name },
            "spec": { "seeds": [] }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_site(name: &str, network: &str) -> GridSite {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": name },
            "spec": { "gridNetworkRef": network }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_site_with_labels(name: &str, network: &str, labels: &[(&str, &str)]) -> GridSite {
        let labels_map: serde_json::Map<String, serde_json::Value> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridSite",
            "metadata": {
                "name": name,
                "labels": labels_map
            },
            "spec": { "gridNetworkRef": network }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider(name: &str, network: &str, models: &[&str]) -> InferenceProvider {
        let models_json: Vec<serde_json::Value> = models.iter().map(|m| serde_json::json!({ "name": m })).collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": models_json
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider_with_selector(
        name: &str,
        network: &str,
        models: &[&str],
        selector: &[(&str, &str)],
    ) -> InferenceProvider {
        let models_json: Vec<serde_json::Value> = models.iter().map(|m| serde_json::json!({ "name": m })).collect();
        let match_labels: serde_json::Map<String, serde_json::Value> = selector
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": models_json,
                "siteSelector": { "matchLabels": match_labels }
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn test_provider_with_phase(name: &str, network: &str, models: &[&str], phase: &str) -> InferenceProvider {
        let models_json: Vec<serde_json::Value> = models.iter().map(|m| serde_json::json!({ "name": m })).collect();
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": models_json
            },
            "status": {
                "phase": phase,
                "matchingSites": [],
                "observedGeneration": 0
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn overlay_json_from_cm(cm: &ConfigMap) -> serde_json::Value {
        let json_str = cm
            .data
            .as_ref()
            .and_then(|d| d.get("grid-config.json"))
            .unwrap_or_else(|| std::process::abort());
        serde_json::from_str(json_str).unwrap_or_else(|_| std::process::abort())
    }

    fn build_cm(overlay: &RoutingOverlay, net: &str, gw: &str) -> ConfigMap {
        build_overlay_configmap(overlay, net, gw, "ns").unwrap_or_else(|_| std::process::abort())
    }

    // -----------------------------------------------------------------------
    // Basic rendering
    // -----------------------------------------------------------------------

    #[test]
    fn empty_network_renders_empty_candidates() {
        let network = test_network("my-net");
        let overlay = render_routing_overlay(&network, &[], &[], "test-site").unwrap_or_else(|_| std::process::abort());
        assert!(overlay.candidates.is_empty(), "no providers should yield no candidates");
    }

    #[test]
    fn provider_in_different_network_is_excluded() {
        let network = test_network("net-a");
        let provider = test_provider("prov", "net-b", &["model-1"]);
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert!(
            overlay.candidates.is_empty(),
            "provider in net-b must be excluded from net-a overlay"
        );
    }

    #[test]
    fn provider_with_two_models_renders_two_candidates() {
        let network = test_network("net-a");
        let provider = test_provider("prov", "net-a", &["model-1", "model-2"]);
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 2, "two models must produce two candidates");
    }

    #[test]
    fn two_providers_with_same_model_produce_two_candidates() {
        let network = test_network("net");
        let p1 = test_provider("prov-a", "net", &["llama-3"]);
        let p2 = test_provider("prov-b", "net", &["llama-3"]);
        let overlay =
            render_routing_overlay(&network, &[], &[p1, p2], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            2,
            "two providers for the same model must produce two distinct candidates"
        );
        let sites: Vec<&str> = overlay.candidates.iter().map(|c| c.site.as_str()).collect();
        assert!(sites.contains(&"prov-a"), "candidate from prov-a must be present");
        assert!(sites.contains(&"prov-b"), "candidate from prov-b must be present");
    }

    #[test]
    fn candidates_are_sorted_by_site_then_name() {
        let network = test_network("net");
        let p1 = test_provider("site-b", "net", &["z-model", "a-model"]);
        let p2 = test_provider("site-a", "net", &["c-model"]);
        let overlay =
            render_routing_overlay(&network, &[], &[p1, p2], "test-site").unwrap_or_else(|_| std::process::abort());
        let names: Vec<&str> = overlay.candidates.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            ["c-model", "a-model", "z-model"],
            "candidates must be sorted: site-a first, then site-b models alphabetically"
        );
    }

    #[test]
    fn input_order_does_not_affect_output() {
        let network = test_network("net");
        let p1 = test_provider("z-site", "net", &["z-model"]);
        let p2 = test_provider("a-site", "net", &["a-model"]);
        let fwd = render_routing_overlay(&network, &[], &[p1.clone(), p2.clone()], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        let rev =
            render_routing_overlay(&network, &[], &[p2, p1], "test-site").unwrap_or_else(|_| std::process::abort());
        let fwd_names: Vec<&str> = fwd.candidates.iter().map(|c| c.name.as_str()).collect();
        let rev_names: Vec<&str> = rev.candidates.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            fwd_names, rev_names,
            "output must be deterministic regardless of input order"
        );
    }

    #[test]
    fn blank_model_name_returns_error() {
        let network = test_network("net");
        let provider = test_provider("prov", "net", &[""]);
        let result = render_routing_overlay(&network, &[], &[provider], "test-site");
        assert!(result.is_err(), "blank model name must return an error");
    }

    // -----------------------------------------------------------------------
    // Site selector
    // -----------------------------------------------------------------------

    #[test]
    fn empty_selector_matches_all_sites_in_network() {
        let network = test_network("net");
        let site_a = test_site("site-a", "net");
        let site_b = test_site("site-b", "net");
        let provider = test_provider("prov", "net", &["model"]);
        let overlay = render_routing_overlay(&network, &[site_a, site_b], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            2,
            "empty selector should produce one candidate per site"
        );
        let sites: Vec<&str> = overlay.candidates.iter().map(|c| c.site.as_str()).collect();
        assert!(sites.contains(&"site-a"), "site-a must be in candidates");
        assert!(sites.contains(&"site-b"), "site-b must be in candidates");
    }

    #[test]
    fn selector_labels_match_only_labeled_sites() {
        let network = test_network("net");
        let site_gpu = test_site_with_labels("gpu-site", "net", &[("hw", "gpu")]);
        let site_cpu = test_site_with_labels("cpu-site", "net", &[("hw", "cpu")]);
        let provider = test_provider_with_selector("prov", "net", &["model"], &[("hw", "gpu")]);
        let overlay = render_routing_overlay(&network, &[site_gpu, site_cpu], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "selector hw=gpu should match only gpu-site"
        );
        assert_eq!(overlay.candidates[0].site, "gpu-site", "site must be gpu-site");
    }

    #[test]
    fn sites_in_another_network_are_ignored() {
        // Sites from net-b are pre-filtered before site matching, leaving
        // network_sites = [] for net-a.  An empty network_sites list means
        // SiteResolution::Unavailable, which triggers the provider-name fallback.
        let network = test_network("net-a");
        let site_other = test_site("site-other", "net-b");
        let provider = test_provider("prov", "net-a", &["model"]);
        let overlay = render_routing_overlay(&network, &[site_other], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "no sites in this network → Unavailable → provider-name fallback"
        );
        assert_eq!(
            overlay.candidates[0].site, "prov",
            "site must fall back to provider name when network has no site inventory"
        );
    }

    #[test]
    fn known_sites_with_selector_no_match_emits_no_candidates() {
        // Site inventory IS present (one site), but the provider's selector
        // requires hw=gpu and only hw=cpu exists.  Selector matched nothing →
        // SiteResolution::Known([]) → no candidates.  Must NOT fall back to
        // provider name.
        let network = test_network("net");
        let site_cpu = test_site_with_labels("cpu-site", "net", &[("hw", "cpu")]);
        let provider = test_provider_with_selector("prov", "net", &["model"], &[("hw", "gpu")]);
        let overlay = render_routing_overlay(&network, &[site_cpu], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert!(
            overlay.candidates.is_empty(),
            "selector matched nothing in a known site inventory — must emit no candidates"
        );
    }

    #[test]
    fn two_providers_same_model_same_site_different_cluster_both_survive() {
        // Two providers on the same site serving the same model.
        // Dedup is on (kind, name, site, cluster); different cluster → both kept.
        let network = test_network("net");
        let site = test_site("site-a", "net");
        let p1 = test_provider("prov-a", "net", &["shared-model"]);
        let p2 = test_provider("prov-b", "net", &["shared-model"]);
        let overlay =
            render_routing_overlay(&network, &[site], &[p1, p2], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            2,
            "two providers with different clusters must produce two candidates even for the same model+site"
        );
        let clusters: Vec<&str> = overlay.candidates.iter().map(|c| c.cluster.as_str()).collect();
        assert!(clusters.contains(&"prov-a"), "prov-a must be in candidates");
        assert!(clusters.contains(&"prov-b"), "prov-b must be in candidates");
    }

    #[test]
    fn provider_with_sites_sets_cluster_to_provider_name() {
        let network = test_network("net");
        let site = test_site("site-a", "net");
        let provider = test_provider("my-provider", "net", &["model"]);
        let overlay = render_routing_overlay(&network, &[site], &[provider], "test-site")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates[0].cluster, "my-provider",
            "cluster must always equal the provider metadata name"
        );
        assert_eq!(overlay.candidates[0].site, "site-a", "site must be resolved site name");
    }

    // -----------------------------------------------------------------------
    // Provider status filtering
    // -----------------------------------------------------------------------

    #[test]
    fn unavailable_provider_is_excluded() {
        let network = test_network("net");
        let provider = test_provider_with_phase("prov", "net", &["model-1"], "Unavailable");
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert!(overlay.candidates.is_empty(), "Unavailable provider must be excluded");
    }

    #[test]
    fn available_provider_is_included() {
        let network = test_network("net");
        let provider = test_provider_with_phase("prov", "net", &["model-1"], "Available");
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay.candidates.len(), 1, "Available provider must be included");
    }

    #[test]
    fn pending_provider_is_included() {
        let network = test_network("net");
        let provider = test_provider_with_phase("prov", "net", &["model-1"], "Pending");
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "Pending provider must be included (default pre-OP-02 state)"
        );
    }

    #[test]
    fn provider_with_absent_status_is_included() {
        let network = test_network("net");
        let provider = test_provider("prov", "net", &["model-1"]);
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.candidates.len(),
            1,
            "Provider with absent status must be included"
        );
    }

    // -----------------------------------------------------------------------
    // ConfigMap builder — fallible serialization
    // -----------------------------------------------------------------------

    #[test]
    fn configmap_name_matches_pattern() {
        let network = test_network("my-net");
        let overlay = render_routing_overlay(&network, &[], &[], "test-site").unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "my-net", "gw");
        assert_eq!(
            cm.metadata.name.as_deref(),
            Some("grid-overlay-my-net-gw"),
            "ConfigMap name must be grid-overlay-{{network}}-{{gateway}}"
        );
    }

    #[test]
    fn configmap_has_correct_labels() {
        let network = test_network("net");
        let overlay = render_routing_overlay(&network, &[], &[], "test-site").unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "net", "gw");
        let labels = cm.metadata.labels.as_ref().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by").map(String::as_str),
            Some("grid-operator"),
        );
        assert_eq!(
            labels.get("grid.praxis-proxy.io/network").map(String::as_str),
            Some("net")
        );
        assert_eq!(
            labels.get("grid.praxis-proxy.io/gateway").map(String::as_str),
            Some("gw")
        );
    }

    #[test]
    fn configmap_data_key_is_grid_config_json() {
        let network = test_network("net");
        let overlay = render_routing_overlay(&network, &[], &[], "test-site").unwrap_or_else(|_| std::process::abort());
        let cm = build_cm(&overlay, "net", "gw");
        assert!(
            cm.data.as_ref().is_some_and(|d| d.contains_key("grid-config.json")),
            "data key must be grid-config.json"
        );
    }

    #[test]
    fn build_overlay_configmap_is_fallible() {
        // This test verifies the signature is Result-based.
        // Serialization of RoutingOverlay cannot currently fail (all fields
        // are plain Strings / booleans), so we just verify the Ok path.
        let network = test_network("net");
        let overlay = render_routing_overlay(&network, &[], &[], "test-site").unwrap_or_else(|_| std::process::abort());
        let result = build_overlay_configmap(&overlay, "net", "gw", "ns");
        assert!(result.is_ok(), "well-formed overlay must serialize without error");
    }

    // -----------------------------------------------------------------------
    // ConfigMap name — collision safety
    // -----------------------------------------------------------------------

    #[test]
    fn normal_name_is_stable() {
        assert_eq!(
            overlay_configmap_name("net", "gw"),
            "grid-overlay-net-gw",
            "short names must not be hashed"
        );
    }

    #[test]
    fn long_name_is_at_most_63_chars() {
        let net = "a".repeat(50);
        let gw = "b".repeat(50);
        let name = overlay_configmap_name(&net, &gw);
        assert!(name.len() <= MAX_K8S_NAME, "name must be ≤63 chars, got {}", name.len());
    }

    #[test]
    fn two_long_names_with_same_prefix_do_not_collide() {
        let net = "a".repeat(50);
        let gw1 = "b".repeat(50);
        let gw2 = "c".repeat(50);
        let n1 = overlay_configmap_name(&net, &gw1);
        let n2 = overlay_configmap_name(&net, &gw2);
        assert_ne!(n1, n2, "different inputs must produce different names");
    }

    #[test]
    fn fnv1a_hash_is_deterministic() {
        assert_eq!(fnv1a_hex8("net/gw"), fnv1a_hex8("net/gw"), "hash must be deterministic");
        assert_ne!(
            fnv1a_hex8("net-a/gw"),
            fnv1a_hex8("net-b/gw"),
            "different inputs must produce different hashes"
        );
    }

    // -----------------------------------------------------------------------
    // JSON payload
    // -----------------------------------------------------------------------

    #[test]
    fn json_overlay_has_correct_top_level_fields() {
        let network = test_network("my-net");
        let overlay = render_routing_overlay(&network, &[], &[], "site-a").unwrap_or_else(|_| std::process::abort());
        let value = overlay_json_from_cm(&build_cm(&overlay, "my-net", "gw"));
        assert_eq!(value.get("network").and_then(serde_json::Value::as_str), Some("my-net"));
        assert_eq!(
            value.get("local_site").and_then(serde_json::Value::as_str),
            Some("site-a")
        );
        assert!(value.get("candidates").and_then(serde_json::Value::as_array).is_some());
    }

    #[test]
    fn local_site_parameter_flows_to_overlay() {
        let network = test_network("my-net");
        let overlay = render_routing_overlay(&network, &[], &[], "site-a").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            overlay.local_site, "site-a",
            "local_site parameter must appear verbatim in the overlay"
        );
        assert_eq!(overlay.network, "my-net", "network field must be the network name");
    }

    #[test]
    fn different_local_site_per_call_produces_different_overlays() {
        // Simulates two gateways in the same network declaring different local sites.
        let network = test_network("my-net");
        let overlay_a = render_routing_overlay(&network, &[], &[], "site-a").unwrap_or_else(|_| std::process::abort());
        let overlay_b = render_routing_overlay(&network, &[], &[], "site-b").unwrap_or_else(|_| std::process::abort());
        assert_eq!(overlay_a.local_site, "site-a", "gateway A must identify site-a");
        assert_eq!(overlay_b.local_site, "site-b", "gateway B must identify site-b");
        assert_eq!(
            overlay_a.network, overlay_b.network,
            "both overlays belong to the same network"
        );
    }

    #[test]
    fn json_candidate_has_correct_fields() {
        let network = test_network("my-net");
        let provider = test_provider("prov-a", "my-net", &["granite-3.3-8b"]);
        let overlay =
            render_routing_overlay(&network, &[], &[provider], "test-site").unwrap_or_else(|_| std::process::abort());
        let value = overlay_json_from_cm(&build_cm(&overlay, "my-net", "gw"));
        let c = value
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .and_then(|a| a.first())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            c.get("kind").and_then(serde_json::Value::as_str),
            Some("inference_model")
        );
        assert_eq!(
            c.get("name").and_then(serde_json::Value::as_str),
            Some("granite-3.3-8b")
        );
        assert_eq!(c.get("site").and_then(serde_json::Value::as_str), Some("prov-a"));
        assert_eq!(c.get("cluster").and_then(serde_json::Value::as_str), Some("prov-a"));
        assert_eq!(c.get("fresh").and_then(serde_json::Value::as_bool), Some(true));
    }

    // -----------------------------------------------------------------------
    // Error paths — missing names (items 14–15 per coverage policy)
    // -----------------------------------------------------------------------

    #[test]
    fn missing_network_name_returns_error() {
        // GridNetwork with no metadata.name must produce an error.
        let network: GridNetwork = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "GridNetwork",
            "metadata": {},
            "spec": { "seeds": [] }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let result = render_routing_overlay(&network, &[], &[], "local");
        assert!(result.is_err(), "network without metadata.name must return an error");
    }

    #[test]
    fn missing_provider_name_returns_error() {
        // InferenceProvider with no metadata.name must produce an error.
        let network = test_network("net");
        let provider: InferenceProvider = serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis-proxy.io/v1alpha1",
            "kind": "InferenceProvider",
            "metadata": {},
            "spec": {
                "gridNetworkRef": "net",
                "providerKind": "self_hosted",
                "backendKind": "local",
                "endpoint": "http://localhost:8000",
                "models": [{"name": "model"}]
            }
        }))
        .unwrap_or_else(|_| std::process::abort());
        let result = render_routing_overlay(&network, &[], &[provider], "local");
        assert!(
            result.is_err(),
            "InferenceProvider without metadata.name must return an error"
        );
    }
}
