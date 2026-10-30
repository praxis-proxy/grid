//! The provider file a provider gateway serves admitted peers from, beside the serving config.
//!
//! It names this site's own inference backends by the stable ID a consumer sends in
//! `x-ai-routing-candidate`, the local dial each one is reached by, and the enrolled sites
//! admitted to reach them. A gateway that predates it never opens the key.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::{
    crd::grid_site::GridSite,
    resources::{
        routing_overlay::{CANDIDATE_KIND, RoutingOverlay},
        serving_config::{DialSpec, DialTransport, RefusedSites, WriteDecision, refusal_changed},
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// `ConfigMap` data key holding the provider file, beside the serving config.
pub(crate) const PROVIDER_KEY: &str = "provider.json";

/// Length of a candidate stable ID, `fnv1a_hex8`.
const STABLE_ID_LEN: usize = 8;

// ---------------------------------------------------------------------------
// Provider File
// ---------------------------------------------------------------------------

/// The provider file, field for field the gateway's `ProviderConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ProviderConfig {
    /// Enrolled sites admitted to reach this site's backends, sorted, never this site.
    pub(crate) admitted_sites: Vec<String>,
    /// This site's servable backends, by stable ID.
    pub(crate) candidates: Vec<ProviderCandidate>,
    /// The local dial of each candidate cluster.
    pub(crate) dial: BTreeMap<String, DialSpec>,
    /// This gateway's own site.
    pub(crate) local_site: String,
    /// Always `v1`.
    pub(crate) version: &'static str,
}

/// One local backend a peer may name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct ProviderCandidate {
    /// The stable ID a consumer sends in `x-ai-routing-candidate`.
    pub(crate) stable_id: String,
    /// Model name.
    pub(crate) name: String,
    /// The cluster whose dial serves it.
    pub(crate) cluster: String,
}

/// The enrolled sites of `network_name` other than `local_site`: declared, not being
/// deleted, and named by a valid site name.
pub(crate) fn admitted_sites(sites: &[GridSite], network_name: &str, local_site: &str) -> Vec<String> {
    sites
        .iter()
        .filter(|site| {
            site.spec.grid_network_ref == network_name
                && crate::controller::grid_site::is_declared(site)
                // A site on its way out is refused at once, not when its finalizer clears.
                && site.metadata.deletion_timestamp.is_none()
        })
        .filter_map(|site| site.metadata.name.clone())
        .filter(|name| name != local_site && certs::validate_site_name(name).is_ok())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The provider file for `overlay`'s local backends over `dials`, admitting `admitted`.
///
/// A candidate is kept only when it is this site's inference model, carries a stable ID, no
/// credential and no site restriction, and its cluster has a local dial other than mutual
/// TLS. A stable ID two backends share is dropped with both, since a peer's request could
/// mean either. `restricted` names clusters whose provider limits which sites may route to it.
pub(crate) fn render_provider(
    overlay: &RoutingOverlay,
    dials: &BTreeMap<String, DialSpec>,
    admitted: Vec<String>,
    restricted: &BTreeSet<&str>,
) -> ProviderConfig {
    let local = overlay.local_site.as_str();
    let servable: BTreeSet<ProviderCandidate> = overlay
        .candidates
        .iter()
        .filter(|candidate| {
            candidate.kind == CANDIDATE_KIND
                && candidate.site == local
                && candidate.credential.is_none()
                && !restricted.contains(candidate.cluster.as_str())
                && dials
                    .get(&candidate.cluster)
                    .is_some_and(|dial| local_dial(dial, local))
        })
        .filter_map(|candidate| {
            let stable_id = candidate.stable_id.as_ref().filter(|id| valid_stable_id(id))?;
            Some(ProviderCandidate {
                stable_id: stable_id.clone(),
                name: candidate.name.clone(),
                cluster: candidate.cluster.clone(),
            })
        })
        .collect();
    let mut by_id: BTreeMap<&str, usize> = BTreeMap::new();
    for candidate in &servable {
        let seen = by_id.entry(candidate.stable_id.as_str()).or_default();
        *seen = seen.saturating_add(1);
    }
    let candidates: Vec<ProviderCandidate> = servable
        .iter()
        .filter(|candidate| by_id.get(candidate.stable_id.as_str()) == Some(&1))
        .cloned()
        .collect();
    let dial = candidates
        .iter()
        .filter_map(|candidate| {
            dials
                .get(&candidate.cluster)
                .map(|dial| (candidate.cluster.clone(), dial.clone()))
        })
        .collect();
    ProviderConfig {
        admitted_sites: admitted.into_iter().filter(|site| site != local).collect(),
        candidates,
        dial,
        local_site: overlay.local_site.clone(),
        version: "v1",
    }
}

/// Clusters of `overlay`'s local inference candidates the provider file leaves out.
pub(crate) fn dropped(overlay: &RoutingOverlay, provider: &ProviderConfig) -> BTreeSet<String> {
    let kept: BTreeSet<&str> = provider
        .candidates
        .iter()
        .map(|candidate| candidate.cluster.as_str())
        .collect();
    overlay
        .candidates
        .iter()
        .filter(|candidate| candidate.kind == CANDIDATE_KIND && candidate.site == overlay.local_site)
        .filter(|candidate| !kept.contains(candidate.cluster.as_str()))
        .map(|candidate| candidate.cluster.clone())
        .collect()
}

/// Warn when the clusters `key`'s provider file leaves out change.
pub(crate) fn warn_dropped(overlay: &RoutingOverlay, provider: &ProviderConfig, key: &str, last: &RefusedSites) {
    let dropped = dropped(overlay, provider);
    if refusal_changed(key, &dropped, last) && !dropped.is_empty() {
        tracing::info!(
            key,
            clusters = ?dropped,
            "provider file: leaving out local clusters peers cannot be served (credential, site restriction, no local dial, or no unique stable id)"
        );
    }
}

/// Whether `stored`, a provider file this operator wrote, admits a site `desired` does not.
///
/// A file that does not parse counts as shrinking, so it is replaced at once.
pub(crate) fn admission_shrinks(stored: Option<&str>, desired: &ProviderConfig) -> bool {
    let Some(stored) = stored else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(stored) else {
        return true;
    };
    let Some(sites) = value.get("admitted_sites").and_then(serde_json::Value::as_array) else {
        return true;
    };
    sites.iter().any(|site| {
        site.as_str()
            .is_none_or(|site| !desired.admitted_sites.iter().any(|kept| kept == site))
    })
}

/// `Write` when `stored` admits a site `desired` does not, else what `otherwise` decides.
///
/// A shrink bypasses write spacing and empty-withdrawal coalescing: until the file is
/// rewritten, the removed site still reaches this site's backends.
pub(crate) fn decide(
    stored: Option<&str>,
    desired: &ProviderConfig,
    otherwise: impl FnOnce() -> WriteDecision,
) -> WriteDecision {
    if admission_shrinks(stored, desired) {
        WriteDecision::Write
    } else {
        otherwise()
    }
}

// ---------------------------------------------------------------------------
// Utility Functions
// ---------------------------------------------------------------------------

/// A dial to this site that the gateway may hand a peer's request to.
fn local_dial(dial: &DialSpec, local_site: &str) -> bool {
    dial.site == local_site && !matches!(dial.transport, DialTransport::MutualTls { .. })
}

/// Eight lowercase hex digits, the shape the gateway accepts.
fn valid_stable_id(id: &str) -> bool {
    id.len() == STABLE_ID_LEN && id.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;
    use crate::resources::{geography::compute_stable_id, routing_overlay::RoutingCandidate, serving_config};

    /// Contract fixture the gateway crate loads with its own types.
    const GOLDEN: &str = include_str!("../../../gateway/tests/testdata/provider.json");

    #[test]
    fn renders_the_contract_the_gateway_loads() {
        let text = serving_config::to_text(&golden()).expect("json");
        assert_eq!(text, GOLDEN.trim_end(), "golden drifted");
        assert!(!text.contains("credential"), "the schema has no credential field");
    }

    #[test]
    fn admitted_sites_are_the_enrolled_sites_minus_this_one() {
        let leaving = site(
            &serde_json::json!({"name": "site-d", "deletionTimestamp": "2026-10-29T00:00:00Z"}),
            "net",
        );
        let stub = site(
            &serde_json::json!({"name": "site-e", "labels": {crate::controller::grid_network::LABEL_AUTO_DISCOVERED: "true"}}),
            "net",
        );
        let sites = vec![
            named("site-c", "net"),
            named("site-a", "net"),
            named("site-b", "net"),
            named("site-b", "net"),
            named("site-x", "other"),
            named("Not_A_Site", "net"),
            leaving,
            stub,
        ];
        assert_eq!(
            admitted_sites(&sites, "net", "site-a"),
            ["site-b", "site-c"],
            "a discovery stub, a deleting site, another network and this site are never admitted"
        );
        let provider = render_provider(
            &overlay(Vec::new()),
            &BTreeMap::new(),
            vec!["site-a".to_owned(), "site-b".to_owned()],
            &BTreeSet::new(),
        );
        assert_eq!(provider.admitted_sites, ["site-b"], "this site never admits itself");
    }

    #[test]
    fn only_local_uncredentialed_unrestricted_locally_dialed_backends_are_emitted() {
        let mut credentialed = local("secret", "pool-secret");
        credentialed.credential = Some(crate::resources::routing_overlay::ProjectedCredential {
            strategy: "bearer_token".to_owned(),
            secret_ref: crate::resources::routing_overlay::ProjectedCredentialRef {
                name: "key".to_owned(),
                namespace: "grid".to_owned(),
                key: "token".to_owned(),
            },
        });
        let mut no_id = local("anon", "pool-anon");
        no_id.stable_id = None;
        let mut bad_id = local("bad", "pool-bad");
        bad_id.stable_id = Some("NOTHEX!!".to_owned());
        let mut tool = local("tool", "pool-tool");
        tool.kind = "mcp_tool".to_owned();
        let remote = candidate("llama", "site-b", "site-b");
        let overlay = overlay(vec![
            local("llama", "pool-llama"),
            credentialed,
            no_id,
            bad_id,
            tool,
            local("mtls", "pool-mtls"),
            local("offsite", "pool-offsite"),
            local("restricted", "pool-restricted"),
            local("undialed", "pool-undialed"),
            remote,
        ]);
        let dials = BTreeMap::from([
            ("pool-llama".to_owned(), plaintext("site-a")),
            ("pool-secret".to_owned(), plaintext("site-a")),
            ("pool-anon".to_owned(), plaintext("site-a")),
            ("pool-bad".to_owned(), plaintext("site-a")),
            ("pool-tool".to_owned(), plaintext("site-a")),
            ("pool-mtls".to_owned(), mutual("site-a")),
            ("pool-offsite".to_owned(), plaintext("site-b")),
            ("pool-restricted".to_owned(), plaintext("site-a")),
            ("site-b".to_owned(), mutual("site-b")),
        ]);
        let provider = render_provider(
            &overlay,
            &dials,
            vec!["site-b".to_owned()],
            &BTreeSet::from(["pool-restricted"]),
        );
        let clusters: Vec<&str> = provider
            .candidates
            .iter()
            .map(|candidate| candidate.cluster.as_str())
            .collect();
        assert_eq!(clusters, ["pool-llama"], "only the plain local backend is servable");
        assert_eq!(
            provider.dial.keys().collect::<Vec<_>>(),
            ["pool-llama"],
            "a remote, mutual TLS or unused dial is never written"
        );
        assert_eq!(
            dropped(&overlay, &provider).len(),
            7,
            "every other local inference cluster is reported as left out"
        );
    }

    #[test]
    fn a_stable_id_two_backends_share_drops_both() {
        let mut twin = local("granite", "pool-granite");
        twin.stable_id = local("llama", "pool-llama").stable_id;
        let overlay = overlay(vec![
            local("llama", "pool-llama"),
            local("llama", "pool-llama"),
            twin,
            local("mistral", "pool-mistral"),
        ]);
        let dials = ["pool-llama", "pool-granite", "pool-mistral"]
            .map(|cluster| (cluster.to_owned(), plaintext("site-a")))
            .into();
        let provider = render_provider(&overlay, &dials, Vec::new(), &BTreeSet::new());
        let names: Vec<&str> = provider
            .candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect();
        assert_eq!(
            names,
            ["mistral"],
            "an exact duplicate folds, a colliding id drops both"
        );
    }

    #[test]
    fn a_shrinking_admitted_set_is_detected_against_what_is_stored() {
        let stored = serving_config::to_text(&golden()).expect("json");
        let mut desired = golden();
        assert!(
            !admission_shrinks(Some(&stored), &desired),
            "the same set does not shrink"
        );
        desired.admitted_sites.push("site-z".to_owned());
        assert!(
            !admission_shrinks(Some(&stored), &desired),
            "a growing set does not shrink"
        );
        desired.admitted_sites.retain(|site| site != "site-c");
        assert!(admission_shrinks(Some(&stored), &desired), "a removed site shrinks");
        assert!(!admission_shrinks(None, &desired), "nothing stored admits nobody");
        assert!(admission_shrinks(Some("{"), &desired), "an unreadable file is replaced");
        assert!(
            admission_shrinks(Some("{\"admitted_sites\": [1]}"), &desired),
            "a malformed entry is replaced"
        );
        assert!(
            admission_shrinks(Some("{}"), &desired),
            "a file without the list is replaced"
        );
    }

    #[test]
    fn a_shrinking_admitted_set_writes_inside_the_gate_window() {
        let stored = serving_config::to_text(&golden()).expect("json");
        let now = std::time::Instant::now();
        let gated = || serving_config::decide_write(Some("old"), "new", Some(now), now);
        assert!(
            matches!(gated(), WriteDecision::Deferred(_)),
            "the fixture is inside the write window"
        );
        assert!(
            matches!(decide(Some(&stored), &golden(), gated), WriteDecision::Deferred(_)),
            "an unchanged admitted set waits like any other change"
        );
        let mut shrunk = golden();
        shrunk.admitted_sites.retain(|site| site != "site-b");
        assert_eq!(
            decide(Some(&stored), &shrunk, gated),
            WriteDecision::Write,
            "a removed site is written at once"
        );
        assert_eq!(
            decide(Some(&stored), &shrunk, || WriteDecision::Unchanged),
            WriteDecision::Write,
            "a shrink is written even where the serving config would not be"
        );
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    /// The provider file the golden fixture holds: two local backends behind plain and TLS dials.
    pub(crate) fn golden() -> ProviderConfig {
        let overlay = overlay(vec![
            local("llama", "local-llama"),
            local("granite", "local-granite"),
            candidate("llama", "site-b", "site-b"),
        ]);
        let mut granite = plaintext("site-a");
        granite.address = "granite.models.svc.cluster.local:8443".to_owned();
        granite.transport = DialTransport::Tls {
            ca_path: Some("/run/secrets/grid-backend-ca/models-ca/ca.crt".to_owned()),
            sni: "granite.models.svc".to_owned(),
        };
        let dials = BTreeMap::from([
            ("local-llama".to_owned(), plaintext("site-a")),
            ("local-granite".to_owned(), granite),
            ("site-b".to_owned(), mutual("site-b")),
        ]);
        render_provider(
            &overlay,
            &dials,
            vec!["site-b".to_owned(), "site-c".to_owned()],
            &BTreeSet::new(),
        )
    }

    fn candidate(name: &str, site: &str, cluster: &str) -> RoutingCandidate {
        serde_json::from_value(serde_json::json!({
            "kind": CANDIDATE_KIND, "name": name, "site": site, "cluster": cluster, "fresh": true,
            "stable_id": compute_stable_id(CANDIDATE_KIND, name, site, cluster),
        }))
        .expect("candidate")
    }

    fn local(name: &str, cluster: &str) -> RoutingCandidate {
        candidate(name, "site-a", cluster)
    }

    fn overlay(candidates: Vec<RoutingCandidate>) -> RoutingOverlay {
        RoutingOverlay {
            network: "net".to_owned(),
            local_site: "site-a".to_owned(),
            candidates,
            excluded: Vec::new(),
            selection_policy: None,
            generated_at: None,
        }
    }

    fn plaintext(site: &str) -> DialSpec {
        DialSpec {
            address: "llama.models.svc.cluster.local:8000".to_owned(),
            base_path: None,
            connect_retries: None,
            connect_timeout_ms: None,
            credential: None,
            read_timeout_ms: None,
            site: site.to_owned(),
            total_connect_timeout_ms: None,
            transport: DialTransport::Plaintext,
            trusted_private: true,
            write_timeout_ms: None,
        }
    }

    fn mutual(site: &str) -> DialSpec {
        DialSpec {
            address: format!("{site}-gateway.grid.example.com:8443"),
            transport: DialTransport::MutualTls {
                sni: certs::site_dns_name(site),
            },
            trusted_private: false,
            ..plaintext(site)
        }
    }

    fn site(metadata: &serde_json::Value, network: &str) -> GridSite {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSite",
            "metadata": metadata.clone(),
            "spec": { "gridNetworkRef": network },
        }))
        .expect("site")
    }

    fn named(name: &str, network: &str) -> GridSite {
        site(&serde_json::json!({ "name": name }), network)
    }
}
