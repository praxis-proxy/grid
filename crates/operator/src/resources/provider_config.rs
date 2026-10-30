//! The provider file a provider gateway serves admitted peers from, beside the serving config.
//!
//! It names this site's own inference backends by the stable ID a consumer sends in
//! `x-ai-routing-candidate`, the local dial each one is reached by, and the enrolled sites
//! admitted to reach them. A gateway that predates it never opens the key.

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::core::v1::ConfigMap;
use serde::Serialize;

use crate::{
    crd::grid_site::GridSite,
    resources::{
        consumer_config::BACKEND_CA_MOUNT_BASE,
        routing_overlay::{CANDIDATE_KIND, RoutingOverlay, overlay_labels},
        serving_config::{
            DIAL_CREDENTIAL_BASE, DialSpec, DialTransport, RefusedSites, configmap_name, refusal_changed,
        },
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// `ConfigMap` data key holding the provider file, beside the serving config.
pub(crate) const PROVIDER_KEY: &str = "provider.json";

/// The field manager the provider file is applied under, apart from the serving config's, so
/// either is written without taking the other's keys away.
pub(crate) const FIELD_MANAGER: &str = "grid-operator-provider";

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
/// A candidate is kept only when it is this site's inference model, carries a stable ID and no
/// site restriction, and its cluster has a local dial other than mutual TLS. A credentialed
/// candidate's dial must send its own token, which replaces anything a peer sends. A stable ID two backends share is
/// dropped with both, since a peer's request could mean either. `restricted` names clusters whose provider limits which
/// sites may route to it.
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
                && !restricted.contains(candidate.cluster.as_str())
                && dials.get(&candidate.cluster).is_some_and(|dial| {
                    local_dial(dial, local) && (candidate.credential.is_none() || dial.credential.is_some())
                })
        })
        .filter_map(|candidate| {
            let stable_id = candidate.stable_id.as_ref().filter(|id| certs::is_stable_id(id))?;
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

/// `stored` with every admitted site not in `admitted` removed, `None` when it removes none.
///
/// It needs nothing but the site list, so it runs before anything that can fail for a gateway:
/// until the file is rewritten, a removed site still reaches this site's backends. A file that
/// does not parse, or has no list, is replaced by one admitting no one.
pub(crate) fn revoked(stored: &str, admitted: &[String]) -> Option<String> {
    let mut value: serde_json::Value = serde_json::from_str(stored).unwrap_or_else(|_| serde_json::json!({}));
    let listed: Vec<serde_json::Value> = value
        .get("admitted_sites")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let kept: Vec<serde_json::Value> = listed
        .iter()
        .filter(|site| {
            site.as_str()
                .is_some_and(|site| admitted.iter().any(|kept| kept == site))
        })
        .cloned()
        .collect();
    if kept.len() == listed.len() && value.get("admitted_sites").is_some() {
        return None;
    }
    let Some(object) = value.as_object_mut() else {
        return Some("{\n  \"admitted_sites\": []\n}".to_owned());
    };
    object.insert("admitted_sites".to_owned(), serde_json::Value::Array(kept));
    serde_json::to_string_pretty(&value).ok()
}

/// The patch that sets only the provider file in `{network}`'s serving `ConfigMap` for `gateway`.
pub(crate) fn build_configmap(text: &str, network_name: &str, gateway_name: &str, namespace: &str) -> ConfigMap {
    let mut labels = overlay_labels(network_name, gateway_name);
    labels.insert("app.kubernetes.io/component".to_owned(), "serving".to_owned());
    ConfigMap {
        metadata: kube::api::ObjectMeta {
            labels: Some(labels),
            name: Some(configmap_name(network_name, gateway_name)),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(PROVIDER_KEY.to_owned(), text.to_owned())])),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Utility Functions
// ---------------------------------------------------------------------------

/// A dial to this site that the gateway may hand a peer's request to: the gateway refuses the
/// whole file over any other, so it is left out here with its candidates.
fn local_dial(dial: &DialSpec, local_site: &str) -> bool {
    let grid_name = |name: &str| certs::is_grid_name(name);
    let ca_within = dial
        .transport
        .ca_path()
        .is_none_or(|path| within(path, BACKEND_CA_MOUNT_BASE));
    let credential_within = dial
        .credential
        .as_ref()
        .is_none_or(|credential| within(&credential.path, DIAL_CREDENTIAL_BASE));
    dial.site == local_site
        && credential_within
        && !matches!(dial.transport, DialTransport::MutualTls { .. })
        && !dial.transport.sni().is_some_and(grid_name)
        && !grid_name(grid_client::address_host(&dial.address))
        && ca_within
}

/// Whether `path` names a file below `base`, with no `..` to climb out.
fn within(path: &str, base: &str) -> bool {
    let path = std::path::Path::new(path);
    path.starts_with(base)
        && path != std::path::Path::new(base)
        && !path.components().any(|part| part == std::path::Component::ParentDir)
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::type_complexity,
    reason = "tests"
)]
mod tests {
    use super::*;
    use crate::resources::{geography::compute_stable_id, routing_overlay::RoutingCandidate, serving_config};

    /// Contract fixture the gateway crate loads with its own types.
    const GOLDEN: &str = include_str!("../../../gateway/tests/testdata/provider.json");

    #[test]
    fn renders_the_contract_the_gateway_loads() {
        let text = serving_config::to_text(&golden()).expect("json");
        assert_eq!(text, GOLDEN.trim_end(), "golden drifted");
        assert!(!text.contains("credential"), "the golden backends carry no credential");
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
    fn only_local_unrestricted_locally_dialed_backends_are_emitted() {
        let mut credentialed = local("secret", "pool-secret");
        credentialed.credential = Some(crate::resources::routing_overlay::ProjectedCredential {
            strategy: "bearer_token".to_owned(),
            secret_ref: crate::resources::routing_overlay::ProjectedCredentialRef {
                name: "key".to_owned(),
                namespace: "grid".to_owned(),
                key: "token".to_owned(),
            },
        });
        let mut token = credentialed.clone();
        token.name = "token".to_owned();
        token.cluster = "pool-token".to_owned();
        token.stable_id = Some(compute_stable_id(CANDIDATE_KIND, "token", "site-a", "pool-token"));
        let mut leaked = token.clone();
        leaked.name = "leaked".to_owned();
        leaked.cluster = "pool-leaked".to_owned();
        leaked.stable_id = Some(compute_stable_id(CANDIDATE_KIND, "leaked", "site-a", "pool-leaked"));
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
            token,
            leaked,
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
            (
                "pool-token".to_owned(),
                with_token("/run/secrets/grid-credentials/key/token"),
            ),
            ("pool-leaked".to_owned(), with_token("/etc/praxis/tls/tls.key")),
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
        let clusters: BTreeSet<&str> = provider
            .candidates
            .iter()
            .map(|candidate| candidate.cluster.as_str())
            .collect();
        assert_eq!(
            clusters,
            BTreeSet::from(["pool-llama", "pool-token"]),
            "a plain local backend, and a credentialed one whose dial sends its own token"
        );
        assert_eq!(
            provider.dial.keys().collect::<Vec<_>>(),
            ["pool-llama", "pool-token"],
            "a remote, mutual TLS, tokenless-for-a-credential or unused dial is never written"
        );
        let sent = provider.dial.get("pool-token").expect("token dial");
        assert_eq!(
            (
                sent.base_path.as_deref(),
                sent.credential.as_ref().map(|credential| credential.path.as_str())
            ),
            (Some("/v1"), Some("/run/secrets/grid-credentials/key/token")),
            "the dial's base path and token ride along"
        );
        assert_eq!(
            dropped(&overlay, &provider).len(),
            8,
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
    fn a_removed_site_is_revoked_from_what_is_stored() {
        let stored = serving_config::to_text(&golden()).expect("json");
        let both = ["site-b".to_owned(), "site-c".to_owned()];
        assert_eq!(revoked(&stored, &both), None, "the same set needs no rewrite");
        let grown = ["site-b".to_owned(), "site-c".to_owned(), "site-z".to_owned()];
        assert_eq!(
            revoked(&stored, &grown),
            None,
            "a growing set waits for the full render"
        );
        let rewritten = revoked(&stored, &["site-c".to_owned()]).expect("a removed site is revoked");
        let value: serde_json::Value = serde_json::from_str(&rewritten).expect("json");
        assert_eq!(
            value["admitted_sites"],
            serde_json::json!(["site-c"]),
            "only the remaining site"
        );
        assert_eq!(
            value["candidates"],
            serde_json::to_value(golden().candidates).expect("json"),
            "backends kept"
        );
        for broken in ["{", "{\"admitted_sites\": [1]}", "{}", "[]"] {
            let replaced = revoked(broken, &both).expect("a malformed file is replaced");
            let parsed: serde_json::Value = serde_json::from_str(&replaced).expect("json");
            assert_eq!(
                parsed["admitted_sites"],
                serde_json::json!([]),
                "{broken}: admits no one"
            );
        }
    }

    #[test]
    fn the_provider_patch_carries_only_its_key() {
        let cm = build_configmap("{}", "grid", "gw", "ns");
        assert_eq!(
            cm.metadata.name.as_deref(),
            Some("grid-serving-grid-gw"),
            "the serving ConfigMap"
        );
        assert_eq!(
            cm.data.expect("data").keys().collect::<Vec<_>>(),
            [PROVIDER_KEY],
            "the serving config and dial file are another manager's"
        );
        assert!(cm.metadata.annotations.is_none(), "no rollout digest");
    }

    #[test]
    fn a_dial_the_gateway_would_refuse_drops_its_backend() {
        let overlay = overlay(vec![local("llama", "pool-llama")]);
        let refused: [(&str, fn(&mut DialSpec)); 5] = [
            ("a grid SNI", |dial| {
                dial.transport = DialTransport::Tls {
                    ca_path: None,
                    sni: "site-b.Grid.Internal.".to_owned(),
                };
            }),
            ("a grid address", |dial| {
                dial.address = "site-b.grid.internal:8443".to_owned();
            }),
            ("the grid apex", |dial| {
                dial.address = "grid.internal:8443".to_owned();
            }),
            ("a CA outside the mount", |dial| {
                dial.transport = DialTransport::Tls {
                    ca_path: Some("/etc/praxis/tls/ca.crt".to_owned()),
                    sni: "llama.models.svc".to_owned(),
                };
            }),
            ("a CA climbing out of the mount", |dial| {
                dial.transport = DialTransport::Tls {
                    ca_path: Some("/run/secrets/grid-backend-ca/../x/ca.crt".to_owned()),
                    sni: "llama.models.svc".to_owned(),
                };
            }),
        ];
        let kept = BTreeMap::from([("pool-llama".to_owned(), plaintext("site-a"))]);
        assert_eq!(
            render_provider(&overlay, &kept, Vec::new(), &BTreeSet::new())
                .candidates
                .len(),
            1,
            "the fixture is servable"
        );
        for (what, break_it) in refused {
            let mut dial = plaintext("site-a");
            break_it(&mut dial);
            let dials = BTreeMap::from([("pool-llama".to_owned(), dial)]);
            let provider = render_provider(&overlay, &dials, Vec::new(), &BTreeSet::new());
            assert!(
                provider.candidates.is_empty() && provider.dial.is_empty(),
                "{what}: dropped"
            );
        }
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

    fn with_token(path: &str) -> DialSpec {
        DialSpec {
            base_path: Some("/v1".to_owned()),
            credential: Some(serving_config::DialCredential {
                path: path.to_owned(),
                strategy: "bearer_token",
            }),
            ..plaintext("site-a")
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
