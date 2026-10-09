//! Tests for endpoint-topology derivation. The renderer's own tests cover the rendered form.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]

use super::*;

/// Build an `InferenceProvider` with the given endpoint and optional backend TLS.
fn provider(name: &str, endpoint: &str, backend_tls: Option<serde_json::Value>) -> InferenceProvider {
    let mut spec = serde_json::json!({
        "gridNetworkRef": "net",
        "providerKind": "self_hosted",
        "backendKind": "local_model",
        "endpoint": endpoint,
        "models": [{ "name": "model-x" }]
    });
    if let Some(tls) = backend_tls {
        spec["backendTls"] = tls;
    }
    serde_json::from_value(serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": name },
        "spec": spec
    }))
    .expect("provider")
}

/// Build a `GridSite` with an optional egress section.
fn site(name: &str, egress: Option<serde_json::Value>) -> GridSite {
    site_in_phase(name, egress, "Active")
}

/// A `GridSite` in a named phase.
fn site_in_phase(name: &str, egress: Option<serde_json::Value>, phase: &str) -> GridSite {
    let mut spec = serde_json::json!({ "gridNetworkRef": "net" });
    if let Some(egress) = egress {
        spec["egress"] = egress;
    }
    serde_json::from_value(serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridSite",
        "metadata": { "name": name },
        "spec": spec,
        "status": { "phase": phase, "capabilities": {}, "message": "", "observedGeneration": 0, "reason": "" }
    }))
    .expect("site")
}

/// The routing identities the fixtures use, allowlisted by the shared `decl`.
static ALLOWED: std::sync::LazyLock<Vec<String>> = std::sync::LazyLock::new(|| {
    ["prov-a", "prov-b", "prov-ghost", "gateway-site-a"]
        .iter()
        .map(|identity| (*identity).to_owned())
        .collect()
});

/// Declarations for one network, which every case here shares.
fn decl<'decl>(
    providers: &'decl [InferenceProvider],
    sites: &'decl [GridSite],
    local_site: &'decl str,
) -> Declarations<'decl> {
    Declarations {
        providers,
        sites,
        local_site,
        network_name: "net",
        // Every fixture identity is allowlisted; the refusals have their own tests.
        from_providers: &ALLOWED,
    }
}

/// A candidate for one cluster at one site. Only those two fields matter here.
fn candidate(cluster: &str, site: &str) -> RoutingCandidate {
    serde_json::from_value(serde_json::json!({
        "kind": "inference_model",
        "name": "model-x",
        "site": site,
        "cluster": cluster,
        "fresh": true
    }))
    .expect("candidate")
}

/// Mutual-TLS egress, the shape a federated site declares.
fn mutual_egress(address: &str, server_name: &str) -> serde_json::Value {
    serde_json::json!({
        "address": address,
        "tls": { "mode": "Mutual", "serverName": server_name }
    })
}

/// Resolve with one local provider at `site-a`, which is also the local site.
fn resolve_local(endpoint: &str, backend_tls: Option<serde_json::Value>) -> Resolved {
    let providers = vec![provider("prov-a", endpoint, backend_tls)];
    let sites = vec![site("site-a", None)];
    let candidates = vec![candidate("prov-a", "site-a")];
    let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
    resolved.get("prov-a").cloned().expect("a local provider resolves")
}

#[test]
fn an_https_endpoint_derives_server_authenticated_tls_against_its_own_host() {
    let got = resolve_local("https://model-gateway.models.svc.cluster.local", None);
    let transport = got.endpoint.transport.expect("transport");
    assert_eq!(got.origin, Origin::DerivedLocal);
    assert_eq!(got.endpoint.address, "model-gateway.models.svc.cluster.local:443");
    assert_eq!(transport.mode, TransportMode::Tls);
    assert_eq!(
        transport.sni.as_deref(),
        Some("model-gateway.models.svc.cluster.local"),
        "SNI defaults to the endpoint host, which is the only name an HTTPS URL carries"
    );
    assert!(
        transport.ca_secret_ref.is_none(),
        "an undeclared CA stays undeclared, which the renderer reads as the process trust store"
    );
}

#[test]
fn a_declared_server_name_and_ca_reach_the_entry() {
    let got = resolve_local(
        "https://10.0.0.7:8443",
        Some(serde_json::json!({
            "serverName": "model-gateway.example.invalid",
            "caSecretRef": { "name": "backend-ca" }
        })),
    );
    let transport = got.endpoint.transport.expect("transport");
    assert_eq!(
        got.endpoint.address, "10.0.0.7:8443",
        "an explicit port wins over the scheme default"
    );
    assert_eq!(
        transport.sni.as_deref(),
        Some("model-gateway.example.invalid"),
        "a declared server name is the point of the field: the address is not the certificate name"
    );
    let ca = transport.ca_secret_ref.expect("ca");
    assert_eq!(ca.name.as_str(), "backend-ca");
}

#[test]
fn an_http_endpoint_derives_plaintext_with_no_server_name() {
    let got = resolve_local("http://model-gateway.models.svc.cluster.local:8000", None);
    let transport = got.endpoint.transport.expect("transport");
    assert_eq!(transport.mode, TransportMode::Plaintext);
    assert!(
        transport.sni.is_none(),
        "the renderer rejects plaintext carrying an SNI, so derivation must not emit one"
    );
}

#[test]
fn a_declared_server_name_is_dropped_on_a_plaintext_endpoint() {
    let got = resolve_local(
        "http://model-gateway.models.svc.cluster.local",
        Some(serde_json::json!({ "serverName": "model-gateway.example.invalid" })),
    );
    let transport = got.endpoint.transport.expect("transport");
    assert_eq!(transport.mode, TransportMode::Plaintext);
    assert!(
        transport.sni.is_none(),
        "an http endpoint with a declared server name would otherwise render PlaintextWithSni"
    );
}

#[test]
fn an_endpoint_that_names_no_usable_host_or_scheme_derives_nothing() {
    for endpoint in [
        "",
        "model-gateway.models.svc.cluster.local",
        "ftp://model-gateway.models.svc.cluster.local",
        "https://",
        "not a url",
    ] {
        let providers = vec![provider("prov-a", endpoint, None)];
        let sites = vec![site("site-a", None)];
        let candidates = vec![candidate("prov-a", "site-a")];
        let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
        assert!(
            resolved.is_empty(),
            "{endpoint:?} produced an entry; an unusable endpoint must leave the cluster for the renderer to report"
        );
    }
}

#[test]
fn an_explicit_entry_survives_derivation_whole() {
    let providers = vec![provider("prov-a", "https://derived.example.invalid", None)];
    let sites = vec![site("site-a", None)];
    let candidates = vec![candidate("prov-a", "site-a")];
    let explicit = vec![ClusterEndpointConfig {
        cluster: "prov-a".to_owned(),
        address: "typed.example.invalid:9443".to_owned(),
        transport: None,
    }];
    let resolved = resolve(&candidates, &explicit, &decl(&providers, &sites, "site-a"));
    let got = resolved.get("prov-a").expect("entry");
    assert_eq!(got.origin, Origin::Explicit);
    assert_eq!(got.endpoint.address, "typed.example.invalid:9443");
    assert!(
        got.endpoint.transport.is_none(),
        "a half-typed entry keeps its own missing transport and fails in the renderer, \
         rather than silently inheriting derived trust"
    );
}

#[test]
fn an_explicit_entry_for_a_cluster_with_no_candidate_is_not_returned() {
    // The controller passes typed entries through itself; resolution is per candidate.
    let explicit = vec![ClusterEndpointConfig {
        cluster: "retired-cluster".to_owned(),
        address: "retired.example.invalid:8443".to_owned(),
        transport: None,
    }];
    let resolved = resolve(&[], &explicit, &decl(&[], &[], "site-a"));
    assert!(resolved.is_empty());
}

#[test]
fn a_remote_candidate_derives_the_sites_provider_hop() {
    let sites = vec![
        site("site-a", None),
        site(
            "site-b",
            Some(mutual_egress(
                "site-b.grid.example.invalid:8443",
                "site-b.grid.internal",
            )),
        ),
    ];
    let candidates = vec![candidate("prov-b", "site-b")];
    let resolved = resolve(&candidates, &[], &decl(&[], &sites, "site-a"));
    let got = resolved.get("prov-b").expect("entry");
    let transport = got.endpoint.transport.clone().expect("transport");
    assert_eq!(got.origin, Origin::DerivedRemote);
    assert_eq!(got.endpoint.address, "site-b.grid.example.invalid:8443");
    assert_eq!(transport.mode, TransportMode::MutualTls);
    assert_eq!(transport.sni.as_deref(), Some("site-b.grid.internal"));
    assert!(
        transport.ca_secret_ref.is_none(),
        "the inter-site CA is the mounted grid identity, not a per-provider Secret"
    );
}

#[test]
fn a_remote_candidate_never_resolves_to_a_provider_endpoint() {
    // Present locally but its candidate says site-b, so locality refuses it.
    let providers = vec![provider("prov-a", "https://internal.site-a.svc.cluster.local", None)];
    let sites = vec![
        site("site-a", None),
        site(
            "site-b",
            Some(mutual_egress(
                "site-b.grid.example.invalid:8443",
                "site-b.grid.internal",
            )),
        ),
    ];
    let candidates = vec![candidate("prov-a", "site-b")];
    let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
    let got = resolved.get("prov-a").expect("entry");
    assert_eq!(got.origin, Origin::DerivedRemote);
    assert_eq!(
        got.endpoint.address, "site-b.grid.example.invalid:8443",
        "a candidate at another site must reach that site's hop, never an address inside a cluster"
    );
    assert!(
        !resolved
            .values()
            .any(|r| r.endpoint.address.contains("internal.site-a")),
        "no resolved entry may carry an in-cluster address for a remote candidate"
    );
}

#[test]
fn a_remote_site_with_nothing_to_derive_from_derives_nothing() {
    let cases = [
        ("no egress section", site("site-b", None)),
        (
            "blank egress address",
            site("site-b", Some(mutual_egress("   ", "site-b.grid.internal"))),
        ),
        (
            "site absent from the inventory",
            site("site-c", Some(mutual_egress("c:8443", "c.internal"))),
        ),
    ];
    for (why, remote) in cases {
        let sites = vec![site("site-a", None), remote];
        let candidates = vec![candidate("prov-b", "site-b")];
        let resolved = resolve(&candidates, &[], &decl(&[], &sites, "site-a"));
        assert!(
            resolved.is_empty(),
            "{why}: derivation produced an entry where it has no verified address"
        );
    }
}

#[test]
fn a_site_in_another_network_is_not_a_source() {
    let foreign: GridSite = serde_json::from_value(serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridSite",
        "metadata": { "name": "site-b" },
        "spec": {
            "gridNetworkRef": "other-net",
            "egress": mutual_egress("site-b.grid.example.invalid:8443", "site-b.grid.internal")
        }
    }))
    .expect("site");
    let sites = vec![site("site-a", None), foreign];
    let candidates = vec![candidate("prov-b", "site-b")];
    let resolved = resolve(&candidates, &[], &decl(&[], &sites, "site-a"));
    assert!(
        resolved.is_empty(),
        "a same-named site in another network must not supply this network's topology"
    );
}

#[test]
fn a_mutual_egress_with_no_server_name_derives_no_server_name() {
    // A hand-written Mutual site may omit serverName; leave SNI unset for the renderer to refuse.
    let sites = vec![
        site("site-a", None),
        site(
            "site-b",
            Some(serde_json::json!({ "address": "site-b.grid.example.invalid:8443", "tls": { "mode": "Mutual" } })),
        ),
    ];
    let candidates = vec![candidate("prov-b", "site-b")];
    let resolved = resolve(&candidates, &[], &decl(&[], &sites, "site-a"));
    let transport = resolved
        .get("prov-b")
        .and_then(|got| got.endpoint.transport.clone())
        .expect("transport");
    assert_eq!(transport.mode, TransportMode::MutualTls);
    assert!(
        transport.sni.is_none(),
        "derivation supplied an SNI the site never declared, so the hop would verify an invented name"
    );
}

#[test]
fn a_blank_server_name_is_treated_as_undeclared() {
    let sites = vec![
        site("site-a", None),
        site("site-b", Some(mutual_egress("site-b.grid.example.invalid:8443", "   "))),
    ];
    let candidates = vec![candidate("prov-b", "site-b")];
    let resolved = resolve(&candidates, &[], &decl(&[], &sites, "site-a"));
    let transport = resolved
        .get("prov-b")
        .and_then(|got| got.endpoint.transport.clone())
        .expect("transport");
    assert!(
        transport.sni.is_none(),
        "a blank server name must not reach the renderer as a present one"
    );
}

#[test]
fn a_plaintext_egress_derives_plaintext() {
    let sites = vec![
        site("site-a", None),
        site(
            "site-b",
            Some(serde_json::json!({ "address": "site-b:8080", "tls": { "mode": "Plaintext" } })),
        ),
    ];
    let candidates = vec![candidate("prov-b", "site-b")];
    let resolved = resolve(&candidates, &[], &decl(&[], &sites, "site-a"));
    let transport = resolved
        .get("prov-b")
        .and_then(|got| got.endpoint.transport.clone())
        .expect("transport");
    assert_eq!(transport.mode, TransportMode::Plaintext);
    assert!(transport.sni.is_none());
}

#[test]
fn mixed_local_and_remote_candidates_for_one_model_each_resolve_in_their_own_way() {
    let providers = vec![provider(
        "prov-a",
        "https://model-gateway.site-a.svc.cluster.local",
        None,
    )];
    let sites = vec![
        site("site-a", None),
        site(
            "site-b",
            Some(mutual_egress(
                "site-b.grid.example.invalid:8443",
                "site-b.grid.internal",
            )),
        ),
    ];
    let candidates = vec![candidate("prov-a", "site-a"), candidate("prov-b", "site-b")];
    let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
    assert_eq!(resolved.len(), 2, "both candidates resolve");
    assert_eq!(resolved["prov-a"].origin, Origin::DerivedLocal);
    assert_eq!(
        resolved["prov-a"].endpoint.transport.as_ref().expect("t").mode,
        TransportMode::Tls
    );
    assert_eq!(resolved["prov-b"].origin, Origin::DerivedRemote);
    assert_eq!(
        resolved["prov-b"].endpoint.transport.as_ref().expect("t").mode,
        TransportMode::MutualTls
    );
}

#[test]
fn the_routing_identity_is_the_key_whether_or_not_the_provider_declares_one() {
    let mut named = provider("prov-a", "https://a.example.invalid", None);
    named.spec.routing_cluster_ref = Some("gateway-site-a".to_owned());
    let providers = vec![named];
    let sites = vec![site("site-a", None)];
    let candidates = vec![candidate("gateway-site-a", "site-a")];
    let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
    assert!(
        resolved.contains_key("gateway-site-a"),
        "derivation keys on routingClusterRef when it is set, as the overlay and the renderer do"
    );
}

/// Declarations that allowlist only what a case names, for the refusal paths.
fn decl_allowing<'decl>(
    providers: &'decl [InferenceProvider],
    sites: &'decl [GridSite],
    allowed: &'decl [String],
) -> Declarations<'decl> {
    Declarations {
        providers,
        sites,
        local_site: "site-a",
        network_name: "net",
        from_providers: allowed,
    }
}

/// A `GridSite` as discovery writes one, with the site id in the annotation.
fn discovered_site(generated_name: &str, site_id: &str, egress: &serde_json::Value) -> GridSite {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridSite",
        "metadata": {
            "name": generated_name,
            "labels": { "grid.praxis.fast/auto-discovered": "true" },
            "annotations": { "grid.praxis.fast/site-id": site_id },
        },
        "spec": { "gridNetworkRef": "net", "egress": egress },
        "status": { "phase": "Active" },
    }))
    .expect("discovered site")
}

#[test]
fn a_remote_candidate_the_gateway_did_not_allowlist_derives_nothing() {
    // The allowlist gates the remote hop too.
    let sites = vec![
        site("site-a", None),
        site(
            "site-b",
            Some(mutual_egress(
                "site-b.grid.example.invalid:8443",
                "site-b.grid.internal",
            )),
        ),
    ];
    let candidates = vec![candidate("prov-b", "site-b")];
    let allowed = vec!["prov-other".to_owned()];
    let resolved = resolve(&candidates, &[], &decl_allowing(&[], &sites, &allowed));
    assert!(
        resolved.is_empty(),
        "a remote candidate outside the allowlist must not derive its site egress"
    );
}

#[test]
fn a_discovered_site_is_found_by_the_id_it_speaks_for() {
    // Matching on the object name would miss discovered stubs.
    let sites = vec![discovered_site(
        "net-site-b-7f3a",
        "site-b",
        &mutual_egress("site-b.grid.example.invalid:8443", "site-b.grid.internal"),
    )];
    let candidates = vec![candidate("prov-b", "site-b")];
    let allowed = vec!["prov-b".to_owned()];
    let resolved = resolve(&candidates, &[], &decl_allowing(&[], &sites, &allowed));
    let got = resolved.get("prov-b").expect("a discovered site still derives");
    assert_eq!(got.endpoint.address, "site-b.grid.example.invalid:8443");
}

/// Every refusal names its reason.
#[test]
#[expect(clippy::too_many_lines, reason = "one row per refusal reason")]
fn each_refusal_is_named() {
    let unavailable = {
        let mut p = provider("prov-down", "https://down.example.invalid", None);
        p.status = serde_json::from_value(serde_json::json!({ "phase": "Unavailable" })).ok();
        p
    };
    let providers = vec![
        provider("prov-a", "https://a.example.invalid", None),
        provider("prov-twin", "https://twin1.example.invalid", None),
        provider("prov-twin", "https://twin2.example.invalid", None),
        provider("prov-port", "https://p.example.invalid:99999", None),
        provider("prov-ip", "https://10.0.0.5:8443", None),
        provider(
            "prov-badname",
            "https://10.0.0.6:8443",
            Some(serde_json::json!({ "serverName": "host:443" })),
        ),
        provider("prov-scheme", "ftp://s.example.invalid", None),
        unavailable,
    ];
    let sites = vec![
        site("site-a", None),
        site_in_phase(
            "site-c",
            Some(mutual_egress("c.example.invalid:8443", "c.grid.internal")),
            "Connecting",
        ),
        site("site-n", None),
    ];
    let allowed: Vec<String> = [
        "prov-twin",
        "prov-port",
        "prov-ip",
        "prov-badname",
        "prov-scheme",
        "prov-down",
        "prov-none",
        "prov-c",
        "prov-n",
        "prov-ghost",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    let cases = [
        ("prov-a", "site-a", Refusal::NotAllowlisted),
        ("prov-twin", "site-a", Refusal::Ambiguous),
        ("prov-port", "site-a", Refusal::EndpointPort),
        ("prov-ip", "site-a", Refusal::ServerNameNeeded),
        ("prov-badname", "site-a", Refusal::ServerNameInvalid),
        ("prov-scheme", "site-a", Refusal::EndpointUnusable),
        ("prov-down", "site-a", Refusal::Unavailable),
        ("prov-none", "site-a", Refusal::NoProvider),
        ("prov-c", "site-c", Refusal::SiteNotActive),
        ("prov-n", "site-n", Refusal::NoEgress),
        ("prov-ghost", "site-ghost", Refusal::SiteUnknown),
    ];
    for (cluster, at, want) in cases {
        let candidates = vec![candidate(cluster, at)];
        let resolution = resolve(&candidates, &[], &decl_allowing(&providers, &sites, &allowed));
        let got: Vec<Refusal> = resolution.refused.iter().map(|r| r.reason).collect();
        assert_eq!(got, [want], "{cluster} at {at}");
        assert!(resolution.resolved.is_empty(), "{cluster}: refused, so not resolved");
    }
    let twice = vec![candidate("prov-a", "site-a"), candidate("prov-a", "site-n")];
    let allowed_a = vec!["prov-a".to_owned()];
    let resolution = resolve(&twice, &[], &decl_allowing(&providers, &sites, &allowed_a));
    assert!(
        resolution
            .refused
            .iter()
            .all(|r| r.reason == Refusal::ClusterAtTwoSites)
            && resolution.refused.len() == 2,
        "one cluster at two sites refuses both: {:?}",
        resolution.refused
    );
}

#[test]
fn a_provider_the_gateway_did_not_allowlist_derives_nothing() {
    // Registering a cluster-scoped CR is not permission to be dialled.
    let providers = vec![provider("prov-a", "https://a.example.invalid", None)];
    let sites = vec![site("site-a", None)];
    let candidates = vec![candidate("prov-a", "site-a")];
    let resolved = resolve(&candidates, &[], &decl_allowing(&providers, &sites, &[]));
    assert!(
        resolved.is_empty(),
        "an empty allowlist must derive nothing, not everything"
    );
}

#[test]
fn a_provider_outside_a_named_allowlist_derives_nothing() {
    // Naming prov-b admits prov-b and nothing else.
    let providers = vec![
        provider("prov-a", "https://a.example.invalid", None),
        provider("prov-b", "https://b.example.invalid", None),
    ];
    let sites = vec![site("site-a", None)];
    let candidates = vec![candidate("prov-a", "site-a"), candidate("prov-b", "site-a")];
    let allowed = vec!["prov-b".to_owned()];
    let resolved = resolve(&candidates, &[], &decl_allowing(&providers, &sites, &allowed));
    assert_eq!(
        resolved.keys().collect::<Vec<_>>(),
        ["prov-b"],
        "a registered provider the gateway did not name is not derived"
    );
}

#[test]
fn a_provider_in_another_network_is_not_a_source() {
    let mut foreign = provider("prov-a", "https://elsewhere.example.invalid", None);
    foreign.spec.grid_network_ref = "other-net".to_owned();
    let providers = vec![foreign];
    let sites = vec![site("site-a", None)];
    let candidates = vec![candidate("prov-a", "site-a")];
    let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
    assert!(
        resolved.is_empty(),
        "a provider in another network must not supply this network's topology"
    );
}

#[test]
fn an_explicitly_unavailable_provider_is_not_a_source() {
    let mut down = provider("prov-a", "https://a.example.invalid", None);
    down.status = Some(serde_json::from_value(serde_json::json!({ "phase": "Unavailable" })).expect("status"));
    let providers = vec![down];
    let sites = vec![site("site-a", None)];
    let candidates = vec![candidate("prov-a", "site-a")];
    let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
    assert!(
        resolved.is_empty(),
        "an Unavailable provider must not supply an endpoint"
    );
}

#[test]
fn two_providers_claiming_one_identity_derive_nothing() {
    // Last-write-wins would silently pick one provider's endpoint.
    let mut second = provider("zzz-shadow", "https://attacker.example.invalid", None);
    second.spec.routing_cluster_ref = Some("prov-a".to_owned());
    let providers = vec![provider("prov-a", "https://real.example.invalid", None), second];
    let sites = vec![site("site-a", None)];
    let candidates = vec![candidate("prov-a", "site-a")];
    let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
    assert!(
        resolved.is_empty(),
        "an ambiguous identity must refuse rather than pick one"
    );
}

#[test]
fn one_cluster_at_two_sites_derives_nothing() {
    // Keying by cluster alone would let site-name order decide the topology.
    let providers = vec![provider("prov-a", "https://a.example.invalid", None)];
    let sites = vec![
        site("site-a", None),
        site(
            "site-b",
            Some(mutual_egress(
                "site-b.grid.example.invalid:8443",
                "site-b.grid.internal",
            )),
        ),
    ];
    let candidates = vec![candidate("prov-a", "site-a"), candidate("prov-a", "site-b")];
    let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
    assert!(
        resolved.is_empty(),
        "a cluster present at two sites must refuse rather than resolve by name order"
    );
}

#[test]
fn a_site_that_is_not_active_is_not_a_source() {
    for phase in ["Pending", "Discovered", "Connecting", "Unreachable", "Left"] {
        let sites = vec![
            site("site-a", None),
            site_in_phase(
                "site-b",
                Some(mutual_egress(
                    "site-b.grid.example.invalid:8443",
                    "site-b.grid.internal",
                )),
                phase,
            ),
        ];
        let candidates = vec![candidate("prov-b", "site-b")];
        let resolved = resolve(&candidates, &[], &decl(&[], &sites, "site-a"));
        assert!(
            resolved.is_empty(),
            "{phase} is not probed and pinned, so its egress must not become an upstream"
        );
    }
}

#[test]
fn a_port_the_url_declares_but_cannot_represent_derives_nothing() {
    // http::Uri reports no port for these, so a scheme default would dial 443.
    for endpoint in [
        "https://model-gw:99999",
        "https://model-gw:65536",
        "https://model-gw:abc",
        "https://model-gw:",
        "https://model-gw:0",
    ] {
        let providers = vec![provider("prov-a", endpoint, None)];
        let sites = vec![site("site-a", None)];
        let candidates = vec![candidate("prov-a", "site-a")];
        let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
        assert!(
            resolved.is_empty(),
            "{endpoint} must refuse rather than derive a scheme default"
        );
    }
}

#[test]
fn an_https_endpoint_named_by_address_needs_a_declared_server_name() {
    // Praxis rejects an IP literal as an SNI.
    let providers = vec![provider("prov-a", "https://10.0.0.7:8443", None)];
    let sites = vec![site("site-a", None)];
    let candidates = vec![candidate("prov-a", "site-a")];
    let resolved = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
    assert!(
        resolved.is_empty(),
        "an address-named https endpoint must refuse without a server name"
    );
}

#[test]
fn a_server_name_that_is_not_a_dns_hostname_is_refused() {
    let long_label = "a".repeat(64);
    let too_long = format!("{}.example", "a.".repeat(124));
    let cases = [
        ("https://model-gw:8443", "10.0.0.7"),
        ("https://model-gw:8443", "::1"),
        ("https://model-gw:8443", "[::1]"),
        ("https://model-gw:8443", "host:443"),
        ("https://model-gw:8443", "a/b"),
        ("https://model-gw:8443", "has space.example"),
        ("https://model-gw:8443", "-lead.example"),
        ("https://model-gw:8443", "trail-.example"),
        ("https://model-gw:8443", long_label.as_str()),
        ("https://model-gw:8443", too_long.as_str()),
    ];
    for (endpoint, name) in cases {
        let providers = vec![provider(
            "prov-a",
            endpoint,
            Some(serde_json::json!({ "serverName": name })),
        )];
        let sites = vec![site("site-a", None)];
        let resolution = resolve(
            &[candidate("prov-a", "site-a")],
            &[],
            &decl(&providers, &sites, "site-a"),
        );
        let got: Vec<Refusal> = resolution.refused.iter().map(|r| r.reason).collect();
        assert_eq!(got, [Refusal::ServerNameInvalid], "{name:?} must refuse, not render");
    }
}

#[test]
fn an_undeclared_url_host_that_is_not_a_dns_hostname_is_refused() {
    let providers = vec![provider("prov-a", "https://model_gw.ns.svc:8443", None)];
    let sites = vec![site("site-a", None)];
    let resolution = resolve(
        &[candidate("prov-a", "site-a")],
        &[],
        &decl(&providers, &sites, "site-a"),
    );
    let got: Vec<Refusal> = resolution.refused.iter().map(|r| r.reason).collect();
    assert_eq!(got, [Refusal::ServerNameInvalid], "the derived SNI is validated too");
}

#[test]
fn a_valid_declared_server_name_at_the_length_limit_resolves() {
    // 63 + 1 + 63 + 1 + 63 + 1 + 61 = 253.
    let name = format!("{a}.{a}.{a}.{b}", a = "a".repeat(63), b = "b".repeat(61));
    assert_eq!(name.len(), 253);
    let got = resolve_local("https://10.0.0.7:8443", Some(serde_json::json!({ "serverName": name })));
    assert_eq!(got.endpoint.transport.and_then(|t| t.sni), Some(name));
}

#[test]
fn a_cluster_with_no_provider_and_no_site_resolves_to_nothing() {
    let candidates = vec![candidate("prov-ghost", "site-ghost")];
    let resolved = resolve(&candidates, &[], &decl(&[], &[], "site-a"));
    assert!(
        resolved.is_empty(),
        "an unresolvable cluster is refused, then withdrawn"
    );
}

#[test]
fn an_mcp_tool_candidate_is_neither_resolved_nor_refused() {
    let mut tool = candidate("prov-ghost", "site-ghost");
    tool.kind = "mcp_tool".to_owned();
    let resolution = resolve(&[tool], &[], &decl(&[], &[], "site-a"));
    assert!(resolution.resolved.is_empty() && resolution.refused.is_empty());
}

/// Contract fixture the gateway crate loads through Praxis.
const DERIVED_RENDER_GOLDEN: &str = include_str!("../../../../gateway/tests/testdata/consumer-config-derived.yaml");

#[test]
#[expect(clippy::too_many_lines, reason = "the render takes every renderer input")]
fn a_derived_tls_render_matches_the_fixture_praxis_loads() {
    let providers = vec![
        provider("prov-a", "https://model-a.models.svc:8443", None),
        provider(
            "prov-b",
            "https://10.0.0.7:8443",
            Some(serde_json::json!({ "serverName": "model-b.example.internal" })),
        ),
    ];
    let sites = vec![site("site-a", None)];
    let candidates = vec![candidate("prov-a", "site-a"), candidate("prov-b", "site-a")];
    let resolution = resolve(&candidates, &[], &decl(&providers, &sites, "site-a"));
    assert!(resolution.refused.is_empty(), "{:?}", resolution.refused);
    let endpoints: Vec<ClusterEndpointConfig> = resolution.resolved.values().map(|r| r.endpoint.clone()).collect();
    let overlay = crate::resources::routing_overlay::RoutingOverlay {
        network: "net".to_owned(),
        local_site: "site-a".to_owned(),
        candidates,
        excluded: Vec::new(),
        selection_policy: None,
        generated_at: None,
    };
    let rendered = crate::resources::consumer_config::render_consumer_config_with_projected(
        &overlay,
        "/run/secrets/grid-credentials",
        &endpoints,
        "/etc/praxis/tls",
        8080,
        &crate::crd::grid_network::TlsConfig::default(),
        "inference-gw",
        "praxis-system",
        None,
        false,
        false,
    )
    .expect("render");
    assert_eq!(rendered.config_yaml, DERIVED_RENDER_GOLDEN, "golden drifted");
}
