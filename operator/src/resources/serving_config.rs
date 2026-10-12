//! Grid serving config the gateway reads from `GRID_SERVING_CONFIG` and re-reads on change.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

use k8s_openapi::api::core::v1::ConfigMap;
use serde::Serialize;

use crate::{
    resources::{
        geography::AdmissionState,
        overlay_envelope::hex_encode,
        routing_overlay::{RoutingCandidate, RoutingOverlay, overlay_labels, scoped_configmap_name},
        tls_backend::sha256,
    },
    signals::SIGNALS_PATH,
};

/// `ConfigMap` data key holding the serving config.
pub(crate) const SERVING_CONFIG_KEY: &str = "serving-config.json";

/// Annotation carrying the SHA-256 of the rendered config, for pod rollouts.
pub(crate) const ANNOTATION_DIGEST: &str = "grid.praxis.fast/serving-digest";

/// Label telling the serving `ConfigMap` apart from the overlay's.
const COMPONENT_LABEL: &str = "app.kubernetes.io/component";

/// Minimum spacing between writes of one `ConfigMap`.
pub(crate) const MIN_WRITE_INTERVAL: Duration = Duration::from_secs(30);

/// Store retention per series, seconds.
const WINDOW_SECS: u64 = 60;

/// How often the gateway polls its local operator, milliseconds.
const GATEWAY_POLL_MS: u64 = 5_000;

/// Gateway connect and request timeout to its local operator, milliseconds.
const GATEWAY_TIMEOUT_MS: u64 = 2_000;

/// Margin past the producers: `Date` is truncated to the second, which under-reads age,
/// and the gateway's fetch runs on past the `Date` it reads.
const MARGIN_MS: u64 = 2_000;

/// How many scrapes the worst-of horizon spans.
const HORIZON_SCRAPES: u32 = 3;

/// The freshness window the gateway reads load over, milliseconds.
///
/// The oldest a relayed sample can be when it arrives: a provider scrape, the
/// operator's peer poll and the budget that poll may take, the gateway's own poll
/// of the operator, and two hops of `Date` truncation. Plus one peer poll of
/// slack, so one late poll does not turn a healthy site unknown. About 82s at the
/// defaults. Sized shorter than one relay cycle, every remote site would be
/// unmeasured for most of every cycle and every gateway would route to its local
/// site in a square wave.
fn load_window_ms(scrape: Duration, peer_interval: Duration, peer_budget: Duration) -> i64 {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let window = ms(scrape)
        .saturating_add(ms(peer_interval))
        .saturating_add(ms(peer_budget))
        .saturating_add(GATEWAY_POLL_MS)
        .saturating_add(MARGIN_MS)
        .saturating_add(ms(peer_interval));
    i64::try_from(window).unwrap_or(i64::MAX)
}

/// How far behind a site's newest sample the gateway reads worst load, milliseconds.
///
/// A few scrapes, so one spike is forgotten within seconds whatever the relay cadence.
/// Anchored at the newest sample on the gateway, so it needs no relay term. About 17s at
/// the defaults, the freshness window before the gateway read its local operator.
fn horizon_ms(scrape: Duration) -> i64 {
    let horizon = u64::try_from(scrape.as_millis())
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::from(HORIZON_SCRAPES))
        .saturating_add(MARGIN_MS);
    i64::try_from(horizon).unwrap_or(i64::MAX)
}

/// Gateway-side cap on candidates (`validate_candidates`).
const MAX_CANDIDATES: usize = 1024;

/// Gateway-side cap on identifier length.
const MAX_NAME_LEN: usize = 256;

/// The only kind `grid_site_route` matches.
const INFERENCE_MODEL: &str = "inference_model";

/// Serving config, field for field the gateway's `GridServingConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ServingConfig {
    /// This gateway's own site.
    pub(crate) local_site: String,
    /// Store retention per series, seconds.
    pub(crate) window_secs: u64,
    /// Freshness window, milliseconds.
    pub(crate) load_window_ms: i64,
    /// Worst-of horizon behind a site's newest sample, milliseconds.
    pub(crate) horizon_ms: i64,
    /// Local site first, then by site, name, cluster.
    pub(crate) candidates: Vec<ServingCandidate>,
    /// Explicit mTLS provider gateways authorized to receive hop context.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) provider_hop_clusters: Vec<String>,
    /// Expected verified TLS SNI for each provider-hop cluster.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) provider_hop_sni: BTreeMap<String, String>,
    /// Sorted by site.
    pub(crate) peers: Vec<ServingPeer>,
}

/// One routable `(model, site, cluster)`, `cluster` naming a `load_balancer` cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ServingCandidate {
    /// Overlay-assigned stable identity used by authenticated provider hops.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stable_id: Option<String>,
    /// Always `inference_model`.
    pub(crate) kind: &'static str,
    /// Model name.
    pub(crate) name: String,
    /// Owning site, also the peer whose signals order it.
    pub(crate) site: String,
    /// Load balancer cluster, and the `grid_provider` label its load is keyed on.
    pub(crate) cluster: String,
    /// Freshness carried from the overlay.
    pub(crate) fresh: bool,
    /// Whether it takes new requests. Omitted when it does, so such a config
    /// still loads on a gateway that predates the field.
    #[serde(skip_serializing_if = "admits_new")]
    pub(crate) admission: AdmissionState,
}

/// Whether `admission` is the default, which the gateway assumes when absent.
#[expect(clippy::trivially_copy_pass_by_ref, reason = "serde skip_serializing_if signature")]
fn admits_new(admission: &AdmissionState) -> bool {
    *admission == AdmissionState::NewAndExisting
}

/// The local operator's signals endpoint and the identity material to reach it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ServingPeer {
    /// Site whose SPIFFE id the operator must present.
    pub(crate) site: String,
    /// `host:port` to dial.
    pub(crate) addr: String,
    /// SNI, which rustls requires though the gateway verifier ignores it.
    pub(crate) server_name: String,
    /// Host header.
    pub(crate) authority: String,
    /// Signals path.
    pub(crate) path: &'static str,
    /// Poll interval, milliseconds.
    pub(crate) interval_ms: u64,
    /// Connect timeout, milliseconds.
    pub(crate) connect_timeout_ms: u64,
    /// Request timeout, milliseconds.
    pub(crate) request_timeout_ms: u64,
    /// Grid CA bundle path.
    pub(crate) grid_ca_path: String,
    /// This site's certificate chain path.
    pub(crate) client_cert_path: String,
    /// This site's private key path.
    pub(crate) client_key_path: String,
}

/// Per-gateway inputs that are not in the overlay.
#[derive(Clone, Debug)]
pub(crate) struct ServingInputs<'input> {
    /// Verified gateway clusters allowed to carry provider-hop context.
    pub(crate) provider_hop_clusters: &'input BTreeSet<String>,
    /// Declared TLS identity for each provider-hop cluster.
    pub(crate) provider_hop_sni: &'input BTreeMap<String, String>,
    /// Directory holding this site's `ca.crt`, `tls.crt`, `tls.key` in the gateway pod.
    pub(crate) tls_mount: &'input str,
    /// Operator-configured address of this site's own signals endpoint.
    pub(crate) local_signals_addr: Option<&'input str>,
    /// How often operators scrape their providers, taken as every site's.
    pub(crate) scrape_interval: Duration,
    /// How often the operator polls each peer.
    pub(crate) peer_interval: Duration,
    /// The most one peer poll may take.
    pub(crate) peer_budget: Duration,
}

/// Render for one gateway. Its only peer is the local operator, which relays
/// every member it collects from.
///
/// An empty candidate list is an authoritative no-route state. It must still
/// be written so a gateway cannot retain candidates from an older config.
pub(crate) fn render(overlay: &RoutingOverlay, inputs: &ServingInputs<'_>) -> ServingConfig {
    let candidates = candidates(overlay);
    let peers = inputs
        .local_signals_addr
        .filter(|_| certs::validate_site_name(&overlay.local_site).is_ok())
        .map(|addr| peer(overlay.local_site.clone(), addr.to_owned(), inputs.tls_mount))
        .into_iter()
        .collect();
    ServingConfig {
        local_site: overlay.local_site.clone(),
        window_secs: WINDOW_SECS,
        load_window_ms: load_window_ms(inputs.scrape_interval, inputs.peer_interval, inputs.peer_budget),
        horizon_ms: horizon_ms(inputs.scrape_interval),
        candidates,
        provider_hop_clusters: inputs.provider_hop_clusters.iter().cloned().collect(),
        provider_hop_sni: inputs.provider_hop_sni.clone(),
        peers,
    }
}

/// A candidate's identity: remote after local, then site, name, cluster.
type CandidateKey<'overlay> = (bool, &'overlay str, &'overlay str, &'overlay str);

/// Each routable candidate once, with its freshness and admission.
///
/// Any stale or more restricted duplicate marks the tuple, whatever the order.
fn dedup<'overlay>(
    overlay: impl Iterator<Item = &'overlay RoutingCandidate>,
    local_site: &str,
) -> BTreeMap<CandidateKey<'overlay>, (bool, AdmissionState, Option<String>)> {
    let mut unique = BTreeMap::new();
    for candidate in overlay.filter(|candidate| routable(candidate)) {
        let key = (
            candidate.site != local_site,
            candidate.site.as_str(),
            candidate.name.as_str(),
            candidate.cluster.as_str(),
        );
        let admission = candidate.admission_state.unwrap_or(AdmissionState::NewAndExisting);
        // Seeded with the identities of both folds, so the first candidate and every
        // duplicate after it take the same path: fresh only if all are, admission as
        // restricted as the most restricted.
        let (fresh, held, stable_id) = unique
            .entry(key)
            .or_insert_with(|| (true, AdmissionState::NewAndExisting, candidate.stable_id.clone()));
        *fresh &= candidate.fresh;
        *held = (*held).max(admission);
        if stable_id.is_none() {
            stable_id.clone_from(&candidate.stable_id);
        }
    }
    unique
}

/// Inference candidates within gateway limits, deduplicated and ordered.
///
/// Past the cap, candidates taking new requests are kept before the rest.
fn candidates(overlay: &RoutingOverlay) -> Vec<ServingCandidate> {
    if overlay.candidates.is_empty() {
        return Vec::new();
    }
    // Local first so cold start, before any signal, prefers this site.
    let unique = dedup(overlay.candidates.iter().chain(&overlay.excluded), &overlay.local_site);
    let mut kept: Vec<_> = unique.into_iter().collect();
    if kept.len() > MAX_CANDIDATES {
        tracing::warn!(
            candidates = kept.len(),
            "serving config: dropping candidates past the gateway cap"
        );
        kept.sort_by_key(|(key, (_, admission, _))| (*admission != AdmissionState::NewAndExisting, *key));
        kept.truncate(MAX_CANDIDATES);
        kept.sort_by_key(|(key, _)| *key);
    }
    kept.into_iter()
        .map(
            |((_, site, name, cluster), (fresh, admission, stable_id))| ServingCandidate {
                stable_id,
                kind: INFERENCE_MODEL,
                name: name.to_owned(),
                site: site.to_owned(),
                cluster: cluster.to_owned(),
                fresh,
                admission,
            },
        )
        .collect()
}

/// An inference candidate the gateway will accept, whatever its admission.
///
/// A candidate not taking new requests stays in, so the gateway can tell a known
/// down model, answered with 503, from an unknown one. Its site must be a DNS-1123
/// label, as enrolled site names are: the gateway carries it in headers and ids.
fn routable(candidate: &RoutingCandidate) -> bool {
    candidate.kind == INFERENCE_MODEL
        && [&candidate.name, &candidate.site, &candidate.cluster]
            .into_iter()
            .all(|id| valid_id(id))
        && certs::validate_site_name(&candidate.site).is_ok()
}

/// The candidate sites the serving config refuses for not being DNS-1123 labels.
fn refused_sites(overlay: &RoutingOverlay) -> BTreeSet<String> {
    overlay
        .candidates
        .iter()
        .chain(&overlay.excluded)
        .filter(|candidate| certs::validate_site_name(&candidate.site).is_err())
        .map(|candidate| candidate.site.clone())
        .collect()
}

/// The refused sites last seen per `GridNetwork`, keyed by network name.
pub(crate) type RefusedSites = Mutex<HashMap<String, BTreeSet<String>>>;

/// Warn when `network`'s set of refused sites changes, so an operator sees why a site never
/// routes. Kept per network, so two networks do not flip each other's warning.
pub(crate) fn warn_refused_sites(overlay: &RoutingOverlay, network: &str, last: &RefusedSites) {
    let refused = refused_sites(overlay);
    let mut last = last.lock().unwrap_or_else(PoisonError::into_inner);
    if last.get(network) != Some(&refused) {
        if !refused.is_empty() {
            tracing::warn!(network, sites = ?refused, "serving config: refusing candidates whose site is not a DNS-1123 label");
        }
        last.insert(network.to_owned(), refused);
    }
}

/// Non-blank and within the gateway's identifier bound.
fn valid_id(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_NAME_LEN
}

/// The local operator entry, presenting this site's own identity from `tls_mount`.
fn peer(site: String, addr: String, tls_mount: &str) -> ServingPeer {
    let name = format!("{site}.{}", certs::SPIFFE_TRUST_DOMAIN);
    ServingPeer {
        site,
        addr,
        server_name: name.clone(),
        authority: name,
        path: SIGNALS_PATH,
        interval_ms: GATEWAY_POLL_MS,
        connect_timeout_ms: GATEWAY_TIMEOUT_MS,
        request_timeout_ms: GATEWAY_TIMEOUT_MS,
        grid_ca_path: format!("{tls_mount}/ca.crt"),
        client_cert_path: format!("{tls_mount}/tls.crt"),
        client_key_path: format!("{tls_mount}/tls.key"),
    }
}

/// Serialize deterministically as JSON, which the gateway parses as YAML.
pub(crate) fn to_text(config: &ServingConfig) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(config)
}

/// Lowercase hex SHA-256 of the rendered text.
pub(crate) fn digest(text: &str) -> String {
    hex_encode(&sha256(text.as_bytes()))
}

/// `grid-serving-{network}-{gateway}`, hash-suffixed like the overlay name.
pub(crate) fn configmap_name(network_name: &str, gateway_name: &str) -> String {
    scoped_configmap_name("grid-serving", network_name, gateway_name)
}

/// The `ConfigMap` the gateway mounts.
pub(crate) fn build_configmap(text: &str, network_name: &str, gateway_name: &str, namespace: &str) -> ConfigMap {
    ConfigMap {
        metadata: kube::api::ObjectMeta {
            annotations: Some(BTreeMap::from([(ANNOTATION_DIGEST.to_owned(), digest(text))])),
            labels: Some(serving_labels(network_name, gateway_name)),
            name: Some(configmap_name(network_name, gateway_name)),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(SERVING_CONFIG_KEY.to_owned(), text.to_owned())])),
        ..Default::default()
    }
}

/// Overlay labels plus a component.
fn serving_labels(network_name: &str, gateway_name: &str) -> BTreeMap<String, String> {
    let mut labels = overlay_labels(network_name, gateway_name);
    labels.insert(COMPONENT_LABEL.to_owned(), "serving".to_owned());
    labels
}

/// What to do with a rendered config.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteDecision {
    /// Stored content already matches.
    Unchanged,
    /// Changed, but written too recently. Retry after this long.
    Deferred(Duration),
    /// Apply it.
    Write,
}

/// Decide from the stored content.
pub(crate) fn decide_write(
    existing: Option<&str>,
    desired: &str,
    last_write: Option<Instant>,
    now: Instant,
) -> WriteDecision {
    let wait = last_write.map_or(Duration::ZERO, |at| {
        MIN_WRITE_INTERVAL.saturating_sub(now.saturating_duration_since(at))
    });
    match existing {
        Some(stored) if stored == desired => WriteDecision::Unchanged,
        Some(_) if !wait.is_zero() => WriteDecision::Deferred(wait),
        // An absent config is never deferred: the gateway is waiting on it.
        _ => WriteDecision::Write,
    }
}

/// Withdrawals are safety-critical: do not let the ordinary write interval
/// leave an older serving route active after Grid has computed no candidates.
pub(crate) fn decide_empty_withdrawal(existing: Option<&str>, desired: &str) -> WriteDecision {
    match existing {
        Some(stored) if stored == desired => WriteDecision::Unchanged,
        _ => WriteDecision::Write,
    }
}

/// Last write time per `namespace/name`, for [`decide_write`].
#[derive(Debug, Default)]
pub(crate) struct WriteGate {
    /// Keyed by `namespace/name`.
    last: Mutex<HashMap<String, Instant>>,
}

impl WriteGate {
    /// When `key` was last written, if this process wrote it.
    pub(crate) fn last(&self, key: &str) -> Option<Instant> {
        self.held().get(key).copied()
    }

    /// Record a write of `key` at `at`.
    pub(crate) fn record(&self, key: &str, at: Instant) {
        self.held().insert(key.to_owned(), at);
    }

    /// The map, even after a panic elsewhere: a timestamp cannot be half-written.
    fn held(&self) -> std::sync::MutexGuard<'_, HashMap<String, Instant>> {
        self.last.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;

    /// Contract fixture the gateway crate parses with its own types.
    const GOLDEN: &str = include_str!("../../../gateway/ai-grid-filters/testdata/serving-config.json");

    /// The local operator's signals Service.
    const LOCAL_ADDR: &str = "grid-operator-signals.grid.svc:9091";
    /// No authenticated provider gateway in this fixture.
    static NO_PROVIDER_HOPS: BTreeSet<String> = BTreeSet::new();
    /// No declared provider-hop identities in the default fixture.
    static NO_PROVIDER_HOP_SNI: BTreeMap<String, String> = BTreeMap::new();

    const INPUTS: ServingInputs<'static> = ServingInputs {
        provider_hop_clusters: &NO_PROVIDER_HOPS,
        provider_hop_sni: &NO_PROVIDER_HOP_SNI,
        tls_mount: "/etc/praxis/tls",
        local_signals_addr: Some(LOCAL_ADDR),
        scrape_interval: Duration::from_secs(5),
        peer_interval: Duration::from_secs(30),
        peer_budget: Duration::from_secs(10),
    };

    fn cand(name: &str, site: &str, cluster: &str, admission: Option<&str>) -> RoutingCandidate {
        let mut value = serde_json::json!({
            "kind": "inference_model", "name": name, "site": site, "cluster": cluster, "fresh": true,
        });
        if let Some(state) = admission {
            value["admission_state"] = serde_json::Value::from(state);
        }
        serde_json::from_value(value).expect("candidate")
    }

    fn overlay(candidates: Vec<RoutingCandidate>) -> RoutingOverlay {
        RoutingOverlay {
            network: "grid".to_owned(),
            local_site: "site-a".to_owned(),
            candidates,
            excluded: Vec::new(),
            selection_policy: None,
            generated_at: None,
        }
    }

    fn two_site() -> RoutingOverlay {
        overlay(vec![
            cand("llama", "site-b", "pool-b", None),
            cand("llama", "site-a", "pool-a", Some("new_and_existing")),
        ])
    }

    fn peer_sites(config: &ServingConfig) -> Vec<(&str, &str)> {
        config
            .peers
            .iter()
            .map(|peer| (peer.site.as_str(), peer.addr.as_str()))
            .collect()
    }

    #[test]
    fn the_load_window_covers_the_oldest_a_sample_can_arrive_plus_a_poll() {
        assert_eq!(
            load_window_ms(Duration::from_secs(5), Duration::from_secs(30), Duration::from_secs(10)),
            82_000,
            "5s scrape + 30s peer poll + 10s budget + 5s gateway poll + 2s Date + 30s slack"
        );
        assert_eq!(
            load_window_ms(Duration::from_secs(1), Duration::from_secs(30), Duration::from_secs(10)),
            78_000
        );
        let oldest_arrival = 5_000 + 30_000 + 10_000 + GATEWAY_POLL_MS + MARGIN_MS;
        assert!(
            i64::try_from(oldest_arrival).unwrap_or(i64::MAX)
                < load_window_ms(Duration::from_secs(5), Duration::from_secs(30), Duration::from_secs(10)),
            "a sample at the worst-case age is still fresh"
        );
    }

    #[test]
    fn the_window_outlasts_one_relay_cycle() {
        let peer_interval = Duration::from_secs(30);
        let window = load_window_ms(Duration::from_secs(5), peer_interval, Duration::from_secs(10));
        assert!(
            window > i64::try_from(2 * peer_interval.as_millis()).unwrap_or(i64::MAX),
            "shorter than two relay cycles, a remote site is unmeasured for most of each cycle"
        );
    }

    #[test]
    fn the_horizon_spans_a_few_scrapes_and_fits_the_retention() {
        assert_eq!(horizon_ms(Duration::from_secs(5)), 17_000, "3 scrapes + 2s margin");
        assert!(
            horizon_ms(Duration::from_secs(5)) <= i64::try_from(WINDOW_SECS * 1_000).unwrap_or(i64::MAX),
            "retention evicts behind the newest sample, so it must hold the horizon"
        );
    }

    #[test]
    fn renders_the_contract_the_gateway_parses() {
        let config = render(&two_site(), &INPUTS);
        assert_eq!(to_text(&config).expect("json"), GOLDEN.trim_end(), "golden drifted");
    }

    #[test]
    fn empty_overlay_renders_authoritative_no_route_config() {
        let config = render(&overlay(Vec::new()), &INPUTS);
        assert!(config.candidates.is_empty(), "the empty overlay withdraws every route");
        assert_eq!(
            peer_sites(&config),
            [("site-a", LOCAL_ADDR)],
            "the local poller remains ready for restoration"
        );
        assert!(
            to_text(&config).expect("json").contains("\"candidates\": []"),
            "the serialized revision carries an empty candidate list"
        );
    }

    #[test]
    fn output_is_independent_of_input_order() {
        let mut shuffled = two_site();
        shuffled.candidates.push(cand("llama", "site-c", "pool-c", None));
        let mut ordered = two_site();
        ordered.candidates.insert(0, cand("llama", "site-c", "pool-c", None));
        let first = to_text(&render(&shuffled, &INPUTS)).expect("json");
        let second = to_text(&render(&ordered, &INPUTS)).expect("json");
        assert_eq!(first, second, "same inputs in any order render the same bytes");
    }

    #[test]
    fn local_candidates_lead_and_duplicates_collapse() {
        let mut dup = two_site();
        dup.candidates.push(cand("llama", "site-b", "pool-b", None));
        let config = render(&dup, &INPUTS);
        let order: Vec<&str> = config.candidates.iter().map(|c| c.site.as_str()).collect();
        assert_eq!(order, ["site-a", "site-b"], "local first, one entry per tuple");
    }

    #[test]
    fn refused_sites_name_only_the_invalid_ones() {
        let sites = refused_sites(&overlay(vec![
            cand("llama", "site-a", "pool-a", None),
            cand("llama", "site.b", "pool-b", None),
        ]));
        assert_eq!(sites, BTreeSet::from(["site.b".to_owned()]));
    }

    #[test]
    fn refused_sites_are_kept_per_network() {
        let last = RefusedSites::default();
        let bad = overlay(vec![cand("llama", "site.b", "pool-b", None)]);
        let good = overlay(vec![cand("llama", "site-a", "pool-a", None)]);
        warn_refused_sites(&bad, "east", &last);
        warn_refused_sites(&good, "west", &last);
        let held = last.into_inner().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(held.get("east"), Some(&BTreeSet::from(["site.b".to_owned()])));
        assert_eq!(
            held.get("west"),
            Some(&BTreeSet::new()),
            "another network does not clear it"
        );
    }

    #[test]
    fn candidates_the_gateway_would_reject_are_dropped() {
        let cases = [
            ("blank cluster", cand("llama", "site-b", " ", None)),
            (
                "oversized name",
                cand(&"m".repeat(MAX_NAME_LEN + 1), "site-b", "pool-b", None),
            ),
            ("site with a dot", cand("llama", "site.b", "pool-b", None)),
            ("site with a slash", cand("llama", "site/b", "pool-b", None)),
            ("site with a quote", cand("llama", "site\"b", "pool-b", None)),
            ("uppercase site", cand("llama", "Site-B", "pool-b", None)),
        ];
        for (label, bad) in cases {
            assert!(render(&overlay(vec![bad]), &INPUTS).candidates.is_empty(), "{label}");
        }
        let mut mcp = cand("tool", "site-b", "pool-b", None);
        mcp.kind = "mcp_tool".to_owned();
        assert!(render(&overlay(vec![mcp]), &INPUTS).candidates.is_empty(), "mcp_tool");
    }

    #[test]
    fn an_excluded_candidate_reaches_the_serving_config_but_not_the_overlay_wire() {
        let mut source = overlay(vec![cand("llama", "site-a", "pool-a", None)]);
        source.excluded = vec![cand("llama", "site-b", "pool-b", Some("none"))];
        let rendered = render(&source, &INPUTS);
        let config: serde_json::Value = serde_json::from_str(&to_text(&rendered).expect("text")).expect("json");
        let pool_b = config["candidates"]
            .as_array()
            .expect("candidates")
            .iter()
            .find(|c| c["cluster"] == "pool-b")
            .expect("the excluded candidate is listed");
        assert_eq!(pool_b["admission"], "none");
        let wire = serde_json::to_string(&source).expect("overlay json");
        assert!(!wire.contains("pool-b"), "the overlay wire is unchanged: {wire}");
    }

    #[test]
    fn past_the_cap_candidates_taking_new_requests_are_kept_first() {
        let excluded: Vec<_> = (0..MAX_CANDIDATES)
            .map(|i| cand("llama", "site-a", &format!("down-{i:04}"), Some("none")))
            .collect();
        let mut source = overlay(vec![cand("llama", "site-z", "up", None)]);
        source.excluded = excluded;
        let rendered = render(&source, &INPUTS);
        assert_eq!(rendered.candidates.len(), MAX_CANDIDATES);
        assert!(
            rendered.candidates.iter().any(|c| c.cluster == "up"),
            "the one admitted candidate survives the cap"
        );
    }

    #[test]
    fn a_candidate_not_taking_new_requests_stays_in_with_its_admission() {
        let rendered = render(
            &overlay(vec![
                cand("llama", "site-a", "pool-a", None),
                cand("llama", "site-b", "pool-b", Some("none")),
                cand("llama", "site-d", "pool-d", Some("existing_only")),
            ]),
            &INPUTS,
        );
        let config: serde_json::Value = serde_json::from_str(&to_text(&rendered).expect("text")).expect("json");
        let admission: Vec<(&str, Option<&str>)> = config["candidates"]
            .as_array()
            .expect("candidates")
            .iter()
            .map(|c| (c["site"].as_str().unwrap_or(""), c["admission"].as_str()))
            .collect();
        assert_eq!(
            admission,
            [
                ("site-a", None),
                ("site-b", Some("none")),
                ("site-d", Some("existing_only"))
            ],
            "the default is omitted, so an older gateway still loads an all-admitted config"
        );
    }

    #[test]
    fn excluded_history_cannot_restore_an_authoritative_empty_revision() {
        let mut withdrawn = overlay(Vec::new());
        withdrawn
            .excluded
            .push(cand("llama", "site-b", "provider-b", Some("none")));
        let config = render(&withdrawn, &INPUTS);
        assert!(
            config.candidates.is_empty(),
            "an empty active overlay withdraws prior excluded history"
        );
    }

    #[test]
    fn authenticated_gateway_candidates_carry_overlay_id_and_mtls_allowlist() {
        let mut candidate = cand("llama", "site-b", "provider-b", None);
        candidate.stable_id = Some("257a9450".to_owned());
        let hops = BTreeSet::from(["provider-b".to_owned()]);
        let hop_sni = BTreeMap::from([("provider-b".to_owned(), "provider-b.example".to_owned())]);
        let inputs = ServingInputs {
            provider_hop_clusters: &hops,
            provider_hop_sni: &hop_sni,
            ..INPUTS
        };
        let config = render(&overlay(vec![candidate]), &inputs);
        assert_eq!(config.provider_hop_clusters, ["provider-b"]);
        assert_eq!(config.provider_hop_sni, hop_sni);
        assert_eq!(config.candidates[0].stable_id.as_deref(), Some("257a9450"));
        let json = to_text(&config).expect("JSON");
        assert!(json.contains("\"provider_hop_clusters\": [\n    \"provider-b\""));
        assert!(json.contains("\"stable_id\": \"257a9450\""));
    }

    #[test]
    fn a_stale_duplicate_wins_in_either_order() {
        let mut stale = cand("llama", "site-b", "pool-b", None);
        stale.fresh = false;
        let fresh = cand("llama", "site-b", "pool-b", None);
        for (label, pair) in [
            ("stale first", [stale.clone(), fresh.clone()]),
            ("stale last", [fresh, stale]),
        ] {
            let config = render(&overlay(pair.to_vec()), &INPUTS);
            assert_eq!(config.candidates.len(), 1, "{label}: one entry");
            assert!(!config.candidates[0].fresh, "{label}: stale");
        }
    }

    #[test]
    fn the_most_restricted_duplicate_wins_in_either_order() {
        let excluded = cand("llama", "site-b", "pool-b", Some("none"));
        let admitted = cand("llama", "site-b", "pool-b", Some("new_and_existing"));
        for (label, pair) in [
            ("excluded first", [excluded.clone(), admitted.clone()]),
            ("excluded last", [admitted, excluded]),
        ] {
            let config = render(&overlay(pair.to_vec()), &INPUTS);
            assert_eq!(config.candidates.len(), 1, "{label}: one entry");
            assert_eq!(
                config.candidates[0].admission,
                AdmissionState::Excluded,
                "{label}: one cluster, so a duplicate calling it unservable closes it"
            );
        }
    }

    #[test]
    fn the_only_peer_is_the_local_operator() {
        let config = render(&two_site(), &INPUTS);
        assert_eq!(
            peer_sites(&config),
            [("site-a", LOCAL_ADDR)],
            "remote sites are read through it"
        );
        let peer = config.peers.first().expect("peer");
        assert_eq!(peer.server_name, format!("site-a.{}", certs::SPIFFE_TRUST_DOMAIN));
        assert_eq!(peer.interval_ms, GATEWAY_POLL_MS);
        let without = ServingInputs {
            local_signals_addr: None,
            ..INPUTS
        };
        assert!(
            render(&two_site(), &without).peers.is_empty(),
            "no local address, no peer"
        );
    }

    #[test]
    fn a_non_dns_local_site_renders_no_peer() {
        let mut bad = two_site();
        bad.local_site = "Site_A".to_owned();
        assert!(
            render(&bad, &INPUTS).peers.is_empty(),
            "the local site name is the SPIFFE id"
        );
    }

    #[test]
    fn write_is_gated_on_content_and_spacing() {
        let base = Instant::now();
        let at = |secs| base.checked_add(Duration::from_secs(secs)).expect("instant");
        let now = at(100);
        let left = MIN_WRITE_INTERVAL.saturating_sub(Duration::from_secs(1));
        let cases = [
            ("absent", None, None, WriteDecision::Write),
            ("absent after a recent write", None, Some(at(99)), WriteDecision::Write),
            ("same content", Some("a"), Some(at(99)), WriteDecision::Unchanged),
            ("same content never written", Some("a"), None, WriteDecision::Unchanged),
            (
                "changed recently",
                Some("b"),
                Some(at(99)),
                WriteDecision::Deferred(left),
            ),
            ("changed at the interval", Some("b"), Some(at(70)), WriteDecision::Write),
            ("changed by someone else", Some("b"), None, WriteDecision::Write),
        ];
        for (label, existing, last, want) in cases {
            assert_eq!(decide_write(existing, "a", last, now), want, "{label}");
        }
    }

    #[test]
    fn empty_withdrawal_bypasses_write_spacing_but_not_content_equality() {
        assert_eq!(
            decide_empty_withdrawal(Some("empty"), "empty"),
            WriteDecision::Unchanged
        );
        assert_eq!(
            decide_empty_withdrawal(Some("old-route"), "empty"),
            WriteDecision::Write
        );
        assert_eq!(decide_empty_withdrawal(None, "empty"), WriteDecision::Write);
    }

    #[test]
    fn gate_remembers_writes_per_key() {
        let gate = WriteGate::default();
        let now = Instant::now();
        gate.record("ns/a", now);
        assert_eq!(gate.last("ns/a"), Some(now), "recorded");
        assert_eq!(gate.last("ns/b"), None, "other keys unaffected");
    }

    #[test]
    fn configmap_carries_name_labels_and_digest() {
        let cm = build_configmap("{}", "grid", "gw", "ns");
        assert_eq!(cm.metadata.name.as_deref(), Some("grid-serving-grid-gw"));
        let annotations = cm.metadata.annotations.expect("annotations");
        assert_eq!(annotations[ANNOTATION_DIGEST], digest("{}"));
        assert_eq!(cm.data.expect("data")[SERVING_CONFIG_KEY], "{}");
        assert_eq!(
            cm.metadata.labels.expect("labels")[COMPONENT_LABEL],
            "serving",
            "an overlay selector does not match it"
        );
        let long = configmap_name(&"n".repeat(60), &"g".repeat(60));
        assert!(long.len() <= 63 && long.starts_with("grid-serving-"), "{long}");
    }
}
