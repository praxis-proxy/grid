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
    signals::{PeerIdentities, PeerTrustMode, SIGNALS_PATH, SignalsEndpoint, dialable_signals_endpoint},
    swim::{MemberStatus, MembershipSnapshot},
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

/// Freshness window the gateway orders over, milliseconds.
const LOAD_WINDOW_MS: i64 = 30_000;

/// Peer poll interval, the operator's default local scrape interval.
const PEER_INTERVAL_MS: u64 = 5_000;

/// Peer connect and request timeout, milliseconds.
const PEER_TIMEOUT_MS: u64 = 2_000;

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
    /// Local site first, then by site, name, cluster.
    pub(crate) candidates: Vec<ServingCandidate>,
    /// Sorted by site.
    pub(crate) peers: Vec<ServingPeer>,
}

/// One routable `(model, site, cluster)`, `cluster` naming a `load_balancer` cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ServingCandidate {
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
}

/// One peer signals endpoint and the local identity material to reach it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ServingPeer {
    /// Site whose SPIFFE id the peer must present.
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
    /// Leaf digests the peer must also match, set only under pin trust.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) pins: Vec<String>,
}

/// Per-gateway inputs that are not in the overlay.
#[derive(Clone, Debug)]
pub(crate) struct ServingInputs<'input> {
    /// Directory holding this site's `ca.crt`, `tls.crt`, `tls.key` in the gateway pod.
    pub(crate) tls_mount: &'input str,
    /// Operator-configured address of this site's own signals endpoint.
    pub(crate) local_signals_addr: Option<&'input str>,
    /// Declared leaf digests per remote site, empty outside pin trust.
    pub(crate) pins: &'input BTreeMap<String, Vec<String>>,
}

/// Render from `(site, signals endpoint)` members, `None` when no candidate survives.
pub(crate) fn render<'member, Members>(
    overlay: &RoutingOverlay,
    members: Members,
    inputs: &ServingInputs<'_>,
) -> Option<ServingConfig>
where
    Members: IntoIterator<Item = (&'member str, &'member str)>,
{
    let candidates = candidates(&overlay.candidates, &overlay.local_site);
    if candidates.is_empty() {
        return None;
    }
    let sites: BTreeSet<&str> = candidates.iter().map(|candidate| candidate.site.as_str()).collect();
    let mut addrs = remote_addrs(members, &sites, &overlay.local_site);
    if let Some(addr) = inputs.local_signals_addr
        && sites.contains(overlay.local_site.as_str())
        && certs::validate_site_name(&overlay.local_site).is_ok()
    {
        addrs.insert(overlay.local_site.clone(), addr.to_owned());
    }
    let peers = addrs
        .into_iter()
        .map(|(site, addr)| {
            let pins = inputs.pins.get(&site).cloned().unwrap_or_default();
            peer(site, addr, inputs.tls_mount, pins)
        })
        .collect();
    Some(ServingConfig {
        local_site: overlay.local_site.clone(),
        window_secs: WINDOW_SECS,
        load_window_ms: LOAD_WINDOW_MS,
        candidates,
        peers,
    })
}

/// Alive members worth dialing at their signals endpoint, with pins, none over unencrypted gossip.
pub(crate) fn dialable_members<'snap>(
    snapshot: &'snap MembershipSnapshot,
    identities: &PeerIdentities,
    trust: PeerTrustMode,
    encrypted: bool,
    fallback_port: u16,
) -> Vec<(&'snap str, String, Vec<String>)> {
    if !encrypted {
        return Vec::new();
    }
    snapshot
        .members
        .iter()
        .filter(|member| member.status == MemberStatus::Alive)
        .filter_map(|member| {
            // One read admits the peer and supplies its pins.
            let pins = match trust {
                PeerTrustMode::Pin => Some(identities.pins_for(&member.site_id)).filter(|pins| !pins.is_empty())?,
                PeerTrustMode::Spiffe => Vec::new(),
            };
            let endpoint = dialable_signals_endpoint(member, fallback_port)?;
            Some((member.site_id.as_str(), endpoint.authority(), pins))
        })
        .collect()
}

/// Admitted inference candidates within gateway limits, deduplicated and ordered.
fn candidates(overlay: &[RoutingCandidate], local_site: &str) -> Vec<ServingCandidate> {
    // Local first so cold start, before any signal, prefers this site.
    let mut unique: BTreeMap<(bool, &str, &str, &str), bool> = BTreeMap::new();
    for candidate in overlay.iter().filter(|candidate| routable(candidate)) {
        let key = (
            candidate.site != local_site,
            candidate.site.as_str(),
            candidate.name.as_str(),
            candidate.cluster.as_str(),
        );
        // Any stale duplicate marks the tuple stale, whatever the order.
        unique
            .entry(key)
            .and_modify(|fresh| *fresh &= candidate.fresh)
            .or_insert(candidate.fresh);
    }
    if unique.len() > MAX_CANDIDATES {
        tracing::warn!(
            candidates = unique.len(),
            "serving config: dropping candidates past the gateway cap"
        );
    }
    unique
        .into_iter()
        .take(MAX_CANDIDATES)
        .map(|((_, site, name, cluster), fresh)| ServingCandidate {
            kind: INFERENCE_MODEL,
            name: name.to_owned(),
            site: site.to_owned(),
            cluster: cluster.to_owned(),
            fresh,
        })
        .collect()
}

/// An inference candidate admitting new requests that the gateway will accept.
fn routable(candidate: &RoutingCandidate) -> bool {
    candidate.kind == INFERENCE_MODEL
        && candidate
            .admission_state
            .is_none_or(|state| state == AdmissionState::NewAndExisting)
        && [&candidate.name, &candidate.site, &candidate.cluster]
            .into_iter()
            .all(|id| valid_id(id))
}

/// Non-blank and within the gateway's identifier bound.
fn valid_id(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_NAME_LEN
}

/// Signals endpoint per remote candidate site, unsafe hosts refused.
fn remote_addrs<'member, Members>(
    members: Members,
    sites: &BTreeSet<&str>,
    local_site: &str,
) -> BTreeMap<String, String>
where
    Members: IntoIterator<Item = (&'member str, &'member str)>,
{
    members
        .into_iter()
        .filter(|(site, _)| *site != local_site && sites.contains(site) && certs::validate_site_name(site).is_ok())
        .filter_map(|(site, endpoint)| {
            let Some(parsed) = SignalsEndpoint::parse(endpoint).filter(SignalsEndpoint::is_dialable) else {
                tracing::warn!(site, endpoint, "serving config: refusing advertised peer address");
                return None;
            };
            Some((site.to_owned(), parsed.authority()))
        })
        .collect()
}

/// One peer entry, presenting this site's own identity from `tls_mount`.
fn peer(site: String, addr: String, tls_mount: &str, pins: Vec<String>) -> ServingPeer {
    let name = format!("{site}.{}", certs::SPIFFE_TRUST_DOMAIN);
    ServingPeer {
        site,
        addr,
        server_name: name.clone(),
        authority: name,
        path: SIGNALS_PATH,
        interval_ms: PEER_INTERVAL_MS,
        connect_timeout_ms: PEER_TIMEOUT_MS,
        request_timeout_ms: PEER_TIMEOUT_MS,
        grid_ca_path: format!("{tls_mount}/ca.crt"),
        client_cert_path: format!("{tls_mount}/tls.crt"),
        client_key_path: format!("{tls_mount}/tls.key"),
        pins,
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

    /// No pins, as under SPIFFE trust.
    static NO_PINS: BTreeMap<String, Vec<String>> = BTreeMap::new();

    const INPUTS: ServingInputs<'static> = ServingInputs {
        tls_mount: "/etc/praxis/tls",
        local_signals_addr: None,
        pins: &NO_PINS,
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
    fn renders_the_contract_the_gateway_parses() {
        let members = [("site-b", "203.0.113.7:9091"), ("site-a", "198.51.100.1:9091")];
        let config = render(&two_site(), members, &INPUTS).expect("routable");
        assert_eq!(to_text(&config).expect("json"), GOLDEN.trim_end(), "golden drifted");
    }

    #[test]
    fn output_is_independent_of_input_order() {
        let forward = [("site-b", "203.0.113.7:9091"), ("site-c", "203.0.113.9:9091")];
        let reverse = [("site-c", "203.0.113.9:9091"), ("site-b", "203.0.113.7:9091")];
        let mut shuffled = two_site();
        shuffled.candidates.push(cand("llama", "site-c", "pool-c", None));
        let mut ordered = two_site();
        ordered.candidates.insert(0, cand("llama", "site-c", "pool-c", None));
        let first = to_text(&render(&shuffled, forward, &INPUTS).expect("routable")).expect("json");
        let second = to_text(&render(&ordered, reverse, &INPUTS).expect("routable")).expect("json");
        assert_eq!(first, second, "same inputs in any order render the same bytes");
    }

    #[test]
    fn local_candidates_lead_and_duplicates_collapse() {
        let mut dup = two_site();
        dup.candidates.push(cand("llama", "site-b", "pool-b", None));
        let config = render(&dup, [], &INPUTS).expect("routable");
        let order: Vec<&str> = config.candidates.iter().map(|c| c.site.as_str()).collect();
        assert_eq!(order, ["site-a", "site-b"], "local first, one entry per tuple");
    }

    #[test]
    fn candidates_the_gateway_would_reject_or_never_admit_are_dropped() {
        let cases = [
            ("excluded", cand("llama", "site-b", "pool-b", Some("none"))),
            (
                "existing only",
                cand("llama", "site-b", "pool-b", Some("existing_only")),
            ),
            ("blank cluster", cand("llama", "site-b", " ", None)),
            (
                "oversized name",
                cand(&"m".repeat(MAX_NAME_LEN + 1), "site-b", "pool-b", None),
            ),
        ];
        for (label, bad) in cases {
            assert!(render(&overlay(vec![bad]), [], &INPUTS).is_none(), "{label}");
        }
        let mut mcp = cand("tool", "site-b", "pool-b", None);
        mcp.kind = "mcp_tool".to_owned();
        assert!(render(&overlay(vec![mcp]), [], &INPUTS).is_none(), "mcp_tool");
    }

    #[test]
    fn unsafe_or_irrelevant_peers_are_refused() {
        let cases = [
            ("loopback", "127.0.0.1:9091"),
            ("loopback v6", "[::1]:9091"),
            ("link local metadata", "169.254.169.254:9091"),
            ("alibaba metadata", "100.100.100.200:9091"),
            ("mapped loopback", "[::ffff:127.0.0.1]:9091"),
            ("link local v6", "[fe80::1]:9091"),
            ("unspecified", "0.0.0.0:9091"),
            ("aws metadata v6", "[fd00:ec2::254]:9091"),
            ("localhost", "localhost:9091"),
            ("no host", ":9091"),
            ("no port", "203.0.113.7"),
            ("bare ipv6", "fd00::1"),
            ("unbracketed ipv6 with a port", "fd00::1:9091"),
            ("userinfo", "x@169.254.169.254:443"),
            ("path and query", "evil.example/x?:9091"),
        ];
        for (label, endpoint) in cases {
            let config = render(&two_site(), [("site-b", endpoint)], &INPUTS).expect("routable");
            assert!(config.peers.is_empty(), "{label}: {endpoint} must be refused");
        }
        let other = render(&two_site(), [("site-z", "203.0.113.9:9091")], &INPUTS).expect("routable");
        assert!(other.peers.is_empty(), "a site with no candidate is not polled");
        let bad_name = overlay(vec![cand("llama", "Site_B", "pool-b", None)]);
        let config = render(&bad_name, [("Site_B", "203.0.113.7:9091")], &INPUTS).expect("routable");
        assert!(
            config.peers.is_empty(),
            "a non-DNS site name would fail the gateway's SNI"
        );
    }

    #[test]
    fn routable_and_named_peers_are_kept() {
        let members = [
            ("site-b", "203.0.113.7:9091"),
            ("site-c", "site-c.example.net:9091"),
            ("site-d", "[2001:db8::1]:9091"),
            ("site-e", "100.64.0.1:9091"),
            ("site-f", "[fd00::5]:9091"),
        ];
        let mut topo = two_site();
        for site in ["site-c", "site-d", "site-e", "site-f"] {
            topo.candidates.push(cand("llama", site, "pool", None));
        }
        let config = render(&topo, members, &INPUTS).expect("routable");
        assert_eq!(
            peer_sites(&config),
            [
                ("site-b", "203.0.113.7:9091"),
                ("site-c", "site-c.example.net:9091"),
                ("site-d", "[2001:db8::1]:9091"),
                ("site-e", "100.64.0.1:9091"),
                ("site-f", "[fd00::5]:9091"),
            ],
        );
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
            let config = render(&overlay(pair.to_vec()), [], &INPUTS).expect("routable");
            assert_eq!(config.candidates.len(), 1, "{label}: one entry");
            assert!(!config.candidates[0].fresh, "{label}: stale");
        }
    }

    fn member(site: &str, status: MemberStatus) -> crate::swim::MemberRecord {
        crate::swim::MemberRecord {
            site_id: site.to_owned(),
            endpoint: format!("{site}.example.net:7946"),
            incarnation: 1,
            status,
            age_secs: 0,
            gateway_address: None,
            site_cert_pem: None,
            signals_address: None,
        }
    }

    /// `pinned` declares a pin, `unpinned` does not.
    fn one_pinned_site() -> PeerIdentities {
        let identities = PeerIdentities::new();
        let pinned = crate::signals::PeerRecord {
            labels: BTreeMap::new(),
            pins: vec!["ab".repeat(32)],
        };
        identities.set(BTreeMap::from([
            ("pinned".to_owned(), pinned),
            ("unpinned".to_owned(), crate::signals::PeerRecord::default()),
        ]));
        identities
    }

    /// Sites `dialable_members` keeps from a fixed membership.
    fn dialable(trust: PeerTrustMode, encrypted: bool) -> Vec<String> {
        let snapshot = MembershipSnapshot {
            members: vec![
                member("pinned", MemberStatus::Alive),
                member("unpinned", MemberStatus::Alive),
                member("suspect", MemberStatus::Suspect),
            ],
        };
        dialable_members(&snapshot, &one_pinned_site(), trust, encrypted, 9091)
            .into_iter()
            .map(|(site, ..)| site.to_owned())
            .collect()
    }

    #[test]
    fn dialable_members_dial_the_gossiped_signals_endpoint() {
        let mut advertised = member("lb", MemberStatus::Alive);
        advertised.signals_address = Some("[2001:db8::7]:9443".to_owned());
        let mut v6 = member("v6", MemberStatus::Alive);
        v6.endpoint = "[2001:db8::9]:7946".to_owned();
        let mut junk = member("junk", MemberStatus::Alive);
        junk.signals_address = Some("no-port".to_owned());
        let mut smuggled = member("smuggled", MemberStatus::Alive);
        smuggled.signals_address = Some("x@169.254.169.254:443".to_owned());
        let snapshot = MembershipSnapshot {
            members: vec![advertised, member("older", MemberStatus::Alive), v6, junk, smuggled],
        };
        let got = dialable_members(&snapshot, &PeerIdentities::new(), PeerTrustMode::Spiffe, true, 9091);
        assert_eq!(
            got,
            [
                ("lb", "[2001:db8::7]:9443".to_owned(), Vec::new()),
                ("older", "older.example.net:9091".to_owned(), Vec::new()),
                ("v6", "[2001:db8::9]:9091".to_owned(), Vec::new()),
            ]
        );
    }

    #[test]
    fn a_pinned_member_carries_the_pins_that_admitted_it() {
        let snapshot = MembershipSnapshot {
            members: vec![
                member("pinned", MemberStatus::Alive),
                member("unpinned", MemberStatus::Alive),
            ],
        };
        let got = dialable_members(&snapshot, &one_pinned_site(), PeerTrustMode::Pin, true, 9091);
        assert_eq!(
            got,
            [("pinned", "pinned.example.net:9091".to_owned(), vec!["ab".repeat(32)])]
        );
    }

    #[test]
    fn dialable_members_by_trust_mode() {
        let (pin, spiffe) = (PeerTrustMode::Pin, PeerTrustMode::Spiffe);
        let cases: [(&str, PeerTrustMode, bool, &[&str]); 4] = [
            ("pin mode keeps only pinned sites", pin, true, &["pinned"]),
            ("spiffe mode reads no pins", spiffe, true, &["pinned", "unpinned"]),
            ("unencrypted gossip yields none", pin, false, &[]),
            ("unencrypted gossip yields none in spiffe mode", spiffe, false, &[]),
        ];
        for (label, trust, encrypted, want) in cases {
            assert_eq!(dialable(trust, encrypted), want, "{label}");
        }
    }

    #[test]
    fn pin_trust_carries_the_declared_pins_to_the_gateway() {
        let pins = BTreeMap::from([("site-b".to_owned(), vec!["ab".repeat(32)])]);
        let inputs = ServingInputs { pins: &pins, ..INPUTS };
        let config = render(&two_site(), [("site-b", "203.0.113.7:9091")], &inputs).expect("routable");
        let peer = config.peers.first().expect("peer");
        assert_eq!(peer.pins, ["ab".repeat(32)]);
        assert!(to_text(&config).expect("json").contains("\"pins\""), "rendered");
        let spiffe = to_text(&render(&two_site(), [("site-b", "203.0.113.7:9091")], &INPUTS).expect("routable"));
        assert!(!spiffe.expect("json").contains("pins"), "absent without pins");
    }

    #[test]
    fn the_local_peer_comes_from_config_not_gossip() {
        let inputs = ServingInputs {
            local_signals_addr: Some("grid-operator-signals.grid.svc:9091"),
            ..INPUTS
        };
        let gossip = [("site-a", "203.0.113.1:9091")];
        let config = render(&two_site(), gossip, &inputs).expect("routable");
        assert_eq!(peer_sites(&config), [("site-a", "grid-operator-signals.grid.svc:9091")]);
        let without = render(&two_site(), gossip, &INPUTS).expect("routable");
        assert!(without.peers.is_empty(), "gossip never names the local peer");
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
