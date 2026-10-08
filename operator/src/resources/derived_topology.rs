//! Derive consumer endpoint topology from provider and `GridSite` declarations.
//!
//! Registering an [`InferenceProvider`] is enough to route to it: this module
//! works out the endpoint topology that a human otherwise types into
//! `consumerConfig.clusterEndpoints`, and emits the same
//! [`ClusterEndpointConfig`] type so the renderer is unchanged.
//!
//! # Why the same type
//!
//! Every fail-closed reason already lives in the renderer. A derived entry is
//! validated by the code that validates a typed one, so derivation cannot
//! invent a new way to fail open, and a cluster it cannot resolve is simply
//! absent, which the renderer already reports as `MissingClusterEndpoint`.
//! Nothing here returns an error of its own. If it ever needs to, that is the
//! signal it has taken on something the renderer should be judging.
//!
//! # The rule this follows
//!
//! Derive a value only when it was discovered from the thing you are about to
//! connect to. A local backend address comes from the provider's own endpoint
//! declaration. A remote one comes from `GridSite.spec.egress.address`, which
//! the remote operator discovered from its provider gateway's Service and which
//! the `GridSite` controller probed over TLS and pinned before the site reached
//! `Active`. Grid 302 is the counterexample: the same field read as a SWIM
//! seed, where nothing listens.
//!
//! [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
//! [`ClusterEndpointConfig`]: crate::crd::grid_network::ClusterEndpointConfig

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    crd::{
        grid_network::{ClusterEndpointConfig, EndpointTransport, TransportMode},
        grid_site::{EgressTls, EgressTlsMode, GridSite},
        inference_provider::{BackendTls, InferenceProvider},
    },
    resources::routing_overlay::{RoutingCandidate, routing_identity},
};

/// Where a resolved entry came from, for status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Origin {
    /// Supplied in `consumerConfig.clusterEndpoints`.
    Explicit,
    /// Derived from the provider's own endpoint, at this site.
    DerivedLocal,
    /// Derived from the provider site's `GridSite` egress.
    DerivedRemote,
}

/// One cluster's resolved endpoint.
#[derive(Clone, Debug)]
pub(crate) struct Resolved {
    /// The entry the renderer consumes.
    pub(crate) endpoint: ClusterEndpointConfig,
    /// Where it came from.
    pub(crate) origin: Origin,
}

/// The declarations resolution reads. Grouped so the inputs stay one thing.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Declarations<'decl> {
    /// Providers registered in this cluster, which is what makes them local.
    pub(crate) providers: &'decl [InferenceProvider],
    /// Site inventory, the source of a remote provider hop.
    pub(crate) sites: &'decl [GridSite],
    /// This gateway's own site.
    pub(crate) local_site: &'decl str,
    /// The network both are scoped to.
    pub(crate) network_name: &'decl str,
    /// Routing identities the gateway owner accepts declarations from.
    ///
    /// Empty derives nothing. An `InferenceProvider` is cluster scoped with a
    /// self-asserted `gridNetworkRef`, so registering one must not by itself
    /// decide where this gateway dials.
    pub(crate) from_providers: &'decl [String],
}

/// Resolve an endpoint for every candidate cluster, deriving what is missing.
///
/// Explicit entries win whole. Field-level merge reads as the friendlier choice
/// and is the dangerous one: a half-typed entry would silently inherit derived
/// trust, so the operator would be supplying a CA for a connection a human
/// thought they had fully described.
///
/// Returns one entry per resolvable cluster, keyed by cluster name. A cluster
/// with no explicit entry and nothing to derive from is absent, which the
/// renderer reports as it always has.
pub(crate) fn resolve(
    candidates: &[RoutingCandidate],
    explicit: &[ClusterEndpointConfig],
    declarations: &Declarations<'_>,
) -> BTreeMap<String, Resolved> {
    let typed: BTreeMap<&str, &ClusterEndpointConfig> =
        explicit.iter().map(|entry| (entry.cluster.as_str(), entry)).collect();
    let local = local_providers(declarations);
    let clusters: BTreeSet<(&str, &str)> = candidates
        .iter()
        .map(|candidate| (candidate.cluster.as_str(), candidate.site.as_str()))
        .collect();

    // One cluster at two sites would otherwise collapse by site-name order,
    // picking a topology nobody declared. Refuse it instead: absence reaches
    // the renderer's existing MissingClusterEndpoint.
    let mut seen = BTreeSet::<&str>::new();
    let mut twice = BTreeSet::<&str>::new();
    for (cluster, _) in &clusters {
        if !seen.insert(cluster) {
            twice.insert(cluster);
        }
    }

    clusters
        .into_iter()
        .filter(|(cluster, _)| !twice.contains(cluster))
        .filter_map(|(cluster, site)| {
            resolve_one(cluster, site, &typed, &local, declarations).map(|entry| (cluster.to_owned(), entry))
        })
        .collect()
}

/// The providers this gateway accepts declarations from, by routing identity.
///
/// Allowlisted by the gateway owner, in this network, not explicitly
/// unavailable, and unambiguous. The sibling overlay paths apply the same
/// network and availability filters.
fn local_providers<'decl>(declarations: &Declarations<'decl>) -> BTreeMap<&'decl str, &'decl InferenceProvider> {
    let mut local: BTreeMap<&str, &InferenceProvider> = BTreeMap::new();
    let mut ambiguous: BTreeSet<&str> = BTreeSet::new();
    for provider in declarations.providers {
        let Some(identity) = routing_identity(provider) else {
            continue;
        };
        if !declarations.from_providers.iter().any(|named| named == identity)
            || provider.spec.grid_network_ref != declarations.network_name
            || crate::resources::routing_overlay::is_explicitly_unavailable(provider)
        {
            continue;
        }
        // Two providers claiming one identity would otherwise resolve by
        // iteration order, silently picking one of their endpoints.
        if local.insert(identity, provider).is_some() {
            ambiguous.insert(identity);
        }
    }
    for identity in &ambiguous {
        local.remove(identity);
    }
    local
}

/// Resolve one cluster: its explicit entry, else a derived local or remote one.
///
/// A cluster is local when a registered [`InferenceProvider`] carries its
/// routing identity **and** the candidate's site is this gateway's own. The
/// provider objects in hand are by definition the ones in this cluster, which is
/// a stronger test than comparing site names alone. Requiring both refuses to
/// guess for a provider declared here whose selector names another site, where
/// its endpoint is reachable from here and its candidate says otherwise.
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
fn resolve_one(
    cluster: &str,
    site: &str,
    typed: &BTreeMap<&str, &ClusterEndpointConfig>,
    local: &BTreeMap<&str, &InferenceProvider>,
    declarations: &Declarations<'_>,
) -> Option<Resolved> {
    if let Some(entry) = typed.get(cluster) {
        return Some(Resolved {
            endpoint: (*entry).clone(),
            origin: Origin::Explicit,
        });
    }
    if site == declarations.local_site {
        // Our own site, so it resolves from a provider we hold or not at all.
        // Falling through to the egress of our own site would hairpin the
        // consumer through its own provider gateway.
        return local.get(cluster).and_then(|provider| derive_local(cluster, provider));
    }
    derive_remote(cluster, site, declarations.sites, declarations.network_name)
}

/// Derive a local backend entry from the provider's own endpoint URL.
///
/// The scheme decides the transport, which is why `backendTls` does not declare
/// one. Trust comes only from `backendTls`: an omitted CA reference means the
/// process trust store, inherited from the explicit path rather than decided
/// again here.
fn derive_local(cluster: &str, provider: &InferenceProvider) -> Option<Resolved> {
    let endpoint = provider.spec.endpoint.trim();
    let uri = endpoint.parse::<http::Uri>().ok()?;
    let host = uri.host()?;
    if host.is_empty() {
        return None;
    }
    let tls = match uri.scheme_str()? {
        "https" => true,
        "http" => false,
        _ => return None,
    };
    let port = endpoint_port(&uri, tls)?;
    let backend = provider.spec.backend_tls.as_deref();
    if tls && !server_name_is_usable(backend, host) {
        return None;
    }
    let transport = backend_transport(backend, host, tls);
    Some(Resolved {
        endpoint: ClusterEndpointConfig {
            cluster: cluster.to_owned(),
            // `Uri::host()` keeps the brackets on an IP literal, so no bracketing here.
            address: format!("{host}:{port}"),
            transport: Some(transport),
        },
        origin: Origin::DerivedLocal,
    })
}

/// Derive a remote provider-hop entry from the provider site's `GridSite`.
///
/// The address is the site's egress address, which is the remote provider
/// gateway's own reachable address rather than an egress-only value. Client
/// identity is the grid identity the consumer already mounts, so nothing about
/// the backend's own credential crosses a site boundary.
fn derive_remote(cluster: &str, site: &str, sites: &[GridSite], network_name: &str) -> Option<Resolved> {
    let site = sites
        .iter()
        .find(|known| known.metadata.name.as_deref() == Some(site) && known.spec.grid_network_ref == network_name)?;
    // Only an Active site has had its address probed over TLS and its leaf
    // pinned. A Discovered or Connecting stub carries an address copied from
    // gossip, which is not something to hand the data plane.
    if !matches!(
        site.status.as_ref().map(|status| &status.phase),
        Some(crate::crd::grid_site::GridSitePhase::Active)
    ) {
        return None;
    }
    let egress = site.spec.egress.as_ref()?;
    let address = egress.address.trim();
    if address.is_empty() {
        return None;
    }
    Some(Resolved {
        endpoint: ClusterEndpointConfig {
            cluster: cluster.to_owned(),
            address: address.to_owned(),
            transport: Some(hop_transport(&egress.tls)),
        },
        origin: Origin::DerivedRemote,
    })
}

/// Transport for a provider hop, projected from the site's declared egress TLS.
///
/// No CA reference: the inter-site CA is the grid identity the consumer already
/// mounts, so nothing about a backend's own trust material crosses a site
/// boundary.
fn hop_transport(tls: &EgressTls) -> EndpointTransport {
    match tls.mode {
        EgressTlsMode::Mutual => EndpointTransport {
            mode: TransportMode::MutualTls,
            sni: tls
                .server_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned),
            ca_secret_ref: None,
        },
        EgressTlsMode::Plaintext => EndpointTransport {
            mode: TransportMode::Plaintext,
            sni: None,
            ca_secret_ref: None,
        },
    }
}

/// Whether a verified TLS connection to `host` has a name it can send.
///
/// Praxis rejects an IP literal as an SNI (RFC 6066), so an https endpoint
/// named by address needs a declared server name. Deriving one would emit a
/// config the gateway refuses to load at startup.
fn server_name_is_usable(backend: Option<&BackendTls>, host: &str) -> bool {
    let declared = backend
        .and_then(|backend| backend.server_name.as_deref())
        .map(str::trim)
        .is_some_and(|name| !name.is_empty());
    declared
        || host
            .trim_matches(|c| c == '[' || c == ']')
            .parse::<std::net::IpAddr>()
            .is_err()
}

/// The port the endpoint declares, or the scheme default when it declares none.
///
/// `http::Uri` accepts a port it cannot represent and then reports none, so
/// `https://host:99999` would otherwise derive `host:443`. Every other unusable
/// endpoint refuses, and so must this one rather than dial somewhere else.
fn endpoint_port(uri: &http::Uri, tls: bool) -> Option<u16> {
    if let Some(port) = uri.port_u16() {
        // Port 0 is not an endpoint, which `swim_endpoint` already decided.
        return (port != 0).then_some(port);
    }
    let authority = uri.authority().map(http::uri::Authority::as_str).unwrap_or_default();
    let host_part = authority.rsplit('@').next().unwrap_or(authority);
    if host_part.rsplit(':').count() > 1 && !host_part.ends_with(']') {
        // A colon the parser could not turn into a port.
        return None;
    }
    Some(if tls { 443 } else { 80 })
}

/// Transport for a local backend: the scheme chooses the mode, the declaration
/// supplies the trust.
///
/// A plaintext entry carries no server name even when one is declared, because
/// the renderer rejects plaintext with an SNI and a declared name is about
/// verification, which plaintext does not do.
fn backend_transport(backend: Option<&BackendTls>, host: &str, tls: bool) -> EndpointTransport {
    if !tls {
        return EndpointTransport {
            mode: TransportMode::Plaintext,
            sni: None,
            ca_secret_ref: None,
        };
    }
    let server_name = backend
        .and_then(|backend| backend.server_name.as_deref())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or(host);
    EndpointTransport {
        mode: TransportMode::Tls,
        sni: Some(server_name.to_owned()),
        ca_secret_ref: backend.and_then(|backend| backend.ca_secret_ref.clone()),
    }
}


#[cfg(test)]
mod tests;

/// One line naming which clusters were derived, for the gateway's status.
///
/// Empty when nothing was derived, so a gateway that supplies its own topology
/// reads exactly as it did before. Bounded: an operator needs to know that
/// derivation happened and where to look, not a full inventory in a status
/// message.
pub(crate) fn derived_summary(resolved: &BTreeMap<String, Resolved>) -> String {
    let derived: Vec<&str> = resolved
        .iter()
        .filter(|(_, entry)| entry.origin != Origin::Explicit)
        .map(|(cluster, _)| cluster.as_str())
        .collect();
    if derived.is_empty() {
        return String::new();
    }
    let total = resolved.len();
    let shown = derived
        .iter()
        .take(MAX_NAMED_CLUSTERS)
        .copied()
        .collect::<Vec<_>>()
        .join(", ");
    let count = derived.len();
    if count > MAX_NAMED_CLUSTERS {
        let rest = count - MAX_NAMED_CLUSTERS;
        return format!("derived {count} of {total} cluster endpoints ({shown}, and {rest} more)");
    }
    format!("derived {count} of {total} cluster endpoints ({shown})")
}

/// How many cluster names a status message carries before it summarises.
const MAX_NAMED_CLUSTERS: usize = 3;
