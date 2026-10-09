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
//! Nothing here returns an error of its own. A cluster it cannot resolve is a
//! named refusal the controller withdraws from the gateway, so the document the
//! gateway loads never names a destination nobody declared, and the status says
//! why for each one.
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

/// Why a candidate cluster could not be resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// A provider carries the identity but the gateway did not name it.
    NotAllowlisted,
    /// The provider is explicitly `Unavailable`.
    Unavailable,
    /// No provider of this network carries the identity.
    NoProvider,
    /// Two providers claim the identity.
    Ambiguous,
    /// One cluster appears at two sites.
    ClusterAtTwoSites,
    /// No `GridSite` of this network has the candidate's site name.
    SiteUnknown,
    /// The site is not `Active`, so its address was never probed.
    SiteNotActive,
    /// The site declares no egress address.
    NoEgress,
    /// The endpoint URL has no usable scheme or host.
    EndpointUnusable,
    /// The URL declares a port it cannot represent.
    EndpointPort,
    /// An `https` endpoint named by address with no declared server name.
    ServerNameNeeded,
}

impl Refusal {
    /// The reason as the status names it.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NotAllowlisted => "not allowlisted",
            Self::Unavailable => "provider unavailable",
            Self::NoProvider => "no provider",
            Self::Ambiguous => "ambiguous identity",
            Self::ClusterAtTwoSites => "cluster at two sites",
            Self::SiteUnknown => "site unknown",
            Self::SiteNotActive => "site not active",
            Self::NoEgress => "no egress",
            Self::EndpointUnusable => "endpoint unusable",
            Self::EndpointPort => "endpoint port",
            Self::ServerNameNeeded => "server name needed",
        }
    }
}

/// One candidate cluster left unresolved, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Refused {
    /// The cluster the candidate names.
    pub(crate) cluster: String,
    /// The site the candidate names.
    pub(crate) site: String,
    /// Why it stays out of the topology.
    pub(crate) reason: Refusal,
}

/// An entry per resolvable cluster, and a refusal for each candidate cluster
/// that could not be resolved.
#[derive(Debug, Default)]
pub(crate) struct Resolution {
    /// Resolved entries by cluster.
    pub(crate) resolved: BTreeMap<String, Resolved>,
    /// What was refused, in candidate order.
    pub(crate) refused: Vec<Refused>,
}

impl std::ops::Deref for Resolution {
    type Target = BTreeMap<String, Resolved>;

    fn deref(&self) -> &Self::Target {
        &self.resolved
    }
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
) -> Resolution {
    let typed: BTreeMap<&str, &ClusterEndpointConfig> =
        explicit.iter().map(|entry| (entry.cluster.as_str(), entry)).collect();
    let local = local_providers(declarations);
    let clusters: BTreeSet<(&str, &str)> = candidates
        .iter()
        .map(|candidate| (candidate.cluster.as_str(), candidate.site.as_str()))
        .collect();

    let twice = clusters_at_two_sites(&clusters);

    let mut resolution = Resolution::default();
    for (cluster, site) in clusters {
        if twice.contains(cluster) {
            resolution.refused.push(Refused {
                cluster: cluster.to_owned(),
                site: site.to_owned(),
                reason: Refusal::ClusterAtTwoSites,
            });
            continue;
        }
        match resolve_one(cluster, site, &typed, &local, declarations) {
            Ok(entry) => {
                resolution.resolved.insert(cluster.to_owned(), entry);
            },
            Err(reason) => resolution.refused.push(Refused {
                cluster: cluster.to_owned(),
                site: site.to_owned(),
                reason,
            }),
        }
    }
    resolution
}

/// The clusters that appear at more than one site. One cluster at two sites
/// would otherwise collapse by site-name order, picking a topology nobody
/// declared, so each is refused instead.
fn clusters_at_two_sites<'cluster>(clusters: &BTreeSet<(&'cluster str, &'cluster str)>) -> BTreeSet<&'cluster str> {
    let mut seen = BTreeSet::<&str>::new();
    let mut twice = BTreeSet::<&str>::new();
    for (cluster, _) in clusters {
        if !seen.insert(cluster) {
            twice.insert(cluster);
        }
    }
    twice
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
) -> Result<Resolved, Refusal> {
    if let Some(entry) = typed.get(cluster) {
        return Ok(Resolved {
            endpoint: (*entry).clone(),
            origin: Origin::Explicit,
        });
    }
    // The allowlist is the trust decision, so it gates a remote candidate as
    // well as a local one. Gating only the local map let a remote candidate
    // reach its site's egress without the gateway owner naming it.
    if !declarations.from_providers.iter().any(|named| named == cluster) {
        return Err(Refusal::NotAllowlisted);
    }
    if site == declarations.local_site {
        // Our own site, so it resolves from a provider we hold or not at all.
        // Falling through to the egress of our own site would hairpin the
        // consumer through its own provider gateway.
        return match local.get(cluster) {
            Some(provider) => derive_local(cluster, provider),
            None => Err(local_refusal(cluster, declarations)),
        };
    }
    derive_remote(cluster, site, declarations.sites, declarations.network_name)
}

/// Why no admitted provider carries `cluster` at this site: nobody does, every
/// carrier is unavailable, or two claim it.
fn local_refusal(cluster: &str, declarations: &Declarations<'_>) -> Refusal {
    let carriers: Vec<&InferenceProvider> = declarations
        .providers
        .iter()
        .filter(|provider| {
            routing_identity(provider) == Some(cluster) && provider.spec.grid_network_ref == declarations.network_name
        })
        .collect();
    let available = carriers
        .iter()
        .filter(|provider| !crate::resources::routing_overlay::is_explicitly_unavailable(provider))
        .count();
    if carriers.is_empty() {
        Refusal::NoProvider
    } else if available == 0 {
        Refusal::Unavailable
    } else {
        Refusal::Ambiguous
    }
}

/// Derive a local backend entry from the provider's own endpoint URL.
///
/// The scheme decides the transport, which is why `backendTls` does not declare
/// one. Trust comes only from `backendTls`: an omitted CA reference means the
/// process trust store, inherited from the explicit path rather than decided
/// again here.
fn derive_local(cluster: &str, provider: &InferenceProvider) -> Result<Resolved, Refusal> {
    let endpoint = provider.spec.endpoint.trim();
    let uri = endpoint
        .parse::<http::Uri>()
        .map_err(|_unparsable| Refusal::EndpointUnusable)?;
    let host = uri
        .host()
        .filter(|host| !host.is_empty())
        .ok_or(Refusal::EndpointUnusable)?;
    let tls = match uri.scheme_str() {
        Some("https") => true,
        Some("http") => false,
        _ => return Err(Refusal::EndpointUnusable),
    };
    let port = endpoint_port(&uri, tls).ok_or(Refusal::EndpointPort)?;
    let backend = provider.spec.backend_tls.as_deref();
    if tls && !server_name_is_usable(backend, host) {
        return Err(Refusal::ServerNameNeeded);
    }
    let transport = backend_transport(backend, host, tls);
    Ok(Resolved {
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
fn derive_remote(cluster: &str, site: &str, sites: &[GridSite], network_name: &str) -> Result<Resolved, Refusal> {
    let site = sites
        .iter()
        .find(|known| {
            crate::controller::grid_network::peer_site_key(known).is_some_and(|(key, _)| key == site)
                && known.spec.grid_network_ref == network_name
        })
        .ok_or(Refusal::SiteUnknown)?;
    // Only an Active site has had its address probed over TLS and its leaf
    // pinned. A Discovered or Connecting stub carries an address copied from
    // gossip, which is not something to hand the data plane.
    if !matches!(
        site.status.as_ref().map(|status| &status.phase),
        Some(crate::crd::grid_site::GridSitePhase::Active)
    ) {
        return Err(Refusal::SiteNotActive);
    }
    let egress = site.spec.egress.as_ref().ok_or(Refusal::NoEgress)?;
    let address = egress.address.trim();
    if address.is_empty() {
        return Err(Refusal::NoEgress);
    }
    Ok(Resolved {
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
pub(crate) fn derived_summary(resolution: &Resolution) -> String {
    let derived: Vec<String> = resolution
        .resolved
        .iter()
        .filter(|(_, entry)| entry.origin != Origin::Explicit)
        .map(|(cluster, _)| cluster.clone())
        .collect();
    let refused: Vec<String> = resolution
        .refused
        .iter()
        .map(|refused| format!("{}: {}", refused.cluster, refused.reason.as_str()))
        .collect();
    let mut parts = Vec::new();
    if !derived.is_empty() {
        let total = resolution.resolved.len();
        parts.push(format!(
            "derived {} of {total} cluster endpoints ({})",
            derived.len(),
            named(&derived)
        ));
    }
    if !refused.is_empty() {
        parts.push(format!("withdrew {} ({})", refused.len(), named(&refused)));
    }
    parts.join("; ")
}

/// The first few of `items`, and how many more there are.
fn named(items: &[String]) -> String {
    let shown = items
        .iter()
        .take(MAX_NAMED_CLUSTERS)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    match items.len().saturating_sub(MAX_NAMED_CLUSTERS) {
        0 => shown,
        rest => format!("{shown}, and {rest} more"),
    }
}

/// How many cluster names a status message carries before it summarises.
const MAX_NAMED_CLUSTERS: usize = 3;
