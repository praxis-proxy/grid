//! Derive consumer endpoint topology from provider and `GridSite` declarations.
//!
//! Works out, for [`InferenceProvider`]s and member catalogs, the endpoint topology a
//! human otherwise types into `consumerConfig.clusterEndpoints`, emitting the
//! same [`ClusterEndpointConfig`] type so the renderer is unchanged.
//!
//! [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
//! [`ClusterEndpointConfig`]: crate::crd::grid_network::ClusterEndpointConfig

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    crd::{
        grid_network::{ClusterEndpointConfig, EndpointTransport, SignalMode, TransportMode},
        grid_site::{EgressTls, EgressTlsMode, GridSite},
        inference_provider::InferenceProvider,
    },
    resources::routing_overlay::{CANDIDATE_KIND, RoutingCandidate, is_explicitly_unavailable, routing_identity},
};

/// Clusters each member site's catalog offers, by site.
pub(crate) type PeerCatalog<'records> = BTreeMap<&'records str, BTreeSet<&'records str>>;

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
    /// The site's spec changed since its last probe, so its address is unprobed.
    SiteNotProbed,
    /// The site declares no egress address.
    NoEgress,
    /// A mutual TLS hop with no server name, which the renderer refuses.
    NoServerName,
    /// The endpoint URL has no usable scheme or host.
    EndpointUnusable,
    /// The URL declares a port it cannot represent.
    EndpointPort,
    /// An `https` endpoint named by IP address with no declared server name.
    ServerNameNeeded,
    /// The endpoint host is not a DNS hostname, so it cannot be the server name.
    ServerNameInvalid,
    /// A remote hop's server name is not the site's own `{site}.grid.internal`.
    ServerNameNotSite,
    /// No member catalog offers the cluster at the candidate's site.
    NotOffered,
    /// More than one `GridSite` of the network keys to the candidate's site.
    SiteKeyAmbiguous,
    /// A member hop whose egress is not mutual TLS, so the peer is not verified.
    MemberHopNotMutual,
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
            Self::SiteNotProbed => "site not probed at this generation",
            Self::NoEgress => "no egress",
            Self::NoServerName => "no server name",
            Self::EndpointUnusable => "endpoint unusable",
            Self::EndpointPort => "endpoint port",
            Self::ServerNameNeeded => "server name needed",
            Self::ServerNameInvalid => "server name invalid",
            Self::ServerNameNotSite => "server name is not the site's",
            Self::NotOffered => "not offered by the member",
            Self::SiteKeyAmbiguous => "site key is ambiguous",
            Self::MemberHopNotMutual => "member hop is not mutual TLS",
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

/// The declarations resolution reads.
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
    /// Remote routing identities that derive without a member catalog.
    pub(crate) from_providers: &'decl [String],
    /// TLS for derived local `https` backends.
    pub(crate) transport: Option<&'decl EndpointTransport>,
    /// The grid's signal transport, which decides whether an unreachable site keeps its hop.
    pub(crate) signal: SignalMode,
    /// What authenticated peer catalogs offer. `None` unless membership is Local.
    pub(crate) peer_catalog: Option<&'decl PeerCatalog<'decl>>,
}

/// What `providers` offer, by the site the record names.
pub(crate) fn peer_catalog<'records>(
    providers: impl IntoIterator<Item = &'records crdt::ProviderState>,
) -> PeerCatalog<'records> {
    let mut catalog = PeerCatalog::new();
    for provider in providers {
        catalog
            .entry(provider.site_id.as_str())
            .or_default()
            .insert(provider.routing_cluster.as_str());
    }
    catalog
}

/// Explicit entries win whole.
pub(crate) fn resolve(
    candidates: &[RoutingCandidate],
    explicit: &[ClusterEndpointConfig],
    declarations: &Declarations<'_>,
) -> Resolution {
    let typed: BTreeMap<&str, &ClusterEndpointConfig> =
        explicit.iter().map(|entry| (entry.cluster.as_str(), entry)).collect();
    let local = local_providers(declarations);
    // Only inference candidates render a load balancer cluster.
    let clusters: BTreeSet<(&str, &str)> = candidates
        .iter()
        .filter(|candidate| candidate.kind == CANDIDATE_KIND)
        .map(|candidate| (candidate.cluster.as_str(), candidate.site.as_str()))
        .collect();
    let twice = clusters_at_two_sites(&clusters);

    let mut resolution = Resolution::default();
    for (cluster, site) in clusters {
        let outcome = if twice.contains(cluster) {
            Err(Refusal::ClusterAtTwoSites)
        } else {
            resolve_one(cluster, site, &typed, &local, declarations)
        };
        match outcome {
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

/// Clusters that appear at more than one site.
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

/// Admitted, available, unambiguous providers in this network, by routing identity.
fn local_providers<'decl>(declarations: &Declarations<'decl>) -> BTreeMap<&'decl str, &'decl InferenceProvider> {
    let mut local: BTreeMap<&str, &InferenceProvider> = BTreeMap::new();
    let mut ambiguous: BTreeSet<&str> = BTreeSet::new();
    for provider in declarations.providers {
        let Some(identity) = routing_identity(provider) else {
            continue;
        };
        if provider.spec.grid_network_ref != declarations.network_name || is_explicitly_unavailable(provider) {
            continue;
        }
        // Otherwise iteration order would pick one endpoint silently.
        if local.insert(identity, provider).is_some() {
            ambiguous.insert(identity);
        }
    }
    for identity in &ambiguous {
        local.remove(identity);
    }
    local
}

/// Whether `fromProviders` names `cluster`.
fn is_named(cluster: &str, declarations: &Declarations<'_>) -> bool {
    declarations.from_providers.iter().any(|name| name == cluster)
}

/// Admit an unnamed remote candidate only as a member site's catalog offers it.
fn admits_member(cluster: &str, site: &str, declarations: &Declarations<'_>) -> Result<(), Refusal> {
    // Only Local membership reads catalogs off identity-verified peers; SWIM keeps the name gate.
    let catalog = declarations.peer_catalog.ok_or(Refusal::NotAllowlisted)?;
    // A local provider's selector can place a candidate at a peer; the peer never offered it.
    if !catalog.get(site).is_some_and(|clusters| clusters.contains(cluster)) {
        return Err(Refusal::NotOffered);
    }
    Ok(())
}

/// Resolve one cluster: its explicit entry, else a derived local or remote one.
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
    if site == declarations.local_site {
        // Own site egress would hairpin through our own provider gateway.
        return match local.get(cluster) {
            Some(provider) => derive_local(cluster, provider, declarations.transport),
            None => Err(local_refusal(cluster, declarations)),
        };
    }
    // The allowlist gates remote candidates too.
    if is_named(cluster, declarations) {
        return derive_remote(cluster, site, declarations);
    }
    admits_member(cluster, site, declarations)?;
    if site_key_ambiguous(site, declarations) {
        return Err(Refusal::SiteKeyAmbiguous);
    }
    let resolved = derive_remote(cluster, site, declarations)?;
    // Only a mutual TLS hop verifies the peer it reaches.
    if !resolved
        .endpoint
        .transport
        .as_ref()
        .is_some_and(|transport| transport.mode == TransportMode::MutualTls)
    {
        return Err(Refusal::MemberHopNotMutual);
    }
    Ok(resolved)
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
        .filter(|provider| !is_explicitly_unavailable(provider))
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
fn derive_local(
    cluster: &str,
    provider: &InferenceProvider,
    declared: Option<&EndpointTransport>,
) -> Result<Resolved, Refusal> {
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
    let transport = backend_transport(host, tls, declared)?;
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
fn derive_remote(cluster: &str, site_name: &str, declarations: &Declarations<'_>) -> Result<Resolved, Refusal> {
    let site = probed_site(site_name, declarations)?;
    let egress = site.spec.gateway_endpoint.as_ref().ok_or(Refusal::NoEgress)?;
    let address = egress.address.trim();
    if address.is_empty() {
        return Err(Refusal::NoEgress);
    }
    let transport = hop_transport(&egress.tls);
    if transport.mode == TransportMode::MutualTls {
        let expected = crate::signals::gateway_server_name(site_name);
        match transport.sni.as_deref() {
            None => return Err(Refusal::NoServerName),
            Some(sni) if !sni.eq_ignore_ascii_case(&expected) => return Err(Refusal::ServerNameNotSite),
            Some(_) => {},
        }
    }
    Ok(Resolved {
        endpoint: ClusterEndpointConfig {
            cluster: cluster.to_owned(),
            address: address.to_owned(),
            transport: Some(transport),
        },
        origin: Origin::DerivedRemote,
    })
}

/// Whether more than one `GridSite` of this network keys to `site`.
fn site_key_ambiguous(site: &str, declarations: &Declarations<'_>) -> bool {
    declarations
        .sites
        .iter()
        .filter(|known| {
            known.spec.grid_network_ref == declarations.network_name
                && crate::controller::grid_network::peer_site_key(known).is_some_and(|(key, _)| key == site)
        })
        .nth(1)
        .is_some()
}

/// The site named `site` in this network, if a probe verified its current spec.
fn probed_site<'site>(site: &str, declarations: &Declarations<'site>) -> Result<&'site GridSite, Refusal> {
    let network_name = declarations.network_name;
    let site = declarations
        .sites
        .iter()
        .find(|known| {
            crate::controller::grid_network::peer_site_key(known).is_some_and(|(key, _)| key == site)
                && known.spec.grid_network_ref == network_name
        })
        .ok_or(Refusal::SiteUnknown)?;
    // Only an Active or Unreachable site's address has been probed; earlier phases carry gossip.
    let status = site.status.as_ref().ok_or(Refusal::SiteNotActive)?;
    if !status.phase.routes(declarations.signal) {
        return Err(Refusal::SiteNotActive);
    }
    // Active for an older spec says nothing about the address now declared.
    if status.observed_generation != site.metadata.generation.unwrap_or(0) {
        return Err(Refusal::SiteNotProbed);
    }
    Ok(site)
}

/// Transport for a provider hop. No CA: the consumer's grid identity carries trust.
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

/// The SNI to send. Praxis refuses a whole document over one bad name.
fn server_name(host: &str) -> Result<&str, Refusal> {
    if crate::signals::is_dns_name(host) {
        Ok(host)
    } else if host
        .trim_matches(|c| c == '[' || c == ']')
        .parse::<std::net::IpAddr>()
        .is_ok()
    {
        Err(Refusal::ServerNameNeeded)
    } else {
        Err(Refusal::ServerNameInvalid)
    }
}

/// The declared port, else the scheme default. `None` for an unrepresentable port.
fn endpoint_port(uri: &http::Uri, tls: bool) -> Option<u16> {
    if let Some(port) = uri.port_u16() {
        // Port 0 is not an endpoint.
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

/// Transport for a local backend. Plaintext carries no SNI, which the renderer rejects.
fn backend_transport(
    host: &str,
    tls: bool,
    declared: Option<&EndpointTransport>,
) -> Result<EndpointTransport, Refusal> {
    if !tls {
        return Ok(EndpointTransport {
            mode: TransportMode::Plaintext,
            sni: None,
            ca_secret_ref: None,
        });
    }
    let sni = server_name(declared.and_then(|transport| transport.sni.as_deref()).unwrap_or(host))?;
    Ok(EndpointTransport {
        mode: TransportMode::Tls,
        sni: Some(sni.to_owned()),
        ca_secret_ref: declared.and_then(|transport| transport.ca_secret_ref.clone()),
    })
}

/// One line naming derived and withdrawn clusters, for status. Empty when neither.
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

#[cfg(test)]
mod tests;
