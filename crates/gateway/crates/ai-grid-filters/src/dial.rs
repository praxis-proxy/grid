//! Upstreams the gateway builds from the dial file beside its serving config.
//!
//! Each dial is validated by praxis' own inline-cluster validator and built into a praxis
//! [`Upstream`] when a config is applied, never per request: a routed request clones the
//! built upstream and its retry policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    path::{Component, Path},
    sync::Arc,
};

use praxis_core::{
    config::{Cluster, FilterEntry, InsecureOptions, RetryPolicy, validate_chain_entries_inline_clusters},
    connectivity::{ConnectionOptions, Upstream},
};
use praxis_filter::FilterError;
use praxis_tls::{CaConfig, CachedClusterTls, CertKeyPair, ClusterTls};

use crate::{
    control::Binding,
    health::{DialHealth, DialState},
    serving::{CredentialStrategy, DialConfig, DialSpec, DialTransport, GridServingConfig, mount_revision},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The chain name praxis' validator reports a refused dial under.
const DIAL_CHAIN: &str = "serving-config-dial";

/// Where the CA bundles server-only TLS dials verify against are mounted.
pub const BACKEND_CA_BASE: &str = "/run/secrets/grid-backend-ca";

/// Where the provider credentials a local dial sends are mounted, as `credential_inject` reads them.
pub const CREDENTIAL_BASE: &str = "/run/secrets/grid-credentials";

/// Where the grid identity is mounted; the chart enforces this path.
pub const GRID_TLS_MOUNT: &str = "/etc/praxis/tls";

/// How long an idle dialed connection is pooled, milliseconds.
const IDLE_TIMEOUT_MS: u64 = 60_000;

/// A dial's connect timeout when the dial file names none, milliseconds.
const CONNECT_TIMEOUT_MS: u64 = 3_000;

/// A dial's connect timeout including TLS when the dial file names none, milliseconds.
const TOTAL_CONNECT_TIMEOUT_MS: u64 = 6_000;

/// A dial's per-read timeout when the dial file names none, milliseconds: a slow first token is not a dead site.
const READ_TIMEOUT_MS: u64 = 600_000;

// ---------------------------------------------------------------------------
// Grid Identity
// ---------------------------------------------------------------------------

/// This site's grid identity, which every mutual TLS dial presents and verifies against.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridIdentity {
    /// The grid CA bundle's path.
    pub ca: String,

    /// This site's certificate chain's path.
    pub cert: String,

    /// This site's private key's path.
    pub key: String,
}

impl GridIdentity {
    /// `ca.crt`, `tls.crt` and `tls.key` under `dir`.
    ///
    /// ```
    /// let identity = ai_grid_filters::GridIdentity::under("/etc/praxis/tls");
    /// assert_eq!(identity.key, "/etc/praxis/tls/tls.key");
    /// ```
    #[must_use]
    pub fn under(dir: &str) -> Self {
        Self {
            ca: format!("{dir}/ca.crt"),
            cert: format!("{dir}/tls.crt"),
            key: format!("{dir}/tls.key"),
        }
    }

    /// The identity `config`'s peers present, else the default mount's.
    #[must_use]
    pub fn of(config: &GridServingConfig) -> Self {
        config.peers.first().map_or_else(Self::default, |peer| Self {
            ca: peer.grid_ca_path.clone(),
            cert: peer.client_cert_path.clone(),
            key: peer.client_key_path.clone(),
        })
    }

    /// The same three files under the one `..data` revision their mount points at now, so a
    /// rotation never pairs one revision's certificate with another's key.
    pub(crate) fn snapshot(&self) -> Self {
        let dir = Path::new(&self.ca).parent();
        let shared = [&self.cert, &self.key]
            .iter()
            .all(|path| Path::new(path).parent() == dir);
        let Some(dir) = dir.filter(|_| shared) else {
            return self.clone();
        };
        let revision = mount_revision(dir);
        let rebase = |path: &str| {
            Path::new(path).file_name().map_or_else(
                || path.to_owned(),
                |name| revision.join(name).to_string_lossy().into_owned(),
            )
        };
        Self {
            ca: rebase(&self.ca),
            cert: rebase(&self.cert),
            key: rebase(&self.key),
        }
    }

    /// The three files, for the rotation digest.
    pub(crate) fn paths(&self) -> [&str; 3] {
        [&self.ca, &self.cert, &self.key]
    }
}

impl Default for GridIdentity {
    fn default() -> Self {
        Self::under(GRID_TLS_MOUNT)
    }
}

// ---------------------------------------------------------------------------
// Dials
// ---------------------------------------------------------------------------

/// A built dial: the upstream a routed request is sent to, and its retry policy.
#[derive(Debug)]
pub struct Dialed {
    /// The longest a request can take to fail every connect attempt.
    connect_budget: std::time::Duration,

    /// The `Authorization` value a local dial sends in place of the caller's.
    credential: Option<http::HeaderValue>,

    /// The dial's passive health, which each routed response records into.
    health: Arc<DialState>,

    /// The connect-failure retry policy, shared by every request.
    retry: Arc<RetryPolicy>,

    /// The upstream with its TLS material already read.
    upstream: Upstream,
}

impl Dialed {
    /// A dial to `cluster` through `upstream`, retried under `retry`.
    #[must_use]
    pub fn new(cluster: &str, upstream: Upstream, retry: Arc<RetryPolicy>) -> Self {
        let health = Arc::new(DialState::new(cluster, Arc::clone(&upstream.address)));
        Self::with_health(upstream, retry, health)
    }

    /// A dial recording its outcomes into `health`.
    pub(crate) fn with_health(upstream: Upstream, retry: Arc<RetryPolicy>, health: Arc<DialState>) -> Self {
        let per_attempt = upstream
            .connection
            .total_connection_timeout
            .or(upstream.connection.connection_timeout)
            .unwrap_or(std::time::Duration::from_millis(TOTAL_CONNECT_TIMEOUT_MS));
        // Every attempt, plus slack for retry backoff and scheduling.
        let connect_budget = per_attempt
            .saturating_mul(retry.effective_max_retries().saturating_add(1))
            .saturating_add(std::time::Duration::from_secs(1));
        Self {
            connect_budget,
            credential: None,
            health,
            retry,
            upstream,
        }
    }

    /// This dial sending `credential` in place of the caller's.
    #[must_use]
    pub(crate) fn with_credential(self, credential: Option<http::HeaderValue>) -> Self {
        Self { credential, ..self }
    }

    /// The longest a request can take to fail every connect attempt.
    pub(crate) fn connect_budget(&self) -> std::time::Duration {
        self.connect_budget
    }

    /// The `Authorization` value this dial sends in place of the caller's.
    pub(crate) fn credential(&self) -> Option<&http::HeaderValue> {
        self.credential.as_ref()
    }

    /// The dial's passive health.
    pub(crate) fn health(&self) -> &Arc<DialState> {
        &self.health
    }

    /// The retry policy a routed request carries.
    pub(crate) fn retry(&self) -> &Arc<RetryPolicy> {
        &self.retry
    }

    /// The upstream a routed request is sent to.
    pub(crate) fn upstream(&self) -> &Upstream {
        &self.upstream
    }
}

/// The dials of one applied config.
#[derive(Debug, Default)]
pub(crate) struct Dials {
    /// Built dials by cluster.
    built: BTreeMap<String, Arc<Dialed>>,

    /// Clusters praxis refused, whose candidates take no traffic.
    refused: BTreeSet<String>,

    /// Clusters whose TLS files could not be read, whose candidates take no traffic.
    undialable: BTreeSet<String>,
}

impl Dials {
    /// Build every dial in `dial`, each validated on its own under the startup `insecure` options.
    ///
    /// A dial praxis refuses, or whose TLS files cannot be read mid-rotation, only takes its
    /// own cluster out of routing.
    pub(crate) fn build(
        dial: &DialConfig,
        insecure: &InsecureOptions,
        identity: &GridIdentity,
        health: &DialHealth,
    ) -> Self {
        let mut dials = Self::default();
        for (name, spec) in &dial.dial {
            let cluster = cluster(name, spec, identity);
            if let Err(error) = validate(&cluster, insecure) {
                tracing::warn!(cluster = %name, %error, "grid: dial refused; its candidates take no traffic");
                dials.refused.insert(name.clone());
                continue;
            }
            match dialed(&cluster, spec, health.state(name, &spec.address)) {
                Ok(built) => {
                    dials.built.insert(name.clone(), Arc::new(built));
                },
                Err(error) => {
                    tracing::warn!(cluster = %name, %error, "grid: dial TLS material unreadable; its candidates take no traffic until it reads");
                    dials.undialable.insert(name.clone());
                },
            }
        }
        let count = |set: &BTreeSet<String>| f64::from(u32::try_from(set.len()).unwrap_or(u32::MAX));
        metrics::gauge!("grid_serving_refused_dial_clusters").set(count(&dials.refused));
        metrics::gauge!("grid_serving_undialable_clusters").set(count(&dials.undialable));
        dials
    }

    /// The built dial for `cluster`.
    pub(crate) fn get(&self, cluster: &str) -> Option<&Arc<Dialed>> {
        self.built.get(cluster)
    }

    /// Each built dial's health, by cluster.
    pub(crate) fn states(&self) -> BTreeMap<Arc<str>, Arc<DialState>> {
        self.built
            .iter()
            .map(|(cluster, dial)| (Arc::from(cluster.as_str()), Arc::clone(dial.health())))
            .collect()
    }

    /// Whether `cluster` has a dial that was refused or could not be read.
    pub(crate) fn unroutable(&self, cluster: &str) -> bool {
        self.refused.contains(cluster) || self.undialable.contains(cluster)
    }
}

/// Refuse a dial file that, joined with `config`, would widen trust: a dial for another site's or a
/// credentialed candidate, a mutual TLS dial to a name other than its site's, anything but grid
/// mutual TLS off this site, a private IP literal trusted off this site, a provider hop dialed under
/// a name other than the one it declares, a dial without the identity of the verified `load_balancer`
/// cluster it shadows, a CA outside the backend CA mount, or peers presenting an identity mutual TLS dials do not.
///
/// # Errors
///
/// Names the first dial that breaks a rule.
pub(crate) fn validate_dials(
    config: &GridServingConfig,
    dials: &DialConfig,
    binding: &Binding,
) -> Result<(), FilterError> {
    for candidate in &config.candidates {
        let Some(dial) = dials.dial.get(&candidate.cluster) else {
            continue;
        };
        if dial.site != candidate.site {
            return Err(format!(
                "grid: dial for cluster '{}' names site '{}', but its candidate '{}' is served by '{}'",
                candidate.cluster, dial.site, candidate.name, candidate.site
            )
            .into());
        }
        // Only a local dial carries a credential, and only one it sends itself.
        let local = dial.site == config.local_site && !matches!(dial.transport, DialTransport::MutualTls { .. });
        if candidate.credential.is_some() && !(local && dial.credential.is_some()) {
            return Err(format!(
                "grid: dial for cluster '{}' serves candidate '{}', whose credential it does not send",
                candidate.cluster, candidate.name
            )
            .into());
        }
        // A mutual TLS dial carries hop context, which names the candidate by its stable id.
        if matches!(dial.transport, DialTransport::MutualTls { .. }) && candidate.stable_id.is_none() {
            return Err(format!(
                "grid: candidate '{}' on mutual TLS dial '{}' is missing stable_id",
                candidate.name, candidate.cluster
            )
            .into());
        }
    }
    let mutual = dials
        .dial
        .values()
        .any(|dial| matches!(dial.transport, DialTransport::MutualTls { .. }));
    if mutual
        && let Some(peer) = config.peers.iter().find(|peer| {
            [
                peer.grid_ca_path.as_str(),
                peer.client_cert_path.as_str(),
                peer.client_key_path.as_str(),
            ] != binding.grid_identity.paths()
        })
    {
        return Err(format!(
            "grid: peer '{}' presents an identity other than the grid TLS mount every mutual TLS dial presents",
            peer.site
        )
        .into());
    }
    for (cluster, dial) in &dials.dial {
        crate::descriptor::validate_site(&format!("dial.{cluster}.site"), &dial.site)?;
        let local = dial.site == config.local_site;
        if let DialTransport::MutualTls { sni } = &dial.transport
            && *sni != certs::site_dns_name(&dial.site)
        {
            return Err(format!(
                "grid: mutual TLS dial for cluster '{cluster}' must verify '{}', not '{sni}'",
                certs::site_dns_name(&dial.site)
            )
            .into());
        }
        if !local && !matches!(dial.transport, DialTransport::MutualTls { .. }) {
            return Err(format!("grid: dial for cluster '{cluster}' leaves this site without grid mutual TLS").into());
        }
        // Off this site only a hostname behind a verified grid handshake: no HTTP byte leaves before it.
        if !local && dial.trusted_private && grid_client::address_host(&dial.address).parse::<IpAddr>().is_ok() {
            return Err(format!("grid: dial for cluster '{cluster}' trusts a private IP literal off this site").into());
        }
        validate_request_shape(cluster, dial, local, binding)?;
        if let Some(ca_path) = dial.transport.ca_path()
            && !within(Path::new(ca_path), &binding.backend_ca_base)
        {
            return Err(format!(
                "grid: dial for cluster '{cluster}' reads CA '{ca_path}' outside {}",
                binding.backend_ca_base.display()
            )
            .into());
        }
        if let Some(declared) = config.provider_hop_sni.get(cluster)
            && !matches!(&dial.transport, DialTransport::MutualTls { sni } if sni == declared)
        {
            return Err(format!(
                "grid: provider hop '{cluster}' declares '{declared}' but is not a mutual TLS dial to it"
            )
            .into());
        }
        if let Some(verified) = binding.backend_tls.get(cluster)
            && !matches!(&dial.transport, DialTransport::MutualTls { sni } if sni == verified)
        {
            return Err(format!(
                "grid: dial for cluster '{cluster}' does not verify '{verified}', as the mutual TLS cluster it replaces does"
            )
            .into());
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Utility Functions
// ---------------------------------------------------------------------------

/// Refuse a credential on a dial that is off this site or grid mutual TLS, or read outside the
/// credential mount, and a base path that is not clean.
fn validate_request_shape(cluster: &str, dial: &DialSpec, local: bool, binding: &Binding) -> Result<(), FilterError> {
    if let Some(credential) = &dial.credential {
        if !local || matches!(dial.transport, DialTransport::MutualTls { .. }) {
            return Err(format!("grid: dial for cluster '{cluster}' would send a credential off this site").into());
        }
        if !within(Path::new(&credential.path), &binding.credential_base) {
            return Err(format!(
                "grid: dial for cluster '{cluster}' reads its credential outside {}",
                binding.credential_base.display()
            )
            .into());
        }
    }
    if let Some(base_path) = &dial.base_path
        && !clean_base_path(base_path)
    {
        return Err(format!("grid: dial for cluster '{cluster}' has an unusable base path '{base_path}'").into());
    }
    Ok(())
}

/// Whether `path` is an absolute path with no query, fragment, empty segment or `..` segment.
pub(crate) fn clean_base_path(path: &str) -> bool {
    path.starts_with('/')
        && !path.contains(['?', '#'])
        && !path.contains("//")
        && !path.split('/').any(|segment| segment == ".." || segment == ".")
}

/// Whether `path` names a file below `base`, with no `..` to climb out.
fn within(path: &Path, base: &Path) -> bool {
    path != base && path.starts_with(base) && !path.components().any(|part| part == Component::ParentDir)
}

/// The praxis cluster `spec` describes, presenting `identity` over mutual TLS, verification always on.
fn cluster(name: &str, spec: &DialSpec, identity: &GridIdentity) -> Cluster {
    let mut cluster = Cluster::with_defaults(name, vec![spec.address.clone().into()]);
    cluster.connection_timeout_ms = spec.connect_timeout_ms.or(Some(CONNECT_TIMEOUT_MS));
    cluster.total_connection_timeout_ms = spec.total_connect_timeout_ms.or(Some(TOTAL_CONNECT_TIMEOUT_MS));
    cluster.read_timeout_ms = spec.read_timeout_ms.or(Some(READ_TIMEOUT_MS));
    cluster.write_timeout_ms = spec.write_timeout_ms;
    cluster.idle_timeout_ms = Some(IDLE_TIMEOUT_MS);
    // Unconfigured, so praxis retries the same address: the dial has no reselector.
    let legacy = RetryPolicy::legacy_default();
    cluster.retry_policy = Some(RetryPolicy {
        max_retries: spec.connect_retries.or(legacy.max_retries),
        // Only a connect failure retries, and a failed connect sent nothing (RFC 9110 9.2.2).
        allow_non_idempotent: Some(true),
        ..legacy
    });
    if spec.trusted_private {
        cluster.trusted_private_endpoints = vec![grid_client::address_host(&spec.address).to_owned()];
    }
    cluster.http.base_path = spec.base_path.as_deref().map(Arc::from);
    cluster.tls = tls(&spec.transport, identity);
    cluster
}

/// The praxis TLS settings for `transport`.
fn tls(transport: &DialTransport, identity: &GridIdentity) -> Option<ClusterTls> {
    let ca = |ca_path: &String| CaConfig {
        ca_path: ca_path.clone(),
        crl_paths: Vec::new(),
    };
    match transport {
        DialTransport::MutualTls { sni } => Some(ClusterTls {
            ca: Some(ca(&identity.ca)),
            client_cert: Some(CertKeyPair {
                cert_path: identity.cert.clone(),
                default: false,
                key_path: identity.key.clone(),
                server_names: Vec::new(),
            }),
            sni: Some(sni.clone()),
            verify: true,
        }),
        DialTransport::Tls { ca_path, sni } => Some(ClusterTls {
            ca: ca_path.as_ref().map(ca),
            client_cert: None,
            sni: Some(sni.clone()),
            verify: true,
        }),
        DialTransport::Plaintext => None,
    }
}

/// Run praxis' inline-cluster validator over `cluster`, as a synthetic `load_balancer`.
///
/// A literal address skips praxis' per-request SSRF check, so this is the only check it gets.
fn validate(cluster: &Cluster, insecure: &InsecureOptions) -> Result<(), FilterError> {
    let refused = |error: String| -> FilterError { format!("grid: dial refused: {error}").into() };
    let mut entry = serde_yaml::Mapping::new();
    entry.insert("filter".into(), "load_balancer".into());
    entry.insert(
        "clusters".into(),
        serde_yaml::to_value(std::slice::from_ref(cluster)).map_err(|error| refused(error.to_string()))?,
    );
    let entry: FilterEntry =
        serde_yaml::from_value(serde_yaml::Value::Mapping(entry)).map_err(|error| refused(error.to_string()))?;
    validate_chain_entries_inline_clusters(DIAL_CHAIN, std::slice::from_ref(&entry), insecure)
        .map_err(|error| refused(error.to_string()))
}

/// Read `cluster`'s TLS material and `spec`'s credential and build its upstream, recording into `health`.
fn dialed(cluster: &Cluster, spec: &DialSpec, health: Arc<DialState>) -> Result<Dialed, String> {
    let credential = spec.credential.as_ref().map(bearer).transpose()?;
    let tls = cluster
        .tls
        .as_ref()
        .map(CachedClusterTls::try_from_config)
        .transpose()
        .map_err(|error| error.to_string())?;
    let retry = Arc::new(cluster.retry_policy.clone().unwrap_or_else(RetryPolicy::legacy_default));
    let address = cluster
        .endpoints
        .first()
        .map(|endpoint| Arc::from(endpoint.address()))
        .ok_or_else(|| "no address".to_owned())?;
    Ok(Dialed::with_health(
        Upstream {
            address,
            authority: None,
            base_path: cluster.http.normalized_base_path().map(Arc::from),
            connection: Arc::new(ConnectionOptions::from(cluster)),
            tls,
        },
        retry,
        health,
    )
    .with_credential(credential))
}

/// The `Authorization` value `credential` sends, read now so a rotated file rebuilds the dial.
fn bearer(credential: &crate::serving::DialCredential) -> Result<http::HeaderValue, String> {
    match credential.strategy {
        CredentialStrategy::BearerToken => {},
    }
    let path = &credential.path;
    let token = std::fs::read(path)
        .map(zeroize::Zeroizing::new)
        .map_err(|error| format!("reading the dial credential {path}: {error}"))?;
    let token = token.trim_ascii();
    if token.is_empty() {
        return Err(format!("the dial credential {path} is empty"));
    }
    let mut value = zeroize::Zeroizing::new(Vec::with_capacity(token.len().saturating_add(7)));
    value.extend_from_slice(b"Bearer ");
    value.extend_from_slice(token);
    let mut header = http::HeaderValue::from_bytes(&value)
        .map_err(|_invalid| format!("the dial credential {path} is not a header value"))?;
    header.set_sensitive(true);
    Ok(header)
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::descriptor::{AdmissionState, CandidateConfig, CapabilityKind};

    #[test]
    fn a_valid_dial_section_passes_every_rule() {
        let mut backend = spec("local", tls("granite.models.svc"), true);
        backend.transport = DialTransport::Tls {
            ca_path: Some(format!("{BACKEND_CA_BASE}/models-ca/ca.crt")),
            sni: "granite.models.svc".to_owned(),
        };
        let config = config(&[
            ("local-plain", spec("local", DialTransport::Plaintext, true)),
            ("local-tls", backend),
            ("east", spec("east", mutual_tls("east"), false)),
        ]);
        validate_dials(&config.serving, &config.dial, &Binding::default()).expect("a dial file within every rule");
    }

    #[test]
    fn a_dial_for_another_sites_candidate_is_refused() {
        let mut config = config(&[("east", spec("east", mutual_tls("east"), false))]);
        config.serving.candidates[0].site = "west".to_owned();
        assert!(
            refused(&config).contains("names site 'east'"),
            "dial.site must match its candidates"
        );
    }

    #[test]
    fn a_mutual_tls_dial_must_verify_its_own_sites_grid_name() {
        let config = config(&[("east", spec("east", mutual_tls("west"), false))]);
        assert!(
            refused(&config).contains("must verify 'east.grid.internal'"),
            "another site's name cannot stand in for the dialed site's"
        );
    }

    #[test]
    fn only_grid_mutual_tls_leaves_this_site() {
        for transport in [DialTransport::Plaintext, tls("east.grid.internal")] {
            let config = config(&[("east", spec("east", transport.clone(), false))]);
            assert!(
                refused(&config).contains("leaves this site without grid mutual TLS"),
                "{transport:?} is local only"
            );
        }
    }

    #[test]
    fn a_credential_rides_only_a_local_dial_and_a_base_path_must_be_clean() {
        let credential = |path: &str| {
            Some(crate::serving::DialCredential {
                path: path.to_owned(),
                strategy: CredentialStrategy::BearerToken,
            })
        };
        let token = format!("{CREDENTIAL_BASE}/grid-model-client-token/token");
        let mut local = spec("local", DialTransport::Plaintext, true);
        local.credential = credential(&token);
        local.base_path = Some("/publishers/p/models/m".to_owned());
        let ok = config(&[("local-plain", local.clone())]);
        validate_dials(&ok.serving, &ok.dial, &Binding::default()).expect("a local dial sends its own token");
        let mut off_site = spec("east", mutual_tls("east"), false);
        off_site.credential = credential(&token);
        let mut local_mutual = spec("local", mutual_tls("local"), true);
        local_mutual.credential = credential(&token);
        for dial in [off_site, local_mutual] {
            assert!(
                refused(&config(&[("east", dial)])).contains("would send a credential off this site"),
                "a token never rides grid mutual TLS or leaves this site"
            );
        }
        let mut outside = local.clone();
        outside.credential = credential("/etc/praxis/tls/tls.key");
        assert!(refused(&config(&[("local-plain", outside)])).contains("reads its credential outside"));
        for bad in ["/a?b=1", "/a#f", "/a//b", "/a/../b", "relative"] {
            let mut path = local.clone();
            path.base_path = Some(bad.to_owned());
            assert!(
                refused(&config(&[("local-plain", path)])).contains("unusable base path"),
                "{bad}"
            );
        }
    }

    #[test]
    fn a_credentialed_candidate_is_never_dialed() {
        let mut config = config(&[("local-plain", spec("local", DialTransport::Plaintext, true))]);
        config.serving.candidates[0].credential = Some(crate::metadata::CandidateCredential {
            strategy: "bearer_token".to_owned(),
            secret_ref: crate::metadata::CredentialRef {
                key: "token".to_owned(),
                name: "grid-model-client".to_owned(),
                namespace: "models".to_owned(),
            },
        });
        assert!(
            refused(&config).contains("whose credential it does not send"),
            "nothing on a dial injects it"
        );
    }

    #[test]
    fn a_dial_replaces_a_verified_cluster_only_under_its_verified_name() {
        for (verified, transport, allowed) in [
            ("local.grid.internal", mutual_tls("local"), true),
            ("stale.grid.internal", mutual_tls("local"), false),
            ("local.grid.internal", tls("local.grid.internal"), false),
            ("local.grid.internal", DialTransport::Plaintext, false),
        ] {
            let binding = Binding {
                backend_tls: BTreeMap::from([("east".to_owned(), verified.to_owned())]),
                ..Binding::default()
            };
            let config = config(&[("east", spec("local", transport.clone(), true))]);
            assert_eq!(
                validate_dials(&config.serving, &config.dial, &binding).is_ok(),
                allowed,
                "{transport:?} over a mutual TLS cluster verified at {verified}"
            );
        }
    }

    #[test]
    fn peers_must_present_the_identity_every_mutual_tls_dial_presents() {
        let mounted = GridIdentity::default();
        let peer = |ca: &str| -> crate::serving::PeerServingConfig {
            serde_yaml::from_str(&format!(
                "{{site: east, addr: 'east.example.com:9443', server_name: east.grid.internal, authority: east.grid.internal, grid_ca_path: {ca}, client_cert_path: {}, client_key_path: {}}}",
                mounted.cert, mounted.key
            ))
            .expect("a peer")
        };
        let mut config = config(&[("east", spec("east", mutual_tls("east"), false))]);
        config.serving.peers = vec![peer(&mounted.ca)];
        validate_dials(&config.serving, &config.dial, &Binding::default()).expect("the mounted identity");
        config.serving.peers = vec![peer("/etc/other/ca.crt")];
        assert!(
            refused(&config).contains("presents an identity other than the grid TLS mount"),
            "a peer with another identity"
        );
        let mut plain = self::config(&[("local-plain", spec("local", DialTransport::Plaintext, true))]);
        plain.serving.peers = vec![peer("/etc/other/ca.crt")];
        validate_dials(&plain.serving, &plain.dial, &Binding::default())
            .expect("no mutual TLS dial presents the mount");
    }

    #[test]
    fn a_server_only_dial_reads_its_ca_only_from_the_backend_ca_mount() {
        let with_ca = |ca_path: &str| {
            let mut dial = spec("local", tls("granite.models.svc"), true);
            dial.transport = DialTransport::Tls {
                ca_path: Some(ca_path.to_owned()),
                sni: "granite.models.svc".to_owned(),
            };
            config(&[("local-tls", dial)])
        };
        let binding = Binding::default();
        let inside = with_ca(&format!("{BACKEND_CA_BASE}/models-ca/ca.crt"));
        validate_dials(&inside.serving, &inside.dial, &binding).expect("a mounted backend CA");
        for outside in [
            "/etc/praxis/tls/ca.crt",
            "/run/secrets/grid-backend-ca-other/ca.crt",
            "/run/secrets/grid-backend-ca/../../../etc/passwd",
            BACKEND_CA_BASE,
            "grid-backend-ca/models-ca/ca.crt",
        ] {
            let config = with_ca(outside);
            assert!(
                validate_dials(&config.serving, &config.dial, &binding)
                    .expect_err(outside)
                    .to_string()
                    .contains("outside /run/secrets/grid-backend-ca"),
                "{outside} is not a backend CA"
            );
        }
    }

    #[test]
    fn an_off_site_private_address_is_trusted_only_for_a_hostname_behind_mutual_tls() {
        for address in [
            "site-b.ec2.internal:8443",
            "internal-grid-nlb-0123.elb.us-east-1.amazonaws.com:8443",
        ] {
            let mut peer = spec("east", mutual_tls("east"), true);
            peer.address = address.to_owned();
            let config = config(&[("east", peer)]);
            validate_dials(&config.serving, &config.dial, &Binding::default()).expect(address);
            let dir = pem_dir("private-peer");
            let dials = Dials::build(
                &config.dial,
                &InsecureOptions::default(),
                &dir.identity(),
                &DialHealth::default(),
            );
            let host = grid_client::address_host(address);
            assert_eq!(
                dials
                    .get("east")
                    .expect(address)
                    .upstream()
                    .connection
                    .trusted_private_endpoints
                    .iter()
                    .map(AsRef::as_ref)
                    .collect::<Vec<&str>>(),
                [host],
                "praxis builds {address}, trusting only its host"
            );
        }
        for literal in ["10.0.0.9:8443", "[fd00::9]:8443"] {
            let mut peer = spec("east", mutual_tls("east"), true);
            peer.address = literal.to_owned();
            assert!(
                refused(&config(&[("east", peer)])).contains("trusts a private IP literal"),
                "{literal} is never trusted off this site"
            );
        }
    }

    #[test]
    fn a_declared_hop_must_be_a_mutual_tls_dial_to_its_declared_name() {
        let mut config = config(&[("east", spec("east", mutual_tls("east"), false))]);
        config
            .serving
            .provider_hop_sni
            .insert("east".to_owned(), "other.grid.internal".to_owned());
        assert!(
            refused(&config).contains("provider hop 'east'"),
            "a hop dialed under another name"
        );

        let mut server_only = self::config(&[("east", spec("local", tls("east.grid.internal"), false))]);
        server_only
            .serving
            .provider_hop_sni
            .insert("east".to_owned(), "east.grid.internal".to_owned());
        assert!(
            refused(&server_only).contains("provider hop 'east'"),
            "a hop without this site's identity"
        );
    }

    #[test]
    fn a_mutual_tls_candidate_needs_a_stable_id() {
        let mut config = config(&[("east", spec("east", mutual_tls("east"), false))]);
        config.serving.candidates[0].stable_id = None;
        assert!(
            refused(&config).contains("missing stable_id"),
            "hop context names the candidate"
        );
    }

    #[test]
    fn praxis_refuses_a_loopback_literal_unless_private_endpoints_are_allowed() {
        let mut dial = spec("local", DialTransport::Plaintext, false);
        dial.address = "127.0.0.1:8080".to_owned();
        let config = config(&[("local-plain", dial)]);
        let dials = build(&config.dial, &InsecureOptions::default());
        assert!(dials.refused.contains("local-plain"), "loopback is SSRF-sensitive");
        assert!(dials.get("local-plain").is_none(), "nothing built");
        let allowed = InsecureOptions {
            allow_private_endpoints: true,
            ..InsecureOptions::default()
        };
        let allowed = build(&config.dial, &allowed);
        assert!(allowed.get("local-plain").is_some(), "allowed by the startup options");
    }

    #[test]
    fn praxis_refuses_trusting_a_private_ip_literal_and_a_bad_timeout() {
        let mut literal = spec("local", DialTransport::Plaintext, true);
        literal.address = "10.0.0.1:8080".to_owned();
        let mut slow = spec("local", DialTransport::Plaintext, true);
        slow.read_timeout_ms = Some(0);
        let dials = build(
            &config(&[("literal", literal), ("slow", slow)]).dial,
            &InsecureOptions::default(),
        );
        assert!(
            dials.refused.contains("literal"),
            "an IP literal cannot be trusted private"
        );
        assert!(dials.refused.contains("slow"), "praxis' timeout checks apply");
    }

    #[test]
    fn a_dial_praxis_refuses_takes_only_its_own_cluster_out() {
        let dir = pem_dir("refused");
        let mut internal = spec("east", mutual_tls("east"), false);
        internal.address = "east.cluster.internal:8443".to_owned();
        let config = config(&[
            ("east", internal),
            ("west", spec("west", mutual_tls("west"), false)),
            ("local-plain", spec("local", DialTransport::Plaintext, true)),
        ]);
        validate_dials(&config.serving, &config.dial, &Binding::default()).expect("no trust rule is broken");
        let dials = Dials::build(
            &config.dial,
            &InsecureOptions::default(),
            &dir.identity(),
            &DialHealth::default(),
        );
        assert!(
            dials.refused.contains("east"),
            "praxis refuses an untrusted .internal name"
        );
        assert!(dials.unroutable("east"), "so east takes no traffic");
        assert!(dials.get("east").is_none(), "east is not built");
        assert!(dials.get("west").is_some(), "west still dials");
        assert!(dials.get("local-plain").is_some(), "the local dial still dials");
        assert!(!dials.unroutable("west"), "west routes");
    }

    #[test]
    fn a_dial_whose_tls_cannot_be_read_is_undialable_not_refused() {
        let config = config(&[("east", spec("east", mutual_tls("east"), false))]);
        let dials = build(&config.dial, &InsecureOptions::default());
        assert!(dials.get("east").is_none(), "nothing to dial with");
        assert!(dials.undialable.contains("east"), "the cluster is undialable");
        assert!(dials.refused.is_empty(), "praxis accepted it");
    }

    #[test]
    fn a_built_dial_carries_its_identity_timeouts_and_retries() {
        let dir = pem_dir("built");
        let mut dial = spec("east", mutual_tls("east"), false);
        dial.connect_timeout_ms = Some(500);
        dial.total_connect_timeout_ms = Some(1_500);
        dial.read_timeout_ms = Some(30_000);
        dial.write_timeout_ms = Some(10_000);
        dial.connect_retries = Some(1);
        let dials = Dials::build(
            &config(&[("east", dial)]).dial,
            &InsecureOptions::default(),
            &dir.identity(),
            &DialHealth::default(),
        );
        let built = dials.get("east").expect("east is built");
        let upstream = built.upstream();
        assert_eq!(&*upstream.address, "east.example.com:8443", "the one address dialed");
        let tls = upstream.tls.as_ref().expect("mutual TLS");
        assert_eq!(
            tls.sni(),
            Some("east.grid.internal"),
            "verified under the site's grid name"
        );
        assert!(tls.verify(), "verification is always on");
        assert!(tls.client_cert().is_some(), "this site's identity is presented");
        assert!(tls.ca().is_some(), "the grid CA is pinned");
        let connection = &upstream.connection;
        assert_eq!(
            connection.connection_timeout,
            Some(std::time::Duration::from_millis(500))
        );
        assert_eq!(
            connection.total_connection_timeout,
            Some(std::time::Duration::from_millis(1_500))
        );
        assert_eq!(connection.read_timeout, Some(std::time::Duration::from_millis(30_000)));
        assert_eq!(connection.write_timeout, Some(std::time::Duration::from_millis(10_000)));
        assert!(
            connection.trusted_private_endpoints.is_empty(),
            "an off-site dial trusts no private address"
        );
        assert_eq!(
            connection.idle_timeout,
            Some(std::time::Duration::from_millis(IDLE_TIMEOUT_MS)),
            "an idle pooled connection is closed"
        );
        let retry = built.retry();
        assert_eq!(retry.effective_max_retries(), 1, "connect retries as written");
        assert_eq!(
            retry.retriable_conditions,
            vec![praxis_core::config::RetriableCondition::ConnectFailure],
            "only a connect failure is retried"
        );
        assert!(retry.allow_non_idempotent(), "a POST that never connected is retried");
        assert!(
            !retry.configured,
            "configured would clear the one address for a reselector the dial lacks"
        );
    }

    #[test]
    fn a_dial_naming_no_timeouts_gets_bounded_connects_and_long_reads() {
        let dials = build(
            &config(&[("local-plain", spec("local", DialTransport::Plaintext, true))]).dial,
            &InsecureOptions::default(),
        );
        let connection = &dials.get("local-plain").expect("built").upstream().connection;
        let ms = |ms: u64| Some(std::time::Duration::from_millis(ms));
        assert_eq!(connection.connection_timeout, ms(CONNECT_TIMEOUT_MS));
        assert_eq!(connection.total_connection_timeout, ms(TOTAL_CONNECT_TIMEOUT_MS));
        assert_eq!(connection.read_timeout, ms(READ_TIMEOUT_MS));
    }

    #[test]
    fn an_identity_is_read_from_the_one_revision_its_mount_points_at() {
        let dir = std::env::temp_dir().join(format!("grid-dial-revision-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("..2026_10_29")).expect("test io");
        let identity = GridIdentity::under(&dir.to_string_lossy());
        assert_eq!(identity.snapshot(), identity, "a plain directory is read as it is");
        std::os::unix::fs::symlink("..2026_10_29", dir.join("..data")).expect("test io");
        let revision = std::fs::canonicalize(dir.join("..2026_10_29")).expect("test io");
        assert_eq!(
            identity.snapshot(),
            GridIdentity::under(&revision.to_string_lossy()),
            "all three under the revision ..data names"
        );
        let _cleaned = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_local_hostname_dial_trusts_only_its_own_host() {
        let dials = build(
            &config(&[("local-plain", spec("local", DialTransport::Plaintext, true))]).dial,
            &InsecureOptions::default(),
        );
        let upstream = dials.get("local-plain").expect("built").upstream();
        assert!(upstream.tls.is_none(), "plaintext");
        assert_eq!(
            upstream
                .connection
                .trusted_private_endpoints
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>(),
            ["local-plain.models.svc.cluster.local"],
            "the dial's own host"
        );
    }

    #[test]
    fn the_dial_section_parses_by_mode_and_refuses_unknown_fields() {
        let parse = |transport: &str| {
            serde_json::from_str::<DialSpec>(&format!(
                r#"{{"site":"east","address":"east:8443","transport":{transport}}}"#
            ))
        };
        let plaintext = parse(r#"{"mode":"plaintext"}"#).expect("plaintext parses");
        assert_eq!(plaintext.transport, DialTransport::Plaintext);
        assert!(!plaintext.trusted_private, "trust is opt in");
        let server_only = parse(r#"{"mode":"tls","sni":"api.example.com"}"#).expect("tls parses");
        assert_eq!(server_only.transport, tls("api.example.com"));
        assert!(
            parse(r#"{"mode":"tls","sni":"a","verify":false}"#).is_err(),
            "there is no switch to turn verification off"
        );
        assert_eq!(
            parse(r#"{"mode":"mutual_tls","sni":"east.grid.internal"}"#)
                .expect("mutual TLS parses")
                .transport,
            mutual_tls("east")
        );
        assert!(
            parse(r#"{"mode":"mutual_tls","sni":"east.grid.internal","ca_path":"/ca"}"#).is_err(),
            "the identity is the gateway's own, never the file's"
        );
        assert!(parse(r#"{"mode":"insecure"}"#).is_err(), "an unknown mode");
        assert!(
            serde_json::from_str::<DialSpec>(
                r#"{"site":"east","address":"east:8443","transport":{"mode":"plaintext"},"weight":2}"#
            )
            .is_err(),
            "an unknown dial field"
        );
    }

    #[test]
    fn the_dial_file_is_versioned_and_a_missing_one_names_no_dial() {
        let path = Path::new("/run/grid/dial.json");
        let file = crate::serving::parse_dial_config(
            path,
            br#"{"version":"v1","dial":{"east":{"site":"east","address":"east:8443","transport":{"mode":"plaintext"}}}}"#,
        )
        .expect("parses");
        assert_eq!(file.dial.len(), 1, "one dial");
        assert_eq!(
            crate::serving::parse_dial_config(path, b"{}").expect("empty parses"),
            DialConfig::default(),
            "an empty file names no dial"
        );
        assert!(
            crate::serving::parse_dial_config(path, br#"{"version":"v2"}"#).is_err(),
            "a later schema is refused rather than misread"
        );
        assert!(
            crate::serving::parse_dial_config(path, br#"{"dials":{}}"#).is_err(),
            "an unknown top-level field is refused"
        );
        let missing = std::env::temp_dir().join(format!("grid-no-dial-{}", std::process::id()));
        assert_eq!(
            crate::serving::load_dial_config(&missing.join("dial.json")).expect("a missing file loads"),
            DialConfig::default(),
            "a missing file names no dial"
        );
        assert_eq!(
            crate::serving::dial_config_path(Path::new("/etc/grid/serving/serving-config.json")),
            PathBuf::from("/etc/grid/serving/dial.json"),
            "beside the serving config in its mount"
        );
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// The message `validate_dials` refuses `config` with.
    fn refused(config: &Joined) -> String {
        validate_dials(&config.serving, &config.dial, &Binding::default())
            .expect_err("refused")
            .to_string()
    }

    /// Build `dial` under `insecure` with an identity that does not exist.
    fn build(dial: &DialConfig, insecure: &InsecureOptions) -> Dials {
        Dials::build(
            dial,
            insecure,
            &GridIdentity::under("/nonexistent"),
            &DialHealth::default(),
        )
    }

    /// A dial to `site` over `transport` at a hostname address.
    fn spec(site: &str, transport: DialTransport, trusted_private: bool) -> DialSpec {
        let host = if site == "local" {
            "local-plain.models.svc.cluster.local".to_owned()
        } else {
            format!("{site}.example.com")
        };
        DialSpec {
            address: format!("{host}:8443"),
            base_path: None,
            connect_retries: None,
            connect_timeout_ms: None,
            credential: None,
            read_timeout_ms: None,
            site: site.to_owned(),
            total_connect_timeout_ms: None,
            transport,
            trusted_private,
            write_timeout_ms: None,
        }
    }

    /// Mutual TLS to `site`.
    fn mutual_tls(site: &str) -> DialTransport {
        DialTransport::MutualTls {
            sni: certs::site_dns_name(site),
        }
    }

    /// Server-only TLS to `sni` against the system roots.
    fn tls(sni: &str) -> DialTransport {
        DialTransport::Tls {
            ca_path: None,
            sni: sni.to_owned(),
        }
    }

    /// A serving config and the dial file beside it.
    struct Joined {
        dial: DialConfig,
        serving: GridServingConfig,
    }

    /// A config at site `local` with one `llama` candidate per dial, on its cluster at its site.
    fn config(dials: &[(&str, DialSpec)]) -> Joined {
        let serving = GridServingConfig {
            local_site: "local".to_owned(),
            window_secs: 60,
            load_window_ms: 30_000,
            horizon_ms: None,
            candidates: dials
                .iter()
                .map(|(cluster, dial)| CandidateConfig {
                    expires_at_ms: None,
                    admission: AdmissionState::default(),
                    cluster: (*cluster).to_owned(),
                    credential: None,
                    fresh: true,
                    kind: CapabilityKind::InferenceModel,
                    name: "llama".to_owned(),
                    publishes_load: true,
                    site: dial.site.clone(),
                    stable_id: Some(format!("candidate-{cluster}")),
                })
                .collect(),
            provider_hop_clusters: Vec::new(),
            provider_hop_sni: BTreeMap::new(),
            peers: Vec::new(),
        };
        let dial = DialConfig {
            dial: dials
                .iter()
                .map(|(cluster, dial)| ((*cluster).to_owned(), dial.clone()))
                .collect(),
            ..DialConfig::default()
        };
        Joined { dial, serving }
    }

    /// A grid CA and a `local` site identity written under a fresh directory.
    struct PemDir {
        dir: PathBuf,
    }

    impl PemDir {
        /// This directory's identity.
        fn identity(&self) -> GridIdentity {
            GridIdentity::under(&self.dir.to_string_lossy())
        }
    }

    fn pem_dir(name: &str) -> PemDir {
        praxis_tls::provider::install();
        let dir = std::env::temp_dir().join(format!("grid-dial-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("test io");
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let leaf = certs::generate_site_cert(&ca, "local").expect("leaf");
        std::fs::write(dir.join("ca.crt"), &ca.cert_pem).expect("test io");
        std::fs::write(dir.join("tls.crt"), &leaf.cert_pem).expect("test io");
        std::fs::write(dir.join("tls.key"), &leaf.key_pem).expect("test io");
        PemDir { dir }
    }
}
