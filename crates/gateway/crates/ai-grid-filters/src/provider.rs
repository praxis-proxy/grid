//! The `grid_provider` filter: serve an admitted peer's request to one of this site's own
//! backends, named by the stable ID its consumer sends.
//!
//! The operator writes `provider.json` beside the serving config: the enrolled sites admitted
//! to reach this site, and each local backend by stable ID with the dial that reaches it. A
//! watch thread rebuilds a snapshot from it and the filter only reads that snapshot. A peer
//! whose subject organization is not admitted gets 403, as does every peer before a good file
//! lands. The file can name only this site's dials, never a mutual TLS one, so a peer's
//! request never leaves this site.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    time::Duration,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use praxis_filter::{FilterAction, FilterError, HttpFilter, HttpFilterContext, TerminalResponse, parse_filter_config};
use serde::Deserialize;

use crate::{
    control::{Binding, WatchCounts, files_digest, read_dial},
    descriptor::{AdmissionState, CandidateConfig, CapabilityKind},
    dial::{Dialed, Dials, validate_dials},
    health::{Admission, DialHealth},
    route::{InFlight, strip_credential_query},
    serving::{DialConfig, DialSpec, DialTransport, DialVersion, GridServingConfig, mount_revision},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The provider file's name, beside the serving config in the same mount.
pub const PROVIDER_FILE: &str = "provider.json";

/// Seconds a peer should wait before retrying a candidate this site does not serve now.
const RETRY_AFTER_SECS: &str = "5";

/// Request header naming the candidate a consumer picked.
const CANDIDATE_HEADER: &str = "x-ai-routing-candidate";

/// The only request headers a peer's request carries to a backend. A caller credential, the
/// grid's hop context and anything else a peer sends stay at this gateway.
const FORWARDED_HEADERS: [&str; 10] = [
    "host",
    "content-type",
    "content-length",
    "transfer-encoding",
    "accept",
    "accept-encoding",
    "user-agent",
    "x-ai-routing-request-id",
    "traceparent",
    "tracestate",
];

/// Consecutive reads a provider file must be missing before admission is withdrawn, so a
/// read racing a mount swap does not refuse every peer.
const MISSING_TICKS: u8 = 2;

// ---------------------------------------------------------------------------
// Provider File
// ---------------------------------------------------------------------------

/// The provider file the operator writes: who may reach this site, and what they may reach.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    /// Enrolled sites, by subject organization, admitted to reach this site's backends.
    pub admitted_sites: Vec<String>,

    /// This site's backends, by stable ID.
    pub candidates: Vec<ProviderCandidate>,

    /// The local dial of each candidate cluster.
    #[serde(default)]
    pub dial: BTreeMap<String, DialSpec>,

    /// This gateway's own site.
    pub local_site: String,

    /// The schema version, so a later one is refused rather than misread.
    pub version: DialVersion,
}

impl ProviderConfig {
    /// The dial file this config's dials form.
    #[must_use]
    pub fn dials(&self) -> DialConfig {
        DialConfig {
            dial: self.dial.clone(),
            version: self.version,
        }
    }
}

/// One local backend a peer may name.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProviderCandidate {
    /// The stable ID a consumer sends in `x-ai-routing-candidate`.
    pub stable_id: String,

    /// Model name.
    pub name: String,

    /// The cluster whose dial serves it.
    pub cluster: String,
}

/// Refuse a provider file that could send a peer's request anywhere but this site.
///
/// # Errors
///
/// Names the first rule broken: a dial to another site, a mutual TLS dial or one verifying or
/// addressing a grid name, a candidate without a dial, a malformed or repeated stable ID, or an
/// admitted site that is not a site name or is this site. The serving dial rules apply too,
/// run over `dial`, the dials [`ProviderConfig::dials`] forms.
pub fn validate_provider(config: &ProviderConfig, dial: &DialConfig, binding: &Binding) -> Result<(), FilterError> {
    crate::descriptor::validate_site("provider.local_site", &config.local_site)?;
    for site in &config.admitted_sites {
        crate::descriptor::validate_site("provider.admitted_sites", site)?;
        if *site == config.local_site {
            return Err("grid: provider file admits this site itself".into());
        }
    }
    let mut seen = BTreeSet::new();
    for candidate in &config.candidates {
        if !certs::is_stable_id(&candidate.stable_id) {
            return Err(format!(
                "grid: provider candidate stable_id {:?} is not 8 lowercase hex",
                candidate.stable_id
            )
            .into());
        }
        if !seen.insert(candidate.stable_id.as_str()) {
            return Err(format!("grid: provider stable_id {} is repeated", candidate.stable_id).into());
        }
        if !dial.dial.contains_key(&candidate.cluster) {
            return Err(format!("grid: provider candidate cluster '{}' has no dial", candidate.cluster).into());
        }
    }
    for (cluster, spec) in &dial.dial {
        if spec.site != config.local_site {
            return Err(format!(
                "grid: provider dial '{cluster}' names site '{}', not this site",
                spec.site
            )
            .into());
        }
        if matches!(spec.transport, DialTransport::MutualTls { .. }) {
            return Err(format!("grid: provider dial '{cluster}' is mutual TLS, a hop to another grid gateway").into());
        }
        if spec.transport.sni().is_some_and(certs::is_grid_name)
            || certs::is_grid_name(grid_client::address_host(&spec.address))
        {
            return Err(format!("grid: provider dial '{cluster}' names a grid gateway").into());
        }
    }
    validate_dials(&as_serving(config), dial, binding)
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// What the filter reads: the admitted sites and each servable backend's built dial.
#[derive(Clone, Debug, Default)]
pub struct ProviderSnapshot {
    /// Admitted subject organizations.
    admitted: BTreeSet<String>,

    /// Built dials by stable ID.
    by_id: HashMap<String, Served>,
}

impl ProviderSnapshot {
    /// Whether `organization` is admitted.
    #[must_use]
    pub fn admits(&self, organization: &str) -> bool {
        self.admitted.contains(organization)
    }

    /// Whether a backend with `stable_id` is served.
    #[must_use]
    pub fn serves(&self, stable_id: &str) -> bool {
        self.by_id.contains_key(stable_id)
    }
}

/// One servable backend: its cluster and built dial.
#[derive(Clone, Debug)]
struct Served {
    /// The cluster, for metrics.
    cluster: Arc<str>,

    /// The built dial.
    dialed: Arc<Dialed>,
}

/// The last good provider file and the digest of the files its dials read.
struct Good {
    /// The config, its admitted sites narrowed by any refused file since.
    config: ProviderConfig,

    /// Its dials.
    dial: DialConfig,

    /// The digest of the CA files its dials read, when it was built.
    digest: [u8; 32],
}

/// The provider file as last read.
#[derive(Debug, Eq, PartialEq)]
enum Seen {
    /// Never read.
    Unread,
    /// Not there.
    Missing,
    /// These bytes.
    File(Vec<u8>),
}

/// Builds snapshots from provider files, keeping the last good one.
struct Builder {
    /// What the gateway fixed at startup.
    binding: Binding,

    /// Dial states, kept across rebuilds by cluster.
    health: DialHealth,

    /// The live snapshot.
    snapshot: Arc<ArcSwap<ProviderSnapshot>>,

    /// The file last handled.
    seen: Seen,

    /// Consecutive reads that found no file.
    missing: u8,

    /// The last good file.
    good: Option<Good>,

    /// Files applied and refused.
    counts: Arc<WatchCounts>,
}

impl Builder {
    /// A builder over `binding` publishing to an empty snapshot.
    fn new(binding: Binding) -> Self {
        Self {
            binding,
            health: DialHealth::default(),
            snapshot: Arc::new(ArcSwap::from_pointee(ProviderSnapshot::default())),
            seen: Seen::Unread,
            missing: 0,
            good: None,
            counts: Arc::new(WatchCounts::default()),
        }
    }

    /// Handle one read of the file, `None` when it is missing.
    fn tick(&mut self, bytes: Option<Vec<u8>>) {
        if bytes.is_none() {
            self.missing = self.missing.saturating_add(1);
            if self.missing < MISSING_TICKS {
                return;
            }
        } else {
            self.missing = 0;
        }
        let seen = bytes.map_or(Seen::Missing, Seen::File);
        if self.seen != seen {
            self.apply(match &seen {
                Seen::File(file) => Some(file),
                Seen::Unread | Seen::Missing => None,
            });
            self.seen = seen;
        } else if let Some(good) = self.good.as_ref()
            && ca_digest(&good.dial) != good.digest
        {
            tracing::info!("grid: provider dial CA files changed; rebuilding");
            let (config, dial) = (good.config.clone(), good.dial.clone());
            self.publish(config, dial);
        }
    }

    /// Apply a changed file: a missing one admits no one, a refused one only narrows admission.
    fn apply(&mut self, bytes: Option<&[u8]>) {
        let Some(bytes) = bytes else {
            tracing::warn!("grid: provider file missing; refusing every peer");
            self.good = None;
            self.snapshot.store(Arc::new(ProviderSnapshot::default()));
            self.counts.count_applied();
            metrics::counter!("grid_provider_config_reload_total", "result" => "applied").increment(1);
            return;
        };
        let parsed = serde_yaml::from_slice::<ProviderConfig>(bytes)
            .map_err(|error| -> FilterError { format!("grid: parsing {PROVIDER_FILE}: {error}").into() });
        let checked = parsed.and_then(|config| {
            let dial = config.dials();
            match validate_provider(&config, &dial, &self.binding) {
                Ok(()) => Ok((config, dial)),
                Err(error) => {
                    self.narrow(&config.admitted_sites);
                    Err(error)
                },
            }
        });
        match checked {
            Ok((config, dial)) => {
                self.publish(config, dial);
                self.counts.count_applied();
                metrics::counter!("grid_provider_config_reload_total", "result" => "applied").increment(1);
            },
            Err(error) => {
                tracing::warn!(%error, "grid: provider file refused; keeping the last good one");
                self.counts.count_rejected();
                metrics::counter!("grid_provider_config_reload_total", "result" => "rejected").increment(1);
            },
        }
    }

    /// Take from the running admission every site a refused file no longer lists.
    fn narrow(&mut self, listed: &[String]) {
        let Some(good) = self.good.as_mut() else {
            return;
        };
        let before = good.config.admitted_sites.len();
        good.config.admitted_sites.retain(|site| listed.contains(site));
        if good.config.admitted_sites.len() == before {
            return;
        }
        tracing::warn!(admitted = ?good.config.admitted_sites, "grid: a refused provider file still narrows admission");
        let mut snapshot = ProviderSnapshot::clone(&self.snapshot.load());
        snapshot.admitted.retain(|site| listed.contains(site));
        self.snapshot.store(Arc::new(snapshot));
    }

    /// Build `config` and make it the live snapshot and the last good file.
    fn publish(&mut self, config: ProviderConfig, dial: DialConfig) {
        let digest = ca_digest(&dial);
        let dials = Dials::build(&dial, &self.binding.insecure, &self.binding.grid_identity, &self.health);
        self.health.adopt(dials.states());
        let by_id = config
            .candidates
            .iter()
            .filter_map(|candidate| {
                dials.get(&candidate.cluster).map(|built| {
                    let served = Served {
                        cluster: Arc::from(candidate.cluster.as_str()),
                        dialed: Arc::clone(built),
                    };
                    (candidate.stable_id.clone(), served)
                })
            })
            .collect();
        let snapshot = ProviderSnapshot {
            admitted: config.admitted_sites.iter().cloned().collect(),
            by_id,
        };
        tracing::info!(
            admitted = snapshot.admitted.len(),
            candidates = snapshot.by_id.len(),
            "grid: provider file applied"
        );
        self.snapshot.store(Arc::new(snapshot));
        self.good = Some(Good { config, dial, digest });
    }
}

/// The running provider: its snapshot, kept current by a watch thread until dropped.
pub struct ProviderRuntime {
    /// The live snapshot.
    snapshot: Arc<ArcSwap<ProviderSnapshot>>,

    /// Dropping it stops the watch thread.
    _stop: mpsc::Sender<()>,
}

impl ProviderRuntime {
    /// The live snapshot the filter reads.
    #[must_use]
    pub fn snapshot(&self) -> Arc<ArcSwap<ProviderSnapshot>> {
        Arc::clone(&self.snapshot)
    }
}

/// Load the provider file at `path` now, then re-read it every `every`.
///
/// A file missing for two reads admits no one. A file that does not parse or validate is
/// refused and the last good one kept, less any site it no longer lists.
///
/// # Errors
///
/// Returns the error spawning the watch thread.
pub fn spawn_provider(path: PathBuf, binding: Binding, every: Duration) -> std::io::Result<ProviderRuntime> {
    praxis_tls::provider::install();
    let mut builder = Builder::new(binding);
    let snapshot = Arc::clone(&builder.snapshot);
    // At startup nothing is admitted yet, so a missing file needs no second read.
    builder.missing = MISSING_TICKS;
    reread(&mut builder, &path);
    let (stop, stopped) = mpsc::channel::<()>();
    std::thread::Builder::new()
        .name("grid-provider-watch".to_owned())
        .spawn(move || {
            while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(every) {
                reread(&mut builder, &path);
            }
        })?;
    Ok(ProviderRuntime { snapshot, _stop: stop })
}

// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

/// The `grid_provider` filter block.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderFilterConfig {
    /// The exact request paths a peer may call.
    allowed_paths: Vec<String>,
}

/// Serves an admitted peer's request to the local backend its stable ID names.
pub(crate) struct GridProviderFilter {
    /// Exact paths a peer may call.
    allowed_paths: BTreeSet<String>,

    /// The live snapshot.
    snapshot: Arc<ArcSwap<ProviderSnapshot>>,
}

impl GridProviderFilter {
    /// Build from a filter block over `snapshot`.
    pub(crate) fn from_config(
        config: &serde_yaml::Value,
        snapshot: Arc<ArcSwap<ProviderSnapshot>>,
    ) -> Result<Box<dyn HttpFilter>, FilterError> {
        let parsed: ProviderFilterConfig = parse_filter_config("grid_provider", config)?;
        if parsed.allowed_paths.is_empty() || parsed.allowed_paths.iter().any(|path| !path.starts_with('/')) {
            return Err("grid_provider: allowed_paths must name at least one absolute path".into());
        }
        Ok(Box::new(Self {
            allowed_paths: parsed.allowed_paths.into_iter().collect(),
            snapshot,
        }))
    }
}

/// The dial a request was sent over, for its outcome to be recorded.
#[derive(Debug)]
struct ProviderState {
    /// The request in flight.
    dialed: Option<InFlight>,
}

#[async_trait]
impl HttpFilter for GridProviderFilter {
    fn name(&self) -> &'static str {
        "grid_provider"
    }

    fn selects_cluster(&self) -> bool {
        true
    }

    async fn on_request(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        let snapshot = self.snapshot.load();
        let organization = ctx.peer_identity.as_ref().and_then(|peer| peer.organization.as_deref());
        if !organization.is_some_and(|organization| snapshot.admits(organization)) {
            tracing::debug!(organization, "grid_provider: peer not admitted");
            return Ok(refuse("forbidden", 403));
        }
        if !matches!(ctx.request.method, http::Method::GET | http::Method::POST) {
            return Ok(refuse("method", 405));
        }
        if !self.allowed_paths.contains(ctx.request.uri.path()) {
            return Ok(refuse("path", 404));
        }
        let mut named = ctx.request.headers.get_all(CANDIDATE_HEADER).iter();
        let Some(id) = (match (named.next(), named.next()) {
            (Some(value), None) => value.to_str().ok().filter(|id| certs::is_stable_id(id)),
            _ => None,
        }) else {
            return Ok(refuse("malformed", 400));
        };
        let Some(Served { cluster, dialed }) = snapshot.by_id.get(id) else {
            return Ok(refuse("candidate", 503));
        };
        forward_only_allowed(ctx);
        // A credentialed backend gets the dial's own token, never anything a peer sent.
        if let Some(token) = dialed.credential() {
            ctx.request_headers_to_set
                .push((http::header::AUTHORIZATION, token.clone()));
        }
        let probe = match dialed.health().admit() {
            Admission::Probe => true,
            // A provider has one backend per ID, so a down dial is still tried rather than refused.
            Admission::Open | Admission::Busy => false,
        };
        ctx.cluster = Some(Arc::clone(cluster));
        ctx.upstream = Some(dialed.upstream().clone());
        ctx.retry_policy = Some(Arc::clone(dialed.retry()));
        ctx.insert_filter_state(ProviderState {
            dialed: Some(InFlight::over(dialed, probe)),
        });
        metrics::counter!("grid_provider_requests_total", "result" => "served").increment(1);
        Ok(FilterAction::Continue)
    }

    async fn on_response(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        let (answered, reached) = (ctx.response_header.is_some(), ctx.upstream_reached);
        if let Some(dialed) = ctx
            .get_filter_state_mut::<ProviderState>()
            .and_then(|state| state.dialed.take())
        {
            dialed.settle(answered, reached);
        }
        Ok(FilterAction::Continue)
    }
}

// ---------------------------------------------------------------------------
// Utility Functions
// ---------------------------------------------------------------------------

/// `config` as a serving config of local candidates, for the serving dial rules.
fn as_serving(config: &ProviderConfig) -> GridServingConfig {
    GridServingConfig {
        local_site: config.local_site.clone(),
        window_secs: 0,
        load_window_ms: 0,
        horizon_ms: None,
        candidates: config
            .candidates
            .iter()
            .map(|candidate| CandidateConfig {
                expires_at_ms: None,
                admission: AdmissionState::NewAndExisting,
                cluster: candidate.cluster.clone(),
                credential: None,
                fresh: true,
                kind: CapabilityKind::InferenceModel,
                name: candidate.name.clone(),
                publishes_load: true,
                site: config.local_site.clone(),
                stable_id: Some(candidate.stable_id.clone()),
            })
            .collect(),
        provider_hop_clusters: Vec::new(),
        provider_hop_sni: BTreeMap::new(),
        peers: Vec::new(),
    }
}

/// A change detector over the CA and token files `dial` reads.
fn ca_digest(dial: &DialConfig) -> [u8; 32] {
    let cas = dial.dial.values().filter_map(|spec| spec.transport.ca_path());
    let tokens = dial
        .dial
        .values()
        .filter_map(|spec| spec.credential.as_ref().map(|credential| credential.path.as_str()));
    files_digest(cas.chain(tokens).collect())
}

/// Read the provider file at `path` from the mount's current revision into `builder`, keeping
/// the last good one when it cannot be read.
fn reread(builder: &mut Builder, path: &Path) {
    let file = path.parent().map_or_else(
        || path.to_path_buf(),
        |dir| mount_revision(dir).join(path.file_name().unwrap_or_default()),
    );
    match read_dial(&file) {
        Ok(bytes) => builder.tick(bytes),
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "grid: provider file unreadable; keeping the last good one");
        },
    }
}

/// Drop every request header but [`FORWARDED_HEADERS`], and the caller's credential query
/// parameters: a backend authorizes nothing a peer sends.
fn forward_only_allowed(ctx: &mut HttpFilterContext<'_>) {
    let dropped: Vec<http::header::HeaderName> = ctx
        .request
        .headers
        .keys()
        .filter(|name| !FORWARDED_HEADERS.contains(&name.as_str()))
        .cloned()
        .collect();
    ctx.request_headers_to_remove.extend(dropped);
    let sent = ctx
        .request
        .uri
        .path_and_query()
        .map_or("/", http::uri::PathAndQuery::as_str);
    if let Some(stripped) = strip_credential_query(sent) {
        ctx.rewritten_path = Some(stripped);
    }
}

/// A refusal with `status`, counted by `reason`; a 503 carries Retry-After.
fn refuse(reason: &'static str, status: u16) -> FilterAction {
    metrics::counter!("grid_provider_requests_total", "result" => reason).increment(1);
    let mut response = TerminalResponse::new(status);
    if status == 503 {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::RETRY_AFTER,
            http::HeaderValue::from_static(RETRY_AFTER_SECS),
        );
        response = response.with_headers(headers);
    }
    FilterAction::TerminalResponse(Box::new(response))
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::type_complexity,
    clippy::too_many_lines,
    reason = "tests"
)]
mod tests {
    use praxis_core::config::InsecureOptions;
    use praxis_tls::TlsPeerIdentity;

    use super::*;
    use crate::route::tests::context;

    /// The operator-rendered contract fixture.
    const GOLDEN: &str = include_str!("../../../tests/testdata/provider.json");

    #[test]
    fn the_operator_render_parses_validates_and_builds() {
        let config: ProviderConfig = serde_yaml::from_str(GOLDEN).expect("the golden parses");
        validate_provider(&config, &config.dials(), &Binding::default()).expect("the golden validates");
        let mut builder = builder(InsecureOptions::default());
        builder.tick(Some(GOLDEN.as_bytes().to_vec()));
        let snapshot = builder.snapshot.load();
        assert!(
            snapshot.admits("site-b") && snapshot.admits("site-c"),
            "both peers admitted"
        );
        assert!(!snapshot.admits("site-a"), "this site never admits itself");
        assert!(snapshot.serves("ccac628c"), "the plaintext backend is built");
        assert!(
            !snapshot.serves("67b6bb1e"),
            "a backend whose CA file cannot be read is not served"
        );
    }

    #[test]
    fn a_file_that_could_route_onward_is_refused_whole() {
        let refusals: [(&str, &str, fn(&mut ProviderConfig)); 12] = [
            ("a dial to another site", "not this site", |config| {
                dial_mut(config).site = "site-b".to_owned();
            }),
            ("a mutual TLS dial", "mutual TLS", |config| {
                dial_mut(config).transport = DialTransport::MutualTls {
                    sni: "site-a.grid.internal".to_owned(),
                };
            }),
            ("a TLS dial verifying a grid name", "names a grid gateway", |config| {
                dial_mut(config).transport = DialTransport::Tls {
                    ca_path: None,
                    sni: "site-b.grid.internal.".to_owned(),
                };
            }),
            ("an address naming a grid gateway", "names a grid gateway", |config| {
                dial_mut(config).address = "site-b.GRID.internal:8443".to_owned();
            }),
            ("the grid apex", "names a grid gateway", |config| {
                dial_mut(config).address = "grid.internal:8443".to_owned();
            }),
            ("a CA outside the backend CA mount", "outside", |config| {
                dial_mut(config).transport = DialTransport::Tls {
                    ca_path: Some("/etc/praxis/tls/ca.crt".to_owned()),
                    sni: "llama.models.svc".to_owned(),
                };
            }),
            ("a candidate with no dial", "has no dial", |config| {
                config.dial.clear();
            }),
            ("a repeated stable id", "repeated", |config| {
                let twin = config.candidates[0].clone();
                config.candidates.push(twin);
            }),
            ("a malformed stable id", "8 lowercase hex", |config| {
                config.candidates[0].stable_id = "CCAC628C".to_owned();
            }),
            ("this site admitted", "this site itself", |config| {
                config.admitted_sites.push("site-a".to_owned());
            }),
            ("an admitted name that is no site", "DNS-1123", |config| {
                config.admitted_sites.push("Site_B".to_owned());
            }),
            ("a bad local site", "DNS-1123", |config| {
                config.local_site = "-".to_owned();
            }),
        ];
        let binding = Binding::default();
        validate_provider(&minimal(), &minimal().dials(), &binding).expect("the fixture is valid");
        for (what, reason, break_it) in refusals {
            let mut config = minimal();
            break_it(&mut config);
            let error = validate_provider(&config, &config.dials(), &binding)
                .err()
                .unwrap_or_else(|| panic!("{what} must be refused"));
            assert!(error.to_string().contains(reason), "{what}: {error}");
        }
        for text in [
            GOLDEN.replace("\"version\": \"v1\"", "\"version\": \"v2\""),
            GOLDEN.replace("\"local_site\"", "\"credential\": {},\n  \"local_site\""),
            GOLDEN.replace("  \"version\": \"v1\"\n", "  \"version\": \"v1\",\n  \"extra\": 1\n"),
        ] {
            assert!(
                serde_yaml::from_str::<ProviderConfig>(&text).is_err(),
                "an unknown version or field is refused: {text}"
            );
        }
    }

    #[test]
    fn a_missing_file_admits_no_one_and_a_bad_one_keeps_the_last_good() {
        let mut builder = builder(loopback());
        builder.tick(None);
        builder.tick(None);
        assert!(!builder.snapshot.load().admits("site-b"), "no file admits no one");
        let good = text(&minimal());
        builder.tick(Some(good.clone().into_bytes()));
        assert!(builder.snapshot.load().admits("site-b"), "a good file admits its sites");
        assert!(builder.snapshot.load().serves("ccac628c"), "and serves its backend");
        for bad in [
            "{".to_owned(),
            good.replace("\"v1\"", "\"v2\""),
            text(&ProviderConfig {
                admitted_sites: vec!["site-a".to_owned(), "site-b".to_owned(), "site-c".to_owned()],
                ..minimal()
            }),
        ] {
            builder.tick(Some(bad.into_bytes()));
            assert!(
                builder.snapshot.load().admits("site-b"),
                "a refused file keeps the last good"
            );
            assert!(!builder.snapshot.load().admits("site-a"), "this site is never admitted");
        }
        assert_eq!(builder.counts.rejected(), 3, "each refusal counted");
        builder.tick(None);
        assert!(
            builder.snapshot.load().admits("site-b"),
            "one missing read races a mount swap and changes nothing"
        );
        builder.tick(Some(good.into_bytes()));
        let removed = text(&ProviderConfig {
            admitted_sites: vec!["site-c".to_owned()],
            ..minimal()
        });
        builder.tick(Some(removed.into_bytes()));
        assert!(!builder.snapshot.load().admits("site-b"), "a removed site is refused");
        assert!(builder.snapshot.load().admits("site-c"), "the remaining one admitted");
        builder.tick(None);
        builder.tick(None);
        assert!(
            !builder.snapshot.load().admits("site-c"),
            "a file gone for two reads admits no one"
        );
    }

    #[test]
    fn a_refused_file_that_drops_a_site_still_refuses_it() {
        let mut builder = builder(loopback());
        builder.tick(Some(text(&minimal()).into_bytes()));
        let refused_and_shrunk = text(&ProviderConfig {
            admitted_sites: vec!["site-c".to_owned(), "site-z".to_owned(), "site-a".to_owned()],
            ..minimal()
        });
        builder.tick(Some(refused_and_shrunk.into_bytes()));
        let snapshot = builder.snapshot.load();
        assert_eq!(builder.counts.rejected(), 1, "the file is refused");
        assert!(!snapshot.admits("site-b"), "the site it dropped is refused");
        assert!(snapshot.admits("site-c"), "a site both list stays admitted");
        assert!(!snapshot.admits("site-z"), "a refused file never adds a site");
        assert!(
            snapshot.serves("ccac628c"),
            "the backends stay as the last good file built them"
        );
    }

    #[tokio::test]
    async fn only_an_admitted_peer_naming_a_served_backend_reaches_it() -> Result<(), FilterError> {
        let filter = filter_over(&minimal());
        let chat = "/v1/chat/completions";
        let cases: [(&str, Option<&str>, http::Method, &str, Option<&str>, u16); 10] = [
            ("no identity", None, http::Method::POST, chat, Some("ccac628c"), 403),
            (
                "unlisted",
                Some("site-z"),
                http::Method::POST,
                chat,
                Some("ccac628c"),
                403,
            ),
            (
                "this site",
                Some("site-a"),
                http::Method::POST,
                chat,
                Some("ccac628c"),
                403,
            ),
            (
                "method",
                Some("site-b"),
                http::Method::DELETE,
                chat,
                Some("ccac628c"),
                405,
            ),
            (
                "path",
                Some("site-b"),
                http::Method::POST,
                "/v1/chat/completions/../x",
                Some("ccac628c"),
                404,
            ),
            ("no candidate", Some("site-b"), http::Method::POST, chat, None, 400),
            (
                "malformed",
                Some("site-b"),
                http::Method::POST,
                chat,
                Some("CCAC628C"),
                400,
            ),
            (
                "not served",
                Some("site-b"),
                http::Method::POST,
                chat,
                Some("67b6bb1e"),
                503,
            ),
            ("served", Some("site-b"), http::Method::POST, chat, Some("ccac628c"), 0),
            (
                "served models",
                Some("site-b"),
                http::Method::GET,
                "/v1/models",
                Some("ccac628c"),
                0,
            ),
        ];
        for (what, organization, method, path, candidate, status) in cases {
            let mut request = request(method, path);
            if let Some(candidate) = candidate {
                request
                    .headers
                    .insert(CANDIDATE_HEADER, http::HeaderValue::from_static(candidate));
            }
            let mut ctx = context(&request);
            ctx.peer_identity = organization.map(peer);
            match filter.on_request(&mut ctx).await? {
                FilterAction::TerminalResponse(response) => {
                    assert_eq!(response.status, status, "{what}");
                    assert!(ctx.upstream.is_none(), "{what}: a refused request has no upstream");
                    assert_eq!(
                        response.headers.contains_key(http::header::RETRY_AFTER),
                        status == 503,
                        "{what}: only an unserved candidate says when to retry"
                    );
                },
                FilterAction::Continue => {
                    assert_eq!(status, 0, "{what} must be refused");
                    let upstream = ctx.upstream.as_ref().expect("served: an upstream");
                    assert_eq!(&*upstream.address, "127.0.0.1:9", "served: the local dial");
                    assert_eq!(ctx.cluster.as_deref(), Some("local-llama"), "served: the cluster");
                    assert!(ctx.retry_policy.is_some(), "served: the dial's retry policy");
                },
                other @ (FilterAction::Reject(_)
                | FilterAction::StreamingTerminalResponse(_)
                | FilterAction::Release
                | FilterAction::BodyDone) => panic!("{what}: unexpected {other:?}"),
            }
        }
        let mut twice = request(http::Method::POST, chat);
        twice
            .headers
            .append(CANDIDATE_HEADER, http::HeaderValue::from_static("ccac628c"));
        twice
            .headers
            .append(CANDIDATE_HEADER, http::HeaderValue::from_static("ccac628c"));
        let mut ctx = context(&twice);
        ctx.peer_identity = Some(peer("site-b"));
        assert!(
            matches!(filter.on_request(&mut ctx).await?, FilterAction::TerminalResponse(response) if response.status == 400),
            "two candidate headers name no one backend"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_served_request_carries_only_the_allowed_headers() -> Result<(), FilterError> {
        let filter = filter_over(&minimal());
        let mut request = request(
            http::Method::POST,
            "/v1/chat/completions?api_key=s3cret&stream=true&token=t",
        );
        for (name, value) in [
            ("authorization", "Bearer caller"),
            ("cookie", "session=1"),
            ("x-api-key", "k"),
            ("x-amz-security-token", "t"),
            ("x-custom-credential", "c"),
            (CANDIDATE_HEADER, "ccac628c"),
            ("x-ai-routing-revision", "7"),
            ("x-ai-routing-request-id", "req-1"),
            ("content-type", "application/json"),
            ("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        ] {
            request.headers.insert(
                http::header::HeaderName::from_static(name),
                http::HeaderValue::from_static(value),
            );
        }
        let mut ctx = context(&request);
        ctx.peer_identity = Some(peer("site-b"));
        assert!(
            matches!(filter.on_request(&mut ctx).await?, FilterAction::Continue),
            "served"
        );
        let removed: BTreeSet<&str> = ctx
            .request_headers_to_remove
            .iter()
            .map(http::header::HeaderName::as_str)
            .collect();
        for name in request.headers.keys() {
            assert_eq!(
                removed.contains(name.as_str()),
                !FORWARDED_HEADERS.contains(&name.as_str()),
                "{name}: only the allowed headers are forwarded"
            );
        }
        assert!(
            removed.contains("authorization") && removed.contains("x-custom-credential"),
            "credentials stripped"
        );
        assert_eq!(
            ctx.rewritten_path.as_deref(),
            Some("/v1/chat/completions?stream=true"),
            "credential query parameters are stripped"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_credentialed_backend_gets_the_dials_token_not_the_peers() -> Result<(), FilterError> {
        let base = std::env::temp_dir().join(format!("provider-token-{}", std::process::id()));
        std::fs::create_dir_all(base.join("model-token")).expect("token dir");
        std::fs::write(base.join("model-token/token"), "s3cret\n").expect("token");
        let mut config = minimal();
        dial_mut(&mut config).credential = Some(crate::serving::DialCredential {
            path: base.join("model-token/token").display().to_string(),
            strategy: crate::serving::CredentialStrategy::BearerToken,
        });
        let mut builder = Builder::new(Binding {
            credential_base: base.clone(),
            insecure: loopback(),
            ..Binding::default()
        });
        let dial = config.dials();
        validate_provider(&config, &dial, &builder.binding)?;
        builder.publish(config, dial);
        let filter = GridProviderFilter {
            allowed_paths: BTreeSet::from(["/v1/chat/completions".to_owned()]),
            snapshot: Arc::clone(&builder.snapshot),
        };
        let mut request = request(http::Method::POST, "/v1/chat/completions");
        request
            .headers
            .insert(CANDIDATE_HEADER, http::HeaderValue::from_static("ccac628c"));
        request.headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static("Bearer peer"),
        );
        let mut ctx = context(&request);
        ctx.peer_identity = Some(peer("site-b"));
        assert!(
            matches!(filter.on_request(&mut ctx).await?, FilterAction::Continue),
            "served"
        );
        assert!(
            ctx.request_headers_to_remove.contains(&http::header::AUTHORIZATION),
            "the peer's Authorization is dropped"
        );
        let sent: Vec<&http::HeaderValue> = ctx
            .request_headers_to_set
            .iter()
            .filter(|(name, _)| *name == http::header::AUTHORIZATION)
            .map(|(_, value)| value)
            .collect();
        assert_eq!(
            sent,
            [&http::HeaderValue::from_static("Bearer s3cret")],
            "only the dial's token is sent"
        );

        let mut outside = minimal();
        dial_mut(&mut outside).credential = Some(crate::serving::DialCredential {
            path: "/etc/praxis/tls/tls.key".to_owned(),
            strategy: crate::serving::CredentialStrategy::BearerToken,
        });
        assert!(
            validate_provider(&outside, &outside.dials(), &builder.binding).is_err(),
            "a token outside the credential mount is refused"
        );
        let _cleaned = std::fs::remove_dir_all(&base);
        Ok(())
    }

    #[test]
    fn the_filter_block_needs_absolute_allowed_paths() {
        let snapshot = Arc::new(ArcSwap::from_pointee(ProviderSnapshot::default()));
        for yaml in [
            "{}",
            "allowed_paths: []",
            "allowed_paths: [v1/models]",
            "allowed_paths: [/a]\nextra: 1",
        ] {
            let config: serde_yaml::Value = serde_yaml::from_str(yaml).expect("yaml");
            assert!(
                GridProviderFilter::from_config(&config, Arc::clone(&snapshot)).is_err(),
                "{yaml} is refused"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    fn minimal() -> ProviderConfig {
        serde_yaml::from_str(
            "admitted_sites: [site-b, site-c]\ncandidates: [{stable_id: ccac628c, name: llama, cluster: local-llama}]\n\
             dial: {local-llama: {address: '127.0.0.1:9', site: site-a, transport: {mode: plaintext}}}\n\
             local_site: site-a\nversion: v1\n",
        )
        .expect("fixture")
    }

    fn dial_mut(config: &mut ProviderConfig) -> &mut DialSpec {
        config.dial.get_mut("local-llama").expect("dial")
    }

    fn text(config: &ProviderConfig) -> String {
        let dial: BTreeMap<&str, serde_json::Value> = config
            .dial
            .iter()
            .map(|(cluster, dial)| {
                let transport = match &dial.transport {
                    DialTransport::Plaintext => serde_json::json!({"mode": "plaintext"}),
                    other @ (DialTransport::MutualTls { .. } | DialTransport::Tls { .. }) => {
                        panic!("unused in fixtures: {other:?}")
                    },
                };
                (
                    cluster.as_str(),
                    serde_json::json!({"address": dial.address, "site": dial.site, "transport": transport}),
                )
            })
            .collect();
        let candidates: Vec<serde_json::Value> = config
            .candidates
            .iter()
            .map(|candidate| {
                serde_json::json!({"stable_id": candidate.stable_id, "name": candidate.name, "cluster": candidate.cluster})
            })
            .collect();
        serde_json::json!({
            "admitted_sites": config.admitted_sites,
            "candidates": candidates,
            "dial": dial,
            "local_site": config.local_site,
            "version": "v1",
        })
        .to_string()
    }

    fn loopback() -> InsecureOptions {
        InsecureOptions {
            allow_private_endpoints: true,
            ..InsecureOptions::default()
        }
    }

    fn builder(insecure: InsecureOptions) -> Builder {
        Builder::new(Binding {
            insecure,
            ..Binding::default()
        })
    }

    fn filter_over(config: &ProviderConfig) -> GridProviderFilter {
        let mut builder = builder(loopback());
        builder.tick(Some(text(config).into_bytes()));
        GridProviderFilter {
            allowed_paths: BTreeSet::from(["/v1/chat/completions".to_owned(), "/v1/models".to_owned()]),
            snapshot: Arc::clone(&builder.snapshot),
        }
    }

    fn request(method: http::Method, path: &str) -> praxis_filter::Request {
        praxis_filter::Request {
            method,
            uri: path.parse().expect("uri"),
            headers: http::HeaderMap::new(),
        }
    }

    fn peer(organization: &str) -> Arc<TlsPeerIdentity> {
        Arc::new(TlsPeerIdentity {
            cert_digest: vec![1; 32],
            organization: Some(organization.to_owned()),
            serial_number: None,
        })
    }
}
