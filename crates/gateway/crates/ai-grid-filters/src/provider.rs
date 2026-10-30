//! The `grid_provider` filter: serve an admitted peer's request to one of this site's own
//! backends, named by the stable ID its consumer sends.
//!
//! The operator writes `provider.json` beside the serving config: the enrolled sites admitted
//! to reach this site, and each local backend by stable ID with the dial that reaches it. A
//! watch thread rebuilds a snapshot from it; the filter only reads that snapshot. A peer whose
//! subject organization is not admitted gets 403, so is every peer before a good file lands.
//! The file can name only this site's dials, never a mutual TLS one, so a peer's request never
//! leaves this site.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use praxis_filter::{FilterAction, FilterError, HttpFilter, HttpFilterContext, TerminalResponse, parse_filter_config};
use serde::Deserialize;

use crate::{
    control::Binding,
    descriptor::{AdmissionState, CandidateConfig, CapabilityKind},
    dial::{Dialed, Dials, validate_dials},
    health::{DialHealth, DialState},
    route::{
        CALLER_CREDENTIAL_HEADERS, OVERLAY_REVISION_HEADER, SELECTED_CANDIDATE_HEADER, dial_outcome,
        strip_credential_query,
    },
    serving::{DialConfig, DialSpec, DialTransport, DialVersion, GridServingConfig},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The provider file's name, beside the serving config in the same mount.
pub const PROVIDER_FILE: &str = "provider.json";

/// Length of a candidate stable ID.
const STABLE_ID_LEN: usize = 8;

/// Seconds a peer should wait before retrying a candidate this site does not serve now.
const RETRY_AFTER_SECS: &str = "5";

/// The grid's own name suffix: a dial verifying one is another site's gateway.
const GRID_SUFFIX: &str = ".grid.internal";

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
/// Names the first rule broken: a dial to another site, a mutual TLS dial or one verifying a
/// grid name, a candidate without a dial, a malformed or repeated stable ID, or an admitted
/// site that is not a site name or is this site. The dial rules the serving config's dials
/// follow apply too.
pub fn validate_provider(config: &ProviderConfig, binding: &Binding) -> Result<(), FilterError> {
    crate::descriptor::validate_site("provider.local_site", &config.local_site)?;
    for site in &config.admitted_sites {
        crate::descriptor::validate_site("provider.admitted_sites", site)?;
        if *site == config.local_site {
            return Err("grid: provider file admits this site itself".into());
        }
    }
    let mut seen = BTreeSet::new();
    for candidate in &config.candidates {
        if !valid_stable_id(&candidate.stable_id) {
            return Err(format!(
                "grid: provider candidate stable_id {:?} is not 8 lowercase hex",
                candidate.stable_id
            )
            .into());
        }
        if !seen.insert(candidate.stable_id.as_str()) {
            return Err(format!("grid: provider stable_id {} is repeated", candidate.stable_id).into());
        }
        if !config.dial.contains_key(&candidate.cluster) {
            return Err(format!("grid: provider candidate cluster '{}' has no dial", candidate.cluster).into());
        }
    }
    for (cluster, dial) in &config.dial {
        if dial.site != config.local_site {
            return Err(format!(
                "grid: provider dial '{cluster}' names site '{}', not this site",
                dial.site
            )
            .into());
        }
        if matches!(dial.transport, DialTransport::MutualTls { .. }) {
            return Err(format!("grid: provider dial '{cluster}' is mutual TLS, a hop to another grid gateway").into());
        }
        let grid_name = |name: &str| name.trim_end_matches('.').to_ascii_lowercase().ends_with(GRID_SUFFIX);
        if dial.transport.sni().is_some_and(grid_name) || grid_name(grid_client::address_host(&dial.address)) {
            return Err(format!("grid: provider dial '{cluster}' names a grid gateway").into());
        }
    }
    let dial = DialConfig {
        dial: config.dial.clone(),
        version: config.version,
    };
    validate_dials(&as_serving(config), &dial, binding)
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// What the filter reads: the admitted sites and each servable backend's built dial.
#[derive(Debug, Default)]
pub struct ProviderSnapshot {
    /// Admitted subject organizations.
    admitted: BTreeSet<String>,

    /// Built dials by stable ID.
    by_id: HashMap<String, Served>,
}

/// One servable backend: its cluster and built dial.
#[derive(Debug)]
struct Served {
    /// The cluster, for metrics.
    cluster: Arc<str>,

    /// The built dial.
    dialed: Arc<Dialed>,
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

/// Builds snapshots from provider files, keeping the last good one.
struct Builder {
    /// What the gateway fixed at startup.
    binding: Binding,

    /// Dial states, kept across rebuilds by cluster.
    health: DialHealth,

    /// The live snapshot.
    snapshot: Arc<ArcSwap<ProviderSnapshot>>,

    /// The last file applied or refused, and the CA digest it was built against.
    settled: Option<(Option<Vec<u8>>, [u8; 32])>,

    /// The last good config, whose CA files a rotation rebuilds against.
    applied: Option<ProviderConfig>,

    /// Files applied and refused.
    counts: Arc<ProviderCounts>,
}

impl Builder {
    /// Apply `bytes`, `None` for a missing file, when it or its CA files changed.
    fn tick(&mut self, bytes: Option<Vec<u8>>) {
        let digest = self.applied.as_ref().map_or([0; 32], ca_digest);
        if self
            .settled
            .as_ref()
            .is_some_and(|(held, held_digest)| *held == bytes && *held_digest == digest)
        {
            return;
        }
        match self.build(bytes.as_deref()) {
            Ok((snapshot, config)) => {
                tracing::info!(
                    admitted = snapshot.admitted.len(),
                    candidates = snapshot.by_id.len(),
                    "grid: provider file applied"
                );
                self.snapshot.store(Arc::new(snapshot));
                let rebuilt = config.as_ref().map_or([0; 32], ca_digest);
                self.applied = config;
                self.settled = Some((bytes, rebuilt));
                self.counts.applied.fetch_add(1, Ordering::SeqCst);
                metrics::counter!("grid_provider_config_reload_total", "result" => "applied").increment(1);
            },
            Err(error) => {
                tracing::warn!(%error, "grid: provider file refused; keeping the last good one");
                self.settled = Some((bytes, digest));
                self.counts.rejected.fetch_add(1, Ordering::SeqCst);
                metrics::counter!("grid_provider_config_reload_total", "result" => "rejected").increment(1);
            },
        }
    }

    /// The snapshot `bytes` describes; a missing file admits no one.
    fn build(&self, bytes: Option<&[u8]>) -> Result<(ProviderSnapshot, Option<ProviderConfig>), FilterError> {
        let Some(bytes) = bytes else {
            return Ok((ProviderSnapshot::default(), None));
        };
        let config: ProviderConfig = serde_yaml::from_slice(bytes)
            .map_err(|error| -> FilterError { format!("grid: parsing {PROVIDER_FILE}: {error}").into() })?;
        validate_provider(&config, &self.binding)?;
        let dial = DialConfig {
            dial: config.dial.clone(),
            version: config.version,
        };
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
        Ok((snapshot, Some(config)))
    }
}

/// Files the watch applied and refused.
#[derive(Debug, Default)]
pub struct ProviderCounts {
    /// Files applied, a missing one included.
    applied: AtomicUsize,

    /// Files refused.
    rejected: AtomicUsize,
}

impl ProviderCounts {
    /// Files applied so far.
    #[must_use]
    pub fn applied(&self) -> usize {
        self.applied.load(Ordering::SeqCst)
    }

    /// Files refused so far.
    #[must_use]
    pub fn rejected(&self) -> usize {
        self.rejected.load(Ordering::SeqCst)
    }
}

/// The running provider: its snapshot, kept current by a watch thread until dropped.
pub struct ProviderRuntime {
    /// The live snapshot.
    snapshot: Arc<ArcSwap<ProviderSnapshot>>,

    /// Files applied and refused.
    counts: Arc<ProviderCounts>,

    /// Dropping it stops the watch thread.
    _stop: mpsc::Sender<()>,
}

impl ProviderRuntime {
    /// The live snapshot the filter reads.
    #[must_use]
    pub fn snapshot(&self) -> Arc<ArcSwap<ProviderSnapshot>> {
        Arc::clone(&self.snapshot)
    }

    /// Files the watch applied and refused.
    #[must_use]
    pub fn counts(&self) -> &ProviderCounts {
        &self.counts
    }
}

/// Load the provider file at `path` now, then re-read it every `every`.
///
/// A missing file admits no one; a file that does not parse or validate is refused and the
/// last good one kept.
///
/// # Errors
///
/// Returns the error spawning the watch thread.
pub fn spawn_provider(path: PathBuf, binding: Binding, every: Duration) -> std::io::Result<ProviderRuntime> {
    praxis_tls::provider::install();
    let snapshot = Arc::new(ArcSwap::from_pointee(ProviderSnapshot::default()));
    let counts = Arc::new(ProviderCounts::default());
    let mut builder = Builder {
        binding,
        health: DialHealth::default(),
        snapshot: Arc::clone(&snapshot),
        settled: None,
        applied: None,
        counts: Arc::clone(&counts),
    };
    reread(&mut builder, &path);
    let (stop, stopped) = mpsc::channel::<()>();
    std::thread::Builder::new()
        .name("grid-provider-watch".to_owned())
        .spawn(move || {
            while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(every) {
                reread(&mut builder, &path);
            }
        })?;
    Ok(ProviderRuntime {
        snapshot,
        counts,
        _stop: stop,
    })
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
    /// The longest the dial's connect attempts can take.
    budget: Duration,
    /// The dial's passive health.
    dialed: Arc<DialState>,
    /// When the request was routed.
    sent: std::time::Instant,
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
        let mut named = ctx.request.headers.get_all(SELECTED_CANDIDATE_HEADER).iter();
        let dialed = match (named.next(), named.next()) {
            (Some(value), None) => value
                .to_str()
                .ok()
                .filter(|id| valid_stable_id(id))
                .and_then(|id| snapshot.by_id.get(id)),
            _ => None,
        };
        let Some(Served { cluster, dialed }) = dialed else {
            return Ok(refuse("candidate", 503));
        };
        strip_caller(ctx);
        ctx.cluster = Some(Arc::clone(cluster));
        ctx.upstream = Some(dialed.upstream().clone());
        ctx.retry_policy = Some(Arc::clone(dialed.retry()));
        ctx.insert_filter_state(ProviderState {
            budget: dialed.connect_budget(),
            dialed: Arc::clone(dialed.health()),
            sent: std::time::Instant::now(),
        });
        metrics::counter!("grid_provider_requests_total", "result" => "served").increment(1);
        Ok(FilterAction::Continue)
    }

    async fn on_response(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        let (answered, reached) = (ctx.response_header.is_some(), ctx.upstream_reached);
        if let Some(state) = ctx.get_filter_state::<ProviderState>()
            && let Some(working) = dial_outcome(answered, reached, state.sent.elapsed(), state.budget)
        {
            state.dialed.record(working, crate::health::monotonic_ms());
        }
        Ok(FilterAction::Continue)
    }
}

// ---------------------------------------------------------------------------
// Utility Functions
// ---------------------------------------------------------------------------

/// Eight lowercase hex digits.
fn valid_stable_id(id: &str) -> bool {
    id.len() == STABLE_ID_LEN && id.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

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

/// A change detector over the CA files `config`'s dials read. Not a security function.
fn ca_digest(config: &ProviderConfig) -> [u8; 32] {
    let mut material = Vec::new();
    for path in config.dial.values().filter_map(|dial| dial.transport.ca_path()) {
        material.extend_from_slice(path.as_bytes());
        match std::fs::read(path) {
            Ok(content) => material.extend_from_slice(&certs::sha256(&content)),
            Err(_) => material.push(0),
        }
    }
    certs::sha256(&material)
}

/// The file's bytes, `None` when it does not exist.
fn read(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Apply the file at `path` through `builder`, keeping the last good one when it cannot be read.
fn reread(builder: &mut Builder, path: &Path) {
    match read(path) {
        Ok(bytes) => builder.tick(bytes),
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "grid: provider file unreadable; keeping the last good one");
        },
    }
}

/// Drop what the peer sent that must not reach a backend: its credentials and grid hop headers.
fn strip_caller(ctx: &mut HttpFilterContext<'_>) {
    ctx.request_headers_to_remove.extend(CALLER_CREDENTIAL_HEADERS);
    for name in [SELECTED_CANDIDATE_HEADER, OVERLAY_REVISION_HEADER] {
        ctx.request_headers_to_remove
            .push(http::header::HeaderName::from_static(name));
    }
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
        validate_provider(&config, &Binding::default()).expect("the golden validates");
        let builder = builder(InsecureOptions::default());
        let (snapshot, _) = builder.build(Some(GOLDEN.as_bytes())).expect("the golden builds");
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
        let base = &Binding::default();
        let refusals: [(&str, fn(&mut ProviderConfig)); 12] = [
            ("a dial to another site", |config| {
                dial_mut(config).site = "site-b".to_owned();
            }),
            ("a mutual TLS dial", |config| {
                dial_mut(config).transport = DialTransport::MutualTls {
                    sni: "site-a.grid.internal".to_owned(),
                };
            }),
            ("a TLS dial verifying a grid name", |config| {
                dial_mut(config).transport = DialTransport::Tls {
                    ca_path: None,
                    sni: "site-b.grid.internal.".to_owned(),
                };
            }),
            ("an address naming a grid gateway", |config| {
                dial_mut(config).address = "site-b.GRID.internal:8443".to_owned();
            }),
            ("a CA outside the backend CA mount", |config| {
                dial_mut(config).transport = DialTransport::Tls {
                    ca_path: Some("/etc/praxis/tls/ca.crt".to_owned()),
                    sni: "llama.models.svc".to_owned(),
                };
            }),
            ("a candidate with no dial", |config| {
                config.dial.clear();
            }),
            ("a repeated stable id", |config| {
                let twin = config.candidates[0].clone();
                config.candidates.push(twin);
            }),
            ("a malformed stable id", |config| {
                config.candidates[0].stable_id = "CCAC628C".to_owned();
            }),
            ("this site admitted", |config| {
                config.admitted_sites.push("site-a".to_owned());
            }),
            ("an admitted name that is no site", |config| {
                config.admitted_sites.push("Site_B".to_owned());
            }),
            ("a bad local site", |config| {
                config.local_site = "-".to_owned();
            }),
            ("a remote candidate's dial", |config| {
                dial_mut(config).site = "site-c".to_owned();
                config.local_site = "site-c".to_owned();
                config.admitted_sites.retain(|site| site != "site-c");
                config.dial.insert(
                    "remote".to_owned(),
                    serde_yaml::from_str("{address: 'x:1', site: site-a, transport: {mode: plaintext}}").expect("dial"),
                );
            }),
        ];
        validate_provider(&minimal(), base).expect("the fixture is valid");
        for (what, break_it) in refusals {
            let mut config = minimal();
            break_it(&mut config);
            assert!(validate_provider(&config, base).is_err(), "{what} must be refused");
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
        assert!(!builder.snapshot.load().admits("site-b"), "no file admits no one");
        let good = text(&minimal());
        builder.tick(Some(good.clone().into_bytes()));
        assert!(builder.snapshot.load().admits("site-b"), "a good file admits its sites");
        assert!(builder.snapshot.load().serves("ccac628c"), "and serves its backend");
        for bad in [
            "{".to_owned(),
            good.replace("\"v1\"", "\"v2\""),
            text(&ProviderConfig {
                admitted_sites: vec!["site-a".to_owned()],
                ..minimal()
            }),
        ] {
            builder.tick(Some(bad.into_bytes()));
            assert!(
                builder.snapshot.load().admits("site-b"),
                "a refused file keeps the last good"
            );
        }
        assert_eq!(builder.counts.rejected(), 3, "each refusal counted");
        let removed = text(&ProviderConfig {
            admitted_sites: vec!["site-c".to_owned()],
            ..minimal()
        });
        builder.tick(Some(removed.into_bytes()));
        assert!(!builder.snapshot.load().admits("site-b"), "a removed site is refused");
        assert!(builder.snapshot.load().admits("site-c"), "the remaining one admitted");
        builder.tick(None);
        assert!(
            !builder.snapshot.load().admits("site-c"),
            "a file that disappears admits no one"
        );
    }

    #[tokio::test]
    async fn only_an_admitted_peer_naming_a_served_backend_reaches_it() -> Result<(), FilterError> {
        let filter = filter_over(&minimal());
        let cases: [(&str, Option<&str>, http::Method, &str, Option<&str>, u16); 9] = [
            (
                "no identity",
                None,
                http::Method::POST,
                "/v1/chat/completions",
                Some("ccac628c"),
                403,
            ),
            (
                "unlisted",
                Some("site-z"),
                http::Method::POST,
                "/v1/chat/completions",
                Some("ccac628c"),
                403,
            ),
            (
                "this site",
                Some("site-a"),
                http::Method::POST,
                "/v1/chat/completions",
                Some("ccac628c"),
                403,
            ),
            (
                "method",
                Some("site-b"),
                http::Method::DELETE,
                "/v1/chat/completions",
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
            (
                "no candidate",
                Some("site-b"),
                http::Method::POST,
                "/v1/chat/completions",
                None,
                503,
            ),
            (
                "malformed",
                Some("site-b"),
                http::Method::POST,
                "/v1/chat/completions",
                Some("CCAC628C"),
                503,
            ),
            (
                "not served",
                Some("site-b"),
                http::Method::POST,
                "/v1/chat/completions",
                Some("67b6bb1e"),
                503,
            ),
            (
                "served",
                Some("site-b"),
                http::Method::POST,
                "/v1/chat/completions",
                Some("ccac628c"),
                0,
            ),
        ];
        for (what, organization, method, path, candidate, status) in cases {
            let mut request = request(method, path);
            if let Some(candidate) = candidate {
                request
                    .headers
                    .insert(SELECTED_CANDIDATE_HEADER, http::HeaderValue::from_static(candidate));
            }
            let mut ctx = context(&request);
            ctx.peer_identity = organization.map(peer);
            let action = filter.on_request(&mut ctx).await?;
            match action {
                FilterAction::TerminalResponse(response) => {
                    assert_eq!(response.status, status, "{what}");
                    assert!(ctx.upstream.is_none(), "{what}: a refused request has no upstream");
                    if status == 503 {
                        assert!(
                            response.headers.contains_key(http::header::RETRY_AFTER),
                            "{what}: a 503 says when to retry"
                        );
                    }
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
        let mut twice = request(http::Method::POST, "/v1/chat/completions");
        twice
            .headers
            .append(SELECTED_CANDIDATE_HEADER, http::HeaderValue::from_static("ccac628c"));
        twice
            .headers
            .append(SELECTED_CANDIDATE_HEADER, http::HeaderValue::from_static("ccac628c"));
        let mut ctx = context(&twice);
        ctx.peer_identity = Some(peer("site-b"));
        assert!(
            matches!(filter.on_request(&mut ctx).await?, FilterAction::TerminalResponse(response) if response.status == 503),
            "two candidate headers name no one backend"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_served_request_carries_no_caller_credential_or_hop_header() -> Result<(), FilterError> {
        let filter = filter_over(&minimal());
        let mut request = request(
            http::Method::POST,
            "/v1/chat/completions?api_key=s3cret&stream=true&token=t",
        );
        request
            .headers
            .insert(SELECTED_CANDIDATE_HEADER, http::HeaderValue::from_static("ccac628c"));
        let mut ctx = context(&request);
        ctx.peer_identity = Some(peer("site-b"));
        assert!(
            matches!(filter.on_request(&mut ctx).await?, FilterAction::Continue),
            "served"
        );
        for name in CALLER_CREDENTIAL_HEADERS
            .iter()
            .map(http::header::HeaderName::as_str)
            .chain([SELECTED_CANDIDATE_HEADER, OVERLAY_REVISION_HEADER])
        {
            assert!(
                ctx.request_headers_to_remove
                    .iter()
                    .any(|removed| removed.as_str() == name),
                "{name} is stripped"
            );
        }
        assert!(
            !ctx.request_headers_to_remove
                .iter()
                .any(|removed| removed.as_str() == "x-ai-routing-request-id"),
            "the request id is kept for correlation"
        );
        assert_eq!(
            ctx.rewritten_path.as_deref(),
            Some("/v1/chat/completions?stream=true"),
            "credential query parameters are stripped"
        );
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
        Builder {
            binding: Binding {
                insecure,
                ..Binding::default()
            },
            health: DialHealth::default(),
            snapshot: Arc::new(ArcSwap::from_pointee(ProviderSnapshot::default())),
            settled: None,
            applied: None,
            counts: Arc::new(ProviderCounts::default()),
        }
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
