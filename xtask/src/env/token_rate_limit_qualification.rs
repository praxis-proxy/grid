//! Evidence-backed qualification for the distributed token-rate-limit topology.
//!
//! This is the first-class replacement for the removed `validate.sh`. It deploys
//! the three-cluster (west/central/east, presented as New York/London/Tokyo)
//! quota topology in explicit dependency order. Forge does not infer
//! cross-cluster capture ordering, so the runner then proves the quota and routing contract
//! with typed evidence. Requests are issued from ephemeral in-cluster curl pods
//! rather than host port-forwards, which keeps cleanup free of host child
//! processes. No secrets, authorization values, Valkey passwords, or kubeconfig
//! contents are ever written to evidence.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

/// Default Forge topology used when no `--forge-config` is supplied.
const TOPOLOGY: &str = "tests/e2e/topologies/grid-token-rate-limit/forge.yaml";
/// Cluster (site) names in deployment order.
const CLUSTERS: [&str; 3] = ["west", "central", "east"];
/// Certificate identities expected by the provider-boundary stack.
const CERTIFICATE_IDENTITIES: [&str; 3] = ["provider-a", "provider-b", "provider-c"];
/// Kubernetes namespace hosting every workload.
const NAMESPACE: &str = "grid-system";
/// KIND cluster name prefix; the context is `kind-<prefix>-<site>`.
const LOGICAL_NETWORK: &str = "grid-token-rate-limit";
/// Prefix for physical Forge and Kind resources.
const PHYSICAL_PREFIX: &str = "grid-token-rate-limit";
/// Maximum explicit run suffix length.
const MAX_RUN_ID_LEN: usize = 24;
/// Maximum generated suffix attempts before failing safely.
const GENERATED_RUN_ID_ATTEMPTS: usize = 8;
/// West consumer gateway A release name.
const CONSUMER_A: &str = "consumer-gateway-a";
/// West consumer gateway B release name.
const CONSUMER_B: &str = "consumer-gateway-b";
/// West consumer gateway releases that share one Alice quota.
const CONSUMERS: [&str; 2] = [CONSUMER_A, CONSUMER_B];
/// Data-plane listener port on each consumer gateway.
const CONSUMER_PORT: &str = "8080";
/// Response header the provider gateway sets for regional attribution.
const PROVIDER_SITE_HEADER: &str = "x-grid-provider-site";
/// Valkey key namespace for the shared sliding-window quota.
const QUOTA_KEY_PREFIX: &str = "praxis:grid-token-rate-limit";
/// Valkey deployment name (west only).
const VALKEY_DEPLOY: &str = "valkey";
/// Pinned curl image for in-cluster probes.
const CURL_IMAGE: &str = "curlimages/curl:8.12.1";
/// Rule capacity in tokens for the shared Alice budget.
const CAPACITY_TOKENS: u32 = 60;
/// Reserved tokens per admitted request.
const RESERVED_TOKENS: u32 = 15;
/// Sliding-window length in seconds.
const WINDOW_SECS: u64 = 60;
/// Additional attempts allowed only when a probe receives no HTTP response.
const TRANSPORT_RETRIES: u32 = 3;
/// Cargo features that must be compiled into the Praxis AI qualification image.
const REQUIRED_GATEWAY_FEATURES: &str = "token-rate-limit-filter,praxis-filter/basic-auth-filter";
/// OCI label used to make the gateway image's feature contract inspectable.
const GATEWAY_FEATURES_LABEL: &str = "org.praxis-proxy.ai.features";
/// Alice principal username.
const ALICE_USER: &str = "alice";
/// Alice principal password used only by this qualification topology.
const ALICE_PASS: &str = "alice-secret";
/// Inference request body; `max_tokens` is small to keep runs fast.
const REQUEST_BODY: &str =
    r#"{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"hello"}],"max_tokens":1}"#;
/// Inference route on the consumer data plane.
const INFERENCE_PATH: &str = "/v1/chat/completions";
/// Monotonic suffix source for unique probe pod names.
static PROBE_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

/// CLI options for `cargo xtask env run-grid-token-rate-limit-qualification`.
#[derive(Debug, clap::Args)]
pub(crate) struct Options {
    /// Source Forge configuration to materialize and deploy.
    #[arg(long, default_value = TOPOLOGY)]
    pub(crate) forge_config: PathBuf,
    /// Evidence output directory. Defaults beside the Forge config.
    #[arg(long)]
    pub(crate) evidence_dir: Option<PathBuf>,
    /// Image tag to materialize and load (e.g. `quota-qualification-20260902T023708Z`).
    #[arg(long)]
    pub(crate) image_tag: Option<String>,
    /// DNS-safe physical resource suffix for deterministic CI/reproduction runs.
    #[arg(long)]
    pub(crate) run_id: Option<String>,
    /// Keep Kind resources after completion for debugging.
    #[arg(long)]
    pub(crate) keep: bool,
}

/// A single credential shape exercised by the qualification.
#[derive(Clone, Copy, Debug)]
enum Credential {
    /// Correct Alice basic-auth credential.
    Valid,
    /// Correct user, wrong password.
    WrongPassword,
    /// No `Authorization` header at all.
    Missing,
    /// Syntactically invalid `Authorization` header.
    Malformed,
}

/// Classification of a data-plane response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// Request admitted (HTTP 200).
    Admitted,
    /// Quota denied (HTTP 429).
    QuotaDenied,
    /// Backend unavailable / fail-closed (HTTP 503).
    FailClosed,
    /// Authentication rejected (HTTP 401).
    Unauthorized,
    /// Any other status or transport failure.
    Other,
}

/// One recorded HTTP probe result.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct HttpResult {
    /// Human label for the probe.
    pub(crate) label: String,
    /// HTTP status code (`0` when no response was received).
    pub(crate) status: u16,
    /// Regional attribution from the provider-site header, if present.
    pub(crate) provider_site: Option<String>,
}

/// One overlay routing candidate as published by the operator.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct OverlayCandidate {
    /// Provider site of the candidate.
    pub(crate) site: String,
    /// Stable candidate identity.
    pub(crate) stable_id: String,
    /// Zero-based selection group.
    pub(crate) selection_group: u32,
}

/// A parsed, validated routing overlay for one consumer gateway.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct OverlayView {
    /// Content-addressed overlay revision.
    pub(crate) revision: String,
    /// Selection policy mode (`roundRobin` for this topology).
    pub(crate) selection_policy: String,
    /// Routing candidates.
    pub(crate) candidates: Vec<OverlayCandidate>,
}

/// Result of one qualification scenario.
#[derive(Debug, Serialize)]
struct ScenarioResult {
    /// Scenario name.
    name: String,
    /// Whether the scenario proved its property.
    passed: bool,
    /// Human-readable detail.
    detail: String,
    /// Structured observed facts.
    facts: BTreeMap<String, serde_json::Value>,
}

/// Machine-readable evidence document.
#[derive(Debug, Serialize)]
struct Evidence {
    /// Evidence schema version.
    schema_version: &'static str,
    /// Topology identifier.
    topology: &'static str,
    /// Run-scoped physical identity.
    run_id: String,
    /// Derived physical resource names and ownership facts.
    run_identity: RunIdentity,
    /// Resources observed and created by this invocation.
    ownership: OwnershipPlan,
    /// Image tag deployed.
    image_tag: String,
    /// Cargo feature contract required of the Praxis AI gateway image.
    required_gateway_features: &'static str,
    /// Resolved Forge config path used.
    resolved_config: String,
    /// Accepted overlays keyed by consumer gateway.
    overlays: BTreeMap<String, OverlayView>,
    /// All recorded HTTP probes.
    http_results: Vec<HttpResult>,
    /// Regional distribution of admitted requests.
    distribution: BTreeMap<String, u32>,
    /// Scenario outcomes.
    scenarios: Vec<ScenarioResult>,
    /// Seconds spent naturally aging authenticated warmup reservations before
    /// scenario execution. This is deliberately time-based and does not
    /// mutate Valkey state.
    warmup_window_reset_seconds: u64,
    /// Whether cluster teardown ran and succeeded.
    cleanup_succeeded: Option<bool>,
}

/// Physical names for one qualification invocation. Logical Grid identities
/// intentionally remain the checked-in topology names.
#[derive(Clone, Debug, Serialize)]
struct RunIdentity {
    /// Validated run suffix.
    run_id: String,
    /// Forge environment metadata name.
    forge_name: String,
    /// Kind cluster prefix.
    cluster_prefix: String,
    /// Docker network name.
    network: String,
    /// Physical Kind names keyed by logical site.
    kind_clusters: BTreeMap<String, String>,
    /// kubectl contexts keyed by logical site.
    kubectl_contexts: BTreeMap<String, String>,
}

impl RunIdentity {
    /// Derive all physical names from one validated run suffix.
    fn new(run_id: &str) -> Self {
        let forge_name = format!("{PHYSICAL_PREFIX}-{run_id}");
        let cluster_prefix = forge_name.clone();
        let network = format!("{forge_name}-net");
        let kind_clusters = CLUSTERS
            .iter()
            .map(|site| ((*site).to_owned(), format!("{cluster_prefix}-{site}")))
            .collect();
        let kubectl_contexts = CLUSTERS
            .iter()
            .map(|site| ((*site).to_owned(), format!("kind-{cluster_prefix}-{site}")))
            .collect();
        Self {
            run_id: run_id.to_owned(),
            forge_name,
            cluster_prefix,
            network,
            kind_clusters,
            kubectl_contexts,
        }
    }

    /// Return the physical Kind cluster name for one logical site.
    fn kind_cluster(&self, site: &str) -> &str {
        self.kind_clusters.get(site).map_or("", String::as_str)
    }

    /// Return the kubectl context for one logical site.
    fn context(&self, site: &str) -> &str {
        self.kubectl_contexts.get(site).map_or("", String::as_str)
    }
}

/// Resources observed before this run and resources successfully created by it.
#[derive(Clone, Debug, Serialize)]
struct OwnershipPlan {
    /// Exact physical clusters found before this run.
    preexisting_clusters: Vec<String>,
    /// Whether the run network existed before this run.
    preexisting_network: bool,
    /// Physical clusters successfully created by this run.
    owned_clusters: Vec<String>,
    /// Whether the run network was successfully created by this run.
    owned_network: bool,
}

/// Accepted overlays keyed by consumer gateway release name.
type Overlays = BTreeMap<String, OverlayView>;

/// Result of exhausting the budget: (admitted provider sites, quota-denial seen).
type ExhaustOutcome = (Vec<String>, bool);

/// Collected qualification artifacts assembled before writing evidence.
struct Collected {
    /// Accepted overlays per consumer gateway.
    overlays: Overlays,
    /// All recorded HTTP probes.
    results: Vec<HttpResult>,
    /// Regional distribution of admitted requests.
    distribution: BTreeMap<String, u32>,
    /// Scenario outcomes.
    scenarios: Vec<ScenarioResult>,
    /// Seconds spent naturally aging authenticated warmup reservations.
    warmup_window_reset_seconds: u64,
}

/// Results and ledger observations from one synchronized burst.
struct ConcurrentEvidence {
    /// Number of successful inference responses.
    admitted: usize,
    /// Highest simultaneous reservation count observed.
    max_active: u64,
    /// Highest simultaneous reserved-token total observed.
    max_active_tokens: u64,
    /// Every response from the burst.
    results: Vec<HttpResult>,
}

/// Quota observations collected around a consumer restart.
struct RestartEvidence {
    /// Shared quota entries before restarting consumer A.
    before: u64,
    /// Shared quota entries after consumer A becomes ready again.
    after: u64,
    /// Consumer B's quota outcome before consumer A restarts.
    peer_before: Outcome,
    /// Consumer B's quota outcome after consumer A restarts.
    peer_after: Outcome,
    /// HTTP status returned by the restarted consumer A.
    restarted_status: u16,
    /// Classified quota outcome returned by the restarted consumer A.
    restarted_outcome: Outcome,
}

impl RestartEvidence {
    /// Whether both replicas continued to observe the exhausted shared window.
    fn passed(&self) -> bool {
        self.before > 0
            && self.after >= self.before
            && self.peer_before == Outcome::QuotaDenied
            && self.peer_after == Outcome::QuotaDenied
            && self.restarted_outcome == Outcome::QuotaDenied
    }

    /// Convert the observations into structured scenario facts.
    fn facts(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([
            ("quota_entries_before".to_owned(), serde_json::json!(self.before)),
            ("quota_entries_after".to_owned(), serde_json::json!(self.after)),
            (
                "peer_before_restart_outcome".to_owned(),
                serde_json::json!(format!("{:?}", self.peer_before)),
            ),
            (
                "peer_after_restart_outcome".to_owned(),
                serde_json::json!(format!("{:?}", self.peer_after)),
            ),
            (
                "restarted_consumer_status".to_owned(),
                serde_json::json!(self.restarted_status),
            ),
            (
                "post_restart_outcome".to_owned(),
                serde_json::json!(format!("{:?}", self.restarted_outcome)),
            ),
        ])
    }
}

/// Shared session context passed to orchestration helpers.
struct Session {
    /// Path to the `praxis-forge` binary.
    forge: PathBuf,
    /// Resolved (materialized) Forge config.
    config: PathBuf,
    /// Run-scoped Forge state directory.
    state_dir: PathBuf,
    /// Evidence output directory.
    evidence: PathBuf,
    /// Image tag deployed.
    image_tag: String,
    /// When false, clusters are torn down on drop/exit.
    keep: bool,
    /// Centralized physical names for this invocation.
    names: RunIdentity,
    /// Ownership ledger used for evidence.
    ownership: std::cell::RefCell<OwnershipPlan>,
}

/// The qualification is a single CLI invocation, so its physical naming
/// context can be installed once for the existing helper graph.
static RUN_IDENTITY: std::sync::OnceLock<RunIdentity> = std::sync::OnceLock::new();

/// Best-effort cleanup on early return. Drop does NOT run on SIGKILL or the
/// default SIGINT handler; teardown on those paths is not guaranteed.
struct CleanupGuard<'guard> {
    /// Borrowed session used to invoke `forge down`.
    session: &'guard Session,
    /// When false, drop tears down clusters.
    disarmed: bool,
}

impl Drop for CleanupGuard<'_> {
    fn drop(&mut self) {
        if !self.disarmed && !self.session.keep {
            drop(teardown(self.session));
        }
    }
}

/// Kubernetes context for a cluster.
fn context_for(cluster: &str) -> String {
    RUN_IDENTITY.get().map_or_else(
        || format!("kind-{PHYSICAL_PREFIX}-{cluster}"),
        |names| names.context(cluster).to_owned(),
    )
}

/// Run a command under a hard `timeout`, capturing output and checking status.
fn run_timed(program: &str, args: &[&str], secs: u64) -> Result<Output, Box<dyn std::error::Error>> {
    let output = spawn_timed(program, args, secs)?;
    if output.status.success() {
        return Ok(output);
    }
    Err(format!(
        "{program} {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )
    .into())
}

/// Run a command under a hard `timeout` without asserting success.
fn spawn_timed(program: &str, args: &[&str], secs: u64) -> Result<Output, Box<dyn std::error::Error>> {
    let deadline = format!("{}s", secs.max(1));
    let output = Command::new("timeout")
        .args(["--signal=TERM", "--kill-after=10s", &deadline, program])
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;
    Ok(output)
}

/// Run `kubectl` against a cluster's namespace, checking success.
fn kubectl(cluster: &str, args: &[&str], secs: u64) -> Result<Output, Box<dyn std::error::Error>> {
    let context = context_for(cluster);
    let mut full = vec!["--context", &context, "-n", NAMESPACE];
    full.extend_from_slice(args);
    run_timed("kubectl", &full, secs)
}

/// Run `kubectl` while preserving non-zero command output for assertions that
/// distinguish a protocol response from a network timeout.
fn kubectl_unchecked(cluster: &str, args: &[&str], secs: u64) -> Result<Output, Box<dyn std::error::Error>> {
    let context = context_for(cluster);
    let mut full = vec!["--context", &context, "-n", NAMESPACE];
    full.extend_from_slice(args);
    spawn_timed("kubectl", &full, secs)
}

/// Verify the required external tools exist.
fn require_tools() -> Result<(), Box<dyn std::error::Error>> {
    for tool in ["kubectl", "kind", "timeout", "docker"] {
        let found = Command::new("sh")
            .args(["-c", &format!("command -v {tool}")])
            .status()?;
        if !found.success() {
            return Err(format!("required tool is unavailable: {tool}").into());
        }
    }
    Ok(())
}

/// Verify that the local AI image declares the feature set required by this
/// topology. Gateway startup still validates the actual filter registration;
/// this label provides an actionable failure before clusters are created.
fn verify_gateway_image_features(tag: &str) -> Result<(), Box<dyn std::error::Error>> {
    let image = format!("praxis-ai:{tag}");
    let format = format!("{{{{ index .Config.Labels \"{GATEWAY_FEATURES_LABEL}\" }}}}");
    let output = run_timed("docker", &["image", "inspect", "--format", &format, &image], 30)?;
    let declared = String::from_utf8(output.stdout)?.trim().to_owned();
    if declares_required_gateway_features(&declared) {
        return Ok(());
    }
    Err(format!(
        "{image} must declare OCI label {GATEWAY_FEATURES_LABEL}={REQUIRED_GATEWAY_FEATURES}; found {declared:?}. Build the AI image with the required Cargo features before running qualification"
    )
    .into())
}

/// Whether a comma-separated image label includes every required feature.
fn declares_required_gateway_features(declared: &str) -> bool {
    REQUIRED_GATEWAY_FEATURES
        .split(',')
        .all(|required| declared.split(',').map(str::trim).any(|feature| feature == required))
}

/// Resolve the evidence directory, defaulting beside the Forge config.
fn resolve_evidence_dir(options: &Options) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(path) = &options.evidence_dir {
        return Ok(path.clone());
    }
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let parent = options.forge_config.parent().unwrap_or_else(|| Path::new("."));
    Ok(parent.join(format!("evidence-token-rate-limit-{stamp}")))
}

/// Validate an explicitly supplied physical run suffix.
fn validate_run_id(run_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    if run_id.is_empty() || run_id.len() > MAX_RUN_ID_LEN {
        return Err(format!("--run-id must contain 1..{MAX_RUN_ID_LEN} ASCII characters").into());
    }
    if !run_id
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || !run_id.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        || !run_id.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
    {
        return Err(
            "--run-id must be lowercase ASCII DNS-label text, beginning and ending with a letter or digit".into(),
        );
    }
    Ok(())
}

/// Generate a collision-resistant short suffix for local execution.
fn generated_run_id(attempt: usize) -> Result<String, Box<dyn std::error::Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let pid = std::process::id();
    let id = format!("q{nanos:x}{pid:x}{attempt:x}");
    let id = id.chars().take(MAX_RUN_ID_LEN).collect::<String>();
    validate_run_id(&id)?;
    Ok(id)
}

/// Return whether the exact physical resources already exist.
#[expect(
    clippy::too_many_lines,
    reason = "Collision preflight keeps cluster and network checks together for one atomic decision."
)]
fn physical_resources_exist(names: &RunIdentity) -> Result<bool, Box<dyn std::error::Error>> {
    let clusters = spawn_timed("kind", &["get", "clusters"], 30)?;
    if !clusters.status.success() {
        return Err(format!(
            "kind get clusters failed during collision preflight: {}",
            String::from_utf8_lossy(&clusters.stderr).trim()
        )
        .into());
    }
    let existing = String::from_utf8_lossy(&clusters.stdout);
    if names
        .kind_clusters
        .values()
        .any(|cluster| existing.lines().any(|line| line.trim() == cluster))
    {
        return Ok(true);
    }
    let network = spawn_timed("docker", &["network", "inspect", &names.network], 30)?;
    if network.status.success() {
        return Ok(true);
    }
    if !String::from_utf8_lossy(&network.stderr)
        .to_ascii_lowercase()
        .contains("no such network")
        && !String::from_utf8_lossy(&network.stderr)
            .to_ascii_lowercase()
            .contains("not found")
    {
        return Err(format!(
            "docker network collision preflight failed for {}: {}",
            names.network,
            String::from_utf8_lossy(&network.stderr).trim()
        )
        .into());
    }
    Ok(false)
}

/// Select a validated run identity and reject explicit collisions.
fn select_run_identity(options: &Options) -> Result<RunIdentity, Box<dyn std::error::Error>> {
    if let Some(run_id) = options.run_id.as_deref() {
        validate_run_id(run_id)?;
        let names = RunIdentity::new(run_id);
        if physical_resources_exist(&names)? {
            return Err(format!("explicit --run-id {run_id:?} collides with an existing cluster or network").into());
        }
        return Ok(names);
    }
    for attempt in 0..GENERATED_RUN_ID_ATTEMPTS {
        let names = RunIdentity::new(&generated_run_id(attempt)?);
        if !physical_resources_exist(&names)? {
            return Ok(names);
        }
    }
    Err(format!("could not find a free generated run identity after {GENERATED_RUN_ID_ATTEMPTS} attempts").into())
}

/// Poll `condition` until it yields a value or the timeout elapses.
fn poll_until<F, T>(timeout: Duration, interval: Duration, condition: F) -> Result<T, Box<dyn std::error::Error>>
where
    F: Fn() -> Result<Option<T>, Box<dyn std::error::Error>>,
{
    let start = Instant::now();
    loop {
        if let Some(value) = condition()? {
            return Ok(value);
        }
        if start.elapsed() >= timeout {
            return Err("timeout waiting for condition".into());
        }
        std::thread::park_timeout(interval);
    }
}

/// curl arguments applying the given credential shape (curl encodes basic-auth).
fn credential_args(credential: Credential) -> Vec<String> {
    match credential {
        Credential::Valid => vec!["-u".to_owned(), format!("{ALICE_USER}:{ALICE_PASS}")],
        Credential::WrongPassword => vec!["-u".to_owned(), format!("{ALICE_USER}:wrong")],
        Credential::Missing => Vec::new(),
        Credential::Malformed => vec!["-H".to_owned(), "Authorization: Basic not-valid-base64".to_owned()],
    }
}

/// Classify a status code into a quota/auth outcome.
fn classify(status: u16) -> Outcome {
    match status {
        200 => Outcome::Admitted,
        429 => Outcome::QuotaDenied,
        503 => Outcome::FailClosed,
        401 => Outcome::Unauthorized,
        _ => Outcome::Other,
    }
}

/// Parse curl header-dump (`-D -`) output into status and provider site.
///
/// The status is read from the last `HTTP/<v> <code>` status line, never from a
/// `-w` trailer: `kubectl run --rm` appends a `pod "..." deleted` message with no
/// separating newline, which would corrupt a trailing status code.
fn parse_probe_output(raw: &str) -> (u16, Option<String>) {
    let status = raw
        .lines()
        .rfind(|line| line.starts_with("HTTP/"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    let site = raw.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.trim()
            .eq_ignore_ascii_case(PROVIDER_SITE_HEADER)
            .then(|| value.trim().to_owned())
    });
    (status, site)
}

/// Build curl arguments for a data-plane probe.
fn curl_args(url: &str, credential: Credential, body: &str) -> Vec<String> {
    let mut args = vec![
        "curl".to_owned(),
        "-sS".to_owned(),
        "--max-time".to_owned(),
        "20".to_owned(),
        "-o".to_owned(),
        "/dev/null".to_owned(),
        "-D".to_owned(),
        "-".to_owned(),
        "-H".to_owned(),
        "Content-Type: application/json".to_owned(),
    ];
    args.extend(credential_args(credential));
    args.extend(["--data".to_owned(), body.to_owned(), url.to_owned()]);
    args
}

/// Pod-security overrides for an ephemeral curl pod in a restricted namespace.
fn curl_pod_overrides(pod: &str, args: &[String]) -> String {
    let tail: Vec<&str> = args.iter().skip(1).map(String::as_str).collect();
    serde_json::json!({
        "spec": {
            "automountServiceAccountToken": false,
            "securityContext": { "runAsNonRoot": true, "seccompProfile": { "type": "RuntimeDefault" } },
            "containers": [{
                "name": pod, "image": CURL_IMAGE, "command": ["curl"], "args": tail,
                "securityContext": {
                    "runAsUser": 1000, "allowPrivilegeEscalation": false,
                    "readOnlyRootFilesystem": true, "capabilities": { "drop": ["ALL"] }
                }
            }]
        }
    })
    .to_string()
}

/// Run one ephemeral curl pod and return its captured output.
fn run_curl_pod(cluster: &str, label: &str, args: &[String]) -> Result<Output, Box<dyn std::error::Error>> {
    let sequence = PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let prefix = label.get(..label.len().min(36)).unwrap_or(label);
    let pod = format!("{prefix}-{sequence}");
    let overrides = curl_pod_overrides(&pod, args);
    let context = context_for(cluster);
    let run_args = [
        "run",
        &pod,
        &format!("--image={CURL_IMAGE}"),
        "--context",
        &context,
        "-n",
        NAMESPACE,
        "--rm",
        "-i",
        "--restart=Never",
        "--pod-running-timeout=60s",
        "--overrides",
        &overrides,
    ];
    spawn_timed("kubectl", &run_args, 90)
}

/// Alternate consumer gateway by index without slice indexing.
fn consumer_for(index: usize) -> &'static str {
    if index.is_multiple_of(2) {
        CONSUMER_A
    } else {
        CONSUMER_B
    }
}

/// Consumer service URL for the inference route.
fn consumer_url(gateway: &str) -> String {
    format!("http://{gateway}.{NAMESPACE}.svc.cluster.local:{CONSUMER_PORT}{INFERENCE_PATH}")
}

/// Issue one inference probe against a consumer gateway and record it.
fn probe(label: &str, gateway: &str, credential: Credential) -> Result<HttpResult, Box<dyn std::error::Error>> {
    let url = consumer_url(gateway);
    let args = curl_args(&url, credential, REQUEST_BODY);
    let output = run_curl_pod("west", label, &args)?;
    let (status, provider_site) = parse_probe_output(&String::from_utf8_lossy(&output.stdout));
    Ok(HttpResult {
        label: label.to_owned(),
        status,
        provider_site,
    })
}

/// Restricted long-lived curl pod used to remove pod scheduling from the
/// concurrency measurement.
#[expect(
    clippy::too_many_lines,
    reason = "pod creation, restricted security, and readiness form one lifecycle"
)]
fn create_concurrency_client(index: usize) -> Result<String, Box<dyn std::error::Error>> {
    let sequence = PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let pod = format!("quota-concurrent-{index}-{sequence}");
    let overrides = serde_json::json!({
        "spec": {
            "automountServiceAccountToken": false,
            "securityContext": {
                "runAsNonRoot": true,
                "runAsUser": 1000,
                "seccompProfile": { "type": "RuntimeDefault" }
            },
            "containers": [{
                "name": pod,
                "image": CURL_IMAGE,
                "command": ["sleep", "300"],
                "securityContext": {
                    "runAsNonRoot": true,
                    "runAsUser": 1000,
                    "allowPrivilegeEscalation": false,
                    "readOnlyRootFilesystem": true,
                    "capabilities": { "drop": ["ALL"] }
                }
            }]
        }
    })
    .to_string();
    kubectl(
        "west",
        &[
            "run",
            &pod,
            &format!("--image={CURL_IMAGE}"),
            "--restart=Never",
            "--overrides",
            &overrides,
            "--command",
            "--",
            "sleep",
            "300",
        ],
        60,
    )?;
    let ready = poll_until(Duration::from_secs(60), Duration::from_secs(1), || {
        let output = kubectl("west", &["get", "pod", &pod, "-o", "json"], 20)?;
        let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        let running = value.pointer("/status/phase").and_then(serde_json::Value::as_str) == Some("Running");
        let container_ready = value
            .pointer("/status/containerStatuses/0/ready")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        Ok((running && container_ready).then_some(()))
    });
    if let Err(error) = ready {
        drop(kubectl(
            "west",
            &["delete", "pod", &pod, "--ignore-not-found=true", "--wait=true"],
            30,
        ));
        return Err(error);
    }
    Ok(pod)
}

/// Issue one request from an already-running concurrency client.
fn exec_concurrency_probe(pod: &str, label: &str, gateway: &str) -> Result<HttpResult, Box<dyn std::error::Error>> {
    let url = consumer_url(gateway);
    let curl = curl_args(&url, Credential::Valid, REQUEST_BODY);
    let mut args = vec!["exec", pod, "--"];
    args.extend(curl.iter().map(String::as_str));
    let output = kubectl("west", &args, 30)?;
    let raw = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let (status, provider_site) = parse_probe_output(&raw);
    Ok(HttpResult {
        label: label.to_owned(),
        status,
        provider_site,
    })
}

/// Best-effort cleanup for pre-created concurrency clients on every exit path.
struct ConcurrencyClients(Vec<String>);

impl Drop for ConcurrencyClients {
    fn drop(&mut self) {
        for pod in &self.0 {
            drop(kubectl(
                "west",
                &["delete", "pod", pod, "--ignore-not-found=true", "--wait=true"],
                30,
            ));
        }
    }
}

/// Run a `valkey-cli` command inside the Valkey pod; never logs the password.
fn valkey_cli(query: &str, secs: u64) -> Result<String, Box<dyn std::error::Error>> {
    let script = format!("valkey-cli -a \"${{VALKEY_PASSWORD}}\" --no-auth-warning {query}");
    let output = kubectl(
        "west",
        &["exec", &format!("deploy/{VALKEY_DEPLOY}"), "--", "sh", "-c", &script],
        secs,
    )?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Count settled quota entries in the shared sliding window.
///
/// The limiter stores each identity's admitted requests in a `...:settled` sorted
/// set; its cardinality is the trailing-window occupancy. Counter keys
/// (`active-count`, `reservation-seq`) and the `:keys` index are intentionally
/// excluded so this reflects real window consumption.
fn quota_entries() -> Result<u64, Box<dyn std::error::Error>> {
    let keys = valkey_cli(&format!("--scan --pattern \"{QUOTA_KEY_PREFIX}*:settled\""), 30)?;
    let mut total: u64 = 0;
    for key in keys.lines().filter(|line| !line.trim().is_empty()) {
        let card = valkey_cli(&format!("zcard \"{key}\""), 20)?;
        total = total.saturating_add(card.trim().parse::<u64>().unwrap_or(0));
    }
    Ok(total)
}

/// Sum actual settled charges from the encoded sorted-set members.
fn settled_tokens() -> Result<u64, Box<dyn std::error::Error>> {
    let keys = valkey_cli(&format!("--scan --pattern \"{QUOTA_KEY_PREFIX}*:settled\""), 30)?;
    let mut total = 0;
    for key in keys.lines().filter(|line| !line.trim().is_empty()) {
        let members = valkey_cli(&format!("zrange \"{key}\" 0 -1"), 20)?;
        for member in members.lines().filter(|line| !line.trim().is_empty()) {
            if let Some(actual) = member.rsplit(':').next() {
                total += actual.parse::<u64>().unwrap_or(0);
            }
        }
    }
    Ok(total)
}

/// Observe the backend's atomic namespace-wide active reservation count.
/// Every rule reservation in this topology uses the same fixed estimate, so
/// the reserved-token total is derived without a slower key scan. Settled
/// entries alone cannot distinguish a refund from a request never admitted.
fn active_reservations() -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let value = valkey_cli(&format!("get \"{QUOTA_KEY_PREFIX}:active-count\""), 20)?;
    let count = value.trim().parse::<u64>().unwrap_or(0);
    let tokens = count.saturating_mul(u64::from(RESERVED_TOKENS));
    Ok((count, tokens))
}

/// Deploy `forge up` (cluster creation only; stacks are applied per phase).
fn forge_up(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    let config = session.config.to_string_lossy().into_owned();
    let state_dir = session.state_dir.to_string_lossy().into_owned();
    let forge = session.forge.to_string_lossy().into_owned();
    run_timed(
        &forge,
        &[
            "--config",
            &config,
            "--state-dir",
            &state_dir,
            "--non-interactive",
            "up",
        ],
        600,
    )?;
    Ok(())
}

/// Load one image into every Kind cluster with an explicit pull-free load.
fn load_image(image: &str) -> Result<(), Box<dyn std::error::Error>> {
    for cluster in CLUSTERS {
        let names = RUN_IDENTITY.get().ok_or("run identity is not initialized")?;
        let name = names.kind_cluster(cluster).to_owned();
        run_timed("kind", &["load", "docker-image", image, "--name", &name], 300)?;
    }
    Ok(())
}

/// Load every immutable image referenced by the topology into all clusters.
fn load_images(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    let tag = &session.image_tag;
    load_image(&format!("praxis-ai:{tag}"))?;
    load_image(&format!("grid-operator:{tag}"))?;
    load_image(&format!("grid-overlay-sync:{tag}"))?;
    load_image(&crate::env::image_overrides::vcr_image())?;
    Ok(())
}

/// Apply a single Forge stack to one cluster.
fn apply_stack(session: &Session, cluster: &str, stack: &str) -> Result<(), Box<dyn std::error::Error>> {
    let config = session.config.to_string_lossy().into_owned();
    let state_dir = session.state_dir.to_string_lossy().into_owned();
    let forge = session.forge.to_string_lossy().into_owned();
    run_timed(
        &forge,
        &[
            "--config",
            &config,
            "--state-dir",
            &state_dir,
            "--non-interactive",
            "stack",
            "apply",
            cluster,
            stack,
        ],
        300,
    )?;
    Ok(())
}

/// Apply a per-site stack (named `<site>-<suffix>`) across all clusters.
fn apply_per_site(session: &Session, suffix: &str) -> Result<(), Box<dyn std::error::Error>> {
    for cluster in CLUSTERS {
        apply_stack(session, cluster, &format!("{cluster}{suffix}"))?;
    }
    Ok(())
}

/// Apply a shared stack (same name) across all clusters.
fn apply_shared(session: &Session, stack: &str) -> Result<(), Box<dyn std::error::Error>> {
    for cluster in CLUSTERS {
        apply_stack(session, cluster, stack)?;
    }
    Ok(())
}

/// Verify a Service load-balancer IP capture is populated for a cluster.
fn verify_lb_ip(cluster: &str, service: &str) -> Result<(), Box<dyn std::error::Error>> {
    let jsonpath = "jsonpath={.status.loadBalancer.ingress[0].ip}";
    let out = kubectl(cluster, &["get", "svc", service, "-o", jsonpath], 60)?;
    let ip = String::from_utf8_lossy(&out.stdout);
    if ip.trim().is_empty() {
        return Err(format!("{cluster}/{service} has no load-balancer IP").into());
    }
    Ok(())
}

/// Apply operator base and confirm each site published a SWIM IP.
fn deploy_operator_bases(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    apply_per_site(session, "-operator-base")?;
    for cluster in CLUSTERS {
        verify_lb_ip(cluster, "grid-operator-swim")?;
    }
    Ok(())
}

/// Apply provider gateways and confirm each site published its gateway IP.
fn deploy_provider_gateways(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    apply_shared(session, "provider-gateway")?;
    for cluster in CLUSTERS {
        verify_lb_ip(cluster, "provider-gateway")?;
    }
    Ok(())
}

/// Apply Grid sites and trust in the required order (peer pins before self-pin).
fn deploy_sites_and_trust(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    apply_per_site(session, "-site")?;
    apply_per_site(session, "-trust-bootstrap")?;
    apply_shared(session, "site-trust-bootstrap")?;
    Ok(())
}

/// Apply the west-only Valkey and both consumer gateways.
fn deploy_consumers(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    apply_stack(session, "west", "valkey")?;
    apply_stack(session, "west", "consumer-west-a")?;
    apply_stack(session, "west", "consumer-west-b")?;
    Ok(())
}

/// Emit a progress line to stderr (never includes secrets).
fn note(message: &str) {
    eprintln!("[token-rate-limit] {message}");
}

/// Deploy the whole topology in explicit dependency order.
fn deploy(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    crate::env::certs::generate_all(
        &CERTIFICATE_IDENTITIES
            .iter()
            .map(|identity| (*identity).to_owned())
            .collect::<Vec<_>>(),
    )?;
    note("forge up (clusters + network)");
    forge_up(session)?;
    note("loading immutable images into all clusters");
    load_images(session)?;
    note("phase: metallb");
    apply_shared(session, "metallb")?;
    note("phase: operator bases + SWIM captures");
    deploy_operator_bases(session)?;
    note("phase: operator seeds");
    apply_per_site(session, "-operator-seed")?;
    note("phase: vcr backends + provider boundary");
    apply_shared(session, "vcr-backend")?;
    apply_shared(session, "provider-boundary")?;
    note("phase: provider gateways + captures");
    deploy_provider_gateways(session)?;
    note("phase: sites + trust");
    deploy_sites_and_trust(session)?;
    note("phase: valkey + consumers");
    deploy_consumers(session)?;
    wait_for_consumers()?;
    Ok(())
}

/// Wait for both consumer gateway deployments to become available.
fn wait_for_consumers() -> Result<(), Box<dyn std::error::Error>> {
    let context = context_for("west");
    for gateway in CONSUMERS {
        crate::env::kubectl::wait_for_rollout_ns(&context, gateway, NAMESPACE, "deployment")?;
    }
    Ok(())
}

/// Read and parse one consumer gateway's accepted overlay `ConfigMap`.
fn read_overlay(gateway: &str) -> Result<OverlayView, Box<dyn std::error::Error>> {
    let name = format!("grid-overlay-{LOGICAL_NETWORK}-{gateway}");
    let out = kubectl("west", &["get", "configmap", &name, "-o", "json"], 30)?;
    let map: serde_json::Value = serde_json::from_slice(&out.stdout)?;
    parse_overlay(&map)
}

/// Parse the routing overlay JSON embedded in a `ConfigMap` value.
fn parse_overlay(config_map: &serde_json::Value) -> Result<OverlayView, Box<dyn std::error::Error>> {
    let raw = config_map
        .get("data")
        .and_then(|data| data.get("routing-overlay.json"))
        .and_then(serde_json::Value::as_str)
        .ok_or("overlay ConfigMap lacks routing-overlay.json")?;
    let value: serde_json::Value = serde_json::from_str(raw)?;
    let overlay = value.get("overlay").ok_or("overlay payload missing")?;
    let candidates = overlay.get("candidates").ok_or("overlay lacks candidates")?;
    let candidates: Vec<OverlayCandidate> = serde_json::from_value(candidates.clone())?;
    let revision = json_str(&value, &["revision", "value"]).ok_or("overlay lacks revision")?;
    let policy = json_str(overlay, &["selection_policy", "mode"]).ok_or("overlay lacks policy")?;
    Ok(OverlayView {
        revision,
        selection_policy: policy,
        candidates,
    })
}

/// Follow a JSON path of object keys to a string leaf.
fn json_str(value: &serde_json::Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(key)?;
    }
    current.as_str().map(str::to_owned)
}

/// Confirm an overlay has three round-robin candidates spanning all sites.
fn validate_overlay(view: &OverlayView) -> Result<(), Box<dyn std::error::Error>> {
    if view.selection_policy != "roundRobin" || view.candidates.len() != 3 {
        return Err("overlay must contain three roundRobin candidates".into());
    }
    let sites: Vec<&str> = view.candidates.iter().map(|c| c.site.as_str()).collect();
    let missing = CLUSTERS.iter().any(|site| !sites.contains(site));
    let unstable = view
        .candidates
        .iter()
        .any(|c| c.stable_id.is_empty() || c.selection_group != 0);
    if missing || unstable {
        return Err("overlay candidates lack stable per-site group-zero identities".into());
    }
    Ok(())
}

/// A single consumer gateway's convergence observation.
struct ConsumerObservation {
    /// Consumer gateway release name.
    gateway: String,
    /// Parsed accepted overlay from the Grid `ConfigMap`, when readable.
    overlay: Option<OverlayView>,
    /// Praxis-reported accepted revision from the gateway log, if present.
    praxis_accepted: Option<String>,
    /// Praxis-reported serving revision from the gateway log, if present.
    praxis_serving: Option<String>,
}

/// Strip ANSI SGR (CSI) sequences emitted by the gateway tracing subscriber.
fn strip_csi_sgr(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character == '\x1b' {
            if chars.next() == Some('[') {
                for final_byte in chars.by_ref() {
                    if final_byte.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else {
            output.push(character);
        }
    }
    output
}

/// Return the latest exact `field=value` from logs.
///
/// The `field=` prefix must sit at a whitespace boundary, so `serving_revision`
/// never matches inside `previous_serving_revision` or `retained_serving_revision`.
/// Both quoted (`field="v"`) and unquoted (`field=v`) values are supported.
fn latest_log_field(logs: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}=");
    logs.lines()
        .rev()
        .find_map(|line| {
            line.match_indices(&prefix).find_map(|(index, _)| {
                let at_boundary = index == 0
                    || line
                        .get(..index)
                        .and_then(|value| value.chars().next_back())
                        .is_some_and(char::is_whitespace);
                if !at_boundary {
                    return None;
                }
                let value = line.get(index + prefix.len()..)?;
                let parsed = value.strip_prefix('"').map_or_else(
                    || value.split_whitespace().next().unwrap_or(""),
                    |quoted| quoted.split('"').next().unwrap_or(""),
                );
                Some(parsed.to_owned())
            })
        })
        .filter(|value| !value.is_empty())
}

/// Read a consumer gateway's Praxis-reported `(accepted, serving)` revisions.
fn consumer_praxis_revisions(gateway: &str) -> (Option<String>, Option<String>) {
    let context = context_for("west");
    let deployment = format!("deployment/{gateway}");
    let args = [
        "--context",
        &context,
        "-n",
        NAMESPACE,
        "logs",
        &deployment,
        "-c",
        "praxis",
        "--tail=400",
    ];
    let logs = match spawn_timed("kubectl", &args, 30) {
        Ok(output) if output.status.success() => strip_csi_sgr(&String::from_utf8_lossy(&output.stdout)),
        _ => return (None, None),
    };
    (
        latest_log_field(&logs, "accepted_revision"),
        latest_log_field(&logs, "serving_revision"),
    )
}

/// Observe one consumer: its accepted overlay plus Praxis accepted/serving revisions.
fn observe_consumer(gateway: &str) -> ConsumerObservation {
    let (praxis_accepted, praxis_serving) = consumer_praxis_revisions(gateway);
    ConsumerObservation {
        gateway: gateway.to_owned(),
        overlay: read_overlay(gateway).ok(),
        praxis_accepted,
        praxis_serving,
    }
}

/// Observe every consumer gateway once.
fn observe_consumers() -> Vec<ConsumerObservation> {
    CONSUMERS.iter().map(|gateway| observe_consumer(gateway)).collect()
}

/// First 12 characters of a revision, for compact diagnostics.
fn short_rev(value: &str) -> &str {
    value.get(..value.len().min(12)).unwrap_or(value)
}

/// Decide whether every consumer is serving the exact accepted Grid revision.
///
/// Returns the validated overlays and the shared revision only when, for both
/// consumers: the overlay is valid, the Grid revision is nonempty, and Praxis's
/// accepted and serving revisions both equal it — and both consumers share one
/// current revision.
fn convergence_ready(observations: &[ConsumerObservation]) -> Option<(Overlays, String)> {
    if observations.len() != CONSUMERS.len() {
        return None;
    }
    let mut overlays: Overlays = BTreeMap::new();
    let mut revisions: Vec<String> = Vec::new();
    for observation in observations {
        let overlay = observation.overlay.as_ref()?;
        validate_overlay(overlay).ok()?;
        let grid = overlay.revision.as_str();
        if grid.is_empty()
            || observation.praxis_accepted.as_deref() != Some(grid)
            || observation.praxis_serving.as_deref() != Some(grid)
        {
            return None;
        }
        overlays.insert(observation.gateway.clone(), overlay.clone());
        revisions.push(grid.to_owned());
    }
    let first = revisions.first()?;
    revisions
        .iter()
        .all(|revision| revision == first)
        .then(|| (overlays, first.clone()))
}

/// Compact per-consumer last-observed revisions for timeout diagnostics.
fn convergence_diagnostics(observations: &[ConsumerObservation]) -> String {
    observations
        .iter()
        .map(|observation| {
            let grid = observation
                .overlay
                .as_ref()
                .map_or("none", |overlay| overlay.revision.as_str());
            let accepted = observation.praxis_accepted.as_deref().unwrap_or("none");
            let serving = observation.praxis_serving.as_deref().unwrap_or("none");
            format!(
                "{}[grid={} praxis_accepted={} praxis_serving={}]",
                observation.gateway,
                short_rev(grid),
                short_rev(accepted),
                short_rev(serving)
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Gate traffic until both consumer gateways are serving the exact overlay
/// revision Grid accepted.
///
/// State-based polling (no fixed success sleep): requires, for both consumers,
/// a valid three-candidate overlay whose Grid revision equals Praxis's accepted
/// and serving revisions, with both consumers on one shared revision, held
/// stable across one poll before returning. On timeout it reports each
/// consumer's last observed Grid/accepted/serving revisions.
fn await_overlay_convergence() -> Result<Overlays, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(240);
    let mut pending: Option<String> = None;
    loop {
        let observations = observe_consumers();
        if let Some((overlays, revision)) = convergence_ready(&observations) {
            if pending.as_deref() == Some(revision.as_str()) {
                return Ok(overlays);
            }
            pending = Some(revision);
        } else {
            pending = None;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "consumers did not reach a stable serving revision matching Grid within 240s: {}",
                convergence_diagnostics(&observations)
            )
            .into());
        }
        std::thread::park_timeout(Duration::from_secs(5));
    }
}

/// Build a passed/failed scenario result.
fn scenario(name: &str, passed: bool, detail: String, facts: BTreeMap<String, serde_json::Value>) -> ScenarioResult {
    ScenarioResult {
        name: name.to_owned(),
        passed,
        detail,
        facts,
    }
}

/// Record a probe into the shared results log and return its outcome.
fn record(results: &mut Vec<HttpResult>, result: HttpResult) -> Outcome {
    let outcome = classify(result.status);
    results.push(result);
    outcome
}

/// Whether a probe failed before receiving any HTTP response.
fn is_transport_failure(status: u16) -> bool {
    status == 0
}

/// Retry only transport failures while retaining every attempt as evidence.
/// Real HTTP responses, including quota and provider errors, are never retried.
fn probe_with_transport_retry(
    results: &mut Vec<HttpResult>,
    label: &str,
    gateway: &str,
    credential: Credential,
) -> Result<HttpResult, Box<dyn std::error::Error>> {
    for retry in 0..=TRANSPORT_RETRIES {
        let mut result = probe(&format!("{label}-transport-{retry}"), gateway, credential)?;
        if !is_transport_failure(result.status) || retry == TRANSPORT_RETRIES {
            label.clone_into(&mut result.label);
            return Ok(result);
        }
        results.push(result);
    }
    Err("transport retry loop ended without a result".into())
}

/// Scenario: valid Alice auth is admitted on both gateways with attribution.
fn scenario_valid_auth(results: &mut Vec<HttpResult>) -> Result<ScenarioResult, Box<dyn std::error::Error>> {
    let mut facts = BTreeMap::new();
    let mut ok = true;
    for gateway in CONSUMERS {
        let result = probe_with_transport_retry(results, &format!("valid-{gateway}"), gateway, Credential::Valid)?;
        let site = result.provider_site.clone();
        let admitted = record(results, result) == Outcome::Admitted;
        facts.insert(
            gateway.to_owned(),
            serde_json::json!({ "admitted": admitted, "site": site }),
        );
        ok = ok && admitted && site.is_some();
    }
    Ok(scenario(
        "valid_auth",
        ok,
        "Alice admitted with regional attribution".to_owned(),
        facts,
    ))
}

/// Issue one probe, record it, and return its classified outcome.
fn probe_and_record(
    results: &mut Vec<HttpResult>,
    label: &str,
    gateway: &str,
    credential: Credential,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let result = probe(label, gateway, credential)?;
    Ok(record(results, result))
}

/// Scenario: missing and malformed auth are rejected before any reservation.
fn scenario_auth_rejection(results: &mut Vec<HttpResult>) -> Result<ScenarioResult, Box<dyn std::error::Error>> {
    let before = quota_entries()?;
    let outcomes = [
        probe_and_record(results, "missing-auth", CONSUMER_A, Credential::Missing)?,
        probe_and_record(results, "malformed-auth", CONSUMER_A, Credential::Malformed)?,
        probe_and_record(results, "wrong-password", CONSUMER_A, Credential::WrongPassword)?,
    ];
    let after = quota_entries()?;
    let all_401 = outcomes.iter().all(|outcome| *outcome == Outcome::Unauthorized);
    let statuses = outcomes
        .iter()
        .map(|outcome| format!("{outcome:?}"))
        .collect::<Vec<_>>();
    let facts = BTreeMap::from([
        ("statuses".to_owned(), serde_json::json!(statuses)),
        (
            "quota_entries_delta".to_owned(),
            serde_json::json!(after.saturating_sub(before)),
        ),
    ]);
    let detail = "bad auth rejected without reserving quota".to_owned();
    Ok(scenario("auth_rejection", all_401 && after <= before, detail, facts))
}

/// Scenario: readiness/probe traffic does not consume the inference quota.
fn scenario_probe_no_quota() -> Result<ScenarioResult, Box<dyn std::error::Error>> {
    let before = quota_entries()?;
    std::thread::park_timeout(Duration::from_secs(15));
    let after = quota_entries()?;
    let spec = kubectl(
        "west",
        &[
            "get",
            "deploy",
            CONSUMER_A,
            "-o",
            "jsonpath={.spec.template.spec.containers[0].readinessProbe}",
        ],
        30,
    )?;
    let probe_spec = String::from_utf8_lossy(&spec.stdout).trim().to_owned();
    let facts = BTreeMap::from([
        ("quota_entries_before".to_owned(), serde_json::json!(before)),
        ("quota_entries_after".to_owned(), serde_json::json!(after)),
        ("readiness_probe".to_owned(), serde_json::json!(probe_spec)),
    ]);
    Ok(scenario(
        "probe_no_quota",
        after == before,
        "quota state unchanged across a probe-only interval (no reservation)".to_owned(),
        facts,
    ))
}

/// Exhaust the shared budget from one gateway; return admitted provider sites.
fn exhaust_budget(results: &mut Vec<HttpResult>) -> Result<ExhaustOutcome, Box<dyn std::error::Error>> {
    let attempts = (CAPACITY_TOKENS / RESERVED_TOKENS).saturating_add(4);
    let mut sites = Vec::new();
    let mut saw_denied = false;
    for index in 0..attempts {
        let gateway = consumer_for(index as usize);
        let result = probe_with_transport_retry(results, &format!("exhaust-{index}"), gateway, Credential::Valid)?;
        if let Some(site) = result.provider_site.clone() {
            sites.push(site);
        }
        if record(results, result) == Outcome::QuotaDenied {
            saw_denied = true;
        }
    }
    Ok((sites, saw_denied))
}

/// Scenario: both gateways share one Alice budget and route across all sites.
fn scenario_shared_budget(
    results: &mut Vec<HttpResult>,
    distribution: &mut BTreeMap<String, u32>,
) -> Result<ScenarioResult, Box<dyn std::error::Error>> {
    reset_window();
    let (sites, saw_denied) = exhaust_budget(results)?;
    for site in &sites {
        *distribution.entry(site.clone()).or_insert(0) += 1;
    }
    let denied_b =
        classify(probe_with_transport_retry(results, "shared-b-after-exhaust", CONSUMER_B, Credential::Valid)?.status);
    let all_sites = CLUSTERS.iter().all(|site| distribution.contains_key(*site));
    let facts = BTreeMap::from([
        ("distribution".to_owned(), serde_json::json!(distribution)),
        (
            "second_gateway_denied".to_owned(),
            serde_json::json!(denied_b == Outcome::QuotaDenied),
        ),
    ]);
    let passed = saw_denied && all_sites && denied_b == Outcome::QuotaDenied;
    Ok(scenario(
        "shared_budget_and_distribution",
        passed,
        "shared budget; routed across sites".to_owned(),
        facts,
    ))
}

/// Fire `count` concurrent valid probes and sample the shared reservation
/// ledger while they overlap. Every client pod is Running before the barrier
/// releases its request, removing pod scheduling from the measurement.
#[expect(
    clippy::too_many_lines,
    reason = "the bounded concurrent probe lifecycle is kept together"
)]
fn concurrent_admitted(count: usize) -> Result<ConcurrentEvidence, Box<dyn std::error::Error>> {
    let mut clients = ConcurrencyClients(Vec::with_capacity(count));
    for index in 0..count {
        clients.0.push(create_concurrency_client(index)?);
    }
    let barrier = Arc::new(Barrier::new(count + 1));
    let completed = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = clients
        .0
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, pod)| {
            let gateway = consumer_for(index).to_owned();
            let barrier = Arc::clone(&barrier);
            let completed = Arc::clone(&completed);
            std::thread::spawn(move || {
                barrier.wait();
                let result = exec_concurrency_probe(&pod, &format!("concurrent-{index}"), &gateway)
                    .map_err(|error| error.to_string());
                completed.fetch_add(1, Ordering::Relaxed);
                result
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    let mut max_active = 0;
    let mut max_active_tokens = 0;
    while completed.load(Ordering::Relaxed) < count {
        let (active, tokens) = active_reservations()?;
        max_active = max_active.max(active);
        max_active_tokens = max_active_tokens.max(tokens);
        if start.elapsed() >= Duration::from_secs(90) {
            return Err("concurrent probes did not complete within 90s".into());
        }
        std::thread::park_timeout(Duration::from_millis(50));
    }
    let mut admitted: usize = 0;
    let mut results = Vec::with_capacity(count);
    for handle in handles {
        let joined = match handle.join() {
            Ok(inner) => inner,
            Err(_panic) => return Err("concurrent probe thread panicked".into()),
        };
        let result = joined?;
        if classify(result.status) == Outcome::Admitted {
            admitted = admitted.saturating_add(1);
        }
        results.push(result);
    }
    Ok(ConcurrentEvidence {
        admitted,
        max_active,
        max_active_tokens,
        results,
    })
}

/// Scenario: concurrent reservations never admit more than the window allows.
///
/// A concurrent burst is evaluated from the reservation ledger, not from a
/// 60/15 arithmetic assumption. Completed requests may reconcile below their
/// reservation and refund capacity, so five successful responses alone do not
/// prove over-admission.
fn scenario_concurrency(results: &mut Vec<HttpResult>) -> Result<ScenarioResult, Box<dyn std::error::Error>> {
    reset_window();
    let burst = 8;
    let concurrent = concurrent_admitted(burst)?;
    results.extend(concurrent.results);
    let settled = settled_tokens()?;
    let facts = BTreeMap::from([
        ("concurrent_burst".to_owned(), serde_json::json!(burst)),
        ("concurrent_admitted".to_owned(), serde_json::json!(concurrent.admitted)),
        (
            "max_active_reservations".to_owned(),
            serde_json::json!(concurrent.max_active),
        ),
        (
            "max_active_reserved_tokens".to_owned(),
            serde_json::json!(concurrent.max_active_tokens),
        ),
        ("settled_actual_tokens".to_owned(), serde_json::json!(settled)),
    ]);
    let passed = concurrent.admitted >= 1
        && concurrent.admitted < burst
        && concurrent.max_active >= 2
        && concurrent.max_active_tokens > u64::from(RESERVED_TOKENS)
        && concurrent.max_active_tokens <= u64::from(CAPACITY_TOKENS)
        && settled <= u64::from(CAPACITY_TOKENS);
    Ok(scenario(
        "concurrency_no_over_admission",
        passed,
        "no over-admission under load".to_owned(),
        facts,
    ))
}

/// Poll until a fresh valid request is admitted after natural window expiry.
fn await_window_reset() -> Result<(), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(WINDOW_SECS.saturating_add(90));
    poll_until(timeout, Duration::from_secs(5), || {
        let result = probe("window-probe", CONSUMER_A, Credential::Valid)?;
        Ok((classify(result.status) == Outcome::Admitted).then_some(()))
    })
}

/// Scenario: the sliding window recovers admission naturally (no mutation).
fn scenario_window_expiry(results: &mut Vec<HttpResult>) -> Result<ScenarioResult, Box<dyn std::error::Error>> {
    reset_window();
    exhaust_budget(results)?;
    let start = Instant::now();
    let recovered = await_window_reset().is_ok();
    let facts = BTreeMap::from([(
        "recovery_seconds".to_owned(),
        serde_json::json!(start.elapsed().as_secs()),
    )]);
    Ok(scenario(
        "window_expiry_recovery",
        recovered,
        "admission recovered after real expiry".to_owned(),
        facts,
    ))
}

/// Scenario: restarting a consumer preserves Valkey-backed quota state.
fn scenario_restart_persistence(results: &mut Vec<HttpResult>) -> Result<ScenarioResult, Box<dyn std::error::Error>> {
    reset_window();
    exhaust_budget(results)?;
    let before = quota_entries()?;
    let peer_before = probe("peer-before-restart", CONSUMER_B, Credential::Valid)?;
    let peer_before_outcome = classify(peer_before.status);
    record(results, peer_before);
    restart_consumer(CONSUMER_A)?;
    let after = quota_entries()?;
    let peer_after = probe("peer-after-restart", CONSUMER_B, Credential::Valid)?;
    let peer_after_outcome = classify(peer_after.status);
    record(results, peer_after);
    let post_restart = probe("restarted-consumer", CONSUMER_A, Credential::Valid)?;
    let post_restart_status = post_restart.status;
    let post_restart_outcome = classify(post_restart.status);
    record(results, post_restart);
    let evidence = RestartEvidence {
        before,
        after,
        peer_before: peer_before_outcome,
        peer_after: peer_after_outcome,
        restarted_status: post_restart_status,
        restarted_outcome: post_restart_outcome,
    };
    Ok(scenario(
        "restart_persistence",
        evidence.passed(),
        "shared exhausted quota remained visible during and after consumer restart".to_owned(),
        evidence.facts(),
    ))
}

/// Restart one west consumer and wait for the namespace-scoped rollout.
fn restart_consumer(consumer: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = context_for("west");
    kubectl("west", &["rollout", "restart", &format!("deployment/{consumer}")], 60)?;
    crate::env::kubectl::wait_for_rollout_ns(&context, consumer, NAMESPACE, "deployment")
}

/// Scale the Valkey deployment to a replica count and wait for rollout.
fn scale_valkey(replicas: u32) -> Result<(), Box<dyn std::error::Error>> {
    kubectl(
        "west",
        &[
            "scale",
            &format!("deploy/{VALKEY_DEPLOY}"),
            &format!("--replicas={replicas}"),
        ],
        60,
    )?;
    if replicas == 0 {
        return kubectl(
            "west",
            &[
                "wait",
                "--for=delete",
                "pod",
                "-l",
                "app.kubernetes.io/name=valkey",
                "--timeout=120s",
            ],
            130,
        )
        .map(drop);
    }
    let context = context_for("west");
    crate::env::kubectl::wait_for_rollout_ns(&context, VALKEY_DEPLOY, NAMESPACE, "deployment")
}

/// Scenario: Valkey outage fails closed (503) and recovery restores service.
fn scenario_valkey_outage(results: &mut Vec<HttpResult>) -> Result<ScenarioResult, Box<dyn std::error::Error>> {
    await_window_reset()?;
    scale_valkey(0)?;
    let outage = probe("valkey-down", CONSUMER_A, Credential::Valid)?;
    let provider_not_contacted = outage.provider_site.is_none();
    let failed_closed = classify(outage.status) == Outcome::FailClosed && provider_not_contacted;
    record(results, outage);
    scale_valkey(1)?;
    let recovered = await_window_reset().is_ok();
    let facts = BTreeMap::from([
        ("failed_closed".to_owned(), serde_json::json!(failed_closed)),
        (
            "provider_not_contacted".to_owned(),
            serde_json::json!(provider_not_contacted),
        ),
        ("recovered".to_owned(), serde_json::json!(recovered)),
    ]);
    Ok(scenario(
        "valkey_outage_fail_closed",
        failed_closed && recovered,
        "fail-closed then recovered".to_owned(),
        facts,
    ))
}

/// Create a long-lived, restricted probe pod. Keeping it alive lets the
/// qualification independently prove admission, Running, DNS, and network
/// behavior instead of treating a rejected `kubectl run --rm` as a policy hit.
#[expect(
    clippy::too_many_lines,
    reason = "probe creation, evidence, and idempotent deletion form one lifecycle"
)]
fn network_probe_pod(
    label: &str,
    allowed: bool,
) -> Result<BTreeMap<String, serde_json::Value>, Box<dyn std::error::Error>> {
    let sequence = PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let pod = format!("{label}-{sequence}");
    let context = context_for("west");
    let overrides = serde_json::json!({
        "spec": {
            "automountServiceAccountToken": false,
            "securityContext": {
                "runAsNonRoot": true,
                "runAsUser": 1000,
                "seccompProfile": { "type": "RuntimeDefault" }
            },
            "containers": [{
                "name": pod,
                "image": CURL_IMAGE,
                "command": ["sleep", "300"],
                "securityContext": {
                    "runAsNonRoot": true,
                    "runAsUser": 1000,
                    "allowPrivilegeEscalation": false,
                    "capabilities": { "drop": ["ALL"] }
                }
            }]
        }
    })
    .to_string();
    let labels = if allowed {
        "grid.praxis-proxy.io/quota-client=true"
    } else {
        "qualification=network-policy-negative"
    };
    let run_args = [
        "run",
        &pod,
        &format!("--image={CURL_IMAGE}"),
        "--context",
        &context,
        "-n",
        NAMESPACE,
        "--restart=Never",
        "--labels",
        labels,
        "--overrides",
        &overrides,
        "--command",
        "--",
        "sleep",
        "300",
    ];
    run_timed("kubectl", &run_args, 60)?;
    let phase = poll_until(Duration::from_secs(60), Duration::from_secs(2), || {
        let output = kubectl("west", &["get", "pod", &pod, "-o", "json"], 20)?;
        let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        Ok((value.pointer("/status/phase").and_then(serde_json::Value::as_str) == Some("Running")).then_some(value))
    });
    let mut facts = BTreeMap::new();
    let running = phase.is_ok();
    facts.insert("pod".to_owned(), serde_json::json!(pod));
    facts.insert("running".to_owned(), serde_json::json!(running));
    if running {
        let dns = kubectl(
            "west",
            &["exec", &pod, "--", "nslookup", "valkey.grid-system.svc.cluster.local"],
            20,
        );
        let dns_ok = dns.as_ref().is_ok_and(|output| output.status.success());
        facts.insert("dns_succeeded".to_owned(), serde_json::json!(dns_ok));
        let connect = kubectl_unchecked(
            "west",
            &[
                "exec",
                &pod,
                "--",
                "sh",
                "-c",
                "printf '*1\\r\\n$4\\r\\nPING\\r\\n' | curl -sS --connect-timeout 3 --max-time 5 --upload-file - telnet://valkey.grid-system.svc.cluster.local:6379",
            ],
            20,
        );
        let connection_output = connect.as_ref().map_or_else(ToString::to_string, |output| {
            format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .trim()
            .to_owned()
        });
        let command_succeeded = connect.as_ref().is_ok_and(|output| output.status.success());
        let connect_ok = redis_connection_reached(command_succeeded, &connection_output);
        facts.insert("connection_allowed".to_owned(), serde_json::json!(connect_ok));
        facts.insert("connection_output".to_owned(), serde_json::json!(connection_output));
        facts.insert(
            "policy_expectation_met".to_owned(),
            serde_json::json!(dns_ok && (connect_ok == allowed)),
        );
    }
    let deleted = kubectl(
        "west",
        &["delete", "pod", &pod, "--ignore-not-found=true", "--wait=true"],
        60,
    )
    .is_ok();
    facts.insert("pod_deleted".to_owned(), serde_json::json!(deleted));
    Ok(facts)
}

/// A Redis protocol response proves the `NetworkPolicy` allowed the connection,
/// even when curl later times out waiting for the persistent socket to close.
fn redis_connection_reached(command_succeeded: bool, output: &str) -> bool {
    command_succeeded || output.contains("+PONG") || output.contains("-NOAUTH")
}

/// Scenario: restricted unlabeled traffic is denied and an allowed quota
/// client can connect. Admission, image, and DNS failures never count as a
/// `NetworkPolicy` success.
fn scenario_network_policy() -> Result<ScenarioResult, Box<dyn std::error::Error>> {
    let negative = network_probe_pod("np-negative", false)?;
    let positive = network_probe_pod("np-positive", true)?;
    let blocked = negative
        .get("policy_expectation_met")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let positive_control = positive
        .get("policy_expectation_met")
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let facts = BTreeMap::from([
        ("unauthorized_probe".to_owned(), serde_json::to_value(negative)?),
        ("permitted_quota_client".to_owned(), serde_json::to_value(positive)?),
        ("blocked".to_owned(), serde_json::json!(blocked)),
        ("positive_control".to_owned(), serde_json::json!(positive_control)),
    ]);
    Ok(scenario(
        "network_policy_denies_unauthorized",
        blocked && positive_control,
        "unlabeled pod denied while permitted quota client connects".to_owned(),
        facts,
    ))
}

/// Tear down all clusters via `forge down`.
fn teardown(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    let config = session.config.to_string_lossy().into_owned();
    let state_dir = session.state_dir.to_string_lossy().into_owned();
    let forge = session.forge.to_string_lossy().into_owned();
    run_timed(
        &forge,
        &[
            "--config",
            &config,
            "--state-dir",
            &state_dir,
            "--non-interactive",
            "down",
        ],
        300,
    )?;
    if session.state_dir.exists() {
        fs::remove_dir_all(&session.state_dir)
            .map_err(|error| format!("remove run-scoped Forge state {}: {error}", session.state_dir.display()))?;
    }
    fs::remove_file(&session.config).map_err(|error| {
        format!(
            "remove run-scoped resolved Forge config {}: {error}",
            session.config.display()
        )
    })?;
    Ok(())
}

/// Write a value as pretty JSON with a trailing newline.
fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}

/// Materialize the Forge config with the requested image tag and verify it.
fn materialize(
    options: &Options,
    names: &RunIdentity,
    state_dir: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let tag = options
        .image_tag
        .as_deref()
        .ok_or("--image-tag is required for a Never-pull run")?;
    let content = fs::read_to_string(&options.forge_config)?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(&content)?;
    apply_run_identity(&mut config, names);
    rewrite_context_references(&mut config, names);
    rewrite_exec_state_references(&mut config, state_dir);
    apply_image_tag(&mut config, tag);
    let rendered = serde_yaml::to_string(&config)?;
    let parent = options
        .forge_config
        .parent()
        .ok_or("Forge config must have a parent directory")?;
    let resolved = parent.join(format!(".forge.resolved.{}.yaml", names.run_id));
    fs::write(&resolved, rendered)?;
    verify_resolved_tag(&resolved, tag)?;
    verify_resolved_names(&resolved, names)?;
    Ok(resolved)
}

/// Point shell steps at the run-scoped Forge state while preserving
/// `template-file` targets, whose `.forge/` prefix Forge resolves itself.
fn rewrite_exec_state_references(value: &mut serde_yaml::Value, state_dir: &Path) {
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            let is_exec = mapping
                .get(serde_yaml::Value::String("type".to_owned()))
                .and_then(serde_yaml::Value::as_str)
                == Some("exec");
            if is_exec {
                if let Some(command) = mapping.get_mut(serde_yaml::Value::String("command".to_owned())) {
                    rewrite_state_path_strings(command, state_dir);
                }
            } else {
                for child in mapping.values_mut() {
                    rewrite_exec_state_references(child, state_dir);
                }
            }
        },
        serde_yaml::Value::Sequence(sequence) => {
            for child in sequence {
                rewrite_exec_state_references(child, state_dir);
            }
        },
        serde_yaml::Value::Null
        | serde_yaml::Value::Bool(_)
        | serde_yaml::Value::Number(_)
        | serde_yaml::Value::String(_)
        | serde_yaml::Value::Tagged(_) => {},
    }
}

/// Rewrite `.forge/` paths contained in one exec command value.
fn rewrite_state_path_strings(value: &mut serde_yaml::Value, state_dir: &Path) {
    match value {
        serde_yaml::Value::String(string) => {
            *string = string.replace(".forge/runtime/", &format!("{}/runtime/", state_dir.display()));
        },
        serde_yaml::Value::Sequence(sequence) => {
            for child in sequence {
                rewrite_state_path_strings(child, state_dir);
            }
        },
        serde_yaml::Value::Mapping(mapping) => {
            for child in mapping.values_mut() {
                rewrite_state_path_strings(child, state_dir);
            }
        },
        serde_yaml::Value::Null
        | serde_yaml::Value::Bool(_)
        | serde_yaml::Value::Number(_)
        | serde_yaml::Value::Tagged(_) => {},
    }
}

/// Inject the physical Forge environment identity while preserving logical
/// Grid resource names inside the checked-in manifests.
fn apply_run_identity(config: &mut serde_yaml::Value, names: &RunIdentity) {
    if let Some(metadata) = config.get_mut("metadata").and_then(serde_yaml::Value::as_mapping_mut) {
        metadata.insert(
            serde_yaml::Value::String("name".to_owned()),
            serde_yaml::Value::String(names.forge_name.clone()),
        );
    }
    if let Some(runtime) = config
        .get_mut("spec")
        .and_then(serde_yaml::Value::as_mapping_mut)
        .and_then(|spec| spec.get_mut("runtime"))
        .and_then(serde_yaml::Value::as_mapping_mut)
    {
        runtime.insert(
            serde_yaml::Value::String("clusterPrefix".to_owned()),
            serde_yaml::Value::String(names.cluster_prefix.clone()),
        );
    }
}

/// Rewrite only the topology's known fixed kubectl context tokens. Logical
/// Kubernetes object names and DNS identities are intentionally untouched.
fn rewrite_context_references(value: &mut serde_yaml::Value, names: &RunIdentity) {
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            for child in mapping.values_mut() {
                rewrite_context_references(child, names);
            }
        },
        serde_yaml::Value::Sequence(sequence) => {
            for child in sequence {
                rewrite_context_references(child, names);
            }
        },
        serde_yaml::Value::String(string) => {
            let old_template = format!("kind-{PHYSICAL_PREFIX}-{{{{ cluster.name }}}}");
            let new_template = format!("kind-{}-{{{{ cluster.name }}}}", names.cluster_prefix);
            *string = string.replace(&old_template, &new_template);
            for site in CLUSTERS {
                let old = format!("kind-{PHYSICAL_PREFIX}-{site}");
                let new = names.context(site);
                *string = string.replace(&old, new);
            }
        },
        serde_yaml::Value::Null
        | serde_yaml::Value::Bool(_)
        | serde_yaml::Value::Number(_)
        | serde_yaml::Value::Tagged(_) => {},
    }
}

/// Rewrite every cluster's image properties to the local `tag` under Never.
fn apply_image_tag(config: &mut serde_yaml::Value, tag: &str) {
    let clusters = config
        .get_mut("spec")
        .and_then(|spec| spec.get_mut("clusters"))
        .and_then(serde_yaml::Value::as_sequence_mut);
    let Some(clusters) = clusters else {
        return;
    };
    for cluster in clusters {
        if let Some(props) = cluster
            .get_mut("properties")
            .and_then(serde_yaml::Value::as_mapping_mut)
        {
            set_image_properties(props, tag);
        }
    }
}

/// Set the image repo/tag/pull-policy properties for one cluster.
fn set_image_properties(props: &mut serde_yaml::Mapping, tag: &str) {
    let pairs = [
        ("gatewayImage", format!("praxis-ai:{tag}")),
        ("gatewayImageRepo", "praxis-ai".to_owned()),
        ("gatewayImageTag", tag.to_owned()),
        ("operatorImage", format!("grid-operator:{tag}")),
        ("operatorImageRepo", "grid-operator".to_owned()),
        ("operatorImageTag", tag.to_owned()),
        ("overlaySyncImage", format!("grid-overlay-sync:{tag}")),
        ("overlaySyncImageRepo", "grid-overlay-sync".to_owned()),
        ("overlaySyncImageTag", tag.to_owned()),
        ("imagePullPolicy", "Never".to_owned()),
    ];
    for (key, value) in pairs {
        props.insert(
            serde_yaml::Value::String(key.to_owned()),
            serde_yaml::Value::String(value),
        );
    }
}

/// Verify every managed image reference in the resolved config carries `tag`.
fn verify_resolved_tag(resolved: &Path, tag: &str) -> Result<(), Box<dyn std::error::Error>> {
    let content = fs::read_to_string(resolved)?;
    for repo in ["praxis-ai", "grid-operator", "grid-overlay-sync"] {
        if !content.contains(&format!("{repo}:{tag}")) {
            return Err(format!("resolved config missing {repo}:{tag} (image mismatch under Never)").into());
        }
    }
    Ok(())
}

/// Ensure all physical names were rendered exactly into the resolved config.
fn verify_resolved_names(resolved: &Path, names: &RunIdentity) -> Result<(), Box<dyn std::error::Error>> {
    let content = fs::read_to_string(resolved)?;
    let config: serde_yaml::Value = serde_yaml::from_str(&content)?;
    let metadata_name = config
        .get("metadata")
        .and_then(|metadata| metadata.get("name"))
        .and_then(serde_yaml::Value::as_str);
    let cluster_prefix = config
        .get("spec")
        .and_then(|spec| spec.get("runtime"))
        .and_then(|runtime| runtime.get("clusterPrefix"))
        .and_then(serde_yaml::Value::as_str);
    if metadata_name != Some(names.forge_name.as_str()) {
        return Err(format!(
            "resolved Forge metadata.name {:?} does not equal {:?}",
            metadata_name, names.forge_name
        )
        .into());
    }
    if cluster_prefix != Some(names.cluster_prefix.as_str()) {
        return Err(format!(
            "resolved Forge runtime.clusterPrefix {:?} does not equal {:?}",
            cluster_prefix, names.cluster_prefix
        )
        .into());
    }
    Ok(())
}

/// Execute every qualification scenario, collecting evidence.
///
/// A scenario that hits an operational error is recorded as a failed scenario
/// (with the error in its detail) rather than aborting the run, so evidence is
/// always written and no failure is hidden.
fn run_scenarios(results: &mut Vec<HttpResult>, distribution: &mut BTreeMap<String, u32>) -> Vec<ScenarioResult> {
    vec![
        guarded("probe_no_quota", scenario_probe_no_quota()),
        guarded("valid_auth", scenario_valid_auth(results)),
        guarded("auth_rejection", scenario_auth_rejection(results)),
        guarded(
            "shared_budget_and_distribution",
            scenario_shared_budget(results, distribution),
        ),
        guarded("concurrency_no_over_admission", scenario_concurrency(results)),
        guarded("window_expiry_recovery", scenario_window_expiry(results)),
        guarded("restart_persistence", scenario_restart_persistence(results)),
        guarded("valkey_outage_fail_closed", scenario_valkey_outage(results)),
        guarded("network_policy_denies_unauthorized", scenario_network_policy()),
    ]
}

/// Convert a scenario outcome into a logged result; errors become failures.
fn guarded(name: &str, outcome: Result<ScenarioResult, Box<dyn std::error::Error>>) -> ScenarioResult {
    match outcome {
        Ok(result) => logged(result),
        Err(error) => logged(scenario(
            name,
            false,
            format!("scenario error: {error}"),
            BTreeMap::new(),
        )),
    }
}

/// Log a completed scenario's outcome and pass it through.
fn logged(result: ScenarioResult) -> ScenarioResult {
    note(&format!(
        "scenario {}: {}",
        result.name,
        if result.passed { "PASS" } else { "FAIL" }
    ));
    result
}

/// Assemble and persist the evidence document plus a human summary.
fn persist_evidence(
    session: &Session,
    collected: Collected,
    cleanup_succeeded: Option<bool>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let all_passed = collected.scenarios.iter().all(|scenario| scenario.passed);
    let summary = summarize(&collected.scenarios, &collected.distribution);
    let evidence = Evidence {
        schema_version: "1",
        topology: LOGICAL_NETWORK,
        run_id: session.names.run_id.clone(),
        run_identity: session.names.clone(),
        ownership: session.ownership.borrow().clone(),
        image_tag: session.image_tag.clone(),
        required_gateway_features: REQUIRED_GATEWAY_FEATURES,
        resolved_config: session.config.to_string_lossy().into_owned(),
        overlays: collected.overlays,
        http_results: collected.results,
        distribution: collected.distribution,
        scenarios: collected.scenarios,
        warmup_window_reset_seconds: collected.warmup_window_reset_seconds,
        cleanup_succeeded,
    };
    write_json(&session.evidence.join("results.json"), &evidence)?;
    fs::write(session.evidence.join("summary.txt"), summary)?;
    Ok(all_passed)
}

/// Build a concise human-readable summary.
fn summarize(scenarios: &[ScenarioResult], distribution: &BTreeMap<String, u32>) -> String {
    let mut lines = vec!["grid-token-rate-limit qualification".to_owned(), String::new()];
    for scenario in scenarios {
        let mark = if scenario.passed { "PASS" } else { "FAIL" };
        lines.push(format!("[{mark}] {}: {}", scenario.name, scenario.detail));
    }
    lines.push(String::new());
    lines.push(format!("provider distribution: {distribution:?}"));
    lines.join("\n")
}

/// Run explicit cleanup while retaining the drop fallback until it succeeds.
fn cleanup_after_run(session: &Session, guard: &mut CleanupGuard<'_>) -> Option<bool> {
    if session.keep {
        guard.disarmed = true;
        return None;
    }
    let succeeded = teardown(session).is_ok();
    if succeeded {
        guard.disarmed = true;
    }
    Some(succeeded)
}

/// Entry point for `cargo xtask env run-grid-token-rate-limit-qualification`.
///
/// # Errors
/// Returns an error if setup, deployment, a scenario, or evidence writing fails.
pub(crate) fn run(options: &Options) -> Result<(), Box<dyn std::error::Error>> {
    let session = prepare(options)?;
    drop(RUN_IDENTITY.set(session.names.clone()));
    note(&format!("required Praxis AI features: {REQUIRED_GATEWAY_FEATURES}"));
    note(&format!(
        "verifying praxis-ai:{} label {GATEWAY_FEATURES_LABEL}",
        session.image_tag
    ));
    verify_gateway_image_features(&session.image_tag)?;
    let mut guard = CleanupGuard {
        session: &session,
        disarmed: false,
    };
    let collected = qualify(&session)?;
    {
        let mut ownership = session.ownership.borrow_mut();
        ownership.owned_clusters = session.names.kind_clusters.values().cloned().collect();
        ownership.owned_network = true;
    }
    let cleanup = cleanup_after_run(&session, &mut guard);
    let passed = persist_evidence(&session, collected, cleanup)?;
    if passed {
        return Ok(());
    }
    Err("one or more qualification scenarios failed; see evidence".into())
}

/// Validate tooling, materialize the config, and build the session.
fn prepare(options: &Options) -> Result<Session, Box<dyn std::error::Error>> {
    require_tools()?;
    let names = select_run_identity(options)?;
    let evidence = resolve_evidence_dir(options)?;
    fs::create_dir_all(&evidence)?;
    let state_dir = options
        .forge_config
        .parent()
        .ok_or("Forge config must have a parent directory")?
        .join(format!(".forge.{}", names.run_id));
    let config = materialize(options, &names, &state_dir)?;
    let forge = PathBuf::from(std::env::var_os("FORGE_BIN").unwrap_or_else(|| "target/debug/praxis-forge".into()));
    Ok(Session {
        forge,
        config,
        state_dir,
        evidence,
        image_tag: options.image_tag.clone().unwrap_or_default(),
        keep: options.keep,
        names,
        ownership: std::cell::RefCell::new(OwnershipPlan {
            preexisting_clusters: Vec::new(),
            preexisting_network: false,
            owned_clusters: Vec::new(),
            owned_network: false,
        }),
    })
}

/// Deploy the topology, await convergence, warm the data plane, run scenarios.
fn qualify(session: &Session) -> Result<Collected, Box<dyn std::error::Error>> {
    deploy(session)?;
    let overlays = await_overlay_convergence()?;
    let mut results = Vec::new();
    note("warming data plane (both gateways must return 200 + provider-site)");
    await_data_plane_ready(&mut results)?;
    note("aging authenticated warmup reservations before qualification scenarios");
    let warmup_window_reset_seconds = reset_window();
    note(&format!(
        "warmup quota reset completed after {warmup_window_reset_seconds}s of natural window aging"
    ));
    let mut distribution = BTreeMap::new();
    let scenarios = run_scenarios(&mut results, &mut distribution);
    Ok(Collected {
        overlays,
        results,
        distribution,
        scenarios,
        warmup_window_reset_seconds,
    })
}

/// Poll bounded authenticated requests until each gateway returns 200 with a
/// valid provider-site header. Every intermediate status is recorded as
/// evidence; a persistent non-ready state fails the qualification (it is never
/// hidden or treated as normal). This readiness gate is distinct from overlay
/// convergence; the overlay existing does not mean routing is live.
fn await_data_plane_ready(results: &mut Vec<HttpResult>) -> Result<(), Box<dyn std::error::Error>> {
    for gateway in CONSUMERS {
        let start = Instant::now();
        loop {
            let result = probe("warmup", gateway, Credential::Valid)?;
            let ready = classify(result.status) == Outcome::Admitted && result.provider_site.is_some();
            note(&format!(
                "warmup {gateway}: status={} site={:?}",
                result.status, result.provider_site
            ));
            results.push(result);
            if ready {
                break;
            }
            if start.elapsed() >= Duration::from_secs(300) {
                return Err(format!("gateway {gateway} not ready (200 + provider-site) within 300s").into());
            }
            std::thread::park_timeout(Duration::from_secs(5));
        }
    }
    Ok(())
}

/// Wait for the sliding window to fully age so the next request starts clean.
///
/// The limiter prunes each identity's settled sorted set lazily, only when a
/// request arrives, so an idle poll for zero entries never completes. Waiting
/// one full window plus a margin guarantees the next request prunes every prior
/// entry. This is a genuine time condition, not a pollable readiness signal, and
/// it never mutates Valkey state.
fn reset_window() -> u64 {
    let deadline = Duration::from_secs(WINDOW_SECS.saturating_add(5));
    let start = Instant::now();
    while start.elapsed() < deadline {
        std::thread::park_timeout(deadline.saturating_sub(start.elapsed()));
    }
    start.elapsed().as_secs()
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn parses_status_and_provider_site_despite_kubectl_pod_deleted_suffix() {
        let raw = "HTTP/1.1 200 OK\r\nX-Grid-Provider-Site: central\r\nConnection: close\r\n\r\npod \"probe-7\" deleted from grid-system namespace\n";
        let (status, site) = parse_probe_output(raw);
        assert_eq!(status, 200);
        assert_eq!(site.as_deref(), Some("central"));
    }

    #[test]
    fn denied_request_has_status_but_no_provider_site() {
        let raw = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 60\r\n\r\npod \"probe-8\" deleted from grid-system namespace\n";
        let (status, site) = parse_probe_output(raw);
        assert_eq!(status, 429);
        assert_eq!(site, None);
    }

    #[test]
    fn classifies_status_codes() {
        assert_eq!(classify(200), Outcome::Admitted);
        assert_eq!(classify(429), Outcome::QuotaDenied);
        assert_eq!(classify(503), Outcome::FailClosed);
        assert_eq!(classify(401), Outcome::Unauthorized);
        assert_eq!(classify(418), Outcome::Other);
    }

    #[test]
    fn gateway_feature_label_requires_quota_and_basic_auth() {
        assert!(declares_required_gateway_features(REQUIRED_GATEWAY_FEATURES));
        assert!(declares_required_gateway_features(
            "http-callout-filter,praxis-filter/basic-auth-filter,token-rate-limit-filter"
        ));
        assert!(!declares_required_gateway_features("token-rate-limit-filter"));
        assert!(!declares_required_gateway_features("praxis-filter/basic-auth-filter"));
    }

    #[test]
    fn missing_credential_sends_no_auth_args() {
        assert!(credential_args(Credential::Missing).is_empty());
        assert_eq!(
            credential_args(Credential::Valid),
            vec!["-u".to_owned(), "alice:alice-secret".to_owned()]
        );
        assert!(credential_args(Credential::Malformed).contains(&"Authorization: Basic not-valid-base64".to_owned()));
    }

    #[test]
    fn curl_pod_overrides_meet_restricted_pod_security() {
        let overrides: serde_json::Value = serde_json::from_str(&curl_pod_overrides(
            "probe",
            &curl_args("http://example.test", Credential::Missing, REQUEST_BODY),
        ))
        .unwrap();
        assert_eq!(
            overrides.pointer("/spec/securityContext/runAsNonRoot"),
            Some(&serde_json::json!(true))
        );
        assert_eq!(
            overrides.pointer("/spec/securityContext/seccompProfile/type"),
            Some(&serde_json::json!("RuntimeDefault"))
        );
        assert_eq!(
            overrides.pointer("/spec/containers/0/securityContext/runAsUser"),
            Some(&serde_json::json!(1000))
        );
        assert_eq!(
            overrides.pointer("/spec/containers/0/securityContext/allowPrivilegeEscalation"),
            Some(&serde_json::json!(false))
        );
        assert_eq!(
            overrides.pointer("/spec/containers/0/securityContext/capabilities/drop/0"),
            Some(&serde_json::json!("ALL"))
        );
    }

    #[test]
    fn redis_response_proves_connectivity_despite_curl_timeout() {
        assert!(redis_connection_reached(false, "-NOAUTH Authentication required"));
        assert!(redis_connection_reached(false, "+PONG"));
        assert!(!redis_connection_reached(
            false,
            "curl: (28) Connection timed out after 3000 milliseconds"
        ));
    }

    #[test]
    fn overlay_requires_three_group_zero_sites() {
        let view = OverlayView {
            revision: "r".into(),
            selection_policy: "roundRobin".into(),
            candidates: CLUSTERS
                .iter()
                .map(|site| OverlayCandidate {
                    site: (*site).into(),
                    stable_id: format!("id-{site}"),
                    selection_group: 0,
                })
                .collect(),
        };
        validate_overlay(&view).unwrap();
    }

    #[test]
    fn overlay_rejects_wrong_policy_or_missing_site() {
        let view = OverlayView {
            revision: "r".into(),
            selection_policy: "deterministic".into(),
            candidates: Vec::new(),
        };
        validate_overlay(&view).unwrap_err();
    }

    fn gate_overlay(revision: &str) -> OverlayView {
        OverlayView {
            revision: revision.into(),
            selection_policy: "roundRobin".into(),
            candidates: CLUSTERS
                .iter()
                .map(|site| OverlayCandidate {
                    site: (*site).into(),
                    stable_id: format!("id-{site}"),
                    selection_group: 0,
                })
                .collect(),
        }
    }

    fn gate_observation(gateway: &str, grid: &str, accepted: &str, serving: &str) -> ConsumerObservation {
        ConsumerObservation {
            gateway: gateway.into(),
            overlay: Some(gate_overlay(grid)),
            praxis_accepted: Some(accepted.into()),
            praxis_serving: Some(serving.into()),
        }
    }

    #[test]
    fn serving_gate_ready_on_exact_accepted_serving_match() {
        let observations = vec![
            gate_observation(CONSUMER_A, "revA", "revA", "revA"),
            gate_observation(CONSUMER_B, "revA", "revA", "revA"),
        ];
        let (overlays, revision) = convergence_ready(&observations).unwrap();
        assert_eq!(revision, "revA");
        assert_eq!(overlays.len(), 2);
    }

    #[test]
    fn serving_gate_not_ready_when_one_consumer_serves_older_revision() {
        let observations = vec![
            gate_observation(CONSUMER_A, "revA", "revA", "revA"),
            gate_observation(CONSUMER_B, "revA", "revA", "revOLD"),
        ];
        assert!(convergence_ready(&observations).is_none());
    }

    #[test]
    fn serving_gate_not_ready_when_consumers_serve_different_revisions() {
        let observations = vec![
            gate_observation(CONSUMER_A, "rev1", "rev1", "rev1"),
            gate_observation(CONSUMER_B, "rev2", "rev2", "rev2"),
        ];
        assert!(convergence_ready(&observations).is_none());
    }

    #[test]
    fn latest_log_field_handles_ansi_colored_output() {
        let raw = "\x1b[32mserving_revision=abc123\x1b[0m";
        let logs = strip_csi_sgr(raw);
        assert_eq!(latest_log_field(&logs, "serving_revision").as_deref(), Some("abc123"));
    }

    #[test]
    fn latest_log_field_handles_quoted_and_unquoted_values() {
        assert_eq!(
            latest_log_field("accepted_revision=plain rest", "accepted_revision").as_deref(),
            Some("plain")
        );
        assert_eq!(
            latest_log_field("accepted_revision=\"quoted value\"", "accepted_revision").as_deref(),
            Some("quoted value")
        );
    }

    #[test]
    fn latest_log_field_rejects_previous_serving_revision_false_match() {
        let line = "previous_serving_revision=OLD serving_revision=NEW retained_serving_revision=KEEP";
        assert_eq!(latest_log_field(line, "serving_revision").as_deref(), Some("NEW"));
        assert_eq!(
            latest_log_field(line, "previous_serving_revision").as_deref(),
            Some("OLD")
        );
    }

    #[test]
    fn timeout_diagnostics_include_both_consumer_revisions() {
        let observations = vec![
            gate_observation(CONSUMER_A, "revAaaaa", "revAaaaa", "revAaaaa"),
            gate_observation(CONSUMER_B, "revBbbbb", "revBbbbb", "revBstale"),
        ];
        let diagnostics = convergence_diagnostics(&observations);
        assert!(diagnostics.contains(CONSUMER_A));
        assert!(diagnostics.contains(CONSUMER_B));
        assert!(diagnostics.contains("revAaaaa"));
        assert!(diagnostics.contains("revBstale"));
    }

    #[test]
    fn http_result_round_trips() {
        let result = HttpResult {
            label: "x".into(),
            status: 429,
            provider_site: None,
        };
        let encoded = serde_json::to_string(&result).unwrap();
        assert_eq!(serde_json::from_str::<HttpResult>(&encoded).unwrap(), result);
    }

    #[test]
    fn only_missing_http_response_is_transport_retryable() {
        assert!(is_transport_failure(0));
        for status in [200, 401, 429, 503] {
            assert!(!is_transport_failure(status));
        }
    }

    #[test]
    fn explicit_run_id_validation_is_strict() {
        for valid in ["quota-a1b2c3", "run1", "a-b"] {
            validate_run_id(valid).unwrap();
        }
        for invalid in [
            "",
            "-leading",
            "trailing-",
            "Upper",
            "has_under",
            "a.b",
            "1234567890123456789012345",
        ] {
            assert!(validate_run_id(invalid).is_err(), "{invalid} must be rejected");
        }
    }

    #[test]
    fn run_identity_separates_physical_names_from_logical_sites() {
        let names = RunIdentity::new("quota-a1b2c3");
        assert_eq!(names.kind_cluster("west"), "grid-token-rate-limit-quota-a1b2c3-west");
        assert_eq!(names.context("west"), "kind-grid-token-rate-limit-quota-a1b2c3-west");
        assert_eq!(names.network, "grid-token-rate-limit-quota-a1b2c3-net");
        assert_eq!(CLUSTERS, ["west", "central", "east"]);
        assert!(names.kind_clusters.values().all(|value| value.len() <= 63));
        assert!(names.network.len() <= 63);
    }

    #[test]
    fn generated_run_ids_are_valid_and_distinct() {
        let first = generated_run_id(0).unwrap();
        let second = generated_run_id(1).unwrap();
        validate_run_id(&first).unwrap();
        validate_run_id(&second).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn materialized_config_contains_run_identity_and_preserves_logical_resources() {
        let mut config: serde_yaml::Value = serde_yaml::from_str(
            "apiVersion: forge.praxis.dev/v1alpha1\nkind: Environment\nmetadata:\n  name: grid-token-rate-limit\nspec:\n  runtime:\n    provider: docker\n    clusterPrefix: grid-token-rate-limit\n  clusters: []\n",
        )
        .unwrap();
        let names = RunIdentity::new("quota-a1b2c3");
        apply_run_identity(&mut config, &names);
        let mut context =
            serde_yaml::Value::String("kubectl --context kind-grid-token-rate-limit-west get pods".to_owned());
        rewrite_context_references(&mut context, &names);
        rewrite_context_references(&mut config, &names);
        let rendered = serde_yaml::to_string(&config).unwrap();
        assert!(rendered.contains("name: grid-token-rate-limit-quota-a1b2c3"));
        assert!(rendered.contains("clusterPrefix: grid-token-rate-limit-quota-a1b2c3"));
        assert!(!rendered.contains("clusterPrefix: grid-token-rate-limit\n"));
        assert_eq!(
            context.as_str(),
            Some("kubectl --context kind-grid-token-rate-limit-quota-a1b2c3-west get pods")
        );
        let mut template = serde_yaml::Value::String("kind-grid-token-rate-limit-{{ cluster.name }}".to_owned());
        rewrite_context_references(&mut template, &names);
        assert_eq!(
            template.as_str(),
            Some("kind-grid-token-rate-limit-quota-a1b2c3-{{ cluster.name }}")
        );
    }

    #[test]
    fn run_state_rewrite_changes_exec_paths_but_not_template_targets() {
        let mut config: serde_yaml::Value = serde_yaml::from_str(
            "spec:\n  stacks:\n    test:\n      steps:\n        - type: template-file\n          target: .forge/runtime/west/provider/praxis.yaml\n          source: source.yaml\n        - type: exec\n          command: [bash, -c, 'cat .forge/runtime/west/provider/praxis.yaml']\n",
        )
        .unwrap();
        rewrite_exec_state_references(&mut config, Path::new("topology/.forge.quota-a1b2c3"));
        let rendered = serde_yaml::to_string(&config).unwrap();
        assert!(rendered.contains("target: .forge/runtime/west/provider/praxis.yaml"));
        assert!(rendered.contains("cat topology/.forge.quota-a1b2c3/runtime/west/provider/praxis.yaml"));
    }
}
