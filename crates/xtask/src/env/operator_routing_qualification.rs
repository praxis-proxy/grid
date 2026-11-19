//! Focused deployed qualification for operator-derived local dials.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Default Forge topology used by this qualification.
const DEFAULT_TOPOLOGY: &str = "tests/e2e/topologies/grid-operator-routing/forge.yaml";
/// Namespace containing the run-owned Gateway and providers.
const NAMESPACE: &str = "grid-system";
/// Forge network name used by the topology.
const NETWORK: &str = "grid-operator-routing";
/// Embedded Gateway Deployment name.
const GATEWAY: &str = "operator-routing-gateway";
/// Serving `ConfigMap` generated for the embedded Gateway.
const GRID_SERVING_CONFIGMAP: &str = "grid-serving-grid-operator-routing-operator-routing-gateway";
/// Forge stack application order for the test topology.
const STACK_APPLICATION_ORDER: [&str; 4] = ["operator", "mock-providers", "grid-resources", "embedded-gateway"];
/// Grid backend ID for Provider A.
const PROVIDER_A: &str = "provider-a";
/// Grid backend ID for Provider B.
const PROVIDER_B: &str = "provider-b";
/// Inference model used by probe requests.
const MODEL: &str = "operator-routing-test-model";
/// Curl client image tag loaded into the run-owned cluster.
const CLIENT_IMAGE: &str = "curlimages/curl:8.10.1";
/// Immutable source for the pinned curl client image.
const CLIENT_IMAGE_REF: &str =
    "curlimages/curl@sha256:fcff5cf7a4b895da7bd2933c914938db2b05d2113fa0d6c55b6d29930408f661";
/// Expected digest for the pinned curl client image.
const CLIENT_IMAGE_DIGEST: &str = "sha256:fcff5cf7a4b895da7bd2933c914938db2b05d2113fa0d6c55b6d29930408f661";
/// In-cluster Gateway endpoint used for inference probes.
const GATEWAY_SERVICE: &str = "http://operator-routing-gateway.grid-system.svc:8080/v1/chat/completions";
/// Maximum time to wait for qualification convergence.
const WAIT_TIMEOUT: Duration = Duration::from_secs(150);
/// Maximum duration for a single Gateway request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
/// Poll interval for Kubernetes state and Gateway requests.
const POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Marker emitted by the curl probe to report its HTTP status.
const REQUEST_MARKER: &str = "OPERATOR_ROUTING_HTTP_STATUS:";
/// Maximum captured bytes retained from either subprocess output stream.
const MAX_CAPTURED_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
/// Grace period between terminating and killing a subprocess group.
const PROCESS_GROUP_TERM_GRACE: Duration = Duration::from_millis(500);
/// Maximum time allowed for subprocess output readers to finish after cleanup.
const OUTPUT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

/// Options for the operator-routing Kind qualification command.
#[derive(Debug, clap::Args)]
pub(crate) struct Options {
    /// Forge topology to deploy.
    #[arg(long, default_value = DEFAULT_TOPOLOGY)]
    pub(crate) forge_config: PathBuf,
    /// Delete the run-owned Kind cluster after completion.
    pub(crate) teardown: bool,
    /// Retain the run-owned cluster after failure when teardown was requested.
    pub(crate) keep_on_failure: bool,
    /// Evidence output directory, or a generated timestamped directory.
    pub(crate) evidence_dir: Option<PathBuf>,
}

/// One named qualification phase and its observed outcome.
#[derive(Debug, Serialize)]
struct Scenario {
    /// Stable phase identifier.
    name: String,
    /// Serialized phase outcome.
    result: Outcome,
    /// Human-readable observation or failure detail.
    detail: String,
    /// Unix-millisecond timestamp when the phase started.
    started_at_unix_ms: u128,
    /// Unix-millisecond timestamp when the phase completed.
    completed_at_unix_ms: u128,
}

/// Serialized outcome used by top-level and per-phase evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Outcome {
    /// Qualification is still in progress.
    Running,
    /// Qualification phase completed successfully.
    Pass,
    /// Qualification phase failed.
    Fail,
}

/// Machine-readable evidence emitted by one qualification run.
#[derive(Debug, Serialize)]
struct Evidence {
    /// Evidence schema version.
    schema_version: u8,
    /// Overall qualification outcome.
    result: Outcome,
    /// Forge topology used by the run.
    topology: String,
    /// Git revision used to build the qualification images.
    source_revision: String,
    /// SHA-256 fingerprint including tracked and untracked source changes.
    source_fingerprint_sha256: String,
    /// Run-owned Kind cluster name.
    cluster_name: String,
    /// Explicit kubectl context for the run-owned cluster.
    kubectl_context: String,
    /// Image references used by each workload.
    image_references: BTreeMap<String, String>,
    /// Immutable local image IDs and runtime IDs.
    image_ids: BTreeMap<String, String>,
    /// Ordered qualification phase outcomes.
    scenarios: Vec<Scenario>,
    /// Additional phase-specific observations.
    observations: BTreeMap<String, serde_json::Value>,
    /// Run-owned resource cleanup result.
    cleanup: String,
}

/// Local image references and immutable identities for this run.
struct Images {
    /// Locally built operator image reference.
    operator: String,
    /// Locally built embedded Gateway image reference.
    grid_gateway: String,
    /// Locally built provider recorder image reference.
    mock_provider: String,
    /// Kubernetes image pull policy for local images.
    pull_policy: String,
    /// Docker image IDs captured before cluster provisioning.
    local_ids: BTreeMap<String, String>,
}

/// Names and paths scoped to one qualification run.
struct RunContext {
    /// Resolved Forge executable path.
    forge: String,
    /// Run-scoped materialized Forge configuration.
    resolved_config: PathBuf,
    /// Run-scoped Forge state directory.
    state_dir: PathBuf,
    /// Directory containing this run's evidence.
    evidence_dir: PathBuf,
    /// Explicit kubectl context for the run-owned cluster.
    context: String,
}

/// One response probe and the backend attribution returned by Grid.
#[derive(Debug, Clone, Serialize)]
struct Probe {
    /// HTTP status returned by the in-cluster Gateway.
    status: Option<u16>,
    /// Grid backend attribution returned by the Gateway.
    grid_backend: Option<String>,
    /// Grid site attribution returned by the Gateway.
    grid_site: Option<String>,
    /// Independent mock-provider identity returned by the recorder.
    mock_provider: Option<String>,
    /// Request transport or decoding error, if any.
    error: Option<String>,
}

impl Probe {
    /// Whether the response is successful and has complete local attribution.
    fn is_success(&self) -> bool {
        self.status == Some(200)
            && self.grid_backend.is_some()
            && self.grid_site.as_deref() == Some("local")
            && self
                .mock_provider
                .as_deref()
                .is_some_and(|provider| !provider.is_empty())
    }
}

/// Return the current Unix timestamp in milliseconds.
fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

/// Create a process-unique identifier for a qualification run.
fn run_id() -> String {
    format!("{}-{}", unix_millis(), std::process::id())
}

/// Derive this run's Kind cluster name from its unique suffix.
fn cluster_name(suffix: &str) -> String {
    format!("grid-operator-routing-{suffix}-local")
}

/// Select the requested evidence path or create a topology-local default.
fn evidence_dir(topology: &Path, requested: Option<&Path>, run_id: &str) -> PathBuf {
    requested.map_or_else(
        || {
            topology
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("evidence")
                .join(format!("operator-routing-{run_id}"))
        },
        Path::to_path_buf,
    )
}

/// Read an image override or use its qualification default.
fn source_value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// Inspect a local Docker image and return its immutable content ID.
fn image_id(image: &str) -> Result<String, String> {
    let mut command = Command::new("docker");
    command.args(["image", "inspect", "--format", "{{.Id}}", image]);
    let output = bounded_command(command, Duration::from_secs(30), "inspect local image")?;
    if !output.status.success() {
        return Err(format!("local image {image:?} is unavailable: {}", stderr(&output)));
    }
    let id = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !id.starts_with("sha256:") {
        return Err(format!("image inspect returned no immutable ID for {image:?}: {id:?}"));
    }
    Ok(id)
}

/// Verify that a local image carries the required repository digest.
fn verify_image_digest(image: &str, expected_digest: &str) -> Result<(), String> {
    let mut command = Command::new("docker");
    command.args(["image", "inspect", "--format", "{{json .RepoDigests}}", image]);
    let output = bounded_command(command, Duration::from_secs(30), "verify pinned fixture image digest")?;
    if !output.status.success() {
        return Err(format!("inspect pinned image {image:?}: {}", stderr(&output)));
    }
    let repo_digests: Vec<String> = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("parse repository digests for {image:?}: {error}"))?;
    if !has_repo_digest(&repo_digests, expected_digest) {
        return Err(format!(
            "local fixture image {image:?} does not carry pinned digest {expected_digest}; found {repo_digests:?}"
        ));
    }
    Ok(())
}

/// Check whether any repository digest reference ends with the expected digest.
fn has_repo_digest(references: &[String], expected_digest: &str) -> bool {
    let expected = format!("@{expected_digest}");
    references.iter().any(|reference| reference.ends_with(&expected))
}

/// Resolve and inspect the locally built images selected for this run.
fn resolve_images() -> Result<Images, String> {
    let pull_policy = source_value("GRID_XTASK_IMAGE_PULL_POLICY", "Never");
    let operator = source_value("GRID_XTASK_OPERATOR_IMAGE", "grid-operator:operator-routing");
    let grid_gateway = source_value("GRID_XTASK_GRID_GATEWAY_IMAGE", "grid-gateway:operator-routing");
    let mock_provider = source_value("GRID_XTASK_MOCK_PROVIDER_IMAGE", "grid-mock-providers:operator-routing");
    if pull_policy == "Never"
        && (env::var_os("GRID_XTASK_OPERATOR_IMAGE").is_none()
            || env::var_os("GRID_XTASK_GRID_GATEWAY_IMAGE").is_none()
            || env::var_os("GRID_XTASK_MOCK_PROVIDER_IMAGE").is_none())
    {
        return Err("when GRID_XTASK_IMAGE_PULL_POLICY=Never, set GRID_XTASK_OPERATOR_IMAGE, GRID_XTASK_GRID_GATEWAY_IMAGE, and GRID_XTASK_MOCK_PROVIDER_IMAGE to source-built tags".to_owned());
    }
    verify_image_digest(CLIENT_IMAGE, CLIENT_IMAGE_DIGEST)?;
    let local_ids = BTreeMap::from([
        ("operator".to_owned(), image_id(&operator)?),
        ("grid_gateway".to_owned(), image_id(&grid_gateway)?),
        ("mock_provider".to_owned(), image_id(&mock_provider)?),
        ("client".to_owned(), image_id(CLIENT_IMAGE)?),
    ]);
    Ok(Images {
        operator,
        grid_gateway,
        mock_provider,
        pull_policy,
        local_ids,
    })
}

/// Split a tagged image reference while preserving registry ports.
fn split_image(image: &str) -> (String, String) {
    let last_slash = image.rfind('/').unwrap_or(0);
    image.rfind(':').filter(|colon| *colon > last_slash).map_or_else(
        || (image.to_owned(), "latest".to_owned()),
        |colon| {
            let repository = image.get(..colon).unwrap_or(image);
            let tag = image.get(colon.saturating_add(1)..).unwrap_or("latest");
            (repository.to_owned(), tag.to_owned())
        },
    )
}

/// Create a run-scoped Forge config with unique image references and cluster names.
fn materialize_config(source: &Path, images: &Images, suffix: &str) -> Result<PathBuf, String> {
    let source_text = fs::read_to_string(source).map_err(|error| format!("read Forge topology: {error}"))?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(&source_text).map_err(|error| error.to_string())?;
    scope_environment_name(&mut config, suffix)?;
    let runtime = config
        .get_mut("spec")
        .and_then(|spec| spec.get_mut("runtime"))
        .and_then(serde_yaml::Value::as_mapping_mut)
        .ok_or("Forge topology has no spec.runtime mapping")?;
    runtime.insert(
        serde_yaml::Value::String("clusterPrefix".to_owned()),
        serde_yaml::Value::String(format!("grid-operator-routing-{suffix}")),
    );
    let clusters = config
        .get_mut("spec")
        .and_then(|spec| spec.get_mut("clusters"))
        .and_then(serde_yaml::Value::as_sequence_mut)
        .ok_or("Forge topology has no spec.clusters list")?;
    for cluster in clusters {
        let properties = cluster
            .get_mut("properties")
            .and_then(serde_yaml::Value::as_mapping_mut)
            .ok_or("Forge cluster has no properties mapping")?;
        for (prefix, image) in [
            ("operator", images.operator.as_str()),
            ("gateway", images.grid_gateway.as_str()),
            ("mockProvider", images.mock_provider.as_str()),
        ] {
            let (repository, tag) = split_image(image);
            for (key, value) in [
                (format!("{prefix}Image"), image.to_owned()),
                (format!("{prefix}ImageRepo"), repository.clone()),
                (format!("{prefix}ImageTag"), tag.clone()),
            ] {
                properties.insert(serde_yaml::Value::String(key), serde_yaml::Value::String(value));
            }
        }
        properties.insert(
            serde_yaml::Value::String("imagePullPolicy".to_owned()),
            serde_yaml::Value::String(images.pull_policy.clone()),
        );
    }
    let parent = source.parent().ok_or("Forge topology has no parent directory")?;
    let destination = parent.join(format!(".forge.resolved-{suffix}.yaml"));
    let serialized = serde_yaml::to_string(&config).map_err(|error| error.to_string())?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&destination)
        .map_err(|error| {
            format!(
                "create unique resolved Forge topology {}: {error}",
                destination.display()
            )
        })?;
    file.write_all(serialized.as_bytes())
        .map_err(|error| format!("write resolved Forge topology: {error}"))?;
    Ok(destination)
}

/// Create a new evidence directory without overwriting a previous run.
fn create_evidence_dir(directory: &Path) -> Result<(), String> {
    let parent = directory
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| format!("create evidence parent {}: {error}", parent.display()))?;
    fs::create_dir(directory).map_err(|error| {
        format!(
            "create unique evidence directory {} (refusing to overwrite existing evidence): {error}",
            directory.display()
        )
    })
}

/// Add the unique run suffix to the Forge environment name.
fn scope_environment_name(config: &mut serde_yaml::Value, suffix: &str) -> Result<(), String> {
    let metadata = config
        .get_mut("metadata")
        .and_then(serde_yaml::Value::as_mapping_mut)
        .ok_or("Forge topology has no metadata mapping")?;
    let name_key = serde_yaml::Value::String("name".to_owned());
    let name = metadata
        .get(&name_key)
        .and_then(serde_yaml::Value::as_str)
        .ok_or("Forge topology has no metadata.name")?;
    let scoped = format!("{name}-{suffix}");
    if scoped.len() > 63 {
        return Err(format!(
            "run-scoped Forge environment name exceeds 63 characters: {scoped}"
        ));
    }
    metadata.insert(name_key, serde_yaml::Value::String(scoped));
    Ok(())
}

/// Run a subprocess with bounded execution and output collection.
fn bounded_command(mut command: Command, timeout: Duration, label: &str) -> Result<Output, String> {
    let started = Instant::now();
    let mut stdout_file =
        tempfile::NamedTempFile::new().map_err(|error| format!("create {label} stdout capture: {error}"))?;
    let mut stderr_file =
        tempfile::NamedTempFile::new().map_err(|error| format!("create {label} stderr capture: {error}"))?;
    command
        .stdout(Stdio::from(
            stdout_file
                .reopen()
                .map_err(|error| format!("open {label} stdout capture: {error}"))?,
        ))
        .stderr(Stdio::from(
            stderr_file
                .reopen()
                .map_err(|error| format!("open {label} stderr capture: {error}"))?,
        ));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;

        // A private process group lets timeout and orphan cleanup target only
        // this command and any descendants it spawned.
        command.process_group(0);
    }
    let mut child = command.spawn().map_err(|error| format!("start {label}: {error}"))?;
    let (status, timed_out) = loop {
        match child.try_wait() {
            Ok(Some(status)) => break (status, false),
            Ok(None) if started.elapsed() < timeout => thread::park_timeout(Duration::from_millis(50)),
            Ok(None) => {
                terminate_command_group(&mut child, label)?;
                let status = wait_for_child_exit(&mut child, OUTPUT_CLEANUP_TIMEOUT, label)?;
                break (status, true);
            },
            Err(error) => {
                drop(terminate_command_group(&mut child, label));
                return Err(format!("wait for {label}: {error}"));
            },
        }
    };
    // A successful parent can leave background descendants behind. They share
    // its isolated process group, so stop them before collecting the files.
    let orphaned_descendants = !timed_out && process_group_exists(child.id())?;
    if orphaned_descendants {
        terminate_process_group(&mut child, label)?;
    }
    let stdout_bytes = read_captured_output(&mut stdout_file, label, "stdout")?;
    let stderr_bytes = read_captured_output(&mut stderr_file, label, "stderr")?;
    let output = Output {
        status,
        stdout: stdout_bytes,
        stderr: stderr_bytes,
    };
    if timed_out {
        return Err(format!(
            "{label} timed out after {timeout:?}: {}",
            output_diagnostics(&output)
        ));
    }
    if orphaned_descendants {
        return Err(format!(
            "{label} exited with status {} but left descendants in its process group; terminated them. stderr: {}",
            output.status,
            output_diagnostics(&output)
        ));
    }
    Ok(output)
}

/// Read bounded command output while retaining a useful truncation marker.
fn read_captured_output(file: &mut tempfile::NamedTempFile, label: &str, stream: &str) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    file.as_file_mut()
        .take((MAX_CAPTURED_OUTPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read {label} {stream}: {error}"))?;
    if bytes.len() > MAX_CAPTURED_OUTPUT_BYTES {
        bytes.truncate(MAX_CAPTURED_OUTPUT_BYTES);
        bytes.extend_from_slice(b"\n[output truncated after 4 MiB]\n");
    }
    Ok(bytes)
}

/// Stop a command's private process group and verify that it exits promptly.
#[cfg(unix)]
fn terminate_command_group(child: &mut std::process::Child, label: &str) -> Result<(), String> {
    let result = terminate_process_group(child, label);
    drop(child.kill());
    result
}

/// Kill the direct child on platforms without process-group support.
#[cfg(not(unix))]
fn terminate_command_group(child: &mut std::process::Child, label: &str) -> Result<(), String> {
    child.kill().map_err(|error| format!("terminate {label}: {error}"))
}

/// Send TERM, then KILL if needed, to a command-owned process group.
#[cfg(unix)]
fn terminate_process_group(child: &mut std::process::Child, label: &str) -> Result<(), String> {
    let process_id = child.id();
    signal_process_group(process_id, "TERM", label)?;
    let grace_deadline = Instant::now() + PROCESS_GROUP_TERM_GRACE;
    while Instant::now() < grace_deadline {
        poll_child_for_cleanup(child, label)?;
        if !process_group_exists(process_id)? {
            break;
        }
        thread::park_timeout(Duration::from_millis(25));
    }
    poll_child_for_cleanup(child, label)?;
    if process_group_exists(process_id)? {
        signal_process_group(process_id, "KILL", label)?;
    }
    let cleanup_deadline = Instant::now() + OUTPUT_CLEANUP_TIMEOUT;
    while Instant::now() < cleanup_deadline {
        poll_child_for_cleanup(child, label)?;
        if !process_group_exists(process_id)? {
            break;
        }
        thread::park_timeout(Duration::from_millis(25));
    }
    if process_group_exists(process_id)? {
        return Err(format!(
            "{label} process group {process_id} remained after TERM and KILL"
        ));
    }
    Ok(())
}

/// Reap the direct child while process-group cleanup waits for descendants.
#[cfg(unix)]
fn poll_child_for_cleanup(child: &mut std::process::Child, label: &str) -> Result<(), String> {
    child
        .try_wait()
        .map(|_| ())
        .map_err(|error| format!("wait for {label} while terminating descendants: {error}"))
}

/// Send a signal to a negative process-group ID using the system `kill` utility.
#[cfg(unix)]
fn signal_process_group(process_id: u32, signal: &str, label: &str) -> Result<(), String> {
    let group = format!("-{process_id}");
    let output = Command::new("kill")
        .args([format!("-{signal}"), "--".to_owned(), group])
        .output()
        .map_err(|error| format!("signal {label} process group with {signal}: {error}"))?;
    // Exit status 1 means the group already exited; other failures retain the
    // utility's diagnostic rather than pretending cleanup succeeded.
    if !output.status.success() && output.status.code() != Some(1) {
        return Err(format!(
            "signal {label} process group with {signal}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

/// Whether a command-owned process group still has members.
#[cfg(unix)]
fn process_group_exists(process_id: u32) -> Result<bool, String> {
    let group = format!("-{process_id}");
    let output = Command::new("kill")
        .args(["-0", "--", &group])
        .output()
        .map_err(|error| format!("check subprocess process group {process_id}: {error}"))?;
    Ok(output.status.success())
}

/// Non-Unix platforms cannot inspect a subprocess group through this helper.
#[cfg(not(unix))]
fn process_group_exists(_process_id: u32) -> Result<bool, String> {
    Ok(false)
}

/// Wait for a terminated child without using an unbounded blocking wait.
fn wait_for_child_exit(
    child: &mut std::process::Child,
    timeout: Duration,
    label: &str,
) -> Result<std::process::ExitStatus, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().map_err(|error| format!("wait for {label}: {error}"))? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{label} child did not exit within {timeout:?} after termination"
            ));
        }
        thread::park_timeout(Duration::from_millis(25));
    }
}

/// Return trimmed stderr text for subprocess diagnostics.
fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).trim().to_owned()
}

/// Format both bounded output streams for process failure diagnostics.
fn output_diagnostics(output: &Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let stderr = stderr(output);
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => "no command output".to_owned(),
        (false, true) => format!("stdout: {stdout}"),
        (true, false) => format!("stderr: {stderr}"),
        (false, false) => format!("stdout: {stdout}\nstderr: {stderr}"),
    }
}

/// Convert a Kind cluster name into its explicit kubectl context name.
fn context_for(cluster: &str) -> String {
    format!("kind-{cluster}")
}

/// Run kubectl against this run's explicit context with a deadline.
fn kubectl(ctx: &RunContext, args: &[&str], timeout: Duration) -> Result<Output, String> {
    let mut command = Command::new("kubectl");
    command.args(["--context", &ctx.context]);
    command.args(args);
    bounded_command(command, timeout, "kubectl")
}

/// Run kubectl and return stdout only when the command succeeds.
fn kubectl_ok(ctx: &RunContext, args: &[&str], timeout: Duration) -> Result<String, String> {
    let output = kubectl(ctx, args, timeout)?;
    if !output.status.success() {
        return Err(format!("kubectl {:?} failed: {}", args, stderr(&output)));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Run kubectl with JSON output and parse the returned object.
fn json_kubectl(ctx: &RunContext, args: &[&str]) -> Result<serde_json::Value, String> {
    let mut owned = args.to_vec();
    owned.extend(["-o", "json"]);
    serde_json::from_str(&kubectl_ok(ctx, &owned, Duration::from_secs(20))?).map_err(|error| error.to_string())
}

#[expect(
    clippy::disallowed_methods,
    reason = "bounded synchronous polling for external process state"
)]
/// Poll a Kubernetes observation until it succeeds or reaches its deadline.
fn wait_until<T>(
    description: &str,
    timeout: Duration,
    mut check: impl FnMut() -> Result<Option<T>, String>,
) -> Result<T, String> {
    let started = Instant::now();
    loop {
        let last = match check() {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => "desired state not observed".to_owned(),
            Err(error) => error,
        };
        if started.elapsed() >= timeout {
            return Err(format!("timed out waiting for {description}: {last}"));
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Verify expected image references and collect image IDs for selected Deployments.
fn image_references_and_runtime_ids(
    ctx: &RunContext,
    images: &Images,
    deployments: &[&str],
) -> Result<BTreeMap<String, serde_json::Value>, String> {
    let mut evidence = BTreeMap::new();
    for &deployment in deployments {
        let value = json_kubectl(ctx, &["-n", NAMESPACE, "get", "deployment", deployment])?;
        let image = value
            .pointer("/spec/template/spec/containers/0/image")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("Deployment {deployment} has no container image"))?;
        let expected_image = match deployment {
            "grid-operator" => &images.operator,
            GATEWAY => &images.grid_gateway,
            _ => &images.mock_provider,
        };
        if image != expected_image {
            return Err(format!(
                "Deployment {deployment} uses {image:?}, expected source-selected image {expected_image:?}"
            ));
        }
        let selector = value
            .pointer("/spec/selector/matchLabels")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| format!("Deployment {deployment} has no selector"))?;
        let label_selector = selector
            .iter()
            .map(|(key, label_value)| format!("{key}={}", label_value.as_str().unwrap_or_default()))
            .collect::<Vec<_>>()
            .join(",");
        let pods = json_kubectl(ctx, &["-n", NAMESPACE, "get", "pods", "-l", &label_selector])?;
        let items = pods
            .get("items")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("pod list for {deployment} is invalid"))?;
        let pod = items
            .iter()
            .find(|pod| pod.pointer("/status/phase").and_then(serde_json::Value::as_str) == Some("Running"))
            .ok_or_else(|| format!("no Running pod for Deployment {deployment}"))?;
        let container = pod
            .pointer("/status/containerStatuses/0")
            .ok_or_else(|| format!("pod for {deployment} has no container status"))?;
        let image_id = container
            .get("imageID")
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| format!("pod for {deployment} has no runtime imageID"))?;
        let ready = container.get("ready").and_then(serde_json::Value::as_bool) == Some(true);
        if !ready {
            return Err(format!("pod for {deployment} is not Ready"));
        }
        evidence.insert(
            deployment.to_owned(),
            serde_json::json!({
                "pod": pod.pointer("/metadata/name"),
                "podUid": pod.pointer("/metadata/uid"),
                "image": image,
                "imageID": image_id,
                "ready": ready,
            }),
        );
    }
    let client = json_kubectl(ctx, &["-n", NAMESPACE, "get", "pod", "qualification-client"])?;
    let client_image = client
        .pointer("/spec/containers/0/image")
        .and_then(serde_json::Value::as_str)
        .ok_or("qualification client Pod has no image")?;
    let client_status = client
        .pointer("/status/containerStatuses/0")
        .ok_or("qualification client Pod has no container status")?;
    let client_image_id = client_status
        .get("imageID")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .ok_or("qualification client Pod has no runtime imageID")?;
    if client_image != CLIENT_IMAGE || client_status.get("ready").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(format!(
            "qualification client Pod is not Ready with expected image {CLIENT_IMAGE:?}: image={client_image:?}"
        ));
    }
    evidence.insert(
        "qualification-client".to_owned(),
        serde_json::json!({
            "pod": client.pointer("/metadata/name"),
            "podUid": client.pointer("/metadata/uid"),
            "image": client_image,
            "imageID": client_image_id,
            "ready": true,
        }),
    );
    Ok(evidence)
}

/// Generate and install the short-lived Grid identity Secret for the Gateway.
fn create_gateway_identity_secret(ctx: &RunContext) -> Result<String, String> {
    let certificates = tempfile::tempdir().map_err(|error| format!("create ephemeral identity directory: {error}"))?;
    let sites = vec!["local".to_owned()];
    super::certs::generate_all_in_dir(&sites, certificates.path())
        .map_err(|error| format!("generate ephemeral Grid Gateway identity: {error}"))?;

    let ca = certificates.path().join("ca.pem");
    let cert = certificates.path().join("local-cert.pem");
    let key = certificates.path().join("local-key.pem");
    let mut command = Command::new("kubectl");
    command.args([
        "--context",
        &ctx.context,
        "-n",
        NAMESPACE,
        "create",
        "secret",
        "generic",
        "operator-routing-gateway-tls",
    ]);
    command.arg(format!("--from-file=ca.crt={}", ca.display()));
    command.arg(format!("--from-file=tls.crt={}", cert.display()));
    command.arg(format!("--from-file=tls.key={}", key.display()));
    let output = bounded_command(
        command,
        Duration::from_secs(30),
        "create ephemeral Grid Gateway identity Secret",
    )?;
    if !output.status.success() {
        return Err(format!(
            "create Grid Gateway identity Secret failed: {}",
            stderr(&output)
        ));
    }
    Ok("created run-local grid identity Secret; certificate and key material discarded".to_owned())
}

/// Wait for the provider to report the expected current-generation availability.
fn wait_provider_available(
    ctx: &RunContext,
    provider: &str,
    expected_endpoint: &str,
) -> Result<serde_json::Value, String> {
    wait_until(
        &format!("InferenceProvider/{provider} current-generation Available"),
        WAIT_TIMEOUT,
        || {
            let value = json_kubectl(ctx, &["get", "inferenceprovider", provider])?;
            let generation = value
                .pointer("/metadata/generation")
                .and_then(serde_json::Value::as_i64);
            let observed = value
                .pointer("/status/observedGeneration")
                .and_then(serde_json::Value::as_i64);
            let endpoint = value
                .pointer("/spec/endpoint")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let phase = value
                .pointer("/status/phase")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if generation.is_some_and(|generation| generation > 0)
                && generation == observed
                && endpoint == expected_endpoint
                && phase == "Available"
            {
                Ok(Some(value))
            } else {
                Ok(None)
            }
        },
    )
}

/// Require a provider entry to publish the exact expected dial address.
fn require_dial(config: &serde_json::Value, provider: &str, expected_address: &str) -> Result<(), String> {
    let dial = config
        .pointer(&format!("/dial/{provider}"))
        .ok_or_else(|| format!("dial.json has no dial for {provider}"))?;
    let actual = dial
        .get("address")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if actual != expected_address {
        return Err(format!(
            "dial for {provider} points to {actual:?}, expected {expected_address:?}"
        ));
    }
    Ok(())
}

/// Read the operator's published and Gateway-mounted dial documents.
fn mounted_dial_config(ctx: &RunContext) -> Result<serde_json::Value, String> {
    let command = [
        "exec",
        "-n",
        NAMESPACE,
        "deployment/operator-routing-gateway",
        "-c",
        "praxis",
        "--",
        "cat",
        "/etc/praxis/grid-serving/dial.json",
    ];
    let output = kubectl(ctx, &command, Duration::from_secs(15))?;
    if !output.status.success() {
        return Err(format!("read mounted dial.json: {}", stderr(&output)));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| format!("mounted dial.json is invalid: {error}"))
}

/// Wait until published and mounted configs both contain the expected provider dial.
fn wait_for_dial(ctx: &RunContext, provider: &str, expected: &str) -> Result<serde_json::Value, String> {
    wait_until(
        &format!("operator and Gateway dial for {provider}={expected}"),
        WAIT_TIMEOUT,
        || {
            let configmap = json_kubectl(ctx, &["-n", NAMESPACE, "get", "configmap", GRID_SERVING_CONFIGMAP])?;
            let published_text = configmap
                .pointer("/data/dial.json")
                .and_then(serde_json::Value::as_str)
                .ok_or("operator serving ConfigMap has no dial.json")?;
            let published: serde_json::Value = serde_json::from_str(published_text)
                .map_err(|error| format!("operator dial.json is not valid JSON: {error}"))?;
            let mounted = mounted_dial_config(ctx)?;
            if published == mounted
                && require_dial(&published, provider, expected).is_ok()
                && require_dial(&mounted, provider, expected).is_ok()
            {
                Ok(Some(serde_json::json!({
                    "configMapUid": configmap.pointer("/metadata/uid"),
                    "configMapResourceVersion": configmap.pointer("/metadata/resourceVersion"),
                    "servingDigest": configmap.pointer("/metadata/annotations/grid.praxis.fast~1serving-digest"),
                    "published": published,
                    "mounted": mounted,
                })))
            } else {
                Ok(None)
            }
        },
    )
}

/// Send one uniquely identified inference probe through the Gateway.
fn request(ctx: &RunContext, request_id: u64) -> Probe {
    let body =
        format!("{{\"model\":\"{MODEL}\",\"messages\":[{{\"role\":\"user\",\"content\":\"probe-{request_id}\"}}]}}");
    let output = kubectl(
        ctx,
        &[
            "exec",
            "-n",
            NAMESPACE,
            "qualification-client",
            "--",
            "curl",
            "--silent",
            "--show-error",
            "--connect-timeout",
            "2",
            "--max-time",
            "6",
            "-D",
            "-",
            "-o",
            "/dev/null",
            "-w",
            &format!("\\n{REQUEST_MARKER}%{{http_code}}\\n"),
            "-H",
            "Authorization: Bearer qualification-only",
            "-H",
            "Content-Type: application/json",
            "--data",
            &body,
            GATEWAY_SERVICE,
        ],
        REQUEST_TIMEOUT,
    );
    let Ok(output) = output else {
        return Probe {
            status: None,
            grid_backend: None,
            grid_site: None,
            mock_provider: None,
            error: Some("kubectl exec/curl failed before returning a response".to_owned()),
        };
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let header = |name: &str| {
        text.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim().to_owned())
        })
    };
    let status = text.lines().find_map(|line| {
        line.trim()
            .strip_prefix(REQUEST_MARKER)
            .and_then(|status| status.parse::<u16>().ok())
    });
    let error = (!output.status.success()).then(|| stderr(&output));
    Probe {
        status,
        grid_backend: header("x-grid-backend"),
        grid_site: header("x-grid-site"),
        mock_provider: header("x-grid-test-provider"),
        error,
    }
}

/// Probe until the expected provider serves successfully or the deadline expires.
fn request_until(
    ctx: &RunContext,
    expected_provider: &str,
    request_id: &mut u64,
    timeout: Duration,
    observations: &mut Vec<Probe>,
) -> Result<(), String> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        let probe = request(ctx, *request_id);
        *request_id = request_id.saturating_add(1);
        observations.push(probe.clone());
        if probe.status == Some(200) {
            let actual_provider = probe
                .mock_provider
                .as_deref()
                .ok_or_else(|| format!("successful response lacks backend provider attribution: {probe:?}"))?;
            validate_attribution(&probe, actual_provider)?;
        }
        let reached = probe.is_success() && validate_attribution(&probe, expected_provider).is_ok();
        if reached {
            return Ok(());
        }
    }
    Err(format!(
        "did not receive a successful response from {expected_provider} within {timeout:?}"
    ))
}

/// Map a recorder identity to the corresponding Grid backend ID.
fn expected_backend(provider: &str) -> Option<&'static str> {
    match provider {
        "provider-a" => Some(PROVIDER_A),
        "provider-b-old" | "provider-b-replacement" => Some(PROVIDER_B),
        _ => None,
    }
}

/// Verify HTTP success and matching Gateway and independent provider attribution.
fn validate_attribution(probe: &Probe, expected_provider: &str) -> Result<(), String> {
    let expected_backend = expected_backend(expected_provider)
        .ok_or_else(|| format!("no expected Grid backend mapping for {expected_provider:?}"))?;
    if !probe.is_success()
        || probe.mock_provider.as_deref() != Some(expected_provider)
        || probe.grid_backend.as_deref() != Some(expected_backend)
    {
        return Err(format!(
            "expected HTTP 200 from {expected_provider} via Grid backend {expected_backend} at site local, got {probe:?}"
        ));
    }
    Ok(())
}

/// Gateway Pod identity captured immediately before a provider outage phase.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct GatewayPodIdentity {
    /// Pod name used to retrieve its logs.
    name: String,
    /// UID used to detect replacement under the same name.
    uid: String,
    /// `praxis` container restart count at phase start.
    restart_count: u64,
}

/// Gateway Pods that existed when a phase began.
type GatewayPodSnapshot = BTreeMap<String, GatewayPodIdentity>;

/// One Gateway log message with its API-provided timestamp and source Pod.
#[derive(Clone, Debug, Eq, PartialEq)]
struct TimedGatewayLog {
    /// RFC3339 timestamp parsed from `kubectl logs --timestamps`.
    timestamp: OffsetDateTime,
    /// Source Gateway Pod name.
    pod: String,
    /// Timestamped log line with ANSI escapes removed.
    line: String,
}

/// Capture the current Gateway Pods and their identities before a phase starts.
fn gateway_pods(ctx: &RunContext) -> Result<GatewayPodSnapshot, String> {
    let deployment = json_kubectl(ctx, &["-n", NAMESPACE, "get", "deployment", GATEWAY])?;
    let selector = deployment
        .pointer("/spec/selector/matchLabels")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("Gateway Deployment {GATEWAY} has no matchLabels selector"))?;
    let selector = selector
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| format!("{key}={value}"))
                .ok_or_else(|| format!("Gateway Deployment selector {key:?} is not a string"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(",");
    if selector.is_empty() {
        return Err(format!("Gateway Deployment {GATEWAY} has an empty selector"));
    }
    let pods = json_kubectl(ctx, &["-n", NAMESPACE, "get", "pods", "-l", &selector])?;
    let items = pods
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "Gateway Pod list has no items array".to_owned())?;
    let mut snapshot = BTreeMap::new();
    for pod in items {
        let name = pod
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "Gateway Pod has no metadata.name".to_owned())?
            .to_owned();
        let uid = pod
            .pointer("/metadata/uid")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("Gateway Pod {name} has no metadata.uid"))?
            .to_owned();
        let containers = pod
            .pointer("/status/containerStatuses")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("Gateway Pod {name} has no container status snapshot"))?;
        let container = containers
            .iter()
            .find(|container| container.pointer("/name").and_then(serde_json::Value::as_str) == Some("praxis"))
            .ok_or_else(|| format!("Gateway Pod {name} has no praxis container status"))?;
        let restart_count = container
            .pointer("/restartCount")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| format!("Gateway Pod {name} has no praxis restart count"))?;
        snapshot.insert(
            name.clone(),
            GatewayPodIdentity {
                name,
                uid,
                restart_count,
            },
        );
    }
    if snapshot.is_empty() {
        return Err(format!("Gateway Deployment {GATEWAY} has no Pods to capture logs from"));
    }
    Ok(snapshot)
}

/// Read timestamp-bounded logs from every current Gateway Pod.
fn gateway_logs_since(
    ctx: &RunContext,
    phase_started: OffsetDateTime,
    baseline: &GatewayPodSnapshot,
) -> Result<Vec<TimedGatewayLog>, String> {
    let current = gateway_pods(ctx)?;
    ensure_gateway_pods_retained(baseline, &current)?;
    let since_time = phase_started
        .format(&Rfc3339)
        .map_err(|error| format!("format phase start timestamp: {error}"))?;
    let mut logs = Vec::new();
    for pod in current.values() {
        let restarted = baseline
            .get(&pod.name)
            .is_some_and(|original| pod.restart_count > original.restart_count);
        for previous in [true, false] {
            if previous && !restarted {
                continue;
            }
            let mut args = vec!["-n", NAMESPACE, "logs", &pod.name, "-c", "praxis"];
            if previous {
                args.push("--previous");
            }
            args.extend(["--timestamps=true", "--since-time", &since_time]);
            let output = kubectl(ctx, &args, Duration::from_secs(20))?;
            if !output.status.success() {
                return Err(format!("read Gateway Pod {} logs: {}", pod.name, stderr(&output)));
            }
            for raw_line in String::from_utf8_lossy(&output.stdout).lines() {
                if let Some(log) = parse_phase_log(raw_line, &pod.name, phase_started)? {
                    logs.push(log);
                }
            }
        }
    }
    Ok(logs)
}

/// Reject a phase if a Gateway Pod present at its start can no longer be attributed.
fn ensure_gateway_pods_retained(baseline: &GatewayPodSnapshot, current: &GatewayPodSnapshot) -> Result<(), String> {
    for (name, original) in baseline {
        match current.get(name) {
            Some(pod) if pod.uid == original.uid => {},
            Some(pod) => {
                return Err(format!(
                    "Gateway Pod {name} changed UID during the provider phase ({} -> {}); phase log attribution is incomplete",
                    original.uid, pod.uid
                ));
            },
            None => {
                return Err(format!(
                    "Gateway Pod {name} from phase start disappeared before its logs were collected; cannot establish phase evidence"
                ));
            },
        }
    }
    Ok(())
}

/// Parse one timestamped log and retain it only when it belongs to this phase.
fn parse_phase_log(
    raw_line: &str,
    pod: &str,
    phase_started: OffsetDateTime,
) -> Result<Option<TimedGatewayLog>, String> {
    let Some((timestamp, line)) = raw_line.split_once(' ') else {
        return Ok(None);
    };
    let timestamp = OffsetDateTime::parse(timestamp, &Rfc3339)
        .map_err(|error| format!("parse timestamped Gateway log from {pod} ({timestamp:?}): {error}"))?;
    Ok((timestamp >= phase_started).then(|| TimedGatewayLog {
        timestamp,
        pod: pod.to_owned(),
        line: strip_ansi_codes(line),
    }))
}

/// Whether a timestamped Gateway log identifies the failed selected endpoint.
fn is_provider_failure_log(line: &str, endpoint: &str) -> bool {
    let plain = strip_ansi_codes(line);
    plain.contains("upstream error")
        && plain.contains("status=502")
        && plain.contains("upstream_address")
        && plain.contains(endpoint)
}

/// Whether a timestamped passive-health update demotes Provider B.
fn is_provider_demotion_log(line: &str) -> bool {
    let plain = strip_ansi_codes(line);
    plain.contains("backend clusters with no healthy endpoint changed")
        && plain
            .split_once("down")
            .is_some_and(|(_, field)| field.contains(PROVIDER_B))
}

/// Return phase-local failure and demotion messages for the selected endpoint.
fn correlated_failure_and_demotion<'log>(
    logs: &'log [TimedGatewayLog],
    endpoint: &str,
) -> Option<(&'log TimedGatewayLog, &'log TimedGatewayLog)> {
    let failure = logs.iter().find(|log| is_provider_failure_log(&log.line, endpoint))?;
    let demotion = logs
        .iter()
        .find(|log| log.timestamp >= failure.timestamp && is_provider_demotion_log(&log.line))?;
    Some((failure, demotion))
}

/// Remove ANSI terminal formatting from a Gateway log line.
fn strip_ansi_codes(line: &str) -> String {
    let mut plain = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\x1b' && chars.peek() == Some(&'[') {
            chars.next();
            for sequence_character in chars.by_ref() {
                if ('@'..='~').contains(&sequence_character) {
                    break;
                }
            }
        } else {
            plain.push(character);
        }
    }
    plain
}

/// Phase boundary and Gateway Pod identities for one provider outage.
struct ProviderOutagePhase<'phase> {
    /// Exact provider endpoint expected in the Gateway failure log.
    endpoint: &'phase str,
    /// RFC3339 wall-clock boundary captured immediately before scale-down.
    started_at: OffsetDateTime,
    /// Gateway Pod identities captured before the boundary.
    gateway_pods_at_start: &'phase GatewayPodSnapshot,
}

/// Correlate a client 5xx, Gateway endpoint failure, and passive demotion in one phase.
fn wait_for_provider_demotion(
    ctx: &RunContext,
    phase: &ProviderOutagePhase<'_>,
    request_id: &mut u64,
    probes: &mut Vec<Probe>,
) -> Result<(String, String), String> {
    let started = Instant::now();
    let mut failure_log: Option<TimedGatewayLog> = None;
    while started.elapsed() < WAIT_TIMEOUT {
        let probe = request(ctx, *request_id);
        *request_id = request_id.saturating_add(1);
        let failed = probe.status.is_some_and(|status| (500..600).contains(&status));
        probes.push(probe);
        if !failed {
            continue;
        }
        let lines = gateway_logs_since(ctx, phase.started_at, phase.gateway_pods_at_start)?;
        if let Some((failure, demotion)) = correlated_failure_and_demotion(&lines, phase.endpoint) {
            return Ok((
                format!("{} [{}]", failure.pod, failure.line),
                format!("{} [{}]", demotion.pod, demotion.line),
            ));
        }
        if failure_log.is_none() {
            failure_log = lines
                .iter()
                .find(|line| is_provider_failure_log(&line.line, phase.endpoint))
                .cloned();
        }
    }
    if failure_log.is_none() {
        return Err(format!(
            "no client 5xx was correlated with a Gateway 502 for upstream {} after {}",
            phase.endpoint, phase.started_at
        ));
    }
    Err(format!(
        "Gateway observed a 502 from upstream {} after {} but did not publish a fresh passive-health demotion",
        phase.endpoint, phase.started_at
    ))
}

/// Wait until the scaled provider Deployment has no desired replicas or selected Pods.
fn wait_for_backend_removal(ctx: &RunContext, deployment: &str, selector: &str) -> Result<serde_json::Value, String> {
    wait_until(&format!("all {deployment} Pods are removed"), WAIT_TIMEOUT, || {
        let deployment_value = json_kubectl(ctx, &["-n", NAMESPACE, "get", "deployment", deployment])?;
        let desired = deployment_value
            .pointer("/spec/replicas")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(1);
        let ready = deployment_value
            .pointer("/status/readyReplicas")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let pods = json_kubectl(ctx, &["-n", NAMESPACE, "get", "pods", "-l", selector])?;
        let no_pods = pods
            .get("items")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty);
        Ok((desired == 0 && ready == 0 && no_pods).then(|| {
            serde_json::json!({
                "deploymentUid": deployment_value.pointer("/metadata/uid"),
                "resourceVersion": deployment_value.pointer("/metadata/resourceVersion"),
                "desiredReplicas": desired,
                "readyReplicas": ready,
                "podsRemaining": 0,
            })
        }))
    })
}

/// Update Provider B's endpoint after its old endpoint is demoted.
fn patch_endpoint(ctx: &RunContext, endpoint: &str) -> Result<(), String> {
    let patch = format!(r#"{{"spec":{{"endpoint":"{endpoint}"}}}}"#);
    kubectl_ok(
        ctx,
        &["patch", "inferenceprovider", PROVIDER_B, "--type=merge", "-p", &patch],
        Duration::from_secs(20),
    )?;
    Ok(())
}

/// Save bounded Kubernetes snapshots and logs after a qualification failure.
fn capture_diagnostics(ctx: &RunContext, evidence: &mut Evidence) {
    for (name, args) in [
        ("pods.json", vec!["-n", NAMESPACE, "get", "pods"]),
        ("deployments.json", vec!["-n", NAMESPACE, "get", "deployments"]),
        ("gridnetwork.json", vec!["get", "gridnetwork", NETWORK]),
        ("inferenceproviders.json", vec!["get", "inferenceproviders"]),
        (
            "serving-configmap.json",
            vec!["-n", NAMESPACE, "get", "configmap", GRID_SERVING_CONFIGMAP],
        ),
        (
            "events.txt",
            vec!["-n", NAMESPACE, "get", "events", "--sort-by=.lastTimestamp"],
        ),
        (
            "gateway-logs.txt",
            vec![
                "-n",
                NAMESPACE,
                "logs",
                &format!("deployment/{GATEWAY}"),
                "-c",
                "praxis",
                "--tail=1000",
            ],
        ),
        (
            "operator-logs.txt",
            vec!["-n", NAMESPACE, "logs", "deployment/grid-operator", "--tail=1000"],
        ),
    ] {
        let rendered = kubectl(ctx, &args, Duration::from_secs(20)).map_or_else(
            |error| format!("diagnostic command failed: {error}\n"),
            |output| {
                format!(
                    "status: {}\n{}{}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    stderr(&output)
                )
            },
        );
        if let Err(error) = fs::write(ctx.evidence_dir.join(name), rendered) {
            evidence
                .observations
                .insert(format!("diagnostic_write_{name}"), serde_json::json!(error.to_string()));
        }
    }
}

/// Record the result and timestamps for one qualification phase.
fn phase<T>(evidence: &mut Evidence, name: &str, run: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    let started_at_unix_ms = unix_millis();
    match run() {
        Ok(value) => {
            evidence.scenarios.push(Scenario {
                name: name.to_owned(),
                result: Outcome::Pass,
                detail: "observed required runtime contract".to_owned(),
                started_at_unix_ms,
                completed_at_unix_ms: unix_millis(),
            });
            Ok(value)
        },
        Err(error) => {
            evidence.scenarios.push(Scenario {
                name: name.to_owned(),
                result: Outcome::Fail,
                detail: error.clone(),
                started_at_unix_ms,
                completed_at_unix_ms: unix_millis(),
            });
            Err(error)
        },
    }
}

/// Add a named list of request observations to the evidence document.
fn record_probe_set(evidence: &mut Evidence, name: &str, probes: &[Probe]) {
    evidence.observations.insert(
        name.to_owned(),
        serde_json::to_value(probes).unwrap_or(serde_json::Value::Null),
    );
}

#[expect(
    clippy::too_many_lines,
    reason = "the qualification's ordered routing phases share request IDs and evidence"
)]
/// Execute the ordered provider routing, failure, recovery, and replacement checks.
fn run_phases(ctx: &RunContext, evidence: &mut Evidence, request_id: &mut u64, images: &Images) -> Result<(), String> {
    let image_data = phase(evidence, "deployments-and-runtime-images", || {
        image_references_and_runtime_ids(
            ctx,
            images,
            &[
                "grid-operator",
                GATEWAY,
                "provider-a",
                "provider-b-old",
                "provider-b-replacement",
            ],
        )
    })?;
    evidence.observations.insert(
        "runtime_images".to_owned(),
        serde_json::to_value(&image_data).unwrap_or_default(),
    );

    let initial_dials = phase(evidence, "operator-publishes-derived-local-dials", || {
        let config = wait_for_dial(ctx, PROVIDER_A, "provider-a.grid-system.svc:8080")?;
        let published = config
            .get("published")
            .ok_or("wait_for_dial omitted published config")?;
        let mounted = config.get("mounted").ok_or("wait_for_dial omitted mounted config")?;
        require_dial(published, PROVIDER_B, "provider-b-old.grid-system.svc:8080")?;
        require_dial(mounted, PROVIDER_B, "provider-b-old.grid-system.svc:8080")?;
        Ok(config)
    })?;
    evidence.observations.insert("initial_dials".to_owned(), initial_dials);

    for provider in [PROVIDER_A, PROVIDER_B] {
        let endpoint = if provider == PROVIDER_A {
            "http://provider-a.grid-system.svc:8080"
        } else {
            "http://provider-b-old.grid-system.svc:8080"
        };
        let status = phase(evidence, &format!("{provider}-available-current-generation"), || {
            wait_provider_available(ctx, provider, endpoint)
        })?;
        evidence.observations.insert(format!("{provider}_status"), status);
    }

    let mut initial = Vec::new();
    let mut seen = BTreeSet::new();
    let initial_result = phase(evidence, "mounted-dials-reach-both-attributed-providers", || {
        for _ in 0..24 {
            let probe = request(ctx, *request_id);
            *request_id = request_id.saturating_add(1);
            initial.push(probe.clone());
            let Some(provider) = probe.mock_provider.as_deref() else {
                return Err(format!("initial request was not successfully attributed: {probe:?}"));
            };
            validate_attribution(&probe, provider)?;
            if !matches!(provider, "provider-a" | "provider-b-old") {
                return Err(format!(
                    "initial request reached unexpected provider {provider:?}: {probe:?}"
                ));
            }
            seen.insert(provider.to_owned());
            if seen.len() == 2 {
                break;
            }
        }
        if seen == BTreeSet::from(["provider-a".to_owned(), "provider-b-old".to_owned()]) {
            Ok(())
        } else {
            Err(format!(
                "expected attributed traffic to both local providers, observed {seen:?}"
            ))
        }
    });
    record_probe_set(evidence, "initial_gateway_requests", &initial);
    initial_result?;

    let mut failures = Vec::new();
    let mut fallback = Vec::new();
    let gateway_pods_at_start = gateway_pods(ctx)?;
    evidence.observations.insert(
        "provider_b_demotion_gateway_pods_at_start".to_owned(),
        serde_json::to_value(&gateway_pods_at_start).map_err(|error| error.to_string())?,
    );
    let mut demotion_started_at = None;
    let demotion_result = phase(evidence, "provider-b-passive-demotion-and-fallback", || {
        let phase_started = OffsetDateTime::now_utc();
        demotion_started_at = Some(
            phase_started
                .format(&Rfc3339)
                .map_err(|error| format!("format provider outage timestamp: {error}"))?,
        );
        kubectl_ok(
            ctx,
            &["-n", NAMESPACE, "scale", "deployment/provider-b-old", "--replicas=0"],
            Duration::from_secs(20),
        )?;
        let removed_backend = wait_for_backend_removal(ctx, "provider-b-old", "app=provider-b-old")?;
        let (failure_log, down_log) = wait_for_provider_demotion(
            ctx,
            &ProviderOutagePhase {
                endpoint: "provider-b-old.grid-system.svc:8080",
                started_at: phase_started,
                gateway_pods_at_start: &gateway_pods_at_start,
            },
            request_id,
            &mut failures,
        )?;
        let provider = wait_provider_available(ctx, PROVIDER_B, "http://provider-b-old.grid-system.svc:8080")?;
        let dials = wait_for_dial(ctx, PROVIDER_B, "provider-b-old.grid-system.svc:8080")?;
        for _ in 0..10 {
            let probe = request(ctx, *request_id);
            *request_id = request_id.saturating_add(1);
            fallback.push(probe.clone());
            validate_attribution(&probe, "provider-a")?;
        }
        Ok((failure_log, down_log, removed_backend, provider, dials))
    });
    record_probe_set(evidence, "passive_health_fallback_requests", &fallback);
    record_probe_set(evidence, "passive_health_failure_requests", &failures);
    if let Some(started_at) = demotion_started_at {
        evidence.observations.insert(
            "provider_b_demotion_phase_started_at".to_owned(),
            serde_json::json!(started_at),
        );
    }
    let (provider_b_failure_log, demotion_down_log, removed_before_demotion, provider_status, retained_dial) =
        demotion_result?;
    evidence.observations.insert(
        "provider_b_failure_log".to_owned(),
        serde_json::json!(provider_b_failure_log),
    );
    evidence
        .observations
        .insert("provider_b_down_log".to_owned(), serde_json::json!(demotion_down_log));
    evidence
        .observations
        .insert("provider_b_old_removed".to_owned(), removed_before_demotion);
    evidence
        .observations
        .insert("provider_b_remained_available".to_owned(), provider_status);
    evidence
        .observations
        .insert("provider_b_dial_retained".to_owned(), retained_dial);

    let mut recovery_requests = Vec::new();
    let recovery_result = phase(evidence, "same-endpoint-passive-health-recovery", || {
        kubectl_ok(
            ctx,
            &["-n", NAMESPACE, "scale", "deployment/provider-b-old", "--replicas=1"],
            Duration::from_secs(20),
        )?;
        let rollout = kubectl_ok(
            ctx,
            &[
                "-n",
                NAMESPACE,
                "rollout",
                "status",
                "deployment/provider-b-old",
                "--timeout=120s",
            ],
            Duration::from_secs(130),
        )?;
        request_until(
            ctx,
            "provider-b-old",
            request_id,
            Duration::from_secs(45),
            &mut recovery_requests,
        )?;
        request_until(
            ctx,
            "provider-b-old",
            request_id,
            Duration::from_secs(20),
            &mut recovery_requests,
        )?;
        Ok(rollout)
    });
    record_probe_set(evidence, "same_endpoint_recovery_requests", &recovery_requests);
    let rollout = recovery_result?;
    evidence
        .observations
        .insert("provider_b_old_rollout".to_owned(), serde_json::json!(rollout.trim()));

    let mut replacement_failures = Vec::new();
    let mut replacement_requests = Vec::new();
    let replacement_gateway_pods_at_start = gateway_pods(ctx)?;
    evidence.observations.insert(
        "replacement_demotion_gateway_pods_at_start".to_owned(),
        serde_json::to_value(&replacement_gateway_pods_at_start).map_err(|error| error.to_string())?,
    );
    let mut replacement_started_at = None;
    let replacement_result = phase(evidence, "endpoint-replacement-and-eventual-recovery", || {
        let phase_started = OffsetDateTime::now_utc();
        replacement_started_at = Some(
            phase_started
                .format(&Rfc3339)
                .map_err(|error| format!("format replacement outage timestamp: {error}"))?,
        );
        kubectl_ok(
            ctx,
            &["-n", NAMESPACE, "scale", "deployment/provider-b-old", "--replicas=0"],
            Duration::from_secs(20),
        )?;
        let removed_backend = wait_for_backend_removal(ctx, "provider-b-old", "app=provider-b-old")?;
        let (failure_log, replacement_down_log) = wait_for_provider_demotion(
            ctx,
            &ProviderOutagePhase {
                endpoint: "provider-b-old.grid-system.svc:8080",
                started_at: phase_started,
                gateway_pods_at_start: &replacement_gateway_pods_at_start,
            },
            request_id,
            &mut replacement_failures,
        )?;
        patch_endpoint(ctx, "http://provider-b-replacement.grid-system.svc:8080")?;
        let updated = wait_provider_available(ctx, PROVIDER_B, "http://provider-b-replacement.grid-system.svc:8080")?;
        let dials = wait_for_dial(ctx, PROVIDER_B, "provider-b-replacement.grid-system.svc:8080")?;
        request_until(
            ctx,
            "provider-b-replacement",
            request_id,
            Duration::from_secs(30),
            &mut replacement_requests,
        )?;
        Ok((failure_log, replacement_down_log, removed_backend, updated, dials))
    });
    record_probe_set(evidence, "endpoint_replacement_failure_requests", &replacement_failures);
    record_probe_set(evidence, "endpoint_replacement_requests", &replacement_requests);
    if let Some(started_at) = replacement_started_at {
        evidence.observations.insert(
            "replacement_demotion_phase_started_at".to_owned(),
            serde_json::json!(started_at),
        );
    }
    let (replacement_failure_log, replacement_down_log, removed_before_replacement, updated, dials) =
        replacement_result?;
    evidence.observations.insert(
        "replacement_failure_log".to_owned(),
        serde_json::json!(replacement_failure_log),
    );
    evidence.observations.insert(
        "provider_b_down_before_replacement".to_owned(),
        serde_json::json!(replacement_down_log),
    );
    evidence.observations.insert(
        "provider_b_old_removed_before_replacement".to_owned(),
        removed_before_replacement,
    );
    evidence
        .observations
        .insert("replacement_provider_status".to_owned(), updated);
    evidence.observations.insert("replacement_dial".to_owned(), dials);

    let images_after = image_references_and_runtime_ids(
        ctx,
        images,
        &["grid-operator", GATEWAY, "provider-a", "provider-b-replacement"],
    )?;
    evidence.observations.insert(
        "runtime_images_after_recovery".to_owned(),
        serde_json::to_value(images_after).unwrap_or_default(),
    );
    Ok(())
}

/// Run a top-level Forge lifecycle command for this run-owned environment.
fn run_forge(ctx: &RunContext, verb: &str, timeout: Duration) -> Result<Output, String> {
    let mut command = super::forge_config::command(&ctx.forge, &ctx.resolved_config)
        .map_err(|error| format!("prepare run-scoped Forge command: {error}"))?;
    command
        .args(["--config"])
        .arg(&ctx.resolved_config)
        .args(["--non-interactive", verb]);
    bounded_command(command, timeout, &format!("Forge {verb}"))
}

/// Validate the resolved topology and its run-scoped Forge plan before provisioning.
fn validate_resolved_forge_config(ctx: &RunContext) -> Result<(), String> {
    for (name, args) in [
        ("forge-config-validation.log", vec!["config", "validate"]),
        ("forge-plan-dry-run.log", vec!["--dry-run", "plan"]),
    ] {
        let mut command = Command::new(&ctx.forge);
        command.args(["--config"]).arg(&ctx.resolved_config).args(args);
        let output = bounded_command(command, Duration::from_secs(60), name)?;
        write_command_output(&ctx.evidence_dir, name, &output)?;
        if !output.status.success() {
            return Err(format!(
                "pinned Forge preflight {name} rejected the resolved topology: {}",
                stderr(&output)
            ));
        }
    }
    Ok(())
}

/// Apply one named Forge stack with bounded execution and retained output.
fn run_forge_stack(ctx: &RunContext, stack: &str, timeout: Duration) -> Result<Output, String> {
    let mut command = super::forge_config::command(&ctx.forge, &ctx.resolved_config)
        .map_err(|error| format!("prepare run-scoped Forge stack command: {error}"))?;
    command
        .args(["--config"])
        .arg(&ctx.resolved_config)
        .args(["--non-interactive", "stack", "apply", "local", stack]);
    bounded_command(command, timeout, &format!("Forge stack apply {stack}"))
}

/// Save one command's status and captured output in the evidence directory.
fn write_command_output(directory: &Path, name: &str, output: &Output) -> Result<(), String> {
    let contents = format!(
        "status: {}\n{}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        stderr(output)
    );
    fs::write(directory.join(name), contents).map_err(|error| format!("save {name}: {error}"))
}

/// Serialize the current evidence snapshot to the run directory.
fn write_evidence(evidence: &Evidence, directory: &Path) -> Result<(), String> {
    let json = serde_json::to_vec_pretty(evidence).map_err(|error| error.to_string())?;
    fs::write(directory.join("evidence.json"), json).map_err(|error| format!("write evidence: {error}"))
}

/// Run a single-cluster derived-dial, passive-health, and endpoint replacement qualification.
#[expect(
    clippy::too_many_lines,
    reason = "the run lifecycle is deliberately kept adjacent to preserve failure and teardown ordering"
)]
pub(crate) fn run(options: &Options) -> Result<(), Box<dyn std::error::Error>> {
    let topology = &options.forge_config;
    let run = run_id();
    let source_fingerprint_sha256 = source_fingerprint()?;
    let evidence_dir = evidence_dir(topology, options.evidence_dir.as_deref(), &run);
    create_evidence_dir(&evidence_dir)?;
    let images = resolve_images().map_err(|error| format!("resolve source-built images: {error}"))?;
    let forge = super::glb::resolve_forge_binary().ok_or("praxis-forge binary not found")?;
    let cluster = cluster_name(&run);
    let context = context_for(&cluster);
    let network = format!("grid-operator-routing-{run}-net");
    if kind_cluster_exists(&cluster)? {
        return Err(format!("run-owned Kind cluster {cluster} already exists; refusing to touch it").into());
    }
    if docker_network_exists(&network)? {
        return Err(format!("run-owned Docker network {network} already exists; refusing to touch it").into());
    }
    let resolved_working_config = materialize_config(topology, &images, &run)?;
    let state_dir = super::forge_config::state_dir_for_config(&resolved_working_config)?;
    if state_dir.exists() {
        fs::remove_file(&resolved_working_config)?;
        return Err(format!(
            "run-scoped Forge state directory already exists; refusing to reuse it: {}",
            state_dir.display()
        )
        .into());
    }
    let resolved_evidence_config = evidence_dir.join("resolved-forge.yaml");
    fs::copy(&resolved_working_config, &resolved_evidence_config)?;
    let ctx = RunContext {
        forge,
        resolved_config: resolved_working_config.clone(),
        state_dir,
        evidence_dir: evidence_dir.clone(),
        context: context.clone(),
    };
    let mut references = BTreeMap::from([
        ("operator".to_owned(), images.operator.clone()),
        ("grid_gateway".to_owned(), images.grid_gateway.clone()),
        ("mock_provider".to_owned(), images.mock_provider.clone()),
        ("client".to_owned(), CLIENT_IMAGE.to_owned()),
        ("client_pinned_source".to_owned(), CLIENT_IMAGE_REF.to_owned()),
    ]);
    let image_ids = images.local_ids.clone();
    let source_revision = git_command(&["rev-parse", "HEAD"])?;
    let mut evidence = Evidence {
        schema_version: 1,
        result: Outcome::Running,
        topology: topology.display().to_string(),
        source_revision,
        source_fingerprint_sha256,
        cluster_name: cluster.clone(),
        kubectl_context: context,
        image_references: std::mem::take(&mut references),
        image_ids,
        scenarios: Vec::new(),
        observations: BTreeMap::from([
            ("run_id".to_owned(), serde_json::json!(run)),
            ("forge_network_name".to_owned(), serde_json::json!(network)),
            (
                "forge_state_dir".to_owned(),
                serde_json::json!(ctx.state_dir.display().to_string()),
            ),
            (
                "resolved_forge_config".to_owned(),
                serde_json::json!(resolved_evidence_config.display().to_string()),
            ),
            (
                "forge_working_config".to_owned(),
                serde_json::json!(resolved_working_config.display().to_string()),
            ),
            ("image_pull_policy".to_owned(), serde_json::json!(images.pull_policy)),
        ]),
        cleanup: "NOT_RUN".to_owned(),
    };
    write_evidence(&evidence, &evidence_dir)?;
    let run_result = (|| -> Result<(), String> {
        validate_resolved_forge_config(&ctx)?;
        let started = run_forge(&ctx, "up", Duration::from_secs(300))?;
        write_command_output(&ctx.evidence_dir, "forge-up.log", &started)?;
        if !started.status.success() {
            return Err(format!("Forge cluster creation failed: {}", stderr(&started)));
        }
        wait_until("run-owned Kind cluster creation", Duration::from_secs(60), || {
            Ok(kind_cluster_exists(&cluster)?.then_some(()))
        })?;
        for (role, image) in [
            ("operator", images.operator.as_str()),
            ("Grid Gateway", images.grid_gateway.as_str()),
            ("mock provider", images.mock_provider.as_str()),
            ("qualification client", CLIENT_IMAGE),
        ] {
            super::image_overrides::load_docker_image_into_kind(image, &cluster)
                .map_err(|error| format!("load {role} image {image} into run-owned cluster: {error}"))?;
        }
        for stack in STACK_APPLICATION_ORDER {
            if stack == "embedded-gateway" {
                let identity = phase(&mut evidence, "gateway-grid-identity-secret", || {
                    create_gateway_identity_secret(&ctx)
                })?;
                evidence
                    .observations
                    .insert("gateway_identity_secret".to_owned(), serde_json::json!(identity));
            }
            let stack_output = run_forge_stack(&ctx, stack, Duration::from_secs(300))?;
            write_command_output(&ctx.evidence_dir, &format!("forge-stack-{stack}.log"), &stack_output)?;
            if !stack_output.status.success() {
                return Err(format!("Forge stack {stack} failed: {}", stderr(&stack_output)));
            }
        }
        wait_until("qualification client pod Running", WAIT_TIMEOUT, || {
            let value = json_kubectl(&ctx, &["-n", NAMESPACE, "get", "pod", "qualification-client"])?;
            Ok((value.pointer("/status/phase").and_then(serde_json::Value::as_str) == Some("Running")).then_some(()))
        })?;
        let mut request_id = 0;
        run_phases(&ctx, &mut evidence, &mut request_id, &images)
    })();
    if let Err(error) = &run_result {
        evidence.result = Outcome::Fail;
        evidence
            .observations
            .insert("first_failure".to_owned(), serde_json::json!(error));
        capture_diagnostics(&ctx, &mut evidence);
    } else {
        evidence.result = Outcome::Pass;
    }
    if options.teardown && (run_result.is_ok() || !options.keep_on_failure) {
        let cleanup = run_forge(&ctx, "down", Duration::from_secs(180));
        match cleanup {
            Ok(output) if output.status.success() => {
                let gone = wait_for_cluster_gone(&cluster, Duration::from_secs(60))
                    .and_then(|()| wait_for_network_gone(&network, Duration::from_secs(60)));
                if let Err(error) = gone {
                    evidence.result = Outcome::Fail;
                    evidence.cleanup = format!("FAIL: {error}");
                } else {
                    if let Err(error) = if ctx.state_dir.exists() {
                        fs::remove_dir_all(&ctx.state_dir)
                    } else {
                        Ok(())
                    } {
                        evidence.result = Outcome::Fail;
                        evidence.cleanup =
                            format!("FAIL: cluster removed but run-scoped Forge state could not be removed: {error}");
                    } else if let Err(error) = fs::remove_file(&resolved_working_config) {
                        evidence.result = Outcome::Fail;
                        evidence.cleanup = format!(
                            "FAIL: cluster and Forge state removed but temporary Forge config could not be removed: {error}"
                        );
                    } else {
                        "PASS: run-owned Forge state removed; Kind cluster and Docker network absent"
                            .clone_into(&mut evidence.cleanup);
                    }
                }
            },
            Ok(output) => {
                evidence.cleanup = format!("FAIL: Forge down returned {}: {}", output.status, stderr(&output));
                evidence.result = Outcome::Fail;
            },
            Err(error) => {
                evidence.cleanup = format!("FAIL: {error}");
                evidence.result = Outcome::Fail;
            },
        }
    } else if options.keep_on_failure && run_result.is_err() {
        "PRESERVED: --keep-on-failure retained run-owned cluster after diagnostics".clone_into(&mut evidence.cleanup);
    } else if !options.teardown {
        "NOT_REQUESTED: run-owned cluster retained".clone_into(&mut evidence.cleanup);
    }
    write_evidence(&evidence, &evidence_dir)?;
    let summary = serde_json::to_string_pretty(&evidence)?;
    fs::write(evidence_dir.join("summary.json"), summary)?;
    eprintln!("operator-routing qualification evidence: {}", evidence_dir.display());
    match run_result {
        Ok(()) if evidence.result == Outcome::Pass => Ok(()),
        Ok(()) => Err(format!("qualification completed but cleanup failed: {}", evidence.cleanup).into()),
        Err(error) => Err(format!(
            "operator-routing qualification failed: {error}; evidence: {}",
            evidence_dir.display()
        )
        .into()),
    }
}

/// Check whether the exact run-owned Kind cluster exists.
fn kind_cluster_exists(cluster: &str) -> Result<bool, String> {
    let mut command = Command::new("kind");
    command.args(["get", "clusters"]);
    let output = bounded_command(command, Duration::from_secs(15), "list Kind clusters")?;
    if !output.status.success() {
        return Err(format!("kind get clusters failed: {}", stderr(&output)));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .any(|line| line.trim() == cluster))
}

/// Check whether the exact run-owned Docker network exists.
fn docker_network_exists(network: &str) -> Result<bool, String> {
    let mut command = Command::new("docker");
    command.args(["network", "ls", "--format", "{{.Name}}"]);
    let output = bounded_command(command, Duration::from_secs(15), "list Docker networks")?;
    if !output.status.success() {
        return Err(format!("docker network ls failed: {}", stderr(&output)));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .any(|line| line.trim() == network))
}

/// Confirm teardown removed the run-owned Kind cluster.
fn wait_for_cluster_gone(cluster: &str, timeout: Duration) -> Result<(), String> {
    wait_until(&format!("run-owned Kind cluster {cluster} removed"), timeout, || {
        Ok((!kind_cluster_exists(cluster)?).then_some(()))
    })
}

/// Confirm teardown removed the run-owned Docker network.
fn wait_for_network_gone(network: &str, timeout: Duration) -> Result<(), String> {
    wait_until(&format!("run-owned Docker network {network} removed"), timeout, || {
        Ok((!docker_network_exists(network)?).then_some(()))
    })
}

/// Run a Git command in the repository root and return trimmed stdout.
fn git_command(args: &[&str]) -> Result<String, String> {
    let output = git_output(args)?;
    if !output.status.success() {
        return Err(format!("git {:?} failed: {}", args, stderr(&output)));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Run a Git command in the repository root and retain its complete status/output.
fn git_output(args: &[&str]) -> Result<Output, String> {
    let mut command = Command::new("git");
    command.args(args);
    bounded_command(command, Duration::from_secs(15), "git provenance")
}

/// Fingerprint the tracked diff and untracked source inputs for this run.
fn source_fingerprint() -> Result<String, String> {
    let diff = git_output(&["diff", "--binary", "HEAD"])?;
    if !diff.status.success() {
        return Err(format!("git diff --binary HEAD failed: {}", stderr(&diff)));
    }
    let untracked = git_output(&["ls-files", "--others", "--exclude-standard", "-z"])?;
    if !untracked.status.success() {
        return Err(format!(
            "git ls-files for source fingerprint failed: {}",
            stderr(&untracked)
        ));
    }
    let mut untracked_files = Vec::new();
    for path in String::from_utf8_lossy(&untracked.stdout)
        .split('\0')
        .filter(|path| !path.is_empty())
    {
        let content =
            fs::read(path).map_err(|error| format!("read untracked source {path:?} for fingerprint: {error}"))?;
        untracked_files.push((path.to_owned(), content));
    }
    Ok(source_fingerprint_from_parts(&diff.stdout, &untracked_files))
}

/// Hash tracked and untracked source changes in a stable, path-aware order.
fn source_fingerprint_from_parts(tracked_diff: &[u8], untracked_files: &[(String, Vec<u8>)]) -> String {
    use sha2::{Digest as _, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(b"tracked-diff\0");
    hasher.update(tracked_diff);
    hasher.update(b"untracked-files\0");
    for (path, content) in untracked_files {
        hasher.update(path.len().to_be_bytes());
        hasher.update(path.as_bytes());
        hasher.update(content.len().to_be_bytes());
        hasher.update(content);
    }
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fs,
        process::Command,
        time::{Duration, Instant},
    };

    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

    use super::{
        CLIENT_IMAGE, CLIENT_IMAGE_DIGEST, CLIENT_IMAGE_REF, GatewayPodIdentity, GatewayPodSnapshot, Outcome, Probe,
        STACK_APPLICATION_ORDER, bounded_command, correlated_failure_and_demotion, create_evidence_dir,
        ensure_gateway_pods_retained, has_repo_digest, is_provider_demotion_log, is_provider_failure_log,
        parse_phase_log, require_dial, source_fingerprint_from_parts, split_image, strip_ansi_codes,
        validate_attribution,
    };

    #[test]
    fn image_reference_split_handles_registry_ports() {
        assert_eq!(
            split_image("registry.example:5000/grid-gateway:source"),
            ("registry.example:5000/grid-gateway".to_owned(), "source".to_owned())
        );
    }

    #[test]
    fn pinned_client_image_uses_the_repository_probe_digest() {
        assert_eq!(
            CLIENT_IMAGE_REF.strip_prefix("curlimages/curl@"),
            Some(CLIENT_IMAGE_DIGEST)
        );
        assert_eq!(
            split_image(CLIENT_IMAGE),
            ("curlimages/curl".to_owned(), "8.10.1".to_owned())
        );
    }

    #[test]
    fn pinned_client_digest_matches_only_a_repo_digest_suffix() {
        let valid = vec![format!("curlimages/curl@{CLIENT_IMAGE_DIGEST}")];
        let wrong = vec!["curlimages/curl@sha256:aaaaaaaaaaaaaaaa".to_owned()];

        assert!(
            has_repo_digest(&valid, CLIENT_IMAGE_DIGEST),
            "the pinned digest suffix must match"
        );
        assert!(
            !has_repo_digest(&wrong, CLIENT_IMAGE_DIGEST),
            "an unrelated digest must not match"
        );
    }

    #[test]
    fn dial_assertion_checks_the_published_address_not_just_candidate_presence() -> Result<(), Box<dyn Error>> {
        let dial = serde_json::json!({"dial":{"provider-b":{"address":"provider-b.grid-system.svc:8080"}}});
        require_dial(&dial, "provider-b", "provider-b.grid-system.svc:8080")?;
        assert!(
            require_dial(&dial, "provider-b", "provider-b-new.grid-system.svc:8080").is_err(),
            "a mismatched published address must fail"
        );
        Ok(())
    }

    #[test]
    fn successful_probe_requires_gateway_and_backend_attribution() -> Result<(), Box<dyn Error>> {
        let valid = Probe {
            status: Some(200),
            grid_backend: Some("provider-b".to_owned()),
            grid_site: Some("local".to_owned()),
            mock_provider: Some("provider-b-old".to_owned()),
            error: None,
        };
        assert!(valid.is_success(), "HTTP 200 with both attributions is a valid probe");
        let missing_backend = Probe {
            grid_backend: None,
            ..valid.clone()
        };
        assert!(
            !missing_backend.is_success(),
            "missing Grid backend attribution must fail"
        );
        validate_attribution(&valid, "provider-b-old")?;
        assert!(
            validate_attribution(&valid, "provider-a").is_err(),
            "a mismatched provider attribution must fail"
        );
        let missing_site = Probe {
            grid_site: None,
            ..valid
        };
        assert!(!missing_site.is_success(), "missing site attribution must fail");
        Ok(())
    }

    #[test]
    fn phase_log_parser_respects_rfc3339_nanosecond_boundary() -> Result<(), Box<dyn Error>> {
        let phase_started = OffsetDateTime::parse("2026-11-03T00:00:00.000000001Z", &Rfc3339)?;
        let prior = parse_phase_log("2026-11-03T00:00:00.000000000Z prior event", "gateway-a", phase_started)?;
        let at_boundary = parse_phase_log(
            "2026-11-03T00:00:00.000000001Z current event",
            "gateway-a",
            phase_started,
        )?;
        let later = parse_phase_log("2026-11-03T00:00:00.000000002Z later event", "gateway-a", phase_started)?;
        assert!(
            prior.is_none(),
            "a log one nanosecond before phase start must be excluded"
        );
        assert_eq!(
            at_boundary.map(|log| log.line),
            Some("current event".to_owned()),
            "a log at the captured phase timestamp must be included"
        );
        assert_eq!(
            later.map(|log| log.line),
            Some("later event".to_owned()),
            "a log after phase start must be included"
        );
        assert!(
            parse_phase_log("untimestamped", "gateway-a", phase_started)?.is_none(),
            "untimestamped output cannot qualify as phase evidence"
        );
        Ok(())
    }

    #[test]
    fn phase_failure_and_demotion_require_matching_endpoint_and_order() -> Result<(), Box<dyn Error>> {
        let started = OffsetDateTime::parse("2026-11-03T00:00:00Z", &Rfc3339)?;
        let before = parse_phase_log(
            "2026-11-02T23:59:59Z grid: backend clusters with no healthy endpoint changed down = {\"provider-b\"}",
            "gateway-a",
            started,
        )?;
        assert!(before.is_none(), "a previous phase demotion must not enter this phase");
        let failure = parse_phase_log(
            "2026-11-03T00:00:01Z upstream error status=502 upstream_address=\"provider-b-old.grid-system.svc:8080\"",
            "gateway-a",
            started,
        )?;
        let wrong_endpoint = parse_phase_log(
            "2026-11-03T00:00:02Z upstream error status=502 upstream_address=\"provider-a.grid-system.svc:8080\"",
            "gateway-a",
            started,
        )?;
        let demotion = parse_phase_log(
            "2026-11-03T00:00:03Z grid: backend clusters with no healthy endpoint changed down={\"provider-b\"}",
            "gateway-a",
            started,
        )?;
        let early_demotion = parse_phase_log(
            "2026-11-03T00:00:00.500000000Z grid: backend clusters with no healthy endpoint changed down = {\"provider-b\"}",
            "gateway-a",
            started,
        )?;
        let provider_a_demotion = parse_phase_log(
            "2026-11-03T00:00:04Z grid: backend clusters with no healthy endpoint changed down = {\"provider-a\"}",
            "gateway-a",
            started,
        )?;
        let failure = failure.ok_or("phase-local endpoint failure should parse")?;
        let wrong_endpoint = wrong_endpoint.ok_or("wrong-endpoint failure should parse")?;
        let demotion = demotion.ok_or("phase-local Provider B demotion should parse")?;
        let early_demotion = early_demotion.ok_or("pre-failure demotion should parse")?;
        let provider_a_demotion = provider_a_demotion.ok_or("Provider A demotion should parse")?;
        assert!(is_provider_failure_log(
            &failure.line,
            "provider-b-old.grid-system.svc:8080"
        ));
        assert!(!is_provider_failure_log(
            &wrong_endpoint.line,
            "provider-b-old.grid-system.svc:8080"
        ));
        assert!(
            is_provider_demotion_log(&demotion.line),
            "the event must identify Provider B as down"
        );
        assert!(
            !is_provider_demotion_log(&provider_a_demotion.line),
            "another provider's demotion is irrelevant"
        );
        let successful_logs = [failure.clone(), demotion];
        assert_eq!(
            correlated_failure_and_demotion(&successful_logs, "provider-b-old.grid-system.svc:8080")
                .map(|(failure_entry, down_entry)| (failure_entry.pod.as_str(), down_entry.pod.as_str())),
            Some(("gateway-a", "gateway-a")),
            "matching post-start failure and later demotion from a Gateway Pod must correlate"
        );
        assert!(
            correlated_failure_and_demotion(
                &[failure.clone(), provider_a_demotion],
                "provider-b-old.grid-system.svc:8080"
            )
            .is_none(),
            "a different backend demotion must not qualify"
        );
        assert!(
            correlated_failure_and_demotion(
                &[early_demotion, failure.clone()],
                "provider-b-old.grid-system.svc:8080"
            )
            .is_none(),
            "a demotion that predates the failed request cannot qualify"
        );
        assert!(
            correlated_failure_and_demotion(&[failure], "provider-b-replacement.grid-system.svc:8080").is_none(),
            "the failure must name the exact old endpoint under test"
        );
        Ok(())
    }

    #[test]
    fn gateway_pod_replacement_or_loss_fails_phase_attribution() -> Result<(), Box<dyn Error>> {
        let original = GatewayPodIdentity {
            name: "gateway-a".to_owned(),
            uid: "uid-old".to_owned(),
            restart_count: 0,
        };
        let baseline = GatewayPodSnapshot::from([("gateway-a".to_owned(), original)]);
        let replaced = GatewayPodSnapshot::from([(
            "gateway-a".to_owned(),
            GatewayPodIdentity {
                name: "gateway-a".to_owned(),
                uid: "uid-new".to_owned(),
                restart_count: 0,
            },
        )]);
        assert!(
            ensure_gateway_pods_retained(&baseline, &replaced).is_err(),
            "a same-name replacement Pod makes phase log continuity unprovable"
        );
        assert!(
            ensure_gateway_pods_retained(&baseline, &GatewayPodSnapshot::new()).is_err(),
            "a removed Gateway Pod makes phase log continuity unprovable"
        );
        let restarted = GatewayPodSnapshot::from([(
            "gateway-a".to_owned(),
            GatewayPodIdentity {
                name: "gateway-a".to_owned(),
                uid: "uid-old".to_owned(),
                restart_count: 1,
            },
        )]);
        ensure_gateway_pods_retained(&baseline, &restarted)?;
        Ok(())
    }

    #[test]
    fn bounded_command_kills_descendant_holding_output_after_parent_exits() -> Result<(), Box<dyn Error>> {
        #[cfg(unix)]
        {
            let directory = tempfile::tempdir()?;
            let child_pid_path = directory.path().join("child.pid");
            let mut command = Command::new("sh");
            command.args([
                "-c",
                "sleep 30 & echo $! > \"$1\"; echo descendant-started; exit 0",
                "qualification-test",
            ]);
            command.arg(&child_pid_path);
            let started = Instant::now();
            let result = bounded_command(command, Duration::from_secs(2), "descendant fixture");
            let elapsed = started.elapsed();
            assert!(
                result.is_err(),
                "a command that leaves a background descendant must fail rather than return early"
            );
            let error = result.err().ok_or("descendant fixture unexpectedly returned success")?;
            assert!(
                error.contains("left descendants in its process group"),
                "failure should explain orphan cleanup, got {error:?}"
            );
            assert!(
                elapsed < Duration::from_secs(2),
                "output collection exceeded its deadline: {elapsed:?}"
            );
            let child_pid = fs::read_to_string(child_pid_path)?.trim().parse::<u32>()?;
            let child_stat = fs::read_to_string(format!("/proc/{child_pid}/stat"));
            if let Ok(stat) = child_stat {
                let state = stat
                    .rsplit_once(") ")
                    .and_then(|(_, tail)| tail.chars().next())
                    .ok_or("could not parse spawned child state")?;
                assert!(
                    matches!(state, 'Z' | 'X'),
                    "descendant process {child_pid} must be gone or zombie after cleanup, state={state}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn bounded_command_keeps_timeout_output_diagnostics() {
        let mut command = Command::new("sh");
        command.args(["-c", "echo timeout-diagnostic >&2; exec sleep 30"]);
        let started = Instant::now();
        let result = bounded_command(command, Duration::from_millis(100), "timeout fixture");
        assert!(result.is_err(), "a command exceeding its deadline must fail");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timeout cleanup must remain bounded: {:?}",
            started.elapsed()
        );
        let error = result.err().unwrap_or_default();
        assert!(
            error.contains("timed out after") && error.contains("timeout-diagnostic"),
            "timeout failure must retain command output, got {error:?}"
        );
    }

    #[test]
    fn outcome_enum_preserves_existing_evidence_strings() -> Result<(), Box<dyn Error>> {
        assert_eq!(serde_json::to_string(&Outcome::Running)?, "\"RUNNING\"");
        assert_eq!(serde_json::to_string(&Outcome::Pass)?, "\"PASS\"");
        assert_eq!(serde_json::to_string(&Outcome::Fail)?, "\"FAIL\"");
        Ok(())
    }

    #[test]
    fn a_failed_response_is_attributed_only_by_a_matching_gateway_upstream_error_log() {
        let log = "upstream error status=502 upstream_address=\"provider-b-old.grid-system.svc:8080\"";
        assert!(is_provider_failure_log(log, "provider-b-old.grid-system.svc:8080"));
        let ansi_log = "upstream error \u{1b}[3mstatus\u{1b}[0m=\u{1b}[0m502 upstream_address=\"provider-b-old.grid-system.svc:8080\"";
        assert!(is_provider_failure_log(ansi_log, "provider-b-old.grid-system.svc:8080"));
        assert_eq!(strip_ansi_codes("status\u{1b}[0m=\u{1b}[0m502"), "status=502");
        assert!(!is_provider_failure_log(log, "provider-a.grid-system.svc:8080"));
        assert!(!is_provider_failure_log(
            "upstream error status=502 upstream_address=\"provider-b-old.grid-system.svc:8080\"",
            "provider-b-replacement.grid-system.svc:8080"
        ));
        assert!(!is_provider_failure_log(
            "upstream error status=503 upstream_address=\"provider-b-old.grid-system.svc:8080\"",
            "provider-b-old.grid-system.svc:8080"
        ));
    }

    #[test]
    fn deployment_stacks_apply_after_cluster_creation_in_dependency_order() {
        assert_eq!(
            STACK_APPLICATION_ORDER,
            ["operator", "mock-providers", "grid-resources", "embedded-gateway"]
        );
    }

    #[test]
    fn fingerprint_changes_when_an_untracked_source_file_changes() {
        let first = source_fingerprint_from_parts(b"tracked", &[("fixture.yaml".to_owned(), b"one".to_vec())]);
        let second = source_fingerprint_from_parts(b"tracked", &[("fixture.yaml".to_owned(), b"two".to_vec())]);
        assert_ne!(first, second);
    }

    #[test]
    fn reused_evidence_directory_fails_without_overwriting_prior_results() -> Result<(), Box<dyn Error>> {
        let parent = tempfile::tempdir()?;
        let evidence_dir = parent.path().join("evidence");
        create_evidence_dir(&evidence_dir)?;
        let prior_result = evidence_dir.join("evidence.json");
        fs::write(&prior_result, b"prior run")?;

        assert!(
            create_evidence_dir(&evidence_dir).is_err(),
            "a reused evidence directory must be rejected"
        );

        assert_eq!(
            fs::read(prior_result)?,
            b"prior run",
            "refusing reuse must preserve existing evidence"
        );
        Ok(())
    }
}
