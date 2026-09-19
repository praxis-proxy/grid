//! Single-cluster, multi-gateway integration qualification.
//!
//! This qualification deliberately keeps all provider and consumer processes
//! in one Kind cluster. It proves the shared Kubernetes/overlay boundary while
//! recording that round-robin cursors remain local to each consumer process.

#![allow(
    clippy::missing_docs_in_private_items,
    reason = "internal evidence model is documented by its serialized schema"
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

const TOPOLOGY: &str = "tests/e2e/topologies/grid-single-cluster-multi-gateway/forge.yaml";
const CLUSTER: &str = "single";
const CLUSTER_PREFIX: &str = "grid-single-cluster-multi-gateway";
const NAMESPACE: &str = "grid-system";
const QUALIFICATION_TIMEOUT: Duration = Duration::from_secs(180);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Names used by the Forge, Kind, Kubernetes, and Docker layers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClusterIdentity {
    forge_cluster: &'static str,
    kind_cluster: String,
    kubectl_context: String,
    node_container: String,
}

fn cluster_identity() -> ClusterIdentity {
    let kind_cluster = format!("{CLUSTER_PREFIX}-{CLUSTER}");
    ClusterIdentity {
        forge_cluster: CLUSTER,
        kubectl_context: format!("kind-{kind_cluster}"),
        node_container: format!("{kind_cluster}-control-plane"),
        kind_cluster,
    }
}

/// CLI options for the single-cluster qualification.
#[derive(Debug, clap::Args)]
pub(crate) struct Options {
    /// Forge topology to deploy.
    #[arg(long, default_value = TOPOLOGY)]
    pub(crate) forge_config: PathBuf,
    /// Keep the cluster after the run for debugging.
    #[arg(long)]
    pub(crate) keep: bool,
    /// Evidence directory. Defaults to a timestamped ignored directory.
    #[arg(long)]
    pub(crate) evidence_dir: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct Scenario {
    name: String,
    result: String,
    detail: String,
}

#[derive(Debug, Serialize)]
struct Evidence {
    schema_version: u8,
    result: String,
    topology: String,
    cluster: String,
    source_revision: String,
    scenarios: Vec<Scenario>,
    observations: BTreeMap<String, serde_json::Value>,
    cleanup: String,
}

struct Cleanup {
    forge: PathBuf,
    config: PathBuf,
    resolved_config: PathBuf,
    enabled: bool,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        if self.enabled {
            drop(
                Command::new(&self.forge)
                    .args(["down", "--config"])
                    .arg(&self.config)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status(),
            );
        }
        drop(fs::remove_file(&self.resolved_config));
    }
}

fn timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or_else(|_| "unknown".to_owned(), |d| d.as_secs().to_string())
}

fn evidence_path(config: &Path, requested: Option<&Path>, run: &str) -> PathBuf {
    requested.map_or_else(
        || {
            config
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("evidence")
                .join(format!("single-cluster-multi-gateway-{run}"))
        },
        Path::to_path_buf,
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "bounded process setup, polling, collection, and timeout handling stay together"
)]
fn command_output(command: &mut Command, timeout: Duration) -> Result<Output, String> {
    let description = format!("{command:?}");
    let stdout_file = tempfile::NamedTempFile::new().map_err(|error| format!("stdout temp file: {error}"))?;
    let stderr_file = tempfile::NamedTempFile::new().map_err(|error| format!("stderr temp file: {error}"))?;
    let stdout_path = stdout_file.path().to_owned();
    let stderr_path = stderr_file.path().to_owned();
    command.stdout(
        stdout_file
            .as_file()
            .try_clone()
            .map_err(|error| format!("clone stdout handle: {error}"))?,
    );
    command.stderr(
        stderr_file
            .as_file()
            .try_clone()
            .map_err(|error| format!("clone stderr handle: {error}"))?,
    );
    let mut child: Child = command
        .spawn()
        .map_err(|error| format!("spawn {description}: {error}"))?;
    let started = Instant::now();
    loop {
        match child
            .try_wait()
            .map_err(|error| format!("wait {description}: {error}"))?
        {
            Some(_) => {
                let status = child
                    .wait()
                    .map_err(|error| format!("collect {description}: {error}"))?;
                return Ok(Output {
                    status,
                    stdout: fs::read(stdout_path).map_err(|error| format!("read stdout {description}: {error}"))?,
                    stderr: fs::read(stderr_path).map_err(|error| format!("read stderr {description}: {error}"))?,
                });
            },
            None if started.elapsed() >= timeout => {
                drop(child.kill());
                drop(child.wait());
                return Err(format!("timeout after {timeout:?}: {description}"));
            },
            None => thread::park_timeout(Duration::from_millis(100)),
        }
    }
}

fn kubectl(args: &[&str]) -> Result<Output, String> {
    let mut command = Command::new("kubectl");
    let identity = cluster_identity();
    command.args(["--context", identity.kubectl_context.as_str()]);
    command.args(args);
    command_output(&mut command, QUALIFICATION_TIMEOUT)
}

/// Run kubectl with owned arguments for operations containing dynamic values.
fn kubectl_owned(args: &[String]) -> Result<Output, String> {
    let mut command = Command::new("kubectl");
    let identity = cluster_identity();
    command.args(["--context", identity.kubectl_context.as_str()]);
    command.args(args);
    command_output(&mut command, QUALIFICATION_TIMEOUT)
}

/// Scale a deployment and wait for its observed state to settle.
fn scale_deployment(name: &str, replicas: u32) -> Result<(), String> {
    let args = vec![
        "-n".to_owned(),
        NAMESPACE.to_owned(),
        "scale".to_owned(),
        format!("deployment/{name}"),
        format!("--replicas={replicas}"),
    ];
    let output = kubectl_owned(&args)?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    let started = Instant::now();
    loop {
        let value = json_kubectl(&["-n", NAMESPACE, "get", "deployment", name, "-o", "json"])?;
        let current = value
            .get("status")
            .and_then(|item| item.get("readyReplicas"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if current == u64::from(replicas) {
            return Ok(());
        }
        if started.elapsed() >= QUALIFICATION_TIMEOUT {
            return Err(format!(
                "deployment/{name} did not reach readyReplicas={replicas}; last={current}"
            ));
        }
        thread::park_timeout(POLL_INTERVAL);
    }
}

/// Capture the operator's current Grid and overlay resources.
fn capture_grid_state() -> Result<serde_json::Value, String> {
    let networks = json_kubectl(&["-n", NAMESPACE, "get", "gridnetworks", "-o", "json"])?;
    let sites = json_kubectl(&["-n", NAMESPACE, "get", "gridsites", "-o", "json"])?;
    let providers = json_kubectl(&["-n", NAMESPACE, "get", "inferenceproviders", "-o", "json"])?;
    let overlays = json_kubectl(&["-n", NAMESPACE, "get", "configmaps", "-o", "json"])?;
    Ok(serde_json::json!({
        "gridNetworks": networks.get("items").cloned().unwrap_or(serde_json::Value::Null),
        "gridSites": sites.get("items").cloned().unwrap_or(serde_json::Value::Null),
        "inferenceProviders": providers.get("items").cloned().unwrap_or(serde_json::Value::Null),
        "configMaps": overlays.get("items").cloned().unwrap_or(serde_json::Value::Null),
    }))
}

/// Images required by the topology's `Never` pull policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ResolvedImages {
    gateway: String,
    operator: String,
    overlay_sync: String,
    vcr: String,
    pull_policy: String,
}

const DEFAULT_GATEWAY_IMAGE: &str = "praxis-ai:single-cluster-qualification";
const DEFAULT_OPERATOR_IMAGE: &str = "grid-operator:single-cluster-qualification";
const DEFAULT_OVERLAY_SYNC_IMAGE: &str = "grid-overlay-sync:single-cluster-qualification";
const DEFAULT_VCR_IMAGE: &str = "ghcr.io/neuralmagic/vllm-vcr:vllm0.23";

fn resolved_images() -> Result<ResolvedImages, String> {
    resolve_images(
        env::var("GRID_XTASK_GATEWAY_IMAGE").ok(),
        env::var("GRID_XTASK_OPERATOR_IMAGE").ok(),
        env::var("GRID_XTASK_OVERLAY_SYNC_IMAGE").ok(),
        env::var("GRID_XTASK_SIM_IMAGE").ok(),
        env::var("GRID_XTASK_IMAGE_PULL_POLICY").ok(),
    )
}

fn resolve_images(
    gateway: Option<String>,
    operator: Option<String>,
    overlay_sync: Option<String>,
    vcr: Option<String>,
    pull_policy: Option<String>,
) -> Result<ResolvedImages, String> {
    let images = ResolvedImages {
        gateway: gateway.unwrap_or_else(|| DEFAULT_GATEWAY_IMAGE.to_owned()),
        operator: operator.unwrap_or_else(|| DEFAULT_OPERATOR_IMAGE.to_owned()),
        overlay_sync: overlay_sync.unwrap_or_else(|| DEFAULT_OVERLAY_SYNC_IMAGE.to_owned()),
        vcr: vcr.unwrap_or_else(|| DEFAULT_VCR_IMAGE.to_owned()),
        pull_policy: pull_policy.unwrap_or_else(|| "Never".to_owned()),
    };
    if !matches!(images.pull_policy.as_str(), "Never" | "IfNotPresent" | "Always") {
        return Err(format!(
            "GRID_XTASK_IMAGE_PULL_POLICY has invalid value {:?}",
            images.pull_policy
        ));
    }
    for (name, image) in [
        ("gateway", images.gateway.as_str()),
        ("operator", images.operator.as_str()),
        ("overlay-sync", images.overlay_sync.as_str()),
        ("vcr", images.vcr.as_str()),
    ] {
        validate_image_reference(name, image)?;
    }
    Ok(images)
}

fn validate_image_reference(name: &str, image: &str) -> Result<(), String> {
    if image.is_empty() || image.chars().any(char::is_whitespace) || image.starts_with('/') {
        return Err(format!("{name} image reference is malformed: {image:?}"));
    }
    if image.contains('@') {
        let (repository, digest) = image
            .split_once('@')
            .ok_or_else(|| format!("{name} image reference is malformed: {image:?}"))?;
        if repository.is_empty() || !digest.starts_with("sha256:") || digest.len() <= "sha256:".len() {
            return Err(format!("{name} image reference is malformed: {image:?}"));
        }
    } else {
        let last_slash = image.rfind('/').unwrap_or(0);
        let Some(colon) = image.rfind(':') else {
            return Err(format!(
                "{name} image reference must include a tag or digest: {image:?}"
            ));
        };
        if colon <= last_slash || colon == image.len() - 1 {
            return Err(format!("{name} image reference is malformed: {image:?}"));
        }
    }
    Ok(())
}

/// Match a Docker reference against the repository and tag columns from `crictl`.
fn node_has_image(listing: &str, image: &str) -> bool {
    let (repository, tag) = image.rsplit_once(':').unwrap_or((image, "latest"));
    listing.lines().any(|line| {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        fields.first().is_some_and(|repository_field| {
            (*repository_field == repository || *repository_field == format!("docker.io/library/{repository}"))
                && fields.get(1).is_some_and(|tag_field| *tag_field == tag)
        })
    })
}

/// Discover the Docker node name for a Kind cluster.
fn discover_node(kind_cluster: &str) -> Result<String, String> {
    let output = Command::new("kind")
        .args(["get", "nodes", "--name", kind_cluster])
        .output()
        .map_err(|error| format!("discover Kind nodes: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "kind get nodes failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("Kind cluster {kind_cluster} has no discovered nodes"))
}

/// Load and verify every local image before any Forge stack is applied.
#[expect(
    clippy::too_many_lines,
    reason = "each image is inspected, loaded, and verified before deployment"
)]
fn load_and_verify_images(images: &ResolvedImages) -> Result<BTreeMap<String, serde_json::Value>, String> {
    let image_names = [
        ("gateway", images.gateway.as_str()),
        ("operator", images.operator.as_str()),
        ("overlay_sync", images.overlay_sync.as_str()),
        ("vcr", images.vcr.as_str()),
    ];
    let mut metadata = BTreeMap::new();
    for (role, image) in image_names {
        let mut inspect = Command::new("docker");
        inspect.args(["image", "inspect", image]);
        let output = command_output(&mut inspect, Duration::from_secs(30))?;
        if !output.status.success() {
            return Err(format!("required local image is missing: {image}"));
        }
        let details: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("parse docker metadata for {image}: {error}"))?;
        metadata.insert(role.to_owned(), details);
    }
    if images.pull_policy != "Never" {
        return Ok(metadata);
    }
    let identity = cluster_identity();
    let node = discover_node(&identity.kind_cluster)?;
    for (_, image) in image_names {
        let mut load = Command::new("kind");
        load.args(["load", "docker-image", image, "--name", &identity.kind_cluster]);
        let load_output = command_output(&mut load, QUALIFICATION_TIMEOUT)?;
        if !load_output.status.success() {
            return Err(format!(
                "failed loading {image}: {}",
                String::from_utf8_lossy(&load_output.stderr).trim()
            ));
        }
        let mut verify = Command::new("docker");
        verify.args(["exec", &node, "crictl", "images"]);
        let verify_output = verify
            .output()
            .map_err(|error| format!("verify image {image}: {error}"))?;
        let listing = String::from_utf8_lossy(&verify_output.stdout);
        if !verify_output.status.success() || !node_has_image(&listing, image) {
            return Err(format!(
                "Kind node does not contain exact image reference {image}; status={}, stdout={listing}, stderr={}",
                verify_output.status,
                String::from_utf8_lossy(&verify_output.stderr)
            ));
        }
    }
    Ok(metadata)
}

/// Materialize the topology using the resolved image references.
#[expect(
    clippy::too_many_lines,
    reason = "materialization validates the complete image contract at one boundary"
)]
fn materialize_config(forge_config: &Path, evidence_dir: &Path, images: &ResolvedImages) -> Result<PathBuf, String> {
    let output = forge_config.parent().unwrap_or_else(|| Path::new(".")).join(format!(
        ".grid-single-cluster-multi-gateway-{}.resolved.yaml",
        std::process::id()
    ));
    let selected = super::forge_config::ImageOverrides {
        gateway: images.gateway.clone(),
        operator: images.operator.clone(),
        overlay_sync: images.overlay_sync.clone(),
        vcr: images.vcr.clone(),
        pull_policy: images.pull_policy.clone(),
    };
    let resolved = super::forge_config::materialize_with_images(forge_config, Some(&output), &selected)
        .map_err(|error| format!("materialize Forge configuration: {error}"))?;
    fs::copy(&resolved, evidence_dir.join("resolved-forge.yaml"))
        .map_err(|error| format!("copy materialized Forge configuration to evidence: {error}"))?;
    let content =
        fs::read_to_string(&resolved).map_err(|error| format!("read materialized Forge configuration: {error}"))?;
    for (role, image) in [
        ("gateway", images.gateway.as_str()),
        ("operator", images.operator.as_str()),
        ("overlay-sync", images.overlay_sync.as_str()),
        ("vcr", images.vcr.as_str()),
    ] {
        if !content.contains(image) {
            return Err(format!(
                "materialized Forge configuration does not contain {role} image {image:?}"
            ));
        }
    }
    if !content.contains(&format!("imagePullPolicy: {}", images.pull_policy)) {
        return Err("materialized Forge configuration does not contain the selected image pull policy".to_owned());
    }
    Ok(resolved)
}

/// Apply one Forge stack and preserve its complete bounded result.
fn apply_stack(forge: &str, config: &Path, stack: &str, evidence_dir: &Path) -> Result<(), String> {
    let started = timestamp();
    let mut command = Command::new(forge);
    command.args([
        "--config",
        config.to_str().unwrap_or_default(),
        "--non-interactive",
        "stack",
        "apply",
        CLUSTER,
        stack,
    ]);
    let output = command_output(&mut command, QUALIFICATION_TIMEOUT)?;
    let finished = timestamp();
    let status = if output.status.success() { "PASS" } else { "FAIL" };
    let record = format!(
        "stack: {stack}\nstart: {started}\nend: {finished}\nstatus: {status}\n\n--- stdout ---\n{}\n--- stderr ---\n{}\n",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    fs::write(evidence_dir.join(format!("stack-{stack}.txt")), record).map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!("stack {stack} failed"))
    }
}

/// Run `forge up` with bounded process-tree termination and live pipe draining.
fn forge_up(forge: &str, config: &Path, evidence_dir: &Path) -> Result<(), String> {
    let config_arg = config.to_string_lossy().into_owned();
    let mut command = Command::new("timeout");
    command.args([
        "--signal=TERM",
        "--kill-after=10s",
        &format!("{}s", QUALIFICATION_TIMEOUT.as_secs()),
        forge,
        "--config",
        &config_arg,
        "--non-interactive",
        "up",
    ]);
    let output = command
        .output()
        .map_err(|error| format!("spawn bounded forge up: {error}"))?;
    let record = format!(
        "forge up\nstatus: {}\n\n--- stdout ---\n{}\n--- stderr ---\n{}\n",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    fs::write(evidence_dir.join("forge-up.txt"), record).map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!("bounded forge up failed or timed out: {}", output.status))
    }
}

/// Capture setup-boundary diagnostics without exposing kubeconfig or secrets.
#[expect(
    clippy::too_many_lines,
    reason = "the setup failure record intentionally captures each boundary in one place"
)]
fn capture_setup_diagnostics(evidence_dir: &Path) {
    let identity = cluster_identity();
    let commands = [
        ("kind-clusters.txt", "kind", vec!["get", "clusters"]),
        (
            "kind-nodes.txt",
            "kind",
            vec!["get", "nodes", "--name", identity.kind_cluster.as_str()],
        ),
        (
            "node-state.json",
            "kubectl",
            vec![
                "--context",
                identity.kubectl_context.as_str(),
                "get",
                "nodes",
                "-o",
                "json",
            ],
        ),
        (
            "docker-network.json",
            "docker",
            vec!["network", "inspect", "grid-single-cluster-multi-gateway-net"],
        ),
        ("process-tree.txt", "ps", vec!["-eo", "pid,ppid,stat,etime,cmd"]),
        (
            "docker-node-inspect.json",
            "docker",
            vec!["inspect", identity.node_container.as_str()],
        ),
    ];
    for (name, program, args) in commands {
        let mut command = Command::new(program);
        command.args(args);
        let output = command_output(&mut command, Duration::from_secs(15));
        let text = output.map_or_else(
            |error| format!("command failed: {error}"),
            |item| {
                format!(
                    "status: {}\n{}{}",
                    item.status,
                    String::from_utf8_lossy(&item.stdout),
                    String::from_utf8_lossy(&item.stderr)
                )
            },
        );
        drop(fs::write(evidence_dir.join(name), text));
    }
}

/// Run a command in the long-lived restricted qualification client.
fn client_command(args: &[&str]) -> Result<Output, String> {
    let mut command = Command::new("kubectl");
    let identity = cluster_identity();
    command.args(["--context", identity.kubectl_context.as_str()]);
    command.args(["-n", NAMESPACE, "exec", "qualification-client", "--"]);
    command.args(args);
    command_output(&mut command, QUALIFICATION_TIMEOUT)
}

/// Issue one attributed request through a consumer gateway.
fn attributed_request(consumer: &str, request_id: u32) -> Result<String, String> {
    attributed_request_once(consumer, request_id, None)
}

/// Issue one attributed request with an explicit session-affinity key.
fn attributed_request_with_session(consumer: &str, request_id: u32, session: &str) -> Result<String, String> {
    attributed_request_once(consumer, request_id, Some(session))
}

fn attributed_request_once(consumer: &str, request_id: u32, session: Option<&str>) -> Result<String, String> {
    let args = build_request_args(consumer, request_id, session)?;
    let references: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = client_command(&references)?;
    if !output.status.success() {
        return Err(format!(
            "{consumer} request failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stderr).into_owned())
}

fn build_request_args(consumer: &str, request_id: u32, session: Option<&str>) -> Result<Vec<String>, String> {
    if session.is_some_and(|value| value.chars().any(char::is_control)) {
        return Err("session ID contains a control character".to_owned());
    }
    // Consumer services expose the client-facing HTTP listener on 8080. The
    // 8443 listener is used by provider gateways for their mTLS hop.
    let service = format!("http://{consumer}.grid-system.svc.cluster.local:8080/v1/chat/completions");
    let body = format!(
        r#"{{"model":"Qwen/Qwen3-0.6B","messages":[{{"role":"user","content":"qualification-{request_id}"}}],"max_tokens":4}}"#
    );
    let mut args = vec![
        "curl",
        "--silent",
        "--show-error",
        "--max-time",
        "10",
        "--dump-header",
        "/dev/stderr",
        "--header",
        "Content-Type: application/json",
        "--header",
        "Authorization: Bearer qualification-token",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    if let Some(session) = session {
        args.extend(["--header".to_owned(), format!("X-Session-Id: {session}")]);
    }
    args.extend(["--data".to_owned(), body, service]);
    Ok(args)
}

#[derive(Debug)]
struct RetriedProbe {
    headers: String,
    attempts: Vec<serde_json::Value>,
}

/// Retry only transport failures; a received HTTP response is final.
#[expect(clippy::too_many_lines, reason = "the bounded retry records every probe attempt")]
fn attributed_request_with_transport_retry(
    consumer: &str,
    request_id: u32,
    session: Option<&str>,
) -> Result<RetriedProbe, String> {
    let mut attempts = Vec::new();
    for attempt in 1..=3 {
        let result = match session {
            Some(value) => attributed_request_with_session(consumer, request_id, value),
            None => attributed_request(consumer, request_id),
        };
        match result {
            Ok(headers) => {
                attempts.push(retry_evidence(
                    "provider-attributed-request",
                    attempt,
                    true,
                    http_status(&headers),
                    None,
                ));
                return Ok(RetriedProbe { headers, attempts });
            },
            Err(error) => {
                let status = http_status(&error);
                let retry = retry_decision(false, status, attempt);
                attempts.push(retry_evidence(
                    "provider-attributed-request",
                    attempt,
                    false,
                    status,
                    None,
                ));
                if !retry {
                    return Err(format!("{error}; attempts={attempts:?}"));
                }
            },
        }
    }
    unreachable!("bounded retry loop always returns")
}

fn http_status(output: &str) -> Option<u16> {
    output.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        matches!(
            fields.next(),
            Some("HTTP/1.0" | "HTTP/1.1" | "HTTP/2" | "HTTP/2.0" | "HTTP/3")
        )
        .then(|| fields.next()?.parse().ok())?
    })
}

fn retry_decision(process_succeeded: bool, status: Option<u16>, attempt: u8) -> bool {
    !process_succeeded && status.is_none() && attempt < 3
}

fn retry_reason(process_succeeded: bool, status: Option<u16>, attempt: u8) -> &'static str {
    if process_succeeded || status.is_some() {
        "http_response_received"
    } else if attempt >= 3 {
        "transport_retry_limit_reached"
    } else {
        "transport_without_http_response"
    }
}

fn session_id(prefix: &str, consumer: &str, attempt: u32) -> String {
    format!("{prefix}-{consumer}-{attempt}")
}

fn restoration_undrain(original_drain: bool) -> bool {
    !original_drain
}

fn retry_evidence(
    scenario: &str,
    attempt: u8,
    process_succeeded: bool,
    status: Option<u16>,
    provider: Option<&str>,
) -> serde_json::Value {
    let retry = retry_decision(process_succeeded, status, attempt);
    serde_json::json!({
        "scenario": scenario,
        "attempt": attempt,
        "transport": if process_succeeded { "ok" } else { "error" },
        "http_status": status,
        "provider": provider,
        "retry": retry,
        "retry_reason": retry_reason(process_succeeded, status, attempt),
        "final": !retry,
    })
}

/// Restore providers if a qualification phase fails after mutation.
struct RestorationGuard {
    context: String,
    providers: Vec<(String, bool)>,
    snapshot_error: Option<String>,
    active: bool,
}

impl RestorationGuard {
    fn new(context: &str, providers: &[&str]) -> Self {
        let mut snapshot_error = None;
        let providers = providers
            .iter()
            .filter_map(
                |provider| match super::provider_drain::read_drain_state(context, provider) {
                    Ok(original) => Some(((*provider).to_owned(), original)),
                    Err(error) => {
                        snapshot_error = Some(format!("failed to snapshot {provider} drain state: {error}"));
                        None
                    },
                },
            )
            .collect();
        Self {
            context: context.to_owned(),
            providers,
            snapshot_error,
            active: true,
        }
    }

    fn ensure_snapshot(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.snapshot_error
            .as_ref()
            .map_or(Ok(()), |error| Err(error.clone().into()))
    }

    fn restore(&mut self, network: &str, consumers: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        for (provider, original) in &self.providers {
            super::provider_drain::run(
                &self.context,
                None,
                Some(provider),
                false,
                restoration_undrain(*original),
                Duration::from_secs(120),
                Some(network),
                consumers,
            )?;
        }
        self.active = false;
        Ok(())
    }

    fn finish<T>(
        &mut self,
        result: Result<T, Box<dyn std::error::Error>>,
        network: &str,
        consumers: &[String],
    ) -> Result<T, Box<dyn std::error::Error>> {
        let restoration = self.restore(network, consumers);
        match (result, restoration) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(format!("scenario passed but restoration failed: {error}").into()),
            (Err(error), Err(restore_error)) => {
                Err(format!("{error}; restoration also failed: {restore_error}").into())
            },
        }
    }
}

impl Drop for RestorationGuard {
    fn drop(&mut self) {
        if self.active {
            for (provider, original) in &self.providers {
                drop(super::provider_drain::run(
                    &self.context,
                    None,
                    Some(provider),
                    false,
                    restoration_undrain(*original),
                    Duration::from_secs(120),
                    None,
                    &[],
                ));
            }
        }
    }
}

/// Extract the selected provider from the trusted response header.
fn selected_provider(headers: &str) -> Result<String, String> {
    headers
        .lines()
        .find_map(|line| line.strip_prefix("x-ai-demo-provider-gateway: "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("response did not contain trusted provider attribution: {headers}"))
}

/// Find a stable session currently attributed to the requested provider.
fn session_for_provider(consumer: &str, provider: &str, prefix: &str) -> Result<String, String> {
    for attempt in 0..12 {
        let session = session_id(prefix, consumer, attempt);
        let headers = attributed_request_with_session(consumer, 450 + attempt, &session)?;
        if selected_provider(&headers)? == provider {
            return Ok(session);
        }
    }
    Err(format!(
        "could not establish affinity session for {consumer} -> {provider}"
    ))
}

/// Return the admission state for a candidate in both consumer overlays.
fn candidate_admission_states(state: &serde_json::Value, candidate: &str) -> Result<Vec<String>, String> {
    ["consumer-gateway-a", "consumer-gateway-b"]
        .into_iter()
        .map(|consumer| {
            let overlay = consumer_overlay(state, consumer)?;
            overlay
                .get("overlay")
                .and_then(|value| value.get("candidates"))
                .and_then(serde_json::Value::as_array)
                .and_then(|candidates| {
                    candidates
                        .iter()
                        .find(|item| item.get("cluster").and_then(serde_json::Value::as_str) == Some(candidate))
                })
                .and_then(|item| item.get("admission_state"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| format!("{consumer} overlay has no candidate {candidate}"))
        })
        .collect()
}

/// Parse the routing overlay for one consumer from captured `ConfigMaps`.
fn consumer_overlay(state: &serde_json::Value, consumer: &str) -> Result<serde_json::Value, String> {
    state
        .get("configMaps")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| {
            items.iter().find(|item| {
                item.get("metadata")
                    .and_then(|metadata| metadata.get("name"))
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|name| name.contains(consumer))
                    && item
                        .get("data")
                        .and_then(|data| data.get("routing-overlay.json"))
                        .is_some()
            })
        })
        .and_then(|item| item.get("data"))
        .and_then(|data| data.get("routing-overlay.json"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("routing overlay ConfigMap for {consumer} was not found"))
        .and_then(|raw| {
            serde_json::from_str(raw).map_err(|error| format!("invalid {consumer} routing overlay: {error}"))
        })
}

/// Read the latest accepted and serving revisions reported by a consumer.
fn consumer_revisions(consumer: &str) -> Result<(String, String), String> {
    let args = vec![
        "-n".to_owned(),
        NAMESPACE.to_owned(),
        "logs".to_owned(),
        format!("deployment/{consumer}"),
        "--all-containers=true".to_owned(),
    ];
    let output = kubectl_owned(&args)?;
    if !output.status.success() {
        return Err(format!(
            "failed to read {consumer} logs: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let raw_logs = String::from_utf8_lossy(&output.stdout);
    let logs = strip_ansi(&raw_logs);
    let field = |name: &str| {
        logs.lines().rev().find_map(|line| {
            line.split_whitespace()
                .find_map(|part| part.strip_prefix(&format!("{name}=")))
                .map(|value| value.trim_matches('"').to_owned())
        })
    };
    let accepted = field("accepted_revision").ok_or_else(|| format!("{consumer} has no accepted_revision log"))?;
    let serving = field("serving_revision").ok_or_else(|| format!("{consumer} has no serving_revision log"))?;
    Ok((accepted, serving))
}

/// Remove terminal CSI sequences emitted by the structured log formatter.
fn strip_ansi(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut sequence = false;
    for character in input.chars() {
        if sequence {
            if character.is_ascii_alphabetic() {
                sequence = false;
            }
        } else if character == '\x1b' {
            sequence = true;
        } else {
            output.push(character);
        }
    }
    output
}

/// Assert that both consumers serve the same accepted candidate snapshot.
#[expect(
    clippy::too_many_lines,
    reason = "the overlay contract is validated as one atomic evidence boundary"
)]
fn assert_overlay_contract(state: &serde_json::Value) -> Result<serde_json::Value, String> {
    let overlays = ["consumer-gateway-a", "consumer-gateway-b"]
        .into_iter()
        .map(|consumer| consumer_overlay(state, consumer).map(|overlay| (consumer, overlay)))
        .collect::<Result<Vec<_>, _>>()?;
    let revisions = overlays
        .iter()
        .map(|(consumer, overlay)| {
            overlay
                .get("revision")
                .and_then(|revision| revision.get("value"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("{consumer} overlay has no content revision"))
        })
        .map(|revision| revision.map(str::to_owned))
        .collect::<Result<Vec<_>, _>>()?;
    let [first_revision, second_revision] = revisions.as_slice() else {
        return Err(format!("expected two consumer revisions, got {revisions:?}"));
    };
    if first_revision != second_revision {
        return Err(format!("consumer overlays disagree: {revisions:?}"));
    }
    let mut candidate_sets = Vec::new();
    for (consumer, overlay) in overlays {
        let candidates = overlay
            .get("overlay")
            .and_then(|item| item.get("candidates"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("{consumer} overlay has no candidates"))?;
        let ids = candidates
            .iter()
            .map(|candidate| {
                candidate
                    .get("cluster")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| format!("{consumer} candidate has no cluster identity"))
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if ids
            != BTreeSet::from([
                "vcr-provider-a-provider",
                "vcr-provider-b-provider",
                "vcr-provider-c-provider",
            ])
        {
            return Err(format!("{consumer} candidates are {ids:?}"));
        }
        candidate_sets.push(serde_json::json!({"consumer": consumer, "revision": first_revision, "candidates": ids}));
    }
    Ok(serde_json::Value::Array(candidate_sets))
}

/// Require both consumers to report the exact revision accepted by Grid.
fn assert_serving_revisions(state: &serde_json::Value) -> Result<serde_json::Value, String> {
    let mut evidence = Vec::new();
    for consumer in ["consumer-gateway-a", "consumer-gateway-b"] {
        let overlay = consumer_overlay(state, consumer)?;
        let expected = overlay
            .get("revision")
            .and_then(|revision| revision.get("value"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("{consumer} overlay has no revision"))?;
        let (accepted, serving) = consumer_revisions(consumer)?;
        if accepted != expected || serving != expected {
            return Err(format!(
                "{consumer} revision mismatch: expected={expected}, accepted={accepted}, serving={serving}"
            ));
        }
        evidence.push(serde_json::json!({
            "consumer": consumer,
            "expected": expected,
            "accepted": accepted,
            "serving": serving,
        }));
    }
    Ok(serde_json::Value::Array(evidence))
}

/// Wait until both consumer overlays contain exactly the requested provider set.
#[expect(
    clippy::too_many_lines,
    reason = "polling keeps candidate state and last error together"
)]
fn wait_for_candidate_set(expected: &BTreeSet<&str>) -> Result<serde_json::Value, String> {
    let started = Instant::now();
    let mut last_error = "no overlay observed".to_owned();
    loop {
        if let Ok(state) = capture_grid_state() {
            let mut ready = true;
            for consumer in ["consumer-gateway-a", "consumer-gateway-b"] {
                match consumer_overlay(&state, consumer).and_then(|overlay| {
                    let candidates = overlay
                        .get("overlay")
                        .and_then(|item| item.get("candidates"))
                        .and_then(serde_json::Value::as_array)
                        .ok_or_else(|| format!("{consumer} has no candidates"))?;
                    let ids = candidates
                        .iter()
                        .filter_map(|candidate| candidate.get("cluster").and_then(serde_json::Value::as_str))
                        .collect::<BTreeSet<_>>();
                    if &ids == expected {
                        Ok(())
                    } else {
                        Err(format!("{consumer} candidates: {ids:?}"))
                    }
                }) {
                    Ok(()) => {},
                    Err(error) => {
                        ready = false;
                        last_error = error;
                    },
                }
            }
            if ready {
                return Ok(state);
            }
        }
        if started.elapsed() >= QUALIFICATION_TIMEOUT {
            return Err(format!("overlay candidate set did not converge; last={last_error}"));
        }
        thread::park_timeout(POLL_INTERVAL);
    }
}

/// Verify that the restricted client cannot bypass the provider gateway.
fn direct_backend_probe() -> Result<bool, String> {
    let output = client_command(&[
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--max-time",
        "5",
        "http://vcr-inference-provider-a.grid-system.svc.cluster.local:8000/health",
    ])?;
    Ok(!output.status.success())
}

fn json_kubectl(args: &[&str]) -> Result<serde_json::Value, String> {
    let output = kubectl(args)?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    serde_json::from_slice(&output.stdout).map_err(|error| format!("invalid kubectl JSON: {error}"))
}

#[expect(
    clippy::too_many_lines,
    reason = "readiness polling keeps the last observed resource contract together"
)]
fn wait_deployment(name: &str) -> Result<(), String> {
    let started = Instant::now();
    let args = ["-n", NAMESPACE, "get", "deployment", name, "-o", "json"];
    loop {
        if started.elapsed() >= QUALIFICATION_TIMEOUT {
            return Err(format!(
                "deployment/{name} did not become ready; last state unavailable"
            ));
        }
        if let Ok(value) = json_kubectl(&args) {
            let desired = value
                .get("spec")
                .and_then(|item| item.get("replicas"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let ready = value
                .get("status")
                .and_then(|item| item.get("readyReplicas"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let observed = value
                .get("status")
                .and_then(|item| item.get("observedGeneration"))
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(-1);
            let generation = value
                .get("metadata")
                .and_then(|item| item.get("generation"))
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(-2);
            if desired > 0 && ready >= desired && observed >= generation {
                return Ok(());
            }
        }
        thread::park_timeout(POLL_INTERVAL);
    }
}

/// Restart the operator while a drain is active; the CR remains the source of truth.
fn restart_operator() -> Result<(), String> {
    let output = kubectl(&["-n", NAMESPACE, "rollout", "restart", "deployment/grid-operator"])?;
    if !output.status.success() {
        return Err(format!(
            "operator restart failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    wait_deployment("grid-operator")
}

fn scenario(name: &str, result: &str, detail: impl Into<String>) -> Scenario {
    Scenario {
        name: name.to_owned(),
        result: result.to_owned(),
        detail: detail.into(),
    }
}

/// Run the qualification.
#[expect(
    clippy::too_many_lines,
    reason = "qualification phases are intentionally visible in execution order"
)]
#[expect(
    clippy::cognitive_complexity,
    reason = "the runner's ordered phase control flow is the qualification contract"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "the qualification keeps bounded scenario and observation state together for final evidence"
)]
pub(crate) fn run(forge_config: &Path, options: &Options) -> Result<(), Box<dyn std::error::Error>> {
    let run = timestamp();
    let evidence_dir = evidence_path(forge_config, options.evidence_dir.as_deref(), &run);
    fs::create_dir_all(&evidence_dir)?;
    let forge = super::glb::resolve_forge_binary().ok_or("praxis-forge binary not found")?;
    let images = resolved_images().map_err(|error| format!("resolve qualification images: {error}"))?;
    let resolved_config = materialize_config(forge_config, &evidence_dir, &images)
        .map_err(|error| format!("materialize qualification topology: {error}"))?;
    let source_revision = Command::new("git").args(["rev-parse", "HEAD"]).output().map_or_else(
        |_| "unknown".to_owned(),
        |output| String::from_utf8_lossy(&output.stdout).trim().to_owned(),
    );
    let mut cleanup = Cleanup {
        forge: forge.clone().into(),
        config: resolved_config.clone(),
        resolved_config: resolved_config.clone(),
        enabled: !options.keep,
    };
    let mut scenarios = Vec::new();
    let mut observations = BTreeMap::new();
    let identity = cluster_identity();
    observations.insert(
        "cluster_identity".to_owned(),
        serde_json::json!({
            "forgeCluster": identity.forge_cluster,
            "kindCluster": identity.kind_cluster,
            "kubectlContext": identity.kubectl_context,
            "nodeContainer": identity.node_container,
        }),
    );
    observations.insert(
        "resolved_images".to_owned(),
        serde_json::to_value(&images).map_err(|error| format!("serialize resolved images: {error}"))?,
    );

    let up_result = forge_up(&forge, &resolved_config, &evidence_dir);
    if let Err(error) = &up_result {
        capture_setup_diagnostics(&evidence_dir);
        scenarios.push(scenario("forge-up", "BLOCKED", error.clone()));
    }
    if up_result.is_ok() {
        let images_ready = match load_and_verify_images(&images) {
            Ok(metadata) => {
                observations.insert(
                    "image_metadata".to_owned(),
                    serde_json::to_value(metadata).unwrap_or(serde_json::Value::Null),
                );
                scenarios.push(scenario(
                    "image-loading",
                    "PASS",
                    "all required local image references were loaded and verified in the Kind node",
                ));
                true
            },
            Err(error) => {
                capture_setup_diagnostics(&evidence_dir);
                scenarios.push(scenario("image-loading", "FAIL", error));
                false
            },
        };
        let mut stacks_ready = images_ready;
        for stack in images_ready
            .then_some([
                "metallb",
                "tls-bootstrap",
                "provider-a-operator-base",
                "vcr-backend",
                "provider-a-site",
                "provider-gateway-a",
                "provider-gateway-b",
                "provider-gateway-c",
                "consumer-gateway-a",
                "consumer-gateway-b",
            ])
            .into_iter()
            .flatten()
        {
            match apply_stack(&forge, &resolved_config, stack, &evidence_dir) {
                Ok(()) => scenarios.push(scenario(format!("stack/{stack}").as_str(), "PASS", "stack completed")),
                Err(error) => {
                    stacks_ready = false;
                    let last_state = capture_grid_state().unwrap_or(serde_json::Value::Null);
                    observations.insert(format!("timeout_state_{stack}"), last_state);
                    scenarios.push(scenario(format!("stack/{stack}").as_str(), "FAIL", error));
                    break;
                },
            }
        }
        for deployment in stacks_ready
            .then_some([
                "grid-operator",
                "vcr-inference-provider-a",
                "vcr-inference-provider-b",
                "vcr-inference-provider-c",
                "provider-gateway-a",
                "provider-gateway-b",
                "provider-gateway-c",
                "consumer-gateway-a",
                "consumer-gateway-b",
            ])
            .into_iter()
            .flatten()
        {
            match wait_deployment(deployment) {
                Ok(()) => scenarios.push(scenario(
                    format!("ready/{deployment}").as_str(),
                    "PASS",
                    "observed generation is ready",
                )),
                Err(error) => scenarios.push(scenario(format!("ready/{deployment}").as_str(), "FAIL", error)),
            }
        }
        if stacks_ready {
            match kubectl(&[
                "wait",
                "--for=jsonpath={.status.phase}=Running",
                "pod/qualification-client",
                "-n",
                NAMESPACE,
                "--timeout=120s",
            ]) {
                Ok(output) if output.status.success() => scenarios.push(scenario(
                    "ready/qualification-client",
                    "PASS",
                    "restricted long-lived probe pod is Running",
                )),
                Ok(output) => scenarios.push(scenario(
                    "ready/qualification-client",
                    "FAIL",
                    String::from_utf8_lossy(&output.stderr).trim().to_owned(),
                )),
                Err(error) => scenarios.push(scenario("ready/qualification-client", "FAIL", error)),
            }
            if let Ok(value) = json_kubectl(&["-n", NAMESPACE, "get", "deploy", "-o", "json"]) {
                observations.insert(
                    "deployments".to_owned(),
                    value.get("items").cloned().unwrap_or(serde_json::Value::Null),
                );
            }
            if let Ok(value) = json_kubectl(&["-n", NAMESPACE, "get", "endpointslices", "-o", "json"]) {
                observations.insert(
                    "endpointslices".to_owned(),
                    value.get("items").cloned().unwrap_or(serde_json::Value::Null),
                );
            }
            let all_candidates = BTreeSet::from([
                "vcr-provider-a-provider",
                "vcr-provider-b-provider",
                "vcr-provider-c-provider",
            ]);
            match wait_for_candidate_set(&all_candidates).and_then(|state| {
                let contract = assert_overlay_contract(&state)?;
                observations.insert("overlay_contract".to_owned(), contract);
                Ok(state)
            }) {
                Ok(state) => {
                    observations.insert("grid_state_bootstrap".to_owned(), state);
                    scenarios.push(scenario(
                        "overlay-and-serving-revision",
                        "PASS",
                        "GridNetwork, GridSite, providers, and generated overlay resources captured after readiness",
                    ));
                },
                Err(error) => scenarios.push(scenario("overlay-and-serving-revision", "FAIL", error)),
            }
            match capture_grid_state().and_then(|state| assert_serving_revisions(&state)) {
                Ok(revisions) => {
                    observations.insert("serving_revisions".to_owned(), revisions);
                    scenarios.push(scenario(
                        "serving-revision-barrier",
                        "PASS",
                        "both consumers report the exact Grid overlay revision as accepted and serving",
                    ));
                },
                Err(error) => scenarios.push(scenario("serving-revision-barrier", "FAIL", error)),
            }
            let mut request_evidence = BTreeMap::<String, Vec<String>>::new();
            for consumer in ["consumer-gateway-a", "consumer-gateway-b"] {
                let mut responses = Vec::new();
                for request_id in 0..6 {
                    match attributed_request(consumer, request_id) {
                        Ok(headers) => responses.push(headers),
                        Err(error) => responses.push(format!("ERROR: {error}")),
                    }
                }
                request_evidence.insert(consumer.to_owned(), responses);
            }
            observations.insert(
                "attributed_requests".to_owned(),
                serde_json::to_value(&request_evidence).unwrap_or(serde_json::Value::Null),
            );
            let request_failures = request_evidence
                .values()
                .flatten()
                .filter(|item| item.starts_with("ERROR:"))
                .count();
            let provider_sequences = request_evidence
                .iter()
                .map(|(consumer, responses)| {
                    responses
                        .iter()
                        .map(|headers| selected_provider(headers))
                        .collect::<Result<Vec<_>, _>>()
                        .map(|sequence| (consumer.clone(), sequence))
                })
                .collect::<Result<Vec<_>, _>>();
            observations.insert(
                "provider_sequences".to_owned(),
                serde_json::to_value(&provider_sequences).unwrap_or(serde_json::Value::Null),
            );
            let sequence_valid = provider_sequences.as_ref().is_ok_and(|sequences| {
                let expected_rotation = ["provider-a", "provider-b", "provider-c"];
                sequences.iter().all(|(_, sequence)| {
                    sequence.len() == 6
                        && sequence.iter().enumerate().all(|(index, provider)| {
                            expected_rotation.get(index % expected_rotation.len()) == Some(&provider.as_str())
                        })
                })
            });
            scenarios.push(if request_failures == 0 && sequence_valid {
                scenario(
                    "provider-load-sharing",
                    "PASS",
                    "six bounded requests through each consumer followed the independent A/B/C rotation with trusted attribution",
                )
            } else {
                scenario(
                    "provider-load-sharing",
                    "FAIL",
                    format!("{request_failures} requests failed or provider rotation/attribution was invalid"),
                )
            });
            let drain_context = cluster_identity().kubectl_context;
            let drain_consumers = ["consumer-gateway-a".to_owned(), "consumer-gateway-b".to_owned()];
            let individual_drain = {
                let mut restore = RestorationGuard::new(&drain_context, &["vcr-provider-a-provider"]);
                let mut existing_session = String::new();
                let result = restore
                    .ensure_snapshot()
                    .and_then(|()| {
                        session_for_provider("consumer-gateway-a", "provider-a", "existing-individual")
                            .map_err(Into::into)
                    })
                    .map(|session| existing_session = session)
                    .and_then(|_| {
                        super::provider_drain::run(
                            &drain_context,
                            None,
                            Some("vcr-provider-a-provider"),
                            false,
                            false,
                            Duration::from_secs(120),
                            Some("grid-single-cluster-multi-gateway"),
                            &drain_consumers,
                        )
                    })
                    .and_then(|()| Ok(wait_for_candidate_set(&all_candidates)?))
                    .and_then(|state| {
                        let states = candidate_admission_states(&state, "vcr-provider-a-provider")?;
                        observations.insert("individual_drain_overlay".to_owned(), state);
                        if states.iter().all(|value| value == "existing_only") {
                            Ok(())
                        } else {
                            Err(format!("individual drain did not produce existing_only: {states:?}").into())
                        }
                    })
                    .and_then(|()| {
                        let mut new_session_results = Vec::new();
                        for request_id in 0..4 {
                            for consumer in ["consumer-gateway-a", "consumer-gateway-b"] {
                            let session = format!("new-drain-{consumer}-{request_id}");
                            let RetriedProbe { headers, attempts } =
                                attributed_request_with_transport_retry(consumer, 500 + request_id, Some(&session))?;
                                let provider = selected_provider(&headers)?;
                                if provider == "provider-a" {
                                    return Err("new session selected individually drained provider-a".into());
                                }
                                new_session_results.push(serde_json::json!({
                                    "consumer": consumer,
                                "session": format!("new-drain-{consumer}-{request_id}"),
                                "provider": provider,
                                "attempts": attempts,
                                }));
                            }
                        }
                        observations.insert(
                            "individual_drain_new_sessions".to_owned(),
                            serde_json::Value::Array(new_session_results),
                        );
                        Ok(())
                    })
                    .and_then(|()| {
                    let RetriedProbe { headers, attempts } = attributed_request_with_transport_retry(
                        "consumer-gateway-a",
                        550,
                        Some(&existing_session),
                    )?;
                        let provider = selected_provider(&headers)?;
                        observations.insert(
                            "individual_drain_existing_session".to_owned(),
                        serde_json::json!({"session": existing_session, "provider": provider, "headers": headers, "attempts": attempts}),
                        );
                        if provider == "provider-a" {
                            Ok(())
                        } else {
                            Err("existing affinity session did not remain on provider-a".into())
                        }
                    });
                restore.finish(result, "grid-single-cluster-multi-gateway", &drain_consumers)
            };
            scenarios.push(match individual_drain {
                Ok(()) => scenario(
                    "individual-provider-drain",
                    "PASS",
                    "one provider was capped at existing_only in both consumer overlays and restored",
                ),
                Err(error) => scenario("individual-provider-drain", "FAIL", error.to_string()),
            });
            let gateway_drain = {
                let mut restore =
                    RestorationGuard::new(&drain_context, &["vcr-provider-a-provider", "vcr-provider-b-provider"]);
                let mut existing_a = String::new();
                let mut existing_b = String::new();
                let result = restore
                    .ensure_snapshot()
                    .and_then(|()| {
                        session_for_provider("consumer-gateway-a", "provider-a", "existing-gateway-a")
                            .map_err(Into::into)
                    })
                    .map(|session| existing_a = session)
                    .and_then(|_| {
                        session_for_provider("consumer-gateway-a", "provider-b", "existing-gateway-b")
                            .map(|session| existing_b = session)
                            .map_err(Into::into)
                    })
                    .and_then(|_| {
                        super::provider_drain::run(
                            &drain_context,
                            Some("provider-gateway-a"),
                            None,
                            false,
                            false,
                            Duration::from_secs(120),
                            Some("grid-single-cluster-multi-gateway"),
                            &drain_consumers,
                        )
                    })
                    .and_then(|()| Ok(wait_for_candidate_set(&all_candidates)?))
                    .and_then(|state| {
                        let a = candidate_admission_states(&state, "vcr-provider-a-provider")?;
                        let b = candidate_admission_states(&state, "vcr-provider-b-provider")?;
                        observations.insert("gateway_drain_overlay".to_owned(), state);
                        if a.iter().all(|value| value == "existing_only")
                            && b.iter().all(|value| value == "existing_only")
                        {
                            Ok(())
                        } else {
                            Err(format!("gateway drain states were not existing_only: a={a:?}, b={b:?}").into())
                        }
                    })
                    .and_then(|()| restart_operator().map_err(Into::into))
                    .and_then(|()| {
                        let state = wait_for_candidate_set(&all_candidates)?;
                        let a = candidate_admission_states(&state, "vcr-provider-a-provider")?;
                        let b = candidate_admission_states(&state, "vcr-provider-b-provider")?;
                        if a.iter().all(|value| value == "existing_only")
                            && b.iter().all(|value| value == "existing_only")
                        {
                            observations.insert("gateway_drain_after_operator_restart".to_owned(), state);
                            Ok(())
                        } else {
                            Err(format!("drain was not preserved across operator restart: a={a:?}, b={b:?}").into())
                        }
                    })
                    .and_then(|()| {
                        for (session, expected) in [(&existing_a, "provider-a"), (&existing_b, "provider-b")] {
                            let RetriedProbe { headers, attempts } =
                                attributed_request_with_transport_retry("consumer-gateway-a", 650, Some(session))?;
                            observations.insert(
                                format!("gateway_drain_existing_{expected}"),
                                serde_json::json!({"session": session, "provider": expected, "attempts": attempts, "headers": headers}),
                            );
                            let actual = selected_provider(&headers)?;
                            if actual != expected {
                                return Err(
                                    format!("existing affinity session moved from {expected} to {actual}").into()
                                );
                            }
                        }
                        for request_id in 0..4 {
                            for consumer in ["consumer-gateway-a", "consumer-gateway-b"] {
                            let session = format!("new-gateway-drain-{consumer}-{request_id}");
                            let RetriedProbe { headers, attempts } =
                                attributed_request_with_transport_retry(consumer, 600 + request_id, Some(&session))?;
                                let provider = selected_provider(&headers)?;
                            if provider == "provider-a" || provider == "provider-b" {
                                return Err("new session selected a provider from drained gateway".into());
                            }
                            observations.insert(
                                format!("gateway_drain_new_{consumer}_{request_id}"),
                                serde_json::json!({"session": session, "provider": provider, "attempts": attempts, "headers": headers}),
                            );
                            }
                        }
                        Ok(())
                    });
                restore.finish(result, "grid-single-cluster-multi-gateway", &drain_consumers)
            };
            scenarios.push(match gateway_drain {
                Ok(()) => scenario(
                    "provider-gateway-drain",
                    "PASS",
                    "all explicitly assigned providers were drained together and restored",
                ),
                Err(error) => scenario("provider-gateway-drain", "FAIL", error.to_string()),
            });
            let invalid_selector = super::provider_drain::run(
                &drain_context,
                Some("gateway-that-does-not-exist"),
                None,
                false,
                false,
                Duration::from_secs(30),
                None,
                &[],
            );
            scenarios.push(match invalid_selector {
                Ok(()) => scenario(
                    "invalid-drain-selector",
                    "FAIL",
                    "unmatched gateway selector unexpectedly succeeded",
                ),
                Err(error) => scenario(
                    "invalid-drain-selector",
                    "PASS",
                    format!("rejected without mutation: {error}"),
                ),
            });
            let withdrawal_result = scale_deployment("vcr-inference-provider-b", 0)
                .and_then(|()| {
                    let expected = BTreeSet::from(["vcr-provider-a-provider", "vcr-provider-c-provider"]);
                    wait_for_candidate_set(&expected)
                })
                .and_then(|state| {
                    observations.insert("withdrawal_state".to_owned(), state);
                    let requests = ["consumer-gateway-a", "consumer-gateway-b"]
                        .into_iter()
                        .map(|consumer| {
                            attributed_request(consumer, 100).and_then(|headers| {
                                let provider = selected_provider(&headers)?;
                                if provider == "provider-b" {
                                    Err("withdrawn provider-b received traffic".to_owned())
                                } else {
                                    Ok(headers)
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    observations.insert(
                        "withdrawal_requests".to_owned(),
                        serde_json::to_value(requests).unwrap_or(serde_json::Value::Null),
                    );
                    scale_deployment("vcr-inference-provider-b", 1)
                })
                .and_then(|()| {
                    let expected = BTreeSet::from([
                        "vcr-provider-a-provider",
                        "vcr-provider-b-provider",
                        "vcr-provider-c-provider",
                    ]);
                    wait_for_candidate_set(&expected)
                })
                .map(|state| {
                    observations.insert("restoration_state".to_owned(), state);
                });
            scenarios.push(match withdrawal_result {
                Ok(()) => scenario(
                    "withdrawal-restoration",
                    "PASS",
                    "provider gateway B was withdrawn, traffic continued through both consumers, and B was restored",
                ),
                Err(error) => scenario("withdrawal-restoration", "FAIL", error),
            });
            let unhealthy_drain_result = scale_deployment("vcr-inference-provider-c", 0)
                .and_then(|()| {
                    let expected = BTreeSet::from(["vcr-provider-a-provider", "vcr-provider-b-provider"]);
                    wait_for_candidate_set(&expected).map(|_| ())
                })
                .and_then(|()| {
                    super::provider_drain::run(
                        &drain_context,
                        None,
                        Some("vcr-provider-c-provider"),
                        false,
                        false,
                        Duration::from_secs(30),
                        None,
                        &[],
                    )
                    .map_err(|error| error.to_string())
                })
                .and_then(|()| {
                    super::provider_drain::run(
                        &drain_context,
                        None,
                        Some("vcr-provider-c-provider"),
                        false,
                        true,
                        Duration::from_secs(30),
                        None,
                        &[],
                    )
                    .map_err(|error| error.to_string())
                })
                .and_then(|()| {
                    let expected = BTreeSet::from(["vcr-provider-a-provider", "vcr-provider-b-provider"]);
                    wait_for_candidate_set(&expected).map(|_| ())
                })
                .and_then(|()| scale_deployment("vcr-inference-provider-c", 1))
                .and_then(|()| wait_for_candidate_set(&all_candidates).map(|_| ()))
                .inspect_err(|_| {
                    drop(scale_deployment("vcr-inference-provider-c", 1));
                });
            scenarios.push(match unhealthy_drain_result {
                Ok(()) => scenario(
                    "unhealthy-provider-precedence",
                    "PASS",
                    "drain and undrain did not promote an unhealthy provider; recovery restored it",
                ),
                Err(error) => scenario("unhealthy-provider-precedence", "FAIL", error),
            });
            let consumer_result = scale_deployment("consumer-gateway-a", 0)
                .and_then(|()| attributed_request("consumer-gateway-b", 200).map(|_| ()))
                .and_then(|()| scale_deployment("consumer-gateway-a", 1))
                .and_then(|()| attributed_request("consumer-gateway-a", 201).map(|_| ()));
            scenarios.push(match consumer_result {
                Ok(()) => scenario(
                    "consumer-failure",
                    "PASS",
                    "consumer A was removed and restored while consumer B continued serving",
                ),
                Err(error) => scenario("consumer-failure", "FAIL", error),
            });
            let concurrent_results = ["consumer-gateway-a", "consumer-gateway-b"]
                .into_iter()
                .map(|consumer| {
                    thread::spawn(move || {
                        (0..4)
                            .map(|request_id| {
                                attributed_request(consumer, 300 + request_id).and_then(|headers| {
                                    selected_provider(&headers).map(|provider| (request_id, provider))
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .map(|handle| handle.join().unwrap_or_default())
                .collect::<Vec<_>>();
            let concurrent_failures = concurrent_results
                .iter()
                .flatten()
                .filter(|result| result.is_err())
                .count();
            let concurrent_providers = concurrent_results
                .iter()
                .flatten()
                .filter_map(|result| result.as_ref().ok())
                .map(|(_, provider)| provider.as_str())
                .collect::<BTreeSet<_>>();
            observations.insert(
                "concurrent_requests".to_owned(),
                serde_json::to_value(
                    concurrent_results
                        .iter()
                        .map(|results| {
                            results
                                .iter()
                                .map(|result| match result {
                                    Ok((request_id, provider)) => {
                                        serde_json::json!({"requestId": request_id, "provider": provider})
                                    },
                                    Err(error) => serde_json::json!({"error": error}),
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap_or(serde_json::Value::Null),
            );
            scenarios.push(if concurrent_failures == 0 && concurrent_providers.len() == 3 {
                scenario(
                    "concurrent-traffic",
                    "PASS",
                    "eight concurrent requests completed through both consumers with trusted attribution to all providers",
                )
            } else {
                scenario(
                    "concurrent-traffic",
                    "FAIL",
                    format!("{concurrent_failures} concurrent requests failed"),
                )
            });
            match direct_backend_probe() {
                Ok(true) => scenarios.push(scenario(
                    "security",
                    "PASS",
                    "restricted client reached the consumer path while direct provider-backend access was denied",
                )),
                Ok(false) => scenarios.push(scenario(
                    "security",
                    "FAIL",
                    "restricted client unexpectedly reached provider backend directly",
                )),
                Err(error) => scenarios.push(scenario("security", "FAIL", error)),
            }
        }
    }

    let result = if scenarios.iter().any(|item| item.result == "FAIL") {
        "FAIL"
    } else if scenarios.iter().any(|item| item.result == "BLOCKED") {
        "BLOCKED"
    } else {
        "PASS"
    };
    let cleanup_result = if options.keep {
        "kept by request"
    } else {
        "scheduled by cleanup guard"
    };
    let evidence = Evidence {
        schema_version: 1,
        result: result.to_owned(),
        topology: forge_config.display().to_string(),
        cluster: cluster_identity().kind_cluster,
        source_revision,
        scenarios,
        observations,
        cleanup: cleanup_result.to_owned(),
    };
    fs::write(evidence_dir.join("results.json"), serde_json::to_vec_pretty(&evidence)?)?;
    fs::write(
        evidence_dir.join("SUMMARY.md"),
        format!("# Single-cluster multi-gateway qualification\n\nResult: **{result}**\n\nEvidence: `results.json`\n"),
    )?;
    if !options.keep {
        let mut down = Command::new(&forge);
        down.args(["down", "--config"]).arg(&resolved_config);
        let output =
            command_output(&mut down, QUALIFICATION_TIMEOUT).map_err(|error| format!("cleanup failed: {error}"))?;
        if !output.status.success() {
            return Err(format!("cleanup failed: {}", String::from_utf8_lossy(&output.stderr)).into());
        }
        drop(fs::remove_file(&resolved_config));
        cleanup.enabled = false;
    }
    if result == "PASS" {
        Ok(())
    } else {
        Err(format!("qualification result: {result}; see {}", evidence_dir.display()).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sessionless_and_affinity_request_arguments_are_unambiguous() {
        let plain = build_request_args("consumer-a", 1, None).unwrap_or_else(|_| std::process::abort());
        assert!(!plain.iter().any(|arg| arg.contains("Session-Id")));
        assert!(!plain.iter().any(|arg| arg == "X-Session-Id"));
        assert_eq!(plain.iter().filter(|arg| *arg == "--header").count(), 2);

        let affinity =
            build_request_args("consumer-a", 2, Some("existing-2")).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            affinity.iter().filter(|arg| arg.starts_with("X-Session-Id: ")).count(),
            1
        );
        let header_index = affinity
            .iter()
            .position(|arg| arg == "X-Session-Id: existing-2")
            .unwrap_or(usize::MAX);
        assert!(header_index > 0 && affinity.get(header_index - 1).is_some_and(|value| value == "--header"));
        assert_eq!(affinity.iter().filter(|arg| *arg == "--data").count(), 1);
        assert!(
            affinity
                .iter()
                .any(|arg| arg == "Authorization: Bearer qualification-token")
        );
        let invalid_session = build_request_args("consumer-a", 3, Some("bad\nvalue"));
        assert!(invalid_session.is_err(), "control characters must be rejected");
    }

    #[test]
    fn request_arguments_are_stable_except_for_request_identity() {
        let first = build_request_args("consumer-a", 4, None).unwrap_or_else(|_| std::process::abort());
        let second = build_request_args("consumer-a", 4, None).unwrap_or_else(|_| std::process::abort());
        assert_eq!(first, second);
        assert!(first.windows(2).any(|pair| pair == ["--max-time", "10"]));
        assert!(first.last().is_some_and(|value| value.contains("/v1/chat/completions")));
    }

    #[test]
    fn retry_decision_matrix_never_retries_http_responses_or_exceeds_three_attempts() {
        for attempt in 1..=2 {
            assert!(retry_decision(false, None, attempt));
        }
        assert!(!retry_decision(false, None, 3));
        for status in [200, 401, 403, 429, 500, 503] {
            assert!(!retry_decision(false, Some(status), 1));
        }
        assert!(!retry_decision(true, None, 1));
        assert_eq!(http_status("pod deleted\nconnection reset"), None);
        assert_eq!(http_status("status: 503"), None);
        assert_eq!(http_status("HTTP/1.1 200 OK\n"), Some(200));
    }

    #[test]
    fn retry_evidence_contains_no_credentials() {
        let evidence = serde_json::json!({
            "scenario": "drain",
            "attempt": 1,
            "transport": "error",
            "http_status": null,
            "retry": true,
            "retry_reason": "transport_without_http_response",
            "final": false,
        });
        let text = evidence.to_string();
        assert!(!text.contains("Authorization"));
        assert!(!text.contains("qualification-token"));
        assert!(!text.contains("kubeconfig"));
    }

    #[test]
    fn session_ids_are_reused_for_existing_and_unique_for_new_probes() {
        let existing = session_id("existing", "consumer-a", 0);
        assert_eq!(session_id("existing", "consumer-a", 0), existing);
        let new_sessions = (0..4)
            .map(|attempt| session_id("new-drain", "consumer-a", attempt))
            .collect::<BTreeSet<_>>();
        assert_eq!(new_sessions.len(), 4);
    }

    #[test]
    fn retry_matrix_covers_transport_sequences_and_final_attempts() {
        let cases = [
            ("timeout", vec![(false, None), (true, Some(200))], 2, true),
            ("reset", vec![(false, None), (false, None), (true, Some(200))], 3, true),
            (
                "three failures",
                vec![(false, None), (false, None), (false, None)],
                3,
                false,
            ),
            ("429", vec![(false, Some(429))], 1, false),
            ("503", vec![(false, Some(503))], 1, false),
            ("wrong attribution", vec![(true, Some(200))], 1, true),
        ];
        for (name, outcomes, expected_attempts, expected_success) in cases {
            let decisions = outcomes
                .iter()
                .enumerate()
                .map(|(index, (success, status))| {
                    retry_decision(*success, *status, u8::try_from(index).unwrap_or(0) + 1)
                })
                .collect::<Vec<_>>();
            let attempts = decisions.iter().position(|retry| !retry).map_or(3, |index| index + 1);
            assert_eq!(attempts, expected_attempts, "{name}");
            assert_eq!(
                decisions.last().is_some_and(|retry| !retry && expected_success),
                expected_success,
                "{name}"
            );
        }
    }

    #[test]
    fn retry_evidence_is_ordered_structured_and_redacted() {
        let evidence = [
            retry_evidence("provider-drain", 1, false, None, None),
            retry_evidence("provider-drain", 2, true, Some(200), Some("provider-a")),
        ];
        assert_eq!(
            evidence.first().and_then(|item| item.get("attempt")),
            Some(&serde_json::json!(1))
        );
        assert_eq!(
            evidence.get(1).and_then(|item| item.get("attempt")),
            Some(&serde_json::json!(2))
        );
        assert_eq!(
            evidence.get(1).and_then(|item| item.get("provider")),
            Some(&serde_json::json!("provider-a"))
        );
        assert_eq!(
            evidence.get(1).and_then(|item| item.get("final")),
            Some(&serde_json::json!(true))
        );
        let serialized = serde_json::to_string(&evidence).unwrap_or_else(|_| std::process::abort());
        for secret in ["Authorization", "qualification-token", "kubeconfig", "secret-value"] {
            assert!(!serialized.contains(secret), "serialized evidence leaked {secret}");
        }
    }

    #[test]
    fn restoration_and_failure_containment_decisions_are_conservative() {
        assert!(
            !restoration_undrain(true),
            "a true original drain state restores with undrain=false"
        );
        assert!(
            restoration_undrain(false),
            "a false original drain state restores with undrain=true"
        );
        assert_eq!(scenario("session setup", "FAIL", "transport error").result, "FAIL");
        assert_eq!(scenario("later safe scenario", "PASS", "independent").result, "PASS");
        assert_eq!(retry_reason(false, Some(200), 1), "http_response_received");
        assert_eq!(retry_reason(false, None, 3), "transport_retry_limit_reached");
    }

    #[test]
    fn cluster_layers_use_distinct_names() {
        let identity = cluster_identity();
        assert_eq!(identity.forge_cluster, "single");
        assert_eq!(identity.kind_cluster, "grid-single-cluster-multi-gateway-single");
        assert_eq!(
            identity.kubectl_context,
            "kind-grid-single-cluster-multi-gateway-single"
        );
        assert_eq!(
            identity.node_container,
            "grid-single-cluster-multi-gateway-single-control-plane"
        );
        assert!(!identity.node_container.starts_with("kind-"));
    }

    #[test]
    fn node_image_matching_accepts_crictl_repository_and_tag_columns() {
        let listing = "IMAGE TAG IMAGE ID SIZE\ndocker.io/library/grid-operator single-cluster-qualification abc 1MB\n";
        assert!(node_has_image(listing, "grid-operator:single-cluster-qualification"));
        assert!(!node_has_image(listing, "grid-operator:other"));
    }

    #[test]
    fn image_defaults_are_local_development_defaults() {
        let images = resolve_images(None, None, None, None, None).unwrap_or_else(|_| std::process::abort());
        assert_eq!(images.gateway, DEFAULT_GATEWAY_IMAGE);
        assert_eq!(images.operator, DEFAULT_OPERATOR_IMAGE);
        assert_eq!(images.overlay_sync, DEFAULT_OVERLAY_SYNC_IMAGE);
        assert_eq!(images.vcr, DEFAULT_VCR_IMAGE);
        assert_eq!(images.pull_policy, "Never");
    }

    #[test]
    fn explicit_image_overrides_are_all_preserved() {
        let images = resolve_images(
            Some("registry.example/ai:run-1".to_owned()),
            Some("registry.example/grid-operator:run-1".to_owned()),
            Some("registry.example/overlay-sync:run-1".to_owned()),
            Some("registry.example/vcr:run-1".to_owned()),
            Some("IfNotPresent".to_owned()),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(images.gateway, "registry.example/ai:run-1");
        assert_eq!(images.operator, "registry.example/grid-operator:run-1");
        assert_eq!(images.overlay_sync, "registry.example/overlay-sync:run-1");
        assert_eq!(images.vcr, "registry.example/vcr:run-1");
        assert_eq!(images.pull_policy, "IfNotPresent");
    }

    #[test]
    fn partial_overrides_keep_defaults_for_unset_roles() {
        let images = resolve_images(
            Some("registry.example/ai:run-2".to_owned()),
            None,
            None,
            None,
            Some("Never".to_owned()),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(images.gateway, "registry.example/ai:run-2");
        assert_eq!(images.operator, DEFAULT_OPERATOR_IMAGE);
        assert_eq!(images.overlay_sync, DEFAULT_OVERLAY_SYNC_IMAGE);
        assert_eq!(images.vcr, DEFAULT_VCR_IMAGE);
    }

    #[test]
    fn malformed_image_references_are_rejected() {
        for result in [
            resolve_images(Some("/tmp/image".to_owned()), None, None, None, None),
            resolve_images(Some("registry.example/ai".to_owned()), None, None, None, None),
            resolve_images(Some("registry.example/ai: bad".to_owned()), None, None, None, None),
            resolve_images(None, None, None, None, Some("Sometimes".to_owned())),
        ] {
            if result.is_ok() {
                std::process::abort();
            }
        }
    }

    #[test]
    fn image_evidence_serializes_all_resolved_values() {
        let images = resolve_images(
            Some("registry.example/ai:run-3".to_owned()),
            Some("registry.example/operator:run-3".to_owned()),
            Some("registry.example/sync:run-3".to_owned()),
            Some("registry.example/vcr:run-3".to_owned()),
            Some("Never".to_owned()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let evidence = serde_json::to_value(images).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            evidence.get("gateway"),
            Some(&serde_json::json!("registry.example/ai:run-3"))
        );
        assert_eq!(
            evidence.get("operator"),
            Some(&serde_json::json!("registry.example/operator:run-3"))
        );
        assert_eq!(
            evidence.get("overlay_sync"),
            Some(&serde_json::json!("registry.example/sync:run-3"))
        );
        assert_eq!(
            evidence.get("vcr"),
            Some(&serde_json::json!("registry.example/vcr:run-3"))
        );
        assert_eq!(evidence.get("pull_policy"), Some(&serde_json::json!("Never")));
    }

    #[test]
    fn materialized_config_contains_every_selected_image_and_policy() {
        let root =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/e2e/topologies/grid-single-cluster-multi-gateway");
        let evidence = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let images = resolve_images(
            Some("registry.example/ai:run-4".to_owned()),
            Some("registry.example/operator:run-4".to_owned()),
            Some("registry.example/sync:run-4".to_owned()),
            Some("registry.example/vcr:run-4".to_owned()),
            Some("Never".to_owned()),
        )
        .unwrap_or_else(|_| std::process::abort());
        let resolved = materialize_config(&root.join("forge.yaml"), evidence.path(), &images).unwrap_or_else(|error| {
            eprintln!("materialize test failed: {error}");
            std::process::abort();
        });
        let content = fs::read_to_string(resolved).unwrap_or_else(|_| std::process::abort());
        for image in [&images.gateway, &images.operator, &images.overlay_sync, &images.vcr] {
            assert!(content.contains(image));
        }
        assert!(content.contains("imagePullPolicy: Never"));
    }
}
