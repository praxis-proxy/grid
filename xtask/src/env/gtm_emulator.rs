//! Two-edge Praxis GTM emulator verifier.
//!
//! This module deliberately verifies a small foundation: one stable HTTPS
//! client name, two independently managed Praxis edge services, basic active
//! health withdrawal, recovery, and deterministic session-header steering.
//! Route-aware drain and in-flight stream handling are separate hardening.

use std::{
    collections::BTreeMap,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

use crate::env::{StepResult, StepStatus, glb, print_validate_all_table, safe_truncate_str};

/// Stable demo hostname presented to clients.
const PUBLIC_NAME: &str = "api.grid-glb.test";

/// Stable HTTPS port published by the Praxis GTM emulator.
const PUBLIC_PORT: u16 = 8443;

/// CA used to verify the stable local HTTPS name.
const PUBLIC_CA: &str = ".forge/runtime/glb-tls/gtm/ca.crt";

/// Request fixture path relative to the demo root.
const REQUEST_FIXTURE_SUFFIX: &str = "fixtures/requests/shared-model.json";

/// Customer-side fixture credential accepted by the current edge profile.
const CUSTOMER_TOKEN: &str = "test-token";

/// Demo affinity key used only by the GTM edge-selection layer.
const EDGE_SESSION_HEADER: &str = "X-Edge-Session-Id";

/// Demo affinity key used by `intelligent_route` for provider selection.
const PROVIDER_SESSION_HEADER: &str = "X-Session-Id";

/// Kubernetes clusters and Praxis Deployments in the complete path.
const PRAXIS_DEPLOYMENTS: &[(&str, &str)] = &[
    ("gtm-emulator", "gtm-emulator"),
    ("east-edge", "edge-gateway"),
    ("east-provider", "provider-gateway"),
    ("west-edge", "edge-gateway"),
    ("west-provider", "provider-gateway"),
];

/// One response observed through the stable HTTPS name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeSample {
    /// HTTP response status.
    status: u16,
    /// Edge attribution set by the selected Praxis edge.
    pub(crate) edge: String,
    /// Provider gateway attribution set by `provider_route`.
    pub(crate) provider_gateway: String,
    /// Backend/provider identity when emitted; VCR falls back to gateway identity.
    pub(crate) provider: String,
    /// Backend request identifier when emitted by a backend.
    pub(crate) backend_request_id: String,
}

/// Restores an edge Deployment if verification returns early.
struct DeploymentRestore {
    /// Cluster containing the edge.
    cluster: &'static str,
    /// Whether restoration remains necessary.
    armed: bool,
}

impl DeploymentRestore {
    /// Mark the Deployment as explicitly restored.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DeploymentRestore {
    fn drop(&mut self) {
        if self.armed {
            let _result = scale_edge(self.cluster, 1);
        }
    }
}

/// Verify the minimal two-edge Praxis GTM foundation.
///
/// # Errors
///
/// Returns an error when a prerequisite or any proof step fails.
#[expect(
    clippy::too_many_lines,
    reason = "the short sequential proof table is clearer when its seven steps remain together"
)]
pub(crate) fn verify(forge_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let forge_bin = glb::resolve_forge_binary().ok_or("praxis-forge binary not found")?;
    let root = super::demo_root(forge_config);
    let fixture = root.join(REQUEST_FIXTURE_SUFFIX);
    if !fixture.exists() {
        return Err(format!("request fixture not found: {}", fixture.display()).into());
    }
    let mut results = Vec::new();

    record("forge config", &mut results, || {
        validate_forge_config(&forge_bin, forge_config)
    });
    record("Praxis workloads", &mut results, check_praxis_workloads);
    record("edge overlays", &mut results, check_overlay_perspectives);
    record("stable HTTPS", &mut results, || {
        let sample = request_path("gtm-stable-url", &fixture)?;
        Ok(format!(
            "https://{PUBLIC_NAME}:{PUBLIC_PORT} returned HTTP {} via {}",
            sample.status, sample.edge
        ))
    });

    let sessions = find_sessions_for_both_edges(&fixture);
    record("two edge identities", &mut results, || {
        let sessions = sessions.as_ref().map_err(ToString::to_string)?;
        Ok(format!(
            "stable URL reached {} and {}",
            sessions.keys().next().map_or("unknown", String::as_str),
            sessions.keys().nth(1).map_or("unknown", String::as_str)
        ))
    });
    record("session stickiness", &mut results, || {
        let sessions = sessions.as_ref().map_err(ToString::to_string)?;
        check_stickiness(sessions, &fixture)
    });
    record("edge withdrawal and recovery", &mut results, || {
        let sessions = sessions.as_ref().map_err(ToString::to_string)?;
        check_withdrawal_and_recovery(sessions, &fixture)
    });

    eprintln!();
    eprintln!("GTM EDGE WITHDRAWAL AND RECOVERY PROOF");
    eprintln!("-------------------------------------------------------------------------------");
    eprintln!(
        "One verified HTTPS name must use both active Praxis edges, preserve healthy\n\
         affinity, withdraw a failed edge, and admit it after recovery."
    );
    eprintln!();
    print_validate_all_table(&results);

    if results.iter().any(|result| result.status != StepStatus::Pass) {
        return Err("two-edge Praxis GTM proof incomplete".into());
    }
    Ok(())
}

/// Record one verifier step without hiding later independent failures.
fn record<F>(label: &'static str, results: &mut Vec<StepResult>, check: F)
where
    F: FnOnce() -> Result<String, Box<dyn std::error::Error>>,
{
    match check() {
        Ok(evidence) => results.push(StepResult::pass(label, evidence)),
        Err(error) => results.push(StepResult::fail(label, error.as_ref())),
    }
}

/// Validate the Forge document using the same binary that manages services.
fn validate_forge_config(forge_bin: &str, config: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new(forge_bin)
        .args(["config", "validate", "--config", &config.display().to_string()])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "Forge config invalid: {}",
            safe_truncate_str(&String::from_utf8_lossy(&output.stderr), 160)
        )
        .into());
    }
    Ok("configuration valid".to_owned())
}

/// Require one ready Praxis Deployment in every demo cluster.
fn check_praxis_workloads() -> Result<String, Box<dyn std::error::Error>> {
    for (cluster, deployment) in PRAXIS_DEPLOYMENTS {
        let ready = kubectl_jsonpath(cluster, &format!("deployment/{deployment}"), "{.status.readyReplicas}")?;
        if ready.trim() != "1" {
            return Err(format!("{cluster} deployment/{deployment} readyReplicas={ready:?}").into());
        }
    }
    Ok("one ready Praxis workload in each of five clusters".to_owned())
}

/// Verify that each edge consumes an operator-rendered local perspective.
fn check_overlay_perspectives() -> Result<String, Box<dyn std::error::Error>> {
    let east = overlay_local_site("east-edge")?;
    let west = overlay_local_site("west-edge")?;
    if east != "east-edge" || west != "west-edge" {
        return Err(format!("overlay local_site mismatch: east={east:?}, west={west:?}").into());
    }
    Ok(format!("east={east}, west={west}"))
}

/// Read the local-site identity and require at least one candidate.
fn overlay_local_site(cluster: &str) -> Result<String, Box<dyn std::error::Error>> {
    let raw = kubectl_jsonpath(
        cluster,
        "configmap/grid-overlay-glb-demo-edge-gateway",
        r"{.data.routing-config\.json}",
    )?;
    let document: serde_json::Value = serde_json::from_str(&raw)?;
    let candidates = document
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates")?;
    if candidates.is_empty() {
        return Err(format!("{cluster} overlay has no candidates").into());
    }
    document
        .get("local_site")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("{cluster} overlay missing local_site").into())
}

/// Find two bounded session IDs that consistently hash to different edges.
fn find_sessions_for_both_edges(fixture: &Path) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let mut sessions = BTreeMap::new();
    for index in 0..64 {
        let session = format!("gtm-edge-discovery-{index}");
        let sample = request_path(&session, fixture)?;
        sessions.entry(sample.edge).or_insert(session);
        if sessions.len() == 2 {
            return Ok(sessions);
        }
    }
    Err(format!("64 session keys reached only {} edge(s)", sessions.len()).into())
}

/// Prove repeated requests with one session ID stay on the same Praxis edge.
fn check_stickiness(sessions: &BTreeMap<String, String>, fixture: &Path) -> Result<String, Box<dyn std::error::Error>> {
    for (expected_edge, session) in sessions {
        for _attempt in 0..3 {
            let sample = request_path(session, fixture)?;
            if sample.edge != *expected_edge {
                return Err(format!("session {session:?} moved from {expected_edge} to {}", sample.edge).into());
            }
        }
    }
    Ok("two session keys remained on their original edges".to_owned())
}

/// Stop one edge, wait for Praxis health withdrawal, then restore and recover.
fn check_withdrawal_and_recovery(
    sessions: &BTreeMap<String, String>,
    fixture: &Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let stopped_edge = "east-edge";
    let session = sessions
        .get(stopped_edge)
        .ok_or("no session key mapped to the east edge")?;

    eprintln!("[WITHDRAWAL 1/3] Stopping east-edge and waiting for its session to move to west-edge.");
    scale_edge(stopped_edge, 0)?;
    let mut restore = DeploymentRestore {
        cluster: stopped_edge,
        armed: true,
    };

    wait_for_edge(session, "west-edge", Duration::from_secs(30), fixture)?;
    eprintln!("[WITHDRAWAL 2/3] West-edge is serving the session; restoring east-edge.");
    scale_edge(stopped_edge, 1)?;
    wait_for_deployment(stopped_edge, "edge-gateway", Duration::from_secs(60))?;
    restore.disarm();
    eprintln!("[WITHDRAWAL 3/3] East-edge is ready; waiting for its original session path to recover.");
    wait_for_edge(session, stopped_edge, Duration::from_secs(30), fixture)?;

    Ok("east withdrawal routed to west; east recovered under the same URL and session key".to_owned())
}

/// Scale one Kubernetes edge Deployment.
fn scale_edge(cluster: &str, replicas: u8) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            &format!("kind-grid-glb-{cluster}"),
            "-n",
            "grid-system",
            "scale",
            "deployment/edge-gateway",
            &format!("--replicas={replicas}"),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to scale {cluster} edge to {replicas}: {}",
            safe_truncate_str(&String::from_utf8_lossy(&output.stderr), 160)
        )
        .into());
    }
    Ok(())
}

/// Wait until a Deployment reports one ready replica.
fn wait_for_deployment(cluster: &str, deployment: &str, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            &format!("kind-grid-glb-{cluster}"),
            "-n",
            "grid-system",
            "rollout",
            "status",
            &format!("deployment/{deployment}"),
            &format!("--timeout={}s", timeout.as_secs()),
        ])
        .output()?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "{cluster} deployment/{deployment} did not become ready: {}",
        safe_truncate_str(&String::from_utf8_lossy(&output.stderr), 160)
    )
    .into())
}

/// Read one Kubernetes field from the demo namespace.
fn kubectl_jsonpath(cluster: &str, resource: &str, jsonpath: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            &format!("kind-grid-glb-{cluster}"),
            "-n",
            "grid-system",
            "get",
            resource,
            "-o",
            &format!("jsonpath={jsonpath}"),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to read {cluster} {resource}: {}",
            safe_truncate_str(&String::from_utf8_lossy(&output.stderr), 160)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

/// Poll through transient 502/503 responses until the expected edge serves.
fn wait_for_edge(
    session: &str,
    expected_edge: &str,
    timeout: Duration,
    fixture: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if request_path(session, fixture).is_ok_and(|sample| sample.edge == expected_edge) {
            return Ok(());
        }
        std::thread::park_timeout(Duration::from_millis(250));
    }
    Err(format!("session {session:?} did not converge to {expected_edge} within {timeout:?}").into())
}

/// Resolve the GTM emulator's `LoadBalancer` IP.
pub(crate) fn resolve_gtm_ip() -> Result<String, Box<dyn std::error::Error>> {
    let ip = kubectl_jsonpath(
        "gtm-emulator",
        "service/gtm-emulator",
        "{.status.loadBalancer.ingress[0].ip}",
    )?;
    let trimmed = ip.trim().to_owned();
    if trimmed.is_empty() {
        return Err("GTM emulator service has no LoadBalancer IP".into());
    }
    Ok(trimmed)
}

/// Send one request through the stable verified HTTPS endpoint.
pub(crate) fn request_path(session: &str, fixture: &Path) -> Result<EdgeSample, Box<dyn std::error::Error>> {
    request_path_with_affinity(session, session, fixture)
}

/// Send one request with independent edge and provider affinity fixtures.
#[expect(
    clippy::too_many_lines,
    reason = "keeping the complete curl trust and request contract together makes security review easier"
)]
pub(crate) fn request_path_with_affinity(
    edge_session: &str,
    provider_session: &str,
    fixture: &Path,
) -> Result<EdgeSample, Box<dyn std::error::Error>> {
    let gtm_ip = kubectl_jsonpath(
        "gtm-emulator",
        "service/gtm-emulator",
        "{.status.loadBalancer.ingress[0].ip}",
    )?;
    let resolve = format!("{PUBLIC_NAME}:{PUBLIC_PORT}:{}", gtm_ip.trim());
    let url = format!("https://{PUBLIC_NAME}:{PUBLIC_PORT}/v1/chat/completions");
    let output = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--max-time",
            "10",
            "--cacert",
            PUBLIC_CA,
            "--noproxy",
            PUBLIC_NAME,
            "--resolve",
            &resolve,
            "--dump-header",
            "-",
            "--header",
            "Content-Type: application/json",
            "--header",
            &format!("Authorization: Bearer {CUSTOMER_TOKEN}"),
            "--header",
            &format!("{EDGE_SESSION_HEADER}: {edge_session}"),
            "--header",
            &format!("{PROVIDER_SESSION_HEADER}: {provider_session}"),
            "--data-binary",
            &format!("@{}", fixture.display()),
            &url,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "stable HTTPS request failed: {}",
            safe_truncate_str(&String::from_utf8_lossy(&output.stderr), 160)
        )
        .into());
    }
    parse_response(&String::from_utf8(output.stdout)?)
}

/// Parse the status and bounded edge attribution from a curl response.
fn parse_response(response: &str) -> Result<EdgeSample, Box<dyn std::error::Error>> {
    let (headers, _body) = response
        .split_once("\r\n\r\n")
        .or_else(|| response.split_once("\n\n"))
        .ok_or("response contained no header terminator")?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or("response contained no HTTP status")?;
    if status != 200 {
        return Err(format!("stable HTTPS request returned HTTP {status}").into());
    }
    let edge = parse_header(headers, "x-grid-demo-edge-gateway")
        .filter(|value| matches!(*value, "east-edge" | "west-edge"))
        .ok_or("response missing valid edge attribution")?;
    let (provider_gateway, provider, backend_request_id) = parse_provider_attribution(headers)?;
    Ok(EdgeSample {
        status,
        edge: edge.to_owned(),
        provider_gateway,
        provider,
        backend_request_id,
    })
}

/// Validate the provider gateway, backend site, and final-hop request evidence.
fn parse_provider_attribution(headers: &str) -> Result<(String, String, String), Box<dyn std::error::Error>> {
    let provider_gateway = parse_header(headers, "x-ai-demo-provider-gateway")
        .filter(|value| matches!(*value, "east-provider" | "west-provider"))
        .ok_or("response missing valid provider-gateway attribution")?;
    // The old mock backend emitted a second identity header and a request ID.
    // vllm-vcr intentionally exposes the normal OpenAI-compatible response
    // surface, so use the authenticated provider gateway as the observable
    // final-hop identity when those mock-only headers are absent.
    let provider = parse_header(headers, "x-grid-demo-provider")
        .filter(|value| matches!(*value, "east-provider" | "east-provider-secondary" | "west-provider"))
        .unwrap_or(provider_gateway);
    let expected_gateway = match provider {
        "east-provider" | "east-provider-secondary" => "east-provider",
        "west-provider" => "west-provider",
        _ => return Err(format!("unknown backend provider attribution {provider}").into()),
    };
    if provider_gateway != expected_gateway {
        return Err(format!(
            "provider attribution mismatch: gateway={provider_gateway}, backend={provider}, expected gateway={expected_gateway}"
        )
        .into());
    }
    let backend_request_id = parse_header(headers, "x-grid-demo-backend-request-id")
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .unwrap_or("not-exposed-by-vcr");
    Ok((
        provider_gateway.to_owned(),
        provider.to_owned(),
        backend_request_id.to_owned(),
    ))
}

/// Find one case-insensitive HTTP response header.
fn parse_header<'resp>(headers: &'resp str, name: &str) -> Option<&'resp str> {
    headers.lines().find_map(|line| {
        let (candidate, value) = line.split_once(':')?;
        candidate.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// Repository root from the xtask crate directory.
    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
    }

    #[test]
    fn parse_response_extracts_edge() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "X-Grid-Demo-Edge-Gateway: west-edge\r\n",
            "X-AI-Demo-Provider-Gateway: east-provider\r\n",
            "X-Grid-Demo-Provider: east-provider\r\n",
            "X-Grid-Demo-Backend-Request-Id: backend-123\r\n",
            "Content-Type: application/json\r\n\r\n{}"
        );
        let sample = parse_response(response).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            sample,
            EdgeSample {
                status: 200,
                edge: "west-edge".to_owned(),
                provider_gateway: "east-provider".to_owned(),
                provider: "east-provider".to_owned(),
                backend_request_id: "backend-123".to_owned(),
            }
        );
    }

    #[test]
    fn parse_response_accepts_second_provider_behind_east_gateway() {
        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "X-Grid-Demo-Edge-Gateway: west-edge\r\n",
            "X-AI-Demo-Provider-Gateway: east-provider\r\n",
            "X-Grid-Demo-Provider: east-provider-secondary\r\n",
            "X-Grid-Demo-Backend-Request-Id: backend-secondary\r\n\r\n{}"
        );
        let sample = parse_response(response).unwrap_or_else(|_| std::process::abort());
        assert_eq!(sample.provider_gateway, "east-provider");
        assert_eq!(sample.provider, "east-provider-secondary");
    }

    #[test]
    fn parse_response_rejects_unknown_edge() {
        let response = "HTTP/1.1 200 OK\r\nX-Grid-Demo-Edge-Gateway: client-value\r\n\r\n{}";
        assert!(parse_response(response).is_err(), "unknown edge must fail");
    }

    #[test]
    fn five_clusters_have_one_named_praxis_deployment() {
        assert_eq!(PRAXIS_DEPLOYMENTS.len(), 5);
        assert!(PRAXIS_DEPLOYMENTS.contains(&("gtm-emulator", "gtm-emulator")));
        assert!(PRAXIS_DEPLOYMENTS.contains(&("east-edge", "edge-gateway")));
        assert!(PRAXIS_DEPLOYMENTS.contains(&("west-provider", "provider-gateway")));
    }

    #[test]
    fn gtm_is_praxis_edge_steering_only() {
        let config = std::fs::read_to_string(
            workspace_root().join("tests/e2e/topologies/grid-glb-demo/configs/gtm-emulator/praxis.yaml"),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert!(config.contains("filter: router"));
        assert!(config.contains("filter: load_balancer"));
        assert!(config.contains("header: X-Edge-Session-Id"));
        assert!(config.contains("type: tcp"));
        for forbidden in [
            "intelligent_route",
            "provider_route",
            "credential_inject",
            "peer_identity_trust",
        ] {
            assert!(!config.contains(forbidden), "GTM must not own {forbidden}");
        }
    }

    #[test]
    fn gtm_registers_edge_health_at_praxis_top_level() {
        let config = std::fs::read_to_string(
            workspace_root().join("tests/e2e/topologies/grid-glb-demo/configs/gtm-emulator/praxis.yaml"),
        )
        .unwrap_or_else(|_| std::process::abort());
        let document: serde_yaml::Value = serde_yaml::from_str(&config).unwrap_or_else(|_| std::process::abort());
        let clusters = document
            .get("clusters")
            .and_then(serde_yaml::Value::as_sequence)
            .unwrap_or_else(|| std::process::abort());
        let grid_edges = clusters
            .iter()
            .find(|cluster| cluster.get("name").and_then(serde_yaml::Value::as_str) == Some("grid-edges"))
            .unwrap_or_else(|| std::process::abort());
        let health = grid_edges.get("health_check").unwrap_or_else(|| std::process::abort());

        assert_eq!(health.get("type").and_then(serde_yaml::Value::as_str), Some("tcp"));
        assert_eq!(
            health.get("unhealthy_threshold").and_then(serde_yaml::Value::as_u64),
            Some(2)
        );
        assert_eq!(
            health.get("healthy_threshold").and_then(serde_yaml::Value::as_u64),
            Some(2)
        );
        assert_eq!(
            grid_edges
                .get("endpoints")
                .and_then(serde_yaml::Value::as_sequence)
                .map_or(0, Vec::len),
            2
        );
    }

    #[test]
    fn all_praxis_workloads_are_kubernetes_deployments() {
        let forge = std::fs::read_to_string(workspace_root().join("tests/e2e/topologies/grid-glb-demo/forge.yaml"))
            .unwrap_or_else(|_| std::process::abort());
        assert!(!forge.contains("\n  services:"));
        assert!(forge.contains("- name: gtm-emulator"));
        assert!(forge.contains("- name: east-edge"));
        assert!(forge.contains("- name: east-provider"));
        assert!(forge.contains("- name: west-edge"));
        assert!(forge.contains("- name: west-provider"));
    }

    #[test]
    fn edge_perspectives_have_distinct_sources_and_keys() {
        let root = workspace_root().join("tests/e2e/topologies/grid-glb-demo");
        let forge = std::fs::read_to_string(root.join("forge.yaml")).unwrap_or_else(|_| std::process::abort());
        let west_network = std::fs::read_to_string(root.join("resources/gridnetwork-west-edge.yaml"))
            .unwrap_or_else(|_| std::process::abort());
        let edge_deployment = std::fs::read_to_string(root.join("resources/edge-gateway-deployment.yaml"))
            .unwrap_or_else(|_| std::process::abort());

        assert!(!forge.contains("overlay-sync"));
        assert!(west_network.contains("localSiteName: west-edge"));
        assert!(edge_deployment.contains("name: grid-overlay-glb-demo-edge-gateway"));
        assert!(edge_deployment.contains("mountPath: /etc/praxis/routing"));
    }

    #[test]
    fn providers_pin_both_edge_certificate_digests() {
        let root = workspace_root().join("tests/e2e/topologies/grid-glb-demo/configs");
        for site in ["east-provider", "west-provider"] {
            let config = std::fs::read_to_string(root.join(format!("{site}/praxis.yaml")))
                .unwrap_or_else(|_| std::process::abort());
            assert!(config.contains("__EDGE_US_EAST_CERT_SHA256__"));
            assert!(config.contains("__EDGE_US_WEST_CERT_SHA256__"));
            assert_eq!(
                config.matches("cert_digest:").count(),
                2,
                "{site} must trust exactly the two demo edges"
            );
        }
    }
}
