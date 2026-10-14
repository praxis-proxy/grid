//! In-cluster workload request client for the workload-inference demo.
//!
//! Creates temporary Kubernetes `Job` resources that send HTTP requests from
//! inside consumer clusters, proving that the inference path originates within
//! the cluster rather than from an external client.

use std::{
    process::Command,
    time::{Duration, Instant},
};

use super::kubectl;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Kubernetes namespace for workload `Job` resources.
const GRID_SYSTEM_NS: &str = "grid-system";

/// Pinned curl image for reproducible in-cluster requests.
const CURL_IMAGE: &str = "curlimages/curl:8.12.1";

/// `ConfigMap` name for the request body fixture.
const REQUEST_BODY_CM: &str = "workload-request-body";

/// `Job` name prefix for workload requests.
const JOB_NAME_PREFIX: &str = "workload-request";

/// Cluster name prefix from the GLB demo config.
const CLUSTER_PREFIX: &str = "grid-glb";

/// Maximum time to wait for a `Job` to complete.
const JOB_TIMEOUT: Duration = Duration::from_secs(60);

/// Consumer gateway `Service` endpoint inside the cluster.
const GATEWAY_SERVICE: &str = "edge-gateway.grid-system.svc.cluster.local:8080";

// ---------------------------------------------------------------------------
// Response type
// ---------------------------------------------------------------------------

/// Parsed response from an in-cluster workload request.
#[derive(Debug)]
pub(crate) struct WorkloadResponse {
    /// HTTP status code from the response.
    pub(crate) status: u16,
    /// Raw response body.
    pub(crate) body: String,
    /// Provider gateway identity from Praxis provider-route attribution.
    pub(crate) provider: String,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Send an inference request from inside a consumer cluster via a temporary `Job`.
///
/// Creates a `ConfigMap` with the request body, runs a curl `Job`, waits for
/// completion, reads the response from pod logs, then cleans up.
///
/// # Errors
///
/// Returns an error if any kubectl operation fails or the `Job` does not
/// complete within the timeout.
pub(crate) fn send_workload_request(
    cluster: &str,
    request_body: &str,
    session_id: Option<&str>,
) -> Result<WorkloadResponse, Box<dyn std::error::Error>> {
    let context = kubectl_context(cluster);
    let job_name = format!(
        "{JOB_NAME_PREFIX}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() % 100_000)
    );

    let result = run_workload_job(&context, &job_name, request_body, session_id);
    cleanup(&context, &job_name);
    result
}

/// Execute the workload Job and return the parsed response.
///
/// Separated from [`send_workload_request`] so that `cleanup` runs
/// regardless of which step fails.
fn run_workload_job(
    context: &str,
    job_name: &str,
    request_body: &str,
    session_id: Option<&str>,
) -> Result<WorkloadResponse, Box<dyn std::error::Error>> {
    create_request_body_configmap(context, request_body)?;
    create_curl_job(context, job_name, session_id)?;
    wait_for_job(context, job_name)?;
    let output = read_job_logs(context, job_name)?;
    Ok(parse_curl_output(&output))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Create or replace the request body `ConfigMap`.
fn create_request_body_configmap(context: &str, body: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": REQUEST_BODY_CM,
            "namespace": GRID_SYSTEM_NS
        },
        "data": {
            "request.json": body
        }
    })
    .to_string();
    kubectl::apply_manifest(context, &manifest)
}

/// Build the curl `Job` manifest.
///
/// Curl writes `STATUS:<code>` as a trailer after the body so the status
/// can be parsed independently of content type (JSON error bodies are
/// common for 4xx/5xx).
#[expect(clippy::too_many_lines, reason = "JSON manifest literal with security context")]
fn build_curl_job_manifest(job_name: &str, session_id: Option<&str>) -> String {
    let url = format!("http://{GATEWAY_SERVICE}/v1/chat/completions");

    let mut curl_args = vec![
        "--silent".to_owned(),
        "--show-error".to_owned(),
        "--max-time".to_owned(),
        "15".to_owned(),
        "--dump-header".to_owned(),
        "-".to_owned(),
        "--write-out".to_owned(),
        "\nSTATUS:%{http_code}\nPROVIDER:%header{x-ai-demo-provider-gateway}".to_owned(),
        "--header".to_owned(),
        "Content-Type: application/json".to_owned(),
    ];

    if let Some(session) = session_id {
        curl_args.push("--header".to_owned());
        curl_args.push(format!("X-Session-Id: {session}"));
    }

    curl_args.push("--data-binary".to_owned());
    curl_args.push("@/request/request.json".to_owned());
    curl_args.push(url);

    serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": GRID_SYSTEM_NS
        },
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 300,
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": false,
                    "containers": [{
                        "name": "curl",
                        "image": CURL_IMAGE,
                        "command": ["curl"],
                        "args": curl_args,
                        "volumeMounts": [{
                            "name": "request-body",
                            "mountPath": "/request",
                            "readOnly": true
                        }],
                        "securityContext": {
                            "runAsNonRoot": true,
                            "runAsUser": 100,
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": {
                                "drop": ["ALL"]
                            }
                        }
                    }],
                    "volumes": [{
                        "name": "request-body",
                        "configMap": {
                            "name": REQUEST_BODY_CM
                        }
                    }]
                }
            }
        }
    })
    .to_string()
}

/// Create a curl `Job` that sends the request and dumps headers to stderr.
fn create_curl_job(context: &str, job_name: &str, session_id: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = build_curl_job_manifest(job_name, session_id);
    kubectl::apply_manifest(context, &manifest)
}

/// Wait for a `Job` to complete or fail.
fn wait_for_job(context: &str, job_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + JOB_TIMEOUT;
    let condition = format!("job/{job_name}");
    loop {
        let output = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "wait",
                "--for=condition=complete",
                "--timeout=10s",
                &condition,
            ])
            .output()?;
        if output.status.success() {
            return Ok(());
        }
        if is_job_failed(context, &condition)? {
            return Err(format!("workload request Job {job_name} failed").into());
        }
        if Instant::now() >= deadline {
            return Err(format!("timeout waiting for Job {job_name}").into());
        }
    }
}

/// Check whether a `Job` has recorded a failure.
fn is_job_failed(context: &str, condition: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let failed = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            condition,
            "-o",
            "jsonpath={.status.failed}",
        ])
        .output()?;
    let count = String::from_utf8_lossy(&failed.stdout);
    Ok(count.trim() == "1")
}

/// Read the combined stdout from `Job` pods.
fn read_job_logs(context: &str, job_name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let selector = format!("job-name={job_name}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "logs",
            "-l",
            &selector,
            "--tail",
            "-1",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl logs for Job {job_name} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Clean up the `Job` and `ConfigMap`.
fn cleanup(context: &str, job_name: &str) {
    for (resource, name) in [("job", job_name), ("configmap", REQUEST_BODY_CM)] {
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "delete",
                    resource,
                    name,
                    "--ignore-not-found",
                ])
                .output(),
        );
    }
}

/// Parse curl output using the `STATUS:<code>` and `PROVIDER:<name>` trailers
/// written by `--write-out`.
fn parse_curl_output(output: &str) -> WorkloadResponse {
    let trimmed = output.trim();
    let mut status = 0_u16;
    let mut provider = String::new();
    let mut body_end = trimmed.len();

    if let Some(idx) = trimmed.rfind("\nSTATUS:") {
        body_end = idx;
        for line in trimmed.get(idx + 1..).unwrap_or("").lines() {
            if let Some(rest) = line.strip_prefix("STATUS:") {
                status = rest.trim().parse::<u16>().unwrap_or(0);
            } else if let Some(rest) = line.strip_prefix("PROVIDER:") {
                rest.trim().clone_into(&mut provider);
            }
        }
    } else if let Some(rest) = trimmed.strip_prefix("STATUS:") {
        let first_line = rest.lines().next().unwrap_or("");
        status = first_line.trim().parse::<u16>().unwrap_or(0);
        body_end = 0;
    }

    let response = trimmed.get(..body_end).unwrap_or("");
    let (headers, body) = split_response_headers(response);
    if provider.is_empty() {
        provider = header_value(headers, "x-ai-demo-provider-gateway").unwrap_or_default();
    }
    let body = body.trim().to_owned();
    WorkloadResponse { status, body, provider }
}

/// Split curl's dumped HTTP headers from the response body.
fn split_response_headers(response: &str) -> (&str, &str) {
    response.find("\r\n\r\n").map_or_else(
        || {
            response
                .find("\n\n")
                .map_or(("", response), |end| response.split_at(end + 2))
        },
        |end| response.split_at(end + 4),
    )
}

/// Read one case-insensitive response header from a dumped header block.
fn header_value(headers: &str, expected: &str) -> Option<String> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.trim()
            .eq_ignore_ascii_case(expected)
            .then(|| value.trim().to_owned())
    })
}

/// Build the kind cluster context name.
fn kubectl_context(cluster_name: &str) -> String {
    format!("kind-{CLUSTER_PREFIX}-{cluster_name}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[expect(clippy::allow_attributes, reason = "blanket test lint suppression")]
    #[allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        reason = "tests"
    )]
    mod inner {
        use super::*;

        #[test]
        fn kubectl_context_format() {
            assert_eq!(kubectl_context("east-edge"), "kind-grid-glb-east-edge");
        }

        #[test]
        fn parse_status_trailer_extracts_code_and_body() {
            let output = "HTTP/1.1 200 OK\r\nX-AI-Demo-Provider-Gateway: east-provider\r\n\r\n{\"choices\":[]}\nSTATUS:200\nPROVIDER:east-provider";
            let response = parse_curl_output(output);
            assert_eq!(response.status, 200);
            assert_eq!(response.body, r#"{"choices":[]}"#);
            assert_eq!(response.provider, "east-provider");
        }

        #[test]
        fn raw_header_supplies_attribution_when_curl_trailer_is_empty() {
            let output =
                "HTTP/1.1 200 OK\r\nx-ai-demo-provider-gateway: west-provider\r\n\r\n{}\nSTATUS:200\nPROVIDER:";
            let response = parse_curl_output(output);
            assert_eq!(response.status, 200);
            assert_eq!(response.provider, "west-provider");
            assert_eq!(response.body, "{}");
        }

        #[test]
        fn parse_status_trailer_detects_error_status() {
            let output = "{\"error\":\"not found\"}\nSTATUS:404\nPROVIDER:";
            let response = parse_curl_output(output);
            assert_eq!(response.status, 404, "JSON error body must not be assumed 200");
            assert_eq!(response.body, r#"{"error":"not found"}"#);
        }

        #[test]
        fn parse_missing_trailer_returns_zero_status() {
            let response = parse_curl_output(r#"{"choices":[]}"#);
            assert_eq!(response.status, 0, "missing trailer must not assume 200");
            assert!(response.provider.is_empty(), "missing trailer must have empty provider");
        }

        #[test]
        fn curl_image_is_pinned() {
            assert!(
                CURL_IMAGE.contains(':'),
                "curl image must be pinned to a specific version"
            );
            assert!(!CURL_IMAGE.ends_with(":latest"), "curl image must not use :latest");
        }

        #[test]
        fn job_manifest_contains_security_context() {
            let manifest = build_curl_job_manifest("test-job", None);
            assert!(manifest.contains("runAsNonRoot"));
            assert!(manifest.contains("allowPrivilegeEscalation"));
            assert!(manifest.contains("readOnlyRootFilesystem"));
            assert!(manifest.contains("ALL"));
        }

        #[test]
        fn job_manifest_includes_session_header() {
            let manifest = build_curl_job_manifest("test-job", Some("session-123"));
            assert!(manifest.contains("X-Session-Id: session-123"));
        }

        #[test]
        fn job_manifest_omits_session_header_when_none() {
            let manifest = build_curl_job_manifest("test-job", None);
            assert!(!manifest.contains("X-Session-Id"));
        }
    }
}
