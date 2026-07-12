//! Provider inference baseline verification.
//!
//! Verifies that each provider cluster's inference-sim deployments
//! are reachable and serve the correct models via Chat Completions.
//! Uses `kubectl port-forward` for host access and `curl` for HTTP.

use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::Duration,
};

use crate::env::{
    config::{ClusterDef, ClusterRole, EnvConfig},
    kind,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum attempts to connect after starting port-forward.
const PORT_FORWARD_RETRIES: u32 = 15;

/// Delay between port-forward readiness retries.
const PORT_FORWARD_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Initial settle time before first port-forward probe.
const PORT_FORWARD_SETTLE: Duration = Duration::from_secs(5);

/// Kubernetes namespace for inference-sim.
const NAMESPACE: &str = "default";

/// Inference-sim service port inside the cluster.
const SERVICE_PORT: u16 = 8000;

/// `curl` connect timeout in seconds.
const CURL_CONNECT_TIMEOUT: u32 = 5;

/// `curl` maximum request time in seconds.
const CURL_MAX_TIME: u32 = 15;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Verify all provider clusters can serve Chat Completions.
///
/// For each provider model: checks deployment readiness, opens a
/// port-forward, queries `/v1/models`, sends a Chat Completions
/// request, and validates the response shape.
///
/// # Errors
///
/// Returns an error if no providers exist or any assertion fails.
pub(crate) fn verify_providers(cfg: &EnvConfig) -> Result<(), Box<dyn std::error::Error>> {
    let mut tally = Tally::default();
    let mut providers_found = false;

    for name in &cfg.clusters.names {
        let Some(def) = cfg.clusters.definitions.get(name) else {
            continue;
        };
        if def.role != ClusterRole::Provider || def.models.is_empty() {
            continue;
        }
        providers_found = true;
        verify_one_provider(name, def, &mut tally)?;
    }

    if !providers_found {
        return Err("no provider clusters with models found".into());
    }

    tally.print_summary()
}

// ---------------------------------------------------------------------------
// Tally
// ---------------------------------------------------------------------------

/// Assertion pass/fail counter.
#[derive(Default)]
pub(crate) struct Tally {
    /// Number of passing assertions.
    pass: u32,
    /// Number of failing assertions.
    fail: u32,
}

impl Tally {
    /// Record a passing assertion.
    pub(crate) fn pass(&mut self, cluster: &str, message: &str) {
        eprintln!("  [PASS] {cluster} {message}");
        self.pass += 1;
    }

    /// Record a failing assertion with context.
    pub(crate) fn fail(&mut self, cluster: &str, message: &str, context: &str) {
        eprintln!("  [FAIL] {cluster} {message}");
        eprintln!("         context: {context}");
        eprintln!("         namespace: {NAMESPACE}");
        self.fail += 1;
    }

    /// Print summary and return error if any assertions failed.
    pub(crate) fn print_summary(self) -> Result<(), Box<dyn std::error::Error>> {
        eprintln!();
        if self.fail == 0 {
            eprintln!("RESULT: PASS provider inference baseline ({} assertions)", self.pass);
            Ok(())
        } else {
            eprintln!("RESULT: FAIL ({} passed, {} failed)", self.pass, self.fail);
            Err("provider verification failed".into())
        }
    }
}

// ---------------------------------------------------------------------------
// Per-provider verification
// ---------------------------------------------------------------------------

/// Verify one provider cluster by checking each configured model.
fn verify_one_provider(name: &str, def: &ClusterDef, tally: &mut Tally) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = kind::kubectl_context(name);

    for model in &def.models {
        verify_one_model(name, &ctx, model, tally)?;
    }
    Ok(())
}

/// Verify one model's deployment, service, and Chat Completions.
fn verify_one_model(name: &str, ctx: &str, model: &str, tally: &mut Tally) -> Result<(), Box<dyn std::error::Error>> {
    let svc = kind::service_name(model);

    if !kind::is_model_deployment_ready(name, model) {
        tally.fail(name, &format!("{svc} deployment available"), ctx);
        return Ok(());
    }
    tally.pass(name, &format!("{svc} deployment available"));

    let local_port = find_free_port()?;
    let mut pf = PortForwardGuard::start(ctx, &svc, local_port, SERVICE_PORT)?;

    if !wait_for_port(local_port) {
        tally.fail(name, &format!("{svc} reachable via port-forward"), ctx);
        pf.stop();
        return Ok(());
    }
    tally.pass(name, &format!("{svc} reachable via port-forward"));

    check_model_listed(name, ctx, model, local_port, tally);
    check_chat_completions(name, ctx, model, local_port, tally);

    pf.stop();
    Ok(())
}

/// Verify the model appears in `/v1/models`.
fn check_model_listed(name: &str, ctx: &str, model: &str, port: u16, tally: &mut Tally) {
    match query_models(port) {
        Ok(models) if models.iter().any(|m| m == model) => {
            tally.pass(name, &format!("model {model} listed by /v1/models"));
        },
        Ok(models) => {
            let listed = models.join(", ");
            tally.fail(name, &format!("model {model} not in /v1/models (found: {listed})"), ctx);
        },
        Err(e) => {
            tally.fail(name, &format!("/v1/models query failed: {e}"), ctx);
        },
    }
}

/// Send a Chat Completions request and validate the response.
fn check_chat_completions(name: &str, ctx: &str, model: &str, port: u16, tally: &mut Tally) {
    let resp = match send_chat_request(port, model) {
        Ok(r) => r,
        Err(e) => {
            tally.fail(name, &format!("chat {model} request failed: {e}"), ctx);
            return;
        },
    };

    if resp.status != 200 {
        let excerpt = safe_truncate(&resp.body, 200);
        tally.fail(
            name,
            &format!(
                "chat {model} returned {} (expected 200)\n         body: {excerpt}",
                resp.status
            ),
            ctx,
        );
        return;
    }
    tally.pass(name, &format!("chat completions {model} returned 200"));

    check_body_shape(name, ctx, model, &resp.body, tally);
}

/// Validate that the response body is Chat Completions-shaped JSON.
fn check_body_shape(name: &str, ctx: &str, model: &str, body: &str, tally: &mut Tally) {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(json) if is_chat_completions_shaped(&json) => {
            tally.pass(name, &format!("chat {model} response is valid Chat Completions JSON"));
        },
        Ok(_) => {
            let excerpt = safe_truncate(body, 200);
            tally.fail(
                name,
                &format!("chat {model} missing expected fields\n         body: {excerpt}"),
                ctx,
            );
        },
        Err(e) => {
            tally.fail(name, &format!("chat {model} not valid JSON: {e}"), ctx);
        },
    }
}

// ---------------------------------------------------------------------------
// Port-forward guard
// ---------------------------------------------------------------------------

/// RAII guard that kills the `kubectl port-forward` child on drop.
pub(crate) struct PortForwardGuard {
    /// The port-forward child process.
    child: Option<Child>,
}

impl PortForwardGuard {
    /// Start a `kubectl port-forward` to a named service.
    pub(crate) fn start(
        context: &str,
        svc_name: &str,
        local_port: u16,
        svc_port: u16,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (resource, mapping) = port_forward_args(svc_name, local_port, svc_port);
        let child = Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                NAMESPACE,
                "port-forward",
                &resource,
                &mapping,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self { child: Some(child) })
    }

    /// Kill the port-forward process.
    pub(crate) fn stop(&mut self) {
        if let Some(ref mut child) = self.child {
            let _kill = child.kill();
            let _wait = child.wait();
        }
        self.child = None;
    }
}

impl Drop for PortForwardGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Build the `kubectl port-forward` resource and mapping arguments.
///
/// Returns `("svc/{svc_name}", "{local_port}:8000")`.
fn port_forward_args(svc_name: &str, local_port: u16, svc_port: u16) -> (String, String) {
    let resource = format!("svc/{svc_name}");
    let mapping = format!("{local_port}:{svc_port}");
    (resource, mapping)
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

/// Response from an HTTP request.
pub(crate) struct HttpResponse {
    /// HTTP status code.
    pub(crate) status: u16,
    /// Response body.
    pub(crate) body: String,
}

/// Query `/v1/models` and return the list of model IDs.
fn query_models(port: u16) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let url = format!("http://127.0.0.1:{port}/v1/models");
    let resp = curl_get(&url)?;
    if resp.status != 200 {
        return Err(format!("/v1/models returned {}", resp.status).into());
    }
    parse_model_ids(&resp.body)
}

/// Send a Chat Completions request and return status + body.
fn send_chat_request(port: u16, model: &str) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let url = format!("http://127.0.0.1:{port}/v1/chat/completions");
    let body = format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"hello"}}],"max_tokens":1}}"#);
    curl_post(&url, &body)
}

/// HTTP GET via `curl`.
fn curl_get(url: &str) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let connect_timeout = CURL_CONNECT_TIMEOUT.to_string();
    let max_time = CURL_MAX_TIME.to_string();
    let output = Command::new("curl")
        .args([
            "-s",
            "-w",
            "\n%{http_code}",
            "--connect-timeout",
            &connect_timeout,
            "--max-time",
            &max_time,
            url,
        ])
        .output()?;
    parse_curl_output(&String::from_utf8(output.stdout)?)
}

/// HTTP POST via `curl`.
fn curl_post(url: &str, body: &str) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let connect_timeout = CURL_CONNECT_TIMEOUT.to_string();
    let max_time = CURL_MAX_TIME.to_string();
    let output = Command::new("curl")
        .args([
            "-s",
            "-w",
            "\n%{http_code}",
            "--connect-timeout",
            &connect_timeout,
            "--max-time",
            &max_time,
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-d",
            body,
            url,
        ])
        .output()?;
    parse_curl_output(&String::from_utf8(output.stdout)?)
}

/// Parse `curl` output where the last line is the HTTP status code.
pub(crate) fn parse_curl_output(raw: &str) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let trimmed = raw.trim_end();
    let (body, code_line) = trimmed.rsplit_once('\n').unwrap_or(("", trimmed));
    let status: u16 = code_line
        .trim()
        .parse()
        .map_err(|e| format!("failed to parse HTTP status: {e}"))?;
    Ok(HttpResponse {
        status,
        body: body.to_owned(),
    })
}

/// Parse model IDs from an OpenAI-compatible `/v1/models` response.
fn parse_model_ids(body: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let json: serde_json::Value = serde_json::from_str(body)?;
    let data = json.get("data").and_then(serde_json::Value::as_array);
    let Some(data) = data else {
        return Err("model list missing 'data' array".into());
    };
    let ids: Vec<String> = data
        .iter()
        .filter_map(|entry| entry.get("id").and_then(serde_json::Value::as_str).map(str::to_owned))
        .collect();
    Ok(ids)
}

/// Check whether a JSON value looks like a Chat Completions response.
///
/// Requires a `choices` array — the standard `OpenAI` success shape.
/// Rejects error-shaped JSON even if it has other fields.
fn is_chat_completions_shaped(json: &serde_json::Value) -> bool {
    json.get("error").is_none() && json.get("choices").is_some_and(serde_json::Value::is_array)
}

// ---------------------------------------------------------------------------
// Networking helpers
// ---------------------------------------------------------------------------

/// Find an available local TCP port.
pub(crate) fn find_free_port() -> Result<u16, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Wait for a local port to accept connections.
#[expect(
    clippy::disallowed_methods,
    reason = "xtask is synchronous; no async runtime available for tokio::time::sleep"
)]
pub(crate) fn wait_for_port(port: u16) -> bool {
    std::thread::sleep(PORT_FORWARD_SETTLE);

    for _ in 0..PORT_FORWARD_RETRIES {
        if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            return true;
        }
        std::thread::sleep(PORT_FORWARD_RETRY_DELAY);
    }
    false
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

/// Truncate a string for diagnostic output (UTF-8 safe).
pub(crate) fn safe_truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.get(..end).unwrap_or(s)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_free_port_returns_nonzero() {
        let port = find_free_port();
        assert!(port.is_ok(), "should find a free port");
        let port = port.unwrap_or_else(|_| std::process::abort());
        assert!(port > 0, "port should be nonzero");
    }

    #[test]
    fn port_forward_args_correct_syntax() {
        let (resource, mapping) = port_forward_args("inference-sim-llama-3-2-8b", 12345, 8000);
        assert_eq!(
            resource, "svc/inference-sim-llama-3-2-8b",
            "resource should be svc/ prefix only"
        );
        assert_eq!(mapping, "12345:8000", "mapping should be local:remote");
    }

    #[test]
    fn parse_curl_output_extracts_status_and_body() {
        let raw = "{\"choices\":[]}\n200";
        let resp = parse_curl_output(raw).unwrap_or_else(|_| std::process::abort());
        assert_eq!(resp.status, 200, "should parse status code");
        assert_eq!(resp.body, "{\"choices\":[]}", "should extract body");
    }

    #[test]
    fn parse_curl_output_no_body() {
        let raw = "404";
        let resp = parse_curl_output(raw).unwrap_or_else(|_| std::process::abort());
        assert_eq!(resp.status, 404, "should parse status-only output");
        assert!(resp.body.is_empty(), "body should be empty");
    }

    #[test]
    fn parse_model_ids_extracts_ids() {
        let body = r#"{"data":[{"id":"mistral-7b","object":"model"},{"id":"llama-3.2-8b","object":"model"}]}"#;
        let ids = parse_model_ids(body).unwrap_or_else(|_| std::process::abort());
        assert_eq!(ids, vec!["mistral-7b", "llama-3.2-8b"], "should extract model IDs");
    }

    #[test]
    fn parse_model_ids_empty_data() {
        let body = r#"{"data":[]}"#;
        let ids = parse_model_ids(body).unwrap_or_else(|_| std::process::abort());
        assert!(ids.is_empty(), "empty data should produce empty list");
    }

    #[test]
    fn parse_model_ids_missing_data() {
        let body = r#"{"error":"not found"}"#;
        let result = parse_model_ids(body);
        assert!(result.is_err(), "missing data array should fail");
    }

    #[test]
    fn is_chat_completions_shaped_with_choices() {
        let json: serde_json::Value = serde_json::from_str(r#"{"choices":[{"message":{"content":"hi"}}]}"#)
            .unwrap_or_else(|_| std::process::abort());
        assert!(is_chat_completions_shaped(&json), "should recognize choices array");
    }

    #[test]
    fn is_chat_completions_shaped_rejects_error() {
        let json: serde_json::Value = serde_json::from_str(r#"{"error":{"message":"fail"},"choices":[]}"#)
            .unwrap_or_else(|_| std::process::abort());
        assert!(
            !is_chat_completions_shaped(&json),
            "should reject error-shaped responses even with choices"
        );
    }

    #[test]
    fn is_chat_completions_shaped_rejects_no_choices() {
        let json: serde_json::Value =
            serde_json::from_str(r#"{"object":"chat.completion"}"#).unwrap_or_else(|_| std::process::abort());
        assert!(
            !is_chat_completions_shaped(&json),
            "should reject responses without choices array"
        );
    }
}
