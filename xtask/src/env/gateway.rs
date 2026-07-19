//! Provider gateway deployment and verification.
//!
//! Deploys mock EPP and Praxis AI gateway into provider clusters,
//! then verifies the provider backend request path through the gateway.
//!
//! Provider gateways terminate mTLS: they require a client certificate
//! signed by the generated test CA. Once the Praxis image includes
//! `peer_identity_trust`, provider gateways should also validate the captured
//! downstream peer identity before allowing requests into the `ext_proc` path.

use std::{path::PathBuf, process::Command};

use crate::env::{
    config::{ClusterDef, ClusterRole, EnvConfig, ProviderBackend},
    image_overrides, kind, kubectl,
    verify::{HttpResponse, PortForwardGuard, Tally, find_free_port, parse_curl_output, safe_truncate, wait_for_port},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Kubernetes namespace for gateway deployments.
const NAMESPACE: &str = "default";

/// Name of the mock EPP Kubernetes Deployment and Service.
const MOCK_EPP_NAME: &str = "mock-epp";

/// Port the mock EPP gRPC server binds on.
const MOCK_EPP_PORT: u16 = 50051;

/// K8s Secret name holding TLS certs for provider gateway pods.
const TLS_SECRET_NAME: &str = "praxis-tls";

/// Path inside the provider pod where TLS certs are mounted.
const CERT_MOUNT_PATH: &str = "/etc/praxis/tls";

/// Host-side CA cert path (relative to grid repo root).
pub(crate) const HOST_CA_CERT: &str = "tests/env/certs/ca.pem";

/// Host-side cert directory.
const HOST_CERTS_DIR: &str = "tests/env/certs";

/// Name of the Praxis AI provider gateway Deployment.
const GATEWAY_DEPLOYMENT: &str = "praxis-provider";

/// Name of the Praxis AI provider gateway Service.
const GATEWAY_SERVICE: &str = "praxis-provider";

/// Name of the `ConfigMap` holding the provider gateway config.
const GATEWAY_CONFIGMAP: &str = "praxis-provider-config";

/// HTTP port for the provider gateway.
const GATEWAY_HTTP_PORT: u16 = 8080;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Deploy mock EPP and provider gateway into all provider clusters.
///
/// # Errors
///
/// Returns an error if any `kubectl` command fails.
pub(crate) fn deploy_all(cfg: &EnvConfig) -> Result<(), Box<dyn std::error::Error>> {
    for name in &cfg.clusters.names {
        let Some(def) = cfg.clusters.definitions.get(name) else {
            continue;
        };
        if def.role != ClusterRole::Provider || def.models.is_empty() {
            continue;
        }
        deploy_provider(name, def)?;
    }
    Ok(())
}

/// Verify provider gateways for all provider clusters.
///
/// Asserts the full `ext_proc → mock EPP → endpoint_selector → provider backend` path.
///
/// # Errors
///
/// Returns an error if any verification command fails unexpectedly or
/// if any assertion fails.
pub(crate) fn verify_all(cfg: &EnvConfig) -> Result<(), Box<dyn std::error::Error>> {
    let consumer_site = cfg
        .consumer_cluster_name()
        .ok_or("no consumer cluster configured in environment config")?;
    let mut tally = Tally::default();
    let mut found = false;

    for name in &cfg.clusters.names {
        let Some(def) = cfg.clusters.definitions.get(name) else {
            continue;
        };
        if def.role != ClusterRole::Provider || def.models.is_empty() {
            continue;
        }
        found = true;
        verify_provider(name, def, consumer_site, &mut tally)?;
    }

    if !found {
        return Err("no provider clusters with models found".into());
    }

    tally.print_summary()
}

// ---------------------------------------------------------------------------
// Per-provider deployment
// ---------------------------------------------------------------------------

/// Deploy mock EPP and provider gateway into one cluster.
fn deploy_provider(name: &str, def: &ClusterDef) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = kind::kubectl_context(name);
    eprintln!("  deploying provider gateway to {ctx}...");

    create_provider_tls_secret(&ctx, name)?;
    apply_mock_epp(&ctx, name, def)?;
    apply_gateway_config(&ctx, def)?;
    apply_gateway_deployment(&ctx)?;
    kubectl::rollout_restart(&ctx, GATEWAY_DEPLOYMENT)?;

    kubectl::wait_for_rollout(&ctx, MOCK_EPP_NAME, name)?;
    kubectl::wait_for_rollout(&ctx, GATEWAY_DEPLOYMENT, name)?;

    eprintln!("  [PASS] provider gateway ready in {ctx}");
    Ok(())
}

/// Create (or update) the TLS secret in a provider cluster.
///
/// The secret contains the site cert/key and the CA cert so the provider
/// gateway can terminate mTLS and validate client certificates.
fn create_provider_tls_secret(context: &str, site_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let certs_dir = PathBuf::from(HOST_CERTS_DIR);
    let cert = certs_dir.join(format!("{site_name}-cert.pem"));
    let key = certs_dir.join(format!("{site_name}-key.pem"));
    let ca = certs_dir.join("ca.pem");

    eprintln!("  creating TLS secret in {context}...");
    apply_tls_secret(context, &cert, &key, &ca)
}

/// Create the K8s Secret for TLS certs using kubectl create secret --dry-run.
fn apply_tls_secret(
    context: &str,
    cert: &std::path::Path,
    key: &std::path::Path,
    ca: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "create",
            "secret",
            "generic",
            TLS_SECRET_NAME,
            &format!("--from-file=tls.crt={}", cert.display()),
            &format!("--from-file=tls.key={}", key.display()),
            &format!("--from-file=ca.crt={}", ca.display()),
            "--namespace",
            NAMESPACE,
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("kubectl create secret failed: {stderr}").into());
    }

    let yaml = String::from_utf8(output.stdout)?;
    kubectl::apply_manifest(context, &yaml)
}

/// Apply mock EPP Deployment + Service.
#[expect(clippy::too_many_lines, reason = "K8s manifest generation")]
fn apply_mock_epp(context: &str, site_name: &str, def: &ClusterDef) -> Result<(), Box<dyn std::error::Error>> {
    let route_args = mock_epp_route_args(def);
    let mock_epp_img = image_overrides::mock_epp_image();
    let pull_policy = image_overrides::image_pull_policy();
    let yaml = format!(
        "apiVersion: apps/v1\n\
         kind: Deployment\n\
         metadata:\n\
         \x20 name: {MOCK_EPP_NAME}\n\
         \x20 namespace: {NAMESPACE}\n\
         \x20 labels:\n\
         \x20   app: {MOCK_EPP_NAME}\n\
         \x20   grid-site: {site_name}\n\
         spec:\n\
         \x20 replicas: 1\n\
         \x20 selector:\n\
         \x20   matchLabels:\n\
         \x20     app: {MOCK_EPP_NAME}\n\
         \x20 template:\n\
         \x20   metadata:\n\
         \x20     labels:\n\
         \x20       app: {MOCK_EPP_NAME}\n\
         \x20   spec:\n\
         \x20     containers:\n\
         \x20       - name: {MOCK_EPP_NAME}\n\
         \x20         image: {mock_epp_img}\n\
         \x20         imagePullPolicy: {pull_policy}\n\
         \x20         ports:\n\
         \x20           - containerPort: {MOCK_EPP_PORT}\n\
         \x20         args:\n\
         {route_args}\n\
         ---\n\
         apiVersion: v1\n\
         kind: Service\n\
         metadata:\n\
         \x20 name: {MOCK_EPP_NAME}\n\
         \x20 namespace: {NAMESPACE}\n\
         spec:\n\
         \x20 selector:\n\
         \x20   app: {MOCK_EPP_NAME}\n\
         \x20 ports:\n\
         \x20   - port: {MOCK_EPP_PORT}\n\
         \x20     targetPort: {MOCK_EPP_PORT}\n",
    );
    kubectl::apply_manifest(context, &yaml)
}

/// Update the mock EPP in a provider cluster to also handle `extra_model`.
///
/// Applies a replacement mock-EPP Deployment that routes both the site's
/// original models (from `original_models`) and `extra_model` to
/// `mock-openai-provider.default.svc:8080`.  All models route to the same
/// backend regardless of name; the original route entries preserve the EPP's
/// existing behavior for the site-specific models, while `extra_model` adds
/// the shared model needed for metrics-routing competition.
///
/// This is xtask validation infrastructure: the update is idempotent and
/// `kubectl apply` leaves the mock-EPP Service unchanged.
#[expect(
    clippy::too_many_lines,
    reason = "K8s Deployment YAML generation with per-route args"
)]
pub(crate) fn apply_mock_epp_with_extra_model(
    context: &str,
    site_name: &str,
    original_models: &[String],
    extra_model: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let target = format!("{}.{}.svc:{}", kind::MOCK_OPENAI_SVC, NAMESPACE, kind::MOCK_OPENAI_PORT);
    let mut routes: Vec<String> = original_models
        .iter()
        .map(|m| format!("            - \"--route={m}={target}\""))
        .collect();
    routes.push(format!("            - \"--route={extra_model}={target}\""));
    let route_args = routes.join("\n");

    let yaml = format!(
        "apiVersion: apps/v1
kind: Deployment
metadata:
  name: {MOCK_EPP_NAME}
  namespace: {NAMESPACE}
  labels:
    app: {MOCK_EPP_NAME}
    grid-site: {site_name}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {MOCK_EPP_NAME}
  template:
    metadata:
      labels:
        app: {MOCK_EPP_NAME}
    spec:
      containers:
        - name: {MOCK_EPP_NAME}
          image: {image}
          imagePullPolicy: {pull_policy}
          ports:
            - containerPort: {MOCK_EPP_PORT}
          args:
{route_args}
",
        image = image_overrides::mock_epp_image(),
        pull_policy = image_overrides::image_pull_policy(),
    );
    kubectl::apply_manifest(context, &yaml)?;
    eprintln!("  [OK] mock-epp patched in {site_name} to also serve {extra_model:?}");
    Ok(())
}

/// Format mock EPP route args from model definitions.
///
/// The target service depends on the cluster's [`ProviderBackend`]:
///
/// - [`ProviderBackend::InferenceSim`]: each model routes to its own per-model inference-sim service
///   (`inference-sim-{model}.{ns}.svc:8000`).
/// - [`ProviderBackend::MockOpenai`]: all models route to a single `mock-openai-provider` service
///   (`mock-openai-provider.{ns}.svc:{port}`).
pub(crate) fn mock_epp_route_args(def: &ClusterDef) -> String {
    def.models
        .iter()
        .map(|m| {
            let target = match def.backend {
                ProviderBackend::InferenceSim => {
                    let svc = kind::service_name(m);
                    format!("{svc}.{NAMESPACE}.svc:8000")
                },
                ProviderBackend::MockOpenai => {
                    format!("{}.{}.svc:{}", kind::MOCK_OPENAI_SVC, NAMESPACE, kind::MOCK_OPENAI_PORT)
                },
            };
            format!("            - \"--route={m}={target}\"")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Apply the gateway `ConfigMap`.
fn apply_gateway_config(context: &str, _def: &ClusterDef) -> Result<(), Box<dyn std::error::Error>> {
    let config_yaml = provider_gateway_config();
    let yaml = format!(
        "apiVersion: v1\n\
         kind: ConfigMap\n\
         metadata:\n\
         \x20 name: {GATEWAY_CONFIGMAP}\n\
         \x20 namespace: {NAMESPACE}\n\
         data:\n\
         \x20 praxis.yaml: |\n\
         {}\n",
        indent_yaml(&config_yaml, 4)
    );
    kubectl::apply_manifest(context, &yaml)
}

/// Provider gateway Praxis config.
///
/// Listener terminates mTLS: requires client certs signed by the generated test CA.
/// The TLS layer verifies CA trust only; it does not enforce the certificate
/// organization. Organization/digest/serial policy belongs in
/// `peer_identity_trust` once the Praxis image includes that filter.
///
/// NOTE: `peer_identity_trust` (filter-level peer identity enforcement) is
/// intentionally omitted here until the AI/Praxis image used by the Kind
/// environment includes the filter. When the Praxis pin/image is bumped, re-add:
///
/// ```text
/// - filter: peer_identity_trust
///   trusted_peers:
///     - organization: ai-grid
/// ```
///
/// The `ext_proc` filter uses `StreamBuffer` body mode.
#[expect(clippy::too_many_lines, reason = "provider Praxis config YAML generation")]
fn provider_gateway_config() -> String {
    format!(
        "listeners:\n\
         \x20 - name: provider\n\
         \x20   address: \"0.0.0.0:{GATEWAY_HTTP_PORT}\"\n\
         \x20   filter_chains:\n\
         \x20     - provider-chain\n\
         \x20   tls:\n\
         \x20     certificates:\n\
         \x20       - cert_path: {CERT_MOUNT_PATH}/tls.crt\n\
         \x20         key_path: {CERT_MOUNT_PATH}/tls.key\n\
         \x20     client_ca:\n\
         \x20       ca_path: {CERT_MOUNT_PATH}/ca.crt\n\
         \x20     client_cert_mode: require\n\
         \x20     hot_reload: false\n\
         filter_chains:\n\
         \x20 - name: provider-chain\n\
         \x20   filters:\n\
         \x20     - filter: ext_proc\n\
         \x20       target: \"http://{MOCK_EPP_NAME}:{MOCK_EPP_PORT}\"\n\
         \x20       processing_mode:\n\
         \x20         request_body_mode: full_duplex_streamed\n\
         \x20         response_header_mode: skip\n\
         \x20       message_timeout_ms: 5000\n\
         \x20       lifecycle_timeout_ms: 10000\n\
         \x20       status_on_error: 503\n\
         \x20     - filter: endpoint_selector\n\
         \x20       source_header: x-gateway-destination-endpoint\n\
         \x20       required: true\n\
         \x20       status_on_required_failure: 503\n\
         \x20       strip_header: true\n\
         admin:\n\
         \x20 address: \"127.0.0.1:9901\"\n\
         shutdown_timeout_secs: 5\n"
    )
}

/// Indent each line of YAML by `spaces` spaces.
fn indent_yaml(yaml: &str, spaces: usize) -> String {
    let prefix = " ".repeat(spaces);
    yaml.lines()
        .map(|l| {
            if l.is_empty() {
                String::new()
            } else {
                format!("{prefix}{l}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Apply the gateway Deployment + Service.
#[expect(clippy::too_many_lines, reason = "K8s manifest generation")]
fn apply_gateway_deployment(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let gateway_img = image_overrides::gateway_image();
    let pull_policy = image_overrides::image_pull_policy();
    let yaml = format!(
        "apiVersion: apps/v1\n\
         kind: Deployment\n\
         metadata:\n\
         \x20 name: {GATEWAY_DEPLOYMENT}\n\
         \x20 namespace: {NAMESPACE}\n\
         spec:\n\
         \x20 replicas: 1\n\
         \x20 selector:\n\
         \x20   matchLabels:\n\
         \x20     app: {GATEWAY_DEPLOYMENT}\n\
         \x20 template:\n\
         \x20   metadata:\n\
         \x20     labels:\n\
         \x20       app: {GATEWAY_DEPLOYMENT}\n\
         \x20   spec:\n\
         \x20     containers:\n\
         \x20       - name: praxis-ai\n\
         \x20         image: {gateway_img}\n\
         \x20         imagePullPolicy: {pull_policy}\n\
         \x20         ports:\n\
         \x20           - containerPort: {GATEWAY_HTTP_PORT}\n\
         \x20         args:\n\
         \x20           - \"--config\"\n\
         \x20           - \"/etc/praxis/praxis.yaml\"\n\
         \x20         volumeMounts:\n\
         \x20           - name: config\n\
         \x20             mountPath: /etc/praxis\n\
         \x20             readOnly: true\n\
         \x20           - name: tls-certs\n\
         \x20             mountPath: {CERT_MOUNT_PATH}\n\
         \x20             readOnly: true\n\
         \x20     volumes:\n\
         \x20       - name: config\n\
         \x20         configMap:\n\
         \x20           name: {GATEWAY_CONFIGMAP}\n\
         \x20       - name: tls-certs\n\
         \x20         secret:\n\
         \x20           secretName: {TLS_SECRET_NAME}\n\
         ---\n\
         apiVersion: v1\n\
         kind: Service\n\
         metadata:\n\
         \x20 name: {GATEWAY_SERVICE}\n\
         \x20 namespace: {NAMESPACE}\n\
         spec:\n\
         \x20 selector:\n\
         \x20   app: {GATEWAY_DEPLOYMENT}\n\
         \x20 ports:\n\
         \x20   - name: http\n\
         \x20     port: {GATEWAY_HTTP_PORT}\n\
         \x20     targetPort: {GATEWAY_HTTP_PORT}\n",
    );
    kubectl::apply_manifest(context, &yaml)
}

// ---------------------------------------------------------------------------
// Per-provider verification
// ---------------------------------------------------------------------------

/// Verify provider gateway for one cluster.
fn verify_provider(
    name: &str,
    def: &ClusterDef,
    consumer_site: &str,
    tally: &mut Tally,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = kind::kubectl_context(name);

    // Check deployments ready.
    check_deployment_ready(name, &ctx, MOCK_EPP_NAME, tally);
    check_deployment_ready(name, &ctx, GATEWAY_DEPLOYMENT, tally);

    // Port-forward to provider gateway.
    let port = find_free_port()?;
    let mut pf = PortForwardGuard::start(&ctx, GATEWAY_SERVICE, port, GATEWAY_HTTP_PORT)?;

    if !wait_for_port(port) {
        tally.fail(name, "provider gateway reachable via port-forward", &ctx);
        pf.stop();
        return Ok(());
    }
    tally.pass(name, "provider gateway reachable via port-forward");

    // Test each configured model via Chat Completions.
    for model in &def.models {
        verify_model(name, &ctx, port, model, name, consumer_site, tally);
    }

    // For mock-openai backend, also verify the Responses API path.
    if def.backend == ProviderBackend::MockOpenai {
        for model in &def.models {
            verify_responses_model(name, &ctx, port, model, name, consumer_site, tally);
        }
    }

    // Test unknown model → 503.
    verify_unknown_model(name, &ctx, port, name, consumer_site, tally);

    // Test spoofed header is ignored (valid model still succeeds).
    if let Some(model) = def.models.first() {
        verify_spoof_ignored(name, &ctx, port, model, name, consumer_site, tally);
    }

    pf.stop();
    Ok(())
}

/// Check a Kubernetes deployment has available replicas.
fn check_deployment_ready(cluster: &str, context: &str, deployment: &str, tally: &mut Tally) {
    let available = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            NAMESPACE,
            "get",
            "deployment",
            deployment,
            "-o",
            "jsonpath={.status.availableReplicas}",
        ])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .is_some_and(|s| {
            let t = s.trim();
            !t.is_empty() && t != "0"
        });

    if available {
        tally.pass(cluster, &format!("{deployment} deployment available"));
    } else {
        tally.fail(cluster, &format!("{deployment} deployment not available"), context);
    }
}

/// Verify a valid model routes to the configured provider backend and returns 200.
#[expect(
    clippy::too_many_arguments,
    reason = "cluster, context, port, model, site, tally all distinct"
)]
fn verify_model(
    cluster: &str,
    context: &str,
    port: u16,
    model: &str,
    site: &str,
    consumer_site: &str,
    tally: &mut Tally,
) {
    match send_chat_request(port, model, site, consumer_site) {
        Ok(resp) if resp.status == 200 => {
            tally.pass(cluster, &format!("model {model} returns 200 via ext_proc path"));
            validate_body(cluster, context, model, &resp.body, tally);
        },
        Ok(resp) => {
            let excerpt = safe_truncate(&resp.body, 200);
            tally.fail(
                cluster,
                &format!(
                    "model {model} returned {} (expected 200)\n         body: {excerpt}",
                    resp.status
                ),
                context,
            );
        },
        Err(e) => {
            tally.fail(cluster, &format!("model {model} request failed: {e}"), context);
        },
    }
}

/// Verify an unknown model fails closed with 503.
#[expect(
    clippy::too_many_arguments,
    reason = "cluster, context, port, site, consumer_site, tally all distinct"
)]
fn verify_unknown_model(cluster: &str, context: &str, port: u16, site: &str, consumer_site: &str, tally: &mut Tally) {
    match send_chat_request(port, "unknown-model-xyz", site, consumer_site) {
        Ok(resp) if resp.status == 503 => {
            tally.pass(cluster, "unknown model fails closed with 503");
        },
        Ok(resp) => {
            tally.fail(
                cluster,
                &format!("unknown model returned {} (expected 503)", resp.status),
                context,
            );
        },
        Err(e) => {
            tally.fail(cluster, &format!("unknown model request failed: {e}"), context);
        },
    }
}

/// Verify that a spoofed destination header is ignored.
///
/// Sends a valid model request with a client-supplied destination header
/// pointing at an unreachable endpoint. The mock EPP processor-set
/// endpoint must win and the request must still succeed.
#[expect(
    clippy::too_many_arguments,
    reason = "cluster, context, port, model, site, tally all distinct"
)]
fn verify_spoof_ignored(
    cluster: &str,
    context: &str,
    port: u16,
    model: &str,
    site: &str,
    consumer_site: &str,
    tally: &mut Tally,
) {
    match send_chat_request_with_spoof(port, model, "10.99.99.99:9999", site, consumer_site) {
        Ok(resp) if resp.status == 200 => {
            tally.pass(
                cluster,
                "spoofed destination header ignored; processor-selected endpoint wins",
            );
        },
        Ok(resp) => {
            tally.fail(
                cluster,
                &format!("spoofed header test returned {} (expected 200)", resp.status),
                context,
            );
        },
        Err(e) => {
            tally.fail(cluster, &format!("spoofed header test failed: {e}"), context);
        },
    }
}

/// Validate Chat Completions JSON response shape.
fn validate_body(cluster: &str, context: &str, model: &str, body: &str, tally: &mut Tally) {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(json) if json.get("choices").is_some_and(serde_json::Value::is_array) => {
            tally.pass(
                cluster,
                &format!("model {model} response is valid Chat Completions JSON"),
            );
        },
        Ok(_) => {
            tally.fail(
                cluster,
                &format!("model {model} response missing choices array"),
                context,
            );
        },
        Err(e) => {
            tally.fail(cluster, &format!("model {model} response not valid JSON: {e}"), context);
        },
    }
}

/// Validate Responses API JSON response shape.
///
/// Checks that the body has `object = "response"`, `status = "completed"`,
/// and a non-empty `output` array.  Rejects anything containing a Chat
/// Completions `choices` field.
pub(crate) fn validate_responses_body(cluster: &str, context: &str, model: &str, body: &str, tally: &mut Tally) {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(json) => {
            let is_response_obj = json.get("object").and_then(serde_json::Value::as_str) == Some("response");
            let is_completed = json.get("status").and_then(serde_json::Value::as_str) == Some("completed");
            let has_output = json
                .get("output")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|output| !output.is_empty());
            let has_choices = json.get("choices").is_some();

            if is_response_obj && is_completed && has_output && !has_choices {
                tally.pass(cluster, &format!("model {model} response is valid Responses API JSON"));
            } else {
                tally.fail(
                    cluster,
                    &format!(
                        "model {model} Responses body invalid \
                         (object={is_response_obj}, status={is_completed}, output={has_output}, choices={has_choices})"
                    ),
                    context,
                );
            }
        },
        Err(e) => {
            tally.fail(
                cluster,
                &format!("model {model} Responses body not valid JSON: {e}"),
                context,
            );
        },
    }
}

/// Verify that a model returns a valid Responses API response.
#[expect(
    clippy::too_many_arguments,
    reason = "cluster/context/port/model/site/consumer_site/tally all distinct"
)]
fn verify_responses_model(
    cluster: &str,
    context: &str,
    port: u16,
    model: &str,
    site: &str,
    consumer_site: &str,
    tally: &mut Tally,
) {
    match send_responses_request(port, model, site, consumer_site) {
        Ok(resp) if resp.status == 200 => {
            tally.pass(cluster, &format!("model {model} returns 200 via Responses API path"));
            validate_responses_body(cluster, context, model, &resp.body, tally);
        },
        Ok(resp) => {
            let excerpt = safe_truncate(&resp.body, 200);
            tally.fail(
                cluster,
                &format!(
                    "model {model} Responses request returned {} (expected 200)\n         body: {excerpt}",
                    resp.status
                ),
                context,
            );
        },
        Err(e) => {
            tally.fail(
                cluster,
                &format!("model {model} Responses request failed: {e}"),
                context,
            );
        },
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

/// Send a Chat Completions request to the provider gateway (mTLS).
///
/// Presents the configured consumer client cert and uses `--resolve` to map
/// the server's SAN hostname to the port-forward address.  Includes a Bearer
/// token required by the `mock-openai` backend; the `inference-sim` backend
/// ignores it.
fn send_chat_request(
    port: u16,
    model: &str,
    site: &str,
    consumer_site: &str,
) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let sni = format!("{site}.grid.internal");
    let url = format!("https://{sni}:{port}/v1/chat/completions");
    let resolve = format!("{sni}:{port}:127.0.0.1");
    let body = format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"hello"}}],"max_tokens":1}}"#);
    curl_post_mtls(&url, &body, None, &resolve, consumer_site)
}

/// Send a Chat Completions request with a spoofed destination header (mTLS).
fn send_chat_request_with_spoof(
    port: u16,
    model: &str,
    spoof: &str,
    site: &str,
    consumer_site: &str,
) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let sni = format!("{site}.grid.internal");
    let url = format!("https://{sni}:{port}/v1/chat/completions");
    let resolve = format!("{sni}:{port}:127.0.0.1");
    let body = format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"hello"}}],"max_tokens":1}}"#);
    curl_post_mtls(&url, &body, Some(spoof), &resolve, consumer_site)
}

/// Send a Responses API request to the provider gateway (mTLS).
///
/// Used when the provider cluster is configured with the `mock-openai` backend,
/// which serves `POST /v1/responses` in addition to Chat Completions.
pub(crate) fn send_responses_request(
    port: u16,
    model: &str,
    site: &str,
    consumer_site: &str,
) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let sni = format!("{site}.grid.internal");
    let url = format!("https://{sni}:{port}/v1/responses");
    let resolve = format!("{sni}:{port}:127.0.0.1");
    let body = format!(r#"{{"model":"{model}"}}"#);
    curl_post_mtls(&url, &body, None, &resolve, consumer_site)
}

/// HTTP POST via curl with mTLS and `--resolve` for hostname mapping.
///
/// Uses the configured consumer client cert to satisfy `client_cert_mode:
/// require`. The `resolve` argument maps the provider's SAN hostname to the
/// port-forward loopback address so rustls hostname verification passes.
///
/// A Bearer token is always included.  The `mock-openai` backend requires it;
/// the `inference-sim` backend ignores unknown headers.
#[expect(
    clippy::too_many_lines,
    reason = "curl arg list for mTLS with optional spoofed header"
)]
pub(crate) fn curl_post_mtls(
    url: &str,
    body: &str,
    spoof_header: Option<&str>,
    resolve: &str,
    consumer_site: &str,
) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let client_cert = format!("{HOST_CERTS_DIR}/{consumer_site}-cert.pem");
    let client_key = format!("{HOST_CERTS_DIR}/{consumer_site}-key.pem");
    let mut args = vec![
        "-s",
        "-w",
        "\n%{http_code}",
        "--connect-timeout",
        "5",
        "--max-time",
        "15",
        "--resolve",
        resolve,
        "--cacert",
        HOST_CA_CERT,
        "--cert",
        &client_cert,
        "--key",
        &client_key,
        "-X",
        "POST",
        "-H",
        "Authorization: Bearer dummy-key",
        "-H",
        "Content-Type: application/json",
    ];
    let spoof_hdr;
    if let Some(s) = spoof_header {
        spoof_hdr = format!("x-gateway-destination-endpoint: {s}");
        args.push("-H");
        args.push(&spoof_hdr);
    }
    args.extend(["-d", body, url]);

    let output = Command::new("curl").args(&args).output()?;
    let raw = String::from_utf8(output.stdout)?;
    parse_curl_output(&raw)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indent_yaml_adds_prefix() {
        let yaml = "key: value\nnested:\n  inner: 1";
        let indented = indent_yaml(yaml, 2);
        assert!(indented.starts_with("  key: value"), "should indent first line");
        assert!(indented.contains("\n  nested:"), "should indent subsequent lines");
    }

    #[test]
    fn mock_epp_route_args_inference_sim_per_model() {
        use crate::env::config::{ClusterDef, ClusterRole, ProviderBackend};
        let def = ClusterDef {
            models: vec!["granite-3.3-8b".to_owned(), "mistral-7b".to_owned()],
            role: ClusterRole::Provider,
            backend: ProviderBackend::InferenceSim,
        };
        let args = mock_epp_route_args(&def);
        assert!(args.contains("--route=granite-3.3-8b="), "must include granite route");
        assert!(args.contains("--route=mistral-7b="), "must include mistral route");
        assert!(args.contains(".default.svc:8000"), "must route to inference-sim port");
        assert!(
            !args.contains(kind::MOCK_OPENAI_SVC),
            "inference-sim mode must not reference mock-openai service"
        );
    }

    #[test]
    fn mock_epp_route_args_mock_openai_all_to_one_service() {
        use crate::env::config::{ClusterDef, ClusterRole, ProviderBackend};
        let def = ClusterDef {
            models: vec!["model-a".to_owned(), "model-b".to_owned()],
            role: ClusterRole::Provider,
            backend: ProviderBackend::MockOpenai,
        };
        let args = mock_epp_route_args(&def);
        assert!(args.contains("--route=model-a="), "must include model-a route");
        assert!(args.contains("--route=model-b="), "must include model-b route");
        assert!(
            args.contains(kind::MOCK_OPENAI_SVC),
            "mock-openai mode must route to mock-openai-provider service"
        );
        let target = format!("{}.default.svc:{}", kind::MOCK_OPENAI_SVC, kind::MOCK_OPENAI_PORT);
        assert!(args.contains(&target), "mock-openai target must include port");
        assert!(
            !args.contains("inference-sim"),
            "mock-openai mode must not reference inference-sim services"
        );
    }

    #[test]
    fn validate_responses_body_accepts_valid_shape() {
        let mut tally = Tally::default();
        let body = r#"{
            "id": "resp-001",
            "object": "response",
            "model": "model-a",
            "status": "completed",
            "output": [{"id": "msg-001", "type": "message"}],
            "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
        }"#;
        validate_responses_body("prov", "ctx", "model-a", body, &mut tally);
        let summary = tally.print_summary();
        assert!(summary.is_ok(), "valid Responses body must pass");
    }

    #[test]
    fn validate_responses_body_rejects_chat_completions_shape() {
        let mut tally = Tally::default();
        let body = r#"{"choices": [{"message": {"content": "hi"}}]}"#;
        validate_responses_body("prov", "ctx", "model-a", body, &mut tally);
        let summary = tally.print_summary();
        assert!(summary.is_err(), "Chat Completions body must fail Responses validation");
    }

    #[test]
    fn validate_responses_body_rejects_missing_status() {
        let mut tally = Tally::default();
        let body = r#"{"object": "response", "output": [], "model": "m"}"#;
        validate_responses_body("prov", "ctx", "model-a", body, &mut tally);
        let summary = tally.print_summary();
        assert!(summary.is_err(), "missing status must fail Responses validation");
    }

    #[test]
    fn validate_responses_body_rejects_missing_output() {
        let mut tally = Tally::default();
        let body = r#"{"object": "response", "status": "completed", "model": "m"}"#;
        validate_responses_body("prov", "ctx", "model-a", body, &mut tally);
        let summary = tally.print_summary();
        assert!(summary.is_err(), "missing output must fail Responses validation");
    }

    #[test]
    fn validate_responses_body_rejects_empty_output() {
        let mut tally = Tally::default();
        let body = r#"{"object": "response", "status": "completed", "output": [], "model": "m"}"#;
        validate_responses_body("prov", "ctx", "model-a", body, &mut tally);
        let summary = tally.print_summary();
        assert!(summary.is_err(), "empty output must fail Responses validation");
    }

    #[test]
    fn validate_responses_body_rejects_invalid_json() {
        let mut tally = Tally::default();
        validate_responses_body("prov", "ctx", "model-a", "{not valid", &mut tally);
        let summary = tally.print_summary();
        assert!(summary.is_err(), "invalid JSON must fail Responses validation");
    }
}
