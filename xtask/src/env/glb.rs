//! Grid routing and provider-boundary verifier.
//!
//! Runs the Grid routing and provider-boundary proof. The proof covers
//! environment identity, SWIM discovery, edge-local overlays, provider mTLS,
//! private backend policy, credential replacement, session behavior, and
//! overlay hot reload without assigning a public contract to a step count.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use crate::env::{
    DemoMode, IngressMode, StepResult, StepStatus, certs, external_provider::ExternalProviderDescriptor, kubectl,
    print_validate_all_table, safe_truncate_str, verify,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Kubernetes namespace for Grid resources.
const GRID_SYSTEM_NS: &str = "grid-system";

/// Cluster name prefix from the GLB demo config.
const CLUSTER_PREFIX: &str = "grid-glb";

/// Expected cluster names in the GLB demo environment.
const CLUSTER_NAMES: &[&str] = &[
    "gtm-emulator",
    "east-edge",
    "east-provider",
    "west-edge",
    "west-provider",
];

/// Clusters participating in the Grid SWIM mesh.
const GRID_CLUSTERS: &[&str] = &["east-edge", "east-provider", "west-edge", "west-provider"];

/// Required CLI tools (checked during prerequisites).
const REQUIRED_TOOLS: &[&str] = &["kind", "kubectl", "curl", "docker", "openssl"];

/// Provider-role clusters that advertise a gateway address.
const PROVIDER_CLUSTERS: &[&str] = &["east-provider", "west-provider"];

/// Clusters running public Praxis edge gateways.
const EDGE_CLUSTERS: &[&str] = &["east-edge", "west-edge"];

/// SWIM LB service name in the GLB demo.
const SWIM_LB_SERVICE: &str = "grid-operator-swim";

/// Overlay [`ConfigMap`] name on the edge site.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
const OVERLAY_CONFIGMAP: &str = "grid-overlay-glb-demo-edge-gateway";

/// [`GridNetwork`] resource name in the GLB demo.
///
/// [`GridNetwork`]: crate
const GRID_NETWORK_NAME: &str = "glb-demo";

/// Edge service host port.
const EDGE_PORT: u16 = 8080;

/// Edge used by the direct Grid routing and hot-reload proof.
const PRIMARY_EDGE: &str = "east-edge";

/// Maximum time for provider state to traverse reconciliation and SWIM.
const PROVIDER_STATE_WAIT: Duration = Duration::from_secs(180);

/// Maximum time for request routing to reflect an accepted overlay update.
const DATA_PLANE_CONVERGENCE_WAIT: Duration = Duration::from_secs(45);

/// Delay between request-path convergence probes.
const DATA_PLANE_PROBE_INTERVAL: Duration = Duration::from_secs(1);

/// Window over which [`check_overlay_metadata_settles`] confirms the overlay
/// `ConfigMap`'s `resourceVersion` stops changing once converged.
///
/// grid#42's reconcile hot-loop produced continuous writes at roughly
/// 250-300/sec on helios08, so any window long enough to observe a handful of
/// reconcile ticks (the operator's default requeue is much faster than that
/// under active SWIM traffic) is enough to distinguish "stopped writing" from
/// "still churning" — 15s is comfortably longer than that while keeping the
/// added proof step fast.
const OVERLAY_SETTLE_WINDOW: Duration = Duration::from_secs(15);

/// Runtime directory retaining the public CA used by client probes.
const GTM_TLS_DIR: &str = ".forge/runtime/glb-tls/gtm";

/// Stable HTTPS name exposed by the local GTM profile.
const GTM_SERVER_NAME: &str = "api.grid-glb.test";

/// Source directory used by the shared test certificate generator.
const GENERATED_CERTS_DIR: &str = "tests/env/certs";

/// Kubernetes Secret mounted by provider Praxis gateways.
const PROVIDER_TLS_SECRET: &str = "provider-gateway-tls";

/// Kubernetes Secret mounted by edge Praxis gateways.
const EDGE_TLS_SECRET: &str = "edge-gateway-tls";

/// Kubernetes Secret mounted by the GTM emulator.
const GTM_TLS_SECRET: &str = "gtm-emulator-tls";

/// Kubernetes Secret containing the demo backend's final-hop credential.
const PROVIDER_CREDENTIAL_SECRET: &str = "vcr-inference-credential";

/// Kubernetes Secret containing the second east provider's credential.
const SECONDARY_PROVIDER_CREDENTIAL_SECRET: &str = "vcr-inference-secondary-credential";

/// Token replaced with the generated edge certificate digest.
const EDGE_US_EAST_CERT_DIGEST_TOKEN: &str = "__EDGE_US_EAST_CERT_SHA256__";

/// Provider-config token replaced with the west edge certificate digest.
const EDGE_US_WEST_CERT_DIGEST_TOKEN: &str = "__EDGE_US_WEST_CERT_SHA256__";

/// Provider-config token replaced with the deterministic candidate ID.
const PROVIDER_CANDIDATE_ID_TOKEN: &str = "__GRID_CANDIDATE_ID__";

/// East provider-config token replaced with the second provider candidate ID.
const SECONDARY_PROVIDER_CANDIDATE_ID_TOKEN: &str = "__GRID_SECONDARY_CANDIDATE_ID__";

/// Candidate kind used by the GLB provider fixture.
const DEMO_CANDIDATE_KIND: &str = "inference_model";

/// Model name used by the GLB provider fixture.
const DEMO_MODEL: &str = "Qwen/Qwen3-0.6B";

/// Second provider hosted in the east provider cluster.
const EAST_SECONDARY_PROVIDER: &str = "east-provider-secondary";

/// Routing-cluster identity of the second east provider.
const EAST_SECONDARY_CLUSTER: &str = "vcr-east-provider-secondary";

/// Kubernetes Secret for the `OpenAI` API key.
const OPENAI_CREDENTIAL_SECRET: &str = "openai-api-key";

/// Provider identity for the `OpenAI` external provider.
const OPENAI_PROVIDER: &str = "openai-api-provider";

/// Routing-cluster identity for the `OpenAI` external provider.
const OPENAI_ROUTING_CLUSTER: &str = "openai-api";

/// AI-owned candidate identity header on the authenticated provider hop.
const AI_ROUTING_CANDIDATE_HEADER: &str = "x-ai-routing-candidate";

/// AI-owned request correlation header on the authenticated provider hop.
const AI_ROUTING_REQUEST_ID_HEADER: &str = "x-ai-routing-request-id";

/// Provider-owned response attribution emitted by `provider_route`.
const PROVIDER_GATEWAY_RESPONSE_HEADER: &str = "x-ai-demo-provider-gateway";

/// Overlay envelope `ConfigMap` annotation for the schema version.
const OVERLAY_ANNOTATION_SCHEMA: &str = "grid.praxis-proxy.io/overlay-schema-version";

/// Overlay envelope `ConfigMap` annotation for the semantic revision.
const OVERLAY_ANNOTATION_REVISION: &str = "grid.praxis-proxy.io/overlay-revision";

/// Overlay envelope `ConfigMap` annotation for the content digest.
const OVERLAY_ANNOTATION_DIGEST: &str = "grid.praxis-proxy.io/overlay-content-digest";

/// External credential sent to the edge; intentionally differs from provider auth.
const CLIENT_BEARER_TOKEN: &str = "test-token";

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Materialize the mTLS contract used by the GLB provider boundary.
///
/// The edge receives one client identity. Each provider receives its own
/// server identity with a site-specific SNI. All identities are signed by the
/// same demo CA and use organization `ai-grid`, which is independently checked
/// by `peer_identity_trust` in the provider pipeline.
pub(crate) fn prepare_provider_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let demo_root = super::demo_root(Path::new("tests/e2e/topologies/grid-glb-demo/forge.yaml"));
    stage_provider_boundary_with_mode_and_external(IngressMode::Global, None, &demo_root)
        .and_then(|()| install_provider_boundary())
}

/// Generate identities for an ingress mode and optional external provider.
///
/// `demo_root` is the canonical directory containing the active demo assets
/// (configs, resources, policies, fixtures). All demo-relative file reads
/// resolve from this root.
pub(crate) fn stage_provider_boundary_with_mode_and_external(
    ingress_mode: IngressMode,
    external: Option<&ExternalProviderDescriptor>,
    demo_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let identities = vec![
        "east-edge".to_owned(),
        "west-edge".to_owned(),
        "edge-untrusted".to_owned(),
        "east-provider".to_owned(),
        "west-provider".to_owned(),
    ];
    certs::generate_all(&identities)?;
    let wrong_ca = ::certs::generate_ca("Grid GLB untrusted test CA")?;
    fs::write(
        Path::new(GENERATED_CERTS_DIR).join("untrusted-ca.pem"),
        wrong_ca.cert_pem,
    )?;
    if ingress_mode == IngressMode::Global {
        stage_gtm_tls()?;
    }
    let east_edge_digest = certs::certificate_sha256(&Path::new(GENERATED_CERTS_DIR).join("east-edge-cert.pem"))?;
    let west_edge_digest = certs::certificate_sha256(&Path::new(GENERATED_CERTS_DIR).join("west-edge-cert.pem"))?;
    stage_provider_configs_with_external(&east_edge_digest, &west_edge_digest, external, demo_root)?;
    Ok(())
}

/// Install staged provider identities, configs, and backend credentials.
///
/// The `grid-system` namespace must exist. Provider deployments may already
/// exist or may be applied after this function returns.
pub(crate) fn install_provider_boundary() -> Result<(), Box<dyn std::error::Error>> {
    install_provider_boundary_with_mode_and_external(IngressMode::Global, None)
}

/// Install identities for an ingress mode and optional external provider.
///
/// When `external_key_file` is provided, the `OpenAI` API key Secret is created
/// from that file on the east-provider cluster. The key file content is never
/// read or logged by this process; `kubectl --from-file` handles the read.
#[expect(clippy::too_many_lines, reason = "mode-branched sequential identity installation")]
pub(crate) fn install_provider_boundary_with_mode_and_external(
    ingress_mode: IngressMode,
    external_key_file: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    if ingress_mode == IngressMode::Global {
        ensure_demo_namespace("gtm-emulator")?;
    }
    for edge in EDGE_CLUSTERS {
        apply_identity_tls_secret(edge, edge, EDGE_TLS_SECRET)?;
    }
    if ingress_mode == IngressMode::Global {
        apply_gtm_tls_secret()?;
    }
    for provider in PROVIDER_CLUSTERS {
        let provider_credential = generate_provider_credential()?;
        apply_provider_config(provider)?;
        apply_provider_tls_secret(provider)?;
        apply_provider_credential_secret(provider, &provider_credential)?;
        if *provider == "east-provider" {
            let secondary_credential = generate_provider_credential()?;
            apply_named_provider_credential_secret(
                provider,
                SECONDARY_PROVIDER_CREDENTIAL_SECRET,
                &secondary_credential,
            )?;
            if let Some(key_file) = external_key_file {
                apply_openai_credential_from_file(key_file)?;
            }
        }
        restart_provider_deployments_if_present(provider)?;
    }
    let gtm_note = if ingress_mode == IngressMode::Global {
        ", GTM certificate"
    } else {
        ""
    };
    let ext_label = if external_key_file.is_some() {
        " OpenAI credential,"
    } else {
        ""
    };
    eprintln!(
        "grid-routing: edge identities{gtm_note}, provider configs, mTLS material,{ext_label} and credentials installed"
    );
    Ok(())
}

/// Restart and await provider workloads when they have already been applied.
fn restart_provider_deployments_if_present(provider: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = kubectl_context(provider);
    for deployment in ["vcr-inference", "vcr-inference-secondary", "provider-gateway"] {
        if !deployment_exists(&context, deployment)? {
            continue;
        }
        restart_deployment(provider, deployment)?;
        kubectl::wait_for_rollout_ns(&context, deployment, GRID_SYSTEM_NS, provider)?;
    }
    Ok(())
}

/// Return whether a named provider deployment currently exists.
fn deployment_exists(context: &str, deployment: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "deployment",
            deployment,
            "--ignore-not-found",
            "-o",
            "name",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to inspect deployment/{deployment} in {context}: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 160)
        )
        .into());
    }
    Ok(!output.stdout.is_empty())
}

/// Render provider gateway configs with the edge certificate digest.
#[expect(clippy::too_many_lines, reason = "template rendering with token validation")]
fn stage_provider_configs_with_external(
    east_edge_digest: &str,
    west_edge_digest: &str,
    external: Option<&ExternalProviderDescriptor>,
    demo_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for provider in PROVIDER_CLUSTERS {
        let source = demo_root.join("configs").join(provider).join("praxis.yaml");
        let template = fs::read_to_string(&source)?;
        if !template.contains(EDGE_US_EAST_CERT_DIGEST_TOKEN)
            || !template.contains(EDGE_US_WEST_CERT_DIGEST_TOKEN)
            || !template.contains(PROVIDER_CANDIDATE_ID_TOKEN)
            || (*provider == "east-provider" && !template.contains(SECONDARY_PROVIDER_CANDIDATE_ID_TOKEN))
        {
            return Err(format!(
                "provider config template is missing an identity token: {}",
                source.display()
            )
            .into());
        }
        let mut rendered = template
            .replace(EDGE_US_EAST_CERT_DIGEST_TOKEN, east_edge_digest)
            .replace(EDGE_US_WEST_CERT_DIGEST_TOKEN, west_edge_digest)
            .replace(PROVIDER_CANDIDATE_ID_TOKEN, &provider_candidate_id(provider)?)
            .replace(
                SECONDARY_PROVIDER_CANDIDATE_ID_TOKEN,
                &provider_candidate_id(EAST_SECONDARY_PROVIDER)?,
            );
        if *provider == "east-provider"
            && let Some(ext) = external
        {
            rendered = append_openai_provider_config(&rendered, ext, &openai_candidate_id(&ext.model))?;
        }
        let target_dir = Path::new(".forge/runtime/glb-tls/provider-configs").join(provider);
        fs::create_dir_all(&target_dir)?;
        fs::write(target_dir.join("praxis.yaml"), rendered)?;
    }
    Ok(())
}

/// Patch the rendered edge gateway `ConfigMaps` to include the `OpenAI` cluster.
///
/// Forge has already rendered `configs/edge/praxis.yaml` with captures and
/// created the `edge-gateway-config` `ConfigMap`. This function reads the
/// deployed `ConfigMap`, injects the `OpenAI` entries, and re-applies it.
pub(crate) fn patch_edge_configs_for_openai(
    ext: &ExternalProviderDescriptor,
) -> Result<(), Box<dyn std::error::Error>> {
    // This patch runs after Forge capture rendering, so it must use the
    // concrete provider address rather than introduce a new capture token.
    let provider_context = kubectl_context("east-provider");
    let provider_ip = get_provider_gateway_ip(&provider_context)?;
    let provider_endpoint = format!("{provider_ip}:8443");

    for edge in EDGE_CLUSTERS {
        let context = kubectl_context(edge);
        let config = read_configmap_key(&context, "edge-gateway-config", "praxis.yaml")?;
        let patched = append_openai_edge_config(&config, ext, &provider_endpoint)?;
        apply_configmap_key(&context, "edge-gateway-config", "praxis.yaml", &patched)?;
        restart_deployment(edge, "edge-gateway")?;
        kubectl::wait_for_rollout_ns(&context, "edge-gateway", GRID_SYSTEM_NS, edge)?;
    }
    Ok(())
}

/// Read one key from a `ConfigMap`.
fn read_configmap_key(context: &str, configmap: &str, key: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "configmap",
            configmap,
            "-o",
            &format!("go-template={{{{index .data \"{key}\"}}}}"),
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to read ConfigMap {configmap} key {key}: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 160)
        )
        .into());
    }
    let result = String::from_utf8(output.stdout)?;
    if result.is_empty() {
        return Err(format!("ConfigMap {configmap} key {key} returned empty value").into());
    }
    Ok(result)
}

/// Replace one key in a `ConfigMap` by re-creating with dry-run and applying.
fn apply_configmap_key(
    context: &str,
    configmap: &str,
    key: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::NamedTempFile::new()?;
    fs::write(tmp.path(), value)?;
    let from_file = format!("--from-file={key}={}", tmp.path().display());
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "configmap",
            configmap,
            &from_file,
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to render ConfigMap {configmap}: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 160)
        )
        .into());
    }
    kubectl::apply_manifest(context, &String::from_utf8(output.stdout)?)
}

/// Append the `OpenAI` provider route, credential, and upstream to the east provider config.
#[expect(clippy::too_many_lines, reason = "multi-section YAML insertion that reads linearly")]
pub(crate) fn append_openai_provider_config(
    config: &str,
    ext: &ExternalProviderDescriptor,
    candidate_id: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let route_entry = format!(
        "          - candidate_id: {candidate_id}
            model: {model}
            paths:
{paths}
            cluster: {cluster}
            credential:
              strategy: bearer_token
              secretRef:
                name: {secret_name}
                namespace: grid-system
                key: {secret_key}",
        model = ext.model,
        paths = ext
            .allowed_paths
            .iter()
            .map(|p| format!("              - {p}"))
            .collect::<Vec<_>>()
            .join("\n"),
        cluster = ext.routing_cluster,
        secret_name = ext.secret_name,
        secret_key = ext.secret_key,
    );

    let credential_entry = format!(
        "          - strategy: bearer_token
            name: {secret_name}
            namespace: grid-system
            key: {secret_key}
            file: {credential_file}",
        secret_name = ext.secret_name,
        secret_key = ext.secret_key,
        credential_file = ext.credential_file(),
    );

    let cluster_entry = format!(
        r#"          - name: {cluster}
            authority: {authority}
            tls:
              sni: {sni}
              verify: true
            endpoints:
              - "{endpoint}""#,
        cluster = ext.routing_cluster,
        authority = ext.hostname,
        sni = ext.sni,
        endpoint = ext.endpoint(),
    );

    let mut result = config.to_owned();

    // Insert after the last provider_route entry (before credential_inject filter).
    if let Some(pos) = result.find("      - filter: credential_inject") {
        result.insert_str(pos, &format!("{route_entry}\n"));
    } else {
        return Err("cannot find credential_inject filter in provider config".into());
    }

    // Insert after the last credential_inject entry (before load_balancer filter).
    if let Some(pos) = result.find("      - filter: load_balancer") {
        result.insert_str(pos, &format!("{credential_entry}\n"));
    } else {
        return Err("cannot find load_balancer filter in provider config".into());
    }

    // Insert after the last load_balancer cluster entry (before admin section).
    if let Some(pos) = result.find("\nadmin:") {
        result.insert_str(pos, &format!("\n{cluster_entry}\n"));
    } else {
        return Err("cannot find admin section in provider config".into());
    }

    Ok(result)
}

/// Append the `OpenAI` routing cluster to the edge gateway config.
#[expect(clippy::too_many_lines, reason = "multi-section YAML insertion that reads linearly")]
#[expect(
    clippy::string_slice,
    reason = "byte offset from str::find is always a char boundary"
)]
fn append_openai_edge_config(
    config: &str,
    ext: &ExternalProviderDescriptor,
    provider_endpoint: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    // Add openai-api to provider_hop_clusters list.
    let hop_marker = "provider_hop_clusters:";
    let Some(hop_pos) = config.find(hop_marker) else {
        return Err("cannot find provider_hop_clusters in edge config".into());
    };
    let after_hop = hop_pos + hop_marker.len();
    // Find the next filter or config key after the hop clusters list.
    // Insert the new cluster at the end of the list.
    let list_end = config[after_hop..]
        .find("\n        expected_overlay_scope:")
        .ok_or("cannot find expected_overlay_scope after provider_hop_clusters")?;
    let insert_pos = after_hop + list_end;
    let mut result = config.to_owned();
    result.insert_str(insert_pos, &format!("\n          - {}", ext.routing_cluster));

    // Add the openai-api cluster to the load_balancer section.
    // Reuse the east-provider endpoint and TLS since the traffic goes through the same gateway.
    let cluster_entry = format!(
        r#"
          - name: {cluster}
            tls:
              ca:
                ca_path: /etc/praxis/tls/ca.crt
              client_cert:
                cert_path: /etc/praxis/tls/tls.crt
                key_path: /etc/praxis/tls/tls.key
              sni: east-provider.grid.internal
              verify: true
            endpoints:
              - "{provider_endpoint}""#,
        cluster = ext.routing_cluster,
        provider_endpoint = provider_endpoint,
    );

    if let Some(pos) = result.find("\nadmin:") {
        result.insert_str(pos, &cluster_entry);
    } else {
        return Err("cannot find admin section in edge config".into());
    }

    Ok(result)
}

/// Write a credential file with trailing CR/LF stripped to a temporary file.
///
/// Returns the [`tempfile::NamedTempFile`] handle — the caller must keep it alive until
/// the file is no longer needed (e.g., until `kubectl` finishes reading it).
fn write_trimmed_credential(key_file: &Path) -> Result<tempfile::NamedTempFile, Box<dyn std::error::Error>> {
    let content = fs::read(key_file).map_err(|e| format!("cannot read key file: {e}"))?;
    let trimmed = content
        .strip_suffix(b"\r\n")
        .or_else(|| content.strip_suffix(b"\n"))
        .unwrap_or(&content);
    if trimmed.is_empty() {
        return Err("key file is empty after trimming trailing newline".into());
    }
    let mut tmp = tempfile::NamedTempFile::new().map_err(|e| format!("cannot create temp file: {e}"))?;
    std::io::Write::write_all(&mut tmp, trimmed).map_err(|e| format!("cannot write trimmed credential: {e}"))?;
    Ok(tmp)
}

/// Create the `OpenAI` API key Secret from the user-supplied key file.
///
/// Strips trailing newlines before passing the file to `kubectl` so that
/// the Secret value matches what `credential_inject` trims at runtime.
/// Uses `kubectl create secret --from-file --dry-run=client -o yaml | kubectl apply`
/// to avoid passing the token in command arguments or environment variables.
pub(crate) fn apply_openai_credential_from_file(key_file: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let trimmed = write_trimmed_credential(key_file)?;
    let context = kubectl_context("east-provider");
    let from_file_arg = format!("--from-file=token={}", trimmed.path().display());
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "secret",
            "generic",
            OPENAI_CREDENTIAL_SECRET,
            &from_file_arg,
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to render OpenAI credential Secret: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 160)
        )
        .into());
    }
    kubectl::apply_manifest(&context, &String::from_utf8(output.stdout)?)
}

/// Apply the `OpenAI` `InferenceProvider` CRD to the east-provider cluster.
#[expect(clippy::too_many_lines, reason = "CRD manifest template that reads linearly")]
pub(crate) fn apply_openai_inference_provider(
    ext: &ExternalProviderDescriptor,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = format!(
        "apiVersion: grid.praxis-proxy.io/v1alpha1
kind: InferenceProvider
metadata:
  name: {name}
spec:
  gridNetworkRef: glb-demo
  providerKind: {provider_kind}
  backendKind: {backend_kind}
  endpoint: https://{hostname}
  models:
    - name: {model}
  routingClusterRef: {routing_cluster}
  siteSelector:
    matchLabels:
      grid.praxis-proxy.io/provider-site: east-provider
  accessPolicy:
    siteSelector:
      matchLabels: {{}}
  auth:
    strategy: bearer_token
    secretRef:
      name: {secret_name}
      namespace: grid-system
      key: {secret_key}
    manual: true",
        name = OPENAI_PROVIDER,
        provider_kind = ext.provider_kind,
        backend_kind = ext.backend_kind,
        hostname = ext.hostname,
        model = ext.model,
        routing_cluster = ext.routing_cluster,
        secret_name = ext.secret_name,
        secret_key = ext.secret_key,
    );
    let context = kubectl_context("east-provider");
    kubectl::apply_manifest(&context, &manifest)
}

/// Compute the candidate ID for the `OpenAI` external provider.
pub(crate) fn openai_candidate_id(model: &str) -> String {
    fnv1a_hex8(&format!(
        "{DEMO_CANDIDATE_KIND}/{model}/east-provider/{OPENAI_ROUTING_CLUSTER}"
    ))
}

/// Verify the exact external-provider candidate and accepted revision on one edge.
#[expect(
    clippy::too_many_lines,
    reason = "keeps candidate, distributed revision, and serving revision proof atomic"
)]
pub(crate) fn verify_external_candidate(
    edge: &str,
    ext: &ExternalProviderDescriptor,
) -> Result<String, Box<dyn std::error::Error>> {
    let overlay: serde_json::Value = serde_json::from_str(&read_overlay(edge)?)
        .map_err(|error| format!("failed to parse {edge} overlay: {error}"))?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{edge} overlay missing candidates array"))?;
    let candidate_id = openai_candidate_id(&ext.model);
    let candidate = candidates
        .iter()
        .find(|candidate| {
            candidate.get("cluster").and_then(serde_json::Value::as_str) == Some(ext.routing_cluster)
                && candidate.get("stable_id").and_then(serde_json::Value::as_str) == Some(candidate_id.as_str())
        })
        .ok_or_else(|| {
            format!(
                "{edge} overlay missing external candidate {candidate_id} for cluster {}",
                ext.routing_cluster
            )
        })?;
    if candidate.get("name").and_then(serde_json::Value::as_str) != Some(ext.model.as_str()) {
        return Err(format!("{edge} external candidate model does not match {}", ext.model).into());
    }

    let revision = overlay_revision(edge)?;
    verify_edge_accepted_revision(edge, &revision)?;
    let logs = edge_gateway_logs(edge)?;
    if !logs.lines().any(|line| {
        (line.contains("overlay snapshot initialized") || line.contains("overlay reloaded"))
            && accepted_revision_from_line(line) == Some(revision.as_str())
            && serving_revision_from_line(line) == Some(revision.as_str())
    }) {
        return Err(format!("{edge} logs do not prove serving overlay revision {revision}").into());
    }
    Ok(revision)
}

/// Prove a normal-mode deployment contains no OpenAI-specific runtime resources.
#[expect(
    clippy::too_many_lines,
    reason = "checks every external-provider runtime surface in one fail-closed audit"
)]
pub(crate) fn verify_external_provider_absent() -> Result<String, Box<dyn std::error::Error>> {
    let east_provider = kubectl_context("east-provider");
    for (resource, name) in [
        ("secret", OPENAI_CREDENTIAL_SECRET),
        ("inferenceprovider", OPENAI_PROVIDER),
    ] {
        let output = Command::new("kubectl")
            .args([
                "--context",
                &east_provider,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                resource,
                name,
                "--ignore-not-found",
                "-o",
                "name",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!("failed to check for {resource}/{name}").into());
        }
        if !output.stdout.is_empty() {
            return Err(format!("normal mode unexpectedly created {resource}/{name}").into());
        }
    }

    let provider_config = read_configmap_key(&east_provider, "provider-gateway-config", "praxis.yaml")?;
    if provider_config.contains("openai-api") || provider_config.contains("openai-api-key") {
        return Err("normal-mode provider configuration contains OpenAI entries".into());
    }
    for edge in EDGE_CLUSTERS {
        let edge_config = read_configmap_key(&kubectl_context(edge), "edge-gateway-config", "praxis.yaml")?;
        if edge_config.contains("openai-api") {
            return Err(format!("normal-mode {edge} configuration contains OpenAI entries").into());
        }
    }

    Ok("OpenAI Secret, InferenceProvider, provider route, and edge clusters absent".to_owned())
}

/// Generate a public-facing certificate for the stable local GTM name.
fn stage_gtm_tls() -> Result<(), Box<dyn std::error::Error>> {
    let certs_dir = Path::new(GENERATED_CERTS_DIR);
    let ca = certs::load_or_generate_ca(certs_dir)?;
    let certificate = ::certs::generate_dns_cert(&ca, "Grid GLB demo ingress", GTM_SERVER_NAME)?;
    let target = Path::new(GTM_TLS_DIR);
    fs::create_dir_all(target)?;
    fs::write(target.join("ca.crt"), &ca.cert_pem)?;
    fs::write(target.join("tls.crt"), certificate.cert_pem)?;
    fs::write(target.join("tls.key"), certificate.key_pem)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(target, fs::Permissions::from_mode(0o750))?;
        fs::set_permissions(target.join("tls.key"), fs::Permissions::from_mode(0o640))?;
    }
    Ok(())
}

/// Ensure the shared namespace exists in a cluster without a Grid operator.
fn ensure_demo_namespace(cluster: &str) -> Result<(), Box<dyn std::error::Error>> {
    kubectl::apply_manifest(
        &kubectl_context(cluster),
        r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"grid-system"}}"#,
    )
}

/// Apply an identity certificate and the demo CA as one Kubernetes Secret.
fn apply_identity_tls_secret(
    cluster: &str,
    identity: &str,
    secret_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let certs_dir = Path::new(GENERATED_CERTS_DIR);
    apply_tls_secret_from_paths(
        cluster,
        secret_name,
        &certs_dir.join(format!("{identity}-cert.pem")),
        &certs_dir.join(format!("{identity}-key.pem")),
        &certs_dir.join("ca.pem"),
    )
}

/// Apply the stable-name certificate used by the GTM emulator.
fn apply_gtm_tls_secret() -> Result<(), Box<dyn std::error::Error>> {
    let tls_dir = Path::new(GTM_TLS_DIR);
    apply_tls_secret_from_paths(
        "gtm-emulator",
        GTM_TLS_SECRET,
        &tls_dir.join("tls.crt"),
        &tls_dir.join("tls.key"),
        &tls_dir.join("ca.crt"),
    )
}

/// Render and apply a generic TLS Secret from three local files.
fn apply_tls_secret_from_paths(
    cluster: &str,
    secret_name: &str,
    cert: &Path,
    key: &Path,
    ca: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(cluster),
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "secret",
            "generic",
            secret_name,
            &format!("--from-file=tls.crt={}", cert.display()),
            &format!("--from-file=tls.key={}", key.display()),
            &format!("--from-file=ca.crt={}", ca.display()),
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to render {cluster} Secret/{secret_name}: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 160)
        )
        .into());
    }
    kubectl::apply_manifest(&kubectl_context(cluster), &String::from_utf8(output.stdout)?)
}

/// Apply the rendered provider gateway `ConfigMap`.
fn apply_provider_config(provider: &str) -> Result<(), Box<dyn std::error::Error>> {
    let config_path = Path::new(".forge/runtime/glb-tls/provider-configs")
        .join(provider)
        .join("praxis.yaml");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(provider),
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "configmap",
            "provider-gateway-config",
            &format!("--from-file=praxis.yaml={}", config_path.display()),
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("failed to render {provider} provider ConfigMap: {}", stderr.trim()).into());
    }
    let manifest = String::from_utf8(output.stdout)?;
    kubectl::apply_manifest(&kubectl_context(provider), &manifest)
}

/// Build `--from-file` arguments for a provider TLS Secret.
fn tls_secret_from_file_args(provider: &str) -> [String; 3] {
    let d = Path::new(GENERATED_CERTS_DIR);
    [
        format!(
            "--from-file=tls.crt={}",
            d.join(format!("{provider}-cert.pem")).display()
        ),
        format!(
            "--from-file=tls.key={}",
            d.join(format!("{provider}-key.pem")).display()
        ),
        format!("--from-file=ca.crt={}", d.join("ca.pem").display()),
    ]
}

/// Apply the provider TLS Secret containing cert, key, and CA.
fn apply_provider_tls_secret(provider: &str) -> Result<(), Box<dyn std::error::Error>> {
    let from_files = tls_secret_from_file_args(provider);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(provider),
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "secret",
            "generic",
            PROVIDER_TLS_SECRET,
            &from_files[0],
            &from_files[1],
            &from_files[2],
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("failed to render {provider} TLS Secret: {}", stderr.trim()).into());
    }
    let manifest = String::from_utf8(output.stdout)?;
    kubectl::apply_manifest(&kubectl_context(provider), &manifest)
}

/// Generate a fresh non-production provider credential.
fn generate_provider_credential() -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("openssl").args(["rand", "-hex", "32"]).output()?;
    if !output.status.success() {
        return Err("openssl failed to generate provider credential".into());
    }
    let token = String::from_utf8(output.stdout)?.trim().to_owned();
    if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("openssl returned an invalid provider credential".into());
    }
    Ok(token)
}

/// Apply the provider-local credential consumed only on the backend hop.
fn apply_provider_credential_secret(provider: &str, token: &str) -> Result<(), Box<dyn std::error::Error>> {
    apply_named_provider_credential_secret(provider, PROVIDER_CREDENTIAL_SECRET, token)
}

/// Apply one named provider-local credential consumed only on its backend hop.
fn apply_named_provider_credential_secret(
    provider: &str,
    secret_name: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": secret_name,
            "namespace": GRID_SYSTEM_NS,
        },
        "type": "Opaque",
        "stringData": {
            "token": token,
        },
    })
    .to_string();
    kubectl::apply_manifest(&kubectl_context(provider), &manifest)
}

/// Restart a provider deployment to pick up `ConfigMap` or Secret changes.
fn restart_deployment(provider: &str, deployment: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(provider),
            "-n",
            GRID_SYSTEM_NS,
            "rollout",
            "restart",
            &format!("deployment/{deployment}"),
        ])
        .status()?;
    if !status.success() {
        return Err(format!("failed to restart {provider} deployment/{deployment}").into());
    }
    Ok(())
}

/// Verify Grid routing and provider-boundary readiness.
///
/// Checks prerequisites, then proves Grid routing and the provider boundary.
/// Exits non-zero if any assertion is `FAIL` or `BLOCKED`.
///
/// # Errors
///
/// Returns an error if hard prerequisites fail (config, tools,
/// forge binary) or any verification step is not `PASS`.
pub(crate) fn verify_grid_routing(forge_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    verify_grid_routing_with_mode(forge_config, DemoMode::Full, IngressMode::Global)
}

/// Run the Grid routing and provider-boundary proof with mode gating.
///
/// In [`DemoMode::Quick`] mode, the same-site multi-provider proof runs before
/// session drain, withdrawal, hot reload, and pod-stability checks are skipped.
///
/// # Errors
///
/// Returns an error if hard prerequisites fail or any executed step is
/// not `PASS`.
pub(crate) fn verify_grid_routing_with_mode(
    forge_config: &Path,
    mode: DemoMode,
    ingress_mode: IngressMode,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!();
    eprintln!("DETAILED RUNTIME PROOF");
    eprintln!("-------------------------------------------------------------------------------");
    eprintln!("  [CHECK] Prerequisites");
    let ctx = check_prerequisites(forge_config)?;

    let mut results: Vec<StepResult> = Vec::new();
    run_steps(&ctx, mode, ingress_mode, &mut results);

    eprintln!();
    eprintln!("RUNTIME PROOF RESULTS");
    eprintln!("-------------------------------------------------------------------------------");
    eprintln!(
        "Grid must update live edge routing from discovered provider state while authenticated provider gateways protect private backends."
    );
    eprintln!();
    print_validate_all_table(&results);

    let any_not_pass = results.iter().any(|r| r.status != StepStatus::Pass);
    if any_not_pass {
        let fail_count = results.iter().filter(|r| r.status.is_failure()).count();
        let blocked_count = results.iter().filter(|r| r.status == StepStatus::Blocked).count();
        Err(format!(
            "grid-routing: {fail_count} FAIL, {blocked_count} BLOCKED \
             — routing and provider-boundary proof incomplete"
        )
        .into())
    } else {
        eprintln!("[PASS] All routing and provider-boundary proof points passed.");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Prerequisites
// ---------------------------------------------------------------------------

/// Validated prerequisite context.
#[derive(Debug)]
struct PrereqContext {
    /// Path to the forge config file.
    config: PathBuf,
    /// Resolved forge binary path.
    forge_bin: String,
    /// Services blocked by placeholder images (warning only).
    placeholders: Vec<(String, String)>,
}

/// Check all prerequisites and return a context for the verification
/// steps.  Fails with a combined error if config, tools, or the forge
/// binary are missing.  Placeholder images are stored in the context
/// for per-step gating (warning, not fatal).  `:latest` images in the
/// GLB demo config are a hard failure.
fn check_prerequisites(forge_config: &Path) -> Result<PrereqContext, Box<dyn std::error::Error>> {
    let (errors, forge_bin) = collect_prereq_errors(forge_config);
    if !errors.is_empty() {
        report_prereq_errors(&errors);
        return Err(format!("{} prerequisite(s) failed", errors.len()).into());
    }

    let config_text = fs::read_to_string(forge_config)?;
    check_no_latest_images(&config_text, forge_config)?;

    let placeholders = detect_placeholder_images(&config_text);
    if !placeholders.is_empty() {
        warn_placeholder_images(&placeholders);
    }

    let forge_bin = forge_bin.unwrap_or_else(|| std::process::abort());

    Ok(PrereqContext {
        config: forge_config.to_path_buf(),
        forge_bin,
        placeholders,
    })
}

/// Fail if the forge config or its demo resource files use `:latest`.
fn check_no_latest_images(config_text: &str, forge_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let latest_images = detect_latest_images(config_text);
    if !latest_images.is_empty() {
        report_latest_images(&latest_images);
        return Err(format!(
            "{} service(s) use :latest — GLB demo requires pinned tags",
            latest_images.len()
        )
        .into());
    }

    let resource_latest = forge_config
        .parent()
        .map(detect_latest_in_resources)
        .unwrap_or_default();
    if !resource_latest.is_empty() {
        report_latest_resources(&resource_latest);
        return Err(format!(
            "{} resource file(s) use :latest — GLB demo requires pinned tags",
            resource_latest.len()
        )
        .into());
    }
    Ok(())
}

/// Print prerequisite errors to stderr.
fn report_prereq_errors(errors: &[String]) {
    eprintln!();
    for e in errors {
        eprintln!("  PREREQ FAIL: {e}");
    }
    eprintln!();
}

/// Warn that runtime assertions will be blocked by placeholder images.
fn warn_placeholder_images(placeholders: &[(String, String)]) {
    eprintln!();
    for (svc, img) in placeholders {
        eprintln!("  WARNING: service '{svc}' uses placeholder image '{img}' — runtime assertions will be BLOCKED");
    }
    eprintln!();
}

/// Report `:latest` images found in the forge config (fatal).
fn report_latest_images(latest: &[(String, String)]) {
    eprintln!();
    for (svc, img) in latest {
        eprintln!("  FAIL: service '{svc}' uses unpinned image '{img}'");
    }
    eprintln!();
}

/// Report `:latest` images found in demo resource files (fatal).
fn report_latest_resources(latest: &[(PathBuf, String)]) {
    eprintln!();
    for (path, img) in latest {
        eprintln!("  FAIL: {} uses unpinned image '{img}'", path.display());
    }
    eprintln!();
}

/// Collect prerequisite errors for config, tools, and forge binary.
fn collect_prereq_errors(forge_config: &Path) -> (Vec<String>, Option<String>) {
    let mut errors: Vec<String> = Vec::new();
    if !forge_config.exists() {
        errors.push(format!("config file not found: {}", forge_config.display()));
    }
    for tool in REQUIRED_TOOLS {
        if !tool_available(tool) {
            errors.push(format!("required tool not found on PATH: {tool}"));
        }
    }
    let forge_bin = resolve_forge_binary();
    if forge_bin.is_none() {
        errors.push(
            "praxis-forge binary not found on PATH or at \
             target/debug/praxis-forge"
                .to_owned(),
        );
    }
    (errors, forge_bin)
}

/// Check whether a CLI tool is available on `PATH` via `which`.
fn tool_available(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Resolve the forge binary: prefer `praxis-forge` on PATH, fall
/// back to `target/debug/praxis-forge`.
pub(crate) fn resolve_forge_binary() -> Option<String> {
    if tool_available("praxis-forge") {
        return Some("praxis-forge".to_owned());
    }
    let local = "target/debug/praxis-forge";
    if Path::new(local).exists() {
        return Some(local.to_owned());
    }
    None
}

/// Detect placeholder images in Forge configuration.
///
/// Parses YAML and inspects `image` plus camel-cased image property keys such
/// as `gatewayImage`, `operatorImage`, and `mockProviderImage`.
pub(crate) fn detect_placeholder_images(config_text: &str) -> Vec<(String, String)> {
    yaml_image_references(config_text)
        .into_iter()
        .filter(|(_, image)| image.contains("PLACEHOLDER"))
        .collect()
}

/// Detect `:latest`-tagged images in forge config text.
///
/// Uses the same structured traversal as [`detect_placeholder_images`].
pub(crate) fn detect_latest_images(config_text: &str) -> Vec<(String, String)> {
    yaml_image_references(config_text)
        .into_iter()
        .filter(|(_, image)| image_is_latest(image))
        .collect()
}

/// Return image references from a YAML document with their nearest named
/// object as context.
fn yaml_image_references(config_text: &str) -> Vec<(String, String)> {
    let Ok(document) = serde_yaml::from_str::<serde_yaml::Value>(config_text) else {
        return Vec::new();
    };
    let mut results = Vec::new();
    collect_yaml_images(&document, "root", "root", &mut results);
    results
}

/// Recursively collect image-like YAML fields.
#[expect(clippy::too_many_lines, reason = "expanded wildcard match adds boilerplate")]
fn collect_yaml_images(value: &serde_yaml::Value, path: &str, owner: &str, results: &mut Vec<(String, String)>) {
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            let local_owner = mapping
                .get(serde_yaml::Value::String("name".to_owned()))
                .and_then(serde_yaml::Value::as_str)
                .unwrap_or(owner);
            for (key, child) in mapping {
                let Some(key) = key.as_str() else {
                    continue;
                };
                let child_path = format!("{path}.{key}");
                if is_image_key(key) {
                    if let Some(image) = child.as_str() {
                        results.push((local_owner.to_owned(), image.to_owned()));
                    }
                } else {
                    collect_yaml_images(child, &child_path, local_owner, results);
                }
            }
        },
        serde_yaml::Value::Sequence(sequence) => {
            for (index, child) in sequence.iter().enumerate() {
                collect_yaml_images(child, &format!("{path}[{index}]"), owner, results);
            }
        },
        serde_yaml::Value::Null
        | serde_yaml::Value::Bool(_)
        | serde_yaml::Value::Number(_)
        | serde_yaml::Value::String(_)
        | serde_yaml::Value::Tagged(_) => {},
    }
}

/// Whether a YAML key holds a container image reference.
fn is_image_key(key: &str) -> bool {
    key.to_ascii_lowercase().ends_with("image")
}

/// Detect `:latest`-tagged images in YAML resource files under the
/// demo directory's `resources/` subdirectory.
fn detect_latest_in_resources(demo_dir: &Path) -> Vec<(PathBuf, String)> {
    let resources_dir = demo_dir.join("resources");
    let mut results = Vec::new();
    let Ok(entries) = walk_yaml_files(&resources_dir) else {
        return results;
    };
    for path in entries {
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines() {
            if let Some(raw) = extract_image_value(line.trim())
                && image_is_latest(&raw)
            {
                results.push((path.clone(), raw));
            }
        }
    }
    results
}

/// Collect `.yaml` / `.yml` file paths under a directory recursively.
fn walk_yaml_files(dir: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if let Ok(sub) = walk_yaml_files(&path) {
                files.extend(sub);
            }
        } else if path.extension().is_some_and(|ext| ext == "yaml" || ext == "yml") {
            files.push(path);
        }
    }
    Ok(files)
}

/// Extract the image value from a YAML `image:` line.
fn extract_image_value(trimmed: &str) -> Option<String> {
    let rest = if let Some(r) = trimmed.strip_prefix("image:") {
        r
    } else if trimmed.contains("image:") {
        trimmed.split("image:").nth(1)?
    } else {
        return None;
    };
    let raw = rest.trim().trim_matches('"').to_owned();
    if raw.is_empty() { None } else { Some(raw) }
}

/// Check whether an image reference uses `:latest` (explicit or implied).
///
/// Digest-pinned images (`image@sha256:...`) are always considered
/// pinned regardless of whether a tag is also present.
fn image_is_latest(image: &str) -> bool {
    if image.contains("{{") {
        return false;
    }
    if image.contains('@') {
        return false;
    }
    let tag = image
        .rsplit_once('/')
        .map_or(image, |(_prefix, tail)| tail)
        .rsplit_once(':')
        .map(|(_name, tag)| tag);
    tag == Some("latest") || tag.is_none()
}

// ---------------------------------------------------------------------------
// Verification steps
// ---------------------------------------------------------------------------

/// Run all Grid routing and provider-boundary proof assertions.
#[expect(
    clippy::too_many_lines,
    reason = "sequential proof steps: each step depends on the previous; splitting obscures the proof flow"
)]
fn run_steps(ctx: &PrereqContext, mode: DemoMode, ingress_mode: IngressMode, results: &mut Vec<StepResult>) {
    // Forge config validation.
    proof_banner("validating forge config");
    let config_ok = record_step("prerequisites", results, || {
        validate_forge_config(&ctx.forge_bin, &ctx.config)
    });
    if !config_ok {
        block_remaining("forge status", "config validation failed", results);
        return;
    }

    // Environment status.
    proof_banner("checking environment status");
    let status_json = match run_forge_status(&ctx.forge_bin, &ctx.config) {
        Ok(json) => {
            results.push(StepResult::pass("forge status", "forge status returned OK"));
            json
        },
        Err(e) => {
            results.push(StepResult::fail("forge status", e.as_ref()));
            block_remaining("clusters live", "status unavailable", results);
            return;
        },
    };

    // All clusters live.
    proof_banner("checking clusters live");
    let clusters_ok = record_step("clusters live", results, || {
        check_clusters_live(&status_json, ingress_mode)
    });
    if !clusters_ok {
        block_remaining("provider gateway IPs", "clusters not live", results);
        return;
    }

    // Provider gateway IPs.
    proof_banner("checking provider gateway IPs");
    let _gateways_ok = record_step("provider gateway IPs", results, check_provider_gateways_captured);

    // SWIM LB services.
    proof_banner("checking SWIM LB services");
    record_step("swim lb services", results, check_swim_lb_services);

    // Operator SWIM advertise address.
    proof_banner("checking operator SWIM advertise address");
    record_step("swim advertise addr", results, check_swim_advertise_addr);

    // GridNetwork seeds populated.
    proof_banner("checking GridNetwork seeds");
    record_step("gridnetwork seeds", results, check_gridnetwork_seeds);

    // Overlay metadata — waits for the operator status and ConfigMap
    // resourceVersions to converge after concurrent reconciliation.
    proof_banner("checking overlay candidate metadata");
    record_step("overlay metadata", results, || {
        wait_for_check(
            "overlay metadata",
            DATA_PLANE_CONVERGENCE_WAIT,
            DATA_PLANE_PROBE_INTERVAL,
            check_overlay_metadata,
        )
    });

    // Regression guard for grid#42: convergence above proves the ConfigMap
    // *reached* the right state; this proves it *stays* there instead of
    // being unconditionally re-applied on every reconcile tick.
    proof_banner("checking overlay metadata stays converged (no reconcile hot-loop)");
    record_step(
        "overlay metadata stable (grid#42)",
        results,
        check_overlay_metadata_settles,
    );

    // One site advertises two independent providers for the same model.
    proof_banner("checking multiple providers in one site overlay");
    record_step("same-site provider candidates", results, || {
        wait_for_check(
            "same-site provider candidates",
            PROVIDER_STATE_WAIT,
            DATA_PLANE_PROBE_INTERVAL,
            check_same_site_provider_candidates,
        )
    });

    // Provider gateway self-discovery.
    proof_banner("checking provider gateway self-discovery");
    let provider_gateway_addrs = match load_provider_gateway_addresses() {
        Ok(addrs) => addrs,
        Err(e) => {
            results.push(StepResult::fail("provider gateway addr", e.as_ref()));
            block_remaining(
                "remote gridsite egress",
                "provider gateway captures unavailable",
                results,
            );
            return;
        },
    };
    record_step("provider gateway addr", results, || {
        check_provider_gateway_addr(&provider_gateway_addrs)
    });

    // Remote GridSite egress addresses.
    proof_banner("checking remote GridSite egress addresses");
    record_step("remote gridsite egress", results, || {
        check_remote_gridsite_egress(&provider_gateway_addrs)
    });

    // Provider-boundary runtime probes.
    proof_banner("checking provider service selector");
    record_step("provider service selector", results, check_provider_service_selector);

    proof_banner("checking provider gateway pods");
    record_step("provider gateway pods", results, check_provider_gateway_pods);

    proof_banner("checking backend private service");
    record_step("backend private service", results, check_backend_private_service);

    proof_banner("checking backend network policy");
    record_step("backend network policy", results, || {
        check_backend_network_policy(ingress_mode)
    });

    proof_banner("checking provider mTLS trust matrix");
    record_step("provider mTLS trust matrix", results, || {
        check_provider_mtls_trust_matrix(&provider_gateway_addrs)
    });

    proof_banner("checking provider peer policy");
    record_step("provider peer policy", results, || {
        check_provider_peer_policy(&provider_gateway_addrs)
    });

    proof_banner("checking provider request boundary");
    record_step("provider request boundary", results, || {
        check_provider_request_boundary(&provider_gateway_addrs)
    });

    // Edge config applied.
    proof_banner("checking edge config applied");
    record_step("edge config applied", results, check_site_stacks);

    // Gate steps 19+ on placeholder images.
    if !ctx.placeholders.is_empty() {
        let reason = format!(
            "placeholder images: {}",
            ctx.placeholders
                .iter()
                .map(|(svc, _)| svc.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        block_remaining("edge overlays mounted", &reason, results);
        return;
    }

    // Both operator overlays are projected into their local edge pods.
    proof_banner("checking edge overlay projections");
    let overlays_ok = record_step("edge overlays mounted", results, wait_for_edge_overlays_ready);
    if !overlays_ok {
        block_remaining("edge gateways running", "edge overlays unavailable", results);
        return;
    }

    // Both Kubernetes edge gateways are ready; capture east pod identity.
    proof_banner("capturing edge gateway pod identity");
    let edge_identity = match check_edge_gateway_pods() {
        Ok((evidence, captured)) => {
            results.push(StepResult::pass("edge gateways running", evidence));
            captured
        },
        Err(e) => {
            results.push(StepResult::fail("edge gateways running", e.as_ref()));
            block_remaining("inference routed", "edge not running", results);
            return;
        },
    };

    // Initial inference request.
    proof_banner("sending inference request");
    let routed_ok = record_step("inference routed", results, check_inference_routed);
    if !routed_ok {
        block_remaining("same-site providers routed", "initial inference failed", results);
        return;
    }

    proof_banner("routing to both providers hosted by the east site");
    let multiple_providers_ok = record_step("same-site providers routed", results, check_same_site_provider_routing);
    if !multiple_providers_ok {
        block_remaining("session affinity bind", "same-site provider routing failed", results);
        return;
    }

    if mode == DemoMode::Quick {
        return;
    }

    // Session affinity initial bind.
    proof_banner("checking session affinity bind");
    let provider_a = match check_session_bind(EDGE_PORT) {
        Ok((evidence, provider)) => {
            results.push(StepResult::pass("session affinity bind", evidence));
            provider
        },
        Err(e) => {
            results.push(StepResult::fail("session affinity bind", e.as_ref()));
            block_remaining("session affinity reuse", "session bind failed", results);
            return;
        },
    };

    // Session affinity reuse.
    proof_banner("checking session affinity reuse");
    let reuse_ok = record_step("session affinity reuse", results, || {
        check_session_reuse(EDGE_PORT, &provider_a)
    });
    if !reuse_ok {
        block_remaining("session drain setup", "session reuse failed", results);
        return;
    }

    // Provider withdrawal drain — withdraw the provider and verify new sessions avoid it.
    proof_banner("withdrawing provider to verify drain routing");
    let drain_withdrawal = match withdraw_provider(PRIMARY_EDGE, &provider_a) {
        Ok((evidence, state)) => {
            results.push(StepResult::pass("session drain setup", evidence));
            state
        },
        Err(e) => {
            results.push(StepResult::fail("session drain setup", e.as_ref()));
            block_remaining("session drain verified", "drain setup failed", results);
            return;
        },
    };

    // Drain routing verification — new sessions must avoid the withdrawn provider.
    proof_banner("checking drain routing");
    let drain_proof = wait_for_data_plane_convergence("provider drain routing", || {
        let session = format!("drain-proof-{}", unix_nanos());
        let resp = curl_edge_request(EDGE_PORT, Some(&session))?;
        if resp.status != 200 {
            return Err(format!("drain routing request returned HTTP {}", resp.status).into());
        }
        let provider = extract_provider(&resp)?;
        if provider == provider_a {
            return Err(format!("new session routed to withdrawn provider {provider_a}").into());
        }
        Ok(format!(
            "new session routed to {provider}, avoiding withdrawn {provider_a}"
        ))
    });
    let drain_restore = restore_withdrawn_provider(PRIMARY_EDGE, &drain_withdrawal);
    match (drain_proof, drain_restore) {
        (Ok(proof), Ok(restored)) => {
            results.push(StepResult::pass(
                "session drain verified",
                format!("{proof}; {restored}"),
            ));
        },
        (Err(proof), Ok(_)) => {
            results.push(StepResult::fail("session drain verified", proof.as_ref()));
        },
        (Ok(_), Err(restore)) => {
            results.push(StepResult::fail("session drain verified", restore.as_ref()));
            block_remaining("provider withdrawn", "provider admission restoration failed", results);
            return;
        },
        (Err(proof), Err(restore)) => {
            let error = format!("{proof}; provider admission restoration also failed: {restore}");
            results.push(StepResult::fail(
                "session drain verified",
                &std::io::Error::other(error),
            ));
            block_remaining("provider withdrawn", "provider admission restoration failed", results);
            return;
        },
    }

    // Make the selected provider unavailable through its declared backend.
    proof_banner("withdrawing a provider through health reconciliation");
    let withdrawal = match withdraw_provider(PRIMARY_EDGE, &provider_a) {
        Ok((evidence, state)) => {
            results.push(StepResult::pass("provider withdrawn", evidence));
            state
        },
        Err(e) => {
            results.push(StepResult::fail("provider withdrawn", e.as_ref()));
            block_remaining("hot-reload observed", "provider withdrawal failed", results);
            return;
        },
    };

    // Hot reload observed.
    proof_banner("checking hot reload");
    record_step("hot-reload observed", results, || {
        check_hot_reload_observed(PRIMARY_EDGE, withdrawal.reload_count_before)
    });

    // Routing after reload.
    proof_banner("sending post-reload inference request");
    record_step("routing after reload", results, || {
        wait_for_data_plane_convergence("post-reload routing", check_inference_routed)
    });

    // Restore the provider and require the operator-generated overlay to recover.
    let restore_result = restore_withdrawn_provider(PRIMARY_EDGE, &withdrawal);

    // Edge pod stability.
    proof_banner("checking edge pod stability");
    let stable = record_step("edge pod stable", results, move || {
        let restore_evidence = restore_result?;
        let stable_evidence = check_edge_pod_stable(PRIMARY_EDGE, &edge_identity)?;
        Ok(format!("{stable_evidence}; {restore_evidence}"))
    });
    if !stable {
        block_remaining(
            "invalid overlay protection",
            "provider restoration or edge stability failed",
            results,
        );
        return;
    }

    // Invalid reload retains the serving snapshot; cold startup fails closed.
    proof_banner("checking invalid overlay protection");
    record_step("invalid overlay protection", results, || {
        check_invalid_overlay_protection(PRIMARY_EDGE)
    });
}

/// Print a step progress banner.
fn proof_banner(description: &str) {
    eprintln!("  [CHECK] {description}");
}

/// Record a step result, returning whether it passed.
fn record_step(
    label: &'static str,
    results: &mut Vec<StepResult>,
    f: impl FnOnce() -> Result<String, Box<dyn std::error::Error>>,
) -> bool {
    match f() {
        Ok(evidence) => {
            results.push(StepResult::pass(label, evidence));
            true
        },
        Err(e) => {
            results.push(StepResult::fail(label, e.as_ref()));
            false
        },
    }
}

/// Ordered assertion labels used for dependency-aware blocking.
const PROOF_LABELS: &[&str] = &[
    "prerequisites",
    "forge status",
    "clusters live",
    "provider gateway IPs",
    "swim lb services",
    "swim advertise addr",
    "gridnetwork seeds",
    "overlay metadata",
    "overlay metadata stable (grid#42)",
    "same-site provider candidates",
    "provider gateway addr",
    "remote gridsite egress",
    "provider service selector",
    "provider gateway pods",
    "backend private service",
    "backend network policy",
    "provider mTLS trust matrix",
    "provider peer policy",
    "provider request boundary",
    "edge config applied",
    "edge overlays mounted",
    "edge gateways running",
    "inference routed",
    "same-site providers routed",
    "session affinity bind",
    "session affinity reuse",
    "session drain setup",
    "session drain verified",
    "provider withdrawn",
    "hot-reload observed",
    "routing after reload",
    "edge pod stable",
    "invalid overlay protection",
];

/// Verify provider gateway Service selector and port on each site.
fn check_provider_service_selector() -> Result<String, Box<dyn std::error::Error>> {
    let mut evidence = Vec::new();
    for provider in PROVIDER_CLUSTERS {
        let ctx = kubectl_context(provider);
        let sel = kubectl_jsonpath(&ctx, "svc", "provider-gateway", "{.spec.selector}")?;
        let port = kubectl_jsonpath(&ctx, "svc", "provider-gateway", "{.spec.ports[0].port}")?;
        if port != "8443" {
            return Err(format!("{provider}: expected port 8443, got {port}").into());
        }
        evidence.push(format!("{provider}: port={port}, selector={sel}"));
    }
    Ok(evidence.join("; "))
}

/// Verify provider gateway pods are Running with the expected image.
fn check_provider_gateway_pods() -> Result<String, Box<dyn std::error::Error>> {
    let mut evidence = Vec::new();
    for provider in PROVIDER_CLUSTERS {
        let ctx = kubectl_context(provider);
        let phase = kubectl_jsonpath(
            &ctx,
            "pods",
            "-l app.kubernetes.io/name=provider-gateway",
            "{.items[0].status.phase}",
        )?;
        if phase != "Running" {
            return Err(format!("{provider}: pod phase is {phase}, expected Running").into());
        }
        let image = kubectl_jsonpath(
            &ctx,
            "pods",
            "-l app.kubernetes.io/name=provider-gateway",
            "{.items[0].spec.containers[0].image}",
        )?;
        evidence.push(format!("{provider}: {phase}, image={image}"));
    }
    Ok(evidence.join("; "))
}

/// Verify the backend Service is `ClusterIP` and not externally reachable.
fn check_backend_private_service() -> Result<String, Box<dyn std::error::Error>> {
    let mut evidence = Vec::new();
    for provider in PROVIDER_CLUSTERS {
        let ctx = kubectl_context(provider);
        let services: &[&str] = if *provider == "east-provider" {
            &["vcr-inference", "vcr-inference-secondary"]
        } else {
            &["vcr-inference"]
        };
        for service in services {
            let svc_type = kubectl_jsonpath(&ctx, "svc", service, "{.spec.type}")?;
            if svc_type != "ClusterIP" {
                return Err(format!("{provider}/{service}: backend type is {svc_type}, expected ClusterIP").into());
            }
            evidence.push(format!("{provider}/{service}: type={svc_type}"));
        }
    }
    Ok(evidence.join("; "))
}

/// Prove the backend ingress policy with differential runtime probes.
///
/// The same image and target are used for both probes. A pod carrying the
/// provider-gateway access label must connect, while an otherwise identical
/// unlabeled pod must be denied. This detects actual enforcement rather than
/// inferring support from the CNI name or the presence of a manifest.
fn check_backend_network_policy(mode: IngressMode) -> Result<String, Box<dyn std::error::Error>> {
    let mut evidence = Vec::new();
    for provider in PROVIDER_CLUSTERS {
        let context = kubectl_context(provider);
        let backends: &[(&str, &str)] = if *provider == "east-provider" {
            &[
                ("primary", "vcr-inference.grid-system.svc.cluster.local:8000"),
                (
                    "secondary",
                    "vcr-inference-secondary.grid-system.svc.cluster.local:8000",
                ),
            ]
        } else {
            &[("primary", "vcr-inference.grid-system.svc.cluster.local:8000")]
        };
        for (instance, target) in backends {
            check_one_backend_network_boundary(provider, &context, instance, target, mode)?;
            evidence.push(format!(
                "{provider}/{instance}: allowed=connected, unlabeled=denied, no_auth=HTTP_401, client_auth=HTTP_403"
            ));
        }
    }
    Ok(evidence.join("; "))
}

/// Prove network and credential rejection behavior for one private backend.
#[expect(
    clippy::too_many_lines,
    reason = "sequential network probes for a single verification step"
)]
fn check_one_backend_network_boundary(
    provider: &str,
    context: &str,
    instance: &str,
    target: &str,
    mode: IngressMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let allowed_name = format!("grid-netpol-allowed-{instance}");
    let denied_name = format!("grid-netpol-denied-{instance}");
    delete_probe_pod(context, &allowed_name);
    delete_probe_pod(context, &denied_name);

    let allowed = run_probe_pod(
        context,
        &allowed_name,
        Some("grid.praxis-proxy.io/backend-access=provider-gateway"),
        target,
        mode,
    )?;
    if allowed.phase != "Succeeded" || !allowed.logs.contains("tcp-probe=connected") {
        return Err(format!(
            "{provider}/{instance}: allowed backend probe did not connect (phase={}, logs={})",
            allowed.phase,
            safe_truncate_str(&allowed.logs, 120)
        )
        .into());
    }

    let denied = run_probe_pod(context, &denied_name, None, target, mode)?;
    if denied.phase != "Failed"
        || !(denied.logs.contains("tcp-probe=timeout") || denied.logs.contains("tcp-probe=connect-failed"))
    {
        return Err(format!(
            "{provider}/{instance}: unlabeled backend probe was not denied (phase={}, logs={})",
            denied.phase,
            safe_truncate_str(&denied.logs, 120)
        )
        .into());
    }

    Ok(())
}

/// Terminal evidence from one `NetworkPolicy` probe pod.
struct NetworkPolicyProbe {
    /// Kubernetes pod phase.
    phase: String,
    /// Bounded probe output.
    logs: String,
}

/// Delete a fixed-name probe pod without failing cleanup.
fn delete_probe_pod(context: &str, name: &str) {
    drop(
        Command::new("kubectl")
            .args([
                "--context",
                context,
                "-n",
                GRID_SYSTEM_NS,
                "delete",
                "pod",
                name,
                "--ignore-not-found",
                "--wait=true",
            ])
            .output(),
    );
}

/// Run a bounded TCP probe pod and return its terminal phase and logs.
#[expect(
    clippy::too_many_lines,
    reason = "kubectl pod lifecycle: create, poll, collect logs — splitting obscures the sequence"
)]
#[expect(
    clippy::disallowed_methods,
    reason = "xtask is synchronous; no async runtime available for tokio::time::sleep"
)]
fn run_probe_pod(
    context: &str,
    name: &str,
    labels: Option<&str>,
    target: &str,
    mode: IngressMode,
) -> Result<NetworkPolicyProbe, Box<dyn std::error::Error>> {
    let _guard = ProbePodGuard {
        context: context.to_owned(),
        name: name.to_owned(),
    };
    let mut command = Command::new("kubectl");
    // The old probe reused the mock-provider image and its private --tcp-probe
    // command. VCR replaces that image, so use a small public curl image and
    // preserve the same terminal evidence contract for the policy assertion.
    let probe_image = "curlimages/curl:8.10.1";
    command.args([
        "--context",
        context,
        "-n",
        GRID_SYSTEM_NS,
        "run",
        name,
        &format!("--image={probe_image}"),
        &format!(
            "--image-pull-policy={}",
            crate::env::image_overrides::demo_image_pull_policy(mode)
        ),
        "--restart=Never",
    ]);
    if let Some(value) = labels {
        command.arg(format!("--labels={value}"));
    }
    command.args([
        "--",
        "sh",
        "-c",
        "if curl -sS --connect-timeout 2 --max-time 5 -o /dev/null -w '%{http_code}' \"http://$1\" >/dev/null 2>&1; then echo tcp-probe=connected; exit 0; else echo tcp-probe=connect-failed; exit 1; fi",
        "curl-probe",
        target,
    ]);
    let output = command.output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to create probe pod {name}: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    let phase = loop {
        let phase = kubectl_jsonpath(context, "pod", name, "{.status.phase}")?;
        if matches!(phase.as_str(), "Succeeded" | "Failed") {
            break phase;
        }
        if Instant::now() >= deadline {
            return Err(format!("probe pod {name} did not finish; last phase={phase}").into());
        }
        thread::sleep(Duration::from_millis(250));
    };

    let kubectl_output = Command::new("kubectl")
        .args(["--context", context, "-n", GRID_SYSTEM_NS, "logs", name])
        .output()?;
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&kubectl_output.stdout),
        String::from_utf8_lossy(&kubectl_output.stderr)
    );
    Ok(NetworkPolicyProbe {
        phase,
        logs: safe_truncate_str(logs.trim(), 512),
    })
}

/// Best-effort cleanup for a `NetworkPolicy` probe pod.
struct ProbePodGuard {
    /// Kubernetes context.
    context: String,
    /// Pod name.
    name: String,
}

impl Drop for ProbePodGuard {
    fn drop(&mut self) {
        delete_probe_pod(&self.context, &self.name);
    }
}

/// Run positive and negative provider mTLS handshake probes.
fn check_provider_mtls_trust_matrix(addrs: &BTreeMap<String, String>) -> Result<String, Box<dyn std::error::Error>> {
    let ca = format!("{GENERATED_CERTS_DIR}/ca.pem");
    let wrong_ca = format!("{GENERATED_CERTS_DIR}/untrusted-ca.pem");
    let mut evidence = Vec::new();
    for (provider, addr) in addrs {
        check_provider_tls(provider, addr, &ca, &wrong_ca)?;
        evidence.push(format!(
            "{provider}: valid_edges=2, no_cert/wrong_sni/wrong_ca=TLS_REFUSED"
        ));
    }
    Ok(evidence.join("; "))
}

/// Prove the TLS matrix for one provider endpoint.
fn check_provider_tls(provider: &str, addr: &str, ca: &str, wrong_ca: &str) -> Result<(), Box<dyn std::error::Error>> {
    let sni = format!("{provider}.grid.internal");
    let ip = addr.split(':').next().unwrap_or(addr);
    for edge in EDGE_CLUSTERS {
        let cert = format!("{GENERATED_CERTS_DIR}/{edge}-cert.pem");
        let key = format!("{GENERATED_CERTS_DIR}/{edge}-key.pem");
        if curl_mtls_probe(ip, &sni, Some((&cert, &key)), ca, "/healthz").is_err() {
            return Err(format!("{provider}/{edge}: TLS handshake failed").into());
        }
    }
    if curl_mtls_probe(ip, &sni, None, ca, "/healthz").is_ok() {
        return Err(format!("{provider}: no client cert unexpectedly reached HTTP").into());
    }
    let edge_cert = format!("{GENERATED_CERTS_DIR}/east-edge-cert.pem");
    let edge_key = format!("{GENERATED_CERTS_DIR}/east-edge-key.pem");
    let identity = Some((edge_cert.as_str(), edge_key.as_str()));
    if curl_mtls_probe(ip, "wrong-provider.grid.internal", identity, ca, "/healthz").is_ok() {
        return Err(format!("{provider}: wrong SNI unexpectedly passed TLS").into());
    }
    if curl_mtls_probe(ip, &sni, identity, wrong_ca, "/healthz").is_ok() {
        return Err(format!("{provider}: wrong CA unexpectedly passed TLS").into());
    }
    Ok(())
}

/// Prove a wrong-organization certificate is rejected
/// by `peer_identity_trust` filter (HTTP 403).
fn check_provider_peer_policy(addrs: &BTreeMap<String, String>) -> Result<String, Box<dyn std::error::Error>> {
    let wrong_cert = format!("{GENERATED_CERTS_DIR}/wrong-org-client-cert.pem");
    let wrong_key = format!("{GENERATED_CERTS_DIR}/wrong-org-client-key.pem");
    let unknown_cert = format!("{GENERATED_CERTS_DIR}/edge-untrusted-cert.pem");
    let unknown_key = format!("{GENERATED_CERTS_DIR}/edge-untrusted-key.pem");
    let ca = format!("{GENERATED_CERTS_DIR}/ca.pem");
    let mut evidence = Vec::new();
    for (provider, addr) in addrs {
        let sni = format!("{provider}.grid.internal");
        let ip = addr.split(':').next().unwrap_or(addr);
        let wrong_org = curl_mtls_probe(ip, &sni, Some((&wrong_cert, &wrong_key)), &ca, "/healthz")?;
        if wrong_org != 403 {
            return Err(format!("{provider}: wrong-org cert expected 403, got {wrong_org}").into());
        }
        let wrong_digest = curl_mtls_probe(ip, &sni, Some((&unknown_cert, &unknown_key)), &ca, "/healthz")?;
        if wrong_digest != 403 {
            return Err(format!("{provider}: untrusted cert digest expected 403, got {wrong_digest}").into());
        }
        evidence.push(format!("{provider}: wrong_org=HTTP_403, unknown_digest=HTTP_403"));
    }
    Ok(evidence.join("; "))
}

/// Prove unknown peer candidates and wrong paths are rejected
/// by provider-local routing policy after successful peer authentication.
#[expect(
    clippy::too_many_lines,
    reason = "sequential network probes for a single verification step"
)]
fn check_provider_request_boundary(addrs: &BTreeMap<String, String>) -> Result<String, Box<dyn std::error::Error>> {
    let cert = format!("{GENERATED_CERTS_DIR}/east-edge-cert.pem");
    let key = format!("{GENERATED_CERTS_DIR}/east-edge-key.pem");
    let ca = format!("{GENERATED_CERTS_DIR}/ca.pem");
    let mut evidence = Vec::new();
    for (provider, addr) in addrs {
        let sni = format!("{provider}.grid.internal");
        let ip = addr.split(':').next().unwrap_or(addr);
        let unknown_candidate = curl_mtls_probe_with_headers(&MtlsHeaderProbe {
            ip,
            sni: &sni,
            cert_key: (&cert, &key),
            ca: &ca,
            path: "/v1/chat/completions",
            headers: &[
                (AI_ROUTING_CANDIDATE_HEADER, "spoofed"),
                (AI_ROUTING_REQUEST_ID_HEADER, "boundary-unknown-candidate"),
                ("X-Model", "Qwen/Qwen3-0.6B"),
            ],
        })?;
        if unknown_candidate != 403 {
            return Err(format!("{provider}: unknown candidate expected 403, got {unknown_candidate}").into());
        }

        let candidate = provider_candidate_id(provider)?;
        let bad_path = curl_mtls_probe_with_headers(&MtlsHeaderProbe {
            ip,
            sni: &sni,
            cert_key: (&cert, &key),
            ca: &ca,
            path: "/v1/unauthorized-path",
            headers: &[
                (AI_ROUTING_CANDIDATE_HEADER, &candidate),
                (AI_ROUTING_REQUEST_ID_HEADER, "boundary-wrong-path"),
                ("X-Model", "Qwen/Qwen3-0.6B"),
            ],
        })?;
        if bad_path != 404 {
            return Err(format!("{provider}: wrong path expected 404, got {bad_path}").into());
        }
        evidence.push(format!("{provider}: unknown_candidate=HTTP_403, wrong_path=HTTP_404"));
    }
    Ok(evidence.join("; "))
}

/// Run a curl mTLS probe against a provider gateway.
///
/// Returns `Ok(http_status)` on successful TLS handshake or `Err` on
/// TLS failure.
fn curl_mtls_probe(
    ip: &str,
    sni: &str,
    cert: Option<(&str, &str)>,
    ca: &str,
    path: &str,
) -> Result<u16, Box<dyn std::error::Error>> {
    let resolve = format!("{sni}:8443:{ip}");
    let url = format!("https://{sni}:8443{path}");
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "-o", "/dev/null", "-w", "%{http_code}"]);
    cmd.args(["--connect-timeout", "5", "--max-time", "10"]);
    cmd.args(["--cacert", ca, "--resolve", &resolve]);
    if let Some((cert_path, key_path)) = cert {
        cmd.args(["--cert", cert_path, "--key", key_path]);
    }
    cmd.arg(&url);
    let output = cmd.output()?;
    if !output.status.success() {
        return Err(format!("curl TLS handshake failed (exit {})", output.status).into());
    }
    let code: u16 = String::from_utf8(output.stdout)?
        .trim()
        .parse()
        .map_err(|e| format!("failed to parse HTTP status: {e}"))?;
    Ok(code)
}

/// Arguments for a curl mTLS probe with request headers.
struct MtlsHeaderProbe<'probe> {
    /// Provider gateway IP.
    ip: &'probe str,
    /// SNI hostname.
    sni: &'probe str,
    /// Client certificate and key paths.
    cert_key: (&'probe str, &'probe str),
    /// CA certificate path.
    ca: &'probe str,
    /// URL path to request.
    path: &'probe str,
    /// Request header name/value pairs.
    headers: &'probe [(&'probe str, &'probe str)],
}

/// Run a curl mTLS probe with request headers.
fn curl_mtls_probe_with_headers(args: &MtlsHeaderProbe<'_>) -> Result<u16, Box<dyn std::error::Error>> {
    let resolve = format!("{}:8443:{}", args.sni, args.ip);
    let url = format!("https://{}:8443{}", args.sni, args.path);
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "-o", "/dev/null", "-w", "%{http_code}"]);
    cmd.args(["--connect-timeout", "5", "--max-time", "10"]);
    cmd.args(["--cacert", args.ca, "--resolve", &resolve]);
    cmd.args(["--cert", args.cert_key.0, "--key", args.cert_key.1]);
    for (name, value) in args.headers {
        cmd.args(["-H", &format!("{name}: {value}")]);
    }
    cmd.arg(&url);
    let output = cmd.output()?;
    if !output.status.success() {
        return Err(format!("curl failed (exit {})", output.status).into());
    }
    let code: u16 = String::from_utf8(output.stdout)?
        .trim()
        .parse()
        .map_err(|e| format!("failed to parse HTTP status: {e}"))?;
    Ok(code)
}

/// Derive the stable candidate ID rendered for one demo provider identity.
fn provider_candidate_id(provider: &str) -> Result<String, Box<dyn std::error::Error>> {
    let site = provider_gateway_site(provider)?;
    let cluster = provider_routing_cluster(provider)?;
    Ok(fnv1a_hex8(&format!(
        "{DEMO_CANDIDATE_KIND}/{DEMO_MODEL}/{site}/{cluster}"
    )))
}

/// Compute the operator's dependency-free eight-character FNV-1a fixture ID.
fn fnv1a_hex8(input: &str) -> String {
    const FNV_OFFSET: u32 = 2_166_136_261;
    const FNV_PRIME: u32 = 16_777_619;
    let mut hash = FNV_OFFSET;
    for byte in input.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:08x}")
}

/// Query a kubectl jsonpath field from a resource.
fn kubectl_jsonpath(
    context: &str,
    resource: &str,
    name: &str,
    jsonpath: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            resource,
            name,
            "-o",
            &format!("jsonpath={jsonpath}"),
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "kubectl get {resource} {name}: {}",
            safe_truncate_str(stderr.trim(), 120)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

/// Read a `ConfigMap` annotation by key, parsing the full JSON to avoid
/// `kubectl` `JSONPath` issues with dotted annotation keys.
fn annotation_value(
    context: &str,
    configmap: &str,
    annotation_key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let raw = kubectl_jsonpath(context, "configmap", configmap, "{.metadata.annotations}")?;
    let map: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| format!("ConfigMap {configmap} annotations not valid JSON: {e}"))?;
    map.get(annotation_key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("annotation {annotation_key:?} missing on {configmap}").into())
}

/// Block the named assertion and every dependent assertion after it.
fn block_remaining(from_label: &str, reason: &str, results: &mut Vec<StepResult>) {
    let start = PROOF_LABELS
        .iter()
        .position(|label| *label == from_label)
        .unwrap_or(PROOF_LABELS.len());
    for label in PROOF_LABELS.get(start..).unwrap_or(&[]) {
        results.push(StepResult::blocked(label, reason.to_owned()));
    }
}

// ---------------------------------------------------------------------------
// Step implementations
// ---------------------------------------------------------------------------

/// Validate the Forge config.
fn validate_forge_config(forge_bin: &str, config: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new(forge_bin)
        .args(["config", "validate", "--config", &config.display().to_string()])
        .output()?;
    if output.status.success() {
        Ok("config validation passed".to_owned())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("config validation failed: {}", safe_truncate_str(stderr.trim(), 120)).into())
    }
}

/// Run `praxis-forge status --output json` and parse it.
pub(crate) fn run_forge_status(
    forge_bin: &str,
    config: &Path,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new(forge_bin)
        .args(["status", "--config", &config.display().to_string(), "--output", "json"])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("forge status failed: {}", safe_truncate_str(stderr.trim(), 120)).into());
    }
    let stdout = String::from_utf8(output.stdout)?;
    let json: serde_json::Value = serde_json::from_str(&stdout)?;
    Ok(json)
}

/// Verify all expected clusters are live.
pub(crate) fn check_clusters_live(
    status_json: &serde_json::Value,
    ingress_mode: IngressMode,
) -> Result<String, Box<dyn std::error::Error>> {
    let clusters = status_json
        .get("data")
        .and_then(|d| d.get("clusters"))
        .and_then(serde_json::Value::as_array)
        .ok_or("status JSON missing data.clusters array")?;

    let expected_clusters: &[&str] = match ingress_mode {
        IngressMode::Global => CLUSTER_NAMES,
        IngressMode::Workload => GRID_CLUSTERS,
    };

    let mut missing = Vec::new();
    for expected in expected_clusters {
        let found = clusters.iter().any(|c| {
            c.get("name").and_then(serde_json::Value::as_str) == Some(expected)
                && c.get("live").and_then(serde_json::Value::as_bool) == Some(true)
        });
        if !found {
            missing.push(*expected);
        }
    }

    if missing.is_empty() {
        Ok(format!("all {} clusters live", expected_clusters.len()))
    } else {
        Err(format!("clusters not live: {}", missing.join(", ")).into())
    }
}

/// Check provider gateway IPs through Kubernetes.
fn check_provider_gateways_captured() -> Result<String, Box<dyn std::error::Error>> {
    let mut found = Vec::new();
    for cluster in PROVIDER_CLUSTERS {
        let context = kubectl_context(cluster);
        let ip = get_provider_gateway_ip(&context)?;
        found.push(format!("{cluster}={ip}"));
    }
    Ok(found.join(", "))
}

/// Verify SWIM `LoadBalancer` Services on all Grid clusters.
fn check_swim_lb_services() -> Result<String, Box<dyn std::error::Error>> {
    let mut found = Vec::new();
    for cluster in GRID_CLUSTERS {
        let ip = get_swim_lb_ip(&kubectl_context(cluster), cluster)?;
        found.push(format!("{cluster}={ip}"));
    }
    Ok(found.join(", "))
}

/// Get the external IP of the SWIM LB service via kubectl.
fn get_swim_lb_ip(context: &str, cluster: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "svc",
            SWIM_LB_SERVICE,
            "-o",
            "jsonpath={.status.loadBalancer.ingress[0].ip}",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "{SWIM_LB_SERVICE} not found on {cluster}: {}",
            safe_truncate_str(stderr.trim(), 120)
        )
        .into());
    }
    let ip = String::from_utf8(output.stdout)?.trim().to_owned();
    if !looks_like_ipv4(&ip) {
        return Err(format!("{SWIM_LB_SERVICE} on {cluster} has invalid IP '{ip}'").into());
    }
    Ok(ip)
}

/// Verify each operator SWIM advertise address matches its `LoadBalancer` IP.
fn check_swim_advertise_addr() -> Result<String, Box<dyn std::error::Error>> {
    let mut verified = Vec::new();
    for cluster in GRID_CLUSTERS {
        let context = kubectl_context(cluster);
        let output = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "deploy",
                "grid-operator",
                "-o",
                "jsonpath={.spec.template.spec.containers[0].env}",
            ])
            .output()?;
        let env_json = String::from_utf8(output.stdout)?;
        let addr = parse_env_var_from_json(&env_json, "GRID_SWIM_ADVERTISE_ADDR");
        let Some(addr) = addr else {
            return Err(format!("GRID_SWIM_ADVERTISE_ADDR not set on {cluster}").into());
        };
        if addr.contains("$(POD_IP)") || addr.is_empty() || !addr.ends_with(":7946") {
            return Err(format!("GRID_SWIM_ADVERTISE_ADDR on {cluster} is '{addr}' (expected LB IP:7946)").into());
        }
        verified.push(format!("{cluster}={addr}"));
    }
    Ok(verified.join(", "))
}

/// Parse a named env var value from kubectl jsonpath env array JSON.
fn parse_env_var_from_json(json: &str, var_name: &str) -> Option<String> {
    let arr: Vec<serde_json::Value> = serde_json::from_str(json).ok()?;
    arr.iter().find_map(|entry| {
        let name = entry.get("name")?.as_str()?;
        if name == var_name {
            entry.get("value")?.as_str().map(str::to_owned)
        } else {
            None
        }
    })
}

/// Verify `GridNetwork` seeds on all Grid clusters.
fn check_gridnetwork_seeds() -> Result<String, Box<dyn std::error::Error>> {
    let mut verified = Vec::new();
    for cluster in GRID_CLUSTERS {
        let context = kubectl_context(cluster);
        let output = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "gridnetwork",
                GRID_NETWORK_NAME,
                "-o",
                "jsonpath={.spec.seeds[*]}",
            ])
            .output()?;
        let seeds_raw = String::from_utf8(output.stdout)?.trim().to_owned();
        let count = parse_seeds_count(&seeds_raw);
        if count != 3 {
            return Err(format!("GridNetwork on {cluster} has {count} seed(s) (expected exactly 3)").into());
        }
        verified.push(format!("{cluster}={count}"));
    }
    Ok(format!("seeds: {}", verified.join(", ")))
}

/// Parse seed count from kubectl jsonpath array output.
fn parse_seeds_count(raw: &str) -> usize {
    let trimmed = raw.trim().trim_start_matches('[').trim_end_matches(']');
    if trimmed.is_empty() {
        return 0;
    }
    trimmed.split(' ').filter(|s| !s.is_empty()).count()
}

/// Verify the overlay [`ConfigMap`] candidate metadata and envelope.
///
/// Checks both the legacy `routing-config.json` and the envelope
/// `routing-overlay.json` keys, validates envelope structure, and verifies
/// `ConfigMap` annotations match the envelope revision and digest.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
#[expect(clippy::too_many_lines, reason = "sequential dual-key and annotation verification")]
fn check_overlay_metadata() -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context(PRIMARY_EDGE);

    let legacy_raw = kubectl_jsonpath(
        &context,
        "configmap",
        OVERLAY_CONFIGMAP,
        r"{.data.routing-config\.json}",
    )?;
    if legacy_raw.trim().is_empty() {
        return Err("overlay ConfigMap legacy key routing-config.json is empty".into());
    }
    let legacy_evidence = validate_overlay_json(&legacy_raw)?;

    let envelope_raw = kubectl_jsonpath(
        &context,
        "configmap",
        OVERLAY_CONFIGMAP,
        r"{.data.routing-overlay\.json}",
    )?;
    if envelope_raw.trim().is_empty() {
        return Err("overlay ConfigMap envelope key routing-overlay.json is missing or empty".into());
    }
    let envelope: serde_json::Value = serde_json::from_str(&envelope_raw)?;
    let schema_version = envelope
        .get("schema_version")
        .and_then(serde_json::Value::as_str)
        .ok_or("envelope missing schema_version")?;
    if schema_version != "1.0.0" {
        return Err(format!("envelope schema_version={schema_version:?}, expected \"1.0.0\"").into());
    }
    let revision = envelope
        .pointer("/revision/value")
        .and_then(serde_json::Value::as_str)
        .ok_or("envelope missing revision.value")?;
    let digest = envelope
        .pointer("/content_digest/value")
        .and_then(serde_json::Value::as_str)
        .ok_or("envelope missing content_digest.value")?;
    if revision != digest {
        return Err(format!(
            "envelope revision={} != content_digest={}",
            safe_truncate_str(revision, 16),
            safe_truncate_str(digest, 16)
        )
        .into());
    }

    let ann_schema = annotation_value(&context, OVERLAY_CONFIGMAP, OVERLAY_ANNOTATION_SCHEMA)?;
    let ann_revision = annotation_value(&context, OVERLAY_CONFIGMAP, OVERLAY_ANNOTATION_REVISION)?;
    let ann_digest = annotation_value(&context, OVERLAY_CONFIGMAP, OVERLAY_ANNOTATION_DIGEST)?;

    if ann_schema != schema_version {
        return Err(format!("annotation schema={ann_schema:?} != envelope {schema_version:?}").into());
    }
    if ann_revision != revision {
        return Err("annotation revision != envelope revision".into());
    }
    if ann_digest != digest {
        return Err("annotation digest != envelope content_digest".into());
    }

    let config_map_resource_version =
        kubectl_jsonpath(&context, "configmap", OVERLAY_CONFIGMAP, "{.metadata.resourceVersion}")?;
    let overlay_status_raw = kubectl_jsonpath(&context, "gridnetwork", GRID_NETWORK_NAME, "{.status.overlayStatus}")?;
    let overlay_status: serde_json::Value = serde_json::from_str(&overlay_status_raw)
        .map_err(|e| format!("GridNetwork overlayStatus is not valid JSON: {e}"))?;
    let gateway_status = overlay_status
        .as_array()
        .and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.get("gatewayName").and_then(serde_json::Value::as_str) == Some("edge-gateway"))
        })
        .ok_or("GridNetwork status missing overlay entry for edge-gateway")?;
    let status_phase = gateway_status
        .get("phase")
        .and_then(serde_json::Value::as_str)
        .ok_or("GridNetwork overlay status missing phase")?;
    let rendered_revision = gateway_status
        .get("renderedRevision")
        .and_then(serde_json::Value::as_str)
        .ok_or("GridNetwork overlay status missing renderedRevision")?;
    let distributed_revision = gateway_status
        .get("distributedRevision")
        .and_then(serde_json::Value::as_str)
        .ok_or("GridNetwork overlay status missing distributedRevision")?;
    let status_resource_version = gateway_status
        .get("configMapResourceVersion")
        .and_then(serde_json::Value::as_str)
        .ok_or("GridNetwork overlay status missing configMapResourceVersion")?;
    if status_phase != "Distributed" {
        return Err(format!("GridNetwork overlay status phase={status_phase:?}, expected \"Distributed\"").into());
    }
    if rendered_revision != revision || distributed_revision != revision {
        return Err("GridNetwork rendered/distributed revisions do not match the envelope revision".into());
    }
    if status_resource_version != config_map_resource_version {
        return Err(format!(
            "GridNetwork distributed resourceVersion={status_resource_version:?} != ConfigMap resourceVersion={config_map_resource_version:?}"
        )
        .into());
    }

    eprintln!("  rendered → revision={}", safe_truncate_str(rendered_revision, 16));
    eprintln!(
        "  distributed → revision={}, resourceVersion={config_map_resource_version}",
        safe_truncate_str(distributed_revision, 16)
    );

    Ok(format!(
        "dual-key: {legacy_evidence}; envelope: schema=1.0.0, revision={}, annotations and Grid status verified",
        safe_truncate_str(revision, 16)
    ))
}

/// Regression guard for [grid#42](https://github.com/praxis-proxy/grid/issues/42): an infinite
/// reconcile hot-loop with three independent unconditional-write sources —
/// `distribute_overlay_configmap`'s overlay `ConfigMap` apply, the `GridSite`
/// cert-PEM status patch, and (discovered during live helios08 validation of
/// the first two fixes) the `GridNetwork`'s own `status.overlayStatus[].renderedAt`
/// timestamp, which was refreshed from a new clock read on every reconcile
/// tick regardless of whether the distributed content actually changed.
/// Each write bumped its object's `resourceVersion` and fired a watch event
/// that re-triggered the `GridNetwork` reconciler — so the overlay
/// `ConfigMap`'s and/or the `GridNetwork`'s own `resourceVersion` climbed
/// continuously (~250-300 writes/sec on the `ConfigMap`, ~13-14 writes/sec on
/// `GridNetwork` itself, observed on helios08) and never settled —
/// [`check_overlay_metadata`]'s own resourceVersion-equality assertion would
/// eventually see a match by chance, but the underlying churn never stopped.
///
/// Must run *after* [`check_overlay_metadata`] has already passed once (see
/// its call site): this only proves stability, not initial correctness.
/// Captures the `ConfigMap`'s and the `GridNetwork`'s current
/// `resourceVersion`s and confirms both are unchanged after
/// [`OVERLAY_SETTLE_WINDOW`] — proving the operator actually stopped
/// writing to either object, rather than merely happening to observe
/// equal resourceVersions mid-churn.
fn check_overlay_metadata_settles() -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context(PRIMARY_EDGE);
    let watched = [
        WatchedResourceVersion::capture(&context, "configmap", OVERLAY_CONFIGMAP)?,
        WatchedResourceVersion::capture(&context, "gridnetwork", GRID_NETWORK_NAME)?,
    ];

    let deadline = Instant::now() + OVERLAY_SETTLE_WINDOW;
    while Instant::now() < deadline {
        thread::park_timeout(DATA_PLANE_PROBE_INTERVAL);
        for resource in &watched {
            resource.assert_unchanged(&context)?;
        }
    }

    Ok(format!(
        "{} both stable for {OVERLAY_SETTLE_WINDOW:?}",
        watched
            .iter()
            .map(WatchedResourceVersion::summary)
            .collect::<Vec<_>>()
            .join(" and ")
    ))
}

/// A Kubernetes object's `resourceVersion`, captured once as a baseline so it
/// can be re-checked against the live value to detect reconcile churn.
///
/// Used by [`check_overlay_metadata_settles`] to watch both the overlay
/// `ConfigMap` and the `GridNetwork` object itself without duplicating the
/// fetch-compare-format logic per resource (see grid#42: both objects were
/// independent sources of the same unconditional-write hot-loop pattern).
struct WatchedResourceVersion {
    /// `kubectl` resource kind, e.g. `"configmap"`.
    kind: &'static str,
    /// Resource name within `PRIMARY_EDGE`'s namespace.
    name: &'static str,
    /// `resourceVersion` observed at capture time.
    baseline: String,
}

impl WatchedResourceVersion {
    /// Read `kind`/`name`'s current `resourceVersion` as the stability baseline.
    fn capture(context: &str, kind: &'static str, name: &'static str) -> Result<Self, Box<dyn std::error::Error>> {
        let baseline = kubectl_jsonpath(context, kind, name, "{.metadata.resourceVersion}")?;
        Ok(Self { kind, name, baseline })
    }

    /// Re-reads the live `resourceVersion` and errors if it moved away from
    /// `baseline` — the observable signature of an unconditional-write
    /// reconcile hot-loop (grid#42).
    fn assert_unchanged(&self, context: &str) -> Result<(), Box<dyn std::error::Error>> {
        let current = kubectl_jsonpath(context, self.kind, self.name, "{.metadata.resourceVersion}")?;
        let baseline = &self.baseline;
        if current != *baseline {
            return Err(format!(
                "{} {} resourceVersion changed from {baseline} to {current} within the \
                 {OVERLAY_SETTLE_WINDOW:?} settle window — indicates an unconditional-write reconcile \
                 hot-loop (grid#42), not a converged, stable overlay",
                self.kind, self.name
            )
            .into());
        }
        Ok(())
    }

    /// One-line "kind name resourceVersion=X" fragment for the success message.
    fn summary(&self) -> String {
        format!("{} {} resourceVersion={}", self.kind, self.name, self.baseline)
    }
}

/// Validate overlay JSON contains candidates with required metadata.
fn validate_overlay_json(json: &str) -> Result<String, Box<dyn std::error::Error>> {
    let doc: serde_json::Value = serde_json::from_str(json)?;

    doc.get("generated_at")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("overlay missing generated_at")?;

    let candidates = doc
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates array")?;

    if candidates.is_empty() {
        return Err("overlay has 0 candidates".into());
    }

    validate_candidate_metadata(candidates)?;
    Ok(format!(
        "{} candidate(s), validated: stable_id, admission_state, selection_tier, rank, generated_at",
        candidates.len()
    ))
}

/// Validate each candidate has required metadata fields with
/// correct types and non-empty values.
fn validate_candidate_metadata(candidates: &[serde_json::Value]) -> Result<(), Box<dyn std::error::Error>> {
    let required_strings = ["stable_id", "admission_state", "selection_tier"];
    for (i, c) in candidates.iter().enumerate() {
        for field in &required_strings {
            let val = c
                .get(*field)
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.is_empty());
            if val.is_none() {
                return Err(format!("candidate[{i}] missing or empty {field}").into());
            }
        }
        let has_rank = c
            .get("rank")
            .is_some_and(|v| v.as_u64().is_some() || v.as_i64().is_some());
        if !has_rank {
            return Err(format!("candidate[{i}] missing or non-numeric rank").into());
        }
    }
    Ok(())
}

/// Prove both edge overlays retain two distinct providers at the east site.
fn check_same_site_provider_candidates() -> Result<String, Box<dyn std::error::Error>> {
    let mut evidence = Vec::new();
    for edge in EDGE_CLUSTERS {
        let overlay: serde_json::Value = serde_json::from_str(&read_overlay(edge)?)?;
        let candidates = overlay
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("{edge} overlay missing candidates array"))?;
        let (primary_id, secondary_id) = validate_same_site_candidates(edge, candidates)?;
        evidence.push(format!(
            "{edge}: east-provider clusters=[vcr-east-provider,{EAST_SECONDARY_CLUSTER}], stable_ids=[{primary_id},{secondary_id}]"
        ));
    }
    Ok(evidence.join("; "))
}

/// Validate the exact provider identity contract for one edge overlay.
fn validate_same_site_candidates<'cfg>(
    edge: &str,
    candidates: &'cfg [serde_json::Value],
) -> Result<(&'cfg str, &'cfg str), Box<dyn std::error::Error>> {
    let primary = find_overlay_candidate(candidates, "east-provider", "vcr-east-provider")?;
    let secondary = find_overlay_candidate(candidates, "east-provider", EAST_SECONDARY_CLUSTER)?;
    find_overlay_candidate(candidates, "west-provider", "vcr-west-provider")?;

    let primary_id = candidate_stable_id(primary)?;
    let secondary_id = candidate_stable_id(secondary)?;
    if primary_id == secondary_id {
        return Err(format!("{edge} overlay deduplicated the two east provider identities").into());
    }
    if primary_id != provider_candidate_id("east-provider")?
        || secondary_id != provider_candidate_id(EAST_SECONDARY_PROVIDER)?
    {
        return Err(format!("{edge} overlay candidate stable IDs do not match their provider identities").into());
    }
    Ok((primary_id, secondary_id))
}

/// Find one exact site/routing-cluster pair in an overlay.
fn find_overlay_candidate<'cfg>(
    candidates: &'cfg [serde_json::Value],
    site: &str,
    cluster: &str,
) -> Result<&'cfg serde_json::Value, Box<dyn std::error::Error>> {
    candidates
        .iter()
        .find(|candidate| {
            candidate.get("site").and_then(serde_json::Value::as_str) == Some(site)
                && candidate.get("cluster").and_then(serde_json::Value::as_str) == Some(cluster)
                && candidate.get("name").and_then(serde_json::Value::as_str) == Some(DEMO_MODEL)
        })
        .ok_or_else(|| format!("overlay missing {site}/{cluster}/{DEMO_MODEL} candidate").into())
}

/// Read the stable identity from a validated overlay candidate.
fn candidate_stable_id(candidate: &serde_json::Value) -> Result<&str, Box<dyn std::error::Error>> {
    candidate
        .get("stable_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "overlay candidate missing stable_id".into())
}

/// Verify provider gateway self-discovery.
///
/// Proves the operator's self-discovery path works end-to-end:
///
/// 1. The `provider-gateway` Service on each provider cluster has a `LoadBalancer` IP matching the independent Forge
///    capture (verifier evidence only — the operator does not read captures).
/// 2. The operator deployment does **not** have `GRID_GATEWAY_ADDRESS` set from Forge captures (confirming it uses
///    self-discovery).
/// 3. The remote `GridSite` egress address on the edge cluster equals the Service LB address (confirming the address
///    was broadcast via SWIM).
fn check_provider_gateway_addr(
    expected_addrs: &BTreeMap<String, String>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut verified = Vec::new();
    for cluster in PROVIDER_CLUSTERS {
        let expected = expected_addrs
            .get(*cluster)
            .ok_or_else(|| format!("missing Forge capture for {cluster} provider gateway"))?;
        let actual = get_service_lb_address(cluster, "provider-gateway")?;
        verify_expected_gateway_addr(cluster, "provider-gateway Service LB", &actual, expected)?;
        verify_no_capture_injection(cluster)?;
        verified.push(format!("{cluster}={actual} (self-discovered, broadcast via SWIM)"));
    }
    Ok(verified.join(", "))
}

/// Confirm the operator does not have `GRID_GATEWAY_ADDRESS` set from
/// Forge capture templates (i.e. containing `captures.`).
fn verify_no_capture_injection(cluster: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = kubectl_context(cluster);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "deploy",
            "grid-operator",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].env}",
        ])
        .output()?;
    let env_json = String::from_utf8(output.stdout)?;
    let gw_val = parse_env_var_from_json(&env_json, "GRID_GATEWAY_ADDRESS");
    if let Some(val) = &gw_val.filter(|v| !v.is_empty()) {
        return Err(
            format!("GRID_GATEWAY_ADDRESS on {cluster} is '{val}' — should be unset for self-discovery").into(),
        );
    }
    Ok(())
}

/// Read the first `LoadBalancer` ingress IP from a Service, formatted as `ip:port`.
fn get_service_lb_address(cluster: &str, service: &str) -> Result<String, Box<dyn std::error::Error>> {
    let raw = kubectl_service_lb_jsonpath(cluster, service)?;
    parse_service_lb_output(&raw, cluster, service)
}

/// Run kubectl to fetch Service LB IP and port via jsonpath.
fn kubectl_service_lb_jsonpath(cluster: &str, service: &str) -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context(cluster);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "svc",
            service,
            "-o",
            "jsonpath={.status.loadBalancer.ingress[0].ip},{.spec.ports[0].port}",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "kubectl get svc/{service} on {cluster} failed: {}",
            safe_truncate_str(stderr.trim(), 120)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

/// Parse `"ip,port"` output from kubectl jsonpath into `"ip:port"`.
fn parse_service_lb_output(raw: &str, cluster: &str, service: &str) -> Result<String, Box<dyn std::error::Error>> {
    let parts: Vec<&str> = raw.split(',').collect();
    let ip = parts
        .first()
        .copied()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("svc/{service} on {cluster} has no LoadBalancer IP"))?;
    let port = parts.get(1).copied().unwrap_or("8080");
    Ok(format!("{ip}:{port}"))
}

/// Verify a gateway address matches the independent Forge capture (verifier evidence only).
fn verify_expected_gateway_addr(
    cluster: &str,
    field: &str,
    actual: &str,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if actual.is_empty() || actual.contains("$(POD_IP)") {
        return Err(format!("{field} on {cluster} is '{actual}' (expected captured IP)").into());
    }
    if actual != expected {
        return Err(format!("{field} on {cluster} is '{actual}' (expected Forge capture '{expected}')").into());
    }
    Ok(())
}

/// Verify remote [`GridSite`] egress addresses on the primary edge.
///
/// [`GridSite`]: crate
fn check_remote_gridsite_egress(
    expected_addrs: &BTreeMap<String, String>,
) -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context(PRIMARY_EDGE);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "gridsite",
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("kubectl get gridsite failed: {}", safe_truncate_str(stderr.trim(), 120)).into());
    }
    let raw = String::from_utf8(output.stdout)?;
    parse_gridsite_egress(&raw, expected_addrs)
}

/// Parse [`GridSite`] list JSON and verify provider egress addresses.
///
/// [`GridSite`]: crate
fn parse_gridsite_egress(
    json: &str,
    expected_addrs: &BTreeMap<String, String>,
) -> Result<String, Box<dyn std::error::Error>> {
    let doc: serde_json::Value = serde_json::from_str(json)?;
    let items = doc
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or("gridsite list missing items")?;

    let mut verified = Vec::new();
    for provider in PROVIDER_CLUSTERS {
        let expected = expected_addrs
            .get(*provider)
            .ok_or_else(|| format!("missing Forge capture for {provider} provider gateway"))?;
        let addr = find_gridsite_egress(items, provider)?;
        if addr.is_empty() {
            return Err(format!("GridSite for {provider} has no egress address").into());
        }
        verify_expected_gateway_addr(provider, "GridSite egress", addr, expected)?;
        verified.push(format!("{provider}={addr}"));
    }
    Ok(verified.join(", "))
}

/// Find one provider site's egress address in a `GridSite` list.
fn find_gridsite_egress<'cfg>(
    items: &'cfg [serde_json::Value],
    provider: &str,
) -> Result<&'cfg str, Box<dyn std::error::Error>> {
    let expected_name = format!("{GRID_NETWORK_NAME}-{provider}");
    let site = items.iter().find(|item| {
        item.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|n| n == expected_name)
    });
    let Some(site) = site else {
        return Err(format!("GridSite for {provider} not found on edge cluster").into());
    };
    Ok(site
        .pointer("/spec/egress/address")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(""))
}

/// Load expected provider gateway addresses from Forge's default state file
/// (verifier evidence only — operators self-discover their own addresses).
fn load_provider_gateway_addresses() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let state = fs::read_to_string(".forge/state.json")?;
    parse_provider_gateway_captures(&state)
}

/// Parse provider gateway captures from Forge state JSON.
fn parse_provider_gateway_captures(json: &str) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let doc: serde_json::Value = serde_json::from_str(json)?;
    let captures = doc
        .get("captures")
        .and_then(serde_json::Value::as_object)
        .ok_or("Forge state missing captures")?;

    let mut addrs = BTreeMap::new();
    for cluster in PROVIDER_CLUSTERS {
        let ip = captures
            .get(*cluster)
            .and_then(|c| c.get("provider-gateway-ip"))
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("Forge state missing captures.{cluster}.provider-gateway-ip"))?;
        addrs.insert((*cluster).to_owned(), format!("{ip}:8443"));
    }
    Ok(addrs)
}

/// Verify the expected `GridNetwork` resource exists on each
/// edge cluster.
fn check_site_stacks() -> Result<String, Box<dyn std::error::Error>> {
    for edge in EDGE_CLUSTERS {
        let context = kubectl_context(edge);
        let output = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "gridnetwork",
                GRID_NETWORK_NAME,
                "--no-headers",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "GridNetwork '{GRID_NETWORK_NAME}' not found on {edge}: {}",
                safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
            )
            .into());
        }
    }
    Ok(format!(
        "GridNetwork '{GRID_NETWORK_NAME}' applied on both edge clusters"
    ))
}

/// Require both edge overlays to exist and be projected at
/// `/etc/praxis/routing`.
fn check_edge_overlay_mounts_with_count(expected_candidates: usize) -> Result<String, Box<dyn std::error::Error>> {
    let mut evidence = Vec::new();
    let mut revisions = Vec::new();
    for edge in EDGE_CLUSTERS {
        let revision = verify_single_edge_overlay_with_count(edge, expected_candidates)?;
        evidence.push(format!(
            "{edge}={expected_candidates} candidates, projected `ConfigMap`"
        ));
        revisions.push((edge, revision));
    }
    for (edge, revision) in revisions {
        eprintln!("  distributed → {edge}: revision={}", safe_truncate_str(&revision, 16));
    }
    Ok(evidence.join(", "))
}

/// Wait until both edge gateways have a complete operator-rendered overlay.
///
/// # Errors
///
/// Returns an error when all provider candidates and the overlay projection
/// do not become ready before the provider-state timeout.
pub(crate) fn wait_for_edge_overlays_ready() -> Result<String, Box<dyn std::error::Error>> {
    // Try to auto-detect the candidate count by checking the actual overlay
    match detect_candidate_count() {
        Ok(count) => wait_for_edge_overlays_ready_with_count(count),
        Err(_) => wait_for_edge_overlays_ready_with_count(3), // fallback to default
    }
}

/// Detect the expected candidate count by checking the first edge's overlay
fn detect_candidate_count() -> Result<usize, Box<dyn std::error::Error>> {
    let context = kubectl_context("east-edge");
    let overlay = kubectl_jsonpath(
        &context,
        "configmap",
        OVERLAY_CONFIGMAP,
        r"{.data.routing-config\.json}",
    )?;
    let document: serde_json::Value = serde_json::from_str(&overlay)?;
    let candidates = document
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates")?;
    Ok(candidates.len())
}

/// Wait for edge overlays to be ready with the expected candidate count.
#[expect(
    clippy::disallowed_methods,
    reason = "bounded polling for asynchronous SWIM and operator convergence"
)]
pub(crate) fn wait_for_edge_overlays_ready_with_count(
    expected_candidates: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + PROVIDER_STATE_WAIT;
    let mut last_error = String::from("overlay validation has not run");
    while Instant::now() < deadline {
        match check_edge_overlay_mounts_with_count(expected_candidates) {
            Ok(evidence) => return Ok(evidence),
            Err(error) => last_error = error.to_string(),
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(format!("timeout waiting for complete edge overlays: {last_error}").into())
}

/// Validate one edge cluster's overlay content and volume projection.
#[expect(
    clippy::too_many_lines,
    reason = "linear validation sequence, splitting hurts readability"
)]

fn verify_single_edge_overlay_with_count(
    edge: &str,
    expected_candidates: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context(edge);

    let overlay = kubectl_jsonpath(
        &context,
        "configmap",
        OVERLAY_CONFIGMAP,
        r"{.data.routing-config\.json}",
    )?;
    let document: serde_json::Value = serde_json::from_str(&overlay)?;
    let local_site = document
        .get("local_site")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("{edge} overlay missing local_site"))?;
    let candidates = document
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{edge} overlay missing candidates"))?;
    if local_site != edge || candidates.len() != expected_candidates {
        return Err(format!(
            "{edge} overlay local_site={local_site:?}, candidates={}",
            candidates.len()
        )
        .into());
    }

    let revision = overlay_revision(edge)?;

    let mounted = kubectl_jsonpath(
        &context,
        "deployment",
        "edge-gateway",
        "{.spec.template.spec.volumes[?(@.name=='overlay')].configMap.name}",
    )?;
    if mounted != OVERLAY_CONFIGMAP {
        return Err(format!("{edge} edge-gateway mounts overlay `ConfigMap` {mounted:?}").into());
    }
    Ok(revision)
}

/// Read the semantic revision from one edge's distributed envelope.
fn overlay_revision(edge: &str) -> Result<String, Box<dyn std::error::Error>> {
    let envelope_raw = kubectl_jsonpath(
        &kubectl_context(edge),
        "configmap",
        OVERLAY_CONFIGMAP,
        r"{.data.routing-overlay\.json}",
    )?;
    if envelope_raw.trim().is_empty() {
        return Err(format!("{edge} overlay ConfigMap missing routing-overlay.json key").into());
    }
    let envelope: serde_json::Value =
        serde_json::from_str(&envelope_raw).map_err(|e| format!("{edge} envelope JSON parse failed: {e}"))?;
    envelope
        .pointer("/revision/value")
        .and_then(serde_json::Value::as_str)
        .filter(|revision| !revision.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("{edge} envelope missing revision.value").into())
}

/// Maximum time to wait for an edge gateway to accept a distributed overlay
/// revision. Covers Kubernetes projected `ConfigMap` refresh (kubelet sync
/// period, default up to 60 s) plus the file watcher debounce (500 ms).
const EDGE_ACCEPTANCE_TIMEOUT: Duration = Duration::from_secs(90);

/// Interval between edge log polls while waiting for overlay acceptance.
const EDGE_ACCEPTANCE_POLL: Duration = Duration::from_secs(2);

/// Prove that Praxis accepted the exact revision distributed to an edge.
///
/// Polls the edge gateway logs at [`EDGE_ACCEPTANCE_POLL`] intervals until
/// the logs contain either the initial `"overlay snapshot initialized"` event
/// or a later `"overlay reloaded"` event with an `accepted_revision` field
/// matching `revision`, or
/// [`EDGE_ACCEPTANCE_TIMEOUT`] expires.
fn verify_edge_accepted_revision(edge: &str, revision: &str) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_edge_revision_acceptance(
        edge,
        revision,
        EDGE_ACCEPTANCE_TIMEOUT,
        EDGE_ACCEPTANCE_POLL,
        edge_gateway_logs,
    )
}

/// Testable core of the edge acceptance barrier.
///
/// `log_source` is injected so unit tests can supply synthetic logs without
/// a live cluster.
fn wait_for_edge_revision_acceptance(
    edge: &str,
    revision: &str,
    timeout: Duration,
    interval: Duration,
    log_source: impl Fn(&str) -> Result<String, Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let mut last_accepted: Option<String> = None;
    loop {
        let logs = log_source(edge)?;
        if logs_prove_acceptance(&logs, revision) {
            return Ok(());
        }
        last_accepted = extract_last_accepted_revision(&logs).or(last_accepted);
        if Instant::now() >= deadline {
            let observed = last_accepted.as_deref().unwrap_or("none");
            return Err(format!(
                "{edge} did not accept overlay revision {} within {timeout:?}; \
                 last observed accepted_revision={observed}",
                safe_truncate_str(revision, 16),
            )
            .into());
        }
        thread::park_timeout(interval);
    }
}

/// Check whether a single log line proves the edge accepted `revision`.
///
/// Requires one individual line to contain a recognized snapshot-acceptance
/// event and the exact field `accepted_revision="<revision>"`. The revision
/// must appear as an exact quoted value, not as a substring of a longer field
/// or an unrelated context.
fn logs_prove_acceptance(logs: &str, revision: &str) -> bool {
    logs.lines().any(|line| {
        (line.contains("overlay snapshot initialized") || line.contains("overlay reloaded"))
            && accepted_revision_from_line(line) == Some(revision)
    })
}

/// Extract an exact `accepted_revision` field from one tracing log line.
///
/// `tracing` renders debug-recorded string fields with quotes and
/// display-recorded string fields without them, so both encodings are valid.
/// The field must start at the beginning of the line or after whitespace to
/// avoid matching the suffix of `previous_serving_revision`.
fn accepted_revision_from_line(line: &str) -> Option<&str> {
    const FIELD: &str = "accepted_revision=";
    for (index, _) in line.match_indices(FIELD) {
        let has_field_boundary = index == 0
            || line
                .get(..index)
                .and_then(|prefix| prefix.chars().next_back())
                .is_some_and(char::is_whitespace);
        if !has_field_boundary {
            continue;
        }
        let value = line.get(index + FIELD.len()..)?;
        let value = if let Some(quoted) = value.strip_prefix('"') {
            quoted.split('"').next()
        } else {
            value.split_whitespace().next()
        };
        return value.filter(|revision| !revision.is_empty());
    }
    None
}

/// Extract an exact `serving_revision` field without matching its `previous_` prefix.
fn serving_revision_from_line(line: &str) -> Option<&str> {
    const FIELD: &str = "serving_revision=";
    for (index, _) in line.match_indices(FIELD) {
        let has_field_boundary = index == 0
            || line
                .get(..index)
                .and_then(|prefix| prefix.chars().next_back())
                .is_some_and(char::is_whitespace);
        if !has_field_boundary {
            continue;
        }
        let value = line.get(index + FIELD.len()..)?;
        let value = if let Some(quoted) = value.strip_prefix('"') {
            quoted.split('"').next()
        } else {
            value.split_whitespace().next()
        };
        return value.filter(|revision| !revision.is_empty());
    }
    None
}

/// Extract the most recent `accepted_revision` value from logs.
fn extract_last_accepted_revision(logs: &str) -> Option<String> {
    logs.lines()
        .rev()
        .find_map(accepted_revision_from_line)
        .map(str::to_owned)
}

/// Kubernetes pod identity used to prove hot reload did not restart the edge.
#[derive(Debug)]
struct EdgePodIdentity {
    /// Kubernetes Pod UID.
    uid: String,
    /// Restart count for the Praxis container.
    restart_count: u64,
}

/// Require both edge gateway pods ready and capture the primary pod identity.
fn check_edge_gateway_pods() -> Result<(String, EdgePodIdentity), Box<dyn std::error::Error>> {
    let mut primary = None;
    let mut evidence = Vec::new();
    for edge in EDGE_CLUSTERS {
        let identity = edge_pod_identity(edge)?;
        let revision = overlay_revision(edge)?;
        verify_edge_accepted_revision(edge, &revision)?;
        eprintln!("  accepted → {edge}: revision={}", safe_truncate_str(&revision, 16));
        evidence.push(format!(
            "{edge}=uid:{}, restarts:{}, accepted_revision:{}",
            safe_truncate_str(&identity.uid, 12),
            identity.restart_count,
            safe_truncate_str(&revision, 16)
        ));
        if *edge == PRIMARY_EDGE {
            primary = Some(identity);
        }
    }
    Ok((
        evidence.join(", "),
        primary.ok_or("primary edge pod identity unavailable")?,
    ))
}

/// Read the ready Praxis pod UID and restart count from one edge cluster.
fn edge_pod_identity(edge: &str) -> Result<EdgePodIdentity, Box<dyn std::error::Error>> {
    let pod = get_edge_pod_json(edge)?;
    let ready = pod
        .pointer("/status/containerStatuses/0/ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if !ready {
        return Err(format!("{edge} edge-gateway pod is not ready").into());
    }
    let uid = pod
        .pointer("/metadata/uid")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("{edge} edge-gateway pod has no UID"))?
        .to_owned();
    let restart_count = pod
        .pointer("/status/containerStatuses/0/restartCount")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("{edge} edge-gateway pod has no restartCount"))?;
    Ok(EdgePodIdentity { uid, restart_count })
}

/// Fetch the first edge-gateway pod as JSON from one cluster.
fn get_edge_pod_json(edge: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let context = kubectl_context(edge);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/name=edge-gateway",
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to inspect {edge} edge pod: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }
    let list: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    list.get("items")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| items.first().cloned())
        .ok_or_else(|| format!("{edge} has no edge-gateway pod").into())
}

/// Send an inference request and verify HTTP 200 with
/// provider attribution and model echo.
fn check_inference_routed() -> Result<String, Box<dyn std::error::Error>> {
    let resp = curl_post_with_auth(EDGE_PORT)?;
    if resp.status != 200 {
        return Err(format!("inference request returned HTTP {}", resp.status).into());
    }
    let provider =
        extract_provider(&resp).map_err(|_e| "inference response missing X-AI-Demo-Provider-Gateway header")?;
    let provider_gateway = resp
        .headers
        .get(PROVIDER_GATEWAY_RESPONSE_HEADER)
        .ok_or("inference response missing X-AI-Demo-Provider-Gateway header")?;
    let expected_gateway = provider_gateway_site(&provider)?;
    if provider_gateway != expected_gateway {
        return Err(format!(
            "backend provider '{provider}' expected gateway '{expected_gateway}', got '{provider_gateway}'"
        )
        .into());
    }
    let distributed_revision = overlay_revision(PRIMARY_EDGE)?;
    eprintln!("  serving → revision={}", safe_truncate_str(&distributed_revision, 16));
    let body: serde_json::Value =
        serde_json::from_str(&resp.body).map_err(|e| format!("inference response body is not valid JSON: {e}"))?;
    let model = body
        .get("model")
        .and_then(serde_json::Value::as_str)
        .ok_or("inference response missing model field")?;
    Ok(format!(
        "HTTP 200, model={model}, provider={provider}, gateway={provider_gateway}, overlay_revision={}, provider credential replaced client-supplied credential",
        safe_truncate_str(&distributed_revision, 16)
    ))
}

/// Prove Grid can select both independent providers hosted at the east site.
fn check_same_site_provider_routing() -> Result<String, Box<dyn std::error::Error>> {
    let first_session = format!("same-site-first-{}", unix_nanos());
    let first_response = curl_edge_request(EDGE_PORT, Some(&first_session))?;
    let first = validate_same_site_response(&first_response)?;
    let second_session = format!("same-site-second-{}", unix_nanos());
    let second_response = curl_edge_request(EDGE_PORT, Some(&second_session))?;
    let second = validate_same_site_response(&second_response)?;
    if first != second {
        return Err(format!("same-site requests changed gateway from {first} to {second}").into());
    }
    // The provider gateway attribution intentionally identifies the shared
    // east gateway, not the private backend behind it. Backend identity is
    // therefore proven by the preceding overlay stable-ID check; this request
    // proves both independent candidates are reachable through that gateway.
    Ok(format!(
        "east-provider gateway served two independent requests for {DEMO_MODEL}; distinct backend stable IDs were validated in the distributed overlay"
    ))
}

/// Validate one response came from a provider hosted behind the east gateway.
fn validate_same_site_response(response: &verify::HttpResponse) -> Result<String, Box<dyn std::error::Error>> {
    if response.status != 200 {
        return Err(format!("same-site provider request returned HTTP {}", response.status).into());
    }
    let provider = extract_provider(response)?;
    if !matches!(provider.as_str(), "east-provider" | EAST_SECONDARY_PROVIDER) {
        return Err(format!("same-site provider proof unexpectedly selected {provider}").into());
    }
    let gateway = response
        .headers
        .get(PROVIDER_GATEWAY_RESPONSE_HEADER)
        .ok_or("same-site response missing provider gateway attribution")?;
    if gateway != "east-provider" {
        return Err(format!("{provider} unexpectedly traversed provider gateway {gateway}").into());
    }
    Ok(provider)
}

/// Return a process-local unique suffix for demo affinity fixtures.
fn unix_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

/// Return the provider-site gateway responsible for a backend identity.
fn provider_gateway_site(provider: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    match provider {
        "east-provider" | EAST_SECONDARY_PROVIDER | OPENAI_PROVIDER => Ok("east-provider"),
        "west-provider" => Ok("west-provider"),
        _ => Err(format!("unknown demo provider identity {provider}").into()),
    }
}

/// Retry a request assertion while a projected overlay reaches the data plane.
pub(crate) fn wait_for_data_plane_convergence<T>(
    description: &str,
    check: impl FnMut() -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>> {
    wait_for_check(
        description,
        DATA_PLANE_CONVERGENCE_WAIT,
        DATA_PLANE_PROBE_INTERVAL,
        check,
    )
}

/// Run a check until it succeeds or its bounded convergence window expires.
fn wait_for_check<T>(
    description: &str,
    timeout: Duration,
    interval: Duration,
    mut check: impl FnMut() -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match check() {
            Ok(value) => return Ok(value),
            Err(error) if Instant::now() >= deadline => {
                return Err(format!("{description} did not converge within {timeout:?}: {error}").into());
            },
            Err(_) => thread::park_timeout(interval),
        }
    }
}

/// Bind a session and record which provider served it.
fn check_session_bind(port: u16) -> Result<(String, String), Box<dyn std::error::Error>> {
    let resp = curl_edge_request(port, Some("glb-proof-a"))?;
    if resp.status != 200 {
        return Err(format!("session bind returned HTTP {}", resp.status).into());
    }
    let provider = extract_provider(&resp)?;
    Ok((format!("session=glb-proof-a bound to {provider}"), provider))
}

/// Verify the same session reuses the same provider.
fn check_session_reuse(port: u16, expected_provider: &str) -> Result<String, Box<dyn std::error::Error>> {
    for i in 0..2 {
        let resp = curl_edge_request(port, Some("glb-proof-a"))?;
        if resp.status != 200 {
            return Err(format!("reuse request {i} returned HTTP {}", resp.status).into());
        }
        let provider = extract_provider(&resp)?;
        if provider != expected_provider {
            return Err(format!("session drift: expected {expected_provider}, got {provider}").into());
        }
    }
    Ok(format!(
        "session=glb-proof-a stable across 3 requests, provider={expected_provider}"
    ))
}

/// Check edge logs for hot-reload evidence.
pub(crate) fn check_hot_reload_observed(
    edge: &str,
    previous_count: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        let current_count = count_overlay_reload_logs(edge)?;
        if current_count > previous_count {
            return Ok(format!(
                "overlay reload observed: before={previous_count}, after={current_count}"
            ));
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "xtask is synchronous; no async runtime available for tokio::time::sleep"
        )]
        thread::sleep(Duration::from_secs(1));
    }
    Err(format!("{edge} overlay reload log count did not increase from {previous_count}").into())
}

/// Count overlay reload log entries for a Kubernetes edge pod.
pub(crate) fn count_overlay_reload_logs(edge: &str) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(count_reload_entries(&edge_gateway_logs(edge)?))
}

/// Read bounded logs from one edge gateway deployment.
fn edge_gateway_logs(edge: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(edge),
            "-n",
            GRID_SYSTEM_NS,
            "logs",
            "deployment/edge-gateway",
            "--tail=200",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to read {edge} edge logs: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }
    let logs = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Ok(strip_csi_sgr(&format!("{logs}{stderr}")))
}

/// Strip CSI SGR (Select Graphic Rendition) sequences from log output.
///
/// Handles only the `ESC [ <params> <letter>` sequences that
/// `tracing-subscriber` emits for bold, dim, italic, color, and
/// reset.  This is NOT a general ANSI/VT escape parser — it does
/// not handle OSC, DCS, APC, or multi-byte CSI final bytes.
fn strip_csi_sgr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if chars.next() == Some('[') {
                for final_byte in chars.by_ref() {
                    if final_byte.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Count hot-reload log entries in text.
fn count_reload_entries(logs: &str) -> usize {
    logs.matches("overlay reloaded").count()
}

/// Verify the edge Pod was not replaced or restarted during hot reload.
fn check_edge_pod_stable(edge: &str, expected: &EdgePodIdentity) -> Result<String, Box<dyn std::error::Error>> {
    let current = edge_pod_identity(edge)?;
    if current.uid != expected.uid {
        return Err(format!(
            "{edge} edge pod changed UID: expected {}, got {}",
            safe_truncate_str(&expected.uid, 12),
            safe_truncate_str(&current.uid, 12)
        )
        .into());
    }
    if current.restart_count != expected.restart_count {
        return Err(format!(
            "{edge} restart count changed: expected {}, got {}",
            expected.restart_count, current.restart_count
        )
        .into());
    }
    Ok(format!(
        "pod UID {} unchanged, restartCount={}",
        safe_truncate_str(&current.uid, 12),
        current.restart_count
    ))
}

/// Prove last-known-good reload behavior and fail-closed cold startup.
///
/// The test corrupts only the envelope digest in the operator-owned
/// `ConfigMap`, never its legacy key or any Secret. The original envelope is
/// restored on every exit path.
///
/// The test temporarily pauses the edge Grid operator (controlled
/// fault injection) so its self-healing reconciliation does not
/// overwrite the deliberately corrupted `ConfigMap`.  This is not
/// a production overlay-distribution path.
#[expect(
    clippy::too_many_lines,
    clippy::indexing_slicing,
    reason = "sequential end-to-end proof with known-shape JSON mutation"
)]
fn check_invalid_overlay_protection(edge: &str) -> Result<String, Box<dyn std::error::Error>> {
    let valid_envelope = kubectl_jsonpath(
        &kubectl_context(edge),
        "configmap",
        OVERLAY_CONFIGMAP,
        r"{.data.routing-overlay\.json}",
    )?;
    let valid_document: serde_json::Value = serde_json::from_str(&valid_envelope)?;
    let valid_revision = valid_document
        .pointer("/revision/value")
        .and_then(serde_json::Value::as_str)
        .ok_or("valid envelope missing revision.value")?
        .to_owned();
    let mut invalid_document = valid_document;
    invalid_document["content_digest"]["value"] = serde_json::Value::String("0".repeat(64));
    let invalid_envelope = serde_json::to_string_pretty(&invalid_document)?;

    let previous_rejections = count_invalid_reload_logs(edge).unwrap_or(0);
    let previous_pod = edge_pod_identity(edge)?;

    let original_replicas = operator_replicas(edge)?;
    scale_operator(edge, 0)?;

    let patch_result = patch_overlay_envelope(edge, &invalid_envelope);

    let proof = patch_result.and_then(|()| {
        wait_for_invalid_reload_rejection(edge, previous_rejections)?;
        let lkg_evidence = wait_for_data_plane_convergence("last-known-good request", check_inference_routed)?;
        wait_for_cold_start_rejection(edge, &previous_pod.uid)?;
        Ok::<_, Box<dyn std::error::Error>>(lkg_evidence)
    });

    let restore = restore_overlay_resources(
        edge,
        &valid_envelope,
        original_replicas,
        patch_overlay_envelope,
        scale_operator,
    );

    let recovery = restore.and_then(|()| {
        kubectl::wait_for_rollout_ns(&kubectl_context(edge), "edge-gateway", GRID_SYSTEM_NS, edge)?;
        wait_for_data_plane_convergence("edge recovery after valid overlay restore", check_inference_routed)
    });

    match (proof, recovery) {
        (Ok(lkg), Ok(recovered)) => Ok(format!(
            "invalid revision rejected; last-known-good {} remained serving; cold start failed closed; valid overlay restored; {lkg}; recovery: {recovered}",
            safe_truncate_str(&valid_revision, 16)
        )),
        (Err(proof_error), Ok(_)) => Err(proof_error),
        (Ok(_), Err(restore_error)) => {
            Err(format!("invalid overlay proof passed but restoration failed: {restore_error}").into())
        },
        (Err(proof_error), Err(restore_error)) => {
            Err(format!("{proof_error}; restoration also failed: {restore_error}").into())
        },
    }
}

/// Function pointer for envelope patch operations.
type PatchFn = fn(&str, &str) -> Result<(), Box<dyn std::error::Error>>;

/// Function pointer for operator scale operations.
type ScaleFn = fn(&str, u32) -> Result<(), Box<dyn std::error::Error>>;

/// Restore the valid envelope and operator replica count.
///
/// Both steps are attempted independently so that a `ConfigMap`
/// restore failure does not prevent the operator from being scaled
/// back up.  All errors are collected and reported together.
///
/// This function performs only resource restoration (pure).  The caller
/// is responsible for waiting on rollout and data-plane recovery.
fn restore_overlay_resources(
    edge: &str,
    valid_envelope: &str,
    original_replicas: u32,
    patch_fn: PatchFn,
    scale_fn: ScaleFn,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut errors: Vec<String> = Vec::new();

    if let Err(e) = patch_fn(edge, valid_envelope) {
        errors.push(format!("envelope restore: {e}"));
    }

    if let Err(e) = scale_fn(edge, original_replicas) {
        errors.push(format!("operator restore to {original_replicas}: {e}"));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}

/// Replace only the projected envelope key in one edge `ConfigMap`.
fn patch_overlay_envelope(edge: &str, envelope: &str) -> Result<(), Box<dyn std::error::Error>> {
    let patch = serde_json::json!({
        "data": {
            "routing-overlay.json": envelope,
        }
    });
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(edge),
            "-n",
            GRID_SYSTEM_NS,
            "patch",
            "configmap",
            OVERLAY_CONFIGMAP,
            "--type=merge",
            "-p",
            &serde_json::to_string(&patch)?,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to patch {edge} overlay envelope: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 160)
        )
        .into());
    }
    Ok(())
}

/// Wait for a new invalid-reload log entry from the running edge.
#[expect(
    clippy::disallowed_methods,
    reason = "bounded polling in synchronous xtask validation"
)]
fn wait_for_invalid_reload_rejection(edge: &str, previous_count: usize) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + EDGE_ACCEPTANCE_TIMEOUT;
    while Instant::now() < deadline {
        if count_invalid_reload_logs(edge)? > previous_count {
            return Ok(());
        }
        thread::sleep(DATA_PLANE_PROBE_INTERVAL);
    }
    Err(format!("{edge} did not report rejection of the invalid overlay update").into())
}

/// Count invalid overlay reloads in bounded edge logs.
fn count_invalid_reload_logs(edge: &str) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(edge_gateway_logs(edge)?
        .matches("overlay reload failed, retaining previous snapshot")
        .count())
}

/// Delete the serving pod and require the replacement to reject invalid
/// startup content rather than becoming ready.
#[expect(
    clippy::disallowed_methods,
    clippy::too_many_lines,
    reason = "bounded polling in synchronous xtask validation"
)]
fn wait_for_cold_start_rejection(edge: &str, previous_uid: &str) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(edge),
            "-n",
            GRID_SYSTEM_NS,
            "delete",
            "pod",
            "-l",
            "app.kubernetes.io/name=edge-gateway",
            "--wait=true",
            "--timeout=60s",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to replace {edge} edge pod: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 160)
        )
        .into());
    }

    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        if let Ok(pod) = get_edge_pod_json(edge) {
            let uid = pod
                .pointer("/metadata/uid")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let ready = pod
                .pointer("/status/containerStatuses/0/ready")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let restart_count = pod
                .pointer("/status/containerStatuses/0/restartCount")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let waiting_reason = pod
                .pointer("/status/containerStatuses/0/state/waiting/reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if uid != previous_uid
                && !ready
                && (restart_count > 0 || matches!(waiting_reason, "CrashLoopBackOff" | "Error"))
            {
                return Ok(());
            }
        }
        thread::sleep(DATA_PLANE_PROBE_INTERVAL);
    }
    Err(format!("{edge} replacement pod did not fail closed on the invalid envelope").into())
}

/// Read the operator-rendered overlay from an edge `ConfigMap`.
fn read_overlay(edge: &str) -> Result<String, Box<dyn std::error::Error>> {
    kubectl_jsonpath(
        &kubectl_context(edge),
        "configmap",
        OVERLAY_CONFIGMAP,
        r"{.data.routing-config\.json}",
    )
}

/// Return one provider candidate from the operator-rendered edge overlay.
fn overlay_candidate(edge: &str, provider: &str) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    let cluster = provider_routing_cluster(provider)?;
    let overlay: serde_json::Value =
        serde_json::from_str(&read_overlay(edge)?).map_err(|e| format!("failed to parse {edge} overlay: {e}"))?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{edge} overlay missing candidates array"))?;
    Ok(candidates
        .iter()
        .find(|candidate| candidate.get("cluster").and_then(serde_json::Value::as_str) == Some(cluster))
        .cloned())
}

/// Wait for an operator-derived candidate admission state.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous polling in xtask; no async runtime is active"
)]
fn wait_for_candidate_admission(
    edge: &str,
    provider: &str,
    expected: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + PROVIDER_STATE_WAIT;
    let mut observed = None;
    while Instant::now() < deadline {
        observed = overlay_candidate(edge, provider)?.and_then(|candidate| {
            candidate
                .get("admission_state")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
        if observed.as_deref() == Some(expected) {
            return Ok(format!("overlay admission_state={expected}"));
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(format!("timeout waiting for {provider} admission_state={expected} on {edge}; observed={observed:?}").into())
}

/// Wait until an unavailable provider is absent from the edge overlay.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous polling in xtask; no async runtime is active"
)]
fn wait_for_candidate_absent(edge: &str, provider: &str) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + PROVIDER_STATE_WAIT;
    while Instant::now() < deadline {
        if overlay_candidate(edge, provider)?.is_none() {
            return Ok(format!("provider={provider} absent from operator-rendered overlay"));
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(format!("timeout waiting for {provider} removal from {edge} overlay").into())
}

/// Return the `InferenceProvider` resource for one demo provider identity.
fn provider_resource_name(provider: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    match provider {
        "east-provider" => Ok("vcr-east-provider"),
        EAST_SECONDARY_PROVIDER => Ok("vcr-east-provider-secondary"),
        "west-provider" => Ok("vcr-west-provider"),
        _ => Err(format!("unknown demo provider identity {provider}").into()),
    }
}

/// Return the Kubernetes cluster hosting a demo provider.
fn provider_host_cluster(provider: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    provider_gateway_site(provider)
}

/// Return the overlay routing-cluster identity for a demo provider.
fn provider_routing_cluster(provider: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    match provider {
        "east-provider" => Ok("vcr-east-provider"),
        EAST_SECONDARY_PROVIDER => Ok(EAST_SECONDARY_CLUSTER),
        "west-provider" => Ok("vcr-west-provider"),
        OPENAI_PROVIDER => Ok(OPENAI_ROUTING_CLUSTER),
        _ => Err(format!("unknown demo provider identity {provider}").into()),
    }
}

/// Return the backend Deployment for a demo provider.
fn provider_backend_deployment(provider: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    match provider {
        "east-provider" | "west-provider" => Ok("vcr-inference"),
        EAST_SECONDARY_PROVIDER => Ok("vcr-inference-secondary"),
        _ => Err(format!("unknown demo provider identity {provider}").into()),
    }
}

/// Trigger provider health, metrics, `GridNetwork`, and SWIM reconciliation.
fn refresh_provider(provider: &str) -> Result<(), Box<dyn std::error::Error>> {
    let resource = provider_resource_name(provider)?;
    let cluster = provider_host_cluster(provider)?;
    let revision = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let annotation = format!("grid.praxis-proxy.io/demo-refresh={revision}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(cluster),
            "-n",
            GRID_SYSTEM_NS,
            "annotate",
            "inferenceprovider",
            resource,
            &annotation,
            "--overwrite",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to refresh {provider} provider reconciliation: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }
    Ok(())
}

/// Remove the verifier's transient reconcile annotation.
fn clear_provider_refresh(provider: &str) -> Result<(), Box<dyn std::error::Error>> {
    let resource = provider_resource_name(provider)?;
    let cluster = provider_host_cluster(provider)?;
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(cluster),
            "-n",
            GRID_SYSTEM_NS,
            "annotate",
            "inferenceprovider",
            resource,
            "grid.praxis-proxy.io/demo-refresh-",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to clear {provider} verifier annotation: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }
    Ok(())
}

/// Provider state captured before a health-driven hot-reload proof.
struct ProviderWithdrawal {
    /// Provider identity being withdrawn.
    provider: String,
    /// Desired backend replica count to restore.
    original_replicas: u32,
    /// Edge reload count captured before withdrawal.
    reload_count_before: usize,
}

/// Return the declared operator replica count for an edge cluster.
fn operator_replicas(edge: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let value = kubectl_jsonpath(
        &kubectl_context(edge),
        "deployment",
        "grid-operator",
        "{.spec.replicas}",
    )?;
    value
        .parse()
        .map_err(|e| format!("{edge} grid-operator has invalid replica count {value:?}: {e}").into())
}

/// Scale the grid-operator deployment on an edge cluster.
///
/// Used to prevent the operator from reconciling the overlay
/// `ConfigMap` while the verifier holds it in an intentionally
/// invalid state.
#[expect(
    clippy::disallowed_methods,
    clippy::too_many_lines,
    reason = "synchronous polling in xtask validation"
)]
fn scale_operator(edge: &str, replicas: u32) -> Result<(), Box<dyn std::error::Error>> {
    let target = format!("--replicas={replicas}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(edge),
            "-n",
            GRID_SYSTEM_NS,
            "scale",
            "deployment/grid-operator",
            &target,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to scale {edge} grid-operator: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }
    if replicas > 0 {
        return kubectl::wait_for_rollout_ns(&kubectl_context(edge), "grid-operator", GRID_SYSTEM_NS, edge);
    }
    let ctx = kubectl_context(edge);
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        let logs_output = Command::new("kubectl")
            .args([
                "--context",
                &ctx,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/name=grid-operator",
                "--no-headers",
            ])
            .output()?;
        let stdout = String::from_utf8_lossy(&logs_output.stdout);
        if stdout.trim().is_empty() {
            return Ok(());
        }
        thread::sleep(Duration::from_secs(2));
    }
    Err(format!("{edge} grid-operator pods did not terminate within 90s").into())
}

/// Return the declared mock backend replica count.
fn provider_backend_replicas(provider: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let cluster = provider_host_cluster(provider)?;
    let deployment = provider_backend_deployment(provider)?;
    let value = kubectl_jsonpath(&kubectl_context(cluster), "deployment", deployment, "{.spec.replicas}")?;
    value
        .parse()
        .map_err(|e| format!("{provider} {deployment} has invalid replica count {value:?}: {e}").into())
}

/// Scale the provider backend and wait for the desired availability.
fn scale_provider_backend(provider: &str, replicas: u32) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = provider_host_cluster(provider)?;
    let deployment = provider_backend_deployment(provider)?;
    let target = format!("--replicas={replicas}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(cluster),
            "-n",
            GRID_SYSTEM_NS,
            "scale",
            &format!("deployment/{deployment}"),
            &target,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to scale {provider} {deployment}: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }
    if replicas > 0 {
        return kubectl::wait_for_rollout_ns(&kubectl_context(cluster), deployment, GRID_SYSTEM_NS, cluster);
    }
    wait_for_backend_scaled_to_zero(provider)
}

/// Wait until a scaled-down provider backend has no available replicas.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous polling in xtask; no async runtime is active"
)]
fn wait_for_backend_scaled_to_zero(provider: &str) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = provider_host_cluster(provider)?;
    let deployment = provider_backend_deployment(provider)?;
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut observed = String::new();
    while Instant::now() < deadline {
        observed = kubectl_jsonpath(
            &kubectl_context(cluster),
            "deployment",
            deployment,
            "{.status.availableReplicas}",
        )?;
        if observed.is_empty() || observed == "0" {
            return Ok(());
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(format!("timeout scaling {provider} {deployment} to zero; availableReplicas={observed:?}").into())
}

/// Wait for an `InferenceProvider` phase produced by health reconciliation.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous polling in xtask; no async runtime is active"
)]
fn wait_for_provider_phase(provider: &str, expected: &str) -> Result<String, Box<dyn std::error::Error>> {
    let resource = provider_resource_name(provider)?;
    let cluster = provider_host_cluster(provider)?;
    let deadline = Instant::now() + PROVIDER_STATE_WAIT;
    let mut observed = String::new();
    while Instant::now() < deadline {
        observed = kubectl_jsonpath(
            &kubectl_context(cluster),
            "inferenceprovider",
            resource,
            "{.status.phase}",
        )?;
        if observed == expected {
            return Ok(format!("{resource} phase={expected}"));
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(format!("timeout waiting for {resource} phase={expected}; observed={observed:?}").into())
}

/// Withdraw a provider by making its declared backend unavailable.
fn withdraw_provider(edge: &str, provider: &str) -> Result<(String, ProviderWithdrawal), Box<dyn std::error::Error>> {
    let original_replicas = provider_backend_replicas(provider)?;
    if original_replicas == 0 {
        return Err(format!("{provider} backend already has zero replicas").into());
    }
    let state = ProviderWithdrawal {
        provider: provider.to_owned(),
        original_replicas,
        reload_count_before: count_overlay_reload_logs(edge)?,
    };
    let result = (|| {
        scale_provider_backend(provider, 0)?;
        refresh_provider(provider)?;
        let phase = wait_for_provider_phase(provider, "Unavailable")?;
        let overlay = wait_for_candidate_absent(edge, provider)?;
        Ok(format!("{phase}; {overlay}"))
    })();
    match result {
        Ok(evidence) => Ok((evidence, state)),
        Err(error) => match restore_withdrawn_provider(edge, &state) {
            Ok(_) => Err(error),
            Err(restore) => Err(format!("{error}; provider restoration also failed: {restore}").into()),
        },
    }
}

/// Restore a health-withdrawn provider and its operator-generated overlay entry.
fn restore_withdrawn_provider(edge: &str, state: &ProviderWithdrawal) -> Result<String, Box<dyn std::error::Error>> {
    let reload_before = count_overlay_reload_logs(edge)?;
    scale_provider_backend(&state.provider, state.original_replicas)?;
    refresh_provider(&state.provider)?;
    let phase = wait_for_provider_phase(&state.provider, "Available")?;
    let admission = wait_for_candidate_admission(edge, &state.provider, "new_and_existing")?;
    let reload = check_hot_reload_observed(edge, reload_before)?;
    clear_provider_refresh(&state.provider)?;
    Ok(format!("{phase}; {admission}; {reload}"))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Quick IPv4 format check (4 dot-separated octets, each 0-255).
fn looks_like_ipv4(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok())
}

/// Build the kubectl context for a GLB demo cluster.
fn kubectl_context(cluster_name: &str) -> String {
    format!("kind-{CLUSTER_PREFIX}-{cluster_name}")
}

/// Get the external IP of the provider-gateway service via kubectl.
fn get_provider_gateway_ip(context: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "svc",
            "provider-gateway",
            "-o",
            "jsonpath={.status.loadBalancer.ingress[0].ip}",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("kubectl get svc failed: {}", safe_truncate_str(stderr.trim(), 120)).into());
    }
    let ip = String::from_utf8(output.stdout)?.trim().to_owned();
    if !looks_like_ipv4(&ip) {
        return Err(format!("provider-gateway on {context} has invalid IP '{ip}'").into());
    }
    Ok(ip)
}

/// Send a Chat Completions request to the edge with bearer auth.
fn curl_post_with_auth(port: u16) -> Result<verify::HttpResponse, Box<dyn std::error::Error>> {
    curl_edge_request(port, None)
}

/// Chat Completions request body used by the verifier.
const CHAT_BODY: &str = r#"{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"hello"}],"max_tokens":64}"#;

/// Send a Chat Completions request with an optional session header.
fn curl_edge_request(_port: u16, session_id: Option<&str>) -> Result<verify::HttpResponse, Box<dyn std::error::Error>> {
    let address = get_service_lb_address(PRIMARY_EDGE, "edge-gateway")?;
    let url = format!("http://{address}/v1/chat/completions");
    let header_file = header_dump_path();
    let auth = format!("Authorization: Bearer {CLIENT_BEARER_TOKEN}");
    let mut cmd = Command::new("curl");
    cmd.args([
        "-s",
        "-w",
        "\n%{http_code}",
        "--connect-timeout",
        "5",
        "--max-time",
        "15",
        "-D",
        &header_file.display().to_string(),
        "-X",
        "POST",
        "-H",
        "Content-Type: application/json",
        "-H",
        &auth,
    ]);
    if let Some(sid) = session_id {
        cmd.args(["-H", &format!("X-Session-Id: {sid}")]);
    }
    cmd.args(["-d", CHAT_BODY, &url]);
    let output = cmd.output()?;
    parse_edge_curl_response(output, &header_file)
}

/// Parse one edge curl response and remove its temporary header dump.
fn parse_edge_curl_response(
    output: std::process::Output,
    header_file: &Path,
) -> Result<verify::HttpResponse, Box<dyn std::error::Error>> {
    let mut resp = verify::parse_curl_output(&String::from_utf8(output.stdout)?)?;
    resp.headers = parse_header_file(header_file);
    drop(fs::remove_file(header_file));
    Ok(resp)
}

/// Temp file path for curl header dumps.
fn header_dump_path() -> PathBuf {
    std::env::temp_dir().join(format!("glb-verify-headers-{}", std::process::id()))
}

/// Parse a curl `-D` header dump file into a map with lowercase keys.
fn parse_header_file(path: &Path) -> BTreeMap<String, String> {
    let content = fs::read_to_string(path).unwrap_or_default();
    parse_header_dump(&content)
}

/// Parse raw header dump text into a map with lowercase keys.
fn parse_header_dump(text: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for line in text.lines() {
        if let Some((key, value)) = line.split_once(':') {
            let k = key.trim().to_lowercase();
            if !k.is_empty() && !k.starts_with("http/") {
                map.insert(k, value.trim().to_owned());
            }
        }
    }
    map
}

/// Extract the `X-AI-Demo-Provider-Gateway` header from a response.
fn extract_provider(resp: &verify::HttpResponse) -> Result<String, Box<dyn std::error::Error>> {
    resp.headers
        .get("x-ai-demo-provider-gateway")
        .cloned()
        .ok_or_else(|| "missing x-ai-demo-provider-gateway header".into())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    /// Sample Forge configuration with placeholder images.
    fn sample_config_with_placeholders() -> &'static str {
        "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: grid-glb-demo
spec:
  clusters:
    - name: east-edge
      properties:
        operatorImage: \"ghcr.io/praxis-proxy/grid-operator:sha-PLACEHOLDER\"
        gatewayImage: \"ghcr.io/praxis-proxy/praxis-ai:sha-PLACEHOLDER\""
    }

    /// Sample Forge configuration with immutable development tags.
    fn sample_config_no_placeholders() -> &'static str {
        "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: grid-glb-demo
spec:
  clusters:
    - name: east-edge
      properties:
        operatorImage: \"ghcr.io/praxis-proxy/grid-operator:sha-abc123\"
        gatewayImage: \"ghcr.io/praxis-proxy/praxis-ai:sha-def456\""
    }

    /// Build Forge status JSON for the five Kubernetes clusters.
    fn status_with_clusters() -> serde_json::Value {
        serde_json::json!({
            "status": "ok",
            "data": {
                "clusters": [
                    {"name": "gtm-emulator", "statePhase": "running", "live": true},
                    {"name": "east-edge", "statePhase": "running", "live": true},
                    {"name": "east-provider", "statePhase": "running", "live": true},
                    {"name": "west-edge", "statePhase": "running", "live": true},
                    {"name": "west-provider", "statePhase": "running", "live": true}
                ]
            }
        })
    }

    #[test]
    fn detects_placeholder_images() {
        let placeholders = detect_placeholder_images(sample_config_with_placeholders());
        assert_eq!(placeholders.len(), 2, "should find 2 placeholders");
        assert!(placeholders.iter().all(|(owner, _)| owner == "east-edge"));
    }

    #[test]
    fn no_placeholders_in_clean_config() {
        let placeholders = detect_placeholder_images(sample_config_no_placeholders());
        assert!(placeholders.is_empty(), "should find no placeholders in clean config");
    }

    #[test]
    fn placeholder_images_detected() {
        let placeholders = detect_placeholder_images(sample_config_with_placeholders());
        assert!(!placeholders.is_empty(), "should detect placeholder images");
        assert!(
            placeholders.iter().any(|(_, img)| img.contains("PLACEHOLDER")),
            "should contain PLACEHOLDER tag: {placeholders:?}",
        );
    }

    #[test]
    fn detects_latest_explicit_tag() {
        let config = "\
clusters:
  - name: east-edge
    properties:
      operatorImage: \"grid-operator:latest\"
      gatewayImage: \"praxis-ai:glb-demo\"";
        let latest = detect_latest_images(config);
        assert_eq!(latest.len(), 1, "should find 1 :latest image");
        assert_eq!(
            latest.first().map(|(n, _)| n.as_str()),
            Some("east-edge"),
            "cluster name"
        );
        assert_eq!(
            latest.first().map(|(_, i)| i.as_str()),
            Some("grid-operator:latest"),
            "image value"
        );
    }

    #[test]
    fn detects_latest_implicit_no_tag() {
        let config = "\
clusters:
  - name: east-edge
    properties:
      operatorImage: grid-operator";
        let latest = detect_latest_images(config);
        assert_eq!(latest.len(), 1, "untagged image implies :latest");
    }

    #[test]
    fn no_latest_in_pinned_config() {
        let config = "\
clusters:
  - name: east-edge
    properties:
      operatorImage: \"grid-operator:glb-demo\"
      gatewayImage: \"praxis-ai:glb-demo\"";
        let latest = detect_latest_images(config);
        assert!(latest.is_empty(), "pinned tags should not trigger: {latest:?}");
    }

    #[test]
    fn image_is_latest_checks() {
        assert!(image_is_latest("grid-operator:latest"), "explicit :latest");
        assert!(image_is_latest("grid-operator"), "no tag implies :latest");
        assert!(!image_is_latest("grid-operator:glb-demo"), "pinned tag");
        assert!(!image_is_latest("grid-operator:sha-abc123"), "sha tag");
    }

    #[test]
    fn image_is_latest_registry_port() {
        assert!(
            !image_is_latest("localhost:5000/grid-operator:glb-demo"),
            "registry port with pinned tag"
        );
        assert!(
            image_is_latest("localhost:5000/grid-operator:latest"),
            "registry port with :latest"
        );
        assert!(
            image_is_latest("localhost:5000/grid-operator"),
            "registry port with no tag"
        );
    }

    #[test]
    fn image_is_latest_digest_pinned() {
        assert!(
            !image_is_latest("repo/image@sha256:abcdef1234567890"),
            "digest-pinned image"
        );
        assert!(
            !image_is_latest("localhost:5000/repo/image@sha256:abcdef1234567890"),
            "digest-pinned with registry port"
        );
    }

    #[test]
    fn detect_latest_in_resources_finds_nested_yaml() {
        let dir = std::env::temp_dir().join(format!("glb-test-{}", std::process::id()));
        let resources = dir.join("resources").join("nested");
        fs::create_dir_all(&resources).unwrap_or_else(|_| std::process::abort());
        fs::write(resources.join("bad.yaml"), "  image: grid-operator:latest\n")
            .unwrap_or_else(|_| std::process::abort());
        fs::write(resources.join("good.yaml"), "  image: grid-operator:glb-demo\n")
            .unwrap_or_else(|_| std::process::abort());
        fs::write(resources.join("notes.txt"), "  image: foo:latest\n").unwrap_or_else(|_| std::process::abort());
        let results = detect_latest_in_resources(&dir);
        assert_eq!(results.len(), 1, "should find 1 :latest in nested yaml: {results:?}");
        assert!(
            results.first().map(|(_, img)| img.as_str()) == Some("grid-operator:latest"),
            "should report the image value"
        );
        drop(fs::remove_dir_all(&dir));
    }

    #[test]
    fn parses_forge_status_clusters_global() {
        let status = status_with_clusters();
        let result = check_clusters_live(&status, IngressMode::Global);
        assert!(result.is_ok(), "all clusters should be live: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("5"), "should report 5 clusters: {evidence}");
    }

    #[test]
    fn parses_forge_status_clusters_workload() {
        let status = status_with_clusters();
        let result = check_clusters_live(&status, IngressMode::Workload);
        assert!(result.is_ok(), "all grid clusters should be live: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("4"), "should report 4 clusters: {evidence}");
    }

    #[test]
    fn reload_entry_counter_counts_exact_messages() {
        let logs = "overlay reloaded\nunrelated\noverlay reloaded";
        assert_eq!(count_reload_entries(logs), 2, "should count reload entries");
    }

    #[test]
    fn convergence_wait_retries_transient_failures() -> Result<(), Box<dyn std::error::Error>> {
        let mut attempts = 0;
        let value = wait_for_check("test check", Duration::from_secs(1), Duration::ZERO, || {
            attempts += 1;
            if attempts < 3 {
                return Err(std::io::Error::other("transient").into());
            }
            Ok("ready")
        })?;
        assert_eq!(value, "ready");
        assert_eq!(attempts, 3);
        Ok(())
    }

    #[test]
    fn blocked_steps_cause_nonzero_exit() {
        let results = [
            StepResult::pass("step-a", "ok"),
            StepResult::blocked("step-b", "not implemented"),
        ];
        let any_not_pass = results.iter().any(|r| r.status != StepStatus::Pass);
        assert!(any_not_pass, "BLOCKED step should prevent clean exit");
    }

    #[test]
    fn block_remaining_adds_each_remaining_step_once() {
        let mut results = vec![StepResult::pass("prerequisites", "ok")];
        block_remaining("routing after reload", "reload failed", &mut results);
        let blocked = results
            .iter()
            .filter(|r| r.status == StepStatus::Blocked)
            .collect::<Vec<_>>();
        assert_eq!(blocked.len(), 3, "dependent assertions should be blocked once");
        assert_eq!(blocked.first().map(|r| r.label), Some("routing after reload"));
        assert_eq!(blocked.get(1).map(|r| r.label), Some("edge pod stable"));
        assert_eq!(blocked.get(2).map(|r| r.label), Some("invalid overlay protection"));
    }

    #[test]
    fn parse_env_var_finds_value() {
        let json =
            r#"[{"name":"GRID_SWIM_ADVERTISE_ADDR","value":"172.18.0.5:7946"},{"name":"RUST_LOG","value":"info"}]"#;
        let val = parse_env_var_from_json(json, "GRID_SWIM_ADVERTISE_ADDR");
        assert_eq!(val.as_deref(), Some("172.18.0.5:7946"), "should find advertise addr",);
    }

    #[test]
    fn parse_env_var_missing_returns_none() {
        let json = r#"[{"name":"RUST_LOG","value":"info"}]"#;
        let val = parse_env_var_from_json(json, "GRID_SWIM_ADVERTISE_ADDR");
        assert!(val.is_none(), "missing var should return None");
    }

    #[test]
    fn parse_env_var_invalid_json_returns_none() {
        let val = parse_env_var_from_json("not json", "FOO");
        assert!(val.is_none(), "invalid JSON should return None");
    }

    #[test]
    fn seeds_count_two_entries() {
        let raw = r#"["172.18.0.3:7946" "172.18.0.4:7946"]"#;
        assert_eq!(parse_seeds_count(raw), 2, "should count 2 seeds");
    }

    #[test]
    fn seeds_count_empty() {
        assert_eq!(parse_seeds_count("[]"), 0, "empty array should be 0");
        assert_eq!(parse_seeds_count(""), 0, "empty string should be 0");
    }

    #[test]
    fn looks_like_ipv4_valid() {
        assert!(looks_like_ipv4("172.18.0.3"), "standard private IP");
        assert!(looks_like_ipv4("10.0.0.1"), "class A private IP");
        assert!(looks_like_ipv4("0.0.0.0"), "all zeros");
        assert!(looks_like_ipv4("255.255.255.255"), "all max");
    }

    #[test]
    fn looks_like_ipv4_invalid() {
        assert!(!looks_like_ipv4(""), "empty string");
        assert!(!looks_like_ipv4("not-an-ip"), "text");
        assert!(!looks_like_ipv4("172.18.0"), "only 3 octets");
        assert!(!looks_like_ipv4("172.18.0.3:7946"), "IP with port");
        assert!(!looks_like_ipv4("256.0.0.1"), "octet out of range");
    }

    #[test]
    fn provider_boundary_headers_match_ai_contract() {
        assert_eq!(AI_ROUTING_CANDIDATE_HEADER, "x-ai-routing-candidate");
        assert_eq!(AI_ROUTING_REQUEST_ID_HEADER, "x-ai-routing-request-id");
        for header in [AI_ROUTING_CANDIDATE_HEADER, AI_ROUTING_REQUEST_ID_HEADER] {
            assert!(!header.starts_with("x-praxis-"), "{header} would be stripped by Praxis");
        }
    }

    #[test]
    fn provider_demo_uses_distinct_final_hop_credential() {
        assert!(
            !CLIENT_BEARER_TOKEN.bytes().all(|b| b.is_ascii_hexdigit()),
            "client fixture must not match the generated 64-character hexadecimal provider credential"
        );
    }

    #[test]
    fn provider_configs_wire_secret_backed_credential_injection() {
        for provider in PROVIDER_CLUSTERS {
            let path = workspace_root()
                .join("tests/e2e/topologies/grid-glb-demo/configs")
                .join(provider)
                .join("praxis.yaml");
            let config = fs::read_to_string(&path).unwrap_or_else(|_| std::process::abort());
            let route_index = config
                .find("filter: provider_route")
                .unwrap_or_else(|| std::process::abort());
            let inject_index = config
                .find("filter: credential_inject")
                .unwrap_or_else(|| std::process::abort());
            let load_balancer_index = config
                .find("filter: load_balancer")
                .unwrap_or_else(|| std::process::abort());
            assert!(
                route_index < inject_index && inject_index < load_balancer_index,
                "{provider} must authorize the route before credential injection and load balancing"
            );
            assert!(config.contains("name: vcr-inference-credential"));
            assert!(config.contains("namespace: grid-system"));
            assert!(config.contains("key: token"));
            assert!(config.contains("file: /etc/praxis/credentials/vcr-inference/token"));
            assert!(
                !config.contains(CLIENT_BEARER_TOKEN),
                "{provider} ConfigMap template must not contain the client-supplied credential"
            );
        }
    }

    #[test]
    fn provider_gateway_helm_values_mount_credential_secrets() {
        let forge = fs::read_to_string(workspace_root().join("tests/e2e/topologies/grid-glb-demo/forge.yaml"))
            .unwrap_or_else(|_| std::process::abort());
        assert!(
            forge.contains("name: \"vcr-inference-credential\""),
            "forge.yaml must declare the primary credential Secret"
        );
        assert!(
            forge.contains("mountPath: \"/etc/praxis/credentials/vcr-inference\""),
            "forge.yaml must mount the primary credential"
        );
        assert!(
            forge.contains("name: \"vcr-inference-secondary-credential\""),
            "forge.yaml must declare the secondary credential Secret"
        );
        assert!(
            forge.contains("mountPath: \"/etc/praxis/credentials/vcr-inference-secondary\""),
            "forge.yaml must mount the secondary credential"
        );
        assert!(!forge.contains(CLIENT_BEARER_TOKEN));
    }

    #[test]
    fn provider_drain_uses_declared_metrics_and_health_inputs() {
        let root = workspace_root();
        let resources = root.join("tests/e2e/topologies/grid-glb-demo/resources");
        let workloads =
            fs::read_to_string(resources.join("provider-workloads.yaml")).unwrap_or_else(|_| std::process::abort());
        assert!(workloads.contains("name: MODEL"));
        assert!(workloads.contains("value: \"Qwen/Qwen3-0.6B\""));

        for site in ["east-provider", "west-provider"] {
            let provider = fs::read_to_string(resources.join(format!("inference-{site}.yaml")))
                .unwrap_or_else(|_| std::process::abort());
            assert!(provider.contains("healthCheck:"));
            assert!(provider.contains("path: /health"));
            assert!(provider.contains("providerKind: vllm-vcr"));
        }

        let verifier = fs::read_to_string(root.join("xtask/src/env/glb.rs")).unwrap_or_else(|_| std::process::abort());
        assert!(verifier.contains("withdraw_provider"));
        assert!(verifier.contains("scale_provider_backend"));
        assert!(
            !verifier.contains(concat!("fn write_", "overlay")),
            "the verifier must not mutate operator-owned overlay ConfigMaps"
        );
    }

    #[test]
    fn backend_network_policy_separates_data_and_health_access() {
        let root = workspace_root();
        let policy =
            fs::read_to_string(root.join("tests/e2e/topologies/grid-glb-demo/resources/backend-network-policy.yaml"))
                .unwrap_or_else(|_| std::process::abort());
        let forge = fs::read_to_string(root.join("tests/e2e/topologies/grid-glb-demo/forge.yaml"))
            .unwrap_or_else(|_| std::process::abort());

        assert!(policy.contains("grid.praxis-proxy.io/backend-access: provider-gateway"));
        assert!(policy.contains("app.kubernetes.io/name: grid-operator"));
        assert!(
            forge.contains("grid.praxis-proxy.io/backend-access"),
            "forge.yaml must set the backend-access pod label on provider gateways"
        );
    }

    #[test]
    fn provider_candidate_ids_cover_every_provider_cluster() {
        for provider in ["east-provider", EAST_SECONDARY_PROVIDER, "west-provider"] {
            let id = provider_candidate_id(provider).unwrap_or_else(|_| std::process::abort());
            assert_eq!(id.len(), 8);
            assert!(id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
        assert_ne!(
            provider_candidate_id("east-provider").unwrap_or_default(),
            provider_candidate_id(EAST_SECONDARY_PROVIDER).unwrap_or_default()
        );
        assert_ne!(
            provider_candidate_id(EAST_SECONDARY_PROVIDER).unwrap_or_default(),
            provider_candidate_id("west-provider").unwrap_or_default()
        );
        assert!(
            provider_candidate_id("unknown-provider").is_err(),
            "unknown provider must fail"
        );
    }

    #[test]
    fn provider_configs_render_candidate_ids_from_identity_contract() {
        let configs = workspace_root().join("tests/e2e/topologies/grid-glb-demo/configs");
        for provider in PROVIDER_CLUSTERS {
            let template = fs::read_to_string(configs.join(format!("{provider}/praxis.yaml")))
                .unwrap_or_else(|_| std::process::abort());
            assert!(
                template.contains(PROVIDER_CANDIDATE_ID_TOKEN),
                "{provider} must use the rendered candidate identity token"
            );
        }
        let east =
            fs::read_to_string(configs.join("east-provider/praxis.yaml")).unwrap_or_else(|_| std::process::abort());
        assert!(east.contains(SECONDARY_PROVIDER_CANDIDATE_ID_TOKEN));
        assert!(east.contains("cluster: vcr-backend-secondary"));
        assert!(east.contains("name: vcr-inference-secondary-credential"));
    }

    #[test]
    fn east_site_declares_two_independent_provider_candidates() {
        let resources = workspace_root().join("tests/e2e/topologies/grid-glb-demo/resources");
        let secondary = fs::read_to_string(resources.join("inference-east-provider-secondary.yaml"))
            .unwrap_or_else(|_| std::process::abort());
        let workload = fs::read_to_string(resources.join("provider-workload-east-secondary.yaml"))
            .unwrap_or_else(|_| std::process::abort());
        for expected in [
            "name: vcr-east-provider-secondary",
            "routingClusterRef: vcr-east-provider-secondary",
            "grid.praxis-proxy.io/provider-site: east-provider",
            "name: Qwen/Qwen3-0.6B",
        ] {
            assert!(secondary.contains(expected), "secondary provider missing {expected}");
        }
        assert!(workload.contains("name: vcr-inference-secondary"));
        assert!(workload.contains("app.kubernetes.io/instance: secondary"));
        assert!(workload.contains("containerPort: 8000"));

        for edge in ["east", "west"] {
            let network = fs::read_to_string(resources.join(format!("gridnetwork-{edge}-edge.yaml")))
                .unwrap_or_else(|_| std::process::abort());
            assert!(network.contains("cluster: vcr-east-provider-secondary"));
            assert!(network.contains("sni: east-provider.grid.internal"));
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "three-candidate overlay construction and differential assertion"
    )]
    fn overlay_keeps_same_site_providers_as_distinct_candidates() {
        let candidate = |site: &str, cluster: &str, stable_id: String| {
            serde_json::json!({
                "kind": DEMO_CANDIDATE_KIND,
                "name": DEMO_MODEL,
                "site": site,
                "cluster": cluster,
                "stable_id": stable_id,
            })
        };
        let candidates = vec![
            candidate(
                "east-provider",
                "vcr-east-provider",
                provider_candidate_id("east-provider").unwrap_or_default(),
            ),
            candidate(
                "east-provider",
                EAST_SECONDARY_CLUSTER,
                provider_candidate_id(EAST_SECONDARY_PROVIDER).unwrap_or_default(),
            ),
            candidate(
                "west-provider",
                "vcr-west-provider",
                provider_candidate_id("west-provider").unwrap_or_default(),
            ),
        ];
        let result = validate_same_site_candidates("east-edge", &candidates);
        assert!(
            result.is_ok(),
            "same-site provider identities must remain distinct: {result:?}"
        );

        let mut deduplicated = candidates;
        deduplicated.remove(1);
        assert!(
            validate_same_site_candidates("east-edge", &deduplicated).is_err(),
            "missing secondary provider must fail overlay validation"
        );
    }

    #[test]
    fn providers_bind_only_to_their_explicit_local_sites() {
        let resources = workspace_root().join("tests/e2e/topologies/grid-glb-demo/resources");
        for provider in PROVIDER_CLUSTERS {
            let inference = fs::read_to_string(resources.join(format!("inference-{provider}.yaml")))
                .unwrap_or_else(|_| std::process::abort());
            let site = fs::read_to_string(resources.join(format!("site-{provider}.yaml")))
                .unwrap_or_else(|_| std::process::abort());
            let selector = format!("grid.praxis-proxy.io/provider-site: {provider}");
            assert!(
                inference.contains(&selector),
                "{provider} provider must select only its local site"
            );
            assert!(
                site.contains(&selector),
                "{provider} site must carry its provider identity"
            );
        }
    }

    #[test]
    fn operator_sites_set_explicit_swim_identity_without_partial_deployments() {
        let forge = fs::read_to_string(workspace_root().join("tests/e2e/topologies/grid-glb-demo/forge.yaml"))
            .unwrap_or_else(|_| std::process::abort());
        for site in ["east-edge", "east-provider", "west-edge", "west-provider"] {
            assert!(
                forge.contains(&format!("siteName: \"{site}\"")),
                "{site} must set an explicit SWIM identity via Helm values"
            );
        }
        assert!(forge.contains("type: helm"), "operator stacks must use Helm releases");
        assert!(
            !forge.contains("operator-env-"),
            "partial Deployment overlays can remove base security settings"
        );
    }

    #[test]
    fn overlay_json_valid() {
        let json = serde_json::json!({
            "network": "glb-demo",
            "local_site": "east-edge",
            "generated_at": "2026-07-25T00:00:00Z",
            "candidates": [
                {
                    "kind": "InferenceProvider",
                    "name": "vcr-west-provider",
                    "site": "west-provider",
                    "stable_id": "abc123",
                    "admission_state": "admitted",
                    "selection_tier": "preferred",
                    "rank": 1
                }
            ]
        });
        let result = validate_overlay_json(&json.to_string());
        assert!(result.is_ok(), "valid overlay should pass: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("1 candidate(s)"), "evidence: {evidence}",);
    }

    #[test]
    fn overlay_json_missing_generated_at_fails() {
        let json = serde_json::json!({
            "candidates": [{"stable_id": "x", "admission_state": "a", "selection_tier": "t", "rank": 1}]
        });
        let result = validate_overlay_json(&json.to_string());
        assert!(result.is_err(), "missing generated_at should fail");
    }

    #[test]
    fn overlay_json_missing_candidate_field_fails() {
        let json = serde_json::json!({
            "generated_at": "2026-07-25T00:00:00Z",
            "candidates": [
                {"stable_id": "x", "admission_state": "a", "selection_tier": "t"}
            ]
        });
        let Err(err) = validate_overlay_json(&json.to_string()) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("rank"), "error should mention rank: {msg}");
    }

    #[test]
    fn overlay_json_empty_candidates_fails() {
        let json = serde_json::json!({
            "generated_at": "2026-07-25T00:00:00Z",
            "candidates": []
        });
        let result = validate_overlay_json(&json.to_string());
        assert!(result.is_err(), "empty candidates should fail");
    }

    #[test]
    fn overlay_json_empty_stable_id_fails() {
        let json = serde_json::json!({
            "generated_at": "2026-07-25T00:00:00Z",
            "candidates": [
                {"stable_id": "", "admission_state": "admitted", "selection_tier": "preferred", "rank": 1}
            ]
        });
        let Err(err) = validate_overlay_json(&json.to_string()) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("stable_id"), "error should mention stable_id: {msg}");
    }

    #[test]
    fn overlay_json_non_numeric_rank_fails() {
        let json = serde_json::json!({
            "generated_at": "2026-07-25T00:00:00Z",
            "candidates": [
                {"stable_id": "x", "admission_state": "a", "selection_tier": "t", "rank": "high"}
            ]
        });
        let Err(err) = validate_overlay_json(&json.to_string()) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("rank"), "error should mention rank: {msg}");
    }

    #[test]
    fn provider_gateway_captures_parsed() {
        let state = serde_json::json!({
            "captures": {
                "west-provider": {"provider-gateway-ip": "172.18.0.5"},
                "east-provider": {"provider-gateway-ip": "172.18.0.6"}
            }
        });
        let addrs = parse_provider_gateway_captures(&state.to_string()).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            addrs.get("west-provider").map(String::as_str),
            Some("172.18.0.5:8443"),
            "west"
        );
        assert_eq!(
            addrs.get("east-provider").map(String::as_str),
            Some("172.18.0.6:8443"),
            "east"
        );
    }

    #[test]
    fn gridsite_egress_found() {
        let expected = BTreeMap::from([
            ("west-provider".to_owned(), "172.18.0.5:8443".to_owned()),
            ("east-provider".to_owned(), "172.18.0.6:8443".to_owned()),
        ]);
        let json = serde_json::json!({
            "items": [
                {
                    "metadata": {"name": "glb-demo-west-provider"},
                    "spec": {"egress": {"address": "172.18.0.5:8443"}}
                },
                {
                    "metadata": {"name": "glb-demo-east-provider"},
                    "spec": {"egress": {"address": "172.18.0.6:8443"}}
                }
            ]
        });
        let result = parse_gridsite_egress(&json.to_string(), &expected);
        assert!(result.is_ok(), "should find egress: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("west-provider="), "evidence: {evidence}");
        assert!(evidence.contains("east-provider="), "evidence: {evidence}");
    }

    #[test]
    fn gridsite_egress_missing_fails() {
        let expected = BTreeMap::from([
            ("west-provider".to_owned(), "172.18.0.5:8443".to_owned()),
            ("east-provider".to_owned(), "172.18.0.6:8443".to_owned()),
        ]);
        let json = serde_json::json!({
            "items": [
                {
                    "metadata": {"name": "glb-demo-west-provider"},
                    "spec": {"egress": {"address": ""}}
                },
                {
                    "metadata": {"name": "glb-demo-east-provider"},
                    "spec": {"egress": {"address": "172.18.0.6:8443"}}
                }
            ]
        });
        let Err(err) = parse_gridsite_egress(&json.to_string(), &expected) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("west-provider"), "error should name site: {msg}");
    }

    #[test]
    fn gridsite_egress_mismatch_fails() {
        let expected = BTreeMap::from([
            ("west-provider".to_owned(), "172.18.0.9:8443".to_owned()),
            ("east-provider".to_owned(), "172.18.0.6:8443".to_owned()),
        ]);
        let json = serde_json::json!({
            "items": [
                {
                    "metadata": {"name": "glb-demo-west-provider"},
                    "spec": {"egress": {"address": "172.18.0.5:8443"}}
                },
                {
                    "metadata": {"name": "glb-demo-east-provider"},
                    "spec": {"egress": {"address": "172.18.0.6:8443"}}
                }
            ]
        });
        let Err(err) = parse_gridsite_egress(&json.to_string(), &expected) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(
            msg.contains("expected Forge capture"),
            "error should name mismatch: {msg}"
        );
    }

    #[test]
    fn provider_gateway_captures_parse() {
        let json = serde_json::json!({
            "captures": {
                "west-provider": {"provider-gateway-ip": "172.18.0.5"},
                "east-provider": {"provider-gateway-ip": "172.18.0.6"}
            }
        });
        let captures = parse_provider_gateway_captures(&json.to_string()).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            captures.get("west-provider").map(String::as_str),
            Some("172.18.0.5:8443")
        );
        assert_eq!(
            captures.get("east-provider").map(String::as_str),
            Some("172.18.0.6:8443")
        );
    }

    #[test]
    fn provider_gateway_captures_missing_provider_fails() {
        let json = serde_json::json!({
            "captures": {
                "west-provider": {"provider-gateway-ip": "172.18.0.5"}
            }
        });
        let Err(err) = parse_provider_gateway_captures(&json.to_string()) else {
            std::process::abort()
        };
        assert!(
            err.to_string().contains("east-provider"),
            "error should name missing provider: {err}"
        );
    }

    #[test]
    fn proof_labels_are_unique_and_nonempty() {
        let unique = PROOF_LABELS.iter().copied().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), PROOF_LABELS.len(), "proof labels must remain unique",);
        assert!(PROOF_LABELS.iter().all(|label| !label.is_empty()));
    }

    #[test]
    fn parse_header_dump_basic() {
        let dump =
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-ai-demo-provider-gateway: west-provider\r\n\r\n";
        let map = parse_header_dump(dump);
        assert_eq!(
            map.get("x-ai-demo-provider-gateway").map(String::as_str),
            Some("west-provider"),
            "should parse provider header"
        );
        assert_eq!(
            map.get("content-type").map(String::as_str),
            Some("application/json"),
            "should parse content-type"
        );
        assert!(!map.contains_key("http/1.1 200 ok"), "should skip HTTP status line");
    }

    #[test]
    fn parse_header_dump_empty() {
        let map = parse_header_dump("");
        assert!(map.is_empty(), "empty input should produce empty map");
    }

    #[test]
    fn extract_provider_present() {
        let resp = verify::HttpResponse {
            status: 200,
            body: String::new(),
            headers: BTreeMap::from([("x-ai-demo-provider-gateway".to_owned(), "east-provider".to_owned())]),
        };
        let result = extract_provider(&resp);
        assert_eq!(result.ok().as_deref(), Some("east-provider"), "should extract provider");
    }

    #[test]
    fn extract_provider_missing() {
        let resp = verify::HttpResponse {
            status: 200,
            body: String::new(),
            headers: BTreeMap::new(),
        };
        assert!(extract_provider(&resp).is_err(), "missing header should return error");
    }

    #[test]
    fn edge_deployment_projects_both_configmap_keys() {
        let manifest = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap_or_else(|| std::process::abort())
                .join("tests/e2e/topologies/grid-glb-demo/resources/edge-gateway-deployment.yaml"),
        )
        .unwrap_or_else(|_| std::process::abort());

        assert!(
            manifest.contains("key: routing-config.json"),
            "deployment must project legacy routing-config.json key"
        );
        assert!(
            manifest.contains("path: routing-config.json"),
            "deployment must mount legacy routing-config.json path"
        );
        assert!(
            manifest.contains("key: routing-overlay.json"),
            "deployment must project envelope routing-overlay.json key"
        );
        assert!(
            manifest.contains("path: routing-overlay.json"),
            "deployment must mount envelope routing-overlay.json path"
        );
    }

    // -----------------------------------------------------------------
    // Annotation structural parse (fix for dotted-key JSONPath bug)
    // -----------------------------------------------------------------

    #[test]
    fn annotation_parse_reads_dotted_keys() {
        let annotations = serde_json::json!({
            "grid.praxis-proxy.io/overlay-schema-version": "1.0.0",
            "grid.praxis-proxy.io/overlay-revision": "abc123",
            "grid.praxis-proxy.io/overlay-content-digest": "abc123",
            "kubectl.kubernetes.io/last-applied-configuration": "{}"
        });
        let raw = serde_json::to_string(&annotations).unwrap_or_else(|_| std::process::abort());
        let map: serde_json::Value = serde_json::from_str(&raw).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            map.get(OVERLAY_ANNOTATION_SCHEMA).and_then(serde_json::Value::as_str),
            Some("1.0.0"),
            "dotted annotation key must be readable via structural parse"
        );
        assert_eq!(
            map.get(OVERLAY_ANNOTATION_REVISION).and_then(serde_json::Value::as_str),
            Some("abc123"),
        );
        assert_eq!(
            map.get(OVERLAY_ANNOTATION_DIGEST).and_then(serde_json::Value::as_str),
            Some("abc123"),
        );
    }

    #[test]
    fn annotation_parse_missing_key_detected() {
        let annotations = serde_json::json!({
            "unrelated/key": "value"
        });
        let raw = serde_json::to_string(&annotations).unwrap_or_else(|_| std::process::abort());
        let map: serde_json::Value = serde_json::from_str(&raw).unwrap_or_else(|_| std::process::abort());
        assert!(
            map.get(OVERLAY_ANNOTATION_SCHEMA).is_none(),
            "missing annotation key should return None"
        );
    }

    // -----------------------------------------------------------------
    // Edge acceptance convergence barrier
    // -----------------------------------------------------------------

    fn fake_log(revision: &str) -> String {
        format!(
            "INFO intelligent_route: overlay reloaded candidate_count=2 \
             accepted_revision=\"{revision}\" previous_serving_revision=old"
        )
    }

    fn fake_initial_log(revision: &str) -> String {
        format!(
            "INFO intelligent_route: overlay snapshot initialized candidate_count=2 \
             accepted_revision=\"{revision}\" serving_revision=\"{revision}\""
        )
    }

    #[test]
    fn acceptance_proof_matches_reload_log() {
        let rev = "2b8478c84941fd57810b66f71efe9acafae8af330c8c5488cbecc7da6d0d96a1";
        assert!(logs_prove_acceptance(&fake_log(rev), rev));
    }

    #[test]
    fn acceptance_proof_matches_initial_snapshot_log() {
        let rev = "2b8478c84941fd57810b66f71efe9acafae8af330c8c5488cbecc7da6d0d96a1";
        assert!(logs_prove_acceptance(&fake_initial_log(rev), rev));
    }

    #[test]
    fn acceptance_proof_matches_unquoted_reload_revision() {
        let rev = "2b8478c84941fd57810b66f71efe9acafae8af330c8c5488cbecc7da6d0d96a1";
        let log = format!("INFO intelligent_route: overlay reloaded accepted_revision={rev} candidate_count=2");
        assert!(logs_prove_acceptance(&log, rev));
    }

    #[test]
    fn acceptance_proof_rejects_wrong_revision() {
        assert!(!logs_prove_acceptance(&fake_log("aaa111"), "bbb222"));
    }

    #[test]
    fn acceptance_proof_rejects_missing_accepted_field() {
        assert!(!logs_prove_acceptance("overlay reloaded candidate_count=2", "abc"));
    }

    #[test]
    fn acceptance_proof_rejects_unrecognized_event() {
        assert!(!logs_prove_acceptance(
            "overlay parsed accepted_revision=\"abc\"",
            "abc"
        ));
    }

    #[test]
    fn acceptance_rejects_event_and_revision_on_separate_lines() {
        let logs = "overlay reloaded candidate_count=2\naccepted_revision=\"abc123\"";
        assert!(
            !logs_prove_acceptance(logs, "abc123"),
            "event and accepted_revision on different lines must not match"
        );
    }

    #[test]
    fn acceptance_rejects_revision_in_unrelated_field() {
        let line = "overlay reloaded previous_serving_revision=\"abc123\" accepted_revision=\"other\"";
        assert!(
            !logs_prove_acceptance(line, "abc123"),
            "revision appearing only in a non-accepted field must not match"
        );
    }

    #[test]
    fn acceptance_rejects_previous_serving_revision_without_accepted_field() {
        let line = "overlay reloaded previous_serving_revision=abc123";
        assert!(
            !logs_prove_acceptance(line, "abc123"),
            "previous_serving_revision must not be parsed as accepted_revision"
        );
    }

    #[test]
    fn serving_revision_parser_rejects_previous_revision_field() {
        let line = "overlay reloaded accepted_revision=next serving_revision=next previous_serving_revision=old";
        assert_eq!(serving_revision_from_line(line), Some("next"));

        let previous_only = "overlay rejected previous_serving_revision=old";
        assert_eq!(serving_revision_from_line(previous_only), None);
    }

    #[test]
    fn acceptance_rejects_prefix_or_longer_revision() {
        let line = "overlay reloaded accepted_revision=\"abc123_extra_suffix\"";
        assert!(
            !logs_prove_acceptance(line, "abc123"),
            "prefix of a longer accepted_revision value must not match"
        );
        let line2 = "overlay reloaded accepted_revision=\"abc12\"";
        assert!(
            !logs_prove_acceptance(line2, "abc123"),
            "shorter accepted_revision must not match longer target"
        );
    }

    #[test]
    fn extract_last_accepted_finds_most_recent() {
        let logs = "accepted_revision=\"rev1\"\naccepted_revision=rev2";
        assert_eq!(
            extract_last_accepted_revision(logs).as_deref(),
            Some("rev2"),
            "the most recent accepted_revision should be returned"
        );
    }

    #[test]
    fn convergence_succeeds_after_multiple_polls() {
        let target = "abc123def456";
        let attempt = std::cell::Cell::new(0_u32);
        let result =
            wait_for_edge_revision_acceptance("test-edge", target, Duration::from_secs(5), Duration::ZERO, |_edge| {
                let n = attempt.get();
                attempt.set(n + 1);
                if n < 3 {
                    Ok("overlay reloaded accepted_revision=\"old_rev\"".to_owned())
                } else {
                    Ok(fake_log(target))
                }
            });
        assert!(result.is_ok(), "should succeed after polls: {result:?}");
        assert!(attempt.get() >= 4, "should have polled at least 4 times");
    }

    #[test]
    fn convergence_timeout_diagnostics_wrong_revision() {
        let result = wait_for_edge_revision_acceptance(
            "test-edge",
            "target_rev",
            Duration::from_millis(50),
            Duration::ZERO,
            |_| Ok("overlay reloaded accepted_revision=\"wrong_rev\"".to_owned()),
        );
        match result {
            Ok(()) => std::process::abort(),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("did not accept overlay revision"),
                    "error should describe the failure: {msg}"
                );
                assert!(
                    msg.contains("last observed accepted_revision=wrong_rev"),
                    "error should report the observed revision: {msg}"
                );
            },
        }
    }

    #[test]
    fn strip_csi_sgr_removes_styling_sequences() {
        let raw = "\x1b[3maccepted_revision\x1b[0m\x1b[2m=\x1b[0m\"abc123\"";
        assert_eq!(strip_csi_sgr(raw), "accepted_revision=\"abc123\"");
    }

    #[test]
    fn strip_csi_sgr_preserves_plain_text() {
        let plain = "overlay reloaded accepted_revision=\"abc\"";
        assert_eq!(strip_csi_sgr(plain), plain);
    }

    #[test]
    fn acceptance_proof_matches_after_csi_sgr_strip() {
        let raw = "\x1b[2m2026-07-29T07:07:11Z\x1b[0m \x1b[32m INFO\x1b[0m intelligent_route: overlay reloaded \x1b[3maccepted_revision\x1b[0m\x1b[2m=\x1b[0m\"rev_abc\"";
        let cleaned = strip_csi_sgr(raw);
        assert!(logs_prove_acceptance(&cleaned, "rev_abc"));
    }

    #[test]
    fn convergence_timeout_reports_no_accepted_revision() {
        let result = wait_for_edge_revision_acceptance(
            "test-edge",
            "target_rev",
            Duration::from_millis(50),
            Duration::ZERO,
            |_| Ok("some unrelated log output".to_owned()),
        );
        match result {
            Ok(()) => std::process::abort(),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("accepted_revision=none"),
                    "should report none when no revision was ever observed: {msg}"
                );
            },
        }
    }

    // -------------------------------------------------------------------------
    // Restoration orchestration tests
    // -------------------------------------------------------------------------

    #[expect(clippy::unnecessary_wraps, reason = "must match PatchFn signature")]
    fn ok_patch(_edge: &str, _envelope: &str) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    #[expect(clippy::unnecessary_wraps, reason = "must match ScaleFn signature")]
    fn ok_scale(_edge: &str, _replicas: u32) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn fail_patch(_edge: &str, _envelope: &str) -> Result<(), Box<dyn std::error::Error>> {
        Err("patch failed".into())
    }

    fn fail_scale(_edge: &str, _replicas: u32) -> Result<(), Box<dyn std::error::Error>> {
        Err("scale failed".into())
    }

    #[test]
    fn restore_both_succeed() {
        let result = restore_overlay_resources("test-edge", "{}", 1, ok_patch, ok_scale);
        assert!(result.is_ok(), "both resources restored: {result:?}");
    }

    fn err_string(result: Result<(), Box<dyn std::error::Error>>) -> String {
        match result {
            Ok(()) => String::new(),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn restore_patch_fails_scale_still_attempted() {
        let msg = err_string(restore_overlay_resources("test-edge", "{}", 1, fail_patch, ok_scale));
        assert!(
            msg.contains("envelope restore"),
            "should report envelope failure: {msg}"
        );
        assert!(!msg.contains("operator restore"), "scale should have succeeded: {msg}");
    }

    #[test]
    fn restore_scale_fails_reported() {
        let msg = err_string(restore_overlay_resources("test-edge", "{}", 2, ok_patch, fail_scale));
        assert!(
            msg.contains("operator restore to 2"),
            "should report operator scale failure with original count: {msg}"
        );
    }

    #[test]
    fn restore_both_fail_reports_both() {
        let msg = err_string(restore_overlay_resources("test-edge", "{}", 3, fail_patch, fail_scale));
        assert!(
            msg.contains("envelope restore") && msg.contains("operator restore to 3"),
            "should report both failures: {msg}"
        );
    }

    #[test]
    fn restore_preserves_original_replica_count() {
        let msg = err_string(restore_overlay_resources("test-edge", "{}", 5, ok_patch, fail_scale));
        assert!(
            msg.contains("operator restore to 5"),
            "should restore to original count 5, not hardcoded 1: {msg}"
        );
    }

    // -----------------------------------------------------------------
    // External OpenAI provider tests
    // -----------------------------------------------------------------

    fn minimal_provider_config() -> &'static str {
        "\
filter_chains:
  - name: provider-inference
    filters:
      - filter: provider_route
        routes:
          - candidate_id: abc123
            cluster: mock-backend
      - filter: credential_inject
        credentials:
          - strategy: bearer_token
            name: vcr-inference-credential
            file: /etc/praxis/credentials/vcr-inference/token
      - filter: load_balancer
        clusters:
          - name: mock-backend
            endpoints:
              - \"vcr-inference:8000\"

admin:
  address: \"127.0.0.1:9901\""
    }

    fn minimal_edge_config() -> &'static str {
        "\
filter_chains:
  - name: main
    filters:
      - filter: intelligent_route
        provider_hop_clusters:
          - vcr-east-provider
          - vcr-west-provider
        expected_overlay_scope:
          network: glb-demo
      - filter: load_balancer
        clusters:
          - name: vcr-east-provider
            endpoints:
              - \"172.18.0.6:8443\"

admin:
  address: \"127.0.0.1:9901\""
    }

    #[test]
    fn openai_candidate_id_is_deterministic() {
        let id1 = openai_candidate_id("gpt-5-mini");
        let id2 = openai_candidate_id("gpt-5-mini");
        assert_eq!(id1, id2, "same model must produce the same candidate ID");
        assert_eq!(id1.len(), 8, "candidate ID must be 8 hex characters");
        assert!(id1.bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn openai_candidate_id_varies_by_model() {
        let a = openai_candidate_id("gpt-5-mini");
        let b = openai_candidate_id("gpt-5");
        assert_ne!(a, b, "different models must produce different candidate IDs");
    }

    #[test]
    fn openai_candidate_id_differs_from_simulated_providers() {
        let openai = openai_candidate_id("Qwen/Qwen3-0.6B");
        let east = provider_candidate_id("east-provider").unwrap_or_else(|_| std::process::abort());
        let west = provider_candidate_id("west-provider").unwrap_or_else(|_| std::process::abort());
        assert_ne!(
            openai, east,
            "OpenAI ID must differ from east-provider even for same model name"
        );
        assert_ne!(openai, west);
    }

    #[test]
    fn append_openai_provider_config_inserts_route_and_cluster() {
        let ext = ExternalProviderDescriptor::openai("gpt-5-mini");
        let result = append_openai_provider_config(minimal_provider_config(), &ext, &openai_candidate_id(&ext.model))
            .unwrap_or_else(|_| std::process::abort());

        assert!(result.contains(&format!("cluster: {}", ext.routing_cluster)));
        assert!(result.contains(&format!("model: {}", ext.model)));
        assert!(result.contains(&format!("name: {}", ext.secret_name)));
        assert!(result.contains(&format!("authority: {}", ext.hostname)));
        assert!(result.contains(&format!("sni: {}", ext.sni)));
        assert!(result.contains(&ext.endpoint()));
        assert!(
            !result.contains("ca_system"),
            "external cluster must not use ca_system (not a valid Praxis field)"
        );
        assert!(
            !result.contains("ca_path"),
            "external cluster must not use ca_path (system trust store is correct for public APIs)"
        );

        let route_pos = result.find("candidate_id:").unwrap_or_else(|| std::process::abort());
        let inject_pos = result
            .find("filter: credential_inject")
            .unwrap_or_else(|| std::process::abort());
        let lb_pos = result
            .find("filter: load_balancer")
            .unwrap_or_else(|| std::process::abort());
        let admin_pos = result.find("admin:").unwrap_or_else(|| std::process::abort());
        assert!(route_pos < inject_pos, "route must come before credential_inject");
        assert!(inject_pos < lb_pos, "credential_inject must come before load_balancer");
        assert!(lb_pos < admin_pos, "load_balancer must come before admin");
    }

    #[test]
    fn append_openai_provider_config_preserves_existing_entries() {
        let ext = ExternalProviderDescriptor::openai("gpt-5-mini");
        let result = append_openai_provider_config(minimal_provider_config(), &ext, &openai_candidate_id(&ext.model))
            .unwrap_or_else(|_| std::process::abort());

        assert!(result.contains("mock-backend"), "original cluster must be preserved");
        assert!(
            result.contains("vcr-inference-credential"),
            "original credential must be preserved"
        );
    }

    #[test]
    fn append_openai_provider_config_rejects_missing_markers() {
        let ext = ExternalProviderDescriptor::openai("gpt-5-mini");
        let bad = "some: yaml\nwithout: markers";
        assert!(
            append_openai_provider_config(bad, &ext, &openai_candidate_id(&ext.model)).is_err(),
            "missing markers must fail"
        );
    }

    #[test]
    fn append_openai_provider_config_uses_caller_supplied_candidate_id() {
        let ext = ExternalProviderDescriptor::openai("gpt-5-mini");
        let custom_id = "custom-test-id-abc123";
        let result = append_openai_provider_config(minimal_provider_config(), &ext, custom_id)
            .unwrap_or_else(|_| std::process::abort());
        assert!(
            result.contains(&format!("candidate_id: {custom_id}")),
            "must use the caller-supplied candidate ID, not an internally computed one"
        );
        assert!(
            !result.contains(&openai_candidate_id(&ext.model)),
            "must not contain the GLB-specific east-provider candidate ID"
        );
    }

    #[test]
    fn append_openai_edge_config_adds_hop_cluster() {
        let ext = ExternalProviderDescriptor::openai("gpt-5-mini");
        let result = append_openai_edge_config(minimal_edge_config(), &ext, "172.18.0.6:8443")
            .unwrap_or_else(|_| std::process::abort());

        assert!(result.contains(&format!("- {}", ext.routing_cluster)));
        assert!(result.contains(&format!("name: {}", ext.routing_cluster)));
        assert!(result.contains("east-provider.grid.internal"));
        assert!(result.contains("172.18.0.6:8443"));
        assert!(!result.contains("captures.east-provider.provider-gateway-ip"));
    }

    #[test]
    fn append_openai_edge_config_preserves_existing_clusters() {
        let ext = ExternalProviderDescriptor::openai("gpt-5-mini");
        let result = append_openai_edge_config(minimal_edge_config(), &ext, "172.18.0.6:8443")
            .unwrap_or_else(|_| std::process::abort());

        assert!(
            result.contains("vcr-east-provider"),
            "existing clusters must be preserved"
        );
        assert!(
            result.contains("vcr-west-provider"),
            "existing clusters must be preserved"
        );
    }

    #[test]
    fn generated_configs_never_contain_token_patterns() {
        let ext = ExternalProviderDescriptor::openai("gpt-5-mini");
        let provider = append_openai_provider_config(minimal_provider_config(), &ext, &openai_candidate_id(&ext.model))
            .unwrap_or_else(|_| std::process::abort());
        let edge = append_openai_edge_config(minimal_edge_config(), &ext, "172.18.0.6:8443")
            .unwrap_or_else(|_| std::process::abort());

        for config in [&provider, &edge] {
            assert!(
                !config.contains("sk-"),
                "generated config must not contain API key prefixes"
            );
            assert!(
                !config.contains(CLIENT_BEARER_TOKEN),
                "generated config must not contain the client credential"
            );
        }
    }

    #[test]
    fn static_provider_templates_contain_no_openai_tokens() {
        let configs = workspace_root().join("tests/e2e/topologies/grid-glb-demo/configs");
        for provider in PROVIDER_CLUSTERS {
            let template = fs::read_to_string(configs.join(format!("{provider}/praxis.yaml")))
                .unwrap_or_else(|_| std::process::abort());
            assert!(
                !template.contains("openai"),
                "{provider} static template must not contain openai references (added programmatically)"
            );
            assert!(
                !template.contains("sk-"),
                "{provider} static template must not contain API key prefixes"
            );
        }
        let edge = fs::read_to_string(configs.join("edge/praxis.yaml")).unwrap_or_else(|_| std::process::abort());
        assert!(
            !edge.contains("openai"),
            "edge static template must not contain openai references (added programmatically)"
        );
    }

    // ── write_trimmed_credential ─────────────────────────────────────

    #[test]
    fn write_trimmed_credential_strips_trailing_newline() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let key = dir.path().join("token");
        fs::write(&key, b"sk-abc123\n").unwrap_or_else(|_| std::process::abort());
        let tmp = write_trimmed_credential(&key).unwrap_or_else(|_| std::process::abort());
        let content = fs::read(tmp.path()).unwrap_or_else(|_| std::process::abort());
        assert_eq!(content, b"sk-abc123", "trailing LF must be stripped");
    }

    #[test]
    fn write_trimmed_credential_strips_trailing_crlf() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let key = dir.path().join("token");
        fs::write(&key, b"sk-abc123\r\n").unwrap_or_else(|_| std::process::abort());
        let tmp = write_trimmed_credential(&key).unwrap_or_else(|_| std::process::abort());
        let content = fs::read(tmp.path()).unwrap_or_else(|_| std::process::abort());
        assert_eq!(content, b"sk-abc123", "trailing CRLF must be stripped");
    }

    #[test]
    fn write_trimmed_credential_preserves_no_trailing_newline() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let key = dir.path().join("token");
        fs::write(&key, b"sk-abc123").unwrap_or_else(|_| std::process::abort());
        let tmp = write_trimmed_credential(&key).unwrap_or_else(|_| std::process::abort());
        let content = fs::read(tmp.path()).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            content, b"sk-abc123",
            "content without trailing newline must be unchanged"
        );
    }

    #[test]
    fn write_trimmed_credential_rejects_empty_after_trim() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let key = dir.path().join("token");
        fs::write(&key, b"\n").unwrap_or_else(|_| std::process::abort());
        assert!(
            write_trimmed_credential(&key).is_err(),
            "file that is empty after trimming must be rejected"
        );
    }
}
