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

use sha2::{Digest as _, Sha256};

use crate::env::{StepResult, StepStatus, certs, kubectl, print_validate_all_table, safe_truncate_str, verify};

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
const SWIM_LB_SERVICE: &str = "operator-swim-lb";

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

/// Queue depth that admits both new and existing sessions.
const PROVIDER_QUEUE_READY: &str = "0.10";

/// Queue depth above the operator's `existing_only` admission threshold.
const PROVIDER_QUEUE_DRAINING: &str = "0.95";

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
const PROVIDER_CREDENTIAL_SECRET: &str = "mock-inference-credential";

/// Token replaced with the generated edge certificate digest.
const EDGE_US_EAST_CERT_DIGEST_TOKEN: &str = "__EDGE_US_EAST_CERT_SHA256__";

/// Provider-config token replaced with the west edge certificate digest.
const EDGE_US_WEST_CERT_DIGEST_TOKEN: &str = "__EDGE_US_WEST_CERT_SHA256__";

/// Provider-config token replaced with the deterministic candidate ID.
const PROVIDER_CANDIDATE_ID_TOKEN: &str = "__GRID_CANDIDATE_ID__";

/// Candidate kind used by the GLB provider fixture.
const DEMO_CANDIDATE_KIND: &str = "inference_model";

/// Model name used by the GLB provider fixture.
const DEMO_MODEL: &str = "sim-model-v1";

/// AI-owned candidate identity header on the authenticated provider hop.
const GRID_PEER_SELECTED_CANDIDATE_HEADER: &str = "x-grid-peer-selected-candidate";

/// AI-owned request correlation header on the authenticated provider hop.
const GRID_PEER_HOP_REQUEST_ID_HEADER: &str = "x-grid-peer-hop-request-id";

/// Provider-owned response attribution emitted by `grid_provider_route`.
const PROVIDER_GATEWAY_RESPONSE_HEADER: &str = "x-grid-demo-provider-gateway";

/// Safe backend capture of provider-owned attribution.
const BACKEND_PROVIDER_CAPTURE_HEADER: &str = "x-grid-demo-backend-provider-attribution";

/// Safe backend capture of the provider-owned request ID.
const BACKEND_REQUEST_ID_CAPTURE_HEADER: &str = "x-grid-demo-backend-request-id";

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
    stage_provider_boundary()?;
    install_provider_boundary()
}

/// Generate and stage all local GLB identities and rendered provider configs.
///
/// This phase does not require Kubernetes. It runs before the provider
/// workload stack so no pod is created with unresolved trust placeholders.
pub(crate) fn stage_provider_boundary() -> Result<(), Box<dyn std::error::Error>> {
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
    stage_gtm_tls()?;
    let east_edge_digest = certificate_sha256(&Path::new(GENERATED_CERTS_DIR).join("east-edge-cert.pem"))?;
    let west_edge_digest = certificate_sha256(&Path::new(GENERATED_CERTS_DIR).join("west-edge-cert.pem"))?;
    stage_provider_configs(&east_edge_digest, &west_edge_digest)?;
    Ok(())
}

/// Install staged provider identities, configs, and backend credentials.
///
/// The `grid-system` namespace must exist. Provider deployments may already
/// exist or may be applied after this function returns.
pub(crate) fn install_provider_boundary() -> Result<(), Box<dyn std::error::Error>> {
    ensure_demo_namespace("gtm-emulator")?;
    for edge in EDGE_CLUSTERS {
        apply_identity_tls_secret(edge, edge, EDGE_TLS_SECRET)?;
    }
    apply_gtm_tls_secret()?;
    for provider in PROVIDER_CLUSTERS {
        let provider_credential = generate_provider_credential()?;
        apply_provider_config(provider)?;
        apply_provider_tls_secret(provider)?;
        apply_provider_credential_secret(provider, &provider_credential)?;
        restart_provider_deployments_if_present(provider)?;
    }
    eprintln!(
        "grid-routing: edge identities, GTM certificate, provider configs, mTLS material, and credentials installed"
    );
    Ok(())
}

/// Restart and await provider workloads when they have already been applied.
fn restart_provider_deployments_if_present(provider: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = kubectl_context(provider);
    for deployment in ["mock-inference", "provider-gateway"] {
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

/// Compute the SHA-256 fingerprint of a PEM certificate.
fn certificate_sha256(cert_path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("openssl")
        .args(["x509", "-in", &cert_path.display().to_string(), "-outform", "DER"])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("failed to decode edge certificate: {}", stderr.trim()).into());
    }
    Ok(format!("{:x}", Sha256::digest(output.stdout)))
}

/// Compute the control-plane trust fingerprint for one generated site certificate.
///
/// Unlike [`certificate_sha256`], `GridSite` trust hashes the normalized PEM
/// bytes because that is the representation distributed through SWIM.
pub(crate) fn site_certificate_fingerprint(site: &str) -> Result<String, Box<dyn std::error::Error>> {
    let path = Path::new(GENERATED_CERTS_DIR).join(format!("{site}-cert.pem"));
    let pem = fs::read_to_string(&path)?;
    Ok(crate::env::operator::sha256_fingerprint(&pem))
}

/// Render provider gateway configs with the edge certificate digest.
fn stage_provider_configs(east_edge_digest: &str, west_edge_digest: &str) -> Result<(), Box<dyn std::error::Error>> {
    for provider in PROVIDER_CLUSTERS {
        let source = Path::new("environments/grid-glb-demo/configs")
            .join(provider)
            .join("praxis.yaml");
        let template = fs::read_to_string(&source)?;
        if !template.contains(EDGE_US_EAST_CERT_DIGEST_TOKEN)
            || !template.contains(EDGE_US_WEST_CERT_DIGEST_TOKEN)
            || !template.contains(PROVIDER_CANDIDATE_ID_TOKEN)
        {
            return Err(format!(
                "provider config template is missing an identity token: {}",
                source.display()
            )
            .into());
        }
        let rendered = template
            .replace(EDGE_US_EAST_CERT_DIGEST_TOKEN, east_edge_digest)
            .replace(EDGE_US_WEST_CERT_DIGEST_TOKEN, west_edge_digest)
            .replace(PROVIDER_CANDIDATE_ID_TOKEN, &provider_candidate_id(provider)?);
        let target_dir = Path::new(".forge/runtime/glb-tls/provider-configs").join(provider);
        fs::create_dir_all(&target_dir)?;
        fs::write(target_dir.join("praxis.yaml"), rendered)?;
    }
    Ok(())
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
    let manifest = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": PROVIDER_CREDENTIAL_SECRET,
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
    eprintln!("grid-routing: checking prerequisites...");
    let ctx = check_prerequisites(forge_config)?;

    let mut results: Vec<StepResult> = Vec::new();
    run_steps(&ctx, &mut results);

    eprintln!();
    eprintln!("## Grid Routing And Provider-Boundary Proof");
    eprintln!();
    eprintln!(
        "User story: As a Grid and provider operator, I need discovered provider changes to update live edge routing while authenticated provider gateways protect private backends."
    );
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
        eprintln!("grid-routing: all proof points PASS");
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
        _ => {},
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
fn run_steps(ctx: &PrereqContext, results: &mut Vec<StepResult>) {
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
    let clusters_ok = record_step("clusters live", results, || check_clusters_live(&status_json));
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

    // Overlay metadata.
    proof_banner("checking overlay candidate metadata");
    record_step("overlay metadata", results, check_overlay_metadata);

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
    record_step("backend network policy", results, check_backend_network_policy);

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
        block_remaining("session affinity bind", "initial inference failed", results);
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

    // Session drain setup.
    proof_banner("setting up provider drain");
    match setup_session_drain(PRIMARY_EDGE, &provider_a) {
        Ok(evidence) => {
            results.push(StepResult::pass("session drain setup", evidence));
        },
        Err(e) => {
            results.push(StepResult::fail("session drain setup", e.as_ref()));
            block_remaining("session drain verified", "drain setup failed", results);
            return;
        },
    };

    // Session drain routing verification.
    proof_banner("checking provider drain");
    let drain_proof = check_session_drain(EDGE_PORT, &provider_a);
    let drain_restore = restore_provider_admission(PRIMARY_EDGE, &provider_a);
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
    record_step("routing after reload", results, check_inference_routed);

    // Restore the provider and require the operator-generated overlay to recover.
    let restore_result = restore_withdrawn_provider(PRIMARY_EDGE, &withdrawal);

    // Edge pod stability.
    proof_banner("checking edge pod stability");
    record_step("edge pod stable", results, move || {
        let restore_evidence = restore_result?;
        let stable_evidence = check_edge_pod_stable(PRIMARY_EDGE, &edge_identity)?;
        Ok(format!("{stable_evidence}; {restore_evidence}"))
    });
}

/// Print a step progress banner.
fn proof_banner(description: &str) {
    eprintln!("grid-routing: {description}...");
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
    "session affinity bind",
    "session affinity reuse",
    "session drain setup",
    "session drain verified",
    "provider withdrawn",
    "hot-reload observed",
    "routing after reload",
    "edge pod stable",
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
        let svc_type = kubectl_jsonpath(&ctx, "svc", "mock-inference", "{.spec.type}")?;
        if svc_type != "ClusterIP" {
            return Err(format!("{provider}: backend type is {svc_type}, expected ClusterIP").into());
        }
        evidence.push(format!("{provider}: type={svc_type}"));
    }
    Ok(evidence.join("; "))
}

/// Prove the backend ingress policy with differential runtime probes.
///
/// The same image and target are used for both probes. A pod carrying the
/// provider-gateway access label must connect, while an otherwise identical
/// unlabeled pod must be denied. This detects actual enforcement rather than
/// inferring support from the CNI name or the presence of a manifest.
#[expect(
    clippy::too_many_lines,
    reason = "sequential network probes for a single verification step"
)]
fn check_backend_network_policy() -> Result<String, Box<dyn std::error::Error>> {
    let mut evidence = Vec::new();
    for provider in PROVIDER_CLUSTERS {
        let context = kubectl_context(provider);
        let target = "mock-inference.grid-system.svc.cluster.local:8080";
        let allowed_name = "grid-netpol-allowed";
        let denied_name = "grid-netpol-denied";

        delete_probe_pod(&context, allowed_name);
        delete_probe_pod(&context, denied_name);

        let allowed = run_probe_pod(
            &context,
            allowed_name,
            Some("grid.praxis-proxy.io/backend-access=provider-gateway"),
            &["--tcp-probe", target, "--tcp-probe-timeout-ms", "2000"],
        )?;
        if allowed.phase != "Succeeded" || !allowed.logs.contains("tcp-probe=connected") {
            return Err(format!(
                "{provider}: allowed backend probe did not connect (phase={}, logs={})",
                allowed.phase,
                safe_truncate_str(&allowed.logs, 120)
            )
            .into());
        }

        let denied = run_probe_pod(
            &context,
            denied_name,
            None,
            &["--tcp-probe", target, "--tcp-probe-timeout-ms", "2000"],
        )?;
        if denied.phase != "Failed"
            || !(denied.logs.contains("tcp-probe=timeout") || denied.logs.contains("tcp-probe=connect-failed"))
        {
            return Err(format!(
                "{provider}: unlabeled backend probe was not denied (phase={}, logs={})",
                denied.phase,
                safe_truncate_str(&denied.logs, 120)
            )
            .into());
        }

        let no_auth = run_probe_pod(
            &context,
            "grid-backend-no-auth",
            Some("grid.praxis-proxy.io/backend-access=provider-gateway"),
            &["--http-probe", target],
        )?;
        require_http_probe_status(provider, "missing credential", &no_auth, 401)?;

        let client_auth = run_probe_pod(
            &context,
            "grid-backend-client-auth",
            Some("grid.praxis-proxy.io/backend-access=provider-gateway"),
            &[
                "--http-probe",
                target,
                "--http-probe-authorization",
                "Bearer test-token",
            ],
        )?;
        require_http_probe_status(provider, "client-supplied credential", &client_auth, 403)?;

        evidence.push(format!(
            "{provider}: allowed=connected, unlabeled=denied, no_auth=HTTP_401, client_auth=HTTP_403"
        ));
    }
    Ok(evidence.join("; "))
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
    probe_args: &[&str],
) -> Result<NetworkPolicyProbe, Box<dyn std::error::Error>> {
    let _guard = ProbePodGuard {
        context: context.to_owned(),
        name: name.to_owned(),
    };
    let mut command = Command::new("kubectl");
    let probe_image = crate::env::image_overrides::glb_mock_provider_image();
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
            crate::env::image_overrides::image_pull_policy()
        ),
        "--restart=Never",
    ]);
    if let Some(value) = labels {
        command.arg(format!("--labels={value}"));
    }
    command.arg("--").args(probe_args);
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

    let output = Command::new("kubectl")
        .args(["--context", context, "-n", GRID_SYSTEM_NS, "logs", name])
        .output()?;
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(NetworkPolicyProbe {
        phase,
        logs: safe_truncate_str(logs.trim(), 512),
    })
}

/// Require one in-cluster HTTP probe to complete with the expected status.
fn require_http_probe_status(
    provider: &str,
    condition: &str,
    probe: &NetworkPolicyProbe,
    expected: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let marker = format!("http-probe=status status={expected}");
    if probe.phase == "Succeeded" && probe.logs.contains(&marker) {
        return Ok(());
    }
    Err(format!(
        "{provider}: {condition} probe expected HTTP {expected} (phase={}, logs={})",
        probe.phase,
        safe_truncate_str(&probe.logs, 120)
    )
    .into())
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
                (GRID_PEER_SELECTED_CANDIDATE_HEADER, "spoofed"),
                (GRID_PEER_HOP_REQUEST_ID_HEADER, "boundary-unknown-candidate"),
                ("X-Model", "sim-model-v1"),
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
                (GRID_PEER_SELECTED_CANDIDATE_HEADER, &candidate),
                (GRID_PEER_HOP_REQUEST_ID_HEADER, "boundary-wrong-path"),
                ("X-Model", "sim-model-v1"),
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
struct MtlsHeaderProbe<'a> {
    /// Provider gateway IP.
    ip: &'a str,
    /// SNI hostname.
    sni: &'a str,
    /// Client certificate and key paths.
    cert_key: (&'a str, &'a str),
    /// CA certificate path.
    ca: &'a str,
    /// URL path to request.
    path: &'a str,
    /// Request header name/value pairs.
    headers: &'a [(&'a str, &'a str)],
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

/// Derive the stable candidate ID rendered for one provider demo site.
fn provider_candidate_id(provider: &str) -> Result<String, Box<dyn std::error::Error>> {
    if !PROVIDER_CLUSTERS.contains(&provider) {
        return Err(format!("no provider candidate ID configured for {provider}").into());
    }
    let cluster = format!("sim-{provider}");
    Ok(fnv1a_hex8(&format!(
        "{DEMO_CANDIDATE_KIND}/{DEMO_MODEL}/{provider}/{cluster}"
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
pub(crate) fn check_clusters_live(status_json: &serde_json::Value) -> Result<String, Box<dyn std::error::Error>> {
    let clusters = status_json
        .get("data")
        .and_then(|d| d.get("clusters"))
        .and_then(serde_json::Value::as_array)
        .ok_or("status JSON missing data.clusters array")?;

    let mut missing = Vec::new();
    for expected in CLUSTER_NAMES {
        let found = clusters.iter().any(|c| {
            c.get("name").and_then(serde_json::Value::as_str) == Some(expected)
                && c.get("live").and_then(serde_json::Value::as_bool) == Some(true)
        });
        if !found {
            missing.push(*expected);
        }
    }

    if missing.is_empty() {
        Ok(format!("all {} clusters live", CLUSTER_NAMES.len()))
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

/// Verify the overlay [`ConfigMap`] candidate metadata.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
fn check_overlay_metadata() -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context(PRIMARY_EDGE);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "cm",
            OVERLAY_CONFIGMAP,
            "-o",
            "jsonpath={.data.grid-config\\.json}",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("overlay ConfigMap not found: {}", safe_truncate_str(stderr.trim(), 120)).into());
    }
    let raw = String::from_utf8(output.stdout)?;
    if raw.trim().is_empty() {
        return Err("overlay ConfigMap data is empty".into());
    }
    validate_overlay_json(&raw)
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
fn find_gridsite_egress<'a>(
    items: &'a [serde_json::Value],
    provider: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
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

/// Require both edge overlays to exist and be projected at `/etc/grid`.
fn check_edge_overlay_mounts() -> Result<String, Box<dyn std::error::Error>> {
    let mut evidence = Vec::new();
    for edge in EDGE_CLUSTERS {
        verify_single_edge_overlay(edge)?;
        evidence.push(format!("{edge}=2 candidates, projected `ConfigMap`"));
    }
    Ok(evidence.join(", "))
}

/// Wait until both edge gateways have a complete operator-rendered overlay.
///
/// # Errors
///
/// Returns an error when both provider candidates and the overlay projection
/// do not become ready before the provider-state timeout.
#[expect(
    clippy::disallowed_methods,
    reason = "bounded polling for asynchronous SWIM and operator convergence"
)]
pub(crate) fn wait_for_edge_overlays_ready() -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + PROVIDER_STATE_WAIT;
    let mut last_error = String::from("overlay validation has not run");
    while Instant::now() < deadline {
        match check_edge_overlay_mounts() {
            Ok(evidence) => return Ok(evidence),
            Err(error) => last_error = error.to_string(),
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(format!("timeout waiting for complete edge overlays: {last_error}").into())
}

/// Validate one edge cluster's overlay content and volume projection.
fn verify_single_edge_overlay(edge: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = kubectl_context(edge);
    let overlay = kubectl_jsonpath(&context, "configmap", OVERLAY_CONFIGMAP, r"{.data.grid-config\.json}")?;
    let document: serde_json::Value = serde_json::from_str(&overlay)?;
    let local_site = document
        .get("local_site")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("{edge} overlay missing local_site"))?;
    let candidates = document
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{edge} overlay missing candidates"))?;
    if local_site != edge || candidates.len() < 2 {
        return Err(format!(
            "{edge} overlay local_site={local_site:?}, candidates={}",
            candidates.len()
        )
        .into());
    }
    let mounted = kubectl_jsonpath(
        &context,
        "deployment",
        "edge-gateway",
        "{.spec.template.spec.volumes[?(@.name=='overlay')].configMap.name}",
    )?;
    if mounted != OVERLAY_CONFIGMAP {
        return Err(format!("{edge} edge-gateway mounts overlay `ConfigMap` {mounted:?}").into());
    }
    Ok(())
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
        evidence.push(format!(
            "{edge}=uid:{}, restarts:{}",
            safe_truncate_str(&identity.uid, 12),
            identity.restart_count
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
#[expect(
    clippy::too_many_lines,
    reason = "sequential verification assertions for a single step"
)]
fn check_inference_routed() -> Result<String, Box<dyn std::error::Error>> {
    let resp = curl_post_with_auth(EDGE_PORT)?;
    if resp.status != 200 {
        return Err(format!("inference request returned HTTP {}", resp.status).into());
    }
    let provider = extract_provider(&resp).map_err(|_e| "inference response missing X-Grid-Demo-Provider header")?;
    let provider_gateway = resp
        .headers
        .get(PROVIDER_GATEWAY_RESPONSE_HEADER)
        .ok_or("inference response missing X-Grid-Demo-Provider-Gateway header")?;
    if provider_gateway != &provider {
        return Err(format!(
            "backend provider attribution '{provider}' does not match provider gateway '{provider_gateway}'"
        )
        .into());
    }
    let backend_provider = resp
        .headers
        .get(BACKEND_PROVIDER_CAPTURE_HEADER)
        .ok_or("inference response missing backend provider-attribution capture")?;
    if backend_provider != provider_gateway {
        return Err(format!(
            "backend captured provider '{backend_provider}' does not match provider gateway '{provider_gateway}'"
        )
        .into());
    }
    let backend_request_id = resp
        .headers
        .get(BACKEND_REQUEST_ID_CAPTURE_HEADER)
        .filter(|value| !value.is_empty())
        .ok_or("inference response missing backend provider-request-id capture")?;
    let body: serde_json::Value =
        serde_json::from_str(&resp.body).map_err(|e| format!("inference response body is not valid JSON: {e}"))?;
    let model = body
        .get("model")
        .and_then(serde_json::Value::as_str)
        .ok_or("inference response missing model field")?;
    Ok(format!(
        "HTTP 200, model={model}, provider={provider}, gateway={provider_gateway}, backend_request_id={}, provider credential replaced client-supplied credential",
        safe_truncate_str(backend_request_id, 16)
    ))
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

/// Drive a provider into `existing_only` through its exported queue metric.
fn setup_session_drain(edge: &str, provider_site: &str) -> Result<String, Box<dyn std::error::Error>> {
    let reload_before = count_overlay_reload_logs(edge).unwrap_or(0);
    let result = (|| {
        set_provider_queue_depth(provider_site, PROVIDER_QUEUE_DRAINING)?;
        refresh_provider(provider_site)?;
        let admission = wait_for_candidate_admission(edge, provider_site, "existing_only")?;
        let reload = check_hot_reload_observed(edge, reload_before)?;
        Ok(format!(
            "site={provider_site}, queue_depth={PROVIDER_QUEUE_DRAINING}, {admission}; {reload}"
        ))
    })();
    if let Err(error) = result {
        return match restore_provider_admission_without_reload(edge, provider_site) {
            Ok(()) => Err(error),
            Err(restore) => Err(format!("{error}; admission restoration also failed: {restore}").into()),
        };
    }
    result
}

/// Verify new sessions avoid a drained provider while existing sessions retain it.
fn check_session_drain(port: u16, drained_provider: &str) -> Result<String, Box<dyn std::error::Error>> {
    let new_session = format!(
        "glb-proof-new-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let new_resp = curl_edge_request(port, Some(&new_session))?;
    if new_resp.status != 200 {
        return Err(format!("drain new-session returned HTTP {}", new_resp.status).into());
    }
    let new_provider = extract_provider(&new_resp)?;
    if new_provider == drained_provider {
        return Err(format!("new session routed to drained provider {drained_provider}").into());
    }
    let old_resp = curl_edge_request(port, Some("glb-proof-a"))?;
    if old_resp.status != 200 {
        return Err(format!("drain old-session returned HTTP {}", old_resp.status).into());
    }
    let old_provider = extract_provider(&old_resp)?;
    if old_provider != drained_provider {
        return Err(format!("existing session lost binding: expected {drained_provider}, got {old_provider}").into());
    }
    Ok(format!(
        "drained={drained_provider}, new session→{new_provider}, bound session→{old_provider}"
    ))
}

/// Check edge logs for hot-reload evidence.
fn check_hot_reload_observed(edge: &str, previous_count: usize) -> Result<String, Box<dyn std::error::Error>> {
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
fn count_overlay_reload_logs(edge: &str) -> Result<usize, Box<dyn std::error::Error>> {
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
    let combined = format!("{logs}{stderr}");
    Ok(count_reload_entries(&combined))
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

/// Read the operator-rendered overlay from an edge `ConfigMap`.
fn read_overlay(edge: &str) -> Result<String, Box<dyn std::error::Error>> {
    kubectl_jsonpath(
        &kubectl_context(edge),
        "configmap",
        OVERLAY_CONFIGMAP,
        r"{.data.grid-config\.json}",
    )
}

/// Return one provider candidate from the operator-rendered edge overlay.
fn overlay_candidate(edge: &str, site: &str) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    let overlay: serde_json::Value =
        serde_json::from_str(&read_overlay(edge)?).map_err(|e| format!("failed to parse {edge} overlay: {e}"))?;
    let candidates = overlay
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{edge} overlay missing candidates array"))?;
    Ok(candidates
        .iter()
        .find(|candidate| candidate.get("site").and_then(serde_json::Value::as_str) == Some(site))
        .cloned())
}

/// Wait for an operator-derived candidate admission state.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous polling in xtask; no async runtime is active"
)]
fn wait_for_candidate_admission(edge: &str, site: &str, expected: &str) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + PROVIDER_STATE_WAIT;
    let mut observed = None;
    while Instant::now() < deadline {
        observed = overlay_candidate(edge, site)?.and_then(|candidate| {
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
    Err(format!("timeout waiting for {site} admission_state={expected} on {edge}; observed={observed:?}").into())
}

/// Wait until an unavailable provider is absent from the edge overlay.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous polling in xtask; no async runtime is active"
)]
fn wait_for_candidate_absent(edge: &str, site: &str) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + PROVIDER_STATE_WAIT;
    while Instant::now() < deadline {
        if overlay_candidate(edge, site)?.is_none() {
            return Ok(format!("site={site} absent from operator-rendered overlay"));
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(format!("timeout waiting for {site} removal from {edge} overlay").into())
}

/// Return the `InferenceProvider` resource for one demo provider site.
fn provider_resource_name(site: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    match site {
        "east-provider" => Ok("sim-east-provider"),
        "west-provider" => Ok("sim-west-provider"),
        _ => Err(format!("unknown demo provider site {site}").into()),
    }
}

/// Set the mock backend's normalized queue metric and wait for its rollout.
fn set_provider_queue_depth(site: &str, queue_depth: &str) -> Result<(), Box<dyn std::error::Error>> {
    let assignment = format!("MOCK_QUEUE_DEPTH={queue_depth}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(site),
            "-n",
            GRID_SYSTEM_NS,
            "set",
            "env",
            "deployment/mock-inference",
            &assignment,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to set {site} queue depth: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }
    kubectl::wait_for_rollout_ns(&kubectl_context(site), "mock-inference", GRID_SYSTEM_NS, site)
}

/// Trigger provider health, metrics, `GridNetwork`, and SWIM reconciliation.
fn refresh_provider(site: &str) -> Result<(), Box<dyn std::error::Error>> {
    let provider = provider_resource_name(site)?;
    let revision = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let annotation = format!("grid.praxis-proxy.io/demo-refresh={revision}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(site),
            "-n",
            GRID_SYSTEM_NS,
            "annotate",
            "inferenceprovider",
            provider,
            &annotation,
            "--overwrite",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to refresh {site} provider reconciliation: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }
    Ok(())
}

/// Remove the verifier's transient reconcile annotation.
fn clear_provider_refresh(site: &str) -> Result<(), Box<dyn std::error::Error>> {
    let provider = provider_resource_name(site)?;
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(site),
            "-n",
            GRID_SYSTEM_NS,
            "annotate",
            "inferenceprovider",
            provider,
            "grid.praxis-proxy.io/demo-refresh-",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to clear {site} verifier annotation: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }
    Ok(())
}

/// Restore normal provider admission and require the generated overlay to recover.
fn restore_provider_admission(edge: &str, site: &str) -> Result<String, Box<dyn std::error::Error>> {
    let reload_before = count_overlay_reload_logs(edge)?;
    set_provider_queue_depth(site, PROVIDER_QUEUE_READY)?;
    refresh_provider(site)?;
    let admission = wait_for_candidate_admission(edge, site, "new_and_existing")?;
    let reload = check_hot_reload_observed(edge, reload_before)?;
    clear_provider_refresh(site)?;
    Ok(format!("queue_depth={PROVIDER_QUEUE_READY}, {admission}; {reload}"))
}

/// Best-effort restoration used when drain setup fails before proof begins.
fn restore_provider_admission_without_reload(edge: &str, site: &str) -> Result<(), Box<dyn std::error::Error>> {
    set_provider_queue_depth(site, PROVIDER_QUEUE_READY)?;
    refresh_provider(site)?;
    wait_for_candidate_admission(edge, site, "new_and_existing")?;
    clear_provider_refresh(site)
}

/// Provider state captured before a health-driven hot-reload proof.
struct ProviderWithdrawal {
    /// Provider site being withdrawn.
    site: String,
    /// Desired backend replica count to restore.
    original_replicas: u32,
    /// Edge reload count captured before withdrawal.
    reload_count_before: usize,
}

/// Return the declared mock backend replica count.
fn provider_backend_replicas(site: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let value = kubectl_jsonpath(
        &kubectl_context(site),
        "deployment",
        "mock-inference",
        "{.spec.replicas}",
    )?;
    value
        .parse()
        .map_err(|e| format!("{site} mock-inference has invalid replica count {value:?}: {e}").into())
}

/// Scale the provider backend and wait for the desired availability.
fn scale_provider_backend(site: &str, replicas: u32) -> Result<(), Box<dyn std::error::Error>> {
    let target = format!("--replicas={replicas}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &kubectl_context(site),
            "-n",
            GRID_SYSTEM_NS,
            "scale",
            "deployment/mock-inference",
            &target,
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to scale {site} mock-inference: {}",
            safe_truncate_str(String::from_utf8_lossy(&output.stderr).trim(), 120)
        )
        .into());
    }
    if replicas > 0 {
        return kubectl::wait_for_rollout_ns(&kubectl_context(site), "mock-inference", GRID_SYSTEM_NS, site);
    }
    wait_for_backend_scaled_to_zero(site)
}

/// Wait until a scaled-down provider backend has no available replicas.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous polling in xtask; no async runtime is active"
)]
fn wait_for_backend_scaled_to_zero(site: &str) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut observed = String::new();
    while Instant::now() < deadline {
        observed = kubectl_jsonpath(
            &kubectl_context(site),
            "deployment",
            "mock-inference",
            "{.status.availableReplicas}",
        )?;
        if observed.is_empty() || observed == "0" {
            return Ok(());
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(format!("timeout scaling {site} mock-inference to zero; availableReplicas={observed:?}").into())
}

/// Wait for an `InferenceProvider` phase produced by health reconciliation.
#[expect(
    clippy::disallowed_methods,
    reason = "synchronous polling in xtask; no async runtime is active"
)]
fn wait_for_provider_phase(site: &str, expected: &str) -> Result<String, Box<dyn std::error::Error>> {
    let provider = provider_resource_name(site)?;
    let deadline = Instant::now() + PROVIDER_STATE_WAIT;
    let mut observed = String::new();
    while Instant::now() < deadline {
        observed = kubectl_jsonpath(&kubectl_context(site), "inferenceprovider", provider, "{.status.phase}")?;
        if observed == expected {
            return Ok(format!("{provider} phase={expected}"));
        }
        thread::sleep(Duration::from_secs(1));
    }
    Err(format!("timeout waiting for {provider} phase={expected}; observed={observed:?}").into())
}

/// Withdraw a provider by making its declared backend unavailable.
fn withdraw_provider(edge: &str, site: &str) -> Result<(String, ProviderWithdrawal), Box<dyn std::error::Error>> {
    let original_replicas = provider_backend_replicas(site)?;
    if original_replicas == 0 {
        return Err(format!("{site} mock-inference already has zero replicas").into());
    }
    let state = ProviderWithdrawal {
        site: site.to_owned(),
        original_replicas,
        reload_count_before: count_overlay_reload_logs(edge)?,
    };
    let result = (|| {
        scale_provider_backend(site, 0)?;
        refresh_provider(site)?;
        let phase = wait_for_provider_phase(site, "Unavailable")?;
        let overlay = wait_for_candidate_absent(edge, site)?;
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
    scale_provider_backend(&state.site, state.original_replicas)?;
    refresh_provider(&state.site)?;
    let phase = wait_for_provider_phase(&state.site, "Available")?;
    let admission = wait_for_candidate_admission(edge, &state.site, "new_and_existing")?;
    let reload = check_hot_reload_observed(edge, reload_before)?;
    clear_provider_refresh(&state.site)?;
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
const CHAT_BODY: &str = r#"{"model":"sim-model-v1","messages":[{"role":"user","content":"hello"}],"max_tokens":64}"#;

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

/// Extract the `X-Grid-Demo-Provider` header from a response.
fn extract_provider(resp: &verify::HttpResponse) -> Result<String, Box<dyn std::error::Error>> {
    resp.headers
        .get("x-grid-demo-provider")
        .cloned()
        .ok_or_else(|| "missing x-grid-demo-provider header".into())
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
    fn parses_forge_status_clusters() {
        let status = status_with_clusters();
        let result = check_clusters_live(&status);
        assert!(result.is_ok(), "all clusters should be live: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("5"), "should report 5 clusters: {evidence}");
    }

    #[test]
    fn reload_entry_counter_counts_exact_messages() {
        let logs = "overlay reloaded\nunrelated\noverlay reloaded";
        assert_eq!(count_reload_entries(logs), 2, "should count reload entries");
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
        assert_eq!(blocked.len(), 2, "dependent assertions should be blocked once");
        assert_eq!(blocked.first().map(|r| r.label), Some("routing after reload"));
        assert_eq!(blocked.get(1).map(|r| r.label), Some("edge pod stable"));
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
        assert_eq!(GRID_PEER_SELECTED_CANDIDATE_HEADER, "x-grid-peer-selected-candidate");
        assert_eq!(GRID_PEER_HOP_REQUEST_ID_HEADER, "x-grid-peer-hop-request-id");
        for header in [GRID_PEER_SELECTED_CANDIDATE_HEADER, GRID_PEER_HOP_REQUEST_ID_HEADER] {
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
                .join("environments/grid-glb-demo/configs")
                .join(provider)
                .join("praxis.yaml");
            let config = fs::read_to_string(&path).unwrap_or_else(|_| std::process::abort());
            let route_index = config
                .find("filter: grid_provider_route")
                .unwrap_or_else(|| std::process::abort());
            let inject_index = config
                .find("filter: grid_credential_inject")
                .unwrap_or_else(|| std::process::abort());
            let load_balancer_index = config
                .find("filter: load_balancer")
                .unwrap_or_else(|| std::process::abort());
            assert!(
                route_index < inject_index && inject_index < load_balancer_index,
                "{provider} must authorize the route before credential injection and load balancing"
            );
            assert!(config.contains("name: mock-inference-credential"));
            assert!(config.contains("namespace: grid-system"));
            assert!(config.contains("key: token"));
            assert!(config.contains("file: /etc/praxis/credentials/mock-inference/token"));
            assert!(
                !config.contains(CLIENT_BEARER_TOKEN),
                "{provider} ConfigMap template must not contain the client-supplied credential"
            );
        }
    }

    #[test]
    fn provider_deployment_mounts_credential_secret() {
        let deployment = fs::read_to_string(
            workspace_root().join("environments/grid-glb-demo/resources/provider-gateway-deployment.yaml"),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert!(deployment.contains("mountPath: /etc/praxis/credentials/mock-inference"));
        assert!(deployment.contains("secretName: mock-inference-credential"));
        assert!(!deployment.contains(CLIENT_BEARER_TOKEN));
    }

    #[test]
    fn provider_drain_uses_declared_metrics_and_health_inputs() {
        let root = workspace_root();
        let resources = root.join("environments/grid-glb-demo/resources");
        let workloads =
            fs::read_to_string(resources.join("provider-workloads.yaml")).unwrap_or_else(|_| std::process::abort());
        assert!(workloads.contains("name: MOCK_QUEUE_DEPTH"));
        assert!(workloads.contains("value: \"0.10\""));

        for site in ["east-provider", "west-provider"] {
            let provider = fs::read_to_string(resources.join(format!("inference-{site}.yaml")))
                .unwrap_or_else(|_| std::process::abort());
            assert!(provider.contains("metricsConfig:"));
            assert!(provider.contains("path: /metrics"));
            assert!(provider.contains("queueDepth: grid_demo_queue_depth"));
        }

        let verifier = fs::read_to_string(root.join("xtask/src/env/glb.rs")).unwrap_or_else(|_| std::process::abort());
        assert!(verifier.contains("set_provider_queue_depth"));
        assert!(verifier.contains("scale_provider_backend"));
        assert!(
            !verifier.contains(concat!("fn write_", "overlay")),
            "the verifier must not mutate operator-owned overlay ConfigMaps"
        );
    }

    #[test]
    fn backend_network_policy_separates_data_and_health_access() {
        let resources = workspace_root().join("environments/grid-glb-demo/resources");
        let policy =
            fs::read_to_string(resources.join("backend-network-policy.yaml")).unwrap_or_else(|_| std::process::abort());
        let gateway = fs::read_to_string(resources.join("provider-gateway-deployment.yaml"))
            .unwrap_or_else(|_| std::process::abort());

        assert!(policy.contains("grid.praxis-proxy.io/backend-access: provider-gateway"));
        assert!(policy.contains("app.kubernetes.io/name: grid-operator"));
        assert!(gateway.contains("grid.praxis-proxy.io/backend-access: provider-gateway"));
        assert!(
            !gateway.contains("app.kubernetes.io/name: grid-operator"),
            "provider gateway must retain its own workload identity"
        );
    }

    #[test]
    fn provider_candidate_ids_cover_every_provider_cluster() {
        for provider in PROVIDER_CLUSTERS {
            let id = provider_candidate_id(provider).unwrap_or_else(|_| std::process::abort());
            assert_eq!(id.len(), 8);
            assert!(id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
        assert_ne!(
            provider_candidate_id("east-provider").unwrap_or_default(),
            provider_candidate_id("west-provider").unwrap_or_default()
        );
        assert!(provider_candidate_id("unknown-provider").is_err());
    }

    #[test]
    fn provider_configs_render_candidate_ids_from_identity_contract() {
        let configs = workspace_root().join("environments/grid-glb-demo/configs");
        for provider in PROVIDER_CLUSTERS {
            let template = fs::read_to_string(configs.join(format!("{provider}/praxis.yaml")))
                .unwrap_or_else(|_| std::process::abort());
            assert!(
                template.contains(PROVIDER_CANDIDATE_ID_TOKEN),
                "{provider} must use the rendered candidate identity token"
            );
        }
    }

    #[test]
    fn providers_bind_only_to_their_explicit_local_sites() {
        let resources = workspace_root().join("environments/grid-glb-demo/resources");
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
        let forge = fs::read_to_string(workspace_root().join("environments/grid-glb-demo/forge.yaml"))
            .unwrap_or_else(|_| std::process::abort());
        for site in ["east-edge", "east-provider", "west-edge", "west-provider"] {
            assert!(
                forge.contains(&format!("GRID_SWIM_SITE_NAME={site}")),
                "{site} must set an explicit SWIM identity"
            );
        }
        assert!(forge.contains("kubectl"));
        assert!(forge.contains("set"));
        assert!(forge.contains("env"));
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
                    "name": "sim-west-provider",
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
        let dump = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-grid-demo-provider: west-provider\r\n\r\n";
        let map = parse_header_dump(dump);
        assert_eq!(
            map.get("x-grid-demo-provider").map(String::as_str),
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
            headers: BTreeMap::from([("x-grid-demo-provider".to_owned(), "east-provider".to_owned())]),
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
}
