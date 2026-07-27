//! Narrated, evidence-backed GLB demo scenarios.

use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

use super::{glb, gtm_emulator, image_overrides, operator};

/// Maximum session keys sampled when discovering observable routing paths.
const MAX_PATH_SAMPLES: usize = 128;

/// Number of repeated requests used for the narrated affinity proof.
const AFFINITY_REPEATS: usize = 3;

/// Independent edge/provider affinity fixtures that reproduce one path.
#[derive(Debug, Clone)]
struct AffinityFixture {
    /// Session fixture consumed only by the GTM layer.
    edge_session: String,
    /// Session fixture consumed only by the Grid routing layer.
    provider_session: String,
}

/// Observed `(edge, provider)` pair to the fixtures that reproduce it.
type ObservedPaths = BTreeMap<(String, String), AffinityFixture>;

/// Resolved config emitted next to the source config to preserve relative paths.
const RESOLVED_CONFIG_NAME: &str = ".forge.resolved.yaml";

/// Ordered cluster names in the local scenario environment.
const CLUSTERS: &[&str] = &[
    "gtm-emulator",
    "east-edge",
    "east-provider",
    "west-edge",
    "west-provider",
];

/// Clusters that participate in Grid discovery and run an operator.
const GRID_CLUSTERS: &[&str] = &["east-edge", "east-provider", "west-edge", "west-provider"];

/// Provider clusters that run the private provider path.
const PROVIDER_CLUSTERS: &[&str] = &["east-provider", "west-provider"];

/// Materialize and deploy a complete GLB demo environment.
///
/// # Errors
///
/// Returns an error when config rendering, cluster creation, image loading,
/// stack application, trust installation, or service startup fails.
pub(crate) fn setup(forge_config: &Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let resolved = materialize_config(forge_config)?;
    let forge = glb::resolve_forge_binary().ok_or("praxis-forge binary not found")?;

    glb::stage_provider_boundary()?;
    run_forge(&forge, &resolved, &["up"])?;
    load_local_images_if_required(&forge, &resolved)?;
    apply_foundation_stacks(&forge, &resolved)?;
    glb::install_provider_boundary()?;
    apply_provider_stacks(&forge, &resolved)?;
    apply_edge_stacks(&forge, &resolved)?;
    apply_gtm_emulator_stack(&forge, &resolved)?;
    let overlay_evidence = glb::wait_for_edge_overlays_ready()?;

    eprintln!(
        "glb-demo: environment is ready using {}; {overlay_evidence}",
        resolved.display()
    );
    Ok(resolved)
}

/// Set up the environment and run every narrated proof.
///
/// # Errors
///
/// Returns an error when setup or any runtime scenario fails.
pub(crate) fn run(forge_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = setup(forge_config)?;
    demonstrate(&resolved)
}

/// Run the complete narrated GLB scenario collection.
///
/// # Errors
///
/// Returns an error when any prerequisite proof, routing scenario, affinity
/// check, or edge withdrawal/recovery check fails.
pub(crate) fn demonstrate(forge_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    print_introduction();

    print_scenario(
        1,
        "Active/active global and provider routing",
        "As an application owner, I need one stable HTTPS endpoint backed by active edges while Grid independently selects an admitted provider.",
    );
    glb::verify_grid_routing(forge_config)?;
    let paths = discover_paths()?;
    print_paths(&paths);

    print_scenario(
        2,
        "Secure provider boundary",
        "As a provider and security owner, I need authenticated Grid traffic, exact local policy, private backend isolation, and final-hop credential replacement.",
    );
    print_provider_boundary_proof();
    print_credential_boundary_proof();

    print_scenario(
        3,
        "Session affinity and provider drain",
        "As an inference client, I need repeated requests to remain on one edge and provider while existing sessions survive a metrics-driven drain and new sessions move safely.",
    );
    prove_affinity(&paths)?;

    print_scenario(
        4,
        "Edge withdrawal and recovery",
        "As a reliability operator, I need a failed edge withdrawn behind the same HTTPS name and returned after recovery.",
    );
    gtm_emulator::verify(forge_config)?;

    print_boundaries();
    Ok(())
}

/// Summarize the provider assertions completed by the preceding strict proof.
fn print_provider_boundary_proof() {
    eprintln!();
    eprintln!(
        "PASS: both providers required mTLS, accepted both pinned edge identities, rejected missing or invalid TLS identities, and enforced exact candidate/model/path policy."
    );
}

/// Summarize final-hop credential and private-backend runtime evidence.
fn print_credential_boundary_proof() {
    eprintln!();
    eprintln!(
        "PASS: both private backends denied unlabeled network access, returned HTTP 401 without credentials and HTTP 403 for the client-supplied fixture, then returned HTTP 200 through provider-local credential replacement."
    );
}

/// Print the architecture and proof policy before executing scenarios.
fn print_introduction() {
    eprintln!("# Grid GLB Demo");
    eprintln!();
    eprintln!("Every PASS below comes from a runtime assertion. Manifest intent is not counted as proof.");
    eprintln!();
    eprintln!("```text");
    eprintln!("client -> stable Praxis HTTPS -> selected Praxis edge");
    eprintln!("       -> Grid-selected Praxis provider gateway -> private backend");
    eprintln!("```");
}

/// Print one scenario and its user story.
fn print_scenario(number: usize, title: &str, user_story: &str) {
    eprintln!();
    eprintln!("## Scenario {number}: {title}");
    eprintln!();
    eprintln!("User story: {user_story}");
}

/// Discover real edge/provider combinations through the stable HTTPS name.
fn discover_paths() -> Result<ObservedPaths, Box<dyn std::error::Error>> {
    let mut paths = BTreeMap::new();
    for index in 0..MAX_PATH_SAMPLES {
        let fixture = AffinityFixture {
            edge_session: format!("narrated-edge-{index}"),
            provider_session: format!("narrated-provider-{index}"),
        };
        let sample = gtm_emulator::request_path_with_affinity(&fixture.edge_session, &fixture.provider_session)?;
        if !paths.keys().any(|(edge, _)| edge == &sample.edge) {
            paths.insert((sample.edge, sample.provider), fixture);
        }
        if paths.len() == 2 {
            break;
        }
    }
    if paths.len() != 2 {
        return Err(format!("path discovery observed {} of 2 Praxis edges", paths.len()).into());
    }
    Ok(paths)
}

/// Print the path matrix observed from live responses.
fn print_paths(paths: &ObservedPaths) {
    eprintln!();
    eprintln!("| GTM-selected edge | Grid-selected provider | Edge fixture | Provider fixture |");
    eprintln!("|---|---|---|---|");
    for ((edge, provider), fixture) in paths {
        eprintln!(
            "| `{edge}` | `{provider}` | `{}` | `{}` |",
            fixture.edge_session, fixture.provider_session
        );
    }

    if let Some(((edge, provider), fixture)) = paths
        .iter()
        .find(|((edge, provider), _)| edge.strip_suffix("-edge") != provider.strip_suffix("-provider"))
    {
        eprintln!();
        eprintln!(
            "Observed crossed path (`{}` / `{}`):",
            fixture.edge_session, fixture.provider_session
        );
        eprintln!();
        eprintln!("```text");
        eprintln!("client -> {edge} public edge -> {edge} Grid overlay");
        eprintln!("       -> {provider} private provider gateway -> {provider} backend");
        eprintln!("       -> {provider} provider gateway -> {edge} edge -> client");
        eprintln!("```");
    }
}

/// Repeat one observed path and require both affinity layers to remain stable.
fn prove_affinity(paths: &ObservedPaths) -> Result<(), Box<dyn std::error::Error>> {
    let ((expected_edge, expected_provider), fixture) = paths
        .first_key_value()
        .ok_or("no observed path available for affinity")?;

    for _attempt in 0..AFFINITY_REPEATS {
        let sample = gtm_emulator::request_path_with_affinity(&fixture.edge_session, &fixture.provider_session)?;
        if sample.edge != *expected_edge || sample.provider != *expected_provider {
            return Err(format!(
                "affinity fixtures moved from {expected_edge}/{expected_provider} to {}/{}",
                sample.edge, sample.provider,
            )
            .into());
        }
    }

    eprintln!();
    eprintln!(
        "PASS: edge fixture `{}` and provider fixture `{}` remained on edge `{expected_edge}` and provider `{expected_provider}` for {AFFINITY_REPEATS} repeated requests.",
        fixture.edge_session, fixture.provider_session
    );
    Ok(())
}

/// Print explicit scope boundaries after all runtime proofs.
fn print_boundaries() {
    eprintln!();
    eprintln!("## Demonstrated Boundary");
    eprintln!();
    eprintln!("- Proven: two Praxis edges, one verified HTTPS name, health withdrawal/recovery, edge stickiness.");
    eprintln!(
        "- Proven: per-edge Grid overlays, metrics-driven provider drain, health-driven withdrawal, hot reload, provider mTLS and peer authorization."
    );
    eprintln!("- Proven: provider-local credential replacement and NetworkPolicy-enforced private backend access.");
    eprintln!(
        "- Not claimed: managed DNS/Anycast, internet DDoS/WAF, geo-latency GTM steering, shared affinity storage, or in-flight stream migration."
    );
}

/// Render image overrides into a Forge config without mutating source files.
fn materialize_config(source: &Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(source)?;
    let rendered = render_config(&content)?;
    let parent = source.parent().unwrap_or_else(|| Path::new("."));
    let output = parent.join(RESOLVED_CONFIG_NAME);
    fs::write(&output, rendered)?;
    Ok(output)
}

/// Render image overrides into one Forge configuration document.
fn render_config(content: &str) -> Result<String, Box<dyn std::error::Error>> {
    validate_image_contract()?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(content)?;
    let spec = mapping_mut(&mut config, "spec")?;
    set_cluster_image_properties(spec)?;
    Ok(serde_yaml::to_string(&config)?)
}

/// Validate the image references and pull policy selected by the environment.
fn validate_image_contract() -> Result<(), Box<dyn std::error::Error>> {
    for (name, image) in [
        ("GRID_XTASK_GATEWAY_IMAGE", image_overrides::glb_gateway_image()),
        ("GRID_XTASK_OPERATOR_IMAGE", image_overrides::glb_operator_image()),
        (
            "GRID_XTASK_MOCK_PROVIDER_IMAGE",
            image_overrides::glb_mock_provider_image(),
        ),
    ] {
        if image.is_empty() || image.chars().any(char::is_whitespace) {
            return Err(format!("{name} must be a non-empty image reference without whitespace").into());
        }
    }

    let pull_policy = image_overrides::image_pull_policy();
    if !matches!(pull_policy.as_str(), "Always" | "IfNotPresent" | "Never") {
        return Err(format!(
            "GRID_XTASK_IMAGE_PULL_POLICY must be Always, IfNotPresent, or Never; got {pull_policy:?}"
        )
        .into());
    }
    Ok(())
}

/// Apply environment-selected images to stack template properties.
fn set_cluster_image_properties(spec: &mut serde_yaml::Mapping) -> Result<(), Box<dyn std::error::Error>> {
    let clusters = sequence_mut(spec, "clusters")?;
    for cluster in clusters {
        let cluster = cluster.as_mapping_mut().ok_or("cluster entry must be a mapping")?;
        let properties = mapping_mut_in(cluster, "properties")?;
        for (key, value) in [
            ("gatewayImage", image_overrides::glb_gateway_image()),
            ("operatorImage", image_overrides::glb_operator_image()),
            ("mockProviderImage", image_overrides::glb_mock_provider_image()),
            ("imagePullPolicy", image_overrides::image_pull_policy()),
        ] {
            properties.insert(yaml_key(key), serde_yaml::Value::String(value));
        }
    }
    Ok(())
}

/// Load local images into Kind when the pull policy is `Never`.
fn load_local_images_if_required(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if image_overrides::should_skip_kind_image_loading() {
        return Ok(());
    }
    let operator = image_overrides::glb_operator_image();
    let gateway = image_overrides::glb_gateway_image();
    let mock = image_overrides::glb_mock_provider_image();
    for image in [&operator, &gateway, &mock] {
        require_local_image(image)?;
    }
    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["cluster", "load-image", cluster, &operator])?;
    }
    for cluster in CLUSTERS {
        run_forge(forge, config, &["cluster", "load-image", cluster, &gateway])?;
    }
    for cluster in PROVIDER_CLUSTERS {
        run_forge(forge, config, &["cluster", "load-image", cluster, &mock])?;
    }
    Ok(())
}

/// Apply shared infrastructure before any identity-dependent workload.
fn apply_foundation_stacks(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    run_forge(forge, config, &["stack", "apply", "gtm-emulator", "metallb"])?;

    // Capture every SWIM address before rendering any operator's peer list.
    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["stack", "apply", cluster, "metallb"])?;
    }
    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["stack", "apply", cluster, "swim-lb"])?;
    }

    // Install the shared operator Deployment only after all Service captures
    // exist, then replace its placeholders before creating any Grid CR.
    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["stack", "apply", cluster, "grid-operator"])?;
    }
    for (cluster, identity_stack) in [
        ("east-edge", "east-edge-operator"),
        ("east-provider", "east-provider-operator"),
        ("west-edge", "west-edge-operator"),
        ("west-provider", "west-provider-operator"),
    ] {
        run_forge(forge, config, &["stack", "apply", cluster, identity_stack])?;
    }
    Ok(())
}

/// Apply provider sites and private provider paths before edge rendering.
fn apply_provider_stacks(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for (cluster, site_stack) in [
        ("east-provider", "east-provider-site"),
        ("west-provider", "west-provider-site"),
    ] {
        run_forge(forge, config, &["stack", "apply", cluster, site_stack])?;
        run_forge(forge, config, &["stack", "apply", cluster, "inference-sim"])?;
    }
    Ok(())
}

/// Apply edge sites and the local Praxis edge in each edge cluster.
fn apply_edge_stacks(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for (cluster, site_stack) in [("east-edge", "east-edge-site"), ("west-edge", "west-edge-site")] {
        run_forge(forge, config, &["stack", "apply", cluster, site_stack])?;
    }
    authorize_provider_sites_for_edges()?;
    for cluster in ["east-edge", "west-edge"] {
        run_forge(forge, config, &["stack", "apply", cluster, "edge-gateway"])?;
    }
    Ok(())
}

/// Pin each provider's SWIM-advertised public certificate on both edge sites.
///
/// SWIM discovery supplies the endpoint and public certificate, but it does
/// not authorize routing. The demo compares the received certificate to the
/// locally generated out-of-band identity before configuring the `GridSite`
/// fingerprint policy. Edge Deployments are applied only after both provider
/// sites reach `Active`, so a missing or mismatched trust record fails closed.
fn authorize_provider_sites_for_edges() -> Result<(), Box<dyn std::error::Error>> {
    const TRUST_TIMEOUT: Duration = Duration::from_secs(120);

    for edge in ["east-edge", "west-edge"] {
        let context = format!("kind-grid-glb-{edge}");
        for provider in PROVIDER_CLUSTERS {
            let site_name = format!("glb-demo-{provider}");
            operator::wait_for_auto_gridsite(&context, &site_name, "glb-demo", TRUST_TIMEOUT)?;
            let expected_fingerprint = glb::site_certificate_fingerprint(provider)?;
            wait_for_expected_site_certificate(&context, &site_name, &expected_fingerprint, TRUST_TIMEOUT)?;
            operator::patch_gridsite_cert_fingerprint(&context, &site_name, &expected_fingerprint)?;
            operator::wait_for_gridsite_phase(&context, &site_name, "Active", TRUST_TIMEOUT)?;
        }
    }
    Ok(())
}

/// Wait for certificate gossip to replace missing or stale site trust material.
fn wait_for_expected_site_certificate(
    context: &str,
    site_name: &str,
    expected_fingerprint: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        if operator::read_gridsite_public_cert_pem(context, site_name)
            .is_some_and(|pem| operator::sha256_fingerprint(&pem) == expected_fingerprint)
        {
            eprintln!("  [OK] GridSite {site_name:?}: advertised certificate matches the staged identity");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("timeout waiting for GridSite {site_name:?} to advertise its expected certificate").into(),
            );
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "bounded polling for asynchronous SWIM certificate propagation"
        )]
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Apply the local managed-GTM stand-in after both edge addresses are known.
fn apply_gtm_emulator_stack(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    run_forge(forge, config, &["stack", "apply", "gtm-emulator", "gtm-emulator"])
}
/// Execute one Forge command and retain its output on failure.
fn run_forge(forge: &str, config: &Path, args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(forge)
        .args(["--config", &config.display().to_string(), "--non-interactive"])
        .args(args)
        .output()?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "praxis-forge {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    )
    .into())
}

/// Require a local Docker image before a `Never`-pull setup.
fn require_local_image(image: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    if status.success() {
        return Ok(());
    }
    Err(format!(
        "required local image {image:?} is absent; build it or set GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent with registry image overrides"
    )
    .into())
}

/// Return a named mapping from a YAML value.
fn mapping_mut<'a>(
    value: &'a mut serde_yaml::Value,
    field: &str,
) -> Result<&'a mut serde_yaml::Mapping, Box<dyn std::error::Error>> {
    let mapping = value.as_mapping_mut().ok_or("YAML root must be a mapping")?;
    mapping_mut_in(mapping, field)
}

/// Return a named child mapping.
fn mapping_mut_in<'a>(
    mapping: &'a mut serde_yaml::Mapping,
    field: &str,
) -> Result<&'a mut serde_yaml::Mapping, Box<dyn std::error::Error>> {
    mapping
        .get_mut(yaml_key(field))
        .and_then(serde_yaml::Value::as_mapping_mut)
        .ok_or_else(|| format!("YAML field {field:?} must be a mapping").into())
}

/// Return a named child sequence.
fn sequence_mut<'a>(
    mapping: &'a mut serde_yaml::Mapping,
    field: &str,
) -> Result<&'a mut Vec<serde_yaml::Value>, Box<dyn std::error::Error>> {
    mapping
        .get_mut(yaml_key(field))
        .and_then(serde_yaml::Value::as_sequence_mut)
        .ok_or_else(|| format!("YAML field {field:?} must be a sequence").into())
}

/// Construct one YAML mapping key.
fn yaml_key(value: &str) -> serde_yaml::Value {
    serde_yaml::Value::String(value.to_owned())
}

#[cfg(test)]
mod setup_tests {
    use super::*;

    /// Repository root from the xtask crate directory.
    fn workspace_root() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map_or_else(|| std::path::PathBuf::from("."), Path::to_path_buf)
    }

    #[test]
    fn materialized_config_uses_glb_image_contract() -> Result<(), Box<dyn std::error::Error>> {
        let source = workspace_root().join("environments/grid-glb-demo/forge.yaml");
        let rendered = render_config(&fs::read_to_string(source)?)?;
        assert!(rendered.contains(&image_overrides::glb_gateway_image()));
        assert!(rendered.contains(&image_overrides::glb_operator_image()));
        assert!(rendered.contains(&image_overrides::glb_mock_provider_image()));
        assert!(rendered.contains(&image_overrides::image_pull_policy()));
        assert!(!rendered.contains("grid-overlay-sync"));
        Ok(())
    }

    #[test]
    fn operator_site_configuration_preserves_the_base_deployment() -> Result<(), Box<dyn std::error::Error>> {
        let forge = fs::read_to_string(workspace_root().join("environments/grid-glb-demo/forge.yaml"))?;
        for site in ["east-edge", "east-provider", "west-edge", "west-provider"] {
            let identity = format!("GRID_SWIM_SITE_NAME={site}");
            let identity_index = forge
                .find(&identity)
                .ok_or_else(|| format!("{site} has no explicit SWIM identity"))?;
            let network = format!("resources/gridnetwork-{site}.yaml");
            let network_index = forge
                .find(&network)
                .ok_or_else(|| format!("{site} has no GridNetwork step"))?;
            assert!(
                identity_index < network_index,
                "{site} identity must be set before its GridNetwork is applied"
            );
            let between = forge
                .get(identity_index..network_index)
                .ok_or_else(|| format!("{site} stack order is invalid"))?;
            assert!(
                between.contains("rollout") && between.contains("status"),
                "{site} operator rollout must complete before its GridNetwork is applied"
            );
        }
        assert!(
            !forge.contains("operator-env-"),
            "partial Deployment overlays can disturb base security settings"
        );
        Ok(())
    }

    #[test]
    fn demo_workloads_use_restricted_container_defaults() -> Result<(), Box<dyn std::error::Error>> {
        let resources = workspace_root().join("environments/grid-glb-demo/resources");
        for manifest in [
            "edge-gateway-deployment.yaml",
            "provider-gateway-deployment.yaml",
            "gtm-emulator-deployment.yaml",
            "provider-workloads.yaml",
        ] {
            let deployment = fs::read_to_string(resources.join(manifest))?;
            for required in [
                "automountServiceAccountToken: false",
                "runAsNonRoot: true",
                "type: RuntimeDefault",
                "allowPrivilegeEscalation: false",
                "readOnlyRootFilesystem: true",
                "- ALL",
            ] {
                assert!(deployment.contains(required), "{manifest} must contain {required:?}");
            }
        }
        Ok(())
    }

    #[test]
    fn default_glb_image_contract_is_valid() {
        assert!(validate_image_contract().is_ok());
    }
}
