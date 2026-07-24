//! The `up` command: bring up all configured clusters.
//!
//! Creates any clusters that do not already exist, updates state,
//! and reports the result.

use std::io::Write;

use crate::{
    cluster::kind as kind_ops,
    config::ClusterSpec,
    context::ForgeContext,
    error::ForgeError,
    networking,
    output::{self, OutputFormat},
    runtime, service,
    state::{self, ClusterPhase, ClusterState, NetworkPhase, ServiceHealth, ServicePhase, ServiceState, lock},
};

/// Run the `up` command.
///
/// # Errors
///
/// Returns [`ForgeError`] if runtime detection, cluster creation,
/// or state persistence fails.
pub fn run(ctx: &ForgeContext<'_>, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let resolved = runtime::resolve(ctx.runner, &ctx.config.spec.runtime.provider)?;
    if wants_network(ctx) {
        networking::require_docker_for_cross_cluster(&resolved.binary)?;
    }
    let _lock = lock::acquire(&ctx.state_dir)?;
    let mut state = state::load(&ctx.state_dir)?;
    state.runtime = Some(resolved.binary.clone());
    let net_result = ensure_network(ctx, &resolved.binary, &mut state)?;
    let results = create_clusters(ctx, &mut state)?;
    let svc_results = start_services(ctx, &resolved.binary, &mut state)?;
    update_digest(ctx, &mut state)?;
    record_operation(&mut state, "up", true);
    if !ctx.dry_run {
        state::save(&ctx.state_dir, &state)?;
    }
    render_all(writer, &net_result, &results, &svc_results, &ctx.format)
}

// ---------------------------------------------------------------
// Cluster creation
// ---------------------------------------------------------------

// ---------------------------------------------------------------
// Network setup
// ---------------------------------------------------------------

/// Result of network setup.
struct NetworkSetup {
    /// Network name.
    name: String,
    /// Whether this was a dry-run skip.
    dry_run: bool,
}

/// Ensure the environment network exists if configured.
fn ensure_network(
    ctx: &ForgeContext<'_>,
    binary: &str,
    state: &mut state::ForgeState,
) -> Result<Option<NetworkSetup>, ForgeError> {
    if !wants_network(ctx) {
        return Ok(None);
    }
    let env_name = &ctx.config.metadata.name;
    let net_name = networking::network_name(env_name);
    if ctx.dry_run {
        return Ok(Some(NetworkSetup {
            name: net_name,
            dry_run: true,
        }));
    }
    networking::create_network(ctx.runner, binary, &net_name, env_name)?;
    set_network_active(state, &net_name);
    Ok(Some(NetworkSetup {
        name: net_name,
        dry_run: false,
    }))
}

/// Check if the config requests cross-cluster networking.
fn wants_network(ctx: &ForgeContext<'_>) -> bool {
    ctx.config.spec.network.as_ref().is_some_and(|n| n.cross_cluster)
}

/// Record the network as active in state, preserving existing pools.
fn set_network_active(state: &mut state::ForgeState, name: &str) {
    if let Some(ref mut net) = state.network {
        name.clone_into(&mut net.name);
        net.phase = NetworkPhase::Active;
        return;
    }
    state.network = Some(state::NetworkState {
        name: name.to_owned(),
        phase: NetworkPhase::Active,
        cidr: None,
        cluster_pools: Vec::new(),
    });
}

// ---------------------------------------------------------------
// Cluster creation
// ---------------------------------------------------------------

/// Result of processing one cluster.
struct ClusterResult {
    /// Cluster config name.
    name: String,
    /// KIND cluster name.
    kind_name: String,
    /// Whether the cluster was created (vs. already existed).
    created: bool,
    /// Whether this was a dry-run skip.
    dry_run: bool,
}

/// Iterate configured clusters, creating any that are missing.
fn create_clusters(ctx: &ForgeContext<'_>, state: &mut state::ForgeState) -> Result<Vec<ClusterResult>, ForgeError> {
    let docker_network = docker_network_for_kind(ctx);
    let mut results = Vec::new();
    for cluster in &ctx.config.spec.clusters {
        let r = process_cluster(ctx, state, cluster, docker_network.as_deref())?;
        results.push(r);
    }
    Ok(results)
}

/// Determine the Docker network name for KIND clusters, if any.
fn docker_network_for_kind(ctx: &ForgeContext<'_>) -> Option<String> {
    ctx.config
        .spec
        .network
        .as_ref()
        .filter(|n| n.cross_cluster)
        .map(|_| networking::network_name(&ctx.config.metadata.name))
}

/// Process a single cluster: create if missing, skip if exists.
fn process_cluster(
    ctx: &ForgeContext<'_>,
    state: &mut state::ForgeState,
    cluster: &ClusterSpec,
    docker_network: Option<&str>,
) -> Result<ClusterResult, ForgeError> {
    let kind_name = kind_ops::kind_cluster_name(&ctx.config.spec.runtime.cluster_prefix, &cluster.name);
    if ctx.dry_run {
        return Ok(dry_run_result(&cluster.name, &kind_name));
    }
    let created = create_if_missing(ctx, &kind_name, cluster, state, docker_network)?;
    Ok(ClusterResult {
        name: cluster.name.clone(),
        kind_name,
        created,
        dry_run: false,
    })
}

/// Build a dry-run result without executing anything.
fn dry_run_result(name: &str, kind_name: &str) -> ClusterResult {
    ClusterResult {
        name: name.to_owned(),
        kind_name: kind_name.to_owned(),
        created: false,
        dry_run: true,
    }
}

/// Create a cluster if it doesn't already exist. Returns true if created.
fn create_if_missing(
    ctx: &ForgeContext<'_>,
    kind_name: &str,
    cluster: &ClusterSpec,
    state: &mut state::ForgeState,
    docker_network: Option<&str>,
) -> Result<bool, ForgeError> {
    if kind_ops::cluster_exists(ctx.runner, kind_name)? {
        ensure_state_entry(state, &cluster.name, kind_name, ClusterPhase::Running);
        return Ok(false);
    }
    kind_ops::create_cluster(ctx.runner, kind_name, &cluster.nodes, &ctx.state_dir, docker_network)?;
    ensure_state_entry(state, &cluster.name, kind_name, ClusterPhase::Running);
    Ok(true)
}

/// Ensure a cluster has an entry in state with the given phase.
fn ensure_state_entry(state: &mut state::ForgeState, name: &str, kind_name: &str, phase: ClusterPhase) {
    if let Some(cs) = state::find_cluster_mut(state, name) {
        cs.phase = phase;
        return;
    }
    state.clusters.push(ClusterState {
        name: name.to_owned(),
        kind_name: kind_name.to_owned(),
        context: kind_ops::kubectl_context(kind_name),
        phase,
    });
}

// ---------------------------------------------------------------
// Service startup
// ---------------------------------------------------------------

/// Result of processing one service.
struct ServiceResult {
    /// Service config name.
    name: String,
    /// Deterministic container name.
    container_name: String,
    /// Whether this was a dry-run skip.
    dry_run: bool,
}

/// Start configured services in dependency order.
fn start_services(
    ctx: &ForgeContext<'_>,
    binary: &str,
    state: &mut state::ForgeState,
) -> Result<Vec<ServiceResult>, ForgeError> {
    if ctx.config.spec.services.is_empty() {
        return Ok(Vec::new());
    }
    let order = service::dependency_order(&ctx.config.spec.services)?;
    let mut results = Vec::new();
    for idx in order {
        let r = start_one_svc(ctx, binary, state, idx)?;
        results.push(r);
    }
    Ok(results)
}

/// Start a single service by index.
fn start_one_svc(
    ctx: &ForgeContext<'_>,
    binary: &str,
    state: &mut state::ForgeState,
    idx: usize,
) -> Result<ServiceResult, ForgeError> {
    let svc = ctx
        .config
        .spec
        .services
        .get(idx)
        .ok_or_else(|| ForgeError::State("service index out of range".to_owned()))?;
    let cname = service::container_name(&ctx.config.metadata.name, &svc.name);
    if ctx.dry_run {
        return Ok(ServiceResult {
            name: svc.name.clone(),
            container_name: cname,
            dry_run: true,
        });
    }
    let params = build_svc_params(binary, &cname, ctx);
    service::start_service(ctx.runner, &params, svc)?;
    let health = run_health_check(svc, &cname);
    upsert_svc_state(state, svc, &cname, &health);
    Ok(ServiceResult {
        name: svc.name.clone(),
        container_name: cname,
        dry_run: false,
    })
}

/// Build service parameters from context.
fn build_svc_params<'a>(binary: &'a str, cname: &'a str, ctx: &'a ForgeContext<'_>) -> service::ServiceParams<'a> {
    service::ServiceParams {
        binary,
        container_name: cname,
        env_name: &ctx.config.metadata.name,
        config_dir: &ctx.config_dir,
    }
}

/// Run a health check if configured, return health status.
fn run_health_check(svc: &crate::config::ServiceSpec, cname: &str) -> ServiceHealth {
    let Some(check) = &svc.health_check else {
        return ServiceHealth::Unknown;
    };
    let _ = cname;
    let Some(host_port) = health_probe_host_port(svc, check.port) else {
        return ServiceHealth::Unhealthy;
    };
    match service::health::wait_for_healthy("127.0.0.1", host_port, check) {
        Ok(true) => ServiceHealth::Healthy,
        _ => ServiceHealth::Unhealthy,
    }
}

/// Resolve a container-side health-check port to a host-reachable port.
fn health_probe_host_port(svc: &crate::config::ServiceSpec, container_port: u16) -> Option<u16> {
    if matches!(svc.network, crate::config::NetworkMode::Host) {
        return Some(container_port);
    }
    svc.ports
        .iter()
        .find(|p| p.container == container_port && p.protocol == "tcp")
        .map(|p| p.host)
}

/// Insert or update a service state entry.
fn upsert_svc_state(
    state: &mut state::ForgeState,
    svc: &crate::config::ServiceSpec,
    cname: &str,
    health: &ServiceHealth,
) {
    let phase = match health {
        ServiceHealth::Unhealthy => ServicePhase::Unhealthy,
        _ => ServicePhase::Running,
    };
    if let Some(ss) = state::find_service_mut(state, &svc.name) {
        ss.phase = phase;
        ss.health = health.clone();
        ss.last_observed = state::now_epoch_secs();
        return;
    }
    state.services.push(ServiceState {
        name: svc.name.clone(),
        container_name: cname.to_owned(),
        image: svc.image.clone(),
        phase,
        health: health.clone(),
        last_observed: state::now_epoch_secs(),
    });
}

// ---------------------------------------------------------------
// State helpers
// ---------------------------------------------------------------

/// Update the config digest in state.
fn update_digest(ctx: &ForgeContext<'_>, state: &mut state::ForgeState) -> Result<(), ForgeError> {
    state.config_digest = Some(state::config_digest(ctx.config)?);
    Ok(())
}

/// Record the last operation in state.
fn record_operation(state: &mut state::ForgeState, operation: &str, success: bool) {
    state.last_operation = Some(state::LastOperation {
        operation: operation.to_owned(),
        timestamp: state::now_epoch_secs(),
        success,
    });
}

// ---------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------

/// Render all results (network, clusters, services).
fn render_all(
    writer: &mut dyn Write,
    net: &Option<NetworkSetup>,
    clusters: &[ClusterResult],
    services: &[ServiceResult],
    format: &OutputFormat,
) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => render_json(writer, net, clusters, services),
        OutputFormat::Text => render_text(writer, net, clusters, services),
    }
}

/// Render results as JSON.
fn render_json(
    writer: &mut dyn Write,
    net: &Option<NetworkSetup>,
    clusters: &[ClusterResult],
    services: &[ServiceResult],
) -> Result<(), ForgeError> {
    let items: Vec<_> = clusters.iter().map(result_to_json).collect();
    let mut data = serde_json::json!({ "clusters": items });
    if let (Some(n), Some(obj)) = (net, data.as_object_mut()) {
        obj.insert(
            "network".to_owned(),
            serde_json::json!({ "name": n.name, "dryRun": n.dry_run }),
        );
    }
    if let (false, Some(obj)) = (services.is_empty(), data.as_object_mut()) {
        let svc_items: Vec<_> = services.iter().map(svc_to_json).collect();
        obj.insert("services".to_owned(), serde_json::json!(svc_items));
    }
    let envelope = output::success(data);
    output::write_json(writer, &envelope)?;
    Ok(())
}

/// Convert one result to a JSON value.
fn result_to_json(r: &ClusterResult) -> serde_json::Value {
    serde_json::json!({
        "name": r.name,
        "kindName": r.kind_name,
        "created": r.created,
        "dryRun": r.dry_run,
    })
}

/// Convert one service result to JSON.
fn svc_to_json(s: &ServiceResult) -> serde_json::Value {
    serde_json::json!({
        "name": s.name,
        "containerName": s.container_name,
        "dryRun": s.dry_run,
    })
}

/// Render results as text.
fn render_text(
    writer: &mut dyn Write,
    net: &Option<NetworkSetup>,
    clusters: &[ClusterResult],
    services: &[ServiceResult],
) -> Result<(), ForgeError> {
    if let Some(n) = net {
        output::write_text(writer, &format_net_text(n))?;
    }
    for r in clusters {
        output::write_text(writer, &format_result_text(r))?;
    }
    for s in services {
        output::write_text(writer, &format_svc_text(s))?;
    }
    Ok(())
}

/// Format a service result as a text line.
fn format_svc_text(s: &ServiceResult) -> String {
    if s.dry_run {
        return format!("would start service '{}' (container: {})", s.name, s.container_name);
    }
    format!("started service '{}' (container: {})", s.name, s.container_name)
}

/// Format a network setup result as a text line.
fn format_net_text(n: &NetworkSetup) -> String {
    if n.dry_run {
        return format!("would create network '{}'", n.name);
    }
    format!("network '{}' ready", n.name)
}

/// Format a single result as a text line.
fn format_result_text(r: &ClusterResult) -> String {
    if r.dry_run {
        return format!("would create cluster '{}' (kind name: {})", r.name, r.kind_name);
    }
    if r.created {
        return format!("created cluster '{}' (kind name: {})", r.name, r.kind_name);
    }
    format!("cluster '{}' already exists (kind name: {})", r.name, r.kind_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::runner::{CommandOutput, MockRunner};

    /// Build a minimal `ForgeConfig` with one cluster.
    fn test_config() -> crate::config::ForgeConfig {
        let yaml = "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: test
spec:
  runtime:
    provider: docker
    clusterPrefix: forge
  clusters:
    - name: hub
  services: []
  stacks: {}
";
        serde_yaml::from_str(yaml).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        })
    }

    /// Build a successful docker-version mock response.
    fn docker_ok() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: "Docker 24.0\n".to_owned(),
            stderr: String::new(),
        }
    }

    /// Build a successful empty command output.
    fn empty_ok() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    /// Docker version check failure (binary not found).
    fn docker_not_found() -> CommandOutput {
        CommandOutput {
            status: 1,
            stdout: String::new(),
            stderr: "not found\n".to_owned(),
        }
    }

    /// Successful Podman version output.
    fn podman_ok() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: "Podman 4.0\n".to_owned(),
            stderr: String::new(),
        }
    }

    /// Create a temp dir for test state.
    fn test_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        })
    }

    /// Build a mock that responds to docker, kind list, kind create.
    fn mock_for_create() -> MockRunner {
        let mut runner = MockRunner::new();
        runner.respond("docker version", docker_ok());
        runner.respond("kind get clusters", empty_ok());
        runner.respond("kind", empty_ok());
        runner
    }

    /// Run `up` with the given context and return output text.
    fn run_up(ctx: &ForgeContext<'_>) -> String {
        let mut buf = Vec::new();
        run(ctx, &mut buf).unwrap_or_else(|_| std::process::abort());
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[test]
    fn up_creates_missing_cluster() {
        let dir = test_dir();
        let config = test_config();
        let runner = mock_for_create();
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let text = run_up(&ctx);
        assert!(runner.was_called("kind create cluster"), "should call kind create");
        assert!(text.contains("created"), "output should mention created: {text}");
    }

    #[test]
    fn up_skips_existing_cluster() {
        let dir = test_dir();
        let config = test_config();
        let mut runner = MockRunner::new();
        runner.respond("docker version", docker_ok());
        runner.respond(
            "kind get clusters",
            CommandOutput {
                status: 0,
                stdout: "forge-hub\n".to_owned(),
                stderr: String::new(),
            },
        );
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let text = run_up(&ctx);
        assert!(!runner.was_called("kind create"), "should not call kind create");
        assert!(text.contains("already exists"), "output should note existing: {text}");
    }

    #[test]
    fn up_dry_run_does_not_create() {
        let dir = test_dir();
        let config = test_config();
        let mut runner = MockRunner::new();
        runner.respond("docker version", docker_ok());
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: true,
        };
        let text = run_up(&ctx);
        assert!(!runner.was_called("kind create"), "dry-run should not call kind create");
        assert!(text.contains("would create"), "should say would create: {text}");
    }

    #[test]
    fn health_probe_maps_container_port_to_host_port() {
        let yaml = "
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata: { name: test }
spec:
  runtime: { provider: docker, clusterPrefix: forge }
  services:
    - name: web
      image: example/web:v1
      ports:
        - { bindAddress: 127.0.0.1, host: 8080, container: 80, protocol: tcp }
  stacks: {}
";
        let config: crate::config::ForgeConfig = serde_yaml::from_str(yaml).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let svc = config.spec.services.first().unwrap_or_else(|| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(health_probe_host_port(svc, 80), Some(8080));
    }

    /// Build a config with `network.crossCluster: true`.
    fn test_config_with_network() -> crate::config::ForgeConfig {
        let yaml = "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: test
spec:
  runtime:
    provider: docker
    clusterPrefix: forge
  network:
    crossCluster: true
  clusters:
    - name: hub
  services: []
  stacks: {}
";
        serde_yaml::from_str(yaml).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        })
    }

    /// Network not-found response for inspect.
    fn net_not_found() -> CommandOutput {
        CommandOutput {
            status: 1,
            stdout: String::new(),
            stderr: "not found\n".to_owned(),
        }
    }

    #[test]
    fn up_creates_network_when_configured() {
        let dir = test_dir();
        let config = test_config_with_network();
        let mut runner = MockRunner::new();
        runner.respond("docker version", docker_ok());
        runner.respond("docker network inspect test-net", net_not_found());
        runner.respond("docker", empty_ok());
        runner.respond("kind get clusters", empty_ok());
        runner.respond("kind", empty_ok());
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let text = run_up(&ctx);
        assert!(runner.was_called("network create"), "should call network create");
        assert!(
            text.contains("network 'test-net' ready"),
            "should report network: {text}"
        );
        assert_kind_create_has_network_env(&runner, "test-net");
    }

    #[test]
    fn up_skips_network_without_config() {
        let dir = test_dir();
        let config = test_config();
        let runner = mock_for_create();
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let text = run_up(&ctx);
        assert!(!runner.was_called("network"), "should not call any network commands");
        assert!(!text.contains("network"), "should not mention network: {text}");
    }

    #[test]
    fn up_dry_run_reports_network() {
        let dir = test_dir();
        let config = test_config_with_network();
        let mut runner = MockRunner::new();
        runner.respond("docker version", docker_ok());
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: true,
        };
        let text = run_up(&ctx);
        assert!(
            !runner.was_called("network create"),
            "dry-run should not create network"
        );
        assert!(
            text.contains("would create network"),
            "should report would create network: {text}"
        );
    }

    #[test]
    fn set_network_active_preserves_existing_pools() {
        let mut st = state::empty();
        st.network = Some(state::NetworkState {
            name: "old-net".to_owned(),
            phase: NetworkPhase::Active,
            cidr: Some("172.18.0.0/16".to_owned()),
            cluster_pools: vec![state::ClusterPool {
                cluster: "hub".to_owned(),
                range: "172.18.255.231-172.18.255.250".to_owned(),
            }],
        });
        set_network_active(&mut st, "test-net");
        let net = st.network.as_ref().unwrap_or_else(|| std::process::abort());
        assert_eq!(net.name, "test-net", "name should update");
        assert_eq!(net.cidr.as_deref(), Some("172.18.0.0/16"), "cidr should be preserved");
        assert_eq!(net.cluster_pools.len(), 1, "pools should be preserved");
    }

    #[test]
    fn cross_cluster_auto_resolved_docker_passes() {
        let config = test_config_cross_auto();
        let dir = test_dir();
        let mut runner = MockRunner::new();
        runner.respond("docker version", docker_ok());
        runner.respond("docker network inspect test-net", net_not_found());
        runner.respond("docker", empty_ok());
        runner.respond("kind get clusters", empty_ok());
        runner.respond("kind", empty_ok());
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let text = run_up(&ctx);
        assert!(
            text.contains("network 'test-net' ready"),
            "auto+docker should succeed: {text}"
        );
    }

    #[test]
    fn cross_cluster_auto_resolved_podman_fails() {
        let config = test_config_cross_auto();
        let dir = test_dir();
        let mut runner = MockRunner::new();
        runner.respond("docker version", docker_not_found());
        runner.respond("podman version", podman_ok());
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let mut buf = Vec::new();
        let result = run(&ctx, &mut buf);
        assert!(result.is_err(), "auto+podman+crossCluster should fail");
        let Err(err) = result else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("Docker"), "error should mention Docker: {msg}");
    }

    #[test]
    fn cross_cluster_explicit_docker_passes() {
        let dir = test_dir();
        let config = test_config_with_network();
        let mut runner = MockRunner::new();
        runner.respond("docker version", docker_ok());
        runner.respond("docker network inspect test-net", net_not_found());
        runner.respond("docker", empty_ok());
        runner.respond("kind get clusters", empty_ok());
        runner.respond("kind", empty_ok());
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let text = run_up(&ctx);
        assert!(
            text.contains("network 'test-net' ready"),
            "explicit docker should succeed: {text}"
        );
    }

    #[test]
    fn no_cross_cluster_podman_allowed() {
        let config = test_config_podman();
        let dir = test_dir();
        let mut runner = MockRunner::new();
        runner.respond("podman version", podman_ok());
        runner.respond("kind get clusters", empty_ok());
        runner.respond("kind", empty_ok());
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let text = run_up(&ctx);
        assert!(
            text.contains("created"),
            "podman without crossCluster should succeed: {text}"
        );
    }

    // Test Utilities

    /// Verify `kind create` was called with the expected Docker network env.
    fn assert_kind_create_has_network_env(runner: &MockRunner, expected: &str) {
        let calls = runner.calls();
        let Some(call) = calls.iter().find(|c| c.to_string().contains("kind create")) else {
            std::process::abort();
        };
        let key = std::ffi::OsString::from("KIND_EXPERIMENTAL_DOCKER_NETWORK");
        let val = call.env.get(&key).map(|v| v.to_string_lossy().into_owned());
        assert_eq!(val.as_deref(), Some(expected), "kind create should set network env");
    }

    /// Config with `crossCluster: true` and `provider: auto`.
    fn test_config_cross_auto() -> crate::config::ForgeConfig {
        let yaml = "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: test
spec:
  runtime:
    provider: auto
    clusterPrefix: forge
  network:
    crossCluster: true
  clusters:
    - name: hub
  services: []
  stacks: {}
";
        serde_yaml::from_str(yaml).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        })
    }

    /// Config with `provider: podman` and no cross-cluster networking.
    fn test_config_podman() -> crate::config::ForgeConfig {
        let yaml = "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: test
spec:
  runtime:
    provider: podman
    clusterPrefix: forge
  clusters:
    - name: hub
  services: []
  stacks: {}
";
        serde_yaml::from_str(yaml).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        })
    }
}
