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
    runtime,
    state::{self, ClusterPhase, ClusterState, NetworkPhase, lock},
};

/// Run the `up` command.
///
/// # Errors
///
/// Returns [`ForgeError`] if runtime detection, cluster creation,
/// or state persistence fails.
pub fn run(ctx: &ForgeContext<'_>, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let resolved = runtime::resolve(ctx.runner, &ctx.config.spec.runtime.provider)?;
    let _lock = lock::acquire(&ctx.state_dir)?;
    let mut state = state::load(&ctx.state_dir)?;
    state.runtime = Some(resolved.binary.clone());
    let net_result = ensure_network(ctx, &resolved.binary, &mut state)?;
    let results = create_clusters(ctx, &mut state)?;
    update_digest(ctx, &mut state)?;
    record_operation(&mut state, "up", true);
    if !ctx.dry_run {
        state::save(&ctx.state_dir, &state)?;
    }
    render_results(writer, &net_result, &results, &ctx.format)
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

/// Record the network as active in state.
fn set_network_active(state: &mut state::ForgeState, name: &str) {
    state.network = Some(state::NetworkState {
        name: name.to_owned(),
        phase: NetworkPhase::Active,
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
    let mut results = Vec::new();
    for cluster in &ctx.config.spec.clusters {
        let r = process_cluster(ctx, state, cluster)?;
        results.push(r);
    }
    Ok(results)
}

/// Process a single cluster: create if missing, skip if exists.
fn process_cluster(
    ctx: &ForgeContext<'_>,
    state: &mut state::ForgeState,
    cluster: &ClusterSpec,
) -> Result<ClusterResult, ForgeError> {
    let kind_name = kind_ops::kind_cluster_name(&ctx.config.spec.runtime.cluster_prefix, &cluster.name);
    if ctx.dry_run {
        return Ok(dry_run_result(&cluster.name, &kind_name));
    }
    let created = create_if_missing(ctx, &kind_name, cluster, state)?;
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
) -> Result<bool, ForgeError> {
    if kind_ops::cluster_exists(ctx.runner, kind_name)? {
        ensure_state_entry(state, &cluster.name, kind_name, ClusterPhase::Running);
        return Ok(false);
    }
    kind_ops::create_cluster(ctx.runner, kind_name, &cluster.nodes, &ctx.state_dir)?;
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

/// Render cluster creation results.
fn render_results(
    writer: &mut dyn Write,
    net: &Option<NetworkSetup>,
    results: &[ClusterResult],
    format: &OutputFormat,
) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => render_json(writer, net, results),
        OutputFormat::Text => render_text(writer, net, results),
    }
}

/// Render results as JSON.
fn render_json(
    writer: &mut dyn Write,
    net: &Option<NetworkSetup>,
    results: &[ClusterResult],
) -> Result<(), ForgeError> {
    let items: Vec<_> = results.iter().map(result_to_json).collect();
    let mut data = serde_json::json!({ "clusters": items });
    if let (Some(n), Some(obj)) = (net, data.as_object_mut()) {
        obj.insert(
            "network".to_owned(),
            serde_json::json!({ "name": n.name, "dryRun": n.dry_run }),
        );
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

/// Render results as text.
fn render_text(
    writer: &mut dyn Write,
    net: &Option<NetworkSetup>,
    results: &[ClusterResult],
) -> Result<(), ForgeError> {
    if let Some(n) = net {
        output::write_text(writer, &format_net_text(n))?;
    }
    for r in results {
        output::write_text(writer, &format_result_text(r))?;
    }
    Ok(())
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
            format: OutputFormat::Text,
            dry_run: true,
        };
        let text = run_up(&ctx);
        assert!(!runner.was_called("kind create"), "dry-run should not call kind create");
        assert!(text.contains("would create"), "should say would create: {text}");
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
            format: OutputFormat::Text,
            dry_run: false,
        };
        let text = run_up(&ctx);
        assert!(runner.was_called("network create"), "should call network create");
        assert!(
            text.contains("network 'test-net' ready"),
            "should report network: {text}"
        );
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
}
