//! The `down` command: tear down all managed clusters.
//!
//! Deletes clusters tracked in state, updates state to `Gone`,
//! and reports the result.

use std::io::Write;

use crate::{
    cluster::kind as kind_ops,
    context::ForgeContext,
    error::ForgeError,
    networking,
    output::{self, OutputFormat},
    runtime,
    state::{self, ClusterPhase, NetworkPhase, lock},
};

/// Run the `down` command.
///
/// # Errors
///
/// Returns [`ForgeError`] if cluster deletion or state
/// persistence fails.
pub fn run(ctx: &ForgeContext<'_>, _force: bool, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let _lock = lock::acquire(&ctx.state_dir)?;
    let mut st = state::load(&ctx.state_dir)?;
    let results = delete_clusters(ctx, &mut st)?;
    let net_result = remove_env_network(ctx, &mut st)?;
    record_operation(&mut st, "down", true);
    if !ctx.dry_run {
        state::save(&ctx.state_dir, &st)?;
    }
    render_results(writer, &results, &net_result, &ctx.format)
}

// ---------------------------------------------------------------
// Cluster deletion
// ---------------------------------------------------------------

/// Result of processing one cluster for deletion.
struct DeleteResult {
    /// Cluster config name.
    name: String,
    /// KIND cluster name.
    kind_name: String,
    /// Whether this was a dry-run skip.
    dry_run: bool,
}

/// Delete clusters in reverse order from state.
fn delete_clusters(ctx: &ForgeContext<'_>, state: &mut state::ForgeState) -> Result<Vec<DeleteResult>, ForgeError> {
    let targets = collect_targets(state);
    let mut results = Vec::new();
    for (name, kind_name) in targets.into_iter().rev() {
        let r = delete_one(ctx, state, &name, &kind_name)?;
        results.push(r);
    }
    Ok(results)
}

/// Collect (name, `kind_name`) pairs from state for deletion.
fn collect_targets(state: &state::ForgeState) -> Vec<(String, String)> {
    state
        .clusters
        .iter()
        .filter(|c| c.phase != ClusterPhase::Gone)
        .map(|c| (c.name.clone(), c.kind_name.clone()))
        .collect()
}

/// Delete a single cluster or report dry-run.
fn delete_one(
    ctx: &ForgeContext<'_>,
    state: &mut state::ForgeState,
    name: &str,
    kind_name: &str,
) -> Result<DeleteResult, ForgeError> {
    if ctx.dry_run {
        return Ok(DeleteResult {
            name: name.to_owned(),
            kind_name: kind_name.to_owned(),
            dry_run: true,
        });
    }
    kind_ops::delete_cluster(ctx.runner, kind_name)?;
    mark_gone(state, name);
    Ok(DeleteResult {
        name: name.to_owned(),
        kind_name: kind_name.to_owned(),
        dry_run: false,
    })
}

/// Mark a cluster as `Gone` in state.
fn mark_gone(state: &mut state::ForgeState, name: &str) {
    if let Some(cs) = state::find_cluster_mut(state, name) {
        cs.phase = ClusterPhase::Gone;
    }
}

// ---------------------------------------------------------------
// Network teardown
// ---------------------------------------------------------------

/// Result of network teardown.
struct NetworkTeardown {
    /// Network name.
    name: String,
    /// Whether this was a dry-run skip.
    dry_run: bool,
}

/// Remove the environment network if one is tracked in state.
fn remove_env_network(
    ctx: &ForgeContext<'_>,
    state: &mut state::ForgeState,
) -> Result<Option<NetworkTeardown>, ForgeError> {
    let net = match &state.network {
        Some(ns) if ns.phase != NetworkPhase::Gone => ns.clone(),
        _ => return Ok(None),
    };
    if ctx.dry_run {
        return Ok(Some(NetworkTeardown {
            name: net.name,
            dry_run: true,
        }));
    }
    let binary = resolve_binary(ctx, state)?;
    let env_name = &ctx.config.metadata.name;
    networking::remove_network(ctx.runner, &binary, &net.name, env_name)?;
    mark_network_gone(state);
    Ok(Some(NetworkTeardown {
        name: net.name,
        dry_run: false,
    }))
}

/// Get the runtime binary from state or by re-detecting.
fn resolve_binary(ctx: &ForgeContext<'_>, state: &state::ForgeState) -> Result<String, ForgeError> {
    if let Some(binary) = &state.runtime {
        return Ok(binary.clone());
    }
    let resolved = runtime::resolve(ctx.runner, &ctx.config.spec.runtime.provider)?;
    Ok(resolved.binary)
}

/// Mark the network as gone in state.
fn mark_network_gone(state: &mut state::ForgeState) {
    if let Some(ref mut ns) = state.network {
        ns.phase = NetworkPhase::Gone;
    }
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

/// Render deletion results.
fn render_results(
    writer: &mut dyn Write,
    results: &[DeleteResult],
    net: &Option<NetworkTeardown>,
    format: &OutputFormat,
) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => render_json(writer, results, net),
        OutputFormat::Text => render_text(writer, results, net),
    }
}

/// Render results as JSON.
fn render_json(
    writer: &mut dyn Write,
    results: &[DeleteResult],
    net: &Option<NetworkTeardown>,
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

/// Convert one result to JSON.
fn result_to_json(r: &DeleteResult) -> serde_json::Value {
    serde_json::json!({
        "name": r.name,
        "kindName": r.kind_name,
        "dryRun": r.dry_run,
    })
}

/// Render results as text.
fn render_text(
    writer: &mut dyn Write,
    results: &[DeleteResult],
    net: &Option<NetworkTeardown>,
) -> Result<(), ForgeError> {
    for r in results {
        output::write_text(writer, &format_result_text(r))?;
    }
    if let Some(n) = net {
        output::write_text(writer, &format_net_text(n))?;
    }
    Ok(())
}

/// Format a network teardown result as a text line.
fn format_net_text(n: &NetworkTeardown) -> String {
    if n.dry_run {
        return format!("would remove network '{}'", n.name);
    }
    format!("removed network '{}'", n.name)
}

/// Format a single result as text.
fn format_result_text(r: &DeleteResult) -> String {
    if r.dry_run {
        return format!("would delete cluster '{}' (kind name: {})", r.name, r.kind_name);
    }
    format!("deleted cluster '{}' (kind name: {})", r.name, r.kind_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::runner::{CommandOutput, MockRunner},
        state::ClusterState,
    };

    /// Build a minimal config.
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

    /// Pre-populate state with a running cluster.
    fn seed_state(state_dir: &std::path::Path) {
        let mut st = state::empty();
        st.clusters.push(ClusterState {
            name: "hub".to_owned(),
            kind_name: "forge-hub".to_owned(),
            context: "kind-forge-hub".to_owned(),
            phase: ClusterPhase::Running,
        });
        state::save(state_dir, &st).unwrap_or_else(|_| std::process::abort());
    }

    #[test]
    fn down_deletes_cluster() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        seed_state(dir.path());
        let config = test_config();
        let mut runner = MockRunner::new();
        runner.respond(
            "kind",
            CommandOutput {
                status: 0,
                stdout: String::new(),
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
        let mut buf = Vec::new();
        run(&ctx, false, &mut buf).unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("kind delete cluster"), "should call kind delete");
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("deleted"), "should say deleted: {text}");
    }

    #[test]
    fn down_dry_run_does_not_delete() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        seed_state(dir.path());
        let config = test_config();
        let runner = MockRunner::new();
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: true,
        };
        let mut buf = Vec::new();
        run(&ctx, false, &mut buf).unwrap_or_else(|_| std::process::abort());
        assert!(!runner.was_called("kind delete"), "dry-run should not call kind delete");
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("would delete"), "should say would delete: {text}");
    }

    /// Pre-populate state with a running cluster and active network.
    fn seed_state_with_network(state_dir: &std::path::Path) {
        let mut st = state::empty();
        st.runtime = Some("docker".to_owned());
        st.clusters.push(ClusterState {
            name: "hub".to_owned(),
            kind_name: "forge-hub".to_owned(),
            context: "kind-forge-hub".to_owned(),
            phase: ClusterPhase::Running,
        });
        st.network = Some(state::NetworkState {
            name: "test-net".to_owned(),
            phase: NetworkPhase::Active,
        });
        state::save(state_dir, &st).unwrap_or_else(|_| std::process::abort());
    }

    /// Labels JSON for ownership verification.
    fn owned_labels() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: r#"{"forge.managed":"true","forge.environment":"test"}"#.to_owned(),
            stderr: String::new(),
        }
    }

    /// Successful empty output.
    fn ok() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    #[test]
    fn down_removes_network() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        seed_state_with_network(dir.path());
        let config = test_config();
        let mut runner = MockRunner::new();
        runner.respond("kind", ok());
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            owned_labels(),
        );
        runner.respond("docker network rm test-net", ok());
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let mut buf = Vec::new();
        run(&ctx, false, &mut buf).unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("network rm"), "should remove network");
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("removed network"), "should report removal: {text}");
    }

    #[test]
    fn down_dry_run_reports_network() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        seed_state_with_network(dir.path());
        let config = test_config();
        let runner = MockRunner::new();
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: true,
        };
        let mut buf = Vec::new();
        run(&ctx, false, &mut buf).unwrap_or_else(|_| std::process::abort());
        assert!(!runner.was_called("network rm"), "dry-run should not remove network");
        let text = String::from_utf8_lossy(&buf);
        assert!(
            text.contains("would remove network"),
            "should report would remove network: {text}"
        );
    }
}
