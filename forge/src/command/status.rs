//! The `status` command: show environment status.
//!
//! Cross-references the persisted state, the live KIND cluster list,
//! and the configuration to produce a unified view.

use std::io::Write;

use crate::{
    cluster::kind as kind_ops,
    context::ForgeContext,
    error::ForgeError,
    output::{self, OutputFormat},
    state,
};

/// Run the `status` command (read-only, no lock).
///
/// # Errors
///
/// Returns [`ForgeError`] if state loading or KIND probing fails.
pub fn run(ctx: &ForgeContext<'_>, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let st = state::load(&ctx.state_dir)?;
    let live = kind_ops::list_clusters(ctx.runner)?;
    let entries = build_entries(ctx, &st, &live);
    let net_info = network_info(&st);
    let svc_entries = service_entries(&st);
    render_all(writer, &entries, &net_info, &svc_entries, &ctx.format)
}

// ---------------------------------------------------------------
// Status entries
// ---------------------------------------------------------------

/// Status information for one cluster.
struct StatusEntry {
    /// Cluster name from config.
    name: String,
    /// KIND cluster name.
    kind_name: String,
    /// State phase, if tracked.
    state_phase: String,
    /// Whether a live KIND cluster was found.
    live: bool,
}

/// Build status entries from config, state, and live clusters.
fn build_entries(ctx: &ForgeContext<'_>, st: &state::ForgeState, live: &[String]) -> Vec<StatusEntry> {
    ctx.config
        .spec
        .clusters
        .iter()
        .map(|c| entry_for_cluster(ctx, st, live, &c.name))
        .collect()
}

/// Build a status entry for one configured cluster.
fn entry_for_cluster(ctx: &ForgeContext<'_>, st: &state::ForgeState, live: &[String], name: &str) -> StatusEntry {
    let kind_name = kind_ops::kind_cluster_name(&ctx.config.spec.runtime.cluster_prefix, name);
    let state_phase = state_phase_label(st, name);
    let is_live = live.contains(&kind_name);
    StatusEntry {
        name: name.to_owned(),
        kind_name,
        state_phase,
        live: is_live,
    }
}

/// Get the state phase label for a cluster, or "unknown".
fn state_phase_label(st: &state::ForgeState, name: &str) -> String {
    state::find_cluster(st, name).map_or_else(|| "unknown".to_owned(), |c| format!("{:?}", c.phase).to_lowercase())
}

// ---------------------------------------------------------------
// Network status
// ---------------------------------------------------------------

/// Network status information.
struct NetInfo {
    /// Network name.
    name: String,
    /// Phase label (e.g. "active", "gone").
    phase: String,
}

/// Extract network status from state.
fn network_info(st: &state::ForgeState) -> Option<NetInfo> {
    st.network.as_ref().map(|ns| NetInfo {
        name: ns.name.clone(),
        phase: format!("{:?}", ns.phase).to_lowercase(),
    })
}

// ---------------------------------------------------------------
// Service status
// ---------------------------------------------------------------

/// Status information for one service.
struct SvcEntry {
    /// Service name.
    name: String,
    /// Container name.
    container_name: String,
    /// Phase label (e.g. "running", "stopped").
    phase: String,
    /// Health label (e.g. "healthy", "unknown").
    health: String,
}

/// Build service status entries from state.
fn service_entries(st: &state::ForgeState) -> Vec<SvcEntry> {
    st.services
        .iter()
        .map(|s| SvcEntry {
            name: s.name.clone(),
            container_name: s.container_name.clone(),
            phase: format!("{:?}", s.phase).to_lowercase(),
            health: format!("{:?}", s.health).to_lowercase(),
        })
        .collect()
}

// ---------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------

/// Render all status entries.
fn render_all(
    writer: &mut dyn Write,
    entries: &[StatusEntry],
    net: &Option<NetInfo>,
    services: &[SvcEntry],
    format: &OutputFormat,
) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => render_json(writer, entries, net, services),
        OutputFormat::Text => render_text(writer, entries, net, services),
    }
}

/// Render entries as JSON.
fn render_json(
    writer: &mut dyn Write,
    entries: &[StatusEntry],
    net: &Option<NetInfo>,
    services: &[SvcEntry],
) -> Result<(), ForgeError> {
    let items: Vec<_> = entries.iter().map(entry_to_json).collect();
    let mut data = serde_json::json!({ "clusters": items });
    if let (Some(n), Some(obj)) = (net, data.as_object_mut()) {
        obj.insert(
            "network".to_owned(),
            serde_json::json!({ "name": n.name, "phase": n.phase }),
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

/// Convert one service entry to JSON.
fn svc_to_json(s: &SvcEntry) -> serde_json::Value {
    serde_json::json!({
        "name": s.name,
        "containerName": s.container_name,
        "phase": s.phase,
        "health": s.health,
    })
}

/// Convert one entry to JSON.
fn entry_to_json(e: &StatusEntry) -> serde_json::Value {
    serde_json::json!({
        "name": e.name,
        "kindName": e.kind_name,
        "statePhase": e.state_phase,
        "live": e.live,
    })
}

/// Render entries as text.
fn render_text(
    writer: &mut dyn Write,
    entries: &[StatusEntry],
    net: &Option<NetInfo>,
    services: &[SvcEntry],
) -> Result<(), ForgeError> {
    if let Some(n) = net {
        output::write_text(writer, &format!("  network: {} ({})", n.name, n.phase))?;
    }
    for e in entries {
        output::write_text(writer, &format_entry_text(e))?;
    }
    for s in services {
        output::write_text(writer, &format_svc_text(s))?;
    }
    Ok(())
}

/// Format a service entry as a text line.
fn format_svc_text(s: &SvcEntry) -> String {
    format!(
        "  {}: phase={}, health={}, container={}",
        s.name, s.phase, s.health, s.container_name
    )
}

/// Format one entry as a text line.
fn format_entry_text(e: &StatusEntry) -> String {
    let live_label = if e.live { "live" } else { "not found" };
    format!(
        "  {}: state={}, kind={} ({})",
        e.name, e.state_phase, e.kind_name, live_label
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::runner::{CommandOutput, MockRunner},
        state::{ClusterPhase, ClusterState},
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

    /// Seed state with a running hub cluster.
    fn seed_running_hub(state_dir: &std::path::Path) {
        let mut st = state::empty();
        st.clusters.push(ClusterState {
            name: "hub".to_owned(),
            kind_name: "forge-hub".to_owned(),
            context: "kind-forge-hub".to_owned(),
            phase: ClusterPhase::Running,
        });
        state::save(state_dir, &st).unwrap_or_else(|_| std::process::abort());
    }

    /// Run status and return output text.
    fn run_status(ctx: &ForgeContext<'_>) -> String {
        let mut buf = Vec::new();
        run(ctx, &mut buf).unwrap_or_else(|_| std::process::abort());
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[test]
    fn status_reports_running_cluster() {
        let dir = test_dir();
        seed_running_hub(dir.path());
        let config = test_config();
        let mut runner = MockRunner::new();
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
        let text = run_status(&ctx);
        assert!(text.contains("running"), "should show running: {text}");
        assert!(text.contains("live"), "should show live: {text}");
    }

    #[test]
    fn status_reports_missing_cluster() {
        let dir = test_dir();
        let config = test_config();
        let mut runner = MockRunner::new();
        runner.respond(
            "kind get clusters",
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
            config_dir: dir.path().to_path_buf(),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let text = run_status(&ctx);
        assert!(text.contains("unknown"), "should show unknown state: {text}");
        assert!(text.contains("not found"), "should show not found: {text}");
    }

    /// Seed state with a running hub cluster and active network.
    fn seed_with_network(state_dir: &std::path::Path) {
        let mut st = state::empty();
        st.clusters.push(ClusterState {
            name: "hub".to_owned(),
            kind_name: "forge-hub".to_owned(),
            context: "kind-forge-hub".to_owned(),
            phase: ClusterPhase::Running,
        });
        st.network = Some(state::NetworkState {
            name: "test-net".to_owned(),
            phase: state::NetworkPhase::Active,
            cidr: None,
            cluster_pools: Vec::new(),
        });
        state::save(state_dir, &st).unwrap_or_else(|_| std::process::abort());
    }

    #[test]
    fn status_reports_network() {
        let dir = test_dir();
        seed_with_network(dir.path());
        let config = test_config();
        let mut runner = MockRunner::new();
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
        let text = run_status(&ctx);
        assert!(text.contains("test-net"), "should show network name: {text}");
        assert!(text.contains("active"), "should show network phase: {text}");
    }
}
