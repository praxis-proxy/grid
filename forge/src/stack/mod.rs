//! Deployment stack lifecycle management.
//!
//! Applies composable deployment stacks to KIND clusters.  Each
//! stack is a sequence of steps (kubectl apply, helm install, etc.)
//! executed through [`CommandRunner`](crate::command::runner::CommandRunner).
//! Templates allow cluster-specific customisation without
//! duplicating stack definitions.

pub mod engine;
pub mod steps;
pub mod template;

use std::io::Write;

use sha2::Digest as _;

use crate::{
    cli::StackCommand,
    config::{ClusterSpec, StackSpec},
    context::ForgeContext,
    error::ForgeError,
    output::{self, OutputFormat},
    state::{self, ClusterPool, StackPhase, StackState},
};

// -------------------------------------------------------------
// Public dispatch
// -------------------------------------------------------------

/// Dispatch a stack subcommand.
///
/// # Errors
///
/// Returns [`ForgeError`] if the operation fails.
pub fn dispatch(ctx: &ForgeContext<'_>, cmd: &StackCommand, writer: &mut dyn Write) -> Result<(), ForgeError> {
    match cmd {
        StackCommand::List => handle_list(ctx, writer),
        StackCommand::Plan { cluster, stack } => handle_plan(ctx, cluster, stack.as_deref(), writer),
        StackCommand::Apply { cluster, stack } => handle_apply(ctx, cluster, stack.as_deref(), writer),
        StackCommand::Status { cluster } => handle_status(ctx, cluster.as_deref(), writer),
    }
}

// -------------------------------------------------------------
// Handlers
// -------------------------------------------------------------

/// List configured stacks.
fn handle_list(ctx: &ForgeContext<'_>, writer: &mut dyn Write) -> Result<(), ForgeError> {
    match &ctx.format {
        OutputFormat::Json => render_list_json(ctx, writer),
        OutputFormat::Text => render_list_text(ctx, writer),
    }
}

/// Show what a stack apply would do.
fn handle_plan(
    ctx: &ForgeContext<'_>,
    cluster_name: &str,
    stack_filter: Option<&str>,
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    let cluster = lookup_cluster(ctx, cluster_name)?;
    let stacks = resolve_stacks(ctx, cluster, stack_filter)?;
    match &ctx.format {
        OutputFormat::Json => render_plan_json(cluster, &stacks, writer),
        OutputFormat::Text => render_plan_text(cluster, &stacks, writer),
    }
}

/// Apply stacks to a cluster.
fn handle_apply(
    ctx: &ForgeContext<'_>,
    cluster_name: &str,
    stack_filter: Option<&str>,
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    let cluster = lookup_cluster(ctx, cluster_name)?;
    let stacks = resolve_stacks(ctx, cluster, stack_filter)?;
    if ctx.dry_run {
        return handle_plan(ctx, cluster_name, stack_filter, writer);
    }
    let results = apply_stacks(ctx, cluster, &stacks)?;
    render_apply(cluster_name, &results, &ctx.format, writer)
}

/// Show applied stack status.
fn handle_status(
    ctx: &ForgeContext<'_>,
    cluster_filter: Option<&str>,
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    let st = state::load(&ctx.state_dir)?;
    let entries = filter_stack_states(&st, cluster_filter);
    match &ctx.format {
        OutputFormat::Json => render_status_json(&entries, writer),
        OutputFormat::Text => render_status_text(&entries, writer),
    }
}

// -------------------------------------------------------------
// Apply logic
// -------------------------------------------------------------

/// Result of one stack apply attempt.
struct ApplyResult {
    /// Stack name.
    name: String,
    /// Number of steps executed.
    steps_executed: usize,
    /// Whether the apply succeeded.
    success: bool,
}

/// Apply resolved stacks and persist state.
fn apply_stacks(
    ctx: &ForgeContext<'_>,
    cluster: &ClusterSpec,
    stacks: &[(&str, &StackSpec)],
) -> Result<Vec<ApplyResult>, ForgeError> {
    let _lock = state::lock::acquire(&ctx.state_dir)?;
    let mut st = state::load(&ctx.state_dir)?;
    let mut results = Vec::new();
    for (name, spec) in stacks {
        let r = apply_one(ctx, cluster, name, spec, &mut st);
        results.push(r);
    }
    state::save(&ctx.state_dir, &st)?;
    Ok(results)
}

/// Apply a single stack and update state.
fn apply_one(
    ctx: &ForgeContext<'_>,
    cluster: &ClusterSpec,
    name: &str,
    spec: &StackSpec,
    st: &mut state::ForgeState,
) -> ApplyResult {
    let digest = stack_digest(spec).ok();
    upsert_stack_state(st, name, &cluster.name, StackPhase::Applying, digest.as_deref());
    let network = build_network_params(ctx, cluster, st);
    match engine::apply_stack(ctx, cluster, name, spec, network.as_ref()) {
        Ok(r) => {
            if let Some(alloc) = &r.pool_allocation {
                record_pool_allocation(st, &cluster.name, alloc);
            }
            upsert_stack_state(st, name, &cluster.name, StackPhase::Applied, digest.as_deref());
            ApplyResult {
                name: name.to_owned(),
                steps_executed: r.steps_executed,
                success: true,
            }
        },
        Err(e) => {
            set_stack_failed(st, name, &cluster.name, digest.as_deref(), &e.to_string());
            ApplyResult {
                name: name.to_owned(),
                steps_executed: 0,
                success: false,
            }
        },
    }
}

// -------------------------------------------------------------
// Lookups
// -------------------------------------------------------------

/// Find a cluster in the config by name.
fn lookup_cluster<'a>(ctx: &'a ForgeContext<'_>, name: &str) -> Result<&'a ClusterSpec, ForgeError> {
    ctx.config
        .spec
        .clusters
        .iter()
        .find(|c| c.name == name)
        .ok_or_else(|| ForgeError::Config(format!("cluster '{name}' not found")))
}

/// Resolve which stacks to apply for a cluster.
fn resolve_stacks<'a>(
    ctx: &'a ForgeContext<'_>,
    cluster: &ClusterSpec,
    stack_filter: Option<&str>,
) -> Result<Vec<(&'a str, &'a StackSpec)>, ForgeError> {
    if let Some(name) = stack_filter {
        let entry = lookup_stack_entry(ctx, name)?;
        return Ok(vec![entry]);
    }
    resolve_cluster_stacks(ctx, cluster)
}

/// Resolve all stacks assigned to a cluster.
fn resolve_cluster_stacks<'a>(
    ctx: &'a ForgeContext<'_>,
    cluster: &ClusterSpec,
) -> Result<Vec<(&'a str, &'a StackSpec)>, ForgeError> {
    let mut result = Vec::new();
    for name in &cluster.stacks {
        result.push(lookup_stack_entry(ctx, name)?);
    }
    Ok(result)
}

/// Find a stack entry (key+value) in the config.
fn lookup_stack_entry<'a>(ctx: &'a ForgeContext<'_>, name: &str) -> Result<(&'a str, &'a StackSpec), ForgeError> {
    ctx.config
        .spec
        .stacks
        .get_key_value(name)
        .map(|(k, v)| (k.as_str(), v))
        .ok_or_else(|| ForgeError::Config(format!("stack '{name}' not found")))
}

// -------------------------------------------------------------
// State management
// -------------------------------------------------------------

/// Insert or update a stack state entry.
fn upsert_stack_state(st: &mut state::ForgeState, name: &str, cluster: &str, phase: StackPhase, digest: Option<&str>) {
    if let Some(existing) = state::find_stack_mut(st, name, cluster) {
        existing.phase = phase;
        existing.digest = digest.map(str::to_owned);
        existing.timestamp = state::now_epoch_secs();
        existing.error = None;
        return;
    }
    st.stacks.push(StackState {
        name: name.to_owned(),
        cluster: cluster.to_owned(),
        phase,
        digest: digest.map(str::to_owned),
        timestamp: state::now_epoch_secs(),
        error: None,
    });
}

/// Mark a stack as failed with an error message.
fn set_stack_failed(st: &mut state::ForgeState, name: &str, cluster: &str, digest: Option<&str>, message: &str) {
    if let Some(existing) = state::find_stack_mut(st, name, cluster) {
        existing.phase = StackPhase::Failed;
        existing.digest = digest.map(str::to_owned);
        existing.timestamp = state::now_epoch_secs();
        existing.error = Some(message.to_owned());
        return;
    }
    st.stacks.push(StackState {
        name: name.to_owned(),
        cluster: cluster.to_owned(),
        phase: StackPhase::Failed,
        digest: digest.map(str::to_owned),
        timestamp: state::now_epoch_secs(),
        error: Some(message.to_owned()),
    });
}

/// Compute a stable digest for the stack spec being applied.
fn stack_digest(spec: &StackSpec) -> Result<String, ForgeError> {
    let json = serde_json::to_string(spec)
        .map_err(|e| ForgeError::State(format!("cannot serialize stack spec for digest: {e}")))?;
    let hash = sha2::Sha256::digest(json.as_bytes());
    Ok(format!("{hash:x}"))
}

/// Filter stack states by optional cluster name.
fn filter_stack_states<'a>(st: &'a state::ForgeState, cluster: Option<&str>) -> Vec<&'a StackState> {
    st.stacks
        .iter()
        .filter(|s| cluster.is_none_or(|c| s.cluster == c))
        .collect()
}

// -------------------------------------------------------------
// Network integration
// -------------------------------------------------------------

/// Build [`engine::NetworkParams`] when cross-cluster networking is enabled.
fn build_network_params<'a>(
    ctx: &'a ForgeContext<'_>,
    cluster: &ClusterSpec,
    st: &'a state::ForgeState,
) -> Option<engine::NetworkParams<'a>> {
    let net_cfg = ctx.config.spec.network.as_ref().filter(|n| n.cross_cluster)?;
    let idx = cluster_index(ctx, &cluster.name);
    Some(engine::NetworkParams {
        cluster_pool: state::find_cluster_pool(st, &cluster.name),
        cluster_index: idx,
        cluster_count: ctx.config.spec.clusters.len(),
        dns_zone: net_cfg.dns_zone(),
    })
}

/// Find a cluster's position in the config cluster list.
fn cluster_index(ctx: &ForgeContext<'_>, name: &str) -> usize {
    ctx.config
        .spec
        .clusters
        .iter()
        .position(|c| c.name == name)
        .unwrap_or(0)
}

/// Record a newly computed pool allocation in state.
fn record_pool_allocation(st: &mut state::ForgeState, cluster: &str, alloc: &engine::PoolAllocation) {
    if let Some(ref mut net) = st.network {
        if net.cidr.is_none() {
            net.cidr = Some(alloc.cidr.clone());
        }
        let already = net.cluster_pools.iter().any(|p| p.cluster == cluster);
        if !already {
            net.cluster_pools.push(ClusterPool {
                cluster: cluster.to_owned(),
                range: alloc.range.clone(),
            });
        }
    }
}

// -------------------------------------------------------------
// List rendering
// -------------------------------------------------------------

/// Render stack list as JSON.
fn render_list_json(ctx: &ForgeContext<'_>, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let stacks: Vec<serde_json::Value> = ctx
        .config
        .spec
        .stacks
        .iter()
        .map(|(name, spec)| stack_list_entry(name, spec))
        .collect();
    let data = serde_json::json!({ "stacks": stacks });
    let result = output::success(data);
    output::write_json(writer, &result)?;
    Ok(())
}

/// Build a JSON entry for one stack in the list.
fn stack_list_entry(name: &str, spec: &StackSpec) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "steps": spec.steps.len(),
        "description": spec.description,
    })
}

/// Render stack list as text.
fn render_list_text(ctx: &ForgeContext<'_>, writer: &mut dyn Write) -> Result<(), ForgeError> {
    output::write_text(writer, &format!("Stacks: {}", ctx.config.spec.stacks.len()))?;
    for (name, spec) in &ctx.config.spec.stacks {
        output::write_text(writer, &format!("  - {name} ({} steps)", spec.steps.len()))?;
    }
    Ok(())
}

// -------------------------------------------------------------
// Plan rendering
// -------------------------------------------------------------

/// Render plan as JSON.
fn render_plan_json(
    cluster: &ClusterSpec,
    stacks: &[(&str, &StackSpec)],
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    let entries: Vec<serde_json::Value> = stacks
        .iter()
        .map(|(name, spec)| plan_entry(&cluster.name, name, spec))
        .collect();
    let data = serde_json::json!({ "cluster": cluster.name, "stacks": entries });
    let result = output::success(data);
    output::write_json(writer, &result)?;
    Ok(())
}

/// Build a JSON entry for one planned stack.
fn plan_entry(cluster: &str, name: &str, spec: &StackSpec) -> serde_json::Value {
    let steps: Vec<serde_json::Value> = spec.steps.iter().map(step_plan_entry).collect();
    serde_json::json!({
        "cluster": cluster,
        "stack": name,
        "steps": steps,
    })
}

/// Build a JSON entry for one planned step.
fn step_plan_entry(step: &crate::config::StepSpec) -> serde_json::Value {
    serde_json::json!({
        "type": step_type_label(step),
        "description": step_description(step),
        "warning": step_warning(step),
    })
}

/// Render plan as text.
fn render_plan_text(
    cluster: &ClusterSpec,
    stacks: &[(&str, &StackSpec)],
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    for (name, spec) in stacks {
        output::write_text(writer, &format!("Stack: {name} -> {}", cluster.name))?;
        render_plan_steps(&spec.steps, writer)?;
    }
    Ok(())
}

/// Render step list for plan text output.
fn render_plan_steps(steps_list: &[crate::config::StepSpec], writer: &mut dyn Write) -> Result<(), ForgeError> {
    for (i, step) in steps_list.iter().enumerate() {
        let idx = i.saturating_add(1);
        let label = step_type_label(step);
        let desc = step_description(step);
        output::write_text(writer, &format!("  {idx}. [{label}] {desc}"))?;
        if let Some(warning) = step_warning(step) {
            output::write_text(writer, &format!("     WARNING: {warning}"))?;
        }
    }
    Ok(())
}

// -------------------------------------------------------------
// Apply rendering
// -------------------------------------------------------------

/// Render apply results.
fn render_apply(
    cluster: &str,
    results: &[ApplyResult],
    format: &OutputFormat,
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => render_apply_json(cluster, results, writer),
        OutputFormat::Text => render_apply_text(cluster, results, writer),
    }
}

/// Render apply results as JSON.
fn render_apply_json(cluster: &str, results: &[ApplyResult], writer: &mut dyn Write) -> Result<(), ForgeError> {
    let entries: Vec<serde_json::Value> = results.iter().map(apply_entry).collect();
    let data = serde_json::json!({ "cluster": cluster, "stacks": entries });
    let result = output::success(data);
    output::write_json(writer, &result)?;
    Ok(())
}

/// Build a JSON entry for one apply result.
fn apply_entry(r: &ApplyResult) -> serde_json::Value {
    serde_json::json!({
        "name": r.name,
        "stepsExecuted": r.steps_executed,
        "success": r.success,
    })
}

/// Render apply results as text.
fn render_apply_text(cluster: &str, results: &[ApplyResult], writer: &mut dyn Write) -> Result<(), ForgeError> {
    for r in results {
        let status = if r.success { "applied" } else { "FAILED" };
        output::write_text(
            writer,
            &format!("{status} stack {} -> {cluster} ({} steps)", r.name, r.steps_executed),
        )?;
    }
    Ok(())
}

// -------------------------------------------------------------
// Status rendering
// -------------------------------------------------------------

/// Render status as JSON.
fn render_status_json(entries: &[&StackState], writer: &mut dyn Write) -> Result<(), ForgeError> {
    let stacks: Vec<serde_json::Value> = entries.iter().map(|s| status_entry(s)).collect();
    let data = serde_json::json!({ "stacks": stacks });
    let result = output::success(data);
    output::write_json(writer, &result)?;
    Ok(())
}

/// Build a JSON entry for one stack state.
fn status_entry(s: &StackState) -> serde_json::Value {
    serde_json::json!({
        "name": s.name,
        "cluster": s.cluster,
        "phase": format!("{:?}", s.phase).to_lowercase(),
        "digest": s.digest,
        "timestamp": s.timestamp,
    })
}

/// Render status as text.
fn render_status_text(entries: &[&StackState], writer: &mut dyn Write) -> Result<(), ForgeError> {
    output::write_text(writer, &format!("Stacks: {}", entries.len()))?;
    for s in entries {
        let phase = format!("{:?}", s.phase).to_lowercase();
        output::write_text(writer, &format!("  {}/{}: {phase}", s.cluster, s.name))?;
    }
    Ok(())
}

// -------------------------------------------------------------
// Step description helpers
// -------------------------------------------------------------

/// Return a short type label for a step.
fn step_type_label(step: &crate::config::StepSpec) -> &'static str {
    match step {
        crate::config::StepSpec::Url { .. } => "url",
        crate::config::StepSpec::Manifest { .. } => "manifest",
        crate::config::StepSpec::Kustomize { .. } => "kustomize",
        crate::config::StepSpec::Helm { .. } => "helm",
        crate::config::StepSpec::Deployment { .. } => "deployment",
        crate::config::StepSpec::Service { .. } => "service",
        crate::config::StepSpec::Wait { .. } => "wait",
        crate::config::StepSpec::Exec { .. } => "exec",
        crate::config::StepSpec::ForEach { .. } => "for-each",
        crate::config::StepSpec::MetallbAutoPool { .. } => "metallb-auto-pool",
        crate::config::StepSpec::CoreDnsForward { .. } => "core-dns-forward",
    }
}

/// Return a human-readable description for a step.
fn step_description(step: &crate::config::StepSpec) -> String {
    match step {
        crate::config::StepSpec::Url { url, .. } => format!("download {url}"),
        crate::config::StepSpec::Manifest { path } => format!("apply {path}"),
        crate::config::StepSpec::Kustomize { path } => format!("kustomize {path}"),
        crate::config::StepSpec::Helm { release, chart, .. } => format!("helm {release} ({chart})"),
        crate::config::StepSpec::Deployment { name, image, .. } => format!("deploy {name} ({image})"),
        crate::config::StepSpec::Service { name, port, .. } => format!("service {name}:{port}"),
        crate::config::StepSpec::Wait { resource, .. } => format!("wait {resource}"),
        crate::config::StepSpec::Exec { command } => command
            .first()
            .map_or_else(|| "exec <empty>".to_owned(), |p| format!("exec {p}")),
        crate::config::StepSpec::ForEach { property, steps } => format!("for-each {property} ({} steps)", steps.len()),
        crate::config::StepSpec::MetallbAutoPool { name } => format!("metallb pool {name}"),
        crate::config::StepSpec::CoreDnsForward { zone, .. } => format!("coredns forward {zone}"),
    }
}

/// Return a warning for steps that deserve explicit operator attention.
fn step_warning(step: &crate::config::StepSpec) -> Option<&'static str> {
    match step {
        crate::config::StepSpec::Exec { .. } => Some("exec is an explicit command escape hatch"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::config::{
        API_VERSION, ClusterSpec, EnvironmentSpec, ForgeConfig, KIND, Metadata, NetworkConfig, NodeConfig,
        RuntimeConfig, StackSpec, StepSpec,
    };

    fn test_stack() -> StackSpec {
        StackSpec {
            description: Some("Base stack".to_owned()),
            steps: vec![
                StepSpec::Manifest {
                    path: "crds.yaml".to_owned(),
                },
                StepSpec::Wait {
                    resource: "deployment/controller".to_owned(),
                    condition: "available".to_owned(),
                    timeout: "60s".to_owned(),
                },
            ],
        }
    }

    fn test_config() -> ForgeConfig {
        ForgeConfig {
            api_version: API_VERSION.to_owned(),
            kind: KIND.to_owned(),
            metadata: Metadata {
                name: "test".to_owned(),
            },
            spec: EnvironmentSpec {
                runtime: RuntimeConfig::default(),
                network: None,
                clusters: vec![ClusterSpec {
                    name: "hub".to_owned(),
                    nodes: NodeConfig::default(),
                    stacks: vec!["base".to_owned()],
                    properties: BTreeMap::new(),
                }],
                services: Vec::new(),
                certificates: None,
                stacks: BTreeMap::from([("base".to_owned(), test_stack())]),
            },
        }
    }

    #[test]
    fn handle_list_renders_configured_stacks() {
        let config = test_config();
        let runner = crate::command::runner::MockRunner::new();
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: std::path::PathBuf::from("/tmp/state"),
            config_dir: std::path::PathBuf::from("/tmp"),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let mut buf = Vec::new();
        handle_list(&ctx, &mut buf).unwrap_or_else(|_| std::process::abort());
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("Stacks: 1"), "should show stack count: {text}");
        assert!(text.contains("base (2 steps)"), "should list base stack: {text}");
    }

    #[test]
    fn handle_plan_renders_step_descriptions() {
        let config = test_config();
        let runner = crate::command::runner::MockRunner::new();
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: std::path::PathBuf::from("/tmp/state"),
            config_dir: std::path::PathBuf::from("/tmp"),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let mut buf = Vec::new();
        handle_plan(&ctx, "hub", None, &mut buf).unwrap_or_else(|_| std::process::abort());
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("Stack: base -> hub"), "should show stack target: {text}");
        assert!(text.contains("[manifest]"), "should describe manifest step: {text}");
        assert!(text.contains("[wait]"), "should describe wait step: {text}");
    }

    #[test]
    fn build_network_params_returns_none_without_cross_cluster() {
        let config = test_config();
        let runner = crate::command::runner::MockRunner::new();
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: std::path::PathBuf::from("/tmp/state"),
            config_dir: std::path::PathBuf::from("/tmp"),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let cluster = &config.spec.clusters.first().unwrap_or_else(|| std::process::abort());
        let st = state::empty();
        let result = build_network_params(&ctx, cluster, &st);
        assert!(result.is_none(), "should return None without crossCluster");
    }

    #[test]
    fn build_network_params_returns_some_with_cross_cluster() {
        let mut config = test_config();
        config.spec.network = Some(NetworkConfig {
            cross_cluster: true,
            dns_zone: None,
        });
        let runner = crate::command::runner::MockRunner::new();
        let ctx = ForgeContext {
            runner: &runner,
            config: &config,
            state_dir: std::path::PathBuf::from("/tmp/state"),
            config_dir: std::path::PathBuf::from("/tmp"),
            format: OutputFormat::Text,
            dry_run: false,
        };
        let cluster = &config.spec.clusters.first().unwrap_or_else(|| std::process::abort());
        let st = state::empty();
        let result = build_network_params(&ctx, cluster, &st);
        assert!(result.is_some(), "should return Some with crossCluster");
        let params = result.unwrap_or_else(|| std::process::abort());
        assert_eq!(params.dns_zone, "forge.test", "should default to forge.test");
        assert_eq!(params.cluster_index, 0, "hub should be index 0");
    }
}
