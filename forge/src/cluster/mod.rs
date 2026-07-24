//! Cluster lifecycle management.
//!
//! Dispatches cluster subcommands to KIND-specific operations in
//! [`kind`].

pub mod kind;

use std::io::Write;

use crate::{
    cli::ClusterCommand,
    cluster::kind as kind_ops,
    context::ForgeContext,
    error::ForgeError,
    output::{self, OutputFormat},
    state::{self, ClusterPhase, ClusterState, lock},
};

/// Dispatch a cluster subcommand.
///
/// # Errors
///
/// Returns [`ForgeError`] if the operation fails.
pub fn dispatch(ctx: &ForgeContext<'_>, cmd: &ClusterCommand, writer: &mut dyn Write) -> Result<(), ForgeError> {
    match cmd {
        ClusterCommand::Create { name } => handle_create(ctx, name, writer),
        ClusterCommand::Delete { name, force } => handle_delete(ctx, name, *force, writer),
        ClusterCommand::List => handle_list(ctx, writer),
        ClusterCommand::Kubeconfig { name, out_file } => handle_kubeconfig(ctx, name, out_file, writer),
        ClusterCommand::LoadImage { name, image } => handle_load_image(ctx, name, image, writer),
        ClusterCommand::Kubectl { name, args } => handle_kubectl(ctx, name, args, writer),
    }
}

// ---------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------

/// Handle `cluster create`.
fn handle_create(ctx: &ForgeContext<'_>, name: &str, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let cluster = lookup_cluster(ctx, name)?;
    let kind_name = cluster_kind_name(ctx, name);
    if ctx.dry_run {
        return report_dry_run(writer, "would create cluster", name, &kind_name, &ctx.format);
    }
    let _lock = lock::acquire(&ctx.state_dir)?;
    let mut state = state::load(&ctx.state_dir)?;
    create_if_missing(ctx, &kind_name, &cluster.nodes, &mut state, name)?;
    state::save(&ctx.state_dir, &state)?;
    report_created(writer, name, &kind_name, &ctx.format)
}

/// Handle `cluster delete`.
fn handle_delete(ctx: &ForgeContext<'_>, name: &str, _force: bool, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let kind_name = cluster_kind_name(ctx, name);
    if ctx.dry_run {
        return report_dry_run(writer, "would delete cluster", name, &kind_name, &ctx.format);
    }
    let _lock = lock::acquire(&ctx.state_dir)?;
    kind_ops::delete_cluster(ctx.runner, &kind_name)?;
    update_phase_gone(ctx, name)?;
    report_deleted(writer, name, &kind_name, &ctx.format)
}

/// Handle `cluster list`.
fn handle_list(ctx: &ForgeContext<'_>, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let clusters = kind_ops::list_clusters(ctx.runner)?;
    render_list(writer, &clusters, &ctx.format)
}

/// Handle `cluster kubeconfig`.
fn handle_kubeconfig(
    ctx: &ForgeContext<'_>,
    name: &str,
    output_path: &Option<std::path::PathBuf>,
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    let kind_name = cluster_kind_name(ctx, name);
    let kubeconfig = kind_ops::get_kubeconfig(ctx.runner, &kind_name)?;
    write_kubeconfig(writer, output_path, &kubeconfig, &ctx.format)
}

/// Handle `cluster load-image`.
fn handle_load_image(
    ctx: &ForgeContext<'_>,
    name: &str,
    image: &str,
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    let kind_name = cluster_kind_name(ctx, name);
    if ctx.dry_run {
        return report_dry_run(writer, "would load image into cluster", name, &kind_name, &ctx.format);
    }
    kind_ops::load_image(ctx.runner, &kind_name, image)?;
    report_image_loaded(writer, name, image, &ctx.format)
}

/// Handle `cluster kubectl`.
fn handle_kubectl(
    ctx: &ForgeContext<'_>,
    name: &str,
    args: &[String],
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    let kind_name = cluster_kind_name(ctx, name);
    let result = kind_ops::run_kubectl(ctx.runner, &kind_name, args)?;
    output::write_text(writer, &result.stdout)?;
    Ok(())
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

/// Look up a cluster in the config by name.
fn lookup_cluster<'a>(ctx: &'a ForgeContext<'_>, name: &str) -> Result<&'a crate::config::ClusterSpec, ForgeError> {
    ctx.config
        .spec
        .clusters
        .iter()
        .find(|c| c.name == name)
        .ok_or_else(|| ForgeError::Config(format!("cluster '{name}' not found in config")))
}

/// Build the KIND cluster name from config prefix and cluster name.
fn cluster_kind_name(ctx: &ForgeContext<'_>, name: &str) -> String {
    kind_ops::kind_cluster_name(&ctx.config.spec.runtime.cluster_prefix, name)
}

/// Create a cluster if it doesn't already exist.
fn create_if_missing(
    ctx: &ForgeContext<'_>,
    kind_name: &str,
    nodes: &crate::config::NodeConfig,
    st: &mut state::ForgeState,
    name: &str,
) -> Result<(), ForgeError> {
    if kind_ops::cluster_exists(ctx.runner, kind_name)? {
        return Ok(());
    }
    kind_ops::create_cluster(ctx.runner, kind_name, nodes, &ctx.state_dir, None)?;
    upsert_cluster_state(st, name, kind_name, ClusterPhase::Running);
    Ok(())
}

/// Insert or update a cluster's state entry.
fn upsert_cluster_state(st: &mut state::ForgeState, name: &str, kind_name: &str, phase: ClusterPhase) {
    if let Some(cs) = state::find_cluster_mut(st, name) {
        cs.phase = phase;
        return;
    }
    st.clusters.push(ClusterState {
        name: name.to_owned(),
        kind_name: kind_name.to_owned(),
        context: kind_ops::kubectl_context(kind_name),
        phase,
    });
}

/// Update a cluster's phase to `Gone` in state.
fn update_phase_gone(ctx: &ForgeContext<'_>, name: &str) -> Result<(), ForgeError> {
    let mut st = state::load(&ctx.state_dir)?;
    if let Some(cs) = state::find_cluster_mut(&mut st, name) {
        cs.phase = ClusterPhase::Gone;
    }
    state::save(&ctx.state_dir, &st)
}

/// Write kubeconfig to file or writer.
fn write_kubeconfig(
    writer: &mut dyn Write,
    output_path: &Option<std::path::PathBuf>,
    kubeconfig: &str,
    format: &OutputFormat,
) -> Result<(), ForgeError> {
    if let Some(path) = output_path {
        std::fs::write(path, kubeconfig).map_err(ForgeError::Io)?;
        return report_text_or_json(writer, &format!("kubeconfig written to {}", path.display()), format);
    }
    output::write_text(writer, kubeconfig)?;
    Ok(())
}

// ---------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------

/// Report a dry-run action.
fn report_dry_run(
    writer: &mut dyn Write,
    action: &str,
    name: &str,
    kind_name: &str,
    format: &OutputFormat,
) -> Result<(), ForgeError> {
    let msg = format!("{action} '{name}' (kind name: {kind_name})");
    report_text_or_json(writer, &msg, format)
}

/// Report a created cluster.
fn report_created(
    writer: &mut dyn Write,
    name: &str,
    kind_name: &str,
    format: &OutputFormat,
) -> Result<(), ForgeError> {
    let msg = format!("created cluster '{name}' (kind name: {kind_name})");
    report_text_or_json(writer, &msg, format)
}

/// Report a deleted cluster.
fn report_deleted(
    writer: &mut dyn Write,
    name: &str,
    kind_name: &str,
    format: &OutputFormat,
) -> Result<(), ForgeError> {
    let msg = format!("deleted cluster '{name}' (kind name: {kind_name})");
    report_text_or_json(writer, &msg, format)
}

/// Report an image loaded.
fn report_image_loaded(
    writer: &mut dyn Write,
    name: &str,
    image: &str,
    format: &OutputFormat,
) -> Result<(), ForgeError> {
    let msg = format!("loaded image '{image}' into cluster '{name}'");
    report_text_or_json(writer, &msg, format)
}

/// Render a cluster list.
fn render_list(writer: &mut dyn Write, clusters: &[String], format: &OutputFormat) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => {
            let envelope = output::success(serde_json::json!({ "clusters": clusters }));
            output::write_json(writer, &envelope)?;
        },
        OutputFormat::Text => {
            for c in clusters {
                output::write_text(writer, c)?;
            }
        },
    }
    Ok(())
}

/// Write a message as text or JSON envelope.
fn report_text_or_json(writer: &mut dyn Write, message: &str, format: &OutputFormat) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => {
            let envelope = output::success(serde_json::json!({ "message": message }));
            output::write_json(writer, &envelope)?;
        },
        OutputFormat::Text => {
            output::write_text(writer, message)?;
        },
    }
    Ok(())
}
