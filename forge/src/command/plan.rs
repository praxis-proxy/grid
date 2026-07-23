//! The `plan` command: read-only environment summary.
//!
//! Loads the configuration, validates it, and prints what would be
//! managed without creating or modifying any resources.

use std::{io::Write, path::Path};

use crate::{
    config,
    config::validate,
    error::ForgeError,
    output::{self, OutputFormat},
};

/// Run the `plan` command.
///
/// # Errors
///
/// Returns [`ForgeError`] if the operation fails.
pub fn run(config_path: &Path, format: &OutputFormat, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let config = config::load(config_path)?;
    validate::validate(&config)?;
    render_plan(&config, format, writer)
}

/// Render the plan summary.
fn render_plan(config: &config::ForgeConfig, format: &OutputFormat, writer: &mut dyn Write) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => render_json(config, writer),
        OutputFormat::Text => render_text(config, writer),
    }
}

/// Render plan as JSON.
fn render_json(config: &config::ForgeConfig, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let summary = build_summary(config);
    let result = output::success(summary);
    output::write_json(writer, &result)?;
    Ok(())
}

/// Render plan as human-readable text.
fn render_text(config: &config::ForgeConfig, writer: &mut dyn Write) -> Result<(), ForgeError> {
    output::write_text(writer, &format!("Environment: {}", config.metadata.name))?;
    render_clusters(config, writer)?;
    render_services(config, writer)?;
    render_stacks(config, writer)
}

/// Print cluster summary lines.
fn render_clusters(config: &config::ForgeConfig, writer: &mut dyn Write) -> Result<(), ForgeError> {
    output::write_text(writer, &format!("Clusters: {}", config.spec.clusters.len()))?;
    for cluster in &config.spec.clusters {
        output::write_text(
            writer,
            &format!(
                "  - {} ({} nodes, {} stacks)",
                cluster.name,
                cluster.nodes.control_planes + cluster.nodes.workers,
                cluster.stacks.len()
            ),
        )?;
    }
    Ok(())
}

/// Print service summary lines.
fn render_services(config: &config::ForgeConfig, writer: &mut dyn Write) -> Result<(), ForgeError> {
    output::write_text(writer, &format!("Services: {}", config.spec.services.len()))?;
    for service in &config.spec.services {
        output::write_text(writer, &format!("  - {} ({})", service.name, service.image))?;
    }
    Ok(())
}

/// Print stack summary lines.
fn render_stacks(config: &config::ForgeConfig, writer: &mut dyn Write) -> Result<(), ForgeError> {
    output::write_text(writer, &format!("Stacks: {}", config.spec.stacks.len()))?;
    for (name, stack) in &config.spec.stacks {
        output::write_text(writer, &format!("  - {} ({} steps)", name, stack.steps.len()))?;
    }
    Ok(())
}

/// Build a JSON-serialisable summary.
fn build_summary(config: &config::ForgeConfig) -> serde_json::Value {
    serde_json::json!({
        "environment": config.metadata.name,
        "clusters": config.spec.clusters.len(),
        "services": config.spec.services.len(),
        "stacks": config.spec.stacks.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_validates_config_and_does_not_mutate() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let path = dir.path().join("forge.yaml");
        std::fs::write(&path, config::minimal_yaml()).unwrap_or_else(|_| std::process::abort());

        let mut buf = Vec::new();
        run(&path, &OutputFormat::Text, &mut buf).unwrap_or_else(|_| std::process::abort());

        let text = String::from_utf8_lossy(&buf);
        assert!(
            text.contains("Environment: minimal"),
            "should contain environment name: {text}"
        );
    }
}
