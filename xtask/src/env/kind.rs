//! Kind cluster lifecycle management.

use std::process::Command;

use crate::env::config::{ClusterDef, ClusterRole};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Prefix for kind cluster names.
const CLUSTER_PREFIX: &str = "grid-";

/// Container image for llm-d inference simulator.
const INFERENCE_SIM_IMAGE: &str = "ghcr.io/llm-d/llm-d-inference-sim:latest";

/// Kubernetes namespace for inference sim deployments.
const NAMESPACE: &str = "default";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create a kind cluster and deploy llm-d-inference-sim if the
/// cluster role is [`ClusterRole::Provider`].
///
/// Idempotent: if the cluster already exists, this is a no-op.
///
/// # Errors
///
/// Returns an error if `kind` or `kubectl` commands fail.
pub(crate) fn create_cluster(name: &str, def: &ClusterDef) -> Result<(), Box<dyn std::error::Error>> {
    let full = cluster_name(name);

    if cluster_exists(&full) {
        eprintln!("  cluster {full} already exists, skipping");
        return Ok(());
    }

    eprintln!("  creating cluster {full}...");
    run_cmd("kind", &["create", "cluster", "--name", &full])?;

    if def.role == ClusterRole::Provider && !def.models.is_empty() {
        deploy_inference_sim(&full, name, def)?;
    }

    Ok(())
}

/// Delete a kind cluster.
///
/// Idempotent: if the cluster does not exist, this is a no-op.
///
/// # Errors
///
/// Returns an error if the `kind` command fails.
pub(crate) fn delete_cluster(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let full = cluster_name(name);

    if !cluster_exists(&full) {
        eprintln!("  cluster {full} does not exist, skipping");
        return Ok(());
    }

    eprintln!("  deleting cluster {full}...");
    run_cmd("kind", &["delete", "cluster", "--name", &full])?;
    Ok(())
}

/// Check whether a kind cluster is running.
pub(crate) fn is_cluster_running(name: &str) -> bool {
    cluster_exists(&cluster_name(name))
}

// ---------------------------------------------------------------------------
// Kubernetes manifests
// ---------------------------------------------------------------------------

/// Generate the Deployment YAML for llm-d-inference-sim.
pub(crate) fn deployment_yaml(site_name: &str, def: &ClusterDef) -> String {
    let model_args = format_model_args(&def.models);
    format!(
        "apiVersion: apps/v1\n\
         kind: Deployment\n\
         metadata:\n\
         \x20 name: inference-sim\n\
         \x20 namespace: {NAMESPACE}\n\
         \x20 labels:\n\
         \x20   app: inference-sim\n\
         \x20   grid-site: {site_name}\n\
         spec:\n\
         \x20 replicas: 1\n\
         \x20 selector:\n\
         \x20   matchLabels:\n\
         \x20     app: inference-sim\n\
         \x20 template:\n\
         \x20   metadata:\n\
         \x20     labels:\n\
         \x20       app: inference-sim\n\
         \x20       grid-site: {site_name}\n\
         \x20   spec:\n\
         \x20     containers:\n\
         \x20       - name: inference-sim\n\
         \x20         image: {INFERENCE_SIM_IMAGE}\n\
         \x20         ports:\n\
         \x20           - containerPort: 8000\n\
         \x20         args:\n\
         {model_args}\n"
    )
}

/// Format model arguments as YAML list items.
fn format_model_args(models: &[String]) -> String {
    models
        .iter()
        .map(|m| format!("            - \"--model\"\n            - \"{m}\""))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Generate the Service YAML for llm-d-inference-sim.
pub(crate) fn service_yaml(site_name: &str) -> String {
    format!(
        "apiVersion: v1\n\
         kind: Service\n\
         metadata:\n\
         \x20 name: inference-sim\n\
         \x20 namespace: {NAMESPACE}\n\
         \x20 labels:\n\
         \x20   grid-site: {site_name}\n\
         spec:\n\
         \x20 selector:\n\
         \x20   app: inference-sim\n\
         \x20 ports:\n\
         \x20   - port: 8000\n\
         \x20     targetPort: 8000\n\
         \x20     protocol: TCP\n"
    )
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Build the full kind cluster name from a site name.
fn cluster_name(name: &str) -> String {
    format!("{CLUSTER_PREFIX}{name}")
}

/// Check whether a kind cluster exists by name.
fn cluster_exists(full_name: &str) -> bool {
    Command::new("kind")
        .args(["get", "clusters"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .is_some_and(|out| out.lines().any(|l| l.trim() == full_name))
}

/// Deploy llm-d-inference-sim to the cluster.
fn deploy_inference_sim(full_name: &str, site_name: &str, def: &ClusterDef) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  deploying inference-sim to {full_name}...");
    let ctx = format!("kind-{full_name}");
    apply_manifest(&ctx, &deployment_yaml(site_name, def))?;
    apply_manifest(&ctx, &service_yaml(site_name))?;
    Ok(())
}

/// Apply a YAML manifest via kubectl.
fn apply_manifest(context: &str, yaml: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut child = Command::new("kubectl")
        .args(["--context", context, "apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .spawn()?;

    if let Some(stdin) = child.stdin.as_mut() {
        std::io::Write::write_all(stdin, yaml.as_bytes())?;
    }

    let status = child.wait()?;
    if !status.success() {
        return Err(format!("kubectl apply failed: {status}").into());
    }
    Ok(())
}

/// Run a command and check for success.
fn run_cmd(cmd: &str, args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new(cmd).args(args).status()?;
    if !status.success() {
        return Err(format!("{cmd} failed: {status}").into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::config::ClusterRole;

    #[test]
    fn cluster_name_has_prefix() {
        assert_eq!(cluster_name("cluster-a"), "grid-cluster-a", "should add grid- prefix");
    }

    #[test]
    fn deployment_yaml_contains_models() {
        let def = ClusterDef {
            models: vec!["granite-3.3-8b".to_owned(), "mistral-7b".to_owned()],
            role: ClusterRole::Provider,
        };
        let yaml = deployment_yaml("cluster-a", &def);
        assert!(yaml.contains("granite-3.3-8b"), "should contain granite model");
        assert!(yaml.contains("mistral-7b"), "should contain mistral model");
        assert!(yaml.contains("inference-sim"), "should have deployment name");
        assert!(yaml.contains("grid-site: cluster-a"), "should have site label");
    }

    #[test]
    fn deployment_yaml_empty_models() {
        let def = ClusterDef {
            models: vec![],
            role: ClusterRole::Consumer,
        };
        let yaml = deployment_yaml("cluster-c", &def);
        assert!(yaml.contains("inference-sim"), "should still have deployment name");
        assert!(!yaml.contains("--model"), "should have no model args");
    }

    #[test]
    fn service_yaml_has_site_label() {
        let yaml = service_yaml("cluster-b");
        assert!(yaml.contains("grid-site: cluster-b"), "should have site label");
        assert!(yaml.contains("port: 8000"), "should expose port 8000");
    }
}
