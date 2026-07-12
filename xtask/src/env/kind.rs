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

/// Timeout for deployment rollout readiness in seconds.
const ROLLOUT_TIMEOUT_SECS: u32 = 120;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create a kind cluster and deploy llm-d-inference-sim if the
/// cluster role is [`ClusterRole::Provider`].
///
/// Idempotent: if the cluster already exists, creation is skipped
/// but provider deployments are still reconciled.
///
/// # Errors
///
/// Returns an error if `kind` or `kubectl` commands fail.
pub(crate) fn create_cluster(name: &str, def: &ClusterDef) -> Result<(), Box<dyn std::error::Error>> {
    let full = cluster_name(name);

    if cluster_exists(&full) {
        eprintln!("  cluster {full} already exists, skipping create");
    } else {
        eprintln!("  creating cluster {full}...");
        run_cmd("kind", &["create", "cluster", "--name", &full])?;
    }

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

/// Check whether an inference-sim deployment has available replicas.
///
/// The deployment name is `inference-sim-{model}` where the model
/// name has dots replaced with dashes for Kubernetes compatibility.
pub(crate) fn is_model_deployment_ready(name: &str, model: &str) -> bool {
    let ctx = kubectl_context(name);
    let deploy = deployment_name(model);
    Command::new("kubectl")
        .args([
            "--context",
            &ctx,
            "-n",
            NAMESPACE,
            "get",
            "deployment",
            &deploy,
            "-o",
            "jsonpath={.status.availableReplicas}",
        ])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .is_some_and(|s| {
            let trimmed = s.trim();
            !trimmed.is_empty() && trimmed != "0"
        })
}

/// Build the Kubernetes-safe deployment/service name for a model.
///
/// Replaces dots with dashes: `granite-3.3-8b` becomes
/// `inference-sim-granite-3-3-8b`.
pub(crate) fn deployment_name(model: &str) -> String {
    format!("inference-sim-{}", model.replace('.', "-"))
}

/// Build the service name for a model (same as deployment name).
pub(crate) fn service_name(model: &str) -> String {
    deployment_name(model)
}

/// Build the kubectl context name for a cluster.
pub(crate) fn kubectl_context(name: &str) -> String {
    format!("kind-{}", cluster_name(name))
}

// ---------------------------------------------------------------------------
// Kubernetes manifests
// ---------------------------------------------------------------------------

/// Generate a Deployment YAML for one model's inference-sim.
///
/// `llm-d-inference-sim` supports one `--model` per process, so
/// each configured model gets its own Deployment and Service.
pub(crate) fn model_deployment_yaml(site_name: &str, model: &str) -> String {
    let name = deployment_name(model);
    format!(
        "apiVersion: apps/v1\n\
         kind: Deployment\n\
         metadata:\n\
         \x20 name: {name}\n\
         \x20 namespace: {NAMESPACE}\n\
         \x20 labels:\n\
         \x20   app: {name}\n\
         \x20   grid-site: {site_name}\n\
         spec:\n\
         \x20 replicas: 1\n\
         \x20 selector:\n\
         \x20   matchLabels:\n\
         \x20     app: {name}\n\
         \x20 template:\n\
         \x20   metadata:\n\
         \x20     labels:\n\
         \x20       app: {name}\n\
         \x20       grid-site: {site_name}\n\
         \x20   spec:\n\
         \x20     containers:\n\
         \x20       - name: inference-sim\n\
         \x20         image: {INFERENCE_SIM_IMAGE}\n\
         \x20         ports:\n\
         \x20           - containerPort: 8000\n\
         \x20         args:\n\
         \x20           - \"--model\"\n\
         \x20           - \"{model}\"\n"
    )
}

/// Generate a Service YAML for one model's inference-sim.
pub(crate) fn model_service_yaml(site_name: &str, model: &str) -> String {
    let name = service_name(model);
    format!(
        "apiVersion: v1\n\
         kind: Service\n\
         metadata:\n\
         \x20 name: {name}\n\
         \x20 namespace: {NAMESPACE}\n\
         \x20 labels:\n\
         \x20   grid-site: {site_name}\n\
         spec:\n\
         \x20 selector:\n\
         \x20   app: {name}\n\
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

/// Deploy one inference-sim per model and wait for readiness.
fn deploy_inference_sim(full_name: &str, site_name: &str, def: &ClusterDef) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = format!("kind-{full_name}");
    for model in &def.models {
        let deploy = deployment_name(model);
        eprintln!("  deploying {deploy} to {full_name}...");
        apply_manifest(&ctx, &model_deployment_yaml(site_name, model))?;
        apply_manifest(&ctx, &model_service_yaml(site_name, model))?;
        wait_for_rollout(&ctx, full_name, &deploy)?;
    }
    Ok(())
}

/// Wait for a deployment to become available.
fn wait_for_rollout(context: &str, full_name: &str, deploy: &str) -> Result<(), Box<dyn std::error::Error>> {
    let timeout = format!("{ROLLOUT_TIMEOUT_SECS}s");
    let resource = format!("deployment/{deploy}");
    eprintln!("  waiting for {deploy} rollout in {full_name} (timeout {timeout})...");
    let status = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            NAMESPACE,
            "rollout",
            "status",
            &resource,
            "--timeout",
            &timeout,
        ])
        .status()?;
    if !status.success() {
        return Err(format!("{deploy} rollout timed out in {full_name}").into());
    }
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

    #[test]
    fn cluster_name_has_prefix() {
        assert_eq!(cluster_name("cluster-a"), "grid-cluster-a", "should add grid- prefix");
    }

    #[test]
    fn model_deployment_yaml_single_model() {
        let yaml = model_deployment_yaml("cluster-a", "granite-3.3-8b");
        assert!(
            yaml.contains("name: inference-sim-granite-3-3-8b"),
            "deployment name should include sanitized model"
        );
        assert!(yaml.contains("\"--model\""), "should have --model arg");
        assert!(yaml.contains("\"granite-3.3-8b\""), "should have model name arg");
        assert!(yaml.contains("grid-site: cluster-a"), "should have site label");
    }

    #[test]
    fn model_service_yaml_has_site_label() {
        let yaml = model_service_yaml("cluster-b", "llama-3.2-8b");
        assert!(
            yaml.contains("name: inference-sim-llama-3-2-8b"),
            "service name should include sanitized model"
        );
        assert!(yaml.contains("grid-site: cluster-b"), "should have site label");
        assert!(yaml.contains("port: 8000"), "should expose port 8000");
    }

    #[test]
    fn deployment_name_sanitizes_dots() {
        assert_eq!(
            deployment_name("granite-3.3-8b"),
            "inference-sim-granite-3-3-8b",
            "dots should be replaced with dashes"
        );
        assert_eq!(
            deployment_name("llama-3.2-8b"),
            "inference-sim-llama-3-2-8b",
            "dots should be replaced with dashes"
        );
    }
}
