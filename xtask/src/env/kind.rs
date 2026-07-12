//! Kind cluster lifecycle management.

use std::process::Command;

use crate::env::config::{ClusterDef, ClusterRole, ProviderBackend};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Prefix for kind cluster names.
const CLUSTER_PREFIX: &str = "grid-";

/// Container image for llm-d inference simulator.
const INFERENCE_SIM_IMAGE: &str = "ghcr.io/llm-d/llm-d-inference-sim:latest";

/// Container image for the `openai` mock-providers backend.
///
/// Loaded into kind clusters via `kind load docker-image` for the
/// [`ProviderBackend::MockOpenai`] backend.
pub(crate) const MOCK_PROVIDER_IMAGE: &str = "grid-mock-providers:latest";

/// Kubernetes Deployment and Service name for the `openai` provider backend.
pub(crate) const MOCK_OPENAI_SVC: &str = "mock-openai-provider";

/// Port used by the `openai` mock-providers HTTP server.
pub(crate) const MOCK_OPENAI_PORT: u16 = 8080;

/// Kubernetes namespace for inference sim deployments.
const NAMESPACE: &str = "default";

/// Timeout for deployment rollout readiness in seconds.
const ROLLOUT_TIMEOUT_SECS: u32 = 120;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create a kind cluster and deploy the configured provider backend.
///
/// For provider clusters, the backend is selected by [`ClusterDef::backend`]:
/// - [`ProviderBackend::InferenceSim`]: deploys one `llm-d-inference-sim` Deployment and Service per model.
/// - [`ProviderBackend::MockOpenai`]: deploys a single `grid-mock-providers` (openai provider) Deployment and Service
///   that handles all models.
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
        match def.backend {
            ProviderBackend::InferenceSim => deploy_inference_sim(&full, name, def)?,
            ProviderBackend::MockOpenai => deploy_mock_openai(&full, name)?,
        }
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

/// Check whether a provider backend deployment has available replicas.
pub(crate) fn is_provider_backend_ready(name: &str, def: &ClusterDef) -> bool {
    let deploy = provider_backend_deployment_name(def);
    is_deployment_ready(name, &deploy)
}

/// Check whether a per-model inference-sim deployment has available replicas.
pub(crate) fn is_model_deployment_ready(name: &str, model: &str) -> bool {
    let deploy = deployment_name(model);
    is_deployment_ready(name, &deploy)
}

/// Check whether a named deployment has available replicas.
fn is_deployment_ready(name: &str, deploy: &str) -> bool {
    let ctx = kubectl_context(name);
    Command::new("kubectl")
        .args([
            "--context",
            &ctx,
            "-n",
            NAMESPACE,
            "get",
            "deployment",
            deploy,
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

/// Build the deployment name used to report provider backend readiness.
///
/// `InferenceSim` clusters use the first configured model's deployment name;
/// `MockOpenai` clusters use the single shared backend deployment.
pub(crate) fn provider_backend_deployment_name(def: &ClusterDef) -> String {
    match def.backend {
        ProviderBackend::InferenceSim => def
            .models
            .first()
            .map_or_else(|| deployment_name("no-model"), |model| deployment_name(model)),
        ProviderBackend::MockOpenai => MOCK_OPENAI_SVC.to_owned(),
    }
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

/// Build the full kind cluster name from a config site name.
pub(crate) fn cluster_name_from_config(name: &str) -> String {
    cluster_name(name)
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

/// Deploy a single `grid-mock-providers` (openai backend) instance and wait
/// for readiness.
///
/// A single Deployment and Service handles all models for this cluster.
/// The mock EPP routes all model requests to this one service.
fn deploy_mock_openai(full_name: &str, site_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = format!("kind-{full_name}");
    eprintln!("  deploying {MOCK_OPENAI_SVC} to {full_name}...");
    apply_manifest(&ctx, &mock_openai_deployment_yaml(site_name))?;
    apply_manifest(&ctx, &mock_openai_service_yaml(site_name))?;
    wait_for_rollout(&ctx, full_name, MOCK_OPENAI_SVC)?;
    Ok(())
}

/// Generate a Deployment YAML for the `mock-openai-provider` backend.
///
/// Deploys a single `grid-mock-providers` instance configured for the
/// `openai` provider, serving all models in the cluster.
#[expect(clippy::too_many_lines, reason = "Kubernetes manifest generation")]
pub(crate) fn mock_openai_deployment_yaml(site_name: &str) -> String {
    format!(
        "apiVersion: apps/v1\n\
         kind: Deployment\n\
         metadata:\n\
         \x20 name: {MOCK_OPENAI_SVC}\n\
         \x20 namespace: {NAMESPACE}\n\
         \x20 labels:\n\
         \x20   app: {MOCK_OPENAI_SVC}\n\
         \x20   grid-site: {site_name}\n\
         spec:\n\
         \x20 replicas: 1\n\
         \x20 selector:\n\
         \x20   matchLabels:\n\
         \x20     app: {MOCK_OPENAI_SVC}\n\
         \x20 template:\n\
         \x20   metadata:\n\
         \x20     labels:\n\
         \x20       app: {MOCK_OPENAI_SVC}\n\
         \x20       grid-site: {site_name}\n\
         \x20   spec:\n\
         \x20     containers:\n\
         \x20       - name: mock-openai\n\
         \x20         image: {MOCK_PROVIDER_IMAGE}\n\
         \x20         imagePullPolicy: Never\n\
         \x20         args:\n\
         \x20           - \"--provider\"\n\
         \x20           - \"openai\"\n\
         \x20           - \"--port\"\n\
         \x20           - \"{MOCK_OPENAI_PORT}\"\n\
         \x20         ports:\n\
         \x20           - containerPort: {MOCK_OPENAI_PORT}\n"
    )
}

/// Generate a Service YAML for the `mock-openai-provider` backend.
pub(crate) fn mock_openai_service_yaml(site_name: &str) -> String {
    format!(
        "apiVersion: v1\n\
         kind: Service\n\
         metadata:\n\
         \x20 name: {MOCK_OPENAI_SVC}\n\
         \x20 namespace: {NAMESPACE}\n\
         \x20 labels:\n\
         \x20   grid-site: {site_name}\n\
         spec:\n\
         \x20 selector:\n\
         \x20   app: {MOCK_OPENAI_SVC}\n\
         \x20 ports:\n\
         \x20   - port: {MOCK_OPENAI_PORT}\n\
         \x20     targetPort: {MOCK_OPENAI_PORT}\n"
    )
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

    #[test]
    fn mock_openai_deployment_yaml_has_correct_name() {
        let yaml = mock_openai_deployment_yaml("site-a");
        assert!(
            yaml.contains(&format!("name: {MOCK_OPENAI_SVC}")),
            "deployment must use MOCK_OPENAI_SVC name"
        );
        assert!(yaml.contains("grid-site: site-a"), "must include site label");
        assert!(yaml.contains(MOCK_PROVIDER_IMAGE), "must reference MOCK_PROVIDER_IMAGE");
        assert!(yaml.contains("imagePullPolicy: Never"), "must use Never pull policy");
        assert!(yaml.contains("--provider"), "must pass --provider arg");
        assert!(yaml.contains("openai"), "must specify openai provider");
    }

    #[test]
    fn mock_openai_service_yaml_has_correct_port() {
        let yaml = mock_openai_service_yaml("site-b");
        assert!(
            yaml.contains(&format!("name: {MOCK_OPENAI_SVC}")),
            "service must use MOCK_OPENAI_SVC name"
        );
        assert!(yaml.contains("grid-site: site-b"), "must include site label");
        assert!(
            yaml.contains(&format!("port: {MOCK_OPENAI_PORT}")),
            "must expose MOCK_OPENAI_PORT"
        );
    }

    #[test]
    fn mock_openai_svc_and_inference_sim_names_differ() {
        assert_ne!(
            MOCK_OPENAI_SVC,
            deployment_name("any-model"),
            "mock-openai service must not collide with inference-sim name pattern"
        );
    }

    #[test]
    fn provider_backend_deployment_name_uses_inference_sim_first_model() {
        let def = ClusterDef {
            models: vec!["model-a".to_owned(), "model-b".to_owned()],
            role: ClusterRole::Provider,
            backend: ProviderBackend::InferenceSim,
        };
        assert_eq!(
            provider_backend_deployment_name(&def),
            deployment_name("model-a"),
            "inference-sim readiness should use the first model deployment"
        );
    }

    #[test]
    fn provider_backend_deployment_name_uses_mock_openai_service() {
        let def = ClusterDef {
            models: vec!["model-a".to_owned()],
            role: ClusterRole::Provider,
            backend: ProviderBackend::MockOpenai,
        };
        assert_eq!(
            provider_backend_deployment_name(&def),
            MOCK_OPENAI_SVC,
            "mock-openai readiness should use the shared backend deployment"
        );
    }
}
