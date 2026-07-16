//! Kind cluster lifecycle management.

use std::process::Command;

use crate::env::{
    config::{ClusterDef, ClusterRole, ProviderBackend},
    kubectl,
};

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

/// Kubernetes Deployment and Service name for the standalone API-provider mock.
///
/// Deployed in the consumer cluster as the target for `api_provider` overlay
/// candidates.  Distinct from [`MOCK_OPENAI_SVC`] (which runs in provider
/// clusters behind the provider gateway mTLS chain) so that the two paths are
/// clearly separate in the consumer Praxis config.
pub(crate) const MOCK_API_SVC: &str = "mock-api-provider";

/// Port used by the mock API-provider HTTP server.
pub(crate) const MOCK_API_PORT: u16 = 8080;

/// Kubernetes Deployment and Service name for the cloud-managed mock.
///
/// Represents an OpenAI-compatible cloud-managed API endpoint in the
/// full-grid routing validation.  Deployed in the consumer cluster so the
/// consumer Praxis gateway can reach it over plain HTTP.  Distinct from
/// [`MOCK_API_SVC`] so the two roles are clearly separate in the consumer
/// Praxis config.
pub(crate) const MOCK_CLOUD_SVC: &str = "mock-cloud-provider";

/// Port used by the mock cloud-provider HTTP server.
pub(crate) const MOCK_CLOUD_PORT: u16 = 8080;

/// Kubernetes namespace for provider backend deployments.
const NAMESPACE: &str = "default";

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

/// Read the raw YAML content of a `ConfigMap` from a cluster.
///
/// Returns the full YAML output of `kubectl get configmap -o yaml`.
///
/// # Errors
///
/// Returns an error if `kubectl` fails or the `ConfigMap` does not exist.
pub(crate) fn kubectl_get_configmap(
    context: &str,
    namespace: &str,
    name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            namespace,
            "get",
            "configmap",
            name,
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl get configmap {name} in {context} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

/// Get the internal IP of the first Kind cluster node.
///
/// Queries `kubectl get nodes` in the given context and returns the
/// `InternalIP` address.  Used to construct `NodePort` endpoint URLs for
/// consumer routing.
///
/// # Errors
///
/// Returns an error if the kubectl command fails or the IP is blank.
pub(crate) fn kind_node_ip(context: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            "nodes",
            "-o",
            "jsonpath={.items[0].status.addresses[?(@.type==\"InternalIP\")].address}",
        ])
        .output()?;
    let ip = String::from_utf8(output.stdout)?.trim().to_owned();
    if ip.is_empty() {
        return Err(format!("could not get node IP for context {context}").into());
    }
    Ok(ip)
}

/// Get the `NodePort` for a named service in `namespace`.
///
/// Returns `None` if the service has no `NodePort` or if the kubectl call
/// fails.
pub(crate) fn service_node_port(context: &str, service: &str, namespace: &str) -> Option<u16> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "get",
            "service",
            service,
            "-n",
            namespace,
            "-o",
            "jsonpath={.spec.ports[0].nodePort}",
        ])
        .output()
        .ok()?;
    let port_str = String::from_utf8(output.stdout).ok()?;
    port_str.trim().parse().ok().filter(|&p: &u16| p > 0)
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

/// Deploy the standalone mock API-provider into a cluster and wait for readiness.
///
/// The mock API-provider represents an OpenAI-compatible external API endpoint
/// in the API fallback validation.  It runs in the **consumer** cluster so that
/// the consumer Praxis gateway can reach it directly over plain HTTP via
/// in-cluster DNS (`mock-api-provider.default.svc:8080`), without mTLS.
///
/// The same `grid-mock-providers` binary is used as for the provider-side mock,
/// but it is deployed independently under a different name so the two roles are
/// clearly distinct in the Praxis consumer config.
pub(crate) fn deploy_mock_api_provider(context: &str, cluster_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  loading {MOCK_PROVIDER_IMAGE} into {cluster_name}...");
    run_cmd(
        "kind",
        &["load", "docker-image", MOCK_PROVIDER_IMAGE, "--name", cluster_name],
    )
    .map_err(|e| {
        format!(
            "{e}\n\
             hint: build the image first with:\n\
             \x20 docker build -t {MOCK_PROVIDER_IMAGE} -f mock-providers/Containerfile ."
        )
    })?;
    eprintln!("  deploying {MOCK_API_SVC} to {cluster_name}...");
    kubectl::apply_manifest(context, &mock_api_provider_deployment_yaml())?;
    kubectl::apply_manifest(context, &mock_api_provider_service_yaml())?;
    kubectl::wait_for_rollout(context, MOCK_API_SVC, cluster_name)?;
    Ok(())
}

/// Delete the mock API-provider Deployment and Service if they exist.
///
/// Best-effort: errors are ignored (both `--ignore-not-found` and command
/// failures) because cleanup is non-critical.
pub(crate) fn delete_mock_api_provider(context: &str) {
    let _d = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            NAMESPACE,
            "delete",
            "deployment",
            MOCK_API_SVC,
            "--ignore-not-found",
        ])
        .status();
    let _s = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            NAMESPACE,
            "delete",
            "service",
            MOCK_API_SVC,
            "--ignore-not-found",
        ])
        .status();
}

/// Generate the Deployment YAML for the standalone mock API-provider.
#[expect(clippy::too_many_lines, reason = "readable multiline Kubernetes Deployment YAML")]
pub(crate) fn mock_api_provider_deployment_yaml() -> String {
    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: {MOCK_API_SVC}
  namespace: {NAMESPACE}
  labels:
    app: {MOCK_API_SVC}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {MOCK_API_SVC}
  template:
    metadata:
      labels:
        app: {MOCK_API_SVC}
    spec:
      containers:
        - name: mock-api
          image: {MOCK_PROVIDER_IMAGE}
          imagePullPolicy: Never
          args:
            - "--provider"
            - "openai"
            - "--port"
            - "{MOCK_API_PORT}"
          ports:
            - containerPort: {MOCK_API_PORT}
"#
    )
}

/// Generate the Service YAML for the standalone mock API-provider.
pub(crate) fn mock_api_provider_service_yaml() -> String {
    format!(
        "apiVersion: v1
kind: Service
metadata:
  name: {MOCK_API_SVC}
  namespace: {NAMESPACE}
spec:
  selector:
    app: {MOCK_API_SVC}
  ports:
    - port: {MOCK_API_PORT}
      targetPort: {MOCK_API_PORT}
"
    )
}

/// Deploy the cloud-managed mock provider into a cluster and wait for readiness.
///
/// Represents a `cloud_managed` API endpoint in the full-grid routing validation.
/// Deployed in the consumer cluster so the consumer Praxis gateway can reach it
/// over plain HTTP via in-cluster DNS (`mock-cloud-provider.default.svc:8080`).
///
/// This mock validates the Grid `backendKind = "cloud_managed"` overlay,
/// scoring, and routing path. It intentionally does not exercise real cloud
/// provider auth or protocols such as AWS `SigV4`, Google `OAuth2`, or Azure AAD.
pub(crate) fn deploy_mock_cloud_provider(context: &str, cluster_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  loading {MOCK_PROVIDER_IMAGE} into {cluster_name}...");
    run_cmd(
        "kind",
        &["load", "docker-image", MOCK_PROVIDER_IMAGE, "--name", cluster_name],
    )
    .map_err(|e| {
        format!(
            "{e}\n\
             hint: build the image first with:\n\
             \x20 docker build -t {MOCK_PROVIDER_IMAGE} -f mock-providers/Containerfile ."
        )
    })?;
    eprintln!("  deploying {MOCK_CLOUD_SVC} to {cluster_name}...");
    kubectl::apply_manifest(context, &mock_cloud_provider_deployment_yaml())?;
    kubectl::apply_manifest(context, &mock_cloud_provider_service_yaml())?;
    kubectl::wait_for_rollout(context, MOCK_CLOUD_SVC, cluster_name)?;
    Ok(())
}

/// Delete the cloud-managed mock provider Deployment and Service if they exist.
///
/// Best-effort: errors are ignored.
pub(crate) fn delete_mock_cloud_provider(context: &str) {
    let _d = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            NAMESPACE,
            "delete",
            "deployment",
            MOCK_CLOUD_SVC,
            "--ignore-not-found",
        ])
        .status();
    let _s = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            NAMESPACE,
            "delete",
            "service",
            MOCK_CLOUD_SVC,
            "--ignore-not-found",
        ])
        .status();
}

/// Generate the Deployment YAML for the cloud-managed mock provider.
#[expect(clippy::too_many_lines, reason = "readable multiline Kubernetes Deployment YAML")]
pub(crate) fn mock_cloud_provider_deployment_yaml() -> String {
    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: {MOCK_CLOUD_SVC}
  namespace: {NAMESPACE}
  labels:
    app: {MOCK_CLOUD_SVC}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {MOCK_CLOUD_SVC}
  template:
    metadata:
      labels:
        app: {MOCK_CLOUD_SVC}
    spec:
      containers:
        - name: mock-cloud
          image: {MOCK_PROVIDER_IMAGE}
          imagePullPolicy: Never
          args:
            - "--provider"
            - "openai"
            - "--port"
            - "{MOCK_CLOUD_PORT}"
          ports:
            - containerPort: {MOCK_CLOUD_PORT}
"#
    )
}

/// Generate the Service YAML for the cloud-managed mock provider.
pub(crate) fn mock_cloud_provider_service_yaml() -> String {
    format!(
        "apiVersion: v1
kind: Service
metadata:
  name: {MOCK_CLOUD_SVC}
  namespace: {NAMESPACE}
spec:
  selector:
    app: {MOCK_CLOUD_SVC}
  ports:
    - port: {MOCK_CLOUD_PORT}
      targetPort: {MOCK_CLOUD_PORT}
"
    )
}

/// Deploy a single `grid-mock-providers` (openai backend) instance and wait
/// for readiness.
///
/// Loads [`MOCK_PROVIDER_IMAGE`] into the kind cluster before deploying because
/// the Deployment uses `imagePullPolicy: Never`.  The image must already be
/// present in the local Docker daemon (built via
/// `docker build -t grid-mock-providers:latest -f mock-providers/Containerfile .`).
///
/// A single Deployment and Service handles all models for this cluster.
/// The mock EPP routes all model requests to this one service.
fn deploy_mock_openai(full_name: &str, site_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Load the image into the cluster before deploying; imagePullPolicy: Never
    // requires the image to be present in the kind node before the Pod starts.
    eprintln!("  loading {MOCK_PROVIDER_IMAGE} into {full_name}...");
    run_cmd(
        "kind",
        &["load", "docker-image", MOCK_PROVIDER_IMAGE, "--name", full_name],
    )
    .map_err(|e| {
        format!(
            "{e}\n\
             hint: build the image first with:\n\
             \x20 docker build -t {MOCK_PROVIDER_IMAGE} -f mock-providers/Containerfile ."
        )
    })?;

    let ctx = format!("kind-{full_name}");
    eprintln!("  deploying {MOCK_OPENAI_SVC} to {full_name}...");
    kubectl::apply_manifest(&ctx, &mock_openai_deployment_yaml(site_name))?;
    kubectl::apply_manifest(&ctx, &mock_openai_service_yaml(site_name))?;
    kubectl::wait_for_rollout(&ctx, MOCK_OPENAI_SVC, full_name)?;
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
        kubectl::apply_manifest(&ctx, &model_deployment_yaml(site_name, model))?;
        kubectl::apply_manifest(&ctx, &model_service_yaml(site_name, model))?;
        kubectl::wait_for_rollout(&ctx, &deploy, full_name)?;
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
