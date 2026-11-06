//! Pure `kubectl` command wrappers shared across [`crate::env`] submodules.
//!
//! These helpers are intentionally minimal: they wrap single `kubectl`
//! invocations with no cluster-state knowledge and no orchestration logic.
//! Call sites remain responsible for error context and sequencing.

use std::{io::Write as _, process::Command};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// `--as` identity used to impersonate the operator `ServiceAccount` in RBAC and VAP checks.
const OPERATOR_SA_IMPERSONATION: &str = "system:serviceaccount:grid-system:grid-operator";

/// Kubernetes namespace targeted by all env subcommand rollout checks.
///
/// Provider, consumer, and mock-backend deployments all target the
/// `default` namespace in the kind test environment.
const ROLLOUT_NAMESPACE: &str = "default";

/// Timeout string passed to `kubectl rollout status --timeout`.
///
/// 120 seconds matches the three separate `ROLLOUT_TIMEOUT_SECS = 120`
/// constants previously defined inline in `consumer`, `gateway`, and `kind`.
const ROLLOUT_TIMEOUT: &str = "120s";

// ---------------------------------------------------------------------------
// Manifest application
// ---------------------------------------------------------------------------

/// Apply a Kubernetes manifest via `kubectl apply -f -`.
///
/// Streams `manifest` to `kubectl`'s standard input so manifests of any
/// size can be applied without writing a temporary file to disk.
///
/// # Errors
///
/// Returns an error if the `kubectl` process cannot be spawned, if
/// writing to stdin fails, or if the command exits with a non-zero status.
pub(crate) fn apply_manifest(context: &str, manifest: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut child = Command::new("kubectl")
        .args(["--context", context, "apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(manifest.as_bytes())?;
    }
    let status = child.wait()?;
    if !status.success() {
        return Err(format!("kubectl apply failed: {status}").into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Rollout status
// ---------------------------------------------------------------------------

/// Wait for a Kubernetes `Deployment` rollout to complete.
///
/// Runs `kubectl rollout status deployment/{deployment} -n default
/// --timeout 120s --context {context}`.  Namespace and timeout are shared
/// constants so every env subcommand applies the same window.
///
/// # Errors
///
/// Returns an error if the `kubectl` process cannot be spawned or if the
/// rollout does not complete within the timeout window.
pub(crate) fn wait_for_rollout(
    context: &str,
    deployment: &str,
    cluster: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_rollout_ns(context, deployment, ROLLOUT_NAMESPACE, cluster)
}

/// Restart a `Deployment` rollout so pods pick up updated volume mounts.
///
/// Runs `kubectl rollout restart deployment/{deployment}` in the default
/// namespace.  Pingora/rustls gateways do not hot-reload TLS material, so
/// a restart is required after updating a mounted Secret.
///
/// # Errors
///
/// Returns an error if the `kubectl` process cannot be spawned or exits
/// with a non-zero status.
pub(crate) fn rollout_restart(context: &str, deployment: &str) -> Result<(), Box<dyn std::error::Error>> {
    rollout_restart_ns(context, deployment, ROLLOUT_NAMESPACE)
}

/// Restart a `Deployment` rollout in an explicit namespace.
pub(crate) fn rollout_restart_ns(
    context: &str,
    deployment: &str,
    namespace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let resource = format!("deployment/{deployment}");
    let status = Command::new("kubectl")
        .args(["--context", context, "-n", namespace, "rollout", "restart", &resource])
        .status()?;
    if !status.success() {
        return Err(format!("kubectl rollout restart {deployment} failed").into());
    }
    Ok(())
}

/// Wait for a `Deployment` rollout in a specific namespace.
///
/// # Errors
///
/// Returns an error if the `kubectl` process cannot be spawned or if the
/// rollout does not complete within the timeout window.
pub(crate) fn wait_for_rollout_ns(
    context: &str,
    deployment: &str,
    namespace: &str,
    cluster: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let resource = format!("deployment/{deployment}");
    eprintln!("  waiting for {deployment} in {cluster} (ns={namespace})...");
    let status = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            namespace,
            "rollout",
            "status",
            &resource,
            "--timeout",
            ROLLOUT_TIMEOUT,
        ])
        .status()?;
    if !status.success() {
        return Err(format!("{deployment} rollout timed out in {cluster}").into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Resource queries
// ---------------------------------------------------------------------------

/// Get a Kubernetes `ConfigMap` as YAML.
///
/// Returns the full YAML output of `kubectl get configmap -o yaml`.
///
/// # Errors
///
/// Returns an error if the `kubectl` process cannot be spawned, if the
/// `ConfigMap` does not exist, or if the command exits with a non-zero status.
pub(crate) fn get_configmap_yaml(
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
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Return `true` if the named namespaced resource exists, `false` if it is absent.
///
/// Uses `--ignore-not-found` so kubectl exits zero in both cases; non-empty output means found.
pub(crate) fn resource_exists(
    context: &str,
    namespace: &str,
    resource: &str,
    name: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            namespace,
            "get",
            resource,
            name,
            "--ignore-not-found",
            "-o",
            "name",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl get {resource} {name} in {namespace} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(!output.stdout.trim_ascii().is_empty())
}

/// Delete a namespaced resource, ignoring absence.
///
/// Returns an error only if `kubectl` itself cannot be spawned.
pub(crate) fn delete_ignoring_not_found(
    context: &str,
    namespace: &str,
    resource: &str,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            namespace,
            "delete",
            resource,
            name,
            "--ignore-not-found",
        ])
        .status()?;
    Ok(())
}

/// Outcome of an impersonated admission request, distinguishing policy denials from other failures.
#[derive(Debug)]
pub(crate) enum SaOutcome {
    /// The request was accepted.
    Allowed,
    /// A `ValidatingAdmissionPolicy` denied the request.
    DeniedByPolicy,
    /// kubectl exited non-zero for a reason unrelated to VAP (e.g. RBAC Forbidden, `NotFound`,
    /// or a connection error). The captured stderr is included for diagnosis.
    OtherFailure(String),
}

impl SaOutcome {
    /// Map a raw kubectl output to the appropriate outcome variant.
    fn classify(output: &std::process::Output) -> Self {
        if output.status.success() {
            return Self::Allowed;
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("ValidatingAdmissionPolicy") {
            Self::DeniedByPolicy
        } else {
            Self::OtherFailure(stderr.into_owned())
        }
    }
}

/// Try to merge-patch a namespaced resource impersonating the `grid-operator` `ServiceAccount`.
///
/// Sends an empty merge-patch `{}` which is a no-op in content but still goes through admission.
/// Returns [`SaOutcome::DeniedByPolicy`] when kubectl stderr contains `ValidatingAdmissionPolicy`,
/// so callers can distinguish a VAP denial from an RBAC rejection or a connection failure.
///
/// # Errors
///
/// Returns an error only if `kubectl` itself cannot be spawned.
pub(crate) fn patch_as_operator_sa(
    context: &str,
    namespace: &str,
    resource: &str,
    name: &str,
) -> Result<SaOutcome, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            namespace,
            "patch",
            resource,
            name,
            "--type=merge",
            "--patch={}",
            &format!("--as={OPERATOR_SA_IMPERSONATION}"),
        ])
        .output()?;
    Ok(SaOutcome::classify(&output))
}

/// Try to delete a namespaced resource impersonating the `grid-operator` `ServiceAccount`.
///
/// Does **not** pass `--ignore-not-found`. Returns [`SaOutcome::DeniedByPolicy`] when kubectl
/// stderr contains `ValidatingAdmissionPolicy`, so callers can distinguish a VAP denial from an
/// RBAC rejection, a 404, or a connection failure.
///
/// # Errors
///
/// Returns an error only if `kubectl` itself cannot be spawned.
pub(crate) fn delete_as_operator_sa(
    context: &str,
    namespace: &str,
    resource: &str,
    name: &str,
) -> Result<SaOutcome, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            namespace,
            "delete",
            resource,
            name,
            &format!("--as={OPERATOR_SA_IMPERSONATION}"),
        ])
        .output()?;
    Ok(SaOutcome::classify(&output))
}
