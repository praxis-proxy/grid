//! Pure `kubectl` command wrappers shared across [`crate::env`] submodules.
//!
//! These helpers are intentionally minimal: they wrap single `kubectl`
//! invocations with no cluster-state knowledge and no orchestration logic.
//! Call sites remain responsible for error context and sequencing.

use std::{io::Write as _, process::Command};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

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
    let resource = format!("deployment/{deployment}");
    eprintln!("  waiting for {deployment} in {cluster}...");
    let status = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            ROLLOUT_NAMESPACE,
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
