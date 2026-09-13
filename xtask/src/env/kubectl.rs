//! Pure `kubectl` command wrappers shared across [`crate::env`] submodules.
//!
//! These helpers are intentionally minimal: they wrap single `kubectl`
//! invocations with no cluster-state knowledge and no orchestration logic.
//! Call sites remain responsible for error context and sequencing.

use std::{
    fs,
    io::Write as _,
    os::unix::process::ExitStatusExt as _,
    path::Path,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

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
    wait_for_rollout_ns_with_evidence(context, deployment, namespace, cluster, None)
}

/// Evidence-aware rollout wait used by qualifications that own an evidence
/// directory.  Diagnostics are deliberately best-effort and cannot replace
/// the original rollout error.
pub(crate) fn wait_for_rollout_ns_with_evidence(
    context: &str,
    deployment: &str,
    namespace: &str,
    cluster: &str,
    evidence_dir: Option<&Path>,
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
        capture_rollout_timeout(context, deployment, namespace, evidence_dir);
        return Err(format!("{deployment} rollout timed out in {cluster}").into());
    }
    Ok(())
}

/// Capture bounded deployment, pod, event, and log state after a rollout timeout.
#[expect(
    clippy::too_many_lines,
    reason = "all rollout timeout state belongs to one failure boundary"
)]
fn capture_rollout_timeout(context: &str, deployment: &str, namespace: &str, evidence_dir: Option<&Path>) {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    let directory = evidence_dir.map(|root| root.join(format!("rollout-timeout-{deployment}-{stamp}")));
    if let Some(path) = &directory {
        drop(fs::create_dir_all(path));
    }

    let deployment_json = diagnostic_command(context, namespace, &["get", "deployment", deployment, "-o", "json"]);
    diagnostic_record(directory.as_deref(), "deployment.json", &deployment_json);

    let selector = serde_json::from_slice::<serde_json::Value>(&deployment_json.stdout)
        .ok()
        .and_then(|value| {
            value
                .get("spec")?
                .get("selector")?
                .get("matchLabels")?
                .as_object()
                .cloned()
        })
        .map(|labels| {
            labels
                .iter()
                .filter_map(|(key, value)| Some(format!("{key}={}", value.as_str()?)))
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|value| !value.is_empty());
    let selector_args = selector.as_deref().map_or_else(
        || vec!["get", "pods", "-o", "wide"],
        |value| vec!["get", "pods", "-l", value, "-o", "wide"],
    );
    let pods_wide = diagnostic_command(context, namespace, &selector_args);
    diagnostic_record(directory.as_deref(), "pods-wide.txt", &pods_wide);
    let replicasets = selector.as_deref().map_or_else(
        || diagnostic_command(context, namespace, &["get", "replicasets", "-o", "wide"]),
        |value| diagnostic_command(context, namespace, &["get", "replicasets", "-l", value, "-o", "wide"]),
    );
    diagnostic_record(directory.as_deref(), "replicasets.txt", &replicasets);
    let events = diagnostic_command(context, namespace, &["get", "events", "--sort-by=.lastTimestamp"]);
    diagnostic_record(directory.as_deref(), "events.txt", &events);
    let describe = diagnostic_command(context, namespace, &["describe", "deployment", deployment]);
    diagnostic_record(directory.as_deref(), "deployment-describe.txt", &describe);

    #[expect(
        clippy::collapsible_if,
        reason = "keep command-output parsing separate from pod iteration"
    )]
    if let Ok(pods) = serde_json::from_slice::<serde_json::Value>(
        &diagnostic_command(
            context,
            namespace,
            &selector.as_deref().map_or_else(
                || vec!["get", "pods", "-o", "json"],
                |value| vec!["get", "pods", "-l", value, "-o", "json"],
            ),
        )
        .stdout,
    ) {
        if let Some(items) = pods.get("items").and_then(serde_json::Value::as_array) {
            for pod in items {
                let Some(name) = pod
                    .get("metadata")
                    .and_then(|value| value.get("name"))
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let pod_file = format!("pod-{name}.json");
                let pod_json = diagnostic_command(context, namespace, &["get", "pod", name, "-o", "json"]);
                diagnostic_record(directory.as_deref(), &pod_file, &pod_json);
                let described = diagnostic_command(context, namespace, &["describe", "pod", name]);
                diagnostic_record(directory.as_deref(), &format!("pod-{name}-describe.txt"), &described);
                if let Some(containers) = pod
                    .get("spec")
                    .and_then(|value| value.get("containers"))
                    .and_then(serde_json::Value::as_array)
                {
                    for container in containers {
                        let Some(container_name) = container.get("name").and_then(serde_json::Value::as_str) else {
                            continue;
                        };
                        let current =
                            diagnostic_command(context, namespace, &["logs", name, "-c", container_name, "--tail=100"]);
                        diagnostic_record(
                            directory.as_deref(),
                            &format!("pod-{name}-{container_name}-current.log"),
                            &current,
                        );
                        let previous = diagnostic_command(
                            context,
                            namespace,
                            &["logs", name, "-c", container_name, "--previous", "--tail=100"],
                        );
                        diagnostic_record(
                            directory.as_deref(),
                            &format!("pod-{name}-{container_name}-previous.log"),
                            &previous,
                        );
                    }
                }
            }
        }
    }
    eprintln!(
        "  rollout diagnostics captured for {deployment}{}",
        directory
            .as_ref()
            .map_or(String::new(), |path| format!(" at {}", path.display()))
    );
}

/// Run one bounded, best-effort diagnostic command.
fn diagnostic_command(context: &str, namespace: &str, args: &[&str]) -> std::process::Output {
    Command::new("timeout")
        .args(["15s", "kubectl", "--context", context, "-n", namespace])
        .args(args)
        .output()
        .unwrap_or_else(|error| std::process::Output {
            status: std::process::ExitStatus::from_raw(1),
            stdout: Vec::new(),
            stderr: error.to_string().into_bytes(),
        })
}

/// Redact and persist one diagnostic command result, also showing a short excerpt.
fn diagnostic_record(directory: Option<&Path>, name: &str, output: &std::process::Output) {
    let stdout = redact_diagnostic(&String::from_utf8_lossy(&output.stdout));
    let stderr = redact_diagnostic(&String::from_utf8_lossy(&output.stderr));
    let text = format!("exit={}\n\nstdout:\n{}\n\nstderr:\n{}\n", output.status, stdout, stderr);
    if let Some(path) = directory {
        drop(fs::write(path.join(name), &text));
    }
    eprintln!("{}", crate::env::safe_truncate_str(&text, 1_500));
}

/// Remove common credential-bearing diagnostic lines before evidence persistence.
fn redact_diagnostic(value: &str) -> String {
    value
        .lines()
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            if ["authorization:", "password:", "token:", "secret:", "privatekey:"]
                .iter()
                .any(|key| lower.trim_start().starts_with(key))
            {
                "[REDACTED]".to_owned()
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
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
