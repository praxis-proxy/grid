//! Kubeconfig export for container-reachable cluster access.
//!
//! Retrieves kubeconfigs from KIND clusters, rewrites loopback
//! server URLs to DNS names reachable on the Forge Docker network,
//! and writes the result atomically under `.forge/runtime/kubeconfig/`.

use std::path::Path;

use crate::{cluster::kind, command::runner::CommandRunner, error::ForgeError};

/// Export a container-reachable kubeconfig for one cluster.
///
/// Retrieves the kubeconfig from Kind, rewrites loopback server URLs
/// to the control-plane container DNS name, and writes the result to
/// `{state_dir}/runtime/kubeconfig/{cluster_name}/config`.
///
/// # Errors
///
/// Returns [`ForgeError`] if the Kind command, YAML rewrite, or file
/// write fails.
pub fn export_kubeconfig(
    runner: &dyn CommandRunner,
    kind_name: &str,
    cluster_name: &str,
    state_dir: &Path,
) -> Result<(), ForgeError> {
    let raw = kind::get_kubeconfig(runner, kind_name)?;
    if raw.trim().is_empty() {
        return Err(ForgeError::State(format!(
            "kind returned an empty kubeconfig for cluster '{kind_name}'"
        )));
    }
    let rewritten = rewrite_kubeconfig(&raw, kind_name)?;
    let dir = state_dir.join("runtime").join("kubeconfig").join(cluster_name);
    write_kubeconfig_file(&dir, &rewritten)
}

/// Rewrite loopback server URLs in a kubeconfig YAML string.
///
/// Parses the YAML, walks `.clusters[].cluster.server`, and replaces
/// loopback hosts with `https://{kind_name}-control-plane:6443`.
/// Non-loopback URLs pass through unchanged.
///
/// # Errors
///
/// Returns [`ForgeError::Yaml`] if the input is not valid YAML.
pub fn rewrite_kubeconfig(yaml: &str, kind_name: &str) -> Result<String, ForgeError> {
    let mut doc: serde_yaml::Value = serde_yaml::from_str(yaml)?;
    rewrite_cluster_entries(&mut doc, kind_name);
    let output = serde_yaml::to_string(&doc)?;
    Ok(output)
}

// ---------------------------------------------------------------
// Server URL rewriting
// ---------------------------------------------------------------

/// Rewrite server URLs in all kubeconfig cluster entries.
fn rewrite_cluster_entries(doc: &mut serde_yaml::Value, kind_name: &str) {
    let Some(clusters) = doc.get_mut("clusters").and_then(serde_yaml::Value::as_sequence_mut) else {
        return;
    };
    for entry in clusters {
        rewrite_cluster_entry(entry, kind_name);
    }
}

/// Rewrite the server URL in one kubeconfig cluster entry.
fn rewrite_cluster_entry(entry: &mut serde_yaml::Value, kind_name: &str) {
    let url = entry
        .get("cluster")
        .and_then(|cluster| cluster.get("server"))
        .and_then(serde_yaml::Value::as_str)
        .map(ToOwned::to_owned);
    let Some(url) = url else {
        return;
    };
    if let Some(rewritten) = rewrite_loopback_url(&url, kind_name)
        && let Some(server) = entry.get_mut("cluster").and_then(|cluster| cluster.get_mut("server"))
    {
        *server = serde_yaml::Value::String(rewritten);
    }
}

/// Return a rewritten URL if the host is a loopback address.
///
/// Replaces the host and port with
/// `{kind_name}-control-plane:6443`. Returns [`None`] if the URL
/// scheme is not `https://` or the host is not loopback.
fn rewrite_loopback_url(server_url: &str, kind_name: &str) -> Option<String> {
    let without_scheme = server_url.strip_prefix("https://")?;
    let host = extract_host(without_scheme);
    if !is_loopback_host(host) {
        return None;
    }
    Some(format!("https://{kind_name}-control-plane:6443"))
}

/// Extract the host portion from a `host:port` string.
///
/// Handles both IPv4 (`host:port`) and bracketed IPv6
/// (`[host]:port`) forms.
fn extract_host(host_port: &str) -> &str {
    if let Some(rest) = host_port.strip_prefix('[') {
        rest.split(']').next().unwrap_or(rest)
    } else {
        host_port.split(':').next().unwrap_or(host_port)
    }
}

/// Check whether a hostname is a loopback address.
fn is_loopback_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "0.0.0.0" | "::1")
}

// ---------------------------------------------------------------
// File output
// ---------------------------------------------------------------

/// Write kubeconfig content atomically to `{dir}/config`.
fn write_kubeconfig_file(dir: &Path, content: &str) -> Result<(), ForgeError> {
    std::fs::create_dir_all(dir).map_err(ForgeError::Io)?;
    let final_path = dir.join("config");
    let tmp_path = dir.join("config.tmp");
    std::fs::write(&tmp_path, content).map_err(ForgeError::Io)?;
    std::fs::rename(&tmp_path, &final_path).map_err(ForgeError::Io)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::runner::{CommandOutput, MockRunner};

    /// Build a minimal kubeconfig YAML with the given server URL.
    fn sample_kubeconfig(server_url: &str) -> String {
        format!(
            "\
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: dGVzdC1jYQ==
    server: {server_url}
  name: kind-test
contexts:
- context:
    cluster: kind-test
    user: kind-test
  name: kind-test
current-context: kind-test
users:
- name: kind-test
  user:
    client-certificate-data: dGVzdC1jZXJ0
    client-key-data: dGVzdC1rZXk=
"
        )
    }

    /// Create a temp dir for test state.
    fn test_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        })
    }

    #[test]
    fn export_calls_kind_get_kubeconfig() {
        let dir = test_dir();
        let mut runner = MockRunner::new();
        runner.respond(
            "kind get kubeconfig --name grid-glb-edge-control",
            CommandOutput {
                status: 0,
                stdout: sample_kubeconfig("https://127.0.0.1:42789"),
                stderr: String::new(),
            },
        );
        export_kubeconfig(&runner, "grid-glb-edge-control", "edge-control", dir.path())
            .unwrap_or_else(|_| std::process::abort());
        assert!(
            runner.was_called("kind get kubeconfig --name grid-glb-edge-control"),
            "should call kind get kubeconfig"
        );
    }

    #[test]
    fn rewrites_loopback_server_url() {
        let input = sample_kubeconfig("https://127.0.0.1:42789");
        let output = rewrite_kubeconfig(&input, "grid-glb-edge-control").unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(
            output.contains("https://grid-glb-edge-control-control-plane:6443"),
            "should rewrite loopback URL: {output}"
        );
        assert!(
            !output.contains("127.0.0.1"),
            "original loopback should be gone: {output}"
        );
    }

    #[test]
    fn preserves_non_loopback_server_url() {
        let input = sample_kubeconfig("https://10.0.0.1:6443");
        let output = rewrite_kubeconfig(&input, "grid-glb-edge-control").unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(
            output.contains("https://10.0.0.1:6443"),
            "non-loopback URL should be preserved: {output}"
        );
    }

    #[test]
    fn writes_to_runtime_kubeconfig_path() {
        let dir = test_dir();
        let mut runner = MockRunner::new();
        runner.respond(
            "kind get kubeconfig --name grid-glb-edge-control",
            CommandOutput {
                status: 0,
                stdout: sample_kubeconfig("https://127.0.0.1:42789"),
                stderr: String::new(),
            },
        );
        export_kubeconfig(&runner, "grid-glb-edge-control", "edge-control", dir.path())
            .unwrap_or_else(|_| std::process::abort());
        let path = dir.path().join("runtime/kubeconfig/edge-control/config");
        assert!(path.exists(), "kubeconfig file should exist at {}", path.display());
        let content = std::fs::read_to_string(&path).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(
            content.contains("control-plane:6443"),
            "file should contain rewritten URL: {content}"
        );
    }

    #[test]
    fn empty_kubeconfig_output_is_error() {
        let dir = test_dir();
        let mut runner = MockRunner::new();
        runner.respond(
            "kind get kubeconfig --name test-hub",
            CommandOutput {
                status: 0,
                stdout: "   \n".to_owned(),
                stderr: String::new(),
            },
        );

        let err = match export_kubeconfig(&runner, "test-hub", "hub", dir.path()) {
            Ok(()) => std::process::abort(),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("empty kubeconfig"),
            "error should explain empty kubeconfig output: {msg}"
        );
    }

    #[test]
    fn command_display_omits_secret_material() {
        let dir = test_dir();
        let mut runner = MockRunner::new();
        runner.respond(
            "kind get kubeconfig --name test-hub",
            CommandOutput {
                status: 0,
                stdout: sample_kubeconfig("https://127.0.0.1:42789"),
                stderr: String::new(),
            },
        );
        export_kubeconfig(&runner, "test-hub", "hub", dir.path()).unwrap_or_else(|_| std::process::abort());
        for call in runner.calls() {
            let display = format!("{call}");
            assert!(
                !display.contains("certificate-authority-data"),
                "display leaks CA: {display}"
            );
            assert!(
                !display.contains("client-certificate-data"),
                "display leaks cert: {display}"
            );
            assert!(!display.contains("client-key-data"), "display leaks key: {display}");
        }
    }
}
