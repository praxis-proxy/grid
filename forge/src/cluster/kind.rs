//! KIND cluster operations.
//!
//! All KIND commands are constructed as structured [`CommandSpec`]
//! values and executed through the [`CommandRunner`] trait.  No
//! shell strings, no implicit `sh -c`.

use std::collections::BTreeMap;

use crate::{
    command::runner::{CommandOutput, CommandRunner, CommandSpec},
    config::NodeConfig,
    error::ForgeError,
};

// ---------------------------------------------------------------
// Naming
// ---------------------------------------------------------------

/// Build the full KIND cluster name: `"{prefix}-{name}"`.
pub fn kind_cluster_name(prefix: &str, name: &str) -> String {
    format!("{prefix}-{name}")
}

/// Build the kubectl context for a KIND cluster: `"kind-{kind_name}"`.
pub fn kubectl_context(kind_name: &str) -> String {
    format!("kind-{kind_name}")
}

// ---------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------

/// Check whether a KIND cluster with the given name exists.
///
/// # Errors
///
/// Returns [`ForgeError`] if the `kind get clusters` command fails.
pub fn cluster_exists(runner: &dyn CommandRunner, kind_name: &str) -> Result<bool, ForgeError> {
    let clusters = list_clusters(runner)?;
    Ok(clusters.iter().any(|c| c == kind_name))
}

/// Create a KIND cluster with a generated config.
///
/// Writes a temporary KIND config file to `config_dir`, runs
/// `kind create cluster`, and cleans up the temp file.
///
/// # Errors
///
/// Returns [`ForgeError`] if the cluster cannot be created.
pub fn create_cluster(
    runner: &dyn CommandRunner,
    kind_name: &str,
    nodes: &NodeConfig,
    config_dir: &std::path::Path,
) -> Result<(), ForgeError> {
    let config_yaml = generate_kind_config(nodes);
    let config_path = write_kind_config(config_dir, kind_name, &config_yaml)?;
    let result = run_create(runner, kind_name, &config_path);
    cleanup_kind_config(&config_path);
    result
}

/// Delete a KIND cluster by name.
///
/// # Errors
///
/// Returns [`ForgeError`] if the deletion command fails.
pub fn delete_cluster(runner: &dyn CommandRunner, kind_name: &str) -> Result<(), ForgeError> {
    let spec = delete_spec(kind_name);
    let output = runner.run(&spec)?;
    check_success(&output, "kind delete cluster")
}

/// List all KIND clusters.
///
/// # Errors
///
/// Returns [`ForgeError`] if the `kind get clusters` command fails.
pub fn list_clusters(runner: &dyn CommandRunner) -> Result<Vec<String>, ForgeError> {
    let spec = list_spec();
    let output = runner.run(&spec)?;
    Ok(parse_cluster_list(&output))
}

/// Get the kubeconfig for a KIND cluster.
///
/// # Errors
///
/// Returns [`ForgeError`] if the command fails.
pub fn get_kubeconfig(runner: &dyn CommandRunner, kind_name: &str) -> Result<String, ForgeError> {
    let spec = kubeconfig_spec(kind_name);
    let output = runner.run(&spec)?;
    check_success(&output, "kind get kubeconfig")?;
    Ok(output.stdout)
}

/// Load a container image into a KIND cluster.
///
/// # Errors
///
/// Returns [`ForgeError`] if the command fails.
pub fn load_image(runner: &dyn CommandRunner, kind_name: &str, image: &str) -> Result<(), ForgeError> {
    let spec = load_image_spec(kind_name, image);
    let output = runner.run(&spec)?;
    check_success(&output, "kind load docker-image")
}

/// Run kubectl against a KIND cluster's context.
///
/// # Errors
///
/// Returns [`ForgeError`] if the command fails to execute.
pub fn run_kubectl(runner: &dyn CommandRunner, kind_name: &str, args: &[String]) -> Result<CommandOutput, ForgeError> {
    let context = kubectl_context(kind_name);
    let spec = kubectl_spec(&context, args);
    runner.run(&spec)
}

// ---------------------------------------------------------------
// KIND config generation
// ---------------------------------------------------------------

/// Generate a KIND cluster config YAML from a [`NodeConfig`].
pub fn generate_kind_config(nodes: &NodeConfig) -> String {
    let mut yaml = String::from("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n");
    for _ in 0..nodes.control_planes {
        yaml.push_str("  - role: control-plane\n");
    }
    for _ in 0..nodes.workers {
        yaml.push_str("  - role: worker\n");
    }
    yaml
}

// ---------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------

/// Write a KIND config to a temp file in the given directory.
fn write_kind_config(dir: &std::path::Path, kind_name: &str, content: &str) -> Result<std::path::PathBuf, ForgeError> {
    let path = dir.join(format!("kind-config-{kind_name}.yaml"));
    std::fs::write(&path, content)
        .map_err(|e| ForgeError::State(format!("cannot write KIND config {}: {e}", path.display())))?;
    Ok(path)
}

/// Run `kind create cluster` with the given config file.
fn run_create(runner: &dyn CommandRunner, kind_name: &str, config_path: &std::path::Path) -> Result<(), ForgeError> {
    let spec = create_spec(kind_name, config_path);
    let output = runner.run(&spec)?;
    check_success(&output, "kind create cluster")
}

/// Remove the temporary KIND config file (best-effort).
fn cleanup_kind_config(path: &std::path::Path) {
    let _ignored = std::fs::remove_file(path);
}

/// Build a `kind create cluster` command spec.
fn create_spec(kind_name: &str, config_path: &std::path::Path) -> CommandSpec {
    CommandSpec {
        program: "kind".into(),
        args: vec![
            "create".into(),
            "cluster".into(),
            "--name".into(),
            kind_name.into(),
            "--config".into(),
            config_path.as_os_str().to_owned(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `kind delete cluster` command spec.
fn delete_spec(kind_name: &str) -> CommandSpec {
    CommandSpec {
        program: "kind".into(),
        args: vec!["delete".into(), "cluster".into(), "--name".into(), kind_name.into()],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `kind get clusters` command spec.
fn list_spec() -> CommandSpec {
    CommandSpec {
        program: "kind".into(),
        args: vec!["get".into(), "clusters".into()],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `kind get kubeconfig` command spec.
fn kubeconfig_spec(kind_name: &str) -> CommandSpec {
    CommandSpec {
        program: "kind".into(),
        args: vec!["get".into(), "kubeconfig".into(), "--name".into(), kind_name.into()],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `kind load docker-image` command spec.
fn load_image_spec(kind_name: &str, image: &str) -> CommandSpec {
    CommandSpec {
        program: "kind".into(),
        args: vec![
            "load".into(),
            "docker-image".into(),
            image.into(),
            "--name".into(),
            kind_name.into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a kubectl command spec with the correct context.
fn kubectl_spec(context: &str, args: &[String]) -> CommandSpec {
    let mut cmd_args: Vec<std::ffi::OsString> = vec!["--context".into(), context.into()];
    cmd_args.extend(args.iter().map(std::ffi::OsString::from));
    CommandSpec {
        program: "kubectl".into(),
        args: cmd_args,
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Parse the output of `kind get clusters` into a list of names.
fn parse_cluster_list(output: &CommandOutput) -> Vec<String> {
    output
        .stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Check command output for success (exit code 0).
fn check_success(output: &CommandOutput, program: &str) -> Result<(), ForgeError> {
    if output.status == 0 {
        return Ok(());
    }
    Err(ForgeError::Command {
        program: program.to_owned(),
        message: format!("exit code {}: {}", output.status, output.stderr.trim()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::runner::{CommandOutput, MockRunner};

    #[test]
    fn kind_cluster_name_format() {
        assert_eq!(kind_cluster_name("forge", "hub"), "forge-hub");
        assert_eq!(kind_cluster_name("dev", "edge"), "dev-edge");
    }

    #[test]
    fn kubectl_context_format() {
        assert_eq!(kubectl_context("forge-hub"), "kind-forge-hub");
    }

    #[test]
    fn generate_kind_config_default_nodes() {
        let nodes = NodeConfig::default();
        let yaml = generate_kind_config(&nodes);
        assert!(yaml.contains("control-plane"), "should have control-plane");
        let cp_count = yaml.matches("control-plane").count();
        assert_eq!(cp_count, 1, "default should have 1 control-plane, got {cp_count}");
        assert!(!yaml.contains("worker"), "default should have no workers");
    }

    #[test]
    fn generate_kind_config_multi_node() {
        let nodes = NodeConfig {
            control_planes: 3,
            workers: 2,
        };
        let yaml = generate_kind_config(&nodes);
        let cp_count = yaml.matches("control-plane").count();
        let w_count = yaml.matches("worker").count();
        assert_eq!(cp_count, 3, "should have 3 control-planes, got {cp_count}");
        assert_eq!(w_count, 2, "should have 2 workers, got {w_count}");
    }

    #[test]
    fn parse_cluster_list_handles_empty() {
        let output = CommandOutput {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
        };
        assert!(
            parse_cluster_list(&output).is_empty(),
            "empty output should yield empty list"
        );
    }

    #[test]
    fn parse_cluster_list_handles_multiple() {
        let output = CommandOutput {
            status: 0,
            stdout: "forge-hub\nforge-edge\n".to_owned(),
            stderr: String::new(),
        };
        let clusters = parse_cluster_list(&output);
        assert_eq!(clusters.len(), 2, "should have 2 clusters");
        assert_eq!(clusters.first().map(String::as_str), Some("forge-hub"), "first cluster");
        assert_eq!(
            clusters.get(1).map(String::as_str),
            Some("forge-edge"),
            "second cluster"
        );
    }

    #[test]
    fn cluster_exists_returns_true_when_found() {
        let mut runner = MockRunner::new();
        runner.respond(
            "kind get clusters",
            CommandOutput {
                status: 0,
                stdout: "forge-hub\nforge-edge\n".to_owned(),
                stderr: String::new(),
            },
        );
        let exists = cluster_exists(&runner, "forge-hub").unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(exists, "forge-hub should exist");
    }

    #[test]
    fn cluster_exists_returns_false_when_missing() {
        let mut runner = MockRunner::new();
        runner.respond(
            "kind get clusters",
            CommandOutput {
                status: 0,
                stdout: "forge-hub\n".to_owned(),
                stderr: String::new(),
            },
        );
        let exists = cluster_exists(&runner, "forge-missing").unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(!exists, "forge-missing should not exist");
    }
}
