//! Container network lifecycle management.
//!
//! Creates, removes, and inspects Docker/Podman networks for Forge
//! environments.  All commands are structured [`CommandSpec`] values
//! executed through [`CommandRunner`].  No shell strings.

use std::collections::BTreeMap;

use crate::{
    command::runner::{CommandOutput, CommandRunner, CommandSpec},
    error::ForgeError,
};

// ---------------------------------------------------------------
// Naming
// ---------------------------------------------------------------

/// Verify the resolved runtime supports cross-cluster networking.
///
/// `KIND_EXPERIMENTAL_DOCKER_NETWORK` is Docker-only; Podman does not
/// support it.  Call after `runtime::resolve()` when cross-cluster
/// networking is configured.
///
/// # Errors
///
/// Returns [`ForgeError::Config`] if the resolved binary is not Docker.
pub fn require_docker_for_cross_cluster(binary: &str) -> Result<(), ForgeError> {
    if binary == "docker" {
        return Ok(());
    }
    Err(ForgeError::Config(format!(
        "cross-cluster networking requires Docker, but runtime resolved to {binary:?}"
    )))
}

/// Build the deterministic network name: `"{env_name}-net"`.
pub fn network_name(env_name: &str) -> String {
    format!("{env_name}-net")
}

// ---------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------

/// Create a container network with ownership labels.
///
/// Idempotent: returns `Ok(())` if the network already exists
/// and is owned by this environment.
///
/// # Errors
///
/// Returns [`ForgeError`] if the network cannot be created or
/// an existing network has mismatched ownership labels.
pub fn create_network(
    runner: &dyn CommandRunner,
    binary: &str,
    net_name: &str,
    env_name: &str,
) -> Result<(), ForgeError> {
    if network_exists(runner, binary, net_name)? {
        return verify_ownership(runner, binary, net_name, env_name);
    }
    let spec = create_spec(binary, net_name, env_name);
    let output = runner.run(&spec)?;
    check_success(&output, "network create")
}

/// Remove a container network after verifying ownership.
///
/// Idempotent: returns `Ok(())` if the network does not exist.
///
/// # Errors
///
/// Returns [`ForgeError`] if ownership verification fails or
/// the network cannot be removed.
pub fn remove_network(
    runner: &dyn CommandRunner,
    binary: &str,
    net_name: &str,
    env_name: &str,
) -> Result<(), ForgeError> {
    if !network_exists(runner, binary, net_name)? {
        return Ok(());
    }
    verify_ownership(runner, binary, net_name, env_name)?;
    let spec = remove_spec(binary, net_name);
    let output = runner.run(&spec)?;
    check_success(&output, "network rm")
}

/// Check whether a container network with the given name exists.
///
/// # Errors
///
/// Returns [`ForgeError`] if the runtime binary cannot execute.
pub fn network_exists(runner: &dyn CommandRunner, binary: &str, net_name: &str) -> Result<bool, ForgeError> {
    let spec = inspect_spec(binary, net_name);
    let output = runner.run(&spec)?;
    Ok(output.status == 0)
}

// ---------------------------------------------------------------
// Ownership
// ---------------------------------------------------------------

/// Verify that an existing network is owned by this environment.
fn verify_ownership(
    runner: &dyn CommandRunner,
    binary: &str,
    net_name: &str,
    env_name: &str,
) -> Result<(), ForgeError> {
    let labels = inspect_labels(runner, binary, net_name)?;
    check_label(&labels, "forge.managed", "true", net_name)?;
    check_label(&labels, "forge.environment", env_name, net_name)
}

/// Fetch labels from an existing network.
fn inspect_labels(
    runner: &dyn CommandRunner,
    binary: &str,
    net_name: &str,
) -> Result<BTreeMap<String, String>, ForgeError> {
    let spec = labels_spec(binary, net_name);
    let output = runner.run(&spec)?;
    check_success(&output, "network inspect")?;
    parse_labels(&output.stdout)
}

/// Verify a single label value matches the expected value.
fn check_label(labels: &BTreeMap<String, String>, key: &str, expected: &str, net_name: &str) -> Result<(), ForgeError> {
    match labels.get(key) {
        Some(v) if v == expected => Ok(()),
        Some(v) => Err(ownership_mismatch(net_name, key, expected, v)),
        None => Err(missing_label(net_name, key)),
    }
}

/// Build an error for a mismatched ownership label.
fn ownership_mismatch(net_name: &str, key: &str, expected: &str, actual: &str) -> ForgeError {
    ForgeError::State(format!("network '{net_name}' has {key}={actual}, expected {expected}"))
}

/// Build an error for a missing ownership label.
fn missing_label(net_name: &str, key: &str) -> ForgeError {
    ForgeError::State(format!(
        "network '{net_name}' missing label {key} \u{2014} not managed by Forge"
    ))
}

// ---------------------------------------------------------------
// Command specs
// ---------------------------------------------------------------

/// Build a `<binary> network create` command spec with labels.
fn create_spec(binary: &str, net_name: &str, env_name: &str) -> CommandSpec {
    CommandSpec {
        program: binary.into(),
        args: vec![
            "network".into(),
            "create".into(),
            "--label".into(),
            "forge.managed=true".into(),
            "--label".into(),
            format!("forge.environment={env_name}").into(),
            net_name.into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `<binary> network rm` command spec.
fn remove_spec(binary: &str, net_name: &str) -> CommandSpec {
    CommandSpec {
        program: binary.into(),
        args: vec!["network".into(), "rm".into(), net_name.into()],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `<binary> network inspect` command spec.
fn inspect_spec(binary: &str, net_name: &str) -> CommandSpec {
    CommandSpec {
        program: binary.into(),
        args: vec!["network".into(), "inspect".into(), net_name.into()],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `<binary> network inspect --format` spec for labels.
fn labels_spec(binary: &str, net_name: &str) -> CommandSpec {
    CommandSpec {
        program: binary.into(),
        args: vec![
            "network".into(),
            "inspect".into(),
            net_name.into(),
            "--format".into(),
            "{{json .Labels}}".into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

// ---------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------

/// Parse JSON labels from `docker network inspect --format` output.
fn parse_labels(stdout: &str) -> Result<BTreeMap<String, String>, ForgeError> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Ok(BTreeMap::new());
    }
    serde_json::from_str(trimmed).map_err(|e| ForgeError::State(format!("cannot parse network labels: {e}")))
}

/// Check command output for success (exit code 0).
fn check_success(output: &CommandOutput, context: &str) -> Result<(), ForgeError> {
    if output.status == 0 {
        return Ok(());
    }
    Err(ForgeError::Command {
        program: context.to_owned(),
        message: format!("exit code {}: {}", output.status, output.stderr.trim()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::runner::MockRunner;

    /// Successful empty command output.
    fn ok() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    /// Failed command output (network not found).
    fn not_found() -> CommandOutput {
        CommandOutput {
            status: 1,
            stdout: String::new(),
            stderr: "network test-net not found\n".to_owned(),
        }
    }

    /// Labels JSON for a Forge-managed network.
    fn owned_labels(env: &str) -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: format!(r#"{{"forge.managed":"true","forge.environment":"{env}"}}"#),
            stderr: String::new(),
        }
    }

    /// Labels JSON for a network not managed by Forge.
    fn foreign_labels() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: r#"{"some.other":"label"}"#.to_owned(),
            stderr: String::new(),
        }
    }

    #[test]
    fn network_name_format() {
        assert_eq!(network_name("test"), "test-net", "simple name");
        assert_eq!(network_name("prod-env"), "prod-env-net", "hyphenated name");
    }

    #[test]
    fn create_when_not_exists() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", not_found());
        runner.respond("docker", ok());

        create_network(&runner, "docker", "test-net", "test").unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("network create"), "should call network create");
        assert!(runner.was_called("forge.managed=true"), "should include managed label");
        assert!(runner.was_called("forge.environment=test"), "should include env label");
    }

    #[test]
    fn create_skips_when_exists_with_correct_owner() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            owned_labels("test"),
        );

        create_network(&runner, "docker", "test-net", "test").unwrap_or_else(|_| std::process::abort());
        assert!(
            !runner.was_called("network create"),
            "should not create existing network"
        );
    }

    #[test]
    fn create_rejects_wrong_owner() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            owned_labels("other-env"),
        );

        let result = create_network(&runner, "docker", "test-net", "test");
        let Err(err) = result else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(
            msg.contains("expected test"),
            "error should mention expected env: {msg}"
        );
    }

    #[test]
    fn create_rejects_unmanaged_network() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            foreign_labels(),
        );

        let result = create_network(&runner, "docker", "test-net", "test");
        assert!(result.is_err(), "should reject unmanaged network");
    }

    #[test]
    fn remove_with_correct_owner() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            owned_labels("test"),
        );
        runner.respond("docker network rm test-net", ok());

        remove_network(&runner, "docker", "test-net", "test").unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("network rm"), "should call network rm");
    }

    #[test]
    fn remove_skips_when_not_exists() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", not_found());

        remove_network(&runner, "docker", "test-net", "test").unwrap_or_else(|_| std::process::abort());
        assert!(
            !runner.was_called("network rm"),
            "should not call rm on missing network"
        );
    }

    #[test]
    fn remove_rejects_wrong_owner() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            owned_labels("other-env"),
        );

        let result = remove_network(&runner, "docker", "test-net", "test");
        assert!(result.is_err(), "should reject mismatched owner on remove");
    }

    #[test]
    fn remove_refuses_unmanaged_network() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            foreign_labels(),
        );

        let result = remove_network(&runner, "docker", "test-net", "test");
        assert!(result.is_err(), "should reject unmanaged network on remove");
        assert!(!runner.was_called("network rm"), "must not remove unmanaged network");
    }

    #[test]
    fn exists_true_when_present() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());

        let exists = network_exists(&runner, "docker", "test-net").unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(exists, "should report network as existing");
    }

    #[test]
    fn exists_false_when_missing() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", not_found());

        let exists = network_exists(&runner, "docker", "test-net").unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(!exists, "should report network as not existing");
    }

    #[test]
    fn parse_labels_valid_json() {
        let input = r#"{"forge.managed":"true","forge.environment":"test"}"#;
        let labels = parse_labels(input).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(
            labels.get("forge.managed").map(String::as_str),
            Some("true"),
            "managed label"
        );
        assert_eq!(
            labels.get("forge.environment").map(String::as_str),
            Some("test"),
            "env label"
        );
    }

    #[test]
    fn parse_labels_empty_string() {
        let labels = parse_labels("").unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(labels.is_empty(), "empty input should yield empty map");
    }

    #[test]
    fn podman_uses_correct_binary() {
        let mut runner = MockRunner::new();
        runner.respond("podman network inspect test-net", not_found());
        runner.respond("podman", ok());

        create_network(&runner, "podman", "test-net", "test").unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("podman"), "should use podman binary");
    }
}
