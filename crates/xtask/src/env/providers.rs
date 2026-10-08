//! Docker container lifecycle for mock provider servers.

use std::process::Command;

use crate::env::config::ProviderConfig;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Docker image name for mock providers.
const IMAGE_NAME: &str = "grid-mock-providers";

/// Container name prefix.
const CONTAINER_PREFIX: &str = "grid-mock-";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Start all mock provider containers.
///
/// Idempotent: skips containers that are already running.
///
/// # Errors
///
/// Returns an error if Docker commands fail.
pub(crate) fn start_all(cfg: &ProviderConfig) -> Result<(), Box<dyn std::error::Error>> {
    start_provider("openai", cfg.openai.port)?;
    start_provider("anthropic", cfg.anthropic.port)?;
    start_provider("bedrock", cfg.bedrock.port)?;
    start_provider("vertex", cfg.vertex.port)?;
    Ok(())
}

/// Stop and remove all mock provider containers.
///
/// Idempotent: skips containers that are not running.
///
/// # Errors
///
/// Returns an error if Docker commands fail.
pub(crate) fn stop_all() -> Result<(), Box<dyn std::error::Error>> {
    stop_provider("openai")?;
    stop_provider("anthropic")?;
    stop_provider("bedrock")?;
    stop_provider("vertex")?;
    Ok(())
}

/// Check whether a provider container is running.
pub(crate) fn is_running(provider: &str) -> bool {
    container_running(&container_name(provider))
}

// ---------------------------------------------------------------------------
// Container commands
// ---------------------------------------------------------------------------

/// Build the Docker run arguments for a provider.
pub(crate) fn docker_run_args(provider: &str, port: u16) -> Vec<String> {
    vec![
        "run".to_owned(),
        "-d".to_owned(),
        "--rm".to_owned(),
        "--name".to_owned(),
        container_name(provider),
        "-p".to_owned(),
        format!("{port}:8080"),
        IMAGE_NAME.to_owned(),
        "--provider".to_owned(),
        provider.to_owned(),
        "--port".to_owned(),
        "8080".to_owned(),
    ]
}

/// Start a single provider container.
fn start_provider(provider: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let name = container_name(provider);

    if container_running(&name) {
        eprintln!("  mock-{provider} already running, skipping");
        return Ok(());
    }

    eprintln!("  starting mock-{provider} on port {port}...");
    let args = docker_run_args(provider, port);
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    run_docker(&arg_refs)
}

/// Stop a single provider container.
fn stop_provider(provider: &str) -> Result<(), Box<dyn std::error::Error>> {
    let name = container_name(provider);

    if !container_exists(&name) {
        eprintln!("  mock-{provider} not running, skipping");
        return Ok(());
    }

    eprintln!("  stopping mock-{provider}...");
    run_docker(&["stop", &name])
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Build the container name for a provider.
fn container_name(provider: &str) -> String {
    format!("{CONTAINER_PREFIX}{provider}")
}

/// Check whether a container is currently running.
fn container_running(name: &str) -> bool {
    container_inspect(name, "running")
}

/// Check whether a container exists (running or stopped).
fn container_exists(name: &str) -> bool {
    docker_cmd()
        .args(["inspect", "--format", "{{.State.Status}}", name])
        .output()
        .ok()
        .is_some_and(|o| o.status.success())
}

/// Inspect a container for a specific state.
fn container_inspect(name: &str, expected_status: &str) -> bool {
    docker_cmd()
        .args(["inspect", "--format", "{{.State.Status}}", name])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .is_some_and(|s| s.trim() == expected_status)
}

/// Run a Docker command.
fn run_docker(args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let status = docker_cmd().args(args).status()?;
    if !status.success() {
        let cmd = format!("docker {}", args.join(" "));
        return Err(format!("{cmd} failed: {status}").into());
    }
    Ok(())
}

/// Build a Docker command, preferring podman.
fn docker_cmd() -> Command {
    if which_exists("podman") {
        Command::new("podman")
    } else {
        Command::new("docker")
    }
}

/// Check whether a command exists on PATH.
fn which_exists(cmd: &str) -> bool {
    Command::new("which")
        .arg(cmd)
        .output()
        .ok()
        .is_some_and(|o| o.status.success())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_name_format() {
        assert_eq!(container_name("openai"), "grid-mock-openai", "wrong container name");
        assert_eq!(container_name("bedrock"), "grid-mock-bedrock", "wrong container name");
    }

    #[test]
    fn docker_run_args_format() {
        let args = docker_run_args("openai", 10_001);
        assert!(args.contains(&"run".to_owned()), "should have run");
        assert!(args.contains(&"-d".to_owned()), "should be detached");
        assert!(
            args.contains(&"grid-mock-openai".to_owned()),
            "should have container name"
        );
        assert!(args.contains(&"10001:8080".to_owned()), "should map port");
        assert!(args.contains(&"openai".to_owned()), "should pass provider");
    }

    #[test]
    fn docker_run_args_different_ports() {
        let args_a = docker_run_args("anthropic", 10_002);
        let args_b = docker_run_args("bedrock", 10_003);
        assert!(args_a.contains(&"10002:8080".to_owned()), "anthropic port");
        assert!(args_b.contains(&"10003:8080".to_owned()), "bedrock port");
        assert!(args_a.contains(&"grid-mock-anthropic".to_owned()), "anthropic name");
        assert!(args_b.contains(&"grid-mock-bedrock".to_owned()), "bedrock name");
    }
}
