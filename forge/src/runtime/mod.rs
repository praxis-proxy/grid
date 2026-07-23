//! Container runtime detection.
//!
//! Probes for Docker and Podman via [`CommandRunner`] and resolves
//! the `Auto` runtime provider to a concrete provider.

use std::collections::BTreeMap;

use crate::{
    command::runner::{CommandRunner, CommandSpec},
    config::RuntimeProvider,
    error::ForgeError,
};

/// A resolved container runtime with its provider and binary name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedRuntime {
    /// The concrete provider (never `Auto`).
    pub provider: RuntimeProvider,
    /// Name of the runtime binary.
    pub binary: String,
}

/// Resolve a runtime provider, auto-detecting if necessary.
///
/// # Errors
///
/// Returns [`ForgeError::Runtime`] if the requested runtime is not
/// available, or if auto-detection finds neither Docker nor Podman.
pub fn resolve(runner: &dyn CommandRunner, requested: &RuntimeProvider) -> Result<ResolvedRuntime, ForgeError> {
    match requested {
        RuntimeProvider::Docker => require_docker(runner),
        RuntimeProvider::Podman => require_podman(runner),
        RuntimeProvider::Auto => auto_detect(runner),
    }
}

/// Auto-detect: try Docker first, then Podman.
fn auto_detect(runner: &dyn CommandRunner) -> Result<ResolvedRuntime, ForgeError> {
    if let Some(rt) = probe_docker(runner) {
        return Ok(rt);
    }
    probe_podman(runner).ok_or_else(|| ForgeError::Runtime("neither docker nor podman found".to_owned()))
}

/// Require Docker to be available.
fn require_docker(runner: &dyn CommandRunner) -> Result<ResolvedRuntime, ForgeError> {
    probe_docker(runner).ok_or_else(|| ForgeError::Runtime("docker not found".to_owned()))
}

/// Require Podman to be available.
fn require_podman(runner: &dyn CommandRunner) -> Result<ResolvedRuntime, ForgeError> {
    probe_podman(runner).ok_or_else(|| ForgeError::Runtime("podman not found".to_owned()))
}

/// Probe Docker by running `docker version`.
fn probe_docker(runner: &dyn CommandRunner) -> Option<ResolvedRuntime> {
    probe_runtime(runner, "docker", RuntimeProvider::Docker)
}

/// Probe Podman by running `podman version`.
fn probe_podman(runner: &dyn CommandRunner) -> Option<ResolvedRuntime> {
    probe_runtime(runner, "podman", RuntimeProvider::Podman)
}

/// Probe a runtime by running `<program> version`.
fn probe_runtime(runner: &dyn CommandRunner, program: &str, provider: RuntimeProvider) -> Option<ResolvedRuntime> {
    let spec = version_spec(program);
    match runner.run(&spec) {
        Ok(out) if out.status == 0 => Some(ResolvedRuntime {
            provider,
            binary: program.to_owned(),
        }),
        _ => None,
    }
}

/// Build a `<program> version` command spec.
fn version_spec(program: &str) -> CommandSpec {
    CommandSpec {
        program: program.into(),
        args: vec!["version".into()],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::runner::{CommandOutput, MockRunner};

    /// Build a mock where the given program succeeds.
    fn mock_with_runtime(program: &str) -> MockRunner {
        let mut runner = MockRunner::new();
        runner.respond(
            &format!("{program} version"),
            CommandOutput {
                status: 0,
                stdout: format!("{program} version 24.0.0\n"),
                stderr: String::new(),
            },
        );
        runner
    }

    #[test]
    fn auto_detects_docker_first() {
        let runner = mock_with_runtime("docker");
        let rt = resolve(&runner, &RuntimeProvider::Auto).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(rt.provider, RuntimeProvider::Docker, "should detect docker");
    }

    #[test]
    fn auto_falls_back_to_podman() {
        let runner = mock_with_runtime("podman");
        let rt = resolve(&runner, &RuntimeProvider::Auto).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(rt.provider, RuntimeProvider::Podman, "should fall back to podman");
    }

    #[test]
    fn auto_fails_when_neither_found() {
        let runner = MockRunner::new();
        let result = resolve(&runner, &RuntimeProvider::Auto);
        assert!(result.is_err(), "should fail when neither runtime found");
    }

    #[test]
    fn explicit_docker_succeeds() {
        let runner = mock_with_runtime("docker");
        let rt = resolve(&runner, &RuntimeProvider::Docker).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(rt.binary, "docker", "binary should be docker");
    }

    #[test]
    fn explicit_docker_fails_when_missing() {
        let runner = MockRunner::new();
        let result = resolve(&runner, &RuntimeProvider::Docker);
        assert!(result.is_err(), "should fail when docker not found");
    }

    #[test]
    fn explicit_podman_succeeds() {
        let runner = mock_with_runtime("podman");
        let rt = resolve(&runner, &RuntimeProvider::Podman).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(rt.binary, "podman", "binary should be podman");
    }
}
