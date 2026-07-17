//! Gateway image build and load automation.
//!
//! Builds the composed Praxis AI gateway image and the mock EPP image
//! from the AI repository, then loads them into all kind clusters.
//!
//! When a cluster is configured with `backend = "mock-openai"`, the
//! `grid-mock-providers` image ([`kind::MOCK_PROVIDER_IMAGE`]) is also
//! loaded into that cluster.

use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

use crate::env::{config::EnvConfig, kind};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default environment variable for the AI repository path.
const AI_REPO_PATH_ENV: &str = "AI_REPO_PATH";

/// Image name for the composed Praxis AI gateway.
pub(crate) const GATEWAY_IMAGE: &str = "localhost/praxis-ai:llmd-ext-proc";

/// Image name for the mock EPP.
pub(crate) const MOCK_EPP_IMAGE: &str = "localhost/praxis-ai-mock-epp:latest";

/// Cargo features to enable for the gateway image.
const GATEWAY_FEATURES: &str = "llmd-ext-proc";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Build the composed gateway and mock EPP images.
///
/// Reads `AI_REPO_PATH` from the environment or uses the provided path.
///
/// # Errors
///
/// Returns an error if Docker/Podman or the build fails, or if required
/// source files are absent from the AI repository checkout.
pub(crate) fn build_all(ai_repo: &Path) -> Result<(), Box<dyn std::error::Error>> {
    verify_ai_repo_source_files(ai_repo)?;
    let engine = docker_engine();
    let build_ctx = ai_repo.parent().ok_or("AI repo path has no parent directory")?;
    eprintln!("  build context: {}", build_ctx.display());
    eprintln!("  AI repo:       {}", ai_repo.display());
    build_gateway_image(&engine, build_ctx, ai_repo)?;
    build_mock_epp_image(&engine, build_ctx, ai_repo)?;
    Ok(())
}

/// Verify that the required source files exist in the AI repository checkout.
///
/// Both files must exist in the selected AI repository checkout.
/// This fails early with a clear diagnostic rather than letting a Docker
/// build fail deep in the invocation chain.
fn verify_ai_repo_source_files(ai_repo: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let composed = ai_repo.join("Containerfile.composed");
    if !composed.exists() {
        return Err(format!(
            "AI repo is missing Containerfile.composed at {path}\n\
             This file is required to build {image}.\n\
             Confirm the AI checkout at {repo} has this file before running build-gateway-images.",
            path = composed.display(),
            image = GATEWAY_IMAGE,
            repo = ai_repo.display(),
        )
        .into());
    }
    let mock_epp_cf = ai_repo.join("integrations/llmd/mock-epp/Containerfile");
    if !mock_epp_cf.exists() {
        return Err(format!(
            "AI repo is missing integrations/llmd/mock-epp/Containerfile at {path}\n\
             This file is required to build {image}.\n\
             Confirm the AI checkout at {repo} has this directory before running build-gateway-images.",
            path = mock_epp_cf.display(),
            image = MOCK_EPP_IMAGE,
            repo = ai_repo.display(),
        )
        .into());
    }
    Ok(())
}

/// Load gateway images into all kind clusters.
///
/// Always loads [`GATEWAY_IMAGE`] and [`MOCK_EPP_IMAGE`] into every cluster.
///
/// For clusters configured with `backend = "mock-openai"`, also loads
/// [`kind::MOCK_PROVIDER_IMAGE`]. This image must exist locally as
/// `grid-mock-providers:latest` before running `load-gateway-images`.
///
/// # Errors
///
/// Returns an error if `kind load docker-image` fails.
pub(crate) fn load_all(cfg: &EnvConfig) -> Result<(), Box<dyn std::error::Error>> {
    use crate::env::config::ProviderBackend;

    for name in &cfg.clusters.names {
        let full = kind::cluster_name_from_config(name);
        for image in &[GATEWAY_IMAGE, MOCK_EPP_IMAGE] {
            eprintln!("  loading {image} into {full}...");
            run_cmd("kind", &["load", "docker-image", image, "--name", &full])?;
        }
        // Load mock-provider image only when a cluster needs it.
        if cfg
            .clusters
            .definitions
            .get(name)
            .is_some_and(|d| d.backend == ProviderBackend::MockOpenai)
        {
            eprintln!("  loading {} into {full}...", kind::MOCK_PROVIDER_IMAGE);
            run_cmd(
                "kind",
                &["load", "docker-image", kind::MOCK_PROVIDER_IMAGE, "--name", &full],
            )?;
        }
    }
    Ok(())
}

/// Resolve the AI repository path from env var or fallback.
///
/// Precedence:
/// 1. `AI_REPO_PATH` environment variable.
/// 2. Provided fallback path.
pub(crate) fn resolve_ai_repo_path(fallback: Option<&Path>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(val) = env::var(AI_REPO_PATH_ENV) {
        let p = PathBuf::from(val);
        if !p.exists() {
            return Err(format!("AI_REPO_PATH does not exist: {}", p.display()).into());
        }
        return Ok(p);
    }
    let p = fallback.ok_or("AI_REPO_PATH env var not set and no fallback provided")?;
    if !p.exists() {
        return Err(format!("AI repo path does not exist: {}", p.display()).into());
    }
    Ok(p.to_owned())
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Build the composed Praxis AI gateway image.
fn build_gateway_image(engine: &str, build_ctx: &Path, ai_repo: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let containerfile = ai_repo.join("Containerfile.composed");
    eprintln!("  building {GATEWAY_IMAGE} with features={GATEWAY_FEATURES}...");

    let status = Command::new(engine)
        .args([
            "build",
            "-f",
            &containerfile.to_string_lossy(),
            "--build-arg",
            &format!("CARGO_FEATURES={GATEWAY_FEATURES}"),
            "-t",
            GATEWAY_IMAGE,
            build_ctx.to_str().ok_or("build context path is not UTF-8")?,
        ])
        .status()?;

    if !status.success() {
        return Err(format!("docker build of {GATEWAY_IMAGE} failed: {status}").into());
    }
    eprintln!("  [PASS] built {GATEWAY_IMAGE}");
    Ok(())
}

/// Build the mock EPP image.
fn build_mock_epp_image(engine: &str, build_ctx: &Path, ai_repo: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let containerfile = ai_repo.join("integrations/llmd/mock-epp/Containerfile");
    eprintln!("  building {MOCK_EPP_IMAGE}...");

    let status = Command::new(engine)
        .args([
            "build",
            "-f",
            &containerfile.to_string_lossy(),
            "-t",
            MOCK_EPP_IMAGE,
            build_ctx.to_str().ok_or("build context path is not UTF-8")?,
        ])
        .status()?;

    if !status.success() {
        return Err(format!("docker build of {MOCK_EPP_IMAGE} failed: {status}").into());
    }
    eprintln!("  [PASS] built {MOCK_EPP_IMAGE}");
    Ok(())
}

/// Detect Docker or Podman.
fn docker_engine() -> String {
    if Command::new("podman")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
    {
        "podman".to_owned()
    } else {
        "docker".to_owned()
    }
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
    fn image_names_are_deterministic() {
        assert_eq!(
            GATEWAY_IMAGE, "localhost/praxis-ai:llmd-ext-proc",
            "gateway image name must be deterministic"
        );
        assert_eq!(
            MOCK_EPP_IMAGE, "localhost/praxis-ai-mock-epp:latest",
            "mock EPP image name must be deterministic"
        );
    }

    #[test]
    fn resolve_ai_repo_path_fallback_to_existing_dir() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let result = resolve_ai_repo_path(Some(dir.path()));
        assert!(
            result.is_ok_and(|p| p == dir.path()),
            "should return fallback path when env var is not set to a valid dir"
        );
    }

    #[test]
    fn resolve_ai_repo_path_fallback_missing_returns_error() {
        let missing = Path::new("/nonexistent/path/that/does/not/exist");
        let result = resolve_ai_repo_path(Some(missing));
        assert!(result.is_err(), "missing path should return error");
    }

    #[test]
    fn build_all_fails_fast_when_containerfile_composed_missing() {
        // verify_ai_repo_source_files must fail before any Docker invocation
        // when Containerfile.composed is absent from the AI repo checkout.
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let result = build_all(dir.path());
        assert!(
            result.is_err(),
            "build_all must fail when Containerfile.composed is absent"
        );
    }

    #[test]
    fn build_all_fails_fast_when_mock_epp_containerfile_missing() {
        // verify_ai_repo_source_files must fail before any Docker invocation
        // when the mock-epp Containerfile is absent from the AI repo checkout.
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        std::fs::write(dir.path().join("Containerfile.composed"), "FROM scratch")
            .unwrap_or_else(|_| std::process::abort());
        let result = build_all(dir.path());
        assert!(
            result.is_err(),
            "build_all must fail when mock-epp Containerfile is absent"
        );
    }
}
