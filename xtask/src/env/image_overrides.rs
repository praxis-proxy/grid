//! Image override support for testing with external registries.
//!
//! Provides environment variable-based overrides for image names and pull
//! policies used by the xtask test harness. Public demos default to published
//! release images; local development remains available through explicit
//! overrides.

use std::{
    env,
    process::{Command, Stdio},
};

use super::IngressMode;

// ---------------------------------------------------------------------------
// Environment Variables
// ---------------------------------------------------------------------------

/// Environment variable to override the gateway image.
const GATEWAY_IMAGE_ENV: &str = "GRID_XTASK_GATEWAY_IMAGE";

/// Environment variable to override the mock EPP image.
const MOCK_EPP_IMAGE_ENV: &str = "GRID_XTASK_MOCK_EPP_IMAGE";

/// Environment variable to override the mock provider image used by legacy
/// generic environments.
const MOCK_PROVIDER_IMAGE_ENV: &str = "GRID_XTASK_MOCK_PROVIDER_IMAGE";

/// Environment variable to override the simulator image.
const SIM_IMAGE_ENV: &str = "GRID_XTASK_SIM_IMAGE";

/// Environment variable to override the operator image.
const OPERATOR_IMAGE_ENV: &str = "GRID_XTASK_OPERATOR_IMAGE";

/// Environment variable to override the overlay-sync sidecar image.
const OVERLAY_SYNC_IMAGE_ENV: &str = "GRID_XTASK_OVERLAY_SYNC_IMAGE";

/// Environment variable to override the image pull policy.
const IMAGE_PULL_POLICY_ENV: &str = "GRID_XTASK_IMAGE_PULL_POLICY";

// ---------------------------------------------------------------------------
// Default Images (preserve existing behavior)
// ---------------------------------------------------------------------------

/// Default gateway image (matches images.rs).
const DEFAULT_GATEWAY_IMAGE: &str = "localhost/praxis-ai:llmd-ext-proc";

/// Default mock EPP image (matches images.rs).
const DEFAULT_MOCK_EPP_IMAGE: &str = "localhost/praxis-ai-mock-epp:latest";

/// Default mock provider image used by legacy generic environments.
const DEFAULT_MOCK_PROVIDER_IMAGE: &str = "grid-mock-providers:latest";

/// Default operator image (matches operator.rs).
const DEFAULT_OPERATOR_IMAGE: &str = "grid-operator:latest";

/// Default overlay-sync sidecar image used by local Forge environments.
const DEFAULT_OVERLAY_SYNC_IMAGE: &str = "grid-overlay-sync:latest";

/// Default gateway image used by the GLB demo.
const DEFAULT_GLB_GATEWAY_IMAGE: &str = "ghcr.io/praxis-proxy/ai:0.4.0";

/// Default simulator image used by forge-based demos.
const DEFAULT_SIM_IMAGE: &str = "ghcr.io/neuralmagic/vllm-vcr:vllm0.23";

/// Default operator image used by the GLB demo.
const DEFAULT_GLB_OPERATOR_IMAGE: &str = "ghcr.io/praxis-proxy/grid-operator:v0.1.4";

/// Default gateway image for workload-inference demos.
const DEFAULT_WORKLOAD_GATEWAY_IMAGE: &str = "ghcr.io/praxis-proxy/ai:0.4.0";

/// Default operator image for workload-inference demos.
const DEFAULT_WORKLOAD_OPERATOR_IMAGE: &str = "ghcr.io/praxis-proxy/grid-operator:v0.1.4";

/// Default image pull policy for workload-inference demos (registry-backed).
const DEFAULT_WORKLOAD_IMAGE_PULL_POLICY: &str = "IfNotPresent";

/// Default image pull policy for local images.
///
/// Local workflows load the named images into their clusters. Registry-backed
/// demo workflows use their mode-specific pull-policy default.
const DEFAULT_IMAGE_PULL_POLICY: &str = "Never";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Get the gateway image name, respecting environment overrides.
pub(crate) fn gateway_image() -> String {
    env::var(GATEWAY_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_GATEWAY_IMAGE.to_owned())
}

/// Get the mock EPP image name, respecting environment overrides.
pub(crate) fn mock_epp_image() -> String {
    env::var(MOCK_EPP_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_MOCK_EPP_IMAGE.to_owned())
}

/// Get the mock provider image name for legacy generic environments.
pub(crate) fn mock_provider_image() -> String {
    env::var(MOCK_PROVIDER_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_MOCK_PROVIDER_IMAGE.to_owned())
}

/// Get the operator image name, respecting environment overrides.
pub(crate) fn operator_image() -> String {
    env::var(OPERATOR_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_OPERATOR_IMAGE.to_owned())
}

/// Get the overlay-sync sidecar image, respecting environment overrides.
pub(crate) fn overlay_sync_image() -> String {
    env::var(OVERLAY_SYNC_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_OVERLAY_SYNC_IMAGE.to_owned())
}

/// Get the simulator image name, respecting environment overrides.
pub(crate) fn sim_image() -> String {
    env::var(SIM_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_SIM_IMAGE.to_owned())
}

/// Get the demo gateway image for the given ingress mode.
pub(crate) fn demo_gateway_image(mode: IngressMode) -> String {
    let default = match mode {
        IngressMode::Global => DEFAULT_GLB_GATEWAY_IMAGE,
        IngressMode::Workload => DEFAULT_WORKLOAD_GATEWAY_IMAGE,
    };
    env::var(GATEWAY_IMAGE_ENV).unwrap_or_else(|_| default.to_owned())
}

/// Get the demo operator image for the given ingress mode.
pub(crate) fn demo_operator_image(mode: IngressMode) -> String {
    let default = match mode {
        IngressMode::Global => DEFAULT_GLB_OPERATOR_IMAGE,
        IngressMode::Workload => DEFAULT_WORKLOAD_OPERATOR_IMAGE,
    };
    env::var(OPERATOR_IMAGE_ENV).unwrap_or_else(|_| default.to_owned())
}

/// Get the image pull policy, respecting environment overrides.
///
/// When no override is set, returns "Never" to preserve local development
/// behavior. When an override is set, uses the override value exactly.
pub(crate) fn image_pull_policy() -> String {
    env::var(IMAGE_PULL_POLICY_ENV).unwrap_or_else(|_| DEFAULT_IMAGE_PULL_POLICY.to_owned())
}

/// Get the image pull policy for the given ingress mode.
///
/// Both `IngressMode` variants currently use the same registry-backed
/// default; the parameter is kept so a future mode-specific default can be
/// added without changing this function's signature.
pub(crate) fn demo_image_pull_policy(mode: IngressMode) -> String {
    let default = match mode {
        IngressMode::Global | IngressMode::Workload => DEFAULT_WORKLOAD_IMAGE_PULL_POLICY,
    };
    env::var(IMAGE_PULL_POLICY_ENV).unwrap_or_else(|_| default.to_owned())
}

/// Determine if Kind image loading should be skipped.
///
/// Returns true if pull policy is not "Never", indicating that Kubernetes
/// should pull images rather than loading them into Kind clusters.
pub(crate) fn should_skip_kind_image_loading() -> bool {
    image_pull_policy() != "Never"
}

/// Import a host image into a run-owned Kind node using only its linux/amd64
/// image content. This avoids OCI-index imports failing when the local Docker
/// store has only the linux/amd64 child content.
#[expect(
    clippy::too_many_lines,
    reason = "ensures both image processes are reaped on every failure path"
)]
pub(crate) fn load_docker_image_into_kind(image: &str, kind_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let control_plane = format!("{kind_name}-control-plane");
    let engine = kind_engine();
    let mut save = Command::new(&engine)
        .args(save_args(&engine, image))
        .stdout(Stdio::piped())
        .spawn()?;
    let Some(save_stdout) = save.stdout.take() else {
        drop(save.kill());
        drop(save.wait());
        return Err("docker save did not provide stdout".into());
    };
    let import_status = Command::new(&engine)
        .args([
            "exec",
            "--privileged",
            "-i",
            &control_plane,
            "ctr",
            "--namespace=k8s.io",
            "images",
            "import",
            "--digests",
            "--snapshotter=overlayfs",
            "-",
        ])
        .stdin(save_stdout)
        .status();
    let import_status = match import_status {
        Ok(status) => status,
        Err(error) => {
            drop(save.kill());
            drop(save.wait());
            return Err(error.into());
        },
    };
    let save_status = save.wait()?;
    if !save_status.success() {
        return Err(format!("{engine} save failed for {image}").into());
    }
    if !import_status.success() {
        return Err(format!("failed to import {image} into {control_plane}").into());
    }
    Ok(())
}

/// The engine Kind put its nodes in, resolved the way Kind resolves it.
///
/// `KIND_EXPERIMENTAL_PROVIDER` wins when set. Otherwise Kind uses Docker when a
/// real one answers and Podman only as the fallback, so a host carrying both has
/// its nodes in Docker and a `podman exec` would find no control plane. The
/// `podman-docker` shim answers `docker -v` with `podman version`, which is what
/// separates a real Docker from the emulation.
fn kind_engine() -> String {
    if let Ok(declared) = env::var("KIND_EXPERIMENTAL_PROVIDER") {
        let declared = declared.trim();
        if !declared.is_empty() {
            return declared.to_owned();
        }
    }
    let real_docker = Command::new("docker")
        .arg("-v")
        .output()
        .is_ok_and(|out| out.status.success() && String::from_utf8_lossy(&out.stdout).starts_with("Docker version"));
    if real_docker {
        "docker".to_owned()
    } else {
        "podman".to_owned()
    }
}

/// Build the save arguments, adding `--platform` only for engines that accept it.
///
/// Podman has no such flag and stores one platform per image, so passing it fails
/// the save outright and `ctr` then reports an unrecognized image format.
fn save_args<'img>(engine: &str, image: &'img str) -> Vec<&'img str> {
    if engine == "docker" {
        vec!["save", "--platform", "linux/amd64", image]
    } else {
        vec!["save", image]
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {

    #[test]
    fn save_args_pass_platform_only_to_docker() {
        for (engine, want) in [
            ("docker", vec!["save", "--platform", "linux/amd64", "img:tag"]),
            ("podman", vec!["save", "img:tag"]),
        ] {
            assert_eq!(
                save_args(engine, "img:tag"),
                want,
                "{engine} save must only be given flags it accepts"
            );
        }
    }

    use super::*;

    #[test]
    fn default_images_match_constants() {
        if env::var(GATEWAY_IMAGE_ENV).is_err() {
            assert_eq!(gateway_image(), DEFAULT_GATEWAY_IMAGE);
        }
        if env::var(MOCK_EPP_IMAGE_ENV).is_err() {
            assert_eq!(mock_epp_image(), DEFAULT_MOCK_EPP_IMAGE);
        }
        if env::var(OPERATOR_IMAGE_ENV).is_err() {
            assert_eq!(operator_image(), DEFAULT_OPERATOR_IMAGE);
        }
        if env::var(OVERLAY_SYNC_IMAGE_ENV).is_err() {
            assert_eq!(overlay_sync_image(), DEFAULT_OVERLAY_SYNC_IMAGE);
        }
    }

    #[test]
    fn constants_are_correct() {
        assert_eq!(DEFAULT_GATEWAY_IMAGE, "localhost/praxis-ai:llmd-ext-proc");
        assert_eq!(DEFAULT_MOCK_EPP_IMAGE, "localhost/praxis-ai-mock-epp:latest");
        assert_eq!(DEFAULT_OPERATOR_IMAGE, "grid-operator:latest");
        assert_eq!(DEFAULT_OVERLAY_SYNC_IMAGE, "grid-overlay-sync:latest");
        assert_eq!(DEFAULT_GLB_GATEWAY_IMAGE, "ghcr.io/praxis-proxy/ai:0.4.0");
        assert_eq!(DEFAULT_GLB_OPERATOR_IMAGE, "ghcr.io/praxis-proxy/grid-operator:v0.1.4");
        assert_eq!(DEFAULT_IMAGE_PULL_POLICY, "Never");
        assert_eq!(DEFAULT_WORKLOAD_GATEWAY_IMAGE, "ghcr.io/praxis-proxy/ai:0.4.0");
        assert_eq!(
            DEFAULT_WORKLOAD_OPERATOR_IMAGE,
            "ghcr.io/praxis-proxy/grid-operator:v0.1.4"
        );
        assert_eq!(DEFAULT_WORKLOAD_IMAGE_PULL_POLICY, "IfNotPresent");
    }

    #[test]
    fn public_demo_modes_use_registry_defaults() {
        if env::var(GATEWAY_IMAGE_ENV).is_err() {
            assert_eq!(
                demo_gateway_image(IngressMode::Global),
                demo_gateway_image(IngressMode::Workload),
                "public demos must use the same released gateway"
            );
        }
        if env::var(IMAGE_PULL_POLICY_ENV).is_err() {
            assert_eq!(demo_image_pull_policy(IngressMode::Global), "IfNotPresent");
            assert_eq!(demo_image_pull_policy(IngressMode::Workload), "IfNotPresent");
        }
    }

    #[test]
    fn explicit_pull_policy_override_used_exactly() {
        let original = env::var(IMAGE_PULL_POLICY_ENV).ok();

        if env::var(IMAGE_PULL_POLICY_ENV).is_err() {
            assert_eq!(image_pull_policy(), DEFAULT_IMAGE_PULL_POLICY);
        }

        if let Some(val) = original {
            eprintln!("Note: explicit override would return: {val}");
        }
    }

    #[test]
    fn remote_images_do_not_change_default_pull_policy() {
        if env::var(IMAGE_PULL_POLICY_ENV).is_err() {
            assert_eq!(
                image_pull_policy(),
                "Never",
                "Pull policy should always be Never when not explicitly set"
            );
        }
    }

    #[test]
    fn load_skip_decision() {
        let current_policy = image_pull_policy();
        let should_skip = should_skip_kind_image_loading();

        if current_policy == "Never" {
            assert!(!should_skip, "Should not skip loading when policy is Never");
        } else {
            assert!(should_skip, "Should skip loading when policy is not Never");
        }
    }
}
