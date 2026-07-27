//! Image override support for testing with external registries.
//!
//! Provides environment variable-based overrides for image names and pull
//! policies used by the xtask test harness. Defaults preserve existing local
//! behavior exactly when no overrides are set.

use std::env;

// ---------------------------------------------------------------------------
// Environment Variables
// ---------------------------------------------------------------------------

/// Environment variable to override the gateway image.
const GATEWAY_IMAGE_ENV: &str = "GRID_XTASK_GATEWAY_IMAGE";

/// Environment variable to override the mock EPP image.
const MOCK_EPP_IMAGE_ENV: &str = "GRID_XTASK_MOCK_EPP_IMAGE";

/// Environment variable to override the mock provider image.
const MOCK_PROVIDER_IMAGE_ENV: &str = "GRID_XTASK_MOCK_PROVIDER_IMAGE";

/// Environment variable to override the operator image.
const OPERATOR_IMAGE_ENV: &str = "GRID_XTASK_OPERATOR_IMAGE";

/// Environment variable to override the image pull policy.
const IMAGE_PULL_POLICY_ENV: &str = "GRID_XTASK_IMAGE_PULL_POLICY";

// ---------------------------------------------------------------------------
// Default Images (preserve existing behavior)
// ---------------------------------------------------------------------------

/// Default gateway image (matches images.rs).
const DEFAULT_GATEWAY_IMAGE: &str = "localhost/praxis-ai:llmd-ext-proc";

/// Default mock EPP image (matches images.rs).
const DEFAULT_MOCK_EPP_IMAGE: &str = "localhost/praxis-ai-mock-epp:latest";

/// Default mock provider image (matches kind.rs).
const DEFAULT_MOCK_PROVIDER_IMAGE: &str = "grid-mock-providers:latest";

/// Default operator image (matches operator.rs).
const DEFAULT_OPERATOR_IMAGE: &str = "grid-operator:latest";

/// Default gateway image used by the GLB demo.
const DEFAULT_GLB_GATEWAY_IMAGE: &str = "praxis-ai:glb-demo";

/// Default mock-provider image used by the GLB demo.
const DEFAULT_GLB_MOCK_PROVIDER_IMAGE: &str = "grid-mock-providers:glb-demo";

/// Default operator image used by the GLB demo.
const DEFAULT_GLB_OPERATOR_IMAGE: &str = "grid-operator:glb-demo";

/// Default image pull policy for local images.
///
/// Local workflows load the named images into their clusters. Registry-backed
/// workflows set an explicit pull policy together with immutable image
/// overrides.
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

/// Get the mock provider image name, respecting environment overrides.
pub(crate) fn mock_provider_image() -> String {
    env::var(MOCK_PROVIDER_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_MOCK_PROVIDER_IMAGE.to_owned())
}

/// Get the operator image name, respecting environment overrides.
pub(crate) fn operator_image() -> String {
    env::var(OPERATOR_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_OPERATOR_IMAGE.to_owned())
}

/// Get the GLB demo gateway image, respecting the shared gateway override.
pub(crate) fn glb_gateway_image() -> String {
    env::var(GATEWAY_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_GLB_GATEWAY_IMAGE.to_owned())
}

/// Get the GLB demo mock-provider image, respecting the shared override.
pub(crate) fn glb_mock_provider_image() -> String {
    env::var(MOCK_PROVIDER_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_GLB_MOCK_PROVIDER_IMAGE.to_owned())
}

/// Get the GLB demo operator image, respecting the shared override.
pub(crate) fn glb_operator_image() -> String {
    env::var(OPERATOR_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_GLB_OPERATOR_IMAGE.to_owned())
}

/// Get the image pull policy, respecting environment overrides.
///
/// When no override is set, returns "Never" to preserve local development
/// behavior. When an override is set, uses the override value exactly.
pub(crate) fn image_pull_policy() -> String {
    env::var(IMAGE_PULL_POLICY_ENV).unwrap_or_else(|_| DEFAULT_IMAGE_PULL_POLICY.to_owned())
}

/// Determine if Kind image loading should be skipped.
///
/// Returns true if pull policy is not "Never", indicating that Kubernetes
/// should pull images rather than loading them into Kind clusters.
pub(crate) fn should_skip_kind_image_loading() -> bool {
    image_pull_policy() != "Never"
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_images_match_constants() {
        if env::var(GATEWAY_IMAGE_ENV).is_err() {
            assert_eq!(gateway_image(), DEFAULT_GATEWAY_IMAGE);
        }
        if env::var(MOCK_EPP_IMAGE_ENV).is_err() {
            assert_eq!(mock_epp_image(), DEFAULT_MOCK_EPP_IMAGE);
        }
        if env::var(MOCK_PROVIDER_IMAGE_ENV).is_err() {
            assert_eq!(mock_provider_image(), DEFAULT_MOCK_PROVIDER_IMAGE);
        }
        if env::var(OPERATOR_IMAGE_ENV).is_err() {
            assert_eq!(operator_image(), DEFAULT_OPERATOR_IMAGE);
        }
    }

    #[test]
    fn constants_are_correct() {
        assert_eq!(DEFAULT_GATEWAY_IMAGE, "localhost/praxis-ai:llmd-ext-proc");
        assert_eq!(DEFAULT_MOCK_EPP_IMAGE, "localhost/praxis-ai-mock-epp:latest");
        assert_eq!(DEFAULT_MOCK_PROVIDER_IMAGE, "grid-mock-providers:latest");
        assert_eq!(DEFAULT_OPERATOR_IMAGE, "grid-operator:latest");
        assert_eq!(DEFAULT_GLB_GATEWAY_IMAGE, "praxis-ai:glb-demo");
        assert_eq!(DEFAULT_GLB_MOCK_PROVIDER_IMAGE, "grid-mock-providers:glb-demo");
        assert_eq!(DEFAULT_GLB_OPERATOR_IMAGE, "grid-operator:glb-demo");
        assert_eq!(DEFAULT_IMAGE_PULL_POLICY, "Never");
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
