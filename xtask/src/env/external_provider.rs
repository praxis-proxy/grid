//! External inference provider descriptor and key-file validation.

use std::{fs, path::Path};

use super::ExternalProvider;

/// Maximum key-file size in bytes.
const MAX_KEY_FILE_BYTES: u64 = 4096;

/// Authentication strategy for an external provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthStrategy {
    /// Bearer token injected via the `Authorization` header.
    BearerToken,
}

/// Descriptor encapsulating the configuration for one external provider.
#[derive(Debug, Clone)]
pub(crate) struct ExternalProviderDescriptor {
    /// Provider variant (used by test assertions and future provider dispatch).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "read by unit tests and reserved for future provider dispatch")
    )]
    pub(crate) kind: ExternalProvider,
    /// `InferenceProvider` `providerKind` field.
    pub(crate) provider_kind: &'static str,
    /// `InferenceProvider` `backendKind` field.
    pub(crate) backend_kind: &'static str,
    /// Public hostname of the external API.
    pub(crate) hostname: &'static str,
    /// Port for the external API.
    pub(crate) port: u16,
    /// TLS SNI value.
    pub(crate) sni: &'static str,
    /// API paths authorized for this provider.
    pub(crate) allowed_paths: &'static [&'static str],
    /// User-supplied model name.
    pub(crate) model: String,
    /// Kubernetes Secret name for the API credential.
    pub(crate) secret_name: &'static str,
    /// Key within the Kubernetes Secret.
    pub(crate) secret_key: &'static str,
    /// Mount path for the credential Secret in the gateway pod.
    pub(crate) mount_path: &'static str,
    /// Routing cluster identity in the overlay and provider config.
    pub(crate) routing_cluster: &'static str,
    /// Authentication strategy (used by test assertions and future credential-inject dispatch).
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "read by unit tests and reserved for future auth-strategy dispatch"
        )
    )]
    pub(crate) auth_strategy: AuthStrategy,
}

impl ExternalProviderDescriptor {
    /// Create the descriptor for `OpenAI` with the user-supplied model.
    pub(crate) fn openai(model: &str) -> Self {
        Self {
            kind: ExternalProvider::OpenAi,
            provider_kind: "open_ai",
            backend_kind: "api_provider",
            hostname: "api.openai.com",
            port: 443,
            sni: "api.openai.com",
            allowed_paths: &["/v1/responses"],
            model: model.to_owned(),
            secret_name: "openai-api-key",
            secret_key: "token",
            mount_path: "/etc/praxis/credentials/openai",
            routing_cluster: "openai-api",
            auth_strategy: AuthStrategy::BearerToken,
        }
    }

    /// Return the `endpoint:port` string for the upstream cluster.
    pub(crate) fn endpoint(&self) -> String {
        format!("{}:{}", self.hostname, self.port)
    }

    /// Return the RFC 1123-compatible name used for Kubernetes resources.
    pub(crate) fn resource_name(&self) -> String {
        format!("external-{}", self.provider_kind.replace('_', "-"))
    }

    /// Return the credential file path inside the gateway pod.
    pub(crate) fn credential_file(&self) -> String {
        format!("{}/{}", self.mount_path, self.secret_key)
    }
}

/// Validate that a key file meets the security requirements for use as an
/// external provider credential.
///
/// The file must:
/// - exist and be a regular file (not a directory, device, or pipe);
/// - not be a symbolic link;
/// - be outside the repository working tree;
/// - not be group- or world-readable (Unix only);
/// - be nonempty after trimming one trailing newline; and
/// - not exceed [`MAX_KEY_FILE_BYTES`].
///
/// This function never reads, prints, logs, or returns the file content.
#[expect(
    clippy::too_many_lines,
    reason = "validation steps are sequential and would be less clear if split"
)]
pub(crate) fn validate_key_file(path: &Path, workspace_root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let meta = path
        .symlink_metadata()
        .map_err(|e| format!("cannot read key file metadata: {e}"))?;

    if meta.file_type().is_symlink() {
        return Err("key file must not be a symbolic link".into());
    }
    if !meta.file_type().is_file() {
        return Err("key file must be a regular file".into());
    }

    let canonical = path
        .canonicalize()
        .map_err(|e| format!("cannot resolve key file path: {e}"))?;
    let canonical_root = workspace_root
        .canonicalize()
        .map_err(|e| format!("cannot resolve workspace root: {e}"))?;
    if canonical.starts_with(&canonical_root) {
        return Err("key file must be outside the repository".into());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(format!("key file must not be group- or world-readable (mode {mode:04o})").into());
        }
    }

    let size = meta.len();
    if size == 0 {
        return Err("key file must not be empty".into());
    }
    if size > MAX_KEY_FILE_BYTES {
        return Err(format!("key file exceeds maximum size ({size} > {MAX_KEY_FILE_BYTES} bytes)").into());
    }

    // Verify the file is not effectively empty (only whitespace/newline).
    let content = fs::read(path).map_err(|e| format!("cannot read key file: {e}"))?;
    let trimmed = if content.last() == Some(&b'\n') {
        content.get(..content.len() - 1).unwrap_or(&content)
    } else {
        &content
    };
    if trimmed.is_empty() {
        return Err("key file is empty after trimming trailing newline".into());
    }

    Ok(())
}

/// Resolve CLI options into an [`ExternalProviderDescriptor`] or validate that
/// no external provider is requested.
///
/// Returns `Ok(None)` when no external provider is selected. Returns an error
/// when the option combination is invalid (e.g., provider selected without a
/// key file or model).
pub(crate) fn resolve_external_provider(
    provider: Option<ExternalProvider>,
    key_file: Option<&Path>,
    model: Option<&str>,
) -> Result<Option<ExternalProviderDescriptor>, Box<dyn std::error::Error>> {
    let Some(provider) = provider else {
        if key_file.is_some() || model.is_some() {
            return Err(
                "--external-provider-key-file and --external-provider-model require --external-provider".into(),
            );
        }
        return Ok(None);
    };

    let key_file = key_file.ok_or("--external-provider openai requires --external-provider-key-file")?;
    let model = model.ok_or("--external-provider openai requires --external-provider-model")?;
    if model.is_empty() {
        return Err("--external-provider-model must not be empty".into());
    }

    match provider {
        ExternalProvider::OpenAi => {
            let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap_or_else(|| Path::new("."));
            validate_key_file(key_file, workspace_root)?;
            Ok(Some(ExternalProviderDescriptor::openai(model)))
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[expect(clippy::allow_attributes, reason = "blanket test lint suppression")]
    #[allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        reason = "tests"
    )]
    mod inner {
        use std::os::unix::fs::PermissionsExt as _;

        use super::*;

        #[test]
        fn openai_descriptor_fields() {
            let desc = ExternalProviderDescriptor::openai("gpt-5-mini");
            assert_eq!(desc.kind, ExternalProvider::OpenAi);
            assert_eq!(desc.provider_kind, "open_ai");
            assert_eq!(desc.resource_name(), "external-open-ai");
            assert_eq!(desc.backend_kind, "api_provider");
            assert_eq!(desc.hostname, "api.openai.com");
            assert_eq!(desc.port, 443);
            assert_eq!(desc.sni, "api.openai.com");
            assert_eq!(desc.allowed_paths, &["/v1/responses"]);
            assert_eq!(desc.model, "gpt-5-mini");
            assert_eq!(desc.secret_name, "openai-api-key");
            assert_eq!(desc.secret_key, "token");
            assert_eq!(desc.mount_path, "/etc/praxis/credentials/openai");
            assert_eq!(desc.routing_cluster, "openai-api");
            assert_eq!(desc.auth_strategy, AuthStrategy::BearerToken);
        }

        #[test]
        fn openai_endpoint_format() {
            let desc = ExternalProviderDescriptor::openai("gpt-5-mini");
            assert_eq!(desc.endpoint(), "api.openai.com:443");
        }

        #[test]
        fn openai_credential_file_path() {
            let desc = ExternalProviderDescriptor::openai("gpt-5-mini");
            assert_eq!(desc.credential_file(), "/etc/praxis/credentials/openai/token");
        }

        #[test]
        fn resolve_none_when_not_requested() {
            let result = resolve_external_provider(None, None, None);
            assert!(result.unwrap().is_none());
        }

        #[test]
        fn resolve_rejects_key_without_provider() {
            let result = resolve_external_provider(None, Some(Path::new("/tmp/key")), None);
            assert!(
                matches!(&result, Err(err) if err.to_string().contains("--external-provider")),
                "must mention --external-provider"
            );
        }

        #[test]
        fn resolve_rejects_model_without_provider() {
            let result = resolve_external_provider(None, None, Some("gpt-5-mini"));
            assert!(result.is_err(), "model without provider must fail");
        }

        #[test]
        fn resolve_rejects_provider_without_key() {
            let result = resolve_external_provider(Some(ExternalProvider::OpenAi), None, Some("gpt-5-mini"));
            assert!(
                matches!(&result, Err(err) if err.to_string().contains("key-file")),
                "must mention key-file"
            );
        }

        #[test]
        fn resolve_rejects_provider_without_model() {
            let result = resolve_external_provider(Some(ExternalProvider::OpenAi), Some(Path::new("/tmp/key")), None);
            assert!(
                matches!(&result, Err(err) if err.to_string().contains("model")),
                "must mention model"
            );
        }

        #[test]
        fn resolve_rejects_empty_model() {
            let result =
                resolve_external_provider(Some(ExternalProvider::OpenAi), Some(Path::new("/tmp/key")), Some(""));
            assert!(result.is_err(), "empty model must fail");
        }

        #[test]
        fn validate_rejects_missing_file() {
            let result = validate_key_file(Path::new("/nonexistent/key-file"), Path::new("/tmp"));
            assert!(result.is_err());
        }

        #[test]
        fn validate_rejects_directory() {
            let dir = tempfile::tempdir().unwrap();
            let result = validate_key_file(dir.path(), Path::new("/tmp"));
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("regular file"));
        }

        #[test]
        fn validate_rejects_symlink() {
            let dir = tempfile::tempdir().unwrap();
            let real = dir.path().join("real-key");
            fs::write(&real, "sk-test-key-value\n").unwrap();
            let link = dir.path().join("link-key");
            std::os::unix::fs::symlink(&real, &link).unwrap();
            let result = validate_key_file(&link, Path::new("/tmp"));
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("symbolic link"));
        }

        #[test]
        fn validate_rejects_empty_file() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("empty-key");
            fs::write(&path, "").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            let repo = tempfile::tempdir().unwrap();
            let result = validate_key_file(&path, repo.path());
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("empty"));
        }

        #[test]
        fn validate_rejects_newline_only_file() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("newline-key");
            fs::write(&path, "\n").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            let repo = tempfile::tempdir().unwrap();
            let result = validate_key_file(&path, repo.path());
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("empty"));
        }

        #[test]
        fn validate_rejects_oversized_file() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("big-key");
            fs::write(
                &path,
                vec![b'x'; usize::try_from(MAX_KEY_FILE_BYTES + 1).unwrap_or_else(|_| std::process::abort())],
            )
            .unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            let repo = tempfile::tempdir().unwrap();
            let result = validate_key_file(&path, repo.path());
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("maximum size"));
        }

        #[test]
        fn validate_rejects_world_readable() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("world-key");
            fs::write(&path, "sk-test-key\n").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
            let repo = tempfile::tempdir().unwrap();
            let result = validate_key_file(&path, repo.path());
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("world-readable"));
        }

        #[test]
        fn validate_rejects_group_readable() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("group-key");
            fs::write(&path, "sk-test-key\n").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
            let repo = tempfile::tempdir().unwrap();
            let result = validate_key_file(&path, repo.path());
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("world-readable"));
        }

        #[test]
        fn validate_rejects_file_inside_repo() {
            let dir = tempfile::tempdir().unwrap();
            let nested = dir.path().join("repo").join("secrets");
            fs::create_dir_all(&nested).unwrap();
            let path = nested.join("key");
            fs::write(&path, "sk-test-key\n").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            let result = validate_key_file(&path, dir.path().join("repo").as_path());
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("outside the repository"));
        }

        #[test]
        fn validate_accepts_valid_key_file() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("valid-key");
            fs::write(&path, "sk-test-key-value\n").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            // workspace root is a *different* directory
            let repo = tempfile::tempdir().unwrap();
            let result = validate_key_file(&path, repo.path());
            assert!(result.is_ok(), "expected Ok, got: {result:?}");
        }

        #[test]
        fn validate_accepts_key_without_trailing_newline() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("key-no-newline");
            fs::write(&path, "sk-test-key-value").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            let repo = tempfile::tempdir().unwrap();
            let result = validate_key_file(&path, repo.path());
            assert!(result.is_ok(), "expected Ok, got: {result:?}");
        }
    }
}
