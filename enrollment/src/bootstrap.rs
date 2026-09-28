//! The `enrollment bootstrap` subcommand.
//!
//! Mints or loads the Grid CA and issues the enrollment endpoint's serving
//! certificate, then writes them as Kubernetes Secrets for a pre-install Job.
//! Compiled only with `--features bootstrap`.
//!
//! Key separation is deliberate: the signing key lives only in the CA-key Secret
//! the enrollment service mounts, while peers receive the bundle Secret, which
//! holds the certificate and never the key. Generation is idempotent and never
//! overwrites an existing CA without `--force-regenerate`.

use std::error::Error;

use clap::Parser;

/// Boxed error that is `Send + Sync` so the async command future stays `Send`.
type BoxError = Box<dyn Error + Send + Sync>;

/// Arguments for `bootstrap`.
#[derive(Parser)]
#[command(name = "bootstrap", about = "Mint or load the Grid CA and write it as Secrets")]
struct BootstrapArgs {
    /// Namespace to read and write Secrets in. Defaults to `POD_NAMESPACE`, then
    /// `default`.
    #[arg(long)]
    namespace: Option<String>,
    /// Common name recorded on the CA certificate.
    #[arg(long, default_value = "grid-ca")]
    common_name: String,
    /// Secret holding the CA signing key (`tls.crt`, `tls.key`). Issuer-only.
    #[arg(long, default_value = "grid-ca-key")]
    ca_key_secret: String,
    /// Secret holding the public CA bundle (`ca.crt`). Never the key.
    #[arg(long, default_value = "grid-ca-bundle")]
    ca_bundle_secret: String,
    /// Secret holding the enrollment serving certificate (`tls.crt`, `tls.key`).
    #[arg(long, default_value = "enrollment-serving-tls")]
    serving_secret: String,
    /// A DNS name the serving certificate must cover. Repeatable.
    #[arg(long = "serving-dns")]
    serving_dns: Vec<String>,
    /// Secret holding the Postgres serving certificate (`tls.crt`, `tls.key`),
    /// issued from the grid CA so the service can connect with sslmode=verify-full.
    #[arg(long, default_value = "grid-db-serving-tls")]
    db_serving_secret: String,
    /// A DNS name the Postgres serving certificate must cover. Repeatable.
    #[arg(long = "db-dns")]
    db_dns: Vec<String>,
    /// Regenerate every certificate and the CA even if the Secrets exist.
    #[arg(long)]
    force_regenerate: bool,
}

/// Run the `bootstrap` subcommand.
///
/// Parses from the process arguments after the binary name, so `bootstrap`
/// sits in the argv0 slot clap ignores and the flags parse as usual.
pub(crate) async fn run() -> Result<(), BoxError> {
    let args = BootstrapArgs::parse_from(std::env::args_os().skip(1));
    bootstrap(&args).await
}

/// Generate or load the CA, then ensure the bundle and serving Secrets.
#[expect(
    clippy::large_stack_frames,
    reason = "one-shot init command; holds the large CaCert/Secret types, off any hot path"
)]
async fn bootstrap(args: &BootstrapArgs) -> Result<(), BoxError> {
    use k8s_openapi::api::core::v1::Secret;
    use kube::api::Api;

    let namespace = args
        .namespace
        .clone()
        .or_else(|| std::env::var("POD_NAMESPACE").ok())
        .unwrap_or_else(|| "default".to_owned());

    let client = kube::Client::try_default().await?;
    let secrets: Api<Secret> = Api::namespaced(client, &namespace);

    let ca = resolve_ca(&secrets, args).await?;
    write_opaque_secret(&secrets, &args.ca_bundle_secret, "ca.crt", &ca.cert_pem).await?;
    ensure_serving(&secrets, &ca, args).await?;
    ensure_db_serving(&secrets, &ca, args).await?;
    Ok(())
}

/// Load the CA from its Secret, or generate and persist a fresh one.
///
/// Load when the Secret exists and no regenerate is forced, so re-runs keep the
/// same CA. The signing key is written only to the CA-key Secret.
async fn resolve_ca(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    args: &BootstrapArgs,
) -> Result<certs::CaCert, BoxError> {
    match load_ca_material(secrets, &args.ca_key_secret).await? {
        Some((cert_pem, key_pem)) if !args.force_regenerate => {
            Ok(certs::load_ca(&args.common_name, &key_pem, &cert_pem)?)
        },
        _ => {
            let ca = certs::generate_ca(&args.common_name)?;
            write_tls_secret(
                secrets,
                &args.ca_key_secret,
                &ca.cert_pem,
                &ca.key_pem,
                args.force_regenerate,
            )
            .await?;
            Ok(ca)
        },
    }
}

/// Issue the serving certificate when it is absent or a regenerate is forced.
async fn ensure_serving(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    ca: &certs::CaCert,
    args: &BootstrapArgs,
) -> Result<(), BoxError> {
    if should_write(
        secret_exists(secrets, &args.serving_secret).await?,
        args.force_regenerate,
    ) {
        let serving = certs::generate_dns_only_cert(ca, &args.common_name, &args.serving_dns)?;
        write_tls_secret(
            secrets,
            &args.serving_secret,
            &serving.cert_pem,
            &serving.key_pem,
            args.force_regenerate,
        )
        .await?;
    }
    Ok(())
}

/// Issue the Postgres serving certificate when it is absent or a regenerate is forced.
///
/// The service connects with sslmode=verify-full, so builtin Postgres needs a
/// grid-CA-issued leaf whose SANs cover the DB Service names in `--db-dns`.
async fn ensure_db_serving(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    ca: &certs::CaCert,
    args: &BootstrapArgs,
) -> Result<(), BoxError> {
    if should_write(
        secret_exists(secrets, &args.db_serving_secret).await?,
        args.force_regenerate,
    ) {
        let serving = certs::generate_dns_only_cert(ca, "grid-enrollment-db", &args.db_dns)?;
        write_tls_secret(
            secrets,
            &args.db_serving_secret,
            &serving.cert_pem,
            &serving.key_pem,
            args.force_regenerate,
        )
        .await?;
    }
    Ok(())
}

/// The `tls.crt`/`tls.key` PEM from a Secret, or `None` if it does not exist.
///
/// Fetches the full Secret to read the key material. One-shot init, off any
/// request path.
async fn load_ca_material(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
) -> Result<Option<(String, String)>, BoxError> {
    let Some(secret) = secrets.get_opt(name).await? else {
        return Ok(None);
    };
    let data = secret.data.as_ref().ok_or("secret has no data")?;
    let cert = data.get("tls.crt").ok_or("secret missing tls.crt")?;
    let key = data.get("tls.key").ok_or("secret missing tls.key")?;
    Ok(Some((
        String::from_utf8(cert.0.clone())?,
        String::from_utf8(key.0.clone())?,
    )))
}

/// Whether a Secret exists, by metadata only, so the full object never lands on
/// the stack.
async fn secret_exists(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
) -> Result<bool, BoxError> {
    Ok(secrets.get_metadata_opt(name).await?.is_some())
}

/// Create or replace a `kubernetes.io/tls` Secret with a certificate and key.
async fn write_tls_secret(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    cert_pem: &str,
    key_pem: &str,
    force: bool,
) -> Result<(), BoxError> {
    let mut string_data = std::collections::BTreeMap::new();
    string_data.insert("tls.crt".to_owned(), cert_pem.to_owned());
    string_data.insert("tls.key".to_owned(), key_pem.to_owned());
    apply_secret(secrets, name, Some("kubernetes.io/tls"), string_data, force).await
}

/// Create or replace an `Opaque` Secret with a single key.
///
/// The bundle is public, so this always refreshes it to match the current CA.
async fn write_opaque_secret(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    key: &str,
    value: &str,
) -> Result<(), BoxError> {
    let mut string_data = std::collections::BTreeMap::new();
    string_data.insert(key.to_owned(), value.to_owned());
    apply_secret(secrets, name, None, string_data, true).await
}

/// Create the Secret, or replace it when it exists and `replace` is set.
#[expect(
    clippy::large_stack_frames,
    reason = "one-shot init command; holds the large k8s Secret type, off any hot path"
)]
async fn apply_secret(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    type_: Option<&str>,
    string_data: std::collections::BTreeMap<String, String>,
    replace: bool,
) -> Result<(), BoxError> {
    use k8s_openapi::{api::core::v1::Secret, apimachinery::pkg::apis::meta::v1::ObjectMeta};
    use kube::api::PostParams;

    // Boxed: the Secret type is large; keep it off this frame's stack.
    let secret = Box::new(Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            ..ObjectMeta::default()
        },
        type_: type_.map(ToOwned::to_owned),
        string_data: Some(string_data),
        ..Secret::default()
    });

    // Existence by metadata only, so the full object never lands on the stack.
    if secrets.get_metadata_opt(name).await?.is_some() {
        if replace {
            secrets.replace(name, &PostParams::default(), &secret).await?;
        }
    } else {
        secrets.create(&PostParams::default(), &secret).await?;
    }
    Ok(())
}

/// Whether to generate and write material: only when it is absent, or a
/// regenerate is forced. This is the idempotency invariant, so a re-run keeps the
/// existing CA and certificates rather than reissuing them.
fn should_write(exists: bool, force: bool) -> bool {
    force || !exists
}

#[cfg(test)]
mod tests {
    use super::should_write;

    #[test]
    fn writes_only_when_absent_or_forced() {
        assert!(!should_write(true, false), "an existing secret is kept, not reissued");
        assert!(should_write(false, false), "an absent secret is created");
        assert!(should_write(true, true), "a forced regenerate overwrites");
        assert!(should_write(false, true), "a forced regenerate creates when absent");
    }
}
