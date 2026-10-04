//! The `enrollment bootstrap` subcommand.
//!
//! Mints or loads the Grid CA and issues the enrollment endpoint's serving
//! certificate, then writes them as Kubernetes Secrets for a pre-install Job.
//! Also creates the builtin Postgres credentials and the local grid-admin token
//! table once, so the chart renders the same on every run. With `--site-name` it
//! issues that site's grid identity straight from the CA, for a hub that hosts
//! enrollment and so cannot enroll itself.
//! Compiled with the `bootstrap` feature, on by default.
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
#[expect(clippy::struct_excessive_bools, reason = "independent clap switches")]
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
    /// Leave the serving certificate alone: a user-provided Secret serves instead.
    #[arg(long)]
    skip_serving: bool,
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
    /// Builtin Postgres Deployment to roll when its serving certificate is
    /// re-issued, since Postgres reads the certificate only at start.
    #[arg(long)]
    db_deployment: Option<String>,
    /// Regenerate every certificate and the CA even if the Secrets exist.
    #[arg(long)]
    force_regenerate: bool,
    /// Leave the CA and every certificate alone: a provided CA serves instead.
    #[arg(long)]
    skip_ca: bool,
    /// Secret for the builtin Postgres credentials, created once with a generated password.
    #[arg(long)]
    db_credentials_secret: Option<String>,
    /// Postgres user in the generated connection URL.
    #[arg(long, default_value = "enrollment")]
    db_user: String,
    /// Postgres database in the generated connection URL.
    #[arg(long, default_value = "enrollment")]
    db_database: String,
    /// Postgres host in the generated connection URL.
    #[arg(long, default_value = "grid-enrollment-db")]
    db_host: String,
    /// CA bundle path the service verifies Postgres against.
    #[arg(long, default_value = "/etc/grid-ca-bundle/ca.crt")]
    db_ca_path: String,
    /// Secret for the local grid-admin token table, created once with a generated token.
    #[arg(long)]
    admin_tokens_secret: Option<String>,
    /// Site to issue a grid identity for, as enrollment would, created once.
    #[arg(long)]
    site_name: Option<String>,
    /// Namespace the site identity and its CA Secret go to.
    #[arg(long, default_value = "grid")]
    site_namespace: String,
    /// Secret for the site identity (`tls.crt`, `tls.key`).
    #[arg(long, default_value = "grid-site-identity")]
    site_secret: String,
    /// Secret for the grid CA (`ca.crt`) beside the site identity.
    #[arg(long, default_value = "grid-ca")]
    site_ca_secret: String,
    /// Secret for the grid's SWIM key (`key`, 32 bytes), created once and copied to
    /// `--site-namespace` when `--site-name` is set.
    #[arg(long)]
    swim_key_secret: Option<String>,
}

/// Run the `bootstrap` subcommand.
///
/// Parses from the process arguments after the binary name, so `bootstrap`
/// sits in the argv0 slot clap ignores and the flags parse as usual.
pub(crate) async fn run() -> Result<(), BoxError> {
    let args = BootstrapArgs::parse_from(std::env::args_os().skip(1));
    Box::pin(bootstrap(&args)).await
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

    check_site_args(args)?;
    let client = kube::Client::try_default().await?;
    let secrets: Api<Secret> = Api::namespaced(client.clone(), &namespace);

    ensure_credentials(&secrets, args).await?;
    Box::pin(ensure_swim_key(&client, &secrets, args)).await?;
    if args.skip_ca {
        return Ok(());
    }
    let ca = resolve_ca(&secrets, args).await?;
    write_opaque_secret(&secrets, &args.ca_bundle_secret, "ca.crt", &ca.cert_pem).await?;
    Box::pin(ensure_site_identity(&client, &ca, args)).await?;
    if !args.skip_serving {
        ensure_serving(&secrets, &ca, args).await?;
    }
    if let (Some(fingerprint), Some(deployment)) = (
        ensure_db_serving(&secrets, &ca, args).await?,
        args.db_deployment.as_deref(),
    ) {
        Box::pin(reconcile_db_roll(client, &namespace, deployment, &fingerprint)).await?;
    }
    Ok(())
}

/// Length of a SWIM key, the operator's `SwimKey`.
const SWIM_KEY_LEN: usize = 32;

/// Create the SWIM key once, then copy that same key beside the site identity.
async fn ensure_swim_key(
    client: &kube::Client,
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    args: &BootstrapArgs,
) -> Result<(), BoxError> {
    let Some(name) = args.swim_key_secret.as_deref() else {
        return Ok(());
    };
    let fresh = zeroize::Zeroizing::new(enrollment::api::random_bytes(SWIM_KEY_LEN)?);
    let key = match Box::pin(create_key_secret(secrets, name, &fresh)).await? {
        Some(existing) => existing,
        None => fresh,
    };
    if key.len() != SWIM_KEY_LEN {
        return Err(format!(
            "Secret {name} holds a SWIM key of {} bytes, not {SWIM_KEY_LEN}",
            key.len()
        )
        .into());
    }
    if args.site_name.is_some() {
        let site_secrets = kube::api::Api::namespaced(client.clone(), &args.site_namespace);
        if let Some(copy) = Box::pin(create_key_secret(&site_secrets, name, &key)).await?
            && copy != key
        {
            return Err(format!(
                "Secret {name} in {} holds a different SWIM key; delete one so both namespaces share it",
                args.site_namespace
            )
            .into());
        }
    }
    Ok(())
}

/// Create `name` with `key` unless it exists, returning the key it already held.
async fn create_key_secret(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    key: &[u8],
) -> Result<Option<zeroize::Zeroizing<Vec<u8>>>, BoxError> {
    use k8s_openapi::{ByteString, api::core::v1::Secret, apimachinery::pkg::apis::meta::v1::ObjectMeta};
    use kube::api::PostParams;

    let secret = Box::new(Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            labels: Some(std::collections::BTreeMap::from([(
                "app.kubernetes.io/managed-by".to_owned(),
                MANAGED_BY.to_owned(),
            )])),
            ..ObjectMeta::default()
        },
        type_: Some("Opaque".to_owned()),
        data: Some(std::collections::BTreeMap::from([(
            "key".to_owned(),
            ByteString(key.to_vec()),
        )])),
        ..Secret::default()
    });
    match secrets.create(&PostParams::default(), &secret).await {
        Ok(_) => Ok(None),
        Err(kube::Error::Api(response)) if response.code == 409 => Ok(Some(Box::pin(held_key(secrets, name)).await?)),
        Err(error) => Err(error.into()),
    }
}

/// The `key` an existing Secret holds, zeroizing whatever else it carries.
async fn held_key(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
) -> Result<zeroize::Zeroizing<Vec<u8>>, BoxError> {
    let mut data = secrets.get(name).await?.data.unwrap_or_default();
    let held = data.remove("key").map(|bytes| bytes.0).unwrap_or_default();
    for other in data.values_mut() {
        zeroize::Zeroize::zeroize(&mut other.0);
    }
    Ok(zeroize::Zeroizing::new(held))
}

/// Refuse a bad `--site-name` before anything is written.
fn check_site_args(args: &BootstrapArgs) -> Result<(), BoxError> {
    let Some(site) = &args.site_name else { return Ok(()) };
    certs::validate_site_name(site)?;
    if args.skip_ca {
        return Err("--site-name needs the bootstrap CA, not --skip-ca".into());
    }
    Ok(())
}

/// Create the builtin Postgres credentials and the grid-admin token table, if asked and absent.
async fn ensure_credentials(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    args: &BootstrapArgs,
) -> Result<(), BoxError> {
    if let Some(name) = &args.db_credentials_secret {
        let password = enrollment::api::random_hex(16)?;
        create_if_absent(secrets, name, db_credentials(args, &password)).await?;
    }
    if let Some(name) = &args.admin_tokens_secret {
        let token = enrollment::api::random_hex(20)?;
        create_if_absent(secrets, name, admin_tokens(&token)).await?;
    }
    Ok(())
}

/// Builtin Postgres credentials: the password and the verify-full connection URL.
fn db_credentials(args: &BootstrapArgs, password: &str) -> std::collections::BTreeMap<String, String> {
    let url = format!(
        "postgres://{}:{password}@{}:5432/{}?sslmode=verify-full&sslrootcert={}",
        args.db_user, args.db_host, args.db_database, args.db_ca_path
    );
    std::collections::BTreeMap::from([
        ("password".to_owned(), password.to_owned()),
        ("DB_CONNECTION_URL".to_owned(), url),
    ])
}

/// A one-line grid-admin token table.
fn admin_tokens(token: &str) -> std::collections::BTreeMap<String, String> {
    std::collections::BTreeMap::from([("tokens".to_owned(), format!("admin:{token}\n"))])
}

/// Argo CD sync options that keep a Secret no manifest renders.
const ARGO_KEEP: &str = "Prune=false,Delete=false";

/// Create an `Opaque` Secret unless one exists, then mark it so Argo CD never
/// prunes it. An existing Secret is never rotated, whoever wrote it.
async fn create_if_absent(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    string_data: std::collections::BTreeMap<String, String>,
) -> Result<(), BoxError> {
    use kube::api::{Patch, PatchParams};

    match apply_secret(secrets, name, Some("Opaque"), string_data, false).await {
        Err(error) if is_conflict(error.as_ref()) => {},
        result => result?,
    }
    secrets
        .patch_metadata(name, &PatchParams::default(), &Patch::Merge(argo_keep_patch()))
        .await?;
    Ok(())
}

/// The merge patch that marks a Secret for Argo CD to keep.
fn argo_keep_patch() -> serde_json::Value {
    serde_json::json!({ "metadata": { "annotations": { "argocd.argoproj.io/sync-options": ARGO_KEEP } } })
}

/// Whether `error` is a create that lost a race to another writer.
fn is_conflict(error: &(dyn Error + Send + Sync + 'static)) -> bool {
    matches!(error.downcast_ref::<kube::Error>(), Some(kube::Error::Api(response)) if response.code == 409)
}

/// Issue the `--site-name` identity from the CA through the enrollment signing
/// path, unless one exists.
///
/// Peers pin the leaf's digest, so an existing identity is kept, even an expired
/// or unchained one, unless `--force-regenerate` is set.
async fn ensure_site_identity(client: &kube::Client, ca: &certs::CaCert, args: &BootstrapArgs) -> Result<(), BoxError> {
    let Some(site) = args.site_name.as_deref() else {
        return Ok(());
    };
    let secrets = &kube::api::Api::namespaced(client.clone(), &args.site_namespace);
    Box::pin(ensure_site_ca(
        secrets,
        &args.site_ca_secret,
        &ca.cert_pem,
        args.force_regenerate,
    ))
    .await?;
    if !args.force_regenerate && Box::pin(kept_site_identity(secrets, ca, site, &args.site_secret)).await? {
        return Ok(());
    }
    let (issued, key_pem) = issue_site_identity(ca, site, crate::load_cert_lifetime())?;
    Box::pin(write_tls_secret(
        secrets,
        &args.site_secret,
        &issued.cert_pem,
        &key_pem,
        args.force_regenerate,
    ))
    .await?;
    log_issued(&issued, &args.site_secret);
    Ok(())
}

/// Whether a site identity exists. One issued by this CA for `site` is kept, even
/// expired, since re-issuing changes the pinned digest. Any other is refused.
async fn kept_site_identity(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    ca: &certs::CaCert,
    site: &str,
    name: &str,
) -> Result<bool, BoxError> {
    let Some(cert_pem) = secret_text(secrets, name, "tls.crt").await? else {
        return Ok(false);
    };
    existing_identity_kept(&ca.cert_pem, &cert_pem, site)
        .map_err(|reason| format!("Secret {name}: {reason}; delete it or set --force-regenerate to re-issue"))?;
    Ok(true)
}

/// Keep an existing identity only if this CA issued it for `site`.
fn existing_identity_kept(ca_cert_pem: &str, cert_pem: &str, site: &str) -> Result<(), String> {
    match certs::verify_site_cert(ca_cert_pem, cert_pem, site) {
        Ok(_) => Ok(()),
        // Validity is checked after issuer and signature, so only the name is left to check.
        Err(certs::VerifyError::NotCurrentlyValid) if names_site(cert_pem, site) => {
            tracing::warn!(site, "keeping a site identity outside its validity period");
            Ok(())
        },
        Err(reason) => Err(format!(
            "it holds an identity that is not {site} from this grid CA ({reason})"
        )),
    }
}

/// Whether the certificate's primary DNS name is `site`'s.
fn names_site(cert_pem: &str, site: &str) -> bool {
    let primary = format!("{site}.{}", certs::SPIFFE_TRUST_DOMAIN);
    certs::cert_dns_sans(cert_pem).is_ok_and(|names| names.contains(&primary))
}

/// Record an issued identity by name and key digest, the audit an enrollment row would hold.
fn log_issued(issued: &certs::EnrolledCert, secret: &str) {
    tracing::info!(
        spiffe_id = %issued.spiffe_id,
        public_key_sha256 = %issued.public_key_sha256,
        secret,
        "issued the site identity"
    );
}

/// A fresh key and the leaf the enrollment service would sign for it.
fn issue_site_identity(
    ca: &certs::CaCert,
    site: &str,
    lifetime: time::Duration,
) -> Result<(certs::EnrolledCert, zeroize::Zeroizing<String>), BoxError> {
    let certs::GeneratedCsr { csr_pem, key_pem } = certs::generate_csr(site)?;
    let issued = certs::sign_csr(ca, site, &csr_pem, certs::Validity::starting_now(lifetime))?;
    Ok((issued, key_pem))
}

/// Create the site's grid CA Secret, refusing one that holds another CA.
async fn ensure_site_ca(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    ca_cert_pem: &str,
    force: bool,
) -> Result<(), BoxError> {
    match Box::pin(secret_text(secrets, name, "ca.crt")).await? {
        Some(bundle) if !force && !certs::bundle_within(ca_cert_pem, &bundle).unwrap_or(false) => Err(format!(
            "Secret {name} holds a different grid CA; refusing to issue a site identity it would not anchor"
        )
        .into()),
        Some(_) if !force => Ok(()),
        _ => Box::pin(write_opaque_secret(secrets, name, "ca.crt", ca_cert_pem)).await,
    }
}

/// One UTF-8 value of a Secret, or `None` when the Secret or key is absent.
async fn secret_text(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    key: &str,
) -> Result<Option<String>, BoxError> {
    let Some(secret) = secrets.get_opt(name).await? else {
        return Ok(None);
    };
    let mut data = secret.data.unwrap_or_default();
    let value = data.remove(key);
    for other in data.values_mut() {
        zeroize::Zeroize::zeroize(&mut other.0);
    }
    Ok(value.map(|bytes| String::from_utf8(bytes.0)).transpose()?)
}

/// Load the CA from its Secret, or generate and persist a fresh one.
///
/// Load when the Secret exists and no regenerate is forced, so re-runs keep the
/// same CA. The signing key is written only to the CA-key Secret.
async fn resolve_ca(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    args: &BootstrapArgs,
) -> Result<certs::CaCert, BoxError> {
    match load_tls_material(secrets, &args.ca_key_secret).await? {
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

/// The `app.kubernetes.io/managed-by` value on every Secret bootstrap writes.
const MANAGED_BY: &str = "grid-enrollment-bootstrap";

/// Another manager's claim on a Secret: an owner reference (External Secrets,
/// Sealed Secrets, an operator), a different `managed-by` label, or cert-manager
/// annotations. An unlabelled Secret counts as ours, since earlier bootstrap runs
/// wrote it without the label.
fn foreign_manager(meta: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta) -> Option<String> {
    if let Some(owner) = meta.owner_references.as_deref().and_then(<[_]>::first) {
        return Some(format!("its owner {} {}", owner.kind, owner.name));
    }
    if let Some(owner) = meta
        .labels
        .as_ref()
        .and_then(|labels| labels.get("app.kubernetes.io/managed-by"))
        && owner != MANAGED_BY
    {
        return Some(format!("app.kubernetes.io/managed-by={owner}"));
    }
    meta.annotations
        .as_ref()
        .is_some_and(|annotations| annotations.keys().any(|key| key.starts_with("cert-manager.io/")))
        .then(|| "cert-manager".to_owned())
}

/// The serving certificate a Secret holds, as far as re-issue is concerned.
enum ServingCert {
    /// No Secret by that name.
    Absent,
    /// The Secret exists without a UTF-8 `tls.crt` or without a `tls.key`.
    Unusable,
    /// The Secret's `tls.crt` PEM.
    Pem(String),
}

/// Read the serving certificate and any other manager's claim on its Secret. API
/// errors propagate, so a transient failure fails the Job instead of overwriting
/// the Secret.
async fn load_serving_cert(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
) -> Result<(ServingCert, Option<String>), BoxError> {
    let Some(secret) = secrets.get_opt(name).await? else {
        return Ok((ServingCert::Absent, None));
    };
    let foreign = foreign_manager(&secret.metadata);
    Ok((serving_cert(secret.data.as_ref()), foreign))
}

/// Classify Secret data: usable only with a UTF-8 `tls.crt` and a non-empty `tls.key`.
fn serving_cert(data: Option<&std::collections::BTreeMap<String, k8s_openapi::ByteString>>) -> ServingCert {
    let has_key = data
        .and_then(|data| data.get("tls.key"))
        .is_some_and(|key| !key.0.is_empty());
    data.and_then(|data| data.get("tls.crt"))
        .and_then(|cert| String::from_utf8(cert.0.clone()).ok())
        .filter(|_| has_key)
        .map_or(ServingCert::Unusable, ServingCert::Pem)
}

/// Issue the serving certificate when needed; see [`serving_needs_issue`]. The CA is
/// preserved: only the leaf is re-signed under it.
async fn ensure_serving(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    ca: &certs::CaCert,
    args: &BootstrapArgs,
) -> Result<(), BoxError> {
    let (current, foreign) = load_serving_cert(secrets, &args.serving_secret).await?;
    if serving_needs_issue(args.force_regenerate, &current, &args.serving_dns, &ca.cert_pem) {
        if let Some(owner) = foreign {
            return Err(format!(
                "serving Secret {} is managed by {owner}; refusing to replace it. Set serving.existingSecretRef to use it, \
                 or choose another serving.secretName",
                args.serving_secret
            )
            .into());
        }
        warn_if_unchained(&args.serving_secret, &current, &ca.cert_pem);
        let serving = certs::generate_dns_only_cert(ca, &args.common_name, &args.serving_dns)?;
        write_tls_secret(secrets, &args.serving_secret, &serving.cert_pem, &serving.key_pem, true).await?;
    }
    Ok(())
}

/// Re-issue a serving leaf once it is this close to expiry. Bootstrap runs on
/// install and upgrade, so an upgrade inside the window renews it.
const RENEW_BEFORE: time::Duration = time::Duration::days(30);

/// Whether to issue the serving certificate: forced, absent, unusable, missing a
/// requested DNS name, within [`RENEW_BEFORE`] of expiry, or not a currently
/// valid leaf of the current CA (the CA was regenerated, or the leaf expired).
fn serving_needs_issue(force: bool, current: &ServingCert, requested: &[String], ca_cert_pem: &str) -> bool {
    force
        || match current {
            ServingCert::Absent | ServingCert::Unusable => true,
            ServingCert::Pem(cert_pem) => {
                serving_sans_missing(cert_pem, requested)
                    || certs::cert_expires_within(cert_pem, RENEW_BEFORE).unwrap_or(true)
                    || certs::verify_issued_by(ca_cert_pem, cert_pem).is_err()
            },
        }
}

/// Warn when an existing leaf is replaced because it does not verify against the
/// current CA, so an unexpected CA regeneration is visible. Logs names and dates only.
fn warn_if_unchained(secret: &str, current: &ServingCert, ca_cert_pem: &str) {
    let ServingCert::Pem(cert_pem) = current else {
        return;
    };
    let Err(reason) = certs::verify_issued_by(ca_cert_pem, cert_pem) else {
        return;
    };
    let (issuer, not_after) = certs::cert_issuer_and_expiry(cert_pem)
        .unwrap_or_else(|_unparseable| ("unparseable".to_owned(), "unknown".to_owned()));
    tracing::warn!(
        secret,
        %reason,
        old_issuer = %issuer,
        old_not_after = %not_after,
        "re-issuing a serving certificate that does not verify against the current grid CA"
    );
}

/// Whether the serving cert lacks any requested DNS name, or cannot be parsed. A
/// subset check: extra names on the cert do not trigger a re-issue.
fn serving_sans_missing(cert_pem: &str, requested: &[String]) -> bool {
    // DNS names compare case-insensitively and ignore a trailing dot.
    fn norm(name: &str) -> String {
        name.trim_end_matches('.').to_ascii_lowercase()
    }
    match certs::cert_dns_sans(cert_pem) {
        Ok(current) => {
            let have: std::collections::BTreeSet<String> = current.iter().map(|name| norm(name)).collect();
            requested.iter().any(|name| !have.contains(&norm(name)))
        },
        Err(_unparseable) => true,
    }
}

/// Issue the Postgres serving certificate when needed; see [`serving_needs_issue`].
/// Returns the fingerprint of the certificate the Secret now holds, issued or kept.
///
/// The service connects with sslmode=verify-full, so builtin Postgres needs a
/// leaf of the current grid CA whose SANs cover the DB Service names in `--db-dns`.
async fn ensure_db_serving(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    ca: &certs::CaCert,
    args: &BootstrapArgs,
) -> Result<Option<String>, BoxError> {
    let (current, foreign) = load_serving_cert(secrets, &args.db_serving_secret).await?;
    if serving_needs_issue(args.force_regenerate, &current, &args.db_dns, &ca.cert_pem) {
        if let Some(owner) = foreign {
            return Err(format!(
                "DB serving Secret {} is managed by {owner}; refusing to replace it. Remove that label or \
                 annotation, or delete the Secret, so bootstrap can issue it",
                args.db_serving_secret
            )
            .into());
        }
        warn_if_unchained(&args.db_serving_secret, &current, &ca.cert_pem);
        let serving = certs::generate_dns_only_cert(ca, "grid-enrollment-db", &args.db_dns)?;
        write_tls_secret(
            secrets,
            &args.db_serving_secret,
            &serving.cert_pem,
            &serving.key_pem,
            true,
        )
        .await?;
        return Ok(Some(certs::canonical_fingerprint(&serving.cert_pem)?));
    }
    Ok(match current {
        ServingCert::Pem(cert_pem) => Some(certs::canonical_fingerprint(&cert_pem)?),
        ServingCert::Absent | ServingCert::Unusable => None,
    })
}

/// Pod template annotation that carries the DB serving certificate's
/// fingerprint, so a new certificate rolls the Postgres pod.
const DB_CERT_ANNOTATION: &str = "grid.praxis.fast/db-serving-cert-sha256";

/// Roll `deployment` when its pod template does not carry `fingerprint`, the
/// certificate the DB Secret holds. Checked on every run, so a roll that failed
/// after a re-issue is retried by the next run. A Deployment that does not exist
/// yet (first install) is skipped.
async fn reconcile_db_roll(
    client: kube::Client,
    namespace: &str,
    deployment: &str,
    fingerprint: &str,
) -> Result<(), BoxError> {
    use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, Patch, PatchParams};

    // Untyped: the typed Deployment overflows the stack-frame budget.
    let resource = ApiResource::from_gvk(&GroupVersionKind::gvk("apps", "v1", "Deployment"));
    let deployments: Api<DynamicObject> = Api::namespaced_with(client, namespace, &resource);
    let Some(current) = Box::pin(deployments.get_opt(deployment)).await? else {
        return Ok(());
    };
    let stamped = current
        .data
        .pointer("/spec/template/metadata/annotations")
        .and_then(|annotations| annotations.get(DB_CERT_ANNOTATION))
        .and_then(serde_json::Value::as_str);
    if !needs_roll(stamped, fingerprint) {
        return Ok(());
    }
    Box::pin(deployments.patch(
        deployment,
        &PatchParams::default(),
        &Patch::Merge(roll_patch(fingerprint)),
    ))
    .await?;
    tracing::info!(
        deployment,
        "rolled the builtin Postgres onto its current serving certificate"
    );
    Ok(())
}

/// Whether a pod template stamped with `stamped` must roll to serve `fingerprint`.
fn needs_roll(stamped: Option<&str>, fingerprint: &str) -> bool {
    stamped != Some(fingerprint)
}

/// The merge patch that stamps `fingerprint` on a Deployment's pod template.
fn roll_patch(fingerprint: &str) -> serde_json::Value {
    serde_json::json!({
        "spec": { "template": { "metadata": { "annotations": { DB_CERT_ANNOTATION: fingerprint } } } }
    })
}

/// The `tls.crt`/`tls.key` PEM from a Secret, or `None` if it does not exist.
async fn load_tls_material(
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
            labels: Some(std::collections::BTreeMap::from([(
                "app.kubernetes.io/managed-by".to_owned(),
                MANAGED_BY.to_owned(),
            )])),
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

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use std::{collections::BTreeMap, sync::LazyLock};

    use clap::Parser as _;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

    use super::{
        BootstrapArgs, MANAGED_BY, ServingCert, admin_tokens, argo_keep_patch, db_credentials, ensure_swim_key,
        existing_identity_kept, foreign_manager, issue_site_identity, needs_roll, roll_patch, serving_cert,
        serving_needs_issue,
    };

    #[test]
    fn an_existing_identity_is_kept_only_for_this_ca_and_site() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let other = certs::generate_ca("grid-ca").expect("other ca");
        let lifetime = certs::DEFAULT_SITE_CERT_LIFETIME;
        let (hub, _hub_key) = issue_site_identity(&ca, "hub", lifetime).expect("hub");
        let (foreign, _foreign_key) = issue_site_identity(&other, "hub", lifetime).expect("foreign");

        assert_eq!(
            existing_identity_kept(&ca.cert_pem, &hub.cert_pem, "hub"),
            Ok(()),
            "valid identity"
        );
        assert!(
            existing_identity_kept(&ca.cert_pem, &hub.cert_pem, "east").is_err(),
            "renamed hub"
        );
        assert!(
            existing_identity_kept(&ca.cert_pem, &foreign.cert_pem, "hub").is_err(),
            "another CA"
        );
    }

    #[test]
    fn an_expired_identity_of_this_site_is_kept() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let now = time::OffsetDateTime::now_utc();
        let past = certs::Validity {
            not_before: now - time::Duration::days(2),
            not_after: now - time::Duration::days(1),
        };
        let csr = certs::generate_csr("hub").expect("csr");
        let expired = certs::sign_csr(&ca, "hub", &csr.csr_pem, past).expect("sign");
        assert_eq!(
            existing_identity_kept(&ca.cert_pem, &expired.cert_pem, "hub"),
            Ok(()),
            "expired, same site"
        );
        assert!(
            existing_identity_kept(&ca.cert_pem, &expired.cert_pem, "east").is_err(),
            "expired, other site"
        );
    }

    #[test]
    fn site_identity_matches_an_enrolled_one() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let (issued, key_pem) = issue_site_identity(&ca, "hub", certs::DEFAULT_SITE_CERT_LIFETIME).expect("issue");
        certs::verify_site_cert(&ca.cert_pem, &issued.cert_pem, "hub").expect("verifies as site hub");
        assert!(
            certs::has_svid_profile(&issued.cert_pem).expect("parse"),
            "X.509-SVID profile"
        );
        assert_eq!(issued.spiffe_id, certs::spiffe_id("hub"), "SPIFFE ID");
        assert!(key_pem.contains("PRIVATE KEY"), "key returned");
    }

    #[test]
    fn the_swim_key_is_opt_in_and_sized_for_the_operator() {
        assert_eq!(BootstrapArgs::parse_from(["bootstrap"]).swim_key_secret, None);
        let args = BootstrapArgs::parse_from(["bootstrap", "--swim-key-secret", "grid-swim-key"]);
        assert_eq!(args.swim_key_secret.as_deref(), Some("grid-swim-key"));
        assert_eq!(
            enrollment::api::random_bytes(super::SWIM_KEY_LEN)
                .expect("random")
                .len(),
            32
        );
    }

    #[test]
    fn site_flags_default_to_the_operator_secret_names() {
        let args = BootstrapArgs::parse_from(["bootstrap", "--site-name", "hub"]);
        assert_eq!(args.site_name.as_deref(), Some("hub"));
        assert_eq!(
            (
                args.site_namespace.as_str(),
                args.site_secret.as_str(),
                args.site_ca_secret.as_str()
            ),
            ("grid", "grid-site-identity", "grid-ca")
        );
    }

    #[test]
    fn db_credentials_match_the_chart_connection_url() {
        let args = BootstrapArgs::parse_from([
            "bootstrap",
            "--db-credentials-secret",
            "grid-enrollment-db",
            "--db-host",
            "grid-enrollment-db",
        ]);
        let data = db_credentials(&args, "s3cret");
        assert_eq!(data.get("password").map(String::as_str), Some("s3cret"));
        assert_eq!(
            data.get("DB_CONNECTION_URL").map(String::as_str),
            Some(
                "postgres://enrollment:s3cret@grid-enrollment-db:5432/enrollment\
                 ?sslmode=verify-full&sslrootcert=/etc/grid-ca-bundle/ca.crt"
            )
        );
    }

    #[test]
    fn argo_keep_patch_touches_only_the_sync_options_annotation() {
        assert_eq!(
            argo_keep_patch(),
            serde_json::json!({
                "metadata": { "annotations": { "argocd.argoproj.io/sync-options": "Prune=false,Delete=false" } }
            })
        );
    }

    #[test]
    fn admin_tokens_is_one_name_token_line() {
        let data = admin_tokens("abc");
        assert_eq!(data.get("tokens").map(String::as_str), Some("admin:abc\n"));
    }

    #[test]
    fn generated_values_are_hex_of_the_requested_length() {
        let value = enrollment::api::random_hex(16).expect("random");
        assert_eq!(value.len(), 32);
        assert!(value.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[test]
    fn serving_cert_needs_both_halves() {
        let bytes = |text: &str| k8s_openapi::ByteString(text.as_bytes().to_vec());
        let pair = BTreeMap::from([
            ("tls.crt".to_owned(), bytes("cert")),
            ("tls.key".to_owned(), bytes("key")),
        ]);
        assert!(matches!(serving_cert(Some(&pair)), ServingCert::Pem(pem) if pem == "cert"));
        for data in [
            BTreeMap::from([("tls.crt".to_owned(), bytes("cert"))]),
            BTreeMap::from([("tls.crt".to_owned(), bytes("cert")), ("tls.key".to_owned(), bytes(""))]),
            BTreeMap::from([("tls.key".to_owned(), bytes("key"))]),
        ] {
            assert!(matches!(serving_cert(Some(&data)), ServingCert::Unusable), "{data:?}");
        }
        assert!(matches!(serving_cert(None), ServingCert::Unusable));
    }

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn secrets_bootstrap_wrote_are_ours_to_replace() {
        let ours = ObjectMeta {
            labels: Some(map(&[("app.kubernetes.io/managed-by", MANAGED_BY)])),
            ..ObjectMeta::default()
        };
        assert_eq!(foreign_manager(&ours), None, "stamped by bootstrap");
        assert_eq!(
            foreign_manager(&ObjectMeta::default()),
            None,
            "unlabelled: written before the label existed"
        );
    }

    #[test]
    fn secrets_another_manager_owns_are_refused() {
        let helm = ObjectMeta {
            labels: Some(map(&[("app.kubernetes.io/managed-by", "Helm")])),
            ..ObjectMeta::default()
        };
        assert_eq!(
            foreign_manager(&helm).as_deref(),
            Some("app.kubernetes.io/managed-by=Helm")
        );
        let issued = ObjectMeta {
            annotations: Some(map(&[("cert-manager.io/certificate-name", "enrollment")])),
            ..ObjectMeta::default()
        };
        assert_eq!(foreign_manager(&issued).as_deref(), Some("cert-manager"));
        let synced = ObjectMeta {
            labels: Some(map(&[("app.kubernetes.io/managed-by", MANAGED_BY)])),
            owner_references: Some(vec![OwnerReference {
                kind: "ExternalSecret".to_owned(),
                name: "enrollment-serving".to_owned(),
                ..OwnerReference::default()
            }]),
            ..ObjectMeta::default()
        };
        assert_eq!(
            foreign_manager(&synced).as_deref(),
            Some("its owner ExternalSecret enrollment-serving"),
            "an owner reference wins even over our own label"
        );
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_owned()).collect()
    }

    /// The current grid CA the tests issue under.
    static CA: LazyLock<certs::CaCert> = LazyLock::new(|| certs::generate_ca("grid-ca").expect("ca"));

    /// A serving cert for `sans`, signed by [`CA`].
    fn serving_pem(sans: &[&str]) -> ServingCert {
        let leaf = certs::generate_dns_only_cert(&CA, "grid-ca", &names(sans)).expect("leaf");
        ServingCert::Pem(leaf.cert_pem)
    }

    /// Whether `current` needs issuing for `requested` under [`CA`].
    fn needs_issue(force: bool, current: &ServingCert, requested: &[String]) -> bool {
        serving_needs_issue(force, current, requested, &CA.cert_pem)
    }

    #[test]
    fn serving_cert_is_kept_when_every_requested_name_is_present() {
        let requested = names(&["enroll.grid.svc", "enroll.apps.example.com"]);
        let current = serving_pem(&["enroll.grid.svc", "enroll.apps.example.com"]);
        assert!(
            !needs_issue(false, &current, &requested),
            "unchanged values keep the cert"
        );
    }

    #[test]
    fn extra_names_on_the_cert_do_not_reissue() {
        let current = serving_pem(&["enroll.grid.svc", "old.apps.example.com"]);
        assert!(!needs_issue(false, &current, &names(&["enroll.grid.svc"])));
    }

    #[test]
    fn a_missing_name_reissues() {
        let current = serving_pem(&["enroll.grid.svc"]);
        let requested = names(&["enroll.grid.svc", "enroll.apps.example.com"]);
        assert!(needs_issue(false, &current, &requested), "a new route.host is added");
    }

    #[test]
    fn names_compare_without_case_or_trailing_dot() {
        let current = serving_pem(&["enroll.apps.example.com"]);
        assert!(!needs_issue(false, &current, &names(&["Enroll.Apps.Example.COM."])));
    }

    #[test]
    fn force_absent_unusable_and_unparseable_reissue() {
        let requested = names(&["enroll.grid.svc"]);
        assert!(
            needs_issue(true, &serving_pem(&["enroll.grid.svc"]), &requested),
            "forced"
        );
        assert!(needs_issue(false, &ServingCert::Absent, &requested), "absent");
        assert!(needs_issue(false, &ServingCert::Unusable, &requested), "no tls.crt");
        let garbage = ServingCert::Pem("not a certificate".to_owned());
        assert!(needs_issue(false, &garbage, &requested), "unparseable");
    }

    #[test]
    fn a_leaf_of_another_ca_reissues() {
        let old_ca = certs::generate_ca("grid-ca").expect("old ca");
        let leaf = certs::generate_dns_only_cert(&old_ca, "grid-ca", &names(&["enroll.grid.svc"])).expect("leaf");
        assert!(
            needs_issue(false, &ServingCert::Pem(leaf.cert_pem), &names(&["enroll.grid.svc"])),
            "a leaf the regenerated CA did not sign is re-issued"
        );
    }

    #[test]
    fn the_roll_patch_stamps_only_the_pod_template_annotation() {
        assert_eq!(
            roll_patch("ab12"),
            serde_json::json!({
                "spec": { "template": { "metadata": { "annotations": {
                    "grid.praxis.fast/db-serving-cert-sha256": "ab12"
                } } } }
            }),
        );
    }

    #[test]
    fn rolls_only_when_the_stamp_differs_from_the_secret() {
        assert!(!needs_roll(Some("ab12"), "ab12"), "already serving it");
        assert!(needs_roll(Some("old"), "ab12"), "a new cert");
        assert!(needs_roll(None, "ab12"), "never stamped");
    }

    /// Secrets by namespace and name, served by [`fake_api`].
    type Store = std::sync::Arc<std::sync::Mutex<BTreeMap<(String, String), k8s_openapi::api::core::v1::Secret>>>;

    /// A Kubernetes API holding `store`: GET reads a Secret, POST creates one or answers 409.
    fn fake_api(store: Store) -> kube::Client {
        use http_body_util::BodyExt as _;

        let service = tower::service_fn(move |req: axum::http::Request<kube::client::Body>| {
            let store = std::sync::Arc::clone(&store);
            async move {
                let (parts, body) = req.into_parts();
                let bytes = body.collect().await.expect("body").to_bytes();
                let (code, value) = answer(&store, &parts.method, parts.uri.path(), &bytes);
                let reply = kube::client::Body::from(serde_json::to_vec(&value).expect("json"));
                Ok::<_, std::convert::Infallible>(
                    axum::http::Response::builder()
                        .status(code)
                        .body(reply)
                        .expect("response"),
                )
            }
        });
        kube::Client::new(service, "grid-enroll")
    }

    /// Serve one request on `/api/v1/namespaces/{namespace}/secrets[/{name}]`.
    fn answer(store: &Store, method: &axum::http::Method, path: &str, body: &[u8]) -> (u16, serde_json::Value) {
        let path: Vec<&str> = path.split('/').collect();
        let namespace = path.get(4).copied().unwrap_or_default().to_owned();
        let mut map = store.lock().expect("lock");
        if method == axum::http::Method::POST {
            let secret: k8s_openapi::api::core::v1::Secret = serde_json::from_slice(body).expect("secret");
            let name = secret.metadata.name.clone().unwrap_or_default();
            return match map.entry((namespace, name)) {
                std::collections::btree_map::Entry::Occupied(_) => (409, failure(409, "AlreadyExists")),
                std::collections::btree_map::Entry::Vacant(slot) => {
                    (201, serde_json::to_value(slot.insert(secret)).expect("json"))
                },
            };
        }
        let name = path.get(6).copied().unwrap_or_default().to_owned();
        map.get(&(namespace, name)).map_or_else(
            || (404, failure(404, "NotFound")),
            |secret| (200, serde_json::to_value(secret).expect("json")),
        )
    }

    fn failure(code: u16, reason: &str) -> serde_json::Value {
        serde_json::json!({"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": reason, "code": code})
    }

    fn put_key(store: &Store, namespace: &str, key: &[u8]) {
        let secret = k8s_openapi::api::core::v1::Secret {
            metadata: ObjectMeta {
                name: Some("grid-swim-key".to_owned()),
                ..ObjectMeta::default()
            },
            data: Some(BTreeMap::from([(
                "key".to_owned(),
                k8s_openapi::ByteString(key.to_vec()),
            )])),
            ..k8s_openapi::api::core::v1::Secret::default()
        };
        store
            .lock()
            .expect("lock")
            .insert((namespace.to_owned(), "grid-swim-key".to_owned()), secret);
    }

    fn key_in(store: &Store, namespace: &str) -> Option<Vec<u8>> {
        store
            .lock()
            .expect("lock")
            .get(&(namespace.to_owned(), "grid-swim-key".to_owned()))?
            .data
            .as_ref()?
            .get("key")
            .map(|bytes| bytes.0.clone())
    }

    /// Run [`ensure_swim_key`] against `store` with `flags`.
    async fn swim_key_run(store: &Store, flags: &[&str]) -> Result<(), super::BoxError> {
        let args = BootstrapArgs::parse_from(["bootstrap", "--swim-key-secret", "grid-swim-key"].iter().chain(flags));
        let client = fake_api(std::sync::Arc::clone(store));
        let secrets = kube::api::Api::namespaced(client.clone(), "grid-enroll");
        Box::pin(ensure_swim_key(&client, &secrets, &args)).await
    }

    const HUB: [&str; 2] = ["--site-name", "hub"];

    #[tokio::test]
    async fn the_swim_key_is_created_once_and_shared_with_the_hub() {
        let store = Store::default();
        swim_key_run(&store, &HUB).await.expect("first run");
        let key = key_in(&store, "grid-enroll").expect("release namespace key");
        assert_eq!(key.len(), 32, "sized for the operator");
        assert_eq!(key_in(&store, "grid"), Some(key.clone()), "the hub gets the same key");
        swim_key_run(&store, &HUB).await.expect("second run");
        assert_eq!(
            key_in(&store, "grid-enroll"),
            Some(key.clone()),
            "a rerun keeps the key"
        );
        assert_eq!(key_in(&store, "grid"), Some(key), "a rerun keeps the hub copy");
    }

    #[tokio::test]
    async fn without_a_site_the_swim_key_stays_in_the_release_namespace() {
        let store = Store::default();
        swim_key_run(&store, &[]).await.expect("run");
        assert!(key_in(&store, "grid-enroll").is_some(), "created");
        assert_eq!(key_in(&store, "grid"), None, "no hub copy");
    }

    #[tokio::test]
    async fn an_existing_swim_key_is_copied_to_the_hub() {
        let store = Store::default();
        put_key(&store, "grid-enroll", &[7; 32]);
        swim_key_run(&store, &HUB).await.expect("run");
        assert_eq!(key_in(&store, "grid-enroll"), Some(vec![7; 32]), "kept");
        assert_eq!(key_in(&store, "grid"), Some(vec![7; 32]), "copied");
    }

    #[tokio::test]
    async fn a_different_hub_swim_key_is_refused() {
        let store = Store::default();
        put_key(&store, "grid-enroll", &[7; 32]);
        put_key(&store, "grid", &[8; 32]);
        let error = swim_key_run(&store, &HUB).await.expect_err("conflict");
        assert!(error.to_string().contains("different SWIM key"), "{error}");
        assert_eq!(key_in(&store, "grid"), Some(vec![8; 32]), "neither key is replaced");
    }

    #[tokio::test]
    async fn a_wrong_sized_swim_key_is_refused() {
        let store = Store::default();
        put_key(&store, "grid-enroll", &[7; 16]);
        let error = swim_key_run(&store, &HUB).await.expect_err("short key");
        assert!(error.to_string().contains("16 bytes"), "{error}");
        assert_eq!(key_in(&store, "grid"), None, "a bad key is not copied");
    }
}
