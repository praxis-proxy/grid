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
    /// Secret, in this namespace, for the CA-signed seed that registers the site's key.
    #[arg(long, default_value = "grid-reserved-seeds")]
    reserved_seeds_secret: String,
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
    let ca = Box::pin(resolve_ca(&client, &secrets, args)).await?;
    write_opaque_secret(&secrets, &args.ca_bundle_secret, "ca.crt", &ca.cert_pem).await?;
    Box::pin(ensure_site_identity(&client, &ca, args, &namespace)).await?;
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
#[expect(
    clippy::too_many_lines,
    reason = "keep or issue the identity, then seed its key, read as one step"
)]
async fn ensure_site_identity(
    client: &kube::Client,
    ca: &certs::CaCert,
    args: &BootstrapArgs,
    namespace: &str,
) -> Result<(), BoxError> {
    let Some(site) = args.site_name.as_deref() else {
        return Ok(());
    };
    let seeds = &kube::api::Api::namespaced(client.clone(), namespace);
    let secrets = &kube::api::Api::namespaced(client.clone(), &args.site_namespace);
    Box::pin(ensure_site_ca(
        secrets,
        &args.site_ca_secret,
        &ca.cert_pem,
        args.force_regenerate,
    ))
    .await?;
    let view = Box::pin(secret_view(secrets, &args.site_secret)).await?;
    let read_as = view.as_ref().map(|view| view.read_as.clone());
    let replace = match identity_action(view.as_ref(), &ca.cert_pem, site, args.force_regenerate) {
        SiteIdentity::Keep => {
            let kept = view
                .and_then(|view| view.tls_crt)
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .unwrap_or_default();
            return Box::pin(ensure_seed(seeds, &args.reserved_seeds_secret, ca, site, &kept, false)).await;
        },
        SiteIdentity::Refuse => {
            return Err(format!(
                "Secret {}: it holds an identity that is not {site} from this grid CA; delete it or set \
                 --force-regenerate to re-issue",
                args.site_secret
            )
            .into());
        },
        SiteIdentity::Issue { replace } => {
            if replace && !args.force_regenerate {
                tracing::warn!(
                    secret = %args.site_secret,
                    "replacing a hub identity no enrollment authority wrote, or whose key does not match it"
                );
            }
            replace
        },
    };
    let (issued, key_pem) = issue_site_identity(ca, site, crate::load_cert_lifetime())?;
    if replace {
        Box::pin(recreate_tls_secret(
            secrets,
            &args.site_secret,
            read_as,
            &issued.cert_pem,
            &key_pem,
        ))
        .await?;
    } else {
        Box::pin(write_tls_secret(
            secrets,
            &args.site_secret,
            &issued.cert_pem,
            &key_pem,
            false,
        ))
        .await?;
    }
    log_issued(&issued, &args.site_secret);
    Box::pin(ensure_seed(
        seeds,
        &args.reserved_seeds_secret,
        ca,
        site,
        &issued.cert_pem,
        true,
    ))
    .await
}

/// Whether the held seed already names `site` with `leaf_key`. A seed for another key,
/// such as one a failed patch left behind, is signed again.
fn seed_is_current(held: Option<&enrollment::SeedRecord>, site: &str, leaf_key: &str, issued: bool) -> bool {
    !issued && held.is_some_and(|seed| seed.site_name == site && seed.key_sha256 == leaf_key)
}

/// Sign a seed registering `site`'s key with the service: always for a key bootstrap
/// just `issued`, else only when no valid seed names the site with the identity's key.
///
/// The generation only grows, so the service applies each re-issue once and never
/// an older seed.
#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the seed's fields and where it goes, each used once"
)]
async fn ensure_seed(
    seeds: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    ca: &certs::CaCert,
    site: &str,
    leaf_pem: &str,
    issued: bool,
) -> Result<(), BoxError> {
    use enrollment::seed::{SEED_SUFFIX, SIGNATURE_SUFFIX};
    use kube::api::{Patch, PatchParams};

    let (seed_key, signature_key) = (format!("{site}{SEED_SUFFIX}"), format!("{site}{SIGNATURE_SUFFIX}"));
    let data = Box::pin(seeds.get_opt(name))
        .await?
        .and_then(|secret| secret.data)
        .unwrap_or_default();
    let text = |key: &str| data.get(key).and_then(|value| String::from_utf8(value.0.clone()).ok());
    let held = text(&seed_key)
        .zip(text(&signature_key))
        .and_then(|(body, signature)| enrollment::seed::verified(&body, &signature, &ca.cert_pem).ok());
    let leaf_key = certs::cert_public_key_sha256(leaf_pem)?;
    if seed_is_current(held.as_ref(), site, &leaf_key, issued) {
        return Ok(());
    }
    let now_ms = u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).unwrap_or(0);
    let generation = held.map_or(0, |seed| seed.generation).saturating_add(1).max(now_ms);
    let seed = enrollment::SeedRecord {
        site_name: site.to_owned(),
        key_sha256: leaf_key,
        generation,
        issued_at: certs::cert_validity(leaf_pem)?.0,
    };
    let (body, signature) = enrollment::seed::sign(&seed, ca)?;
    let patch = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {
            "name": name,
            "labels": { "app.kubernetes.io/managed-by": MANAGED_BY },
            "annotations": { "argocd.argoproj.io/sync-options": ARGO_KEEP },
        },
        "data": {
            seed_key: k8s_openapi::ByteString(body.into_bytes()),
            signature_key: k8s_openapi::ByteString(signature.into_bytes()),
        },
    });
    Box::pin(seeds.patch(name, &PatchParams::apply(MANAGED_BY).force(), &Patch::Apply(&patch))).await?;
    tracing::info!(
        site,
        key_sha256 = %seed.key_sha256,
        generation,
        secret = name,
        "signed the reserved site seed"
    );
    Ok(())
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
    let view = Box::pin(secret_view(secrets, name)).await?;
    match hub_ca_write(view.as_ref(), ca_cert_pem, force) {
        HubCa::Keep => Ok(()),
        HubCa::Refuse => Err(format!(
            "Secret {name} holds a different grid CA; refusing to issue a site identity it would not anchor"
        )
        .into()),
        HubCa::Write => {
            if view.is_some() && !force {
                tracing::warn!(secret = name, "replacing a grid CA no enrollment authority wrote");
            }
            Box::pin(write_opaque_secret(secrets, name, "ca.crt", ca_cert_pem)).await
        },
    }
}

/// The parts of a Secret the CA decision reads.
#[derive(Clone, Debug, Default)]
struct SecretView {
    /// Written by bootstrap, or by the operator from an enrollment response.
    authoritative: bool,
    /// Its `ca.crt`, if any.
    ca_crt: Option<Vec<u8>>,
    /// Its `tls.crt`, if any.
    tls_crt: Option<Vec<u8>>,
    /// Whether its `tls.key` is the key for `tls_crt`.
    key_matches: bool,
    /// The uid and resourceVersion read, so a replace deletes only this Secret.
    read_as: kube::api::Preconditions,
}

/// Writers whose CA in a Secret is the grid's: bootstrap, and the operator storing
/// what enrollment returned. An unlabelled CA, such as an operator's self-signed
/// placeholder, is not.
const CA_AUTHORITIES: [&str; 2] = [MANAGED_BY, "grid-operator"];

/// Read Secret `name` for the CA decision, `None` when it is absent.
async fn secret_view(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
) -> Result<Option<SecretView>, BoxError> {
    let Some(secret) = Box::pin(secrets.get_opt(name)).await?.map(Box::new) else {
        return Ok(None);
    };
    let authoritative = secret
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get("app.kubernetes.io/managed-by"))
        .is_some_and(|owner| CA_AUTHORITIES.contains(&owner.as_str()));
    let read_as = kube::api::Preconditions {
        uid: secret.metadata.uid.clone(),
        resource_version: secret.metadata.resource_version.clone(),
    };
    let mut data = secret.data.unwrap_or_default();
    let mut take = |key: &str| data.remove(key).map(|bytes| bytes.0);
    let (ca_crt, tls_crt) = (take("ca.crt"), take("tls.crt"));
    let key_matches = pair_matches(data.get("tls.key").map(|key| key.0.as_slice()), tls_crt.as_deref());
    for other in data.values_mut() {
        zeroize::Zeroize::zeroize(&mut other.0);
    }
    Ok(Some(SecretView {
        authoritative,
        ca_crt,
        tls_crt,
        key_matches,
        read_as,
    }))
}

/// Whether `key` is the private key for `cert`, both PEM.
fn pair_matches(key: Option<&[u8]>, cert: Option<&[u8]>) -> bool {
    let key = key.and_then(|bytes| std::str::from_utf8(bytes).ok());
    let cert = cert.and_then(|bytes| std::str::from_utf8(bytes).ok());
    key.zip(cert)
        .is_some_and(|(key, cert)| certs::key_matches_cert(key, cert))
}

/// One distributed copy, `None` when absent or, for `hub`, not authoritative.
fn ca_copy(name: &str, view: Option<&SecretView>, hub: bool) -> Option<enrollment::CaCopy> {
    let view = view.filter(|view| !hub || view.authoritative)?;
    let parsed = view
        .ca_crt
        .as_ref()
        .ok_or_else(|| format!("Secret {name} has no ca.crt"))
        .and_then(|bytes| String::from_utf8(bytes.clone()).map_err(|_bad| format!("Secret {name} ca.crt is not UTF-8")))
        .and_then(|pem| certs::bundle_fingerprints(&pem).map_err(|err| format!("Secret {name} ca.crt: {err}")));
    Some(match parsed {
        Ok(fingerprints) => enrollment::CaCopy::Holds(fingerprints),
        Err(why) => enrollment::CaCopy::Unreadable(why),
    })
}

/// Decide the grid CA before anything is written, from the key Secret's certificate,
/// the CA bundle, and the hub's CA Secret.
fn plan_ca(
    args: &BootstrapArgs,
    key_cert: Option<&str>,
    bundle: Option<&SecretView>,
    hub: Option<&SecretView>,
    identity: Option<&SecretView>,
) -> Result<enrollment::CaAction, BoxError> {
    let key = match (args.force_regenerate, key_cert) {
        (false, Some(cert_pem)) => Some(certs::canonical_fingerprint(cert_pem)?),
        _ => None,
    };
    let copies: Vec<enrollment::CaCopy> = [
        ca_copy(&args.ca_bundle_secret, bundle, false),
        ca_copy(&args.site_ca_secret, hub, true),
    ]
    .into_iter()
    .flatten()
    .collect();
    let action = enrollment::ca_action(key.as_deref(), args.force_regenerate, &copies);
    // An identity an authority issued must come from the decided CA, checked before any write.
    let site = args.site_name.as_deref().unwrap_or_default();
    let issued = identity.filter(|view| view.authoritative && !args.force_regenerate);
    Ok(match (action, issued, key_cert) {
        (enrollment::CaAction::Mint, Some(_), _) => enrollment::CaAction::Refuse(format!(
            "Secret {} holds a hub identity from a grid CA that is no longer here",
            args.site_secret
        )),
        (enrollment::CaAction::Load, Some(view), Some(ca_pem))
            if identity_action(Some(view), ca_pem, site, false) == SiteIdentity::Refuse =>
        {
            enrollment::CaAction::Refuse(format!(
                "Secret {} holds a hub identity another grid CA issued",
                args.site_secret
            ))
        },
        (action, ..) => action,
    })
}

/// What to do with the hub's identity Secret for the CA in `ca_cert_pem`.
#[derive(Debug, PartialEq, Eq)]
enum SiteIdentity {
    /// An authority issued it from this CA for this site: keep it, even expired.
    Keep,
    /// Issue one, replacing what is there when `replace`.
    Issue {
        /// A placeholder or a regeneration overwrites the Secret.
        replace: bool,
    },
    /// An authority issued it from another CA or for another site.
    Refuse,
}

/// Decide the hub's identity Secret: absent, regenerating, or a placeholder no
/// authority wrote is issued; an authority's is kept only from this CA for this site.
fn identity_action(view: Option<&SecretView>, ca_cert_pem: &str, site: &str, force: bool) -> SiteIdentity {
    let Some(view) = view else {
        return SiteIdentity::Issue { replace: force };
    };
    if force || !view.authoritative {
        return SiteIdentity::Issue { replace: true };
    }
    let cert = view
        .tls_crt
        .as_ref()
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .unwrap_or_default();
    match existing_identity_kept(ca_cert_pem, cert, site) {
        // Without its key the identity cannot serve, so it is issued again.
        Ok(()) if !view.key_matches => SiteIdentity::Issue { replace: true },
        Ok(()) => SiteIdentity::Keep,
        Err(_) => SiteIdentity::Refuse,
    }
}

/// What to do with the hub's CA Secret once the CA is decided.
#[derive(Debug, PartialEq, Eq)]
enum HubCa {
    /// It already holds this CA.
    Keep,
    /// Write this CA: absent, regenerating, or a placeholder no authority wrote.
    Write,
    /// An authoritative copy holds another CA.
    Refuse,
}

/// Decide the hub's CA Secret for the CA in `ca_cert_pem`.
fn hub_ca_write(view: Option<&SecretView>, ca_cert_pem: &str, force: bool) -> HubCa {
    let Some(view) = view.filter(|view| view.authoritative && !force) else {
        return HubCa::Write;
    };
    let holds = view
        .ca_crt
        .as_ref()
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .is_some_and(|bundle| certs::bundle_within(ca_cert_pem, bundle).unwrap_or(false));
    if holds { HubCa::Keep } else { HubCa::Refuse }
}

/// Load the CA from its Secret, or generate and persist one when none is distributed.
/// A lost key with the CA already out is refused, since a new CA would split the grid.
///
/// Load when the Secret exists and no regenerate is forced, so re-runs keep the
/// same CA. The signing key is written only to the CA-key Secret.
#[expect(
    clippy::too_many_lines,
    reason = "load, refuse, or mint the CA reads as one decision"
)]
async fn resolve_ca(
    client: &kube::Client,
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    args: &BootstrapArgs,
) -> Result<certs::CaCert, BoxError> {
    // A regeneration never reads the old key, so a corrupt one cannot block it.
    let material = if args.force_regenerate {
        None
    } else {
        Box::pin(load_tls_material(secrets, &args.ca_key_secret)).await?
    };
    let bundle = Box::pin(secret_view(secrets, &args.ca_bundle_secret)).await?;
    let (hub, identity) = match args.site_name {
        Some(_) => {
            let site = kube::api::Api::namespaced(client.clone(), &args.site_namespace);
            (
                Box::pin(secret_view(&site, &args.site_ca_secret)).await?,
                Box::pin(secret_view(&site, &args.site_secret)).await?,
            )
        },
        None => (None, None),
    };
    let key_cert = material.as_ref().map(|(cert_pem, _key)| cert_pem.as_str());
    match plan_ca(args, key_cert, bundle.as_ref(), hub.as_ref(), identity.as_ref())? {
        enrollment::CaAction::Load => {
            let (cert_pem, key_pem) = material.ok_or("the CA key Secret vanished")?;
            Ok(certs::load_ca(&args.common_name, &key_pem, &cert_pem)?)
        },
        enrollment::CaAction::Refuse(reason) => {
            tracing::error!(secret = %args.ca_key_secret, %reason, "refusing to change the grid CA");
            Err(format!(
                "{reason}. Restore Secret {} from backup. A new CA would split the grid; to start over \
                 on purpose, set ca.forceRegenerate, and every site must re-enroll",
                args.ca_key_secret
            )
            .into())
        },
        enrollment::CaAction::Mint => {
            let ca = certs::generate_ca(&args.common_name)?;
            Box::pin(write_tls_secret(
                secrets,
                &args.ca_key_secret,
                &ca.cert_pem,
                &ca.key_pem,
                args.force_regenerate,
            ))
            .await?;
            // A concurrent bootstrap may have stored its own CA first: never sign with an unstored one.
            let stored = Box::pin(load_tls_material(secrets, &args.ca_key_secret)).await?;
            if stored.as_ref().map(|(cert_pem, _key)| cert_pem.as_str()) != Some(ca.cert_pem.as_str()) {
                return Err(format!(
                    "another bootstrap stored a different CA in Secret {} at the same time; run it again",
                    args.ca_key_secret
                )
                .into());
            }
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

/// Delete Secret `name` only if it is still the one read as `read_as`.
async fn delete_as_read(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    read_as: Option<kube::api::Preconditions>,
) -> Result<(), BoxError> {
    let params = kube::api::DeleteParams {
        preconditions: read_as,
        ..Default::default()
    };
    match Box::pin(secrets.delete(name, &params)).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 409 => {
            Err(format!("Secret {name} changed after it was read; not replacing it").into())
        },
        Err(error) => Err(error.into()),
    }
}

/// Replace Secret `name` with a `kubernetes.io/tls` one: delete, then create, since a
/// Secret's type cannot change in place and a placeholder may be `Opaque`.
async fn recreate_tls_secret(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    read_as: Option<kube::api::Preconditions>,
    cert_pem: &str,
    key_pem: &str,
) -> Result<(), BoxError> {
    Box::pin(delete_as_read(secrets, name, read_as)).await?;
    let string_data = std::collections::BTreeMap::from([
        ("tls.crt".to_owned(), cert_pem.to_owned()),
        ("tls.key".to_owned(), key_pem.to_owned()),
    ]);
    let secret = Box::new(k8s_openapi::api::core::v1::Secret {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(name.to_owned()),
            labels: Some(std::collections::BTreeMap::from([(
                "app.kubernetes.io/managed-by".to_owned(),
                MANAGED_BY.to_owned(),
            )])),
            ..Default::default()
        },
        type_: Some("kubernetes.io/tls".to_owned()),
        string_data: Some(string_data),
        ..Default::default()
    });
    // A create that loses to another writer fails rather than leaving its Secret in place.
    Box::pin(secrets.create(&kube::api::PostParams::default(), &secret)).await?;
    Ok(())
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
        BootstrapArgs, CA_AUTHORITIES, HubCa, MANAGED_BY, SecretView, ServingCert, SiteIdentity, admin_tokens,
        argo_keep_patch, db_credentials, ensure_swim_key, existing_identity_kept, foreign_manager, hub_ca_write,
        identity_action, issue_site_identity, needs_roll, plan_ca, roll_patch, seed_is_current, serving_cert,
        serving_needs_issue,
    };

    /// A CA Secret written by `writer`, holding `pem`.
    fn view(writer: Option<&str>, pem: &str) -> SecretView {
        SecretView {
            authoritative: writer.is_some_and(|writer| CA_AUTHORITIES.contains(&writer)),
            ca_crt: Some(pem.as_bytes().to_vec()),
            tls_crt: None,
            key_matches: false,
            read_as: kube::api::Preconditions::default(),
        }
    }

    /// An identity Secret written by `writer`, holding a hub leaf `ca` issued.
    fn identity(writer: Option<&str>, ca: &certs::CaCert) -> SecretView {
        let (issued, _key) = issue_site_identity(ca, "hub", certs::DEFAULT_SITE_CERT_LIFETIME).expect("leaf");
        SecretView {
            authoritative: writer.is_some_and(|writer| CA_AUTHORITIES.contains(&writer)),
            ca_crt: None,
            tls_crt: Some(issued.cert_pem.into_bytes()),
            key_matches: true,
            read_as: kube::api::Preconditions::default(),
        }
    }

    fn hub_args(extra: &[&str]) -> BootstrapArgs {
        BootstrapArgs::parse_from(["bootstrap", "--site-name", "hub"].iter().chain(extra))
    }

    /// The whole bootstrap order on a hub where the operator self-signed first: the
    /// CA, then the hub's CA Secret, then its identity, then a second run.
    #[test]
    #[expect(clippy::too_many_lines, reason = "the whole bootstrap order, then a second run")]
    fn an_operator_placeholder_does_not_block_a_first_hub_install() {
        let placeholder = certs::generate_ca("grid-ca").expect("placeholder");
        let (hub_ca, hub_identity) = (view(None, &placeholder.cert_pem), identity(None, &placeholder));
        let args = hub_args(&[]);
        assert_eq!(
            plan_ca(&args, None, None, Some(&hub_ca), Some(&hub_identity)).expect("plan"),
            enrollment::CaAction::Mint,
            "an unlabelled CA and identity are not the grid's"
        );
        let minted = certs::generate_ca("grid-ca").expect("minted");
        assert_eq!(hub_ca_write(Some(&hub_ca), &minted.cert_pem, false), HubCa::Write);
        assert_eq!(
            identity_action(Some(&hub_identity), &minted.cert_pem, "hub", false),
            SiteIdentity::Issue { replace: true },
            "the placeholder identity is replaced"
        );

        // The second run sees what the first wrote.
        let (bundle, written_ca, written_identity) = (
            view(Some(MANAGED_BY), &minted.cert_pem),
            view(Some(MANAGED_BY), &minted.cert_pem),
            identity(Some(MANAGED_BY), &minted),
        );
        assert_eq!(
            plan_ca(
                &args,
                Some(&minted.cert_pem),
                Some(&bundle),
                Some(&written_ca),
                Some(&written_identity)
            )
            .expect("plan"),
            enrollment::CaAction::Load
        );
        assert_eq!(hub_ca_write(Some(&written_ca), &minted.cert_pem, false), HubCa::Keep);
        assert_eq!(
            identity_action(Some(&written_identity), &minted.cert_pem, "hub", false),
            SiteIdentity::Keep
        );
    }

    #[test]
    fn a_seed_for_another_key_is_signed_again() {
        let seed = |key: &str| enrollment::SeedRecord {
            site_name: "hub".to_owned(),
            key_sha256: key.to_owned(),
            generation: 1,
            issued_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        assert!(seed_is_current(Some(&seed("k1")), "hub", "k1", false));
        assert!(!seed_is_current(Some(&seed("k0")), "hub", "k1", false), "a stale key");
        assert!(
            !seed_is_current(Some(&seed("k1")), "other", "k1", false),
            "another site"
        );
        assert!(!seed_is_current(Some(&seed("k1")), "hub", "k1", true), "a new identity");
        assert!(!seed_is_current(None, "hub", "k1", false), "no seed");
    }

    #[test]
    fn an_authoritative_identity_without_its_key_is_issued_again() {
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let keyless = SecretView {
            key_matches: false,
            ..identity(Some(MANAGED_BY), &ca)
        };
        assert_eq!(
            identity_action(Some(&keyless), &ca.cert_pem, "hub", false),
            SiteIdentity::Issue { replace: true }
        );
    }

    #[test]
    fn an_authoritative_identity_from_another_ca_refuses_before_any_write() {
        let gone = certs::generate_ca("grid-ca").expect("gone");
        let grid = certs::generate_ca("grid-ca").expect("grid");
        let args = hub_args(&[]);
        let refused = |action: enrollment::CaAction| matches!(action, enrollment::CaAction::Refuse(_));
        let orphan = identity(Some(MANAGED_BY), &gone);
        assert!(
            refused(plan_ca(&args, None, None, None, Some(&orphan)).expect("plan")),
            "nothing left but an identity: its CA is out there"
        );
        let bundle = view(Some(MANAGED_BY), &grid.cert_pem);
        assert!(
            refused(plan_ca(&args, Some(&grid.cert_pem), Some(&bundle), None, Some(&orphan)).expect("plan")),
            "the hub's identity came from another CA"
        );
    }

    #[test]
    fn a_surviving_enrolled_hub_ca_refuses_a_new_one_and_admits_its_restored_key() {
        let grid = certs::generate_ca("grid-ca").expect("grid");
        let hub = view(Some("grid-operator"), &grid.cert_pem);
        let args = hub_args(&[]);
        assert!(
            matches!(
                plan_ca(&args, None, None, Some(&hub), None).expect("plan"),
                enrollment::CaAction::Refuse(_)
            ),
            "the key and bundle are gone, but the hub still uses the CA"
        );
        assert_eq!(
            plan_ca(&args, Some(&grid.cert_pem), None, Some(&hub), None).expect("plan"),
            enrollment::CaAction::Load,
            "restoring the key recovers"
        );
        assert_eq!(hub_ca_write(Some(&hub), &grid.cert_pem, false), HubCa::Keep);
    }

    #[test]
    fn a_wrong_backup_or_an_unreadable_copy_refuses_and_a_rotation_bundle_does_not() {
        let grid = certs::generate_ca("grid-ca").expect("grid");
        let other = certs::generate_ca("grid-ca").expect("other");
        let old = certs::generate_ca("grid-ca").expect("old");
        let args = hub_args(&[]);
        let bundle = view(Some(MANAGED_BY), &grid.cert_pem);
        let refused = |action: enrollment::CaAction| matches!(action, enrollment::CaAction::Refuse(_));
        assert!(
            refused(plan_ca(&args, Some(&other.cert_pem), Some(&bundle), None, None).expect("plan")),
            "another CA's key"
        );
        let garbage = view(Some(MANAGED_BY), "not pem");
        assert!(
            refused(plan_ca(&args, None, Some(&garbage), None, None).expect("plan")),
            "an unreadable bundle is not an absent one"
        );
        let rotating = view(Some(MANAGED_BY), &format!("{}{}", old.cert_pem, grid.cert_pem));
        assert_eq!(
            plan_ca(&args, Some(&grid.cert_pem), Some(&bundle), Some(&rotating), None).expect("plan"),
            enrollment::CaAction::Load,
            "a hub bundle holding the replaced CA too still holds this one"
        );
    }

    #[test]
    fn force_regenerate_mints_without_reading_a_corrupt_key() {
        let grid = certs::generate_ca("grid-ca").expect("grid");
        let bundle = view(Some(MANAGED_BY), &grid.cert_pem);
        let args = hub_args(&["--force-regenerate"]);
        assert_eq!(
            plan_ca(&args, Some("corrupt"), Some(&bundle), None, None).expect("plan"),
            enrollment::CaAction::Mint
        );
        assert_eq!(hub_ca_write(Some(&bundle), &grid.cert_pem, true), HubCa::Write);
    }

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
