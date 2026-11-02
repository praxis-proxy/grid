//! Serves the enrollment interface.

use std::{net::SocketAddr, str::FromStr as _, sync::Arc, time::Duration};

use axum_server::Handle;
use enrollment::{
    AppState, GridAdmins, SharedCa, Store,
    authz::Authorizer,
    ca::{Anchored, CONFIRM_ROTATION_VAR, RecordedCa},
    router,
    tls::{PeerAcceptor, TlsAcceptor, TlsConfig},
};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use tokio::signal;

/// The `enrollment bootstrap` subcommand: mint or load the Grid CA and write it
/// as Secrets for a pre-install Job. Off by default so the serving binary carries
/// no CLI or Kubernetes client.
#[cfg(feature = "bootstrap")]
mod bootstrap;
/// The `enrollment invite` subcommand.
#[cfg(feature = "bootstrap")]
mod invite;

/// How long in-flight requests are given to finish once a shutdown signal lands.
/// Kept below the Kubernetes default 30s termination grace period, so the drain
/// has margin before SIGKILL rather than racing it.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(25);
/// How often the server certificate is reloaded from disk, so a rotated secret
/// is picked up without a restart.
const TLS_RELOAD_INTERVAL: Duration = Duration::from_secs(60);

/// Where the CA that signs enrolled certificates is read from.
const CA_CERT_PATH: &str = "ENROLLMENT_CA_CERT";
/// The CA private key.
const CA_KEY_PATH: &str = "ENROLLMENT_CA_KEY";
/// Address to listen on.
const LISTEN_ADDR: &str = "ENROLLMENT_LISTEN_ADDR";
/// Common name recorded for the CA when loading it.
const CA_COMMON_NAME: &str = "ENROLLMENT_CA_COMMON_NAME";
/// Table of grid-admins allowed to mint and revoke site tokens.
const GRID_ADMIN_TOKENS: &str = "ENROLLMENT_GRID_ADMIN_TOKENS";
/// Path to the optional role table. Roles live apart from the tokens so a
/// token's meaning never depends on the token's own characters.
const GRID_ADMIN_ROLES: &str = "ENROLLMENT_GRID_ADMIN_ROLES";
/// How many seconds an issued certificate lasts.
const CERT_LIFETIME_SECS: &str = "ENROLLMENT_CERT_LIFETIME_SECS";
/// Set to `false` to refuse every renewal.
const RENEWALS_ENABLED: &str = "ENROLLMENT_ROTATION_ENABLED";
/// Comma-separated site names issued outside enrollment, refused at mint and redeem.
const RESERVED_SITES: &str = "ENROLLMENT_RESERVED_SITES";
/// Whether each enrollment is recorded as a `CertificateSigningRequest`.
#[cfg(feature = "csr-record")]
const CSR_RECORD: &str = "ENROLLMENT_CSR_RECORD";
/// The signer name records are filed under.
#[cfg(feature = "csr-record")]
const CSR_SIGNER_NAME: &str = "ENROLLMENT_CSR_SIGNER_NAME";
/// Directory of reserved-name seeds bootstrap signed with the CA key.
const RESERVED_SEEDS_DIR: &str = "ENROLLMENT_RESERVED_SEEDS_DIR";
/// Server certificate presented to callers, PEM.
const TLS_CERT_PATH: &str = "ENROLLMENT_TLS_CERT";
/// Private key for the server certificate, PEM.
const TLS_KEY_PATH: &str = "ENROLLMENT_TLS_KEY";
/// Postgres connection URL.
///
/// Named to match MaaS, which carries it under this key in the `maas-db-config`
/// secret, so a deployment beside MaaS points at the database already there.
const DB_CONNECTION_URL: &str = "DB_CONNECTION_URL";

/// Reject a hook subcommand on a serve-only build.
#[cfg(not(feature = "bootstrap"))]
fn reject_bootstrap_without_feature(
    args: impl IntoIterator<Item = std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(command) = args
        .into_iter()
        .nth(1)
        .filter(|arg| arg == "bootstrap" || arg == "invite")
    {
        return Err(format!(
            "`enrollment {}` is unavailable: this image was built \
             without --features bootstrap (use deploy/enrollment/Containerfile)",
            command.to_string_lossy()
        )
        .into());
    }
    Ok(())
}

/// Whether the first argument asks for the version.
///
/// Answered before the subscriber is installed and before any subcommand
/// dispatch, so `--version` prints the one line and nothing else. The server
/// has no top-level clap command to hang `version` on, so this mirrors what the
/// gateway does in its preflight.
fn version_requested() -> bool {
    std::env::args().skip(1).any(|arg| arg == "--version" || arg == "-V")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if version_requested() {
        use std::io::Write as _;
        writeln!(std::io::stdout().lock(), "grid-enrollment {}", version::get())?;
        return Ok(());
    }
    tracing_subscriber::fmt::init();
    version::get().log_startup("grid-enrollment");

    // `enrollment bootstrap` runs the one-shot CA/serving-Secret init and exits.
    // No server is started.
    #[cfg(feature = "bootstrap")]
    if std::env::args_os().nth(1).is_some_and(|arg| arg == "bootstrap") {
        return Box::pin(bootstrap::run())
            .await
            .map_err(|err| -> Box<dyn std::error::Error> { err });
    }
    #[cfg(feature = "bootstrap")]
    if std::env::args_os().nth(1).is_some_and(|arg| arg == "invite") {
        return Box::pin(invite::run())
            .await
            .map_err(|err| -> Box<dyn std::error::Error> { err });
    }
    #[cfg(not(feature = "bootstrap"))]
    reject_bootstrap_without_feature(std::env::args_os())?;

    let ca_cert_path = std::env::var(CA_CERT_PATH).map_err(|error| format!("{CA_CERT_PATH}: {error}"))?;
    let ca_key_path = std::env::var(CA_KEY_PATH).map_err(|error| format!("{CA_KEY_PATH}: {error}"))?;
    let common_name = std::env::var(CA_COMMON_NAME).unwrap_or_else(|_unset| "grid-ca".to_owned());
    let listen = std::env::var(LISTEN_ADDR).unwrap_or_else(|_unset| "0.0.0.0:8443".to_owned());

    let ca = certs::load_ca(
        &common_name,
        &std::fs::read_to_string(&ca_key_path)?,
        &std::fs::read_to_string(&ca_cert_path)?,
    )?;

    let tls_cert = std::env::var(TLS_CERT_PATH)
        .map_err(|_unset| format!("{TLS_CERT_PATH} is required: the CA-signing service must not serve in the clear"))?;
    let tls_key = std::env::var(TLS_KEY_PATH)
        .map_err(|_unset| format!("{TLS_KEY_PATH} is required: the CA-signing service must not serve in the clear"))?;
    let tls = load_tls(&tls_cert, &tls_key, &ca.cert_pem)?;

    let store = open_store().await?;
    let confirm = std::env::var(CONFIRM_ROTATION_VAR).ok();
    // Boxed: kept off the startup stack frame.
    Box::pin(anchor_ca(&store, &ca.cert_pem, confirm.as_deref())).await?;

    // Boxed: the Kubernetes-RBAC authorizer builds a large future under the sar
    // feature, kept off the startup stack frame.
    let authorizer = Box::pin(build_authorizer()).await?;
    #[cfg(feature = "csr-record")]
    let recorder = load_recorder(&authorizer).await?;
    let state = Arc::new(AppState {
        store,
        ca: SharedCa::new(ca),
        authorizer,
        cert_lifetime: load_cert_lifetime(),
        reserved_sites: load_reserved_sites()?,
        renewals_enabled: load_renewals_enabled()?,
        #[cfg(feature = "csr-record")]
        recorder,
    });

    #[cfg(feature = "site-invite-controller")]
    Box::pin(start_site_invite_controller(Arc::clone(&state))).await?;

    // Reload the server certificate on an interval so a rotated TLS secret is
    // served without a restart. A failed reload keeps the current certificate.
    tokio::spawn(reload_tls(tls.clone(), tls_cert, tls_key, Arc::clone(&state)));
    // Reload the signing CA the same way, so a regenerated or restored CA signs
    // new site certificates without a restart.
    tokio::spawn(reload_ca(
        Arc::clone(&state),
        confirm,
        common_name,
        ca_cert_path,
        ca_key_path,
    ));
    // Apply reserved-name seeds now and as bootstrap rewrites them.
    if let Ok(dir) = std::env::var(RESERVED_SEEDS_DIR) {
        let mut reported = std::collections::HashMap::new();
        Box::pin(enrollment::seed::apply(
            &state,
            std::path::Path::new(&dir),
            &mut reported,
        ))
        .await;
        tokio::spawn(reapply_seeds(Arc::clone(&state), dir, reported));
    }

    // Drain in-flight requests on SIGTERM or SIGINT rather than cutting them off,
    // so a rolling deploy does not abort an enrollment mid-issue.
    let handle = Handle::new();
    tokio::spawn(shutdown_signal(handle.clone()));

    let addr: SocketAddr = listen.parse()?;
    tracing::info!(%addr, "enrollment service listening over https");
    // The acceptor hands each connection's verified client leaf to the renewal handler.
    let server = axum_server::bind(addr).acceptor(PeerAcceptor(TlsAcceptor::new(tls)));
    Box::pin(server.handle(handle).serve(router(state).into_make_service())).await?;
    Ok(())
}

/// Start the `SiteInvite` controller only when the service runs in Kubernetes.
#[cfg(feature = "site-invite-controller")]
async fn start_site_invite_controller(state: Arc<AppState>) -> Result<(), String> {
    if std::env::var_os("KUBERNETES_SERVICE_HOST").is_none() {
        return Ok(());
    }
    let client = Box::pin(kube::Client::try_default()).await.map_err(|error| {
        tracing::warn!(%error, "SiteInvite controller unavailable; enrollment will continue without it");
    });
    let Ok(client) = client else {
        return Ok(());
    };
    let namespace = std::env::var("ENROLLMENT_INVITE_SECRET_NAMESPACE")
        .or_else(|_| std::env::var("POD_NAMESPACE"))
        .map_err(|error| {
            tracing::warn!(%error, "SiteInvite controller unavailable; enrollment will continue without it");
        });
    let Ok(namespace) = namespace else {
        return Ok(());
    };
    tokio::spawn(enrollment::site_invite::run_when_available(client, state, namespace));
    Ok(())
}

/// Trigger a graceful drain when the process is asked to stop.
///
/// Waits for SIGINT (Ctrl-C) or, on Unix, SIGTERM (the signal Kubernetes sends on
/// pod termination), then gives in-flight requests [`DRAIN_TIMEOUT`] to finish.
async fn shutdown_signal(handle: Handle<SocketAddr>) {
    let interrupt = async {
        if signal::ctrl_c().await.is_err() {
            tracing::error!("could not listen for SIGINT");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            },
            Err(err) => tracing::error!(%err, "could not listen for SIGTERM"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = interrupt => {},
        () = terminate => {},
    }

    tracing::info!(
        drain_secs = DRAIN_TIMEOUT.as_secs(),
        "shutdown signal received, draining"
    );
    handle.graceful_shutdown(Some(DRAIN_TIMEOUT));
}

/// Reload the server certificate from disk on an interval.
///
/// A rotated TLS secret is picked up in place. A reload that fails, for instance
/// a half-written file, is logged and the current certificate stays in use.
#[expect(
    clippy::infinite_loop,
    reason = "a background reloader runs for the life of the process"
)]
async fn reload_tls(config: TlsConfig, cert_path: String, key_path: String, state: Arc<AppState>) {
    let mut ticker = tokio::time::interval(TLS_RELOAD_INTERVAL);
    let mut last_expiry_warning: Option<std::time::Instant> = None;
    loop {
        ticker.tick().await;
        warn_if_serving_cert_expiring(&cert_path, &mut last_expiry_warning).await;
        // Rebuilt with the current CA too, so client certificates follow a CA swap.
        let ca_pem = state.ca.current().cert_pem.clone();
        let reloaded = enrollment::tls::reload(
            &config,
            std::path::Path::new(&cert_path),
            std::path::Path::new(&key_path),
            &ca_pem,
        );
        if let Err(err) = reloaded {
            tracing::warn!(%err, "server certificate reload failed, keeping the current certificate");
        }
    }
}

/// Refuse a CA other than the one the database records, since bootstrap's guard reads deletable Secrets.
async fn anchor_ca(store: &Store, cert_pem: &str, confirm: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let fingerprint = certs::cert_public_key_sha256(cert_pem)?;
    let anchored = enrollment::ca::anchor(store, &fingerprint, confirm).await?;
    if let Anchored::Replaced { previous } = &anchored {
        tracing::warn!(
            old_fingerprint = %previous,
            new_fingerprint = %fingerprint,
            "grid CA replaced as {CONFIRM_ROTATION_VAR} confirmed; every site must re-enroll"
        );
    } else {
        tracing::info!(%fingerprint, ?anchored, "grid CA checked against the database record");
    }
    Ok(())
}

/// Reload the signing CA from disk on an interval, swapping it in when its
/// certificate changes. A reload that fails keeps the current CA.
#[expect(
    clippy::infinite_loop,
    reason = "a background reloader runs for the life of the process"
)]
async fn reload_ca(
    state: Arc<AppState>,
    confirm: Option<String>,
    common_name: String,
    cert_path: String,
    key_path: String,
) {
    let record = RecordedCa {
        store: &state.store,
        confirm: confirm.as_deref(),
    };
    let mut ticker = tokio::time::interval(TLS_RELOAD_INTERVAL);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let reloaded = Box::pin(state.ca.reload_from_files(
            record,
            &common_name,
            std::path::Path::new(&cert_path),
            std::path::Path::new(&key_path),
        ))
        .await;
        match reloaded {
            Ok(Some((old, new))) => tracing::warn!(
                old_fingerprint = %old,
                new_fingerprint = %new,
                "signing CA changed on disk; new site certificates are signed by the new CA"
            ),
            Ok(None) => {},
            Err(err) => tracing::error!(%err, "signing CA reload refused, keeping the current CA"),
        }
    }
}

/// Apply reserved-name seeds on an interval, as bootstrap rewrites them.
#[expect(
    clippy::infinite_loop,
    reason = "a background reloader runs for the life of the process"
)]
async fn reapply_seeds(state: Arc<AppState>, dir: String, mut reported: std::collections::HashMap<String, String>) {
    let mut ticker = tokio::time::interval(TLS_RELOAD_INTERVAL);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        Box::pin(enrollment::seed::apply(
            &state,
            std::path::Path::new(&dir),
            &mut reported,
        ))
        .await;
    }
}

/// Serving cert expiry this close raises a warning, matching bootstrap's renewal window.
const EXPIRY_WARNING_WINDOW: time::Duration = time::Duration::days(30);
/// Repeat the expiry warning at most this often.
const EXPIRY_WARNING_EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// Warn, at most daily, when the serving cert is within [`EXPIRY_WARNING_WINDOW`]
/// of expiry: bootstrap renews it only on an install or upgrade.
async fn warn_if_serving_cert_expiring(cert_path: &str, last: &mut Option<std::time::Instant>) {
    if last.is_some_and(|at| at.elapsed() < EXPIRY_WARNING_EVERY) {
        return;
    }
    let Ok(cert) = tokio::fs::read_to_string(cert_path).await else {
        return;
    };
    if certs::cert_expires_within(&cert, EXPIRY_WARNING_WINDOW).unwrap_or(false) {
        let not_after = certs::cert_issuer_and_expiry(&cert).map_or_else(|_bad| "unknown".to_owned(), |(_, at)| at);
        tracing::warn!(
            %not_after,
            "enrollment serving certificate expires within 30 days; run helm upgrade to renew it"
        );
        *last = Some(std::time::Instant::now());
    }
}

/// Load the mandatory server TLS material, failing closed when it is absent.
///
/// The service signs CSRs with the grid CA, so it must not serve in the clear. A
/// missing certificate or key returns an error here, before any socket is bound,
/// the same fail-closed posture as the CA material.
#[cfg(not(feature = "fips"))]
fn load_tls(cert: &str, key: &str, grid_ca_pem: &str) -> Result<TlsConfig, Box<dyn std::error::Error>> {
    // Install the ring provider process-wide, as the operator does, before rustls
    // builds any configuration.
    if rustls::crypto::ring::default_provider().install_default().is_err() {
        tracing::debug!("a rustls crypto provider was already installed");
    }
    Ok(enrollment::tls::server_config(
        std::path::Path::new(cert),
        std::path::Path::new(key),
        grid_ca_pem,
    )?)
}

/// Load the mandatory server TLS material through system openssl (fips build).
///
/// The openssl acceptor builds synchronously, with no provider to install.
#[cfg(feature = "fips")]
fn load_tls(cert: &str, key: &str, grid_ca_pem: &str) -> Result<TlsConfig, Box<dyn std::error::Error>> {
    Ok(enrollment::tls::server_config(
        std::path::Path::new(cert),
        std::path::Path::new(key),
        grid_ca_pem,
    )?)
}

/// Open the store the enrollment record lives in.
///
/// Falls back to keeping requests in this process, which loses them on restart
/// and shares nothing between replicas. That suits a local trial and nothing
/// else, so it says so.
async fn open_store() -> Result<Store, Box<dyn std::error::Error>> {
    match std::env::var(DB_CONNECTION_URL) {
        Ok(url) => {
            require_db_tls(&url)?;
            let store = Store::postgres(&url).await?;
            tracing::info!("enrollment records are kept in Postgres");
            Ok(store)
        },
        Err(_unset) => {
            tracing::warn!(
                "{DB_CONNECTION_URL} is not set, so enrollment records are kept in memory and lost on restart"
            );
            Ok(Store::memory())
        },
    }
}

/// Refuse a Postgres URL whose server certificate is not fully verified.
///
/// Parsed by the connection's own parser, so every spelling is judged as it will connect.
fn require_db_tls(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    if !matches!(PgConnectOptions::from_str(url)?.get_ssl_mode(), PgSslMode::VerifyFull) {
        return Err(format!(
            "{DB_CONNECTION_URL} must set sslmode=verify-full with a CA bundle (sslrootcert): \
             a lesser mode leaves the server certificate unverified"
        )
        .into());
    }
    Ok(())
}

/// Read the reserved site names, refusing one that is not a valid site name.
fn load_reserved_sites() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let raw = std::env::var(RESERVED_SITES).unwrap_or_default();
    let sites: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|site| !site.is_empty())
        .map(str::to_owned)
        .collect();
    for site in &sites {
        certs::validate_site_name(site).map_err(|err| format!("{RESERVED_SITES} entry {site:?}: {err}"))?;
    }
    if !sites.is_empty() {
        tracing::info!(?sites, "site names reserved from enrollment");
    }
    Ok(sites)
}

/// A `true` or `false` variable, `default` when unset or empty.
fn parse_bool_env(name: &str, raw: Option<&str>, default: bool) -> Result<bool, String> {
    match raw.map(str::trim) {
        None | Some("") => Ok(default),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(other) => Err(format!("{name} must be true or false, not {other:?}")),
    }
}

/// Build the recorder when enrollments are to be recorded, sharing the
/// authorizer's API server client when it has one.
#[cfg(feature = "csr-record")]
async fn load_recorder(authorizer: &Authorizer) -> Result<Option<Arc<enrollment::record::Recorder>>, String> {
    if !parse_bool_env(CSR_RECORD, std::env::var(CSR_RECORD).ok().as_deref(), false)? {
        return Ok(None);
    }
    let signer_name = csr_signer_name(std::env::var(CSR_SIGNER_NAME).ok().as_deref())?;
    let client = match kube_client_of(authorizer) {
        Some(client) => client,
        None => kube::Client::try_default()
            .await
            .map_err(|error| format!("{CSR_RECORD}=true needs a Kubernetes client: {error}"))?,
    };
    let recorder = enrollment::record::Recorder::new(client, signer_name);
    warn_missing_access(&recorder).await;
    tracing::info!(
        signer = recorder.signer_name(),
        "enrollments recorded as CertificateSigningRequests"
    );
    Ok(Some(Arc::new(recorder)))
}

/// Say at startup which rights a record would lack, so a missing role shows
/// before the first enrollment rather than in its log line.
#[cfg(feature = "csr-record")]
async fn warn_missing_access(recorder: &enrollment::record::Recorder) {
    match Box::pin(recorder.missing_access()).await {
        Ok(missing) if missing.is_empty() => {},
        Ok(missing) => tracing::warn!(?missing, "records will fail: this identity lacks these rights"),
        Err(error) => tracing::warn!(%error, "could not check the record's rights at startup"),
    }
}

/// The signer name to file under: the variable, else the default; domain-qualified.
#[cfg(feature = "csr-record")]
fn csr_signer_name(raw: Option<&str>) -> Result<String, String> {
    let name = raw
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or(enrollment::record::DEFAULT_SIGNER_NAME);
    if name.contains('/') {
        Ok(name.to_owned())
    } else {
        Err(format!(
            "{CSR_SIGNER_NAME} must be a domain-qualified signer name, not {name:?}"
        ))
    }
}

/// The authorizer's API server client, when it has one, so both share a pool.
#[cfg(feature = "csr-record")]
fn kube_client_of(authorizer: &Authorizer) -> Option<kube::Client> {
    match authorizer {
        #[cfg(feature = "sar")]
        Authorizer::Kube(kube) => Some(kube.client()),
        _other => None,
    }
}

/// Whether renewals are signed: on unless the variable says `false`.
fn load_renewals_enabled() -> Result<bool, String> {
    let enabled = parse_bool_env(RENEWALS_ENABLED, std::env::var(RENEWALS_ENABLED).ok().as_deref(), true)?;
    if !enabled {
        tracing::info!("rotation disabled: every rotation is refused; issued identities stay valid until they expire");
    }
    Ok(enabled)
}

/// Read how long issued certificates should last.
///
/// Expiry is the only thing that removes a member, so this is what bounds how
/// long a decision to admit someone stays in force.
fn load_cert_lifetime() -> time::Duration {
    let configured = std::env::var(CERT_LIFETIME_SECS)
        .ok()
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|secs| *secs > 0)
        .map(time::Duration::seconds);

    let lifetime = configured.unwrap_or(certs::DEFAULT_SITE_CERT_LIFETIME);
    tracing::info!(
        seconds = lifetime.whole_seconds(),
        "issued certificates expire after this"
    );
    lifetime
}

/// A file's contents, `None` when it is not there.
fn read_optional(path: &str) -> Result<Option<String>, std::io::Error> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(absent) if absent.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Read the grid-admin token table.
///
/// No table means nobody can mint, rather than anybody.
fn load_grid_admins() -> Result<GridAdmins, Box<dyn std::error::Error>> {
    let admins = match std::env::var(GRID_ADMIN_TOKENS) {
        Ok(path) => {
            let admins = GridAdmins::from_table(&std::fs::read_to_string(&path)?)?;
            match std::env::var(GRID_ADMIN_ROLES)
                .ok()
                .map(|roles| read_optional(&roles))
                .transpose()?
            {
                // A Secret without the key mounts no file, so absent means no roles.
                Some(Some(roles)) => admins.with_roles(&roles).map_err(|error| error.to_string())?,
                Some(None) | None => admins,
            }
        },
        Err(_unset) => GridAdmins::default(),
    };

    if admins.is_empty() {
        tracing::warn!(
            "no grid-admin tokens configured, so no site token can be minted: set {GRID_ADMIN_TOKENS} to a file of name:token lines"
        );
    } else {
        tracing::info!(grid_admins = admins.len(), "grid-admin tokens loaded");
    }
    Ok(admins)
}

/// Build the grid-admin authorization backend.
///
/// Defaults to the grid-admin token table. Built with `--features sar` and
/// `ENROLLMENT_AUTHZ=kube`, it reuses Kubernetes RBAC (`TokenReview` +
/// `SubjectAccessReview`) instead, the `FlightCtl` pattern. Only the token's
/// origin and who decides differ between the two.
#[cfg_attr(
    not(feature = "sar"),
    expect(clippy::unused_async, reason = "async only when the sar backend is built")
)]
async fn build_authorizer() -> Result<Authorizer, Box<dyn std::error::Error>> {
    // Read the choice unconditionally and fail closed: a request for a backend
    // this binary cannot provide (`kube` without `--features sar`, or an
    // unrecognized value) must not silently fall back to the token table while
    // believing something stronger is deciding.
    match std::env::var("ENROLLMENT_AUTHZ").ok().as_deref() {
        None | Some("" | "local") => {
            tracing::info!("grid-admin authorization: grid-admin token table");
            Ok(Authorizer::Local(load_grid_admins()?))
        },
        Some("kube") => {
            #[cfg(feature = "sar")]
            {
                let audience = std::env::var("ENROLLMENT_TOKEN_AUDIENCE")
                    .unwrap_or_else(|_| enrollment::authz::DEFAULT_TOKEN_AUDIENCE.to_owned());
                let kube = enrollment::authz::KubeAuthorizer::connect(audience.clone())
                    .await
                    .map_err(std::io::Error::other)?;
                tracing::info!(
                    %audience,
                    namespace = kube.namespace(),
                    "grid-admin authorization: Kubernetes RBAC (SubjectAccessReview)"
                );
                Ok(Authorizer::Kube(kube))
            }
            #[cfg(not(feature = "sar"))]
            Err("ENROLLMENT_AUTHZ=kube requires a build with --features sar".into())
        },
        Some(other) => Err(format!("unknown ENROLLMENT_AUTHZ={other:?}; expected \"local\" or \"kube\"").into()),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "bootstrap"))]
    use super::reject_bootstrap_without_feature;
    use super::{anchor_ca, parse_bool_env, require_db_tls};

    #[cfg(feature = "csr-record")]
    #[test]
    fn the_signer_name_defaults_and_must_be_qualified() {
        assert_eq!(super::csr_signer_name(None).as_deref(), Ok("grid.praxis.fast/site"));
        assert_eq!(
            super::csr_signer_name(Some(" ")).as_deref(),
            Ok("grid.praxis.fast/site")
        );
        assert_eq!(
            super::csr_signer_name(Some("example.com/x")).as_deref(),
            Ok("example.com/x")
        );
        assert!(matches!(super::csr_signer_name(Some("plain")), Err(message) if message.contains("plain")));
    }

    #[test]
    fn a_bool_variable_takes_its_default_when_unset() {
        assert_eq!(parse_bool_env("X", None, false), Ok(false));
        assert_eq!(parse_bool_env("X", Some(""), true), Ok(true));
        assert_eq!(parse_bool_env("X", Some(" true "), false), Ok(true));
        assert_eq!(parse_bool_env("X", Some("false"), true), Ok(false));
        assert!(matches!(parse_bool_env("X", Some("yes"), false), Err(message) if message.contains("yes")));
    }

    #[cfg(not(feature = "bootstrap"))]
    #[test]
    fn bootstrap_without_the_feature_is_rejected() {
        let args = ["enrollment", "bootstrap"].map(std::ffi::OsString::from);
        let result = reject_bootstrap_without_feature(args);
        assert!(
            matches!(&result, Err(error) if error.to_string().contains("--features bootstrap")),
            "a serve-only build must reject bootstrap and name the feature: {result:?}"
        );
    }

    #[cfg(not(feature = "bootstrap"))]
    #[test]
    fn a_non_bootstrap_invocation_is_allowed() -> Result<(), Box<dyn std::error::Error>> {
        let args = ["enrollment"].map(std::ffi::OsString::from);
        reject_bootstrap_without_feature(args)
    }

    #[tokio::test]
    async fn a_freshly_minted_ca_does_not_start_against_a_recorded_one() -> Result<(), Box<dyn std::error::Error>> {
        let store = enrollment::Store::memory();
        let original = certs::generate_ca("grid-ca")?;
        anchor_ca(&store, &original.cert_pem, None).await?;
        let minted = certs::generate_ca("grid-ca")?;
        let refused = anchor_ca(&store, &minted.cert_pem, None).await;
        assert!(refused.is_err(), "a different CA must not start");
        let old = certs::cert_public_key_sha256(&original.cert_pem)?;
        anchor_ca(&store, &minted.cert_pem, Some(&old)).await
    }

    #[test]
    fn a_url_short_of_verify_full_is_refused_in_every_build() {
        // These fall back to PGSSLMODE, as the connection does, so judge them only without it.
        if std::env::var_os("PGSSLMODE").is_none() {
            for url in [
                "postgres://u:p@h/db",
                "postgres://u:p@h/db?SSLMODE=verify-full",
                "postgres://u:p@h/db?sslmode=",
            ] {
                assert!(require_db_tls(url).is_err(), "{url} must be refused");
            }
        }
        for url in [
            "postgres://u:p@h/db?sslmode=disable",
            "postgres://u:p@h/db?sslmode=allow",
            "postgres://u:p@h/db?sslmode=prefer",
            "postgres://u:p@h/db?sslmode=require",
            "postgres://u:p@h/db?sslmode=verify-ca",
            "postgres://u:p@h/db?sslmode=Require",
            "postgres://u:p@h/db?sslmode=VERIFY-CA",
            "postgres://u:p@h/db?sslmode=verify-full&sslmode=require",
            "postgres://u:p@h/db?sslmode=verify-full&ssl-mode=verify-ca",
            "postgres://u:p@h/db?ssl-mode=require",
            "postgres://u:p@h/db?sslmode=%72equire",
            "postgres://u:p@h/db?sslmode=verify-full%20",
        ] {
            assert!(require_db_tls(url).is_err(), "{url} must be refused");
        }
    }

    #[test]
    fn verify_full_is_accepted() {
        for url in [
            "postgres://u:p@h/db?sslmode=verify-full",
            "postgres://u:p@h/db?ssl-mode=verify-full",
            "postgres://u:p@h/db?sslmode=require&sslmode=verify-full",
        ] {
            assert!(require_db_tls(url).is_ok(), "{url} fully verifies the server");
        }
    }
}
