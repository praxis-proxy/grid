//! Serves the enrollment interface.

use std::{net::SocketAddr, str::FromStr as _, sync::Arc, time::Duration};

use axum_server::Handle;
use enrollment::{AppState, GridAdmins, Store, authz::Authorizer, router};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use tokio::signal;

/// Server TLS config: rustls by default, system openssl under `fips`.
#[cfg(not(feature = "fips"))]
type TlsConfig = axum_server::tls_rustls::RustlsConfig;
/// Server TLS config: rustls by default, system openssl under `fips`.
#[cfg(feature = "fips")]
type TlsConfig = axum_server::tls_openssl::OpenSSLConfig;

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
/// How many seconds an issued certificate lasts.
const CERT_LIFETIME_SECS: &str = "ENROLLMENT_CERT_LIFETIME_SECS";
/// Server certificate presented to callers, PEM.
const TLS_CERT_PATH: &str = "ENROLLMENT_TLS_CERT";
/// Private key for the server certificate, PEM.
const TLS_KEY_PATH: &str = "ENROLLMENT_TLS_KEY";
/// Postgres connection URL.
///
/// Named to match MaaS, which carries it under this key in the `maas-db-config`
/// secret, so a deployment beside MaaS points at the database already there.
const DB_CONNECTION_URL: &str = "DB_CONNECTION_URL";

#[tokio::main]
#[expect(
    clippy::too_many_lines,
    reason = "startup wires config, TLS, store, authz, drain, and cert reload in one sequence"
)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let ca_cert_path = std::env::var(CA_CERT_PATH)?;
    let ca_key_path = std::env::var(CA_KEY_PATH)?;
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
    let tls = load_tls(&tls_cert, &tls_key).await?;

    let state = Arc::new(AppState {
        store: open_store().await?,
        ca,
        // Boxed: the Kubernetes-RBAC authorizer builds a large future under the sar
        // feature, kept off the startup stack frame.
        authorizer: Box::pin(build_authorizer()).await?,
        cert_lifetime: load_cert_lifetime(),
    });

    // Reload the server certificate on an interval so a rotated TLS secret is
    // served without a restart. A failed reload keeps the current certificate.
    tokio::spawn(reload_tls(tls.clone(), tls_cert, tls_key));

    // Drain in-flight requests on SIGTERM or SIGINT rather than cutting them off,
    // so a rolling deploy does not abort an enrollment mid-issue.
    let handle = Handle::new();
    tokio::spawn(shutdown_signal(handle.clone()));

    let addr: SocketAddr = listen.parse()?;
    tracing::info!(%addr, "enrollment service listening over https");
    #[cfg(not(feature = "fips"))]
    let server = axum_server::bind_rustls(addr, tls);
    #[cfg(feature = "fips")]
    let server = axum_server::bind_openssl(addr, tls);
    Box::pin(server.handle(handle).serve(router(state).into_make_service())).await?;
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
async fn reload_tls(config: TlsConfig, cert_path: String, key_path: String) {
    let mut ticker = tokio::time::interval(TLS_RELOAD_INTERVAL);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        // rustls reloads asynchronously, the openssl acceptor reload synchronously.
        #[cfg(not(feature = "fips"))]
        let reloaded = config.reload_from_pem_file(&cert_path, &key_path).await;
        #[cfg(feature = "fips")]
        let reloaded = config.reload_from_pem_file(&cert_path, &key_path);
        if let Err(err) = reloaded {
            tracing::warn!(%err, "server certificate reload failed, keeping the current certificate");
        }
    }
}

/// Load the mandatory server TLS material, failing closed when it is absent.
///
/// The service signs CSRs with the grid CA, so it must not serve in the clear. A
/// missing certificate or key returns an error here, before any socket is bound,
/// the same fail-closed posture as the CA material.
#[cfg(not(feature = "fips"))]
async fn load_tls(cert: &str, key: &str) -> Result<TlsConfig, Box<dyn std::error::Error>> {
    // Install the ring provider process-wide, as the operator does, before rustls
    // builds any configuration.
    if rustls::crypto::ring::default_provider().install_default().is_err() {
        tracing::debug!("a rustls crypto provider was already installed");
    }
    Ok(Box::pin(axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)).await?)
}

/// Load the mandatory server TLS material through system openssl (fips build).
///
/// The openssl acceptor builds synchronously, with no provider to install.
#[cfg(feature = "fips")]
#[expect(
    clippy::unused_async,
    reason = "matches the default build's async load_tls signature"
)]
async fn load_tls(cert: &str, key: &str) -> Result<TlsConfig, Box<dyn std::error::Error>> {
    Ok(axum_server::tls_openssl::OpenSSLConfig::from_pem_file(cert, key)?)
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

/// Refuse a Postgres URL that could cross the network in plaintext.
///
/// The store holds token digests, pinned names, and grid-admin identities, so the
/// database hop must be encrypted. sslmode disable, allow, and prefer permit a
/// plaintext fallback and are rejected before any connection. A fips build then
/// requires verify-full and fails closed on anything less, since the FIPS posture
/// rests on a fully verified server. A default build accepts require and verify-ca
/// with a warning that verify-full is the intended posture.
fn require_db_tls(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mode = PgConnectOptions::from_str(url)?.get_ssl_mode();
    if matches!(mode, PgSslMode::Disable | PgSslMode::Allow | PgSslMode::Prefer) {
        return Err(format!(
            "{DB_CONNECTION_URL} must require TLS: set sslmode=verify-full, or at least require. disable, allow, and prefer permit a plaintext fallback"
        )
        .into());
    }
    if !matches!(mode, PgSslMode::VerifyFull) {
        #[cfg(feature = "fips")]
        return Err(format!(
            "{DB_CONNECTION_URL} must set sslmode=verify-full in a fips build: a lesser mode leaves the server certificate unverified"
        )
        .into());
        #[cfg(not(feature = "fips"))]
        tracing::warn!(
            "Postgres TLS is on but the server certificate is not fully verified. Set sslmode=verify-full with a CA bundle to close a man-in-the-middle path"
        );
    }
    Ok(())
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

/// Read the grid-admin token table.
///
/// No table means nobody can mint, rather than anybody.
fn load_grid_admins() -> Result<GridAdmins, std::io::Error> {
    let admins = match std::env::var(GRID_ADMIN_TOKENS) {
        Ok(path) => GridAdmins::from_table(&std::fs::read_to_string(&path)?),
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
                let kube = enrollment::authz::KubeAuthorizer::connect()
                    .await
                    .map_err(std::io::Error::other)?;
                tracing::info!("grid-admin authorization: Kubernetes RBAC (SubjectAccessReview)");
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
    use super::require_db_tls;

    #[test]
    fn a_url_that_permits_plaintext_is_refused() {
        for url in [
            "postgres://u:p@h/db",
            "postgres://u:p@h/db?sslmode=disable",
            "postgres://u:p@h/db?sslmode=allow",
            "postgres://u:p@h/db?sslmode=prefer",
        ] {
            assert!(
                require_db_tls(url).is_err(),
                "{url} permits a plaintext fallback and must be refused"
            );
        }
    }

    #[test]
    fn verify_full_is_accepted() {
        assert!(
            require_db_tls("postgres://u:p@h/db?sslmode=verify-full").is_ok(),
            "verify-full fully verifies the server and must be accepted in every build"
        );
    }

    #[cfg(not(feature = "fips"))]
    #[test]
    fn a_partially_verified_url_is_accepted_outside_fips() {
        for url in [
            "postgres://u:p@h/db?sslmode=require",
            "postgres://u:p@h/db?sslmode=verify-ca",
        ] {
            assert!(
                require_db_tls(url).is_ok(),
                "{url} encrypts the hop and must be accepted in a default build"
            );
        }
    }

    #[cfg(feature = "fips")]
    #[test]
    fn a_partially_verified_url_is_refused_under_fips() {
        for url in [
            "postgres://u:p@h/db?sslmode=require",
            "postgres://u:p@h/db?sslmode=verify-ca",
        ] {
            assert!(
                require_db_tls(url).is_err(),
                "{url} leaves the server unverified and a fips build must fail closed"
            );
        }
    }
}
