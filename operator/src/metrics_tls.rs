//! TLS for the metrics and health listener, reloaded when its material rotates.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use k8s_openapi::api::core::v1::Secret;
use kube::Api;
use tokio::sync::watch;
use zeroize::Zeroizing;

use crate::resources::tls_backend::{self, ServerTlsConfig};

/// How long a client may take to finish its handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a client may take to send its request headers.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Connections served at once, counted from before the handshake.
const MAX_CONNECTIONS: usize = 128;

/// A PEM certificate chain and its private key.
type Material = (Zeroizing<Vec<u8>>, Zeroizing<Vec<u8>>);

/// Why material could not be read.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum ReadError {
    /// Not written yet, which enrollment fixes.
    #[error("{0}")]
    Absent(String),
    /// Present but unusable, or unreadable.
    #[error("{0}")]
    Failed(String),
}

/// Where the listener's certificate and key come from.
pub enum MetricsTls {
    /// PEM files, which must be readable at startup.
    Files {
        /// PEM certificate chain.
        cert: PathBuf,
        /// PEM private key.
        key: PathBuf,
    },
    /// The site identity Secret, waited for until enrollment writes it.
    SiteIdentity {
        /// Secrets in the operator namespace.
        secrets: Api<Secret>,
        /// The identity Secret's name.
        name: String,
    },
    /// Material a test sets and changes.
    #[cfg(test)]
    Fake(std::sync::Arc<std::sync::Mutex<Option<(String, String)>>>),
}

impl MetricsTls {
    /// TLS from files, from the site identity Secret, or plaintext when nothing is set.
    ///
    /// # Errors
    ///
    /// Returns a description when only one file is set, or files and the Secret both are.
    pub fn from_args(
        cert: Option<PathBuf>,
        key: Option<PathBuf>,
        site_secret: Option<String>,
        client: &kube::Client,
    ) -> Result<Option<Self>, String> {
        match (cert, key, site_secret) {
            (Some(cert), Some(key), None) => Ok(Some(Self::Files { cert, key })),
            (None, None, Some(name)) => Ok(Some(Self::SiteIdentity {
                secrets: Api::default_namespaced(client.clone()),
                name,
            })),
            (None, None, None) => Ok(None),
            (_, _, Some(_)) => Err("metrics TLS takes files or the site identity Secret, not both".to_owned()),
            _ => Err("metrics TLS needs both a certificate and a key".to_owned()),
        }
    }

    /// Whether missing material is waited for rather than refused.
    const fn waits(&self) -> bool {
        !matches!(self, Self::Files { .. })
    }

    /// The certificate and key as they are now.
    async fn read(&self) -> Result<Material, ReadError> {
        match self {
            Self::Files { cert, key } => Ok((
                read_file(cert).map_err(ReadError::Failed)?,
                read_file(key).map_err(ReadError::Failed)?,
            )),
            Self::SiteIdentity { secrets, name } => read_secret(secrets, name).await,
            #[cfg(test)]
            Self::Fake(material) => material
                .lock()
                .map_err(|e| ReadError::Failed(e.to_string()))?
                .clone()
                .map(|(cert, key)| (Zeroizing::new(cert.into_bytes()), Zeroizing::new(key.into_bytes())))
                .ok_or_else(|| ReadError::Absent("metrics TLS: no material yet".to_owned())),
        }
    }
}

impl std::fmt::Debug for MetricsTls {
    /// Names the source, never its material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Files { cert, key } => f.debug_struct("Files").field("cert", cert).field("key", key).finish(),
            Self::SiteIdentity { name, .. } => f.debug_struct("SiteIdentity").field("name", name).finish(),
            #[cfg(test)]
            Self::Fake(_) => f.write_str("Fake"),
        }
    }
}

/// One PEM file. A small blocking read, off the request path.
fn read_file(path: &Path) -> Result<Zeroizing<Vec<u8>>, String> {
    std::fs::read(path)
        .map(Zeroizing::new)
        .map_err(|e| format!("metrics TLS: reading {}: {e}", path.display()))
}

/// `tls.crt` and `tls.key` from Secret `name`.
async fn read_secret(secrets: &Api<Secret>, name: &str) -> Result<Material, ReadError> {
    let secret = secrets
        .get_opt(name)
        .await
        .map_err(|e| ReadError::Failed(format!("metrics TLS: reading Secret {name}: {e}")))?
        .ok_or_else(|| ReadError::Absent(format!("metrics TLS: Secret {name} does not exist yet")))?;
    let mut data = secret.data.unwrap_or_default();
    let mut take = |field: &str| {
        data.remove(field)
            .map(|bytes| Zeroizing::new(bytes.0))
            .ok_or_else(|| ReadError::Absent(format!("metrics TLS: Secret {name} has no {field} yet")))
    };
    // The key first, so a missing certificate never drops it unzeroized.
    let key = take("tls.key")?;
    Ok((take("tls.crt")?, key))
}

/// A change detector over the certificate and key. Not a security function.
fn digest((cert, key): &Material) -> [u8; 32] {
    let mut material = Vec::with_capacity(64);
    material.extend_from_slice(&certs::sha256(cert));
    material.extend_from_slice(&certs::sha256(key));
    certs::sha256(&material)
}

/// Build the server config from `material`.
fn build((cert, key): &Material) -> Result<ServerTlsConfig, String> {
    tls_backend::build_server_only_config(cert, key).map_err(|e| format!("metrics TLS: {e}"))
}

/// One reload check.
#[derive(Debug)]
enum Reload {
    /// The material is unchanged.
    Unchanged,
    /// The material changed and loaded.
    Loaded(ServerTlsConfig),
    /// The material changed and failed to load, so `applied` is kept for a retry.
    Failed(String),
}

/// Reload `tls` when its digest differs from `applied`, recording the new digest only on success.
async fn reload(tls: &MetricsTls, applied: &mut [u8; 32]) -> Reload {
    let material = match tls.read().await {
        Ok(material) => material,
        Err(error) => return Reload::Failed(error.to_string()),
    };
    let next = digest(&material);
    if next == *applied {
        return Reload::Unchanged;
    }
    match build(&material) {
        Ok(config) => {
            *applied = next;
            Reload::Loaded(config)
        },
        Err(error) => Reload::Failed(error),
    }
}

/// The first usable config, waiting for site identity material and refusing missing files.
///
/// Absent material is logged once. Any other error is logged at warn each time it changes.
async fn first_config(tls: &MetricsTls, every: Duration) -> Result<(ServerTlsConfig, [u8; 32]), String> {
    let mut last: Option<ReadError> = None;
    loop {
        let loaded = tls
            .read()
            .await
            .and_then(|material| Ok((build(&material).map_err(ReadError::Failed)?, digest(&material))));
        let error = match loaded {
            Ok(loaded) => return Ok(loaded),
            Err(error) if !tls.waits() => return Err(error.to_string()),
            Err(error) => error,
        };
        if last.as_ref() != Some(&error) {
            match &error {
                ReadError::Absent(_) => {
                    tracing::info!(%error, "metrics TLS: waiting for the site identity; not listening yet");
                },
                ReadError::Failed(_) => {
                    tracing::warn!(%error, "metrics TLS: site identity unusable; not listening yet");
                },
            }
            last = Some(error);
        }
        tokio::time::sleep(every).await;
    }
}

/// Load `tls`, then reload it every `every` once its material changes.
///
/// Site identity material is waited for. A failed reload keeps serving the last
/// good config and retries.
///
/// # Errors
///
/// Returns a description when files cannot be loaded at startup, so the listener
/// never falls back to plaintext.
pub async fn watch_tls(tls: MetricsTls, every: Duration) -> Result<watch::Receiver<ServerTlsConfig>, String> {
    let (config, mut applied) = first_config(&tls, every).await?;
    let (sender, receiver) = watch::channel(config);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(every).await;
            match reload(&tls, &mut applied).await {
                Reload::Unchanged => {},
                Reload::Loaded(next) => {
                    if sender.send(next).is_err() {
                        return;
                    }
                    tracing::info!("metrics TLS: certificate reloaded");
                },
                Reload::Failed(error) => {
                    tracing::warn!(%error, "metrics TLS: reload failed; keeping the last good certificate");
                },
            }
        }
    });
    Ok(receiver)
}

/// Wait for the TLS material, then bind `addr` and serve `app` over TLS.
///
/// Nothing listens until the material loads, so the port is never plaintext.
///
/// # Errors
///
/// Returns why the material could not load, the bind failed, or serving stopped.
pub async fn serve_metrics_tls(
    addr: &str,
    app: axum::Router,
    tls: MetricsTls,
    every: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tls = watch_tls(tls, every).await?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener
        .local_addr()
        .map_or_else(|_| addr.to_owned(), |a| a.to_string());
    tracing::info!(addr = %bound, tls = true, "metrics server started");
    Err(serve_tls(listener, app, tls).await.into())
}

/// Serve `app` over TLS on `listener`, each connection on the config current when it arrived.
///
/// Returns only when the reloader has stopped, so a certificate can no longer rotate.
pub async fn serve_tls(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    mut tls: watch::Receiver<ServerTlsConfig>,
) -> std::io::Error {
    let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    loop {
        let Ok(slot) = std::sync::Arc::clone(&slots).acquire_owned().await else {
            return std::io::Error::other("metrics TLS: connection slots closed");
        };
        let accepted = tokio::select! {
            changed = tls.changed() => {
                if changed.is_err() {
                    return std::io::Error::other("metrics TLS: the certificate reloader stopped");
                }
                continue;
            },
            accepted = listener.accept() => accepted,
        };
        let (tcp, remote) = match accepted {
            Ok(accepted) => accepted,
            Err(error) => {
                // Out of descriptors and the like pass, and a hot retry would spin.
                tracing::warn!(%error, "metrics: accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            },
        };
        let config = tls.borrow().clone();
        tokio::spawn(serve_connection(tcp, remote, config, app.clone(), slot));
    }
}

/// Handshake one connection and serve HTTP/1.1 on it, holding `_slot` until it closes.
#[cfg_attr(
    not(feature = "fips"),
    expect(clippy::large_stack_frames, reason = "async future over a rustls handshake")
)]
async fn serve_connection(
    tcp: tokio::net::TcpStream,
    remote: std::net::SocketAddr,
    config: ServerTlsConfig,
    app: axum::Router,
    _slot: tokio::sync::OwnedSemaphorePermit,
) {
    let Ok(Ok(stream)) = tokio::time::timeout(HANDSHAKE_TIMEOUT, tls_backend::accept(tcp, &config)).await else {
        tracing::debug!(%remote, "metrics: handshake failed or timed out");
        return;
    };
    let service = hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
        use tower::Service as _;
        app.clone().call(request)
    });
    let served = hyper::server::conn::http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
        .await;
    if let Err(error) = served {
        tracing::debug!(%remote, %error, "metrics: connection ended");
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A `localhost` certificate `ca` issued, as PEM certificate and key.
    fn leaf(ca: &certs::CaCert) -> (String, String) {
        let leaf = certs::generate_dns_cert(ca, "operator-metrics", "localhost").expect("fixture");
        (leaf.cert_pem, leaf.key_pem)
    }

    /// Files under `dir` holding a certificate `ca` issued.
    fn issue(dir: &Path, ca: &certs::CaCert) -> MetricsTls {
        let (cert, key) = leaf(ca);
        std::fs::write(dir.join("tls.crt"), cert).expect("fixture");
        std::fs::write(dir.join("tls.key"), key).expect("fixture");
        MetricsTls::Files {
            cert: dir.join("tls.crt"),
            key: dir.join("tls.key"),
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("metrics-tls-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("fixture");
        dir
    }

    /// GET `path` from `port`, trusting only `ca`.
    async fn get(port: u16, path: &str, ca: &certs::CaCert) -> Result<String, String> {
        let client = tls_backend::build_tls_client_config(ca.cert_pem.as_bytes(), None, None).expect("fixture");
        crate::metrics_scraper::scrape_metrics(
            &format!("https://localhost:{port}{path}"),
            Duration::from_secs(5),
            Some(Arc::new(client)),
            None,
        )
        .await
        .map_err(|e| e.to_string())
    }

    fn health() -> axum::Router {
        axum::Router::new().route("/healthz", axum::routing::get(|| async { "ok" }))
    }

    /// Serve `tls` on a fresh port without waiting for it to bind.
    fn spawn_serving(tls: MetricsTls, every: Duration) -> u16 {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|probe| probe.local_addr())
            .expect("fixture")
            .port();
        tokio::spawn(async move { serve_metrics_tls(&format!("127.0.0.1:{port}"), health(), tls, every).await });
        port
    }

    /// Wait until `ca` verifies a health check on `port`.
    async fn served_by(port: u16, ca: &certs::CaCert) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while get(port, "/healthz", ca).await.is_err() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "never served a certificate from that CA"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn serves_health_over_tls() {
        tls_backend::init_process_crypto();
        let dir = temp_dir("serve");
        let ca = certs::generate_ca("metrics-ca").expect("fixture");
        let port = spawn_serving(issue(&dir, &ca), Duration::from_secs(60));
        served_by(port, &ca).await;
        assert_eq!(get(port, "/healthz", &ca).await.as_deref(), Ok("ok"));
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[tokio::test]
    async fn a_rotated_certificate_is_served_without_a_restart() {
        tls_backend::init_process_crypto();
        let dir = temp_dir("rotate");
        let old = certs::generate_ca("old-ca").expect("fixture");
        let port = spawn_serving(issue(&dir, &old), Duration::from_millis(20));
        served_by(port, &old).await;

        let new = certs::generate_ca("new-ca").expect("fixture");
        issue(&dir, &new);
        served_by(port, &new).await;
        assert!(
            get(port, "/healthz", &old).await.is_err(),
            "the old certificate is gone"
        );
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[tokio::test]
    async fn a_site_waits_for_its_identity_then_serves_and_rotates_it() {
        tls_backend::init_process_crypto();
        let material = Arc::new(Mutex::new(None));
        let port = spawn_serving(MetricsTls::Fake(Arc::clone(&material)), Duration::from_millis(20));

        tokio::time::sleep(Duration::from_millis(200)).await;
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect_err("nothing listens before the identity exists");

        let enrolled = certs::generate_ca("grid-ca").expect("fixture");
        *material.lock().expect("fixture") = Some(leaf(&enrolled));
        served_by(port, &enrolled).await;

        let renewed = certs::generate_ca("renewed-ca").expect("fixture");
        *material.lock().expect("fixture") = Some(leaf(&renewed));
        served_by(port, &renewed).await;
        assert!(
            get(port, "/healthz", &enrolled).await.is_err(),
            "the renewal replaced the old leaf"
        );
    }

    #[tokio::test]
    async fn a_failed_reload_keeps_the_last_good_certificate_and_retries() {
        tls_backend::init_process_crypto();
        let dir = temp_dir("keep");
        let ca = certs::generate_ca("metrics-ca").expect("fixture");
        let tls = issue(&dir, &ca);
        let (_, mut applied) = first_config(&tls, Duration::from_secs(60)).await.expect("fixture");
        assert!(matches!(reload(&tls, &mut applied).await, Reload::Unchanged));

        std::fs::write(dir.join("tls.key"), "not a key").expect("fixture");
        let before = applied;
        assert!(
            matches!(reload(&tls, &mut applied).await, Reload::Failed(_)),
            "a bad key is refused"
        );
        assert_eq!(applied, before, "the failure is retried");
        assert!(
            matches!(reload(&tls, &mut applied).await, Reload::Failed(_)),
            "and retried again"
        );

        issue(&dir, &ca);
        assert!(
            matches!(reload(&tls, &mut applied).await, Reload::Loaded(_)),
            "a fixed pair loads"
        );
        assert!(matches!(reload(&tls, &mut applied).await, Reload::Unchanged));
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[tokio::test]
    async fn unreadable_files_fail_closed_at_startup() {
        let tls = MetricsTls::Files {
            cert: PathBuf::from("/nonexistent/tls.crt"),
            key: PathBuf::from("/nonexistent/tls.key"),
        };
        let error = watch_tls(tls, Duration::from_secs(60)).await.expect_err("fails closed");
        assert!(error.contains("/nonexistent/tls.crt"), "{error}");
    }

    #[tokio::test]
    async fn files_or_the_site_identity_or_neither() {
        let client =
            kube::Client::try_from(kube::Config::new("http://127.0.0.1:9".parse().expect("fixture"))).expect("fixture");
        let args = |cert: Option<&str>, key: Option<&str>, site: Option<&str>| {
            MetricsTls::from_args(
                cert.map(PathBuf::from),
                key.map(PathBuf::from),
                site.map(str::to_owned),
                &client,
            )
        };
        assert!(args(None, None, None).expect("plaintext").is_none());
        assert!(matches!(
            args(Some("c"), Some("k"), None),
            Ok(Some(MetricsTls::Files { .. }))
        ));
        assert!(matches!(
            args(None, None, Some("grid-site-identity")),
            Ok(Some(MetricsTls::SiteIdentity { .. }))
        ));
        args(Some("c"), None, None).expect_err("a certificate alone");
        args(None, Some("k"), None).expect_err("a key alone");
        args(Some("c"), Some("k"), Some("grid-site-identity")).expect_err("files and the Secret");
    }

    #[tokio::test]
    async fn serving_stops_once_the_reloader_is_gone() {
        tls_backend::init_process_crypto();
        let ca = certs::generate_ca("metrics-ca").expect("fixture");
        let (cert, key) = leaf(&ca);
        let config = tls_backend::build_server_only_config(cert.as_bytes(), key.as_bytes()).expect("fixture");
        let (sender, receiver) = watch::channel(config);
        drop(sender);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("fixture");
        let stopped = tokio::time::timeout(
            Duration::from_secs(5),
            serve_tls(listener, axum::Router::new(), receiver),
        )
        .await
        .expect("serving stops rather than serving a certificate that can no longer rotate");
        assert!(stopped.to_string().contains("reloader stopped"), "{stopped}");
    }

    #[test]
    fn the_signals_listener_refuses_an_empty_client_ca() {
        tls_backend::init_process_crypto();
        let ca = certs::generate_ca("grid-ca").expect("fixture");
        let site = certs::generate_site_cert(&ca, "east").expect("fixture");
        for empty in [&b""[..], b"not a certificate"] {
            tls_backend::build_server_config(empty, site.cert_pem.as_bytes(), site.key_pem.as_bytes())
                .expect_err("mTLS never runs without client roots");
        }
    }
}
