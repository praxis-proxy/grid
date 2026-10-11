//! Reqwest clients for an endpoint identified by its chain to the grid roots.
//!
//! Trusts only the roots given. A peer identified by its leaf uses
//! [`PeerScraper`], and a caller wanting the platform roots builds its own.
//!
//! [`PeerScraper`]: crate::PeerScraper

use std::time::Duration;

/// How long a client waits, per phase, because a request deadline cannot
/// bound a connect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Timeouts {
    /// Longest wait for a connection, including the TLS handshake.
    pub connect: Duration,
    /// Longest wait for the whole request, headers and body together.
    pub request: Duration,
}

impl Timeouts {
    /// The grid's defaults, for a caller with no interval of its own.
    pub const DEFAULT: Self = Self {
        connect: Duration::from_secs(5),
        request: Duration::from_secs(30),
    };
    /// Bounds for enrollment and identity rotation, which cross sites over
    /// whatever network the deployment has.
    pub const ENROLLMENT: Self = Self {
        connect: Duration::from_secs(10),
        request: Duration::from_secs(30),
    };

    /// Bounds for a caller that repeats on `interval`, which outlasting it
    /// would turn into a backlog.
    #[must_use]
    pub fn for_interval(interval: Duration) -> Self {
        Self {
            connect: Self::DEFAULT.connect.min(interval),
            request: interval,
        }
    }
}

impl Default for Timeouts {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Why a client could not be built.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// The trust material parsed to no certificate, refused here so the error
    /// names the bundle rather than an unknown issuer.
    #[error("{0} holds no certificate")]
    NoRoots(&'static str),
    /// The trust material could not be parsed.
    #[error("parsing {context}")]
    Roots {
        /// What the material was for, for whoever reads the log.
        context: &'static str,
        /// The underlying parse failure.
        #[source]
        source: reqwest::Error,
    },
    /// The certificate and key did not load as an identity.
    #[error("building the client identity: {0}")]
    Identity(#[source] reqwest::Error),
    /// reqwest refused the configuration.
    #[error("building the client: {0}")]
    Client(#[source] reqwest::Error),
    /// No rustls crypto provider is installed in the process.
    ///
    /// Checked here because reqwest panics instead of returning, and the
    /// release profile aborts rather than unwinds.
    #[error("no rustls crypto provider is installed")]
    NoProvider,
}

/// Parse a PEM bundle into roots, refusing an empty one.
///
/// # Errors
///
/// [`BuildError::Roots`] when the bundle does not parse, [`BuildError::NoRoots`]
/// when it parses to nothing.
pub fn roots_from_pem(pem: &[u8], context: &'static str) -> Result<Vec<reqwest::Certificate>, BuildError> {
    let roots = reqwest::Certificate::from_pem_bundle(pem).map_err(|source| BuildError::Roots { context, source })?;
    if roots.is_empty() {
        return Err(BuildError::NoRoots(context));
    }
    Ok(roots)
}

/// A client identity from a certificate and its key.
///
/// # Errors
///
/// [`BuildError::Identity`] when the pair does not load.
#[cfg(not(feature = "fips"))]
pub fn identity(cert: &str, key: &str) -> Result<reqwest::Identity, BuildError> {
    reqwest::Identity::from_pem(zeroize::Zeroizing::new(format!("{cert}{key}")).as_bytes())
        .map_err(BuildError::Identity)
}

/// A client identity from a certificate and its key, taken apart because the
/// fips backend wants them separate.
///
/// # Errors
///
/// [`BuildError::Identity`] when the pair does not load.
#[cfg(feature = "fips")]
pub fn identity(cert: &str, key: &str) -> Result<reqwest::Identity, BuildError> {
    reqwest::Identity::from_pkcs8_pem(cert.as_bytes(), key.as_bytes()).map_err(BuildError::Identity)
}

/// A client that trusts only `roots`, over HTTPS, following no redirect
/// because every grid endpoint is named directly.
///
/// # Errors
///
/// [`BuildError::Client`] when reqwest refuses the configuration, and
/// [`BuildError::NoProvider`] when no rustls provider is installed.
pub fn trusted_client(
    roots: Vec<reqwest::Certificate>,
    identity: Option<reqwest::Identity>,
    timeouts: Timeouts,
) -> Result<reqwest::Client, BuildError> {
    #[cfg(not(feature = "fips"))]
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        return Err(BuildError::NoProvider);
    }
    let mut builder = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .min_tls_version(reqwest::tls::Version::TLS_1_2)
        .connect_timeout(timeouts.connect)
        .timeout(timeouts.request)
        .tls_certs_only(roots);
    if let Some(identity) = identity {
        builder = builder.identity(identity);
    }
    builder.build().map_err(BuildError::Client)
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn an_empty_bundle_is_refused_rather_than_trusting_the_platform() {
        let error = roots_from_pem(b"", "test bundle").expect_err("an empty bundle must not build");
        assert!(matches!(error, BuildError::NoRoots("test bundle")), "{error}");
    }

    #[test]
    fn garbage_is_reported_against_its_context() {
        let error = roots_from_pem(b"-----BEGIN CERTIFICATE-----\nnope\n", "grid CA").expect_err("must not parse");
        assert!(
            matches!(error, BuildError::Roots { context: "grid CA", .. }),
            "the variant and its context carry the report, not the message: {error}"
        );
        assert!(
            std::error::Error::source(&error).is_some(),
            "the parse failure is the cause, so it must survive as one"
        );
    }

    #[test]
    fn an_interval_bounds_the_request_and_keeps_the_connect_default() {
        let timeouts = Timeouts::for_interval(Duration::from_secs(5));
        assert_eq!(timeouts.request, Duration::from_secs(5));
        assert_eq!(timeouts.connect, Timeouts::DEFAULT.connect);
    }

    #[test]
    fn an_interval_under_the_connect_default_clamps_it() {
        let timeouts = Timeouts::for_interval(Duration::from_secs(1));
        assert_eq!(timeouts.request, Duration::from_secs(1));
        assert_eq!(
            timeouts.connect,
            Duration::from_secs(1),
            "a connect bound past the deadline never fires"
        );
    }

    #[test]
    fn enrollment_keeps_the_longer_handshake_budget() {
        assert!(Timeouts::ENROLLMENT.connect > Timeouts::DEFAULT.connect);
    }

    #[test]
    fn the_default_bounds_both_phases() {
        let timeouts = Timeouts::default();
        assert!(timeouts.connect > Duration::ZERO, "a client must bound its connect");
        assert!(
            timeouts.request > timeouts.connect,
            "the request deadline covers the connect"
        );
    }
}
