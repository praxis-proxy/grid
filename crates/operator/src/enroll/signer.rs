//! The signer contract: enroll with an invite, renew with the current identity.
//!
//! One implementation today, the enrollment service over HTTPS. The check that an issued
//! leaf names this site stays with the callers, so every implementation passes it.

use std::{
    borrow::Cow,
    sync::{Mutex, PoisonError},
};

use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{chain, error_chain, is_tls_failure, pem_roots, read_capped, renew::Held};
use crate::resources::tls_backend;

/// The enroll route on the enrollment service.
pub(super) const ENROLL_PATH: &str = "/v1alpha1/enrollments";

/// The renewal route on the enrollment service.
pub(super) const RENEW_PATH: &str = "/v1alpha1/rotations";

/// What a signer issues: the leaf and the grid CA it chains to.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Issued {
    /// Issued leaf, PEM.
    pub(super) certificate: String,
    /// Grid CA, PEM.
    pub(super) ca_certificate: String,
}

/// Why a signer issued nothing, in the outcomes status distinguishes.
#[derive(Debug, thiserror::Error)]
pub(super) enum SignerError {
    /// The signer did not accept the credential or certificate presented.
    #[error("{0}")]
    Unauthenticated(String),
    /// The signer received no client certificate, as when TLS ends before it.
    #[error("{0}")]
    NoClientCertificate(String),
    /// The site name is enrolled already.
    #[error("{0}")]
    NameTaken(String),
    /// The site name's enrollment was deleted and the credential does not re-admit it.
    #[error("{0}")]
    NameDeleted(String),
    /// The signer refused the identity or credential presented, which a new one fixes.
    #[error("{0}")]
    Forbidden(String),
    /// The signer refused the request for another reason it answered.
    #[error("{0}")]
    Refused(String),
    /// The signer refuses every renewal: rotation is turned off at the signer.
    #[error("{0}")]
    Disabled(String),
    /// No usable answer came back. `sent` is set when the request may have reached the signer.
    #[error("{detail}")]
    Transport {
        /// Whether the request may have reached the signer.
        sent: bool,
        /// What failed.
        detail: String,
    },
    /// The signer's serving certificate was refused. Nothing was sent. `lapsed` is set when
    /// it was refused only for its validity window, which the signer fixes by rotating.
    #[error("{detail}")]
    Untrusted {
        /// Whether only the certificate's validity window was refused.
        lapsed: bool,
        /// What the handshake said.
        detail: String,
    },
    /// The local material or request could not be built. Nothing was sent.
    #[error("{0}")]
    Request(String),
    /// The signer answered with something that is not an issued certificate.
    #[error("{0}")]
    Invalid(String),
}

/// A leaf a signer issued for this site, and the grid CA it chains to, both checked.
pub(super) struct Leaf {
    /// Issued leaf, PEM.
    pub(super) cert: String,
    /// Grid CA, PEM.
    pub(super) ca: String,
}

/// Why an issued leaf was refused.
#[derive(Debug)]
pub(super) enum LeafError {
    /// The returned CA does not pass against the one expected.
    Ca(certs::VerifyError),
    /// The leaf is not one certificate.
    Certificate(String),
    /// The leaf is not valid for the site from the returned CA.
    NotForSite(String),
    /// A public key could not be read.
    Key(String),
    /// The leaf carries a key other than the request's.
    KeyMismatch,
}

/// The leaf `issued` holds, if its CA passes against `anchor_pem`, it is valid for `site`
/// from that CA, and it carries the key of `csr_pem`. The one check both enrollment and
/// renewal make; each words the refusal for its own report.
pub(super) fn issued_leaf(issued: &Issued, anchor_pem: &str, site: &str, csr_pem: &str) -> Result<Leaf, LeafError> {
    let ca = certs::anchored_ca(&issued.ca_certificate, anchor_pem).map_err(LeafError::Ca)?;
    let cert = certs::leaf_only(&issued.certificate).map_err(|e| LeafError::Certificate(e.to_string()))?;
    certs::verify_site_cert(&ca, &cert, site).map_err(|e| LeafError::NotForSite(e.to_string()))?;
    let leaf_key = certs::cert_public_key(&cert).map_err(|e| LeafError::Key(e.to_string()))?;
    let csr_key = certs::csr_public_key(csr_pem).map_err(|e| LeafError::Key(e.to_string()))?;
    if leaf_key != csr_key {
        return Err(LeafError::KeyMismatch);
    }
    Ok(Leaf { cert, ca })
}

/// A signer this site enrolls and renews through.
pub(super) trait Signer {
    /// Present `credential`, the opaque invite the signer issued, and ask for a certificate for `csr_pem`.
    fn enroll(&self, credential: &str, csr_pem: &str) -> impl Future<Output = Result<Issued, SignerError>> + Send;
    /// Present `held`'s certificate and ask for a certificate for `csr_pem`.
    fn renew(&self, held: &Held, csr_pem: &str) -> impl Future<Output = Result<Issued, SignerError>> + Send;
}

/// The clients a signer presents, with a digest of their material. Rebuilt when that
/// material changes and otherwise held, so a retry keeps the warm pool.
#[derive(Debug, Default)]
pub(super) struct Clients(Mutex<Option<([u8; 32], reqwest::Client)>>);

/// A signer's base URL, as [`super::signer_url`] admits it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SignerUrl(Url);

impl SignerUrl {
    /// `raw` as a signer base URL.
    ///
    /// # Errors
    ///
    /// As [`super::signer_url`].
    pub(super) fn parse(raw: &str) -> Result<Self, String> {
        super::signer_url(raw).map(Self)
    }

    /// The URL as text.
    #[cfg(test)]
    pub(super) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// `path` under the base URL.
    pub(super) fn join(&self, path: &str) -> String {
        format!("{}{path}", self.0.as_str().trim_end_matches('/'))
    }

    /// The renewal route.
    pub(super) fn renewal(&self) -> String {
        self.join(RENEW_PATH)
    }
}

/// The enrollment service over HTTPS.
pub(super) struct HttpsSigner<'signer> {
    /// The signer's base URL.
    base: &'signer SignerUrl,
    /// The PEM pinning the signer's serving certificate: the only trust to enroll, and
    /// trusted beside the held grid CA to renew.
    pin: Option<&'signer str>,
    /// Clients kept across calls.
    clients: &'signer Clients,
}

/// Request body for both routes.
#[derive(Serialize)]
struct CsrRequest<'csr> {
    /// PKCS#10 request, PEM.
    csr: &'csr str,
}

/// Error body the service returns.
#[derive(Deserialize)]
struct ErrorBody {
    /// Machine-readable code.
    error: String,
    /// Human message.
    message: String,
}

impl<'signer> HttpsSigner<'signer> {
    /// The signer at `base`, pinned to `pin`, reusing `clients`.
    #[must_use]
    pub(super) const fn new(base: &'signer SignerUrl, pin: Option<&'signer str>, clients: &'signer Clients) -> Self {
        Self { base, pin, clients }
    }

    /// A client trusting only `trust`, presenting `identity` (cert, key) when set, built
    /// once per distinct trust and identity so a retry keeps the warm pool.
    fn client(&self, trust: &str, identity: Option<(&str, &str)>) -> Result<reqwest::Client, SignerError> {
        let mut digest = Sha256::new().chain_update(trust.as_bytes());
        if let Some((cert, key)) = identity {
            digest = digest.chain_update(cert.as_bytes()).chain_update(key.as_bytes());
        }
        let material: [u8; 32] = digest.finalize().into();
        let cached = self
            .clients
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(|(stored, client)| (*stored == material).then(|| client.clone()));
        if let Some(client) = cached {
            return Ok(client);
        }
        let roots = pem_roots(trust, "signer trust").map_err(|e| SignerError::Request(e.to_string()))?;
        let presented = identity
            .map(|(cert, key)| grid_client::identity(cert, key))
            .transpose()
            .map_err(|e| SignerError::Request(error_chain(&e)))?;
        let client = grid_client::trusted_client(roots, presented, grid_client::Timeouts::ENROLLMENT)
            .map_err(|e| SignerError::Request(error_chain(&e)))?;
        *self.clients.0.lock().unwrap_or_else(PoisonError::into_inner) = Some((material, client.clone()));
        Ok(client)
    }
}

impl Signer for HttpsSigner<'_> {
    async fn enroll(&self, credential: &str, csr_pem: &str) -> Result<Issued, SignerError> {
        let pin = self
            .pin
            .ok_or_else(|| SignerError::Request("enrolling needs a caBundleRef pinning the signer".to_owned()))?;
        let request = self
            .client(pin, None)?
            .post(self.base.join(ENROLL_PATH))
            .bearer_auth(credential)
            .json(&CsrRequest { csr: csr_pem });
        Box::pin(post(request)).await
    }

    async fn renew(&self, held: &Held, csr_pem: &str) -> Result<Issued, SignerError> {
        // The held grid CA, and the pin: a Secret-only bundle on a host GRID_SIGNER_HOSTS
        // confines, so a ConfigMap edit cannot point renewal at a signer of its own.
        let trust = match self.pin {
            Some(pin) => Cow::Owned(format!("{}\n{pin}", held.ca)),
            None => Cow::Borrowed(held.ca.as_str()),
        };
        let request = self
            .client(&trust, Some((&held.cert, &held.key)))?
            .post(self.base.renewal())
            .json(&CsrRequest { csr: csr_pem });
        Box::pin(post(request)).await
    }
}

/// Send `request` and decode the answer.
async fn post(request: reqwest::RequestBuilder) -> Result<Issued, SignerError> {
    let response = request.send().await.map_err(|e| unsent(&e))?;
    let status = response.status();
    let body = read_capped(response)
        .await
        .map_err(|detail| SignerError::Transport { sent: true, detail })?;
    classify(status, &body)
}

/// Why a request did not complete before an answer: retried only when it never left.
///
/// The detail names the signer and the kind of failure only, since it reaches status and the
/// raw error carries times and addresses that change between identical failures; it is logged.
fn unsent(error: &reqwest::Error) -> SignerError {
    if error.is_builder() {
        return SignerError::Request(error_chain(error));
    }
    let signer = error.url().map_or("the signer", Url::as_str);
    tracing::warn!(signer, error = %error_chain(error), "the request to the signer did not complete");
    if is_tls_failure(error) {
        return SignerError::Untrusted {
            lapsed: chain(error).any(tls_backend::is_lapsed_certificate),
            detail: signer.to_owned(),
        };
    }
    let detail = if error.is_connect() {
        format!("could not connect to {signer}")
    } else if error.is_timeout() {
        format!("{signer} did not answer in time")
    } else {
        format!("the request to {signer} failed")
    };
    SignerError::Transport {
        sent: !error.is_connect(),
        detail,
    }
}

/// Decode an answer from the service.
pub(super) fn classify(status: StatusCode, body: &[u8]) -> Result<Issued, SignerError> {
    if status.is_success() {
        return serde_json::from_slice(body).map_err(|e| SignerError::Invalid(format!("decoding: {e}")));
    }
    let error = serde_json::from_slice::<ErrorBody>(body).ok();
    let detail = error.as_ref().map_or_else(
        || status.to_string(),
        |err| format!("{status} {}: {}", err.error, err.message),
    );
    let code = error.map(|err| err.error);
    Err(match (status, code.as_deref()) {
        (StatusCode::UNAUTHORIZED, Some("identity_required")) => SignerError::NoClientCertificate(detail),
        (StatusCode::UNAUTHORIZED, _) => SignerError::Unauthenticated(detail),
        (StatusCode::SERVICE_UNAVAILABLE, Some("rotation_disabled")) => SignerError::Disabled(detail),
        (StatusCode::CONFLICT, Some("name_taken")) => SignerError::NameTaken(detail),
        (StatusCode::CONFLICT, Some("name_deleted")) => SignerError::NameDeleted(detail),
        (StatusCode::FORBIDDEN, _) => SignerError::Forbidden(detail),
        // A timeout or a server error may have reached the signer.
        (StatusCode::REQUEST_TIMEOUT, _) => SignerError::Transport { sent: true, detail },
        _ if status.is_server_error() => SignerError::Transport { sent: true, detail },
        _ => SignerError::Refused(detail),
    })
}
