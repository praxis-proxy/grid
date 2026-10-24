//! The signer contract: enroll with an invite, renew with the current identity.
//!
//! One implementation today, the enrollment service over HTTPS. The check that an issued
//! leaf names this site stays with the callers, so every implementation passes it.

use std::sync::{Mutex, PoisonError};

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
    /// The signer refused the request, with its status and machine-readable code.
    #[error("{detail}")]
    Refused {
        /// The answer's status.
        status: StatusCode,
        /// The error code the signer named, if any.
        code: Option<String>,
        /// What the signer said.
        detail: String,
    },
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

/// The enrollment service over HTTPS.
pub(super) struct HttpsSigner<'clients> {
    /// The signer's base URL.
    base: Url,
    /// The PEM pinning the signer. Renewal falls back to the held grid CA.
    pin: Option<String>,
    /// Clients kept across calls.
    clients: &'clients Clients,
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

impl<'clients> HttpsSigner<'clients> {
    /// The signer at `base`, pinned to `pin`, reusing `clients`.
    #[must_use]
    pub(super) const fn new(base: Url, pin: Option<String>, clients: &'clients Clients) -> Self {
        Self { base, pin, clients }
    }

    /// `path` under the base URL.
    pub(super) fn endpoint(base: &Url, path: &str) -> String {
        format!("{}{path}", base.as_str().trim_end_matches('/'))
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
            .as_deref()
            .ok_or_else(|| SignerError::Request("enrolling needs a caBundleRef pinning the signer".to_owned()))?;
        let request = self
            .client(pin, None)?
            .post(Self::endpoint(&self.base, ENROLL_PATH))
            .bearer_auth(credential)
            .json(&CsrRequest { csr: csr_pem });
        Box::pin(post(request)).await
    }

    async fn renew(&self, held: &Held, csr_pem: &str) -> Result<Issued, SignerError> {
        let trust = self.pin.as_deref().unwrap_or(&held.ca);
        let request = self
            .client(trust, Some((&held.cert, &held.key)))?
            .post(Self::endpoint(&self.base, RENEW_PATH))
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
fn unsent(error: &reqwest::Error) -> SignerError {
    if error.is_builder() {
        return SignerError::Request(error_chain(error));
    }
    if is_tls_failure(error) {
        return SignerError::Untrusted {
            lapsed: chain(error).any(tls_backend::is_lapsed_certificate),
            detail: error_chain(error),
        };
    }
    SignerError::Transport {
        sent: !error.is_connect(),
        detail: error_chain(error),
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
    Err(match status {
        StatusCode::UNAUTHORIZED => SignerError::Unauthenticated(detail),
        StatusCode::SERVICE_UNAVAILABLE if code.as_deref() == Some("rotation_disabled") => {
            SignerError::Disabled(detail)
        },
        // A timeout or a server error may have reached the signer.
        StatusCode::REQUEST_TIMEOUT => SignerError::Transport { sent: true, detail },
        _ if status.is_server_error() => SignerError::Transport { sent: true, detail },
        _ => SignerError::Refused { status, code, detail },
    })
}
