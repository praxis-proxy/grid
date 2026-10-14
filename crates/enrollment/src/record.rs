//! Records each enrollment as a Kubernetes `CertificateSigningRequest`.
//!
//! The request is the site's PKCS#10, re-encoded in the one PEM label the API
//! server parses, under the grid's signer name. The service approves it on the
//! invite's authority and completes it with the certificate it issued. The
//! record never gates issue: it is written after the issuing transaction commits,
//! off the request path, and a failure is logged. An issued request is deleted by
//! the API server's cleaner about an hour after approval, so `kubectl get csr` is
//! a one-hour window; the durable trail is the audit log and the service's own
//! enrollment record.

use std::{sync::Arc, time::Duration};

use k8s_openapi::{
    ByteString,
    api::{
        authorization::v1::{ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec},
        certificates::v1::{CertificateSigningRequest, CertificateSigningRequestSpec},
    },
};
use kube::api::{Api, ObjectMeta, Patch, PatchParams, PostParams};
use uuid::Uuid;

/// The signer name enrollments are recorded under when none is configured.
pub const DEFAULT_SIGNER_NAME: &str = "grid.praxis.fast/site";

/// Label naming the site a record belongs to. For listing only: labels are
/// writable, and the name the grid trusts is the SPIFFE name in the issued
/// certificate in `status.certificate`, which the signer pinned from the invite.
pub const SITE_LABEL: &str = "grid.praxis.fast/site-name";

/// The reason an approval on the invite's authority carries.
pub const APPROVED_REASON: &str = "InviteRedeemed";

/// The usages the issued leaf carries: digital signature with server and client
/// authentication, the profile `certs` signs with.
pub const USAGES: [&str; 3] = ["digital signature", "server auth", "client auth"];

/// The field manager on every write.
const FIELD_MANAGER: &str = "grid-enrollment";

/// The PEM label the API server parses a request under.
const CSR_PEM_LABEL: &str = "CERTIFICATE REQUEST";

/// The PEM labels a request may arrive under.
const REQUEST_LABELS: [&str; 2] = [CSR_PEM_LABEL, "NEW CERTIFICATE REQUEST"];

/// How long one record may take, all three calls included.
const RECORD_DEADLINE: Duration = Duration::from_secs(10);

/// One enrollment to record.
#[derive(Clone, Debug)]
pub struct Record {
    /// The site name the invite pinned.
    pub site: String,
    /// The enrollment's id in the service's store.
    pub enrollment_id: Uuid,
    /// The site's certificate request, PEM, as received.
    pub csr_pem: String,
    /// The issued certificate, PEM.
    pub cert_pem: String,
}

/// Why a record was not written, naming the record and the call that failed.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    /// The request PEM did not parse, so there is nothing to record.
    #[error("record {name}: the request is not a PEM block")]
    Request {
        /// The record's name.
        name: String,
    },
    /// Creating the request failed.
    #[error("record {name}: create failed: {source}")]
    Create {
        /// The record's name.
        name: String,
        /// The API error.
        #[source]
        source: kube::Error,
    },
    /// The request exists but its approval failed.
    #[error("record {name}: created but not approved: {source}")]
    Approve {
        /// The record's name.
        name: String,
        /// The API error.
        #[source]
        source: kube::Error,
    },
    /// The request is approved but the certificate was not written.
    #[error("record {name}: approved but not completed: {source}")]
    Complete {
        /// The record's name.
        name: String,
        /// The API error.
        #[source]
        source: kube::Error,
    },
    /// The deadline passed with calls outstanding.
    #[error("record {name}: not finished within {deadline:?}")]
    Deadline {
        /// The record's name.
        name: String,
        /// The deadline.
        deadline: Duration,
    },
}

impl RecordError {
    /// The error for a log line: an API refusal by code and reason only, since
    /// the server's message echoes the rejected field.
    #[must_use]
    pub fn summary(&self) -> String {
        let (name, step, source) = match self {
            Self::Create { name, source } => (name, "create", source),
            Self::Approve { name, source } => (name, "approval", source),
            Self::Complete { name, source } => (name, "completion", source),
            Self::Request { .. } | Self::Deadline { .. } => return self.to_string(),
        };
        if let kube::Error::Api(response) = source {
            format!("record {name}: {step} refused: {} {}", response.code, response.reason)
        } else {
            format!("record {name}: {step} failed: {source}")
        }
    }
}

/// Writes enrollment records to the API server.
pub struct Recorder {
    /// The cluster-scoped requests API.
    requests: Api<CertificateSigningRequest>,
    /// The access-review API, for the startup check.
    reviews: Api<SelfSubjectAccessReview>,
    /// The signer name records are filed under.
    signer_name: String,
}

impl std::fmt::Debug for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recorder")
            .field("signer_name", &self.signer_name)
            .finish_non_exhaustive()
    }
}

impl Recorder {
    /// A recorder over `client`, filing under `signer_name`.
    #[must_use]
    pub fn new(client: kube::Client, signer_name: String) -> Self {
        Self {
            requests: Api::all(client.clone()),
            reviews: Api::all(client),
            signer_name,
        }
    }

    /// The signer name records are filed under.
    #[must_use]
    pub fn signer_name(&self) -> &str {
        &self.signer_name
    }

    /// The rights a record needs that this identity lacks, as `verb resource` strings.
    ///
    /// Empty when every call would be allowed. Asked once at startup, so a missing
    /// role shows before the first enrollment rather than in its log line.
    ///
    /// # Errors
    ///
    /// Returns the API error when a review cannot be made.
    pub async fn missing_access(&self) -> Result<Vec<String>, kube::Error> {
        let needed = [
            ("create", "certificatesigningrequests", None, None),
            ("patch", "certificatesigningrequests", Some("approval"), None),
            ("patch", "certificatesigningrequests", Some("status"), None),
            ("approve", "signers", None, Some(self.signer_name.as_str())),
            ("sign", "signers", None, Some(self.signer_name.as_str())),
        ];
        let mut missing = Vec::new();
        for (verb, resource, subresource, name) in needed {
            if !Box::pin(self.allowed(verb, resource, subresource, name)).await? {
                let what = subresource.map_or_else(|| resource.to_owned(), |sub| format!("{resource}/{sub}"));
                missing.push(format!("{verb} {what}"));
            }
        }
        Ok(missing)
    }

    /// Whether this identity may `verb` the certificates resource.
    async fn allowed(
        &self,
        verb: &str,
        resource: &str,
        subresource: Option<&str>,
        name: Option<&str>,
    ) -> Result<bool, kube::Error> {
        let review = SelfSubjectAccessReview {
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    group: Some("certificates.k8s.io".to_owned()),
                    resource: Some(resource.to_owned()),
                    subresource: subresource.map(str::to_owned),
                    name: name.map(str::to_owned),
                    verb: Some(verb.to_owned()),
                    ..ResourceAttributes::default()
                }),
                ..SelfSubjectAccessReviewSpec::default()
            },
            ..SelfSubjectAccessReview::default()
        };
        let answer = self.reviews.create(&PostParams::default(), &review).await?;
        Ok(answer.status.is_some_and(|status| status.allowed))
    }

    /// Create the request, approve it, and complete it with the issued certificate.
    /// Returns the record's name.
    ///
    /// # Errors
    ///
    /// Returns which call failed. A partial record, created but not approved or
    /// completed, is left for the operator to read and the API server to collect.
    pub async fn record(&self, record: &Record) -> Result<String, RecordError> {
        let name = record_name(&record.site, record.enrollment_id);
        // Boxed: each kube call builds a large future, kept off this frame.
        Box::pin(self.create(&name, record)).await?;
        Box::pin(self.approve(&name)).await?;
        Box::pin(self.complete(&name, &record.cert_pem)).await?;
        Ok(name)
    }

    /// Create the request. A name already taken is refused, not adopted: nothing
    /// retries a record, so an existing object under this name is not this
    /// service's.
    async fn create(&self, name: &str, record: &Record) -> Result<(), RecordError> {
        let request = build(name, &self.signer_name, &record.site, &record.csr_pem)
            .ok_or_else(|| RecordError::Request { name: name.to_owned() })?;
        let post = PostParams {
            field_manager: Some(FIELD_MANAGER.to_owned()),
            ..PostParams::default()
        };
        self.requests
            .create(&post, &request)
            .await
            .map(|_created| ())
            .map_err(|source| RecordError::Create {
                name: name.to_owned(),
                source,
            })
    }

    /// Approve the request on the invite's authority.
    async fn approve(&self, name: &str) -> Result<(), RecordError> {
        self.requests
            .patch_approval(name, &patch_params(), &Patch::Merge(approval_patch()))
            .await
            .map(|_approved| ())
            .map_err(|source| RecordError::Approve {
                name: name.to_owned(),
                source,
            })
    }

    /// Complete the request with the issued certificate.
    async fn complete(&self, name: &str, cert_pem: &str) -> Result<(), RecordError> {
        self.requests
            .patch_status(name, &patch_params(), &Patch::Merge(certificate_patch(cert_pem)))
            .await
            .map(|_completed| ())
            .map_err(|source| RecordError::Complete {
                name: name.to_owned(),
                source,
            })
    }
}

/// Write `record` off the request path, within the deadline, and log the outcome.
pub fn spawn(recorder: Arc<Recorder>, record: Record) {
    tokio::spawn(async move {
        let outcome = match tokio::time::timeout(RECORD_DEADLINE, recorder.record(&record)).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(RecordError::Deadline {
                name: record_name(&record.site, record.enrollment_id),
                deadline: RECORD_DEADLINE,
            }),
        };
        match outcome {
            Ok(written) => {
                tracing::info!(enrollment_id = %record.enrollment_id, record = %written, "enrollment recorded");
            },
            Err(error) => {
                tracing::error!(enrollment_id = %record.enrollment_id, error = %error.summary(), "enrollment not recorded");
            },
        }
    });
}

/// The field manager on every patch.
fn patch_params() -> PatchParams {
    PatchParams {
        field_manager: Some(FIELD_MANAGER.to_owned()),
        ..PatchParams::default()
    }
}

/// `<site>-<enrollment id>`: deterministic, so a retry names the same record.
#[must_use]
pub fn record_name(site: &str, enrollment_id: Uuid) -> String {
    format!("{site}-{}", enrollment_id.simple())
}

/// The record for one enrollment, before approval. `None` when the request is
/// not a PEM block.
#[must_use]
pub fn build(name: &str, signer_name: &str, site: &str, csr_pem: &str) -> Option<CertificateSigningRequest> {
    Some(CertificateSigningRequest {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            labels: Some([(SITE_LABEL.to_owned(), site.to_owned())].into()),
            ..ObjectMeta::default()
        },
        spec: CertificateSigningRequestSpec {
            signer_name: signer_name.to_owned(),
            request: ByteString(canonical_request(csr_pem)?.into_bytes()),
            usages: Some(USAGES.iter().map(|usage| (*usage).to_owned()).collect()),
            ..CertificateSigningRequestSpec::default()
        },
        status: None,
    })
}

/// The one request block, under the PEM label the API server parses. A legacy
/// `NEW CERTIFICATE REQUEST` block enrolls and must still record; anything that
/// is not exactly one request block records nothing, so no other material can
/// reach the object or an error message.
fn canonical_request(csr_pem: &str) -> Option<String> {
    let blocks = pem::parse_many(csr_pem).ok()?;
    let [block] = blocks.as_slice() else {
        return None;
    };
    if !REQUEST_LABELS.contains(&block.tag()) {
        return None;
    }
    let config = pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF);
    Some(pem::encode_config(
        &pem::Pem::new(CSR_PEM_LABEL, block.contents().to_vec()),
        config,
    ))
}

/// The approval condition, as a merge patch on the approval subresource. Only
/// `status` is sent: the typed object would also send an empty immutable spec.
fn approval_patch() -> serde_json::Value {
    serde_json::json!({
        "status": {
            "conditions": [{
                "type": "Approved",
                "status": "True",
                "reason": APPROVED_REASON,
                "message": "issued on the single-use invite's authority",
            }]
        }
    })
}

/// The issued certificate, as a merge patch on the status subresource.
fn certificate_patch(cert_pem: &str) -> serde_json::Value {
    serde_json::json!({ "status": { "certificate": ByteString(cert_pem.as_bytes().to_vec()) } })
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use std::sync::{Arc, Mutex};

    use http_body_util::BodyExt as _;
    use kube::client::Body;
    use rcgen::{CertificateParams, DnType, KeyPair};
    use uuid::Uuid;

    use super::{
        APPROVED_REASON, DEFAULT_SIGNER_NAME, Record, RecordError, Recorder, SITE_LABEL, approval_patch, build,
        canonical_request, certificate_patch, record_name,
    };

    const ID: Uuid = Uuid::from_u128(0x9F86_D081_884C_7D65_9A2F_EAA0_C55A_D015);

    fn csr_pem() -> String {
        let key = KeyPair::generate().expect("key");
        let mut params = CertificateParams::default();
        params.distinguished_name.push(DnType::CommonName, "west");
        params.serialize_request(&key).expect("csr").pem().expect("pem")
    }

    #[test]
    fn the_name_is_the_site_and_the_whole_enrollment_id() {
        assert_eq!(record_name("west", ID), "west-9f86d081884c7d659a2feaa0c55ad015");
    }

    #[test]
    fn the_record_carries_the_request_under_the_signer_name() {
        let csr = csr_pem();
        let record = build("west-1", "grid.example/site", "west", &csr).expect("a PEM request builds");
        assert_eq!(record.metadata.name.as_deref(), Some("west-1"));
        assert_eq!(
            record
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(SITE_LABEL))
                .map(String::as_str),
            Some("west")
        );
        assert_eq!(record.spec.signer_name, "grid.example/site");
        assert_eq!(record.spec.request.0, csr.as_bytes());
        assert_eq!(record.spec.expiration_seconds, None, "the signer decides the lifetime");
        assert_eq!(
            record.spec.usages.as_deref(),
            Some(
                &[
                    "digital signature".to_owned(),
                    "server auth".to_owned(),
                    "client auth".to_owned()
                ][..]
            )
        );
        assert!(record.status.is_none(), "a new record is pending");
    }

    #[test]
    fn a_legacy_pem_label_is_recorded_under_the_label_the_api_server_parses() {
        let legacy = csr_pem().replace("CERTIFICATE REQUEST", "NEW CERTIFICATE REQUEST");
        let canonical = canonical_request(&legacy).expect("parses");
        assert!(canonical.starts_with("-----BEGIN CERTIFICATE REQUEST-----"));
        assert!(!canonical.contains("NEW CERTIFICATE"));
        assert!(build("n", DEFAULT_SIGNER_NAME, "west", "not a pem").is_none());
        let bundled = format!(
            "-----BEGIN PRIVATE KEY-----\nMA==\n-----END PRIVATE KEY-----\n{}",
            csr_pem()
        );
        assert!(canonical_request(&bundled).is_none(), "a bundle records nothing");
        let certificate = csr_pem().replace("CERTIFICATE REQUEST", "CERTIFICATE");
        assert!(
            canonical_request(&certificate).is_none(),
            "a non-request label records nothing"
        );
    }

    #[test]
    fn the_patches_touch_status_only() {
        let approval = approval_patch();
        assert!(
            approval.get("spec").is_none(),
            "an empty spec would be an immutable-field write"
        );
        assert_eq!(approval["status"]["conditions"][0]["type"], "Approved");
        assert_eq!(approval["status"]["conditions"][0]["status"], "True");
        assert_eq!(approval["status"]["conditions"][0]["reason"], "InviteRedeemed");
        assert_eq!(APPROVED_REASON, "InviteRedeemed");
        let complete = certificate_patch("-----BEGIN CERTIFICATE-----\n");
        assert!(complete.get("spec").is_none());
        assert_eq!(
            complete["status"]["certificate"],
            "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg=="
        );
        assert!(
            complete["status"].get("conditions").is_none(),
            "completion keeps the approval"
        );
    }

    /// One request the fake server saw: method, path, body.
    type Seen = (String, String, String);
    /// The fake server's log.
    type Log = Arc<Mutex<Vec<Seen>>>;

    /// A kube client whose API server is `respond`, recording every request it sees.
    fn fake_server<F>(respond: F) -> (kube::Client, Log)
    where
        F: Fn(&str, &str, usize) -> (u16, &'static str) + Send + Sync + Clone + 'static,
    {
        let seen: Log = Arc::default();
        let log = Arc::clone(&seen);
        let service = tower::service_fn(move |request: axum::http::Request<Body>| {
            let respond = respond.clone();
            let log = Arc::clone(&log);
            async move {
                let (parts, body) = request.into_parts();
                let body = body
                    .collect()
                    .await
                    .map(http_body_util::Collected::to_bytes)
                    .unwrap_or_default();
                let body = String::from_utf8_lossy(&body).into_owned();
                let path = parts.uri.path().to_owned();
                let method = parts.method.to_string();
                let count = {
                    let mut log = log.lock().expect("log");
                    log.push((method.clone(), path.clone(), body));
                    log.len()
                };
                let (code, reply) = respond(&method, &path, count);
                Ok::<_, std::convert::Infallible>(json_reply(code, reply))
            }
        });
        (kube::Client::new(service, "default"), seen)
    }

    /// A JSON response with `code` and `body`.
    fn json_reply(code: u16, body: &str) -> axum::http::Response<Body> {
        axum::http::Response::builder()
            .status(code)
            .header("content-type", "application/json")
            .body(Body::from(body.as_bytes().to_vec()))
            .expect("response")
    }

    const CREATED: &str = r#"{"apiVersion":"certificates.k8s.io/v1","kind":"CertificateSigningRequest","metadata":{"name":"x"},"spec":{"request":"","signerName":"s"}}"#;
    const CONFLICT: &str = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"exists","reason":"AlreadyExists","code":409}"#;
    const DENIED: &str = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"forbidden","reason":"Forbidden","code":403}"#;

    fn record() -> Record {
        Record {
            site: "west".to_owned(),
            enrollment_id: ID,
            csr_pem: csr_pem(),
            cert_pem: "-----BEGIN CERTIFICATE-----\nMA==\n-----END CERTIFICATE-----\n".to_owned(),
        }
    }

    #[tokio::test]
    async fn a_record_is_three_calls_in_order_and_the_patches_carry_no_spec() {
        let (client, seen) = fake_server(|_method, _path, _count| (200, CREATED));
        let recorder = Recorder::new(client, DEFAULT_SIGNER_NAME.to_owned());
        let name = recorder.record(&record()).await.expect("recorded");
        let calls: Vec<Seen> = seen.lock().expect("log").clone();
        let base = "/apis/certificates.k8s.io/v1/certificatesigningrequests";
        let expected = [
            ("POST", base.to_owned()),
            ("PATCH", format!("{base}/{name}/approval")),
            ("PATCH", format!("{base}/{name}/status")),
        ];
        let routes: Vec<(&str, &str)> = calls
            .iter()
            .map(|(method, path, _body)| (method.as_str(), path.as_str()))
            .collect();
        let wanted: Vec<(&str, &str)> = expected.iter().map(|(method, path)| (*method, path.as_str())).collect();
        assert_eq!(routes, wanted);
        let created: serde_json::Value = serde_json::from_str(&calls[0].2).expect("the create body is JSON");
        let request: k8s_openapi::ByteString =
            serde_json::from_value(created["spec"]["request"].clone()).expect("the request is base64 bytes");
        assert!(
            String::from_utf8_lossy(&request.0).starts_with("-----BEGIN CERTIFICATE REQUEST-----"),
            "the create carries the PEM request"
        );
        assert!(!calls[1].2.contains("\"spec\""), "the approval patch carries no spec");
        assert!(!calls[2].2.contains("\"spec\""), "the status patch carries no spec");
    }

    #[tokio::test]
    async fn a_taken_name_is_refused_and_logged_by_code_not_message() {
        let (client, seen) = fake_server(|method, _path, _count| {
            if method == "POST" {
                (409, CONFLICT)
            } else {
                (200, CREATED)
            }
        });
        let recorder = Recorder::new(client, DEFAULT_SIGNER_NAME.to_owned());
        let error = recorder.record(&record()).await.expect_err("the name is taken");
        assert_eq!(seen.lock().expect("log").len(), 1, "nothing is adopted");
        let summary = error.summary();
        assert!(summary.contains("create refused: 409 AlreadyExists"), "{summary}");
        assert!(
            !summary.contains("exists\""),
            "the server's message is not repeated: {summary}"
        );
    }

    #[tokio::test]
    async fn the_error_names_the_call_that_failed() {
        let (client, _seen) = fake_server(|_method, path, _count| {
            if path.ends_with("/approval") {
                (403, DENIED)
            } else {
                (200, CREATED)
            }
        });
        let recorder = Recorder::new(client, DEFAULT_SIGNER_NAME.to_owned());
        let error = recorder.record(&record()).await.expect_err("approval is refused");
        assert!(
            matches!(&error, RecordError::Approve { name, .. } if name.starts_with("west-")),
            "{error}"
        );
    }
}
