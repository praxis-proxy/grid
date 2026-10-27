//! An enrollment in flight, kept beside the identity so a restart or a lost answer resends
//! the same request and an issued certificate survives until it is stored.

use std::collections::BTreeMap;

use k8s_openapi::{ByteString, api::core::v1::Secret};
use serde_json::{Value, json};
use zeroize::Zeroizing;

use super::{Attempt, Backoff, Created, EnrollError, Issued, Store, Target, with_backoff};

/// The request, PEM.
const CSR: &str = "csr.pem";

/// Its key, PEM.
const KEY: &str = "tls.key";

/// The issued leaf, PEM.
const CERT: &str = "tls.crt";

/// The CA it was issued under, PEM.
const CA: &str = "ca.crt";

/// Set while a request has gone out and no definitive answer came back.
const UNANSWERED: &str = "grid.praxis.fast/enrollment-unanswered";

/// The Secret holding `target`'s enrollment in flight.
pub(super) fn name(target: &Target) -> String {
    format!("{}-pending", target.site_secret)
}

/// An enrollment in flight.
pub(super) struct Pending {
    /// The Secret's name.
    name: String,
    /// The request, PEM.
    pub(super) csr: String,
    /// Its key, PEM.
    pub(super) key: Zeroizing<String>,
    /// What the signer issued for it, once it answered.
    pub(super) issued: Option<Issued>,
    /// Whether an earlier request went out with no definitive answer.
    pub(super) unanswered: bool,
    /// The resource version a write must match.
    version: String,
}

/// One value of `secret` as text.
fn text(data: &BTreeMap<String, ByteString>, key: &str) -> Option<String> {
    data.get(key).and_then(|value| String::from_utf8(value.0.clone()).ok())
}

/// The enrollment in flight for `target`, `None` when none is held.
pub(super) async fn read<S: Store + Sync>(store: &S, target: &Target) -> Result<Option<Pending>, EnrollError> {
    let name = name(target);
    let Some(secret) = store.get(&name).await? else {
        return Ok(None);
    };
    Ok(of(name, secret))
}

/// The enrollment `secret` holds, `None` once cleared.
fn of(name: String, secret: Secret) -> Option<Pending> {
    let unanswered = secret
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(UNANSWERED))
        .is_some_and(|value| value == "true");
    let version = secret.metadata.resource_version.unwrap_or_default();
    let data = secret.data.unwrap_or_default();
    let issued = text(&data, CERT)
        .zip(text(&data, CA))
        .map(|(certificate, ca_certificate)| Issued {
            certificate,
            ca_certificate,
        });
    Some(Pending {
        csr: text(&data, CSR)?,
        key: Zeroizing::new(text(&data, KEY)?),
        name,
        issued,
        unanswered,
        version,
    })
}

/// Keep `csr` and `key` for `target` before the request goes out. A cleared Secret left by
/// an earlier enrollment is reused.
pub(super) async fn stash<S: Store + Sync>(
    store: &S,
    target: &Target,
    (csr, key): (String, Zeroizing<String>),
    backoff: Backoff,
) -> Result<Pending, EnrollError> {
    let name = name(target);
    let data = BTreeMap::from([
        (CSR.to_owned(), ByteString(csr.clone().into_bytes())),
        (KEY.to_owned(), ByteString(key.as_bytes().to_vec())),
    ]);
    if !created(store, &super::secret(&name, "Opaque", data), backoff).await? {
        let version = version_of(store, &name).await?;
        let patch = json!({
            "metadata": { "resourceVersion": version, "annotations": { UNANSWERED: Value::Null } },
            "data": { CSR: ByteString(csr.clone().into_bytes()), KEY: ByteString(key.as_bytes().to_vec()) },
        });
        patched(store, &name, &patch, backoff).await?;
    }
    Ok(Pending {
        version: version_of(store, &name).await?,
        name,
        csr,
        key,
        issued: None,
        unanswered: false,
    })
}

/// Create `secret`, `false` when one by its name exists, retrying what is transient.
async fn created<S: Store + Sync>(store: &S, secret: &Secret, backoff: Backoff) -> Result<bool, EnrollError> {
    with_backoff(backoff, "keeping the enrollment request", || async move {
        Ok(match store.create(secret).await? {
            Created::Yes => Attempt::Done(true),
            Created::AlreadyExists => Attempt::Done(false),
            Created::Retry(reason) => Attempt::Retry(reason),
        })
    })
    .await
}

/// The resource version of Secret `name`.
async fn version_of<S: Store + Sync>(store: &S, name: &str) -> Result<String, EnrollError> {
    Ok(store
        .get(name)
        .await?
        .and_then(|held| held.metadata.resource_version)
        .unwrap_or_default())
}

/// Merge `patch` into Secret `name`, retrying what is transient.
async fn patched<S: Store + Sync>(store: &S, name: &str, patch: &Value, backoff: Backoff) -> Result<(), EnrollError> {
    with_backoff(backoff, "keeping the enrollment in flight", || store.patch(name, patch)).await
}

impl Pending {
    /// Mark a request about to go out again as unanswered.
    pub(super) async fn sending<S: Store + Sync>(&mut self, store: &S, backoff: Backoff) -> Result<(), EnrollError> {
        if self.unanswered {
            return Ok(());
        }
        let patch = json!({
            "metadata": { "resourceVersion": self.version, "annotations": { UNANSWERED: "true" } },
        });
        patched(store, &self.name, &patch, backoff).await?;
        self.refresh(store).await
    }

    /// Record a definitive answer that issued nothing, so a later refusal is not read as a
    /// spent invite.
    pub(super) async fn answered<S: Store + Sync>(&mut self, store: &S, backoff: Backoff) -> Result<(), EnrollError> {
        let patch = json!({
            "metadata": { "resourceVersion": self.version, "annotations": { UNANSWERED: Value::Null } },
        });
        patched(store, &self.name, &patch, backoff).await?;
        self.unanswered = false;
        self.refresh(store).await
    }

    /// Keep what the signer issued, so it outlives this process until it is stored.
    pub(super) async fn issued<S: Store + Sync>(
        &mut self,
        store: &S,
        issued: &Issued,
        backoff: Backoff,
    ) -> Result<(), EnrollError> {
        let patch = json!({
            "metadata": { "resourceVersion": self.version, "annotations": { UNANSWERED: Value::Null } },
            "data": {
                CERT: ByteString(issued.certificate.clone().into_bytes()),
                CA: ByteString(issued.ca_certificate.clone().into_bytes()),
            },
        });
        patched(store, &self.name, &patch, backoff).await?;
        self.unanswered = false;
        self.refresh(store).await
    }

    /// The resource version a next write must match.
    async fn refresh<S: Store + Sync>(&mut self, store: &S) -> Result<(), EnrollError> {
        self.version = version_of(store, &self.name).await?;
        Ok(())
    }
}

/// Clear `target`'s enrollment in flight once its identity is stored, leaving no key behind.
pub(super) async fn clear<S: Store + Sync>(store: &S, target: &Target, backoff: Backoff) -> Result<(), EnrollError> {
    let name = name(target);
    if !store.exists(&name).await? {
        return Ok(());
    }
    let patch = json!({
        "metadata": { "annotations": { UNANSWERED: Value::Null } },
        "data": { CSR: Value::Null, KEY: Value::Null, CERT: Value::Null, CA: Value::Null },
    });
    patched(store, &name, &patch, backoff).await
}
