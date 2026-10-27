//! An enrollment in flight, kept beside the identity so a restart or a lost answer resends
//! the same request and an issued certificate survives until it is stored.

use std::collections::BTreeMap;

use k8s_openapi::{ByteString, api::core::v1::Secret};
use kube::api::ObjectMeta;
use serde_json::{Value, json};
use zeroize::Zeroizing;

use super::{Attempt, Backoff, Created, EnrollError, Issued, MANAGED_BY, Settings, Store, Target, with_backoff};

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

/// The site the request names.
const SITE: &str = "grid.praxis.fast/enrollment-site";

/// A digest of the grid CA the answer must anchor to.
const ANCHOR: &str = "grid.praxis.fast/enrollment-anchor";

/// A digest of the invite the request is sent with.
const INVITE: &str = "grid.praxis.fast/enrollment-invite";

/// Every annotation written here.
const ANNOTATIONS: [&str; 4] = [UNANSWERED, SITE, ANCHOR, INVITE];

/// The Secret holding `target`'s enrollment in flight.
pub(super) fn name(target: &Target) -> String {
    name_for(&target.site_secret)
}

/// The Secret holding the enrollment in flight for the identity Secret `site_secret`.
fn name_for(site_secret: &str) -> String {
    format!("{site_secret}-pending")
}

/// Whether `secrets` holds enrollment's own request in flight for the identity Secret
/// `site_secret`, which only a configured site leaves.
pub(super) async fn held_for(secrets: &kube::Api<Secret>, site_secret: &str) -> Result<bool, kube::Error> {
    let name = name_for(site_secret);
    let held = secrets.get_opt(&name).await?;
    Ok(held.is_some_and(|secret| ours(&name, &secret.metadata, secret.type_.as_deref()).is_ok()))
}

/// What a kept request was made for: a request made for anything else is discarded.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Binding {
    /// The site name.
    site: String,
    /// A digest of the grid CA the answer must anchor to.
    anchor: String,
    /// A digest of the invite.
    invite: String,
}

impl Binding {
    /// What a request under `settings` with the invite `token` is made for.
    pub(super) fn of(settings: &Settings, token: &str) -> Self {
        Self {
            site: settings.site_name.clone(),
            anchor: digest(settings.anchor_pem.as_bytes()),
            invite: digest(token.as_bytes()),
        }
    }

    /// The binding `meta` records, `None` when one is missing.
    fn recorded(meta: &ObjectMeta) -> Option<Self> {
        let annotations = meta.annotations.as_ref()?;
        Some(Self {
            site: annotations.get(SITE)?.clone(),
            anchor: annotations.get(ANCHOR)?.clone(),
            invite: annotations.get(INVITE)?.clone(),
        })
    }
}

/// `bytes`' SHA-256, in hex.
fn digest(bytes: &[u8]) -> String {
    crate::resources::tls_backend::sha256(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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

/// Refuse a Secret by the pending name that enrollment did not write, so a planted one is never
/// adopted: it carries exactly enrollment's label, only its annotations, and no owner or finalizer.
fn ours(name: &str, meta: &ObjectMeta, type_: Option<&str>) -> Result<(), EnrollError> {
    let why = foreign(meta).or_else(|| (type_ != Some("Opaque")).then_some("it is not of type Opaque"));
    match why {
        None => Ok(()),
        Some(why) => Err(EnrollError::PendingForeign(format!(
            "Secret {name} was not written by enrollment ({why}); delete it so enrollment can keep its request \
             there"
        ))),
    }
}

/// Why `meta` is not enrollment's, as far as its metadata shows.
fn foreign(meta: &ObjectMeta) -> Option<&'static str> {
    let labelled = meta.labels.as_ref().is_some_and(|labels| {
        labels.len() == 1 && labels.get("app.kubernetes.io/managed-by").map(String::as_str) == Some(MANAGED_BY)
    });
    let annotated = meta
        .annotations
        .iter()
        .flatten()
        .all(|(key, _)| ANNOTATIONS.contains(&key.as_str()));
    if !labelled {
        Some("it lacks exactly the label app.kubernetes.io/managed-by=grid-operator")
    } else if !annotated {
        Some("it carries annotations enrollment does not write")
    } else if meta.owner_references.as_ref().is_some_and(|owners| !owners.is_empty()) {
        Some("it has owner references")
    } else if meta
        .finalizers
        .as_ref()
        .is_some_and(|finalizers| !finalizers.is_empty())
    {
        Some("it has finalizers")
    } else {
        None
    }
}

/// One value of `data` as text.
fn text(data: &BTreeMap<String, ByteString>, key: &str) -> Option<String> {
    data.get(key).and_then(|value| String::from_utf8(value.0.clone()).ok())
}

/// The enrollment in flight for `target`, made for `binding`. `None` when none is held. One
/// made for anything else, holding a key not its request's, or whose kept certificate expired
/// is discarded.
pub(super) async fn read<S: Store + Sync>(
    store: &S,
    target: &Target,
    binding: &Binding,
    backoff: Backoff,
) -> Result<Option<Pending>, EnrollError> {
    let name = name(target);
    let Some(secret) = store.get(&name).await? else {
        return Ok(None);
    };
    ours(&name, &secret.metadata, secret.type_.as_deref())?;
    let recorded = Binding::recorded(&secret.metadata);
    let Some(held) = of(name, secret) else {
        return Ok(None);
    };
    let why = if recorded.as_ref() != Some(binding) {
        Some("it was made for another site name, grid CA or invite")
    } else if !certs::key_matches_csr(&held.key, &held.csr) {
        Some("its key is not the request's")
    } else if held.issued.as_ref().is_some_and(|issued| expired(&issued.certificate)) {
        Some("the certificate kept for it has expired")
    } else {
        None
    };
    if let Some(why) = why {
        tracing::warn!(secret = %held.name, why, "discarding the enrollment request in flight");
        clear(store, target, backoff).await?;
        return Ok(None);
    }
    Ok(Some(held))
}

/// Whether `certificate` is past its validity, or unreadable.
fn expired(certificate: &str) -> bool {
    let now = time::OffsetDateTime::now_utc();
    !certs::cert_validity(certificate).is_ok_and(|(_, not_after)| not_after > now)
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

/// Keep `csr` and `key` for `target`, made for `binding`, before the request goes out. The
/// Secret is created; one by its name is reused only when enrollment wrote it.
pub(super) async fn stash<S: Store + Sync>(
    store: &S,
    target: &Target,
    (csr, key): (String, Zeroizing<String>),
    (binding, backoff): (&Binding, Backoff),
) -> Result<Pending, EnrollError> {
    let name = name(target);
    let data = BTreeMap::from([
        (CSR.to_owned(), ByteString(csr.clone().into_bytes())),
        (KEY.to_owned(), ByteString(key.as_bytes().to_vec())),
    ]);
    let mut secret = super::secret(&name, "Opaque", data);
    secret.metadata.annotations = Some(BTreeMap::from([
        (SITE.to_owned(), binding.site.clone()),
        (ANCHOR.to_owned(), binding.anchor.clone()),
        (INVITE.to_owned(), binding.invite.clone()),
    ]));
    if !created(store, &secret, backoff).await? {
        Box::pin(reuse(store, &name, &secret, backoff)).await?;
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

/// Write `secret` over the Secret `name` enrollment left, refusing one it did not write.
async fn reuse<S: Store + Sync>(store: &S, name: &str, secret: &Secret, backoff: Backoff) -> Result<(), EnrollError> {
    let held = store
        .get(name)
        .await?
        .ok_or_else(|| EnrollError::Kube(format!("Secret {name} vanished under the write")))?;
    ours(name, &held.metadata, held.type_.as_deref())?;
    let patch = overwrite(secret, held.metadata.resource_version.as_deref());
    patched(store, name, &patch, backoff).await
}

/// The merge patch writing `secret` over the Secret at `version`. Every key and annotation is
/// named, so the merge drops what an earlier request kept.
fn overwrite(secret: &Secret, version: Option<&str>) -> Value {
    let named = |keys: &[&str]| -> serde_json::Map<String, Value> {
        keys.iter().map(|key| ((*key).to_owned(), Value::Null)).collect()
    };
    let mut annotations = named(&ANNOTATIONS);
    annotations.extend(
        secret
            .metadata
            .annotations
            .iter()
            .flatten()
            .map(|(key, value)| (key.clone(), json!(value))),
    );
    let mut data = named(&[CSR, KEY, CERT, CA]);
    data.extend(
        secret
            .data
            .iter()
            .flatten()
            .map(|(key, value)| (key.clone(), json!(value))),
    );
    json!({
        "metadata": { "resourceVersion": version, "annotations": annotations },
        "data": data,
    })
}

/// Create `secret`, `false` when one by its name exists, retrying what is transient.
async fn created<S: Store + Sync>(store: &S, secret: &Secret, backoff: Backoff) -> Result<bool, EnrollError> {
    with_backoff(
        backoff,
        "keeping the enrollment request",
        EnrollError::Kube,
        || async move {
            Ok(match store.create(secret).await? {
                Created::Yes => Attempt::Done(true),
                Created::AlreadyExists => Attempt::Done(false),
                Created::Retry(reason) => Attempt::Retry(reason),
            })
        },
    )
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
    with_backoff(backoff, "keeping the enrollment in flight", EnrollError::Kube, || {
        store.patch(name, patch)
    })
    .await
}

impl Pending {
    /// Mark a request about to go out as unanswered.
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

/// What [`clear`] found.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Swept {
    /// No Secret by the pending name, or one enrollment wrote and holds nothing.
    Nothing,
    /// Enrollment's Secret held a request or key, now gone.
    Cleared,
    /// A Secret by the pending name that enrollment did not write, left alone, with why.
    Foreign(String),
}

/// The patch clearing `secret`'s request, key and answer, conditioned on the version and
/// UID of the object as read, so it applies to that object and no other.
fn clearing(secret: &Secret) -> Value {
    let mut metadata = serde_json::Map::new();
    for (key, value) in [
        ("resourceVersion", &secret.metadata.resource_version),
        ("uid", &secret.metadata.uid),
    ] {
        if let Some(value) = value {
            metadata.insert(key.to_owned(), json!(value));
        }
    }
    metadata.insert(
        "annotations".to_owned(),
        json!({ UNANSWERED: Value::Null, SITE: Value::Null, ANCHOR: Value::Null, INVITE: Value::Null }),
    );
    json!({
        "metadata": metadata,
        "data": { CSR: Value::Null, KEY: Value::Null, CERT: Value::Null, CA: Value::Null },
    })
}

/// Clear `target`'s enrollment in flight, leaving no key behind. Idempotent: a Secret already
/// cleared is not written again. A Secret by the pending name that enrollment did not write is
/// left alone. The write names the object it judged, so a Secret replaced or rewritten after
/// the read is refused by the API server, read again and judged again rather than stripped.
pub(super) async fn clear<S: Store + Sync>(store: &S, target: &Target, backoff: Backoff) -> Result<Swept, EnrollError> {
    let name = name(target);
    let name = &name;
    with_backoff(
        backoff,
        "clearing the enrollment kept",
        EnrollError::Kube,
        || async move {
            let Some(secret) = store.get(name).await? else {
                return Ok(Attempt::Done(Swept::Nothing));
            };
            // The whole object decides, as it does before a request is kept there: a Secret of
            // another type with enrollment's labels is not enrollment's either.
            if let Err(why) = ours(name, &secret.metadata, secret.type_.as_deref()) {
                return Ok(Attempt::Done(Swept::Foreign(format!("{why}, left alone"))));
            }
            if secret.data.as_ref().is_none_or(BTreeMap::is_empty) {
                return Ok(Attempt::Done(Swept::Nothing));
            }
            // A write the API server refuses for its version or UID, or for anything else it
            // answers, is read again and judged again rather than forced.
            Ok(match store.patch(name, &clearing(&secret)).await {
                Ok(Attempt::Done(())) => Attempt::Done(Swept::Cleared),
                Ok(Attempt::Retry(reason)) | Err(EnrollError::Kube(reason)) => Attempt::Retry(reason),
                Err(error) => return Err(error),
            })
        },
    )
    .await
}
