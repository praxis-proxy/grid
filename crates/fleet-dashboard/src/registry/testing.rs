#![expect(
    clippy::expect_used,
    reason = "test support; a broken fake should abort the test loudly"
)]
//! An in-memory Kubernetes API for the `ConfigMap`s and Secrets of one
//! namespace, speaking just enough of the list/watch protocol for kube-rs
//! watchers: a LIST answers with the current objects and a resourceVersion;
//! a WATCH replays every event after that version, then streams new ones.

use std::{
    collections::BTreeMap,
    convert::Infallible,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use bytes::Bytes;
use futures::StreamExt as _;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use hyper::body::Frame;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use percent_encoding::percent_decode_str;
use serde::Serialize;
use tokio::sync::Notify;

/// The namespace every fake object lives in.
pub(super) const NAMESPACE: &str = "aigrid-fleet";

/// One resource kind's objects and event log.
struct Kind {
    /// The `kind` of the list envelope.
    envelope: &'static str,
    /// Current objects by name.
    items: BTreeMap<String, serde_json::Value>,
    /// Every event since the beginning: `(object name, JSON line)`.
    events: Vec<(String, Bytes)>,
    /// Set to make every open watch stream report `410 Gone` once, as the API
    /// server does after compaction, so watchers list again.
    expire: bool,
    /// LIST requests served.
    lists: usize,
    /// Wakes watch streams on a new event or a disconnect.
    notify: Arc<Notify>,
}

impl Kind {
    fn new(envelope: &'static str) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            envelope,
            items: BTreeMap::new(),
            events: Vec::new(),
            expire: false,
            lists: 0,
            notify: Arc::new(Notify::new()),
        }))
    }
}

/// The fake API: build a client with [`FakeApi::client`], then mutate objects.
pub(super) struct FakeApi {
    /// `ConfigMap`s in [`NAMESPACE`].
    configmaps: Arc<Mutex<Kind>>,
    /// Secrets in [`NAMESPACE`].
    secrets: Arc<Mutex<Kind>>,
}

impl FakeApi {
    /// An empty API.
    pub(super) fn new() -> Self {
        Self {
            configmaps: Kind::new("ConfigMapList"),
            secrets: Kind::new("SecretList"),
        }
    }

    /// A kube client talking to this fake.
    pub(super) fn client(&self) -> kube::Client {
        let (configmaps, secrets) = (Arc::clone(&self.configmaps), Arc::clone(&self.secrets));
        let service = tower::service_fn(move |request: Request<kube::client::Body>| {
            let (configmaps, secrets) = (Arc::clone(&configmaps), Arc::clone(&secrets));
            async move { Ok::<_, Infallible>(respond(&configmaps, &secrets, &request)) }
        });
        kube::Client::new(service, NAMESPACE)
    }

    /// Creates or replaces a `ConfigMap`, emitting ADDED or MODIFIED.
    pub(super) fn put_configmap(&self, configmap: &ConfigMap) {
        put(
            &self.configmaps,
            configmap.metadata.name.clone().expect("named configmap"),
            configmap,
        );
    }

    /// Deletes a `ConfigMap`, emitting DELETED.
    pub(super) fn delete_configmap(&self, name: &str) {
        delete(&self.configmaps, name);
    }

    /// Creates or replaces a Secret, emitting ADDED or MODIFIED.
    pub(super) fn put_secret(&self, secret: &Secret) {
        put(
            &self.secrets,
            secret.metadata.name.clone().expect("named secret"),
            secret,
        );
    }

    /// Deletes a Secret, emitting DELETED.
    pub(super) fn delete_secret(&self, name: &str) {
        delete(&self.secrets, name);
    }

    /// Removes a Secret from the object set without any event, so only a
    /// relist can observe its absence.
    pub(super) fn forget_secret_silently(&self, name: &str) {
        lock(&self.secrets).items.remove(name);
    }

    /// Makes every open watch report `410 Gone` and end, forcing watchers to
    /// list again.
    pub(super) fn restart_watches(&self) {
        for kind in [&self.configmaps, &self.secrets] {
            let mut kind = lock(kind);
            kind.expire = true;
            kind.notify.notify_waiters();
        }
    }

    /// LIST requests served so far, across both kinds.
    pub(super) fn list_count(&self) -> usize {
        lock(&self.configmaps).lists.saturating_add(lock(&self.secrets).lists)
    }
}

/// Locks a kind, tolerating poison from a panicked test.
fn lock(kind: &Arc<Mutex<Kind>>) -> MutexGuard<'_, Kind> {
    kind.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Stores `object` under `name` and logs the matching event.
fn put<T: Serialize>(kind: &Arc<Mutex<Kind>>, name: String, object: &T) {
    let json = serde_json::to_value(object).expect("serializable object");
    let mut guard = lock(kind);
    let event_type = if guard.items.contains_key(&name) {
        "MODIFIED"
    } else {
        "ADDED"
    };
    guard.items.insert(name.clone(), json.clone());
    emit(&mut guard, name, event_type, &json);
    drop(guard);
}

/// Removes `name` and logs DELETED with the last known object.
fn delete(kind: &Arc<Mutex<Kind>>, name: &str) {
    let mut kind = lock(kind);
    if let Some(json) = kind.items.remove(name) {
        emit(&mut kind, name.to_owned(), "DELETED", &json);
    }
}

/// Appends one watch event line and wakes streams.
fn emit(kind: &mut Kind, name: String, event_type: &str, object: &serde_json::Value) {
    let line = serde_json::json!({ "type": event_type, "object": object });
    let mut bytes = serde_json::to_vec(&line).expect("serializable event");
    bytes.push(b'\n');
    kind.events.push((name, Bytes::from(bytes)));
    kind.notify.notify_waiters();
}

/// Routes one request to a list or a watch of the right kind.
fn respond(
    configmaps: &Arc<Mutex<Kind>>,
    secrets: &Arc<Mutex<Kind>>,
    request: &Request<kube::client::Body>,
) -> Response<BoxBody<Bytes, Infallible>> {
    let path = request.uri().path();
    let kind = if path.ends_with("/configmaps") {
        configmaps
    } else if path.ends_with("/secrets") {
        secrets
    } else {
        return status(StatusCode::NOT_FOUND, format!("fake api: no route for {path}"));
    };
    let params = query_params(request.uri().query().unwrap_or_default());
    let wanted = params
        .get("fieldSelector")
        .and_then(|selector| selector.strip_prefix("metadata.name="))
        .map(str::to_owned);
    if params.get("watch").is_some_and(|flag| flag == "true") {
        let from = params
            .get("resourceVersion")
            .and_then(|version| version.parse().ok())
            .unwrap_or(0);
        watch(kind, wanted, from)
    } else {
        list(kind, wanted.as_deref())
    }
}

/// Decodes `a=b&c=d` into a map.
fn query_params(query: &str) -> BTreeMap<String, String> {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| {
            (
                percent_decode_str(key).decode_utf8_lossy().into_owned(),
                percent_decode_str(value).decode_utf8_lossy().into_owned(),
            )
        })
        .collect()
}

/// The current objects, optionally only the one named `wanted`.
fn list(kind: &Arc<Mutex<Kind>>, wanted: Option<&str>) -> Response<BoxBody<Bytes, Infallible>> {
    let mut guard = lock(kind);
    guard.lists = guard.lists.saturating_add(1);
    let items: Vec<&serde_json::Value> = guard
        .items
        .iter()
        .filter(|(name, _)| wanted.is_none_or(|wanted| wanted == name.as_str()))
        .map(|(_, json)| json)
        .collect();
    let body = serde_json::json!({
        "kind": guard.envelope,
        "apiVersion": "v1",
        "metadata": { "resourceVersion": guard.events.len().to_string() },
        "items": items,
    });
    drop(guard);
    status(StatusCode::OK, body.to_string())
}

/// The `410 Gone` watch event that tells a watcher to list again.
const GONE: &str = concat!(
    r#"{"type":"ERROR","object":{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","#,
    r#""message":"too old resource version","reason":"Expired","code":410}}"#,
    "
"
);

/// A stream of every event after `from`, then new ones, until the next
/// [`FakeApi::restart_watches`] makes it report `410 Gone` and end.
fn watch(kind: &Arc<Mutex<Kind>>, wanted: Option<String>, from: usize) -> Response<BoxBody<Bytes, Infallible>> {
    let lines = event_lines(Arc::clone(kind), wanted, from);
    let body = StreamBody::new(lines.map(|line| Ok::<_, Infallible>(Frame::data(line))));
    Response::builder()
        .status(StatusCode::OK)
        .body(BodyExt::boxed(body))
        .expect("response")
}

/// The watch event lines for `wanted` (or every object) after `from`.
fn event_lines(kind: Arc<Mutex<Kind>>, wanted: Option<String>, from: usize) -> impl futures::Stream<Item = Bytes> {
    futures::stream::unfold((kind, from, false), move |(kind, mut next, ended)| {
        let wanted = wanted.clone();
        async move {
            if ended {
                return None;
            }
            loop {
                let notified = {
                    let mut guard = lock(&kind);
                    if guard.expire {
                        guard.expire = false;
                        return Some((Bytes::from_static(GONE.as_bytes()), (Arc::clone(&kind), next, true)));
                    }
                    if let Some((name, line)) = guard.events.get(next) {
                        next = next.saturating_add(1);
                        if wanted.as_deref().is_none_or(|wanted| wanted == name) {
                            return Some((line.clone(), (Arc::clone(&kind), next, false)));
                        }
                        continue;
                    }
                    Arc::clone(&guard.notify)
                };
                notified.notified().await;
            }
        }
    })
}

/// A complete response with a fixed body.
fn status(code: StatusCode, body: String) -> Response<BoxBody<Bytes, Infallible>> {
    Response::builder()
        .status(code)
        .body(Full::new(Bytes::from(body)).boxed())
        .expect("response")
}
