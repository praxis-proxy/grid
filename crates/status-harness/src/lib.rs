//! An in-memory Kubernetes API for status conformance tests, and the status invariants.
//!
//! The API serves GET, LIST, POST, PUT, PATCH and DELETE over JSON objects keyed by
//! their API path, emulates server-side apply for `status.conditions` keyed by `type`
//! with per-manager ownership, and records every write with its field manager. A test
//! runs a reconciler against it for several rounds and checks the invariants in
//! [`invariants`] over what the API observed.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};

use serde_json::{Map, Value, json};

pub mod diff;
pub mod invariants;

/// Why an invariant failed. Its debug form is the text itself, so `expect` prints the
/// status table as written rather than as an escaped string.
pub struct Failure(String);

impl From<String> for Failure {
    fn from(why: String) -> Self {
        Self(why)
    }
}

impl std::fmt::Debug for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\n{}", self.0)
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One write the API received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Write {
    /// The HTTP method.
    pub method: String,
    /// The object path, without the `/status` suffix.
    pub path: String,
    /// Whether the write went to the status subresource.
    pub status: bool,
    /// The `fieldManager` query parameter, when given.
    pub manager: Option<String>,
    /// The request content type.
    pub content_type: String,
    /// The request body.
    pub body: Value,
}

/// What a round observed on one object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observed {
    /// `metadata.generation`.
    pub generation: i64,
    /// `status`, as stored.
    pub status: Value,
    /// `status.conditions`, as stored.
    pub conditions: Vec<Value>,
}

impl Observed {
    /// The condition of `type_`, if held.
    #[must_use]
    pub fn condition(&self, type_: &str) -> Option<&Value> {
        self.conditions
            .iter()
            .find(|condition| condition.get("type").and_then(Value::as_str) == Some(type_))
    }

    /// `status.<field>` as text, empty when absent.
    #[must_use]
    pub fn field(&self, field: &str) -> &str {
        self.status.get(field).and_then(Value::as_str).unwrap_or_default()
    }
}

/// The API's objects, writes and condition ownership.
#[derive(Clone, Default)]
struct Inner {
    /// Objects by path.
    objects: BTreeMap<String, Value>,
    /// Every write, in order.
    writes: Vec<Write>,
    /// Condition type to field manager, by object path.
    owners: BTreeMap<String, BTreeMap<String, String>>,
    /// The last resource version handed out.
    version: u64,
    /// Reads of a path that answer 500, as a transient API failure, and how many are left.
    failing: BTreeMap<String, usize>,
}

/// An in-memory Kubernetes API.
#[derive(Clone, Default)]
pub struct FakeApi {
    /// The shared state.
    inner: Arc<Mutex<Inner>>,
}

/// How a PATCH request addresses its object.
#[derive(Clone, Copy)]
struct PatchTarget<'req> {
    /// Whether the write went to the status subresource.
    status: bool,
    /// The field manager, for an apply.
    manager: Option<&'req str>,
    /// The request content type, which picks the patch kind.
    content_type: &'req str,
}

impl FakeApi {
    /// An empty API.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Store `object` at `path`, replacing what was there, with a fresh resource version.
    pub fn put(&self, path: &str, mut object: Value) {
        let mut inner = self.lock();
        let version = inner.next_version();
        stamp(&mut object, &version);
        inner.objects.insert(path.to_owned(), object);
    }

    /// Answer the next `times` reads of `path`, an object or a list, with a 500.
    pub fn fail_reads(&self, path: &str, times: usize) {
        self.lock().failing.insert(path.to_owned(), times);
    }

    /// The object at `path`.
    #[must_use]
    pub fn get(&self, path: &str) -> Option<Value> {
        self.lock().objects.get(path).cloned()
    }

    /// Remove the object at `path`.
    pub fn remove(&self, path: &str) {
        drop(self.lock().objects.remove(path));
    }

    /// Edit the object at `path` in place, bumping `metadata.generation` when `spec_change`.
    pub fn edit<F: FnOnce(&mut Value)>(&self, path: &str, spec_change: bool, edit: F) {
        let mut inner = self.lock();
        let version = inner.next_version();
        if let Some(object) = inner.objects.get_mut(path) {
            edit(object);
            if spec_change {
                let generation = object
                    .pointer("/metadata/generation")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    .saturating_add(1);
                set(object, "/metadata/generation", json!(generation));
            }
            stamp(object, &version);
        }
    }

    /// What is stored at `path`, as a round observes it.
    #[must_use]
    pub fn observe(&self, path: &str) -> Option<Observed> {
        let object = self.get(path)?;
        let status = object.get("status").cloned().unwrap_or(Value::Null);
        let conditions = status
            .get("conditions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Some(Observed {
            generation: object
                .pointer("/metadata/generation")
                .and_then(Value::as_i64)
                .unwrap_or(0),
            status,
            conditions,
        })
    }

    /// Every write so far.
    #[must_use]
    pub fn writes(&self) -> Vec<Write> {
        self.lock().writes.clone()
    }

    /// The field manager of each condition type on `path`, as applied.
    #[must_use]
    pub fn owners(&self, path: &str) -> BTreeMap<String, String> {
        self.lock().owners.get(path).cloned().unwrap_or_default()
    }

    /// A client for this API whose default namespace is `namespace`.
    #[must_use]
    pub fn client(&self, namespace: &str) -> kube::Client {
        let api = self.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let api = api.clone();
            async move {
                let (parts, body) = request.into_parts();
                let bytes = http_body_util::BodyExt::collect(body)
                    .await
                    .map(http_body_util::Collected::to_bytes)
                    .unwrap_or_default();
                let (code, value) = api.answer(&parts, &bytes);
                let payload = serde_json::to_vec(&value).unwrap_or_default();
                let mut response = http::Response::new(kube::client::Body::from(payload));
                *response.status_mut() =
                    http::StatusCode::from_u16(code).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static("application/json"),
                );
                Ok::<_, std::convert::Infallible>(response)
            }
        });
        kube::Client::new(service, namespace)
    }

    /// The state, through a poisoned lock too.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Answer one request.
    fn answer(&self, parts: &http::request::Parts, bytes: &[u8]) -> (u16, Value) {
        let write = Write::from_request(parts, bytes);
        let dry_run = query_param(parts.uri.query(), "dryRun").is_some();
        let mut inner = self.lock();
        // A dry run answers as the write would, and leaves nothing behind.
        let before = dry_run.then(|| inner.clone());
        if parts.method != http::Method::GET && !dry_run {
            inner.writes.push(write.clone());
        }
        let target = PatchTarget {
            status: write.status,
            manager: write.manager.as_deref(),
            content_type: &write.content_type,
        };
        let failed = parts.method == http::Method::GET && inner.scripted_failure(&write.path);
        let answer = match parts.method {
            http::Method::GET if failed => (500, failure(500, "InternalError", "scripted failure")),
            http::Method::GET => inner.read(&write.path),
            http::Method::POST => inner.create(&write.path, write.body),
            http::Method::PUT => inner.replace(&write.path, write.status, write.body),
            http::Method::PATCH => inner.patch(&write.path, target, &write.body),
            http::Method::DELETE => inner.delete(&write.path),
            _ => (405, failure(405, "MethodNotAllowed", "unsupported method")),
        };
        if let Some(before) = before {
            *inner = before;
        }
        answer
    }
}

impl Write {
    /// The request as a write record, whatever its method.
    fn from_request(parts: &http::request::Parts, bytes: &[u8]) -> Self {
        let (path, status) = split_status(parts.uri.path());
        Self {
            method: parts.method.to_string(),
            path,
            status,
            manager: query_param(parts.uri.query(), "fieldManager"),
            content_type: parts
                .headers
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned(),
            body: serde_json::from_slice(bytes)
                .or_else(|_json| serde_yaml::from_slice(bytes))
                .unwrap_or(Value::Null),
        }
    }
}

impl Inner {
    /// Whether this read of `path` answers a scripted 500, counting it down.
    fn scripted_failure(&mut self, path: &str) -> bool {
        self.failing.get_mut(path).is_some_and(|left| {
            let failing = *left > 0;
            *left = left.saturating_sub(1);
            failing
        })
    }

    /// The next resource version.
    fn next_version(&mut self) -> String {
        self.version = self.version.saturating_add(1);
        self.version.to_string()
    }

    /// GET an object, or list a collection.
    fn read(&self, path: &str) -> (u16, Value) {
        if let Some(object) = self.objects.get(path) {
            return (200, object.clone());
        }
        let prefix = format!("{path}/");
        let items: Vec<&Value> = self
            .objects
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.starts_with(&prefix))
            .filter(|(key, _)| !key.trim_start_matches(&prefix).contains('/'))
            .map(|(_, object)| object)
            .collect();
        if items.is_empty() && !is_collection(path) {
            return (404, failure(404, "NotFound", &format!("{path} not found")));
        }
        (
            200,
            json!({ "apiVersion": "v1", "kind": "List", "metadata": { "resourceVersion": self.version.to_string() }, "items": items }),
        )
    }

    /// POST an object into a collection.
    fn create(&mut self, collection: &str, mut body: Value) -> (u16, Value) {
        let Some(name) = body.pointer("/metadata/name").and_then(Value::as_str) else {
            return (422, failure(422, "Invalid", "metadata.name is required"));
        };
        let path = format!("{collection}/{name}");
        if self.objects.contains_key(&path) {
            return (409, failure(409, "AlreadyExists", &format!("{path} already exists")));
        }
        let version = self.next_version();
        stamp(&mut body, &version);
        if body.pointer("/metadata/generation").is_none() {
            set(&mut body, "/metadata/generation", json!(1));
        }
        self.objects.insert(path, body.clone());
        (201, body)
    }

    /// PUT an object, or its status when `status`.
    fn replace(&mut self, path: &str, status: bool, body: Value) -> (u16, Value) {
        let Some(held) = self.objects.get(path) else {
            return (404, failure(404, "NotFound", &format!("{path} not found")));
        };
        let mut object = subresource(held, body, status);
        let version = self.next_version();
        stamp(&mut object, &version);
        self.objects.insert(path.to_owned(), object.clone());
        (200, object)
    }

    /// DELETE an object.
    fn delete(&mut self, path: &str) -> (u16, Value) {
        match self.objects.remove(path) {
            Some(object) => (200, object),
            None => (404, failure(404, "NotFound", &format!("{path} not found"))),
        }
    }

    /// PATCH an object or its status.
    fn patch(&mut self, path: &str, target: PatchTarget<'_>, body: &Value) -> (u16, Value) {
        let Some(held) = self.objects.get(path).cloned() else {
            return (404, failure(404, "NotFound", &format!("{path} not found")));
        };
        let mut object = held.clone();
        let scope = if target.status { "/status" } else { "" };
        let content_type = target.content_type;
        let outcome = if content_type.starts_with("application/apply-patch") {
            let owners = self.owners.entry(path.to_owned()).or_default();
            apply(owners, &mut object, scope, target.manager.unwrap_or("unknown"), body)
        } else if content_type.starts_with("application/merge-patch") {
            merge_into(&mut object, scope, body);
            Ok(())
        } else if content_type.starts_with("application/json-patch") {
            json_patch(&mut object, body)
        } else {
            Err(format!("unsupported patch content type {content_type}"))
        };
        if let Err(why) = outcome {
            return (422, failure(422, "Invalid", &why));
        }
        let mut object = subresource(&held, object, target.status);
        let version = self.next_version();
        stamp(&mut object, &version);
        self.objects.insert(path.to_owned(), object.clone());
        (200, object)
    }
}

/// What a write may change of `held`, as the status subresource enforces: a status write
/// changes only `status`, and a main-resource write keeps the stored `status` and bumps
/// `metadata.generation` only when `spec` changed.
fn subresource(held: &Value, mut written: Value, status: bool) -> Value {
    if status {
        let mut object = held.clone();
        match written.get("status") {
            Some(value) => set(&mut object, "/status", value.clone()),
            None => remove_at(&mut object, "/status"),
        }
        return object;
    }
    match held.get("status") {
        Some(value) => set(&mut written, "/status", value.clone()),
        None => remove_at(&mut written, "/status"),
    }
    let generation = held
        .pointer("/metadata/generation")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let bumped = if written.get("spec") == held.get("spec") {
        generation
    } else {
        generation.saturating_add(1)
    };
    set(&mut written, "/metadata/generation", json!(bumped));
    written
}

/// Server-side apply `body` under `manager`, within `scope`, with `owners` the condition
/// ownership of that object.
fn apply(
    owners: &mut BTreeMap<String, String>,
    object: &mut Value,
    scope: &str,
    manager: &str,
    body: &Value,
) -> Result<(), String> {
    let applied = if scope.is_empty() {
        body.clone()
    } else {
        body.pointer(scope).cloned().unwrap_or(Value::Null)
    };
    let Some(applied) = applied.as_object() else {
        return Err("an apply carries an object".to_owned());
    };
    let target = scoped(object, scope)?;
    for (key, value) in applied {
        if key == "apiVersion" || key == "kind" || key == "metadata" {
            continue;
        }
        if key == "conditions" {
            apply_conditions(target, value, manager, owners);
        } else {
            set(target, &format!("/{key}"), value.clone());
        }
    }
    // Condition ownership belongs to the status subresource; a main-resource apply leaves it.
    if !scope.is_empty() && applied.get("conditions").is_none() && owners.values().any(|owner| owner == manager) {
        // An apply carrying no conditions gives up every condition this manager held.
        apply_conditions(target, &Value::Array(Vec::new()), manager, owners);
    }
    Ok(())
}

/// The object at `scope` inside `object`, created empty when absent or not an object.
fn scoped<'obj>(object: &'obj mut Value, scope: &str) -> Result<&'obj mut Value, String> {
    if object.pointer(scope).is_none_or(|node| !node.is_object()) {
        set(object, scope, json!({}));
    }
    object.pointer_mut(scope).ok_or_else(|| format!("no object at {scope}"))
}

/// Apply `conditions`, a list keyed by `type`, under `manager`: upsert what it carries,
/// remove what the manager held before and no longer carries.
fn apply_conditions(target: &mut Value, conditions: &Value, manager: &str, owners: &mut BTreeMap<String, String>) {
    let applied: Vec<Value> = conditions.as_array().cloned().unwrap_or_default();
    let applied_types: Vec<String> = applied.iter().filter_map(type_of).collect();
    let mut held: Vec<Value> = target
        .get("conditions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    held.retain(|condition| {
        type_of(condition).is_none_or(|type_| {
            owners.get(&type_).map(String::as_str) != Some(manager) || applied_types.contains(&type_)
        })
    });
    owners.retain(|type_, owner| owner != manager || applied_types.contains(type_));
    for condition in applied {
        if let Some(type_) = type_of(&condition) {
            // A held type keeps its position, as server-side apply merges a list map.
            match held
                .iter_mut()
                .find(|existing| type_of(existing).as_deref() == Some(type_.as_str()))
            {
                Some(existing) => *existing = condition,
                None => held.push(condition),
            }
            owners.insert(type_, manager.to_owned());
        }
    }
    set(target, "/conditions", Value::Array(held));
}

/// A condition's `type`.
fn type_of(condition: &Value) -> Option<String> {
    condition.get("type").and_then(Value::as_str).map(str::to_owned)
}

/// RFC 7386 merge of `patch` into `object` at `scope`: objects merge, `null` removes, the rest replaces.
fn merge_into(object: &mut Value, scope: &str, patch: &Value) {
    let patch = if scope.is_empty() {
        patch.clone()
    } else {
        // A status merge patch carries the whole document; take its scoped part.
        patch.pointer(scope).cloned().unwrap_or(Value::Null)
    };
    if object.pointer(scope).is_none() {
        set(object, scope, json!({}));
    }
    if let Some(target) = object.pointer_mut(scope) {
        merge_value(target, &patch);
    }
}

/// Merge `patch` into `target`.
fn merge_value(target: &mut Value, patch: &Value) {
    match (target.as_object_mut(), patch.as_object()) {
        (Some(target), Some(patch)) => {
            for (key, value) in patch {
                if value.is_null() {
                    drop(target.remove(key));
                } else {
                    merge_value(target.entry(key.clone()).or_insert(Value::Null), value);
                }
            }
        },
        _ => *target = patch.clone(),
    }
}

/// Apply a JSON patch: `add`, `replace`, `remove` and `test`, on JSON pointer paths.
fn json_patch(object: &mut Value, operations: &Value) -> Result<(), String> {
    let Some(operations) = operations.as_array() else {
        return Err("a json patch is an array".to_owned());
    };
    for operation in operations {
        let op = operation.get("op").and_then(Value::as_str).unwrap_or_default();
        let path = operation.get("path").and_then(Value::as_str).unwrap_or_default();
        let value = operation.get("value").cloned().unwrap_or(Value::Null);
        match op {
            "add" | "replace" => add_at(object, path, value),
            "remove" => remove_at(object, path),
            "test" => {
                let held = object.pointer(path).cloned().unwrap_or(Value::Null);
                if held != value {
                    return Err(format!("test failed at {path}"));
                }
            },
            other => return Err(format!("unsupported json patch op {other}")),
        }
    }
    Ok(())
}

/// Add `value` at `path`, appending when the last segment is `-`.
fn add_at(object: &mut Value, path: &str, value: Value) {
    if let Some(parent) = path.strip_suffix("/-") {
        if object.pointer(parent).is_none() {
            set(object, parent, Value::Array(Vec::new()));
        }
        if let Some(array) = object.pointer_mut(parent).and_then(Value::as_array_mut) {
            array.push(value);
        }
        return;
    }
    set(object, path, value);
}

/// Remove the value at `path`.
fn remove_at(object: &mut Value, path: &str) {
    let Some((parent, key)) = path.rsplit_once('/') else {
        return;
    };
    match object.pointer_mut(parent) {
        Some(Value::Object(map)) => drop(map.remove(key)),
        Some(Value::Array(array)) => {
            if let Ok(index) = key.parse::<usize>()
                && index < array.len()
            {
                drop(array.remove(index));
            }
        },
        _ => {},
    }
}

/// Set `value` at the JSON pointer `path`, creating objects on the way.
fn set(object: &mut Value, path: &str, value: Value) {
    if path.is_empty() {
        *object = value;
        return;
    }
    let mut node = object;
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let Some((last, parents)) = segments.split_last() else {
        return;
    };
    for segment in parents {
        if !node.is_object() {
            *node = Value::Object(Map::new());
        }
        node = match node.as_object_mut() {
            Some(map) => map
                .entry((*segment).to_owned())
                .or_insert_with(|| Value::Object(Map::new())),
            None => return,
        };
    }
    if !node.is_object() {
        *node = Value::Object(Map::new());
    }
    if let Some(map) = node.as_object_mut() {
        map.insert((*last).to_owned(), value);
    }
}

/// Stamp a resource version on `object`.
fn stamp(object: &mut Value, version: &str) {
    set(object, "/metadata/resourceVersion", json!(version));
}

/// Whether `path` names a collection rather than an object: an even depth below `/api/v1` or
/// `/apis/<group>/<version>`, counting a `namespaces/<name>` pair as scope.
fn is_collection(path: &str) -> bool {
    let segments: Vec<&str> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    let rest = match segments.first().copied() {
        Some("api") => segments.get(2..),
        Some("apis") => segments.get(3..),
        _ => None,
    };
    let Some(mut rest) = rest else {
        return false;
    };
    if rest.first().copied() == Some("namespaces") {
        rest = rest.get(2..).unwrap_or_default();
    }
    rest.len() == 1
}

/// Split a `/status` suffix off `path`.
fn split_status(path: &str) -> (String, bool) {
    path.strip_suffix("/status")
        .map_or_else(|| (path.to_owned(), false), |object| (object.to_owned(), true))
}

/// The value of `key` in `query`.
fn query_param(query: Option<&str>, key: &str) -> Option<String> {
    query?
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(name, _)| *name == key)
        .map(|(_, value)| value.to_owned())
}

/// A Kubernetes `Status` failure body.
fn failure(code: u16, reason: &str, message: &str) -> Value {
    json!({ "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": code, "reason": reason, "message": message })
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use serde_json::{Value, json};

    use super::FakeApi;

    const PATH: &str = "/apis/grid.praxis.fast/v1alpha1/gridsites/west";
    const APPLY: &str = "application/apply-patch+yaml";

    /// An API holding one object at generation 1 with a spec and a status.
    fn seeded() -> FakeApi {
        let api = FakeApi::new();
        api.put(
            PATH,
            json!({ "metadata": { "name": "west", "generation": 1 }, "spec": { "x": 1 }, "status": { "phase": "P" } }),
        );
        api
    }

    /// Send one request and return its status code.
    fn send(api: &FakeApi, method: &str, uri: &str, content_type: &str, body: &Value) -> u16 {
        let (parts, ()) = http::Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", content_type)
            .body(())
            .expect("request")
            .into_parts();
        api.answer(&parts, &serde_json::to_vec(body).expect("body")).0
    }

    /// Apply `conditions` to the status as `manager`.
    fn apply(api: &FakeApi, manager: &str, conditions: &[&str]) -> u16 {
        let conditions: Vec<Value> = conditions
            .iter()
            .map(|type_| json!({ "type": type_, "status": "True", "reason": type_ }))
            .collect();
        let uri = format!("{PATH}/status?fieldManager={manager}&force=true");
        send(
            api,
            "PATCH",
            &uri,
            APPLY,
            &json!({ "status": { "conditions": conditions } }),
        )
    }

    /// The condition types stored, in order.
    fn types(api: &FakeApi) -> Vec<String> {
        api.observe(PATH)
            .expect("stored")
            .conditions
            .iter()
            .filter_map(|condition| condition["type"].as_str().map(str::to_owned))
            .collect()
    }

    /// The stored object.
    fn stored(api: &FakeApi) -> Value {
        api.get(PATH).expect("stored")
    }

    #[test]
    fn a_status_put_changes_only_the_status() {
        let api = seeded();
        let body =
            json!({ "metadata": { "name": "west", "generation": 9 }, "spec": { "x": 2 }, "status": { "phase": "Q" } });
        assert_eq!(
            send(&api, "PUT", &format!("{PATH}/status"), "application/json", &body),
            200,
            "accepted"
        );
        let object = stored(&api);
        assert_eq!(object["spec"], json!({ "x": 1 }), "the spec is kept");
        assert_eq!(object["metadata"]["generation"], 1, "the generation is kept");
        assert_eq!(object["status"]["phase"], "Q", "the status is replaced");
    }

    #[test]
    fn a_main_put_keeps_the_status_and_bumps_the_generation_only_on_a_spec_change() {
        let api = seeded();
        let edited = json!({ "metadata": { "name": "west" }, "spec": { "x": 2 }, "status": { "phase": "Q" } });
        assert_eq!(send(&api, "PUT", PATH, "application/json", &edited), 200, "accepted");
        assert_eq!(
            stored(&api)["status"]["phase"],
            "P",
            "a main-resource write drops status"
        );
        assert_eq!(
            stored(&api)["metadata"]["generation"],
            2,
            "a spec change bumps the generation"
        );
        assert_eq!(
            send(&api, "PUT", PATH, "application/json", &edited),
            200,
            "accepted again"
        );
        assert_eq!(
            stored(&api)["metadata"]["generation"],
            2,
            "an unchanged spec keeps the generation"
        );
    }

    #[test]
    fn a_main_patch_drops_status_and_a_status_patch_drops_the_rest() {
        let api = seeded();
        let main = json!({ "metadata": { "labels": { "a": "b" } }, "status": { "phase": "Q" } });
        assert_eq!(
            send(&api, "PATCH", PATH, "application/merge-patch+json", &main),
            200,
            "merge"
        );
        assert_eq!(stored(&api)["metadata"]["labels"]["a"], "b", "metadata merges");
        assert_eq!(
            stored(&api)["status"]["phase"],
            "P",
            "a main-resource patch drops status"
        );
        assert_eq!(
            stored(&api)["metadata"]["generation"],
            1,
            "metadata alone keeps the generation"
        );
        let operations = json!([
            { "op": "replace", "path": "/spec/x", "value": 3 },
            { "op": "replace", "path": "/status/phase", "value": "R" }
        ]);
        let uri = format!("{PATH}/status");
        assert_eq!(
            send(&api, "PATCH", &uri, "application/json-patch+json", &operations),
            200,
            "json patch"
        );
        assert_eq!(stored(&api)["spec"]["x"], 1, "a status patch drops the spec change");
        assert_eq!(stored(&api)["status"]["phase"], "R", "a status patch changes status");
    }

    #[test]
    fn an_apply_that_drops_a_condition_removes_it() {
        let api = seeded();
        assert_eq!(apply(&api, "m", &["A", "B"]), 200, "first apply");
        assert_eq!(apply(&api, "m", &["A"]), 200, "second apply");
        assert_eq!(types(&api), ["A"], "B was dropped from the apply");
        assert_eq!(api.owners(PATH).len(), 1, "B is no longer owned");
    }

    #[test]
    fn an_apply_with_no_conditions_releases_every_condition_it_held() {
        let api = seeded();
        assert_eq!(apply(&api, "m", &["A"]), 200, "apply A");
        let uri = format!("{PATH}/status?fieldManager=m&force=true");
        assert_eq!(
            send(&api, "PATCH", &uri, APPLY, &json!({ "status": { "phase": "Q" } })),
            200,
            "apply phase"
        );
        assert!(types(&api).is_empty(), "the manager released A");
        assert_eq!(stored(&api)["status"]["phase"], "Q", "the phase applied");
    }

    #[test]
    fn two_managers_keep_their_own_types() {
        let api = seeded();
        assert_eq!(apply(&api, "one", &["A"]), 200, "one applies A");
        assert_eq!(apply(&api, "two", &["B"]), 200, "two applies B");
        assert_eq!(apply(&api, "one", &["A", "C"]), 200, "one applies A and C");
        assert_eq!(types(&api), ["A", "B", "C"], "two's B survives one's apply");
        assert_eq!(apply(&api, "two", &[]), 200, "two applies none");
        assert_eq!(types(&api), ["A", "C"], "two's empty apply removes only B");
        let owners = api.owners(PATH);
        assert_eq!(owners.get("A").map(String::as_str), Some("one"), "one owns A");
        assert_eq!(owners.get("C").map(String::as_str), Some("one"), "one owns C");
    }

    #[test]
    fn a_main_resource_apply_leaves_the_status_conditions_alone() {
        let api = seeded();
        assert_eq!(apply(&api, "m", &["A"]), 200, "status apply");
        let uri = format!("{PATH}?fieldManager=m&force=true");
        assert_eq!(
            send(&api, "PATCH", &uri, APPLY, &json!({ "spec": { "x": 2 } })),
            200,
            "main apply"
        );
        assert_eq!(types(&api), ["A"], "the status condition stays");
        assert_eq!(
            api.owners(PATH).get("A").map(String::as_str),
            Some("m"),
            "and stays owned"
        );
        assert_eq!(
            stored(&api)["metadata"]["generation"],
            2,
            "the spec change bumps the generation"
        );
    }

    #[test]
    fn a_dry_run_answers_and_leaves_no_state() {
        let api = seeded();
        let before = stored(&api);
        let uri = format!("{PATH}/status?fieldManager=m&force=true&dryRun=All");
        let body = json!({ "status": { "conditions": [{ "type": "A", "status": "True", "reason": "A" }] } });
        assert_eq!(send(&api, "PATCH", &uri, APPLY, &body), 200, "the dry run answers");
        assert_eq!(stored(&api), before, "the object is unchanged");
        assert!(api.owners(PATH).is_empty(), "no ownership is recorded");
        assert!(api.writes().is_empty(), "no write is recorded");
    }
}
