//! Objects and paths the cases share.

use serde_json::{Value, json};
use status_harness::{FakeApi, Observed};

/// The grid API group and version.
pub(crate) const GROUP: &str = "/apis/grid.praxis.fast/v1alpha1";

/// The name every case's network carries.
pub(crate) const NETWORK: &str = "net-1";

/// The path of the `GridNetwork` named `name`.
pub(crate) fn network_path(name: &str) -> String {
    format!("{GROUP}/gridnetworks/{name}")
}

/// The path of the `GridSite` named `name`.
pub(crate) fn site_path(name: &str) -> String {
    format!("{GROUP}/gridsites/{name}")
}

/// The path of the `InferenceProvider` named `name`.
pub(crate) fn provider_path(name: &str) -> String {
    format!("{GROUP}/inferenceproviders/{name}")
}

/// A `GridNetwork` with an empty spec at generation 1.
pub(crate) fn network(name: &str) -> Value {
    json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridNetwork",
        "metadata": { "name": name, "generation": 1, "uid": format!("uid-{name}") },
        "spec": {}
    })
}

/// A declared peer `GridSite` in `network` at generation 1, with `labels`.
pub(crate) fn site(name: &str, network: &str, labels: &[(&str, &str)]) -> Value {
    let labels: serde_json::Map<String, Value> = labels
        .iter()
        .map(|(key, value)| ((*key).to_owned(), json!(value)))
        .collect();
    json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridSite",
        "metadata": { "name": name, "generation": 1, "uid": format!("uid-{name}"), "labels": labels },
        "spec": { "gridNetworkRef": network }
    })
}

/// An `InferenceProvider` in `network` with no metrics endpoint, at generation 1.
pub(crate) fn provider(name: &str, network: &str) -> Value {
    json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "InferenceProvider",
        "metadata": { "name": name, "generation": 1, "uid": format!("uid-{name}") },
        "spec": {
            "gridNetworkRef": network,
            "providerKind": "self_hosted",
            "backendKind": "local",
            "endpoint": "http://127.0.0.1:1",
            "models": [{ "name": "model" }]
        }
    })
}

/// The object at `path`, typed.
pub(crate) fn stored<K: serde::de::DeserializeOwned>(api: &FakeApi, path: &str) -> K {
    serde_json::from_value(api.get(path).expect("object seeded")).expect("object deserializes")
}

/// What `path` reads after a round.
pub(crate) fn observe(api: &FakeApi, path: &str) -> Observed {
    api.observe(path).expect("object still stored")
}

/// Run `round` `count` times against `api`, observing `path` after each.
pub(crate) async fn rounds<F, Fut>(api: &FakeApi, path: &str, count: usize, mut round: F) -> Vec<Observed>
where
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut observed = Vec::with_capacity(count);
    for index in 0..count {
        round(index).await;
        observed.push(observe(api, path));
    }
    observed
}

/// The reason `path` holds on its condition of `type_`, empty when absent.
pub(crate) fn reason_of(api: &FakeApi, path: &str, type_: &str) -> String {
    observe(api, path)
        .condition(type_)
        .and_then(|condition| condition["reason"].as_str())
        .unwrap_or_default()
        .to_owned()
}
