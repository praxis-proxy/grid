//! Shared Kubernetes Secret test doubles for `resources` unit tests.
//!
//! Extracted from `secret.rs`/`endpoint_tls.rs`, which had grown byte-identical
//! copies of the same mocked `kube::Client` builder — kept here once so the
//! two copies cannot drift independently.
//!
//! Declared behind `#[cfg(test)]` at the `mod test_doubles;` site in
//! `resources/mod.rs`, so this whole module compiles out of non-test builds.

use std::collections::HashMap;

use k8s_openapi::{ByteString, api::core::v1::Secret};

/// Build a `kube::Client` backed by an in-memory map of Secret name to
/// `Secret`, so Secret-reading code can be exercised without a real cluster.
/// Any name not present in the map returns HTTP 404.
#[expect(
    clippy::too_many_lines,
    reason = "test mock builder: 404-vs-200 branches are the whole point"
)]
pub(crate) fn mock_kube_client_with_secrets(secrets: HashMap<&'static str, Secret>) -> kube::Client {
    let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
        let secrets = secrets.clone();
        async move {
            let name = req.uri().path().rsplit('/').next().unwrap_or_default().to_owned();
            let response = secrets.get(name.as_str()).map_or_else(
                || {
                    let not_found = serde_json::json!({
                        "kind": "Status",
                        "apiVersion": "v1",
                        "status": "Failure",
                        "message": format!("secrets \"{name}\" not found"),
                        "reason": "NotFound",
                        "code": 404,
                    });
                    http::Response::builder()
                        .status(404)
                        .body(kube::client::Body::from(
                            serde_json::to_vec(&not_found).unwrap_or_else(|_| std::process::abort()),
                        ))
                        .unwrap_or_else(|_| std::process::abort())
                },
                |secret| {
                    http::Response::builder()
                        .status(200)
                        .body(kube::client::Body::from(
                            serde_json::to_vec(secret).unwrap_or_else(|_| std::process::abort()),
                        ))
                        .unwrap_or_else(|_| std::process::abort())
                },
            );
            Ok::<_, std::convert::Infallible>(response)
        }
    });
    kube::Client::new(service, "default")
}

/// Build a Secret with a single `data` key.
pub(crate) fn secret_with_key(key: &str, value: &[u8]) -> Secret {
    let mut data = std::collections::BTreeMap::new();
    data.insert(key.to_owned(), ByteString(value.to_vec()));
    Secret {
        data: Some(data),
        ..Default::default()
    }
}
