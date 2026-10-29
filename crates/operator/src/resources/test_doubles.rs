//! Shared Kubernetes Secret test doubles for `resources` unit tests.
//!
//! Extracted from `secret.rs`/`endpoint_tls.rs`, which had grown byte-identical
//! copies of the same mocked `kube::Client` builder, kept here once so the
//! two copies cannot drift independently.
//!
//! Declared behind `#[cfg(test)]` at the `mod test_doubles;` site in
//! `resources/mod.rs`, so this whole module compiles out of non-test builds.

use std::{collections::HashMap, sync::Arc, time::Duration};

use k8s_openapi::{ByteString, api::core::v1::Secret};

use crate::crd::inference_provider::InferenceProvider;

/// Build a `kube::Client` backed by an in-memory map of Secret name to
/// `Secret`, so Secret-reading code can be exercised without a real cluster.
/// Any name not present in the map returns HTTP 404.
pub(crate) fn mock_kube_client_with_secrets(secrets: HashMap<&'static str, Secret>) -> kube::Client {
    mock_kube_client_with_objects(secrets)
}

/// Like [`mock_kube_client_with_secrets`], for `ConfigMaps`.
pub(crate) fn mock_kube_client_with_config_maps(
    config_maps: HashMap<&'static str, k8s_openapi::api::core::v1::ConfigMap>,
) -> kube::Client {
    mock_kube_client_with_objects(config_maps)
}

/// Like [`mock_kube_client_with_secrets`], for raw JSON bodies keyed by the last path segment.
pub(crate) fn mock_kube_client_with_json(objects: HashMap<&'static str, serde_json::Value>) -> kube::Client {
    mock_kube_client_with_objects(objects)
}

/// Like [`mock_kube_client_with_secrets`], for `GridNetworks`, including status patches.
pub(crate) fn mock_kube_client_with_grid_networks(
    networks: HashMap<&'static str, crate::crd::grid_network::GridNetwork>,
) -> kube::Client {
    mock_kube_client_with_objects(networks)
}

/// Build a `kube::Client` that lists the given `InferenceProvider` objects and no `GridSite` objects.
pub(crate) fn mock_kube_client_with_providers(providers: Vec<InferenceProvider>) -> kube::Client {
    let providers = Arc::new(providers);
    let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
        let providers = Arc::clone(&providers);
        async move {
            let body = kube_list_body(req.uri().path(), providers.as_slice());
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(200)
                    .body(kube::client::Body::from(body))
                    .unwrap_or_else(|_| std::process::abort()),
            )
        }
    });
    kube::Client::new(service, "default")
}

/// Build the list response for a mocked Kubernetes API path.
fn kube_list_body(path: &str, providers: &[InferenceProvider]) -> Vec<u8> {
    let list = if path.ends_with("/inferenceproviders") {
        serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "InferenceProviderList",
            "metadata": {"resourceVersion": "1"},
            "items": providers,
        })
    } else if path.ends_with("/gridsites") {
        serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSiteList",
            "metadata": {"resourceVersion": "1"},
            "items": [],
        })
    } else {
        serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Failure",
            "reason": "NotFound",
            "code": 404,
        })
    };
    serde_json::to_vec(&list).unwrap_or_else(|_| std::process::abort())
}

/// A `kube::Client` answering requests by object name from `objects`, 404 otherwise.
/// Status patches are matched by their parent object's name.
fn mock_kube_client_with_objects<T>(objects: HashMap<&'static str, T>) -> kube::Client
where
    T: serde::Serialize + Clone + Send + Sync + 'static,
{
    let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
        let objects = objects.clone();
        async move {
            let path = req.uri().path();
            let name = if req.method() == http::Method::PATCH && path.ends_with("/status") {
                path.rsplit('/').nth(1).unwrap_or_default()
            } else {
                path.rsplit('/').next().unwrap_or_default()
            };
            let response = objects.get(name).map_or_else(
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

/// Build a `ConfigMap` with a single `data` key.
pub(crate) fn config_map_with_key(key: &str, value: &str) -> k8s_openapi::api::core::v1::ConfigMap {
    k8s_openapi::api::core::v1::ConfigMap {
        data: Some(std::collections::BTreeMap::from([(key.to_owned(), value.to_owned())])),
        ..Default::default()
    }
}

/// Build an endpoint TLS config that trusts the CA in `Secret/{secret_name}`.
pub(crate) fn endpoint_tls_for_ca(secret_name: &str) -> crate::crd::inference_provider::EndpointTlsConfig {
    crate::crd::inference_provider::EndpointTlsConfig {
        ca_secret_ref: Some(crate::crd::grid_network::SecretRef {
            name: secret_name.to_owned(),
            namespace: "default".to_owned(),
            key: None,
        }),
        ca_config_map_ref: None,
        client_certificate_secret_ref: None,
    }
}

/// Start a one-shot HTTP server that waits `delay` before writing `response`.
pub(crate) async fn start_delayed_http_server(response: Vec<u8>, delay: Duration) -> String {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|_| std::process::abort());
    let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0_u8; 4096];
            drop(stream.read(&mut buf).await);
            tokio::time::sleep(delay).await;
            drop(stream.write_all(&response).await);
        }
    });
    format!("http://127.0.0.1:{port}")
}

/// Start a one-shot HTTPS endpoint and return its localhost URL.
///
/// The endpoint consumes one HTTP request and writes `response` as a raw HTTP
/// response. Its server certificate must be valid for `localhost`.
#[cfg(not(feature = "fips"))]
pub(crate) async fn start_tls_http_server(server_cert_pem: &str, server_key_pem: &str, response: Vec<u8>) -> String {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let server_certs = CertificateDer::pem_slice_iter(server_cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|_| std::process::abort());
    let server_key = PrivateKeyDer::from_pem_slice(server_key_pem.as_bytes()).unwrap_or_else(|_| std::process::abort());
    let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|_| std::process::abort())
        .with_no_client_auth()
        .with_single_cert(server_certs, server_key)
        .unwrap_or_else(|_| std::process::abort());
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|_| std::process::abort());
    let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await
            && let Ok(tls_stream) = acceptor.accept(stream).await
        {
            let (mut reader, mut writer) = tokio::io::split(tls_stream);
            let mut buffer = [0_u8; 4096];
            drop(reader.read(&mut buffer).await);
            drop(writer.write_all(&response).await);
        }
    });

    format!("https://localhost:{port}")
}

/// OpenSSL implementation of [`start_tls_http_server`] for FIPS tests.
#[cfg(feature = "fips")]
#[expect(clippy::too_many_lines, reason = "OpenSSL test server setup")]
pub(crate) async fn start_tls_http_server(server_cert_pem: &str, server_key_pem: &str, response: Vec<u8>) -> String {
    use openssl::{
        pkey::PKey,
        ssl::{Ssl, SslAcceptor, SslMethod},
        x509::X509,
    };
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let certificate = X509::from_pem(server_cert_pem.as_bytes()).unwrap_or_else(|_| std::process::abort());
    let private_key = PKey::private_key_from_pem(server_key_pem.as_bytes()).unwrap_or_else(|_| std::process::abort());
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap_or_else(|_| std::process::abort());
    builder
        .set_certificate(&certificate)
        .unwrap_or_else(|_| std::process::abort());
    builder
        .set_private_key(&private_key)
        .unwrap_or_else(|_| std::process::abort());
    builder.check_private_key().unwrap_or_else(|_| std::process::abort());
    let acceptor = builder.build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|_| std::process::abort());
    let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();

    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(ssl) = Ssl::new(acceptor.context()) else {
            return;
        };
        let Ok(mut tls_stream) = tokio_openssl::SslStream::new(ssl, stream) else {
            return;
        };
        if std::pin::Pin::new(&mut tls_stream).accept().await.is_ok() {
            let (mut reader, mut writer) = tokio::io::split(tls_stream);
            let mut buffer = [0_u8; 4096];
            drop(reader.read(&mut buffer).await);
            drop(writer.write_all(&response).await);
        }
    });

    format!("https://localhost:{port}")
}
