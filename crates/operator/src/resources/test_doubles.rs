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
pub(crate) fn mock_kube_client_with_secrets(secrets: HashMap<&'static str, Secret>) -> kube::Client {
    mock_kube_client_with_objects(secrets)
}

/// Like [`mock_kube_client_with_secrets`], for `ConfigMaps`.
pub(crate) fn mock_kube_client_with_config_maps(
    config_maps: HashMap<&'static str, k8s_openapi::api::core::v1::ConfigMap>,
) -> kube::Client {
    mock_kube_client_with_objects(config_maps)
}

/// A `kube::Client` answering a GET by object name from `objects`, 404 otherwise.
#[expect(
    clippy::too_many_lines,
    reason = "test mock builder: 404-vs-200 branches are the whole point"
)]
fn mock_kube_client_with_objects<T>(objects: HashMap<&'static str, T>) -> kube::Client
where
    T: serde::Serialize + Clone + Send + Sync + 'static,
{
    let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
        let secrets = objects.clone();
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

/// Start a one-shot HTTPS endpoint and return its localhost URL.
///
/// The endpoint consumes one HTTP request and writes `response` as a raw HTTP
/// response. Its server certificate must be valid for `localhost`.
#[cfg(not(feature = "fips"))]
#[expect(clippy::too_many_lines, reason = "TLS test server setup")]
pub(crate) async fn start_tls_http_server(server_cert_pem: &str, server_key_pem: &str, response: Vec<u8>) -> String {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let server_certs = CertificateDer::pem_slice_iter(server_cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|_| std::process::abort());
    let server_key = PrivateKeyDer::from_pem_slice(server_key_pem.as_bytes()).unwrap_or_else(|_| std::process::abort());
    let server_config =
        rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap_or_else(|_| std::process::abort())
            .with_no_client_auth()
            .with_single_cert(server_certs, server_key)
            .unwrap_or_else(|_| std::process::abort());
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config));
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
