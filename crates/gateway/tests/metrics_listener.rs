//! The opt-in metrics listener serves only `GET /metrics` over TLS, to any Host.

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    reason = "tests; waits poll a deadline with thread::sleep"
)]
mod tests {
    use std::{
        io::{Read as _, Write as _},
        net::{TcpListener, TcpStream},
        path::PathBuf,
        process::{Child, Command, Stdio},
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };

    use certs::{generate_ca, generate_site_cert};
    use rustls::{
        ClientConfig, ClientConnection, RootCertStore, StreamOwned,
        pki_types::{CertificateDer, ServerName, pem::PemObject as _},
    };

    /// Bound on how long a test waits for the child.
    const TIMEOUT: Duration = Duration::from_secs(30);

    /// The SAN `generate_site_cert` gives site `metrics`.
    const SERVER_NAME: &str = "metrics.grid.internal";

    fn free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    /// A running gateway, killed on drop.
    struct Gateway(Child);

    impl Drop for Gateway {
        fn drop(&mut self) {
            drop(self.0.kill());
            drop(self.0.wait());
        }
    }

    /// Paths and client trust for one test's cert, key, and config.
    struct Setup {
        dir: PathBuf,
        client: Arc<ClientConfig>,
        listener_port: u16,
    }

    fn setup(name: &str) -> Setup {
        let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let ca = generate_ca("metrics test CA").unwrap();
        let leaf = generate_site_cert(&ca, "metrics").unwrap();
        std::fs::write(dir.join("tls.crt"), &leaf.cert_pem).unwrap();
        std::fs::write(dir.join("tls.key"), &leaf.key_pem).unwrap();
        let listener_port = free_port();
        std::fs::write(
            dir.join("praxis.yaml"),
            format!(
                "listeners:\n  - name: default\n    address: \"127.0.0.1:{listener_port}\"\n    filter_chains: [main]\n\
                 filter_chains:\n  - name: main\n    filters:\n      - filter: static_response\n        status: 200\n"
            ),
        )
        .unwrap();
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(ca.cert_pem.as_bytes()).unwrap())
            .unwrap();
        let client = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Setup {
            dir,
            client: Arc::new(client),
            listener_port,
        }
    }

    fn start(setup: &Setup, metrics_port: u16) -> Gateway {
        Gateway(
            Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
                .arg("--config")
                .arg(setup.dir.join("praxis.yaml"))
                .env_remove("GRID_SERVING_CONFIG")
                .env("GRID_METRICS_ADDR", format!("127.0.0.1:{metrics_port}"))
                .env("GRID_METRICS_TLS_CERT", setup.dir.join("tls.crt"))
                .env("GRID_METRICS_TLS_KEY", setup.dir.join("tls.key"))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }

    /// Send `head` over TLS and return the full response, or `None` when the connection fails.
    fn exchange(client: &Arc<ClientConfig>, port: u16, head: &str) -> Option<String> {
        let stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
        let name = ServerName::try_from(SERVER_NAME).ok()?;
        let conn = ClientConnection::new(Arc::clone(client), name).ok()?;
        let mut tls = StreamOwned::new(conn, stream);
        tls.write_all(head.as_bytes()).ok()?;
        let mut response = String::new();
        tls.read_to_string(&mut response).ok()?;
        Some(response)
    }

    /// The response to `head`, retrying until the listener is up.
    fn eventually(client: &Arc<ClientConfig>, port: u16, head: &str) -> String {
        let deadline = Instant::now().checked_add(TIMEOUT).unwrap();
        loop {
            if let Some(response) = exchange(client, port, head) {
                return response;
            }
            assert!(Instant::now() < deadline, "metrics listener never answered on {port}");
            thread::sleep(Duration::from_millis(100));
        }
    }

    #[test]
    fn only_get_metrics_is_served_and_any_host_is_accepted() {
        let setup = setup("metrics-listener-routes");
        let port = free_port();
        let _gateway = start(&setup, port);

        let metrics = eventually(
            &setup.client,
            port,
            "GET /metrics HTTP/1.1\r\nHost: grid-gateway.grid-system.svc\r\n\r\n",
        );
        assert!(metrics.starts_with("HTTP/1.1 200 "), "{metrics}");
        assert!(metrics.contains("text/plain; version=0.0.4"), "{metrics}");

        let kv = eventually(&setup.client, port, "GET /api/kv HTTP/1.1\r\nHost: x\r\n\r\n");
        assert!(kv.starts_with("HTTP/1.1 404 "), "{kv}");
        let wrong_method = eventually(
            &setup.client,
            port,
            "POST /metrics HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n",
        );
        assert!(wrong_method.starts_with("HTTP/1.1 405 "), "{wrong_method}");
    }

    #[test]
    fn connections_past_the_cap_are_closed_and_slots_come_back() {
        let setup = setup("metrics-listener-cap");
        let port = free_port();
        let _gateway = start(&setup, port);
        eventually(&setup.client, port, "GET /metrics HTTP/1.1\r\n\r\n");

        // Four idle connections hold every slot until the handshake deadline.
        let held: Vec<TcpStream> = std::iter::repeat_with(|| TcpStream::connect(("127.0.0.1", port)).unwrap())
            .take(4)
            .collect();
        // Poll inside the 2s handshake deadline, which would free the held slots.
        let deadline = Instant::now().checked_add(Duration::from_millis(1500)).unwrap();
        let refused = loop {
            if exchange(&setup.client, port, "GET /metrics HTTP/1.1\r\n\r\n").is_none() {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            thread::sleep(Duration::from_millis(50));
        };
        assert!(refused, "a fifth connection is closed");
        drop(held);
        let after = eventually(&setup.client, port, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(after.starts_with("HTTP/1.1 200 "), "{after}");
    }

    #[test]
    fn a_port_praxis_binds_stops_startup() {
        let setup = setup("metrics-listener-collision");
        let mut gateway = start(&setup, setup.listener_port);
        let deadline = Instant::now().checked_add(TIMEOUT).unwrap();
        let status = loop {
            if let Some(status) = gateway.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "the gateway kept running on a colliding port"
            );
            thread::sleep(Duration::from_millis(100));
        };
        assert!(!status.success(), "{status}");
    }
}
