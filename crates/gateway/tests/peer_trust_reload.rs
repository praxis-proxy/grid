//! A provider listener's `peer_identity_trust` allowlist changes on a config reload, with no restart.

#[path = "support/logs.rs"]
mod logs;
#[path = "support/ports.rs"]
mod ports;

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::disallowed_methods,
    reason = "tests; waits poll a deadline with thread::sleep"
)]
mod tests {
    use std::{
        io::{BufRead as _, BufReader, Read as _, Write as _},
        net::TcpStream,
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        sync::{Arc, Mutex},
        thread,
        time::{Duration, Instant},
    };

    use certs::{CaCert, SiteCertOutput, generate_ca, generate_site_cert};
    use rustls::{
        ClientConfig, ClientConnection, RootCertStore, StreamOwned,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _},
    };

    use super::ports::{bound_listener, free_port};

    /// How long one expected change may take.
    const DEADLINE: Duration = Duration::from_secs(20);

    /// Collect `pipe`'s lines into `lines` on a background thread.
    fn drain(pipe: impl std::io::Read + Send + 'static, lines: Arc<Mutex<Vec<String>>>) {
        thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                lines.lock().expect("output").push(line);
            }
        });
    }

    /// A plaintext backend that answers every request on a connection it keeps open.
    fn keepalive_backend() -> String {
        let listener = bound_listener();
        let addr = listener.local_addr().expect("backend addr").to_string();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                thread::spawn(move || {
                    let mut request = Vec::new();
                    let mut buf = [0_u8; 1024];
                    loop {
                        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                            match stream.read(&mut buf) {
                                Ok(0) | Err(_) => return,
                                Ok(n) => request.extend_from_slice(buf.get(..n).unwrap_or_default()),
                            }
                        }
                        request.clear();
                        if stream
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                            .is_err()
                        {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    /// A provider listener that requires a Grid-CA client certificate, admits `trusted` by
    /// organization, keeps connections alive, and forwards to `backend`.
    fn provider_config(work: &Path, listen: u16, admin: u16, backend: &str, trusted: &[&str]) -> String {
        let peers: String = trusted
            .iter()
            .map(|site| format!("              - organization: {site}\n"))
            .collect();
        format!(
            "insecure_options:\n  allow_private_endpoints: true\nadmin:\n  metrics_address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    downstream_keepalive_timeout_ms: 30000\n    tls:\n      certificates:\n        - cert_path: {cert}\n          key_path: {key}\n      client_ca:\n        ca_path: {ca}\n      client_cert_mode: require\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: peer_identity_trust\n        trusted_peers:\n{peers}      - filter: router\n        routes:\n          - path: /v1/models\n            cluster: backend\n      - filter: load_balancer\n        clusters:\n          - name: backend\n            endpoints: [\"{backend}\"]\n",
            cert = work.join("tls.crt").display(),
            key = work.join("tls.key").display(),
            ca = work.join("ca.crt").display(),
        )
    }

    /// The gateway binary with its output drained, killed on drop.
    struct Gateway {
        child: Child,
        output: Arc<Mutex<Vec<String>>>,
    }

    impl Gateway {
        fn start(work: &Path, yaml: &str) -> Self {
            let config = work.join("praxis.yaml");
            std::fs::write(&config, yaml).expect("praxis config");
            let mut child = Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
                .arg("--config")
                .arg(&config)
                .env("RUST_LOG", "info")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn gateway");
            let output = Arc::new(Mutex::new(Vec::new()));
            drain(child.stdout.take().expect("stdout"), Arc::clone(&output));
            drain(child.stderr.take().expect("stderr"), Arc::clone(&output));
            Self { child, output }
        }

        fn eventually(&self, what: &str, done: impl Fn() -> bool) {
            let deadline = Instant::now() + DEADLINE;
            while !done() {
                if Instant::now() > deadline {
                    let output = self.output.lock().expect("output").join("\n");
                    panic!("timed out waiting for {what}; gateway output:\n{output}");
                }
                thread::sleep(Duration::from_millis(100));
            }
        }

        /// The process id, unchanged across reloads when the gateway never restarts.
        fn pid(&self) -> u32 {
            self.child.id()
        }

        fn running(&mut self) -> bool {
            self.child.try_wait().expect("try_wait").is_none()
        }
    }

    impl Drop for Gateway {
        fn drop(&mut self) {
            super::logs::gateway_output_on_failure(&self.output);
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }

    /// A keep-alive mTLS client presenting `site`'s leaf.
    struct Client {
        stream: StreamOwned<ClientConnection, TcpStream>,
    }

    impl Client {
        fn connect(ca: &CaCert, site: &SiteCertOutput, listen: u16) -> std::io::Result<Self> {
            let mut roots = RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(ca.cert_pem.as_bytes()) {
                roots.add(cert.expect("ca pem")).expect("root");
            }
            let chain = CertificateDer::pem_slice_iter(site.cert_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .expect("client chain");
            let key = PrivateKeyDer::from_pem_slice(site.key_pem.as_bytes()).expect("client key");
            let config = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .expect("versions")
                .with_root_certificates(roots)
                .with_client_auth_cert(chain, key)
                .expect("client config");
            let name = ServerName::try_from("gateway.grid.internal").expect("server name");
            let connection = ClientConnection::new(Arc::new(config), name).map_err(std::io::Error::other)?;
            let tcp = TcpStream::connect(("127.0.0.1", listen))?;
            tcp.set_read_timeout(Some(Duration::from_secs(5)))?;
            Ok(Self {
                stream: StreamOwned::new(connection, tcp),
            })
        }

        /// One request on this connection, the status, or `None` when the connection is gone.
        fn status(&mut self) -> Option<u16> {
            self.stream
                .write_all(b"GET /v1/models HTTP/1.1\r\nHost: gateway.grid.internal\r\n\r\n")
                .ok()?;
            let mut head = Vec::new();
            let mut byte = [0_u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                match self.stream.read(&mut byte) {
                    Ok(1) => head.extend_from_slice(&byte),
                    _ => return None,
                }
            }
            let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
            let length: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or(0);
            let mut body = vec![0_u8; length];
            self.stream.read_exact(&mut body).ok()?;
            head.split_whitespace().nth(1)?.parse().ok()
        }
    }

    /// A fresh connection's status for `site`, `None` when it cannot connect or is cut.
    fn fresh(ca: &CaCert, site: &SiteCertOutput, listen: u16) -> Option<u16> {
        Client::connect(ca, site, listen).ok()?.status()
    }

    #[test]
    fn the_peer_allowlist_changes_on_reload_without_a_restart() {
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("peer-trust-reload-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&work);
        std::fs::create_dir_all(&work).expect("work dir");
        let ca = generate_ca("grid-ca").expect("ca");
        let server = generate_site_cert(&ca, "gateway").expect("gateway cert");
        std::fs::write(work.join("ca.crt"), &ca.cert_pem).expect("ca");
        std::fs::write(work.join("tls.crt"), &server.cert_pem).expect("cert");
        std::fs::write(work.join("tls.key"), &server.key_pem).expect("key");
        let (site_a, site_b) = (
            generate_site_cert(&ca, "site-a").expect("a"),
            generate_site_cert(&ca, "site-b").expect("b"),
        );
        let (listen, admin) = (free_port(), free_port());
        let backend = keepalive_backend();
        let mut gateway = Gateway::start(&work, &provider_config(&work, listen, admin, &backend, &["site-a"]));
        let pid = gateway.pid();

        gateway.eventually("site-a admitted", || fresh(&ca, &site_a, listen) == Some(200));
        assert_eq!(fresh(&ca, &site_b, listen), Some(403), "site-b is not yet trusted");

        std::fs::write(
            work.join("praxis.yaml"),
            provider_config(&work, listen, admin, &backend, &["site-a", "site-b"]),
        )
        .expect("rewrite: a and b");
        gateway.eventually("site-b admitted after the reload", || {
            fresh(&ca, &site_b, listen) == Some(200)
        });
        assert_eq!(fresh(&ca, &site_a, listen), Some(200), "site-a is still trusted");

        // A keep-alive connection from site-a, opened while it is trusted.
        let mut open_a = Client::connect(&ca, &site_a, listen).expect("open site-a connection");
        assert_eq!(open_a.status(), Some(200), "site-a's open connection is admitted");
        let mut open_b = Client::connect(&ca, &site_b, listen).expect("open site-b connection");
        assert_eq!(open_b.status(), Some(200), "site-b's open connection is admitted");
        // Control: an open connection serves a second request with no reload in between.
        let mut idle = Client::connect(&ca, &site_b, listen).expect("open control connection");
        assert_eq!(idle.status(), Some(200), "the control connection is admitted");
        thread::sleep(Duration::from_secs(1));
        assert_eq!(
            idle.status(),
            Some(200),
            "an open connection serves again without a reload"
        );

        std::fs::write(
            work.join("praxis.yaml"),
            provider_config(&work, listen, admin, &backend, &["site-b"]),
        )
        .expect("rewrite: b only");
        gateway.eventually("site-a refused after the reload", || {
            fresh(&ca, &site_a, listen) == Some(403)
        });
        assert_eq!(fresh(&ca, &site_b, listen), Some(200), "site-b stays trusted");
        // The pipeline is pinned per request, so a connection opened before the reload is judged anew.
        assert_eq!(
            open_a.status(),
            Some(403),
            "site-a's already-open connection is refused on its next request"
        );
        assert_eq!(
            open_b.status(),
            Some(200),
            "site-b's already-open connection is still admitted"
        );

        assert!(gateway.running(), "the gateway never exited");
        assert_eq!(gateway.pid(), pid, "no restart");
        let output = gateway.output.lock().expect("output").join("\n");
        assert!(
            !output.contains("restart required"),
            "no reload asked for a restart:\n{output}"
        );
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }
}
