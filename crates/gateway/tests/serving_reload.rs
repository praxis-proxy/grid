//! The gateway binary reloads its serving config and site identity from kubelet-style mounts.

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_methods,
    clippy::too_many_lines,
    reason = "tests; waits poll a deadline with thread::sleep"
)]
mod tests {
    use std::{
        io::{BufRead as _, BufReader, Read as _, Write as _},
        net::{TcpListener, TcpStream},
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use certs::{CaCert, DEFAULT_TRUST_DOMAIN, GridSpiffeClientVerifier, generate_ca, generate_site_cert};
    use rustls::{
        ServerConfig, ServerConnection, StreamOwned,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _},
    };

    /// How long any one expected change may take: the watch re-reads every 5s.
    const DEADLINE: Duration = Duration::from_secs(20);

    /// A directory laid out like a kubelet `ConfigMap` or `Secret` volume.
    ///
    /// Each write lands in a fresh timestamped directory, `..data` is swapped to it
    /// atomically, and every key is a symlink through `..data`.
    struct Mount {
        dir: PathBuf,
        generation: u32,
    }

    impl Mount {
        fn new(dir: PathBuf) -> Self {
            std::fs::create_dir_all(&dir).expect("mount dir");
            Self { dir, generation: 0 }
        }

        fn path(&self, key: &str) -> String {
            self.dir.join(key).to_string_lossy().into_owned()
        }

        fn write(&mut self, files: &[(&str, &[u8])]) {
            self.generation += 1;
            let generation = format!("..gen_{}", self.generation);
            let staged = self.dir.join(&generation);
            std::fs::create_dir(&staged).expect("generation dir");
            for (key, content) in files {
                std::fs::write(staged.join(key), content).expect("write key");
            }
            let link = self.dir.join("..data_tmp");
            std::os::unix::fs::symlink(&generation, &link).expect("data link");
            std::fs::rename(&link, self.dir.join("..data")).expect("swap ..data");
            for (key, _) in files {
                let entry = self.dir.join(key);
                if entry.symlink_metadata().is_err() {
                    std::os::unix::fs::symlink(Path::new("..data").join(key), &entry).expect("key link");
                }
            }
            if self.generation > 1 {
                std::fs::remove_dir_all(self.dir.join(format!("..gen_{}", self.generation - 1)))
                    .expect("old generation");
            }
        }
    }

    /// A peer's signals endpoint over mutual TLS that records each client leaf it sees.
    struct Peer {
        addr: String,
        seen: Arc<Mutex<Vec<[u8; 32]>>>,
        hop_requests: Arc<AtomicUsize>,
        stop: Arc<AtomicBool>,
    }

    impl Peer {
        fn start(ca: &CaCert, site: &str) -> Self {
            let identity = generate_site_cert(ca, site).expect("peer cert");
            let chain = CertificateDer::pem_slice_iter(identity.cert_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .expect("peer chain");
            let key = PrivateKeyDer::from_pem_slice(identity.key_pem.as_bytes()).expect("peer key");
            let provider = rustls::crypto::ring::default_provider();
            let verifier = GridSpiffeClientVerifier::new(
                ca.cert_pem.as_bytes(),
                DEFAULT_TRUST_DOMAIN,
                provider.signature_verification_algorithms,
            )
            .expect("client verifier");
            let config = Arc::new(
                ServerConfig::builder_with_provider(Arc::new(provider))
                    .with_safe_default_protocol_versions()
                    .expect("versions")
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(chain, key)
                    .expect("server config"),
            );
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind peer");
            let addr = listener.local_addr().expect("peer addr").to_string();
            let seen = Arc::new(Mutex::new(Vec::new()));
            let hop_requests = Arc::new(AtomicUsize::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let body = format!(
                "HTTP/1.1 200 OK\r\nDate: Thu, 01 Jan 1970 00:00:01 GMT\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{line}",
                len = signals_line(site).len(),
                line = signals_line(site),
            );
            let (recorded, stopping) = (Arc::clone(&seen), Arc::clone(&stop));
            let recorded_hops = Arc::clone(&hop_requests);
            thread::spawn(move || {
                for stream in listener.incoming() {
                    if stopping.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(stream) = stream else { continue };
                    let Ok(conn) = ServerConnection::new(Arc::clone(&config)) else {
                        continue;
                    };
                    let mut tls = StreamOwned::new(conn, stream);
                    let mut request = Vec::new();
                    let mut buf = [0_u8; 1024];
                    // Read the request head. A refused handshake ends the read early.
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match tls.read(&mut buf) {
                            Ok(0) | Err(_) => break,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    if String::from_utf8_lossy(&request)
                        .to_ascii_lowercase()
                        .contains("x-ai-routing-candidate: candidate-east")
                    {
                        recorded_hops.fetch_add(1, Ordering::SeqCst);
                    }
                    let Some(leaf) = tls.conn.peer_certificates().and_then(<[_]>::first) else {
                        continue;
                    };
                    recorded.lock().expect("seen").push(certs::sha256(leaf.as_ref()));
                    let _sent = tls.write_all(body.as_bytes()).and_then(|()| tls.flush());
                }
            });
            Self {
                addr,
                seen,
                hop_requests,
                stop,
            }
        }

        fn polls(&self) -> usize {
            self.seen.lock().expect("seen").len()
        }

        fn saw(&self, fingerprint: &[u8; 32]) -> bool {
            self.seen.lock().expect("seen").contains(fingerprint)
        }

        fn hops(&self) -> usize {
            self.hop_requests.load(Ordering::SeqCst)
        }
    }

    impl Drop for Peer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            // Wake the accept loop so it sees the stop flag.
            let _woken = TcpStream::connect(&self.addr);
        }
    }

    /// One signals exposition line for `site`.
    fn signals_line(site: &str) -> String {
        format!(r#"inference_pool_average_queue_size{{grid_site="{site}",grid_provider="pool-{site}"}} 1 1000"#)
    }

    /// The serving config polling each of `peers` with the identity in `identity`.
    fn serving_config(peers: &[(&str, &Peer)], identity: &Mount) -> String {
        let candidates: Vec<String> = peers
            .iter()
            .map(|(site, _)| {
                format!(r#"{{"kind":"inference_model","name":"llama","site":"{site}","cluster":"pool-{site}"}}"#)
            })
            .collect();
        let peers: Vec<String> = peers
            .iter()
            .map(|(site, peer)| {
                format!(
                    r#"{{"site":"{site}","addr":"{addr}","server_name":"{site}.grid.internal","authority":"{site}.grid.internal","interval_ms":200,"connect_timeout_ms":1000,"request_timeout_ms":1000,"grid_ca_path":"{ca}","client_cert_path":"{cert}","client_key_path":"{key}"}}"#,
                    addr = peer.addr,
                    ca = identity.path("ca.crt"),
                    cert = identity.path("tls.crt"),
                    key = identity.path("tls.key"),
                )
            })
            .collect();
        format!(
            r#"{{"local_site":"local","window_secs":60,"load_window_ms":30000,"candidates":[{}],"peers":[{}]}}"#,
            candidates.join(","),
            peers.join(",")
        )
    }

    /// The SHA-256 of the leaf a PEM chain starts with.
    fn leaf_fingerprint(cert_pem: &str) -> [u8; 32] {
        let leaf = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
            .next()
            .expect("a leaf")
            .expect("parse leaf");
        certs::sha256(leaf.as_ref())
    }

    fn free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .expect("bind")
            .local_addr()
            .expect("addr")
            .port()
    }

    /// Collect `pipe`'s lines into `lines` on a background thread.
    fn drain(pipe: impl std::io::Read + Send + 'static, lines: Arc<Mutex<Vec<String>>>) {
        thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                lines.lock().expect("output").push(line);
            }
        });
    }

    /// The gateway with its output drained in the background, so it never blocks on a full pipe.
    struct Gateway {
        child: Child,
        output: Arc<Mutex<Vec<String>>>,
        listen: u16,
        admin: u16,
    }

    impl Gateway {
        fn start(work: &Path, serving: &Mount) -> Self {
            let (listen, admin) = (free_port(), free_port());
            let config = format!(
                "admin:\n  address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: static_response\n        status: 200\n"
            );
            Self::start_with_config(work, serving, listen, admin, &config)
        }

        fn start_with_config(work: &Path, serving: &Mount, listen: u16, admin: u16, yaml: &str) -> Self {
            let config = work.join("praxis.yaml");
            std::fs::write(&config, yaml).expect("praxis config");
            let mut child = Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
                .arg("--config")
                .arg(&config)
                .env("GRID_SERVING_CONFIG", serving.path("serving-config.json"))
                .env("RUST_LOG", "info")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn gateway");
            let output = Arc::new(Mutex::new(Vec::new()));
            drain(child.stdout.take().expect("stdout"), Arc::clone(&output));
            drain(child.stderr.take().expect("stderr"), Arc::clone(&output));
            Self {
                child,
                output,
                listen,
                admin,
            }
        }

        fn route_status(&self) -> u16 {
            let mut stream = TcpStream::connect(("127.0.0.1", self.listen)).expect("connect gateway");
            stream
                .write_all(b"GET /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nX-Model: llama\r\nConnection: close\r\n\r\n")
                .expect("send request");
            let mut response = String::new();
            stream.read_to_string(&mut response).expect("read response");
            response
                .split_whitespace()
                .nth(1)
                .expect("HTTP status")
                .parse()
                .expect("numeric HTTP status")
        }

        /// The rejected count from the admin metrics, zero until the series exists.
        fn rejected(&self) -> u64 {
            let Ok(mut stream) = TcpStream::connect(("127.0.0.1", self.admin)) else {
                return 0;
            };
            let _sent = stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
            let mut body = String::new();
            let _read = stream.read_to_string(&mut body);
            body.lines()
                .find(|line| {
                    line.starts_with("grid_serving_config_reload_total") && line.contains(r#"result="rejected""#)
                })
                .and_then(|line| line.rsplit(' ').next())
                .and_then(|value| value.parse().ok())
                .unwrap_or(0)
        }

        /// Wait for `done`, failing with the gateway's output after the deadline.
        fn eventually(&self, what: &str, done: impl Fn() -> bool) {
            let deadline = Instant::now() + DEADLINE;
            while !done() {
                if Instant::now() > deadline {
                    let output = self.output.lock().expect("output").join("\n");
                    panic!("timed out waiting for {what}; gateway output:\n{output}");
                }
                thread::sleep(Duration::from_millis(50));
            }
        }
    }

    impl Drop for Gateway {
        fn drop(&mut self) {
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }

    #[test]
    fn the_gateway_reloads_serving_config_and_identity_from_mounts() {
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("serving-reload-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&work);
        let ca = generate_ca("grid-ca").expect("ca");
        let first = generate_site_cert(&ca, "local").expect("client cert");
        let mut identity = Mount::new(work.join("identity"));
        identity.write(&[
            ("ca.crt", ca.cert_pem.as_bytes()),
            ("tls.crt", first.cert_pem.as_bytes()),
            ("tls.key", first.key_pem.as_bytes()),
        ]);
        let (east, west) = (Peer::start(&ca, "east"), Peer::start(&ca, "west"));
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[(
            "serving-config.json",
            serving_config(&[("east", &east)], &identity).as_bytes(),
        )]);

        let gateway = Gateway::start(&work, &serving);
        gateway.eventually("east polled over mTLS", || east.polls() > 0);
        assert!(
            east.saw(&leaf_fingerprint(&first.cert_pem)),
            "east saw the gateway's site identity"
        );

        serving.write(&[(
            "serving-config.json",
            serving_config(&[("west", &west)], &identity).as_bytes(),
        )]);
        gateway.eventually("west polled after it replaced east", || west.polls() > 0);
        // A poll already in flight can land after the swap, so wait for the count to settle.
        let last = std::cell::Cell::new(east.polls());
        gateway.eventually("east's poller stopped", || {
            thread::sleep(Duration::from_millis(500));
            let now = east.polls();
            last.replace(now) == now
        });
        let east_polls = east.polls();
        let quiet_until = Instant::now() + Duration::from_secs(1);
        gateway.eventually("a second of east staying quiet", || Instant::now() > quiet_until);
        assert_eq!(east.polls(), east_polls, "the removed peer is no longer polled");

        let renewed = generate_site_cert(&ca, "local").expect("renewed client cert");
        identity.write(&[
            ("ca.crt", ca.cert_pem.as_bytes()),
            ("tls.crt", renewed.cert_pem.as_bytes()),
            ("tls.key", renewed.key_pem.as_bytes()),
        ]);
        let renewed_leaf = leaf_fingerprint(&renewed.cert_pem);
        gateway.eventually("west sees the renewed client cert", || west.saw(&renewed_leaf));

        let rejected = gateway.rejected();
        serving.write(&[("serving-config.json", b"{not json")]);
        gateway.eventually("the malformed config counted as rejected", || {
            gateway.rejected() > rejected
        });
        let west_polls = west.polls();
        gateway.eventually("west still polled on the last good config", || {
            west.polls() > west_polls
        });

        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn praxis_backend_reload_cannot_inherit_provider_hop_trust() {
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("backend-reload-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&work);
        let ca = generate_ca("grid-ca").expect("ca");
        let local = generate_site_cert(&ca, "local").expect("client cert");
        let mut identity = Mount::new(work.join("identity"));
        identity.write(&[
            ("ca.crt", ca.cert_pem.as_bytes()),
            ("tls.crt", local.cert_pem.as_bytes()),
            ("tls.key", local.key_pem.as_bytes()),
        ]);
        let backend = Peer::start(&ca, "east");
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[(
            "serving-config.json",
            br#"{"local_site":"local","window_secs":60,"load_window_ms":30000,"candidates":[{"kind":"inference_model","name":"llama","site":"local","cluster":"pool-east","stable_id":"candidate-east"}],"provider_hop_clusters":["pool-east"],"provider_hop_sni":{"pool-east":"east.grid.internal"},"peers":[]}"#,
        )]);
        let (listen, admin) = (free_port(), free_port());
        let config = |tls: &str| {
            format!(
                "insecure_options:\n  allow_private_endpoints: true\nadmin:\n  address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: grid_site_route\n        model_header: X-Model\n      - filter: load_balancer\n        clusters:\n          - name: pool-east\n{tls}            endpoints: [\"{addr}\"]\n",
                addr = backend.addr
            )
        };
        let verified = format!(
            "            tls:\n              ca: {{ ca_path: {ca} }}\n              client_cert: {{ cert_path: {cert}, key_path: {key} }}\n              sni: east.grid.internal\n              verify: true\n",
            ca = identity.path("ca.crt"),
            cert = identity.path("tls.crt"),
            key = identity.path("tls.key"),
        );
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &config(&verified));
        gateway.eventually("verified backend route", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        assert_eq!(gateway.route_status(), 200, "the original verified backend routes");
        assert_eq!(backend.hops(), 1, "the verified route carries its candidate identity");

        for (label, tls) in [
            ("plaintext", String::new()),
            (
                "changed SNI",
                verified.replace("east.grid.internal", "other.grid.internal"),
            ),
        ] {
            let before = gateway
                .output
                .lock()
                .expect("output")
                .iter()
                .filter(|line| line.contains("changed its verified TLS identity"))
                .count();
            std::fs::write(work.join("praxis.yaml"), config(&tls)).expect("rewrite praxis config");
            gateway.eventually(label, || {
                gateway
                    .output
                    .lock()
                    .expect("output")
                    .iter()
                    .filter(|line| line.contains("changed its verified TLS identity"))
                    .count()
                    > before
            });
            let previous_hops = backend.hops();
            assert_eq!(
                gateway.route_status(),
                200,
                "{label} reload leaves the verified route live"
            );
            assert_eq!(
                backend.hops(),
                previous_hops + 1,
                "{label} reload keeps hop context on the original verified route"
            );
        }

        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }
}
