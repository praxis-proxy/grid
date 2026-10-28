//! The gateway binary reloads its serving config and site identity from kubelet-style mounts.

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_methods,
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
        routed: Arc<AtomicUsize>,
        routed_heads: Arc<Mutex<Vec<String>>>,
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
            let routed = Arc::new(AtomicUsize::new(0));
            let routed_heads = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let body = format!(
                "HTTP/1.1 200 OK\r\nDate: Thu, 01 Jan 1970 00:00:01 GMT\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{line}",
                len = signals_line(site).len(),
                line = signals_line(site),
            );
            let (recorded, stopping) = (Arc::clone(&seen), Arc::clone(&stop));
            let recorded_hops = Arc::clone(&hop_requests);
            let recorded_routes = Arc::clone(&routed);
            let recorded_heads = Arc::clone(&routed_heads);
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
                    let head = String::from_utf8_lossy(&request).to_ascii_lowercase();
                    if head.contains("x-ai-routing-candidate: candidate-east") {
                        recorded_hops.fetch_add(1, Ordering::SeqCst);
                    }
                    if head.contains("/v1/chat/completions") {
                        recorded_routes.fetch_add(1, Ordering::SeqCst);
                        recorded_heads.lock().expect("heads").push(head.clone());
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
                routed,
                routed_heads,
                stop,
            }
        }

        fn routed(&self) -> usize {
            self.routed.load(Ordering::SeqCst)
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
                "admin:\n  metrics_address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: static_response\n        status: 200\n"
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
            self.reloads("rejected")
        }

        /// The serving reloads that ended in `result`, zero until the series exists.
        fn reloads(&self, result: &str) -> u64 {
            let Ok(mut stream) = TcpStream::connect(("127.0.0.1", self.admin)) else {
                return 0;
            };
            let _sent = stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
            let mut body = String::new();
            let _read = stream.read_to_string(&mut body);
            body.lines()
                .find(|line| {
                    line.starts_with("grid_serving_config_reload_total")
                        && line.contains(&format!(r#"result="{result}""#))
                })
                .and_then(|line| line.rsplit(' ').next())
                .and_then(|value| value.parse().ok())
                .unwrap_or(0)
        }

        /// The value of the first admin metrics series named `name`, if it exists.
        fn metric(&self, name: &str) -> Option<f64> {
            let mut stream = TcpStream::connect(("127.0.0.1", self.admin)).ok()?;
            let _sent = stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
            let mut body = String::new();
            let _read = stream.read_to_string(&mut body);
            body.lines()
                .find(|line| line.split([' ', '{']).next() == Some(name))
                .and_then(|line| line.rsplit(' ').next())
                .and_then(|value| value.parse().ok())
        }

        /// Swap in `doc` as the serving config and wait until it is applied.
        fn serve(&self, serving: &mut Mount, doc: &serde_yaml::Value) {
            let applied = self.reloads("applied");
            serving.write(&[(
                "serving-config.json",
                serde_yaml::to_string(doc).expect("yaml").as_bytes(),
            )]);
            self.eventually("the rewritten serving file is applied", || {
                self.reloads("applied") > applied
            });
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
                "insecure_options:\n  allow_private_endpoints: true\nadmin:\n  metrics_address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: grid_site_route\n        model_header: X-Model\n      - filter: load_balancer\n        clusters:\n          - name: pool-east\n{tls}            endpoints: [\"{addr}\"]\n",
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

    /// A plaintext upstream that counts requests, and those carrying provider-hop context.
    struct Plain {
        addr: String,
        requests: Arc<AtomicUsize>,
        hops: Arc<AtomicUsize>,
        heads: Arc<Mutex<Vec<String>>>,
    }

    impl Plain {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind plaintext");
            let addr = listener.local_addr().expect("plaintext addr").to_string();
            let (requests, hops) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
            let (counted, hopped) = (Arc::clone(&requests), Arc::clone(&hops));
            let heads = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&heads);
            thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { continue };
                    let mut request = Vec::new();
                    let mut buf = [0_u8; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match stream.read(&mut buf) {
                            Ok(0) | Err(_) => break,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    let head = String::from_utf8_lossy(&request).to_ascii_lowercase();
                    if head.contains("/v1/chat/completions") {
                        counted.fetch_add(1, Ordering::SeqCst);
                        recorded.lock().expect("heads").push(head.clone());
                    }
                    if head.contains("x-ai-routing-candidate") {
                        hopped.fetch_add(1, Ordering::SeqCst);
                    }
                    let _sent = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                }
            });
            Self {
                addr,
                requests,
                hops,
                heads,
            }
        }
    }

    /// A `grid_site_route` gateway whose one `load_balancer` cluster, pool-east, dials `endpoint` over `tls`.
    fn routed_praxis(listen: u16, admin: u16, endpoint: &str, tls: &str) -> String {
        format!(
            "insecure_options:\n  allow_private_endpoints: true\nadmin:\n  metrics_address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: grid_site_route\n        model_header: X-Model\n      - filter: load_balancer\n        clusters:\n          - name: pool-east\n{tls}            endpoints: [\"{endpoint}\"]\n"
        )
    }

    /// Grid mutual TLS to pool-east at `sni`, with the identity in `identity`.
    fn verified_tls(identity: &Mount, sni: &str) -> String {
        format!(
            "            tls:\n              ca: {{ ca_path: {ca} }}\n              client_cert: {{ cert_path: {cert}, key_path: {key} }}\n              sni: {sni}\n              verify: true\n",
            ca = identity.path("ca.crt"),
            cert = identity.path("tls.crt"),
            key = identity.path("tls.key"),
        )
    }

    /// A serving config routing llama to each `(cluster, candidate id, hop SNI)`, each a provider hop.
    fn hop_serving(hops: &[(&str, &str, &str)]) -> String {
        let candidates: Vec<String> = hops
            .iter()
            .map(|(cluster, id, _)| {
                format!(r#"{{"kind":"inference_model","name":"llama","site":"local","cluster":"{cluster}","stable_id":"{id}"}}"#)
            })
            .collect();
        let clusters: Vec<String> = hops.iter().map(|(cluster, ..)| format!(r#""{cluster}""#)).collect();
        let sni: Vec<String> = hops
            .iter()
            .map(|(cluster, _, sni)| format!(r#""{cluster}":"{sni}""#))
            .collect();
        format!(
            r#"{{"local_site":"local","window_secs":60,"load_window_ms":30000,"candidates":[{}],"provider_hop_clusters":[{}],"provider_hop_sni":{{{}}},"peers":[]}}"#,
            candidates.join(","),
            clusters.join(","),
            sni.join(",")
        )
    }

    /// The site identity a gateway presents to pool-east.
    fn site_identity(work: &Path, ca: &CaCert) -> Mount {
        let local = generate_site_cert(ca, "local").expect("client cert");
        let mut identity = Mount::new(work.join("identity"));
        identity.write(&[
            ("ca.crt", ca.cert_pem.as_bytes()),
            ("tls.crt", local.cert_pem.as_bytes()),
            ("tls.key", local.key_pem.as_bytes()),
        ]);
        identity
    }

    fn fresh_work(name: &str) -> PathBuf {
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&work);
        work
    }

    fn logged(gateway: &Gateway, text: &str) -> bool {
        gateway
            .output
            .lock()
            .expect("output")
            .iter()
            .any(|line| line.contains(text))
    }

    #[test]
    fn a_stale_serving_file_naming_a_withdrawn_hop_still_starts_the_gateway() {
        let work = fresh_work("withdrawn-hop");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca);
        let east = Peer::start(&ca, "east");
        let mut serving = Mount::new(work.join("serving"));
        // The rollout dropped pool-gone from praxis.yaml before the operator rewrote this file.
        serving.write(&[(
            "serving-config.json",
            hop_serving(&[
                ("pool-east", "candidate-east", "east.grid.internal"),
                ("pool-gone", "candidate-gone", "gone.grid.internal"),
            ])
            .as_bytes(),
        )]);
        let (listen, admin) = (free_port(), free_port());
        let praxis = routed_praxis(
            listen,
            admin,
            &east.addr,
            &verified_tls(&identity, "east.grid.internal"),
        );
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &praxis);
        gateway.eventually("the gateway listens", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        for sent in 1..=10 {
            assert_eq!(gateway.route_status(), 200, "request {sent} routes to the bound site");
        }
        assert_eq!(
            east.hops(),
            10,
            "every request to the verified backend carries hop context"
        );
        assert!(
            logged(&gateway, "lack a matching verified TLS backend"),
            "the dropped hop is logged"
        );
        assert_eq!(
            gateway.metric("grid_serving_unbound_provider_hops"),
            Some(1.0),
            "the dropped hop is exported"
        );
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_hop_whose_sni_changed_takes_no_traffic_until_its_file_binds() {
        let work = fresh_work("changed-sni");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca);
        let east = Peer::start(&ca, "east");
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[(
            "serving-config.json",
            hop_serving(&[("pool-east", "candidate-east", "stale.grid.internal")]).as_bytes(),
        )]);
        let (listen, admin) = (free_port(), free_port());
        let praxis = routed_praxis(
            listen,
            admin,
            &east.addr,
            &verified_tls(&identity, "east.grid.internal"),
        );
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &praxis);
        gateway.eventually("the gateway listens", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        assert_ne!(gateway.route_status(), 200, "no candidate binds, so nothing routes");
        assert_eq!(east.routed(), 0, "no request reached the backend under the stale SNI");

        serving.write(&[(
            "serving-config.json",
            hop_serving(&[("pool-east", "candidate-east", "east.grid.internal")]).as_bytes(),
        )]);
        gateway.eventually("the rewritten file binds and routes", || gateway.route_status() == 200);
        assert!(east.hops() >= 1, "the bound route carries hop context");
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_hop_declared_on_a_plaintext_cluster_never_reaches_it() {
        let work = fresh_work("plaintext-hop");
        let plain = Plain::start();
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[(
            "serving-config.json",
            hop_serving(&[("pool-east", "candidate-east", "east.grid.internal")]).as_bytes(),
        )]);
        let (listen, admin) = (free_port(), free_port());
        let praxis = routed_praxis(listen, admin, &plain.addr, "");
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &praxis);
        gateway.eventually("the gateway listens", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        for _ in 0..5 {
            assert_ne!(gateway.route_status(), 200, "an unverified hop cluster is unroutable");
        }
        assert_eq!(
            plain.requests.load(Ordering::SeqCst),
            0,
            "no request reached the plaintext backend"
        );
        assert_eq!(plain.hops.load(Ordering::SeqCst), 0, "no hop context left in cleartext");
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    /// The operator's gridSiteRoute fixtures, rendered by its consumer config golden test.
    const SITE_ROUTE_PRAXIS: &str = include_str!("testdata/consumer-config-site-route.yaml");
    const SITE_ROUTE_SERVING: &str = include_str!("testdata/serving-config-site-route.json");

    /// The operator fixtures pointed at local test servers: the local model at `local`, site-b at
    /// `remote` (east's identity), and the local operator's signals at `signals`.
    #[expect(
        clippy::too_many_arguments,
        reason = "each address the fixtures point at is its own server"
    )]
    fn site_route_fixtures(
        listen: u16,
        admin: u16,
        local: &str,
        remote: &str,
        signals: &str,
        identity: &Mount,
    ) -> (String, String) {
        let tls = identity.dir.to_string_lossy().into_owned();
        let credentials = identity.dir.with_file_name("credentials");
        std::fs::create_dir_all(credentials.join("grid-model-client")).expect("credential dir");
        std::fs::write(credentials.join("grid-model-client/token"), SITE_ROUTE_TOKEN).expect("token");
        let credentials = credentials.to_string_lossy().into_owned();
        let point = |text: &str| {
            text.replace("site-b.grid.internal", "east.grid.internal")
                .replace("candidate-site-b", "candidate-east")
                .replace("/etc/praxis/tls", &tls)
                .replace("/run/secrets/grid-credentials", &credentials)
        };
        let praxis = format!(
            "insecure_options:\n  allow_private_endpoints: true\n  allow_private_health_checks: true\n{}",
            point(SITE_ROUTE_PRAXIS)
                .replace("0.0.0.0:8080", &format!("127.0.0.1:{listen}"))
                .replace("127.0.0.1:9901", &format!("127.0.0.1:{admin}"))
                .replace("10.96.0.20:8000", local)
                .replace("203.0.113.20:8443", remote)
        );
        let serving = point(SITE_ROUTE_SERVING).replace("grid-operator-signals.grid.svc:9091", signals);
        (praxis, serving)
    }

    /// The local model's bearer token, mounted where the operator's render reads it.
    const SITE_ROUTE_TOKEN: &str = "grid-model-client-token";

    /// POST a chat request whose body names `model`, with `header` as the caller's own X-Model.
    fn chat(listen: u16, model: &str, header: Option<&str>) -> u16 {
        chat_as(listen, model, header, None)
    }

    /// [`chat`], with the caller's own Authorization.
    fn chat_as(listen: u16, model: &str, header: Option<&str>, authorization: Option<&str>) -> u16 {
        let headers = format!(
            "{}{}",
            header.map_or_else(String::new, |value| format!("X-Model: {value}\r\n")),
            authorization.map_or_else(String::new, |value| format!("Authorization: {value}\r\n"))
        );
        post(listen, &format!(r#"{{"model":"{model}","messages":[]}}"#), &headers)
    }

    /// POST `body` to the chat path with the caller's own `spoof` header lines.
    fn post(listen: u16, body: &str, spoof: &str) -> u16 {
        post_to(listen, "/v1/chat/completions", body, spoof)
    }

    /// POST `body` to `path` with the caller's own `spoof` header lines.
    fn post_to(listen: u16, path: &str, body: &str, spoof: &str) -> u16 {
        let mut stream = TcpStream::connect(("127.0.0.1", listen)).expect("connect gateway");
        stream
            .write_all(
                format!(
                    "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n{spoof}Content-Length: {len}\r\nConnection: close\r\n\r\n{body}",
                    len = body.len()
                )
                .as_bytes(),
            )
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

    /// The heads of the next ten requests the other site's gateway takes.
    fn east_heads_after(gateway: &Gateway, east: &Peer, listen: u16) -> Vec<String> {
        let before = east.routed();
        gateway.eventually("the other site takes ten more requests", || {
            let _sent = chat_as(listen, "llama", None, Some("Bearer caller-token"));
            east.routed() >= before + 10
        });
        east.routed_heads
            .lock()
            .expect("heads")
            .iter()
            .skip(before)
            .cloned()
            .collect()
    }

    /// Every credential header a caller might send, each carrying a marker.
    const CALLER_CREDENTIALS: &str = "X-Model: llama\r\nAuthorization: Bearer caller-token\r\nProxy-Authorization: Basic cHJveHk=\r\nCookie: session=caller\r\nx-api-key: caller-key\r\napi-key: caller-key\r\nx-goog-api-key: caller-key\r\nOcp-Apim-Subscription-Key: caller-key\r\n";

    /// What must not appear in a request that leaves this site.
    const CALLER_CREDENTIAL_MARKERS: [&str; 6] = [
        "authorization:",
        "cookie:",
        "api-key:",
        "ocp-apim-subscription-key:",
        "caller-token",
        "caller-key",
    ];

    /// A port with nothing listening on it.
    fn dead_endpoint() -> String {
        format!("127.0.0.1:{}", free_port())
    }

    #[test]
    fn the_operator_site_route_render_runs_and_sends_hop_context_only_across_sites() {
        let work = fresh_work("site-route-render");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca);
        let (east, signals, local) = (Peer::start(&ca, "east"), Peer::start(&ca, "site-a"), Plain::start());
        let (listen, admin) = (free_port(), free_port());
        let (praxis, serving_json) =
            site_route_fixtures(listen, admin, &local.addr, &east.addr, &signals.addr, &identity);
        // Uncredentialed, so the caller's own credentials are the only ones in play.
        let mut doc: serde_yaml::Value = serde_yaml::from_str(&serving_json).expect("serving");
        assert!(
            doc["candidates"][0]
                .as_mapping_mut()
                .and_then(|local_candidate| local_candidate.remove("credential"))
                .is_some(),
            "the fixture's local candidate is credentialed"
        );
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[(
            "serving-config.json",
            serde_yaml::to_string(&doc).expect("yaml").as_bytes(),
        )]);
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &praxis);
        gateway.eventually("the gateway listens", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        let caller = CALLER_CREDENTIALS;
        for sent in 1..=30 {
            assert_eq!(
                post(listen, r#"{"model":"llama","messages":[]}"#, caller),
                200,
                "request {sent} routes"
            );
        }
        assert!(
            !local.heads.lock().expect("heads").is_empty() && east.routed() > 0,
            "both sites took traffic"
        );
        for head in local.heads.lock().expect("heads").iter() {
            assert!(
                head.contains("authorization: bearer caller-token"),
                "an uncredentialed local model still authenticates the caller: {head}"
            );
        }
        for head in east.routed_heads.lock().expect("heads").iter() {
            for credential in CALLER_CREDENTIAL_MARKERS {
                assert!(
                    !head.contains(credential),
                    "{credential} never crosses to another site: {head}"
                );
            }
        }
        let (to_local, to_east) = (local.requests.load(Ordering::SeqCst), east.routed());
        assert_eq!(to_local + to_east, 30, "each request reached exactly one site");
        assert_eq!(
            local.hops.load(Ordering::SeqCst),
            0,
            "the local plaintext model never sees hop context"
        );
        assert_eq!(
            east.hops(),
            to_east,
            "every request to the other site's gateway carries hop context"
        );
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_dead_local_cluster_in_the_operator_render_is_demoted_by_its_health_check() {
        let work = fresh_work("site-route-dead-local");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca);
        let (east, signals) = (Peer::start(&ca, "east"), Peer::start(&ca, "site-a"));
        let (listen, admin) = (free_port(), free_port());
        let (praxis, serving_json) =
            site_route_fixtures(listen, admin, &dead_endpoint(), &east.addr, &signals.addr, &identity);
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[("serving-config.json", serving_json.as_bytes())]);
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &praxis);
        gateway.eventually("the gateway listens", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        gateway.eventually(
            "the dead local cluster is demoted, so ten requests in a row route",
            || (0..10).all(|_| chat(listen, "llama", None) == 200),
        );
        let routed = east.routed();
        for _ in 0..10 {
            assert_eq!(
                chat(listen, "llama", None),
                200,
                "demoted, the local cluster takes nothing"
            );
        }
        assert_eq!(east.routed(), routed + 10, "the other site serves every request");
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_caller_model_header_does_not_steer_the_operator_site_route_render() {
        let work = fresh_work("site-route-spoof");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca);
        let (east, signals, local) = (Peer::start(&ca, "east"), Peer::start(&ca, "site-a"), Plain::start());
        let (listen, admin) = (free_port(), free_port());
        let (praxis, serving_json) =
            site_route_fixtures(listen, admin, &local.addr, &east.addr, &signals.addr, &identity);
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[("serving-config.json", serving_json.as_bytes())]);
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &praxis);
        gateway.eventually("the gateway listens", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        gateway.eventually("the gateway routes llama", || chat(listen, "llama", None) == 200);
        let reached = local.requests.load(Ordering::SeqCst) + east.routed();
        for _ in 0..5 {
            assert_ne!(
                chat(listen, "unserved", Some("llama")),
                200,
                "the body names an unserved model, so a caller X-Model of llama must not route it"
            );
        }
        assert_ne!(
            post(listen, r#"{"messages":[]}"#, "X-Model: llama\r\n"),
            200,
            "a body that names no model is not routed by a caller X-Model"
        );
        assert_eq!(
            local.requests.load(Ordering::SeqCst) + east.routed(),
            reached,
            "no spoofed request reached a backend"
        );
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn the_operator_site_route_render_injects_the_local_credential_and_keeps_it_on_this_site() {
        let work = fresh_work("site-route-credential");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca);
        let (east, signals, local) = (Peer::start(&ca, "east"), Peer::start(&ca, "site-a"), Plain::start());
        let (listen, admin) = (free_port(), free_port());
        let (praxis, serving_json) =
            site_route_fixtures(listen, admin, &local.addr, &east.addr, &signals.addr, &identity);
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[("serving-config.json", serving_json.as_bytes())]);
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &praxis);
        gateway.eventually("the gateway listens", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        for sent in 1..=30 {
            assert_eq!(
                chat_as(listen, "llama", None, Some("Bearer caller-token")),
                200,
                "request {sent} routes"
            );
        }
        let local_heads = local.heads.lock().expect("heads").clone();
        let east_heads = east.routed_heads.lock().expect("heads").clone();
        assert!(
            !local_heads.is_empty() && !east_heads.is_empty(),
            "both sites took traffic"
        );
        let token = format!("authorization: bearer {SITE_ROUTE_TOKEN}");
        for head in &local_heads {
            assert!(head.contains(&token), "the local model gets the grid token: {head}");
            assert!(
                !head.contains("caller-token"),
                "the caller's credential is replaced: {head}"
            );
        }
        for head in &east_heads {
            assert!(
                !head.contains(SITE_ROUTE_TOKEN),
                "this site's token never leaves it: {head}"
            );
        }

        // A remote candidate naming this site's credential still never carries it across.
        let mut doc: serde_yaml::Value = serde_yaml::from_str(&serving_json).expect("serving");
        let local_credential = doc["candidates"][0]["credential"].clone();
        assert!(
            !local_credential.is_null(),
            "the fixture's local candidate carries the credential"
        );
        doc["candidates"][1]["credential"] = local_credential;
        gateway.serve(&mut serving, &doc);
        let remote = east_heads_after(&gateway, &east, listen);
        assert!(
            remote.iter().all(|head| !head.contains(SITE_ROUTE_TOKEN)),
            "a remote candidate never selects a local credential: {remote:?}"
        );

        // Nor does the other site's hop cluster named by a candidate that claims this site.
        doc["candidates"][1]["site"] = doc["candidates"][0]["site"].clone();
        gateway.serve(&mut serving, &doc);
        let relabelled = east_heads_after(&gateway, &east, listen);
        assert!(
            relabelled.iter().all(|head| !head.contains(SITE_ROUTE_TOKEN)),
            "a hop cluster never carries this site's credential: {relabelled:?}"
        );
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_remote_site_without_a_hop_never_receives_the_caller_credentials() {
        let work = fresh_work("remote-plaintext");
        let remote = Plain::start();
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[(
            "serving-config.json",
            br#"{"local_site":"local","window_secs":60,"load_window_ms":30000,"candidates":[{"kind":"inference_model","name":"llama","site":"east","cluster":"pool-east"}],"peers":[]}"#,
        )]);
        let (listen, admin) = (free_port(), free_port());
        let praxis = routed_praxis(listen, admin, &remote.addr, "");
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &praxis);
        gateway.eventually("the gateway listens", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        for sent in 1..=5 {
            assert_eq!(
                post(listen, r#"{"model":"llama","messages":[]}"#, CALLER_CREDENTIALS),
                200,
                "request {sent} routes to the other site"
            );
        }
        let heads = remote.heads.lock().expect("heads").clone();
        assert_eq!(heads.len(), 5, "every request reached the other site");
        for head in &heads {
            for credential in CALLER_CREDENTIAL_MARKERS {
                assert!(
                    !head.contains(credential),
                    "{credential} never leaves this site: {head}"
                );
            }
        }
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_remote_site_never_receives_the_caller_credential_in_the_query() {
        let work = fresh_work("remote-query");
        let remote = Plain::start();
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[(
            "serving-config.json",
            br#"{"local_site":"local","window_secs":60,"load_window_ms":30000,"candidates":[{"kind":"inference_model","name":"llama","site":"east","cluster":"pool-east"}],"peers":[]}"#,
        )]);
        let (listen, admin) = (free_port(), free_port());
        let praxis = routed_praxis(listen, admin, &remote.addr, "");
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &praxis);
        gateway.eventually("the gateway listens", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        assert_eq!(
            post_to(
                listen,
                "/v1/chat/completions?key=caller-secret&trace=1",
                r#"{"model":"llama","messages":[]}"#,
                "X-Model: llama\r\n"
            ),
            200,
            "the request routes to the other site"
        );
        let heads = remote.heads.lock().expect("heads").clone();
        let head = heads.first().expect("the request reached the other site");
        let line = head.lines().next().expect("request line");
        assert!(
            !head.contains("caller-secret"),
            "the credential never leaves this site: {head}"
        );
        assert!(line.contains("trace=1"), "an ordinary parameter still arrives: {line}");
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    /// The operator's bootstrap config, written before a delegated gateway's first rollout.
    const BOOTSTRAP_PRAXIS: &str = include_str!("testdata/consumer-config-bootstrap.yaml");

    #[test]
    fn the_bootstrap_config_starts_beside_a_serving_file_and_answers_503_to_retry() {
        let work = fresh_work("bootstrap");
        let mut serving = Mount::new(work.join("serving"));
        serving.write(&[(
            "serving-config.json",
            hop_serving(&[("pool-east", "candidate-east", "east.grid.internal")]).as_bytes(),
        )]);
        let (listen, admin) = (free_port(), free_port());
        let praxis = BOOTSTRAP_PRAXIS
            .replace("0.0.0.0:8080", &format!("127.0.0.1:{listen}"))
            .replace("127.0.0.1:9901", &format!("127.0.0.1:{admin}"));
        let gateway = Gateway::start_with_config(&work, &serving, listen, admin, &praxis);
        gateway.eventually("the gateway listens", || {
            TcpStream::connect(("127.0.0.1", listen)).is_ok()
        });
        let mut stream = TcpStream::connect(("127.0.0.1", listen)).expect("connect gateway");
        stream
            .write_all(b"POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .expect("send");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read");
        let head = response.to_ascii_lowercase();
        assert!(head.starts_with("http/1.1 503"), "{response}");
        assert!(
            head.contains("retry-after: 5"),
            "a client backs off and retries: {response}"
        );
        assert!(
            response.contains(r#""type":"service_unavailable""#),
            "an OpenAI-shaped error: {response}"
        );
        assert_eq!(
            gateway.metric("grid_serving_unbound_provider_hops"),
            Some(1.0),
            "the serving file's hop has no backend yet, so it is dropped rather than fatal"
        );
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }
}
