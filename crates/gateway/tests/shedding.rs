//! A model sheds once every site serving it is full, answering 429 with Retry-After, and routes
//! again once one site has room.
//!
//! Full is measured: a site at the ceiling it has shown, with work queued behind that. The
//! gateway learns each site's ceiling from the in-flight count the operators publish, so the
//! test first teaches every site a ceiling, then fills them all, then drains one.

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::min_ident_chars,
    clippy::arithmetic_side_effects,
    clippy::disallowed_methods,
    clippy::too_many_lines,
    reason = "tests; waits poll a deadline with thread::sleep"
)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::{BufRead as _, BufReader, ErrorKind, Read as _, Write as _},
        net::{TcpListener, TcpStream},
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU8, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use certs::{CaCert, DEFAULT_TRUST_DOMAIN, GridSpiffeClientVerifier, generate_ca, generate_site_cert};
    use rustls::{
        ServerConfig, ServerConnection, StreamOwned,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _},
    };

    /// How often the gateway polls each peer.
    const POLL: Duration = Duration::from_millis(200);
    /// The stated bound on time to exclusion and to rejoin: one poll, plus slack for a loaded runner.
    const BOUND: Duration = Duration::from_millis(3 * 200 + 1_000);
    /// The load window, short so a partition outlasts it within the test.
    const WINDOW: Duration = Duration::from_millis(1_000);
    /// The gap between client requests.
    const PACE: Duration = Duration::from_millis(10);
    /// How long setup may take.
    const DEADLINE: Duration = Duration::from_secs(20);

    const SITES: [&str; 3] = ["site-a", "site-b", "site-d"];

    /// What a fake peer operator publishes: in-flight at the ceiling with nothing queued.
    const TEACH: u8 = 0;
    /// In-flight at the ceiling with work queued: full.
    const FULL: u8 = 1;
    /// Well under the ceiling: room.
    const ROOM: u8 = 2;
    /// Accept and drop every poll, as a partitioned link.
    const PARTITIONED: u8 = 3;
    /// Every site's ceiling, requests.
    const CEILING: u32 = 100;

    /// A peer operator's signals endpoint over mutual TLS, publishing per `mode`.
    struct Peer {
        addr: String,
        mode: Arc<AtomicU8>,
        stop: Arc<AtomicBool>,
    }

    impl Peer {
        fn start(ca: &CaCert, site: &'static str) -> Self {
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
            let mode = Arc::new(AtomicU8::new(TEACH));
            let stop = Arc::new(AtomicBool::new(false));
            let (serving, stopping) = (Arc::clone(&mode), Arc::clone(&stop));
            thread::spawn(move || {
                for stream in listener.incoming() {
                    if stopping.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(stream) = stream else { continue };
                    let publishing = serving.load(Ordering::SeqCst);
                    if publishing == PARTITIONED {
                        continue;
                    }
                    let _bounded = stream.set_read_timeout(Some(Duration::from_secs(1)));
                    let Ok(conn) = ServerConnection::new(Arc::clone(&config)) else {
                        continue;
                    };
                    let mut tls = StreamOwned::new(conn, stream);
                    let mut request = Vec::new();
                    let mut buf = [0_u8; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match tls.read(&mut buf) {
                            Ok(0) | Err(_) => break,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    let body = signals(site, publishing);
                    // Samples stamped at the Date header read as age zero.
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nDate: Thu, 01 Jan 1970 00:00:01 GMT\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _sent = tls.write_all(response.as_bytes()).and_then(|()| tls.flush());
                }
            });
            Self { addr, mode, stop }
        }
    }

    impl Drop for Peer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _woken = TcpStream::connect(&self.addr);
        }
    }

    /// What `site`'s operator publishes in `mode`: its in-flight count and its queue.
    fn signals(site: &str, mode: u8) -> String {
        let labels = format!(r#"grid_site="{site}",grid_provider="pool-{site}""#);
        let (in_flight, queued) = match mode {
            FULL => (CEILING, 5),
            ROOM => (CEILING / 5, 0),
            _ => (CEILING, 0),
        };
        format!(
            "llm_d_epp_average_queue_size{{{labels}}} {queued} 1000\nllm_d_epp_average_running_requests{{{labels}}} {in_flight} 1000\nllm_d_epp_ready_endpoints{{{labels}}} 1 1000\n"
        )
    }

    /// A plain HTTP backend answering 200 with its site name.
    fn backend(site: &'static str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind backend");
        let port = listener.local_addr().expect("backend addr").port();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                thread::spawn(move || {
                    let mut head = Vec::new();
                    let mut buf = [0_u8; 1024];
                    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                        match stream.read(&mut buf) {
                            Ok(0) => return,
                            Ok(n) => head.extend_from_slice(&buf[..n]),
                            Err(e) if e.kind() == ErrorKind::Interrupted => {},
                            Err(_) => return,
                        }
                    }
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{site}",
                        site.len()
                    );
                    let _sent = stream.write_all(response.as_bytes());
                });
            }
        });
        port
    }

    fn free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .expect("bind")
            .local_addr()
            .expect("addr")
            .port()
    }

    fn drain(pipe: impl std::io::Read + Send + 'static, lines: Arc<Mutex<Vec<String>>>) {
        thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                lines.lock().expect("output").push(line);
            }
        });
    }

    struct Gateway {
        child: Child,
        output: Arc<Mutex<Vec<String>>>,
        listen: u16,
        admin: u16,
    }

    impl Gateway {
        fn start(work: &Path, backends: &[(&str, u16)], peers: &[(&str, &Peer)], identity: &Path) -> Self {
            let (listen, admin) = (free_port(), free_port());
            let clusters = |indent: &str| -> String {
                backends
                    .iter()
                    .map(|(site, port)| {
                        format!("{indent}- name: \"pool-{site}\"\n{indent}  endpoints: [\"127.0.0.1:{port}\"]\n")
                    })
                    .collect()
            };
            let config = work.join("praxis.yaml");
            std::fs::write(
                &config,
                format!(
                    "admin:\n  address: \"127.0.0.1:{admin}\"\n# The test backends are on loopback.\ninsecure_options:\n  allow_private_endpoints: true\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nclusters:\n{top}filter_chains:\n  - name: main\n    filters:\n      - filter: grid_site_route\n        model_header: X-Gateway-Model-Name\n        availability: {{smoothing: 1.0, ceiling_floor: 1.0, shedding: true, full_after_ms: 0, room_after_ms: 0}}\n      - filter: load_balancer\n        clusters:\n{inline}",
                    top = clusters("  "),
                    inline = clusters("          "),
                ),
            )
            .expect("praxis config");
            let path = |key: &str| identity.join(key).to_string_lossy().into_owned();
            let candidates: Vec<String> = SITES
                .iter()
                .map(|site| {
                    format!(r#"{{"kind":"inference_model","name":"llama","site":"{site}","cluster":"pool-{site}"}}"#)
                })
                .collect();
            let peers: Vec<String> = peers
                .iter()
                .map(|(site, peer)| {
                    format!(
                        r#"{{"site":"{site}","addr":"{addr}","server_name":"{site}.grid.internal","authority":"{site}.grid.internal","interval_ms":{poll},"connect_timeout_ms":1000,"request_timeout_ms":1000,"grid_ca_path":"{ca}","client_cert_path":"{cert}","client_key_path":"{key}"}}"#,
                        addr = peer.addr,
                        poll = POLL.as_millis(),
                        ca = path("ca.crt"),
                        cert = path("tls.crt"),
                        key = path("tls.key"),
                    )
                })
                .collect();
            let serving = work.join("serving-config.json");
            std::fs::write(
                &serving,
                format!(
                    r#"{{"local_site":"hub","window_secs":60,"load_window_ms":{window},"candidates":[{}],"peers":[{}]}}"#,
                    candidates.join(","),
                    peers.join(","),
                    window = WINDOW.as_millis(),
                ),
            )
            .expect("serving config");
            let mut child = Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
                .arg("--config")
                .arg(&config)
                .env("GRID_SERVING_CONFIG", &serving)
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

        /// One request for `llama`: its status, the site that served it, and Retry-After.
        /// The admin listener's Prometheus exposition.
        fn metrics(&self) -> String {
            let mut stream = TcpStream::connect(("127.0.0.1", self.admin)).expect("connect admin");
            write!(
                stream,
                "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
            )
            .expect("send");
            let mut response = String::new();
            let _read = stream.read_to_string(&mut response);
            response
        }

        fn request(&self) -> Option<(u16, String, Option<String>)> {
            let (head, body) = self.exchange()?;
            let status = head.split(' ').nth(1)?.parse().ok()?;
            let retry_after = head
                .lines()
                .find_map(|l| {
                    l.strip_prefix("retry-after: ")
                        .or_else(|| l.strip_prefix("Retry-After: "))
                })
                .map(str::to_owned);
            Some((status, body, retry_after))
        }

        /// One request for llama: the response head and body.
        fn exchange(&self) -> Option<(String, String)> {
            let mut stream = TcpStream::connect(("127.0.0.1", self.listen)).ok()?;
            stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
            stream
                .write_all(
                    b"GET /v1/models HTTP/1.1\r\nHost: gw\r\nX-Gateway-Model-Name: llama\r\nConnection: close\r\n\r\n",
                )
                .ok()?;
            let mut response = String::new();
            stream.read_to_string(&mut response).ok()?;
            let (head, body) = response.split_once("\r\n\r\n")?;
            Some((head.to_owned(), body.to_owned()))
        }

        fn eventually(&self, what: &str, mut done: impl FnMut() -> bool) {
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

    /// Requests per serving site over `n` paced requests; any non-200 counts under `error`.
    fn split(gateway: &Gateway, n: usize) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for _ in 0..n {
            let key = match gateway.request() {
                Some((200, site, _)) => site,
                _ => "error".to_owned(),
            };
            *counts.entry(key).or_default() += 1;
            thread::sleep(PACE);
        }
        counts
    }

    /// Every site fills and the model sheds with 429 and Retry-After; one site drains and
    /// the model routes again, to that site.
    #[test]
    fn a_model_sheds_when_every_site_is_full_and_routes_again_when_one_drains() {
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("shedding-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&work);
        let identity = work.join("identity");
        std::fs::create_dir_all(&identity).expect("identity dir");
        let ca = generate_ca("grid-ca").expect("ca");
        let hub = generate_site_cert(&ca, "hub").expect("hub cert");
        for (key, pem) in [
            ("ca.crt", &ca.cert_pem),
            ("tls.crt", &hub.cert_pem),
            ("tls.key", &hub.key_pem),
        ] {
            std::fs::write(identity.join(key), pem).expect("identity file");
        }
        let peers: Vec<(&str, Peer)> = SITES.iter().map(|site| (*site, Peer::start(&ca, site))).collect();
        let backends: Vec<(&str, u16)> = SITES.iter().map(|site| (*site, backend(site))).collect();
        let gateway = Gateway::start(
            &work,
            &backends,
            &peers.iter().map(|(site, peer)| (*site, peer)).collect::<Vec<_>>(),
            &identity,
        );
        // Every site teaches its ceiling and, at it with nothing queued, still serves.
        gateway.eventually("every site serving at its ceiling", || {
            let counts = split(&gateway, 9);
            SITES.iter().all(|site| counts.contains_key(*site))
        });

        // Every site fills: at its ceiling with work queued. The model sheds.
        for (_, peer) in &peers {
            peer.mode.store(FULL, Ordering::SeqCst);
        }
        let started = Instant::now();
        let mut shed = false;
        while started.elapsed() < DEADLINE {
            if matches!(gateway.request(), Some((429, _, Some(_)))) {
                shed = true;
                break;
            }
            thread::sleep(PACE);
        }
        let grid_lines = || {
            gateway
                .metrics()
                .lines()
                .filter(|line| line.starts_with("grid_route_") || line.starts_with("grid_signals"))
                .collect::<Vec<_>>()
                .join("\n")
        };
        assert!(shed, "never shed; selection state:\n{}", grid_lines());
        let to_shed = started.elapsed();
        assert!(to_shed <= BOUND, "shed after {to_shed:?}, bound {BOUND:?}");
        let (status, body, retry_after) = gateway.request().expect("a response while shed");
        assert_eq!(status, 429, "at capacity is a rate limit, not an outage");
        assert!(retry_after.is_some(), "a shed answer carries Retry-After");
        assert!(
            body.contains("capacity_exhausted"),
            "an OpenAI-style error names the cause: {body}"
        );
        let metrics = gateway.metrics();
        let counted = |prefix: &str| -> f64 {
            metrics
                .lines()
                .filter(|line| line.starts_with(prefix))
                .filter_map(|line| line.rsplit_once(' ').and_then(|(_, value)| value.parse::<f64>().ok()))
                .sum()
        };
        assert!(
            counted("grid_route_decisions_total{site=\"\",cluster=\"\",reason=\"shed\"}") > 0.0,
            "shed answers are counted:\n{metrics}"
        );
        assert!(
            metrics.contains("grid_route_shedding{model=\"llama\"} 1"),
            "the model reads as shedding:\n{metrics}"
        );

        // One site drains: the model routes again, to that site.
        peers[0].1.mode.store(ROOM, Ordering::SeqCst);
        let resuming = Instant::now();
        gateway.eventually("routing again", || matches!(gateway.request(), Some((200, _, _))));
        let to_resume = resuming.elapsed();
        assert!(to_resume <= BOUND, "resumed after {to_resume:?}, bound {BOUND:?}");
        let counts = split(&gateway, 60);
        assert_eq!(
            counts.keys().collect::<Vec<_>>(),
            vec![&"site-a"],
            "only the drained site has room: {counts:?}"
        );
        let after = gateway.metrics();
        assert!(
            after.contains("grid_route_shedding{model=\"llama\"} 0"),
            "the model no longer reads as shedding:\n{after}"
        );

        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }
}
