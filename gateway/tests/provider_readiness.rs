//! A site whose operator reports its provider not ready gets no traffic, and rejoins when it recovers.
//!
//! Time to exclusion is one peer poll after the peer publishes zero ready endpoints,
//! and time to rejoin one poll after it publishes 1. With the operator's defaults (5s
//! scrape, 2 zero scrapes, 5s poll) that is at most about 15s to exclude and 10s to rejoin.

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
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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

    /// What a fake peer operator publishes.
    const READY: u8 = 0;
    const NOT_READY: u8 = 1;
    /// Load only, as an operator that does not publish readiness.
    const OMITTED: u8 = 2;
    /// Accept and drop every poll, as a partitioned link.
    const PARTITIONED: u8 = 3;

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
            let mode = Arc::new(AtomicU8::new(READY));
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
                    // Stamped now and dated now, as an operator serving a fresh scrape: age zero,
                    // and each poll a new observation rather than one republished.
                    let now = SystemTime::now();
                    let stamp = now.duration_since(UNIX_EPOCH).map_or(0, |since| since.as_millis());
                    let body = signals(site, publishing, stamp);
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nDate: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        httpdate::fmt_http_date(now),
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

    /// What `site`'s operator publishes in `mode` at `stamp`: an idle queue, and its readiness verdict.
    fn signals(site: &str, mode: u8, stamp: u128) -> String {
        let labels = format!(r#"grid_site="{site}",grid_provider="pool-{site}""#);
        let load = format!("llm_d_epp_average_queue_size{{{labels}}} 0 {stamp}\n");
        match mode {
            OMITTED => load,
            _ => format!(
                "{load}llm_d_epp_ready_endpoints{{{labels}}} {} {stamp}\n",
                u8::from(mode == READY)
            ),
        }
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
                    "admin:\n  address: \"127.0.0.1:{admin}\"\n# The test backends are on loopback.\ninsecure_options:\n  allow_private_endpoints: true\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nclusters:\n{top}filter_chains:\n  - name: main\n    filters:\n      - filter: grid_site_route\n        model_header: X-Gateway-Model-Name\n      - filter: load_balancer\n        clusters:\n{inline}",
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

        /// One request for `llama`: its status, the site that served it, and Retry-After.
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

    /// Paced requests until twenty in a row are served by a site `pred` accepts, and how long
    /// until the first of them.
    ///
    /// Twenty, since sites are picked at random: with one rejected site of three still picked,
    /// twenty accepted in a row has probability (2/3)^20, below 1 in 3000.
    fn time_until(gateway: &Gateway, mut pred: impl FnMut(&str) -> bool) -> Duration {
        let start = Instant::now();
        let mut streak = 0;
        let mut streak_start = start;
        while streak < 20 {
            assert!(start.elapsed() < DEADLINE, "never settled");
            let sent = Instant::now();
            match gateway.request() {
                Some((200, site, _)) if pred(&site) => {
                    if streak == 0 {
                        streak_start = sent;
                    }
                    streak += 1;
                },
                _ => streak = 0,
            }
            thread::sleep(PACE);
        }
        streak_start.duration_since(start)
    }

    #[test]
    fn site_b_not_ready_routes_to_the_other_sites_and_rejoins_when_ready() {
        let work =
            PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("provider-readiness-{}", std::process::id()));
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
        let site_b = &peers[1].1;

        gateway.eventually("every site serving", || {
            let counts = split(&gateway, 9);
            SITES.iter().all(|site| counts.contains_key(*site))
        });
        // site-b's provider goes not ready.
        site_b.mode.store(NOT_READY, Ordering::SeqCst);
        let to_exclusion = time_until(&gateway, |site| site != "site-b");
        assert!(
            to_exclusion <= BOUND,
            "excluded after {to_exclusion:?}, bound {BOUND:?}"
        );
        let excluded = split(&gateway, 200);
        assert_eq!(
            excluded.get("error"),
            None,
            "no client errors once excluded: {excluded:?}"
        );
        assert_eq!(excluded.get("site-b"), None, "nothing to site-b: {excluded:?}");
        assert!(
            // Each takes about 100 of 200 at random, standard deviation about 7.
            excluded.get("site-a").copied().unwrap_or(0) >= 70 && excluded.get("site-d").copied().unwrap_or(0) >= 70,
            "site-a and site-d share the load: {excluded:?}"
        );

        // Its link partitions past the load window: the last verdict still stands.
        site_b.mode.store(PARTITIONED, Ordering::SeqCst);
        thread::sleep(WINDOW * 3);
        let partitioned = split(&gateway, 50);
        assert_eq!(
            partitioned.get("site-b"),
            None,
            "a silent peer is not readmitted: {partitioned:?}"
        );
        assert_eq!(partitioned.get("error"), None, "{partitioned:?}");

        // It heals with an operator that no longer publishes readiness: absent reads as ready.
        site_b.mode.store(OMITTED, Ordering::SeqCst);
        let healed = Instant::now();
        while !gateway
            .request()
            .is_some_and(|(status, site, _)| status == 200 && site == "site-b")
        {
            assert!(healed.elapsed() < DEADLINE, "site-b never rejoined without the series");
            thread::sleep(PACE);
        }
        assert!(
            healed.elapsed() <= BOUND,
            "rejoined without the series after {:?}",
            healed.elapsed()
        );

        // Back to publishing, not ready, then ready.
        site_b.mode.store(NOT_READY, Ordering::SeqCst);
        time_until(&gateway, |site| site != "site-b");
        site_b.mode.store(READY, Ordering::SeqCst);
        let start = Instant::now();
        while !gateway
            .request()
            .is_some_and(|(status, site, _)| status == 200 && site == "site-b")
        {
            assert!(start.elapsed() < DEADLINE, "site-b never rejoined");
            thread::sleep(PACE);
        }
        let to_rejoin = start.elapsed();
        assert!(to_rejoin <= BOUND, "rejoined after {to_rejoin:?}, bound {BOUND:?}");
        let rejoined = split(&gateway, 300);
        assert_eq!(rejoined.get("error"), None, "{rejoined:?}");
        // Each takes about 100 of 300 at random, standard deviation about 8.
        for site in SITES {
            let share = rejoined.get(site).copied().unwrap_or(0);
            assert!(
                (70..=130).contains(&share),
                "{site} took {share} of 300, a herd: {rejoined:?}"
            );
        }

        // Every site not ready: a known model answers 503 with Retry-After, not 404.
        for (_, peer) in &peers {
            peer.mode.store(NOT_READY, Ordering::SeqCst);
        }
        gateway.eventually("503 once every site is excluded", || {
            gateway.request().is_some_and(|(status, _, after)| {
                status == 503
                    && after
                        .and_then(|a| a.parse::<u8>().ok())
                        .is_some_and(|a| (3..=7).contains(&a))
            })
        });

        let metrics = gateway.metrics();
        let counted = |prefix: &str| -> f64 {
            metrics
                .lines()
                .filter(|line| line.starts_with(prefix))
                .filter_map(|line| line.rsplit_once(' ').and_then(|(_, value)| value.parse::<f64>().ok()))
                .sum()
        };
        for site in SITES {
            let routed = counted(&format!("grid_route_decisions_total{{site=\"{site}\","));
            assert!(routed > 0.0, "no routing decisions for {site}:\n{metrics}");
        }
        let refused = counted("grid_route_decisions_total{site=\"\",cluster=\"\",reason=\"not_ready\"}");
        assert!(refused > 0.0, "the 503s are not counted:\n{metrics}");
        let score = "grid_route_site_score{site=\"site-b\",cluster=\"pool-site-b\"} NaN";
        assert!(metrics.contains(score), "an excluded site keeps a score:\n{metrics}");

        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }
}
