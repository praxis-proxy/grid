//! The gateway dials sites the serving config alone describes, with no praxis cluster for them.

#[path = "support/http.rs"]
mod http;
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
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use ai_grid_filters::{Binding, DialConfig, GridIdentity, GridServingConfig};
    use certs::{CaCert, DEFAULT_TRUST_DOMAIN, GridSpiffeClientVerifier, generate_ca, generate_site_cert};
    use rustls::{
        ServerConfig, ServerConnection, StreamOwned,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _},
    };

    use super::{
        http::read_request,
        ports::{bound_listener, free_port},
    };

    /// How long any one expected change may take: the watch re-reads every 5s.
    const DEADLINE: Duration = Duration::from_secs(20);

    /// The serving config the operator renders beside its dial file.
    const DIAL_SERVING: &str = include_str!("testdata/serving-config-dial.json");

    /// The dial file the operator renders beside the serving config.
    const DIAL_FILE: &str = include_str!("testdata/dial.json");

    #[test]
    fn the_operator_dial_render_loads_and_builds_every_dial() {
        let work = fresh_work("dial-golden");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca, "site-a");
        let tls = identity.dir.to_string_lossy().into_owned();
        let backend_ca_base = work.join("backend-ca");
        std::fs::create_dir_all(backend_ca_base.join("models-ca")).expect("backend CA dir");
        std::fs::write(backend_ca_base.join("models-ca/ca.crt"), &ca.cert_pem).expect("backend CA");
        let backend_ca_base_text = backend_ca_base.to_string_lossy().into_owned();
        let render = DIAL_FILE.replace(ai_grid_filters::BACKEND_CA_BASE, &backend_ca_base_text);
        let mut config: GridServingConfig = serde_yaml::from_str(DIAL_SERVING).expect("the serving render parses");
        let identity_of = GridIdentity::of(&config);
        assert_eq!(
            identity_of,
            GridIdentity::under("/etc/praxis/tls"),
            "the rendered peer presents the grid TLS mount"
        );
        config.peers.clear();
        let binding = Binding {
            backend_ca_base,
            grid_identity: GridIdentity::under(&tls),
            ..Binding::default()
        };
        let load = |text: &str| {
            let dial: DialConfig = serde_yaml::from_str(text).expect("the dial render parses");
            ai_grid_filters::spawn_grid_routing(&config, &dial, binding.clone())
        };

        let runtime = load(&render).expect("the render loads");
        let snapshot = runtime.snapshot().load_full();
        let mut dialed: Vec<&str> = snapshot
            .candidates
            .iter()
            .filter(|candidate| candidate.dial.is_some())
            .map(|candidate| &*candidate.cluster)
            .collect();
        dialed.sort_unstable();
        assert_eq!(
            dialed,
            ["local-granite", "local-llama", "site-b"],
            "every rendered dial is built"
        );
        assert!(
            snapshot.provider_hop_clusters.contains("site-b"),
            "the remote dial carries hop context"
        );
        drop(runtime);

        for (from, to) in [
            ("\"sni\": \"site-b.grid.internal\"", "\"sni\": \"site-c.grid.internal\""),
            ("\"mode\": \"mutual_tls\"", "\"mode\": \"plaintext\""),
            (&*backend_ca_base_text, "/etc/praxis/tls"),
        ] {
            assert!(render.contains(from), "the fixture still carries {from}");
            let refused = load(&render.replace(from, to)).expect("a refused dial file never stops startup");
            assert!(
                refused
                    .snapshot()
                    .load()
                    .candidates
                    .iter()
                    .all(|candidate| candidate.dial.is_none()),
                "{to} builds a dial, so the trust rule guards nothing"
            );
        }
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_dialed_site_gets_hop_context_and_never_the_callers_credential() {
        let work = fresh_work("dial-hop");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca, "local");
        let east = Site::start(&ca, "east");
        let mut serving = Mount::new(work.join("serving"));
        dial_serving(&[("east", &east)], &identity).write(&mut serving);

        let gateway = Gateway::start(&work, &serving);
        for sent in 1..=3 {
            let (status, head) = gateway.chat();
            assert_eq!(status, 200, "request {sent} reaches east over its dial");
            assert!(head.contains("x-grid-site: east"), "attributed to east: {head}");
        }
        let heads = east.heads();
        assert_eq!(heads.len(), 3, "every request reached east");
        for head in &heads {
            assert!(
                head.contains("x-ai-routing-candidate: candidate-east"),
                "hop context names the candidate: {head}"
            );
            assert!(
                !head.contains("authorization:"),
                "the caller's credential stays on this site: {head}"
            );
        }
        assert!(
            east.saw(&leaf_fingerprint(&identity.leaf)),
            "east saw this site's identity"
        );
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_site_presenting_another_sites_name_is_never_sent_a_request() {
        let work = fresh_work("dial-wrong-name");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca, "local");
        let west = Site::start(&ca, "west");
        let mut serving = Mount::new(work.join("serving"));
        dial_serving(&[("east", &west)], &identity).write(&mut serving);

        let gateway = Gateway::start(&work, &serving);
        let (status, _) = gateway.chat();
        assert_eq!(status, 502, "west's certificate does not name east");
        assert!(west.heads().is_empty(), "no request reached the wrong site");
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_site_added_through_the_serving_mount_alone_routes() {
        let work = fresh_work("dial-hot-add");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca, "local");
        let (east, west) = (Site::start(&ca, "east"), Site::start(&ca, "west"));
        let mut serving = Mount::new(work.join("serving"));
        dial_serving(&[("east", &east)], &identity).write(&mut serving);

        let gateway = Gateway::start(&work, &serving);
        assert_eq!(gateway.chat().0, 200, "east routes");
        gateway.serve(&mut serving, &dial_serving(&[("west", &west)], &identity));
        for sent in 1..=3 {
            let (status, head) = gateway.chat();
            assert_eq!(status, 200, "request {sent} routes");
            assert!(head.contains("x-grid-site: west"), "to the added site: {head}");
        }
        assert_eq!(west.heads().len(), 3, "west took every request after the rewrite");
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_rotated_identity_keeps_routing_with_the_renewed_leaf() {
        let work = fresh_work("dial-rotate");
        let ca = generate_ca("grid-ca").expect("ca");
        let mut identity = site_identity(&work, &ca, "local");
        let east = Site::start(&ca, "east");
        let mut serving = Mount::new(work.join("serving"));
        dial_serving(&[("east", &east)], &identity).write(&mut serving);

        let gateway = Gateway::start(&work, &serving);
        assert_eq!(gateway.chat().0, 200, "east routes on the first identity");
        let renewed = generate_site_cert(&ca, "local").expect("renewed cert");
        identity.mount.write(&[
            ("ca.crt", ca.cert_pem.as_bytes()),
            ("tls.crt", renewed.cert_pem.as_bytes()),
            ("tls.key", renewed.key_pem.as_bytes()),
        ]);
        let renewed_leaf = leaf_fingerprint(&renewed.cert_pem);
        gateway.eventually("east sees the renewed leaf on a routed request", || {
            gateway.chat().0 == 200 && east.saw(&renewed_leaf)
        });
        assert_eq!(gateway.chat().0, 200, "routing continues on the renewed identity");
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_killed_site_is_demoted_and_the_live_one_takes_the_traffic() {
        let work = fresh_work("dial-killed");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca, "local");
        let (east, west) = (Site::start(&ca, "east"), Site::start(&ca, "west"));
        let mut serving = Mount::new(work.join("serving"));
        dial_serving(&[("east", &east), ("west", &west)], &identity).write(&mut serving);

        let gateway = Gateway::start(&work, &serving);
        gateway.eventually("both sites take traffic", || {
            gateway.chat();
            !east.heads().is_empty() && !west.heads().is_empty()
        });
        drop(east);
        gateway.eventually("east is down", || {
            gateway.chat();
            gateway.metric(r#"grid_dial_down{cluster="east"}"#) == Some(1.0)
        });
        for sent in 1..=10 {
            let (status, head) = gateway.chat();
            assert_eq!(status, 200, "request {sent} avoids the dead site");
            assert!(head.contains("x-grid-site: west"), "to the live site: {head}");
        }
        assert!(
            gateway
                .metric(r#"grid_dial_failures_total{cluster="east"}"#)
                .is_some_and(|failures| failures >= 2.0),
            "the failures that took east down are counted"
        );
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_local_dial_sends_its_token_under_the_providers_base_path() {
        let work = fresh_work("dial-local-token");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca, "local");
        let secret = work.join("credentials/grid-model-client-token");
        std::fs::create_dir_all(&secret).expect("credential dir");
        std::fs::write(secret.join("token"), "site-token\n").expect("token");
        let listener = bound_listener();
        let addr = listener.local_addr().expect("backend addr").to_string();
        let heads = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&heads);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let Ok((head, _body)) = read_request(&mut stream) else {
                    continue;
                };
                recorded.lock().expect("heads").push(head);
                let _sent = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
            }
        });
        let mut serving = Mount::new(work.join("serving"));
        Revision {
            dial: format!(
                r#"{{"version":"v1","dial":{{"local-model":{{"address":"{addr}","site":"local","transport":{{"mode":"plaintext"}},"base_path":"/publishers/x/models/y","credential":{{"strategy":"bearer_token","path":"{token}"}}}}}}}}"#,
                token = secret.join("token").display(),
            ),
            serving: format!(
                r#"{{"local_site":"local","window_secs":60,"load_window_ms":30000,"candidates":[{{"kind":"inference_model","name":"llama","site":"local","cluster":"local-model","credential":{{"strategy":"bearer_token","secretRef":{{"name":"grid-model-client-token","namespace":"grid","key":"token"}}}}}}],"peers":[{}]}}"#,
                local_peer(&identity),
            ),
        }
        .write(&mut serving);

        let gateway = Gateway::start(&work, &serving);
        let (status, head) = gateway.chat();
        assert_eq!(status, 200, "the local dial routes: {head}");
        let heads = heads.lock().expect("heads").clone();
        let sent = heads.first().expect("the backend saw the request").to_ascii_lowercase();
        assert!(
            sent.starts_with("get /publishers/x/models/y/v1/chat/completions "),
            "sent under the provider's base path: {sent}"
        );
        assert!(
            sent.contains("authorization: bearer site-token"),
            "with the dial's token: {sent}"
        );
        assert!(!sent.contains("caller-secret"), "never the caller's: {sent}");
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_post_whose_first_connect_fails_is_retried_on_the_dial() {
        let work = fresh_work("dial-retry-post");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca, "local");
        let east = Site::dropping_first(&ca, "east");
        let mut serving = Mount::new(work.join("serving"));
        dial_serving(&[("east", &east)], &identity).write(&mut serving);

        let gateway = Gateway::start(&work, &serving);
        let (status, head) = gateway.post_chat();
        assert_eq!(status, 200, "the POST is retried after the failed handshake: {head}");
        assert_eq!(east.heads().len(), 1, "sent once, on the retried connection");
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_client_that_leaves_after_the_site_took_its_request_never_demotes_the_site() {
        let work = fresh_work("dial-client-abort");
        let ca = generate_ca("grid-ca").expect("ca");
        let identity = site_identity(&work, &ca, "local");
        let east = Site::holding(&ca, "east");
        let mut serving = Mount::new(work.join("serving"));
        dial_serving(&[("east", &east)], &identity).write(&mut serving);

        let gateway = Gateway::start(&work, &serving);
        for sent in 1..=3 {
            let stream = gateway.send_chat();
            gateway.eventually("east took the request", || east.heads().len() == sent);
            drop(stream);
        }
        gateway.eventually("every abandoned request ran to its end", || {
            gateway.metric("praxis_http_active_requests") == Some(0.0)
        });
        assert_ne!(
            gateway.metric(r#"grid_dial_down{cluster="east"}"#),
            Some(1.0),
            "a client leaving says nothing about east"
        );
        assert!(
            gateway
                .metric(r#"grid_dial_failures_total{cluster="east"}"#)
                .is_none_or(|failures| failures == 0.0),
            "no dial failure is counted"
        );
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// A directory laid out like a kubelet `ConfigMap` or `Secret` volume.
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

    /// This site's identity mount and the leaf it started with.
    struct Identity {
        dir: PathBuf,
        leaf: String,
        mount: Mount,
    }

    fn site_identity(work: &Path, ca: &CaCert, site: &str) -> Identity {
        let cert = generate_site_cert(ca, site).expect("site cert");
        let mut mount = Mount::new(work.join("identity"));
        mount.write(&[
            ("ca.crt", ca.cert_pem.as_bytes()),
            ("tls.crt", cert.cert_pem.as_bytes()),
            ("tls.key", cert.key_pem.as_bytes()),
        ]);
        Identity {
            dir: mount.dir.clone(),
            leaf: cert.cert_pem,
            mount,
        }
    }

    /// A site's gateway over grid mutual TLS, recording each request head and client leaf.
    struct Site {
        addr: String,
        heads: Arc<Mutex<Vec<String>>>,
        seen: Arc<Mutex<Vec<[u8; 32]>>>,
        stop: Arc<AtomicBool>,
    }

    impl Site {
        fn start(ca: &CaCert, site: &str) -> Self {
            Self::start_with(ca, site, true, false)
        }

        /// A site that takes each request and never answers, holding the connection open.
        fn holding(ca: &CaCert, site: &str) -> Self {
            Self::start_with(ca, site, false, false)
        }

        /// A site that hangs up its first connection before the TLS handshake.
        fn dropping_first(ca: &CaCert, site: &str) -> Self {
            Self::start_with(ca, site, true, true)
        }

        fn start_with(ca: &CaCert, site: &str, answer: bool, drop_first: bool) -> Self {
            let identity = generate_site_cert(ca, site).expect("site cert");
            let chain = CertificateDer::pem_slice_iter(identity.cert_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .expect("site chain");
            let key = PrivateKeyDer::from_pem_slice(identity.key_pem.as_bytes()).expect("site key");
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
            let listener = bound_listener();
            let addr = listener.local_addr().expect("site addr").to_string();
            let heads = Arc::new(Mutex::new(Vec::new()));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let (recorded_heads, recorded_leaves, stopping) =
                (Arc::clone(&heads), Arc::clone(&seen), Arc::clone(&stop));
            thread::spawn(move || {
                let mut dropping = drop_first;
                for stream in listener.incoming() {
                    if stopping.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(stream) = stream else { continue };
                    if std::mem::take(&mut dropping) {
                        drop(stream);
                        continue;
                    }
                    let Ok(conn) = ServerConnection::new(Arc::clone(&config)) else {
                        continue;
                    };
                    let mut tls = StreamOwned::new(conn, stream);
                    let Ok((head, _body)) = read_request(&mut tls) else {
                        continue;
                    };
                    if let Some(leaf) = tls.conn.peer_certificates().and_then(<[_]>::first) {
                        recorded_leaves.lock().expect("seen").push(certs::sha256(leaf.as_ref()));
                    }
                    recorded_heads.lock().expect("heads").push(head.to_ascii_lowercase());
                    if !answer {
                        // Held open until the gateway hangs up.
                        thread::spawn(move || {
                            let mut sink = [0_u8; 1];
                            while tls.read(&mut sink).is_ok_and(|read| read > 0) {}
                        });
                        continue;
                    }
                    let _sent = tls
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                        .and_then(|()| tls.flush());
                }
            });
            Self {
                addr,
                heads,
                seen,
                stop,
            }
        }

        fn heads(&self) -> Vec<String> {
            self.heads.lock().expect("heads").clone()
        }

        fn saw(&self, fingerprint: &[u8; 32]) -> bool {
            self.seen.lock().expect("seen").contains(fingerprint)
        }
    }

    impl Drop for Site {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _woken = TcpStream::connect(&self.addr);
        }
    }

    /// A serving config routing `llama` to each `(site, server)` over a mutual TLS dial, polling nothing.
    fn dial_serving(sites: &[(&str, &Site)], identity: &Identity) -> Revision {
        let candidates: Vec<String> = sites
            .iter()
            .map(|(site, _)| {
                format!(
                    r#"{{"kind":"inference_model","name":"llama","site":"{site}","cluster":"{site}","stable_id":"candidate-{site}"}}"#
                )
            })
            .collect();
        let dials: Vec<String> = sites
            .iter()
            .map(|(site, server)| {
                format!(
                    r#""{site}":{{"address":"{addr}","connect_timeout_ms":1000,"site":"{site}","transport":{{"mode":"mutual_tls","sni":"{site}.grid.internal"}}}}"#,
                    addr = server.addr,
                )
            })
            .collect();
        Revision {
            dial: format!(r#"{{"version":"v1","dial":{{{}}}}}"#, dials.join(",")),
            serving: format!(
                r#"{{"local_site":"local","window_secs":60,"load_window_ms":30000,"candidates":[{}],"peers":[{}]}}"#,
                candidates.join(","),
                local_peer(identity),
            ),
        }
    }

    /// The local operator entry presenting `identity`, polling a port nothing listens on.
    fn local_peer(identity: &Identity) -> String {
        format!(
            r#"{{"site":"local","addr":"127.0.0.1:{port}","server_name":"local.grid.internal","authority":"local.grid.internal","interval_ms":60000,"connect_timeout_ms":100,"request_timeout_ms":100,"grid_ca_path":"{ca}","client_cert_path":"{cert}","client_key_path":"{key}"}}"#,
            port = free_port(),
            ca = identity.mount.path("ca.crt"),
            cert = identity.mount.path("tls.crt"),
            key = identity.mount.path("tls.key"),
        )
    }

    /// One revision of the serving mount: the serving config and the dial file beside it.
    struct Revision {
        dial: String,
        serving: String,
    }

    impl Revision {
        /// Swap both files in at once, as the kubelet swaps one `ConfigMap`.
        fn write(&self, mount: &mut Mount) {
            mount.write(&[
                ("serving-config.json", self.serving.as_bytes()),
                (ai_grid_filters::DIAL_FILE, self.dial.as_bytes()),
            ]);
        }
    }

    /// The SHA-256 of the leaf a PEM chain starts with.
    fn leaf_fingerprint(cert_pem: &str) -> [u8; 32] {
        let leaf = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
            .next()
            .expect("a leaf")
            .expect("parse leaf");
        certs::sha256(leaf.as_ref())
    }

    fn fresh_work(name: &str) -> PathBuf {
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&work);
        work
    }

    /// Collect `pipe`'s lines into `lines` on a background thread.
    fn drain(pipe: impl std::io::Read + Send + 'static, lines: Arc<Mutex<Vec<String>>>) {
        thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                lines.lock().expect("output").push(line);
            }
        });
    }

    /// The gateway with no praxis cluster: `grid_site_route` declares none and nothing balances.
    struct Gateway {
        child: Child,
        output: Arc<Mutex<Vec<String>>>,
        listen: u16,
        admin: u16,
    }

    impl Gateway {
        fn start(work: &Path, serving: &Mount) -> Self {
            let (listen, admin) = (free_port(), free_port());
            let yaml = format!(
                "insecure_options:\n  allow_private_endpoints: true\nadmin:\n  metrics_address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: grid_site_route\n        model_header: X-Model\n        clusters: []\n"
            );
            let config = work.join("praxis.yaml");
            std::fs::write(&config, yaml).expect("praxis config");
            let mut child = Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
                .arg("--config")
                .arg(&config)
                .env("GRID_SERVING_CONFIG", serving.path("serving-config.json"))
                .env("GRID_TLS_DIR", work.join("identity"))
                .env("GRID_CREDENTIAL_DIR", work.join("credentials"))
                .env("RUST_LOG", "info")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn gateway");
            let output = Arc::new(Mutex::new(Vec::new()));
            drain(child.stdout.take().expect("stdout"), Arc::clone(&output));
            drain(child.stderr.take().expect("stderr"), Arc::clone(&output));
            let gateway = Self {
                child,
                output,
                listen,
                admin,
            };
            gateway.eventually("the gateway listens", || {
                TcpStream::connect(("127.0.0.1", listen)).is_ok()
            });
            gateway
        }

        /// Send one chat request carrying a caller credential, returning the open connection.
        fn send_chat(&self) -> TcpStream {
            let mut stream = TcpStream::connect(("127.0.0.1", self.listen)).expect("connect gateway");
            stream
                .write_all(b"GET /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nX-Model: llama\r\nAuthorization: Bearer caller-secret\r\nConnection: close\r\n\r\n")
                .expect("send request");
            stream
        }

        /// One chat POST with a body: its status and lowercased response head.
        fn post_chat(&self) -> (u16, String) {
            let mut stream = TcpStream::connect(("127.0.0.1", self.listen)).expect("connect gateway");
            let body = r#"{"model":"llama","messages":[]}"#;
            stream
                .write_all(
                    format!(
                        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nX-Model: llama\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .expect("send request");
            Self::response(stream)
        }

        /// One chat request carrying a caller credential: its status and lowercased response head.
        fn chat(&self) -> (u16, String) {
            Self::response(self.send_chat())
        }

        /// The status and lowercased head `stream` answers with.
        fn response(mut stream: TcpStream) -> (u16, String) {
            let mut response = String::new();
            stream.read_to_string(&mut response).expect("read response");
            let status = response
                .split_whitespace()
                .nth(1)
                .and_then(|status| status.parse().ok())
                .expect("numeric HTTP status");
            let head = response
                .split("\r\n\r\n")
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase();
            (status, head)
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

        /// The value of the admin metrics series that starts with `series`, if it exists.
        fn metric(&self, series: &str) -> Option<f64> {
            let mut stream = TcpStream::connect(("127.0.0.1", self.admin)).ok()?;
            let _sent = stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
            let mut body = String::new();
            let _read = stream.read_to_string(&mut body);
            body.lines()
                .find(|line| line.starts_with(series))
                .and_then(|line| line.rsplit(' ').next())
                .and_then(|value| value.parse().ok())
        }

        /// Swap in `revision` and wait until it is applied.
        fn serve(&self, serving: &mut Mount, revision: &Revision) {
            let applied = self.reloads("applied");
            revision.write(serving);
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
            super::logs::gateway_output_on_failure(&self.output);
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }
}
