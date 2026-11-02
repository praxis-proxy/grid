//! A provider gateway serves only admitted peers, only to its own backends, and never forwards
//! a peer's credentials, with admission reloaded from the provider file in place.

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
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_methods,
    clippy::too_many_lines,
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
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use certs::{CaCert, SiteCertOutput, generate_ca, generate_dns_only_cert, generate_site_cert};
    use rustls::{
        ClientConfig, ClientConnection, RootCertStore, StreamOwned,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _},
    };

    use super::{
        http::read_request,
        logs::gateway_output_on_failure,
        ports::{bound_listener, free_port},
    };

    /// How long one expected change may take: the provider file is re-read every 5s.
    const DEADLINE: Duration = Duration::from_secs(30);

    /// The stable ID the backend is served under.
    const SERVED: &str = "ccac628c";

    /// A stable ID this site does not serve, as a consumer sends for another site's model.
    const ELSEWHERE: &str = "0badf00d";

    #[test]
    fn a_provider_admits_listed_sites_only_and_reloads_admission_in_place() {
        let work = fresh_work("provider-admission");
        let ca = generate_ca("grid-ca").expect("ca");
        let other_ca = generate_ca("other-ca").expect("other ca");
        let tls = write_identity(&work, &ca, "site-a");
        let (west, north) = (
            generate_site_cert(&ca, "site-b").expect("west"),
            generate_site_cert(&ca, "site-c").expect("north"),
        );
        let local = generate_site_cert(&ca, "site-a").expect("this site's own leaf");
        let unnamed = generate_dns_only_cert(&ca, "site-b", &[]).expect("a Grid-CA leaf with no SPIFFE ID");
        let foreign = generate_site_cert(&other_ca, "site-b").expect("another CA's leaf");
        let backend = Backend::start();
        let remote = Backend::start();
        let mut mount = Mount::new(work.join("serving"));
        mount.write(&[]);
        let mut gateway = Gateway::start(&work, &tls, &provider_config(&work, &tls), Some(&mount), &[]);
        eventually("the listener", || {
            TcpStream::connect(("127.0.0.1", gateway.listen)).is_ok()
        });
        assert_eq!(
            gateway.chat(&ca, &west, SERVED),
            Some(403),
            "a missing provider file refuses every peer"
        );

        mount.write(&[("provider.json", provider_file(&["site-b"], &backend).as_bytes())]);
        eventually("site-b admitted", || gateway.chat(&ca, &west, SERVED) == Some(200));
        assert_eq!(
            gateway.chat(&ca, &north, SERVED),
            Some(403),
            "an unlisted Grid-CA site is refused"
        );
        assert_eq!(
            gateway.chat(&ca, &local, SERVED),
            Some(403),
            "this site's own leaf is refused"
        );
        assert_eq!(
            gateway.chat(&ca, &unnamed, SERVED),
            None,
            "a leaf with no SPIFFE ID fails the handshake"
        );
        assert_eq!(
            gateway.chat(&ca, &foreign, SERVED),
            None,
            "another CA's leaf fails the handshake"
        );
        assert_eq!(
            gateway.chat(&ca, &west, ELSEWHERE),
            Some(503),
            "a stable ID this site does not serve has no upstream"
        );
        let served = backend.heads();
        let head = served
            .last()
            .expect("the backend took the admitted request")
            .to_ascii_lowercase();
        for stripped in [
            "authorization:",
            "cookie:",
            "x-api-key:",
            "proxy-authorization:",
            "x-goog-api-key:",
            "x-ai-routing-candidate:",
            "x-ai-routing-revision:",
            "api_key=",
            "token=",
        ] {
            assert!(!head.contains(stripped), "{stripped} never reaches the backend: {head}");
        }
        assert!(
            head.contains("x-ai-routing-request-id: req-1"),
            "the request id is kept: {head}"
        );
        assert!(head.contains("stream=true"), "other query parameters are kept: {head}");
        assert!(
            head.starts_with("post /v1/chat/completions?"),
            "the path is kept: {head}"
        );
        assert!(
            !head.contains("x-peer-extra:"),
            "an unlisted header never reaches the backend: {head}"
        );

        let mut open_west = Client::connect(&ca, &west, gateway.listen).expect("open site-b connection");
        assert_eq!(
            open_west.chat(SERVED),
            Some(200),
            "site-b's open connection is admitted"
        );

        let refusals = gateway.lines("provider file refused");
        for bad in [
            provider_file(&["site-b"], &backend).replace("\"v1\"", "\"v2\""),
            provider_file(&["site-b"], &backend).replace(
                "\"mode\": \"plaintext\"",
                "\"mode\": \"mutual_tls\", \"sni\": \"site-a.grid.internal\"",
            ),
            provider_file(&["site-b"], &remote).replace("\"site\": \"site-a\"", "\"site\": \"site-d\""),
            provider_file(&["site-b", "site-a"], &backend),
        ] {
            let before = gateway.lines("provider file refused");
            mount.write(&[("provider.json", bad.as_bytes())]);
            eventually("the refusal", || gateway.lines("provider file refused") > before);
            assert_eq!(
                gateway.chat(&ca, &west, SERVED),
                Some(200),
                "a refused file keeps the last good one"
            );
            assert_eq!(
                gateway.chat(&ca, &local, SERVED),
                Some(403),
                "this site is never admitted"
            );
        }
        assert_eq!(
            gateway.lines("provider file refused"),
            refusals + 4,
            "each bad file refused"
        );
        assert_eq!(remote.connections(), 0, "a refused remote dial never connects");

        let refused_and_shrunk =
            provider_file(&["site-c"], &backend).replace("\"site\": \"site-a\"", "\"site\": \"site-d\"");
        mount.write(&[("provider.json", refused_and_shrunk.as_bytes())]);
        eventually("a refused file that drops site-b refusing it", || {
            gateway.chat(&ca, &west, SERVED) == Some(403)
        });
        assert_eq!(
            gateway.chat(&ca, &north, SERVED),
            Some(403),
            "a refused file never adds a site"
        );
        mount.write(&[("provider.json", provider_file(&["site-b"], &backend).as_bytes())]);
        eventually("site-b admitted again", || {
            gateway.chat(&ca, &west, SERVED) == Some(200)
        });
        assert_eq!(
            open_west.chat(SERVED),
            Some(200),
            "site-b's open connection is admitted again"
        );

        mount.write(&[("provider.json", provider_file(&["site-c"], &backend).as_bytes())]);
        eventually("site-c admitted", || gateway.chat(&ca, &north, SERVED) == Some(200));
        assert_eq!(gateway.chat(&ca, &west, SERVED), Some(403), "a removed site is refused");
        assert_eq!(
            open_west.chat(SERVED),
            Some(403),
            "a removed site's open connection is refused on its next request"
        );

        mount.write(&[]);
        eventually("the removed file refusing site-c", || {
            gateway.chat(&ca, &north, SERVED) == Some(403)
        });
        gateway.assert_running();
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn a_config_that_could_admit_an_unverified_peer_or_route_onward_never_starts() {
        let work = fresh_work("provider-preflight");
        let ca = generate_ca("grid-ca").expect("ca");
        let tls = write_identity(&work, &ca, "site-a");
        let good = provider_config(&work, &tls);
        let mut mount = Mount::new(work.join("serving"));
        mount.write(&[]);
        let lb = "      - filter: load_balancer\n        clusters:\n          - name: elsewhere\n            endpoints: [\"127.0.0.1:9\"]\n";
        for (what, yaml, env) in [
            (
                "a load_balancer after grid_provider",
                format!("{good}{lb}"),
                &[][..],
            ),
            (
                "grid_provider after another filter",
                good.replace(
                    "      - filter: grid_provider\n",
                    "      - filter: headers\n        request_set:\n          - name: x-a\n            value: b\n      - filter: grid_provider\n",
                ),
                &[],
            ),
            (
                "a listener that only requests a client certificate",
                good.replace("client_cert_mode: require_named", "client_cert_mode: request"),
                &[],
            ),
            (
                "a listener that requires a certificate but not a grid SVID",
                good.replace("client_cert_mode: require_named", "client_cert_mode: require"),
                &[],
            ),
            (
                "a static_response after grid_provider",
                format!("{good}      - filter: static_response\n        status: 200\n"),
                &[],
            ),
            (
                "a serving config beside the provider file",
                good.clone(),
                &[("GRID_SERVING_CONFIG", "/nonexistent/serving-config.json")][..],
            ),
        ] {
            let mut gateway = Gateway::start(&work, &tls, &yaml, Some(&mount), env);
            let deadline = Instant::now() + DEADLINE;
            let status = loop {
                if let Some(status) = gateway.child.try_wait().expect("try_wait") {
                    break status;
                }
                assert!(Instant::now() < deadline, "{what}: the gateway must refuse to start");
                thread::sleep(Duration::from_millis(100));
            };
            assert!(!status.success(), "{what}: exits with an error");
            let wanted = if env.is_empty() { "grid_provider" } else { "GRID_SERVING_CONFIG" };
            eventually(&format!("{what}: the reason"), || gateway.lines(wanted) > 0);
        }
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn the_chart_render_serves_admitted_peers_only() {
        let work = fresh_work("provider-chart");
        let ca = generate_ca("grid-ca").expect("ca");
        let tls = write_identity(&work, &ca, "site-a");
        let Some(yaml) = chart_render(&work, &tls) else {
            return;
        };
        let (west, north) = (
            generate_site_cert(&ca, "site-b").expect("west"),
            generate_site_cert(&ca, "site-c").expect("north"),
        );
        let backend = Backend::start();
        let mut mount = Mount::new(work.join("serving"));
        mount.write(&[("provider.json", provider_file(&["site-b"], &backend).as_bytes())]);
        let mut gateway = Gateway::start(&work, &tls, &yaml, Some(&mount), &[]);
        eventually("site-b admitted", || gateway.chat(&ca, &west, SERVED) == Some(200));
        assert_eq!(
            gateway.chat(&ca, &north, SERVED),
            Some(403),
            "an unlisted site is refused"
        );
        assert_eq!(
            gateway.chat(&ca, &west, ELSEWHERE),
            Some(503),
            "an unserved stable ID has no upstream"
        );
        mount.write(&[("provider.json", provider_file(&["site-c"], &backend).as_bytes())]);
        eventually("site-b removed", || gateway.chat(&ca, &west, SERVED) == Some(403));
        assert_eq!(
            gateway.chat(&ca, &north, SERVED),
            Some(200),
            "the listed site is admitted"
        );
        gateway.assert_running();
        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// The provider listener and chain, pointed at this test's identity and free ports.
    fn provider_config(work: &Path, tls: &Path) -> String {
        let (listen, admin) = (free_port(), free_port());
        std::fs::write(work.join("ports"), format!("{listen} {admin}")).expect("ports");
        let tls = tls.display();
        format!(
            "insecure_options:\n  allow_private_endpoints: true\nadmin:\n  metrics_address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: gateway\n    address: \"127.0.0.1:{listen}\"\n    tls:\n      certificates:\n        - cert_path: \"{tls}/tls.crt\"\n          key_path: \"{tls}/tls.key\"\n      client_ca:\n        ca_path: \"{tls}/ca.crt\"\n      client_cert_mode: require_named\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: grid_provider\n        allowed_paths: [/v1/chat/completions, /v1/models]\n"
        )
    }

    /// The chart's provider render, moved to this test's identity and free ports, or `None`
    /// when helm is absent and skipping is asked for.
    fn chart_render(work: &Path, tls: &Path) -> Option<String> {
        let chart = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../charts/praxis-gateway");
        let output = match Command::new("helm")
            .args(["template", "render"])
            .arg(&chart)
            .args([
                "--set=praxisConfig.source=render",
                "--set=grid.siteName=site-a",
                "--set=grid.networkName=grid",
                "--set=image.flavor=grid-gateway",
                "--set=praxisConfig.render.role=provider",
                "--set=gridIdentity.tlsSecretName=grid-site-identity",
                "--set=praxisConfig.render.peerTrust.mode=spiffe",
                "--set=praxisConfig.render.gridServing.enabled=true",
                "--set=praxisConfig.render.gridServing.gatewayRefName=grid-gateway",
                "--set=praxisConfig.render.provider.allowedPaths[0]=/v1/chat/completions",
            ])
            .args(["--show-only", "templates/gateway-config.yaml"])
            .output()
        {
            Ok(output) => output,
            Err(_) if std::env::var("GRID_SKIP_CHART_RENDER").is_ok_and(|skip| skip == "1") => return None,
            Err(error) => panic!("helm is needed to render the chart ({error}); set GRID_SKIP_CHART_RENDER=1 to skip"),
        };
        assert!(
            output.status.success(),
            "helm template: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let config_map: serde_yaml::Value = serde_yaml::from_slice(&output.stdout).expect("a ConfigMap");
        let yaml = config_map["data"]["praxis.yaml"].as_str().expect("praxis.yaml");
        let (listen, admin) = (free_port(), free_port());
        std::fs::write(work.join("ports"), format!("{listen} {admin}")).expect("ports");
        // The backend is on loopback, which praxis refuses without the test-only opt-in.
        Some(
            format!("insecure_options:\n  allow_private_endpoints: true\n{yaml}")
                .replace("/etc/praxis/tls/", &format!("{}/", tls.display()))
                .replace("0.0.0.0:8080", &format!("127.0.0.1:{listen}"))
                .replace("127.0.0.1:9901", &format!("127.0.0.1:{admin}")),
        )
    }

    /// A provider file admitting `sites` to `backend` under [`SERVED`].
    fn provider_file(sites: &[&str], backend: &Backend) -> String {
        let sites = sites
            .iter()
            .map(|site| format!("\"{site}\""))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "{{\n  \"admitted_sites\": [{sites}],\n  \"candidates\": [{{\"stable_id\": \"{SERVED}\", \"name\": \"llama\", \"cluster\": \"local-llama\"}}],\n  \"dial\": {{\"local-llama\": {{\"address\": \"{}\", \"site\": \"site-a\", \"transport\": {{\"mode\": \"plaintext\"}}}}}},\n  \"local_site\": \"site-a\",\n  \"version\": \"v1\"\n}}\n",
            backend.addr
        )
    }

    /// The gateway's identity under `work/tls`.
    fn write_identity(work: &Path, ca: &CaCert, site: &str) -> PathBuf {
        let dir = work.join("tls");
        std::fs::create_dir_all(&dir).expect("tls dir");
        let leaf = generate_site_cert(ca, site).expect("gateway leaf");
        std::fs::write(dir.join("ca.crt"), &ca.cert_pem).expect("ca");
        std::fs::write(dir.join("tls.crt"), &leaf.cert_pem).expect("cert");
        std::fs::write(dir.join("tls.key"), &leaf.key_pem).expect("key");
        dir
    }

    fn fresh_work(name: &str) -> PathBuf {
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&work);
        std::fs::create_dir_all(&work).expect("work dir");
        work
    }

    /// A directory laid out like a kubelet `ConfigMap` volume.
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

        /// Swap in a generation holding exactly `files`; a key left out stops resolving.
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
            let entry = self.dir.join("provider.json");
            if entry.symlink_metadata().is_err() {
                std::os::unix::fs::symlink(Path::new("..data").join("provider.json"), &entry).expect("key link");
            }
            // Kept one write longer, so a read that resolved the old revision still finds it.
            if self.generation > 2 {
                std::fs::remove_dir_all(self.dir.join(format!("..gen_{}", self.generation - 2)))
                    .expect("old generation");
            }
        }
    }

    /// A plaintext backend that records each request head on kept-open connections.
    struct Backend {
        addr: String,
        heads: Arc<Mutex<Vec<String>>>,
        connections: Arc<AtomicUsize>,
    }

    impl Backend {
        fn start() -> Self {
            let listener = bound_listener();
            let addr = listener.local_addr().expect("backend addr").to_string();
            let heads = Arc::new(Mutex::new(Vec::new()));
            let connections = Arc::new(AtomicUsize::new(0));
            let (seen, counted) = (Arc::clone(&heads), Arc::clone(&connections));
            thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { continue };
                    counted.fetch_add(1, Ordering::SeqCst);
                    let seen = Arc::clone(&seen);
                    thread::spawn(move || {
                        while let Ok((head, _body)) = read_request(&mut stream) {
                            seen.lock().expect("heads").push(head);
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
            Self {
                addr,
                heads,
                connections,
            }
        }

        fn heads(&self) -> Vec<String> {
            self.heads.lock().expect("heads").clone()
        }

        fn connections(&self) -> usize {
            self.connections.load(Ordering::SeqCst)
        }
    }

    /// Wait until `done`, failing past [`DEADLINE`].
    fn eventually(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + DEADLINE;
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            thread::sleep(Duration::from_millis(100));
        }
    }

    /// Collect `pipe`'s lines into `lines` on a background thread.
    fn drain(pipe: impl std::io::Read + Send + 'static, lines: Arc<Mutex<Vec<String>>>) {
        thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                lines.lock().expect("output").push(line);
            }
        });
    }

    /// The gateway binary serving `yaml` as a provider, killed on drop.
    struct Gateway {
        child: Child,
        output: Arc<Mutex<Vec<String>>>,
        listen: u16,
        pid: u32,
    }

    impl Gateway {
        fn start(work: &Path, tls: &Path, yaml: &str, mount: Option<&Mount>, env: &[(&str, &str)]) -> Self {
            let config = work.join("praxis.yaml");
            std::fs::write(&config, yaml).expect("praxis config");
            let ports = std::fs::read_to_string(work.join("ports")).expect("ports");
            let listen = ports
                .split_whitespace()
                .next()
                .and_then(|port| port.parse().ok())
                .expect("listen port");
            let mut command = Command::new(env!("CARGO_BIN_EXE_grid-gateway"));
            command
                .arg("--config")
                .arg(&config)
                .env("RUST_LOG", "info")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if let Some(mount) = mount {
                command.env("GRID_PROVIDER_CONFIG", mount.path("provider.json"));
            }
            command.env("GRID_TLS_DIR", tls);
            command.envs(env.iter().copied());
            let mut child = command.spawn().expect("spawn gateway");
            let output = Arc::new(Mutex::new(Vec::new()));
            drain(child.stdout.take().expect("stdout"), Arc::clone(&output));
            drain(child.stderr.take().expect("stderr"), Arc::clone(&output));
            let pid = child.id();
            Self {
                child,
                output,
                listen,
                pid,
            }
        }

        /// A chat request on a fresh connection presenting `leaf`, naming `candidate`.
        fn chat(&self, ca: &CaCert, leaf: &SiteCertOutput, candidate: &str) -> Option<u16> {
            Client::connect(ca, leaf, self.listen).ok()?.chat(candidate)
        }

        fn lines(&self, needle: &str) -> usize {
            self.output
                .lock()
                .expect("output")
                .iter()
                .filter(|line| line.contains(needle))
                .count()
        }

        fn assert_running(&mut self) {
            assert!(
                self.child.try_wait().expect("try_wait").is_none(),
                "the gateway never exited"
            );
            assert_eq!(self.child.id(), self.pid, "no restart");
        }
    }

    impl Drop for Gateway {
        fn drop(&mut self) {
            gateway_output_on_failure(&self.output);
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }

    /// A keep-alive mutual TLS client presenting `leaf`.
    struct Client {
        stream: StreamOwned<ClientConnection, TcpStream>,
    }

    impl Client {
        fn connect(ca: &CaCert, leaf: &SiteCertOutput, listen: u16) -> std::io::Result<Self> {
            let mut roots = RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(ca.cert_pem.as_bytes()) {
                roots.add(cert.expect("ca pem")).expect("root");
            }
            let chain = CertificateDer::pem_slice_iter(leaf.cert_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .expect("client chain");
            let key = PrivateKeyDer::from_pem_slice(leaf.key_pem.as_bytes()).expect("client key");
            let config = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .expect("versions")
                .with_root_certificates(roots)
                .with_client_auth_cert(chain, key)
                .expect("client config");
            let name = ServerName::try_from("site-a.grid.internal").expect("server name");
            let connection = ClientConnection::new(Arc::new(config), name).map_err(std::io::Error::other)?;
            let tcp = TcpStream::connect(("127.0.0.1", listen))?;
            tcp.set_read_timeout(Some(Duration::from_secs(5)))?;
            Ok(Self {
                stream: StreamOwned::new(connection, tcp),
            })
        }

        /// One chat request carrying every credential a caller might send, the status, or
        /// `None` when the connection is refused or cut.
        fn chat(&mut self, candidate: &str) -> Option<u16> {
            let body = r#"{"model":"llama"}"#;
            let request = format!(
                "POST /v1/chat/completions?api_key=s3cret&stream=true&token=t HTTP/1.1\r\nHost: site-a.grid.internal\r\nAuthorization: Bearer caller\r\nProxy-Authorization: Basic x\r\nCookie: session=1\r\nX-Api-Key: k\r\nX-Goog-Api-Key: g\r\nx-ai-routing-candidate: {candidate}\r\nx-ai-routing-revision: 7\r\nx-ai-routing-request-id: req-1\r\nX-Peer-Extra: 1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            self.stream.write_all(request.as_bytes()).ok()?;
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
            let mut rest = vec![0_u8; length];
            self.stream.read_exact(&mut rest).ok()?;
            head.split_whitespace().nth(1)?.parse().ok()
        }
    }
}
