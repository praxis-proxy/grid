//! A request the grid router cannot route is answered by the router and logged below WARN.

#[path = "support/logs.rs"]
mod logs;
#[path = "support/ports.rs"]
mod ports;
#[path = "support/http.rs"]
mod test_http;

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::arithmetic_side_effects,
    clippy::too_many_lines,
    reason = "tests; waits poll a deadline with thread::sleep"
)]
mod tests {
    use std::{
        io::{BufRead as _, BufReader, Read as _, Write as _},
        net::TcpStream,
        path::PathBuf,
        process::{Child, Command, Stdio},
        sync::{Arc, Mutex},
        thread,
        time::{Duration, Instant},
    };

    use super::ports::{bound_listener, free_port};

    /// How long the gateway may take to start listening.
    const DEADLINE: Duration = Duration::from_secs(20);

    /// Collect `pipe`'s lines into `lines` on a background thread.
    fn drain(pipe: impl std::io::Read + Send + 'static, lines: Arc<Mutex<Vec<String>>>) {
        thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                lines.lock().expect("output").push(line);
            }
        });
    }

    /// The gateway with its output collected in the background.
    struct Gateway {
        child: Child,
        output: Arc<Mutex<Vec<String>>>,
        listen: u16,
        admin: u16,
    }

    impl Gateway {
        /// Route `llama` to a local cluster whose only endpoint refuses connections.
        fn start(work: &std::path::Path) -> Self {
            Self::start_at(work, &format!("127.0.0.1:{}", free_port()))
        }

        /// Route both configured models to `endpoint` for counter attribution tests.
        fn start_at(work: &std::path::Path, endpoint: &str) -> Self {
            std::fs::create_dir_all(work).expect("work dir");
            let (listen, admin) = (free_port(), free_port());
            let serving = work.join("serving-config.json");
            std::fs::write(
                &serving,
                r#"{"local_site":"local","window_secs":60,"load_window_ms":30000,"candidates":[{"kind":"inference_model","name":"llama","site":"local","cluster":"pool-local"},{"kind":"inference_model","name":"qwen","site":"local","cluster":"pool-local"}],"peers":[]}"#,
            )
            .expect("serving config");
            let config = work.join("praxis.yaml");
            std::fs::write(
                &config,
                format!(
                    "insecure_options:\n  allow_private_endpoints: true\nadmin:\n  metrics_address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: grid_site_route\n        model_header: X-Model\n      - filter: load_balancer\n        clusters:\n          - name: pool-local\n            endpoints: [\"{endpoint}\"]\n"
                ),
            )
            .expect("praxis config");
            let mut child = Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
                .arg("--config")
                .arg(&config)
                .env("GRID_SERVING_CONFIG", &serving)
                .env("RUST_LOG", "debug")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn gateway");
            let output = Arc::new(Mutex::new(Vec::new()));
            drain(child.stdout.take().expect("stdout"), Arc::clone(&output));
            drain(child.stderr.take().expect("stderr"), Arc::clone(&output));
            let mut gateway = Self {
                child,
                output,
                listen,
                admin,
            };
            let deadline = Instant::now() + DEADLINE;
            while TcpStream::connect(("127.0.0.1", listen)).is_err() {
                assert!(
                    Instant::now() < deadline && gateway.child.try_wait().ok().flatten().is_none(),
                    "gateway never listened; output:\n{}",
                    gateway.output()
                );
                thread::sleep(Duration::from_millis(50));
            }
            gateway
        }

        fn output(&self) -> String {
            self.output.lock().expect("output").join("\n")
        }

        fn lines(&self) -> usize {
            self.output.lock().expect("output").len()
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

        /// The status code of a GET for `path` with `headers`.
        fn status(&self, path: &str, headers: &str) -> u16 {
            let mut stream = TcpStream::connect(("127.0.0.1", self.listen)).expect("connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("read timeout");
            write!(
                stream,
                "GET {path} HTTP/1.1\r\nHost: grid.example\r\n{headers}Connection: close\r\n\r\n"
            )
            .expect("send");
            let mut response = String::new();
            let _read = stream.read_to_string(&mut response);
            response
                .split_whitespace()
                .nth(1)
                .and_then(|code| code.parse().ok())
                .unwrap_or_else(|| panic!("no status line in {response:?}"))
        }
    }

    impl Drop for Gateway {
        fn drop(&mut self) {
            super::logs::gateway_output_on_failure(&self.output);
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }

    /// Whether a log line is at WARN or ERROR.
    fn loud(line: &str) -> bool {
        line.split_whitespace()
            .take(3)
            .any(|word| word.contains("WARN") || word.contains("ERROR"))
    }

    #[test]
    fn an_unrouted_request_is_answered_by_the_router_below_warn() {
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("unrouted-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&work);
        let gateway = Gateway::start(&work);
        let before = gateway.lines();

        assert_eq!(gateway.status("/health", ""), 400, "no model in the request");
        assert_eq!(
            gateway.status("/v1/models", "X-Model: granite\r\n"),
            404,
            "a model with no admitted candidate"
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        while !gateway.output().contains("grid_site_route: no model in the request") {
            assert!(
                Instant::now() < deadline,
                "no debug line; output:\n{}",
                gateway.output()
            );
            thread::sleep(Duration::from_millis(50));
        }
        let metrics = gateway.metrics();
        // Unmeasured, so the order ranks it last with an infinite score.
        let score = "grid_route_site_score{site=\"local\",cluster=\"pool-local\"} inf";
        assert!(metrics.contains(score), "no {score:?} in:\n{metrics}");
        // No model and an unknown model both count as bad_request.
        let refused = "grid_route_decisions_total{site=\"\",cluster=\"\",model=\"\",reason=\"bad_request\"} 2";
        assert!(metrics.contains(refused), "no {refused:?} in:\n{metrics}");
        // Labels come from the serving config and a closed set, never from the request.
        for series in metrics
            .lines()
            .filter(|line| line.starts_with("grid_route_decisions_total{"))
        {
            let label = |name: &str| {
                series
                    .split_once(&format!("{name}=\""))
                    .and_then(|(_, rest)| rest.split_once('"'))
                    .map_or_else(|| panic!("no {name} in {series}"), |(value, _)| value.to_owned())
            };
            assert!(
                ["", "local"].contains(&label("site").as_str()),
                "request-derived site: {series}"
            );
            assert!(
                ["", "llama", "qwen"].contains(&label("model").as_str()),
                "model labels must come from configuration, never an unknown request: {series}"
            );
            assert!(
                [
                    "routed",
                    "fallback",
                    "not_ready",
                    "no_route",
                    "bad_request",
                    "shed",
                    "fenced"
                ]
                .contains(&label("reason").as_str()),
                "reason outside the closed set: {series}"
            );
        }
        let output = gateway.output.lock().expect("output").clone();
        let loud_lines: Vec<&String> = output.iter().skip(before).filter(|line| loud(line)).collect();
        assert!(
            loud_lines.is_empty(),
            "an unrouted request logged at WARN or ERROR: {loud_lines:#?}"
        );

        drop(gateway);
        let _cleaned = std::fs::remove_dir_all(&work);
    }
    #[test]
    fn traffic_for_two_models_on_one_cluster_is_counted_without_a_join() {
        let backend = bound_listener();
        let endpoint = backend.local_addr().unwrap_or_else(|_| std::process::abort());
        let server = thread::spawn(move || {
            for stream in backend.incoming().take(3) {
                let mut stream = stream.unwrap_or_else(|_| std::process::abort());
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap_or_else(|_| std::process::abort());
                let _request = super::test_http::read_request(&mut stream).unwrap_or_else(|_| std::process::abort());
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                    .unwrap_or_else(|_| std::process::abort());
            }
        });
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("model-decisions-{}", std::process::id()));
        let gateway = Gateway::start_at(&work, &endpoint.to_string());
        for model in ["llama", "llama", "qwen"] {
            assert_eq!(
                gateway.status("/v1/models", &format!("X-Model: {model}\r\n")),
                200,
                "the configured model must reach the shared backend"
            );
        }
        let metrics = gateway.metrics();
        for (model, count) in [("llama", 2), ("qwen", 1)] {
            let series = format!(
                "grid_route_decisions_total{{site=\"local\",cluster=\"pool-local\",model=\"{model}\",reason=\"routed\"}} {count}"
            );
            assert!(
                metrics.contains(&series),
                "model traffic must be counted directly, without a candidate-info join: {metrics}"
            );
        }
        drop(gateway);
        server.join().unwrap_or_else(|_| std::process::abort());
        let _cleaned = std::fs::remove_dir_all(&work);
    }
}
