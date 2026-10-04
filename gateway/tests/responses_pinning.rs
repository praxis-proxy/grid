//! Responses API ids carry the site that stored them, and requests naming one go back there.

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "tests; waits poll a deadline with thread::sleep"
)]
mod tests {
    use std::{
        io::{BufRead as _, BufReader, Read as _, Write as _},
        net::{TcpListener, TcpStream},
        path::PathBuf,
        process::{Child, Command, Stdio},
        sync::{Arc, Mutex},
        thread,
        time::{Duration, Instant},
    };

    /// How long the gateway may take to start listening.
    const DEADLINE: Duration = Duration::from_secs(20);

    fn free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .expect("bind")
            .local_addr()
            .expect("addr")
            .port()
    }

    /// What the backend received: the request line and the body.
    type Seen = Arc<Mutex<Vec<(String, String)>>>;

    /// A Responses API backend that stores nothing and names every response `resp_abc`.
    fn backend() -> (u16, Seen) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind backend");
        let port = listener.local_addr().expect("addr").port();
        let seen: Seen = Arc::default();
        let recorded = Arc::clone(&seen);
        thread::spawn(move || {
            for stream in listener.incoming().map_while(Result::ok) {
                let recorded = Arc::clone(&recorded);
                thread::spawn(move || serve(stream, &recorded));
            }
        });
        (port, seen)
    }

    /// Answer one connection's request, echoing any `previous_response_id` it sent.
    fn serve(stream: TcpStream, seen: &Seen) {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() || line.is_empty() {
            return;
        }
        let mut length = 0;
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).is_err() || header == "\r\n" || header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse().unwrap_or(0);
            }
        }
        let mut body = vec![0; length];
        let _read = reader.read_exact(&mut body);
        let body = String::from_utf8_lossy(&body).into_owned();
        let previous = body
            .split("\"previous_response_id\":\"")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .map_or_else(|| "null".to_owned(), |id| format!("\"{id}\""));
        seen.lock().expect("seen").push((line.trim().to_owned(), body));
        let id = if line.contains("/v1/conversations") {
            "conv_c1"
        } else {
            "resp_abc"
        };
        let answer = format!(r#"{{"id":"{id}","object":"response","previous_response_id":{previous}}}"#);
        let mut answering = reader.into_inner();
        let _written = write!(
            answering,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
            answer.len()
        );
    }

    /// Collect `pipe`'s lines into `lines` on a background thread.
    fn drain(pipe: impl std::io::Read + Send + 'static, lines: Arc<Mutex<Vec<String>>>) {
        thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                lines.lock().expect("output").push(line);
            }
        });
    }

    /// The gateway routing `llama` at site `local` to the backend.
    struct Gateway {
        child: Child,
        output: Arc<Mutex<Vec<String>>>,
        listen: u16,
    }

    impl Gateway {
        fn start(work: &std::path::Path, backend: u16) -> Self {
            std::fs::create_dir_all(work).expect("work dir");
            let (listen, admin) = (free_port(), free_port());
            let serving = work.join("serving-config.json");
            std::fs::write(
                &serving,
                r#"{"local_site":"local","window_secs":60,"load_window_ms":30000,"candidates":[{"kind":"inference_model","name":"llama","site":"local","cluster":"pool-local"}],"peers":[]}"#,
            )
            .expect("serving config");
            let config = work.join("praxis.yaml");
            std::fs::write(
                &config,
                format!(
                    "insecure_options:\n  allow_private_endpoints: true\nadmin:\n  address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: grid_site_route\n        model_header: X-Model\n      - filter: load_balancer\n        clusters:\n          - name: pool-local\n            endpoints: [\"127.0.0.1:{backend}\"]\n"
                ),
            )
            .expect("praxis config");
            let mut child = Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
                .arg("--config")
                .arg(&config)
                .env("GRID_SERVING_CONFIG", &serving)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn gateway");
            let output = Arc::new(Mutex::new(Vec::new()));
            drain(child.stdout.take().expect("stdout"), Arc::clone(&output));
            drain(child.stderr.take().expect("stderr"), Arc::clone(&output));
            let mut gateway = Self { child, output, listen };
            let deadline = Instant::now() + DEADLINE;
            while TcpStream::connect(("127.0.0.1", listen)).is_err() {
                assert!(
                    Instant::now() < deadline && gateway.child.try_wait().ok().flatten().is_none(),
                    "gateway never listened; output:\n{}",
                    gateway.output.lock().expect("output").join("\n")
                );
                thread::sleep(Duration::from_millis(50));
            }
            gateway
        }

        /// The status and body of `method path` with `body`.
        fn send(&self, method: &str, path: &str, body: &str) -> (u16, String) {
            let mut stream = TcpStream::connect(("127.0.0.1", self.listen)).expect("connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("read timeout");
            write!(
                stream,
                "{method} {path} HTTP/1.1\r\nHost: grid.example\r\nX-Model: llama\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .expect("send");
            let mut response = String::new();
            let _read = stream.read_to_string(&mut response);
            let status = response
                .split_whitespace()
                .nth(1)
                .and_then(|code| code.parse().ok())
                .unwrap_or_else(|| panic!("no status line in {response:?}"));
            (status, response)
        }
    }

    impl Drop for Gateway {
        fn drop(&mut self) {
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
        }
    }

    #[test]
    fn response_ids_name_their_site_and_follow_ups_go_back_to_it() {
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("pinning-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&work);
        let (port, seen) = backend();
        let gateway = Gateway::start(&work, port);

        let (status, created) = gateway.send("POST", "/v1/responses", r#"{"model":"llama","input":"hi"}"#);
        assert_eq!(status, 200, "{created}");
        assert!(
            created.contains(r#""id":"resp_local.abc""#),
            "the id names its site: {created}"
        );

        let follow_up = r#"{"model":"llama","input":"and then?","previous_response_id":"resp_local.abc"}"#;
        let (continued_status, continued) = gateway.send("POST", "/v1/responses", follow_up);
        assert_eq!(continued_status, 200, "{continued}");
        assert!(
            continued.contains(r#""previous_response_id":"resp_local.abc""#),
            "{continued}"
        );
        let sent = seen.lock().expect("seen")[1].1.clone();
        assert!(
            sent.contains(r#""previous_response_id":"resp_abc""#),
            "the site sees its own id: {sent}"
        );

        let (fetched_status, fetched) = gateway.send("GET", "/v1/responses/resp_local.abc", "");
        assert_eq!(fetched_status, 200, "{fetched}");
        assert_eq!(seen.lock().expect("seen")[2].0, "GET /v1/responses/resp_abc HTTP/1.1");
        assert!(fetched.contains(r#""id":"resp_local.abc""#), "{fetched}");

        let (cancelled, _) = gateway.send("POST", "/v1/responses/resp_local.abc/cancel", "");
        assert_eq!(cancelled, 200);
        assert_eq!(
            seen.lock().expect("seen")[3].0,
            "POST /v1/responses/resp_abc/cancel HTTP/1.1"
        );

        let (created_status, conversation) = gateway.send("POST", "/v1/conversations", "{}");
        assert_eq!(created_status, 200, "{conversation}");
        assert!(conversation.contains(r#""id":"conv_local.c1""#), "{conversation}");
        let (items, _) = gateway.send("POST", "/v1/conversations/conv_local.c1/items", "{}");
        assert_eq!(items, 200);
        assert_eq!(
            seen.lock().expect("seen")[5].0,
            "POST /v1/conversations/conv_c1/items HTTP/1.1"
        );

        for unknown in [
            "/v1/responses/resp_abc",
            "/v1/responses/resp_mars.abc",
            "/v1/conversations/conv_c1",
        ] {
            assert_eq!(gateway.send("GET", unknown, "").0, 404, "{unknown}");
        }
        assert_eq!(
            seen.lock().expect("seen").len(),
            6,
            "the gateway answered the unknown ids itself"
        );
    }
}
