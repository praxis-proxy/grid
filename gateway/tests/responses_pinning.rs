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
            let key = work.join("tag.key");
            std::fs::write(&key, [7_u8; 32]).expect("tag key");
            let serving = work.join("serving-config.json");
            std::fs::write(
                &serving,
                r#"{"local_site":"local","window_secs":60,"load_window_ms":30000,"candidates":[{"kind":"inference_model","name":"llama","site":"local","cluster":"pool-local"}],"peers":[]}"#,
            )
            .expect("serving config");
            let config = work.join("praxis.yaml");
            let key = key.display().to_string();
            std::fs::write(
                &config,
                format!(
                    "insecure_options:\n  allow_private_endpoints: true\nadmin:\n  address: \"127.0.0.1:{admin}\"\nlisteners:\n  - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: grid_site_route\n        model_header: X-Model\n        prefix_affinity: {{tag_key_path: {key:?}}}\n      - filter: load_balancer\n        clusters:\n          - name: pool-local\n            endpoints: [\"127.0.0.1:{backend}\"]\n"
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

    /// The first id in `body` starting with `prefix`.
    fn id_in(body: &str, prefix: &str) -> String {
        let tail = body
            .split(prefix)
            .nth(1)
            .unwrap_or_else(|| panic!("no {prefix} id in {body}"));
        format!("{prefix}{}", tail.split('"').next().unwrap_or_default())
    }

    #[test]
    fn response_ids_name_their_site_and_follow_ups_go_back_to_it() {
        let work = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("pinning-{}", std::process::id()));
        let _cleared = std::fs::remove_dir_all(&work);
        let (port, seen) = backend();
        let gateway = Gateway::start(&work, port);

        let (status, created) = gateway.send("POST", "/v1/responses", r#"{"model":"llama","input":"hi"}"#);
        assert_eq!(status, 200, "{created}");
        let id = id_in(&created, "resp_local.");
        assert!(
            id.ends_with(".abc") && id.split('.').count() == 4,
            "site, cluster, mac, id: {id}"
        );

        let follow_up = format!(r#"{{"model":"llama","input":"and then?","previous_response_id":"{id}"}}"#);
        let (continued_status, continued) = gateway.send("POST", "/v1/responses", &follow_up);
        assert_eq!(continued_status, 200, "{continued}");
        assert!(
            continued.contains(&format!(r#""previous_response_id":"{id}""#)),
            "{continued}"
        );
        let sent = seen.lock().expect("seen")[1].1.clone();
        assert!(
            sent.contains(r#""previous_response_id":"resp_abc""#),
            "the site sees its own id: {sent}"
        );

        // Past the inline limit the body is read on the blocking pool, and still comes back stripped.
        let large = format!(
            r#"{{"model":"llama","input":"{}","previous_response_id":"{id}"}}"#,
            "words ".repeat(50_000)
        );
        let (large_status, large_answer) = gateway.send("POST", "/v1/responses", &large);
        assert_eq!(large_status, 200, "{large_answer}");
        let large_sent = seen.lock().expect("seen").remove(2).1;
        assert!(
            large_sent.ends_with(r#""previous_response_id":"resp_abc"}"#),
            "the large body is stripped too"
        );

        let (fetched_status, fetched) = gateway.send("GET", &format!("/v1/responses/{id}"), "");
        assert_eq!(fetched_status, 200, "{fetched}");
        assert_eq!(seen.lock().expect("seen")[2].0, "GET /v1/responses/resp_abc HTTP/1.1");
        assert!(fetched.contains(&format!(r#""id":"{id}""#)), "{fetched}");

        let (cancelled, _) = gateway.send("POST", &format!("/v1/responses/{id}/cancel"), "");
        assert_eq!(cancelled, 200);
        assert_eq!(
            seen.lock().expect("seen")[3].0,
            "POST /v1/responses/resp_abc/cancel HTTP/1.1"
        );

        let (created_status, conversation) = gateway.send("POST", "/v1/conversations", "{}");
        assert_eq!(created_status, 200, "{conversation}");
        let conversation_id = id_in(&conversation, "conv_local.");
        let (items, _) = gateway.send("POST", &format!("/v1/conversations/{conversation_id}/items"), "{}");
        assert_eq!(items, 200);
        assert_eq!(
            seen.lock().expect("seen")[5].0,
            "POST /v1/conversations/conv_c1/items HTTP/1.1"
        );

        // A tag the gateway did not make, or one aimed at another site, is refused here.
        let parts: Vec<&str> = id.split('.').collect();
        let forged_mac = format!("{}.{}.{}.{}", parts[0], parts[1], "0".repeat(16), parts[3]);
        let steered = id.replacen("resp_local.", "resp_mars.", 1);
        for unknown in [
            "/v1/responses/resp_abc",
            "/v1/responses/resp_mars.abc",
            "/v1/conversations/conv_c1",
            &format!("/v1/responses/{forged_mac}"),
            &format!("/v1/responses/{steered}"),
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
