//! Startup must install a log subscriber, or reload results and errors are dropped.

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        io::{self, BufRead as _, BufReader, Read},
        net::TcpListener,
        path::PathBuf,
        process::{Child, Command, Stdio},
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    type TestResult = Result<(), Box<dyn Error>>;

    /// Must match `STARTUP_MESSAGE` in `src/main.rs`.
    const STARTUP_MESSAGE: &str = "starting grid-gateway";

    /// Bound on how long either test waits for the child.
    const TIMEOUT: Duration = Duration::from_secs(30);

    /// Write a minimal config, named `name`, listening on a free loopback port.
    fn write_config(name: &str) -> io::Result<PathBuf> {
        let port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
        let yaml = format!(
            "listeners:\n  - name: default\n    address: \"127.0.0.1:{port}\"\n    filter_chains: [main]\n\
             filter_chains:\n  - name: main\n    filters:\n      - filter: static_response\n        status: 200\n"
        );
        std::fs::write(&path, yaml)?;
        Ok(path)
    }

    /// Start the gateway on `config`, with `serving_config` as `GRID_SERVING_CONFIG` when set.
    fn spawn(
        config: PathBuf,
        serving_config: Option<&str>,
        otlp_headers: Option<&str>,
    ) -> io::Result<(Child, mpsc::Receiver<String>)> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_grid-gateway"));
        command
            .arg("--config")
            .arg(config)
            .env("RUST_LOG", "info")
            .env_remove("OTEL_EXPORTER_OTLP_PROTOCOL")
            .env_remove("OTEL_EXPORTER_OTLP_TRACES_HEADERS")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match serving_config {
            Some(path) => command.env("GRID_SERVING_CONFIG", path),
            None => command.env_remove("GRID_SERVING_CONFIG"),
        };
        match otlp_headers {
            Some(headers) => command.env("OTEL_EXPORTER_OTLP_HEADERS", headers),
            None => command.env_remove("OTEL_EXPORTER_OTLP_HEADERS"),
        };
        let mut child = command.spawn()?;
        let (lines_tx, lines_rx) = mpsc::channel();
        let missing = || io::Error::other("child output not piped");
        forward(child.stdout.take().ok_or_else(missing)?, lines_tx.clone());
        forward(child.stderr.take().ok_or_else(missing)?, lines_tx);
        Ok((child, lines_rx))
    }

    /// Send each line from `reader` to `lines` on its own thread.
    fn forward(reader: impl Read + Send + 'static, lines: mpsc::Sender<String>) {
        thread::spawn(move || {
            for line in BufReader::new(reader).lines().map_while(Result::ok) {
                drop(lines.send(line));
            }
        });
    }

    #[test]
    fn startup_emits_a_log_line() -> TestResult {
        let (mut child, lines) = spawn(write_config("startup-log.yaml")?, None, None)?;
        let mut seen = Vec::new();
        let found = loop {
            match lines.recv_timeout(TIMEOUT) {
                Ok(line) if line.contains(STARTUP_MESSAGE) => break true,
                Ok(line) => seen.push(line),
                Err(_) => break false,
            }
        };
        child.kill()?;
        child.wait()?;
        assert!(found, "no startup log line within {TIMEOUT:?}; output: {seen:#?}");
        Ok(())
    }

    #[test]
    fn fatal_startup_flushes_its_logs() -> TestResult {
        let (mut child, lines) = spawn(write_config("fatal-log.yaml")?, Some("/nonexistent/serving.json"), None)?;
        // Both streams close on exit, which disconnects the channel.
        let deadline = Instant::now() + TIMEOUT;
        let mut output = Vec::new();
        loop {
            match lines.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(line) => output.push(line),
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    child.kill()?;
                    child.wait()?;
                    return Err(format!("gateway still running after {TIMEOUT:?}").into());
                },
            }
        }
        let status = child.wait()?;
        assert!(!status.success(), "a missing serving config must fail startup");
        assert!(
            output.iter().any(|line| line.contains(STARTUP_MESSAGE)),
            "the startup line was not flushed before exit: {output:#?}"
        );
        assert!(
            output.iter().any(|line| line.contains("/nonexistent/serving.json")),
            "the serving config error was not reported: {output:#?}"
        );
        Ok(())
    }

    #[test]
    fn server_pipeline_error_is_reported_before_tracing_guard_flush() -> TestResult {
        let path = write_config("invalid-filter.yaml")?;
        let yaml = std::fs::read_to_string(&path)?
            .replace("filter: static_response", "filter: not_registered_for_startup_test");
        std::fs::write(&path, yaml)?;
        let (mut child, lines) = spawn(path, None, None)?;

        // Both streams close on exit, which disconnects the channel.
        let deadline = Instant::now() + TIMEOUT;
        let mut output = Vec::new();
        loop {
            match lines.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(line) => output.push(line),
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    child.kill()?;
                    child.wait()?;
                    return Err(format!("gateway still running after {TIMEOUT:?}").into());
                },
            }
        }
        let status = child.wait()?;
        assert!(!status.success(), "an invalid pipeline filter must fail startup");
        assert!(
            output.iter().any(|line| line.contains("fatal error; exiting")),
            "the server startup error was not logged before the tracing guard dropped: {output:#?}"
        );
        Ok(())
    }

    #[test]
    fn credentialed_https_grpc_exporter_starts() -> TestResult {
        let path = write_config("credentialed-https-grpc.yaml")?;
        let mut yaml = std::fs::read_to_string(&path)?;
        yaml.push_str("telemetry:\n  otlp_endpoint: https://127.0.0.1:4317\n");
        std::fs::write(&path, yaml)?;

        let (mut child, lines_rx) = spawn(path, None, Some("Authorization=test-only"))?;

        let deadline = Instant::now() + TIMEOUT;
        let mut output = Vec::new();
        let started = loop {
            match lines_rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(line) if line.contains(STARTUP_MESSAGE) => break true,
                Ok(line) => output.push(line),
                Err(_) => break false,
            }
        };
        drop(child.kill());
        child.wait()?;
        assert!(started, "credentialed HTTPS/gRPC exporter did not start: {output:#?}");
        Ok(())
    }
}
