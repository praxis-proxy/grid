//! The operator's generated consumer config must build filter pipelines in this binary,
//! so a praxis-ai filter schema the render emits and the pinned crate rejects fails here.
#[path = "support/ports.rs"]
mod ports;

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        io::{self, BufRead as _, BufReader, Read},
        net::TcpStream,
        path::PathBuf,
        process::{Child, Command, Stdio},
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    use super::ports::free_port;

    type TestResult = Result<(), Box<dyn Error>>;

    /// Bound on how long the test waits for the listener.
    const TIMEOUT: Duration = Duration::from_secs(30);

    /// `credential_inject` as `consumer_config.rs` renders it for a bearer-token provider:
    /// projected mode with no inline credentials. A static terminal stands in for the
    /// `load_balancer`, whose private-endpoint gate needs an RFC1918 resolver.
    fn write_config(port: u16, mount_base: &str) -> io::Result<PathBuf> {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("generated-consumer-config.yaml");
        let yaml = format!(
            r#"admin:
  metrics_address: "127.0.0.1:0"
listeners:
  - name: public
    address: "127.0.0.1:{port}"
    filter_chains: [consumer-chain]
filter_chains:
  - name: consumer-chain
    filters:
      - filter: json_body_field
        field: model
        header: X-Model
      - filter: credential_inject
        credentials: []
        projected_credential_mount_base: "{mount_base}"
      - filter: static_response
        status: 200
"#
        );
        std::fs::write(&path, yaml)?;
        Ok(path)
    }

    fn spawn(config: PathBuf) -> io::Result<(Child, mpsc::Receiver<String>)> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
            .arg("--config")
            .arg(config)
            .env("RUST_LOG", "info")
            .env_remove("GRID_SERVING_CONFIG")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let (lines_tx, lines_rx) = mpsc::channel();
        let missing = || io::Error::other("child output not piped");
        forward(child.stdout.take().ok_or_else(missing)?, lines_tx.clone());
        forward(child.stderr.take().ok_or_else(missing)?, lines_tx);
        Ok((child, lines_rx))
    }

    fn forward(reader: impl Read + Send + 'static, lines: mpsc::Sender<String>) {
        thread::spawn(move || {
            for line in BufReader::new(reader).lines().map_while(Result::ok) {
                drop(lines.send(line));
            }
        });
    }

    /// Whether the child opens `port` before it exits or the deadline passes, with its output.
    fn wait_for_listener(
        child: &mut Child,
        lines: &mpsc::Receiver<String>,
        port: u16,
    ) -> io::Result<(bool, Vec<String>)> {
        let started = Instant::now();
        let mut output = Vec::new();
        loop {
            if TcpStream::connect_timeout(&([127, 0, 0, 1], port).into(), Duration::from_millis(200)).is_ok() {
                return Ok((true, output));
            }
            if let Some(status) = child.try_wait()? {
                output.push(format!("gateway exited: {status}"));
                return Ok((false, output));
            }
            if started.elapsed() >= TIMEOUT {
                return Ok((false, output));
            }
            // Drains output and paces the poll; a closed channel means the child is exiting.
            if let Ok(line) = lines.recv_timeout(Duration::from_millis(100)) {
                output.push(line);
            }
        }
    }

    #[test]
    fn the_generated_consumer_filters_build() -> TestResult {
        let port = free_port();
        let mount_base = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("grid-credentials");
        std::fs::create_dir_all(&mount_base)?;
        let (mut child, lines) = spawn(write_config(port, &mount_base.display().to_string())?)?;

        // The listener only opens once every filter in the chain has been built.
        let (listening, mut output) = wait_for_listener(&mut child, &lines, port)?;
        drop(child.kill());
        child.wait()?;
        while let Ok(line) = lines.try_recv() {
            output.push(line);
        }
        assert!(
            listening,
            "the generated consumer config did not start a listener: {output:#?}"
        );
        Ok(())
    }
}
