//! `grid-gateway`: the grid data-plane operand.
//!
//! A Praxis gateway assembled in the grid repo, deployed and configured by the
//! grid operator. Operator is the control plane. This binary is the operand it
//! manages.
//!
//! It links the Praxis library, registers the routing filters over the builtin
//! registry, and runs the Praxis server on the operator-supplied config. When
//! the operator sets `GRID_SERVING_CONFIG`, it also starts the cross-site pollers
//! and registers `grid_site_route` over the snapshot they keep fresh. This crate
//! is its own Cargo workspace so Praxis resolves independently of the operator's
//! Kubernetes client stack. See `deploy/gateway/Containerfile` and the
//! `gateway-image` make target.

use std::{ffi::OsStr, process::ExitCode};

use praxis_core::config::{Config, ConfigFile, DEFAULT_CONFIG};
use tracing::info;

mod metrics_listener;

/// Log line emitted once tracing is up; the startup test waits for it.
const STARTUP_MESSAGE: &str = "starting grid-gateway";
/// OTel-standard environment variable used as the OTLP endpoint fallback.
const OTLP_ENDPOINT_ENV_VAR: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// OTel-standard environment variable containing exporter headers.
const OTLP_HEADERS_ENV_VAR: &str = "OTEL_EXPORTER_OTLP_HEADERS";
/// Signal-specific headers are merged into the exporter after Praxis config.
const OTLP_TRACES_HEADERS_ENV_VAR: &str = "OTEL_EXPORTER_OTLP_TRACES_HEADERS";
/// Praxis currently selects the exporter protocol from this variable.
const OTLP_PROTOCOL_ENV_VAR: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";

/// The one startup line, with the build it came from. The startup test waits for it.
fn log_startup() {
    let build = version::get();
    info!(
        version = %build,
        commit = build.git_commit,
        tree = build.git_tree_state,
        built = build.build_date,
        rustc = build.rustc_version,
        platform = build.platform,
        "{STARTUP_MESSAGE}"
    );
}

/// The config file the operator wrote, and the config resolved from it.
///
/// The path is `--config <path>` or the positional argument, else the default search
/// path. Read once so the reload watcher baselines on the bytes that run.
fn load_config() -> (Option<ConfigFile>, Config) {
    let explicit = config_arg(std::env::args().skip(1)).unwrap_or_else(|err| praxis::fatal(&err));
    let config_file = praxis::resolve_config_path(explicit.as_deref())
        .as_deref()
        .map(ConfigFile::read)
        .transpose()
        .unwrap_or_else(|err| praxis::fatal(&err));
    let config = praxis::with_bootstrap_logging(|| Config::from_config_file_or(config_file.as_ref(), DEFAULT_CONFIG))
        .unwrap_or_else(|err| praxis::fatal(&err));
    (config_file, config)
}

/// Everything that runs before a config is read: answer `--version`, else install the
/// crypto provider anything building a TLS config needs.
///
/// `Some(exit)` means the process is done. Writes through `io::Write` because the lint
/// set denies `println!`.
fn preflight() -> Option<ExitCode> {
    use std::io::Write as _;

    if std::env::args().skip(1).any(|arg| arg == "--version" || arg == "-V") {
        let written = writeln!(std::io::stdout().lock(), "grid-gateway {}", version::get());
        return Some(if written.is_ok() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        });
    }
    praxis::install_crypto_provider();
    None
}

fn main() -> ExitCode {
    if let Some(exit) = preflight() {
        return exit;
    }

    let (config_file, config) = load_config();

    validate_otlp_endpoint_transport(&config).unwrap_or_else(|err| praxis::fatal(&err));

    // Without a subscriber every log line, including reload results, is dropped.
    let tracing_guard = praxis::init_tracing(&config).unwrap_or_else(|err| praxis::fatal(&err));
    let log_level = Some(tracing_guard.log_level_state());
    let log_output = config.runtime.logging.output;
    log_startup();

    // Before grid routing starts, so its metrics record into the installed recorder.
    if let Err(err) = start_metrics_listener(&config) {
        return praxis::report_fatal(&err, log_output);
    }

    let mut registry = praxis_filter::FilterRegistry::with_builtins();
    praxis_ai_filters::register_ai_filters(&mut registry, None);

    // Grid cross-site routing is wired when the operator provides a serving
    // config. spawn_grid_routing starts one poller per peer and returns the
    // runtime holding their handles. grid_site_route registers over the snapshot
    // the pollers refresh. Dropping the runtime stops the pollers, so it is
    // bound until the server returns.
    let grid_runtime = match std::env::var("GRID_SERVING_CONFIG")
        .ok()
        .map(|path| start_grid_routing(&path, &mut registry))
    {
        Some(Err(err)) => return praxis::report_fatal(&err, log_output),
        Some(Ok(runtime)) => Some(runtime),
        None => None,
    };

    // Use the returning server path so both the routing runtime and tracing
    // provider can shut down cleanly after the listeners stop.
    let result = praxis::try_run_server_with_registry(config, registry, config_file, log_level);
    drop(grid_runtime);
    let exit_code = result.map_or_else(|err| praxis::report_fatal(&err, log_output), |()| ExitCode::SUCCESS);
    // The Praxis guard shuts down the OTLP provider and flushes queued spans.
    drop(tracing_guard);
    exit_code
}

/// Refuse credentialed export without explicit TLS and unsupported HTTP export.
///
/// The OTLP SDK can merge generic or trace-specific environment headers into
/// programmatic headers. Treat every source as potentially credential-bearing.
fn validate_otlp_endpoint_transport(config: &Config) -> Result<(), &'static str> {
    let environment_endpoint = std::env::var(OTLP_ENDPOINT_ENV_VAR).ok();
    let generic_headers = std::env::var_os(OTLP_HEADERS_ENV_VAR);
    let traces_headers = std::env::var_os(OTLP_TRACES_HEADERS_ENV_VAR);
    let headers_present = otlp_headers_present(
        config
            .telemetry
            .otlp_headers
            .as_ref()
            .is_some_and(|headers| !headers.is_empty()),
        generic_headers.as_deref(),
        traces_headers.as_deref(),
    );
    let protocol = std::env::var(OTLP_PROTOCOL_ENV_VAR).ok();

    validate_otlp_endpoint_transport_values(
        config.telemetry.otlp_endpoint.as_deref(),
        environment_endpoint.as_deref(),
        headers_present,
        protocol.as_deref(),
    )
}

/// The SDK may append either environment source even when config has an empty map.
fn otlp_headers_present(configured: bool, generic: Option<&OsStr>, traces: Option<&OsStr>) -> bool {
    configured || generic.is_some_and(|value| !value.is_empty()) || traces.is_some_and(|value| !value.is_empty())
}

/// Validate all header sources against the endpoint Praxis will pass to OTLP.
fn validate_otlp_endpoint_transport_values(
    configured_endpoint: Option<&str>,
    environment_endpoint: Option<&str>,
    headers_present: bool,
    protocol: Option<&str>,
) -> Result<(), &'static str> {
    let endpoint = configured_endpoint.or_else(|| environment_endpoint.filter(|value| !value.trim().is_empty()));
    if endpoint.is_some() && protocol.is_some_and(|value| value.trim() == "http/protobuf") {
        return Err("OTLP HTTP/protobuf is unsupported by this gateway build; use OTLP/gRPC");
    }
    let endpoint_uses_https = endpoint
        .and_then(|value| value.trim().split_once("://"))
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("https"));

    if headers_present && !endpoint_uses_https {
        return Err(
            "OTLP exporter headers require an explicit HTTPS endpoint; refusing to send OTLP credentials without TLS",
        );
    }

    Ok(())
}

/// Start the opt-in metrics listener when its env vars are set.
///
/// # Errors
///
/// Returns the settings, port, cert, or bind error.
fn start_metrics_listener(config: &Config) -> Result<(), String> {
    let listener = metrics_listener::MetricsListener::from_env(|name| std::env::var(name).ok())?;
    // Praxis installs the recorder only when the admin server starts, after grid routing
    // publishes its first snapshot, so install it now when anything will serve metrics.
    if listener.is_some() || config.admin.address.is_some() {
        praxis_protocol::http::pingora::metrics::install_prometheus_recorder();
    }
    let Some(listener) = listener else {
        return Ok(());
    };
    listener.check_ports(config)?;
    // The thread serves for the life of the process.
    drop(listener.spawn()?);
    Ok(())
}

/// Start the cross-site pollers and register `grid_site_route` over their snapshot.
///
/// # Errors
///
/// Returns the error from loading the serving config, starting the pollers, or
/// registering the filters.
fn start_grid_routing(
    path: &str,
    registry: &mut praxis_filter::FilterRegistry,
) -> Result<ai_grid_filters::GridRuntime, praxis_filter::FilterError> {
    let config = ai_grid_filters::load_serving_config(path)?;
    let mut runtime = ai_grid_filters::spawn_grid_routing(&config)?;
    ai_grid_filters::register_grid_filters(
        registry,
        runtime.snapshot(),
        runtime.affinity(),
        runtime.health(),
        runtime.tuning(),
    )?;
    // The operator rewrites the file on membership and topology changes.
    runtime
        .watch(path, SERVING_RELOAD_INTERVAL)
        .map_err(|err| -> praxis_filter::FilterError { format!("grid: watching {path}: {err}").into() })?;
    Ok(runtime)
}

/// How often the grid serving config file is re-read.
const SERVING_RELOAD_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Usage line for a malformed command line.
const USAGE: &str = "usage: grid-gateway [--config <path> | -c <path> | <path>]";

/// Config path from the arguments after the program name.
///
/// # Errors
///
/// Returns the usage line for a missing flag value, an unknown flag, or extra
/// arguments.
fn config_arg<I: IntoIterator<Item = String>>(args: I) -> Result<Option<String>, String> {
    let mut args = args.into_iter();
    let path = match args.next() {
        None => return Ok(None),
        Some(flag) if flag == "--config" || flag == "-c" => args.next().filter(|path| !path.starts_with('-')),
        Some(arg) => match arg.strip_prefix("--config=") {
            Some(path) => Some(path.to_owned()),
            None if !arg.starts_with('-') => Some(arg),
            None => None,
        },
    };
    match (path, args.next()) {
        (Some(path), None) if !path.is_empty() => Ok(Some(path)),
        _ => Err(USAGE.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::{USAGE, config_arg, otlp_headers_present, validate_otlp_endpoint_transport_values};

    fn parse(args: &[&str]) -> Result<Option<String>, String> {
        config_arg(args.iter().map(|arg| (*arg).to_owned()))
    }

    #[test]
    fn accepts_every_config_form() {
        for args in [
            &["/etc/grid/gateway.yaml"][..],
            &["--config", "/etc/grid/gateway.yaml"],
            &["-c", "/etc/grid/gateway.yaml"],
            &["--config=/etc/grid/gateway.yaml"],
        ] {
            assert_eq!(parse(args), Ok(Some("/etc/grid/gateway.yaml".to_owned())), "{args:?}");
        }
    }

    #[test]
    fn no_arguments_uses_the_default_search_path() {
        assert_eq!(parse(&[]), Ok(None), "no arguments");
    }

    #[test]
    fn rejects_malformed_command_lines() {
        for args in [
            &["--config"][..],
            &["--config="],
            &["--validate"],
            &["a.yaml", "b.yaml"],
            &["--config", "a.yaml", "b.yaml"],
            &["--config", "--validate"],
            &["-c", "--config"],
        ] {
            assert_eq!(parse(args), Err(USAGE.to_owned()), "{args:?}");
        }
    }

    #[test]
    fn rejects_http_endpoint_when_otlp_headers_are_configured() {
        assert!(
            validate_otlp_endpoint_transport_values(Some("http://collector:4317"), None, true, None).is_err(),
            "an explicitly configured HTTP endpoint must not receive headers"
        );
    }

    #[test]
    fn rejects_http_environment_fallback_when_otlp_headers_are_configured() {
        assert!(
            validate_otlp_endpoint_transport_values(None, Some("http://collector:4317"), true, None).is_err(),
            "the OTEL_EXPORTER_OTLP_ENDPOINT fallback must not receive headers over HTTP"
        );
    }

    #[test]
    fn accepts_https_endpoints_with_otlp_headers() {
        for (configured, fallback) in [
            (Some("https://collector:4317"), None),
            (None, Some("https://collector:4317")),
        ] {
            assert!(
                validate_otlp_endpoint_transport_values(configured, fallback, true, None).is_ok(),
                "HTTPS endpoints must remain usable with headers"
            );
        }
    }

    #[test]
    fn preserves_http_behavior_when_otlp_headers_are_absent() {
        assert!(
            validate_otlp_endpoint_transport_values(Some("http://collector:4317"), None, false, None).is_ok(),
            "HTTP without exporter headers remains supported"
        );
    }

    #[test]
    fn configured_endpoint_takes_precedence_over_environment_fallback() {
        assert!(
            validate_otlp_endpoint_transport_values(
                Some("https://configured:4317"),
                Some("http://fallback:4317"),
                true,
                None,
            )
            .is_ok(),
            "an unused HTTP fallback must not reject the configured HTTPS endpoint"
        );
    }

    #[test]
    fn rejects_http_when_trace_specific_headers_are_present() {
        let headers_present = otlp_headers_present(false, None, Some(OsStr::new("Authorization=secret")));
        assert!(
            validate_otlp_endpoint_transport_values(Some("http://collector:4317"), None, headers_present, None)
                .is_err(),
            "trace-specific exporter headers must not be sent over HTTP"
        );
    }

    #[test]
    fn empty_configured_headers_do_not_hide_environment_headers() {
        let headers_present = otlp_headers_present(false, Some(OsStr::new("Authorization=secret")), None);
        assert!(
            validate_otlp_endpoint_transport_values(Some("http://collector:4317"), None, headers_present, None)
                .is_err(),
            "the OTLP SDK can merge environment headers into an empty configured map"
        );
    }

    #[test]
    fn rejects_schemeless_endpoints_with_headers_regardless_of_insecure_flags() {
        for endpoint in [Some("collector:4317"), None] {
            assert!(
                validate_otlp_endpoint_transport_values(endpoint, Some("collector:4317"), true, None).is_err(),
                "a schemeless endpoint must not carry credentials; OTLP_INSECURE may make it plaintext"
            );
        }
    }

    #[test]
    fn rejects_headers_without_an_endpoint() {
        assert!(
            validate_otlp_endpoint_transport_values(None, None, true, None).is_err(),
            "headers require an explicit HTTPS endpoint even without an OTLP endpoint override"
        );
    }

    #[test]
    fn rejects_http_protobuf_until_praxis_supports_the_batch_processor_pairing() {
        assert!(
            validate_otlp_endpoint_transport_values(
                Some("https://collector:4318"),
                None,
                false,
                Some("http/protobuf"),
            )
            .is_err(),
            "the shipped Praxis HTTP client is incompatible with its thread-based batch processor"
        );
    }
}
