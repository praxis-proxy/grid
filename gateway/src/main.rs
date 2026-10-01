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

use std::process::ExitCode;

use praxis_core::config::{Config, ConfigFile, DEFAULT_CONFIG};
use tracing::info;

/// Log line emitted once tracing is up; the startup test waits for it.
const STARTUP_MESSAGE: &str = "starting grid-gateway";

fn main() -> ExitCode {
    // Install the crypto provider before anything builds a TLS config.
    praxis::install_crypto_provider();

    // The operator writes the config. The path is `--config <path>` or the
    // positional argument, else the default search path. Read it once so the
    // reload watcher baselines on the bytes that run.
    let explicit = config_arg(std::env::args().skip(1)).unwrap_or_else(|err| praxis::fatal(&err));
    let config_file = praxis::resolve_config_path(explicit.as_deref())
        .as_deref()
        .map(ConfigFile::read)
        .transpose()
        .unwrap_or_else(|err| praxis::fatal(&err));
    let config = praxis::with_bootstrap_logging(|| Config::from_config_file_or(config_file.as_ref(), DEFAULT_CONFIG))
        .unwrap_or_else(|err| praxis::fatal(&err));

    // Without a subscriber every log line, including reload results, is dropped.
    let tracing_guard = praxis::init_tracing(&config).unwrap_or_else(|err| praxis::fatal(&err));
    let log_level = Some(tracing_guard.log_level_state());
    let log_output = config.runtime.logging.output;
    info!(version = env!("CARGO_PKG_VERSION"), "{STARTUP_MESSAGE}");

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

    // Returning instead of exiting drops the guard, flushing queued log lines.
    let result = praxis::try_run_server_with_registry(config, registry, config_file, log_level);
    drop(grid_runtime);
    result.map_or_else(|err| praxis::report_fatal(&err, log_output), |()| ExitCode::SUCCESS)
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
    let runtime = ai_grid_filters::spawn_grid_routing(&config)?;
    ai_grid_filters::register_grid_filters(registry, runtime.snapshot())?;
    Ok(runtime)
}

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
    use super::{USAGE, config_arg};

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
}
