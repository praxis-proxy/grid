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

use std::{
    collections::{BTreeMap, BTreeSet},
    process::ExitCode,
};

use praxis_core::config::{Config, ConfigFile, DEFAULT_CONFIG};
use serde::Deserialize;
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
        .map(|path| start_grid_routing(&path, &config, &mut registry))
    {
        Some(Err(err)) => return praxis::report_fatal(&err, log_output),
        Some(Ok(runtime)) => Some(runtime),
        None => None,
    };

    // Returning instead of exiting drops the guard, flushing queued log lines.
    // The provider-hop TLS binding is checked against this loaded Praxis
    // config. Keep it fixed while serving revisions hot-reload independently.
    let watched_config_file = if grid_runtime.is_some() { None } else { config_file };
    let result = praxis::try_run_server_with_registry(config, registry, watched_config_file, log_level);
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
    praxis_config: &Config,
    registry: &mut praxis_filter::FilterRegistry,
) -> Result<ai_grid_filters::GridRuntime, praxis_filter::FilterError> {
    let config = ai_grid_filters::load_serving_config(path)?;
    let backends = provider_hop_backends(praxis_config)?;
    let mut runtime = ai_grid_filters::spawn_grid_routing(&config, backends)?;
    runtime.watch_config(path)?;
    ai_grid_filters::register_grid_filters(registry, runtime.snapshot())?;
    Ok(runtime)
}

/// Minimal view of Praxis's load-balancer filter config for trust validation.
#[derive(Deserialize)]
struct LoadBalancerBackends {
    /// Configured cluster entries.
    clusters: Vec<BackendCluster>,
}

/// One configured upstream cluster.
#[derive(Deserialize)]
struct BackendCluster {
    /// Cluster identifier.
    name: String,
    /// Upstream TLS settings, absent for plaintext.
    tls: Option<BackendTls>,
}

/// TLS properties required before a backend can carry provider-hop context.
#[derive(Deserialize)]
struct BackendTls {
    /// Expected server name.
    sni: String,
    /// Certificate verification switch.
    verify: bool,
    /// Trusted CA bundle.
    ca: Option<BackendCa>,
    /// Mutual-TLS client identity.
    client_cert: Option<BackendClientCert>,
}

/// CA trust input used by the load balancer.
#[derive(Deserialize)]
struct BackendCa {
    /// CA certificate path.
    ca_path: String,
}

/// Client identity used by the load balancer.
#[derive(Deserialize)]
struct BackendClientCert {
    /// Client certificate path.
    cert_path: String,
    /// Client private key path.
    key_path: String,
}

/// Read the effective load-balancer transport for the chain using Grid routing.
/// Only verified mTLS backends can satisfy an operator provider-hop declaration.
#[expect(clippy::too_many_lines, reason = "keeps the backend transport checks together")]
fn provider_hop_backends(config: &Config) -> Result<BTreeMap<String, String>, praxis_filter::FilterError> {
    let mut backends = BTreeMap::new();
    let mut unverified = BTreeSet::new();
    for chain in &config.filter_chains {
        if !chain
            .filters
            .iter()
            .any(|filter| filter.filter_type == "grid_site_route")
        {
            continue;
        }
        for filter in chain
            .filters
            .iter()
            .filter(|filter| filter.filter_type == "load_balancer")
        {
            let parsed: LoadBalancerBackends =
                serde_yaml::from_value(filter.config.clone()).map_err(|error| -> praxis_filter::FilterError {
                    format!("grid: parsing load_balancer backends: {error}").into()
                })?;
            for backend in parsed.clusters {
                let verified_sni = backend.tls.filter(|tls| {
                    tls.verify
                        && !tls.sni.trim().is_empty()
                        && tls.ca.as_ref().is_some_and(|ca| !ca.ca_path.trim().is_empty())
                        && tls
                            .client_cert
                            .as_ref()
                            .is_some_and(|cert| !cert.cert_path.trim().is_empty() && !cert.key_path.trim().is_empty())
                });
                if let Some(tls) = verified_sni {
                    if unverified.contains(&backend.name) || backends.insert(backend.name.clone(), tls.sni).is_some() {
                        return Err(format!("grid: ambiguous provider-hop backend {:?}", backend.name).into());
                    }
                } else {
                    if backends.contains_key(&backend.name) {
                        return Err(format!("grid: ambiguous provider-hop backend {:?}", backend.name).into());
                    }
                    unverified.insert(backend.name);
                }
            }
        }
    }
    Ok(backends)
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
#[expect(clippy::expect_used, reason = "test fixtures use checked parsing")]
mod tests {
    use praxis_core::config::{Config, DEFAULT_CONFIG, FilterChainConfig};

    use super::{USAGE, config_arg, provider_hop_backends};

    fn config_with_backend(backend: &str) -> Config {
        let mut config = Config::from_yaml(DEFAULT_CONFIG).expect("default Praxis config");
        let chain = format!(
            "name: grid\nfilters:\n  - filter: grid_site_route\n  - filter: load_balancer\n    clusters:\n      - name: provider-a\n{backend}"
        );
        config.filter_chains = vec![serde_yaml::from_str::<FilterChainConfig>(&chain).expect("filter chain")];
        config
    }

    #[test]
    fn provider_hop_backends_require_verified_mutual_tls() {
        let matching = config_with_backend(
            "        tls:\n          sni: provider-a.grid.internal\n          verify: true\n          ca: { ca_path: /tls/ca.crt }\n          client_cert: { cert_path: /tls/tls.crt, key_path: /tls/tls.key }\n",
        );
        assert_eq!(
            provider_hop_backends(&matching)
                .expect("backends")
                .get("provider-a")
                .map(String::as_str),
            Some("provider-a.grid.internal")
        );
        for backend in [
            "        endpoints: [provider-a:80]\n",
            "        tls: { sni: provider-a.grid.internal, verify: true }\n",
            "        tls: { sni: provider-a.grid.internal, verify: false, ca: { ca_path: /tls/ca.crt }, client_cert: { cert_path: /tls/tls.crt, key_path: /tls/tls.key } }\n",
        ] {
            assert!(
                provider_hop_backends(&config_with_backend(backend))
                    .expect("backends")
                    .is_empty()
            );
        }
    }

    #[test]
    fn provider_hop_backends_reject_verified_plaintext_name_collision() {
        let verified = "        tls:\n          sni: provider-a.grid.internal\n          verify: true\n          ca: { ca_path: /tls/ca.crt }\n          client_cert: { cert_path: /tls/tls.crt, key_path: /tls/tls.key }\n";
        let plaintext = "        endpoints: [provider-a:80]\n";
        for (first, second) in [(verified, plaintext), (plaintext, verified)] {
            let mut config = config_with_backend(first);
            let mut other = config_with_backend(second).filter_chains.remove(0);
            other.name = "other-grid".to_owned();
            config.filter_chains.push(other);
            assert!(
                provider_hop_backends(&config)
                    .err()
                    .is_some_and(|error| error.to_string().contains("ambiguous provider-hop backend"))
            );
        }
    }

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
