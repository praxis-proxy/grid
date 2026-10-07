//! Entry point for the fleet dashboard. Wiring only; everything with behavior
//! lives in the library crate, where it is tested.

use std::{error::Error, path::Path, sync::Arc, time::Duration};

use clap::Parser as _;
use fleet_dashboard::{
    api::{Assets, router, shutdown_requested},
    collector::{Collector, Metrics, Options as CollectorOptions},
    config::ConfigFile,
    demo,
    metrics::{CentralSource, Client, PerSiteSource, SecretReader, Source, build_http_client},
    model::Config,
    options::{MetricsMode, Options, parse_listen},
    queries::QuerySet,
    registry::{RegistryStore, SiteList},
};
use tokio::sync::watch;
use tracing_subscriber::{EnvFilter, filter::LevelFilter};

/// How long a shutdown waits for open connections, as the Go server did.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Where sites and their metrics come from.
struct Backends {
    /// The site list the collector follows.
    sites: watch::Receiver<SiteList>,
    /// Where site metrics are read.
    source: Source,
    /// The registry store to sync before polling; absent in demo mode.
    store: Option<Arc<RegistryStore>>,
    /// Keeps the demo's static site list alive for the life of the process.
    _demo_sites: Option<watch::Sender<SiteList>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_tracing();
    if rustls::crypto::ring::default_provider().install_default().is_err() {
        tracing::warn!("rustls default CryptoProvider already installed; continuing");
    }
    let options = Options::parse();
    version::get().log_startup("grid-fleet-dashboard");
    options.validate()?;
    let config = ConfigFile::load(options.config.as_deref())?;
    let backends = backends(&options).await?;
    serve(options, config, backends).await
}

/// JSON logs at `info` unless `RUST_LOG` says otherwise.
fn init_tracing() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
}

/// Serves until SIGINT or SIGTERM, then drains connections for at most
/// [`DRAIN_TIMEOUT`]. The HTTP server starts first so `/healthz` answers while
/// the registry syncs; the collector starts once the registry has listed, so
/// `/readyz` means real data.
async fn serve(options: Options, config: ConfigFile, backends: Backends) -> Result<(), Box<dyn Error>> {
    let address = parse_listen(&options.listen)?;
    let api_config = api_config(&options, &config);
    let registry = backends.store;
    let collector = Arc::new(collector(&options, config, backends.sites, backends.source)?);
    let shutting_down = arm_shutdown();
    let app = router(
        Arc::clone(&collector),
        api_config,
        Assets::embedded(),
        shutting_down.clone(),
    );
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, mode = ?options.metrics_mode, demo = options.demo, version = version::VERSION, "serving");
    let poll_loop = async move {
        if let Some(registry) = registry {
            registry.run().await?;
        }
        collector.run().await;
        Ok::<(), Box<dyn Error>>(())
    };
    let graceful = axum::serve(listener, app).with_graceful_shutdown(shutdown_requested(shutting_down.clone()));
    tokio::select! {
        served = graceful => served?,
        finished = poll_loop => finished?,
        () = drained(shutting_down) => tracing::warn!(timeout = ?DRAIN_TIMEOUT, "connections did not drain; exiting"),
    }
    Ok(())
}

/// A switch that turns true on SIGINT or SIGTERM.
fn arm_shutdown() -> watch::Receiver<bool> {
    let (shutdown, shutting_down) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown.send_replace(true);
    });
    shutting_down
}

/// Resolves [`DRAIN_TIMEOUT`] after shutdown is requested.
async fn drained(shutting_down: watch::Receiver<bool>) {
    shutdown_requested(shutting_down).await;
    tokio::time::sleep(DRAIN_TIMEOUT).await;
}

/// What the SPA reads once at load.
fn api_config(options: &Options, config: &ConfigFile) -> Config {
    Config {
        hub: config.hub.clone(),
        poll_interval_seconds: options.poll_interval.as_secs(),
        version: version::VERSION.to_owned(),
        thresholds: config.api_thresholds(),
        user: None,
    }
}

/// The collector over `sites` and `source` with the configured queries,
/// thresholds, and hub.
fn collector(
    options: &Options,
    config: ConfigFile,
    sites: watch::Receiver<SiteList>,
    source: Source,
) -> Result<Collector, Box<dyn Error>> {
    Ok(Collector::new(CollectorOptions {
        sites,
        source,
        queries: QuerySet::defaults()?.with(&config.queries),
        thresholds: config.thresholds,
        hub: config.hub,
        interval: options.poll_interval,
        site_timeout: options.site_timeout,
        metrics: Arc::new(Metrics::new()?),
        clock: Arc::new(time::OffsetDateTime::now_utc),
    }))
}

/// Builds the site list and metrics source the options ask for.
async fn backends(options: &Options) -> Result<Backends, Box<dyn Error>> {
    if options.demo {
        let (sender, receiver) = watch::channel(Arc::new(demo::sites()));
        return Ok(Backends {
            sites: receiver,
            source: Source::Demo,
            store: None,
            _demo_sites: Some(sender),
        });
    }
    let client = kube_client(options.kubeconfig.as_deref()).await?;
    let store = Arc::new(RegistryStore::new(
        client,
        &options.namespace,
        &options.registry_configmap,
        &options.registry_key,
    ));
    let source = match options.metrics_mode {
        MetricsMode::PerSite => Source::PerSite(PerSiteSource::new(secrets(&store), options.site_timeout)),
        MetricsMode::Central => central_source(options)?,
    };
    Ok(Backends {
        sites: store.sites(),
        source,
        store: Some(store),
        _demo_sites: None,
    })
}

/// One central store, trusting the platform roots plus `--central-ca-file`.
/// The token comes from `FLEET_CENTRAL_TOKEN`, never an option, so it cannot
/// appear in a process listing.
fn central_source(options: &Options) -> Result<Source, Box<dyn Error>> {
    let ca = options.central_ca_file.as_deref().map(std::fs::read).transpose()?;
    let http = build_http_client(ca.as_deref(), options.site_timeout)?;
    let token = std::env::var("FLEET_CENTRAL_TOKEN").unwrap_or_default();
    let url = options.central_url.clone().unwrap_or_default();
    Ok(Source::Central(CentralSource::new(
        Client::new(&url, &token, http),
        &options.cluster_label,
    )))
}

/// The registry store as the per-site source's secret reader.
fn secrets(store: &Arc<RegistryStore>) -> Arc<dyn SecretReader> {
    Arc::<RegistryStore>::clone(store)
}

/// In-cluster configuration, or the kubeconfig at `path` for development.
async fn kube_client(path: Option<&Path>) -> Result<kube::Client, Box<dyn Error>> {
    let Some(path) = path else {
        return Ok(kube::Client::try_default().await?);
    };
    let kubeconfig = kube::config::Kubeconfig::read_from(path)?;
    let config = kube::Config::from_custom_kubeconfig(kubeconfig, &kube::config::KubeConfigOptions::default()).await?;
    Ok(kube::Client::try_from(config)?)
}

/// Resolves on SIGINT or SIGTERM.
async fn shutdown_signal() {
    let mut terminate = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(err) => {
            tracing::warn!(error = %err, "SIGTERM handler unavailable; only SIGINT stops the server");
            return tokio::signal::ctrl_c().await.unwrap_or_default();
        },
    };
    tokio::select! {
        interrupt = tokio::signal::ctrl_c() => interrupt.unwrap_or_default(),
        _ = terminate.recv() => {},
    }
}
