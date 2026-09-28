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
//! is its own Cargo workspace so praxis-proxy 0.7.0 resolves independently of the
//! operator's Kubernetes client stack. See `deploy/gateway/Containerfile` and the
//! `gateway-image` make target.

fn main() {
    // Install the crypto provider before anything builds a TLS config.
    praxis::install_crypto_provider();

    let mut registry = praxis_filter::FilterRegistry::with_builtins();
    praxis_ai_filters::register_ai_filters(&mut registry, None);

    // Grid cross-site routing is wired when the operator provides a serving
    // config. spawn_grid_routing starts one poller per peer and returns the
    // runtime holding their handles. grid_site_route registers over the snapshot
    // the pollers refresh. The runtime is bound for the process's life: dropping
    // it stops the pollers, and run_server never returns, so the binding lives as
    // long as the server.
    let _grid_runtime = std::env::var("GRID_SERVING_CONFIG").ok().map(|path| {
        let config = ai_grid_filters::load_serving_config(&path).unwrap_or_else(|err| praxis::fatal(&err));
        let runtime = ai_grid_filters::spawn_grid_routing(&config).unwrap_or_else(|err| praxis::fatal(&err));
        ai_grid_filters::register_grid_filters(&mut registry, runtime.snapshot())
            .unwrap_or_else(|err| praxis::fatal(&err));
        runtime
    });

    // The operator writes the config. The path is the first argument, else the
    // default search path.
    let explicit = std::env::args().nth(1);
    let config_path = praxis::resolve_config_path(explicit.as_deref());
    let config = praxis::load_config(explicit.as_deref()).unwrap_or_else(|err| praxis::fatal(&err));

    // Runs the Pingora server and never returns.
    praxis::run_server_with_registry(config, registry, config_path, None);
}
