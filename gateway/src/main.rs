//! `grid-gateway`: the grid data-plane operand.
//!
//! A Praxis gateway assembled in the grid repo, deployed and configured by the
//! grid operator. Operator is the control plane. This binary is the operand it
//! manages.
//!
//! It links the Praxis library, registers the generic routing filters over the
//! builtin registry, and runs the Praxis server on the operator-supplied config.
//! The grid live-signals filter registers at the routing milestone. This crate
//! is its own Cargo workspace so praxis-proxy 0.7.0 resolves independently of the
//! operator's Kubernetes client stack. See `deploy/gateway/Containerfile` and the
//! `gateway-image` make target.

fn main() {
    // Install the crypto provider before anything builds a TLS config.
    praxis::install_crypto_provider();

    let mut registry = praxis_filter::FilterRegistry::with_builtins();
    praxis_ai_filters::register_ai_filters(&mut registry, None);

    // The operator writes the config. The path is the first argument, else the
    // default search path.
    let explicit = std::env::args().nth(1);
    let config_path = praxis::resolve_config_path(explicit.as_deref());
    let config = praxis::load_config(explicit.as_deref()).unwrap_or_else(|err| praxis::fatal(&err));

    // Runs the Pingora server and never returns.
    praxis::run_server_with_registry(config, registry, config_path, None);
}
