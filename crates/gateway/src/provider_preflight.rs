//! Startup and reload checks for a config that serves peers through `grid_provider`.
//!
//! The filter admits a peer by its certificate's organization, so the listener must have
//! verified that certificate, and nothing in the pipeline may route the request anywhere
//! but the local backend the filter picked.

use praxis_core::config::{Config, FailureMode, FilterEntry, Listener, ProtocolKind};
use praxis_tls::ClientCertMode;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The filter this preflight guards.
const PROVIDER_FILTER: &str = "grid_provider";

/// Filters that may run ahead of `grid_provider`.
const BEFORE_PROVIDER: [&str; 1] = ["trace_context"];

/// Filters that pick an upstream, and so could send a peer's request onward.
const ROUTING_FILTERS: [&str; 5] = [
    "load_balancer",
    "router",
    "grid_site_route",
    "intelligent_route",
    "provider_route",
];

// ---------------------------------------------------------------------------
// Preflight
// ---------------------------------------------------------------------------

/// Refuse a config in which `grid_provider` could admit an unverified peer or route onward.
///
/// Every listener whose chains hold `grid_provider` must be HTTP with TLS requiring a client
/// certificate verified against a CA, and no TCP routing; its pipeline must hold the filter
/// once, first after `trace_context`, unconditioned and failing closed, with no routing
/// filter and no branch chain. A config that holds the filter declares no top-level clusters.
///
/// # Errors
///
/// Names the first rule broken.
pub(crate) fn check(config: &Config) -> Result<(), String> {
    let mut serves = false;
    for listener in &config.listeners {
        let (filters, conditioned) = pipeline(config, listener);
        if !filters.iter().any(|filter| filter.filter_type == PROVIDER_FILTER) {
            continue;
        }
        serves = true;
        let name = &listener.name;
        if conditioned {
            return Err(format!(
                "grid: listener {name:?} runs grid_provider in a conditional chain"
            ));
        }
        check_listener(listener)?;
        check_filters(name, &filters)?;
    }
    if serves && !config.clusters.is_empty() {
        return Err("grid: a config serving grid_provider declares no clusters".to_owned());
    }
    Ok(())
}

/// The listener must verify every client's certificate and route nothing itself.
fn check_listener(listener: &Listener) -> Result<(), String> {
    let name = &listener.name;
    if listener.protocol != ProtocolKind::Http || listener.cluster.is_some() || listener.upstream.is_some() {
        return Err(format!(
            "grid: listener {name:?} serving grid_provider must be HTTP with no upstream"
        ));
    }
    let Some(tls) = listener.tls.as_ref() else {
        return Err(format!("grid: listener {name:?} serving grid_provider has no TLS"));
    };
    let required = match tls.client_cert_mode {
        ClientCertMode::Require | ClientCertMode::RequireNamed => true,
        ClientCertMode::None | ClientCertMode::Request => false,
    };
    if !required || tls.client_ca.is_none() {
        return Err(format!(
            "grid: listener {name:?} serving grid_provider must require a client certificate verified by client_ca"
        ));
    }
    Ok(())
}

/// The pipeline must hold `grid_provider` once, first, and nothing that routes.
fn check_filters(name: &str, filters: &[&FilterEntry]) -> Result<(), String> {
    let mut leading = filters
        .iter()
        .skip_while(|filter| BEFORE_PROVIDER.contains(&filter.filter_type.as_str()));
    let Some(first) = leading.next().filter(|filter| filter.filter_type == PROVIDER_FILTER) else {
        return Err(format!(
            "grid: listener {name:?} must run grid_provider first, after trace_context only"
        ));
    };
    if !first.conditions.is_empty()
        || !first.response_conditions.is_empty()
        || first.failure_mode != FailureMode::Closed
    {
        return Err(format!(
            "grid: listener {name:?} must run grid_provider unconditionally, failing closed"
        ));
    }
    for filter in filters {
        let kind = filter.filter_type.as_str();
        if ROUTING_FILTERS.contains(&kind) {
            return Err(format!("grid: listener {name:?} runs {kind} beside grid_provider"));
        }
        if filter.branch_chains.is_some() {
            return Err(format!(
                "grid: listener {name:?} runs a branch chain beside grid_provider"
            ));
        }
    }
    if filters
        .iter()
        .filter(|filter| filter.filter_type == PROVIDER_FILTER)
        .count()
        != 1
    {
        return Err(format!("grid: listener {name:?} runs grid_provider more than once"));
    }
    Ok(())
}

/// The filters `listener` runs, in order, and whether any of its chains is conditional.
fn pipeline<'config>(config: &'config Config, listener: &Listener) -> (Vec<&'config FilterEntry>, bool) {
    let chains: Vec<_> = listener
        .filter_chains
        .iter()
        .filter_map(|name| config.filter_chains.iter().find(|chain| chain.name == *name))
        .collect();
    let conditioned = chains.iter().any(|chain| !chain.conditions.is_empty());
    (chains.iter().flat_map(|chain| &chain.filters).collect(), conditioned)
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::type_complexity,
    reason = "tests"
)]
mod tests {
    use super::*;

    /// The listener and chain the chart renders for a provider.
    const GOOD: &str = "listeners:\n  - name: gateway\n    address: 127.0.0.1:8443\n    tls:\n      certificates:\n        - cert_path: /tls/tls.crt\n          key_path: /tls/tls.key\n      client_ca:\n        ca_path: /tls/ca.crt\n      client_cert_mode: require_named\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: trace_context\n      - filter: grid_provider\n        allowed_paths: [/v1/models]\n      - filter: headers\n        response_set:\n          - name: X-Grid-Provider-Site\n            value: site-a\n";

    #[test]
    fn the_chart_shape_passes() {
        check(&parse(GOOD)).expect("the chart's provider shape passes");
        check(&parse(&GOOD.replace("require_named", "require"))).expect("require passes");
        let without = GOOD.replace(
            "      - filter: grid_provider\n        allowed_paths: [/v1/models]\n",
            "",
        );
        check(&parse(&without)).expect("a config without grid_provider is not this check's");
    }

    #[test]
    fn a_routing_filter_beside_grid_provider_is_refused() {
        for routing in ROUTING_FILTERS {
            let mut config = parse(GOOD);
            config.filter_chains[0].filters.push(entry(routing));
            assert!(
                check(&config).is_err(),
                "{routing} beside grid_provider must be refused"
            );
        }
        let mut branched = parse(GOOD);
        let mut headers = entry("headers");
        headers.branch_chains = Some(Vec::new());
        branched.filter_chains[0].filters.push(headers);
        assert!(
            check(&branched).is_err(),
            "a branch chain beside grid_provider must be refused"
        );
    }

    #[test]
    fn grid_provider_must_come_first_once_unconditioned() {
        let late = GOOD.replace(
            "      - filter: trace_context\n",
            "      - filter: trace_context\n      - filter: headers\n        request_set:\n          - name: x-a\n            value: b\n",
        );
        let twice = GOOD.replace(
            "      - filter: headers\n",
            "      - filter: grid_provider\n        allowed_paths: [/v1/models]\n      - filter: headers\n",
        );
        let conditioned = GOOD.replace(
            "        allowed_paths: [/v1/models]\n",
            "        allowed_paths: [/v1/models]\n        conditions:\n          - when:\n              path_prefix: /v1\n",
        );
        let open = GOOD.replace(
            "        allowed_paths: [/v1/models]\n",
            "        allowed_paths: [/v1/models]\n        failure_mode: open\n",
        );
        for (what, yaml) in [
            ("late", late),
            ("twice", twice),
            ("conditioned", conditioned),
            ("open", open),
        ] {
            assert!(check(&parse(&yaml)).is_err(), "{what} must be refused");
        }
    }

    #[test]
    fn a_listener_that_does_not_require_a_verified_client_is_refused() {
        let broken: [(&str, fn(&mut Listener)); 6] = [
            ("request", |listener| {
                listener.tls.as_mut().expect("tls").client_cert_mode = ClientCertMode::Request;
            }),
            ("none", |listener| {
                listener.tls.as_mut().expect("tls").client_cert_mode = ClientCertMode::None;
            }),
            ("no client_ca", |listener| {
                listener.tls.as_mut().expect("tls").client_ca = None;
            }),
            ("no TLS", |listener| listener.tls = None),
            ("tcp", |listener| listener.protocol = ProtocolKind::Tcp),
            ("an upstream", |listener| {
                listener.upstream = Some("10.0.0.1:80".to_owned());
            }),
        ];
        for (what, break_it) in broken {
            let mut config = parse(GOOD);
            break_it(&mut config.listeners[0]);
            assert!(check(&config).is_err(), "{what} must be refused");
        }
    }

    #[test]
    fn top_level_clusters_beside_grid_provider_are_refused() {
        let mut config = parse(GOOD);
        let other = parse(
            "clusters:\n  - name: elsewhere\n    endpoints: [10.0.0.1:80]\nlisteners:\n  - name: x\n    address: 127.0.0.1:1\n    filter_chains: [c]\nfilter_chains:\n  - name: c\n    filters:\n      - filter: load_balancer\n        clusters:\n          - name: elsewhere\n            endpoints: [10.0.0.1:80]\n",
        );
        config.clusters = other.clusters;
        assert!(check(&config).is_err(), "a top-level cluster could be routed to");
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    fn entry(kind: &str) -> FilterEntry {
        serde_yaml::from_str(&format!("filter: {kind}")).expect("entry")
    }

    fn parse(yaml: &str) -> Config {
        Config::from_yaml(yaml).expect("config")
    }
}
