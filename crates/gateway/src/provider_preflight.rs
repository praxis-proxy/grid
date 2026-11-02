//! Startup and reload checks for a config that serves peers through `grid_provider`.
//!
//! The filter admits a peer by its certificate's organization, so the listener must have
//! verified a grid SVID against the grid CA, and nothing in the config may route a request
//! anywhere but the local backend the filter picked. The organization names the site by
//! issuance, not by this check, so `client_ca` must be the dedicated grid issuer.

use praxis_core::config::{Config, FailureMode, FilterEntry, Listener, ProtocolKind};
use praxis_tls::ClientCertMode;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The filter this preflight guards.
const PROVIDER_FILTER: &str = "grid_provider";

/// Filters that may run ahead of `grid_provider`.
const BEFORE_PROVIDER: [&str; 1] = ["trace_context"];

/// Filters that may run after `grid_provider`, none of which picks an upstream.
const AFTER_PROVIDER: [&str; 2] = ["rate_limit", "headers"];

/// Filters that pick an upstream, refused anywhere in a config that serves `grid_provider`.
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
/// A config holding the filter declares no top-level cluster and no routing filter in any
/// chain. Every listener whose chains hold it must be HTTP, with no upstream, requiring a named
/// grid SVID verified by `grid_ca`. Its pipeline holds `trace_context` at most, then the filter
/// once, unconditioned and failing closed, then only `rate_limit` and `headers`, with no branch.
///
/// # Errors
///
/// Names the first rule broken.
pub(crate) fn check(config: &Config, grid_ca: &str) -> Result<(), String> {
    let serving: Vec<(&Listener, Vec<&FilterEntry>, bool)> = config
        .listeners
        .iter()
        .map(|listener| {
            let (filters, conditioned) = pipeline(config, listener);
            (listener, filters, conditioned)
        })
        .filter(|(_, filters, _)| filters.iter().any(|filter| filter.filter_type == PROVIDER_FILTER))
        .collect();
    if serving.is_empty() {
        return Ok(());
    }
    if !config.clusters.is_empty() {
        return Err("grid: a config serving grid_provider declares no clusters".to_owned());
    }
    let routing = config
        .filter_chains
        .iter()
        .flat_map(|chain| &chain.filters)
        .find(|filter| ROUTING_FILTERS.contains(&filter.filter_type.as_str()));
    if let Some(filter) = routing {
        return Err(format!(
            "grid: a config serving grid_provider runs {}",
            filter.filter_type
        ));
    }
    for (listener, filters, conditioned) in serving {
        let name = &listener.name;
        if conditioned {
            return Err(format!(
                "grid: listener {name:?} runs grid_provider in a conditional chain"
            ));
        }
        check_listener(listener, grid_ca)?;
        check_filters(name, &filters)?;
    }
    Ok(())
}

/// The listener must require a named grid SVID and route nothing itself.
fn check_listener(listener: &Listener, grid_ca: &str) -> Result<(), String> {
    let name = &listener.name;
    if listener.protocol != ProtocolKind::Http || listener.cluster.is_some() || listener.upstream.is_some() {
        return Err(format!(
            "grid: listener {name:?} serving grid_provider must be HTTP with no upstream"
        ));
    }
    let Some(tls) = listener.tls.as_ref() else {
        return Err(format!("grid: listener {name:?} serving grid_provider has no TLS"));
    };
    let named = match tls.client_cert_mode {
        ClientCertMode::RequireNamed => true,
        ClientCertMode::None | ClientCertMode::Request | ClientCertMode::Require => false,
    };
    if !named {
        return Err(format!(
            "grid: listener {name:?} serving grid_provider must set client_cert_mode require_named"
        ));
    }
    if tls.client_ca.as_ref().is_none_or(|ca| ca.ca_path != grid_ca) {
        return Err(format!(
            "grid: listener {name:?} serving grid_provider must verify clients with the grid CA {grid_ca}"
        ));
    }
    Ok(())
}

/// The pipeline must hold `grid_provider` once, first, and after it only what picks no upstream.
fn check_filters(name: &str, filters: &[&FilterEntry]) -> Result<(), String> {
    if let Some(filter) = filters.iter().find(|filter| filter.branch_chains.is_some()) {
        return Err(format!(
            "grid: listener {name:?} runs a branch chain in {}",
            filter.filter_type
        ));
    }
    let mut rest = filters
        .iter()
        .skip_while(|filter| BEFORE_PROVIDER.contains(&filter.filter_type.as_str()));
    let Some(first) = rest.next().filter(|filter| filter.filter_type == PROVIDER_FILTER) else {
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
    if let Some(filter) = rest.find(|filter| !AFTER_PROVIDER.contains(&filter.filter_type.as_str())) {
        return Err(format!(
            "grid: listener {name:?} runs {} after grid_provider; only rate_limit and headers may",
            filter.filter_type
        ));
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

    /// The grid CA the chart's provider listener verifies with.
    const GRID_CA: &str = "/tls/ca.crt";

    /// The listener and chain the chart renders for a provider.
    const GOOD: &str = "listeners:\n  - name: gateway\n    address: 127.0.0.1:8443\n    tls:\n      certificates:\n        - cert_path: /tls/tls.crt\n          key_path: /tls/tls.key\n      client_ca:\n        ca_path: /tls/ca.crt\n      client_cert_mode: require_named\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - filter: trace_context\n      - filter: grid_provider\n        allowed_paths: [/v1/models]\n      - filter: rate_limit\n        mode: global\n        rate: 20\n        burst: 40\n      - filter: headers\n        response_set:\n          - name: X-Grid-Provider-Site\n            value: site-a\n";

    #[test]
    fn the_chart_shape_passes() {
        check(&parse(GOOD), GRID_CA).expect("the chart's provider shape passes");
        let without = GOOD.replace(
            "      - filter: grid_provider\n        allowed_paths: [/v1/models]\n",
            "",
        );
        check(&parse(&without), GRID_CA).expect("a config without grid_provider is not this check's");
    }

    #[test]
    fn a_routing_filter_anywhere_beside_grid_provider_is_refused() {
        for routing in ROUTING_FILTERS {
            let mut config = parse(GOOD);
            config.filter_chains[0].filters.push(entry(routing));
            assert_refused(&config, &format!("runs {routing}"));
            let mut elsewhere = parse(GOOD);
            let mut other = elsewhere.filter_chains[0].clone();
            other.name = "other".to_owned();
            other.filters = vec![entry(routing)];
            elsewhere.filter_chains.push(other);
            assert_refused(&elsewhere, &format!("runs {routing}"));
        }
    }

    #[test]
    fn only_rate_limit_and_headers_may_follow_grid_provider() {
        for after in ["static_response", "url_rewrite", "credential_inject", "model_to_header"] {
            let mut config = parse(GOOD);
            config.filter_chains[0].filters.push(entry(after));
            assert_refused(&config, &format!("runs {after} after grid_provider"));
        }
        let mut branched = parse(GOOD);
        let mut headers = entry("headers");
        headers.branch_chains = Some(Vec::new());
        branched.filter_chains[0].filters.push(headers);
        assert_refused(&branched, "branch chain");
        let mut clustered = parse(GOOD);
        clustered.clusters =
            vec![serde_yaml::from_str("name: elsewhere\nendpoints: [\"10.0.0.1:80\"]").expect("cluster")];
        assert_refused(&clustered, "declares no clusters");
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
        for (yaml, reason) in [
            (late, "must run grid_provider first"),
            (twice, "runs grid_provider after grid_provider"),
            (conditioned, "unconditionally"),
            (open, "unconditionally"),
        ] {
            assert_refused(&parse(&yaml), reason);
        }
    }

    #[test]
    fn a_listener_that_does_not_require_a_named_grid_svid_is_refused() {
        let broken: [(&str, &str, fn(&mut Listener)); 8] = [
            ("require", "require_named", |listener| {
                listener.tls.as_mut().expect("tls").client_cert_mode = ClientCertMode::Require;
            }),
            ("request", "require_named", |listener| {
                listener.tls.as_mut().expect("tls").client_cert_mode = ClientCertMode::Request;
            }),
            ("none", "require_named", |listener| {
                listener.tls.as_mut().expect("tls").client_cert_mode = ClientCertMode::None;
            }),
            ("no client_ca", "grid CA", |listener| {
                listener.tls.as_mut().expect("tls").client_ca = None;
            }),
            ("another CA", "grid CA", |listener| {
                listener
                    .tls
                    .as_mut()
                    .expect("tls")
                    .client_ca
                    .as_mut()
                    .expect("ca")
                    .ca_path = "/other/ca.crt".to_owned();
            }),
            ("no TLS", "has no TLS", |listener| listener.tls = None),
            ("tcp", "must be HTTP", |listener| listener.protocol = ProtocolKind::Tcp),
            ("an upstream", "must be HTTP", |listener| {
                listener.upstream = Some("10.0.0.1:80".to_owned());
            }),
        ];
        for (what, reason, break_it) in broken {
            let mut config = parse(GOOD);
            break_it(&mut config.listeners[0]);
            let error = check(&config, GRID_CA).expect_err(what);
            assert!(error.contains(reason), "{what}: {error}");
        }
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    fn assert_refused(config: &Config, reason: &str) {
        let error = check(config, GRID_CA).expect_err(reason);
        assert!(error.contains(reason), "expected {reason:?}: {error}");
    }

    fn entry(kind: &str) -> FilterEntry {
        serde_yaml::from_str(&format!("filter: {kind}")).expect("entry")
    }

    fn parse(yaml: &str) -> Config {
        Config::from_yaml(yaml).expect("config")
    }
}
