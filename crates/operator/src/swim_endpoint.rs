//! Parsing and startup resolution for SWIM endpoints.

use std::{
    collections::BTreeSet,
    io,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    time::Duration,
};

use futures::StreamExt as _;

/// Maximum time allowed for one DNS lookup.
pub const DNS_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(5);
/// Maximum time allowed for one seed-list reconciliation.
pub const DNS_RESOLUTION_AGGREGATE_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum number of DNS lookups running concurrently.
const DNS_RESOLUTION_CONCURRENCY: usize = 32;

/// One endpoint-resolution failure that can be reported without discarding
/// successful entries from the same configuration list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointResolutionFailure {
    /// Configuration source, such as `GRID_SWIM_SEEDS`.
    pub source: String,
    /// The configured endpoint that failed.
    pub endpoint: String,
    /// Bounded, actionable failure detail.
    pub reason: String,
}

/// Result of resolving a configured seed list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeedResolution {
    /// Whether at least one non-empty endpoint was configured.
    pub configured: bool,
    /// All usable addresses, sorted and deduplicated.
    pub addresses: Vec<SocketAddr>,
    /// Invalid or unresolved entries, in configuration order.
    pub failures: Vec<EndpointResolutionFailure>,
}

impl SeedResolution {
    /// Return whether a non-empty configuration produced no usable address.
    pub fn is_degraded(&self) -> bool {
        self.configured && (!self.failures.is_empty() || self.addresses.is_empty())
    }
}

/// A SWIM endpoint before DNS resolution.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum SwimEndpoint {
    /// A literal IPv4 or bracketed IPv6 socket address.
    Literal(SocketAddr),
    /// A DNS hostname and its UDP port.
    Hostname {
        /// DNS name to resolve.
        host: String,
        /// UDP port.
        port: u16,
    },
}

impl SwimEndpoint {
    /// Return the configured endpoint text in a stable display form.
    pub fn as_text(&self) -> String {
        match self {
            Self::Literal(addr) => addr.to_string(),
            Self::Hostname { host, port } => format!("{host}:{port}"),
        }
    }

    /// Return the DNS lookup input, if this endpoint is a hostname.
    fn host_port(&self) -> Option<(&str, u16)> {
        match self {
            Self::Literal(_) => None,
            Self::Hostname { host, port } => Some((host, *port)),
        }
    }
}

impl FromStr for SwimEndpoint {
    type Err = String;

    #[expect(
        clippy::too_many_lines,
        reason = "endpoint parsing keeps all accepted and rejected forms together"
    )]
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() || value.chars().any(char::is_whitespace) {
            return Err("endpoint must be non-empty and contain no whitespace".to_owned());
        }
        if value.contains("://") || value.contains('/') {
            return Err("endpoint must be host:port, not a URL or path".to_owned());
        }

        if let Ok(addr) = value.parse::<SocketAddr>() {
            return nonzero_port(addr.port()).map(|()| Self::Literal(addr));
        }

        let (host, port_text) = if let Some(rest) = value.strip_prefix('[') {
            let Some((host, suffix)) = rest.split_once(']') else {
                return Err("bracketed IPv6 endpoint is missing ]".to_owned());
            };
            let Some(port_text) = suffix.strip_prefix(':') else {
                return Err("bracketed host must be followed by :port".to_owned());
            };
            if host.is_empty() || port_text.is_empty() || port_text.contains(':') {
                return Err("invalid bracketed host:port endpoint".to_owned());
            }
            if !matches!(host.parse::<IpAddr>(), Ok(IpAddr::V6(_))) {
                return Err("only IPv6 literals may use brackets".to_owned());
            }
            (host, port_text)
        } else {
            if value.matches(':').count() != 1 {
                return Err("IPv6 addresses must be bracketed and hostnames require one :port".to_owned());
            }
            let Some((host, port_text)) = value.split_once(':') else {
                return Err("endpoint must be host:port".to_owned());
            };
            (host, port_text)
        };

        if host.is_empty() || host.contains('[') || host.contains(']') {
            return Err("endpoint host is empty or malformed".to_owned());
        }
        if host.parse::<IpAddr>().is_err() {
            validate_hostname(host)?;
        }
        let port = port_text
            .parse::<u16>()
            .map_err(|_error| "endpoint port must be an integer in 1..=65535".to_owned())?;
        nonzero_port(port)?;
        Ok(Self::Hostname {
            host: host.to_owned(),
            port,
        })
    }
}

/// Validate the DNS label syntax accepted by the endpoint parser.
fn validate_hostname(host: &str) -> Result<(), String> {
    for label in host.trim_end_matches('.').split('.') {
        if label.is_empty()
            || label.starts_with('-')
            || label.ends_with('-')
            || !label.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
        {
            return Err("endpoint hostname contains an invalid DNS label".to_owned());
        }
    }
    Ok(())
}

/// Reject the zero port, which is not a usable SWIM endpoint port.
fn nonzero_port(port: u16) -> Result<(), String> {
    if port == 0 {
        Err("endpoint port must be nonzero".to_owned())
    } else {
        Ok(())
    }
}

/// Resolve one endpoint with the production Tokio resolver.
///
/// # Errors
///
/// Returns an actionable error if parsing or bounded DNS resolution fails.
pub async fn resolve_endpoint(endpoint: &SwimEndpoint, source: &str) -> Result<Vec<SocketAddr>, String> {
    resolve_endpoint_with(endpoint, source, DNS_RESOLUTION_TIMEOUT, |host, port| {
        let host = host.to_owned();
        async move { Ok(tokio::net::lookup_host((host, port)).await?.collect()) }
    })
    .await
}

/// Resolve a configured list, retaining every usable address deterministically.
///
/// # Errors
///
/// Returns an actionable error identifying the configuration source and endpoint
/// when parsing or bounded DNS resolution fails.
pub async fn resolve_endpoint_list(raw: &[String], source: &str) -> Result<Vec<SocketAddr>, String> {
    let result = resolve_endpoint_list_partial(raw, source).await;
    if let Some(failure) = result.failures.first() {
        return Err(format!(
            "{} endpoint {:?}: {}",
            failure.source, failure.endpoint, failure.reason
        ));
    }
    Ok(result.addresses)
}

/// Resolve a configured list while retaining successful entries when others fail.
pub async fn resolve_endpoint_list_partial(raw: &[String], source: &str) -> SeedResolution {
    resolve_endpoint_list_partial_with(
        raw,
        source,
        DNS_RESOLUTION_TIMEOUT,
        DNS_RESOLUTION_AGGREGATE_TIMEOUT,
        |host, port| {
            let host = host.to_owned();
            async move { Ok(tokio::net::lookup_host((host, port)).await?.collect()) }
        },
    )
    .await
}

/// Injectable implementation of the partial seed resolver.
#[expect(
    clippy::too_many_lines,
    reason = "partial resolution keeps parse, bounded concurrency, diagnostics, and deterministic finalization together"
)]
pub async fn resolve_endpoint_list_partial_with<F, Fut>(
    raw: &[String],
    source: &str,
    timeout: Duration,
    aggregate_timeout: Duration,
    resolver: F,
) -> SeedResolution
where
    F: Fn(&str, u16) -> Fut + Copy + Send + Sync + 'static,
    Fut: Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'static,
{
    let mut failures = Vec::new();
    let mut pending = Vec::new();
    for item in raw.iter().map(String::as_str).map(str::trim).filter(|s| !s.is_empty()) {
        match item.parse::<SwimEndpoint>() {
            Ok(endpoint) => pending.push((item.to_owned(), endpoint)),
            Err(reason) => failures.push(EndpointResolutionFailure {
                source: source.to_owned(),
                endpoint: item.to_owned(),
                reason: format!("invalid endpoint: {reason}"),
            }),
        }
    }
    let configured = !pending.is_empty() || !failures.is_empty();
    let mut unresolved: BTreeSet<String> = pending.iter().map(|(item, _)| item.clone()).collect();
    let jobs = futures::stream::iter(pending.into_iter().map(|(item, endpoint)| async move {
        let result = resolve_endpoint_with(&endpoint, source, timeout, resolver).await;
        (item, result)
    }))
    .buffer_unordered(DNS_RESOLUTION_CONCURRENCY);
    tokio::pin!(jobs);
    let deadline = tokio::time::sleep(aggregate_timeout);
    tokio::pin!(deadline);
    let mut addresses = Vec::new();
    loop {
        tokio::select! {
            item = jobs.next() => {
                let Some((endpoint, result)) = item else { break };
                unresolved.remove(&endpoint);
                match result {
                    Ok(mut endpoint_addresses) => addresses.append(&mut endpoint_addresses),
                    Err(reason) => failures.push(EndpointResolutionFailure {
                        source: source.to_owned(), endpoint, reason,
                    }),
                }
            }
            () = &mut deadline => {
                for endpoint in unresolved {
                    failures.push(EndpointResolutionFailure {
                        source: source.to_owned(),
                        endpoint,
                        reason: format!("DNS resolution exceeded aggregate timeout of {aggregate_timeout:?}"),
                    });
                }
                break;
            }
        }
    }
    addresses.sort_unstable();
    addresses.dedup();
    SeedResolution {
        configured,
        addresses,
        failures,
    }
}

/// Resolve a configured list with an injectable resolver.
///
/// # Errors
///
/// Returns the first invalid or unresolved endpoint as an actionable error.
pub async fn resolve_endpoint_list_with<F, Fut>(
    raw: &[String],
    source: &str,
    timeout: Duration,
    resolver: F,
) -> Result<Vec<SocketAddr>, String>
where
    F: Fn(&str, u16) -> Fut + Copy + Send + Sync + 'static,
    Fut: Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'static,
{
    let result = resolve_endpoint_list_partial_with(raw, source, timeout, timeout, resolver).await;
    if let Some(failure) = result.failures.first() {
        return Err(format!(
            "{} endpoint {:?}: {}",
            failure.source, failure.endpoint, failure.reason
        ));
    }
    Ok(result.addresses)
}

/// Resolve an endpoint with an injectable resolver for deterministic tests.
///
/// # Errors
///
/// Returns an actionable error when the injected resolver fails, times out, or
/// returns no usable addresses.
pub async fn resolve_endpoint_with<F, Fut>(
    endpoint: &SwimEndpoint,
    source: &str,
    timeout: Duration,
    resolver: F,
) -> Result<Vec<SocketAddr>, String>
where
    F: FnOnce(&str, u16) -> Fut,
    Fut: Future<Output = io::Result<Vec<SocketAddr>>>,
{
    let Some((host, port)) = endpoint.host_port() else {
        return match endpoint {
            SwimEndpoint::Literal(addr) => Ok(vec![*addr]),
            SwimEndpoint::Hostname { .. } => Err(format!("{source} endpoint resolution failed")),
        };
    };
    let result = tokio::time::timeout(timeout, resolver(host, port))
        .await
        .map_err(|_error| format!("{source} DNS resolution timed out for {host}:{port}"))?
        .map_err(|error| format!("{source} DNS resolution failed for {host}:{port}: {error}"))?;
    let mut addresses = result;
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        return Err(format!(
            "{source} DNS resolution returned no addresses for {host}:{port}"
        ));
    }
    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(value: &str) -> SocketAddr {
        value.parse().unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn parses_ipv4_literal() {
        assert_eq!(
            "10.0.0.4:7946".parse(),
            Ok(SwimEndpoint::Literal(addr("10.0.0.4:7946")))
        );
    }

    #[test]
    fn parses_bracketed_ipv6_literal() {
        assert_eq!(
            "[2001:db8::4]:7946".parse(),
            Ok(SwimEndpoint::Literal(addr("[2001:db8::4]:7946")))
        );
    }

    #[test]
    fn parses_hostname() {
        assert_eq!(
            "grid-swim.example.internal:7946".parse(),
            Ok(SwimEndpoint::Hostname {
                host: "grid-swim.example.internal".to_owned(),
                port: 7946,
            })
        );
    }

    #[test]
    fn rejects_invalid_endpoint_shapes() {
        for value in [
            "host",
            "host:0",
            "host:",
            "http://host:7946",
            "/host:7946",
            "2001:db8::4:7946",
        ] {
            assert!(value.parse::<SwimEndpoint>().is_err(), "{value} must be rejected");
        }
    }

    #[tokio::test]
    async fn resolves_multiple_addresses_deterministically_and_deduplicates() {
        let endpoint = "grid.example:7946"
            .parse::<SwimEndpoint>()
            .unwrap_or_else(|_| std::process::abort());
        let result = resolve_endpoint_with(
            &endpoint,
            "GRID_SWIM_SEEDS",
            Duration::from_secs(1),
            |_host, _port| async {
                Ok(vec![
                    addr("10.0.0.2:7946"),
                    addr("10.0.0.1:7946"),
                    addr("10.0.0.2:7946"),
                ])
            },
        )
        .await;
        assert_eq!(result, Ok(vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")]));
    }

    #[tokio::test]
    async fn literal_seed_lists_trim_blanks_and_deduplicate() {
        let raw = vec![
            " 10.0.0.2:7946 ".to_owned(),
            String::new(),
            "10.0.0.1:7946".to_owned(),
            "10.0.0.2:7946".to_owned(),
        ];
        let result = resolve_endpoint_list(&raw, "GridNetwork.spec.seeds").await;
        assert_eq!(result, Ok(vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")]));
    }

    #[tokio::test]
    async fn mixed_literal_and_hostname_seed_lists_resolve_deterministically() {
        let raw = vec!["10.0.0.2:7946".to_owned(), "grid.example:7946".to_owned()];
        let result =
            resolve_endpoint_list_with(&raw, "GRID_SWIM_SEEDS", Duration::from_secs(1), |_host, _port| async {
                Ok(vec![addr("10.0.0.1:7946")])
            })
            .await;
        assert_eq!(result, Ok(vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")]));
    }

    #[tokio::test]
    async fn partial_seed_resolution_keeps_healthy_entries() {
        let raw = vec!["missing.example:7946".to_owned(), "healthy.example:7946".to_owned()];
        let result = resolve_endpoint_list_partial_with(
            &raw,
            "GRID_SWIM_SEEDS",
            Duration::from_secs(1),
            Duration::from_secs(1),
            |host, _port| {
                let host = host.to_owned();
                async move {
                    if host == "missing.example" {
                        Err(io::Error::new(io::ErrorKind::NotFound, "not found"))
                    } else {
                        Ok(vec![addr("10.0.0.2:7946")])
                    }
                }
            },
        )
        .await;
        assert_eq!(result.addresses, vec![addr("10.0.0.2:7946")]);
        assert_eq!(result.failures.len(), 1);
        assert!(result.is_degraded());
    }

    #[tokio::test]
    async fn partial_seed_resolution_sorts_deduplicates_and_keeps_parse_failures() {
        let raw = vec![
            "healthy.example:7946".to_owned(),
            "bad".to_owned(),
            "other.example:7946".to_owned(),
        ];
        let result = resolve_endpoint_list_partial_with(
            &raw,
            "GridNetwork.spec.seeds",
            Duration::from_secs(1),
            Duration::from_secs(1),
            |host, _port| {
                let host = host.to_owned();
                async move {
                    Ok(if host == "healthy.example" {
                        vec![addr("10.0.0.2:7946"), addr("10.0.0.1:7946")]
                    } else {
                        vec![addr("10.0.0.1:7946")]
                    })
                }
            },
        )
        .await;
        assert_eq!(result.addresses, vec![addr("10.0.0.1:7946"), addr("10.0.0.2:7946")]);
        assert_eq!(result.failures.len(), 1);
        assert!(
            result
                .failures
                .first()
                .is_some_and(|failure| failure.reason.contains("invalid endpoint"))
        );
    }

    #[tokio::test]
    async fn all_failed_nonempty_seed_list_is_degraded_but_empty_is_valid() {
        let failed = resolve_endpoint_list_partial_with(
            &["missing.example:7946".to_owned()],
            "GridNetwork.spec.seeds",
            Duration::from_secs(1),
            Duration::from_secs(1),
            |_host, _port| async { Err(io::Error::new(io::ErrorKind::NotFound, "not found")) },
        )
        .await;
        assert!(failed.configured);
        assert!(failed.addresses.is_empty());
        assert!(failed.is_degraded());

        let empty = resolve_endpoint_list_partial(&[], "GridNetwork.spec.seeds").await;
        assert!(!empty.configured);
        assert!(empty.addresses.is_empty());
        assert!(!empty.is_degraded());
    }

    #[tokio::test]
    async fn timed_out_seed_does_not_discard_other_success() {
        let raw = vec!["slow.example:7946".to_owned(), "fast.example:7946".to_owned()];
        let result = resolve_endpoint_list_partial_with(
            &raw,
            "GRID_SWIM_SEEDS",
            Duration::from_millis(20),
            Duration::from_millis(100),
            |host, _port| {
                let host = host.to_owned();
                async move {
                    if host == "slow.example" {
                        std::future::pending::<io::Result<Vec<SocketAddr>>>().await
                    } else {
                        Ok(vec![addr("10.0.0.3:7946")])
                    }
                }
            },
        )
        .await;
        assert_eq!(result.addresses, vec![addr("10.0.0.3:7946")]);
        assert_eq!(result.failures.len(), 1);
        assert!(
            result
                .failures
                .first()
                .is_some_and(|failure| failure.reason.contains("timed out"))
        );
    }

    #[tokio::test]
    async fn reports_dns_failure_with_source_and_endpoint() {
        let endpoint = "missing.example:7946"
            .parse::<SwimEndpoint>()
            .unwrap_or_else(|_| std::process::abort());
        let result = resolve_endpoint_with(
            &endpoint,
            "GRID_SWIM_ADVERTISE_ADDR",
            Duration::from_secs(1),
            |_host, _port| async { Err(io::Error::new(io::ErrorKind::NotFound, "not found")) },
        )
        .await;
        let error = result.err().unwrap_or_else(|| std::process::abort());
        assert!(error.contains("GRID_SWIM_ADVERTISE_ADDR"));
        assert!(error.contains("missing.example:7946"));
    }

    #[tokio::test]
    async fn reports_dns_timeout() {
        let endpoint = "slow.example:7946"
            .parse::<SwimEndpoint>()
            .unwrap_or_else(|_| std::process::abort());
        let result = resolve_endpoint_with(
            &endpoint,
            "GRID_SWIM_SEEDS",
            Duration::from_millis(1),
            |_host, _port| async { std::future::pending::<io::Result<Vec<SocketAddr>>>().await },
        )
        .await;
        let error = result.err().unwrap_or_else(|| std::process::abort());
        assert!(error.contains("GRID_SWIM_SEEDS"));
        assert!(error.contains("timed out"));
    }
}
