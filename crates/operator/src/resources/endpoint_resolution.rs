//! SSRF-safe endpoint validation, resolution, and DNS pinning.
//!
//! The MCP probe and model discovery validate every address DNS returns and
//! then connect only to that validated set. This prevents a hostname from
//! rebinding between validation and connection.

use std::{
    future, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    task::{Context, Poll},
    time::Duration,
};

use hyper_util::client::legacy::connect::dns::Name;
use tower::Service;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Alibaba Cloud instance metadata service IPv4 endpoint.
///
/// This endpoint is in RFC 6598 shared address space instead of the
/// link-local range used by the other major cloud metadata services.
pub(crate) const ALIBABA_CLOUD_METADATA_V4: Ipv4Addr = Ipv4Addr::new(100, 100, 100, 200);

/// AWS instance metadata service IPv6 endpoint.
pub(crate) const AWS_METADATA_V6: Ipv6Addr = Ipv6Addr::new(0xFD00, 0x0EC2, 0, 0, 0, 0, 0, 0x0254);

/// GCP instance metadata service IPv6 endpoint.
pub(crate) const GCP_METADATA_V6: Ipv6Addr = Ipv6Addr::new(0xFD20, 0x00CE, 0, 0, 0, 0, 0, 0x0254);

/// EKS Pod Identity agent IPv6 endpoint.
pub(crate) const EKS_POD_IDENTITY_V6: Ipv6Addr = Ipv6Addr::new(0xFD00, 0x0EC2, 0, 0, 0, 0, 0, 0x0023);

/// Akamai Cloud instance metadata service IPv6 endpoint.
pub(crate) const AKAMAI_METADATA_V6: Ipv6Addr = Ipv6Addr::new(0xFD00, 0xA9FE, 0xA9FE, 0, 0, 0, 0, 1);

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Result of validating an outbound HTTP endpoint before DNS resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EndpointUrlValidation {
    /// The URL passed all synchronous checks.
    Ok,
    /// The URL could not be parsed.
    InvalidUrl,
    /// The scheme is neither `http` nor `https`.
    UnsupportedScheme,
    /// The URL authority embeds `user:pass@` credentials.
    EmbeddedCredentials,
    /// The URL has no host component.
    MissingHost,
    /// The host is SSRF-sensitive.
    BlockedHost,
}

/// Validate an outbound endpoint before resolving it.
///
/// Only `http` and `https` URLs with a host and no embedded credentials are
/// eligible for a request; SSRF-sensitive literal or localhost-style hosts
/// are refused.
pub(crate) fn validate_endpoint_url(url: &str) -> EndpointUrlValidation {
    if let Some(rest) = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://"))
        && (rest.is_empty() || rest.starts_with('/'))
    {
        return EndpointUrlValidation::MissingHost;
    }
    let Ok(uri) = url.parse::<http::Uri>() else {
        return EndpointUrlValidation::InvalidUrl;
    };
    validate_parsed_endpoint(&uri)
}

/// Validate a parsed endpoint's scheme and authority.
fn validate_parsed_endpoint(uri: &http::Uri) -> EndpointUrlValidation {
    match uri.scheme_str() {
        Some("http" | "https") => {},
        _ => return EndpointUrlValidation::UnsupportedScheme,
    }
    if uri
        .authority()
        .is_some_and(|authority| authority.as_str().contains('@'))
    {
        return EndpointUrlValidation::EmbeddedCredentials;
    }
    validate_endpoint_host(uri.host())
}

/// Validate an endpoint host after scheme and credential validation.
fn validate_endpoint_host(host: Option<&str>) -> EndpointUrlValidation {
    let Some(host) = host else {
        return EndpointUrlValidation::MissingHost;
    };
    let host = host.trim_matches(|c| c == '[' || c == ']');
    if host.is_empty() {
        return EndpointUrlValidation::MissingHost;
    }
    if is_blocked_hostname(host) {
        return EndpointUrlValidation::BlockedHost;
    }
    if let Ok(ip) = host.parse::<IpAddr>()
        && is_ssrf_sensitive(&normalize_mapped_ipv4(ip))
    {
        return EndpointUrlValidation::BlockedHost;
    }
    EndpointUrlValidation::Ok
}

/// Whether `host` is a hostname that must never be resolved for an outbound request.
pub(crate) fn is_blocked_hostname(host: &str) -> bool {
    let lower = host.to_ascii_lowercase();
    lower == "localhost" || lower.ends_with(".localhost")
}

/// Whether no DNS-resolved address is SSRF-sensitive.
pub(crate) fn check_resolved_addrs(addrs: &[SocketAddr]) -> bool {
    addrs
        .iter()
        .all(|addr| !is_ssrf_sensitive(&normalize_mapped_ipv4(addr.ip())))
}

/// Loopback, link-local, unspecified, and cloud-metadata addresses are
/// SSRF-sensitive. Other unique-local addresses remain available for services.
fn is_ssrf_sensitive(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback() || v4.is_link_local() || v4.is_unspecified() || *v4 == ALIBABA_CLOUD_METADATA_V4
        },
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unicast_link_local()
                || *v6 == AWS_METADATA_V6
                || *v6 == GCP_METADATA_V6
                || *v6 == EKS_POD_IDENTITY_V6
                || *v6 == AKAMAI_METADATA_V6
        },
    }
}

/// Normalize an IPv4-mapped IPv6 address before applying SSRF checks.
fn normalize_mapped_ipv4(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 @ IpAddr::V4(_) => v4,
    }
}

// ---------------------------------------------------------------------------
// Resolution and pinning
// ---------------------------------------------------------------------------

/// A DNS resolution failure or blocked DNS answer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EndpointResolutionError {
    /// DNS resolution timed out.
    #[error("DNS resolution timed out")]
    DnsTimeout,
    /// DNS resolution failed.
    #[error("DNS resolution failed")]
    Dns,
    /// At least one DNS answer was SSRF-sensitive.
    #[error("DNS resolution returned a blocked address")]
    BlockedAddress,
}

/// Hostname and validated DNS answers to use for the subsequent connection.
pub(crate) struct ResolvedEndpoint {
    /// Present for a hostname; literal IP addresses need no DNS pin.
    hostname: Option<String>,
    /// Validated socket addresses returned by DNS.
    addrs: Vec<SocketAddr>,
}

impl ResolvedEndpoint {
    /// Return the hostname that needs pinning, when the endpoint used DNS.
    pub(crate) fn hostname(&self) -> Option<&str> {
        self.hostname.as_deref()
    }

    /// Return the checked socket addresses returned by DNS.
    pub(crate) fn addrs(&self) -> &[SocketAddr] {
        &self.addrs
    }

    /// Build a DNS resolver which can return only this resolution's checked addresses.
    pub(crate) fn into_pinned_resolver(self) -> PinnedResolver {
        PinnedResolver {
            hostname: self.hostname,
            addrs: self.addrs,
        }
    }
}

/// Resolve `uri` and fail closed when an answer is blocked.
///
/// The returned [`ResolvedEndpoint`] must be passed to the HTTP transport so
/// it connects to the exact checked addresses rather than resolving again.
pub(crate) async fn resolve_endpoint(
    uri: &http::Uri,
    timeout: Duration,
) -> Result<ResolvedEndpoint, EndpointResolutionError> {
    let (hostname, addrs) = lookup_endpoint(uri, timeout).await?;
    let Some(hostname) = hostname else {
        return Ok(ResolvedEndpoint { hostname: None, addrs });
    };
    resolved_hostname(&hostname, addrs)
}

/// Resolve a test endpoint without applying the outbound SSRF policy.
#[cfg(test)]
pub(crate) async fn resolve_endpoint_for_test(
    uri: &http::Uri,
    timeout: Duration,
) -> Result<ResolvedEndpoint, EndpointResolutionError> {
    let (hostname, addrs) = lookup_endpoint(uri, timeout).await?;
    Ok(ResolvedEndpoint { hostname, addrs })
}

/// Resolve the endpoint host and retain its DNS answers without applying policy.
async fn lookup_endpoint(
    uri: &http::Uri,
    timeout: Duration,
) -> Result<(Option<String>, Vec<SocketAddr>), EndpointResolutionError> {
    let host = uri.host().ok_or(EndpointResolutionError::Dns)?;
    let host = host.trim_matches(|c| c == '[' || c == ']');
    if host.parse::<IpAddr>().is_ok() {
        return Ok((None, Vec::new()));
    }
    let port = uri
        .port_u16()
        .unwrap_or_else(|| if uri.scheme_str() == Some("https") { 443 } else { 80 });
    let addrs = tokio::time::timeout(timeout, tokio::net::lookup_host((host, port)))
        .await
        .map_err(|_elapsed| EndpointResolutionError::DnsTimeout)?
        .map_err(|_dns_error| EndpointResolutionError::Dns)?
        .collect();
    Ok((Some(host.to_owned()), addrs))
}

/// Check one hostname's DNS answers and retain them for connection pinning.
fn resolved_hostname(hostname: &str, addrs: Vec<SocketAddr>) -> Result<ResolvedEndpoint, EndpointResolutionError> {
    if addrs.is_empty() {
        return Err(EndpointResolutionError::Dns);
    }
    if !check_resolved_addrs(&addrs) {
        return Err(EndpointResolutionError::BlockedAddress);
    }
    Ok(ResolvedEndpoint {
        hostname: Some(hostname.to_owned()),
        addrs,
    })
}

/// A Hyper DNS resolver pinned to the already-validated answers of one endpoint.
#[derive(Clone, Debug)]
pub(crate) struct PinnedResolver {
    /// The sole hostname this resolver may answer.
    hostname: Option<String>,
    /// Validated answers to return to Hyper.
    addrs: Vec<SocketAddr>,
}

impl Service<Name> for PinnedResolver {
    /// A mismatched hostname is a connection failure, not a reason to perform fresh DNS.
    type Error = io::Error;
    /// The retained answer is immediately available.
    type Future = future::Ready<Result<Self::Response, Self::Error>>;
    /// Hyper accepts any iterator of socket addresses from a resolver.
    type Response = std::vec::IntoIter<SocketAddr>;

    /// A pinned resolver has no asynchronous readiness state.
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    /// Return only the validated answer set for the expected hostname.
    fn call(&mut self, req: Name) -> Self::Future {
        if self.hostname.as_deref() == Some(req.as_str()) {
            return future::ready(Ok(self.addrs.clone().into_iter()));
        }
        future::ready(Err(io::Error::other("attempted DNS lookup for an unpinned hostname")))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use http::{Request, StatusCode};
    use http_body_util::Empty;
    use hyper_util::{client::legacy::Client, rt::TokioExecutor};

    use super::*;
    use crate::resources::tls_backend::build_native_connector_with_resolver;

    fn addr(value: &str) -> SocketAddr {
        value.parse().unwrap_or_else(|_| std::process::abort())
    }

    struct ControllableDns {
        answer: Mutex<SocketAddr>,
    }

    impl ControllableDns {
        fn new(answer: SocketAddr) -> Self {
            Self {
                answer: Mutex::new(answer),
            }
        }

        fn resolve(&self, hostname: &str) -> SocketAddr {
            assert_eq!(
                hostname, "models.example",
                "fixture should resolve the expected hostname"
            );
            *self.answer.lock().unwrap_or_else(|_| std::process::abort())
        }

        fn set_answer(&self, answer: SocketAddr) {
            *self.answer.lock().unwrap_or_else(|_| std::process::abort()) = answer;
        }
    }

    #[test]
    /// RFC 9110 Sections 4.2.1–4.2.2 require an HTTP authority to contain a host.
    fn empty_authority_host_is_missing_for_http_and_https() {
        for url in ["http://:80", "https://:443"] {
            assert_eq!(validate_endpoint_url(url), EndpointUrlValidation::MissingHost);
        }
    }

    #[test]
    fn unique_local_answers_stay_dialable_for_dual_stack_clusters() {
        let ula = vec![addr("[fd12:3456:789a:1::10]:8080")];
        assert!(
            check_resolved_addrs(&ula),
            "unique-local addresses carry dual-stack cluster services and must stay dialable"
        );
        assert_eq!(
            validate_endpoint_url("http://[fd00::1]:8080"),
            EndpointUrlValidation::Ok,
            "a unique-local literal must stay eligible"
        );
    }

    #[test]
    fn cloud_metadata_ipv6_answers_are_blocked() {
        for metadata_address in [
            "[fd00:ec2::254]:80",
            "[fd20:ce::254]:80",
            "[fd00:ec2::23]:80",
            "[fd00:a9fe:a9fe::1]:80",
        ] {
            let resolved_address = [addr(metadata_address)];
            assert!(
                !check_resolved_addrs(&resolved_address),
                "{metadata_address} serves cloud credentials and must stay blocked inside the unique-local range"
            );
        }
    }

    #[test]
    fn safe_dns_answers_are_retained_for_connection_pinning() {
        let addresses = vec![addr("10.96.0.42:8080"), addr("10.96.0.43:8080")];

        let resolved = resolved_hostname("models.example", addresses.clone());

        assert_eq!(resolved.unwrap_or_else(|_| std::process::abort()).addrs(), addresses);
    }

    #[test]
    fn mixed_dns_answers_are_rejected() {
        let addresses = vec![addr("10.96.0.42:8080"), addr("127.0.0.1:8080")];

        assert!(
            matches!(
                resolved_hostname("models.example", addresses),
                Err(EndpointResolutionError::BlockedAddress)
            ),
            "one blocked DNS answer must reject the entire answer set"
        );
    }

    #[tokio::test]
    async fn pinned_resolver_does_not_follow_a_later_dns_change() {
        let mut resolver = resolved_hostname("models.example", vec![addr("10.96.0.42:8080")])
            .unwrap_or_else(|_| std::process::abort())
            .into_pinned_resolver();
        let name = "models.example".parse().unwrap_or_else(|_| std::process::abort());

        let answers: Vec<SocketAddr> = resolver
            .call(name)
            .await
            .unwrap_or_else(|_| std::process::abort())
            .collect();

        assert_eq!(answers, vec![addr("10.96.0.42:8080")]);
        assert!(
            !check_resolved_addrs(&[addr("127.0.0.1:8080")]),
            "a later blocked answer cannot affect a connection already pinned to the safe answer"
        );
    }

    #[tokio::test]
    async fn pinned_client_uses_original_address_after_dns_changes() {
        #[cfg(not(feature = "fips"))]
        drop(rustls::crypto::ring::default_provider().install_default());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_| std::process::abort());
        let address = listener.local_addr().unwrap_or_else(|_| std::process::abort());
        let dns = Arc::new(ControllableDns::new(address));
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt as _;
                drop(
                    stream
                        .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                        .await,
                );
            }
        });

        let validated_address = dns.resolve("models.example");
        let mut resolver = PinnedResolver {
            hostname: Some("models.example".to_owned()),
            addrs: vec![validated_address],
        };
        let original = resolver
            .call("models.example".parse().unwrap_or_else(|_| std::process::abort()))
            .await
            .unwrap_or_else(|_| std::process::abort())
            .collect::<Vec<_>>();
        assert_eq!(
            original,
            vec![validated_address],
            "resolver must retain the original answer"
        );

        dns.set_answer(SocketAddr::new(
            validated_address.ip(),
            validated_address.port().saturating_add(1),
        ));
        let changed_address = dns.resolve("models.example");
        assert_ne!(
            changed_address, validated_address,
            "fixture DNS answer must change after validation"
        );

        let connector = build_native_connector_with_resolver(resolver).unwrap_or_else(|_| std::process::abort());
        let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new()).build(connector);
        let response = client
            .request(
                Request::get("http://models.example/")
                    .body(Empty::new())
                    .unwrap_or_else(|_| std::process::abort()),
            )
            .await
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(
            response.status(),
            StatusCode::NO_CONTENT,
            "client must use the pinned address"
        );
    }

    #[tokio::test]
    async fn pinned_resolver_rejects_a_different_hostname() {
        let mut resolver = PinnedResolver {
            hostname: Some("models.example".to_owned()),
            addrs: vec![addr("10.96.0.42:8080")],
        };
        let name = "other.example".parse().unwrap_or_else(|_| std::process::abort());

        assert!(
            resolver.call(name).await.is_err(),
            "resolver must reject an unpinned hostname"
        );
    }
}
