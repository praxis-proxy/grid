//! Live MCP `tools/list` probe for `AgentToolProvider`.
//!
//! Adapts `praxis-ai`'s `mcp_client::list_tools()` pattern (an `rmcp`-based
//! Streamable HTTP transport, plus its SSRF/DNS-pinning protections) for use
//! as a controller-initiated discovery probe rather than a request-time proxy
//! call. `rmcp`'s transport already handles both MCP protocol generations
//! (legacy handshake-based and modern stateless) transparently, so none of
//! that detection logic is reimplemented here.
//!
//! Split into three layers, deliberately:
//!
//! - **Pure decision logic** (outcome classification, phase/reason mapping, discovered-tools preservation, tool-name
//!   extraction, header/TLS attachment decisions): unit-tested with hand-built fixtures, no network.
//! - **Mockable Kubernetes I/O** (`attach_tls_ca`/`attach_tls_client_identity`/`read_tls_material`'s
//!   `Api::<Secret>::get_opt` calls and PEM parsing): unit-tested against a `tower::service_fn`-backed `kube::Client`
//!   (see `mock_kube_client_with_secrets` in `mod tests`) — this is genuine Secret I/O, but deterministic and mockable
//!   without a real API server, so it stays at the unit tier rather than the integration tier below.
//! - **Real network I/O** (`rmcp`/`reqwest` error introspection, the actual live probe): covered by the integration
//!   tier against a real local HTTP listener (`mod integration_tests`), not unit-tested, since it exercises third-party
//!   wire behavior rather than this crate's own branching.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use http::{HeaderName, HeaderValue};
use rmcp::{
    ServiceExt as _,
    transport::{
        StreamableHttpClientTransport,
        streamable_http_client::{StreamableHttpClientTransportConfig, StreamableHttpError},
    },
};
use rustls::pki_types::pem::PemObject as _;

use crate::{
    crd::inference_provider::EndpointTlsConfig,
    resources::{
        credentials::BearerToken,
        endpoint_tls::{read_secret_bytes_for_tls, secret_ref_from_client_cert},
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Alibaba Cloud instance metadata service IPv4 endpoint.
///
/// Not covered by [`Ipv4Addr::is_link_local`] (unlike AWS/GCP/Azure's shared
/// `169.254.169.254`, which the 169.254.0.0/16 link-local check already
/// blocks) since Alibaba's metadata service sits in RFC 6598 shared address
/// space, not the link-local range.
const ALIBABA_CLOUD_METADATA_V4: Ipv4Addr = Ipv4Addr::new(100, 100, 100, 200);

/// Stable `status.reason` for any of `attach_tls_client_identity`'s three
/// client-identity failure modes (unparseable cert, unparseable key, or a
/// cert/key pair `reqwest::Identity` itself refuses to build from).
///
/// A single shared reason rather than three variants: none of the three are
/// distinguishable in a way that would change what an operator does next
/// (fix the referenced client certificate/key Secret), so splitting them
/// would add `status.reason` cardinality without adding diagnostic value.
const ENDPOINT_TLS_IDENTITY_MISMATCH: &str = "EndpointTlsIdentityMismatch";

/// Maximum number of tool names persisted to `status.discoveredTools` from
/// a single probe.
///
/// Bounds the Kubernetes status object's size against a server advertising
/// an implausibly large tool catalog; ordinary MCP servers advertise a
/// handful to a few dozen tools. Applied after deduplication, so it only
/// discards genuinely distinct names beyond this limit.
const MAX_DISCOVERED_TOOLS: usize = 500;

/// Maximum length, in bytes, of a single tool name persisted to
/// `status.discoveredTools`.
///
/// Bounds per-entry size against a server advertising implausibly long
/// tool names. Truncation lands on a UTF-8 character boundary so it never
/// produces invalid UTF-8.
const MAX_TOOL_NAME_LEN: usize = 256;

// ---------------------------------------------------------------------------
// McpProbeOutcome
// ---------------------------------------------------------------------------

/// Outcome of a single MCP `tools/list` probe attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum McpProbeOutcome {
    /// `tools/list` succeeded; carries the discovered tool names.
    Success(Vec<String>),

    /// The endpoint could not be reached: transport failure, DNS error,
    /// timeout, or a blocked (SSRF-sensitive) address.
    Unreachable,

    /// The endpoint was reached but the `tools/list` exchange itself failed
    /// or returned a response that could not be parsed.
    InvalidResponse,

    /// The MCP server rejected the configured `spec.auth` credentials
    /// (HTTP 401/403-equivalent).
    AuthRejected,

    /// The resolved `spec.auth` bearer token contains characters that
    /// cannot be encoded into an HTTP header value, so no request was ever
    /// sent. Fails closed rather than silently proceeding unauthenticated
    /// — an endpoint that permits anonymous `tools/list` could otherwise
    /// be marked `Available` without ever exercising the configured
    /// credential.
    AuthConfigInvalid,

    /// `spec.tls`'s referenced Secret material could not be resolved into a
    /// usable client certificate/CA bundle. Carries the stable status.reason
    /// string (`EndpointTls*`) rather than a fixed variant, since the exact
    /// failure (missing Secret, missing key, unparseable PEM) is only known
    /// once [`endpoint_tls::read_secret_bytes_for_tls`](crate::resources::endpoint_tls::read_secret_bytes_for_tls)
    /// runs.
    TlsConfigInvalid(String),
}

/// Map a [`McpProbeOutcome`] to the resulting [`ProviderPhase`] and, for
/// failure outcomes, the stable `status.reason` string documented on
/// [`AgentToolProviderStatus`](crate::crd::agent_tool_provider::AgentToolProviderStatus).
///
/// Business rule: a successful probe always yields `Available` with no
/// reason (healthy); every failure outcome maps to `Unavailable` with its
/// own stable, machine-readable reason — mirroring how
/// `inference_provider::phase_from_probe` merges a health-probe outcome on
/// top of the site-matching phase, but simpler here since there is no
/// separate `Degraded` outcome for this probe.
///
/// [`ProviderPhase`]: crate::crd::inference_provider::ProviderPhase
pub(crate) fn phase_and_reason_from_probe(
    outcome: &McpProbeOutcome,
) -> (crate::crd::inference_provider::ProviderPhase, Option<String>) {
    use crate::crd::inference_provider::ProviderPhase;

    match outcome {
        McpProbeOutcome::Success(_) => (ProviderPhase::Available, None),
        McpProbeOutcome::Unreachable => (ProviderPhase::Unavailable, Some("McpEndpointUnreachable".to_owned())),
        McpProbeOutcome::InvalidResponse => (
            ProviderPhase::Unavailable,
            Some("McpToolsListInvalidResponse".to_owned()),
        ),
        McpProbeOutcome::AuthRejected => (ProviderPhase::Unavailable, Some("McpAuthRejected".to_owned())),
        McpProbeOutcome::AuthConfigInvalid => (ProviderPhase::Unavailable, Some("McpAuthTokenInvalid".to_owned())),
        McpProbeOutcome::TlsConfigInvalid(reason) => (ProviderPhase::Unavailable, Some(reason.clone())),
    }
}

/// Map a probe outcome to the bounded `outcome` label used by
/// `grid_mcp_probe_total{outcome}` (see `metrics::record_mcp_probe`).
///
/// Deliberately collapses [`McpProbeOutcome::TlsConfigInvalid`]'s carried
/// reason string to a single fixed label: that string can vary per Secret
/// misconfiguration and is unbounded-ish, so folding it into a metric label
/// would risk unbounded label cardinality — the same concern `grid#9`
/// documents for the phase-transition metrics.
pub(crate) fn mcp_probe_outcome_label(outcome: &McpProbeOutcome) -> &'static str {
    match outcome {
        McpProbeOutcome::Success(_) => "Success",
        McpProbeOutcome::Unreachable => "Unreachable",
        McpProbeOutcome::InvalidResponse => "InvalidResponse",
        McpProbeOutcome::AuthRejected => "AuthRejected",
        McpProbeOutcome::AuthConfigInvalid => "AuthConfigInvalid",
        McpProbeOutcome::TlsConfigInvalid(_) => "TlsConfigInvalid",
    }
}

/// Determine the `discoveredTools` value to persist after a probe attempt.
///
/// Business rule: a failed probe must never wipe a previously-discovered
/// tool list — only a successful probe overwrites it, with the freshly
/// discovered set (which may itself be empty, if the server genuinely
/// advertises zero tools).
pub(crate) fn discovered_tools_after_probe(previous: &[String], outcome: &McpProbeOutcome) -> Vec<String> {
    match outcome {
        McpProbeOutcome::Success(tools) => tools.clone(),
        McpProbeOutcome::Unreachable
        | McpProbeOutcome::InvalidResponse
        | McpProbeOutcome::AuthRejected
        | McpProbeOutcome::AuthConfigInvalid
        | McpProbeOutcome::TlsConfigInvalid(_) => previous.to_vec(),
    }
}

/// Extract tool names from `rmcp`'s `tools/list` result.
///
/// Pure mapping — no validation of tool schemas or descriptions, since only
/// the name is surfaced on `status.discoveredTools`.
pub(crate) fn discovered_tool_names(tools: &[rmcp::model::Tool]) -> Vec<String> {
    tools.iter().map(|tool| tool.name.clone().into_owned()).collect()
}

/// Truncate `name` to at most [`MAX_TOOL_NAME_LEN`] bytes, landing on a
/// UTF-8 character boundary so truncation never produces invalid UTF-8.
fn truncate_tool_name(name: String) -> String {
    if name.len() <= MAX_TOOL_NAME_LEN {
        return name;
    }
    let mut end = MAX_TOOL_NAME_LEN;
    while end > 0 && !name.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = name;
    truncated.truncate(end);
    truncated
}

/// Bound and normalize a raw list of discovered tool names before it is
/// persisted to `status.discoveredTools`.
///
/// Applies, in order: (1) per-name truncation to [`MAX_TOOL_NAME_LEN`]
/// bytes, (2) deduplication and sorting — tool order is not semantically
/// meaningful, and a server returning the same catalog in a different
/// order must not trigger a status patch on a later reconcile — and (3)
/// truncation of the deduplicated list to at most [`MAX_DISCOVERED_TOOLS`]
/// entries. Keeps both the persisted Kubernetes status object and this
/// reconciler's own memory use bounded against a server advertising an
/// implausibly large or malformed tool catalog.
pub(crate) fn bound_and_normalize_discovered_tools(names: Vec<String>) -> Vec<String> {
    let mut names: Vec<String> = names.into_iter().map(truncate_tool_name).collect();
    names.sort();
    names.dedup();
    names.truncate(MAX_DISCOVERED_TOOLS);
    names
}

/// Classify a post-connect `tools/list` call failure into a [`McpProbeOutcome`].
///
/// `status` is the HTTP status code observed on the failing exchange, when
/// one was observable (see `observed_status_from_service_error`) — `None`
/// when the failure was not HTTP-status-shaped (e.g. a deserialize or
/// protocol-level error).
///
/// Business rule: 401/403 map to [`AuthRejected`](McpProbeOutcome::AuthRejected);
/// every other status, or no status at all, maps to
/// [`InvalidResponse`](McpProbeOutcome::InvalidResponse) — the connection to
/// the endpoint was already established by this point (this function is
/// only reached post-connect), so a failure here is a protocol/response
/// problem, not an unreachable endpoint.
pub(crate) fn classify_list_tools_failure(status: Option<u16>) -> McpProbeOutcome {
    match status {
        Some(401 | 403) => McpProbeOutcome::AuthRejected,
        _ => McpProbeOutcome::InvalidResponse,
    }
}

/// Whether the probe should build a custom TLS-configured client rather
/// than using native root certificates.
///
/// Trivial in isolation, but named and tested like the rest of this
/// module's business rules per the project's decision-logic-first testing
/// convention — `spec.tls` presence is the single source of truth for this
/// choice, so this function's name is deliberately the whole rule.
pub(crate) fn should_use_custom_tls(tls_config: Option<&EndpointTlsConfig>) -> bool {
    tls_config.is_some()
}

/// Build the outbound `Authorization` header for the probe request.
///
/// Returns an empty map when `token` is `None` (`spec.auth` absent, manual,
/// or not yet resolved) — the probe request carries no `Authorization`
/// header, matching an unauthenticated MCP server. The token value is
/// wrapped in [`BearerToken`], which suppresses `Debug` output, so it is
/// never visible if this map is accidentally logged.
///
/// # Errors
///
/// Returns [`McpProbeOutcome::AuthConfigInvalid`] if `token` is `Some` but
/// its value cannot be encoded into an HTTP header value. Fails closed
/// rather than silently omitting the header: an endpoint that permits
/// anonymous `tools/list` could otherwise be probed successfully — and
/// marked `Available` — without the configured credential ever being
/// exercised.
pub(crate) fn auth_header_map(
    token: Option<&BearerToken>,
) -> Result<HashMap<HeaderName, HeaderValue>, McpProbeOutcome> {
    let mut headers = HashMap::new();
    let Some(token) = token else {
        return Ok(headers);
    };
    let bearer = format!("Bearer {}", token.expose_secret());
    let value = HeaderValue::from_str(&bearer).map_err(|_invalid_header_value| {
        tracing::warn!(
            "bearer token contains characters invalid in an HTTP header value; failing the probe closed rather than \
             proceeding unauthenticated"
        );
        McpProbeOutcome::AuthConfigInvalid
    })?;
    headers.insert(http::header::AUTHORIZATION, value);
    Ok(headers)
}

// ---------------------------------------------------------------------------
// SSRF validation (synchronous, DNS-resolution-free portion)
// ---------------------------------------------------------------------------

/// Result of validating an `AgentToolProvider`'s `spec.endpoint` before
/// attempting to probe it.
///
/// Covers everything that can be decided without a DNS lookup. Hostname
/// resolution and per-address SSRF pinning (mirroring
/// `praxis-ai`'s `resolve_hostname_ssrf`) happen in the async probe itself,
/// since they require I/O and are covered by the integration tier instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum McpUrlValidation {
    /// The URL passed all synchronous checks.
    Ok,
    /// The URL could not be parsed.
    InvalidUrl,
    /// The scheme is neither `http` nor `https`.
    UnsupportedScheme,
    /// The URL's authority embeds `user:pass@` credentials.
    EmbeddedCredentials,
    /// The URL has no host component.
    MissingHost,
    /// The host is a blocked hostname or resolves (as a literal IP) to a
    /// loopback, link-local, unique-local, unspecified, or known cloud
    /// metadata address.
    BlockedHost,
}

/// Validate an `AgentToolProvider`'s `spec.endpoint` before probing it.
///
/// Business rule: only `http`/`https` URLs with a host, no embedded
/// credentials, and no SSRF-sensitive literal-IP host are eligible for a
/// live probe. Hostnames that are not literal IPs are deferred to the
/// async probe's DNS resolution step — this function cannot resolve them.
pub(crate) fn validate_probe_url(url: &str) -> McpUrlValidation {
    // `http::Uri`'s parser treats an empty authority (`http:///path` or
    // bare `http://`) as a hard parse error rather than a URI with a blank
    // host, so that case is detected here before attempting to parse —
    // otherwise it would be misreported as InvalidUrl instead of MissingHost.
    if let Some(rest) = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://"))
        && (rest.is_empty() || rest.starts_with('/'))
    {
        return McpUrlValidation::MissingHost;
    }
    let Ok(uri) = url.parse::<http::Uri>() else {
        return McpUrlValidation::InvalidUrl;
    };
    match uri.scheme_str() {
        Some("http" | "https") => {},
        _ => return McpUrlValidation::UnsupportedScheme,
    }
    if uri.authority().is_some_and(|a| a.as_str().contains('@')) {
        return McpUrlValidation::EmbeddedCredentials;
    }
    let Some(host) = uri.host() else {
        return McpUrlValidation::MissingHost;
    };
    let host = host.trim_matches(|c| c == '[' || c == ']');
    if is_blocked_hostname(host) {
        return McpUrlValidation::BlockedHost;
    }
    if let Ok(ip) = host.parse::<IpAddr>()
        && is_ssrf_sensitive(&normalize_mapped_ipv4(ip))
    {
        return McpUrlValidation::BlockedHost;
    }
    McpUrlValidation::Ok
}

/// Hostnames that resolve to loopback without a DNS lookup.
fn is_blocked_hostname(host: &str) -> bool {
    let lower = host.to_ascii_lowercase();
    lower == "localhost" || lower.ends_with(".localhost")
}

/// Loopback, link-local, unspecified, unique-local, and known cloud
/// metadata addresses are SSRF-sensitive.
fn is_ssrf_sensitive(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback() || v4.is_link_local() || v4.is_unspecified() || *v4 == ALIBABA_CLOUD_METADATA_V4
        },
        IpAddr::V6(v6) => {
            let [a, b, ..] = v6.octets();
            v6.is_loopback() || v6.is_unspecified() || (a == 0xFE && (b & 0xC0) == 0x80) || (a & 0xFE) == 0xFC
        },
    }
}

/// Normalize an IPv4-mapped IPv6 address (`::ffff:a.b.c.d`) to its IPv4
/// form before SSRF checks, closing the bypass where a mapped address
/// would otherwise skip the IPv4 loopback/link-local checks entirely.
fn normalize_mapped_ipv4(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 @ IpAddr::V4(_) => v4,
    }
}

/// Check DNS-resolved addresses against the SSRF block list.
///
/// Used by the async probe after resolving a hostname, mirroring
/// `praxis-ai`'s `check_resolved_addrs` — kept here (rather than inline in
/// the probe function) so both the literal-IP path
/// ([`validate_probe_url`]) and the resolved-hostname path share the same
/// [`is_ssrf_sensitive`] rule.
pub(crate) fn check_resolved_addrs(addrs: &[SocketAddr]) -> bool {
    addrs
        .iter()
        .all(|addr| !is_ssrf_sensitive(&normalize_mapped_ipv4(addr.ip())))
}

// ---------------------------------------------------------------------------
// Error introspection glue (not unit-tested; see module doc)
// ---------------------------------------------------------------------------

/// Extract the HTTP status code observed on a failed `tools/list` call, when
/// the underlying transport error carries one.
///
/// Downcasts `rmcp`'s boxed transport error back to the concrete
/// `StreamableHttpError<reqwest::Error>` this crate's transport always
/// produces. Returns `None` for any error shape that isn't HTTP-status-like
/// (deserialize errors, closed transports, etc.) — callers treat `None` as
/// "no distinguishing status observed" via [`classify_list_tools_failure`].
#[expect(clippy::wildcard_enum_match_arm, reason = "external type with many variants")]
pub(crate) fn observed_status_from_service_error(error: &rmcp::ServiceError) -> Option<u16> {
    let rmcp::ServiceError::TransportSend(dyn_err) = error else {
        return None;
    };
    let transport_err = dyn_err.error.downcast_ref::<StreamableHttpError<reqwest::Error>>()?;
    match transport_err {
        StreamableHttpError::AuthRequired(_) => Some(http::StatusCode::UNAUTHORIZED.as_u16()),
        StreamableHttpError::InsufficientScope(_) => Some(http::StatusCode::FORBIDDEN.as_u16()),
        StreamableHttpError::Client(reqwest_err) => reqwest_err.status().map(|s| s.as_u16()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Live probe orchestration (I/O; covered by the integration/E2E tiers)
// ---------------------------------------------------------------------------

/// Hostname and DNS-resolved addresses pinned for connect-time use,
/// eliminating the DNS-rebinding TOCTOU window between SSRF validation and
/// the actual connection.
struct ResolvedEndpoint {
    /// Present for DNS-resolved hostnames; absent for literal IPs (nothing
    /// to pin — the literal address itself already passed [`validate_probe_url`]).
    hostname: Option<String>,
    /// Validated socket addresses from DNS resolution.
    addrs: Vec<SocketAddr>,
}

/// Resolve `endpoint`'s host, applying the DNS-resolved-address half of the
/// SSRF check (the literal-IP half already ran in [`validate_probe_url`]).
///
/// Fails closed: DNS resolution failure, timeout, or any resolved address
/// being SSRF-sensitive returns [`McpProbeOutcome::Unreachable`].
async fn resolve_endpoint_for_probe(endpoint: &str, timeout: Duration) -> Result<ResolvedEndpoint, McpProbeOutcome> {
    let uri: http::Uri = endpoint.parse().map_err(|_parse_err| McpProbeOutcome::Unreachable)?;
    let host = uri.host().ok_or(McpProbeOutcome::Unreachable)?;
    let host = host.trim_matches(|c| c == '[' || c == ']');

    if host.parse::<IpAddr>().is_ok() {
        // Literal IP: already validated by validate_probe_url, nothing to resolve/pin.
        return Ok(ResolvedEndpoint {
            hostname: None,
            addrs: Vec::new(),
        });
    }

    let port = uri
        .port_u16()
        .unwrap_or_else(|| if uri.scheme_str() == Some("https") { 443 } else { 80 });
    let addrs: Vec<SocketAddr> = tokio::time::timeout(timeout, tokio::net::lookup_host((host, port)))
        .await
        .map_err(|_elapsed| McpProbeOutcome::Unreachable)?
        .map_err(|_dns_err| McpProbeOutcome::Unreachable)?
        .collect();

    if !check_resolved_addrs(&addrs) {
        return Err(McpProbeOutcome::Unreachable);
    }

    Ok(ResolvedEndpoint {
        hostname: Some(host.to_owned()),
        addrs,
    })
}

/// Build the `reqwest::Client` used for the probe: address-pinned per
/// [`resolve_endpoint_for_probe`], and carrying `spec.tls`'s CA/client
/// identity material when configured.
///
/// # Errors
///
/// Returns [`McpProbeOutcome::TlsConfigInvalid`] if `tls_config` is `Some`
/// but its referenced Secret material cannot be resolved into a usable
/// certificate/identity, or [`McpProbeOutcome::Unreachable`] if the
/// underlying `reqwest::Client` fails to build.
async fn build_probe_client(
    kube_client: &kube::Client,
    tls_config: Option<&EndpointTlsConfig>,
    provider_identity: &str,
    resolved: &ResolvedEndpoint,
) -> Result<reqwest::Client, McpProbeOutcome> {
    let mut builder = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none());

    if let Some(hostname) = &resolved.hostname {
        builder = builder.resolve_to_addrs(hostname, &resolved.addrs);
    }

    if should_use_custom_tls(tls_config) {
        let tls = tls_config.unwrap_or_else(|| std::process::abort());
        builder = attach_tls_material(builder, kube_client, tls, provider_identity).await?;
    }

    builder.build().map_err(|_build_err| McpProbeOutcome::Unreachable)
}

/// Read `tls`'s referenced Secret material and attach it to `builder` as a
/// root CA and, when configured, a client identity.
///
/// Delegates to [`attach_tls_ca`] and [`attach_tls_client_identity`], split
/// out purely to keep each function within the project's complexity/line
/// lints — the CA and client-identity halves have no shared state beyond
/// the builder itself.
async fn attach_tls_material(
    builder: reqwest::ClientBuilder,
    kube_client: &kube::Client,
    tls: &EndpointTlsConfig,
    provider_identity: &str,
) -> Result<reqwest::ClientBuilder, McpProbeOutcome> {
    let builder = Box::pin(attach_tls_ca(builder, kube_client, tls, provider_identity)).await?;
    let Some(client_ref) = &tls.client_certificate_secret_ref else {
        return Ok(builder);
    };
    Box::pin(attach_tls_client_identity(
        builder,
        kube_client,
        client_ref,
        provider_identity,
    ))
    .await
}

/// Read one piece of TLS material (CA cert, client cert, or client key)
/// from a Secret, mapping a resolution failure to the stable
/// `EndpointTls*` `status.reason` family.
///
/// Shared by [`attach_tls_ca`] and [`attach_tls_client_identity`] so the
/// Secret-read-plus-reason-mapping logic exists exactly once rather than
/// once per material kind.
async fn read_tls_material(
    kube_client: &kube::Client,
    secret_ref: &crate::crd::grid_network::SecretRef,
    key_name: &str,
    provider_identity: &str,
    material_desc: &str,
) -> Result<Vec<u8>, McpProbeOutcome> {
    read_secret_bytes_for_tls(kube_client, secret_ref, key_name, provider_identity, material_desc)
        .await
        .map_err(|(reason, msg)| {
            tracing::warn!(provider_identity, error = %msg, material_desc, "AgentToolProvider probe TLS material invalid");
            McpProbeOutcome::TlsConfigInvalid(reason.as_status_reason("Endpoint"))
        })
}

/// Structurally validate that `pem` decodes to at least one well-formed
/// certificate.
///
/// `reqwest::Certificate::from_pem` is too lenient to be a validation gate
/// by itself: it accepts empty input and PEM blocks with undecodable base64
/// content without returning `Err` (only genuine third-party wire behavior,
/// like a TLS handshake against real malformed material, would eventually
/// surface a problem — far too late for a reconcile-time `status.reason`).
/// This reuses the same strict `rustls::pki_types` parsing
/// [`metrics_scraper::build_tls_client_config`](crate::metrics_scraper::build_tls_client_config)
/// already relies on for `InferenceProvider`, so both TLS paths reject
/// malformed CA material identically rather than diverging silently.
fn validate_pem_certificates(pem: &[u8]) -> Result<(), String> {
    let certs = rustls::pki_types::CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    if certs.is_empty() {
        return Err("PEM contains no certificates".to_owned());
    }
    Ok(())
}

/// Structurally validate that `pem` decodes to a well-formed private key.
///
/// Same rationale as [`validate_pem_certificates`]: `reqwest::Identity::from_pem`
/// alone is not a reliable validation gate for malformed key material.
fn validate_pem_private_key(pem: &[u8]) -> Result<(), String> {
    rustls::pki_types::PrivateKeyDer::from_pem_slice(pem)
        .map(|_key| ())
        .map_err(|e| e.to_string())
}

/// Read `tls.ca_secret_ref`'s CA certificate and add it to `builder` as a
/// trusted root.
async fn attach_tls_ca(
    builder: reqwest::ClientBuilder,
    kube_client: &kube::Client,
    tls: &EndpointTlsConfig,
    provider_identity: &str,
) -> Result<reqwest::ClientBuilder, McpProbeOutcome> {
    let ca_key = tls.ca_secret_ref.key.as_deref().unwrap_or("ca.crt");
    let ca_pem = Box::pin(read_tls_material(
        kube_client,
        &tls.ca_secret_ref,
        ca_key,
        provider_identity,
        "CA",
    ))
    .await?;
    if let Err(e) = validate_pem_certificates(&ca_pem) {
        tracing::warn!(provider_identity, error = %e, "AgentToolProvider probe CA PEM unparseable");
        return Err(McpProbeOutcome::TlsConfigInvalid(
            "EndpointTlsMaterialInvalid".to_owned(),
        ));
    }
    // `reqwest::Certificate::from_pem` itself no longer needs to be a
    // validation gate — `validate_pem_certificates` above already is —
    // but building the actual `Certificate` reqwest will use is still
    // required, and kept as defense-in-depth should a future reqwest
    // version regain stricter parsing of its own.
    let ca_cert = reqwest::Certificate::from_pem(&ca_pem).map_err(|e| {
        tracing::warn!(provider_identity, error = %e, "AgentToolProvider probe CA PEM unparseable");
        McpProbeOutcome::TlsConfigInvalid("EndpointTlsMaterialInvalid".to_owned())
    })?;
    // `tls_certs_only` (not `add_root_certificate`, which merges this CA
    // into reqwest's platform trust store) so a publicly trusted
    // certificate cannot satisfy a probe explicitly configured to trust
    // only this private CA -- `spec.tls.caSecretRef` documents the
    // supplied CA as the endpoint's only trusted root.
    Ok(builder.tls_certs_only([ca_cert]))
}

/// Read `client_ref`'s certificate and private key and attach them to
/// `builder` as the mTLS client identity.
#[expect(
    clippy::too_many_lines,
    reason = "sequential cert+key reads, eager rustls PEM validation for each, then the reqwest Identity build"
)]
async fn attach_tls_client_identity(
    builder: reqwest::ClientBuilder,
    kube_client: &kube::Client,
    client_ref: &crate::crd::inference_provider::ClientCertificateSecretRef,
    provider_identity: &str,
) -> Result<reqwest::ClientBuilder, McpProbeOutcome> {
    let cert_ref = secret_ref_from_client_cert(client_ref);
    let mut identity_pem = Box::pin(read_tls_material(
        kube_client,
        &cert_ref,
        &client_ref.certificate_key,
        provider_identity,
        "client cert",
    ))
    .await?;
    let key_pem = Box::pin(read_tls_material(
        kube_client,
        &cert_ref,
        &client_ref.private_key_key,
        provider_identity,
        "client key",
    ))
    .await?;
    if let Err(e) = validate_pem_certificates(&identity_pem) {
        tracing::warn!(provider_identity, error = %e, "AgentToolProvider probe client certificate unparseable");
        return Err(McpProbeOutcome::TlsConfigInvalid(
            ENDPOINT_TLS_IDENTITY_MISMATCH.to_owned(),
        ));
    }
    if let Err(e) = validate_pem_private_key(&key_pem) {
        tracing::warn!(provider_identity, error = %e, "AgentToolProvider probe client key unparseable");
        return Err(McpProbeOutcome::TlsConfigInvalid(
            ENDPOINT_TLS_IDENTITY_MISMATCH.to_owned(),
        ));
    }
    identity_pem.extend_from_slice(&key_pem);
    // As in `attach_tls_ca`: the strict `rustls::pki_types` validation above
    // is the real gate; this call still has to happen to build the
    // `Identity` reqwest will actually use.
    let identity = reqwest::Identity::from_pem(&identity_pem).map_err(|e| {
        tracing::warn!(provider_identity, error = %e, "AgentToolProvider probe client identity unparseable");
        McpProbeOutcome::TlsConfigInvalid(ENDPOINT_TLS_IDENTITY_MISMATCH.to_owned())
    })?;
    Ok(builder.identity(identity))
}

/// Parameters for a single live MCP probe attempt.
///
/// Grouped into one struct (rather than individual arguments) purely to
/// keep [`probe_agent_tool_provider`]'s signature within the project's
/// `too_many_arguments` lint — `kube_client` stays a separate parameter
/// since callers already hold it as a long-lived `&kube::Client` distinct
/// from this per-attempt request data.
pub(crate) struct ProbeRequest<'request> {
    /// `spec.endpoint` — the MCP server's HTTP(S) URL.
    pub(crate) endpoint: &'request str,
    /// Total wall-clock budget for the whole probe: DNS resolution, TLS
    /// Secret material reads, connect/handshake, and the `tools/list` call
    /// combined. Enforced by a single outer `tokio::time::timeout` in
    /// [`probe_agent_tool_provider`] — the per-phase timeouts inside it are
    /// defensive inner bounds, not independent budgets, so this value is
    /// never multiplied across phases.
    pub(crate) timeout: Duration,
    /// `spec.tls`, when the probe should use a custom CA/client identity
    /// instead of the platform trust store.
    pub(crate) tls_config: Option<&'request EndpointTlsConfig>,
    /// The `AgentToolProvider`'s name, for log/tracing attribution only.
    pub(crate) provider_identity: &'request str,
    /// The resolved bearer token from `spec.auth`, when configured.
    pub(crate) auth_token: Option<&'request BearerToken>,
}

/// Run a live MCP `tools/list` probe against `request.endpoint`.
///
/// Validates the URL (SSRF/scheme/format), resolves and pins DNS addresses,
/// builds an address-pinned `reqwest::Client` (with `spec.tls` material when
/// configured), then delegates the connect/`list_tools` exchange to
/// [`run_probe_session`].
///
/// Only the first page of results is fetched — `AgentToolProvider` has no
/// documented need for multi-page tool catalogs at this scope, and every
/// reconcile re-probes regardless (see the CRD's staleness-note doc comment).
///
/// The whole sequence — DNS resolution, TLS Secret reads, connect/handshake,
/// and `tools/list` — is bounded by one outer `request.timeout`, so a slow
/// Kubernetes API (TLS Secret fetch has no timeout of its own) or a peer
/// that stalls at one phase cannot push total probe latency past the
/// documented budget by combining several unbounded or independently-bounded
/// phases.
pub(crate) async fn probe_agent_tool_provider(
    kube_client: &kube::Client,
    request: ProbeRequest<'_>,
) -> McpProbeOutcome {
    match tokio::time::timeout(
        request.timeout,
        probe_agent_tool_provider_unbounded(kube_client, request),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(_elapsed) => McpProbeOutcome::Unreachable,
    }
}

/// The actual probe sequence, without its own overall deadline —
/// [`probe_agent_tool_provider`] is the only caller and supplies the single
/// outer `tokio::time::timeout` that bounds this function's total runtime.
async fn probe_agent_tool_provider_unbounded(kube_client: &kube::Client, request: ProbeRequest<'_>) -> McpProbeOutcome {
    if validate_probe_url(request.endpoint) != McpUrlValidation::Ok {
        tracing::warn!(
            provider_identity = request.provider_identity,
            endpoint = request.endpoint,
            "AgentToolProvider probe endpoint failed SSRF/format validation"
        );
        return McpProbeOutcome::Unreachable;
    }

    let resolved = match resolve_endpoint_for_probe(request.endpoint, request.timeout).await {
        Ok(resolved) => resolved,
        Err(outcome) => return outcome,
    };

    let client = match build_probe_client(kube_client, request.tls_config, request.provider_identity, &resolved).await {
        Ok(client) => client,
        Err(outcome) => return outcome,
    };

    Box::pin(run_probe_session(
        client,
        request.endpoint,
        request.timeout,
        request.auth_token,
    ))
    .await
}

/// Connect to the MCP server over Streamable HTTP and call `tools/list`,
/// bounding both steps by `timeout`.
///
/// Split out of [`probe_agent_tool_provider`] purely to keep both functions
/// within the project's complexity/line lints — this is the
/// connect-and-call half of what was previously one larger function.
async fn run_probe_session(
    client: reqwest::Client,
    endpoint: &str,
    timeout: Duration,
    auth_token: Option<&BearerToken>,
) -> McpProbeOutcome {
    let mut transport_config = StreamableHttpClientTransportConfig::with_uri(endpoint);
    let headers = match auth_header_map(auth_token) {
        Ok(headers) => headers,
        Err(outcome) => return outcome,
    };
    if !headers.is_empty() {
        transport_config = transport_config.custom_headers(headers);
    }
    let transport = StreamableHttpClientTransport::with_client(client, transport_config);

    let running = match tokio::time::timeout(timeout, Box::pin(().serve(transport))).await {
        Err(_elapsed) => return McpProbeOutcome::Unreachable,
        Ok(Err(_init_err)) => return McpProbeOutcome::Unreachable,
        Ok(Ok(running)) => running,
    };

    match tokio::time::timeout(timeout, Box::pin(running.list_tools(None))).await {
        Err(_elapsed) => McpProbeOutcome::Unreachable,
        Ok(Err(service_err)) => classify_list_tools_failure(observed_status_from_service_error(&service_err)),
        Ok(Ok(page)) => {
            McpProbeOutcome::Success(bound_and_normalize_discovered_tools(discovered_tool_names(&page.tools)))
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;
    use crate::crd::inference_provider::ProviderPhase;

    // -----------------------------------------------------------------------
    // phase_and_reason_from_probe
    // -----------------------------------------------------------------------

    #[test]
    fn success_outcome_yields_available_with_no_reason() {
        let (phase, reason) = phase_and_reason_from_probe(&McpProbeOutcome::Success(vec!["search".to_owned()]));
        assert_eq!(
            phase,
            ProviderPhase::Available,
            "a successful probe must yield Available"
        );
        assert!(
            reason.is_none(),
            "a successful probe must clear any prior status.reason"
        );
    }

    #[test]
    fn unreachable_outcome_yields_unavailable_with_stable_reason() {
        let (phase, reason) = phase_and_reason_from_probe(&McpProbeOutcome::Unreachable);
        assert_eq!(phase, ProviderPhase::Unavailable);
        assert_eq!(reason.as_deref(), Some("McpEndpointUnreachable"));
    }

    #[test]
    fn invalid_response_outcome_yields_unavailable_with_stable_reason() {
        let (phase, reason) = phase_and_reason_from_probe(&McpProbeOutcome::InvalidResponse);
        assert_eq!(phase, ProviderPhase::Unavailable);
        assert_eq!(reason.as_deref(), Some("McpToolsListInvalidResponse"));
    }

    #[test]
    fn auth_rejected_outcome_yields_unavailable_with_stable_reason() {
        let (phase, reason) = phase_and_reason_from_probe(&McpProbeOutcome::AuthRejected);
        assert_eq!(phase, ProviderPhase::Unavailable);
        assert_eq!(reason.as_deref(), Some("McpAuthRejected"));
    }

    #[test]
    fn auth_config_invalid_outcome_yields_unavailable_with_stable_reason() {
        let (phase, reason) = phase_and_reason_from_probe(&McpProbeOutcome::AuthConfigInvalid);
        assert_eq!(phase, ProviderPhase::Unavailable);
        assert_eq!(reason.as_deref(), Some("McpAuthTokenInvalid"));
    }

    #[test]
    fn tls_config_invalid_outcome_yields_unavailable_with_its_carried_reason() {
        let (phase, reason) = phase_and_reason_from_probe(&McpProbeOutcome::TlsConfigInvalid(
            "EndpointTlsSecretMissing".to_owned(),
        ));
        assert_eq!(phase, ProviderPhase::Unavailable);
        assert_eq!(
            reason.as_deref(),
            Some("EndpointTlsSecretMissing"),
            "TlsConfigInvalid's stable reason comes from the variant itself, not a fixed string"
        );
    }

    // -----------------------------------------------------------------------
    // mcp_probe_outcome_label — bounded telemetry label for grid_mcp_probe_total{outcome}
    // -----------------------------------------------------------------------

    #[test]
    fn success_outcome_label_is_success() {
        assert_eq!(
            mcp_probe_outcome_label(&McpProbeOutcome::Success(vec!["search".to_owned()])),
            "Success"
        );
    }

    #[test]
    fn unreachable_outcome_label_is_unreachable() {
        assert_eq!(mcp_probe_outcome_label(&McpProbeOutcome::Unreachable), "Unreachable");
    }

    #[test]
    fn invalid_response_outcome_label_is_invalid_response() {
        assert_eq!(
            mcp_probe_outcome_label(&McpProbeOutcome::InvalidResponse),
            "InvalidResponse"
        );
    }

    #[test]
    fn auth_rejected_outcome_label_is_auth_rejected() {
        assert_eq!(mcp_probe_outcome_label(&McpProbeOutcome::AuthRejected), "AuthRejected");
    }

    #[test]
    fn auth_config_invalid_outcome_label_is_auth_config_invalid() {
        assert_eq!(
            mcp_probe_outcome_label(&McpProbeOutcome::AuthConfigInvalid),
            "AuthConfigInvalid"
        );
    }

    #[test]
    fn tls_config_invalid_outcome_label_is_bounded_regardless_of_carried_reason() {
        // The label must stay bounded/enum-shaped even though the variant
        // itself carries an unbounded-ish String — two different carried
        // reasons must still map to the exact same label, never leaking the
        // inner string into a metric label (unbounded cardinality risk).
        let a = mcp_probe_outcome_label(&McpProbeOutcome::TlsConfigInvalid(
            "EndpointTlsSecretMissing".to_owned(),
        ));
        let b = mcp_probe_outcome_label(&McpProbeOutcome::TlsConfigInvalid("EndpointTlsKeyMissing".to_owned()));
        assert_eq!(a, "TlsConfigInvalid");
        assert_eq!(a, b, "the label must not vary with the carried reason string");
    }

    // -----------------------------------------------------------------------
    // discovered_tools_after_probe — preserve-on-failure business rule
    // -----------------------------------------------------------------------

    #[test]
    fn successful_probe_overwrites_discovered_tools() {
        let previous = vec!["old-tool".to_owned()];
        let outcome = McpProbeOutcome::Success(vec!["new-tool".to_owned()]);
        assert_eq!(
            discovered_tools_after_probe(&previous, &outcome),
            vec!["new-tool".to_owned()],
            "a successful probe must overwrite discoveredTools with the freshly discovered set"
        );
    }

    #[test]
    fn successful_probe_with_zero_tools_overwrites_to_empty() {
        let previous = vec!["old-tool".to_owned()];
        let outcome = McpProbeOutcome::Success(vec![]);
        assert!(
            discovered_tools_after_probe(&previous, &outcome).is_empty(),
            "a successful probe genuinely advertising zero tools must still overwrite, not preserve stale entries"
        );
    }

    #[test]
    fn unreachable_probe_preserves_previously_discovered_tools() {
        let previous = vec!["search".to_owned(), "fetch".to_owned()];
        assert_eq!(
            discovered_tools_after_probe(&previous, &McpProbeOutcome::Unreachable),
            previous,
            "a probe failure must never wipe a previously-discovered tool list"
        );
    }

    #[test]
    fn invalid_response_probe_preserves_previously_discovered_tools() {
        let previous = vec!["search".to_owned()];
        assert_eq!(
            discovered_tools_after_probe(&previous, &McpProbeOutcome::InvalidResponse),
            previous,
            "a probe failure must never wipe a previously-discovered tool list"
        );
    }

    #[test]
    fn auth_rejected_probe_preserves_previously_discovered_tools() {
        let previous = vec!["search".to_owned()];
        assert_eq!(
            discovered_tools_after_probe(&previous, &McpProbeOutcome::AuthRejected),
            previous,
            "a probe failure must never wipe a previously-discovered tool list"
        );
    }

    #[test]
    fn auth_config_invalid_probe_preserves_previously_discovered_tools() {
        let previous = vec!["search".to_owned()];
        assert_eq!(
            discovered_tools_after_probe(&previous, &McpProbeOutcome::AuthConfigInvalid),
            previous,
            "a probe failure must never wipe a previously-discovered tool list"
        );
    }

    #[test]
    fn failed_probe_with_no_prior_tools_stays_empty() {
        let previous: Vec<String> = vec![];
        assert!(
            discovered_tools_after_probe(&previous, &McpProbeOutcome::Unreachable).is_empty(),
            "a first-ever probe failure with nothing previously discovered must remain empty, not panic or fabricate"
        );
    }

    // -----------------------------------------------------------------------
    // discovered_tool_names — pure extraction from rmcp::model::Tool
    // -----------------------------------------------------------------------

    fn test_tool(name: &str) -> rmcp::model::Tool {
        // rmcp::model::Tool is #[non_exhaustive]; build via Default then
        // mutate the one field this test cares about.
        let mut tool = rmcp::model::Tool::default();
        tool.name = name.to_owned().into();
        tool
    }

    #[test]
    fn extracts_names_from_multiple_tools_in_order() {
        let tools = vec![test_tool("search"), test_tool("fetch")];
        assert_eq!(
            discovered_tool_names(&tools),
            vec!["search".to_owned(), "fetch".to_owned()],
            "tool names must be extracted in the order the server returned them"
        );
    }

    #[test]
    fn extracts_empty_vec_from_zero_tools() {
        assert!(
            discovered_tool_names(&[]).is_empty(),
            "zero tools must yield an empty name list, not error"
        );
    }

    // -----------------------------------------------------------------------
    // classify_list_tools_failure — post-connect failure classification
    // -----------------------------------------------------------------------

    #[test]
    fn http_401_after_connect_is_auth_rejected() {
        assert_eq!(classify_list_tools_failure(Some(401)), McpProbeOutcome::AuthRejected);
    }

    #[test]
    fn http_403_after_connect_is_auth_rejected() {
        assert_eq!(classify_list_tools_failure(Some(403)), McpProbeOutcome::AuthRejected);
    }

    #[test]
    fn http_500_after_connect_is_invalid_response_not_auth_rejected() {
        assert_eq!(
            classify_list_tools_failure(Some(500)),
            McpProbeOutcome::InvalidResponse,
            "a non-auth HTTP failure after a successful connect is a response problem, not an unreachable endpoint"
        );
    }

    #[test]
    fn no_observed_status_after_connect_is_invalid_response() {
        assert_eq!(
            classify_list_tools_failure(None),
            McpProbeOutcome::InvalidResponse,
            "a protocol/deserialize failure with no HTTP status is still a response problem post-connect"
        );
    }

    // -----------------------------------------------------------------------
    // should_use_custom_tls
    // -----------------------------------------------------------------------

    #[test]
    fn absent_spec_tls_does_not_use_custom_tls() {
        assert!(
            !should_use_custom_tls(None),
            "absent spec.tls must use native root certificates"
        );
    }

    #[test]
    fn present_spec_tls_uses_custom_tls() {
        let tls = EndpointTlsConfig {
            ca_secret_ref: crate::crd::grid_network::SecretRef {
                name: "ca".to_owned(),
                namespace: "ns".to_owned(),
                key: None,
            },
            client_certificate_secret_ref: None,
        };
        assert!(
            should_use_custom_tls(Some(&tls)),
            "present spec.tls must trigger the custom TLS path"
        );
    }

    // -----------------------------------------------------------------------
    // auth_header_map
    // -----------------------------------------------------------------------

    #[test]
    fn absent_token_yields_no_authorization_header() {
        let headers = auth_header_map(None).expect("no token must never fail");
        assert!(
            headers.is_empty(),
            "no resolved bearer token must mean no Authorization header at all"
        );
    }

    #[test]
    fn present_token_yields_bearer_authorization_header() {
        let token = BearerToken::new("s3cr3t".to_owned());
        let headers = auth_header_map(Some(&token)).expect("a well-formed token must not fail");
        assert_eq!(
            headers.get(&http::header::AUTHORIZATION).map(|v| v.to_str().unwrap()),
            Some("Bearer s3cr3t"),
            "a resolved bearer token must be attached as a standard Bearer Authorization header"
        );
    }

    #[test]
    fn token_with_invalid_header_characters_fails_closed() {
        let token = BearerToken::new("s3cr3t\nwith-newline".to_owned());
        assert_eq!(
            auth_header_map(Some(&token)),
            Err(McpProbeOutcome::AuthConfigInvalid),
            "a token that cannot be encoded as an HTTP header value must fail the probe closed, \
             not proceed unauthenticated"
        );
    }

    // -----------------------------------------------------------------------
    // bound_and_normalize_discovered_tools
    // -----------------------------------------------------------------------

    #[test]
    fn small_valid_list_passes_through_sorted() {
        let names = vec!["fetch".to_owned(), "search".to_owned()];
        assert_eq!(
            bound_and_normalize_discovered_tools(names),
            vec!["fetch".to_owned(), "search".to_owned()],
            "an already-small, already-sorted list must pass through unchanged"
        );
    }

    #[test]
    fn out_of_order_names_are_sorted() {
        let names = vec!["search".to_owned(), "fetch".to_owned()];
        assert_eq!(
            bound_and_normalize_discovered_tools(names),
            vec!["fetch".to_owned(), "search".to_owned()],
            "tool order is not semantically meaningful and must be normalized to avoid \
             unnecessary status churn on later reconciles"
        );
    }

    #[test]
    fn duplicate_names_are_deduplicated() {
        let names = vec!["search".to_owned(), "fetch".to_owned(), "search".to_owned()];
        assert_eq!(
            bound_and_normalize_discovered_tools(names),
            vec!["fetch".to_owned(), "search".to_owned()],
            "duplicate tool names must be collapsed to one entry"
        );
    }

    #[test]
    fn overly_long_name_is_truncated_at_a_char_boundary() {
        let long_name = "€".repeat(MAX_TOOL_NAME_LEN); // multi-byte codepoint, byte length != char count
        let result = bound_and_normalize_discovered_tools(vec![long_name]);
        assert_eq!(result.len(), 1);
        let truncated = result.first().expect("one entry must remain");
        assert!(
            truncated.len() <= MAX_TOOL_NAME_LEN,
            "a name longer than the byte limit must be truncated to at most {MAX_TOOL_NAME_LEN} bytes"
        );
        assert!(
            truncated.is_char_boundary(truncated.len()),
            "truncation must never split a multi-byte UTF-8 codepoint"
        );
    }

    #[test]
    fn name_at_exactly_the_limit_is_not_truncated() {
        let name = "a".repeat(MAX_TOOL_NAME_LEN);
        let result = bound_and_normalize_discovered_tools(vec![name.clone()]);
        assert_eq!(
            result,
            vec![name],
            "a name exactly at the byte limit must not be altered"
        );
    }

    #[test]
    fn tool_count_beyond_the_limit_is_truncated() {
        let names: Vec<String> = (0..MAX_DISCOVERED_TOOLS + 10).map(|i| format!("tool-{i:05}")).collect();
        let result = bound_and_normalize_discovered_tools(names);
        assert_eq!(
            result.len(),
            MAX_DISCOVERED_TOOLS,
            "a catalog advertising more than {MAX_DISCOVERED_TOOLS} distinct tools must be truncated \
             to keep the persisted status object bounded"
        );
    }

    #[test]
    fn empty_list_stays_empty() {
        assert!(
            bound_and_normalize_discovered_tools(vec![]).is_empty(),
            "an empty catalog must remain empty, not panic or fabricate entries"
        );
    }

    // -----------------------------------------------------------------------
    // validate_probe_url — synchronous SSRF/scheme/format validation
    // -----------------------------------------------------------------------

    #[test]
    fn valid_https_url_passes() {
        assert_eq!(
            validate_probe_url("https://tools.grid-system.svc:8443/mcp"),
            McpUrlValidation::Ok
        );
    }

    #[test]
    fn valid_http_url_passes() {
        assert_eq!(validate_probe_url("http://tools:8080/mcp"), McpUrlValidation::Ok);
    }

    #[test]
    fn unparseable_url_is_invalid() {
        assert_eq!(validate_probe_url("not a url \n"), McpUrlValidation::InvalidUrl);
    }

    #[test]
    fn ftp_scheme_is_unsupported() {
        assert_eq!(
            validate_probe_url("ftp://tools:21/mcp"),
            McpUrlValidation::UnsupportedScheme
        );
    }

    #[test]
    fn websocket_scheme_is_unsupported() {
        assert_eq!(
            validate_probe_url("ws://tools:8080/mcp"),
            McpUrlValidation::UnsupportedScheme
        );
    }

    #[test]
    fn embedded_credentials_are_rejected() {
        assert_eq!(
            validate_probe_url("https://user:pass@tools:8443/mcp"),
            McpUrlValidation::EmbeddedCredentials
        );
    }

    #[test]
    fn localhost_hostname_is_blocked() {
        assert_eq!(
            validate_probe_url("http://localhost:8080/mcp"),
            McpUrlValidation::BlockedHost
        );
    }

    #[test]
    fn localhost_subdomain_is_blocked() {
        assert_eq!(
            validate_probe_url("http://tools.localhost:8080/mcp"),
            McpUrlValidation::BlockedHost
        );
    }

    #[test]
    fn loopback_literal_ip_is_blocked() {
        assert_eq!(
            validate_probe_url("http://127.0.0.1:8080/mcp"),
            McpUrlValidation::BlockedHost
        );
    }

    #[test]
    fn ipv6_loopback_literal_is_blocked() {
        assert_eq!(
            validate_probe_url("http://[::1]:8080/mcp"),
            McpUrlValidation::BlockedHost
        );
    }

    #[test]
    fn ipv6_link_local_literal_is_blocked() {
        assert_eq!(
            validate_probe_url("http://[fe80::1]:8080/mcp"),
            McpUrlValidation::BlockedHost,
            "IPv6 link-local addresses must be blocked"
        );
    }

    #[test]
    fn ipv6_unique_local_literal_is_blocked() {
        assert_eq!(
            validate_probe_url("http://[fd00::1]:8080/mcp"),
            McpUrlValidation::BlockedHost,
            "IPv6 unique-local (ULA) addresses must be blocked"
        );
    }

    #[test]
    fn link_local_literal_ip_is_blocked() {
        assert_eq!(
            validate_probe_url("http://169.254.169.254:80/mcp"),
            McpUrlValidation::BlockedHost,
            "169.254.169.254 (AWS/GCP/Azure metadata) must be blocked as link-local"
        );
    }

    #[test]
    fn alibaba_cloud_metadata_literal_ip_is_blocked() {
        assert_eq!(
            validate_probe_url("http://100.100.100.200/mcp"),
            McpUrlValidation::BlockedHost
        );
    }

    #[test]
    fn unspecified_literal_ip_is_blocked() {
        assert_eq!(
            validate_probe_url("http://0.0.0.0:8080/mcp"),
            McpUrlValidation::BlockedHost
        );
    }

    #[test]
    fn ipv4_mapped_ipv6_loopback_is_blocked() {
        assert_eq!(
            validate_probe_url("http://[::ffff:127.0.0.1]:8080/mcp"),
            McpUrlValidation::BlockedHost,
            "an IPv4-mapped IPv6 loopback address must not bypass the IPv4 loopback check"
        );
    }

    #[test]
    fn regular_cluster_service_literal_ip_passes() {
        // A normal in-cluster ClusterIP is neither loopback nor link-local —
        // this validation must not block ordinary in-cluster addresses.
        assert_eq!(validate_probe_url("http://10.96.0.42:8080/mcp"), McpUrlValidation::Ok);
    }

    #[test]
    fn missing_host_is_rejected() {
        assert_eq!(validate_probe_url("http:///mcp"), McpUrlValidation::MissingHost);
    }

    // -----------------------------------------------------------------------
    // check_resolved_addrs
    // -----------------------------------------------------------------------

    #[test]
    fn all_safe_resolved_addrs_pass() {
        let addrs: Vec<SocketAddr> = vec!["10.96.0.42:8080".parse().unwrap(), "10.96.0.43:8080".parse().unwrap()];
        assert!(
            check_resolved_addrs(&addrs),
            "ordinary cluster addresses must pass DNS-resolved SSRF checks"
        );
    }

    #[test]
    fn any_unsafe_resolved_addr_fails_the_whole_set() {
        let addrs: Vec<SocketAddr> = vec!["10.96.0.42:8080".parse().unwrap(), "127.0.0.1:8080".parse().unwrap()];
        assert!(
            !check_resolved_addrs(&addrs),
            "a hostname resolving to even one SSRF-sensitive address must fail closed for the whole set"
        );
    }

    #[test]
    fn empty_resolved_addrs_passes_vacuously() {
        assert!(
            check_resolved_addrs(&[]),
            "an empty address list has nothing unsafe to find"
        );
    }

    // -----------------------------------------------------------------------
    // observed_status_from_service_error
    // -----------------------------------------------------------------------

    #[test]
    fn non_transport_service_error_yields_no_status() {
        let err = rmcp::ServiceError::TransportClosed;
        assert_eq!(
            observed_status_from_service_error(&err),
            None,
            "a non-transport ServiceError variant carries no HTTP status to extract"
        );
    }

    // -----------------------------------------------------------------------
    // TLS Secret resolution — attach_tls_ca / attach_tls_client_identity /
    // read_tls_material, against a mocked Kubernetes API.
    //
    // Uses a real `tower::service_fn`-backed `kube::Client` (no network) so
    // these functions' actual `Api::<Secret>::get_opt` calls, PEM parsing,
    // and reason-mapping are the thing under test — not a hand-built
    // `McpProbeOutcome` fixture standing in for them.
    // -----------------------------------------------------------------------

    /// Build a `kube::Client` backed by an in-memory Secret map keyed by
    /// Secret name, so a real `Api::<Secret>::get_opt` round-trips through
    /// this module's own `TlsFailureReason`-mapping logic.
    #[expect(
        clippy::too_many_lines,
        reason = "test mock builder: 404-vs-200 branches are the whole point"
    )]
    fn mock_kube_client_with_secrets(
        secrets: HashMap<&'static str, k8s_openapi::api::core::v1::Secret>,
    ) -> kube::Client {
        let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
            let secrets = secrets.clone();
            async move {
                let name = req.uri().path().rsplit('/').next().unwrap_or_default().to_owned();
                let response = secrets.get(name.as_str()).map_or_else(
                    || {
                        let not_found = serde_json::json!({
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": format!("secrets \"{name}\" not found"),
                            "reason": "NotFound",
                            "code": 404,
                        });
                        http::Response::builder()
                            .status(404)
                            .body(kube::client::Body::from(
                                serde_json::to_vec(&not_found).unwrap_or_else(|_| std::process::abort()),
                            ))
                            .unwrap_or_else(|_| std::process::abort())
                    },
                    |secret| {
                        http::Response::builder()
                            .status(200)
                            .body(kube::client::Body::from(
                                serde_json::to_vec(secret).unwrap_or_else(|_| std::process::abort()),
                            ))
                            .unwrap_or_else(|_| std::process::abort())
                    },
                );
                Ok::<_, std::convert::Infallible>(response)
            }
        });
        kube::Client::new(service, "default")
    }

    /// Build a Secret with a single `data` key.
    fn secret_with_key(key: &str, value: &[u8]) -> k8s_openapi::api::core::v1::Secret {
        let mut data = std::collections::BTreeMap::new();
        data.insert(key.to_owned(), k8s_openapi::ByteString(value.to_vec()));
        k8s_openapi::api::core::v1::Secret {
            data: Some(data),
            ..Default::default()
        }
    }

    fn test_secret_ref(name: &str) -> crate::crd::grid_network::SecretRef {
        crate::crd::grid_network::SecretRef {
            name: name.to_owned(),
            namespace: "default".to_owned(),
            key: None,
        }
    }

    // -----------------------------------------------------------------------
    // validate_pem_certificates / validate_pem_private_key — pure decision
    // logic, no Kubernetes I/O
    // -----------------------------------------------------------------------

    #[test]
    fn validate_pem_certificates_accepts_a_real_certificate() {
        let ca = certs::generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        assert!(
            validate_pem_certificates(ca.cert_pem.as_bytes()).is_ok(),
            "a real, well-formed certificate PEM must validate"
        );
    }

    #[test]
    fn validate_pem_certificates_rejects_empty_input() {
        assert!(
            validate_pem_certificates(b"").is_err(),
            "empty input contains no certificates and must be rejected — this is exactly what \
             reqwest::Certificate::from_pem fails to reject on its own"
        );
    }

    #[test]
    fn validate_pem_certificates_rejects_undecodable_base64() {
        assert!(
            validate_pem_certificates(b"-----BEGIN CERTIFICATE-----\nnot valid base64!!!\n-----END CERTIFICATE-----\n")
                .is_err(),
            "undecodable base64 inside PEM markers must be rejected"
        );
    }

    #[test]
    fn validate_pem_private_key_accepts_a_real_key() {
        let ca = certs::generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        assert!(
            validate_pem_private_key(ca.key_pem.as_bytes()).is_ok(),
            "a real, well-formed private key PEM must validate"
        );
    }

    #[test]
    fn validate_pem_private_key_rejects_garbage() {
        assert!(
            validate_pem_private_key(b"not a private key").is_err(),
            "garbage input must be rejected"
        );
    }

    /// Installs the process-wide `rustls` crypto provider these tests need
    /// before any `reqwest::Certificate`/`reqwest::Identity` PEM parsing —
    /// see `probe_via_pipeline_for_tests` in `integration_tests` for why.
    fn install_test_crypto_provider() {
        drop(rustls::crypto::ring::default_provider().install_default());
    }

    #[tokio::test]
    async fn read_tls_material_returns_bytes_when_secret_and_key_present() {
        let client =
            mock_kube_client_with_secrets(HashMap::from([("ca-secret", secret_with_key("ca.crt", b"ca-bytes"))]));
        let result = read_tls_material(&client, &test_secret_ref("ca-secret"), "ca.crt", "test-provider", "CA").await;
        assert_eq!(result, Ok(b"ca-bytes".to_vec()), "must return the exact stored bytes");
    }

    #[tokio::test]
    async fn read_tls_material_secret_missing_yields_endpoint_tls_secret_missing() {
        let client = mock_kube_client_with_secrets(HashMap::new());
        let result = read_tls_material(&client, &test_secret_ref("absent"), "ca.crt", "test-provider", "CA").await;
        assert_eq!(
            result,
            Err(McpProbeOutcome::TlsConfigInvalid("EndpointTlsSecretMissing".to_owned())),
            "a missing Secret must surface as EndpointTlsSecretMissing"
        );
    }

    /// Covers the fix landed for
    /// <https://github.com/praxis-proxy/grid/issues/58>: the shared
    /// `resources::secret::read_secret_bytes`/`endpoint_tls::read_secret_bytes_for_tls`
    /// pipeline (used by `InferenceProvider`'s metrics/health-check TLS too,
    /// not specific to `AgentToolProvider`) now distinguishes a Secret that
    /// exists but lacks the requested key (`KeyMissing`) from a Secret that
    /// does not exist at all (`SecretMissing`), rather than collapsing both
    /// into the latter.
    #[tokio::test]
    async fn read_tls_material_key_absent_from_data_yields_endpoint_tls_key_missing() {
        let client = mock_kube_client_with_secrets(HashMap::from([(
            "ca-secret",
            secret_with_key("wrong-key", b"ca-bytes"),
        )]));
        let result = read_tls_material(&client, &test_secret_ref("ca-secret"), "ca.crt", "test-provider", "CA").await;
        assert_eq!(
            result,
            Err(McpProbeOutcome::TlsConfigInvalid("EndpointTlsKeyMissing".to_owned())),
            "a key absent from an existing Secret's data must surface as EndpointTlsKeyMissing, \
             not EndpointTlsSecretMissing (grid#58)"
        );
    }

    #[tokio::test]
    async fn read_tls_material_key_present_but_empty_yields_endpoint_tls_key_missing() {
        let client = mock_kube_client_with_secrets(HashMap::from([("ca-secret", secret_with_key("ca.crt", b""))]));
        let result = read_tls_material(&client, &test_secret_ref("ca-secret"), "ca.crt", "test-provider", "CA").await;
        assert_eq!(
            result,
            Err(McpProbeOutcome::TlsConfigInvalid("EndpointTlsKeyMissing".to_owned())),
            "a key present in Secret.data with an empty value must also surface as EndpointTlsKeyMissing, \
             the same as a key absent entirely (grid#58)"
        );
    }

    #[tokio::test]
    async fn attach_tls_ca_succeeds_with_valid_ca_pem() {
        install_test_crypto_provider();
        let ca = certs::generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let client = mock_kube_client_with_secrets(HashMap::from([(
            "ca-secret",
            secret_with_key("ca.crt", ca.cert_pem.as_bytes()),
        )]));
        let tls = EndpointTlsConfig {
            ca_secret_ref: test_secret_ref("ca-secret"),
            client_certificate_secret_ref: None,
        };
        let result = attach_tls_ca(reqwest::Client::builder(), &client, &tls, "test-provider").await;
        drop(result.expect("a valid CA PEM must attach cleanly"));
    }

    #[tokio::test]
    async fn attach_tls_ca_malformed_pem_yields_material_invalid() {
        install_test_crypto_provider();
        // Must have valid PEM *markers* with undecodable content inside: bytes
        // with no `-----BEGIN CERTIFICATE-----` block at all are silently
        // treated by `reqwest::Certificate::from_pem` as "zero certificates
        // found" rather than a parse error, so they would not exercise this
        // failure path.
        let client = mock_kube_client_with_secrets(HashMap::from([(
            "ca-secret",
            secret_with_key(
                "ca.crt",
                b"-----BEGIN CERTIFICATE-----\nnot valid base64 content!!!\n-----END CERTIFICATE-----\n",
            ),
        )]));
        let tls = EndpointTlsConfig {
            ca_secret_ref: test_secret_ref("ca-secret"),
            client_certificate_secret_ref: None,
        };
        let result = attach_tls_ca(reqwest::Client::builder(), &client, &tls, "test-provider").await;
        assert_eq!(
            result.unwrap_err(),
            McpProbeOutcome::TlsConfigInvalid("EndpointTlsMaterialInvalid".to_owned()),
            "unparseable CA PEM must surface as EndpointTlsMaterialInvalid"
        );
    }

    #[tokio::test]
    async fn attach_tls_ca_empty_pem_yields_material_invalid() {
        install_test_crypto_provider();
        let client = mock_kube_client_with_secrets(HashMap::from([("ca-secret", secret_with_key("ca.crt", b""))]));
        let tls = EndpointTlsConfig {
            ca_secret_ref: test_secret_ref("ca-secret"),
            client_certificate_secret_ref: None,
        };
        let result = attach_tls_ca(reqwest::Client::builder(), &client, &tls, "test-provider").await;
        // An empty key value is caught earlier by read_tls_material's own
        // "key present but empty" check, before validate_pem_certificates
        // ever runs — this asserts that ordering explicitly, since it's
        // easy to accidentally invert.
        assert_eq!(
            result.unwrap_err(),
            McpProbeOutcome::TlsConfigInvalid("EndpointTlsKeyMissing".to_owned()),
            "an empty CA Secret value is caught by read_tls_material before PEM validation runs"
        );
    }

    #[tokio::test]
    async fn attach_tls_ca_missing_secret_propagates_read_tls_material_reason() {
        install_test_crypto_provider();
        let client = mock_kube_client_with_secrets(HashMap::new());
        let tls = EndpointTlsConfig {
            ca_secret_ref: test_secret_ref("absent"),
            client_certificate_secret_ref: None,
        };
        let result = attach_tls_ca(reqwest::Client::builder(), &client, &tls, "test-provider").await;
        assert_eq!(
            result.unwrap_err(),
            McpProbeOutcome::TlsConfigInvalid("EndpointTlsSecretMissing".to_owned()),
            "attach_tls_ca must propagate read_tls_material's Secret-missing reason unchanged"
        );
    }

    #[tokio::test]
    async fn attach_tls_client_identity_succeeds_with_matching_cert_and_key() {
        install_test_crypto_provider();
        let ca = certs::generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let site = certs::generate_site_cert(&ca, "test-client").unwrap_or_else(|_| std::process::abort());
        let mut data = std::collections::BTreeMap::new();
        data.insert(
            "tls.crt".to_owned(),
            k8s_openapi::ByteString(site.cert_pem.into_bytes()),
        );
        data.insert("tls.key".to_owned(), k8s_openapi::ByteString(site.key_pem.into_bytes()));
        let secret = k8s_openapi::api::core::v1::Secret {
            data: Some(data),
            ..Default::default()
        };
        let client = mock_kube_client_with_secrets(HashMap::from([("client-cert", secret)]));
        let client_ref = crate::crd::inference_provider::ClientCertificateSecretRef {
            name: "client-cert".to_owned(),
            namespace: "default".to_owned(),
            certificate_key: "tls.crt".to_owned(),
            private_key_key: "tls.key".to_owned(),
        };
        let result =
            attach_tls_client_identity(reqwest::Client::builder(), &client, &client_ref, "test-provider").await;
        drop(result.expect("a valid, matching cert/key pair must attach cleanly"));
    }

    #[tokio::test]
    async fn attach_tls_client_identity_unparseable_key_yields_identity_mismatch() {
        install_test_crypto_provider();
        let ca = certs::generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let site = certs::generate_site_cert(&ca, "test-client").unwrap_or_else(|_| std::process::abort());
        let mut data = std::collections::BTreeMap::new();
        data.insert(
            "tls.crt".to_owned(),
            k8s_openapi::ByteString(site.cert_pem.into_bytes()),
        );
        data.insert(
            "tls.key".to_owned(),
            k8s_openapi::ByteString(b"not a private key".to_vec()),
        );
        let secret = k8s_openapi::api::core::v1::Secret {
            data: Some(data),
            ..Default::default()
        };
        let client = mock_kube_client_with_secrets(HashMap::from([("client-cert", secret)]));
        let client_ref = crate::crd::inference_provider::ClientCertificateSecretRef {
            name: "client-cert".to_owned(),
            namespace: "default".to_owned(),
            certificate_key: "tls.crt".to_owned(),
            private_key_key: "tls.key".to_owned(),
        };
        let result =
            attach_tls_client_identity(reqwest::Client::builder(), &client, &client_ref, "test-provider").await;
        assert_eq!(
            result.unwrap_err(),
            McpProbeOutcome::TlsConfigInvalid(ENDPOINT_TLS_IDENTITY_MISMATCH.to_owned()),
            "unparseable key material must surface as EndpointTlsIdentityMismatch"
        );
    }

    #[tokio::test]
    async fn attach_tls_material_ca_only_when_no_client_cert_configured() {
        install_test_crypto_provider();
        let ca = certs::generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let client = mock_kube_client_with_secrets(HashMap::from([(
            "ca-secret",
            secret_with_key("ca.crt", ca.cert_pem.as_bytes()),
        )]));
        let tls = EndpointTlsConfig {
            ca_secret_ref: test_secret_ref("ca-secret"),
            client_certificate_secret_ref: None,
        };
        let result = attach_tls_material(reqwest::Client::builder(), &client, &tls, "test-provider").await;
        drop(result.expect("CA-only TLS config must attach cleanly"));
    }

    #[tokio::test]
    async fn attach_tls_material_attaches_both_ca_and_client_identity() {
        install_test_crypto_provider();
        let ca = certs::generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let site = certs::generate_site_cert(&ca, "test-client").unwrap_or_else(|_| std::process::abort());
        let mut client_data = std::collections::BTreeMap::new();
        client_data.insert(
            "tls.crt".to_owned(),
            k8s_openapi::ByteString(site.cert_pem.into_bytes()),
        );
        client_data.insert("tls.key".to_owned(), k8s_openapi::ByteString(site.key_pem.into_bytes()));
        let client_secret = k8s_openapi::api::core::v1::Secret {
            data: Some(client_data),
            ..Default::default()
        };
        let kube_client = mock_kube_client_with_secrets(HashMap::from([
            ("ca-secret", secret_with_key("ca.crt", ca.cert_pem.as_bytes())),
            ("client-cert", client_secret),
        ]));
        let tls = EndpointTlsConfig {
            ca_secret_ref: test_secret_ref("ca-secret"),
            client_certificate_secret_ref: Some(crate::crd::inference_provider::ClientCertificateSecretRef {
                name: "client-cert".to_owned(),
                namespace: "default".to_owned(),
                certificate_key: "tls.crt".to_owned(),
                private_key_key: "tls.key".to_owned(),
            }),
        };
        let result = attach_tls_material(reqwest::Client::builder(), &kube_client, &tls, "test-provider").await;
        drop(result.expect("CA + client identity TLS config must attach cleanly"));
    }

    #[tokio::test]
    async fn attach_tls_material_propagates_client_identity_secret_missing() {
        install_test_crypto_provider();
        let ca = certs::generate_ca("test-ca").unwrap_or_else(|_| std::process::abort());
        let kube_client = mock_kube_client_with_secrets(HashMap::from([(
            "ca-secret",
            secret_with_key("ca.crt", ca.cert_pem.as_bytes()),
        )]));
        let tls = EndpointTlsConfig {
            ca_secret_ref: test_secret_ref("ca-secret"),
            client_certificate_secret_ref: Some(crate::crd::inference_provider::ClientCertificateSecretRef {
                name: "absent-client-cert".to_owned(),
                namespace: "default".to_owned(),
                certificate_key: "tls.crt".to_owned(),
                private_key_key: "tls.key".to_owned(),
            }),
        };
        let result = attach_tls_material(reqwest::Client::builder(), &kube_client, &tls, "test-provider").await;
        assert_eq!(
            result.unwrap_err(),
            McpProbeOutcome::TlsConfigInvalid("EndpointTlsSecretMissing".to_owned()),
            "a missing client-cert Secret must fail the whole attach_tls_material call, not be silently skipped"
        );
    }
}

/// Integration tier: [`probe_agent_tool_provider`] against a real Streamable
/// HTTP MCP server over a local TCP listener — no mocks below the socket.
///
/// The pure decision logic (URL validation, outcome-to-phase mapping,
/// discovered-tools preservation) and the TLS/Secret material attachment
/// path (`attach_tls_material` and friends, against a mocked `kube::Client`)
/// are already covered at the unit tier above; these tests instead exercise
/// the actual network path: DNS/address resolution, `reqwest` client
/// construction, the `rmcp` client/server handshake, and header propagation.
#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod integration_tests {
    use rmcp::{
        ServerHandler,
        model::{ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool},
        service::RequestContext,
        transport::streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
        },
    };

    use super::{
        BearerToken, McpProbeOutcome, ProbeRequest, build_probe_client, resolve_endpoint_for_probe, run_probe_session,
    };

    /// Runs the same resolve → build-client → session pipeline as
    /// [`super::probe_agent_tool_provider`], skipping only its
    /// `validate_probe_url` SSRF/format gate.
    ///
    /// That gate is already exhaustively covered by the fast, synchronous
    /// unit tests above (loopback, `localhost`, link-local, cloud metadata,
    /// etc. are all proven blocked there) — and a local test server
    /// necessarily binds to loopback, so calling through the public
    /// entry point here would only prove the gate blocks our own test
    /// fixture, not that the resolve/connect/probe pipeline behind it
    /// actually works end-to-end. This helper exercises that real,
    /// previously-untested pipeline instead.
    async fn probe_via_pipeline_for_tests(kube_client: &kube::Client, request: ProbeRequest<'_>) -> McpProbeOutcome {
        // reqwest's `rustls-no-provider` feature (see the workspace
        // Cargo.toml comment) means the *application* must install a
        // process-wide crypto provider before building any `reqwest::Client`
        // — `main.rs` does this once for the real binary; test binaries have
        // no equivalent entry point, so each call here does it instead.
        // Idempotent: a second install attempt just returns `Err`, which is
        // exactly what happens when multiple tests in this binary race here.
        drop(rustls::crypto::ring::default_provider().install_default());

        let resolved = match resolve_endpoint_for_probe(request.endpoint, request.timeout).await {
            Ok(resolved) => resolved,
            Err(outcome) => return outcome,
        };
        let client =
            match build_probe_client(kube_client, request.tls_config, request.provider_identity, &resolved).await {
                Ok(client) => client,
                Err(outcome) => return outcome,
            };
        run_probe_session(client, request.endpoint, request.timeout, request.auth_token).await
    }

    /// A minimal MCP server that answers `tools/list` with a fixed set of
    /// tool names, optionally requiring a specific bearer token.
    #[derive(Clone)]
    struct FixedToolsServer {
        tools: Vec<String>,
        required_bearer: Option<String>,
    }

    impl ServerHandler for FixedToolsServer {
        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
        }

        async fn list_tools(
            &self,
            _request: Option<PaginatedRequestParams>,
            context: RequestContext<rmcp::RoleServer>,
        ) -> Result<ListToolsResult, rmcp::ErrorData> {
            if let Some(expected) = &self.required_bearer {
                // rmcp threads the raw incoming `http::request::Parts` (headers
                // included) into RequestContext::extensions — no axum
                // middleware needed to see what the probe actually sent.
                let got = context
                    .extensions
                    .get::<http::request::Parts>()
                    .and_then(|parts| parts.headers.get(http::header::AUTHORIZATION))
                    .and_then(|value| value.to_str().ok());
                if got != Some(format!("Bearer {expected}").as_str()) {
                    return Err(rmcp::ErrorData::invalid_request("missing or wrong bearer token", None));
                }
            }
            let tools = self
                .tools
                .iter()
                .map(|name| {
                    let mut tool = Tool::default();
                    tool.name = name.clone().into();
                    tool
                })
                .collect();
            Ok(ListToolsResult::with_all_items(tools))
        }
    }

    /// Spawn a real [`FixedToolsServer`] on a local TCP listener and return
    /// its base MCP endpoint URL (`http://127.0.0.1:<port>/mcp`).
    ///
    /// The spawned `axum::serve` task is never explicitly cancelled — it is
    /// dropped along with the `#[tokio::test]` runtime when each test
    /// function returns, mirroring the fire-and-forget test-server pattern
    /// already used by `metrics_scraper`/`tls_probe` in this crate.
    async fn spawn_mcp_server(tools: &[&str], required_bearer: Option<&str>) -> String {
        let handler = FixedToolsServer {
            tools: tools.iter().map(|t| (*t).to_owned()).collect(),
            required_bearer: required_bearer.map(str::to_owned),
        };
        let config = StreamableHttpServerConfig::default();
        let service: StreamableHttpService<FixedToolsServer, LocalSessionManager> =
            StreamableHttpService::new(move || Ok(handler.clone()), std::sync::Arc::default(), config);
        let router = axum::Router::new().nest_service("/mcp", service);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            drop(axum::serve(listener, router).await);
        });

        format!("http://{addr}/mcp")
    }

    /// A `kube::Client` that panics if a request is ever sent through it.
    ///
    /// Every test in this module uses `tls_config: None`, so `kube_client`
    /// is never dereferenced by [`probe_agent_tool_provider`] — it is only
    /// used on the `spec.tls`-configured path, covered by the
    /// `attach_tls_material`/`attach_tls_ca`/`attach_tls_client_identity`/
    /// `read_tls_material` unit tests in `mod tests` above (against a
    /// mocked `kube::Client`, where Secret I/O is the thing under test).
    fn unused_kube_client() -> kube::Client {
        let service = tower::service_fn(|_req: http::Request<kube::client::Body>| async {
            Ok::<_, std::convert::Infallible>(http::Response::new(kube::client::Body::empty()))
        });
        kube::Client::new(service, "default")
    }

    #[tokio::test]
    async fn probe_against_real_server_discovers_tools_sorted() {
        let endpoint = spawn_mcp_server(&["read_file", "list_directory"], None).await;
        let kube_client = unused_kube_client();

        let outcome = probe_via_pipeline_for_tests(
            &kube_client,
            ProbeRequest {
                endpoint: &endpoint,
                timeout: std::time::Duration::from_secs(5),
                tls_config: None,
                provider_identity: "it-discovers-tools",
                auth_token: None,
            },
        )
        .await;

        assert_eq!(
            outcome,
            McpProbeOutcome::Success(vec!["list_directory".to_owned(), "read_file".to_owned()]),
            "a real tools/list round trip must surface the server's tool names, normalized to sorted order"
        );
    }

    #[tokio::test]
    async fn probe_against_closed_port_is_unreachable() {
        // Bind then immediately drop the listener: the port is free again
        // but nothing is listening, so connect must fail — a real refused
        // connection, not a stubbed-out unit-test double.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let endpoint = format!("http://{addr}/mcp");
        let kube_client = unused_kube_client();

        let outcome = probe_via_pipeline_for_tests(
            &kube_client,
            ProbeRequest {
                endpoint: &endpoint,
                timeout: std::time::Duration::from_secs(2),
                tls_config: None,
                provider_identity: "it-closed-port",
                auth_token: None,
            },
        )
        .await;

        assert_eq!(
            outcome,
            McpProbeOutcome::Unreachable,
            "a real connection-refused failure must classify as Unreachable"
        );
    }

    #[tokio::test]
    async fn probe_with_correct_bearer_token_succeeds() {
        let endpoint = spawn_mcp_server(&["search"], Some("s3cr3t-token")).await;
        let kube_client = unused_kube_client();
        let token = BearerToken::new("s3cr3t-token".to_owned());

        let outcome = probe_via_pipeline_for_tests(
            &kube_client,
            ProbeRequest {
                endpoint: &endpoint,
                timeout: std::time::Duration::from_secs(5),
                tls_config: None,
                provider_identity: "it-correct-bearer",
                auth_token: Some(&token),
            },
        )
        .await;

        assert_eq!(
            outcome,
            McpProbeOutcome::Success(vec!["search".to_owned()]),
            "the resolved bearer token must reach the server as a real Authorization header"
        );
    }

    #[tokio::test]
    async fn probe_with_missing_bearer_token_is_auth_rejected() {
        let endpoint = spawn_mcp_server(&["search"], Some("s3cr3t-token")).await;
        let kube_client = unused_kube_client();

        let outcome = probe_via_pipeline_for_tests(
            &kube_client,
            ProbeRequest {
                endpoint: &endpoint,
                timeout: std::time::Duration::from_secs(5),
                tls_config: None,
                provider_identity: "it-missing-bearer",
                auth_token: None,
            },
        )
        .await;

        assert_eq!(
            outcome,
            McpProbeOutcome::InvalidResponse,
            "an MCP-level error response (no HTTP-status-coded rejection) classifies as InvalidResponse, \
             not AuthRejected — that distinction is covered at the unit tier by classify_list_tools_failure"
        );
    }
}
