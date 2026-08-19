//! Provider gateway address self-discovery.
//!
//! Resolves the data-plane gateway address this operator advertises to SWIM
//! peers (populates `GridSite.spec.egress.address`): an explicit override wins,
//! else a background poller discovers the Service `LoadBalancer` address.

use std::{sync::Arc, time::Duration};

use clap::Args;
use k8s_openapi::api::core::v1::Service;
use kube::{Api, Client};

use crate::swim_runtime::SwimHandle;

/// Trims a value and rejects it when nothing remains.
///
/// Clap applies a default only when the variable is absent, so a blank one
/// would otherwise reach discovery as an empty name.
///
/// # Errors
///
/// When `raw` is blank.
fn parse_non_blank(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("must not be blank".to_owned());
    }
    Ok(trimmed.to_owned())
}

/// Gateway self-discovery configuration.
///
/// Explicit group id: clap derives it from the struct name, and duplicates
/// panic at startup.
#[derive(Args, Debug, Clone)]
#[group(id = "gateway")]
pub struct Config {
    /// Explicit gateway address (host:port); skips discovery when set.
    ///
    /// Blank means unset here, unlike the discovery fields.
    #[arg(long = "gateway-address", env = "GRID_GATEWAY_ADDRESS")]
    pub address: Option<String>,

    /// Gateway Service name to discover.
    #[arg(
        long = "gateway-service-name",
        env = "GRID_GATEWAY_SERVICE_NAME",
        default_value = "provider-gateway",
        value_parser = parse_non_blank
    )]
    pub service_name: String,

    /// Namespace of the gateway Service.
    #[arg(
        long = "gateway-namespace",
        env = "GRID_GATEWAY_NAMESPACE",
        default_value = "grid-system",
        value_parser = parse_non_blank
    )]
    pub namespace: String,

    /// Port appended to the discovered address.
    #[arg(
        long = "gateway-port",
        env = "GRID_GATEWAY_PORT",
        default_value_t = 8080,
        value_parser = clap::value_parser!(u16).range(1..=65535)
    )]
    pub port: u16,

    /// Discovery poll interval, milliseconds.
    ///
    /// Bounded 100ms..=1h: `poll_loop` sleeps on it, so zero busy-polls the API
    /// and an out-of-range value never fires.
    #[arg(
        long = "gateway-discovery-interval-ms",
        env = "GRID_GATEWAY_DISCOVERY_INTERVAL_MS",
        default_value_t = 5000,
        value_parser = clap::value_parser!(u64).range(100..=3_600_000)
    )]
    pub discovery_interval_ms: u64,
}

impl Config {
    /// Override address, blank treated as unset.
    fn address_override(&self) -> Option<&str> {
        self.address.as_deref().map(str::trim).filter(|s| !s.is_empty())
    }

    /// Poll interval as a `Duration`.
    fn discovery_interval(&self) -> Duration {
        Duration::from_millis(self.discovery_interval_ms)
    }
}

/// Resolve the gateway address: explicit override, else Service discovery.
///
/// `Ok(None)` means no address yet.
///
/// # Errors
///
/// Kubernetes API failures.
pub async fn resolve(client: &Client, config: &Config) -> Result<Option<String>, kube::Error> {
    if let Some(addr) = config.address_override() {
        tracing::info!(addr = %addr, "using explicit gateway address override");
        return Ok(Some(addr.to_owned()));
    }
    discover_from_service(client, config).await
}

/// Poll for the gateway Service address and re-announce it via SWIM.
///
/// No-op when an explicit address override is set.
pub async fn run_discovery_poller(client: Client, swim: Arc<SwimHandle>, config: Config) {
    if config.address_override().is_some() {
        tracing::info!("gateway address override set; skipping discovery poller");
        return;
    }
    let interval = config.discovery_interval();
    let interval_ms = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX);
    tracing::info!(interval_ms, "starting gateway address discovery poller");
    poll_loop(&client, &swim, interval, &config).await;
}

/// Inner polling loop; separated to satisfy clippy complexity lints.
async fn poll_loop(client: &Client, swim: &SwimHandle, interval: Duration, config: &Config) -> ! {
    loop {
        tokio::time::sleep(interval).await;
        match discover_from_service(client, config).await {
            // Re-announce even if unchanged: a peer may have joined since.
            Ok(Some(addr)) => {
                if let Err(e) = swim.set_gateway_address(Some(addr.clone())) {
                    tracing::warn!(error = %e, "failed to update gateway address on SWIM handle");
                }
            },
            Ok(_) => {},
            Err(e) => {
                tracing::warn!(error = %e, "gateway discovery poll failed; will retry");
            },
        }
    }
}

/// Look up the gateway Service and extract its `LoadBalancer` address.
async fn discover_from_service(client: &Client, config: &Config) -> Result<Option<String>, kube::Error> {
    let api: Api<Service> = Api::namespaced(client.clone(), &config.namespace);
    if let Some(svc) = api.get_opt(&config.service_name).await? {
        let addr = extract_lb_address(&svc, config.port);
        log_discovery_result(&config.service_name, &config.namespace, &addr);
        Ok(addr)
    } else {
        tracing::info!(
            service = %config.service_name,
            namespace = %config.namespace,
            "gateway Service not found; address unavailable"
        );
        Ok(None)
    }
}

/// Log the outcome of Service-based discovery.
fn log_discovery_result(service: &str, namespace: &str, addr: &Option<String>) {
    if let Some(a) = addr {
        tracing::info!(
            service = %service,
            namespace = %namespace,
            addr = %a,
            "discovered gateway address from Service"
        );
    } else {
        tracing::info!(service = %service, namespace = %namespace, "gateway Service has no LoadBalancer address yet");
    }
}

/// Extract the first `LoadBalancer` ingress address as `"<host>:<port>"`.
///
/// Prefers `.ip` over `.hostname`; `None` when there is no ingress.
pub fn extract_lb_address(svc: &Service, port: u16) -> Option<String> {
    let ingress = svc.status.as_ref()?.load_balancer.as_ref()?.ingress.as_ref()?;
    let first = ingress.first()?;
    let host = first
        .ip
        .as_deref()
        .or(first.hostname.as_deref())
        .filter(|s| !s.is_empty())?;
    Some(format!("{host}:{port}"))
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;
    use k8s_openapi::api::core::v1::{LoadBalancerIngress, LoadBalancerStatus, Service, ServiceStatus};

    use super::*;

    fn svc_with_ip(ip: &str) -> Service {
        Service {
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(vec![LoadBalancerIngress {
                        ip: Some(ip.to_owned()),
                        hostname: None,
                        ports: None,
                        ip_mode: None,
                    }]),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn svc_with_hostname(hostname: &str) -> Service {
        Service {
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(vec![LoadBalancerIngress {
                        ip: None,
                        hostname: Some(hostname.to_owned()),
                        ports: None,
                        ip_mode: None,
                    }]),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn svc_no_ingress() -> Service {
        Service {
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus { ingress: Some(vec![]) }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn svc_no_status() -> Service {
        Service::default()
    }

    /// Parse a `Config` in isolation for validation tests.
    fn parse_gateway(args: &[&str]) -> Result<Config, clap::Error> {
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            gateway: Config,
        }
        Cli::try_parse_from(std::iter::once("test").chain(args.iter().copied())).map(|c| c.gateway)
    }

    #[test]
    fn extract_ip_address() {
        assert_eq!(
            extract_lb_address(&svc_with_ip("172.19.0.5"), 8080),
            Some("172.19.0.5:8080".to_owned())
        );
    }

    #[test]
    fn extract_hostname_address() {
        assert_eq!(
            extract_lb_address(&svc_with_hostname("gateway.example.com"), 8080),
            Some("gateway.example.com:8080".to_owned())
        );
    }

    #[test]
    fn ip_preferred_over_hostname() {
        let svc = Service {
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(vec![LoadBalancerIngress {
                        ip: Some("10.0.0.1".to_owned()),
                        hostname: Some("host.example.com".to_owned()),
                        ports: None,
                        ip_mode: None,
                    }]),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(extract_lb_address(&svc, 9090), Some("10.0.0.1:9090".to_owned()));
    }

    #[test]
    fn no_ingress_returns_none() {
        assert_eq!(extract_lb_address(&svc_no_ingress(), 8080), None);
    }

    #[test]
    fn no_status_returns_none() {
        assert_eq!(extract_lb_address(&svc_no_status(), 8080), None);
    }

    #[test]
    fn custom_port() {
        assert_eq!(
            extract_lb_address(&svc_with_ip("192.168.1.1"), 443),
            Some("192.168.1.1:443".to_owned())
        );
    }

    #[test]
    fn port_and_interval_default() {
        assert!(matches!(parse_gateway(&[]), Ok(g) if g.port == 8080 && g.discovery_interval_ms == 5000));
    }

    #[test]
    fn valid_port_accepted() {
        assert!(matches!(parse_gateway(&["--gateway-port", "443"]), Ok(g) if g.port == 443));
    }

    #[test]
    fn zero_port_rejected() {
        assert!(parse_gateway(&["--gateway-port", "0"]).is_err());
    }

    #[test]
    fn out_of_range_port_rejected() {
        assert!(parse_gateway(&["--gateway-port", "99999"]).is_err());
    }

    #[test]
    fn non_numeric_port_rejected() {
        assert!(parse_gateway(&["--gateway-port", "abc"]).is_err());
    }

    #[test]
    fn zero_interval_rejected() {
        assert!(parse_gateway(&["--gateway-discovery-interval-ms", "0"]).is_err());
    }

    #[test]
    fn below_floor_interval_rejected() {
        assert!(parse_gateway(&["--gateway-discovery-interval-ms", "99"]).is_err());
    }

    #[test]
    fn above_ceiling_interval_rejected() {
        assert!(parse_gateway(&["--gateway-discovery-interval-ms", "3600001"]).is_err());
    }

    #[test]
    fn ceiling_interval_accepted() {
        let parsed = parse_gateway(&["--gateway-discovery-interval-ms", "3600000"]);
        assert!(matches!(parsed, Ok(g) if g.discovery_interval_ms == 3_600_000));
    }

    #[test]
    fn floor_interval_accepted() {
        let parsed = parse_gateway(&["--gateway-discovery-interval-ms", "100"]);
        assert!(matches!(parsed, Ok(g) if g.discovery_interval_ms == 100));
    }

    #[test]
    fn blank_service_name_rejected() {
        assert!(parse_gateway(&["--gateway-service-name", ""]).is_err());
    }

    #[test]
    fn whitespace_service_name_rejected() {
        assert!(parse_gateway(&["--gateway-service-name", "   "]).is_err());
    }

    #[test]
    fn blank_namespace_rejected() {
        assert!(parse_gateway(&["--gateway-namespace", ""]).is_err());
    }

    #[test]
    fn whitespace_namespace_rejected() {
        assert!(parse_gateway(&["--gateway-namespace", "\t "]).is_err());
    }

    #[test]
    fn discovery_names_are_trimmed() {
        let parsed = parse_gateway(&[
            "--gateway-service-name",
            " edge-gateway ",
            "--gateway-namespace",
            " grid ",
        ]);
        assert!(matches!(parsed, Ok(g) if g.service_name == "edge-gateway" && g.namespace == "grid"));
    }

    #[test]
    fn discovery_names_default_when_absent() {
        let parsed = parse_gateway(&[]);
        assert!(
            matches!(parsed, Ok(g) if g.service_name == "provider-gateway" && g.namespace == "grid-system"),
            "defaults still apply when the flags are not supplied"
        );
    }

    #[test]
    fn absent_address_is_unset() {
        assert!(matches!(parse_gateway(&[]), Ok(g) if g.address_override().is_none()));
    }

    #[test]
    fn blank_address_treated_as_unset() {
        assert!(matches!(parse_gateway(&["--gateway-address", "   "]), Ok(g) if g.address_override().is_none()));
    }

    #[test]
    fn address_is_trimmed() {
        assert!(
            matches!(parse_gateway(&["--gateway-address", "  10.0.0.1:8443  "]), Ok(g)
                if g.address_override() == Some("10.0.0.1:8443"))
        );
    }
}
