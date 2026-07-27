//! Provider gateway address self-discovery.
//!
//! Resolves the data-plane gateway address that this operator
//! advertises to SWIM peers.  The address is used to populate
//! `GridSite.spec.egress.address` on remote clusters.
//!
//! # Resolution order
//!
//! 1. If `GRID_GATEWAY_ADDRESS` env var is set and non-empty, use it (explicit override for testing or non-standard
//!    topologies).  No background polling runs in this case.
//! 2. Otherwise, look up a Kubernetes `LoadBalancer` Service by name and extract its external address.  A background
//!    poller retries periodically until the address appears, then continues polling and re-announcing the current
//!    address.
//!
//! # Configuration
//!
//! | Env var | Default | Purpose |
//! |---|---|---|
//! | `GRID_GATEWAY_ADDRESS` | (none) | Explicit override; skips discovery and polling |
//! | `GRID_GATEWAY_SERVICE_NAME` | `provider-gateway` | Service to look up |
//! | `GRID_GATEWAY_NAMESPACE` | `grid-system` | Namespace of the Service |
//! | `GRID_GATEWAY_PORT` | `8080` | Port to append to discovered IP |
//! | `GRID_GATEWAY_DISCOVERY_INTERVAL_MS` | `5000` | Polling interval in milliseconds |

use std::{sync::Arc, time::Duration};

use k8s_openapi::api::core::v1::Service;
use kube::{Api, Client};

use crate::swim_runtime::SwimHandle;

/// Default Service name for gateway self-discovery.
const DEFAULT_SERVICE_NAME: &str = "provider-gateway";

/// Default namespace for gateway Service lookup.
const DEFAULT_NAMESPACE: &str = "grid-system";

/// Default port appended to the discovered address.
const DEFAULT_PORT: u16 = 8080;

/// Default polling interval for gateway discovery.
const DEFAULT_DISCOVERY_INTERVAL_MS: u64 = 5000;

/// Resolve the gateway address, preferring an explicit env-var override.
///
/// Returns `None` when neither the env var nor the Service provides
/// a usable address (e.g. Service has no `LoadBalancer` ingress yet).
///
/// # Errors
///
/// Returns an error only on Kubernetes API failures.  A missing
/// Service or pending `LoadBalancer` is returned as `Ok(None)`.
pub async fn resolve(client: &Client) -> Result<Option<String>, kube::Error> {
    if let Some(addr) = env_override() {
        tracing::info!(addr = %addr, "using explicit GRID_GATEWAY_ADDRESS override");
        return Ok(Some(addr));
    }
    discover_from_service(client).await
}

/// Read the explicit `GRID_GATEWAY_ADDRESS` override from the environment.
///
/// Returns `None` when the env var is absent or blank.
pub fn env_override() -> Option<String> {
    std::env::var("GRID_GATEWAY_ADDRESS")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Parse the discovery polling interval from `GRID_GATEWAY_DISCOVERY_INTERVAL_MS`.
pub fn discovery_interval() -> Duration {
    Duration::from_millis(
        std::env::var("GRID_GATEWAY_DISCOVERY_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_DISCOVERY_INTERVAL_MS),
    )
}

/// Run the gateway address discovery poller.
///
/// Periodically resolves the Service address and re-announces it through the
/// SWIM handle. Runs until the process exits.
/// Once an address has been discovered, a later pending/missing Service
/// address is treated as transient and the last-good address is retained.
///
/// This is a no-op when `GRID_GATEWAY_ADDRESS` is set (the explicit
/// override takes precedence and never changes at runtime).
pub async fn run_discovery_poller(client: Client, swim: Arc<SwimHandle>) {
    if env_override().is_some() {
        tracing::info!("GRID_GATEWAY_ADDRESS override set; skipping discovery poller");
        return;
    }
    let interval = discovery_interval();
    let interval_ms = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX);
    tracing::info!(interval_ms, "starting gateway address discovery poller");
    poll_loop(&client, &swim, interval).await;
}

/// Inner polling loop; separated to satisfy clippy complexity/loop lints.
async fn poll_loop(client: &Client, swim: &SwimHandle, interval: Duration) -> ! {
    loop {
        tokio::time::sleep(interval).await;
        match discover_from_service(client).await {
            Ok(Some(addr)) => {
                // Re-announce an unchanged address as well. A peer may join
                // after the previous metadata broadcast, or retain an
                // invalidation key from an earlier operator instance.
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

/// Look up the provider gateway Service and extract its `LoadBalancer` address.
async fn discover_from_service(client: &Client) -> Result<Option<String>, kube::Error> {
    let service_name = env_or_default("GRID_GATEWAY_SERVICE_NAME", DEFAULT_SERVICE_NAME);
    let namespace = env_or_default("GRID_GATEWAY_NAMESPACE", DEFAULT_NAMESPACE);
    let port = std::env::var("GRID_GATEWAY_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);

    let api: Api<Service> = Api::namespaced(client.clone(), &namespace);
    if let Some(svc) = api.get_opt(&service_name).await? {
        let addr = extract_lb_address(&svc, port);
        log_discovery_result(&service_name, &namespace, &addr);
        Ok(addr)
    } else {
        tracing::info!(
            service = %service_name,
            namespace = %namespace,
            "provider gateway Service not found; gateway address unavailable"
        );
        Ok(None)
    }
}

/// Read an env var, falling back to `default` when absent or blank.
fn env_or_default(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

/// Log the outcome of Service-based discovery.
fn log_discovery_result(service: &str, namespace: &str, addr: &Option<String>) {
    if let Some(a) = addr {
        tracing::info!(
            service = %service,
            namespace = %namespace,
            addr = %a,
            "discovered provider gateway address from Service"
        );
    } else {
        tracing::info!(
            service = %service,
            namespace = %namespace,
            "provider gateway Service has no LoadBalancer address yet"
        );
    }
}

/// Extract the first `LoadBalancer` ingress address from a Service,
/// formatted as `"<ip-or-hostname>:<port>"`.
///
/// Prefers `.ip` over `.hostname`.  Returns `None` when the Service
/// has no `LoadBalancer` ingress entries.
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

    #[test]
    fn extract_ip_address() {
        let svc = svc_with_ip("172.19.0.5");
        assert_eq!(
            extract_lb_address(&svc, 8080),
            Some("172.19.0.5:8080".to_owned()),
            "should extract IP"
        );
    }

    #[test]
    fn extract_hostname_address() {
        let svc = svc_with_hostname("gateway.example.com");
        assert_eq!(
            extract_lb_address(&svc, 8080),
            Some("gateway.example.com:8080".to_owned()),
            "should fall back to hostname"
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
        assert_eq!(
            extract_lb_address(&svc, 9090),
            Some("10.0.0.1:9090".to_owned()),
            "IP should be preferred over hostname"
        );
    }

    #[test]
    fn no_ingress_returns_none() {
        assert_eq!(extract_lb_address(&svc_no_ingress(), 8080), None, "empty ingress list");
    }

    #[test]
    fn no_status_returns_none() {
        assert_eq!(extract_lb_address(&svc_no_status(), 8080), None, "no status at all");
    }

    #[test]
    fn custom_port() {
        let svc = svc_with_ip("192.168.1.1");
        assert_eq!(
            extract_lb_address(&svc, 443),
            Some("192.168.1.1:443".to_owned()),
            "custom port should be appended"
        );
    }

    #[test]
    fn default_interval_is_5s() {
        assert_eq!(discovery_interval(), Duration::from_millis(5000), "default interval");
    }
}
