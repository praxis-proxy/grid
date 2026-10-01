//! SWIM advertise endpoint: explicit, else the SWIM Service `LoadBalancer`, else the Pod IP.

use std::{
    fmt::Display,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use k8s_openapi::api::core::v1::Service;
use tokio::time::Instant;

/// Poll interval while waiting for the SWIM Service `LoadBalancer` address.
pub const LB_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Wait after which a missing `LoadBalancer` address is logged as an error.
pub const LB_PATIENCE: Duration = Duration::from_secs(180);

/// Poll interval once patience runs out, and for watching after startup.
pub const LB_SLOW_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Bound on one lookup.
pub const LB_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Named SWIM port on the chart's Service.
const SWIM_PORT_NAME: &str = "swim-udp";

/// Consecutive polls that must miss the advertised address before it counts as changed.
pub const LB_CHANGE_CONFIRMATIONS: u32 = 3;

/// Least time between warnings about failed lookups while watching.
pub const LB_ERROR_WARN_EVERY: Duration = Duration::from_secs(300);

/// Why a lookup yielded nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupError {
    /// Worth retrying.
    Retry(String),
    /// Never going to work, such as a Service that is not a `LoadBalancer`.
    Fatal(String),
}

impl Display for LookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retry(reason) | Self::Fatal(reason) => f.write_str(reason),
        }
    }
}

/// Where the advertise endpoint comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Configured endpoint text, used as given.
    Explicit(String),
    /// This Service's `LoadBalancer` address, waited for without fallback.
    LoadBalancer(String),
    /// The Pod IP, with the chart-rendered text if `POD_IP` is unusable.
    Pod(String),
    /// The socket's local address.
    Local,
}

/// Choose the advertise source. An address equal to the fallback is the chart's Pod IP marker.
#[must_use]
pub fn plan(explicit: Option<String>, fallback: Option<String>, service: Option<String>) -> Plan {
    let nonblank = |value: Option<String>| value.map(|v| v.trim().to_owned()).filter(|v| !v.is_empty());
    let (explicit, fallback) = (nonblank(explicit), nonblank(fallback));
    let chart_marker = explicit.is_some() && explicit == fallback;
    match (nonblank(service), explicit) {
        (Some(service), None) => Plan::LoadBalancer(service),
        (Some(service), Some(_)) if chart_marker => Plan::LoadBalancer(service),
        (_, Some(value)) if chart_marker => Plan::Pod(value),
        (_, Some(value)) => Plan::Explicit(value),
        (_, None) => Plan::Local,
    }
}

/// `"<lb>:<port>"` for the SWIM Service's `swim-udp` or bind port, `None` until it has ingress.
#[must_use]
pub fn lb_endpoint(svc: &Service, bind_port: u16) -> Option<String> {
    preferred_endpoint(svc, swim_port(svc, bind_port)?)
}

/// `"<host>:<port>"` for the first ingress with an IP, else the first with a hostname.
fn preferred_endpoint(svc: &Service, port: u16) -> Option<String> {
    let ingress = svc.status.as_ref()?.load_balancer.as_ref()?.ingress.as_deref()?;
    let host = ingress
        .iter()
        .find_map(|entry| entry.ip.as_deref().filter(|ip| !ip.is_empty()))
        .or_else(|| {
            ingress
                .iter()
                .find_map(|entry| entry.hostname.as_deref().filter(|host| !host.is_empty()))
        })?;
    Some(host_port(host, port))
}

/// `"<host>:<port>"`, bracketing an IPv6 host.
fn host_port(host: &str, port: u16) -> String {
    match host.parse::<IpAddr>() {
        Ok(ip) => SocketAddr::new(ip, port).to_string(),
        Err(_) => format!("{host}:{port}"),
    }
}

/// Named signals port on the chart's Service.
const SIGNALS_PORT_NAME: &str = "signals";

/// `"<lb>:<port>"` for the Service port named `signals`, `None` without that port or ingress.
#[must_use]
pub fn lb_signals_endpoint(svc: &Service) -> Option<String> {
    let port = svc
        .spec
        .as_ref()?
        .ports
        .as_ref()?
        .iter()
        .find(|p| p.name.as_deref() == Some(SIGNALS_PORT_NAME))?
        .port;
    let text = preferred_endpoint(svc, u16::try_from(port).ok()?)?;
    crate::signals::SignalsEndpoint::parse(&text).map(|endpoint| endpoint.authority())
}

/// The SWIM endpoint and the signals endpoint from one read of the Service, `None` until it has ingress.
#[must_use]
pub fn lb_advertised(svc: &Service, bind_port: u16) -> Option<(String, Option<String>)> {
    Some((lb_endpoint(svc, bind_port)?, lb_signals_endpoint(svc)))
}

/// Every ingress as `"<lb>:<port>"` on the SWIM port, in Service order.
#[must_use]
pub fn lb_endpoints(svc: &Service, bind_port: u16) -> Vec<String> {
    let Some(port) = swim_port(svc, bind_port) else {
        return Vec::new();
    };
    let ingress = svc
        .status
        .as_ref()
        .and_then(|status| status.load_balancer.as_ref())
        .and_then(|lb| lb.ingress.as_deref())
        .unwrap_or_default();
    ingress
        .iter()
        .filter_map(|entry| {
            let host = entry
                .ip
                .as_deref()
                .or(entry.hostname.as_deref())
                .filter(|h| !h.is_empty())?;
            Some(host_port(host, port))
        })
        .collect()
}

/// The Service's `swim-udp` port, else the one equal to the bind port.
fn swim_port(svc: &Service, bind_port: u16) -> Option<u16> {
    let ports = svc.spec.as_ref()?.ports.as_ref()?;
    let port = ports
        .iter()
        .find(|p| p.name.as_deref() == Some(SWIM_PORT_NAME))
        .or_else(|| ports.iter().find(|p| p.port == i32::from(bind_port)))?
        .port;
    u16::try_from(port).ok()
}

/// Refuse a Service that can never get a `LoadBalancer` address.
///
/// # Errors
///
/// Returns [`LookupError::Fatal`] naming the Service type when it is not `LoadBalancer`.
pub fn require_load_balancer(svc: &Service) -> Result<(), LookupError> {
    let kind = svc
        .spec
        .as_ref()
        .and_then(|spec| spec.type_.as_deref())
        .unwrap_or("ClusterIP");
    if kind == "LoadBalancer" {
        return Ok(());
    }
    Err(LookupError::Fatal(format!(
        "GRID_SWIM_SERVICE_NAME names a {kind} Service; it must be type LoadBalancer"
    )))
}

/// `"<pod ip>:<port>"`, bracketing IPv6.
#[must_use]
pub fn pod_endpoint(pod_ip: &str, port: u16) -> Option<String> {
    let ip: IpAddr = pod_ip.trim().parse().ok()?;
    Some(SocketAddr::new(ip, port).to_string())
}

/// Poll `lookup` until it yields a value, logging misses at error and slowing past `patience`.
///
/// # Errors
///
/// Returns the reason the first time `lookup` fails with [`LookupError::Fatal`].
pub async fn wait_for_lb<T, F, Fut>(mut lookup: F, interval: Duration, patience: Duration) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>, LookupError>>,
{
    let started = Instant::now();
    let mut attempt: u32 = 0;
    loop {
        attempt = attempt.saturating_add(1);
        let overdue = started.elapsed() >= patience;
        match poll_once(&mut lookup).await {
            Ok(found) => return Ok(found),
            Err(LookupError::Fatal(reason)) => return Err(reason),
            Err(outcome) if overdue => {
                tracing::error!(attempt, %outcome, "SWIM Service has no usable LoadBalancer address; not ready");
            },
            Err(outcome) => tracing::info!(attempt, %outcome, "SWIM Service not ready; will retry"),
        }
        tokio::time::sleep(if overdue { LB_SLOW_POLL_INTERVAL } else { interval }).await;
    }
}

/// One bounded lookup, `Err` saying why it yielded nothing.
async fn poll_once<T, F, Fut>(lookup: &mut F) -> Result<T, LookupError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>, LookupError>>,
{
    match tokio::time::timeout(LB_LOOKUP_TIMEOUT, lookup()).await {
        Ok(Ok(Some(found))) => Ok(found),
        Ok(Ok(None)) => Err(LookupError::Retry("no usable LoadBalancer address yet".to_owned())),
        Ok(Err(LookupError::Retry(error))) => Err(LookupError::Retry(format!("lookup failed: {error}"))),
        Ok(Err(fatal)) => Err(fatal),
        Err(_elapsed) => Err(LookupError::Retry("lookup timed out".to_owned())),
    }
}

/// Poll the listed ingress, not its DNS answers, until `advertised` misses [`LB_CHANGE_CONFIRMATIONS`] polls in a row.
pub async fn watch_lb<F, Fut>(mut lookup: F, advertised: &str, interval: Duration) -> Vec<String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<Vec<String>>, LookupError>>,
{
    let mut missing: u32 = 0;
    let mut warned_at: Option<Instant> = None;
    loop {
        tokio::time::sleep(interval).await;
        let Some(current) = ingress_now(&mut lookup, &mut warned_at).await else {
            continue;
        };
        if current.iter().any(|text| text == advertised) {
            missing = 0;
            continue;
        }
        missing = missing.saturating_add(1);
        tracing::warn!(%advertised, ?current, missing, "SWIM LoadBalancer no longer lists the advertised address");
        if missing >= LB_CHANGE_CONFIRMATIONS {
            return current;
        }
    }
}

/// The ingress text, `None` after a failed lookup, warned at most every [`LB_ERROR_WARN_EVERY`].
async fn ingress_now<F, Fut>(lookup: &mut F, warned_at: &mut Option<Instant>) -> Option<Vec<String>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<Vec<String>>, LookupError>>,
{
    let error = match tokio::time::timeout(LB_LOOKUP_TIMEOUT, lookup()).await {
        Ok(Ok(found)) => return Some(found.unwrap_or_default()),
        Ok(Err(error)) => error.to_string(),
        Err(_elapsed) => "lookup timed out".to_owned(),
    };
    if warned_at.is_none_or(|at| at.elapsed() >= LB_ERROR_WARN_EVERY) {
        *warned_at = Some(Instant::now());
        tracing::warn!(%error, "SWIM Service LoadBalancer lookup failed while watching");
    } else {
        tracing::debug!(%error, "SWIM Service LoadBalancer lookup failed while watching");
    }
    None
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use std::cell::Cell;

    use k8s_openapi::api::core::v1::{
        LoadBalancerIngress, LoadBalancerStatus, ServicePort, ServiceSpec, ServiceStatus,
    };

    use super::*;

    fn svc(ports: &[(Option<&str>, i32)], ingress_ip: Option<&str>) -> Service {
        Service {
            spec: Some(ServiceSpec {
                ports: Some(
                    ports
                        .iter()
                        .map(|(name, port)| ServicePort {
                            name: name.map(str::to_owned),
                            port: *port,
                            ..Default::default()
                        })
                        .collect(),
                ),
                ..Default::default()
            }),
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(
                        ingress_ip
                            .map(|ip| LoadBalancerIngress {
                                ip: Some(ip.to_owned()),
                                ..Default::default()
                            })
                            .into_iter()
                            .collect(),
                    ),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "case table")]
    fn plan_picks_the_source() {
        let own = |v: Option<&str>| v.map(str::to_owned);
        let lb = || Plan::LoadBalancer("swim".to_owned());
        let cases = [
            (
                "user address over the service",
                Some("a:1"),
                None,
                Some("swim"),
                Plan::Explicit("a:1".to_owned()),
            ),
            (
                "user address unlike the marker",
                Some("a:1"),
                Some("p:1"),
                Some("swim"),
                Plan::Explicit("a:1".to_owned()),
            ),
            (
                "user address alone",
                Some("a:1"),
                None,
                None,
                Plan::Explicit("a:1".to_owned()),
            ),
            (
                "chart marker defers to the service",
                Some("p:1"),
                Some("p:1"),
                Some("swim"),
                lb(),
            ),
            ("service alone", None, None, Some(" swim "), lb()),
            (
                "chart marker without a service",
                Some("p:1"),
                Some("p:1"),
                None,
                Plan::Pod("p:1".to_owned()),
            ),
            ("blank service is unset", None, None, Some("  "), Plan::Local),
            ("nothing configured", None, None, None, Plan::Local),
        ];
        for (name, advertise, fallback, service, want) in cases {
            assert_eq!(plan(own(advertise), own(fallback), own(service)), want, "{name}");
        }
    }

    #[test]
    fn lb_endpoint_picks_the_swim_port() {
        let cases = [
            (
                "named swim port",
                svc(&[(Some("signals"), 9091), (Some("swim-udp"), 7000)], Some("10.0.0.9")),
                Some("10.0.0.9:7000"),
            ),
            (
                "unnamed port equal to the bind port",
                svc(&[(None, 9091), (None, 7946)], Some("10.0.0.9")),
                Some("10.0.0.9:7946"),
            ),
            (
                "never the first unrelated port",
                svc(&[(Some("signals"), 9091)], Some("10.0.0.9")),
                None,
            ),
            (
                "ipv6 ingress is bracketed",
                svc(&[(Some("swim-udp"), 7946)], Some("fd00::9")),
                Some("[fd00::9]:7946"),
            ),
            ("no ingress yet", svc(&[(Some("swim-udp"), 7946)], None), None),
            ("no ports", svc(&[], Some("10.0.0.9")), None),
            ("no spec", Service::default(), None),
        ];
        for (name, service, want) in cases {
            assert_eq!(lb_endpoint(&service, 7946).as_deref(), want, "{name}");
        }
    }

    #[test]
    fn pod_endpoint_formats() {
        let cases = [
            ("ipv4", "10.1.2.3", Some("10.1.2.3:7946")),
            ("ipv6 bracketed", "fd00::1", Some("[fd00::1]:7946")),
            ("empty", "", None),
            ("unexpanded downward api", "$(POD_IP)", None),
        ];
        for (name, ip, want) in cases {
            assert_eq!(pod_endpoint(ip, 7946).as_deref(), want, "{name}");
        }
    }

    /// One scripted lookup result.
    #[derive(Clone, Copy)]
    enum Step {
        Lb,
        Pending,
        Fail,
        Hang,
        Fatal,
    }
    use Step::{Fail, Fatal, Hang, Lb, Pending};

    /// A lookup that replays `step`.
    async fn scripted(step: Step) -> Result<Option<String>, LookupError> {
        match step {
            Lb => Ok(Some("1.2.3.4:7946".to_owned())),
            Pending => Ok(None),
            Fail => Err(LookupError::Retry("forbidden".to_owned())),
            Hang => std::future::pending().await,
            Fatal => Err(LookupError::Fatal("not a LoadBalancer".to_owned())),
        }
    }

    /// Paused clock: sleeps and timeouts advance instantly.
    #[tokio::test(start_paused = true)]
    async fn wait_for_lb_never_falls_back() {
        type Case<'case> = (&'case str, &'case [Step], usize, Result<&'case str, &'case str>);
        let cases: [Case<'_>; 5] = [
            ("lb on first poll", &[Lb], 1, Ok("1.2.3.4:7946")),
            ("lb after pending", &[Pending, Pending, Lb], 3, Ok("1.2.3.4:7946")),
            ("lb after an error and a hang", &[Fail, Hang, Lb], 3, Ok("1.2.3.4:7946")),
            ("lb long past patience", &[Pending; 60], 61, Ok("1.2.3.4:7946")),
            (
                "a service that is not a load balancer",
                &[Pending, Fatal],
                2,
                Err("not a LoadBalancer"),
            ),
        ];
        for (name, steps, want_polls, want) in cases {
            let polls = Cell::new(0_usize);
            let mut script = steps.iter().copied();
            let lookup = || {
                polls.set(polls.get().saturating_add(1));
                scripted(script.next().unwrap_or(Lb))
            };
            let got = wait_for_lb(lookup, LB_POLL_INTERVAL, LB_PATIENCE).await;
            assert_eq!(got.as_deref().map_err(String::as_str), want, "{name}");
            assert_eq!(polls.get(), want_polls, "{name}: polls");
        }
    }

    /// Replays ingress sets, `None` for a failed lookup, then the advertised set forever.
    fn ingress_script(
        steps: Vec<Option<Vec<&'static str>>>,
        advertised: &'static str,
    ) -> impl FnMut() -> std::future::Ready<Result<Option<Vec<String>>, LookupError>> {
        let mut steps = steps.into_iter();
        move || {
            std::future::ready(match steps.next() {
                Some(Some(set)) => Ok(Some(set.into_iter().map(str::to_owned).collect())),
                Some(None) => Err(LookupError::Retry("forbidden".to_owned())),
                None => Ok(Some(vec![advertised.to_owned()])),
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn watch_lb_follows_the_hostname_not_its_dns() {
        let host = "nlb-1.elb.example:7946";
        let narrowed = || Some(vec![host]);
        let watched = watch_lb(ingress_script(vec![narrowed(); 5], host), host, LB_SLOW_POLL_INTERVAL);
        assert!(
            tokio::time::timeout(LB_SLOW_POLL_INTERVAL * 20, watched).await.is_err(),
            "DNS narrowing behind one hostname does not restart"
        );
        let renamed = || Some(vec!["nlb-2.elb.example:7946"]);
        let changed = watch_lb(
            ingress_script(vec![renamed(), renamed(), renamed()], host),
            host,
            LB_SLOW_POLL_INTERVAL,
        )
        .await;
        assert_eq!(changed, ["nlb-2.elb.example:7946"], "a new hostname restarts");
    }

    #[test]
    fn an_ip_ingress_is_preferred_over_an_earlier_hostname() {
        let mut service = svc(&[(Some("swim-udp"), 7946), (Some("signals"), 9091)], None);
        if let Some(lb) = service.status.as_mut().and_then(|status| status.load_balancer.as_mut()) {
            lb.ingress = Some(vec![
                LoadBalancerIngress {
                    hostname: Some("lb.example".to_owned()),
                    ..Default::default()
                },
                LoadBalancerIngress {
                    ip: Some("10.0.0.9".to_owned()),
                    ..Default::default()
                },
            ]);
        }
        assert_eq!(lb_endpoint(&service, 7946).as_deref(), Some("10.0.0.9:7946"));
        assert_eq!(lb_signals_endpoint(&service).as_deref(), Some("10.0.0.9:9091"));
    }

    #[tokio::test(start_paused = true)]
    #[expect(clippy::too_many_lines, reason = "case table of ingress sequences")]
    async fn watch_lb_restarts_only_on_a_sustained_change() {
        let advertised = "1.2.3.4:7946";
        let other = || Some(vec!["5.6.7.8:7946"]);
        let quiet = [
            ("reordered ingress", vec![Some(vec!["5.6.7.8:7946", "1.2.3.4:7946"]); 5]),
            (
                "a flap",
                vec![other(), other(), Some(vec!["1.2.3.4:7946"]), other(), other()],
            ),
            ("failed lookups", vec![None, None, None, None]),
            (
                "a failure inside a change",
                vec![other(), None, Some(vec!["1.2.3.4:7946"]), other()],
            ),
        ];
        for (name, steps) in quiet {
            let watched = watch_lb(ingress_script(steps, advertised), advertised, LB_SLOW_POLL_INTERVAL);
            let polls = u32::try_from(20_usize).expect("small");
            assert!(
                tokio::time::timeout(LB_SLOW_POLL_INTERVAL * polls, watched)
                    .await
                    .is_err(),
                "{name}: no restart"
            );
        }
        let started = Instant::now();
        let changed = watch_lb(
            ingress_script(vec![other(), other(), other()], advertised),
            advertised,
            LB_SLOW_POLL_INTERVAL,
        )
        .await;
        assert_eq!(changed, ["5.6.7.8:7946"], "sustained change");
        assert_eq!(
            started.elapsed(),
            LB_SLOW_POLL_INTERVAL * LB_CHANGE_CONFIRMATIONS,
            "after three polls"
        );
        let emptied = watch_lb(
            ingress_script(vec![Some(Vec::new()); 3], advertised),
            advertised,
            LB_SLOW_POLL_INTERVAL,
        )
        .await;
        assert!(emptied.is_empty(), "ingress removed");
    }

    #[test]
    fn only_a_load_balancer_service_is_accepted() {
        let typed = |kind: Option<&str>| Service {
            spec: Some(ServiceSpec {
                type_: kind.map(str::to_owned),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(require_load_balancer(&typed(Some("LoadBalancer"))), Ok(()));
        for kind in [Some("ClusterIP"), Some("NodePort"), None] {
            assert!(
                matches!(
                    require_load_balancer(&typed(kind)),
                    Err(LookupError::Fatal(reason)) if reason.contains("LoadBalancer")
                ),
                "{kind:?} accepted"
            );
        }
    }

    #[test]
    fn the_signals_endpoint_is_the_service_port_on_the_lb_address() {
        let cases = [
            (
                "named signals port, not the swim one",
                svc(&[(Some("swim-udp"), 7946), (Some("signals"), 19091)], Some("10.0.0.9")),
                Some("10.0.0.9:19091"),
            ),
            (
                "ipv6 ingress",
                svc(&[(Some("signals"), 9091)], Some("fd00::9")),
                Some("[fd00::9]:9091"),
            ),
            (
                "no signals port",
                svc(&[(Some("swim-udp"), 7946)], Some("10.0.0.9")),
                None,
            ),
            ("no ingress yet", svc(&[(Some("signals"), 9091)], None), None),
        ];
        for (name, service, want) in cases {
            assert_eq!(lb_signals_endpoint(&service).as_deref(), want, "{name}");
        }
    }

    #[test]
    fn the_signals_endpoint_is_read_with_the_swim_one() {
        let ports = [(Some("swim-udp"), 7946), (Some("signals"), 9091)];
        assert_eq!(lb_advertised(&svc(&ports, None), 7946), None, "waits for ingress");
        assert_eq!(
            lb_advertised(&svc(&ports, Some("10.0.0.9")), 7946),
            Some(("10.0.0.9:7946".to_owned(), Some("10.0.0.9:9091".to_owned())))
        );
        assert_eq!(
            lb_advertised(&svc(&ports[..1], Some("10.0.0.9")), 7946),
            Some(("10.0.0.9:7946".to_owned(), None)),
            "no signals port"
        );
    }

    #[test]
    fn lb_endpoints_lists_every_ingress() {
        let mut service = svc(&[(Some("swim-udp"), 7946)], Some("10.0.0.9"));
        if let Some(ingress) = service
            .status
            .as_mut()
            .and_then(|status| status.load_balancer.as_mut())
            .and_then(|lb| lb.ingress.as_mut())
        {
            ingress.push(LoadBalancerIngress {
                ip: Some("fd00::9".to_owned()),
                ..Default::default()
            });
            ingress.push(LoadBalancerIngress {
                hostname: Some("lb.example".to_owned()),
                ..Default::default()
            });
        }
        assert_eq!(
            lb_endpoints(&service, 7946),
            ["10.0.0.9:7946", "[fd00::9]:7946", "lb.example:7946"]
        );
        assert!(lb_endpoints(&svc(&[(Some("signals"), 9091)], Some("10.0.0.9")), 7946).is_empty());
    }
}
