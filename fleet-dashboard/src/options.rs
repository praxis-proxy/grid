//! Command-line options, each settable as `FLEET_<UPPER_SNAKE>` for container
//! use. `FLEET_CENTRAL_TOKEN` is deliberately not an option, so a token can
//! never appear in a process listing.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::{ArgAction, Parser, ValueEnum};

use crate::registry::default_namespace;

/// Where site metrics come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum MetricsMode {
    /// Query each site's own Prometheus with that site's token.
    #[value(name = "perSite")]
    PerSite,
    /// Query one central store, scoping by a cluster label.
    #[value(name = "central")]
    Central,
}

/// Serves the AI Grid fleet map for one hub cluster.
#[derive(Debug, Clone, Parser)]
#[command(name = "fleet-dashboard", version, about)]
pub struct Options {
    /// Address to serve HTTP on; a bare `:port` listens on every interface.
    #[arg(long, env = "FLEET_LISTEN", default_value = ":8080")]
    pub listen: String,
    /// Namespace of the registry `ConfigMap` and the site Secrets.
    #[arg(long, env = "FLEET_NAMESPACE", default_value_t = default_namespace())]
    pub namespace: String,
    /// `ConfigMap` holding the EPP file-discovery document.
    #[arg(long, env = "FLEET_REGISTRY_CONFIGMAP", default_value = "epp-clusters")]
    pub registry_configmap: String,
    /// Key inside the registry `ConfigMap`.
    #[arg(long, env = "FLEET_REGISTRY_KEY", default_value = "clusters.yaml")]
    pub registry_key: String,
    /// `perSite` or `central`.
    #[arg(long, env = "FLEET_METRICS_MODE", default_value = "perSite", value_enum)]
    pub metrics_mode: MetricsMode,
    /// Prometheus-compatible URL for central mode.
    #[arg(long, env = "FLEET_CENTRAL_URL")]
    pub central_url: Option<String>,
    /// PEM bundle to trust for the central store, besides the platform roots.
    #[arg(long, env = "FLEET_CENTRAL_CA_FILE")]
    pub central_ca_file: Option<PathBuf>,
    /// Label that identifies a site in central mode.
    #[arg(long, env = "FLEET_CLUSTER_LABEL", default_value = "cluster")]
    pub cluster_label: String,
    /// How often every site is polled, such as `15s`.
    #[arg(long, env = "FLEET_POLL_INTERVAL", default_value = "15s", value_parser = parse_duration)]
    pub poll_interval: Duration,
    /// Per-site query timeout, such as `8s`.
    #[arg(long, env = "FLEET_SITE_TIMEOUT", default_value = "8s", value_parser = parse_duration)]
    pub site_timeout: Duration,
    /// Optional config file: thresholds, hub, and query overrides.
    #[arg(long, env = "FLEET_CONFIG")]
    pub config: Option<PathBuf>,
    /// Kubeconfig path for development; in-cluster when absent.
    #[arg(long, env = "FLEET_KUBECONFIG")]
    pub kubeconfig: Option<PathBuf>,
    /// Serve a synthetic fleet instead of reading the cluster.
    #[arg(long, env = "FLEET_DEMO", action = ArgAction::Set, num_args = 0..=1, default_missing_value = "true", default_value_t = false)]
    pub demo: bool,
}

/// An option combination that cannot run.
#[derive(Debug, thiserror::Error)]
pub enum OptionsError {
    /// Faster polling would only load the spokes.
    #[error("poll-interval must be at least 5s")]
    PollIntervalTooShort,
    /// Central mode has nowhere to send queries.
    #[error("central-url is required when metrics-mode is central")]
    CentralUrlRequired,
}

impl Options {
    /// Checks the constraints the parser cannot express.
    ///
    /// # Errors
    ///
    /// [`OptionsError`] naming the offending option.
    pub fn validate(&self) -> Result<(), OptionsError> {
        if self.poll_interval < Duration::from_secs(5) {
            return Err(OptionsError::PollIntervalTooShort);
        }
        if self.metrics_mode == MetricsMode::Central && self.central_url.as_deref().is_none_or(str::is_empty) {
            return Err(OptionsError::CentralUrlRequired);
        }
        Ok(())
    }
}

/// A duration with a Go-style unit: `250ms`, `15s`, `2m`, `1h`.
///
/// # Errors
///
/// A message naming what is wrong with `text`.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let unit_at = text
        .find(|character: char| !(character.is_ascii_digit() || character == '.'))
        .ok_or_else(|| format!("{text:?}: missing unit (ms, s, m, h)"))?;
    let (number, unit) = text.split_at(unit_at);
    let value: f64 = number.parse().map_err(|err| format!("{text:?}: {err}"))?;
    let scale = match unit {
        "ms" => 0.001,
        "s" => 1.0,
        "m" => 60.0,
        "h" => 3600.0,
        _ => return Err(format!("{text:?}: unknown unit {unit:?}")),
    };
    Duration::try_from_secs_f64(value * scale).map_err(|err| format!("{text:?}: {err}"))
}

/// A listen address; a bare `:port` means every interface, as in Go.
///
/// # Errors
///
/// The address parse error for anything that is not `host:port`.
pub fn parse_listen(text: &str) -> Result<SocketAddr, std::net::AddrParseError> {
    if text.starts_with(':') {
        format!("0.0.0.0{text}").parse()
    } else {
        text.parse()
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use std::time::Duration;

    use clap::{CommandFactory as _, Parser as _};

    use super::{MetricsMode, Options, parse_duration, parse_listen};

    #[test]
    fn defaults_match_the_go_flags() {
        let options = Options::try_parse_from(["fleet-dashboard"]).unwrap();
        assert_eq!(options.listen, ":8080");
        assert_eq!(
            (options.registry_configmap.as_str(), options.registry_key.as_str()),
            ("epp-clusters", "clusters.yaml")
        );
        assert_eq!(
            (options.metrics_mode, options.cluster_label.as_str()),
            (MetricsMode::PerSite, "cluster")
        );
        assert_eq!(
            (options.poll_interval, options.site_timeout),
            (Duration::from_secs(15), Duration::from_secs(8))
        );
        assert!(
            !options.demo && options.config.is_none() && options.kubeconfig.is_none(),
            "{options:?}"
        );
        options.validate().unwrap();
    }

    #[test]
    fn every_flag_can_be_set_from_a_fleet_environment_variable() {
        for arg in Options::command()
            .get_arguments()
            .filter(|arg| arg.get_long().is_some())
        {
            let flag = arg.get_long().unwrap();
            let want = format!("FLEET_{}", flag.to_uppercase().replace('-', "_"));
            assert_eq!(
                arg.get_env().map(|env| env.to_string_lossy().into_owned()),
                Some(want),
                "--{flag}"
            );
        }
    }

    #[test]
    fn the_demo_flag_is_bare_or_explicit() {
        assert!(
            Options::try_parse_from(["fleet-dashboard", "--demo"]).unwrap().demo,
            "bare flag"
        );
        assert!(
            !Options::try_parse_from(["fleet-dashboard", "--demo=false"])
                .unwrap()
                .demo,
            "explicit false"
        );
    }

    #[test]
    fn an_unknown_metrics_mode_is_rejected_by_the_parser() {
        let err = Options::try_parse_from(["fleet-dashboard", "--metrics-mode", "weird"]).unwrap_err();
        assert!(err.to_string().contains("metrics-mode"), "{err}");
    }

    #[test]
    fn central_mode_requires_a_url() {
        let options = Options::try_parse_from(["fleet-dashboard", "--metrics-mode", "central"]).unwrap();
        let err = options.validate().unwrap_err();
        assert!(err.to_string().contains("central-url"), "{err}");
        let with_url = Options::try_parse_from([
            "fleet-dashboard",
            "--metrics-mode",
            "central",
            "--central-url",
            "https://x",
        ])
        .unwrap();
        with_url.validate().unwrap();
    }

    #[test]
    fn a_poll_interval_under_five_seconds_is_rejected() {
        let options = Options::try_parse_from(["fleet-dashboard", "--poll-interval", "1s"]).unwrap();
        let err = options.validate().unwrap_err();
        assert!(err.to_string().contains("poll-interval"), "{err}");
    }

    #[test]
    fn durations_use_go_suffixes() {
        assert_eq!(parse_duration("15s").unwrap(), Duration::from_secs(15));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("1.5s").unwrap(), Duration::from_millis(1500));
        for bad in ["", "15", "15x", "s", "-1s"] {
            assert!(parse_duration(bad).is_err(), "{bad:?} must not parse");
        }
    }

    #[test]
    fn a_bare_port_listens_on_every_interface() {
        assert_eq!(parse_listen(":8080").unwrap().to_string(), "0.0.0.0:8080");
        assert_eq!(parse_listen("127.0.0.1:9").unwrap().to_string(), "127.0.0.1:9");
        assert!(parse_listen("nope").is_err(), "a hostname is not accepted");
    }
}
