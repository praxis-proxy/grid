//! Command-line interface, parsed once at startup from flags/environment.

use std::{net::SocketAddr, time::Duration};

use clap::{Args, Parser};

use crate::{enroll, gateway};

/// grid-operator command-line interface.
#[derive(Parser, Debug, Clone)]
#[command(name = "grid-operator", about = "AI Grid Kubernetes operator")]
pub struct Cli {
    /// Gateway self-discovery options.
    #[command(flatten)]
    pub gateway: gateway::Config,

    /// Site auto-enroll options.
    #[command(flatten)]
    pub enrollment: enroll::Config,

    /// SWIM runtime options.
    #[command(flatten)]
    pub swim: SwimArgs,

    /// Signals serving and peer polling options.
    #[command(flatten)]
    pub signals: SignalsArgs,
}

/// SWIM runtime options.
#[derive(Args, Debug, Clone)]
#[group(id = "swim")]
pub struct SwimArgs {
    /// Hold all SWIM traffic until a `GridNetwork` loads the key or declares none.
    #[arg(
        long = "swim-require-key",
        env = "GRID_SWIM_REQUIRE_KEY",
        default_value_t = true,
        action = clap::ArgAction::Set,
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub require_key: bool,
}

/// Signals serving and peer polling options, used under signalTransport poll.
#[derive(Args, Debug, Clone)]
#[group(id = "signals")]
pub struct SignalsArgs {
    /// Signals listener address, else `[::]:9091` falling back to `0.0.0.0:9091`.
    #[arg(long = "signals-addr", env = "GRID_SIGNALS_ADDR")]
    pub addr: Option<SocketAddr>,

    /// Signals endpoint gossiped to peers, `ip:port`, `[ipv6]:port`, or `dns-name:port`.
    #[arg(
        long = "signals-advertise-addr",
        env = "GRID_SIGNALS_ADVERTISE_ADDR",
        value_parser = parse_signals_endpoint
    )]
    pub advertise_addr: Option<String>,

    /// Port dialed for a peer that gossips no signals endpoint.
    #[arg(
        long = "signals-peer-port",
        env = "GRID_SIGNALS_PEER_PORT",
        default_value_t = crate::signals::DEFAULT_PEER_PORT
    )]
    pub peer_port: u16,

    /// Concurrent authenticated connections one peer site may hold.
    #[arg(
        long = "signals-max-per-peer",
        env = "GRID_SIGNALS_MAX_PER_PEER",
        default_value_t = 8
    )]
    pub max_per_peer: usize,

    /// This site's own signals endpoint, for its gateway.
    #[arg(
        long = "signals-local-addr",
        env = "GRID_SIGNALS_LOCAL_ADDR",
        value_parser = parse_local_signals_endpoint
    )]
    pub local_addr: Option<String>,

    /// Local provider scrape interval, seconds.
    #[arg(
        long = "signals-scrape-interval-secs",
        env = "GRID_SIGNALS_SCRAPE_INTERVAL_SECS",
        default_value_t = 5
    )]
    pub scrape_interval_secs: u64,

    /// Local provider scrape interval, milliseconds, which wins over the seconds form.
    #[arg(long = "signals-scrape-interval-ms", env = "GRID_SIGNALS_SCRAPE_INTERVAL_MS")]
    pub scrape_interval_ms: Option<u64>,

    /// Peer poll round interval, seconds.
    #[arg(
        long = "signals-peer-interval-secs",
        env = "GRID_SIGNALS_PEER_INTERVAL_SECS",
        default_value_t = 30
    )]
    pub peer_interval_secs: u64,

    /// Per-attempt peer request timeout, seconds.
    #[arg(
        long = "signals-peer-timeout-secs",
        env = "GRID_SIGNALS_PEER_TIMEOUT_SECS",
        default_value_t = 5
    )]
    pub peer_timeout_secs: u64,

    /// Peers polled at once.
    #[arg(
        long = "signals-peer-concurrency",
        env = "GRID_SIGNALS_PEER_CONCURRENCY",
        default_value_t = 8
    )]
    pub peer_concurrency: usize,

    /// Attempts per peer per round.
    #[arg(
        long = "signals-peer-attempts",
        env = "GRID_SIGNALS_PEER_ATTEMPTS",
        default_value_t = 3
    )]
    pub peer_attempts: u32,

    /// Base retry backoff, milliseconds.
    #[arg(
        long = "signals-peer-backoff-ms",
        env = "GRID_SIGNALS_PEER_BACKOFF_MS",
        default_value_t = 50
    )]
    pub peer_backoff_ms: u64,

    /// Total time budget per peer per round, seconds.
    #[arg(
        long = "signals-peer-budget-secs",
        env = "GRID_SIGNALS_PEER_BUDGET_SECS",
        default_value_t = 10
    )]
    pub peer_budget_secs: u64,

    /// A poll slower than this is counted slow, milliseconds.
    #[arg(
        long = "signals-peer-slow-ms",
        env = "GRID_SIGNALS_PEER_SLOW_MS",
        default_value_t = 1_000
    )]
    pub peer_slow_ms: u64,

    /// How long a polled peer signal is served, seconds.
    #[arg(
        long = "signals-peer-ttl-secs",
        env = "GRID_SIGNALS_PEER_TTL_SECS",
        default_value_t = 120
    )]
    pub peer_ttl_secs: u64,

    /// Newline-separated signals asked of each peer. Empty asks for everything.
    #[arg(long = "signals-peer-collect", env = "GRID_SIGNALS_PEER_COLLECT", default_value = "")]
    pub peer_collect: String,
}

/// A signals endpoint in its normalized `host:port` form.
fn parse_signals_endpoint(text: &str) -> Result<String, String> {
    crate::signals::SignalsEndpoint::parse(text.trim())
        .map(|endpoint| endpoint.authority())
        .ok_or_else(|| format!("{text:?} is not ip:port, [ipv6]:port, or dns-name:port"))
}

/// [`parse_signals_endpoint`], keeping a blank value as unset.
fn parse_local_signals_endpoint(text: &str) -> Result<String, String> {
    if text.trim().is_empty() {
        return Ok(String::new());
    }
    parse_signals_endpoint(text)
}

impl SignalsArgs {
    /// Local scrape interval, floored at 50ms.
    #[must_use]
    pub fn scrape_interval(&self) -> Duration {
        let ms = self
            .scrape_interval_ms
            .unwrap_or_else(|| self.scrape_interval_secs.saturating_mul(1_000));
        Duration::from_millis(ms.max(50))
    }

    /// Peer signal lifetime, at least one second.
    #[must_use]
    pub fn peer_ttl(&self) -> Duration {
        Duration::from_secs(self.peer_ttl_secs.max(1))
    }

    /// Signals asked of each peer.
    #[must_use]
    pub fn peer_collect(&self) -> Vec<String> {
        self.peer_collect
            .lines()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect()
    }

    /// This site's own signals endpoint, `None` when blank.
    #[must_use]
    pub fn local_addr(&self) -> Option<String> {
        self.local_addr.clone().filter(|addr| !addr.trim().is_empty())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use clap::{CommandFactory as _, Parser as _};

    use super::Cli;

    /// Runs clap's definition assertions on the real command.
    #[test]
    fn command_definition_is_valid() {
        Cli::command().debug_assert();
    }

    /// Catches duplicate group ids in release, where `debug_assert` is a no-op.
    #[test]
    fn argument_group_ids_are_unique() {
        let ids: Vec<String> = Cli::command().get_groups().map(|g| g.get_id().to_string()).collect();
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "duplicate clap group ids: {ids:?}");
    }

    /// The advertised signals endpoint is parsed strictly and normalized.
    #[test]
    fn signals_advertise_addr_is_strict() {
        let parse = |value: &str| {
            Cli::try_parse_from(["grid-operator", "--signals-advertise-addr", value])
                .map(|cli| cli.signals.advertise_addr)
        };
        assert_eq!(
            parse("[FD00::1]:9091").ok().flatten().as_deref(),
            Some("[fd00::1]:9091")
        );
        assert_eq!(
            parse("East.Example:9091").ok().flatten().as_deref(),
            Some("east.example:9091")
        );
        for bad in ["fd00::1", "x@169.254.169.254:443", "evil.example/x?:9091"] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    /// The local signals endpoint is parsed strictly; blank stays unset.
    #[test]
    fn signals_local_addr_is_strict() {
        let parse = |value: &str| {
            Cli::try_parse_from(["grid-operator", "--signals-local-addr", value]).map(|cli| cli.signals.local_addr())
        };
        assert_eq!(
            parse("East.Example:9091").ok().flatten().as_deref(),
            Some("east.example:9091"),
            "normalized"
        );
        assert_eq!(parse(" ").ok().flatten(), None, "blank is unset");
        for bad in ["east.example", "east.example:9091/x"] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    /// The real parser accepts an empty argv. Asserts no values: `GRID_*` leak in.
    #[test]
    #[expect(
        clippy::assertions_on_result_states,
        reason = "parser smoke test only needs to assert successful parsing"
    )]
    fn empty_argv_parses() {
        assert!(Cli::try_parse_from(["grid-operator"]).is_ok());
    }
}
