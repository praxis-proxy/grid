//! Outbound HTTP transport for the grid: how a client is built, how long it
//! waits, and which failures are worth another attempt.
//!
//! Two client shapes, split by how a peer is identified rather than by
//! preference. [`PeerScraper`] dials a peer identified by its leaf, pinned or
//! by SPIFFE id, which needs a rustls verifier on one backend and an OpenSSL
//! callback on the other, so it is built on hyper. The `reqwest-client`
//! feature builds clients for a peer identified by its chain to the grid
//! roots, which reqwest expresses under both backends.
//!
//! The reqwest TLS backend follows this crate's own backend feature, so a
//! caller forwards `fips` here rather than naming a backend itself.
//!
//! The signals poller, which writes each scraped exposition into the
//! `grid-signals` `LoadStore`, also lives here.

#[cfg(all(feature = "rcgen", feature = "fips"))]
compile_error!("features `rcgen` and `fips` are mutually exclusive");
#[cfg(all(feature = "reqwest-client", not(any(feature = "rcgen", feature = "fips"))))]
compile_error!("`reqwest-client` needs one of `rcgen` or `fips` to pick a TLS backend");

#[cfg(feature = "peer-scraper")]
mod mtls;
#[cfg(feature = "peer-scraper")]
mod poller;
#[cfg(feature = "reqwest-client")]
mod reqwest_client;
mod retry;
#[cfg(feature = "peer-scraper")]
mod scrape;

#[cfg(feature = "peer-scraper")]
mod feedback;

#[cfg(feature = "peer-scraper")]
pub use feedback::Reporter;
#[cfg(feature = "peer-scraper")]
pub use mtls::{MtlsError, PeerScraper};
#[cfg(feature = "peer-scraper")]
pub use poller::{
    FetchError, PinnedTls, PollHandle, PollerConfig, Scrape, SignalSource, build_url, deserialize_interval_ms, spawn,
    spawn_from_config, spawn_on_thread, spawn_on_thread_held,
};
#[cfg(feature = "reqwest-client")]
pub use reqwest_client::{BuildError, Timeouts, identity, roots_from_pem, trusted_client};
#[cfg(feature = "reqwest-client")]
pub use retry::classify;
pub use retry::{RequestOutcome, from_status};
