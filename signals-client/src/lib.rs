//! Grid signal poller: the mTLS scrape transport that feeds the signals source.
//!
//! Polls the local operator's `/v1/site/signals` and writes each scraped
//! exposition into the `grid-signals` `LoadStore`. This is the transport layer
//! of the signals source. `grid-signals` stays a dashmap-only leaf. Tokio and
//! the pinned client live here.

mod mtls;
mod poller;
mod scrape;

pub use mtls::{MtlsError, PeerScraper};
pub use poller::{
    FetchError, PinnedTls, PollHandle, PollerConfig, Scrape, SignalSource, build_url, deserialize_interval_ms, spawn,
    spawn_from_config, spawn_on_thread,
};
