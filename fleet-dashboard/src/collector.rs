//! Polls every registered site, derives health, and keeps the fleet snapshot
//! and its short history for the API.

mod fleet;
mod metrics;
mod series;
mod site;
#[cfg(test)]
pub(crate) mod testing;

pub use fleet::{Clock, Collector, Options};
pub use metrics::Metrics;
pub use series::SeriesError;
pub use site::{SiteMetrics, SitePoller};
