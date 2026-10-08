//! The hub EPP's multicluster file-discovery document, turned into dashboard
//! sites.

mod spec;
mod store;
#[cfg(test)]
mod testing;

pub use spec::{EndpointSpec, Site, build, parse_endpoints};
pub use store::{RegistryStore, StoreError, default_namespace};

/// The current site list as published by whoever owns the registry; readers
/// hold a `tokio::sync::watch::Receiver<SiteList>` and are woken on change.
pub type SiteList = std::sync::Arc<Vec<Site>>;
