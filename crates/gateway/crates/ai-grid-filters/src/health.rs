//! Which backend clusters have no healthy endpoint, resolved off the request path.
//!
//! The route filter publishes Praxis's health registry here; the control step reads
//! it on its own tick, so a request never judges raw health.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use arc_swap::{ArcSwap, ArcSwapOption};
use praxis_core::health::{ClusterHealthState, HealthRegistry};

/// Clusters whose every endpoint is down, kept by the control step.
#[derive(Debug, Default)]
pub struct ClusterHealth {
    /// The pipeline's registry, published by the route filter.
    registry: ArcSwapOption<HashMap<Arc<str>, ClusterHealthState>>,
    /// The clusters with no healthy endpoint as of the last tick.
    down: ArcSwap<BTreeSet<Arc<str>>>,
}

impl ClusterHealth {
    /// Publish `registry` when it is not the one already held: one lock-free load per request.
    pub(crate) fn observe(&self, registry: Option<&HealthRegistry>) {
        let Some(registry) = registry else {
            return;
        };
        let held = self.registry.load();
        if !held.as_ref().is_some_and(|held| Arc::ptr_eq(held, registry)) {
            self.registry.store(Some(Arc::clone(registry)));
        }
    }

    /// Whether praxis has reported any cluster health yet.
    ///
    /// Without a registry the gateway knows nothing about backend health, so demotion
    /// stays off rather than treating every cluster as healthy or as down.
    pub(crate) fn observed(&self) -> bool {
        self.registry.load().is_some()
    }

    /// The clusters that were down as of the last [`Self::update`].
    pub(crate) fn down(&self) -> Arc<BTreeSet<Arc<str>>> {
        self.down.load_full()
    }

    /// Recompute the down set from the registry, returning whether it changed.
    pub(crate) fn update(&self) -> bool {
        let now = self.registry.load().as_deref().map(down_clusters).unwrap_or_default();
        if **self.down.load() == now {
            return false;
        }
        tracing::info!(down = ?now, "grid: backend clusters with no healthy endpoint changed");
        self.down.store(Arc::new(now));
        true
    }
}

/// Clusters in `registry` with endpoints, none of them healthy.
fn down_clusters(registry: &HashMap<Arc<str>, ClusterHealthState>) -> BTreeSet<Arc<str>> {
    registry
        .iter()
        .filter(|(_, state)| {
            let (healthy, total) = state.endpoint_counts();
            total > 0 && healthy == 0
        })
        .map(|(name, _)| Arc::clone(name))
        .collect()
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use praxis_core::health::{ClusterHealthEntry, EndpointHealth};

    use super::*;

    fn registry(clusters: &[(&str, &[bool])]) -> HealthRegistry {
        Arc::new(
            clusters
                .iter()
                .map(|(name, endpoints)| {
                    let health: Vec<EndpointHealth> = endpoints
                        .iter()
                        .map(|healthy| {
                            let endpoint = EndpointHealth::new();
                            if !healthy {
                                endpoint.mark_unhealthy();
                            }
                            endpoint
                        })
                        .collect();
                    let addresses = (0..endpoints.len())
                        .map(|i| Arc::from(format!("10.0.0.{i}:80")))
                        .collect();
                    let entry = ClusterHealthEntry::new(health, addresses, None, None);
                    (Arc::from(*name), Arc::new(entry))
                })
                .collect(),
        )
    }

    #[test]
    fn a_cluster_is_down_only_when_every_endpoint_is() {
        let health = ClusterHealth::default();
        health.observe(Some(&registry(&[
            ("site-a", &[false]),
            ("site-b", &[false, true]),
            ("site-d", &[true]),
        ])));
        assert!(health.update(), "the first tick finds site-a down");
        assert_eq!(*health.down(), BTreeSet::from([Arc::from("site-a")]));
        assert!(!health.update(), "an unchanged registry is not news");
    }

    #[test]
    fn a_recovered_cluster_leaves_the_down_set() {
        let health = ClusterHealth::default();
        health.observe(Some(&registry(&[("site-a", &[false])])));
        assert!(health.update());
        health.observe(Some(&registry(&[("site-a", &[true])])));
        assert!(health.update(), "the recovered cluster leaves the down set");
        assert!(health.down().is_empty());
    }

    #[test]
    fn nothing_is_down_and_nothing_is_observed_without_a_registry() {
        let health = ClusterHealth::default();
        health.observe(None);
        assert!(!health.observed(), "no registry, so demotion stays off");
        assert!(!health.update());
        assert!(health.down().is_empty());
        health.observe(Some(&registry(&[("site-a", &[true])])));
        assert!(health.observed(), "praxis reported, so demotion applies");
    }
}
