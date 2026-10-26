//! Holds a candidate back from the lease while an unelected operator replica still writes.
//!
//! A replica rendered without leader election never yields, so a rollout onto an elected
//! render would otherwise run one beside the new holder.

use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client, api::ListParams};

/// The operator's container in its pod.
const OPERATOR_CONTAINER: &str = "operator";

/// The variable that marks a replica as one that elects.
const LEASE_ENV: &str = "GRID_LEADER_LEASE";

/// Why a candidate does not contend yet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Waiting {
    /// These replicas run unelected and may still write.
    Unelected(Vec<String>),
    /// The replicas could not be listed, so none is assumed safe.
    Unreadable(String),
}

impl Waiting {
    /// The reason label it is reported under.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Unelected(_) => "unelected_replica",
            Self::Unreadable(_) => "replicas_unreadable",
        }
    }
}

/// Whether a candidate may contend for the lease now.
pub trait ContendGate: Send + Sync {
    /// `Ok` when it may, else why not.
    fn check(&self) -> impl Future<Output = Result<(), Waiting>> + Send;
}

/// A gate that never holds a candidate back.
#[derive(Clone, Copy, Debug, Default)]
pub struct Open;

impl ContendGate for Open {
    async fn check(&self) -> Result<(), Waiting> {
        Ok(())
    }
}

/// The operator's own replicas, found by its Deployment's selector.
#[derive(Clone)]
pub struct Replicas {
    /// Pods in the operator's namespace.
    pods: Api<Pod>,
    /// The Deployment's selector, as a label selector; `None` holds nothing back.
    selector: Option<String>,
}

impl std::fmt::Debug for Replicas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replicas")
            .field("selector", &self.selector)
            .finish_non_exhaustive()
    }
}

impl Replicas {
    /// The replicas `selector` matches in the client's namespace. Without one, nothing is
    /// held back.
    #[must_use]
    pub fn new(client: &Client, selector: Option<String>) -> Self {
        Self {
            pods: Api::default_namespaced(client.clone()),
            selector: selector.filter(|selector| !selector.is_empty()),
        }
    }
}

impl ContendGate for Replicas {
    async fn check(&self) -> Result<(), Waiting> {
        let Some(selector) = &self.selector else {
            return Ok(());
        };
        let listed = Box::pin(self.pods.list(&ListParams::default().labels(selector)))
            .await
            .map_err(|error| Waiting::Unreadable(error.to_string()))?;
        let unelected = unelected(&listed.items);
        if unelected.is_empty() {
            Ok(())
        } else {
            Err(Waiting::Unelected(unelected))
        }
    }
}

/// The pods in `pods` that run the operator unelected: still running or terminating, with an
/// operator container that names no lease. A finished pod writes nothing.
#[must_use]
pub fn unelected(pods: &[Pod]) -> Vec<String> {
    pods.iter()
        .filter(|pod| {
            let phase = pod.status.as_ref().and_then(|status| status.phase.as_deref());
            !matches!(phase, Some("Succeeded" | "Failed"))
        })
        .filter(|pod| {
            pod.spec.as_ref().is_some_and(|spec| {
                spec.containers
                    .iter()
                    .find(|container| container.name == OPERATOR_CONTAINER)
                    .is_some_and(|operator| !operator.env.iter().flatten().any(|variable| variable.name == LEASE_ENV))
            })
        })
        .map(|pod| pod.metadata.name.clone().unwrap_or_default())
        .collect()
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::unelected;

    fn pod(name: &str, elected: bool, phase: &str, terminating: bool) -> k8s_openapi::api::core::v1::Pod {
        let env = if elected {
            serde_json::json!([{ "name": "POD_NAMESPACE", "value": "grid" }, { "name": "GRID_LEADER_LEASE", "value": "grid-operator" }])
        } else {
            serde_json::json!([{ "name": "POD_NAMESPACE", "value": "grid" }])
        };
        let metadata = if terminating {
            serde_json::json!({ "name": name, "deletionTimestamp": "2026-10-26T00:00:00Z" })
        } else {
            serde_json::json!({ "name": name })
        };
        serde_json::from_value(serde_json::json!({
            "metadata": metadata,
            "spec": { "containers": [{ "name": "operator", "image": "grid-operator", "env": env }] },
            "status": { "phase": phase }
        }))
        .expect("pod")
    }

    #[test]
    fn a_replica_without_the_lease_variable_is_unelected() {
        let pods = [pod("old", false, "Running", false), pod("new", true, "Running", false)];
        assert_eq!(unelected(&pods), vec!["old".to_owned()]);
    }

    #[test]
    fn a_terminating_unelected_replica_still_counts() {
        assert_eq!(
            unelected(&[pod("old", false, "Running", true)]),
            vec!["old".to_owned()],
            "it may still be writing while it stands down"
        );
    }

    #[test]
    fn a_finished_replica_does_not_count() {
        assert!(
            unelected(&[
                pod("done", false, "Succeeded", false),
                pod("crashed", false, "Failed", false)
            ])
            .is_empty()
        );
    }

    #[test]
    fn elected_replicas_hold_nothing_back() {
        assert!(unelected(&[pod("a", true, "Running", false), pod("b", true, "Pending", false)]).is_empty());
    }
}
