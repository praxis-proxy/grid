//! GridNetwork watch decisions independent of kube's unstable stream controls.

use futures::{Stream, StreamExt as _, stream::BoxStream};
use kube::{
    Api, Resource, ResourceExt as _,
    runtime::{
        WatchStreamExt as _,
        controller::{ReconcileReason, ReconcileRequest, trigger_with},
        reflector::{self, ObjectRef},
        watcher::{self, Event},
    },
};

use crate::{
    controller::grid_network::GRID_NETWORK_WITHDRAWAL_FINALIZER,
    crd::{grid_network::GridNetwork, inference_provider::ConditionStatus},
    gateway_ages::GATEWAY_AGES_REJECTED,
};

/// Inputs whose changes require routing reconciliation.
#[derive(PartialEq, Eq)]
struct RoutingInputs {
    /// Spec revision assigned by the API server.
    generation: Option<i64>,
    /// Whether withdrawal cleanup is installed.
    has_finalizer: bool,
    /// Whether withdrawal has begun.
    is_deleting: bool,
}

/// Inputs consumed by the gateway-age rollup.
#[derive(PartialEq, Eq)]
struct RejectionInputs {
    /// Rejection state.
    status: ConditionStatus,
    /// Machine-readable rejection cause.
    reason: String,
    /// Human-readable rejection detail.
    message: String,
}

/// Last observation of one live object.
struct Observation<T> {
    /// API identity, including namespace when present.
    reference: ObjectRef<GridNetwork>,
    /// Distinguishes replacement objects with the same name.
    uid: Option<String>,
    /// Inputs last emitted by this watch.
    inputs: T,
}

/// Reflect every observation before suppressing irrelevant routing updates.
pub fn routing_watch<Source: Stream<Item = watcher::Result<Event<GridNetwork>>>>(
    stream: Source,
) -> (
    reflector::Store<GridNetwork>,
    impl Stream<Item = watcher::Result<GridNetwork>>,
) {
    let (reader, writer) = reflector::store();
    (reader, routing_updates(stream.reflect(writer)))
}

/// Watch related resources, including deletions, without growing the async caller's stack.
pub fn related_requests<Other>(
    api: Api<Other>,
    mapper: fn(Other) -> Option<ObjectRef<GridNetwork>>,
) -> BoxStream<'static, watcher::Result<ReconcileRequest<GridNetwork>>>
where
    Other: Resource<DynamicType = ()> + Clone + serde::de::DeserializeOwned + std::fmt::Debug + Send + 'static,
{
    trigger_with(
        watcher::watcher(api, watcher::Config::default()).touched_objects(),
        mapper,
    )
    .boxed()
}

/// Request every currently reflected network when external state changes.
pub fn bulk_requests<Source: Stream<Item = ()>>(
    stream: Source,
    reader: reflector::Store<GridNetwork>,
) -> impl Stream<Item = watcher::Result<ReconcileRequest<GridNetwork>>> {
    stream.flat_map(move |()| {
        futures::stream::iter(reader.state().into_iter().map(|network| {
            Ok(ReconcileRequest {
                obj_ref: ObjectRef::from_obj(&*network),
                reason: ReconcileReason::BulkReconcile,
            })
        }))
    })
}

/// Emit routing changes while retaining watch errors and lifecycle bookkeeping.
pub fn routing_updates<Source: Stream<Item = watcher::Result<Event<GridNetwork>>>>(
    stream: Source,
) -> impl Stream<Item = watcher::Result<GridNetwork>> {
    changed_updates(stream, |network| RoutingInputs {
        generation: network.metadata.generation,
        has_finalizer: network
            .finalizers()
            .iter()
            .any(|value| value == GRID_NETWORK_WITHDRAWAL_FINALIZER),
        is_deleting: network.metadata.deletion_timestamp.is_some(),
    })
}

/// Emit only changes to the rejection condition consumed by the rollup.
pub fn rollup_updates<Source: Stream<Item = watcher::Result<Event<GridNetwork>>>>(
    stream: Source,
) -> impl Stream<Item = watcher::Result<GridNetwork>> {
    changed_updates(stream, |network| {
        network
            .status
            .as_ref()
            .and_then(|status| {
                status
                    .conditions
                    .iter()
                    .find(|condition| condition.type_ == GATEWAY_AGES_REJECTED)
            })
            .map(|condition| RejectionInputs {
                status: condition.status,
                reason: condition.reason.clone(),
                message: condition.message.clone(),
            })
    })
}

/// Keep decisions scoped to one watch, resetting them when the server relists.
fn changed_updates<T: PartialEq>(
    stream: impl Stream<Item = watcher::Result<Event<GridNetwork>>>,
    inputs: impl Fn(&GridNetwork) -> T,
) -> impl Stream<Item = watcher::Result<GridNetwork>> {
    let mut observations: Vec<Observation<T>> = Vec::new();
    stream.filter_map(move |result| {
        let output = match result {
            Err(error) => Some(Err(error)),
            Ok(Event::Init) => {
                observations.clear();
                None
            },
            Ok(Event::InitDone) => None,
            Ok(Event::Delete(network)) => {
                observations.retain(|entry| entry.reference != ObjectRef::from_obj(&network));
                None
            },
            Ok(Event::Apply(network) | Event::InitApply(network)) => {
                let reference = ObjectRef::from_obj(&network);
                let uid = network.metadata.uid.clone();
                let current = inputs(&network);
                if let Some(previous) = observations.iter_mut().find(|entry| entry.reference == reference) {
                    if previous.uid == uid && previous.inputs == current {
                        return std::future::ready(None);
                    }
                    previous.uid = uid;
                    previous.inputs = current;
                } else {
                    observations.push(Observation {
                        reference,
                        uid,
                        inputs: current,
                    });
                }
                Some(Ok(network))
            },
        };
        std::future::ready(output)
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures::{FutureExt as _, StreamExt as _, channel::mpsc, stream};
    use kube::runtime::{
        controller::{Action, Config, applier, trigger_self, trigger_with},
        reflector::ObjectRef,
        watcher::{self, Event},
    };

    use crate::{
        crd::grid_network::GridNetwork,
        network_watch::{bulk_requests, rollup_updates, routing_updates, routing_watch},
    };

    #[tokio::test]
    async fn routing_cache_handles_independent_objects_recreation_deletion_and_relist() {
        let first = network("first", "uid-1");
        let second = network("second", "uid-2");
        let replacement = network("first", "replacement");
        let updates = routing_updates(stream::iter(vec![
            Ok(Event::Init),
            Ok(Event::InitApply(first.clone())),
            Ok(Event::InitApply(second.clone())),
            Ok(Event::InitDone),
            Ok(Event::Apply(first.clone())),
            Ok(Event::Apply(second.clone())),
            Err(watcher::Error::NoResourceVersion),
            Ok(Event::Apply(replacement.clone())),
            Ok(Event::Delete(replacement.clone())),
            Ok(Event::Apply(replacement.clone())),
            Ok(Event::Init),
            Ok(Event::InitApply(second.clone())),
            Ok(Event::InitDone),
            Ok(Event::Apply(second)),
            Ok(Event::Apply(first)),
        ]))
        .collect::<Vec<_>>()
        .await;

        assert_eq!(
            updates.len(),
            7,
            "duplicates are suppressed while lifecycle events reset observations"
        );
        assert!(
            matches!(updates.get(2), Some(Err(watcher::Error::NoResourceVersion))),
            "errors remain visible"
        );
        assert_eq!(
            updates
                .last()
                .and_then(|value| value.as_ref().ok())
                .and_then(|network| network.metadata.name.as_deref()),
            Some("first"),
            "relist prunes objects absent from the new list"
        );
    }

    #[tokio::test]
    async fn removing_the_withdrawal_finalizer_triggers_reconciliation() {
        let plain = network("net", "uid");
        let mut finalizing = plain.clone();
        finalizing.metadata.finalizers = Some(vec![
            crate::controller::grid_network::GRID_NETWORK_WITHDRAWAL_FINALIZER.to_owned(),
        ]);
        let updates = routing_updates(stream::iter(
            [finalizing, plain].into_iter().map(|network| Ok(Event::Apply(network))),
        ))
        .collect::<Vec<_>>()
        .await;
        assert_eq!(
            updates.len(),
            2,
            "removal changes lifecycle inputs without changing generation"
        );
    }

    #[tokio::test]
    async fn rejection_condition_presence_reason_and_message_are_rollup_inputs() {
        let absent = network("net", "uid");
        let rejected = with_rejection(&absent, "Rejected", "first");
        let reason = with_rejection(&absent, "AnotherReason", "first");
        let message = with_rejection(&absent, "AnotherReason", "second");
        let updates = rollup_updates(stream::iter(
            vec![absent.clone(), rejected.clone(), rejected, reason, message, absent]
                .into_iter()
                .map(|network| Ok(Event::Apply(network))),
        ))
        .collect::<Vec<_>>()
        .await;
        assert_eq!(
            updates.len(),
            5,
            "presence, reason, message and removal pass while identical conditions do not"
        );
    }

    #[tokio::test]
    async fn suppressed_updates_still_refresh_the_store_and_bulk_requests() {
        let first = network("first", "uid-1");
        let second = network("second", "uid-2");
        let mut latest = first.clone();
        latest.metadata.resource_version = Some("2".to_owned());
        let (store, updates) = routing_watch(stream::iter(vec![
            Ok(Event::Init),
            Ok(Event::InitApply(first)),
            Ok(Event::InitApply(second)),
            Ok(Event::InitDone),
            Ok(Event::Apply(latest)),
        ]));
        assert_eq!(
            updates.collect::<Vec<_>>().await.len(),
            2,
            "status-only update is suppressed"
        );
        assert!(
            store.wait_until_ready().await.is_ok(),
            "initial list completes even when control events are filtered"
        );
        assert_eq!(
            store
                .get(&ObjectRef::new("first"))
                .and_then(|network| network.metadata.resource_version.clone())
                .as_deref(),
            Some("2"),
            "store holds the latest suppressed observation"
        );
        let requests = bulk_requests(stream::iter(vec![(), ()]), store)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            requests.len(),
            4,
            "each membership or catalog event reaches both networks"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn applier_preserves_timer_and_error_retries_with_fresh_status() {
        let original = network("net", "uid");
        let (sender, receiver) = mpsc::unbounded();
        let (store, updates) = routing_watch(receiver);
        for event in [Event::Init, Event::InitApply(original.clone()), Event::InitDone] {
            assert!(sender.unbounded_send(Ok(event)).is_ok(), "initial list sent");
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::new(AtomicUsize::new(0));
        let recorded = Arc::clone(&observed);
        let mut reconciles = Box::pin(applier(
            move |network, calls: Arc<AtomicUsize>| {
                recorded.store(
                    usize::from(network.metadata.resource_version.as_deref() == Some("2")),
                    Ordering::SeqCst,
                );
                let attempt = calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(match attempt {
                    0 => Ok(Action::requeue(Duration::from_secs(5))),
                    1 => Err(std::io::Error::other("retry me")),
                    _ => Ok(Action::await_change()),
                })
            },
            |_, _, _| Action::requeue(Duration::from_secs(2)),
            Arc::clone(&calls),
            store,
            trigger_self(updates, ()),
            Config::default(),
        ));
        assert!(
            matches!(reconciles.next().await, Some(Ok(_))),
            "initial reconciliation runs"
        );
        let mut latest = original;
        latest.metadata.resource_version = Some("2".to_owned());
        assert!(
            sender.unbounded_send(Ok(Event::Apply(latest))).is_ok(),
            "status update sent"
        );
        assert!(
            reconciles.next().now_or_never().is_none(),
            "status alone does not reconcile"
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(
            matches!(reconciles.next().await, Some(Err(_))),
            "timer triggers despite unchanged inputs"
        );
        assert_eq!(observed.load(Ordering::SeqCst), 1, "timer reads refreshed status");
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(
            matches!(reconciles.next().await, Some(Ok(_))),
            "error policy schedules another attempt"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3, "both retry paths run exactly once");
    }

    #[tokio::test]
    async fn dependent_requests_reconcile_an_unchanged_network() {
        let (sender, receiver) = mpsc::unbounded();
        let (store, updates) = routing_watch(
            stream::iter(vec![
                Ok(Event::Init),
                Ok(Event::InitApply(network("net", "uid"))),
                Ok(Event::InitDone),
            ])
            .chain(stream::pending()),
        );
        let related = trigger_with(receiver, |reference: ObjectRef<GridNetwork>| Some(reference));
        let requests = stream::select(trigger_self(updates, ()), related);
        let mut reconciles = Box::pin(applier(
            |_, _| std::future::ready(Ok::<_, std::io::Error>(Action::await_change())),
            |_, _, _| Action::await_change(),
            Arc::new(()),
            store,
            requests,
            Config::default(),
        ));
        assert!(
            matches!(reconciles.next().await, Some(Ok(_))),
            "initial reconciliation runs"
        );
        for _ in 0..3 {
            assert!(
                sender
                    .unbounded_send(Ok::<_, watcher::Error>(ObjectRef::new("net")))
                    .is_ok(),
                "mapped dependent event sent"
            );
            assert!(
                matches!(reconciles.next().await, Some(Ok(_))),
                "each dependent request bypasses the primary filter"
            );
        }
    }

    #[tokio::test]
    async fn related_watches_map_initial_objects_and_deletions_to_their_network() {
        use crate::{
            controller::grid_network,
            crd::{agent_tool_provider::AgentToolProvider, grid_site::GridSite, inference_provider::InferenceProvider},
            network_watch::related_requests,
        };
        let sources = [
            related_requests(
                kube::Api::<InferenceProvider>::all(related_client(
                    "InferenceProvider",
                    &serde_json::json!({"gridNetworkRef": "net", "providerKind": "self_hosted", "backendKind": "local", "endpoint": "http://localhost:8000", "models": []}),
                )),
                grid_network::network_refs_from_inference_provider,
            ),
            related_requests(
                kube::Api::<AgentToolProvider>::all(related_client(
                    "AgentToolProvider",
                    &serde_json::json!({"gridNetworkRef": "net", "endpoint": "http://localhost:9090", "tools": []}),
                )),
                grid_network::network_refs_from_agent_tool_provider,
            ),
            related_requests(
                kube::Api::<GridSite>::all(related_client(
                    "GridSite",
                    &serde_json::json!({"gridNetworkRef": "net"}),
                )),
                grid_network::network_refs_from_grid_site,
            ),
        ];
        for mut requests in sources {
            for _ in 0..2 {
                let result = tokio::time::timeout(Duration::from_secs(1), requests.next()).await;
                assert!(
                    matches!(result, Ok(Some(Ok(request))) if request.obj_ref.name == "net"),
                    "initial observation and deletion both reach the owning network"
                );
            }
        }
    }

    #[tokio::test]
    async fn dropping_the_applier_cancels_in_flight_reconciliation() {
        let (store, updates) = routing_watch(
            stream::iter(vec![
                Ok(Event::Init),
                Ok(Event::InitApply(network("net", "uid"))),
                Ok(Event::InitDone),
            ])
            .chain(stream::pending()),
        );
        let active = Arc::new(AtomicUsize::new(0));
        let marker = Arc::clone(&active);
        let (started, mut beginning) = mpsc::unbounded();
        let mut reconciles = Box::pin(applier(
            move |_, _| {
                let guard = ActiveReconcile(Arc::clone(&marker));
                marker.fetch_add(1, Ordering::SeqCst);
                assert!(started.unbounded_send(()).is_ok(), "start observed");
                Box::pin(async move {
                    let _guard = guard;
                    std::future::pending::<Result<Action, std::io::Error>>().await
                })
            },
            |_, _, _| Action::await_change(),
            Arc::new(()),
            store,
            trigger_self(updates, ()),
            Config::default(),
        ));
        tokio::select! {
            value = beginning.next() => assert_eq!(value, Some(()), "reconciliation started"),
            result = reconciles.next() => assert!(result.is_none(), "reconciliation must remain in flight"),
        }
        assert_eq!(active.load(Ordering::SeqCst), 1, "reconciliation started");
        drop(reconciles);
        assert_eq!(
            active.load(Ordering::SeqCst),
            0,
            "writer cancellation drops the reconcile future immediately"
        );
    }

    // Test Utilities

    /// Tracks cancellation of a directly owned reconcile future.
    struct ActiveReconcile(Arc<AtomicUsize>);

    impl Drop for ActiveReconcile {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// API boundary serving an initial list followed by a deletion watch event.
    fn related_client(kind: &str, spec: &serde_json::Value) -> kube::Client {
        let object = serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1", "kind": kind,
            "metadata": {"name": "related", "uid": "related-uid", "resourceVersion": "1"}, "spec": spec,
        });
        let list = serde_json::json!({"apiVersion": "grid.praxis.fast/v1alpha1", "kind": format!("{kind}List"), "metadata": {"resourceVersion": "1"}, "items": [object]}).to_string();
        let deletion = serde_json::json!({"type": "DELETED", "object": object}).to_string();
        kube::Client::new(
            tower::service_fn(move |request: http::Request<kube::client::Body>| {
                let body = if request.uri().query().is_some_and(|query| query.contains("watch=true")) {
                    deletion.clone()
                } else {
                    list.clone()
                };
                async move { Ok::<_, std::convert::Infallible>(http::Response::new(axum::body::Body::from(body))) }
            }),
            "default",
        )
    }

    /// Minimal server-style object with a stable generation.
    fn network(name: &str, uid: &str) -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1", "kind": "GridNetwork",
            "metadata": {"name": name, "uid": uid, "generation": 1, "resourceVersion": "1"},
            "spec": {"seeds": [], "gridId": "grid"}
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    /// Populate only the condition inputs the rollup consumes.
    fn with_rejection(network: &GridNetwork, reason: &str, message: &str) -> GridNetwork {
        let mut network = network.clone();
        network.status = Some(serde_json::from_value(serde_json::json!({
            "conditions": [{"type": "GatewayAgesRejected", "status": "True", "reason": reason, "message": message, "lastTransitionTime": "2026-01-01T00:00:00Z"}]
        })).unwrap_or_else(|_| std::process::abort()));
        network
    }
}
