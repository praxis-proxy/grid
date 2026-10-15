//! Provider revisions bound to a fenced collection pass.

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use grid_signals::revision;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{Api, Client};

use crate::{crd::inference_provider::InferenceProvider, resources::routing_overlay, signals::Observation};

/// Bound one publication-authority read so a stalled API cannot hold every scrape.
const FENCE_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether the immutable startup reservation still owns publication authority.
///
/// This read follows provider collection. A replacement reserves its epoch before
/// reading providers, so an obsolete publisher cannot qualify a later collection.
pub(crate) async fn holds_fence(client: &Client, site: &str, high: u64) -> Result<bool, kube::Error> {
    let api: Api<ConfigMap> = Api::default_namespaced(client.clone());
    let name = format!("grid-swim-revision-hwm-{site}");
    match tokio::time::timeout(FENCE_TIMEOUT, api.get(&name))
        .await
        .map_err(|error| kube::Error::Service(Box::new(error)))?
    {
        Ok(held) => Ok(held
            .data
            .as_ref()
            .and_then(|data| data.get("nodeGenerationHighWatermark"))
            .and_then(|value| value.parse::<u64>().ok())
            == Some(high)),
        Err(kube::Error::Api(error)) if error.code == 404 => Ok(false),
        Err(error) => Err(error),
    }
}

/// Bind fresh samples, or an empty invalidation, to their collection snapshot.
///
/// Aliased source objects cannot qualify the same routing provider. A metadata
/// row never qualifies observations retained from an earlier collection pass.
pub(crate) fn qualify(
    collected: &mut HashMap<String, Vec<Observation>>,
    providers: &[InferenceProvider],
    network: &str,
    epoch: u64,
    sequence: u64,
) {
    let providers = providers
        .iter()
        .filter(|provider| provider.spec.grid_network_ref == network);
    let mut identities = BTreeMap::<&str, usize>::new();
    for provider in providers.clone() {
        if let Some(identity) = routing_overlay::routing_identity(provider) {
            let count = identities.entry(identity).or_default();
            *count = count.saturating_add(1);
        }
    }
    for provider in providers {
        let Some(identity) = routing_overlay::routing_identity(provider) else {
            continue;
        };
        let observations = collected.entry(identity.to_owned()).or_default();
        if identities.get(identity) != Some(&1) {
            continue;
        }
        if observations.len() > revision::MAX_BATCH_SAMPLES {
            observations.clear();
        }
        if let Some(metadata) = metadata(provider, epoch, sequence, observations.len()) {
            observations.push(metadata);
        }
    }
}

/// Revision metadata for a source object whose identity is representable on the wire.
fn metadata(provider: &InferenceProvider, epoch: u64, sequence: u64, samples: usize) -> Option<Observation> {
    let uid = provider.metadata.uid.as_deref().filter(|uid| revision::valid_id(uid))?;
    let generation = u64::try_from(provider.metadata.generation?)
        .ok()
        .filter(|value| *value > 0 && *value <= revision::MAX_GENERATION)?;
    Some(Observation {
        metric: revision::METRIC.to_owned(),
        labels: BTreeMap::from([
            (revision::UID_LABEL.to_owned(), uid.to_owned()),
            (revision::EPOCH_LABEL.to_owned(), epoch.to_string()),
            (revision::SEQUENCE_LABEL.to_owned(), sequence.to_string()),
            (revision::SAMPLES_LABEL.to_owned(), samples.to_string()),
        ]),
        value: generation.to_string().parse().ok()?,
        timestamp_ms: None,
        age: Duration::ZERO,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use super::*;

    #[test]
    fn only_source_uid_and_spec_generation_define_revision() -> Result<(), Box<dyn std::error::Error>> {
        let mut provider = provider()?;
        let original = metadata(&provider, 1, 1, 0);
        provider.metadata.resource_version = Some("42".to_owned());
        provider.status = Some(serde_json::from_value(
            serde_json::json!({"phase":"Available","observedGeneration":1}),
        )?);
        provider.metadata.labels = Some(BTreeMap::from([("role".to_owned(), "inference".to_owned())]));
        assert_eq!(
            metadata(&provider, 1, 1, 0),
            original,
            "status/metadata writes preserve the revision"
        );
        provider.metadata.generation = Some(2);
        assert_ne!(
            metadata(&provider, 1, 1, 0),
            original,
            "a spec edit changes the revision"
        );
        provider.metadata.generation = Some(1);
        provider.metadata.uid = Some("replacement".to_owned());
        assert_ne!(
            metadata(&provider, 1, 1, 0),
            original,
            "same-name recreation changes the source"
        );
        Ok(())
    }

    #[test]
    fn missing_or_unrepresentable_source_identity_is_not_qualified() -> Result<(), Box<dyn std::error::Error>> {
        for (uid, generation) in [
            (None, Some(1)),
            (Some(""), Some(1)),
            (Some("u1"), None),
            (Some("u1"), Some(0)),
            (Some("u1"), Some(9_007_199_254_740_992)),
        ] {
            let mut provider = provider()?;
            provider.metadata.uid = uid.map(str::to_owned);
            provider.metadata.generation = generation;
            assert!(
                metadata(&provider, 1, 1, 0).is_none(),
                "invalid source must not authorize learning"
            );
        }
        Ok(())
    }

    #[test]
    fn no_metrics_or_failed_scrape_still_publishes_revision_invalidation() -> Result<(), Box<dyn std::error::Error>> {
        let mut collected = HashMap::new();
        qualify(&mut collected, &[provider()?], "net", 1, 2);
        let rows = collected.get("model").ok_or("missing invalidation")?;
        assert_eq!(rows.len(), 1);
        let metadata = rows.first().ok_or("missing metadata")?;
        assert_eq!(metadata.metric, revision::METRIC);
        assert_eq!(
            metadata.labels.get(revision::SAMPLES_LABEL).map(String::as_str),
            Some("0")
        );
        Ok(())
    }

    #[test]
    fn aliased_providers_never_choose_an_arbitrary_source() -> Result<(), Box<dyn std::error::Error>> {
        let first = provider()?;
        let mut second = first.clone();
        second.metadata.name = Some("other".to_owned());
        second.metadata.uid = Some("u2".to_owned());
        second.spec.routing_cluster_ref = Some("model".to_owned());
        let mut collected = HashMap::new();
        qualify(&mut collected, &[first, second], "net", 1, 1);
        assert!(
            collected.get("model").is_some_and(Vec::is_empty),
            "clear old qualification on collision"
        );
        Ok(())
    }

    #[test]
    fn foreign_network_alias_does_not_suppress_local_revision() -> Result<(), Box<dyn std::error::Error>> {
        let local = provider()?;
        let mut foreign = local.clone();
        foreign.metadata.name = Some("foreign".to_owned());
        foreign.metadata.uid = Some("foreign-uid".to_owned());
        foreign.spec.grid_network_ref = "other".to_owned();
        foreign.spec.routing_cluster_ref = Some("model".to_owned());
        let mut providers = [local, foreign];
        for _ in 0..2 {
            let mut collected = HashMap::new();
            qualify(&mut collected, &providers, "net", 1, 1);
            let rows = collected.get("model").ok_or("missing local publication")?;
            assert_eq!(rows.len(), 1, "only the local provider emits revision metadata");
            assert_eq!(
                rows.first()
                    .and_then(|row| row.labels.get(revision::UID_LABEL))
                    .map(String::as_str),
                Some("u1"),
                "an unrelated network alias must not suppress or replace the local revision"
            );
            providers.reverse();
        }
        Ok(())
    }

    #[tokio::test]
    async fn superseded_epoch_cannot_publish_a_later_collection() -> Result<(), Box<dyn std::error::Error>> {
        let active = Arc::new(AtomicU64::new(20));
        let client = fence_client(Arc::clone(&active));
        let mut source = provider()?;
        let mut replacement = HashMap::new();
        assert!(
            holds_fence(&client, "east", 20).await?,
            "replacement owns the reservation"
        );
        qualify(&mut replacement, &[source.clone()], "net", 11, 1);
        source.metadata.generation = Some(2);
        assert!(
            !holds_fence(&client, "east", 10).await?,
            "old writer collecting a later spec remains fenced"
        );
        assert_eq!(
            replacement
                .get("model")
                .and_then(|rows| rows.first())
                .map(|row| row.value),
            Some(1.0)
        );
        active.store(0, Ordering::Relaxed);
        assert!(
            !holds_fence(&client, "east", 20).await?,
            "deleted reservation fails closed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn startup_during_collection_never_qualifies_a_pre_reservation_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::controller::grid_network::{OperatorCtx, refresh_signals};
        let listed = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let client = collection_client(provider()?, Arc::clone(&listed), Arc::clone(&release), 10);
        let ctx = Arc::new(OperatorCtx::new(
            client.clone(),
            None,
            crate::crd::grid_network::SignalMode::Poll,
        ));
        let task_ctx = Arc::clone(&ctx);
        let collecting = tokio::spawn(async move { refresh_signals(&task_ctx, &client, "net").await });
        tokio::time::timeout(Duration::from_secs(2), listed.notified()).await?;
        let handle = publisher().await?;
        assert!(
            ctx.set_swim(handle),
            "reservation attaches while the old provider snapshot is in flight"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), collecting).await???;
        assert!(
            ctx.signals.current().iter().all(|row| row.metric != revision::METRIC),
            "the new epoch cannot qualify an earlier source read"
        );
        Ok(())
    }

    #[tokio::test]
    async fn collection_losing_its_fence_cannot_publish_or_reacquire() -> Result<(), Box<dyn std::error::Error>> {
        use crate::controller::grid_network::{OperatorCtx, refresh_signals};
        let listed = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        release.notify_one();
        let client = collection_client(provider()?, Arc::clone(&listed), Arc::clone(&release), 20);
        let ctx = OperatorCtx::new(
            client.clone(),
            Some(publisher().await?),
            crate::crd::grid_network::SignalMode::Poll,
        );
        refresh_signals(&ctx, &client, "net").await?;
        tokio::time::timeout(Duration::from_secs(1), listed.notified()).await?;
        assert!(
            ctx.signals.current().is_empty(),
            "superseded epoch cannot publish its collected source"
        );
        let recovered = collection_client(provider()?, Arc::clone(&listed), Arc::clone(&release), 10);
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), refresh_signals(&ctx, &recovered, "net")).await??;
        tokio::time::timeout(Duration::from_secs(1), listed.notified()).await?;
        assert!(
            ctx.signals.current().is_empty(),
            "lost authority is permanent for this process"
        );
        Ok(())
    }

    #[tokio::test]
    async fn signals_network_mismatch_preserves_current_publisher() -> Result<(), Box<dyn std::error::Error>> {
        use crate::controller::grid_network::{OperatorCtx, refresh_signals};
        let release = Arc::new(tokio::sync::Notify::new());
        let client = collection_client(provider()?, Arc::default(), Arc::clone(&release), 10);
        let ctx = OperatorCtx::new(
            client.clone(),
            Some(publisher().await?),
            crate::crd::grid_network::SignalMode::Poll,
        );
        release.notify_one();
        refresh_signals(&ctx, &client, "net").await?;
        let before = ctx.signals.current();
        refresh_signals(&ctx, &client, "unrelated").await?;
        assert_eq!(
            ctx.signals.current(),
            before,
            "a mismatch leaves the current publication intact"
        );
        release.notify_one();
        refresh_signals(&ctx, &client, "net").await?;
        assert!(
            ctx.signals.current().iter().any(|row| {
                row.metric == revision::METRIC
                    && row.labels.get(revision::SEQUENCE_LABEL).map(String::as_str) == Some("2")
            }),
            "the bound network must keep publishing after a mismatch"
        );
        Ok(())
    }

    #[tokio::test]
    async fn signals_network_binding_requires_a_publisher() -> Result<(), Box<dyn std::error::Error>> {
        use crate::controller::grid_network::{OperatorCtx, refresh_signals};
        let listed = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let client = collection_client(provider()?, Arc::clone(&listed), Arc::clone(&release), 10);
        let ctx = OperatorCtx::new(client.clone(), None, crate::crd::grid_network::SignalMode::Poll);
        for network in ["net", "unrelated", "net"] {
            release.notify_one();
            refresh_signals(&ctx, &client, network).await?;
            tokio::time::timeout(Duration::from_secs(1), listed.notified()).await?;
        }
        assert!(
            ctx.signals.current().iter().all(|row| row.metric != revision::METRIC),
            "a pass without a publisher must not emit revision metadata"
        );
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn an_unresponsive_fence_read_is_bounded_and_retryable() -> Result<(), Box<dyn std::error::Error>> {
        let stalled = Client::new(
            tower::service_fn(|_: http::Request<kube::client::Body>| async {
                std::future::pending::<Result<http::Response<kube::client::Body>, std::convert::Infallible>>().await
            }),
            "default",
        );
        let started = tokio::time::Instant::now();
        assert!(
            matches!(holds_fence(&stalled, "east", 10).await, Err(kube::Error::Service(_))),
            "a timeout is transient, not a permanent lost fence"
        );
        assert_eq!(started.elapsed(), FENCE_TIMEOUT);
        assert!(
            holds_fence(&fence_client(Arc::new(AtomicU64::new(10))), "east", 10).await?,
            "a subsequent API read can recover"
        );
        Ok(())
    }

    /// A process with a fixed epoch reserved before collecting any configuration.
    async fn publisher() -> Result<Arc<crate::swim_runtime::SwimHandle>, Box<dyn std::error::Error>> {
        use crate::swim_runtime::{KeyState, RevisionLease, SwimConfig, start};
        Ok(start(SwimConfig {
            bind_addr: "127.0.0.1:0".parse()?,
            advertise_addr: None,
            site_name: "east".to_owned(),
            seeds: Vec::new(),
            signals_address: None,
            gateway_address: None,
            key: KeyState::Plain,
            revision_lease: RevisionLease {
                first_revision: 1,
                last_revision: 100,
                first_node_generation: 1,
                last_node_generation: 10,
            },
            revision_renewer: None,
        })
        .await?)
    }

    /// API fixture holding a provider snapshot behind a deterministic collection barrier.
    fn collection_client(
        provider: InferenceProvider,
        listed: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        high: u64,
    ) -> Client {
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let provider = provider.clone();
            let listed = Arc::clone(&listed);
            let release = Arc::clone(&release);
            async move {
                let body = if request.uri().path().ends_with("inferenceproviders") {
                    assert!(
                        request
                            .uri()
                            .query()
                            .is_none_or(|query| !query.contains("resourceVersion=0")),
                        "collection requires a consistent API snapshot"
                    );
                    listed.notify_one();
                    release.notified().await;
                    serde_json::json!({"apiVersion":"grid.praxis.fast/v1alpha1","kind":"InferenceProviderList","items":[provider]})
                } else if request.uri().path().contains("configmaps/grid-swim") {
                    serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"grid-swim-revision-hwm-east"},"data":{"nodeGenerationHighWatermark":high.to_string()}})
                } else {
                    serde_json::json!({"apiVersion":"grid.praxis.fast/v1alpha1","kind":"GridSiteList","items":[]})
                };
                http::Response::builder().body(kube::client::Body::from(body.to_string().into_bytes()))
            }
        });
        Client::new(service, "default")
    }

    /// A source provider with a valid Kubernetes object identity.
    fn provider() -> Result<InferenceProvider, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "model", "uid": "u1", "generation": 1},
            "spec": {"gridNetworkRef": "net", "providerKind": "self_hosted", "backendKind": "local", "endpoint": "http://model:8000", "models": [{"name": "model"}]}
        }))
    }

    /// Linearizable API fixture: each GET reads the current persisted reservation.
    fn fence_client(active: Arc<AtomicU64>) -> Client {
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let high = active.load(Ordering::Relaxed);
            async move {
                assert_eq!(request.method(), http::Method::GET);
                assert_eq!(
                    request.uri().path(),
                    "/api/v1/namespaces/default/configmaps/grid-swim-revision-hwm-east"
                );
                assert!(
                    request.uri().query().is_none(),
                    "never request cached resourceVersion=0"
                );
                let (status, body) = if high == 0 {
                    (
                        404,
                        serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","message":"missing","code":404}),
                    )
                } else {
                    (
                        200,
                        serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"grid-swim-revision-hwm-east"},"data":{"nodeGenerationHighWatermark":high.to_string()}}),
                    )
                };
                http::Response::builder()
                    .status(status)
                    .body(kube::client::Body::from(body.to_string().into_bytes()))
            }
        });
        Client::new(service, "default")
    }
}
