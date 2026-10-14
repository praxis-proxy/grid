//! Read source-local pool metadata using the hosting cluster's client.
//! A UID needs authenticated site scoping and does not bind cached metrics.

use kube::{
    Api, Client,
    core::{ApiResource, DynamicObject, GroupVersionKind, ObjectMeta},
};

use crate::crd::inference_provider::InferencePoolRef;

/// Source resolution without conflating unconfigured and unresolved providers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PoolResolution {
    /// No pool reference: preserve the identityless provider path.
    Legacy,
    /// The object was not found or has no UID. This does not prove physical deletion.
    Unresolved,
    /// Source UID, to be scoped by the authenticated hosting site before publication.
    Resolved(String),
}

/// A failed lookup must not authorize legacy fallback or predecessor promotion.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResolveError {
    /// Invalid explicit name or namespace.
    #[error("invalid inference pool reference")]
    InvalidReference,
    /// Kubernetes refused the read or could not return it.
    #[error("inference pool lookup failed: {0}")]
    Kubernetes(#[from] kube::Error),
    /// The API response did not match the requested object.
    #[error("invalid inference pool identity metadata")]
    InvalidMetadata,
}

/// Read the exact referenced source pool's UID without consulting telemetry.
///
/// An absent reference makes no API request. A configured reference never falls
/// back to legacy on failure. Callers must order concurrent reads before using
/// their results and must not treat unresolved identity as topology withdrawal.
///
/// # Errors
///
/// Returns [`ResolveError`] for invalid references, API failures or invalid metadata.
pub(crate) async fn resolve(
    client: &Client,
    reference: Option<&InferencePoolRef>,
) -> Result<PoolResolution, ResolveError> {
    let Some(reference) = reference else {
        return Ok(PoolResolution::Legacy);
    };
    if !reference.is_valid() {
        return Err(ResolveError::InvalidReference);
    }
    let Some(metadata) = source_metadata(client, reference).await? else {
        return Ok(PoolResolution::Unresolved);
    };
    if metadata.name.as_deref() != Some(&reference.name) || metadata.namespace.as_deref() != Some(&reference.namespace)
    {
        return Err(ResolveError::InvalidMetadata);
    }
    let uid = metadata.uid.filter(|value| !value.trim().is_empty());
    Ok(uid.map_or(PoolResolution::Unresolved, PoolResolution::Resolved))
}

/// Read current metadata, distinguishing a missing object from an unavailable API.
async fn source_metadata(client: &Client, reference: &InferencePoolRef) -> Result<Option<ObjectMeta>, kube::Error> {
    let resource = ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("inference.networking.k8s.io", "v1", "InferencePool"),
        "inferencepools",
    );
    let api = Api::<DynamicObject>::namespaced_with(client.clone(), &reference.namespace, &resource);
    match api.get_metadata(&reference.name).await {
        Ok(pool) => Ok(Some(pool.metadata)),
        Err(kube::Error::Api(status))
            if status.is_not_found()
                && status.details.as_ref().is_some_and(|details| {
                    details.name == reference.name && details.group == resource.group && details.kind == resource.plural
                }) =>
        {
            Ok(None)
        },
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    use serde_json::{Value, json};

    use super::*;

    /// An identityless provider never contacts the Kubernetes pool API.
    #[tokio::test]
    async fn absent_reference_is_legacy_without_io() {
        let (client, requests) = scripted_client(vec![]);
        assert_eq!(resolve(&client, None).await.ok(), Some(PoolResolution::Legacy));
        assert!(requests.lock().unwrap_or_else(|_| std::process::abort()).is_empty());
    }

    /// Resolve the source object using a metadata-only request to the explicit namespace.
    #[tokio::test]
    async fn exact_namespaced_metadata_lookup() {
        let (client, requests) = scripted_client(vec![(200, metadata(Some("uid-a")))]);
        assert_eq!(
            resolve(&client, Some(&reference())).await.ok(),
            Some(PoolResolution::Resolved("uid-a".to_owned()))
        );
        let mut recorded = requests.lock().unwrap_or_else(|_| std::process::abort());
        assert_eq!(recorded.len(), 1);
        let request = recorded.pop().unwrap_or_else(|| std::process::abort());
        drop(recorded);
        assert_eq!(request.method(), http::Method::GET);
        assert_eq!(
            request.uri().path(),
            "/apis/inference.networking.k8s.io/v1/namespaces/serving/inferencepools/pool"
        );
        assert!(
            request.uri().query().is_none(),
            "never request resourceVersion=0 cached reads"
        );
        let accept = request
            .headers()
            .get(http::header::ACCEPT)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        assert!(
            accept.contains("as=PartialObjectMetadata"),
            "read identity without depending on the pool spec"
        );
    }

    /// Same-name recreation changes UID even when the reader never sees absence.
    #[tokio::test]
    async fn recreation_needs_no_intermediate_not_found() {
        let (client, _) = scripted_client(vec![(200, metadata(Some("uid-a"))), (200, metadata(Some("uid-b")))]);
        for uid in ["uid-a", "uid-b"] {
            assert_eq!(
                resolve(&client, Some(&reference())).await.ok(),
                Some(PoolResolution::Resolved(uid.to_owned()))
            );
        }
    }

    /// Desired-spec and watch revisions do not identify a new object incarnation.
    #[tokio::test]
    async fn metadata_revision_changes_preserve_uid() {
        let original = metadata(Some("uid-a"));
        let mut revised = original.clone();
        let fields = revised
            .get_mut("metadata")
            .and_then(Value::as_object_mut)
            .unwrap_or_else(|| std::process::abort());
        fields.insert("generation".to_owned(), json!(4));
        fields.insert("resourceVersion".to_owned(), json!("999"));
        fields.insert("labels".to_owned(), json!({"display": "renamed"}));
        let (client, _) = scripted_client(vec![(200, original), (200, revised)]);
        for _ in 0..2 {
            assert_eq!(
                resolve(&client, Some(&reference())).await.ok(),
                Some(PoolResolution::Resolved("uid-a".to_owned()))
            );
        }
    }

    /// Missing or UID-less objects never downgrade an opted-in provider to legacy.
    #[tokio::test]
    async fn missing_identity_stays_unresolved() {
        let (client, _) = scripted_client(vec![
            (404, missing_pool()),
            (200, metadata(None)),
            (200, metadata(Some(""))),
            (200, metadata(Some(" "))),
        ]);
        for _ in 0..4 {
            assert_eq!(
                resolve(&client, Some(&reference())).await.ok(),
                Some(PoolResolution::Unresolved)
            );
        }
    }

    /// Failed reads cannot recover a UID from an earlier successful lookup.
    #[tokio::test]
    async fn api_errors_do_not_reuse_prior_resolution() {
        for code in [403, 404, 429, 500, 504] {
            let (client, _) = scripted_client(vec![(200, metadata(Some("uid-a"))), (code, failure(code))]);
            assert_eq!(
                resolve(&client, Some(&reference())).await.ok(),
                Some(PoolResolution::Resolved("uid-a".to_owned()))
            );
            assert!(matches!(
                resolve(&client, Some(&reference())).await,
                Err(ResolveError::Kubernetes(_))
            ));
        }
    }

    /// A 404 for another resource cannot establish absence of this pool.
    #[tokio::test]
    async fn mismatched_not_found_details_remain_errors() {
        for field in ["name", "group", "kind"] {
            let mut response = missing_pool();
            response
                .get_mut("details")
                .and_then(Value::as_object_mut)
                .unwrap_or_else(|| std::process::abort())
                .insert(field.to_owned(), json!("another"));
            let (client, _) = scripted_client(vec![(404, response)]);
            assert!(matches!(
                resolve(&client, Some(&reference())).await,
                Err(ResolveError::Kubernetes(_))
            ));
        }
    }

    /// Transport failure is unresolved authority, never legacy fallback.
    #[tokio::test]
    async fn transport_timeout_remains_an_error() {
        let service = tower::service_fn(|_request: http::Request<kube::client::Body>| async {
            Err::<http::Response<kube::client::Body>, _>(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "scripted timeout",
            ))
        });
        let client = Client::new(service, "default");
        assert!(matches!(
            resolve(&client, Some(&reference())).await,
            Err(ResolveError::Kubernetes(_))
        ));
    }

    /// Undecodable and mismatched responses cannot establish a source identity.
    #[tokio::test]
    async fn malformed_metadata_is_rejected() {
        let (client, _) = scripted_client(vec![(200, json!("not an object"))]);
        assert!(matches!(
            resolve(&client, Some(&reference())).await,
            Err(ResolveError::Kubernetes(_))
        ));
        for field in ["name", "namespace"] {
            let mut response = metadata(Some("uid-a"));
            response
                .get_mut("metadata")
                .and_then(Value::as_object_mut)
                .unwrap_or_else(|| std::process::abort())
                .insert(field.to_owned(), json!("another"));
            let (mismatched, _) = scripted_client(vec![(200, response)]);
            assert!(matches!(
                resolve(&mismatched, Some(&reference())).await,
                Err(ResolveError::InvalidMetadata)
            ));
        }
    }

    /// Invalid references cannot become requests against a different API path.
    #[tokio::test]
    async fn invalid_reference_is_rejected_before_io() {
        let (client, requests) = scripted_client(vec![]);
        for invalid in ["", " pool", "a/b", "../secrets"] {
            let mut pool = reference();
            pool.name = invalid.to_owned();
            assert!(matches!(
                resolve(&client, Some(&pool)).await,
                Err(ResolveError::InvalidReference)
            ));
            pool = reference();
            pool.namespace = invalid.to_owned();
            assert!(matches!(
                resolve(&client, Some(&pool)).await,
                Err(ResolveError::InvalidReference)
            ));
        }
        assert!(requests.lock().unwrap_or_else(|_| std::process::abort()).is_empty());
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// Recorded HTTP requests, including metadata content negotiation.
    type Requests = Arc<Mutex<Vec<http::Request<kube::client::Body>>>>;

    /// Source pool fixture, deliberately outside the client's default namespace.
    fn reference() -> InferencePoolRef {
        InferencePoolRef {
            name: "pool".to_owned(),
            namespace: "serving".to_owned(),
        }
    }

    /// Metadata returned by the hosting cluster's Kubernetes API.
    fn metadata(uid: Option<&str>) -> Value {
        json!({"apiVersion": "meta.k8s.io/v1", "kind": "PartialObjectMetadata", "metadata": {
            "name": "pool", "namespace": "serving", "uid": uid, "generation": 1, "resourceVersion": "10"
        }})
    }

    /// Kubernetes API error body corresponding to the HTTP status.
    fn failure(code: u16) -> Value {
        let reason = match code {
            404 => "NotFound",
            403 => "Forbidden",
            429 => "TooManyRequests",
            504 => "Timeout",
            _ => "InternalError",
        };
        json!({"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": reason, "code": code})
    }

    /// An object-specific 404, distinct from an API route that does not exist.
    fn missing_pool() -> Value {
        let mut response = failure(404);
        response
            .as_object_mut()
            .unwrap_or_else(|| std::process::abort())
            .insert(
                "details".to_owned(),
                json!({
                    "group": "inference.networking.k8s.io", "kind": "inferencepools", "name": "pool"
                }),
            );
        response
    }

    /// A real kube client over a deterministic HTTP service, without a cluster.
    fn scripted_client(responses: Vec<(u16, Value)>) -> (Client, Requests) {
        let queued = Arc::new(Mutex::new(VecDeque::from(responses)));
        let requests = Requests::default();
        let recorded = Arc::clone(&requests);
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let (code, body) = queued
                .lock()
                .unwrap_or_else(|_| std::process::abort())
                .pop_front()
                .unwrap_or_else(|| std::process::abort());
            recorded.lock().unwrap_or_else(|_| std::process::abort()).push(request);
            async move {
                let response = http::Response::builder()
                    .status(code)
                    .body(kube::client::Body::from(body.to_string().into_bytes()))
                    .unwrap_or_else(|_| std::process::abort());
                Ok::<_, Infallible>(response)
            }
        });
        (Client::new(service, "default"), requests)
    }
}
