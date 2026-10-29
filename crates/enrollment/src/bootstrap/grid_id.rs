//! Stable grid identity owned by bootstrap, independently of the signing CA.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Api, Client,
    api::{ApiResource, DynamicObject, GroupVersionKind, ObjectMeta, PostParams},
};

use super::BoxError;

/// Load bootstrap's persisted ID, or adopt the existing grid's ID before minting one.
/// Creation is atomic: concurrent bootstrap Jobs read the winning object on conflict.
#[expect(
    clippy::large_stack_frames,
    reason = "one-shot bootstrap holds Kubernetes objects off the request path"
)]
pub(super) async fn ensure(client: &Client, namespace: &str, name: &str, network: &str) -> Result<String, BoxError> {
    let states: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    if let Some(state) = Box::pin(states.get_opt(name)).await? {
        return stored_id(&state);
    }
    let resource = ApiResource::from_gvk(&GroupVersionKind::gvk("grid.praxis.fast", "v1alpha1", "GridNetwork"));
    let networks: Api<DynamicObject> = Api::all_with(client.clone(), &resource);
    let existing = Box::pin(networks.get_opt(network)).await?;
    let adopted = existing.as_ref().and_then(network_id);
    let id = adopted.map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);
    validate(&id)?;
    let state = ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/managed-by".to_owned(),
                "grid-enrollment-bootstrap".to_owned(),
            )])),
            annotations: Some(BTreeMap::from([
                ("helm.sh/resource-policy".to_owned(), "keep".to_owned()),
                (
                    "argocd.argoproj.io/sync-options".to_owned(),
                    "Prune=false,Delete=false".to_owned(),
                ),
            ])),
            ..ObjectMeta::default()
        },
        data: Some(BTreeMap::from([("gridId".to_owned(), id)])),
        immutable: Some(true),
        ..ConfigMap::default()
    };
    let persisted = match Box::pin(states.create(&PostParams::default(), &state)).await {
        Ok(created) => stored_id(&created)?,
        Err(kube::Error::Api(error)) if error.code == 409 => stored_id(&Box::pin(states.get(name)).await?)?,
        Err(error) => return Err(error.into()),
    };
    if adopted.is_some_and(|previous| previous != persisted) {
        return Err(
            "bootstrap state conflicts with the existing GridNetwork ID; refusing to change grid identity".into(),
        );
    }
    Ok(persisted)
}

/// Prefer the declared ID, retaining legacy status-only IDs during migration.
fn network_id(network: &DynamicObject) -> Option<&str> {
    ["/spec/gridId", "/status/gridId"]
        .into_iter()
        .filter_map(|path| network.data.pointer(path).and_then(serde_json::Value::as_str))
        .find(|id| !id.is_empty())
}

/// Refuse damaged bootstrap state instead of generating a replacement identity.
fn stored_id(state: &ConfigMap) -> Result<String, BoxError> {
    let id = state
        .data
        .as_ref()
        .and_then(|data| data.get("gridId"))
        .ok_or("bootstrap ConfigMap has no gridId; restore its original value")?;
    validate(id)?;
    Ok(id.clone())
}

/// Match `GridNetwork`'s nonempty DNS-label grid ID constraint.
fn validate(id: &str) -> Result<(), BoxError> {
    let alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if id.is_empty()
        || id.len() > 63
        || !id.bytes().all(|byte| alphanumeric(byte) || byte == b'-')
        || !id.bytes().next().is_some_and(alphanumeric)
        || !id.bytes().next_back().is_some_and(alphanumeric)
    {
        return Err("gridId must be a nonempty DNS label of at most 63 characters".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use status_harness::FakeApi;

    use super::*;

    #[tokio::test]
    async fn a_new_grid_gets_one_persisted_id_and_retries_only_read_it() -> Result<(), BoxError> {
        let api = FakeApi::new();
        let client = api.client("grid");
        let first = ensure(&client, "grid", "grid-state", "grid").await?;
        assert_eq!(uuid::Uuid::parse_str(&first)?.get_version_num(), 4);
        assert_eq!(ensure(&client, "grid", "grid-state", "grid").await?, first);
        let writes = api.writes();
        assert_eq!(writes.len(), 1);
        let write = writes.first().ok_or("bootstrap did not write its state")?;
        assert_eq!(write.method, "POST");
        assert_eq!(write.body.get("immutable"), Some(&json!(true)));
        assert_eq!(write.body.pointer("/data/gridId"), Some(&json!(first)));
        Ok(())
    }

    #[tokio::test]
    async fn bootstrap_adopts_declared_or_legacy_ids_without_touching_the_network() -> Result<(), BoxError> {
        for (spec, status, expected) in [("declared", "old", "declared"), ("", "legacy", "legacy")] {
            let api = FakeApi::new();
            api.put(
                "/apis/grid.praxis.fast/v1alpha1/gridnetworks/grid",
                json!({
                    "apiVersion": "grid.praxis.fast/v1alpha1", "kind": "GridNetwork",
                    "metadata": {"name": "grid"}, "spec": {"gridId": spec}, "status": {"gridId": status}
                }),
            );
            assert_eq!(
                ensure(&api.client("grid"), "grid", "grid-state", "grid").await?,
                expected
            );
            assert!(
                api.writes()
                    .iter()
                    .all(|write| write.method == "POST" && write.path == "/api/v1/namespaces/grid/configmaps")
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_state_and_failed_reads_never_mint_another_id() {
        for data in [json!({}), json!({"gridId": ""}), json!({"gridId": "not/a/grid"})] {
            let api = FakeApi::new();
            api.put(
                "/api/v1/namespaces/grid/configmaps/grid-state",
                json!({
                    "apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "grid-state"}, "data": data
                }),
            );
            let result = ensure(&api.client("grid"), "grid", "grid-state", "grid").await;
            assert!(matches!(result, Err(error) if error.to_string().contains("gridId")));
            assert!(api.writes().is_empty());
        }
        let api = FakeApi::new();
        api.fail_reads("/api/v1/namespaces/grid/configmaps/grid-state", 1);
        let result = ensure(&api.client("grid"), "grid", "grid-state", "grid").await;
        assert!(matches!(result, Err(error) if !error.to_string().is_empty()));
        assert!(api.writes().is_empty());
    }
}
