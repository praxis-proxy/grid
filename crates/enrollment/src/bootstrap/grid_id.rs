//! Stable grid identity owned by bootstrap, independently of the signing CA.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Api, Client,
    api::{ObjectMeta, PostParams},
};

use super::BoxError;

/// Load bootstrap's persisted ID, or mint and persist one. It reads no Grid kind, so the
/// hub needs no Grid CRD; a migrating grid pre-creates the `ConfigMap` with its ID.
/// Creation is atomic: concurrent bootstrap Jobs read the winning object on conflict.
pub(super) async fn ensure(client: &Client, namespace: &str, name: &str) -> Result<String, BoxError> {
    let states: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    if let Some(state) = Box::pin(states.get_opt(name)).await? {
        return stored_id(&state);
    }
    let id = uuid::Uuid::new_v4().to_string();
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
    match Box::pin(states.create(&PostParams::default(), &state)).await {
        Ok(created) => stored_id(&created),
        Err(kube::Error::Api(error)) if error.code == 409 => stored_id(&Box::pin(states.get(name)).await?),
        Err(error) => Err(error.into()),
    }
}

/// Refuse damaged or mutable bootstrap state instead of generating a replacement identity.
fn stored_id(state: &ConfigMap) -> Result<String, BoxError> {
    let id = state
        .data
        .as_ref()
        .and_then(|data| data.get("gridId"))
        .ok_or("bootstrap ConfigMap has no gridId; restore its original value")?;
    validate(id)?;
    // A mutable state could be edited into another grid ID after members adopted this one.
    if state.immutable != Some(true) {
        // Patch in place: recreating it opens a window in which bootstrap mints a new ID.
        return Err("bootstrap ConfigMap must set immutable: true; patch it in place with {\"immutable\":true}, do not recreate it".into());
    }
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
        let first = ensure(&client, "grid", "grid-state").await?;
        assert_eq!(uuid::Uuid::parse_str(&first)?.get_version_num(), 4);
        assert_eq!(
            ensure(&client, "grid", "grid-state").await?,
            first,
            "a retry must reuse the saved id"
        );
        let writes = api.writes();
        assert_eq!(writes.len(), 1, "only the first run writes the state");
        let write = writes.first().ok_or("bootstrap did not write its state")?;
        assert_eq!(write.method, "POST");
        assert_eq!(write.body.get("immutable"), Some(&json!(true)));
        assert_eq!(write.body.pointer("/data/gridId"), Some(&json!(first)));
        Ok(())
    }

    #[tokio::test]
    async fn a_precreated_state_keeps_its_id_and_mints_none() -> Result<(), BoxError> {
        let api = FakeApi::new();
        api.put(
            "/api/v1/namespaces/grid/configmaps/grid-state",
            json!({
                "apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "grid-state"},
                "data": {"gridId": "existing-grid"}, "immutable": true
            }),
        );
        assert_eq!(
            ensure(&api.client("grid"), "grid", "grid-state").await?,
            "existing-grid",
            "a pre-created state keeps its id"
        );
        assert!(api.writes().is_empty(), "a pre-created state is never rewritten");
        Ok(())
    }

    #[tokio::test]
    async fn a_mutable_precreated_state_is_refused() {
        for immutable in [json!(null), json!(false)] {
            let api = FakeApi::new();
            api.put(
                "/api/v1/namespaces/grid/configmaps/grid-state",
                json!({
                    "apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "grid-state"},
                    "data": {"gridId": "existing-grid"}, "immutable": immutable
                }),
            );
            let result = ensure(&api.client("grid"), "grid", "grid-state").await;
            assert!(
                matches!(&result, Err(error) if error.to_string().contains("immutable: true")),
                "immutable={immutable}: {result:?}"
            );
            assert!(api.writes().is_empty(), "a refused state is never rewritten");
        }
    }

    #[tokio::test]
    async fn a_lost_create_race_returns_the_winning_id() -> Result<(), BoxError> {
        let api = FakeApi::new();
        api.win_create_race(
            "/api/v1/namespaces/grid/configmaps/grid-state",
            json!({
                "apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "grid-state"},
                "data": {"gridId": "winning-grid"}, "immutable": true
            }),
        );
        assert_eq!(
            ensure(&api.client("grid"), "grid", "grid-state").await?,
            "winning-grid",
            "the loser of a create race adopts the winner's id"
        );
        let writes = api.writes();
        assert_eq!(writes.len(), 1, "the loser posts once and then only reads");
        assert!(writes.iter().all(|write| write.method == "POST"));
        assert_eq!(
            api.get("/api/v1/namespaces/grid/configmaps/grid-state")
                .and_then(|state| state.pointer("/data/gridId").cloned()),
            Some(json!("winning-grid")),
            "the winner's state is never overwritten"
        );
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_state_and_failed_reads_never_mint_another_id() {
        for data in [json!({}), json!({"gridId": ""}), json!({"gridId": "not/a/grid"})] {
            let api = FakeApi::new();
            api.put(
                "/api/v1/namespaces/grid/configmaps/grid-state",
                json!({
                    "apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "grid-state"}, "data": data,
                    "immutable": true
                }),
            );
            let result = ensure(&api.client("grid"), "grid", "grid-state").await;
            assert!(matches!(result, Err(error) if error.to_string().contains("gridId")));
            assert!(api.writes().is_empty());
        }
        let api = FakeApi::new();
        api.fail_reads("/api/v1/namespaces/grid/configmaps/grid-state", 1);
        let result = ensure(&api.client("grid"), "grid", "grid-state").await;
        assert!(matches!(result, Err(error) if !error.to_string().is_empty()));
        assert!(api.writes().is_empty());
    }
}
