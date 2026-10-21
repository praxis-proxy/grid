//! Create this site's `GridNetwork` and `GridSite` once its identity exists, create-only.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{
    Api, Client,
    api::{DeleteParams, ObjectMeta, PostParams},
};

use crate::{
    controller::grid_network::GridModes,
    crd::{
        grid_network::{GridNetwork, GridNetworkSpec, PeerTrustConfig, SecretRef, SignalTransportConfig, TlsConfig},
        grid_site::{GridSite, GridSiteSpec},
    },
    error::OperatorError,
    local_site::PROVIDER_SITE_LABEL,
    site_name::{self, Identity},
};

/// Label on the records the operator created, so they read apart from declared ones.
pub const CREATED_BY_LABEL: &str = "grid.praxis.fast/created-by";

/// The operator's value for [`CREATED_BY_LABEL`].
const CREATED_BY: &str = "grid-operator";

/// `ConfigMap` recording the creation, so an admin's delete is not undone.
pub const MARKER: &str = "grid-operator-local-records";

/// What the operator creates its records from.
#[derive(Clone, Debug)]
pub struct Plan {
    /// `GridNetwork` to create when none exists.
    pub network: String,
    /// This site's name, from its identity.
    pub site: String,
    /// The identity Secret.
    pub identity: Identity,
    /// The grid CA Secret beside it.
    pub ca_secret: String,
    /// The SWIM key Secret beside it, when it exists.
    pub swim_key_secret: Option<String>,
    /// The modes the install starts in, declared so the network never contradicts them.
    pub modes: GridModes,
}

/// What [`ensure`] did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    /// The marker was already there, so nothing was checked.
    AlreadyCreated,
    /// The records were checked, each flag saying whether it was created now.
    Checked {
        /// The `GridNetwork` was created.
        network: bool,
        /// The `GridSite` was created.
        site: bool,
    },
}

/// The `GridNetwork` for `plan`, naming its identity Secrets.
///
/// # Errors
///
/// Returns [`OperatorError::Json`] if the empty spec does not deserialize.
pub fn grid_network(plan: &Plan) -> Result<GridNetwork, OperatorError> {
    let beside = |name: &str| SecretRef {
        name: name.to_owned(),
        namespace: plan.identity.namespace.clone(),
        key: None,
    };
    let mut spec: GridNetworkSpec = serde_json::from_value(serde_json::json!({}))?;
    spec.tls = TlsConfig {
        site_secret_ref: Some(beside(&plan.identity.secret)),
        ca_secret_ref: Some(beside(&plan.ca_secret)),
        swim_key_secret_ref: plan.swim_key_secret.as_deref().map(beside),
    };
    spec.signal_transport = Some(SignalTransportConfig {
        mode: plan.modes.signal,
    });
    spec.peer_trust = Some(PeerTrustConfig { mode: plan.modes.trust });
    let mut network = GridNetwork::new(&plan.network, spec);
    network.metadata.labels = Some(created_by());
    Ok(network)
}

/// The `GridSite` for `plan` in `network`, labelled so its providers match it.
#[must_use]
pub fn grid_site(plan: &Plan, network: &str) -> GridSite {
    let spec = GridSiteSpec {
        grid_network_ref: network.to_owned(),
        gateway_endpoint: None,
        region: None,
        sovereignty_zone: None,
        zone: None,
        trust: None,
    };
    let mut site = GridSite::new(&plan.site, spec);
    let mut labels = created_by();
    labels.insert(PROVIDER_SITE_LABEL.to_owned(), plan.site.clone());
    site.metadata.labels = Some(labels);
    site
}

/// Create whichever of this site's records is missing, once.
///
/// The marker goes first, so a crash leaves records missing rather than undoing an admin's
/// delete. A failed create removes it again, so the next start retries.
///
/// # Errors
///
/// Returns the [`OperatorError`] of a read or create that fails other than by conflict.
pub async fn ensure(client: &Client, namespace: &str, plan: &Plan) -> Result<Outcome, OperatorError> {
    let markers: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    if !Box::pin(mark(&markers, plan)).await? {
        return Ok(Outcome::AlreadyCreated);
    }
    let created = Box::pin(create_records(client, plan)).await;
    if created.is_err()
        && let Err(error) = Box::pin(markers.delete(MARKER, &DeleteParams::default())).await
    {
        tracing::warn!(%error, "local records marker not removed; delete {MARKER} to retry");
    }
    created
}

/// Create the missing `GridNetwork` and `GridSite`, reusing an existing network.
async fn create_records(client: &Client, plan: &Plan) -> Result<Outcome, OperatorError> {
    let existing = Box::pin(site_name::sole_network(client)).await?;
    let (network, created_network) = if let Some(found) = existing {
        (found.metadata.name.unwrap_or_default(), false)
    } else {
        let api: Api<GridNetwork> = Api::all(client.clone());
        let created = Box::pin(create(&api, &grid_network(plan)?)).await?;
        (plan.network.clone(), created)
    };
    let sites: Api<GridSite> = Api::all(client.clone());
    let created_site = Box::pin(create(&sites, &grid_site(plan, &network))).await?;
    Ok(Outcome::Checked {
        network: created_network,
        site: created_site,
    })
}

/// Write the marker, `false` when it was already there.
async fn mark(markers: &Api<ConfigMap>, plan: &Plan) -> Result<bool, OperatorError> {
    let marker = ConfigMap {
        metadata: ObjectMeta {
            name: Some(MARKER.to_owned()),
            labels: Some(created_by()),
            ..ObjectMeta::default()
        },
        data: Some(BTreeMap::from([("site".to_owned(), plan.site.clone())])),
        ..ConfigMap::default()
    };
    create(markers, &marker).await
}

/// Create `object`, `false` when one of its name already exists.
async fn create<K>(api: &Api<K>, object: &K) -> Result<bool, OperatorError>
where
    K: kube::Resource + Clone + std::fmt::Debug + serde::de::DeserializeOwned + serde::Serialize + Send + Sync,
{
    match Box::pin(api.create(&PostParams::default(), object)).await {
        Ok(_) => Ok(true),
        Err(error) => {
            let error = OperatorError::from(error);
            if error.is_conflict() { Ok(false) } else { Err(error) }
        },
    }
}

/// The labels marking a record as the operator's.
fn created_by() -> BTreeMap<String, String> {
    BTreeMap::from([(CREATED_BY_LABEL.to_owned(), CREATED_BY.to_owned())])
}

/// The SWIM key Secret's name when it exists beside the identity.
///
/// # Errors
///
/// Returns [`OperatorError::Kube`] when reading the Secret fails.
pub async fn swim_key_beside(
    client: &Client,
    identity: &Identity,
    name: &str,
) -> Result<Option<String>, OperatorError> {
    let found = Box::pin(Api::<Secret>::namespaced(client.clone(), &identity.namespace).get_opt(name)).await?;
    Ok(found.map(|_| name.to_owned()))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::crd::grid_network::{PeerTrustMode, SignalMode};

    /// One scripted apiserver answer: method, path suffix, status code, body.
    type Answer = (&'static str, &'static str, u16, serde_json::Value);

    fn plan(swim_key: Option<&str>) -> Plan {
        Plan {
            network: "grid".to_owned(),
            site: "hub".to_owned(),
            identity: Identity {
                namespace: "grid".to_owned(),
                secret: "grid-site-identity".to_owned(),
            },
            ca_secret: "grid-ca".to_owned(),
            swim_key_secret: swim_key.map(str::to_owned),
            modes: GridModes::without_network(Some(SignalMode::Poll), Some(PeerTrustMode::Pin)),
        }
    }

    /// An apiserver answering from `script`, 404 otherwise, recording each request.
    fn apiserver(script: Vec<Answer>) -> (Client, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
            let (method, path) = (req.method().to_string(), req.uri().path().to_owned());
            log.lock().expect("log").push(format!("{method} {path}"));
            let (code, body) = script
                .iter()
                .find(|(m, suffix, ..)| *m == method && path.ends_with(suffix))
                .map_or_else(|| (404, status(404)), |(.., code, body)| (*code, body.clone()));
            async move {
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(code)
                        .body(kube::client::Body::from(serde_json::to_vec(&body).expect("body")))
                        .expect("response"),
                )
            }
        });
        (Client::new(service, "grid"), seen)
    }

    fn status(code: u16) -> serde_json::Value {
        serde_json::json!({ "kind": "Status", "apiVersion": "v1", "status": "Failure", "code": code })
    }

    fn networks(items: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1", "kind": "GridNetworkList", "metadata": {}, "items": items,
        })
    }

    fn object(kind: &str, name: &str) -> serde_json::Value {
        let api_version = if kind == "ConfigMap" {
            "v1"
        } else {
            "grid.praxis.fast/v1alpha1"
        };
        serde_json::json!({
            "apiVersion": api_version, "kind": kind, "metadata": { "name": name },
            "spec": { "gridNetworkRef": "grid" },
        })
    }

    fn requested(seen: &Arc<Mutex<Vec<String>>>, method: &str, suffix: &str) -> bool {
        seen.lock()
            .expect("log")
            .iter()
            .any(|r| r.starts_with(method) && r.ends_with(suffix))
    }

    #[test]
    fn the_network_names_the_identity_secrets_and_its_creator() {
        let network = grid_network(&plan(Some("grid-swim-key"))).expect("network");
        let tls = &network.spec.tls;
        let site = tls.site_secret_ref.as_ref().expect("site ref");
        assert_eq!(
            (site.name.as_str(), site.namespace.as_str()),
            ("grid-site-identity", "grid")
        );
        assert_eq!(tls.ca_secret_ref.as_ref().map(|r| r.name.as_str()), Some("grid-ca"));
        assert_eq!(
            tls.swim_key_secret_ref.as_ref().map(|r| r.name.as_str()),
            Some("grid-swim-key")
        );
        let labels = network.metadata.labels.expect("labels");
        assert_eq!(labels.get(CREATED_BY_LABEL).map(String::as_str), Some("grid-operator"));
    }

    #[test]
    fn the_network_declares_the_install_modes() {
        let network = grid_network(&plan(None)).expect("network");
        assert_eq!(network.spec.signal_transport.map(|t| t.mode), Some(SignalMode::Poll));
        assert_eq!(network.spec.peer_trust.map(|t| t.mode), Some(PeerTrustMode::Pin));
    }

    #[test]
    fn a_missing_swim_key_is_left_out() {
        let network = grid_network(&plan(None)).expect("network");
        assert!(network.spec.tls.swim_key_secret_ref.is_none());
    }

    #[test]
    fn the_site_is_labelled_so_its_providers_match_it() {
        let site = grid_site(&plan(None), "dagobah-local");
        assert_eq!(site.metadata.name.as_deref(), Some("hub"));
        assert_eq!(site.spec.grid_network_ref, "dagobah-local");
        let labels = site.metadata.labels.expect("labels");
        assert_eq!(labels.get(PROVIDER_SITE_LABEL).map(String::as_str), Some("hub"));
        assert_eq!(labels.get(CREATED_BY_LABEL).map(String::as_str), Some("grid-operator"));
    }

    #[tokio::test]
    async fn a_present_marker_writes_nothing_else() {
        let (client, seen) = apiserver(vec![("POST", "/configmaps", 409, status(409))]);
        let outcome = Box::pin(ensure(&client, "grid", &plan(None))).await.expect("ensure");
        assert_eq!(outcome, Outcome::AlreadyCreated);
        assert_eq!(seen.lock().expect("log").len(), 1, "only the marker write");
    }

    #[tokio::test]
    async fn a_fresh_site_gets_both_records_after_the_marker() {
        let (client, seen) = apiserver(vec![
            ("POST", "/configmaps", 201, object("ConfigMap", MARKER)),
            ("GET", "/gridnetworks", 200, networks(&serde_json::json!([]))),
            ("POST", "/gridnetworks", 201, object("GridNetwork", "grid")),
            ("POST", "/gridsites", 201, object("GridSite", "hub")),
        ]);
        let outcome = Box::pin(ensure(&client, "grid", &plan(None))).await.expect("ensure");
        assert_eq!(
            outcome,
            Outcome::Checked {
                network: true,
                site: true
            }
        );
        let first = seen.lock().expect("log").first().cloned().unwrap_or_default();
        assert!(first.ends_with("/configmaps"), "the marker goes first, got {first}");
    }

    #[tokio::test]
    async fn an_existing_network_is_used_and_an_existing_site_left_alone() {
        let existing = serde_json::json!([{ "metadata": { "name": "dagobah-local" }, "spec": {} }]);
        let (client, seen) = apiserver(vec![
            ("POST", "/configmaps", 201, object("ConfigMap", MARKER)),
            ("GET", "/gridnetworks", 200, networks(&existing)),
            ("POST", "/gridsites", 409, status(409)),
        ]);
        let outcome = Box::pin(ensure(&client, "grid", &plan(None))).await.expect("ensure");
        assert_eq!(
            outcome,
            Outcome::Checked {
                network: false,
                site: false
            }
        );
        assert!(!requested(&seen, "POST", "/gridnetworks"), "no second network");
    }

    #[tokio::test]
    async fn a_refused_network_create_is_an_error_and_no_site_follows() {
        let (client, seen) = apiserver(vec![
            ("POST", "/configmaps", 201, object("ConfigMap", MARKER)),
            ("GET", "/gridnetworks", 200, networks(&serde_json::json!([]))),
            ("POST", "/gridnetworks", 403, status(403)),
        ]);
        ensure(&client, "grid", &plan(None))
            .await
            .expect_err("a 403 is not a conflict");
        assert!(!requested(&seen, "POST", "/gridsites"));
    }

    #[tokio::test]
    async fn a_failed_create_removes_the_marker_so_the_next_start_retries() {
        let (client, seen) = apiserver(vec![
            ("POST", "/configmaps", 201, object("ConfigMap", MARKER)),
            ("GET", "/gridnetworks", 500, status(500)),
            ("DELETE", "/configmaps/grid-operator-local-records", 200, status(200)),
        ]);
        Box::pin(ensure(&client, "grid", &plan(None)))
            .await
            .expect_err("the list failed");
        assert!(requested(&seen, "DELETE", "/configmaps/grid-operator-local-records"));
    }

    #[tokio::test]
    async fn a_failed_marker_write_creates_nothing() {
        let (client, seen) = apiserver(vec![("POST", "/configmaps", 500, status(500))]);
        ensure(&client, "grid", &plan(None))
            .await
            .expect_err("the marker failed");
        assert_eq!(seen.lock().expect("log").len(), 1);
    }
}
