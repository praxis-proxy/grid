//! Routes the Services peers reach the operator on to the lease holder alone.
//!
//! Every replica is ready, so those Services carry no selector. The holder writes one
//! `EndpointSlice` per Service naming its own address, and a successor overwrites it,
//! so two replicas are never endpoints at once.

use std::{collections::BTreeMap, net::IpAddr, time::Duration};

use k8s_openapi::{
    api::{
        core::v1::{ObjectReference, Service},
        discovery::v1::{Endpoint, EndpointConditions, EndpointPort, EndpointSlice},
    },
    apimachinery::pkg::{apis::meta::v1::OwnerReference, util::intstr::IntOrString},
};
use kube::{
    Api, Client, Resource as _,
    api::{DeleteParams, ListParams, ObjectMeta, PostParams, ResourceExt as _},
};

/// How long one Service's slice write may take.
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

/// How often the holder checks its slices still name it.
const RECHECK_EVERY: Duration = Duration::from_secs(10);

/// The `endpointslice.kubernetes.io/managed-by` value on the slices the built-in controller writes.
const CONTROLLER: &str = "endpointslice-controller.k8s.io";

/// The `endpointslice.kubernetes.io/managed-by` value on the slices this writes.
pub const MANAGED_BY: &str = "grid-operator";

/// This replica, as an endpoint.
#[derive(Clone, Debug)]
pub struct Me {
    /// The pod's address.
    pub address: IpAddr,
    /// The pod's name.
    pub pod: String,
    /// The node it runs on, which a `Local` traffic policy routes by.
    pub node: Option<String>,
}

/// Writes the holder's slices for `services`.
pub struct HolderRoutes {
    /// Services in the operator's namespace.
    services: Api<Service>,
    /// Slices in the operator's namespace.
    slices: Api<EndpointSlice>,
    /// The operator's namespace.
    namespace: String,
    /// The Services routed to the holder alone.
    names: Vec<String>,
    /// This replica.
    me: Me,
    /// Container port numbers by port name, for a Service whose target port is named.
    ports: BTreeMap<String, i32>,
    /// Where routing stands, held across each write so a withdrawal cannot interleave with one.
    state: tokio::sync::Mutex<Routing>,
}

/// Where routing to this replica stands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Routing {
    /// Not yet routed to.
    Waiting,
    /// The slices name this replica.
    Routed,
    /// Stood down: never routed to again.
    Withdrawn,
}

impl std::fmt::Debug for HolderRoutes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HolderRoutes")
            .field("names", &self.names)
            .field("me", &self.me)
            .finish_non_exhaustive()
    }
}

impl HolderRoutes {
    /// Routes for `names` in the client's namespace, naming `me`, with `ports` resolving
    /// a named target port.
    #[must_use]
    pub fn new(client: &Client, names: Vec<String>, me: Me, ports: BTreeMap<String, i32>) -> Self {
        Self {
            services: Api::default_namespaced(client.clone()),
            slices: Api::default_namespaced(client.clone()),
            namespace: client.default_namespace().to_owned(),
            names,
            me,
            ports,
            state: tokio::sync::Mutex::new(Routing::Waiting),
        }
    }

    /// Once `serving` resolves, route every Service to this replica, then keep it so until
    /// [`HolderRoutes::withdraw`]. Nothing is written once `holding` lapses, so a holder that
    /// stalled past its lease never writes over a successor's slice.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the state is held across each write, so a withdrawal cannot interleave with one"
    )]
    pub async fn route_when<Serving: Future<Output = &'static str>>(
        &self,
        serving: Serving,
        holding: crate::leader::Holding,
    ) {
        let started = tokio::time::Instant::now();
        let why = serving.await;
        {
            let mut state = self.state.lock().await;
            if *state != Routing::Waiting || self.names.is_empty() || !holding.holds() {
                return;
            }
            *state = Routing::Routed;
            Box::pin(self.write_all(Some(&holding))).await;
        }
        tracing::info!(
            after_ms = started.elapsed().as_millis(),
            why,
            "routing peer traffic to this replica"
        );
        crate::metrics::set_routed(true);
        loop {
            tokio::time::sleep(crate::enroll::jittered(RECHECK_EVERY)).await;
            let state = self.state.lock().await;
            if *state != Routing::Routed {
                return;
            }
            if !holding.holds() {
                continue;
            }
            Box::pin(self.write_all(Some(&holding))).await;
        }
    }

    /// Stop routing to this replica, if it was routed to. Only a slice still naming it is
    /// cleared, so a successor's is never touched.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the state is held across each write, so a withdrawal cannot interleave with one"
    )]
    pub async fn withdraw(&self) {
        let mut state = self.state.lock().await;
        if std::mem::replace(&mut *state, Routing::Withdrawn) != Routing::Routed {
            return;
        }
        Box::pin(self.write_all(None)).await;
        crate::metrics::set_routed(false);
        tracing::info!("stopped routing peer traffic to this replica");
    }

    /// Write every Service's slice: naming this replica while `holding` holds, checked before
    /// each write so none lands once the lease has lapsed; with `None`, clearing what names it.
    async fn write_all(&self, holding: Option<&crate::leader::Holding>) {
        for name in &self.names {
            if holding.is_some_and(|holding| !holding.holds()) {
                tracing::warn!(service = %name, "lease lapsed mid-write; not routing to this replica");
                return;
            }
            Box::pin(self.write_logged(name, holding.is_some())).await;
        }
    }

    /// Write `service`'s slice as [`HolderRoutes::write`] does, bounded, logging a failure.
    async fn write_logged(&self, service: &str, routed: bool) {
        match tokio::time::timeout(WRITE_TIMEOUT, Box::pin(self.write(service, routed))).await {
            Ok(Ok(())) => {},
            Ok(Err(error)) => tracing::warn!(service, %error, routed, "holder EndpointSlice not written"),
            Err(_elapsed) => tracing::warn!(service, routed, "holder EndpointSlice write timed out"),
        }
    }

    /// Write `service`'s slice, naming this replica when `routed`, or clear it when it still does.
    #[expect(
        clippy::large_stack_frames,
        reason = "holds whole EndpointSlice values; the caller boxes it"
    )]
    async fn write(&self, service: &str, routed: bool) -> Result<(), kube::Error> {
        let name = slice_name(service);
        let live = Box::pin(self.slices.get_opt(&name)).await?;
        if !routed {
            let Some(mut live) = live.filter(|live| names(live, &self.me.address)) else {
                return Ok(());
            };
            live.endpoints = Vec::new();
            Box::pin(self.slices.replace(&name, &PostParams::default(), &live)).await?;
            return Ok(());
        }
        let Some(owner) = Box::pin(self.services.get_opt(service)).await? else {
            return Ok(());
        };
        Box::pin(self.clear_controller_slices(service)).await?;
        let mut desired = self.slice(&owner);
        match live {
            Some(live) if same(&live, &desired) => Ok(()),
            Some(live) => {
                desired.metadata.resource_version = live.resource_version();
                Box::pin(self.slices.replace(&name, &PostParams::default(), &desired))
                    .await
                    .map(drop)
            },
            None => Box::pin(self.slices.create(&PostParams::default(), &desired))
                .await
                .map(drop),
        }
    }

    /// `service`'s ports as this replica serves them, a named target port resolved.
    fn ports(&self, service: &Service) -> Vec<EndpointPort> {
        service
            .spec
            .as_ref()
            .and_then(|spec| spec.ports.as_ref())
            .into_iter()
            .flatten()
            .filter_map(|port| {
                let number = match &port.target_port {
                    Some(IntOrString::Int(number)) => Some(*number),
                    Some(IntOrString::String(named)) => self.ports.get(named).copied(),
                    None => Some(port.port),
                }?;
                Some(EndpointPort {
                    name: port.name.clone(),
                    port: Some(number),
                    protocol: port.protocol.clone(),
                    app_protocol: port.app_protocol.clone(),
                })
            })
            .collect()
    }

    /// Delete the slices the `EndpointSlice` controller left on `service`. It stops managing a
    /// Service once its selector goes, but never deletes what it wrote, which would route to
    /// the pods that ran before the upgrade beside this replica's slice.
    async fn clear_controller_slices(&self, service: &str) -> Result<(), kube::Error> {
        let left = ListParams::default().labels(&format!(
            "kubernetes.io/service-name={service},endpointslice.kubernetes.io/managed-by={CONTROLLER}"
        ));
        for slice in Box::pin(self.slices.list(&left)).await?.items {
            let Some(name) = slice.metadata.name else {
                continue;
            };
            match Box::pin(self.slices.delete(&name, &DeleteParams::default())).await {
                Ok(_) => tracing::info!(service, slice = %name, "deleted an EndpointSlice the controller left"),
                Err(kube::Error::Api(status)) if status.code == 404 => {},
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// The slice routing `service` to this replica.
    fn slice(&self, service: &Service) -> EndpointSlice {
        let service_name = service.name_any();
        let ports = self.ports(service);
        EndpointSlice {
            metadata: ObjectMeta {
                name: Some(slice_name(&service_name)),
                namespace: Some(self.namespace.clone()),
                labels: Some(BTreeMap::from([
                    ("kubernetes.io/service-name".to_owned(), service_name.clone()),
                    (
                        "endpointslice.kubernetes.io/managed-by".to_owned(),
                        MANAGED_BY.to_owned(),
                    ),
                ])),
                owner_references: service.controller_owner_ref(&()).map(|owner| {
                    vec![OwnerReference {
                        controller: None,
                        block_owner_deletion: None,
                        ..owner
                    }]
                }),
                ..ObjectMeta::default()
            },
            address_type: if self.me.address.is_ipv4() { "IPv4" } else { "IPv6" }.to_owned(),
            endpoints: vec![self.endpoint()],
            ports: Some(ports),
        }
    }

    /// This replica as a ready endpoint.
    fn endpoint(&self) -> Endpoint {
        Endpoint {
            addresses: vec![self.me.address.to_string()],
            conditions: Some(EndpointConditions {
                ready: Some(true),
                serving: Some(true),
                terminating: Some(false),
            }),
            node_name: self.me.node.clone(),
            target_ref: Some(ObjectReference {
                kind: Some("Pod".to_owned()),
                name: Some(self.me.pod.clone()),
                namespace: Some(self.namespace.clone()),
                ..ObjectReference::default()
            }),
            ..Endpoint::default()
        }
    }
}

/// The holder's slice for `service`.
#[must_use]
pub fn slice_name(service: &str) -> String {
    format!("{service}-holder")
}

/// Whether `slice` names `address`.
fn names(slice: &EndpointSlice, address: &IpAddr) -> bool {
    let address = address.to_string();
    slice
        .endpoints
        .iter()
        .any(|endpoint| endpoint.addresses.contains(&address))
}

/// Whether `live` already routes as `desired` does.
fn same(live: &EndpointSlice, desired: &EndpointSlice) -> bool {
    live.endpoints == desired.endpoints
        && live.ports == desired.ports
        && live.address_type == desired.address_type
        && live.metadata.labels == desired.metadata.labels
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use std::{
        collections::BTreeMap,
        net::IpAddr,
        sync::{Arc, Mutex, PoisonError},
        time::Duration,
    };

    use k8s_openapi::api::discovery::v1::EndpointSlice;

    use super::{HolderRoutes, Me, slice_name};

    /// An apiserver holding objects by path, refusing a stale `PUT` with 409, listing a
    /// collection by label and deleting by name. `slow` delays the next write to a path.
    #[derive(Clone, Default)]
    struct Objects(
        Arc<Mutex<BTreeMap<String, serde_json::Value>>>,
        Arc<Mutex<Option<(String, Duration)>>>,
    );

    /// Whether `object` carries every `key=value` in `selector`.
    fn labelled(object: &serde_json::Value, selector: &str) -> bool {
        selector.split(',').filter(|pair| !pair.is_empty()).all(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            object
                .pointer("/metadata/labels")
                .and_then(|labels| labels.get(key))
                .and_then(serde_json::Value::as_str)
                == Some(value)
        })
    }

    impl Objects {
        fn client(&self) -> kube::Client {
            use http_body_util::BodyExt as _;
            let objects = self.clone();
            let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
                let objects = objects.clone();
                async move {
                    let respond = |code: u16, body: serde_json::Value| {
                        http::Response::builder()
                            .status(code)
                            .body(kube::client::Body::from(body.to_string().into_bytes()))
                            .expect("response")
                    };
                    let status = |code: u16, reason: &str| serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Failure","reason":reason,"code":code});
                    let method = request.method().clone();
                    let path = request.uri().path().to_owned();
                    let query = request.uri().query().unwrap_or_default().to_owned();
                    let bytes = request.into_body().collect().await.expect("body").to_bytes();
                    let slow = objects
                        .1
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take_if(|(on, _)| method != http::Method::GET && path.contains(on.as_str()));
                    if let Some((_, delay)) = slow {
                        tokio::time::sleep(delay).await;
                    }
                    let mut held = objects.0.lock().unwrap_or_else(PoisonError::into_inner);
                    let selector = query
                        .split('&')
                        .find_map(|pair| pair.strip_prefix("labelSelector="))
                        .map(|selector| selector.replace("%3D", "=").replace("%2C", ",").replace("%2F", "/"));
                    let reply = if method == http::Method::GET && path.ends_with("/endpointslices") {
                        let prefix = format!("{path}/");
                        let items: Vec<serde_json::Value> = held
                            .iter()
                            .filter(|(key, object)| {
                                key.starts_with(&prefix) && labelled(object, selector.as_deref().unwrap_or_default())
                            })
                            .map(|(_, object)| object.clone())
                            .collect();
                        respond(
                            200,
                            serde_json::json!({"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSliceList", "metadata": {}, "items": items}),
                        )
                    } else if method == http::Method::GET {
                        held.get(&path)
                            .cloned()
                            .map_or_else(|| respond(404, status(404, "NotFound")), |object| respond(200, object))
                    } else if method == http::Method::DELETE {
                        held.remove(&path).map_or_else(
                            || respond(404, status(404, "NotFound")),
                            |_| {
                                respond(
                                    200,
                                    serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Success"}),
                                )
                            },
                        )
                    } else {
                        let mut object: serde_json::Value = serde_json::from_slice(&bytes).expect("object");
                        let name = object
                            .pointer("/metadata/name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                            .to_owned();
                        let key = if method == http::Method::POST {
                            format!("{path}/{name}")
                        } else {
                            path
                        };
                        let live = held
                            .get(&key)
                            .and_then(|live| live.pointer("/metadata/resourceVersion").cloned());
                        let fresh = match &live {
                            None => method == http::Method::POST,
                            Some(live) => object.pointer("/metadata/resourceVersion") == Some(live),
                        };
                        if fresh {
                            let next = live
                                .and_then(|live| live.as_str()?.parse::<u64>().ok())
                                .map_or(1, |version| version.saturating_add(1));
                            if let Some(metadata) =
                                object.get_mut("metadata").and_then(serde_json::Value::as_object_mut)
                            {
                                metadata.insert("resourceVersion".to_owned(), serde_json::json!(next.to_string()));
                            }
                            held.insert(key, object.clone());
                            respond(200, object)
                        } else {
                            respond(409, status(409, "Conflict"))
                        }
                    };
                    drop(held);
                    Ok::<_, std::convert::Infallible>(reply)
                }
            });
            kube::Client::new(service, "grid")
        }

        fn insert(&self, path: &str, object: serde_json::Value) {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(path.to_owned(), object);
        }

        fn slow_next_write(&self, on: &str, delay: Duration) {
            *self.1.lock().unwrap_or_else(PoisonError::into_inner) = Some((on.to_owned(), delay));
        }

        fn slice(&self, service: &str) -> Option<EndpointSlice> {
            let path = format!(
                "/apis/discovery.k8s.io/v1/namespaces/grid/endpointslices/{}",
                slice_name(service)
            );
            let held = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            held.get(&path)
                .map(|object| serde_json::from_value(object.clone()).expect("slice"))
        }
    }

    /// A store holding the signals Service, its target port named.
    fn with_service() -> Objects {
        let objects = Objects::default();
        objects.insert(
            "/api/v1/namespaces/grid/services/grid-signals",
            serde_json::json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "grid-signals", "namespace": "grid", "uid": "svc-uid", "resourceVersion": "1"},
                "spec": {"ports": [{"name": "signals", "port": 9091, "targetPort": "signals", "protocol": "TCP"}]}
            }),
        );
        objects
    }

    fn routes(objects: &Objects, address: &str, pod: &str) -> HolderRoutes {
        let me = Me {
            address: address.parse::<IpAddr>().expect("address"),
            pod: pod.to_owned(),
            node: Some(format!("{pod}-node")),
        };
        HolderRoutes::new(
            &objects.client(),
            vec!["grid-signals".to_owned()],
            me,
            BTreeMap::from([("signals".to_owned(), 19091)]),
        )
    }

    fn addresses(objects: &Objects) -> Vec<String> {
        objects
            .slice("grid-signals")
            .map(|slice| {
                slice
                    .endpoints
                    .into_iter()
                    .flat_map(|endpoint| endpoint.addresses)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Run `route_when` with a ready gate until it has routed, then stop polling it.
    async fn route(routes: &HolderRoutes) {
        let routing = routes.route_when(std::future::ready("test"), crate::leader::Holding::unelected());
        drop(tokio::time::timeout(Duration::from_millis(50), routing).await);
    }

    #[tokio::test(start_paused = true)]
    async fn the_holder_names_itself_on_its_services_slice() {
        let objects = with_service();
        route(&routes(&objects, "10.0.0.1", "a")).await;
        let slice = objects.slice("grid-signals").expect("written");
        let labels = slice.metadata.labels.clone().unwrap_or_default();
        assert_eq!(
            labels.get("kubernetes.io/service-name").map(String::as_str),
            Some("grid-signals")
        );
        assert_eq!(
            labels.get("endpointslice.kubernetes.io/managed-by").map(String::as_str),
            Some(super::MANAGED_BY)
        );
        assert_eq!(slice.address_type, "IPv4");
        let endpoint = slice.endpoints.first().expect("one endpoint");
        assert_eq!(endpoint.addresses, vec!["10.0.0.1".to_owned()]);
        assert_eq!(
            endpoint.node_name.as_deref(),
            Some("a-node"),
            "a Local policy routes by node"
        );
        assert_eq!(endpoint.conditions.as_ref().and_then(|c| c.ready), Some(true));
        let port = slice.ports.as_ref().and_then(|ports| ports.first()).expect("one port");
        assert_eq!(port.name.as_deref(), Some("signals"));
        assert_eq!(
            port.port,
            Some(19091),
            "the named target port resolves to the bound listener"
        );
        let owner = slice
            .metadata
            .owner_references
            .as_ref()
            .and_then(|owners| owners.first())
            .expect("owner");
        assert_eq!((owner.kind.as_str(), owner.uid.as_str()), ("Service", "svc-uid"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_successor_replaces_its_predecessor_so_one_replica_is_routed() {
        let objects = with_service();
        let old = routes(&objects, "10.0.0.1", "a");
        route(&old).await;
        route(&routes(&objects, "10.0.0.2", "b")).await;
        assert_eq!(addresses(&objects), vec!["10.0.0.2".to_owned()], "never two endpoints");
        old.withdraw().await;
        assert_eq!(
            addresses(&objects),
            vec!["10.0.0.2".to_owned()],
            "a predecessor's withdrawal leaves its successor's slice alone"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn withdrawing_clears_the_slice_and_stops_the_recheck() {
        let objects = with_service();
        let routes = routes(&objects, "10.0.0.1", "a");
        let routing = routes.route_when(std::future::ready("test"), crate::leader::Holding::unelected());
        let withdrawn = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            routes.withdraw().await;
            assert!(addresses(&objects).is_empty(), "cleared");
            // Past several rechecks: none writes it back.
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(3)).await;
        };
        let finished = tokio::time::timeout(super::RECHECK_EVERY.saturating_mul(4), async {
            tokio::join!(routing, withdrawn);
        })
        .await;
        assert!(finished.is_ok(), "the recheck loop ends once withdrawn");
        assert!(addresses(&objects).is_empty(), "no recheck routed it back");
    }

    #[tokio::test(start_paused = true)]
    async fn a_holder_that_stands_down_before_it_serves_is_never_routed() {
        let objects = with_service();
        let routes = routes(&objects, "10.0.0.1", "a");
        routes.withdraw().await;
        route(&routes).await;
        assert!(objects.slice("grid-signals").is_none(), "nothing written");
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_is_routed_until_the_holder_serves() {
        let objects = with_service();
        let routes = routes(&objects, "10.0.0.1", "a");
        let (serving, gate) = tokio::sync::oneshot::channel::<()>();
        let routing = routes.route_when(
            async {
                drop(gate.await);
                "published"
            },
            crate::leader::Holding::unelected(),
        );
        let checked = async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            assert!(objects.slice("grid-signals").is_none(), "not before the first publish");
            let _sent = serving.send(());
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(
                addresses(&objects),
                vec!["10.0.0.1".to_owned()],
                "routed once it serves"
            );
        };
        drop(tokio::time::timeout(Duration::from_secs(61), async { tokio::join!(routing, checked) }).await);
        assert_eq!(addresses(&objects), vec!["10.0.0.1".to_owned()]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_holder_that_stalled_past_its_lease_never_writes_over_its_successor() {
        let objects = with_service();
        let stale = routes(&objects, "10.0.0.1", "a");
        let holding = crate::leader::Holding::renewed_now();
        let routing = stale.route_when(std::future::ready("test"), holding.clone());
        let succeeded = async {
            tokio::time::sleep(crate::leader::RENEW_DEADLINE).await;
            assert!(!holding.holds(), "lapsed with no renewal");
            route(&routes(&objects, "10.0.0.2", "b")).await;
            // Past several of the stale holder's rechecks.
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(3)).await;
        };
        drop(
            tokio::time::timeout(super::RECHECK_EVERY.saturating_mul(5), async {
                tokio::join!(routing, succeeded)
            })
            .await,
        );
        assert_eq!(
            addresses(&objects),
            vec!["10.0.0.2".to_owned()],
            "the successor's slice stands"
        );
    }

    /// A slice the built-in controller wrote for `service` before its selector went.
    fn controller_slice(objects: &Objects, name: &str, service: &str, managed_by: &str) {
        objects.insert(
            &format!("/apis/discovery.k8s.io/v1/namespaces/grid/endpointslices/{name}"),
            serde_json::json!({
                "apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
                "metadata": {"name": name, "namespace": "grid", "resourceVersion": "1", "labels": {
                    "kubernetes.io/service-name": service,
                    "endpointslice.kubernetes.io/managed-by": managed_by,
                }},
                "addressType": "IPv4",
                "endpoints": [{"addresses": ["10.0.0.9"], "conditions": {"ready": true}}],
            }),
        );
    }

    fn stored(objects: &Objects, name: &str) -> bool {
        let held = objects.0.lock().unwrap_or_else(PoisonError::into_inner);
        held.contains_key(&format!(
            "/apis/discovery.k8s.io/v1/namespaces/grid/endpointslices/{name}"
        ))
    }

    #[tokio::test(start_paused = true)]
    async fn the_holder_deletes_the_slices_the_controller_left_on_its_services() {
        let objects = with_service();
        controller_slice(&objects, "grid-signals-abcde", "grid-signals", super::CONTROLLER);
        controller_slice(&objects, "other-abcde", "other", super::CONTROLLER);
        controller_slice(&objects, "grid-signals-mirror", "grid-signals", "someone-else");
        route(&routes(&objects, "10.0.0.1", "a")).await;
        assert!(
            !stored(&objects, "grid-signals-abcde"),
            "the controller's leftover would route to the old pod"
        );
        assert!(stored(&objects, "other-abcde"), "another Service's slice is left alone");
        assert!(
            stored(&objects, "grid-signals-mirror"),
            "a slice another manager owns is left alone"
        );
        assert_eq!(addresses(&objects), vec!["10.0.0.1".to_owned()]);
    }

    #[tokio::test(start_paused = true)]
    async fn no_service_is_routed_once_the_lease_lapses_mid_pass() {
        let objects = with_service();
        objects.insert(
            "/api/v1/namespaces/grid/services/grid-swim",
            serde_json::json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "grid-swim", "namespace": "grid", "uid": "swim-uid", "resourceVersion": "1"},
                "spec": {"ports": [{"name": "swim-udp", "port": 7946, "targetPort": 7946, "protocol": "UDP"}]}
            }),
        );
        let me = Me {
            address: "10.0.0.1".parse().expect("address"),
            pod: "a".to_owned(),
            node: None,
        };
        let routes = HolderRoutes::new(
            &objects.client(),
            vec!["grid-signals".to_owned(), "grid-swim".to_owned()],
            me,
            BTreeMap::from([("signals".to_owned(), 19091)]),
        );
        let holding = crate::leader::Holding::renewed_now();
        // The pass starts with a little lease left, and its first write outlasts it.
        tokio::time::advance(crate::leader::RENEW_DEADLINE.saturating_sub(Duration::from_millis(500))).await;
        objects.slow_next_write("/endpointslices", Duration::from_secs(1));
        let routing = routes.route_when(std::future::ready("test"), holding);
        drop(tokio::time::timeout(Duration::from_secs(5), routing).await);
        assert!(
            objects.slice("grid-signals").is_some(),
            "the write under way when the lease held lands"
        );
        assert!(
            objects.slice("grid-swim").is_none(),
            "the next one does not, once the lease has lapsed"
        );
    }
}
