//! Routes the Services peers reach the operator on to the lease holder alone.
//!
//! Every replica is ready, so those Services carry no selector. The holder writes one
//! `EndpointSlice` per Service naming its own address, and a successor overwrites it,
//! so two replicas are never endpoints at once.
//!
//! A Service that once had a selector keeps what the built-in controllers wrote for it: the
//! `EndpointSlice` controller's slices and the endpoints controller's v1 `Endpoints` object,
//! which the mirroring controller mirrors into a slice of its own once the selector is gone.
//! Both would route peers to pods that no longer serve, so the holder removes them. The v1 object
//! is read on every pass until a read finds it gone; those reads then stop until a pass sees the
//! Service with a selector again, under a new uid or without a confirmed slice, or a new holder starts.
//! An object recreated after that read waits until then.

use std::{
    collections::{BTreeMap, HashMap},
    net::IpAddr,
    time::Duration,
};

use k8s_openapi::{
    api::{
        core::v1::{Endpoints, ObjectReference, Service},
        discovery::v1::{Endpoint, EndpointConditions, EndpointPort, EndpointSlice},
    },
    apimachinery::pkg::{apis::meta::v1::OwnerReference, util::intstr::IntOrString},
};
use kube::{
    Api, Client, Resource as _,
    api::{DeleteParams, ListParams, ObjectMeta, PostParams, Preconditions, ResourceExt as _},
};

/// How long one Service's slice write may take.
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

/// How often the holder checks its slices still name it.
const RECHECK_EVERY: Duration = Duration::from_secs(10);

/// The `endpointslice.kubernetes.io/managed-by` value on the slices the built-in controller writes.
const CONTROLLER: &str = "endpointslice-controller.k8s.io";

/// The `endpointslice.kubernetes.io/managed-by` value on the slices this writes.
pub const MANAGED_BY: &str = "grid-operator";

/// The label whose value `"true"` keeps the mirroring controller off a v1 `Endpoints` object; the
/// endpoints controller sets it on the objects it manages.
const SKIP_MIRROR: &str = "endpointslice.kubernetes.io/skip-mirror";

/// How long a standing condition on a Service's legacy `Endpoints` object waits between warnings.
const WARN_EVERY: Duration = Duration::from_secs(300);

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
    /// v1 `Endpoints` objects in the operator's namespace.
    endpoints: Api<Endpoints>,
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
    /// Services whose legacy `Endpoints` object a pass found absent, by name, with the Service's
    /// uid as read then; locked only briefly. Dropped when a pass sees the Service with a selector,
    /// under another uid or without a confirmed slice.
    legacy_absent: std::sync::Mutex<HashMap<String, String>>,
    /// When each Service's legacy `Endpoints` object was last warned about, locked only briefly.
    warned: std::sync::Mutex<HashMap<String, tokio::time::Instant>>,
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

/// What a pass established about a Service's legacy `Endpoints` object.
#[derive(Debug, Eq, PartialEq)]
enum LegacyCleanup {
    /// Not found absent; a later pass reads again.
    Pending,
    /// A read found no object while the Service had no selector.
    Absent {
        /// The Service's uid as read in this pass.
        service_uid: String,
    },
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
            endpoints: Api::default_namespaced(client.clone()),
            namespace: client.default_namespace().to_owned(),
            names,
            me,
            ports,
            state: tokio::sync::Mutex::new(Routing::Waiting),
            legacy_absent: std::sync::Mutex::new(HashMap::new()),
            warned: std::sync::Mutex::new(HashMap::new()),
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
    /// A routed pass then removes, best effort and within one more [`WRITE_TIMEOUT`] in all, the
    /// legacy `Endpoints` object of each Service it published and has not yet found absent, so a
    /// withdrawal waiting on the routing state is delayed by at most that much.
    async fn write_all(&self, holding: Option<&crate::leader::Holding>) {
        let mut candidates = Vec::new();
        for name in &self.names {
            if holding.is_some_and(|holding| !holding.holds()) {
                tracing::warn!(service = %name, "lease lapsed mid-write; not routing to this replica");
                return;
            }
            match Box::pin(self.write_logged(name, holding.is_some())).await {
                Some(service) => {
                    if self.legacy_check_needed(&service) {
                        candidates.push(service);
                    }
                },
                None if holding.is_some() => self.forget_legacy_absence(name),
                None => {},
            }
        }
        let Some(holding) = holding else {
            return;
        };
        let cleanup = async {
            for service in &candidates {
                if !holding.holds() {
                    tracing::debug!("lease lapsed after publishing; legacy Endpoints cleanup waits for a later pass");
                    return;
                }
                if let LegacyCleanup::Absent { service_uid } =
                    Box::pin(self.clear_legacy_endpoints(service, holding)).await
                {
                    self.remember_legacy_absence(&service.name_any(), service_uid);
                }
            }
        };
        if tokio::time::timeout(WRITE_TIMEOUT, cleanup).await.is_err() {
            tracing::debug!("legacy Endpoints cleanup ran out of time; retried on the next recheck");
        }
    }

    /// Write `service`'s slice as [`HolderRoutes::write`] does, bounded, logging a failure: the
    /// Service whose slice now names this replica, once a routed write is confirmed.
    async fn write_logged(&self, service: &str, routed: bool) -> Option<Service> {
        match tokio::time::timeout(WRITE_TIMEOUT, Box::pin(self.write(service, routed))).await {
            Ok(Ok(published)) => published,
            Ok(Err(error)) => {
                tracing::warn!(service, %error, routed, "holder EndpointSlice not written");
                None
            },
            Err(_elapsed) => {
                tracing::warn!(service, routed, "holder EndpointSlice write timed out");
                None
            },
        }
    }

    /// Write `service`'s slice, naming this replica when `routed`, or clear it when it still does.
    /// A routed write returns the Service once its slice is confirmed; a clearing write or a
    /// Service that does not exist returns `None`.
    #[expect(
        clippy::large_stack_frames,
        reason = "holds whole EndpointSlice values; the caller boxes it"
    )]
    async fn write(&self, service: &str, routed: bool) -> Result<Option<Service>, kube::Error> {
        let name = slice_name(service);
        let live = Box::pin(self.slices.get_opt(&name)).await?;
        if !routed {
            let Some(mut live) = live.filter(|live| names(live, &self.me.address)) else {
                return Ok(None);
            };
            live.endpoints = Vec::new();
            Box::pin(self.slices.replace(&name, &PostParams::default(), &live)).await?;
            return Ok(None);
        }
        let Some(owner) = Box::pin(self.services.get_opt(service)).await? else {
            return Ok(None);
        };
        Box::pin(self.clear_controller_slices(service)).await?;
        let mut desired = self.slice(&owner);
        match live {
            Some(live) if same(&live, &desired) => {},
            Some(live) => {
                desired.metadata.resource_version = live.resource_version();
                Box::pin(self.slices.replace(&name, &PostParams::default(), &desired)).await?;
            },
            None => {
                Box::pin(self.slices.create(&PostParams::default(), &desired)).await?;
            },
        }
        Ok(Some(owner))
    }

    /// Delete the v1 `Endpoints` object left on `service` from before the holder routed it: the
    /// endpoints controller wrote one while the Service had a selector and leaves it when the
    /// selector goes, and the mirroring controller then mirrors it into a slice beside the
    /// holder's. Only the shape that history leaves is removed, see [`legacy_endpoints`];
    /// anything else stays and is reported once per [`WARN_EVERY`]. Absence is established by a
    /// later read, never by the delete's answer, since finalizers may hold the object: a read
    /// finding none, for a Service without a selector and with a uid, while the lease holds, is
    /// what [`LegacyCleanup::Absent`] reports.
    async fn clear_legacy_endpoints(&self, service: &Service, holding: &crate::leader::Holding) -> LegacyCleanup {
        let name = service.name_any();
        let live = match Box::pin(self.endpoints.get_opt(&name)).await {
            Ok(Some(live)) => live,
            Ok(None) => return absence(service, holding),
            Err(error) => {
                self.warn_bounded(&name, "legacy Endpoints object not read", &error.to_string());
                return LegacyCleanup::Pending;
            },
        };
        let identity = match legacy_endpoints(service, &live, &self.namespace) {
            Ok(identity) => identity,
            Err(why) => {
                self.warn_bounded(
                    &name,
                    "Endpoints object on a holder Service left alone",
                    &why.to_string(),
                );
                return LegacyCleanup::Pending;
            },
        };
        if !holding.holds() {
            return LegacyCleanup::Pending;
        }
        let params = DeleteParams {
            preconditions: Some(Preconditions {
                uid: Some(identity.uid),
                resource_version: Some(identity.resource_version),
            }),
            ..DeleteParams::default()
        };
        match Box::pin(self.endpoints.delete(&name, &params)).await {
            Ok(_) => tracing::info!(
                service = %name,
                "deleting the Endpoints object left from selector-based routing; a later read confirms it is gone"
            ),
            Err(kube::Error::Api(status)) if status.code == 404 => {},
            Err(kube::Error::Api(status)) if status.code == 409 => {
                tracing::debug!(service = %name, "the Endpoints object changed under its deletion; read again next recheck");
            },
            Err(error) => self.warn_bounded(&name, "legacy Endpoints object not deleted", &error.to_string()),
        }
        LegacyCleanup::Pending
    }

    /// Whether `service`'s legacy `Endpoints` object is still to be read: not once this holder found
    /// it absent for this Service, by uid, without a selector. Any other record of the Service is
    /// dropped, so a selector restored or a Service replaced under its name is read again.
    fn legacy_check_needed(&self, service: &Service) -> bool {
        let name = service.name_any();
        let mut absent = self
            .legacy_absent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let unchanged = !has_selector(service) && service.uid().is_some_and(|uid| absent.get(&name) == Some(&uid));
        if !unchanged {
            absent.remove(&name);
        }
        !unchanged
    }

    /// Forget that `service`'s legacy object was found absent: its slice was not confirmed this
    /// pass, so nothing about it is known.
    fn forget_legacy_absence(&self, service: &str) {
        self.legacy_absent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(service);
    }

    /// Record that `service`, at `uid`, had no legacy object.
    fn remember_legacy_absence(&self, service: &str, uid: String) {
        self.legacy_absent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(service.to_owned(), uid);
    }

    /// Warn `what` about `service` at most once per [`WARN_EVERY`], so a standing condition such
    /// as a missing grant does not repeat on every recheck.
    fn warn_bounded(&self, service: &str, what: &str, detail: &str) {
        let now = tokio::time::Instant::now();
        let mut warned = self.warned.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if warned
            .get(service)
            .is_some_and(|at| now.saturating_duration_since(*at) < WARN_EVERY)
        {
            return;
        }
        warned.insert(service.to_owned(), now);
        drop(warned);
        tracing::warn!(service, detail, "{what}");
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

/// What a read finding no legacy object on `service` establishes: [`LegacyCleanup::Absent`] for a
/// Service without a selector and with a uid, while `holding` still holds; otherwise nothing.
fn absence(service: &Service, holding: &crate::leader::Holding) -> LegacyCleanup {
    let uid = service.uid().filter(|uid| !uid.is_empty());
    match uid {
        Some(service_uid) if !has_selector(service) && holding.holds() => {
            tracing::debug!(service = %service.name_any(), "no legacy Endpoints object; not read again until the record is dropped");
            LegacyCleanup::Absent { service_uid }
        },
        _ => LegacyCleanup::Pending,
    }
}

/// Whether `service` carries a nonempty selector, so the endpoints controller manages its object.
fn has_selector(service: &Service) -> bool {
    service
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.as_ref())
        .is_some_and(|selector| !selector.is_empty())
}

/// Why a v1 `Endpoints` object on a holder Service is left alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum NotLegacy {
    /// Still written by the endpoints controller.
    #[error("the Service has a selector, so the endpoints controller still manages the object")]
    ServiceHasSelector,
    /// Carries an owner reference.
    #[error("the object has an owner")]
    Owned,
    /// Excluded from `EndpointSlice` mirroring by its label.
    #[error("the object is labelled skip-mirror, so the mirroring controller leaves it alone")]
    SkipMirror,
    /// Missing the uid or resourceVersion required by legacy cleanup.
    #[error("the object has no uid or resourceVersion to precondition a delete on")]
    NoIdentity,
    /// An address without a `targetRef`.
    #[error("an address names no object")]
    AddressWithoutTarget,
    /// An address that is not a named Pod here.
    #[error("an address names something other than a named Pod in the operator's namespace")]
    AddressNotLocalPod,
    /// Addresses that serve no port.
    #[error("a subset lists addresses but no ports")]
    SubsetWithoutPorts,
    /// A port the Service does not carry.
    #[error("a port is none of the Service's")]
    ForeignPort,
    /// Nothing stale is routed.
    #[error("the object lists no addresses, so nothing stale is routed")]
    NoAddresses,
}

/// What a delete of a legacy `Endpoints` object is preconditioned on.
#[derive(Clone, Debug, Eq, PartialEq)]
struct LegacyIdentity {
    /// The object's resourceVersion as read.
    resource_version: String,
    /// The object's uid.
    uid: String,
}

/// The identity of `live`, when it has the shape the endpoints controller leaves on `service`
/// once the selector goes: unowned; the skip-mirror label not at `"true"`, the one value that
/// keeps the mirroring controller away; every ready or not-ready address a named Pod in
/// `namespace`; every subset with addresses carrying ports whose names and protocols are the
/// Service's, their numbers uncompared, since the target port may have moved since the object was
/// written. Otherwise why it is left alone. A shape, not proof of who wrote the object.
fn legacy_endpoints(service: &Service, live: &Endpoints, namespace: &str) -> Result<LegacyIdentity, NotLegacy> {
    if has_selector(service) {
        return Err(NotLegacy::ServiceHasSelector);
    }
    let spec = service.spec.as_ref();
    let metadata = &live.metadata;
    if metadata
        .owner_references
        .as_ref()
        .is_some_and(|owners| !owners.is_empty())
    {
        return Err(NotLegacy::Owned);
    }
    if metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(SKIP_MIRROR))
        .is_some_and(|value| value == "true")
    {
        return Err(NotLegacy::SkipMirror);
    }
    let (Some(uid), Some(version)) = (
        metadata.uid.clone().filter(|uid| !uid.is_empty()),
        metadata.resource_version.clone().filter(|version| !version.is_empty()),
    ) else {
        return Err(NotLegacy::NoIdentity);
    };
    let service_ports: Vec<(Option<&str>, &str)> = spec
        .and_then(|spec| spec.ports.as_ref())
        .into_iter()
        .flatten()
        .map(|port| (port.name.as_deref(), port.protocol.as_deref().unwrap_or("TCP")))
        .collect();
    let mut any_address = false;
    for subset in live.subsets.iter().flatten() {
        let listed = subset
            .addresses
            .iter()
            .flatten()
            .chain(subset.not_ready_addresses.iter().flatten());
        let mut subset_addresses = false;
        for address in listed {
            any_address = true;
            subset_addresses = true;
            let Some(target) = address.target_ref.as_ref() else {
                return Err(NotLegacy::AddressWithoutTarget);
            };
            if target.kind.as_deref() != Some("Pod")
                || target.name.as_deref().is_none_or(str::is_empty)
                || target.namespace.as_deref().is_some_and(|other| other != namespace)
            {
                return Err(NotLegacy::AddressNotLocalPod);
            }
        }
        if subset_addresses && subset.ports.as_ref().is_none_or(Vec::is_empty) {
            return Err(NotLegacy::SubsetWithoutPorts);
        }
        for port in subset.ports.iter().flatten() {
            let key = (port.name.as_deref(), port.protocol.as_deref().unwrap_or("TCP"));
            if !service_ports.contains(&key) {
                return Err(NotLegacy::ForeignPort);
            }
        }
    }
    if !any_address {
        return Err(NotLegacy::NoAddresses);
    }
    Ok(LegacyIdentity {
        resource_version: version,
        uid,
    })
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

    use super::{HolderRoutes, LegacyIdentity, Me, NotLegacy, slice_name};

    /// An apiserver holding objects by path, refusing a stale `PUT` with 409, listing a
    /// collection by label and deleting by name, an `Endpoints` object only with matching
    /// preconditions and once its Service's holder slice is stored. The next write or the next
    /// read of a path can be delayed; a path may refuse deletes with 403; one path's next read may
    /// leave the stored version moved on, as a write in flight does. Every request is recorded as
    /// it arrives, before any delay.
    #[derive(Clone, Default)]
    struct Objects {
        forbidden: Arc<Mutex<Vec<String>>>,
        held: Arc<Mutex<BTreeMap<String, serde_json::Value>>>,
        moves_on: Arc<Mutex<Option<String>>>,
        requests: Arc<Mutex<Vec<(http::Method, String)>>>,
        slow_read: Arc<Mutex<Option<(String, Duration)>>>,
        slow_write: Arc<Mutex<Option<(String, Duration)>>>,
    }

    /// The apiserver's answer to reading `path`: the object, its stored version moved on afterwards
    /// when `moves_on`, or 404.
    fn get_reply(
        held: &mut BTreeMap<String, serde_json::Value>,
        path: &str,
        moves_on: bool,
    ) -> (u16, serde_json::Value) {
        match held.get_mut(path) {
            None => (
                404,
                serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}),
            ),
            Some(object) => {
                let reply = object.clone();
                if moves_on {
                    bump_version(object);
                }
                (200, reply)
            },
        }
    }

    /// The apiserver's answer to deleting `path`: 403 when forbidden; for an `Endpoints` object
    /// 422 without both preconditions, 409 when they miss the stored object, and 409 while its
    /// Service's holder slice is not stored, which proves publication came first; else the object
    /// goes, or 404.
    fn delete_reply(
        held: &mut BTreeMap<String, serde_json::Value>,
        path: &str,
        body: &[u8],
        forbidden: bool,
    ) -> (u16, serde_json::Value) {
        let status = |code: u16, reason: &str| serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Failure","reason":reason,"code":code});
        let options: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
        let wanted = |field: &str| options.pointer(&format!("/preconditions/{field}")).cloned();
        let stored = |field: &str| {
            held.get(path)
                .and_then(|object| object.pointer(&format!("/metadata/{field}")).cloned())
        };
        let endpoints = path.contains("/endpoints/");
        let unconditioned = wanted("uid").is_none() || wanted("resourceVersion").is_none();
        let mismatched = held.contains_key(path)
            && (wanted("uid") != stored("uid") || wanted("resourceVersion") != stored("resourceVersion"));
        let service = path.rsplit('/').next().unwrap_or_default();
        let unpublished = !held.contains_key(&format!(
            "/apis/discovery.k8s.io/v1/namespaces/grid/endpointslices/{}",
            slice_name(service)
        ));
        if forbidden {
            (403, status(403, "Forbidden"))
        } else if endpoints && unconditioned {
            (422, status(422, "Invalid"))
        } else if endpoints && mismatched {
            (409, status(409, "Conflict"))
        } else if endpoints && unpublished {
            (409, status(409, "SliceNotPublished"))
        } else if held.remove(path).is_some() {
            (
                200,
                serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Success"}),
            )
        } else {
            (404, status(404, "NotFound"))
        }
    }

    /// Move `object`'s resourceVersion on by one, as a write in flight does.
    fn bump_version(object: &mut serde_json::Value) {
        let next = object
            .pointer("/metadata/resourceVersion")
            .and_then(serde_json::Value::as_str)
            .and_then(|version| version.parse::<u64>().ok())
            .map_or(1, |version| version.saturating_add(1));
        if let Some(metadata) = object.get_mut("metadata").and_then(serde_json::Value::as_object_mut) {
            metadata.insert("resourceVersion".to_owned(), serde_json::json!(next.to_string()));
        }
    }

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
                    objects
                        .requests
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push((method.clone(), path.clone()));
                    let query = request.uri().query().unwrap_or_default().to_owned();
                    let bytes = request.into_body().collect().await.expect("body").to_bytes();
                    if let Some(delay) = objects.delay(&method, &path) {
                        tokio::time::sleep(delay).await;
                    }
                    let mut held = objects.held.lock().unwrap_or_else(PoisonError::into_inner);
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
                        let moves_on = objects
                            .moves_on
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .take_if(|on| *on == path)
                            .is_some();
                        let (code, body) = get_reply(&mut held, &path, moves_on);
                        respond(code, body)
                    } else if method == http::Method::DELETE {
                        let forbidden = objects
                            .forbidden
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .contains(&path);
                        let (code, body) = delete_reply(&mut held, &path, &bytes, forbidden);
                        respond(code, body)
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
            self.held
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(path.to_owned(), object);
        }

        /// The delay a knob holds for this request, taken once: the next write or the next read
        /// of a path.
        fn delay(&self, method: &http::Method, path: &str) -> Option<Duration> {
            let write = self
                .slow_write
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take_if(|(on, _)| *method != http::Method::GET && path.contains(on.as_str()))
                .map(|(_, delay)| delay);
            let read = self
                .slow_read
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take_if(|(on, _)| *method == http::Method::GET && path.contains(on.as_str()))
                .map(|(_, delay)| delay);
            write.or(read)
        }

        fn slow_next_write(&self, on: &str, delay: Duration) {
            *self.slow_write.lock().unwrap_or_else(PoisonError::into_inner) = Some((on.to_owned(), delay));
        }

        fn slow_next_read(&self, on: &str, delay: Duration) {
            *self.slow_read.lock().unwrap_or_else(PoisonError::into_inner) = Some((on.to_owned(), delay));
        }

        fn forbid_delete(&self, path: &str) {
            self.forbidden
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(path.to_owned());
        }

        fn move_on_after_read(&self, path: &str) {
            *self.moves_on.lock().unwrap_or_else(PoisonError::into_inner) = Some(path.to_owned());
        }

        /// How many requests with `method` named exactly `path`.
        fn count(&self, method: &http::Method, path: &str) -> usize {
            self.requests
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .filter(|(got, at)| got == method && at == path)
                .count()
        }

        /// How many reads named exactly `path`.
        fn reads(&self, path: &str) -> usize {
            self.count(&http::Method::GET, path)
        }

        /// How many deletes named exactly `path`.
        fn deletes(&self, path: &str) -> usize {
            self.count(&http::Method::DELETE, path)
        }

        /// Empty the stored holder slice of `service`, as a write by another hand would.
        fn empty_slice(&self, service: &str) {
            let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(slice) = held.get_mut(&slice_path(service)) {
                slice["endpoints"] = serde_json::json!([]);
            }
        }

        /// Remove the stored object at exactly `path`.
        fn remove(&self, path: &str) {
            self.held.lock().unwrap_or_else(PoisonError::into_inner).remove(path);
        }

        fn has(&self, path: &str) -> bool {
            self.held
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains_key(path)
        }

        fn slice(&self, service: &str) -> Option<EndpointSlice> {
            let held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
            held.get(&slice_path(service))
                .map(|object| serde_json::from_value(object.clone()).expect("slice"))
        }
    }

    /// A store holding the signals Service.
    fn with_service() -> Objects {
        let objects = Objects::default();
        objects.insert(SERVICE_PATH, signals_service("svc-uid", "1", None));
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
        let held = objects.held.lock().unwrap_or_else(PoisonError::into_inner);
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
        let routes = with_swim(&objects);
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

    #[tokio::test(start_paused = true)]
    async fn the_holder_removes_the_endpoints_object_left_from_selector_based_routing() {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        route(&routes(&objects, "10.0.0.1", "a")).await;
        assert_eq!(
            addresses(&objects),
            vec!["10.0.0.1".to_owned()],
            "the slice is published first"
        );
        assert!(
            !objects.has(LEGACY_PATH),
            "the leftover would be mirrored beside the holder's slice"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_owned_endpoints_object_on_a_holder_service_is_left_alone() {
        let objects = with_service();
        let owned = serde_json::json!({"ownerReferences": [{"apiVersion": "v1", "kind": "Service", "name": "grid-signals", "uid": "svc-uid"}]});
        objects.insert(LEGACY_PATH, legacy_object(&owned, &legacy_subsets("grid")));
        route(&routes(&objects, "10.0.0.1", "a")).await;
        assert!(objects.has(LEGACY_PATH), "an owned object is somebody's");
        assert_eq!(
            addresses(&objects),
            vec!["10.0.0.1".to_owned()],
            "leaving an owned Endpoints object alone does not stop holder routing"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_endpoints_object_naming_a_pod_elsewhere_is_left_alone() {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("elsewhere")),
        );
        route(&routes(&objects, "10.0.0.1", "a")).await;
        assert!(objects.has(LEGACY_PATH), "not the shape selector-based routing leaves");
    }

    #[tokio::test(start_paused = true)]
    async fn a_service_with_a_selector_keeps_its_endpoints_object() {
        let objects = with_service();
        objects.insert(
            "/api/v1/namespaces/grid/services/grid-signals",
            serde_json::json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "grid-signals", "namespace": "grid", "uid": "svc-uid", "resourceVersion": "1"},
                "spec": {"selector": {"app": "grid-operator"}, "ports": [{"name": "signals", "port": 9091, "targetPort": "signals", "protocol": "TCP"}]}
            }),
        );
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        route(&routes(&objects, "10.0.0.1", "a")).await;
        assert!(objects.has(LEGACY_PATH), "the endpoints controller still manages it");
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_endpoints_delete_does_not_stop_routing() {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        objects.forbid_delete(LEGACY_PATH);
        route(&routes(&objects, "10.0.0.1", "a")).await;
        assert_eq!(
            addresses(&objects),
            vec!["10.0.0.1".to_owned()],
            "routing stands without the grant"
        );
        assert!(objects.has(LEGACY_PATH), "left for a chart that grants the delete");
    }

    #[tokio::test(start_paused = true)]
    async fn legacy_cleanup_waits_for_a_published_slice() {
        let objects = Objects::default();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        route(&routes(&objects, "10.0.0.1", "a")).await;
        assert!(objects.slice("grid-signals").is_none(), "no Service, no slice");
        assert!(
            objects.has(LEGACY_PATH),
            "nothing is cleaned up for a Service that was not routed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_stale_endpoints_read_is_retried_on_the_next_recheck() {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        objects.move_on_after_read(LEGACY_PATH);
        let routes = routes(&objects, "10.0.0.1", "a");
        let routing = routes.route_when(std::future::ready("test"), crate::leader::Holding::unelected());
        let checked = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(objects.has(LEGACY_PATH), "the first delete met a moved-on version");
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
            assert!(
                !objects.has(LEGACY_PATH),
                "the next recheck read it again and deleted it"
            );
        };
        drop(
            tokio::time::timeout(super::RECHECK_EVERY.saturating_mul(3), async {
                tokio::join!(routing, checked)
            })
            .await,
        );
        assert!(
            !objects.has(LEGACY_PATH),
            "a stale delete is retried with a fresh version on a later recheck"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_legacy_read_stops_once_a_pass_finds_the_object_gone() {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        let holder = routes(&objects, "10.0.0.1", "a");
        observe(&holder, 10, async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!objects.has(LEGACY_PATH), "deleted on the first pass");
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
            assert_eq!(
                objects.reads(LEGACY_PATH),
                2,
                "one read found and deleted it, the next confirmed it gone"
            );
            let service_reads = objects.reads(SERVICE_PATH);
            objects.empty_slice("grid-signals");
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(6)).await;
            assert_eq!(
                objects.reads(LEGACY_PATH),
                2,
                "no pass reads it after the confirming one"
            );
            assert!(
                objects.reads(SERVICE_PATH) > service_reads,
                "the passes go on without it"
            );
            assert_eq!(
                addresses(&objects),
                vec!["10.0.0.1".to_owned()],
                "a later pass repaired the emptied slice"
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_service_without_a_legacy_object_is_read_once() {
        let objects = with_service();
        let holder = routes(&objects, "10.0.0.1", "a");
        observe(&holder, 8, async {
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(6)).await;
            assert_eq!(
                objects.reads(LEGACY_PATH),
                1,
                "the first read finds nothing and is the last"
            );
            assert_eq!(objects.deletes(LEGACY_PATH), 0, "nothing to delete");
            assert!(objects.reads(SERVICE_PATH) > 1, "the passes go on without it");
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn an_object_recreated_before_the_confirming_read_is_removed() {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        let holder = routes(&objects, "10.0.0.1", "a");
        observe(&holder, 10, async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!objects.has(LEGACY_PATH), "gone after the first pass");
            objects.insert(
                LEGACY_PATH,
                legacy_object(
                    &serde_json::json!({"uid": "ep-uid-2", "resourceVersion": "9"}),
                    &legacy_subsets("grid"),
                ),
            );
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
            assert!(
                !objects.has(LEGACY_PATH),
                "the confirming read met the replacement and deleted it"
            );
            assert_eq!(
                objects.deletes(LEGACY_PATH),
                2,
                "each object was deleted under its own identity"
            );
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(6)).await;
            assert_eq!(
                objects.reads(LEGACY_PATH),
                3,
                "the read after the second delete confirmed absence and was the last"
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn an_object_recreated_after_the_confirming_read_waits_for_new_routes() {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        let holder = routes(&objects, "10.0.0.1", "a");
        observe(
            &holder,
            12,
            Box::pin(async {
                tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
                assert_eq!(objects.reads(LEGACY_PATH), 2, "deleted, then confirmed gone");
                objects.insert(
                    LEGACY_PATH,
                    legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
                );
                tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(6)).await;
                assert!(objects.has(LEGACY_PATH), "this holder no longer reads the object");
                assert_eq!(objects.reads(LEGACY_PATH), 2, "the recreation goes unread");
                holder.withdraw().await;
                route(&routes(&objects, "10.0.0.2", "b")).await;
                assert!(!objects.has(LEGACY_PATH), "a new holder reads once more and removes it");
            }),
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn an_object_left_alone_is_read_again_each_pass() {
        let objects = with_service();
        let owned = serde_json::json!({"ownerReferences": [{"apiVersion": "v1", "kind": "Service", "name": "grid-signals", "uid": "svc-uid"}]});
        objects.insert(LEGACY_PATH, legacy_object(&owned, &legacy_subsets("grid")));
        let holder = routes(&objects, "10.0.0.1", "a");
        observe(&holder, 4, async {
            tokio::time::sleep(Duration::from_secs(35)).await;
            assert!(objects.has(LEGACY_PATH), "still somebody's");
            assert_eq!(
                (objects.reads(SERVICE_PATH), objects.reads(LEGACY_PATH)),
                (4, 4),
                "a present object is read on every one of the four passes"
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_delete_is_retried_on_the_next_pass() {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        objects.forbid_delete(LEGACY_PATH);
        let holder = routes(&objects, "10.0.0.1", "a");
        observe(&holder, 4, async {
            tokio::time::sleep(Duration::from_secs(35)).await;
            assert!(objects.has(LEGACY_PATH), "left for a chart that grants the delete");
            assert_eq!(
                (objects.reads(LEGACY_PATH), objects.deletes(LEGACY_PATH)),
                (4, 4),
                "a refused delete is tried again on every one of the four passes"
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn each_service_completes_its_cleanup_independently() {
        let objects = with_service();
        let holder = with_swim(&objects);
        let owned = serde_json::json!({"ownerReferences": [{"apiVersion": "v1", "kind": "Service", "name": "grid-swim", "uid": "swim-uid"}]});
        objects.insert(
            SWIM_PATH,
            legacy_object_of("grid-swim", &owned, &legacy_subsets("grid")),
        );
        observe(&holder, 4, async {
            tokio::time::sleep(Duration::from_secs(35)).await;
            assert_eq!(
                objects.reads(LEGACY_PATH),
                1,
                "the signals object was found absent once"
            );
            assert_eq!(
                (objects.reads(SWIM_SERVICE_PATH), objects.reads(SWIM_PATH)),
                (4, 4),
                "the SWIM object is present and read on every one of the four passes"
            );
            assert!(
                objects.slice("grid-signals").is_some() && objects.slice("grid-swim").is_some(),
                "both slices stand"
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_confirmed_absence_survives_the_other_services_timeout() {
        let objects = with_service();
        let holder = with_swim(&objects);
        objects.slow_next_read(SWIM_PATH, super::WRITE_TIMEOUT.saturating_mul(2));
        observe(&holder, 4, async {
            tokio::time::sleep(super::WRITE_TIMEOUT.saturating_add(Duration::from_millis(50))).await;
            assert_eq!(
                (objects.reads(LEGACY_PATH), objects.reads(SWIM_PATH)),
                (1, 1),
                "both were read; the SWIM read outlasted the cleanup budget"
            );
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
            assert_eq!(
                objects.reads(LEGACY_PATH),
                1,
                "the signals absence recorded before the timeout stands"
            );
            assert_eq!(objects.reads(SWIM_PATH), 2, "the SWIM read is retried");
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_selector_seen_restored_rearms_the_cleanup() {
        let objects = with_service();
        let holder = routes(&objects, "10.0.0.1", "a");
        observe(&holder, 10, async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(objects.reads(LEGACY_PATH), 1, "found absent");
            objects.insert(
                SERVICE_PATH,
                signals_service("svc-uid", "2", Some(serde_json::json!({"app": "grid"}))),
            );
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
            assert!(
                objects.reads(LEGACY_PATH) >= 2,
                "a Service with its selector back is read again"
            );
            objects.insert(SERVICE_PATH, signals_service("svc-uid", "3", None));
            objects.insert(
                LEGACY_PATH,
                legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
            );
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
            assert!(
                !objects.has(LEGACY_PATH),
                "the object the selector's return left is removed without a restart"
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_service_replaced_under_its_name_rearms_the_cleanup() {
        let objects = with_service();
        let holder = routes(&objects, "10.0.0.1", "a");
        observe(&holder, 6, async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(objects.reads(LEGACY_PATH), 1, "found absent");
            objects.insert(SERVICE_PATH, signals_service("svc-uid-2", "1", None));
            objects.insert(
                LEGACY_PATH,
                legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
            );
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
            assert!(
                !objects.has(LEGACY_PATH),
                "a new Service under the old name is read again and its object removed"
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_pass_without_a_confirmed_slice_rearms_the_cleanup() {
        let objects = with_service();
        let holder = routes(&objects, "10.0.0.1", "a");
        observe(&holder, 8, async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(objects.reads(LEGACY_PATH), 1, "found absent");
            objects.remove(SERVICE_PATH);
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
            objects.insert(SERVICE_PATH, signals_service("svc-uid", "2", None));
            objects.insert(
                LEGACY_PATH,
                legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
            );
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
            assert!(
                !objects.has(LEGACY_PATH),
                "a pass that could not confirm the slice forgot the absence, so the object is read and removed"
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_lease_lost_during_the_confirming_read_records_nothing() {
        let objects = with_service();
        let holder = routes(&objects, "10.0.0.1", "a");
        let holding = crate::leader::Holding::renewed_now();
        tokio::time::advance(crate::leader::RENEW_DEADLINE.saturating_sub(Duration::from_millis(500))).await;
        objects.slow_next_read(LEGACY_PATH, Duration::from_secs(1));
        let routing = holder.route_when(std::future::ready("test"), holding);
        drop(tokio::time::timeout(Duration::from_secs(5), routing).await);
        assert_eq!(objects.reads(LEGACY_PATH), 1, "the read ran while the lease held");
        assert!(
            holder
                .legacy_absent
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty(),
            "an absence read that outlasted the lease is not remembered"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn legacy_cleanup_does_not_run_once_the_lease_lapsed() {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        let routes = routes(&objects, "10.0.0.1", "a");
        let holding = crate::leader::Holding::renewed_now();
        tokio::time::advance(crate::leader::RENEW_DEADLINE.saturating_sub(Duration::from_millis(500))).await;
        objects.slow_next_write("/endpointslices", Duration::from_secs(1));
        let routing = routes.route_when(std::future::ready("test"), holding);
        drop(tokio::time::timeout(Duration::from_secs(5), routing).await);
        assert!(
            objects.slice("grid-signals").is_some(),
            "the slice write under way when the lease held lands"
        );
        assert!(objects.has(LEGACY_PATH), "a holder whose lease lapsed deletes nothing");
    }

    #[tokio::test(start_paused = true)]
    async fn the_lease_is_checked_again_after_the_endpoints_read() {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        let routes = routes(&objects, "10.0.0.1", "a");
        let holding = crate::leader::Holding::renewed_now();
        tokio::time::advance(crate::leader::RENEW_DEADLINE.saturating_sub(Duration::from_millis(500))).await;
        objects.slow_next_read(LEGACY_PATH, Duration::from_secs(1));
        let routing = routes.route_when(std::future::ready("test"), holding);
        drop(tokio::time::timeout(Duration::from_secs(5), routing).await);
        assert_eq!(
            addresses(&objects),
            vec!["10.0.0.1".to_owned()],
            "the slice was written while the lease held"
        );
        assert!(
            objects.has(LEGACY_PATH),
            "the lease lapsed during the read, so no delete follows it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_cleanup_does_not_hold_up_a_withdrawal() -> Result<(), tokio::time::error::Elapsed> {
        let objects = with_service();
        objects.insert(
            LEGACY_PATH,
            legacy_object(&serde_json::json!({}), &legacy_subsets("grid")),
        );
        objects.slow_next_read(LEGACY_PATH, Duration::from_secs(30));
        let routes = routes(&objects, "10.0.0.1", "a");
        let routing = routes.route_when(std::future::ready("test"), crate::leader::Holding::unelected());
        let withdrawn = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let asked = tokio::time::Instant::now();
            routes.withdraw().await;
            assert!(
                asked.elapsed() <= super::WRITE_TIMEOUT.saturating_add(Duration::from_millis(100)),
                "a withdrawal waits for the cleanup budget at most"
            );
            assert!(addresses(&objects).is_empty(), "cleared");
            tokio::time::sleep(super::RECHECK_EVERY.saturating_mul(2)).await;
            assert!(
                objects.has(LEGACY_PATH),
                "the cut-off cleanup never resumes, and none runs on the withdrawal path"
            );
        };
        tokio::time::timeout(super::RECHECK_EVERY.saturating_mul(3), async {
            tokio::join!(routing, withdrawn)
        })
        .await?;
        assert!(
            objects.has(LEGACY_PATH),
            "withdrawal prevents timed-out cleanup from resuming or running again"
        );
        Ok(())
    }

    #[test]
    fn the_shape_selector_based_routing_leaves_is_recognised() -> Result<(), serde_json::Error> {
        assert_eq!(
            judged(&serde_json::json!({}), &legacy_subsets("grid"))?,
            Ok(LegacyIdentity {
                resource_version: "7".to_owned(),
                uid: "ep-uid".to_owned(),
            }),
            "an unowned object naming a local Pod on the Service's port is the legacy shape"
        );
        Ok(())
    }

    #[test]
    fn an_endpoints_object_without_addresses_is_not_legacy() -> Result<(), serde_json::Error> {
        assert_eq!(
            judged(&serde_json::json!({}), &serde_json::json!([]))?,
            Err(NotLegacy::NoAddresses),
            "an object without addresses routes nothing stale, so it is left alone"
        );
        Ok(())
    }

    #[test]
    fn the_skip_mirror_label_at_true_marks_an_object_the_mirroring_controller_skips() -> Result<(), serde_json::Error> {
        let labelled = serde_json::json!({"labels": {"endpointslice.kubernetes.io/skip-mirror": "true"}});
        assert_eq!(
            judged(&labelled, &legacy_subsets("grid"))?,
            Err(NotLegacy::SkipMirror),
            "skip-mirror at true means the mirroring controller never mirrors it"
        );
        Ok(())
    }

    #[test]
    fn the_skip_mirror_label_at_another_value_marks_nothing() -> Result<(), serde_json::Error> {
        let labelled = serde_json::json!({"labels": {"endpointslice.kubernetes.io/skip-mirror": "false"}});
        assert_eq!(
            judged(&labelled, &legacy_subsets("grid"))?,
            Ok(LegacyIdentity {
                resource_version: "7".to_owned(),
                uid: "ep-uid".to_owned(),
            }),
            "only the value true keeps the mirroring controller away"
        );
        Ok(())
    }

    #[test]
    fn a_port_the_service_lacks_is_not_legacy() -> Result<(), serde_json::Error> {
        let subsets = serde_json::json!([{
            "addresses": [{"ip": "10.0.0.9", "targetRef": {"kind": "Pod", "name": "old", "namespace": "grid"}}],
            "ports": [{"name": "metrics", "port": 9100, "protocol": "TCP"}]
        }]);
        assert_eq!(
            judged(&serde_json::json!({}), &subsets)?,
            Err(NotLegacy::ForeignPort),
            "a port the Service does not carry is not this Service's traffic"
        );
        Ok(())
    }

    #[test]
    fn an_address_without_a_pod_name_is_not_legacy() -> Result<(), serde_json::Error> {
        let subsets = serde_json::json!([{
            "addresses": [{"ip": "10.0.0.9", "targetRef": {"kind": "Pod", "namespace": "grid"}}],
            "ports": [{"name": "signals", "port": 19091, "protocol": "TCP"}]
        }]);
        assert_eq!(
            judged(&serde_json::json!({}), &subsets)?,
            Err(NotLegacy::AddressNotLocalPod),
            "a Pod reference without a name is not a recognised target"
        );
        Ok(())
    }

    #[test]
    fn a_subset_with_addresses_but_no_ports_is_not_legacy() -> Result<(), serde_json::Error> {
        let subsets = serde_json::json!([{
            "addresses": [{"ip": "10.0.0.9", "targetRef": {"kind": "Pod", "name": "old", "namespace": "grid"}}]
        }]);
        assert_eq!(
            judged(&serde_json::json!({}), &subsets)?,
            Err(NotLegacy::SubsetWithoutPorts),
            "addresses without ports are not the shape selector-based routing leaves"
        );
        Ok(())
    }

    #[test]
    fn a_missing_version_blocks_the_delete_precondition() -> Result<(), serde_json::Error> {
        assert_eq!(
            judged(&serde_json::json!({"resourceVersion": ""}), &legacy_subsets("grid"))?,
            Err(NotLegacy::NoIdentity),
            "legacy cleanup requires a nonempty resourceVersion to reject stale reads"
        );
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// The signals Service at `uid` and `version`, its target port named, with `selector` when given.
    fn signals_service(uid: &str, version: &str, selector: Option<serde_json::Value>) -> serde_json::Value {
        let mut service = serde_json::json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "grid-signals", "namespace": "grid", "uid": uid, "resourceVersion": version},
            "spec": {"ports": [{"name": "signals", "port": 9091, "targetPort": "signals", "protocol": "TCP"}]}
        });
        if let (Some(spec), Some(selector)) = (
            service.get_mut("spec").and_then(serde_json::Value::as_object_mut),
            selector,
        ) {
            spec.insert("selector".to_owned(), selector);
        }
        service
    }

    /// Add the SWIM Service to `objects`: routes for it and the signals Service.
    fn with_swim(objects: &Objects) -> HolderRoutes {
        objects.insert(
            SWIM_SERVICE_PATH,
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
        HolderRoutes::new(
            &objects.client(),
            vec!["grid-signals".to_owned(), "grid-swim".to_owned()],
            me,
            BTreeMap::from([("signals".to_owned(), 19091)]),
        )
    }

    /// Run `routes` as the holder, unelected, while `checked` observes it, for `rechecks` at most;
    /// the observations must all finish within that.
    async fn observe<Checked: Future<Output = ()>>(routes: &HolderRoutes, rechecks: u32, checked: Checked) {
        let routing = routes.route_when(std::future::ready("test"), crate::leader::Holding::unelected());
        let mut observed = false;
        let observation = async {
            checked.await;
            observed = true;
        };
        drop(
            tokio::time::timeout(super::RECHECK_EVERY.saturating_mul(rechecks), async {
                tokio::join!(routing, observation)
            })
            .await,
        );
        assert!(observed, "every observation finished before the deadline");
    }

    /// The path of `service`'s holder slice.
    fn slice_path(service: &str) -> String {
        format!(
            "/apis/discovery.k8s.io/v1/namespaces/grid/endpointslices/{}",
            slice_name(service)
        )
    }

    /// Where the signals Service's v1 `Endpoints` object lives.
    const LEGACY_PATH: &str = "/api/v1/namespaces/grid/endpoints/grid-signals";
    const SWIM_PATH: &str = "/api/v1/namespaces/grid/endpoints/grid-swim";
    const SERVICE_PATH: &str = "/api/v1/namespaces/grid/services/grid-signals";
    const SWIM_SERVICE_PATH: &str = "/api/v1/namespaces/grid/services/grid-swim";

    /// A v1 `Endpoints` object named like the signals Service, `extra` merged into its metadata.
    fn legacy_object(extra: &serde_json::Value, subsets: &serde_json::Value) -> serde_json::Value {
        legacy_object_of("grid-signals", extra, subsets)
    }

    /// A v1 `Endpoints` object named like the Service `name`, `extra` merged into its metadata.
    fn legacy_object_of(name: &str, extra: &serde_json::Value, subsets: &serde_json::Value) -> serde_json::Value {
        let mut object = serde_json::json!({
            "apiVersion": "v1", "kind": "Endpoints",
            "metadata": {"name": name, "namespace": "grid", "uid": "ep-uid", "resourceVersion": "7"},
            "subsets": subsets
        });
        if let (Some(metadata), Some(extra)) = (
            object.get_mut("metadata").and_then(serde_json::Value::as_object_mut),
            extra.as_object(),
        ) {
            metadata.extend(extra.clone());
        }
        object
    }

    /// Subsets naming one old pod in `namespace` on the signals port.
    fn legacy_subsets(namespace: &str) -> serde_json::Value {
        serde_json::json!([{
            "addresses": [{"ip": "10.0.0.9", "targetRef": {"kind": "Pod", "name": "old", "namespace": namespace}}],
            "ports": [{"name": "signals", "port": 19091, "protocol": "TCP"}]
        }])
    }

    /// The signals Service as the chart renders it without a selector.
    fn service_without_selector() -> Result<k8s_openapi::api::core::v1::Service, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "grid-signals", "namespace": "grid"},
            "spec": {"ports": [{"name": "signals", "port": 9091, "targetPort": "signals", "protocol": "TCP"}]}
        }))
    }

    /// What [`super::legacy_endpoints`] makes of an object with `extra` metadata and `subsets` on
    /// the selector-less signals Service.
    fn judged(
        extra: &serde_json::Value,
        subsets: &serde_json::Value,
    ) -> Result<Result<LegacyIdentity, NotLegacy>, serde_json::Error> {
        let live: k8s_openapi::api::core::v1::Endpoints = serde_json::from_value(legacy_object(extra, subsets))?;
        Ok(super::legacy_endpoints(&service_without_selector()?, &live, "grid"))
    }
}
