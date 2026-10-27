use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use k8s_openapi::{ByteString, api::core::v1::ConfigMap};
use kube::api::ObjectMeta;
use serde_json::json;

use super::*;
use crate::{
    enroll::tests::{Reply, SITE, TOKEN_SECRET, serve},
    operator_config::{CONFIG_KEY, DEFAULT_CONFIG_MAP},
    rollup::{AVAILABLE, DEGRADED, PROGRESSING},
};

const NAMESPACE: &str = "grid-system";

/// The API objects the mock serves, and what was written.
#[derive(Clone, Default)]
struct World {
    sites: Vec<GridSite>,
    secrets: BTreeMap<String, Secret>,
    /// Status apply patches, by site name.
    patches: Vec<(String, Value)>,
    /// Secret names read by GET, data or metadata.
    reads: Vec<String>,
    /// Requests the mock does not model.
    unexpected: Vec<String>,
    /// Bumped on every Secret write, so resource versions change.
    version: u64,
    /// Secret GETs that answer 500 once each, as a transient API failure.
    flaky: Vec<String>,
    /// Query strings of status patches.
    patch_queries: Vec<String>,
    /// The operator config `ConfigMap`, when one exists.
    config: Option<ConfigMap>,
    /// Events recorded, as (type, reason).
    events: Vec<(String, String)>,
}

impl World {
    fn put_secret(&mut self, name: &str, data: &[(&str, &str)], labels: &[(&str, &str)]) {
        self.version = self.version.saturating_add(1);
        let secret = Secret {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(NAMESPACE.to_owned()),
                resource_version: Some(self.version.to_string()),
                labels: Some(labels.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()),
                ..ObjectMeta::default()
            },
            data: Some(
                data.iter()
                    .map(|(k, v)| ((*k).to_owned(), ByteString(v.as_bytes().to_vec())))
                    .collect(),
            ),
            ..Secret::default()
        };
        self.secrets.insert(name.to_owned(), secret);
    }

    /// The `Enrolled` conditions written to `site`, in order.
    fn verdicts(&self, site: &str) -> Vec<(String, String)> {
        self.verdicts_of(site, ENROLLED)
    }

    /// The conditions of `type_` written to `site`, in order.
    fn written(&self, site: &str, type_: &str) -> Vec<Value> {
        self.patches
            .iter()
            .filter(|(name, _)| name == site)
            .filter_map(|(_, patch)| patch.pointer("/status/conditions").and_then(Value::as_array).cloned())
            .flatten()
            .filter(|c| c.get("type").and_then(Value::as_str) == Some(type_))
            .collect()
    }

    /// The messages of the `type_` conditions written to `site`, in order.
    fn messages_of(&self, site: &str, type_: &str) -> Vec<String> {
        self.written(site, type_)
            .iter()
            .map(|c| c.get("message").and_then(Value::as_str).unwrap_or_default().to_owned())
            .collect()
    }

    /// The conditions of `type_` written to `site`, in order.
    fn verdicts_of(&self, site: &str, type_: &str) -> Vec<(String, String)> {
        self.written(site, type_)
            .iter()
            .map(|c| {
                let field = |key: &str| c.get(key).and_then(Value::as_str).unwrap_or_default().to_owned();
                (field("status"), field("reason"))
            })
            .collect()
    }

    /// GET `GridSite` `name`, as last patched.
    fn get_site(&self, name: &str) -> (u16, Value) {
        self.sites
            .iter()
            .find(|site| site.metadata.name.as_deref() == Some(name))
            .map_or_else(not_found, |site| (200, serde_json::to_value(site).unwrap_or_default()))
    }

    /// GET Secret `name`.
    fn get_secret(&mut self, name: &str) -> (u16, Value) {
        self.reads.push(name.to_owned());
        if let Some(at) = self.flaky.iter().position(|flaky| flaky == name) {
            self.flaky.remove(at);
            return (
                500,
                json!({"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 500}),
            );
        }
        self.secrets.get(name).map_or_else(not_found, |secret| {
            (200, serde_json::to_value(secret).unwrap_or_default())
        })
    }

    /// POST a Secret, stored unless a dry run.
    fn post_secret(&mut self, query: &str, body: &[u8]) -> (u16, Value) {
        let secret: Secret = serde_json::from_slice(body).unwrap_or_default();
        let name = secret.metadata.name.clone().unwrap_or_default();
        if self.secrets.contains_key(&name) {
            let conflict = json!({"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "AlreadyExists", "code": 409});
            return (409, conflict);
        }
        if !query.contains("dryRun") {
            self.version = self.version.saturating_add(1);
            self.secrets.insert(name, secret.clone());
        }
        (201, serde_json::to_value(&secret).unwrap_or_default())
    }

    /// Set the operator config document to `text`.
    fn configure(&mut self, text: &str) {
        self.config = Some(ConfigMap {
            metadata: ObjectMeta {
                name: Some(DEFAULT_CONFIG_MAP.to_owned()),
                namespace: Some(NAMESPACE.to_owned()),
                uid: Some("uid-config".to_owned()),
                ..ObjectMeta::default()
            },
            data: Some(BTreeMap::from([(CONFIG_KEY.to_owned(), text.to_owned())])),
            ..ConfigMap::default()
        });
    }

    /// POST an Event, held as (type, reason).
    fn post_event(&mut self, body: &[u8]) -> (u16, Value) {
        let event: Value = serde_json::from_slice(body).unwrap_or_default();
        let field = |key: &str| event.get(key).and_then(Value::as_str).unwrap_or_default().to_owned();
        self.events.push((field("type"), field("reason")));
        (201, event)
    }

    /// Apply a status patch to `GridSite` `name`, holding its `Enrolled` condition.
    fn patch_status(&mut self, name: &str, body: &[u8]) -> (u16, Value) {
        let applied: Value = serde_json::from_slice(body).unwrap_or_default();
        let conditions: Vec<Condition> = applied
            .pointer("/status/conditions")
            .cloned()
            .and_then(|c| serde_json::from_value(c).ok())
            .unwrap_or_default();
        self.patches.push((name.to_owned(), applied));
        let Some(site) = self.sites.iter_mut().find(|s| s.metadata.name.as_deref() == Some(name)) else {
            return not_found();
        };
        let status = site.status.get_or_insert_with(Default::default);
        // An apply carrying no conditions gives up what its manager held.
        if conditions.is_empty() {
            status.conditions.clear();
        }
        for written in conditions {
            status.conditions.retain(|held| held.type_ != written.type_);
            status.conditions.push(written);
        }
        (200, serde_json::to_value(&*site).unwrap_or_default())
    }
}

fn not_found() -> (u16, Value) {
    (
        404,
        json!({"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "NotFound", "code": 404}),
    )
}

fn list(kind: &str, items: &Value) -> (u16, Value) {
    (
        200,
        json!({"apiVersion": "v1", "kind": kind, "metadata": {"resourceVersion": "1"}, "items": items}),
    )
}

const SITES: &str = "/apis/grid.praxis.fast/v1alpha1/gridsites";

/// Answer one request against `world`.
fn answer(world: &mut World, method: &http::Method, path: &str, query: &str, body: &[u8]) -> (u16, Value) {
    let secrets = format!("/api/v1/namespaces/{NAMESPACE}/secrets");
    let site_status = path
        .strip_prefix(&format!("{SITES}/"))
        .and_then(|rest| rest.strip_suffix("/status"));
    match (method.as_str(), path) {
        ("GET", SITES) => list("GridSiteList", &serde_json::to_value(&world.sites).unwrap_or_default()),
        ("GET", p) if p.starts_with(&format!("{SITES}/")) => world.get_site(p.trim_start_matches(&format!("{SITES}/"))),
        ("GET", "/apis/grid.praxis.fast/v1alpha1/gridnetworks") => list("GridNetworkList", &json!([])),
        ("GET", p) if p.starts_with(&format!("{secrets}/")) => {
            world.get_secret(p.trim_start_matches(&format!("{secrets}/")))
        },
        ("POST", p) if p == secrets => world.post_secret(query, body),
        ("POST", p) if p == format!("/apis/events.k8s.io/v1/namespaces/{NAMESPACE}/events") => world.post_event(body),
        ("PATCH", _) if site_status.is_some() => {
            world.patch_queries.push(query.to_owned());
            world.patch_status(site_status.unwrap_or_default(), body)
        },
        _ => {
            world.unexpected.push(format!("{method} {path}?{query}"));
            not_found()
        },
    }
}

/// A client served from `world`.
fn client(world: &Arc<Mutex<World>>) -> Client {
    let world = Arc::clone(world);
    let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
        let world = Arc::clone(&world);
        async move {
            let (parts, body) = req.into_parts();
            let bytes = http_body_util::BodyExt::collect(body)
                .await
                .map(http_body_util::Collected::to_bytes)
                .unwrap_or_default();
            let (status, value) = {
                let mut world = world.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                answer(
                    &mut world,
                    &parts.method,
                    parts.uri.path(),
                    parts.uri.query().unwrap_or_default(),
                    &bytes,
                )
            };
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(kube::client::Body::from(serde_json::to_vec(&value).unwrap_or_default()))
                    .unwrap_or_else(|_| std::process::abort()),
            )
        }
    });
    Client::new(service, NAMESPACE)
}

fn site(name: &str, spec: &Value) -> GridSite {
    let mut spec = spec.clone();
    if let Some(object) = spec.as_object_mut() {
        object.insert("gridNetworkRef".to_owned(), json!("grid"));
    }
    serde_json::from_value(json!({
        "apiVersion": "grid.praxis.fast/v1alpha1",
        "kind": "GridSite",
        "metadata": {"name": name, "uid": format!("uid-{name}"), "generation": 1},
        "spec": spec,
    }))
    .expect("site")
}

/// The operator config naming this site, its signer at `url` pinned by Secret
/// `grid-ca-bundle`, and the invite unless `invite` is false.
fn document(url: &str, invite: bool) -> String {
    let mut text = format!(
        "site:\n  name: {SITE}\n  signerEndpoint:\n    url: {url}\n    caBundleRef: {{kind: Secret, name: grid-ca-bundle}}\n"
    );
    if invite {
        text.push_str("  inviteSecretRef:\n    name: ");
        text.push_str(TOKEN_SECRET);
        text.push('\n');
    }
    text
}

/// This cluster's own `GridSite`, declared.
fn own_site() -> GridSite {
    site(SITE, &json!({}))
}

/// A world holding the own `GridSite` and the operator config for `url`, with the invite
/// unless `invite` is false.
fn enrolling(url: &str, invite: bool) -> Arc<Mutex<World>> {
    let world = world(vec![own_site()]);
    world.lock().expect("lock").configure(&document(url, invite));
    world
}

/// The operator config as the watch would hand it over.
fn snapshot(world: &Arc<Mutex<World>>) -> Snapshot {
    world
        .lock()
        .expect("lock")
        .config
        .as_ref()
        .map_or_else(Snapshot::absent, Snapshot::of)
}

/// One step of `enrollment` against `world`, under the identity naming `identity`.
async fn run_step(enrollment: &mut Enrollment, world: &Arc<Mutex<World>>, identity: Option<&str>) -> Step {
    Box::pin(enrollment.step(&client(world), identity, &snapshot(world)))
        .await
        .expect("step")
}

/// The `Enrolled` conditions written to the local site, with the lock released.
fn verdicts(world: &Arc<Mutex<World>>) -> Vec<(String, String)> {
    world.lock().expect("lock").verdicts(SITE)
}

fn world(sites: Vec<GridSite>) -> Arc<Mutex<World>> {
    Arc::new(Mutex::new(World {
        sites,
        ..World::default()
    }))
}

fn invited(world: &Arc<Mutex<World>>, token: &str) {
    world
        .lock()
        .expect("lock")
        .put_secret(TOKEN_SECRET, &[("token", token)], &[("grid.praxis.fast/site", SITE)]);
}

fn pinned(world: &Arc<Mutex<World>>, ca_pem: &str) {
    world.lock().expect("lock").put_secret(
        "grid-ca-bundle",
        &[("ca.crt", ca_pem)],
        &[(SIGNER_BUNDLE_LABEL, "true")],
    );
}

#[test]
fn the_own_site_is_the_grid_site_of_the_name_given() {
    let sites = [site("west", &json!({})), own_site()];
    assert_eq!(
        own(&sites, Some(SITE)).and_then(|site| site.metadata.name.as_deref()),
        Some(SITE)
    );
    assert!(own(&sites, Some("north")).is_none(), "no GridSite of that name");
    assert!(own(&sites, None).is_none(), "no name, no own site");
}

#[test]
fn only_a_grid_site_holding_our_conditions_is_this_clusters_own() {
    let mut site = own_site();
    assert!(!is_own(&site), "a peer holds none");
    site.status = Some(crate::crd::grid_site::GridSiteStatus {
        conditions: vec![gate(ACCEPTED, "True", ACCEPTED)],
        ..Default::default()
    });
    assert!(is_own(&site));
}

#[test]
fn a_condition_already_held_is_not_rewritten_and_keeps_its_transition_time() {
    let mut site = own_site();
    let verdict = Verdict::awaiting_invite();
    let first = condition(&site, ENROLLED, &verdict, "2026-10-22T10:00:00Z").expect("new");
    assert_eq!(first.last_transition_time, "2026-10-22T10:00:00Z");
    assert_eq!(first.observed_generation, Some(1));
    site.status = Some(crate::crd::grid_site::GridSiteStatus {
        conditions: vec![first],
        ..Default::default()
    });
    assert!(
        condition(&site, ENROLLED, &verdict, "2026-10-22T11:00:00Z").is_none(),
        "unchanged"
    );
    assert!(
        condition(&site, ROTATION_ON_SCHEDULE, &verdict, "2026-10-22T11:00:00Z").is_some(),
        "a held Enrolled says nothing about RotationOnSchedule"
    );
    let issued = condition(
        &site,
        ENROLLED,
        &Verdict::issued("spiffe://grid.internal/site/site-d"),
        "2026-10-22T12:00:00Z",
    )
    .expect("changed");
    assert_eq!(issued.last_transition_time, "2026-10-22T12:00:00Z");
}

#[test]
fn a_status_change_moves_the_transition_time() {
    let mut site = own_site();
    let held = condition(&site, ENROLLED, &Verdict::awaiting_invite(), "2026-10-22T10:00:00Z").expect("new");
    site.status = Some(crate::crd::grid_site::GridSiteStatus {
        conditions: vec![held],
        ..Default::default()
    });
    let unknown = condition(
        &site,
        ENROLLED,
        &Verdict::awaiting_first_attempt(),
        "2026-10-22T11:00:00Z",
    )
    .expect("changed");
    assert_eq!(
        unknown.last_transition_time, "2026-10-22T11:00:00Z",
        "False to Unknown is a transition"
    );
}

#[test]
fn a_lapsed_signer_certificate_reads_untrusted_and_is_retried() {
    assert_eq!(reason(&EnrollError::SignerLapsed(String::new())), "SignerUntrusted");
    assert!(
        definitive(&EnrollError::SignerUntrusted(String::new()))
            && !definitive(&EnrollError::SignerLapsed(String::new())),
        "a pin mismatch waits for an input change; a lapsed certificate is retried, since the signer fixes it"
    );
}

#[test]
fn every_enrollment_failure_has_its_own_reason() {
    let errors = [
        EnrollError::Config(String::new()),
        EnrollError::SignerUntrusted(String::new()),
        EnrollError::RefNotFound(String::new()),
        EnrollError::NameMismatch(String::new()),
        EnrollError::TokenRejected(String::new()),
        EnrollError::NameTaken(String::new()),
        EnrollError::NameDeleted(String::new()),
        EnrollError::Refused(String::new()),
        EnrollError::MaybeSpent(String::new()),
        EnrollError::InvalidResponse(String::new()),
        EnrollError::NotStored(String::new()),
        EnrollError::Kube(String::new()),
    ];
    let reasons: std::collections::BTreeSet<_> = errors.iter().map(reason).collect();
    assert_eq!(reasons.len(), errors.len(), "{reasons:?}");
    assert_eq!(
        reason(&EnrollError::Kube(String::new())),
        "StoreUnavailable",
        "the store, not the implementation"
    );
    assert!(
        reasons
            .iter()
            .all(|r| r.chars().next().is_some_and(char::is_uppercase) && r.chars().all(char::is_alphanumeric)),
        "CamelCase: {reasons:?}"
    );
}

#[tokio::test]
async fn without_an_operator_config_nothing_is_read_or_written() {
    let world = world(vec![site("west", &json!({}))]);
    let step = run_step(&mut Enrollment::default(), &world, None).await;
    assert_eq!(step, Step::NoLocalSite);
    let world = world.lock().expect("lock").clone();
    assert!(world.patches.is_empty() && world.reads.is_empty(), "{:?}", world.reads);
}

#[tokio::test]
async fn a_refused_config_is_accepted_false_on_the_own_site_and_nothing_is_sent() {
    let world = world(vec![own_site()]);
    world
        .lock()
        .expect("lock")
        .configure("site:\n  name: site-d\n  signerEndpont: {}\n");
    invited(&world, "secret");
    identified(&world);
    let step = run_step(&mut Enrollment::default(), &world, Some(SITE)).await;
    assert_eq!(step, Step::Refused);
    let held = world.lock().expect("lock").clone();
    assert_eq!(
        held.verdicts_of(SITE, ACCEPTED),
        [("False".to_owned(), "OperatorConfigInvalid".to_owned())]
    );
    assert!(
        held.messages_of(SITE, ACCEPTED)
            .iter()
            .all(|message| message.contains("signerEndpont")),
        "the message names the field at fault"
    );
    assert_eq!(
        held.verdicts_of(SITE, AVAILABLE),
        [("False".to_owned(), "OperatorConfigInvalid".to_owned())],
        "the roll-up copies the failing gate"
    );
    assert!(held.verdicts(SITE).is_empty(), "Enrolled says nothing about acceptance");
    assert!(!held.reads.contains(&TOKEN_SECRET.to_owned()), "the invite is not read");
}

#[tokio::test]
async fn before_its_grid_site_exists_a_refused_config_is_an_event_once() {
    let world = world(Vec::new());
    world.lock().expect("lock").configure("site: [");
    let mut enrollment = Enrollment::default();
    for _ in 0..2 {
        let step = run_step(&mut enrollment, &world, None).await;
        assert_eq!(step, Step::Refused);
    }
    let held = world.lock().expect("lock").clone();
    assert_eq!(
        held.events,
        [("Warning".to_owned(), "OperatorConfigInvalid".to_owned())],
        "recorded once while unchanged"
    );
    assert!(held.patches.is_empty(), "no GridSite to write to");
}

#[tokio::test]
async fn a_site_without_an_invite_waits_for_one() {
    let world = enrolling("https://enroll.example.com", false);
    let step = run_step(&mut Enrollment::default(), &world, None).await;
    assert_eq!(step, Step::AwaitingInvite);
    assert_eq!(
        verdicts(&world),
        [("False".to_owned(), "AwaitingInvite".to_owned())],
        "a state the admin must act on is False"
    );
    let waiting = world.lock().expect("lock").messages_of(SITE, IDENTITY_VALID);
    assert!(
        waiting
            .iter()
            .any(|message| message.starts_with("waiting for a site identity")),
        "the site says it waits for its identity: {waiting:?}"
    );
}

/// The identity Secret, so the site reads as enrolled.
fn identified(world: &Arc<Mutex<World>>) {
    world
        .lock()
        .expect("lock")
        .put_secret(crate::enroll::IDENTITY_SECRET, &[("tls.crt", "x")], &[]);
}

#[tokio::test]
async fn a_config_naming_another_site_than_the_identity_is_refused_and_never_enrolled() {
    let world = world(vec![own_site()]);
    world
        .lock()
        .expect("lock")
        .configure(&document("https://enroll.example.com", true).replace(SITE, "east"));
    invited(&world, "secret");
    identified(&world);
    let step = run_step(&mut Enrollment::default(), &world, Some(SITE)).await;
    assert_eq!(step, Step::Refused);
    let held = world.lock().expect("lock").clone();
    assert_eq!(
        held.verdicts_of(SITE, ACCEPTED),
        [("False".to_owned(), "SiteNameMismatch".to_owned())]
    );
    assert!(!held.reads.contains(&TOKEN_SECRET.to_owned()), "the invite is not read");
}

#[tokio::test]
async fn with_an_identity_and_no_config_the_named_site_reads_enrolled() {
    let world = world(vec![own_site()]);
    identified(&world);
    let step = run_step(&mut Enrollment::default(), &world, Some(SITE)).await;
    assert_eq!(step, Step::Present);
    let held = world.lock().expect("lock").clone();
    assert_eq!(
        held.verdicts_of(SITE, ACCEPTED),
        [("True".to_owned(), "Accepted".to_owned())]
    );
    assert_eq!(held.verdicts(SITE), [("True".to_owned(), "IdentityPresent".to_owned())]);
}

#[tokio::test]
async fn a_renamed_site_clears_what_the_old_grid_site_held() {
    let mut old = site("east", &json!({}));
    old.status = Some(crate::crd::grid_site::GridSiteStatus {
        conditions: vec![gate(ACCEPTED, "True", ACCEPTED)],
        ..Default::default()
    });
    let world = world(vec![own_site(), old]);
    world
        .lock()
        .expect("lock")
        .configure(&document("https://enroll.example.com", false));
    run_step(&mut Enrollment::default(), &world, None).await;
    let held = world.lock().expect("lock").clone();
    let cleared = held
        .patches
        .iter()
        .filter(|(name, patch)| name == "east" && patch.pointer("/status/conditions") == Some(&json!([])))
        .count();
    assert_eq!(
        cleared,
        TYPES.len() + 1,
        "every manager, the own roll-up's included, gives up what it held on east"
    );
}

#[tokio::test]
async fn an_existing_identity_is_enrolled_and_the_invite_is_never_read() {
    let world = enrolling("https://enroll.example.com", true);
    invited(&world, "secret");
    world
        .lock()
        .expect("lock")
        .put_secret(crate::enroll::IDENTITY_SECRET, &[("tls.crt", "x")], &[]);
    let mut enrollment = Enrollment::default();
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Present);
    assert_eq!(
        run_step(&mut enrollment, &world, Some(SITE)).await,
        Step::Present,
        "named by the identity too"
    );
    let world = world.lock().expect("lock").clone();
    assert_eq!(
        world.verdicts(SITE),
        [("True".to_owned(), "IdentityPresent".to_owned())],
        "written once"
    );
    assert!(
        !world.reads.contains(&TOKEN_SECRET.to_owned()),
        "the invite is not read"
    );
}

#[tokio::test]
async fn the_site_enrolls_through_the_operator_config() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Sign).await;
    let world = enrolling(settings.base.as_str(), true);
    invited(&world, "secret");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    let step = run_step(&mut enrollment, &world, None).await;
    assert_eq!(step, Step::Enrolled);
    let later = run_step(&mut enrollment, &world, None).await;
    assert_eq!(later, Step::Present, "a later poll finds the identity");
    assert_eq!(mock.calls(), 1);
    let world = world.lock().expect("lock").clone();
    assert_eq!(
        world.verdicts(SITE),
        [
            ("Unknown".to_owned(), "AwaitingFirstAttempt".to_owned()),
            ("True".to_owned(), "Issued".to_owned())
        ]
    );
    progressed_then_available(&world);
    let identity = world
        .secrets
        .get(crate::enroll::IDENTITY_SECRET)
        .expect("identity written");
    assert_eq!(identity.type_.as_deref(), Some("kubernetes.io/tls"));
    assert!(world.secrets.contains_key(crate::enroll::CA_SECRET), "grid CA written");
    assert!(world.unexpected.is_empty(), "{:?}", world.unexpected);
}

#[tokio::test]
async fn before_its_grid_site_exists_nothing_is_sent_and_an_event_says_why_once() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Sign).await;
    let world = world(Vec::new());
    world
        .lock()
        .expect("lock")
        .configure(&document(settings.base.as_str(), true));
    invited(&world, "secret");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    for _ in 0..2 {
        assert_eq!(run_step(&mut enrollment, &world, None).await, Step::NoLocalSite);
    }
    assert_eq!(mock.calls(), 0, "nothing is sent without the GridSite to report on");
    let held = world.lock().expect("lock").clone();
    assert_eq!(held.events, [("Warning".to_owned(), "AwaitingGridSite".to_owned())]);
    assert!(held.patches.is_empty());

    world.lock().expect("lock").sites.push(own_site());
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Enrolled);
    assert_eq!(verdicts(&world).last(), Some(&("True".to_owned(), "Issued".to_owned())));
}

/// `Progressing` went `True` then `False`, and `Available` rolled up `True`.
fn progressed_then_available(world: &World) {
    assert_eq!(
        world.verdicts_of(SITE, PROGRESSING),
        [
            ("True".to_owned(), "Enrolling".to_owned()),
            ("False".to_owned(), "AsExpected".to_owned())
        ]
    );
    assert_eq!(
        world.verdicts_of(SITE, IDENTITY_VALID),
        [("Unknown".to_owned(), "AwaitingFirstAttempt".to_owned())],
        "not evaluated until the renewal loop reads the identity"
    );
    assert_eq!(
        world.verdicts_of(SITE, AVAILABLE).last(),
        Some(&("Unknown".to_owned(), "AwaitingFirstAttempt".to_owned())),
        "the roll-up waits on the one gate not evaluated yet"
    );
}

#[tokio::test]
async fn a_spent_invite_is_not_sent_again_until_it_is_replaced() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Spent).await;
    let world = enrolling(settings.base.as_str(), true);
    invited(&world, "spent");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Failed);
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Unchanged);
    assert_eq!(mock.calls(), 1, "a refused invite is not resent");
    assert_eq!(
        verdicts(&world).last(),
        Some(&("False".to_owned(), "InviteInvalid".to_owned()))
    );
    invited(&world, "fresh");
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Failed);
    assert_eq!(mock.calls(), 2, "a replaced invite is sent");
}

#[tokio::test]
async fn a_missing_pin_is_reported_and_retried_once_it_appears() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Sign).await;
    let world = enrolling(settings.base.as_str(), true);
    invited(&world, "secret");
    let mut enrollment = Enrollment::default();
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Failed);
    assert_eq!(
        world.lock().expect("lock").verdicts_of(SITE, RESOLVED_REFS).last(),
        Some(&("False".to_owned(), "RefNotFound".to_owned())),
        "a missing reference is on ResolvedRefs, not Enrolled"
    );
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Unchanged);
    assert_eq!(mock.calls(), 0, "nothing is sent without a pin");
    pinned(&world, &mock.ca.cert_pem);
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Enrolled);
    assert_eq!(mock.calls(), 1);
}

#[tokio::test]
async fn an_invite_minted_for_another_site_is_refused_unsent() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Sign).await;
    let world = enrolling(settings.base.as_str(), true);
    world.lock().expect("lock").put_secret(
        TOKEN_SECRET,
        &[("token", "secret")],
        &[("grid.praxis.fast/site", "site-e")],
    );
    pinned(&world, &mock.ca.cert_pem);
    assert_eq!(run_step(&mut Enrollment::default(), &world, None).await, Step::Failed);
    assert_eq!(mock.calls(), 0, "the token is not sent");
    assert_eq!(
        verdicts(&world).last(),
        Some(&("False".to_owned(), "NameMismatch".to_owned()))
    );
}

#[tokio::test]
async fn a_bundle_that_does_not_opt_in_is_never_pinned() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Sign).await;
    let world = enrolling(settings.base.as_str(), true);
    invited(&world, "secret");
    world
        .lock()
        .expect("lock")
        .put_secret("grid-ca-bundle", &[("ca.crt", &mock.ca.cert_pem)], &[]);
    assert_eq!(run_step(&mut Enrollment::default(), &world, None).await, Step::Failed);
    assert_eq!(mock.calls(), 0, "the token is not sent");
    assert_eq!(
        world.lock().expect("lock").verdicts_of(SITE, RESOLVED_REFS).last(),
        Some(&("False".to_owned(), "RefNotFound".to_owned())),
        "a missing reference is on ResolvedRefs, not Enrolled"
    );
}

#[tokio::test]
async fn a_signer_error_is_retried_on_the_next_poll() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::ServerError).await;
    let world = enrolling(settings.base.as_str(), true);
    invited(&world, "secret");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Failed);
    assert_eq!(
        verdicts(&world).last(),
        Some(&("False".to_owned(), "InviteMaybeSpent".to_owned()))
    );
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Failed);
    assert_eq!(
        mock.calls(),
        2,
        "a 503 may not have spent the token, so it is asked again"
    );
    let progressing = world.lock().expect("lock").verdicts_of(SITE, PROGRESSING);
    assert!(
        !progressing.iter().any(|(status, _)| status == "False"),
        "a resend holds Progressing True across polls, so lastTransitionTime does not churn: {progressing:?}"
    );
    assert_eq!(progressing.last(), Some(&("True".to_owned(), "Retrying".to_owned())));
}

#[tokio::test]
async fn a_transient_api_failure_keeps_a_refused_invite_unsent() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Spent).await;
    let world = enrolling(settings.base.as_str(), true);
    invited(&world, "spent");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Failed);
    for flaky in [crate::enroll::IDENTITY_SECRET, TOKEN_SECRET] {
        world.lock().expect("lock").flaky.push(flaky.to_owned());
        assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Failed, "{flaky}");
        assert_eq!(
            run_step(&mut enrollment, &world, None).await,
            Step::Unchanged,
            "{flaky}"
        );
    }
    assert_eq!(mock.calls(), 1, "a refused invite is never resent");
    retried_then_settled(&world.lock().expect("lock").verdicts_of(SITE, PROGRESSING));
}

/// A store failure read as `Progressing` `True` `Retrying`, and the retry settled it.
fn retried_then_settled(progressing: &[(String, String)]) {
    assert!(
        progressing.contains(&("True".to_owned(), "Retrying".to_owned())),
        "a store failure is progress the next poll retries: {progressing:?}"
    );
    assert_eq!(
        progressing.last(),
        Some(&("False".to_owned(), "AsExpected".to_owned())),
        "the retry read the store and found nothing changed"
    );
}

#[tokio::test]
async fn an_unrelated_spec_edit_does_not_resend_a_refused_invite() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Spent).await;
    let world = enrolling(settings.base.as_str(), true);
    invited(&world, "spent");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Failed);
    {
        let mut held = world.lock().expect("lock");
        let site = held.sites.first_mut().expect("site");
        site.spec.zone = Some("us-east-1b".to_owned());
        site.metadata.generation = Some(2);
        drop(held);
    }
    assert_eq!(run_step(&mut enrollment, &world, None).await, Step::Unchanged);
    assert_eq!(mock.calls(), 1);
}

#[tokio::test]
async fn the_condition_is_applied_by_its_own_field_manager() {
    let world = enrolling("https://enroll.example.com", false);
    assert_eq!(
        run_step(&mut Enrollment::default(), &world, None).await,
        Step::AwaitingInvite
    );
    let queries = world.lock().expect("lock").patch_queries.clone();
    assert!(
        queries
            .iter()
            .all(|q| q.contains("fieldManager=grid-operator-") && q.contains("force=true")),
        "{queries:?}"
    );
    assert!(!queries.is_empty());
}

/// A condition as held.
fn gate(type_: &str, status: &str, reason: &str) -> Condition {
    Condition {
        type_: type_.to_owned(),
        status: status.to_owned(),
        reason: reason.to_owned(),
        message: format!("{reason} happened"),
        last_transition_time: "2026-10-22T10:00:00Z".to_owned(),
        observed_generation: Some(1),
    }
}

/// Drop the condition `type_` from `held`.
fn without(held: &mut GridSite, type_: &str) {
    if let Some(status) = held.status.as_mut() {
        status.conditions.retain(|c| c.type_ != type_);
    }
}

/// The roll-up of `site` with nothing in flight, as (type, status, reason).
fn rolled(site: &GridSite) -> Vec<(String, String, String)> {
    own_rollup(site, None, "2026-10-22T12:00:00Z")
        .into_iter()
        .map(|c| (c.type_, c.status, c.reason))
        .collect()
}

/// The (status, reason) `type_` reads in `rolled`.
fn reads<'rolled>(rolled: &'rolled [(String, String, String)], type_: &str) -> (&'rolled str, &'rolled str) {
    rolled
        .iter()
        .find(|(held, ..)| held == type_)
        .map(|(_, status, reason)| (status.as_str(), reason.as_str()))
        .expect("rolled up")
}

#[test]
fn degraded_mirrors_a_rotation_that_will_not_happen() {
    let mut site = own_site();
    site.status = Some(crate::crd::grid_site::GridSiteStatus {
        conditions: vec![gate(ROTATION_ON_SCHEDULE, "False", "RotationOverdue")],
        ..Default::default()
    });
    assert_eq!(reads(&rolled(&site), DEGRADED), ("True", "RotationOverdue"));
    for status in ["True", "Unknown"] {
        site.status = Some(crate::crd::grid_site::GridSiteStatus {
            conditions: vec![gate(ROTATION_ON_SCHEDULE, status, "RotationDisabled")],
            ..Default::default()
        });
        assert_eq!(reads(&rolled(&site), DEGRADED), ("False", "AsExpected"), "{status}");
    }
}

#[test]
fn available_copies_the_first_failing_gate_in_order() {
    let mut site = own_site();
    assert_eq!(
        reads(&rolled(&site), AVAILABLE),
        ("Unknown", "AwaitingFirstCheck"),
        "a gate not written yet is not evaluated, never True"
    );
    site.status = Some(crate::crd::grid_site::GridSiteStatus {
        conditions: vec![
            gate(ROTATION_ON_SCHEDULE, "False", "RenewalFailing"),
            gate(ENROLLED, "True", "Issued"),
            gate(ACCEPTED, "True", ACCEPTED),
            gate(RESOLVED_REFS, "True", RESOLVED_REFS),
            gate(IDENTITY_VALID, "Unknown", "AwaitingFirstAttempt"),
        ],
        ..Default::default()
    });
    assert_eq!(reads(&rolled(&site), AVAILABLE), ("Unknown", "AwaitingFirstAttempt"));
    without(&mut site, IDENTITY_VALID);
    site.status
        .as_mut()
        .expect("status")
        .conditions
        .push(gate(IDENTITY_VALID, "True", IDENTITY_VALID));
    assert_eq!(
        reads(&rolled(&site), AVAILABLE),
        ("True", AVAILABLE),
        "RotationOnSchedule False raises Degraded, not Available"
    );
    assert_eq!(reads(&rolled(&site), DEGRADED), ("True", "RenewalFailing"));
}
