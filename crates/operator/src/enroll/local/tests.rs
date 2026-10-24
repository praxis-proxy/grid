use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use k8s_openapi::ByteString;
use kube::api::ObjectMeta;
use serde_json::json;

use super::*;
use crate::enroll::tests::{Reply, SITE, TOKEN_SECRET, serve};

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

    /// The messages of the `type_` conditions written to `site`, in order.
    fn messages_of(&self, site: &str, type_: &str) -> Vec<String> {
        self.patches
            .iter()
            .filter(|(name, _)| name == site)
            .filter_map(|(_, patch)| patch.pointer("/status/conditions/0").cloned())
            .filter(|c| c.get("type").and_then(Value::as_str) == Some(type_))
            .map(|c| c.get("message").and_then(Value::as_str).unwrap_or_default().to_owned())
            .collect()
    }

    /// The conditions of `type_` written to `site`, in order.
    fn verdicts_of(&self, site: &str, type_: &str) -> Vec<(String, String)> {
        self.patches
            .iter()
            .filter(|(name, _)| name == site)
            .filter_map(|(_, patch)| patch.pointer("/status/conditions/0").cloned())
            .filter(|c| c.get("type").and_then(Value::as_str) == Some(type_))
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

    /// Apply a status patch to `GridSite` `name`, holding its `Enrolled` condition.
    fn patch_status(&mut self, name: &str, body: &[u8]) -> (u16, Value) {
        let applied: Value = serde_json::from_slice(body).unwrap_or_default();
        let condition: Option<Condition> = applied
            .pointer("/status/conditions/0")
            .cloned()
            .and_then(|c| serde_json::from_value(c).ok());
        self.patches.push((name.to_owned(), applied));
        let Some(site) = self.sites.iter_mut().find(|s| s.metadata.name.as_deref() == Some(name)) else {
            return not_found();
        };
        let status = site.status.get_or_insert_with(Default::default);
        match &condition {
            // An apply carrying no conditions gives up what its manager held.
            None => status.conditions.clear(),
            Some(written) => status.conditions.retain(|held| held.type_ != written.type_),
        }
        status.conditions.extend(condition);
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

/// `spec.local` naming the signer at `url`, pinned by Secret `grid-ca-bundle`, with the
/// invite unless `invite` is false.
fn local_spec(url: &str, invite: bool) -> Value {
    let mut local =
        json!({"signerEndpoint": {"url": url, "caBundleRef": {"kind": "Secret", "name": "grid-ca-bundle"}}});
    if invite && let Some(object) = local.as_object_mut() {
        object.insert("inviteSecretRef".to_owned(), json!({"name": TOKEN_SECRET}));
    }
    json!({"local": local})
}

/// The local site at `url`, pinned by Secret `grid-ca-bundle`, with the invite unless `invite` is false.
fn local_site(url: &str, invite: bool) -> GridSite {
    site(SITE, &local_spec(url, invite))
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
fn before_an_identity_the_local_site_is_the_one_carrying_spec_local() {
    let peer = site("west", &json!({}));
    let own = local_site("https://enroll.example.com", false);
    let sites = [peer.clone(), own];
    assert!(matches!(local(&sites, None), Local::One(site) if site.metadata.name.as_deref() == Some(SITE)));
    assert!(matches!(local(&[peer], None), Local::Absent), "peers never enroll");
    let two = [
        local_site("https://enroll.example.com", false),
        site("east", &local_spec("https://enroll.example.com", false)),
    ];
    assert!(matches!(local(&two, None), Local::Several(several) if several.len() == 2));
}

#[test]
fn once_the_identity_names_the_site_its_grid_site_wins_and_spec_local_elsewhere_is_a_stray() {
    let sites = [
        site(SITE, &json!({})),
        site("east", &local_spec("https://enroll.example.com", true)),
        site("west", &json!({})),
    ];
    let resolve = |listed: &[GridSite], identity: &str| match local(listed, Some(identity)) {
        Local::Named { site, strays } => (
            site.and_then(|site| site.metadata.name.clone()),
            strays
                .iter()
                .filter_map(|stray| stray.metadata.name.clone())
                .collect::<Vec<_>>(),
        ),
        other @ (Local::Absent | Local::One(_) | Local::Several(_)) => (Some(format!("{other:?}")), Vec::new()),
    };
    assert_eq!(
        resolve(&sites, SITE),
        (Some(SITE.to_owned()), vec!["east".to_owned()]),
        "the named site, without spec.local; spec.local elsewhere is a stray"
    );
    assert_eq!(
        resolve(&sites, "north"),
        (None, vec!["east".to_owned()]),
        "no GridSite declared for the identity's site"
    );
    assert_eq!(
        resolve(&sites[1..], "east"),
        (Some("east".to_owned()), Vec::new()),
        "the named site carrying spec.local is not a stray"
    );
}

#[test]
fn a_condition_already_held_is_not_rewritten_and_keeps_its_transition_time() {
    let mut site = local_site("https://enroll.example.com", false);
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
    let mut site = local_site("https://enroll.example.com", false);
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
async fn without_a_local_site_nothing_is_read_or_written() {
    let world = world(vec![site("west", &json!({}))]);
    let step = Enrollment::default().step(&client(&world), None).await.expect("step");
    assert_eq!(step, Step::NoLocalSite);
    let world = world.lock().expect("lock").clone();
    assert!(world.patches.is_empty() && world.reads.is_empty(), "{:?}", world.reads);
}

#[tokio::test]
async fn several_local_sites_enroll_none_and_say_so_on_each() {
    let world = world(vec![
        local_site("https://enroll.example.com", true),
        site("east", &local_spec("https://enroll.example.com", false)),
    ]);
    invited(&world, "secret");
    let step = Enrollment::default().step(&client(&world), None).await.expect("step");
    assert_eq!(step, Step::Duplicate);
    let world = world.lock().expect("lock").clone();
    for name in [SITE, "east"] {
        assert_eq!(
            world.verdicts_of(name, ACCEPTED),
            [("False".to_owned(), "DuplicateLocalSite".to_owned())],
            "{name}"
        );
        assert_eq!(
            world.verdicts_of(name, AVAILABLE),
            [("False".to_owned(), "DuplicateLocalSite".to_owned())],
            "{name}: the roll-up copies the failing gate"
        );
        assert!(
            world.verdicts(name).is_empty(),
            "{name}: Enrolled says nothing about acceptance"
        );
    }
    assert!(
        !world.reads.contains(&TOKEN_SECRET.to_owned()),
        "the invite is not read"
    );
}

#[tokio::test]
async fn a_site_without_an_invite_waits_for_one() {
    let world = world(vec![local_site("https://enroll.example.com", false)]);
    let step = Enrollment::default().step(&client(&world), None).await.expect("step");
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
async fn with_an_identity_a_stray_spec_local_is_reported_and_never_enrolled() {
    let world = world(vec![
        site(SITE, &json!({})),
        site("east", &local_spec("https://enroll.example.com", true)),
    ]);
    invited(&world, "secret");
    identified(&world);
    let step = Enrollment::default()
        .step(&client(&world), Some(SITE))
        .await
        .expect("step");
    assert_eq!(step, Step::Present, "the named site, which carries no spec.local");
    let held = world.lock().expect("lock").clone();
    assert_eq!(held.verdicts(SITE), [("True".to_owned(), "IdentityPresent".to_owned())]);
    assert_eq!(
        held.verdicts_of("east", ACCEPTED),
        [("False".to_owned(), "NotLocalSite".to_owned())]
    );
    assert!(
        !held.reads.contains(&TOKEN_SECRET.to_owned()),
        "the stray's invite is not read"
    );
}

#[tokio::test]
async fn with_an_identity_but_no_grid_site_named_after_it_only_strays_are_reported() {
    let world = world(vec![site("east", &local_spec("https://enroll.example.com", true))]);
    invited(&world, "secret");
    identified(&world);
    let step = Enrollment::default()
        .step(&client(&world), Some(SITE))
        .await
        .expect("step");
    assert_eq!(step, Step::NoLocalSite);
    let held = world.lock().expect("lock").clone();
    assert_eq!(
        held.verdicts_of("east", ACCEPTED),
        [("False".to_owned(), "NotLocalSite".to_owned())]
    );
    assert!(held.reads.is_empty(), "{:?}", held.reads);
}

#[tokio::test]
async fn an_existing_identity_is_enrolled_and_the_invite_is_never_read() {
    let world = world(vec![local_site("https://enroll.example.com", true)]);
    invited(&world, "secret");
    world
        .lock()
        .expect("lock")
        .put_secret(crate::enroll::IDENTITY_SECRET, &[("tls.crt", "x")], &[]);
    let mut enrollment = Enrollment::default();
    assert_eq!(
        enrollment.step(&client(&world), None).await.expect("step"),
        Step::Present
    );
    assert_eq!(
        enrollment.step(&client(&world), Some(SITE)).await.expect("step"),
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
async fn the_local_site_enrolls_from_its_grid_site() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Sign).await;
    let world = world(vec![local_site(settings.base.as_str(), true)]);
    invited(&world, "secret");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    let step = Box::pin(enrollment.step(&client(&world), None)).await.expect("step");
    assert_eq!(step, Step::Enrolled);
    let later = Box::pin(enrollment.step(&client(&world), None)).await.expect("step");
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
    let world = world(vec![local_site(settings.base.as_str(), true)]);
    invited(&world, "spent");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Failed
    );
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Unchanged
    );
    assert_eq!(mock.calls(), 1, "a refused invite is not resent");
    assert_eq!(
        verdicts(&world).last(),
        Some(&("False".to_owned(), "InviteInvalid".to_owned()))
    );
    invited(&world, "fresh");
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Failed
    );
    assert_eq!(mock.calls(), 2, "a replaced invite is sent");
}

#[tokio::test]
async fn a_missing_pin_is_reported_and_retried_once_it_appears() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Sign).await;
    let world = world(vec![local_site(settings.base.as_str(), true)]);
    invited(&world, "secret");
    let mut enrollment = Enrollment::default();
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Failed
    );
    assert_eq!(
        world.lock().expect("lock").verdicts_of(SITE, RESOLVED_REFS).last(),
        Some(&("False".to_owned(), "RefNotFound".to_owned())),
        "a missing reference is on ResolvedRefs, not Enrolled"
    );
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Unchanged
    );
    assert_eq!(mock.calls(), 0, "nothing is sent without a pin");
    pinned(&world, &mock.ca.cert_pem);
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Enrolled
    );
    assert_eq!(mock.calls(), 1);
}

#[tokio::test]
async fn an_invite_minted_for_another_site_is_refused_unsent() {
    crate::init_process_crypto();
    let (mock, settings) = serve(Reply::Sign).await;
    let world = world(vec![local_site(settings.base.as_str(), true)]);
    world.lock().expect("lock").put_secret(
        TOKEN_SECRET,
        &[("token", "secret")],
        &[("grid.praxis.fast/site", "site-e")],
    );
    pinned(&world, &mock.ca.cert_pem);
    assert_eq!(
        Box::pin(Enrollment::default().step(&client(&world), None))
            .await
            .expect("step"),
        Step::Failed
    );
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
    let world = world(vec![local_site(settings.base.as_str(), true)]);
    invited(&world, "secret");
    world
        .lock()
        .expect("lock")
        .put_secret("grid-ca-bundle", &[("ca.crt", &mock.ca.cert_pem)], &[]);
    assert_eq!(
        Box::pin(Enrollment::default().step(&client(&world), None))
            .await
            .expect("step"),
        Step::Failed
    );
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
    let world = world(vec![local_site(settings.base.as_str(), true)]);
    invited(&world, "secret");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Failed
    );
    assert_eq!(
        verdicts(&world).last(),
        Some(&("False".to_owned(), "InviteMaybeSpent".to_owned()))
    );
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Failed
    );
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
    let world = world(vec![local_site(settings.base.as_str(), true)]);
    invited(&world, "spent");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Failed
    );
    for flaky in [crate::enroll::IDENTITY_SECRET, TOKEN_SECRET] {
        world.lock().expect("lock").flaky.push(flaky.to_owned());
        assert_eq!(
            Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
            Step::Failed,
            "{flaky}"
        );
        assert_eq!(
            Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
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
    let world = world(vec![local_site(settings.base.as_str(), true)]);
    invited(&world, "spent");
    pinned(&world, &mock.ca.cert_pem);
    let mut enrollment = Enrollment::default();
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Failed
    );
    {
        let mut held = world.lock().expect("lock");
        let site = held.sites.first_mut().expect("site");
        site.spec.zone = Some("us-east-1b".to_owned());
        site.metadata.generation = Some(2);
        drop(held);
    }
    assert_eq!(
        Box::pin(enrollment.step(&client(&world), None)).await.expect("step"),
        Step::Unchanged
    );
    assert_eq!(mock.calls(), 1);
}

#[tokio::test]
async fn the_condition_is_applied_by_its_own_field_manager() {
    let world = world(vec![local_site("https://enroll.example.com", false)]);
    assert_eq!(
        Enrollment::default().step(&client(&world), None).await.expect("step"),
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

#[test]
fn degraded_mirrors_a_rotation_that_will_not_happen() {
    let overdue = Verdict::new("False", "RotationOverdue", "due".to_owned());
    let raised = degraded(&overdue);
    assert_eq!(
        (raised.status, raised.reason.as_ref(), raised.message.as_str()),
        ("True", "RotationOverdue", "due")
    );
    for status in ["True", "Unknown"] {
        let quiet = degraded(&Verdict::new(status, "RotationDisabled", String::new()));
        assert_eq!(
            (quiet.status, quiet.reason.as_ref()),
            ("False", "AsExpected"),
            "{status}"
        );
    }
}

#[test]
fn available_copies_the_first_failing_gate_in_order() {
    let mut site = local_site("https://enroll.example.com", false);
    assert!(available(&site).is_none(), "nothing to roll up yet");
    site.status = Some(crate::crd::grid_site::GridSiteStatus {
        conditions: vec![
            gate(ROTATION_ON_SCHEDULE, "False", "RenewalFailing"),
            gate(ENROLLED, "True", "Issued"),
            gate(ACCEPTED, "True", "AsExpected"),
            gate(IDENTITY_VALID, "Unknown", "AwaitingFirstCheck"),
        ],
        ..Default::default()
    });
    let unknown = available(&site).expect("gates present");
    assert_eq!(
        (unknown.status, unknown.reason.as_ref()),
        ("Unknown", "AwaitingFirstCheck")
    );
    assert_eq!(
        unknown.message, "AwaitingFirstCheck happened",
        "the gate's message is copied"
    );
    without(&mut site, IDENTITY_VALID);
    let healthy = available(&site).expect("gates present");
    assert_eq!(
        (healthy.status, healthy.reason.as_ref()),
        ("True", "AsExpected"),
        "RotationOnSchedule False raises Degraded, not Available"
    );
}
