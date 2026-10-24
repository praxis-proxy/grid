use std::sync::Mutex;

use time::Duration as Span;

use super::*;

/// The identity Secret in memory, with a version a write must match.
struct Memory {
    /// The Secret's data and its version.
    held: Mutex<(BTreeMap<String, String>, u64)>,
}

impl Memory {
    fn holding(cert: &str, key: &str, ca: &str) -> Self {
        let data = BTreeMap::from([
            ("tls.crt".to_owned(), cert.to_owned()),
            ("tls.key".to_owned(), key.to_owned()),
            ("ca.crt".to_owned(), ca.to_owned()),
        ]);
        Self {
            held: Mutex::new((data, 1)),
        }
    }

    fn get(&self, key: &str) -> Option<String> {
        self.held.lock().expect("lock").0.get(key).cloned()
    }

    fn version(&self) -> u64 {
        self.held.lock().expect("lock").1
    }
}

impl IdentityStore for Memory {
    async fn read(&self, _target: &Target) -> Result<Held, RenewError> {
        let (data, version) = &*self.held.lock().expect("lock");
        let get = |key: &str| data.get(key).cloned();
        Ok(Held {
            cert: get("tls.crt").expect("cert"),
            key: Zeroizing::new(get("tls.key").expect("key")),
            ca: get("ca.crt").expect("ca"),
            pending: get(PENDING_CSR).zip(get(PENDING_KEY).map(Zeroizing::new)),
            version: version.to_string(),
            rotated: data.contains_key(PREVIOUS_CERT),
        })
    }

    async fn write(
        &self,
        _target: &Target,
        version: &str,
        data: BTreeMap<&'static str, Option<Zeroizing<String>>>,
    ) -> Result<String, RenewError> {
        let (held, current) = &mut *self.held.lock().expect("lock");
        if version != current.to_string() {
            return Err(RenewError::Kube("409 conflict".to_owned()));
        }
        for (key, value) in data {
            match value {
                Some(text) => held.insert(key.to_owned(), text.to_string()),
                None => held.remove(key),
            };
        }
        *current = current.saturating_add(1);
        Ok(current.to_string())
    }
}

/// How the fake service answers.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Answer {
    Sign,
    /// Sign, then lose the response.
    Lose,
    Refuse,
    /// Sign for another site.
    Wrong,
}

/// A service signing with `ca` for the site the presented leaf names.
struct Service {
    ca: certs::CaCert,
    answer: Mutex<Answer>,
    asked: Mutex<Vec<String>>,
}

impl Service {
    fn new(ca: &certs::CaCert) -> Self {
        Self {
            ca: certs::load_ca("grid-ca", &ca.key_pem, &ca.cert_pem).expect("ca"),
            answer: Mutex::new(Answer::Sign),
            asked: Mutex::new(Vec::new()),
        }
    }

    fn answer(&self, answer: Answer) {
        *self.answer.lock().expect("lock") = answer;
    }

    fn asked(&self) -> Vec<String> {
        self.asked.lock().expect("lock").clone()
    }
}

impl Signer for Service {
    async fn enroll(&self, _credential: &str, _csr_pem: &str) -> Result<Issued, SignerError> {
        Err(SignerError::Request("renewal tests never enroll".to_owned()))
    }

    async fn renew(&self, held: &Held, csr_pem: &str) -> Result<Issued, SignerError> {
        let site = site_of(&held.cert).map_err(|e| SignerError::Request(e.to_string()))?;
        certs::verify_site_cert(&self.ca.cert_pem, &held.cert, &site)
            .map_err(|e| SignerError::Unauthenticated(e.to_string()))?;
        self.asked
            .lock()
            .expect("lock")
            .push(certs::verify_csr(csr_pem).expect("csr"));
        let answer = *self.answer.lock().expect("lock");
        let name = if answer == Answer::Wrong {
            "other"
        } else {
            site.as_str()
        };
        if answer == Answer::Refuse {
            return Err(forbidden());
        }
        let cert = certs::sign_csr(&self.ca, name, csr_pem, certs::Validity::default()).expect("sign");
        if answer == Answer::Lose {
            return Err(SignerError::Transport {
                sent: true,
                detail: "connection reset".to_owned(),
            });
        }
        Ok(Issued {
            certificate: cert.cert_pem,
            ca_certificate: self.ca.cert_pem.clone(),
        })
    }
}

/// The signer's 403 for a refused identity.
fn forbidden() -> SignerError {
    SignerError::Refused {
        status: StatusCode::FORBIDDEN,
        code: Some("identity_refused".to_owned()),
        detail: "403".to_owned(),
    }
}

/// A gateway Deployment in memory: its identity annotation and how often it was patched.
struct Gateway {
    present: bool,
    annotation: Mutex<Option<String>>,
    patches: Mutex<u32>,
    /// Fail the next patch, as an API error would.
    fail_next: Mutex<bool>,
}

impl Gateway {
    fn new(present: bool) -> Self {
        Self {
            present,
            annotation: Mutex::new(None),
            patches: Mutex::new(0),
            fail_next: Mutex::new(false),
        }
    }
}

impl GatewayRoller for Gateway {
    async fn roll(&self, fingerprint: &str, owed: bool) -> Result<GatewayRoll, RenewError> {
        if !self.present {
            return Ok(GatewayRoll::NoGateway);
        }
        let current = self.annotation.lock().expect("lock").clone();
        if roll_patch(current.as_deref(), fingerprint, owed).is_none() {
            return Ok(GatewayRoll::Unchanged);
        }
        if std::mem::take(&mut *self.fail_next.lock().expect("lock")) {
            return Err(RenewError::Kube("500 patch failed".to_owned()));
        }
        *self.annotation.lock().expect("lock") = Some(fingerprint.to_owned());
        *self.patches.lock().expect("lock") += 1;
        Ok(GatewayRoll::Rolled)
    }
}

#[tokio::test]
async fn a_renewal_rolls_the_gateway_onto_the_new_identity_once() {
    let (ca, store, now) = site(Span::days(21));
    let gateway = Gateway::new(true);
    let checked = check(&store, &Service::new(&ca), &target(), now).await;
    assert_eq!(rolls_after(&checked), Some(true), "{checked:?}");

    let rolled = roll_gateway(&store, &gateway, &target(), true).await.expect("rolled");
    assert_eq!(rolled, GatewayRoll::Rolled);
    let renewed = certs::canonical_fingerprint(&store.get("tls.crt").expect("cert")).expect("fingerprint");
    assert_eq!(
        gateway.annotation.lock().expect("lock").as_deref(),
        Some(renewed.as_str())
    );

    let again = roll_gateway(&store, &gateway, &target(), false).await.expect("again");
    assert_eq!(again, GatewayRoll::Unchanged, "the same leaf rolls nothing");
    assert_eq!(*gateway.patches.lock().expect("lock"), 1);
}

#[tokio::test]
async fn a_site_without_a_gateway_skips_the_roll() {
    let (_ca, store, _now) = site(Span::days(1));
    let rolled = roll_gateway(&store, &Gateway::new(false), &target(), true)
        .await
        .expect("checked");
    assert_eq!(rolled, GatewayRoll::NoGateway);
}

#[tokio::test]
async fn a_failed_first_roll_after_a_rotation_is_retried_on_a_recheck() {
    let (ca, store, now) = site(Span::days(21));
    let gateway = Gateway::new(true);
    let checked = check(&store, &Service::new(&ca), &target(), now).await;
    assert_eq!(rolls_after(&checked), Some(true), "{checked:?}");

    *gateway.fail_next.lock().expect("lock") = true;
    let failed = roll_gateway(&store, &gateway, &target(), true).await;
    assert!(failed.is_err(), "the first patch fails: {failed:?}");
    assert!(gateway.annotation.lock().expect("lock").is_none(), "nothing patched");

    let retried = roll_gateway(&store, &gateway, &target(), false).await.expect("retried");
    assert_eq!(
        retried,
        GatewayRoll::Rolled,
        "a rotated identity is owed its roll on a recheck"
    );
    let renewed = certs::canonical_fingerprint(&store.get("tls.crt").expect("cert")).expect("fingerprint");
    assert_eq!(
        gateway.annotation.lock().expect("lock").as_deref(),
        Some(renewed.as_str()),
        "the retry annotates the gateway with the rotated identity's fingerprint"
    );
}

#[tokio::test]
async fn a_later_check_catches_up_a_missed_roll() {
    let (_ca, store, _now) = site(Span::days(1));
    let gateway = Gateway::new(true);
    let fresh = roll_gateway(&store, &gateway, &target(), false).await.expect("checked");
    assert_eq!(
        fresh,
        GatewayRoll::Unchanged,
        "a recheck leaves a gateway it never rolled"
    );

    *gateway.annotation.lock().expect("lock") = Some("older-leaf".to_owned());
    let caught_up = roll_gateway(&store, &gateway, &target(), false).await.expect("checked");
    assert_eq!(caught_up, GatewayRoll::Rolled, "a gateway left on an older leaf rolls");
}

#[test]
#[expect(clippy::too_many_lines, reason = "one assertion per declared trust")]
fn renewal_follows_the_declared_peer_trust() {
    let network = |trust: Option<&str>| -> GridNetwork {
        let spec = trust.map_or_else(
            || serde_json::json!({ "seeds": [] }),
            |mode| serde_json::json!({ "seeds": [], "peerTrust": { "mode": mode } }),
        );
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "grid" },
            "spec": spec,
        }))
        .expect("network")
    };
    let spiffe = GridModes::WITHOUT_NETWORK;
    let pin_install = GridModes::without_network(None, Some(PeerTrustMode::Pin));
    assert!(
        declared_renews(&[], spiffe),
        "an install declaring nothing trusts by SPIFFE ID"
    );
    assert!(
        !declared_renews(&[], pin_install),
        "a pin install does not renew before its GridNetwork"
    );
    assert!(
        declared_renews(&[network(Some("spiffe"))], pin_install),
        "a declared GridNetwork outranks the install"
    );
    assert!(declared_renews(&[network(Some("spiffe"))], spiffe));
    assert!(
        !declared_renews(&[network(Some("pin"))], spiffe),
        "a pinned peer refuses a renewed leaf"
    );
    assert!(!declared_renews(&[network(None)], spiffe), "pin is the default");
    assert!(
        !declared_renews(&[network(Some("spiffe")), network(Some("spiffe"))], spiffe),
        "several GridNetworks fail closed"
    );
    let (mut last, mut failures) = (None, 0);
    let off = Checked::Off {
        why: Off::UnsupportedUnderPin,
        due: Due::At(OffsetDateTime::now_utc()),
    };
    assert_eq!(settle(&Ok(off), &mut last, &mut failures), CHECK_EVERY);

    let pinned = declared_trust(&[network(Some("pin"))], spiffe);
    assert!(!pinned.renews);
    assert_eq!(pinned.declared, "GridNetwork grid declares peerTrust.mode pin");
    let refusal = pinned.refusal();
    assert!(
        refusal.contains("spec.local.signerEndpoint.rotation is Enabled") && refusal.contains("peerTrust.mode pin"),
        "names both fields: {refusal}"
    );
    assert!(
        declared_trust(&[], pin_install)
            .declared
            .contains("no GridNetwork exists yet"),
        "the install is named while no GridNetwork exists"
    );
}

#[tokio::test(start_paused = true)]
async fn a_declared_trust_change_wakes_the_loop_before_its_next_check() {
    let (declared, changes) = tokio::sync::watch::channel(PeerTrustMode::Spiffe);
    let mut changes = Some(changes);
    let waker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        declared.send_replace(PeerTrustMode::Pin);
        declared
    });
    let started = tokio::time::Instant::now();
    pause(CHECK_EVERY, &mut changes).await;
    assert!(
        started.elapsed() < CHECK_EVERY,
        "woke on the change, not the hourly check"
    );
    assert!(changes.is_some(), "still listening");

    drop(waker.await.expect("sender"));
    pause(CHECK_EVERY, &mut changes).await;
    assert!(changes.is_none(), "a closed channel leaves only the timer");
}

#[test]
fn a_renewal_or_a_recheck_rolls_but_not_an_expiry_or_a_failure() {
    let now = OffsetDateTime::now_utc();
    assert_eq!(rolls_after(&Ok(Checked::Renewed(now))), Some(true));
    assert_eq!(rolls_after(&Ok(Checked::Waiting(now))), Some(false));
    assert_eq!(rolls_after(&Ok(Checked::Expired(now))), None);
    let off = Checked::Off {
        why: Off::UnsupportedUnderPin,
        due: Due::At(now),
    };
    assert_eq!(rolls_after(&Ok(off)), None, "pin trust never rolls");
    assert_eq!(rolls_after(&Err(RenewError::Transport("down".to_owned()))), None);
}

#[test]
fn the_roll_patch_sets_the_identity_annotation_only_when_it_changes() {
    assert!(roll_patch(Some("ab"), "ab", true).is_none());
    assert!(
        roll_patch(None, "ab", false).is_none(),
        "a recheck never starts rolling"
    );
    let patch = roll_patch(Some("old"), "ab", false).expect("patch");
    let set = patch
        .pointer("/spec/template/metadata/annotations")
        .and_then(|annotations| annotations.get(GATEWAY_IDENTITY_ANNOTATION));
    assert_eq!(set, Some(&serde_json::json!("ab")));
    assert!(
        roll_patch(None, "ab", true).is_some(),
        "a renewal rolls a gateway never rolled"
    );
}

fn target() -> Target {
    Target {
        site_secret: "grid-site-identity".to_owned(),
        ca_secret: "grid-site-identity".to_owned(),
    }
}

/// A grid CA, a leaf for `site` valid over `[now - age, now - age + 30d]`, and its store.
fn site(age: Span) -> (certs::CaCert, Memory, OffsetDateTime) {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let now = OffsetDateTime::now_utc();
    let not_before = now.saturating_sub(age);
    let validity = certs::Validity {
        not_before,
        not_after: not_before.saturating_add(Span::days(30)),
    };
    let csr = certs::generate_csr("east").expect("csr");
    let leaf = certs::sign_csr(&ca, "east", &csr.csr_pem, validity).expect("leaf");
    let store = Memory::holding(&leaf.cert_pem, &csr.key_pem, &ca.cert_pem);
    (ca, store, now)
}

#[test]
fn a_third_of_the_lifetime_left_is_due() {
    let start = OffsetDateTime::UNIX_EPOCH;
    let end = start.saturating_add(Span::days(30));
    assert_eq!(
        due(start, end, start.saturating_add(Span::days(1))),
        Due::At(start.saturating_add(Span::days(20)))
    );
    assert_eq!(due(start, end, start.saturating_add(Span::days(20))), Due::Now);
    assert_eq!(due(start, end, start.saturating_add(Span::days(29))), Due::Now);
    assert_eq!(due(start, end, end), Due::Expired);
}

#[tokio::test]
async fn an_identity_in_its_window_is_renewed_in_place() {
    let (ca, store, now) = site(Span::days(21));
    let before = store.get("tls.crt").expect("cert");
    let service = Service::new(&ca);

    let checked = check(&store, &service, &target(), now).await.expect("renewed");
    assert!(matches!(checked, Checked::Renewed(_)), "{checked:?}");
    let after = store.get("tls.crt").expect("cert");
    certs::verify_site_cert(&ca.cert_pem, &after, "east").expect("the same site, from the same CA");
    let key = store.get("tls.key").expect("key");
    assert_eq!(
        certs::cert_public_key_sha256(&after).ok(),
        service.asked().first().cloned(),
        "the stored key is the one the service certified"
    );
    assert!(key.contains("PRIVATE KEY"), "the new key is stored beside it");
    assert_ne!(after, before, "a new leaf");
    assert_eq!(store.get(PREVIOUS_CERT), Some(before), "the replaced leaf is kept");
    assert_eq!(store.get(PENDING_KEY), None, "nothing left in flight");
    assert_eq!(store.get(PENDING_CSR), None);
}

#[tokio::test]
async fn an_identity_not_yet_due_is_left_alone() {
    let (ca, store, now) = site(Span::days(5));
    let service = Service::new(&ca);
    let checked = check(&store, &service, &target(), now).await.expect("checked");
    assert!(matches!(checked, Checked::Waiting(_)), "{checked:?}");
    assert!(service.asked().is_empty(), "no call");
    assert_eq!(store.version(), 1, "no write");
}

#[tokio::test]
async fn an_expired_identity_cannot_renew() {
    let (ca, store, now) = site(Span::days(31));
    let service = Service::new(&ca);
    let checked = check(&store, &service, &target(), now).await.expect("checked");
    assert!(matches!(checked, Checked::Expired(_)), "{checked:?}");
    assert!(service.asked().is_empty(), "an expired leaf is never presented");
}

#[tokio::test]
async fn a_lost_response_retries_with_the_same_key() {
    let (ca, store, now) = site(Span::days(25));
    let service = Service::new(&ca);
    service.answer(Answer::Lose);
    let lost = check(&store, &service, &target(), now).await;
    assert!(matches!(lost, Err(RenewError::Transport(_))), "{lost:?}");
    assert!(
        store.get(PENDING_KEY).is_some(),
        "the key the service may have recorded is kept"
    );

    service.answer(Answer::Sign);
    check(&store, &service, &target(), now).await.expect("retried");
    let asked = service.asked();
    assert_eq!(asked.len(), 2);
    assert_eq!(asked.first(), asked.last(), "the retry asks for the same key");
}

#[tokio::test]
async fn a_refusal_or_a_bad_answer_leaves_the_identity_unchanged() {
    for answer in [Answer::Refuse, Answer::Wrong] {
        let (ca, store, now) = site(Span::days(25));
        let before = store.get("tls.crt");
        let service = Service::new(&ca);
        service.answer(answer);
        let failed = check(&store, &service, &target(), now).await;
        assert!(
            matches!(failed, Err(RenewError::Refused(_) | RenewError::Invalid(_))),
            "{failed:?}"
        );
        assert_eq!(store.get("tls.crt"), before, "the current leaf stays in place");
    }
}

#[tokio::test]
async fn a_renewal_from_another_ca_is_refused() {
    let (ca, store, now) = site(Span::days(25));
    let other = certs::generate_ca("grid-ca").expect("other");
    let service = Service::new(&ca);
    // The service answers with a CA the site does not hold.
    *service.answer.lock().expect("lock") = Answer::Sign;
    let impostor = Service {
        ca: certs::load_ca("grid-ca", &other.key_pem, &other.cert_pem).expect("ca"),
        answer: Mutex::new(Answer::Sign),
        asked: Mutex::new(Vec::new()),
    };
    let refused = check(&store, &impostor, &target(), now).await;
    assert!(
        matches!(refused, Err(RenewError::Unauthenticated(_) | RenewError::Invalid(_))),
        "{refused:?}"
    );
    drop(service);
}

#[test]
fn a_default_leaf_renews_at_day_120() {
    let not_before = OffsetDateTime::UNIX_EPOCH;
    let not_after = not_before.saturating_add(certs::DEFAULT_SITE_CERT_LIFETIME);
    assert_eq!(
        renew_after(not_before, not_after),
        not_before.saturating_add(Span::days(120))
    );
}

#[test]
fn a_renewal_schedules_the_next_from_the_new_leaf() {
    let (mut last, mut failures) = (None, 0);
    let renewed = Ok(Checked::Renewed(OffsetDateTime::now_utc()));
    assert_eq!(settle(&renewed, &mut last, &mut failures), RETRY_INITIAL);
}

#[test]
fn retries_back_off_to_the_cap() {
    assert_eq!(retry_delay(1), RETRY_INITIAL);
    assert_eq!(retry_delay(2), RETRY_INITIAL.saturating_mul(2));
    assert_eq!(retry_delay(30), RETRY_MAX);
    let wait = Duration::from_secs(600);
    let jittered = jittered(wait);
    assert!(jittered >= wait && jittered <= wait.saturating_add(Duration::from_secs(60)));
}

fn grid_site(name: &str, local: Option<serde_json::Value>) -> GridSite {
    let mut spec = serde_json::json!({"gridNetworkRef": "grid"});
    if let (Some(local), Some(object)) = (local, spec.as_object_mut()) {
        object.insert("local".to_owned(), local);
    }
    serde_json::from_value(serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1", "kind": "GridSite", "metadata": {"name": name}, "spec": spec,
    }))
    .expect("site")
}

#[test]
fn spec_local_says_whether_and_where_to_renew() {
    let signer = serde_json::json!({"signerEndpoint": {"url": "https://enroll.example.com"}});
    assert_eq!(declared(None), Declared::NoSigner, "no GridSite");
    assert_eq!(declared(Some(&grid_site("east", None))), Declared::NoSigner, "no local");
    assert_eq!(
        declared(Some(&grid_site("east", Some(serde_json::json!({}))))),
        Declared::NoSigner,
        "local without a signer"
    );
    assert!(
        matches!(
            declared(Some(&grid_site("east", Some(signer)))),
            Declared::Renews(endpoint) if endpoint.url == "https://enroll.example.com"
        ),
        "a declared signer renews by default"
    );
    let disabled = serde_json::json!({"signerEndpoint": {"url": "https://enroll.example.com", "rotation": "Disabled"}});
    assert_eq!(
        declared(Some(&grid_site("east", Some(disabled)))),
        Declared::Disabled,
        "a signer with rotation Disabled enrolls only"
    );
}

/// A client that answers nothing, for resolutions that never reach the API.
fn no_client() -> Client {
    let service = tower::service_fn(|_req: http::Request<kube::client::Body>| async {
        Ok::<_, std::convert::Infallible>(
            http::Response::builder()
                .status(404)
                .body(kube::client::Body::from(Vec::new()))
                .expect("response"),
        )
    });
    Client::new(service, "grid-system")
}

/// Resolve the signer for `site`, off the API.
async fn resolve(site: Option<GridSite>) -> Resolved {
    let own = Own {
        site,
        strays: Vec::new(),
    };
    resolve_signer(&no_client(), &own).await.expect("resolved")
}

#[tokio::test]
async fn only_spec_local_names_the_signer() {
    assert_eq!(
        resolve(None).await,
        Resolved::Off(Off::NoSigner),
        "no GridSite names no signer, whatever the identity Secret is annotated with"
    );
    assert_eq!(
        resolve(Some(grid_site("east", Some(serde_json::json!({}))))).await,
        Resolved::Off(Off::NoSigner),
        "local without a signer"
    );
    let declared = serde_json::json!({"signerEndpoint": {"url": "https://enroll.example.com"}});
    assert_eq!(
        resolve(Some(grid_site("east", Some(declared)))).await,
        Resolved::Signer(Endpoint {
            base: reqwest::Url::parse("https://enroll.example.com").expect("url"),
            pin: None,
        }),
        "a signer without a bundle renews pinned to the held grid CA"
    );
    let disabled = serde_json::json!({"signerEndpoint": {"url": "https://enroll.example.com", "rotation": "Disabled"}});
    assert_eq!(
        resolve(Some(grid_site("east", Some(disabled)))).await,
        Resolved::Off(Off::Disabled),
        "Disabled keeps the signer for enrollment only"
    );
}

/// A client answering every request with `status` and `body`.
fn client_answering(status: u16, body: serde_json::Value) -> Client {
    let service = tower::service_fn(move |_req: http::Request<kube::client::Body>| {
        let body = serde_json::to_vec(&body).expect("body");
        async move {
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(kube::client::Body::from(body))
                    .expect("response"),
            )
        }
    });
    Client::new(service, "grid-system")
}

/// Resolve a declared signer pinned to the Secret `grid-ca-bundle`, read through `client`.
async fn resolve_pinned(client: &Client) -> Result<Resolved, RenewError> {
    let declared = serde_json::json!({
        "signerEndpoint": {"url": "https://enroll.example.com", "caBundleRef": {"name": "grid-ca-bundle", "kind": "Secret"}}
    });
    let own = Own {
        site: Some(grid_site("east", Some(declared))),
        strays: Vec::new(),
    };
    resolve_signer(client, &own).await
}

#[tokio::test]
async fn a_bundle_secret_that_does_not_opt_in_is_material_not_kubernetes() {
    let unlabelled = serde_json::json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": "grid-ca-bundle", "namespace": "grid-system"},
        "data": {"ca.crt": "LS0t"}
    });
    let refused = resolve_pinned(&client_answering(200, unlabelled)).await;
    assert!(
        matches!(
            &refused,
            Err(RenewError::Material(why))
                if why.starts_with("local.signerEndpoint.caBundleRef: ") && why.contains(local::SIGNER_BUNDLE_LABEL)
        ),
        "{refused:?}"
    );
    let failed = resolve_pinned(&client_answering(
        500,
        serde_json::json!({"kind": "Status", "code": 500}),
    ))
    .await;
    assert!(
        matches!(&failed, Err(RenewError::Kube(why)) if why.starts_with("reading Secret grid-ca-bundle")),
        "{failed:?}"
    );
}

/// A check, the signer it went through, and the status and reason it earns.
type Case<'signer> = (
    Result<Checked, RenewError>,
    Option<&'signer String>,
    &'signer str,
    &'signer str,
);

/// The renewal URL of the signer named in `spec.local`.
fn declared_signer() -> String {
    "https://enroll.example.com/v1alpha1/rotations".to_owned()
}

#[test]
fn rotation_on_schedule_follows_the_check() {
    let now = OffsetDateTime::now_utc();
    let named = declared_signer();
    let off = |why: Off, due: Due| Ok(Checked::Off { why, due });
    let cases: [Case<'_>; 8] = [
        (Ok(Checked::Waiting(now)), Some(&named), "True", "AsExpected"),
        (Ok(Checked::Renewed(now)), Some(&named), "True", "AsExpected"),
        (Ok(Checked::Waiting(now)), None, "False", "RenewalFailing"),
        (Ok(Checked::Expired(now)), Some(&named), "False", "RotationOverdue"),
        (
            off(Off::UnsupportedUnderPin, Due::At(now)),
            None,
            "True",
            "RotationDisabled",
        ),
        (off(Off::NoSigner, Due::Now), None, "False", "RotationOverdue"),
        (off(Off::NoSigner, Due::At(now)), None, "True", "RotationDisabled"),
        (off(Off::Disabled, Due::At(now)), None, "True", "RotationDisabled"),
    ];
    for (checked, signer, status, reason) in cases {
        let verdict = rotation_verdict(&checked, signer);
        assert_eq!(
            (verdict.status, verdict.reason.as_ref()),
            (status, reason),
            "{checked:?}"
        );
        let named_in_message =
            verdict.message.contains("/v1alpha1/rotations") && verdict.message.contains("spec.local");
        assert!(reason != "AsExpected" || named_in_message, "{}", verdict.message);
    }
}

#[test]
fn why_nothing_renews_names_the_fix() {
    assert!(Off::NoSigner.message().contains("names no signer"));
    assert!(Off::UnsupportedUnderPin.message().contains("see Accepted"));
    assert!(Off::Disabled.message().contains("rotation is Disabled"));
}

#[test]
fn a_signer_refusal_maps_to_what_renewal_reports() {
    let refused = |status: u16| SignerError::Refused {
        status: StatusCode::from_u16(status).expect("status"),
        code: None,
        detail: status.to_string(),
    };
    assert!(matches!(renew_error(refused(403)), RenewError::Refused(_)));
    assert!(
        matches!(renew_error(refused(409)), RenewError::Transport(_)),
        "only 403 needs re-enrollment"
    );
    assert!(matches!(
        renew_error(SignerError::Disabled("off".to_owned())),
        RenewError::SignerDisabled(_)
    ));
    assert!(matches!(
        renew_error(SignerError::Unauthenticated("401".to_owned())),
        RenewError::Unauthenticated(_)
    ));
}

#[test]
fn a_failed_renewal_says_whether_to_retry_or_re_enroll() {
    let named = declared_signer();
    for (error, reason) in [
        (RenewError::Transport("down".to_owned()), "RenewalFailing"),
        (RenewError::Kube("409".to_owned()), "RenewalFailing"),
        (RenewError::Unauthenticated("401".to_owned()), "RenewalRefused"),
        (RenewError::Refused("frozen".to_owned()), "RenewalRefused"),
        (RenewError::SignerDisabled("off".to_owned()), "RotationOverdue"),
    ] {
        let verdict = rotation_verdict(&Err(error), Some(&named));
        assert_eq!((verdict.status, verdict.reason.as_ref()), ("False", reason));
    }
}

#[test]
fn identity_valid_follows_what_was_read() {
    let now = OffsetDateTime::now_utc();
    for (identity, status, reason) in [
        (Identity::Missing("Secret x".to_owned()), "False", "IdentityMissing"),
        (
            Identity::Unreadable("junk".to_owned()),
            "False",
            "CertificateUnreadable",
        ),
        (Identity::Expired(now), "False", "CertificateExpired"),
        (Identity::Unusable, "False", "CertificateUnusable"),
        (Identity::Valid(now), "True", "AsExpected"),
    ] {
        let verdict = identity_verdict(&identity);
        assert_eq!(
            (verdict.status, verdict.reason.as_ref()),
            (status, reason),
            "{identity:?}"
        );
    }
}

#[test]
fn disabled_rotation_neither_rolls_nor_retries() {
    let disabled = || Checked::Off {
        why: Off::Disabled,
        due: Due::At(OffsetDateTime::now_utc()),
    };
    assert_eq!(rolls_after(&Ok(disabled())), None, "Disabled never rolls");
    let (mut last, mut failures) = (None, 0);
    assert_eq!(settle(&Ok(disabled()), &mut last, &mut failures), CHECK_EVERY);
    assert!(Off::Disabled.message().contains("Disabled"));
}

/// The verdicts of `round` for condition `type_`, as (status, reason).
fn verdicts_of(round: &Round, type_: &str) -> Vec<(&'static str, String)> {
    round_verdicts(round, &target(), true)
        .into_iter()
        .filter(|(written, ..)| *written == type_)
        .map(|(_, _, verdict)| (verdict.status, verdict.reason.into_owned()))
        .collect()
}

/// A round in which nothing renews for `why`, with the identity valid.
fn off_round(why: Off, refused: Option<String>) -> Round {
    let now = OffsetDateTime::now_utc();
    Round {
        checked: Ok(Checked::Off { why, due: Due::At(now) }),
        signer: None,
        identity: Some(Identity::Valid(now)),
        refused,
    }
}

#[test]
fn rotation_enabled_under_pin_is_an_accepted_conflict_and_holds_renewal_back() {
    let trust = Trust {
        renews: false,
        declared: "GridNetwork grid declares peerTrust.mode pin".to_owned(),
    };
    let refused = off_round(Off::UnsupportedUnderPin, Some(trust.refusal()));
    assert_eq!(
        verdicts_of(&refused, local::ACCEPTED),
        vec![("False", "RotationUnsupportedUnderPinTrust".to_owned())]
    );
    assert_eq!(
        verdicts_of(&refused, local::ROTATION_ON_SCHEDULE),
        vec![("True", "RotationDisabled".to_owned())],
        "the rotation reason set stays closed; the conflict is on Accepted"
    );
    let (manager, message) = round_verdicts(&refused, &target(), true)
        .into_iter()
        .find(|(type_, ..)| *type_ == local::ACCEPTED)
        .map(|(_, manager, verdict)| (manager, verdict.message))
        .expect("Accepted");
    assert_eq!(
        manager,
        local::ACCEPTED_FIELD_MANAGER,
        "the same manager as the enrollment step"
    );
    assert!(
        message.contains("spec.local.signerEndpoint.rotation is Enabled") && message.contains("peerTrust.mode pin"),
        "{message}"
    );
}

#[test]
fn disabled_rotation_and_no_signer_are_accepted() {
    for why in [Off::Disabled, Off::NoSigner] {
        assert_eq!(
            verdicts_of(&off_round(why, None), local::ACCEPTED),
            vec![("True", "AsExpected".to_owned())],
            "{why:?}"
        );
    }
}

#[test]
fn an_enrolled_site_keeps_issued_while_the_identity_is_present() {
    let now = OffsetDateTime::now_utc();
    let round = Round {
        checked: Ok(Checked::Waiting(now)),
        signer: None,
        identity: Some(Identity::Valid(now)),
        refused: None,
    };
    let written = |enrolled: bool| -> Vec<String> {
        round_verdicts(&round, &target(), enrolled)
            .into_iter()
            .filter(|(type_, ..)| *type_ == local::ENROLLED)
            .map(|(_, _, verdict)| verdict.reason.into_owned())
            .collect()
    };
    assert!(written(true).is_empty(), "an enrolled site keeps Issued");
    assert_eq!(written(false), vec!["IdentityPresent".to_owned()]);
}
