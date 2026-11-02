use std::sync::{Arc, Mutex};

use reqwest::StatusCode;
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
            renewer: None,
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
    SignerError::Forbidden("403 identity_refused".to_owned())
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
        due: Due::At(OffsetDateTime::now_utc().saturating_add(Span::days(2))),
        renewer: None,
    };
    assert_eq!(settle(&Ok(off), &mut last, &mut failures), CHECK_EVERY);

    let pinned = declared_trust(&[network(Some("pin"))], spiffe);
    assert!(!pinned.renews);
    assert_eq!(pinned.declared, "GridNetwork grid declares peerTrust.mode pin");
    let refusal = pinned.refusal();
    assert!(
        refusal.message.contains("site.signerEndpoint.rotation is Enabled")
            && refusal.message.contains("peerTrust.mode pin"),
        "names both fields: {}",
        refusal.message
    );
    assert_eq!(refusal.reason, "RotationUnsupportedUnderPinTrust");
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
    let mut config = ConfigWatch::fixed(Snapshot::absent());
    let started = tokio::time::Instant::now();
    pause(CHECK_EVERY, &mut changes, &mut config).await;
    assert!(
        started.elapsed() < CHECK_EVERY,
        "woke on the change, not the hourly check"
    );
    assert!(changes.is_some(), "still listening");

    drop(waker.await.expect("sender"));
    pause(CHECK_EVERY, &mut changes, &mut config).await;
    assert!(changes.is_none(), "a closed channel leaves only the timer");
}

#[tokio::test(start_paused = true)]
async fn an_operator_config_edit_wakes_the_loop_before_its_next_check() {
    let (edits, watched) = tokio::sync::watch::channel(Arc::new(Snapshot::absent()));
    let mut config = ConfigWatch::from_receiver(watched);
    let editor = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        edits.send_replace(Arc::new(Snapshot::of_config(
            crate::operator_config::OperatorConfig::default(),
        )));
        edits
    });
    let started = tokio::time::Instant::now();
    pause(CHECK_EVERY, &mut None, &mut config).await;
    assert!(
        started.elapsed() < CHECK_EVERY,
        "woke on the edit, not the hourly check"
    );
    drop(editor.await.expect("sender"));
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
        renewer: None,
    };
    assert_eq!(rolls_after(&Ok(off)), None, "pin trust never rolls");
    assert_eq!(rolls_after(&Err(RenewError::Transport("down".to_owned()))), None);
}

#[test]
fn a_gateway_follows_an_identity_cert_manager_renews_and_a_reissue_is_owed_a_roll() {
    let off = |why, renewer: Option<&str>| Checked::Off {
        why,
        due: Due::At(OffsetDateTime::now_utc()),
        renewer: renewer.map(str::to_owned),
    };
    let cert_manager = Some("site-d-identity");
    assert_eq!(rolls_after(&Ok(off(Off::NoSigner, cert_manager))), Some(false));
    assert_eq!(rolls_after(&Ok(off(Off::Disabled, cert_manager))), Some(false));
    assert_eq!(rolls_after(&Ok(off(Off::NoSigner, None))), None, "nothing renews it");
    assert_eq!(
        rolls_after(&Ok(off(Off::UnsupportedUnderPin, cert_manager))),
        None,
        "pin trust never rolls"
    );
    assert_eq!(rolls_after(&Ok(off(Off::Refused, cert_manager))), None);

    let revision = |value: &str| kube::api::ObjectMeta {
        annotations: Some(BTreeMap::from([(CERT_MANAGER_REVISION.to_owned(), value.to_owned())])),
        ..kube::api::ObjectMeta::default()
    };
    assert!(
        !reissued(&revision("1")),
        "a first issuance leaves a fresh install running"
    );
    assert!(reissued(&revision("2")), "a reissue is owed a roll");
    assert!(!reissued(&revision("x")) && !reissued(&kube::api::ObjectMeta::default()));
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

/// A signer that never answers.
struct Silent;

impl Signer for Silent {
    async fn enroll(&self, _credential: &str, _csr_pem: &str) -> Result<Issued, SignerError> {
        std::future::pending().await
    }

    async fn renew(&self, _held: &Held, _csr_pem: &str) -> Result<Issued, SignerError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn a_stand_down_gives_up_the_request_and_keeps_the_identity_and_the_kept_key() {
    let (_ca, store, now) = site(Span::days(25));
    let before = store.get("tls.crt");
    let (trigger, stop) = crate::shutdown::Trigger::new();
    trigger.trigger();
    let held = store.read(&target()).await.expect("held");
    let given_up = check_held(&store, &Silent, &target(), held, (now, Some(&stop))).await;
    assert!(matches!(given_up, Err(RenewError::StoodDown)), "{given_up:?}");
    assert_eq!(
        store.get("tls.crt"),
        before,
        "nothing is written once the request is given up"
    );
    assert!(
        store.get(PENDING_KEY).is_some(),
        "the kept key lets the next start resend it"
    );
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

/// The operator config `document` as loaded, naming site `east`.
fn loaded(document: &str) -> Loaded {
    match crate::operator_config::parse(&format!("site:\n  name: east\n{document}")) {
        Ok(parsed) => Loaded::Valid(Box::new(parsed.config)),
        Err(why) => Loaded::Invalid(why),
    }
}

/// A signer at `https://enroll.example.com`, with `extra` lines under it.
fn signer(extra: &str) -> String {
    format!("  signerEndpoint:\n    url: https://enroll.example.com\n{extra}")
}

#[test]
fn the_operator_config_says_whether_and_where_to_renew() {
    assert_eq!(declared(&Loaded::Absent, Some("east")), Declared::NoSigner, "no config");
    assert_eq!(
        declared(&loaded(""), Some("east")),
        Declared::NoSigner,
        "a site without a signer"
    );
    assert!(
        matches!(
            declared(&loaded(&signer("")), Some("east")),
            Declared::Renews(endpoint) if endpoint.url == "https://enroll.example.com"
        ),
        "a declared signer renews by default"
    );
    assert_eq!(
        declared(&loaded(&signer("    rotation: Disabled\n")), Some("east")),
        Declared::Disabled,
        "a signer with rotation Disabled enrolls only"
    );
}

#[test]
fn a_refused_operator_config_renews_through_nothing() {
    assert!(
        matches!(
            declared(&Loaded::Invalid("site.name: bad".to_owned()), Some("east")),
            Declared::Refused(refusal) if refusal.reason == "OperatorConfigInvalid"
        ),
        "a refused config renews through nothing"
    );
    assert!(
        matches!(
            declared(&loaded(&signer("")), Some("west")),
            Declared::Refused(refusal) if refusal.reason == "SiteNameMismatch" && refusal.message.contains("west")
        ),
        "a config naming another site than the identity is refused"
    );
}

/// Resolve the signer `loaded` names for site `east`.
fn resolve(loaded: &Loaded) -> Resolved<'_> {
    resolve_signer(&declared(loaded, Some("east"))).expect("resolved")
}

#[test]
fn only_the_operator_config_names_the_signer() {
    assert_eq!(
        resolve(&Loaded::Absent),
        Resolved::Off(Off::NoSigner),
        "no config names no signer, whatever the identity Secret is annotated with"
    );
    assert_eq!(
        resolve(&loaded("")),
        Resolved::Off(Off::NoSigner),
        "a site without a signer"
    );
    assert_eq!(
        resolve(&loaded(&signer(""))),
        Resolved::Signer(Endpoint {
            base: SignerUrl::parse("https://enroll.example.com").expect("url"),
            pin: None,
        }),
        "a signer renews trusted through the held grid CA"
    );
    assert_eq!(
        resolve(&loaded(&signer("    rotation: Disabled\n"))),
        Resolved::Off(Off::Disabled),
        "Disabled keeps the signer for enrollment only"
    );
    assert_eq!(
        resolve(&Loaded::Invalid("site.name: bad".to_owned())),
        Resolved::Off(Off::Refused),
        "a refused config is not acted on"
    );
}

/// An identity for `east` from a fresh grid CA.
fn held_identity() -> Held {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let leaf = certs::generate_site_cert(&ca, "east").expect("leaf");
    Held {
        cert: leaf.cert_pem,
        key: Zeroizing::new(leaf.key_pem),
        ca: ca.cert_pem,
        pending: None,
        version: "1".to_owned(),
        rotated: false,
        renewer: None,
    }
}

#[tokio::test]
async fn renewal_trusts_the_pin_beside_the_held_grid_ca() {
    crate::init_process_crypto();
    let held = held_identity();
    let (base, pin) = super::super::tests::byo_serving_signer(&held.ca, axum::Router::new(), false).await;
    let base = SignerUrl::parse(base.as_str()).expect("signer url");
    let clients = Clients::default();

    let unpinned = HttpsSigner::new(&base, None, &clients).renew(&held, "csr").await;
    let unpinned = unpinned.err().map(renew_error);
    assert!(
        matches!(unpinned, Some(RenewError::Untrusted(_))),
        "the held grid CA alone does not trust a serving certificate it did not issue: {unpinned:?}"
    );
    let reason = rotation_verdict(&Err(unpinned.expect("refused")), None).reason;
    assert_eq!(
        reason, "SignerUntrusted",
        "an untrusted signer is not a transport failure"
    );

    let pinned = HttpsSigner::new(&base, Some(&pin), &clients).renew(&held, "csr").await;
    assert!(
        matches!(&pinned, Err(SignerError::Refused(detail)) if detail.starts_with("404")),
        "the pin trusts the signer's serving certificate, so the request reaches it"
    );
}

/// A check, the signer it went through, and the status and reason it earns.
type Case<'signer> = (
    Result<Checked, RenewError>,
    Option<&'signer String>,
    &'signer str,
    &'signer str,
);

/// The renewal URL of the signer the operator config names.
fn declared_signer() -> String {
    "https://enroll.example.com/v1alpha1/rotations".to_owned()
}

#[test]
fn rotation_on_schedule_follows_the_check() {
    let now = OffsetDateTime::now_utc();
    let named = declared_signer();
    let off = |why: Off, due: Due| {
        Ok(Checked::Off {
            why,
            due,
            renewer: None,
        })
    };
    let cases: [Case<'_>; 10] = [
        (Ok(Checked::Waiting(now)), Some(&named), "True", "RotationOnSchedule"),
        (Ok(Checked::Renewed(now)), Some(&named), "True", "RotationOnSchedule"),
        (Ok(Checked::Waiting(now)), None, "False", "RenewalFailing"),
        (Ok(Checked::Expired(now)), Some(&named), "False", "RotationOverdue"),
        (
            off(Off::UnsupportedUnderPin, Due::At(now)),
            None,
            "True",
            "RotationOnSchedule",
        ),
        (off(Off::NoSigner, Due::Now), None, "False", "RotationOverdue"),
        (off(Off::NoSigner, Due::At(now)), None, "True", "RotationOnSchedule"),
        (off(Off::Disabled, Due::At(now)), None, "True", "RotationOnSchedule"),
        (off(Off::Refused, Due::At(now)), None, "Unknown", "NotEvaluated"),
        (off(Off::Refused, Due::Now), None, "False", "RotationOverdue"),
    ];
    for (checked, signer, status, reason) in cases {
        let verdict = rotation_verdict(&checked, signer);
        assert_eq!(
            (verdict.status.as_str(), verdict.reason.as_ref()),
            (status, reason),
            "{checked:?}"
        );
        let named_in_message =
            verdict.message.contains("/v1alpha1/rotations") && verdict.message.contains("site.signerEndpoint");
        assert!(
            signer.is_none() || reason != "RotationOnSchedule" || named_in_message,
            "{}",
            verdict.message
        );
    }
}

#[test]
fn a_cert_manager_certificate_on_the_secret_reads_as_renewed_until_due() {
    let now = OffsetDateTime::now_utc();
    let named = Some("site-d-identity");
    for why in [Off::NoSigner, Off::Disabled] {
        let held = off_verdict(why, Due::At(now), named);
        assert_eq!(
            (held.status.as_str(), held.reason.as_ref(), held.message.as_str()),
            (
                "True",
                "RotationOnSchedule",
                "renewed by cert-manager Certificate site-d-identity"
            ),
            "{why:?}"
        );
        let unknown = off_verdict(why, Due::At(now), None);
        assert_eq!(
            (unknown.status.as_str(), unknown.reason.as_ref()),
            ("True", "RotationOnSchedule")
        );
        assert!(
            unknown
                .message
                .ends_with("nothing is known to renew this identity; it is kept until it expires"),
            "{}",
            unknown.message
        );
    }
    let held_back = off_verdict(Off::Refused, Due::At(now), named);
    assert_eq!(
        held_back.reason, "NotEvaluated",
        "a refused config is never read as renewed"
    );
}

#[test]
fn a_due_identity_its_certificate_has_not_replaced_is_overdue() {
    for why in [Off::NoSigner, Off::Disabled] {
        for due in [Due::Now, Due::Expired] {
            let overdue = off_verdict(why, due, Some("site-d-identity"));
            assert_eq!(
                (overdue.status.as_str(), overdue.reason.as_ref()),
                ("False", "RotationOverdue"),
                "{why:?}/{due:?} with a renewer names the certificate"
            );
            assert!(overdue.message.contains("site-d-identity"), "{}", overdue.message);
            assert_eq!(
                off_verdict(why, due, None).reason,
                "RotationOverdue",
                "{why:?}/{due:?} without a renewer is still overdue"
            );
        }
    }
}

#[test]
fn the_renewer_is_the_annotated_certificate() {
    let annotated = |value: &str| kube::api::ObjectMeta {
        annotations: Some(BTreeMap::from([(
            CERT_MANAGER_CERTIFICATE.to_owned(),
            value.to_owned(),
        )])),
        ..kube::api::ObjectMeta::default()
    };
    assert_eq!(
        renewer_of(&annotated("site-d-identity")).as_deref(),
        Some("site-d-identity")
    );
    assert_eq!(renewer_of(&annotated("")), None);
    assert_eq!(renewer_of(&kube::api::ObjectMeta::default()), None);
}

#[test]
fn a_401_for_a_missing_client_certificate_says_renewal_needs_passthrough() {
    let body = br#"{"error":"identity_required","message":"rotation requires the site's current grid certificate over mutual TLS"}"#;
    let refused = super::super::signer::classify(StatusCode::UNAUTHORIZED, body)
        .err()
        .map(renew_error);
    assert!(matches!(refused, Some(RenewError::NeedsPassthrough(_))), "{refused:?}");
    let verdict = rotation_verdict(&Err(refused.expect("refused")), None);
    assert_eq!(verdict.reason, "RenewalNeedsPassthrough");
    assert!(verdict.message.contains("passthrough Route"), "{}", verdict.message);
    assert!(
        !verdict.message.contains("re-enroll"),
        "re-enrolling does not fix it: {}",
        verdict.message
    );
    let invalid = br#"{"error":"invalid_token","message":"a valid site token is required"}"#;
    assert!(matches!(
        super::super::signer::classify(StatusCode::UNAUTHORIZED, invalid)
            .err()
            .map(renew_error),
        Some(RenewError::Unauthenticated(_))
    ));
}

#[test]
fn why_nothing_renews_names_the_fix() {
    assert!(Off::NoSigner.message().contains("names no site.signerEndpoint"));
    assert!(Off::Refused.message().contains("see Accepted"));
    assert!(Off::UnsupportedUnderPin.message().contains("see Accepted"));
    assert!(Off::Disabled.message().contains("rotation is Disabled"));
}

#[test]
fn a_signer_refusal_maps_to_what_renewal_reports() {
    assert!(matches!(
        renew_error(SignerError::Forbidden("403".to_owned())),
        RenewError::Refused(_)
    ));
    assert!(
        matches!(
            renew_error(SignerError::Refused("409".to_owned())),
            RenewError::Transport(_)
        ),
        "only a forbidden identity needs re-enrollment"
    );
    assert!(
        matches!(
            renew_error(SignerError::NameTaken("409".to_owned())),
            RenewError::Transport(_)
        ),
        "an enrollment-only refusal is not a renewal verdict"
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
        assert_eq!(
            (verdict.status.as_str(), verdict.reason.as_ref()),
            ("False", reason),
            "expected reason {reason}"
        );
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
        (Identity::Valid(now, String::new()), "True", "IdentityValid"),
    ] {
        let verdict = identity_verdict(&identity);
        assert_eq!(
            (verdict.status.as_str(), verdict.reason.as_ref()),
            (status, reason),
            "{identity:?}"
        );
    }
}

#[test]
fn disabled_rotation_neither_rolls_nor_retries() {
    let disabled = || Checked::Off {
        why: Off::Disabled,
        due: Due::At(OffsetDateTime::now_utc().saturating_add(Span::days(2))),
        renewer: None,
    };
    assert_eq!(rolls_after(&Ok(disabled())), None, "Disabled never rolls");
    let (mut last, mut failures) = (None, 0);
    assert_eq!(settle(&Ok(disabled()), &mut last, &mut failures), CHECK_EVERY);
    let soon = Checked::Off {
        why: Off::Disabled,
        due: Due::At(OffsetDateTime::now_utc().saturating_add(Span::minutes(10))),
        renewer: None,
    };
    let wait = settle(&Ok(soon), &mut last, &mut failures);
    assert!(
        wait <= Duration::from_secs(600) && wait > Duration::from_secs(590),
        "checked again when renewal falls due, so an overdue identity is reported then: {wait:?}"
    );
    assert!(Off::Disabled.message().contains("Disabled"));
}

/// The verdicts of `round` for condition `type_`, as (status, reason).
fn verdicts_of(round: &Round, type_: &str) -> Vec<(&'static str, String)> {
    round_verdicts(round, &target(), true, (Verdict::own_site(), None))
        .into_iter()
        .filter(|(written, ..)| *written == type_)
        .map(|(_, _, verdict)| (verdict.status.as_str(), verdict.reason.into_owned()))
        .collect()
}

/// A round in which nothing renews for `why`, with the identity valid.
fn off_round(why: Off) -> Round {
    let now = OffsetDateTime::now_utc();
    Round {
        checked: Ok(Checked::Off {
            why,
            due: Due::At(now),
            renewer: None,
        }),
        signer: None,
        identity: Some(Identity::Valid(now, String::new())),
    }
}

/// Peer trust that does or does not renew.
fn trust(renews: bool) -> Trust {
    Trust {
        renews,
        declared: "GridNetwork grid declares peerTrust.mode pin".to_owned(),
    }
}

#[test]
fn rotation_enabled_under_pin_is_an_accepted_conflict_and_holds_renewal_back() {
    let refused = accepted(&loaded(&signer("")), Some("east"), (&[], &trust(false)));
    assert_eq!(
        (refused.status.as_str(), refused.reason.as_ref()),
        ("False", "RotationUnsupportedUnderPinTrust")
    );
    assert!(
        refused.message.contains("site.signerEndpoint.rotation is Enabled")
            && refused.message.contains("peerTrust.mode pin"),
        "{}",
        refused.message
    );
    assert_eq!(
        verdicts_of(&off_round(Off::UnsupportedUnderPin), local::ROTATION_ON_SCHEDULE),
        vec![("True", local::ROTATION_ON_SCHEDULE.to_owned())],
        "the rotation reason set stays closed; the conflict is on Accepted"
    );
    let accepted_once = accepted(&loaded(&signer("")), Some("east"), (&[], &trust(true)));
    assert_eq!(
        accepted_once.status.as_str(),
        "True",
        "spiffe trust renews, so nothing conflicts"
    );
}

#[test]
fn disabled_rotation_no_signer_and_no_config_are_accepted() {
    for document in [signer("    rotation: Disabled\n"), String::new()] {
        let verdict = accepted(&loaded(&document), Some("east"), (&[], &trust(false)));
        assert_eq!(
            (verdict.status.as_str(), verdict.reason.as_ref()),
            ("True", local::ACCEPTED),
            "{document}"
        );
    }
    let off = accepted(&Loaded::Absent, Some("east"), (&[], &trust(false)));
    assert_eq!(
        (off.status.as_str(), off.message.as_str()),
        ("True", "no operator config, enrollment off")
    );
}

#[test]
fn every_install_waits_for_a_declared_grid_id() {
    let unset: GridNetwork = serde_json::from_value(serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1", "kind": "GridNetwork",
        "metadata": { "name": "grid" }, "spec": {}
    }))
    .expect("network");
    let named = GridNetwork {
        spec: crate::crd::grid_network::GridNetworkSpec {
            grid_id: "dagobah".to_owned(),
            ..unset.spec.clone()
        },
        ..unset.clone()
    };
    let verdict = accepted(&loaded(""), Some("east"), (std::slice::from_ref(&unset), &trust(true)));
    assert_eq!(
        (verdict.status.as_str(), verdict.reason.as_ref()),
        ("False", "GridIdUnset")
    );
    assert!(
        verdict.message.contains("spec.gridId") && verdict.message.contains("GridNetwork grid"),
        "{}",
        verdict.message
    );
    let joined = accepted(&loaded(""), Some("east"), (std::slice::from_ref(&named), &trust(true)));
    assert_eq!(joined.status.as_str(), "True");
    let standalone = accepted(&Loaded::Absent, None, (std::slice::from_ref(&unset), &trust(true)));
    assert_eq!(
        (standalone.status, standalone.reason.as_ref()),
        (ConditionStatus::False, "GridIdUnset"),
        "a standalone operator also waits for bootstrap"
    );
}

#[test]
fn a_refused_config_is_not_accepted_and_holds_renewal_back() {
    let invalid = accepted(
        &Loaded::Invalid("site.name: bad".to_owned()),
        Some("east"),
        (&[], &trust(true)),
    );
    assert_eq!(
        (invalid.status.as_str(), invalid.reason.as_ref()),
        ("False", "OperatorConfigInvalid")
    );
    let mismatch = accepted(&loaded(""), Some("west"), (&[], &trust(true)));
    assert_eq!(
        (mismatch.status.as_str(), mismatch.reason.as_ref()),
        ("False", "SiteNameMismatch")
    );
    assert_eq!(
        verdicts_of(&off_round(Off::Refused), local::ROTATION_ON_SCHEDULE),
        vec![("Unknown", "NotEvaluated".to_owned())],
        "renewal is held back, and not evaluated, until the config is fixed"
    );
}

#[test]
fn a_round_that_never_read_the_identity_says_nothing_about_it() {
    let failed = Round::failed(RenewError::Kube("apiserver unavailable".to_owned()));
    let written: Vec<&str> = round_verdicts(
        &failed,
        &target(),
        false,
        (Verdict::own_site(), Some(Verdict::resolved())),
    )
    .into_iter()
    .map(|(type_, ..)| type_)
    .collect();
    assert_eq!(
        written,
        [local::ACCEPTED, local::RESOLVED_REFS],
        "no Enrolled, IdentityValid or RotationOnSchedule from a failed read"
    );
}

#[test]
fn an_identity_gone_reads_enrolled_false_and_one_written_outside_a_signer_names_its_renewer() {
    let now = OffsetDateTime::now_utc();
    let gone = Round {
        identity: Some(Identity::Missing("Secret site-tls".to_owned())),
        ..Round::failed(RenewError::Missing(String::new()))
    };
    assert_eq!(
        verdicts_of(&gone, local::ENROLLED),
        vec![("False", "IdentityMissing".to_owned())]
    );
    let outside = Round {
        checked: Ok(Checked::Off {
            why: Off::NoSigner,
            due: Due::At(now),
            renewer: Some("site-d-identity".to_owned()),
        }),
        signer: None,
        identity: Some(Identity::Valid(now, String::new())),
    };
    let message = round_verdicts(&outside, &target(), true, (Verdict::own_site(), None))
        .into_iter()
        .find(|(type_, ..)| *type_ == local::ENROLLED)
        .map(|(.., verdict)| verdict.message)
        .expect("Enrolled says what renews it, even once enrolled");
    assert_eq!(
        message,
        "identity provisioned outside the signer, renewed by cert-manager Certificate site-d-identity"
    );
}

#[test]
fn an_enrolled_site_keeps_its_issuance_message_while_the_identity_is_present() {
    let now = OffsetDateTime::now_utc();
    let round = Round {
        checked: Ok(Checked::Waiting(now)),
        signer: None,
        identity: Some(Identity::Valid(now, String::new())),
    };
    let written = |enrolled: bool| -> Vec<String> {
        round_verdicts(&round, &target(), enrolled, (Verdict::own_site(), None))
            .into_iter()
            .filter(|(type_, ..)| *type_ == local::ENROLLED)
            .map(|(_, _, verdict)| verdict.reason.into_owned())
            .collect()
    };
    assert!(written(true).is_empty(), "an enrolled site keeps its issuance message");
    assert_eq!(written(false), vec![local::ENROLLED.to_owned()]);
}

#[test]
fn an_external_renewer_has_a_grace_past_due_before_it_reads_overdue() {
    let renew_at = OffsetDateTime::now_utc();
    let within = renew_at.saturating_add(Span::minutes(5));
    let past = renew_at.saturating_add(EXTERNAL_GRACE).saturating_add(Span::minutes(1));
    assert_eq!(
        external_due(Due::Now, renew_at, true, within),
        Due::At(renew_at.saturating_add(EXTERNAL_GRACE)),
        "cert-manager renews at the same point, so it has the grace to replace the identity"
    );
    assert_eq!(
        external_due(Due::Now, renew_at, true, past),
        Due::Now,
        "then it is overdue"
    );
    assert_eq!(
        external_due(Due::Now, renew_at, false, within),
        Due::Now,
        "no renewer named, no grace"
    );
    assert_eq!(external_due(Due::Expired, renew_at, true, within), Due::Expired);
}
