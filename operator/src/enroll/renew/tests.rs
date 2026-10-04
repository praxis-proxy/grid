use std::sync::Mutex;

use time::Duration as Span;

use super::*;

/// The identity Secret in memory, with a version a write must match.
struct Memory(Mutex<(BTreeMap<String, String>, u64)>);

impl Memory {
    fn holding(cert: &str, key: &str, ca: &str) -> Self {
        let data = BTreeMap::from([
            ("tls.crt".to_owned(), cert.to_owned()),
            ("tls.key".to_owned(), key.to_owned()),
            ("ca.crt".to_owned(), ca.to_owned()),
        ]);
        Self(Mutex::new((data, 1)))
    }

    fn get(&self, key: &str) -> Option<String> {
        self.0.lock().expect("lock").0.get(key).cloned()
    }

    fn version(&self) -> u64 {
        self.0.lock().expect("lock").1
    }
}

impl IdentityStore for Memory {
    async fn read(&self, _target: &Target) -> Result<Held, RenewError> {
        let (data, version) = &*self.0.lock().expect("lock");
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
        let (held, current) = &mut *self.0.lock().expect("lock");
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

impl Renewer for Service {
    async fn renew(&self, held: &Held, csr_pem: &str) -> Result<Enrollment, RenewError> {
        let site = site_of(&held.cert)?;
        certs::verify_site_cert(&self.ca.cert_pem, &held.cert, &site)
            .map_err(|e| RenewError::Unauthenticated(e.to_string()))?;
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
        match answer {
            Answer::Refuse => return Err(RenewError::Refused("403".to_owned())),
            Answer::Sign | Answer::Lose | Answer::Wrong => {},
        }
        let cert = certs::sign_csr(&self.ca, name, csr_pem, certs::Validity::default()).expect("sign");
        if answer == Answer::Lose {
            return Err(RenewError::Transport("connection reset".to_owned()));
        }
        Ok(Enrollment {
            certificate: cert.cert_pem,
            ca_certificate: self.ca.cert_pem.clone(),
        })
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
    assert_eq!(settle(&Ok(Checked::Off), &mut last, &mut failures), CHECK_EVERY);
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
    assert_eq!(rolls_after(&Ok(Checked::Off)), None, "pin trust never rolls");
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

#[test]
fn renewal_refuses_an_enrollment_url_a_site_certificate_could_answer() {
    let config = |url: &str| super::super::Config {
        enabled: false,
        url: Some(url.to_owned()),
        ca_file: None,
        grid_ca_file: None,
        site_name: None,
        token_secret: None,
        token_secret_key: "token".to_owned(),
        identity_secret: "grid-site-identity".to_owned(),
        ca_secret: "grid-ca".to_owned(),
        renew: true,
    };
    drop(Settings::from_config(&config("https://grid-enrollment.grid-enrollment.svc:8443")).expect("in-cluster URL"));
    for url in [
        "https://hub.grid.internal:8443",
        "https://HUB.GRID.INTERNAL.:8443",
        "https://grid.internal",
        "http://grid-enrollment.svc",
    ] {
        assert!(Settings::from_config(&config(url)).is_err(), "{url}");
    }
}
