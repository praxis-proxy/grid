use std::collections::BTreeMap;

use kube::api::ObjectMeta;

use super::*;

const SITE: &str = "site-d";

/// A document naming `site-d` with a signer and an invite.
const FULL: &str = "
site:
  name: site-d
  signerEndpoint:
    url: https://grid-enrollment.grid-enrollment.svc:8443
    caBundleRef: {kind: Secret, name: grid-ca-bundle}
  inviteSecretRef:
    name: grid-invite-site-d
";

fn map(text: Option<&str>) -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(DEFAULT_CONFIG_MAP.to_owned()),
            namespace: Some("grid-system".to_owned()),
            uid: Some("uid-config".to_owned()),
            ..ObjectMeta::default()
        },
        data: text.map(|text| BTreeMap::from([(CONFIG_KEY.to_owned(), text.to_owned())])),
        ..ConfigMap::default()
    }
}

fn refused(text: &str) -> String {
    parse(text).expect_err("refused")
}

#[test]
fn a_full_site_section_parses_with_its_defaults() {
    let parsed = parse(FULL).expect("parses");
    assert!(parsed.ignored.is_empty());
    let site = parsed.config.site.expect("site");
    assert_eq!(site.name, SITE);
    let signer = site.signer_endpoint.expect("signer");
    assert_eq!(signer.rotation, Rotation::Enabled, "rotation defaults to Enabled");
    let pin = signer.ca_bundle_ref.expect("pin");
    assert_eq!((pin.kind, pin.key.as_str()), (BundleKind::Secret, "ca.crt"));
    assert_eq!(site.invite_secret_ref.expect("invite").key, "token");
}

#[test]
fn every_section_is_optional() {
    for empty in ["", "   \n", "# nothing set\n", "{}", "version: 1\n"] {
        assert_eq!(parse(empty), Ok(Parsed::default()), "{empty:?}");
    }
    let named_only = parse("site:\n  name: site-d\n").expect("parses");
    assert_eq!(named_only.config.site.map(|site| site.signer_endpoint), Some(None));
}

#[test]
fn an_unknown_key_inside_a_section_is_refused_with_its_path() {
    for (text, names) in [
        ("site:\n  name: site-d\n  sigerEndpoint: {}\n", "sigerEndpoint"),
        (
            "site:\n  name: site-d\n  signerEndpoint:\n    url: https://signer.example.com\n    rotate: Enabled\n",
            "site.signerEndpoint",
        ),
    ] {
        let why = refused(text);
        assert!(why.contains(names), "{text:?}: {why}");
    }
}

#[test]
#[expect(clippy::too_many_lines, reason = "one case per refused field")]
fn a_refused_field_is_named_in_the_message() {
    let cases = [
        ("site: {}\n", "name"),
        ("site:\n  name: Not_A_Site\n", "site.name"),
        (
            "site:\n  name: site-d\n  signerEndpoint:\n    url: http://signer.example.com\n",
            "site.signerEndpoint.url must be an https URL",
        ),
        (
            "site:\n  name: site-d\n  signerEndpoint:\n    url: https://site-a.grid.internal\n",
            "site.signerEndpoint.url must not name a host under grid.internal",
        ),
        (
            "site:\n  name: site-d\n  signerEndpoint:\n    url: https://signer.example.com\n    rotation: Sometimes\n",
            "rotation",
        ),
        (
            "site:\n  name: site-d\n  signerEndpoint:\n    url: https://signer.example.com\n    caBundleRef: {name: grid-ca}\n",
            "kind",
        ),
        (
            "site:\n  name: site-d\n  signerEndpoint:\n    url: https://signer.example.com\n    caBundleRef: {kind: Secret, name: ''}\n",
            "site.signerEndpoint.caBundleRef.name: must not be empty",
        ),
        (
            "site:\n  name: site-d\n  inviteSecretRef: {name: grid-invite-site-d}\n",
            "site.inviteSecretRef needs site.signerEndpoint with a caBundleRef",
        ),
        (
            "site:\n  name: site-d\n  signerEndpoint:\n    url: https://signer.example.com\n  inviteSecretRef: {name: grid-invite-site-d}\n",
            "site.inviteSecretRef needs site.signerEndpoint with a caBundleRef",
        ),
        ("site: [", ""),
    ];
    for (text, names) in cases {
        let why = refused(text);
        assert!(why.contains(names), "{text:?}: {why}");
    }
}

#[test]
fn a_bundle_must_be_a_secret() {
    let why = refused(&FULL.replace(
        "{kind: Secret, name: grid-ca-bundle}",
        "{kind: ConfigMap, name: grid-ca}",
    ));
    assert!(why.contains("ConfigMap") && why.contains("Secret"), "{why}");
}

#[test]
fn only_the_signer_hosts_the_install_allows_are_used() {
    let allowed = SignerHosts::from_list(" grid-enrollment.grid-enrollment.svc:8443 , signer.example.com. ");
    assert!(!allowed.is_any());
    drop(parse_with(FULL, &allowed).expect("host and port match"));
    let other_port = FULL.replace(":8443", ":9443");
    assert!(
        parse_with(&other_port, &allowed).is_err(),
        "a host:port entry pins the port"
    );
    let any_port = FULL.replace("grid-enrollment.grid-enrollment.svc:8443", "SIGNER.example.com:9443");
    assert!(
        parse_with(&any_port, &allowed).is_ok(),
        "a host entry allows any port, in any case"
    );
    let elsewhere = FULL.replace("grid-enrollment.grid-enrollment.svc", "attacker.example.net");
    let why = parse_with(&elsewhere, &allowed).expect_err("refused");
    assert!(
        why.contains("site.signerEndpoint.url") && why.contains("attacker.example.net"),
        "{why}"
    );
    assert!(SignerHosts::from_list(" , ").is_any(), "an empty list allows any");
    drop(parse_with(&elsewhere, &SignerHosts::any()).expect("no list allows any host"));
}

#[test]
fn a_config_map_without_the_key_is_invalid_and_never_absent() {
    let snapshot = Snapshot::of(&map(None));
    assert_eq!(snapshot.loaded, Loaded::Invalid(format!("{CONFIG_KEY}: key missing")));
    assert_eq!(
        snapshot.object.and_then(|object| object.name).as_deref(),
        Some(DEFAULT_CONFIG_MAP),
        "an Event can name the ConfigMap"
    );
}

#[test]
fn the_site_section_reads_unset_invalid_or_the_site() {
    assert_eq!(Loaded::Absent.site(), Section::Unset);
    assert_eq!(Loaded::Unread.site(), Section::Unset);
    assert_eq!(Loaded::Valid(Box::default()).site(), Section::Unset);
    assert_eq!(Loaded::Invalid("bad".to_owned()).site(), Section::Invalid("bad"));
    let valid = Snapshot::of(&map(Some(FULL))).loaded;
    assert!(matches!(valid.site(), Section::Site(site) if site.name == SITE));
}

/// Where a test sends watch events.
type Events = futures::channel::mpsc::UnboundedSender<Result<watcher::Event<ConfigMap>, watcher::Error>>;

/// Held by every watch case: the ignored-sections gauge is one per process.
static WATCH: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A watch fed from the returned sender.
fn watching() -> (Events, ConfigWatch) {
    let (events_tx, events_rx) = futures::channel::mpsc::unbounded();
    let (tx, rx) = tokio::sync::watch::channel(Arc::new(Snapshot::unread()));
    drop(tokio::spawn(follow(events_rx, tx, None, SignerHosts::any())));
    (events_tx, ConfigWatch(rx))
}

/// Send `event` down `events`.
fn send(events: &Events, event: watcher::Event<ConfigMap>) {
    events.unbounded_send(Ok(event)).expect("send");
}

#[tokio::test]
async fn the_watch_picks_up_an_edit() {
    let _watch = WATCH.lock().await;
    let (events, mut config) = watching();
    send(&events, watcher::Event::Init);
    send(&events, watcher::Event::InitApply(map(Some(FULL))));
    send(&events, watcher::Event::InitDone);
    let first = config.read().await;
    assert!(matches!(first.loaded.site(), Section::Site(site) if site.signer_endpoint.is_some()));

    let edited = FULL.replace(
        "caBundleRef: {kind: Secret, name: grid-ca-bundle}",
        "caBundleRef: {kind: Secret, name: grid-ca}",
    );
    send(&events, watcher::Event::Apply(map(Some(&edited))));
    config.changed().await;
    let current = config.current();
    let pin = match current.loaded.site() {
        Section::Site(site) => site
            .signer_endpoint
            .as_ref()
            .and_then(|signer| signer.ca_bundle_ref.as_ref()),
        Section::Unset | Section::Invalid(_) => None,
    }
    .expect("pin");
    assert_eq!(
        (pin.kind, pin.name.as_str()),
        (BundleKind::Secret, "grid-ca"),
        "the edit applies"
    );
}

#[tokio::test]
async fn a_metadata_only_write_is_not_republished() {
    let _watch = WATCH.lock().await;
    let (events, mut config) = watching();
    send(&events, watcher::Event::Init);
    send(&events, watcher::Event::InitApply(map(Some(FULL))));
    send(&events, watcher::Event::InitDone);
    let first = config.read().await;

    let mut touched = map(Some(FULL));
    touched.metadata.resource_version = Some("2".to_owned());
    send(&events, watcher::Event::Apply(touched));
    let republished = tokio::time::timeout(std::time::Duration::from_millis(100), config.changed()).await;
    assert!(republished.is_err(), "a resourceVersion bump alone republishes nothing");
    assert_eq!(config.current(), first);
}

#[tokio::test]
async fn a_broken_edit_is_refused_whole_and_a_delete_reads_absent() {
    let _watch = WATCH.lock().await;
    let (events, mut config) = watching();
    send(&events, watcher::Event::Init);
    send(&events, watcher::Event::InitApply(map(Some(FULL))));
    send(&events, watcher::Event::InitDone);
    drop(config.read().await);

    send(
        &events,
        watcher::Event::Apply(map(Some("site: {name: site-d, extra: 1}\n"))),
    );
    config.changed().await;
    assert!(
        matches!(config.current().loaded, Loaded::Invalid(_)),
        "a broken edit is refused whole"
    );

    send(&events, watcher::Event::Delete(map(Some(FULL))));
    config.changed().await;
    assert_eq!(config.current().loaded, Loaded::Absent);
}

#[tokio::test]
async fn an_ignored_section_is_counted_and_clears_with_it() {
    let _watch = WATCH.lock().await;
    let (events, mut config) = watching();
    send(&events, watcher::Event::Init);
    send(
        &events,
        watcher::Event::InitApply(map(Some(&format!("{FULL}membership: {{source: Local}}\n")))),
    );
    send(&events, watcher::Event::InitDone);
    assert_eq!(config.read().await.ignored, ["membership"]);
    assert_eq!(crate::metrics::operator_config_ignored_sections(), 1);
    send(&events, watcher::Event::Apply(map(Some(FULL))));
    config.changed().await;
    assert!(config.current().ignored.is_empty());
    assert_eq!(crate::metrics::operator_config_ignored_sections(), 0);
}

#[tokio::test]
async fn a_relist_without_the_config_map_reads_absent() {
    let _watch = WATCH.lock().await;
    let (events, mut config) = watching();
    send(&events, watcher::Event::Init);
    send(&events, watcher::Event::InitDone);
    assert_eq!(config.read().await.loaded, Loaded::Absent);
}

#[test]
fn a_relist_publishes_only_once_it_completes() {
    let mut fold = Fold {
        hosts: SignerHosts::any(),
        relisting: None,
    };
    assert_eq!(fold.apply(watcher::Event::Init), None);
    assert_eq!(fold.apply(watcher::Event::InitApply(map(Some(FULL)))), None);
    let done = fold.apply(watcher::Event::InitDone).expect("published");
    assert!(matches!(done.loaded, Loaded::Valid(_)));
}
