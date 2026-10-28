//! This operator's own settings: one `ConfigMap` in its namespace, watched through the API so
//! an edit applies without a restart. Grid-wide settings stay on the `GridNetwork`.

use std::sync::Arc;

use futures::{Stream, StreamExt as _};
use k8s_openapi::api::core::v1::{ConfigMap, ObjectReference};
use kube::{
    Api, Client, Resource as _,
    runtime::{
        WatchStreamExt as _,
        events::{Event, EventType, Recorder, Reporter},
        watcher,
    },
};
use serde::{Deserialize, Serialize};

/// The `ConfigMap`'s name unless the install names another.
pub const DEFAULT_CONFIG_MAP: &str = "grid-operator-config";

/// The key holding the document.
pub const CONFIG_KEY: &str = "config.yaml";

/// The document under [`CONFIG_KEY`]. Every section is optional. An unknown key inside a
/// section is refused, and an unknown section is ignored, so an older operator keeps
/// running on a document a newer one extends.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OperatorConfig {
    /// This cluster's own site: how it enrolls and renews its identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site: Option<SiteConfig>,
}

/// This cluster's own site.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SiteConfig {
    /// The site name. The invite's `grid.praxis.fast/site` label and the issued certificate
    /// must match it, and its `GridSite` is the one of this name.
    pub name: String,

    /// The signer this site enrolls and renews its identity through. Unset, nothing renews
    /// the identity, and `RotationOnSchedule` says so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer_endpoint: Option<SignerEndpoint>,

    /// Secret holding the one-time invite token this site enrolls with. The token is opaque:
    /// the operator presents it and never parses it. Read only while the site has no identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invite_secret_ref: Option<InviteSecretRef>,
}

/// Whether this operator renews the site identity through the signer.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum Rotation {
    /// Renew when a third of the identity's lifetime remains. Refused under pin peer
    /// trust, where a renewed leaf would not be trusted.
    #[default]
    Enabled,
    /// Use the signer to enroll only. The identity is kept until it expires unless
    /// another writer renews it.
    Disabled,
}

/// The signer a site enrolls and renews its identity through.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SignerEndpoint {
    /// Base URL of the signer: https, naming a host with an optional numeric port, with no
    /// credentials, no query or fragment, no percent-encoding before the path, and not under
    /// `grid.internal`.
    pub url: String,

    /// CA bundle pinning the signer's serving certificate. Required to enroll. Renewal
    /// falls back to the grid CA the site holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_bundle_ref: Option<BundleRef>,

    /// Grid CA the signer's returned CA must match, when it differs from `caBundleRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grid_ca_bundle_ref: Option<BundleRef>,

    /// Whether this operator renews the identity through the signer. Enrollment runs
    /// whatever this says.
    #[serde(default)]
    pub rotation: Rotation,
}

/// A PEM bundle in a `Secret` in the operator namespace. The object must
/// carry the label `grid.praxis.fast/signer-bundle: "true"`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BundleRef {
    /// Kind of the object holding the bundle.
    pub kind: BundleKind,
    /// Object name.
    pub name: String,
    /// Key holding the PEM bundle.
    #[serde(default = "default_bundle_key")]
    pub key: String,
}

/// Kind of object a [`BundleRef`] names. Only a `Secret`: a `ConfigMap` bundle could be
/// created by anyone who can edit this config, pinning a signer of their own.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BundleKind {
    /// A `Secret`.
    Secret,
}

/// The invite token's `Secret` in the operator namespace. The Secret must carry the label
/// `grid.praxis.fast/site: <site name>`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InviteSecretRef {
    /// Secret name.
    pub name: String,
    /// Key holding the token.
    #[serde(default = "default_invite_key")]
    pub key: String,
}

/// Default [`BundleRef::key`].
fn default_bundle_key() -> String {
    "ca.crt".to_owned()
}

/// Default [`InviteSecretRef::key`].
fn default_invite_key() -> String {
    "token".to_owned()
}

/// Longest object name or key a reference may carry.
const MAX_NAME: usize = 253;

/// Longest signer URL.
const MAX_URL: usize = 512;

/// Refuse an empty or overlong `value` at `path`.
fn bounded(path: &str, value: &str, max: usize) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{path}: must not be empty"));
    }
    if value.len() > max {
        return Err(format!("{path}: at most {max} characters"));
    }
    Ok(())
}

impl BundleRef {
    /// Refuse an empty name or key.
    fn validate(&self, path: &str) -> Result<(), String> {
        bounded(&format!("{path}.name"), &self.name, MAX_NAME)?;
        bounded(&format!("{path}.key"), &self.key, MAX_NAME)
    }
}

/// The signer hosts the install allows, set where the operator runs and never in its config,
/// so an edit to the config cannot point enrollment at a signer of its own.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SignerHosts(Option<Vec<Authority>>);

/// One allowed signer: a host, with the port it pins or any port.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Authority {
    /// The host as a URL names it: lowercase, no trailing dot, IPv6 in its canonical form.
    host: String,
    /// The port the entry pins, `None` for any.
    port: Option<u16>,
}

impl Authority {
    /// `entry` as `host` or `host:port`, read with the rules a URL's host follows so that
    /// `Signer.example.com.:8443`, `[2001:DB8::1]:8443` and `2001:db8::1` all match what the
    /// config's URL canonicalizes to. `None` when it is not a host.
    fn parse(entry: &str) -> Option<Self> {
        let entry = entry.trim();
        if entry.is_empty() {
            return None;
        }
        // A bare IPv6 address has no brackets; a URL needs them.
        let url = reqwest::Url::parse(&format!("https://{entry}"))
            .or_else(|_| reqwest::Url::parse(&format!("https://[{entry}]")))
            .ok()?;
        // The URL parser drops a port equal to the scheme's default, so read whether the
        // entry wrote one from its text: after the bracket of an IPv6 host, else the one
        // colon a host or IPv4 address may carry.
        let wrote_port = match entry.rsplit_once(']') {
            Some((_host, rest)) => rest.starts_with(':'),
            None => entry.matches(':').count() == 1,
        };
        Some(Self {
            host: canonical_host(&url)?,
            port: wrote_port.then(|| url.port_or_known_default()).flatten(),
        })
    }

    /// Whether `url` is this host, on this port when one is pinned.
    fn admits(&self, url: &reqwest::Url) -> bool {
        canonical_host(url).as_deref() == Some(self.host.as_str())
            && self.port.is_none_or(|port| url.port_or_known_default() == Some(port))
    }
}

/// `url`'s host as compared: lowercase, no trailing dot, IPv6 in brackets as the URL parser
/// writes it.
fn canonical_host(url: &reqwest::Url) -> Option<String> {
    let host = url.host_str()?.trim_end_matches('.').to_ascii_lowercase();
    (!host.is_empty()).then_some(host)
}

impl SignerHosts {
    /// Any host: the install names none.
    #[must_use]
    pub const fn any() -> Self {
        Self(None)
    }

    /// The hosts in `list`, comma-separated, each `host` or `host:port`, read with the
    /// rules the config's URL follows. Empty allows any. An entry that is not a host is
    /// kept as unmatched, so it allows nothing.
    #[must_use]
    pub fn from_list(list: &str) -> Self {
        let hosts: Vec<Authority> = list
            .split(',')
            .filter(|entry| !entry.trim().is_empty())
            .map(|entry| {
                Authority::parse(entry).unwrap_or_else(|| {
                    tracing::warn!(entry, "GRID_SIGNER_HOSTS entry is not a host; it allows nothing");
                    Authority {
                        host: String::new(),
                        port: None,
                    }
                })
            })
            .collect();
        Self((!hosts.is_empty()).then_some(hosts))
    }

    /// Whether the install names no hosts, so any is used.
    #[must_use]
    pub const fn is_any(&self) -> bool {
        self.0.is_none()
    }

    /// Refuse `url` unless its host, or its host and port, is allowed.
    fn admit(&self, url: &reqwest::Url) -> Result<(), String> {
        let Some(hosts) = &self.0 else {
            return Ok(());
        };
        if hosts.iter().any(|allowed| allowed.admits(url)) {
            return Ok(());
        }
        let listed: Vec<String> = hosts
            .iter()
            .map(|allowed| match allowed.port {
                Some(port) => format!("{}:{port}", allowed.host),
                None => allowed.host.clone(),
            })
            .collect();
        Err(format!(
            "site.signerEndpoint.url: host {} is not a signer this install allows ({})",
            canonical_host(url).unwrap_or_default(),
            listed.join(", ")
        ))
    }
}

impl SiteConfig {
    /// Refuse what the operator cannot act on, naming the field.
    fn validate(&self, hosts: &SignerHosts) -> Result<(), String> {
        certs::validate_site_name(&self.name).map_err(|e| format!("site.name: {e}"))?;
        if let Some(signer) = &self.signer_endpoint {
            bounded("site.signerEndpoint.url", &signer.url, MAX_URL)?;
            hosts.admit(&crate::enroll::signer_url(&signer.url)?)?;
            if let Some(pin) = &signer.ca_bundle_ref {
                pin.validate("site.signerEndpoint.caBundleRef")?;
            }
            if let Some(anchor) = &signer.grid_ca_bundle_ref {
                anchor.validate("site.signerEndpoint.gridCaBundleRef")?;
            }
        }
        if let Some(invite) = &self.invite_secret_ref {
            if self
                .signer_endpoint
                .as_ref()
                .is_none_or(|signer| signer.ca_bundle_ref.is_none())
            {
                return Err(
                    "site.inviteSecretRef needs site.signerEndpoint with a caBundleRef to enroll through".to_owned(),
                );
            }
            bounded("site.inviteSecretRef.name", &invite.name, MAX_NAME)?;
            bounded("site.inviteSecretRef.key", &invite.key, MAX_NAME)?;
        }
        Ok(())
    }
}

/// The document version this operator reads. A document without `version` is this one.
pub const VERSION: u64 = 1;

/// The top-level sections this operator reads, beside `version`.
const SECTIONS: [&str; 1] = ["site"];

/// A parsed document, and the top-level sections this operator does not know and ignored.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Parsed {
    /// What the known sections say.
    pub config: OperatorConfig,
    /// Unknown top-level sections, in document order.
    pub ignored: Vec<String>,
}

/// Parse and validate the document `text`, allowing any signer host. An empty document sets
/// nothing.
///
/// # Errors
///
/// Why the document is refused, naming the field at fault: an unsupported `version`, or an
/// unknown or invalid field inside a known section.
pub fn parse(text: &str) -> Result<Parsed, String> {
    parse_with(text, &SignerHosts::any())
}

/// Parse and validate the document `text`, allowing only the signer `hosts`.
///
/// # Errors
///
/// As [`parse`], and a signer host `hosts` does not allow.
pub fn parse_with(text: &str, hosts: &SignerHosts) -> Result<Parsed, String> {
    let value: serde_yaml::Value = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    let mut document = match value {
        serde_yaml::Value::Null => return Ok(Parsed::default()),
        serde_yaml::Value::Mapping(document) => document,
        other @ (serde_yaml::Value::Bool(_)
        | serde_yaml::Value::Number(_)
        | serde_yaml::Value::String(_)
        | serde_yaml::Value::Sequence(_)
        | serde_yaml::Value::Tagged(_)) => {
            return Err(format!(
                "the document is a mapping of sections, not {}",
                kind_of(&other)
            ));
        },
    };
    read_version(&mut document)?;
    let ignored = unknown_sections(&mut document);
    // Re-read as text, so an error names the path within the section.
    let known = serde_yaml::to_string(&document).map_err(|e| e.to_string())?;
    let config: OperatorConfig = serde_yaml::from_str(&known).map_err(|e| e.to_string())?;
    if let Some(site) = &config.site {
        site.validate(hosts)?;
    }
    Ok(Parsed { config, ignored })
}

/// Take `version` out of `document`, refusing any but [`VERSION`].
fn read_version(document: &mut serde_yaml::Mapping) -> Result<(), String> {
    let Some(version) = document.remove("version") else {
        return Ok(());
    };
    if version.as_u64() == Some(VERSION) {
        return Ok(());
    }
    Err(format!(
        "version: {} is not supported: this operator reads version {VERSION}",
        serde_yaml::to_string(&version).unwrap_or_default().trim()
    ))
}

/// Take the sections this operator does not read out of `document`, returning their names.
fn unknown_sections(document: &mut serde_yaml::Mapping) -> Vec<String> {
    let mut ignored = Vec::new();
    document.retain(|key, _section| {
        let known = key.as_str().is_some_and(|name| SECTIONS.contains(&name));
        if !known {
            ignored.push(key.as_str().map_or_else(|| format!("{key:?}"), str::to_owned));
        }
        known
    });
    ignored
}

/// The YAML kind of `value`, for a refusal.
const fn kind_of(value: &serde_yaml::Value) -> &'static str {
    match value {
        serde_yaml::Value::Null => "null",
        serde_yaml::Value::Bool(_) => "a boolean",
        serde_yaml::Value::Number(_) => "a number",
        serde_yaml::Value::String(_) => "a string",
        serde_yaml::Value::Sequence(_) => "a list",
        serde_yaml::Value::Mapping(_) => "a mapping",
        serde_yaml::Value::Tagged(_) => "a tagged value",
    }
}

/// What the `ConfigMap` held when last read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Loaded {
    /// Not read yet.
    Unread,
    /// No `ConfigMap`: this operator runs on its defaults and the site does not enroll itself.
    Absent,
    /// Refused as a whole, with why. No section of it is acted on.
    Invalid(String),
    /// Read and valid.
    Valid(Box<OperatorConfig>),
}

/// What the document says about this cluster's own site.
#[derive(Debug, PartialEq, Eq)]
pub enum Section<'config> {
    /// Nothing: no `ConfigMap`, or no `site` section.
    Unset,
    /// The document was refused, with why.
    Invalid(&'config str),
    /// The site section.
    Site(&'config SiteConfig),
}

impl<'config> Section<'config> {
    /// The site name, when the section names one.
    #[must_use]
    pub fn name(&self) -> Option<&'config str> {
        match self {
            Self::Site(site) => Some(site.name.as_str()),
            Self::Unset | Self::Invalid(_) => None,
        }
    }
}

impl Loaded {
    /// Whether the document names this site, or was refused and may: either holds off
    /// self-signing until it is read clean.
    #[must_use]
    pub fn names_site(&self) -> bool {
        !matches!(self.site(), Section::Unset)
    }

    /// The `site` section.
    #[must_use]
    pub fn site(&self) -> Section<'_> {
        match self {
            Self::Unread | Self::Absent => Section::Unset,
            Self::Invalid(why) => Section::Invalid(why),
            Self::Valid(config) => config.site.as_ref().map_or(Section::Unset, Section::Site),
        }
    }
}

/// The document as last read, and the `ConfigMap` it came from.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    /// What it held.
    pub loaded: Loaded,
    /// Unknown top-level sections the document carried and this operator ignored.
    pub ignored: Vec<String>,
    /// The `ConfigMap`, for Events, `None` while absent.
    pub object: Option<ObjectReference>,
    /// Whether a `site` section was read since this process started, by this document or an
    /// earlier one. Once set it stays, so a deleted `ConfigMap` cannot hand the identity to
    /// self-signing: a placeholder would read as enrolled.
    pub site_seen: bool,
}

impl Snapshot {
    /// Not read yet.
    #[must_use]
    pub const fn unread() -> Self {
        Self {
            loaded: Loaded::Unread,
            ignored: Vec::new(),
            object: None,
            site_seen: false,
        }
    }

    /// No `ConfigMap`.
    #[must_use]
    pub const fn absent() -> Self {
        Self {
            loaded: Loaded::Absent,
            ignored: Vec::new(),
            object: None,
            site_seen: false,
        }
    }

    /// A `ConfigMap` that cannot be read, refused with `why`.
    #[must_use]
    pub const fn unreadable(why: String) -> Self {
        Self {
            loaded: Loaded::Invalid(why),
            ignored: Vec::new(),
            object: None,
            site_seen: true,
        }
    }

    /// A valid document with no `ConfigMap` behind it.
    #[must_use]
    pub fn of_config(config: OperatorConfig) -> Self {
        let loaded = Loaded::Valid(Box::new(config));
        Self {
            site_seen: loaded.names_site(),
            loaded,
            ignored: Vec::new(),
            object: None,
        }
    }

    /// What `map` holds, allowing any signer host.
    #[must_use]
    pub fn of(map: &ConfigMap) -> Self {
        Self::read(map, &SignerHosts::any())
    }

    /// What `map` holds, allowing only the signer `hosts`.
    #[must_use]
    pub fn read(map: &ConfigMap, hosts: &SignerHosts) -> Self {
        let parsed = match map.data.as_ref().and_then(|data| data.get(CONFIG_KEY)) {
            None => Err(format!("{CONFIG_KEY}: key missing")),
            Some(text) => parse_with(text, hosts),
        };
        let (loaded, ignored) = match parsed {
            Ok(Parsed { config, ignored }) => (Loaded::Valid(Box::new(config)), ignored),
            Err(why) => (Loaded::Invalid(why), Vec::new()),
        };
        // A metadata-only write moves resourceVersion and changes nothing read.
        let mut object = map.object_ref(&());
        object.resource_version = None;
        Self {
            site_seen: loaded.names_site(),
            loaded,
            ignored,
            object: Some(object),
        }
    }
}

/// The latest [`Snapshot`], kept current by a watch on the `ConfigMap`.
#[derive(Clone, Debug)]
pub struct ConfigWatch(tokio::sync::watch::Receiver<Arc<Snapshot>>);

impl ConfigWatch {
    /// Watch the `ConfigMap` `name` in `namespace`, allowing only the signer `hosts`.
    #[must_use]
    pub fn spawn(client: Client, namespace: &str, name: &str, hosts: SignerHosts) -> Self {
        let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
        let events =
            watcher(api, watcher::Config::default().fields(&format!("metadata.name={name}"))).default_backoff();
        let (tx, rx) = tokio::sync::watch::channel(Arc::new(Snapshot::unread()));
        let what = format!("ConfigMap {namespace}/{name}");
        drop(tokio::spawn(follow(events, tx, Some(client), hosts, what)));
        Self(rx)
    }

    /// Always `snapshot`.
    #[must_use]
    pub fn fixed(snapshot: Snapshot) -> Self {
        Self(tokio::sync::watch::channel(Arc::new(snapshot)).1)
    }

    /// Whatever `receiver` is sent.
    #[must_use]
    pub const fn from_receiver(receiver: tokio::sync::watch::Receiver<Arc<Snapshot>>) -> Self {
        Self(receiver)
    }

    /// The document as last read.
    #[must_use]
    pub fn current(&self) -> Arc<Snapshot> {
        Arc::clone(&self.0.borrow())
    }

    /// The document once it has been read.
    pub async fn read(&mut self) -> Arc<Snapshot> {
        let read = self
            .0
            .wait_for(|snapshot| snapshot.loaded != Loaded::Unread)
            .await
            .map(|snapshot| Arc::clone(&snapshot));
        // The watch stopped before a read: nothing more will arrive.
        read.unwrap_or_else(|_closed| self.current())
    }

    /// Resolve when the document next changes. Never resolves once the watch has stopped.
    pub async fn changed(&mut self) {
        if self.0.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// How long the first read may take before the document is refused as unreadable, so a
/// watch that never completes, as under RBAC drift, does not hold startup.
const FIRST_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Fold watch `events` into `tx` until every receiver is gone. `what` names the `ConfigMap`
/// for a refusal. A watch the API forbids, or one that completes no read within
/// [`FIRST_READ_TIMEOUT`], publishes the document as refused, naming the error; a later
/// read replaces that.
async fn follow<S>(
    events: S,
    tx: tokio::sync::watch::Sender<Arc<Snapshot>>,
    client: Option<Client>,
    hosts: SignerHosts,
    what: String,
) where
    S: Stream<Item = Result<watcher::Event<ConfigMap>, watcher::Error>> + Send,
{
    let mut fold = Fold {
        hosts,
        ..Fold::default()
    };
    let mut events = std::pin::pin!(events);
    let mut deadline = std::pin::pin!(tokio::time::sleep(FIRST_READ_TIMEOUT));
    let mut read = false;
    let mut last_error: Option<String> = None;
    while !tx.is_closed() {
        let event = if read {
            events.next().await
        } else {
            tokio::select! {
                event = events.next() => event,
                () = &mut deadline => {
                    read = true;
                    let why = timed_out(&what, last_error.as_deref());
                    Box::pin(publish_next(&tx, client.as_ref(), &Snapshot::unreadable(why))).await;
                    continue;
                },
            }
        };
        let Some(event) = event else { return };
        if let Some(next) = settle(&mut fold, event, (&what, read), &mut last_error) {
            read = true;
            Box::pin(publish_next(&tx, client.as_ref(), &next)).await;
        }
    }
}

/// Why the first read gave up: the wait, and the last watcher error if any.
fn timed_out(what: &str, last_error: Option<&str>) -> String {
    let after = last_error.map(|error| format!(": {error}")).unwrap_or_default();
    format!("cannot read {what} within {}s{after}", FIRST_READ_TIMEOUT.as_secs())
}

/// The snapshot `event` settles on: a watch event through `fold`; a forbidden watch before
/// any read, as a refusal; and any other watcher error as nothing new, remembered in
/// `last_error`. Once `read`, every error keeps the last read: the watch reconnects, and a
/// document that applied stays applied.
fn settle(
    fold: &mut Fold,
    event: Result<watcher::Event<ConfigMap>, watcher::Error>,
    (what, read): (&str, bool),
    last_error: &mut Option<String>,
) -> Option<Snapshot> {
    match event {
        Ok(event) => fold.apply(event),
        Err(error) if !read && forbidden(&error) => Some(Snapshot::unreadable(format!("cannot read {what}: {error}"))),
        Err(error) => {
            tracing::warn!(%error, "watching the operator config failed; retrying");
            *last_error = Some(error.to_string());
            None
        },
    }
}

/// Whether the API refused the watch outright, which no retry fixes.
fn forbidden(error: &watcher::Error) -> bool {
    match error {
        watcher::Error::InitialListFailed(error)
        | watcher::Error::WatchStartFailed(error)
        | watcher::Error::WatchFailed(error) => {
            matches!(error, kube::Error::Api(status) if status.is_forbidden())
        },
        watcher::Error::WatchError(status) => status.is_forbidden(),
        watcher::Error::NoResourceVersion => false,
    }
}

/// Publish `next` when it changed, with its metrics and Events.
async fn publish_next(tx: &tokio::sync::watch::Sender<Arc<Snapshot>>, client: Option<&Client>, next: &Snapshot) {
    if publish(tx, next) {
        crate::metrics::set_operator_config_ignored_sections(next.ignored.len());
        note_ignored(client, next).await;
    }
}

/// Send `next` down `tx` when it changed, logging what it holds. Returns whether it changed.
fn publish(tx: &tokio::sync::watch::Sender<Arc<Snapshot>>, next: &Snapshot) -> bool {
    tx.send_if_modified(|held| {
        if **held == *next {
            return false;
        }
        match &next.loaded {
            Loaded::Invalid(why) => tracing::warn!(error = %why, "operator config refused"),
            Loaded::Absent => tracing::info!("no operator config; running on defaults"),
            Loaded::Unread | Loaded::Valid(_) => tracing::info!("operator config read"),
        }
        *held = Arc::new(next.clone());
        true
    })
}

/// The longest Event note, within the API's soft 1 KB limit.
const MAX_NOTE: usize = 1000;

/// What a Warning Event says about the sections `ignored`, bounded.
fn ignored_note(ignored: &[String]) -> String {
    let mut note = format!(
        "sections this operator does not read were ignored, the rest applied: {}",
        ignored.join(", ")
    );
    if note.len() > MAX_NOTE {
        let mut end = MAX_NOTE.saturating_sub(3);
        while !note.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        note.truncate(end);
        note.push_str("...");
    }
    note
}

/// Log, and record as a Warning Event on the `ConfigMap`, the sections `snapshot` ignored.
async fn note_ignored(client: Option<&Client>, snapshot: &Snapshot) {
    if snapshot.ignored.is_empty() {
        return;
    }
    let note = ignored_note(&snapshot.ignored);
    tracing::warn!(sections = ?snapshot.ignored, "{note}");
    let (Some(client), Some(object)) = (client, snapshot.object.as_ref()) else {
        return;
    };
    let reporter = Reporter {
        controller: "grid-operator".into(),
        instance: None,
    };
    let event = Event {
        type_: EventType::Warning,
        reason: "OperatorConfigSectionIgnored".to_owned(),
        note: Some(note),
        action: "ReadConfig".to_owned(),
        secondary: None,
    };
    if let Err(error) = Box::pin(Recorder::new(client.clone(), reporter).publish(&event, object)).await {
        tracing::debug!(%error, "recording an ignored-section Event failed");
    }
}

/// Watch events folded into what the `ConfigMap` holds.
#[derive(Debug, Default)]
struct Fold {
    /// The signer hosts the install allows.
    hosts: SignerHosts,
    /// What a relist found so far, published once it completes.
    relisting: Option<Snapshot>,
    /// Whether any snapshot so far carried a `site` section.
    site_seen: bool,
}

impl Fold {
    /// The snapshot `event` settles on, `None` while a relist is under way. Once a snapshot
    /// carried a `site` section, every later one says so too.
    fn apply(&mut self, event: watcher::Event<ConfigMap>) -> Option<Snapshot> {
        let mut next = match event {
            watcher::Event::Init => {
                self.relisting = Some(Snapshot::absent());
                None
            },
            watcher::Event::InitApply(map) => {
                self.relisting = Some(Snapshot::read(&map, &self.hosts));
                None
            },
            watcher::Event::InitDone => Some(self.relisting.take().unwrap_or_else(Snapshot::absent)),
            watcher::Event::Apply(map) => Some(Snapshot::read(&map, &self.hosts)),
            watcher::Event::Delete(_) => Some(Snapshot::absent()),
        }?;
        self.site_seen |= next.site_seen;
        next.site_seen = self.site_seen;
        Some(next)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests;
