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

/// A PEM bundle in a `Secret` or `ConfigMap` in the operator namespace. The object must
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

/// Kind of object a [`BundleRef`] names.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BundleKind {
    /// A `Secret`.
    Secret,
    /// A `ConfigMap`.
    ConfigMap,
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

impl SiteConfig {
    /// Refuse what the operator cannot act on, naming the field.
    fn validate(&self) -> Result<(), String> {
        certs::validate_site_name(&self.name).map_err(|e| format!("site.name: {e}"))?;
        if let Some(signer) = &self.signer_endpoint {
            bounded("site.signerEndpoint.url", &signer.url, MAX_URL)?;
            crate::enroll::signer_url(&signer.url)?;
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

/// Parse and validate the document `text`. An empty document sets nothing.
///
/// # Errors
///
/// Why the document is refused, naming the field at fault: an unsupported `version`, or an
/// unknown or invalid field inside a known section.
pub fn parse(text: &str) -> Result<Parsed, String> {
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
        site.validate()?;
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
}

impl Snapshot {
    /// Not read yet.
    #[must_use]
    pub const fn unread() -> Self {
        Self {
            loaded: Loaded::Unread,
            ignored: Vec::new(),
            object: None,
        }
    }

    /// No `ConfigMap`.
    #[must_use]
    pub const fn absent() -> Self {
        Self {
            loaded: Loaded::Absent,
            ignored: Vec::new(),
            object: None,
        }
    }

    /// A valid document with no `ConfigMap` behind it.
    #[must_use]
    pub fn of_config(config: OperatorConfig) -> Self {
        Self {
            loaded: Loaded::Valid(Box::new(config)),
            ignored: Vec::new(),
            object: None,
        }
    }

    /// What `map` holds.
    #[must_use]
    pub fn of(map: &ConfigMap) -> Self {
        let parsed = match map.data.as_ref().and_then(|data| data.get(CONFIG_KEY)) {
            None => Err(format!("{CONFIG_KEY}: key missing")),
            Some(text) => parse(text),
        };
        let (loaded, ignored) = match parsed {
            Ok(Parsed { config, ignored }) => (Loaded::Valid(Box::new(config)), ignored),
            Err(why) => (Loaded::Invalid(why), Vec::new()),
        };
        Self {
            loaded,
            ignored,
            object: Some(map.object_ref(&())),
        }
    }
}

/// The latest [`Snapshot`], kept current by a watch on the `ConfigMap`.
#[derive(Clone, Debug)]
pub struct ConfigWatch(tokio::sync::watch::Receiver<Arc<Snapshot>>);

impl ConfigWatch {
    /// Watch the `ConfigMap` `name` in `namespace`.
    #[must_use]
    pub fn spawn(client: Client, namespace: &str, name: &str) -> Self {
        let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
        let events =
            watcher(api, watcher::Config::default().fields(&format!("metadata.name={name}"))).default_backoff();
        let (tx, rx) = tokio::sync::watch::channel(Arc::new(Snapshot::unread()));
        drop(tokio::spawn(follow(events, tx, Some(client))));
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

/// Fold watch `events` into `tx` until every receiver is gone.
async fn follow<S>(events: S, tx: tokio::sync::watch::Sender<Arc<Snapshot>>, client: Option<Client>)
where
    S: Stream<Item = Result<watcher::Event<ConfigMap>, watcher::Error>> + Send,
{
    let mut fold = Fold::default();
    let mut events = std::pin::pin!(events);
    while let Some(event) = events.next().await {
        let next = match event {
            Ok(event) => fold.apply(event),
            Err(error) => {
                tracing::warn!(%error, "watching the operator config failed; retrying");
                None
            },
        };
        if let Some(next) = next
            && publish(&tx, &next)
        {
            crate::metrics::set_operator_config_ignored_sections(next.ignored.len());
            Box::pin(note_ignored(client.as_ref(), &next)).await;
        }
        if tx.is_closed() {
            return;
        }
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
    /// What a relist found so far, published once it completes.
    relisting: Option<Snapshot>,
}

impl Fold {
    /// The snapshot `event` settles on, `None` while a relist is under way.
    fn apply(&mut self, event: watcher::Event<ConfigMap>) -> Option<Snapshot> {
        match event {
            watcher::Event::Init => {
                self.relisting = Some(Snapshot::absent());
                None
            },
            watcher::Event::InitApply(map) => {
                self.relisting = Some(Snapshot::of(&map));
                None
            },
            watcher::Event::InitDone => Some(self.relisting.take().unwrap_or_else(Snapshot::absent)),
            watcher::Event::Apply(map) => Some(Snapshot::of(&map)),
            watcher::Event::Delete(_) => Some(Snapshot::absent()),
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests;
