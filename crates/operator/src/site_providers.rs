//! The providers a site serves, published for peers to build routing candidates from.
//!
//! Membership-free: a peer reads this over the same mTLS listener as its signals, so a
//! declared peer yields candidates without gossip. Load stays on the signals path.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{Arc, PoisonError, RwLock},
};

use serde::{Deserialize, Serialize};

/// Header carrying [`LocalProviders::version`] on every signals answer.
pub const VERSION_HEADER: &str = "x-grid-providers-version";

/// Header naming the providers body's contract version on a providers answer.
pub const CONTRACT_HEADER: &str = "x-grid-providers-contract";

/// The providers contract this build writes, and the only major it reads.
pub const CONTRACT_VERSION: u32 = 1;

/// Whether a reader takes a body under contract `header`: one with no header predates
/// the header and is version 1, and any other major is refused.
#[must_use]
pub fn reads_contract(header: Option<&str>) -> bool {
    header.is_none_or(|value| {
        value
            .trim()
            .split('.')
            .next()
            .and_then(|major| major.parse::<u32>().ok())
            == Some(CONTRACT_VERSION)
    })
}

/// The path a site's providers are served on, beside [`crate::signals::SIGNALS_PATH`].
pub const PROVIDERS_PATH: &str = "/v1/site/providers";

/// How a body that left records out closes.
const TRUNCATED_CLOSE: &str = r#"],"truncated":true}"#;

/// Most provider records served or accepted in one body.
pub const MAX_PROVIDERS: usize = 256;

/// Largest providers body served or accepted.
pub const MAX_BODY_BYTES: usize = 256 * 1024;

/// Models one provider record may name. A record naming more is refused.
pub const MAX_MODELS_PER_RECORD: usize = 64;

/// One provider record as a peer reads it.
///
/// Carries its network and no site: a reader keeps only its own network's records
/// and stamps the site from who it polled, so a peer cannot speak for another site.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WireProvider {
    /// Grid network the provider belongs to.
    pub network_id: String,
    /// Provider identity within the serving site.
    pub provider_id: String,
    /// Routing cluster identity a candidate names.
    pub routing_cluster: String,
    /// Models served.
    #[serde(default)]
    pub models: Vec<String>,
    /// MCP tools served.
    #[serde(default)]
    pub tools: Vec<String>,
    /// Backend locality kind.
    #[serde(default)]
    pub backend_kind: String,
    /// Relative configured capacity.
    pub capacity_weight: u32,
    /// Lifecycle phase observed by the serving site.
    pub phase: crdt::ProviderPhase,
    /// Labels a consumer site must carry to use the provider, empty for every site. In key
    /// order, so a record serializes the same each time.
    #[serde(default)]
    pub match_labels: BTreeMap<String, String>,
    /// Monotonic per-site revision.
    pub revision: u64,
}

impl From<crdt::ProviderState> for WireProvider {
    fn from(state: crdt::ProviderState) -> Self {
        Self {
            network_id: state.network_id,
            provider_id: state.provider_id,
            routing_cluster: state.routing_cluster,
            models: state.models,
            tools: state.tools,
            backend_kind: state.backend_kind,
            capacity_weight: state.capacity_weight,
            phase: state.phase,
            match_labels: state.access_policy.match_labels,
            revision: state.revision,
        }
    }
}

impl WireProvider {
    /// The provider state `site` serves, `None` for a record no candidate can use.
    #[must_use]
    pub fn into_state(self, site: &str) -> Option<crdt::ProviderState> {
        // Both names travel as header values at the gateway, so a bad one is dropped here
        // rather than failing the whole serving document there.
        let header_safe = |name: &str| !name.is_empty() && http::HeaderValue::from_str(name).is_ok();
        if !header_safe(&self.provider_id)
            || !header_safe(&self.routing_cluster)
            || !crdt::is_valid_capacity_weight(self.capacity_weight)
            || self.models.len() > MAX_MODELS_PER_RECORD
        {
            return None;
        }
        Some(crdt::ProviderState {
            network_id: self.network_id,
            site_id: site.to_owned(),
            provider_id: self.provider_id,
            routing_cluster: self.routing_cluster,
            models: self.models,
            tools: self.tools,
            backend_kind: self.backend_kind,
            capacity_weight: self.capacity_weight,
            phase: self.phase,
            metrics: crdt::ProviderMetricsSnapshot::default(),
            access_policy: crdt::ProviderAccessPolicy {
                match_labels: self.match_labels,
            },
            revision: self.revision,
            writer_id: site.to_owned(),
        })
    }
}

/// The body a site serves.
#[derive(Deserialize, Serialize)]
struct Body<Providers> {
    /// Provider records.
    providers: Providers,
    /// Whether records were left out to keep the body within its bounds.
    #[serde(default)]
    truncated: bool,
}

/// The records in a peer's body, `None` when it is too large, malformed, or lists too many.
#[must_use]
pub fn parse(body: &str) -> Option<Vec<WireProvider>> {
    if body.len() > MAX_BODY_BYTES {
        return None;
    }
    serde_json::from_str::<Body<Vec<WireProvider>>>(body)
        .ok()
        .inspect(|body| {
            if body.truncated {
                tracing::warn!(
                    records = body.providers.len(),
                    "peer served a truncated providers catalog"
                );
            }
        })
        .map(|body| body.providers)
        .filter(|providers| providers.len() <= MAX_PROVIDERS)
}

/// The first eight bytes of `bytes`' SHA-256, in hex.
fn digest(bytes: &[u8]) -> String {
    crate::resources::tls_backend::sha256(bytes)
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Who is reading this site's providers.
#[derive(Clone, Copy, Debug)]
pub enum Reader<'labels> {
    /// This site's own gateway, which sees every provider.
    Local,
    /// A peer, carrying the labels this site holds for it.
    Peer(&'labels BTreeMap<String, String>),
}

/// This site's own providers, as served on [`PROVIDERS_PATH`], keyed by network.
///
/// In network order, so the version and a truncated body are the same each render.
#[derive(Clone, Debug, Default)]
pub struct LocalProviders(Arc<RwLock<Catalog>>, Arc<RwLock<Option<String>>>);

/// What [`LocalProviders`] holds: the records by network, and each reader's body as rendered
/// from them, emptied whenever they change.
#[derive(Debug, Default)]
pub struct Catalog {
    /// The records, by network.
    networks: BTreeMap<String, Vec<WireProvider>>,
    /// Each reader's body and its version, by the labels it reads under, `None` for this site.
    rendered: std::sync::Mutex<RenderedByReader>,
}

/// Rendered bodies by the labels their reader reads under, `None` for this site.
type RenderedByReader = HashMap<Option<BTreeMap<String, String>>, Rendered>;

/// One reader's body and the version naming it, rendered together so they always agree.
#[derive(Clone, Debug)]
pub struct Rendered {
    /// The JSON body.
    pub body: bytes::Bytes,
    /// A digest of exactly `body`.
    pub version: String,
}

/// Readers whose bodies are kept rendered; past it the kept ones are dropped and rendered again.
const MAX_RENDERED: usize = 1024;

impl LocalProviders {
    /// Replace what `network` serves, ordered by provider so a capped body keeps the same records.
    pub fn replace(&self, network: &str, mut providers: Vec<WireProvider>) {
        providers.sort_by(|a, b| a.provider_id.cmp(&b.provider_id));
        let serialized = {
            let mut held = self.0.write().unwrap_or_else(PoisonError::into_inner);
            if held.networks.get(network) == Some(&providers) {
                return;
            }
            held.networks.insert(network.to_owned(), providers);
            held.rendered.get_mut().unwrap_or_else(PoisonError::into_inner).clear();
            serde_json::to_vec(&held.networks).ok()
        };
        let version = serialized.map(|bytes| digest(&bytes));
        *self.1.write().unwrap_or_else(PoisonError::into_inner) = version;
    }

    /// Forget every network `keep` refuses, as when its `GridNetwork` is deleted. Once none is
    /// held the catalog reads as unpublished again.
    pub fn retain_networks<Keep: Fn(&str) -> bool>(&self, keep: Keep) {
        let serialized = {
            let mut held = self.0.write().unwrap_or_else(PoisonError::into_inner);
            let before = held.networks.len();
            held.networks.retain(|network, _| keep(network));
            if held.networks.len() == before {
                return;
            }
            held.rendered.get_mut().unwrap_or_else(PoisonError::into_inner).clear();
            (!held.networks.is_empty())
                .then(|| serde_json::to_vec(&held.networks).ok())
                .flatten()
        };
        *self.1.write().unwrap_or_else(PoisonError::into_inner) = serialized.map(|bytes| digest(&bytes));
    }

    /// The version of what `reader` is served: a digest of its rendered body, `None` until
    /// published. It changes only when that reader's catalog does, and says nothing about
    /// records outside its scope.
    #[must_use]
    pub fn version_for(&self, reader: Reader<'_>) -> Option<String> {
        self.rendered(reader).map(|rendered| rendered.version)
    }

    /// `reader`'s body and its version from one read of the catalog, rendered once per change
    /// and reader; `None` until published.
    #[must_use]
    pub fn rendered(&self, reader: Reader<'_>) -> Option<Rendered> {
        let key = match reader {
            Reader::Local => None,
            Reader::Peer(labels) => Some(labels.clone()),
        };
        let held = self.0.read().unwrap_or_else(PoisonError::into_inner);
        let mut kept = held.rendered.lock().unwrap_or_else(PoisonError::into_inner);
        let rendered = if let Some(rendered) = kept.get(&key) {
            rendered.clone()
        } else {
            let body = render_from(&held.networks, reader)?;
            let rendered = Rendered {
                version: digest(body.as_bytes()),
                body: bytes::Bytes::from(body),
            };
            if kept.len() >= MAX_RENDERED {
                kept.clear();
            }
            kept.insert(key, rendered.clone());
            rendered
        };
        drop(kept);
        drop(held);
        Some(rendered)
    }

    /// A digest of everything served, `None` until published.
    #[must_use]
    pub fn version(&self) -> Option<String> {
        self.1.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// The JSON body for `reader`, within [`MAX_PROVIDERS`] and [`MAX_BODY_BYTES`], `None`
    /// until a network has been published, so a restarting site never answers with no providers.
    ///
    /// A peer sees only providers whose access policy its labels satisfy, since reading a
    /// provider is not a wider right than routing to it.
    #[must_use]
    pub fn render(&self, reader: Reader<'_>) -> Option<String> {
        let rendered = self.rendered(reader)?;
        String::from_utf8(rendered.body.to_vec()).ok()
    }
}

/// The JSON body `reader` is served from `held`, within [`MAX_PROVIDERS`] and
/// [`MAX_BODY_BYTES`], `None` when nothing is published.
fn render_from(held: &BTreeMap<String, Vec<WireProvider>>, reader: Reader<'_>) -> Option<String> {
    let mut body = String::from(r#"{"providers":["#);
    if held.is_empty() {
        return None;
    }
    let visible = interleaved(held).filter(|provider| match reader {
        Reader::Local => true,
        Reader::Peer(labels) => crate::signals::labels_satisfy(&provider.match_labels, labels),
    });
    let mut count = 0;
    let mut truncated = false;
    for provider in visible {
        let Ok(record) = serde_json::to_string(provider) else {
            continue;
        };
        // The truncated close is counted with each record, so the cap holds for the whole body.
        let full = body.len() + record.len() + usize::from(count > 0) + TRUNCATED_CLOSE.len() > MAX_BODY_BYTES;
        if count == MAX_PROVIDERS || full {
            truncated = true;
            break;
        }
        if count > 0 {
            body.push(',');
        }
        body.push_str(&record);
        count += 1;
    }
    body.push_str(if truncated { TRUNCATED_CLOSE } else { "]}" });
    Some(body)
}

/// Every network's providers, one from each in turn, so a body cut at its bound keeps a share
/// of each network rather than only the first.
fn interleaved(held: &BTreeMap<String, Vec<WireProvider>>) -> impl Iterator<Item = &WireProvider> {
    let deepest = held.values().map(Vec::len).max().unwrap_or(0);
    (0..deepest).flat_map(move |rank| held.values().filter_map(move |providers| providers.get(rank)))
}

/// Providers polled from peers, keyed by the peer they were read from.
///
/// Held as served and stamped on read. A silent peer's catalog stays while its
/// `GridSite` names it, as poll keeps a silent peer's candidates.
#[derive(Clone, Debug, Default)]
pub struct PeerProviders(Arc<PeerCatalogs>);

/// The held catalogs, and a signal for whoever renders from them.
#[derive(Debug, Default)]
struct PeerCatalogs {
    /// Each peer's catalog, in name order since the records a round returns are read in it.
    held: RwLock<BTreeMap<String, Vec<WireProvider>>>,
    /// Marked whenever a catalog arrives, changes or is forgotten.
    changed: tokio::sync::watch::Sender<()>,
    /// When each held peer's catalog first arrived, as an order: what the overlay's ceiling
    /// has left once every peer has its share goes in it.
    arrived: std::sync::Mutex<(HashMap<String, u64>, u64)>,
    /// How many peers the `GridSite`s declare, so the ceiling is shared among them from the
    /// first catalog on rather than spent by whoever arrives first after a restart.
    declared: std::sync::atomic::AtomicUsize,
    /// The verdicts of the last admission, by peer. One operator serves one `GridNetwork`,
    /// so one admission's verdicts are the whole picture.
    verdicts: std::sync::Mutex<HashMap<String, Verdict>>,
}

/// What the last admission said of a peer's catalog, when it said anything.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Verdict {
    /// Refused: the condition reason naming the cause, and what an admin is told, naming the
    /// peer in the way.
    Refused {
        /// The condition reason.
        reason: &'static str,
        /// The detail.
        message: String,
    },
    /// Accepted, holding or crowding what another peer names: the detail.
    Noted(String),
}

impl Verdict {
    /// Refused for `reason`.
    fn refused(reason: &'static str, message: String) -> Self {
        Self::Refused { reason, message }
    }

    /// Whether the catalog was accepted.
    #[must_use]
    pub const fn accepted(&self) -> bool {
        matches!(self, Self::Noted(_))
    }

    /// The condition reason: the cause when refused, else the condition's type.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Refused { reason, .. } => reason,
            Self::Noted(_) => crate::peer_reach::CATALOG_ACCEPTED,
        }
    }

    /// What an admin is told.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::Refused { message, .. } | Self::Noted(message) => message,
        }
    }
}

/// How the overlay's ceiling was shared this admission.
struct Shared {
    /// What each peer over its share may contribute: its share plus what fit of the remainder.
    allowance: HashMap<String, usize>,
    /// Each peer that took part of the remainder, with how much, in the order it did.
    took: Vec<(String, usize)>,
    /// The share each peer is owed.
    share: usize,
    /// The peers the share was divided among.
    peers: usize,
}

impl PeerProviders {
    /// Replace what `site` serves.
    pub fn replace(&self, site: &str, providers: Vec<WireProvider>) {
        let changed = {
            let mut held = self.0.held.write().unwrap_or_else(PoisonError::into_inner);
            let changed = held.get(site) != Some(&providers);
            if changed {
                held.insert(site.to_owned(), providers);
            }
            changed
        };
        let mut arrived = self.0.arrived.lock().unwrap_or_else(PoisonError::into_inner);
        let (order, next) = &mut *arrived;
        if !order.contains_key(site) {
            order.insert(site.to_owned(), *next);
            *next = next.wrapping_add(1);
        }
        drop(arrived);
        if changed {
            self.0.changed.send_replace(());
        }
    }

    /// How many peers the `GridSite`s declare, which the overlay's ceiling is shared among.
    pub fn declare(&self, peers: usize) {
        self.0.declared.store(peers, std::sync::atomic::Ordering::Relaxed);
    }

    /// Forget every peer `keep` refuses.
    pub fn retain_sites<Keep: Fn(&str) -> bool>(&self, keep: Keep) {
        let forgot = {
            let mut held = self.0.held.write().unwrap_or_else(PoisonError::into_inner);
            let before = held.len();
            held.retain(|site, _| keep(site));
            held.len() != before
        };
        self.0
            .arrived
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .0
            .retain(|site, _| keep(site));
        if forgot {
            self.0.changed.send_replace(());
        }
    }

    /// One item after each change to the held catalogs, a burst of changes coalesced.
    ///
    /// Under poll no membership event follows a catalog read, so the overlay renders on this.
    pub fn changes(&self) -> impl futures::Stream<Item = ()> + Send + 'static {
        futures::stream::unfold(self.0.changed.subscribe(), |mut changed| async move {
            changed.changed().await.ok()?;
            Some(((), changed))
        })
    }

    /// Whether a catalog is held for `site`.
    #[must_use]
    pub fn holds(&self, site: &str) -> bool {
        self.0
            .held
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(site)
    }

    /// [`Self::admit`] with no cluster bound to a site.
    #[must_use]
    pub fn remote_for(&self, network: &str, reserved: &HashSet<String>) -> Vec<crdt::ProviderState> {
        self.admit(network, reserved, &ClusterSites::new())
    }

    /// Admit `network`'s held records for routing, stamped as their peer's whatever the body
    /// said, and record this admission's verdict on each peer for [`Self::verdicts`].
    ///
    /// The cluster a record names selects where this site's gateway sends the request, so a
    /// record is dropped when its cluster is one of this site's own (`reserved`), when `hops`
    /// binds it to another site's gateway, when the cluster is named after another peer, or
    /// when peers contest it with nothing to settle who it is. A peer whose catalog would take
    /// the overlay past its ceiling is admitted up to its share and what is left of the
    /// remainder, records in id order and models in name order.
    #[must_use]
    pub fn admit(&self, network: &str, reserved: &HashSet<String>, hops: &ClusterSites) -> Vec<crdt::ProviderState> {
        let held = self.0.held.read().unwrap_or_else(PoisonError::into_inner);
        let in_network: Vec<(&String, &WireProvider)> = held
            .iter()
            .flat_map(|(site, providers)| {
                providers
                    .iter()
                    .filter(|provider| provider.network_id == network)
                    .map(move |provider| (site, provider))
            })
            .collect();
        let shared = self.share_ceiling(&in_network);
        let claims = claimants(in_network.iter().copied(), reserved, hops);
        let mut verdicts = HashMap::new();
        let (kept, short) = routed(&in_network, &shared, (reserved, hops, &claims), &mut verdicts);
        for site in held.keys() {
            crate::metrics::set_peer_catalog_cut(site, short.contains_key(site.as_str()));
        }
        for (site, note) in notes(&shared, &short, &claims) {
            verdicts.entry(site).or_insert(Verdict::Noted(note));
        }
        drop(held);
        *self.0.verdicts.lock().unwrap_or_else(PoisonError::into_inner) = verdicts;
        kept
    }

    /// Each peer the last admission said something of: refused for naming a cluster it may
    /// not route, else accepted while holding what another peer names, cut to its share of the
    /// overlay's ceiling, or leaving another peer short by taking the ceiling's remainder.
    #[must_use]
    pub fn verdicts(&self) -> HashMap<String, Verdict> {
        self.0.verdicts.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Share the overlay's candidate ceiling among the peers: each is owed the ceiling divided
    /// by the peers declared, counting one candidate per model, and what is left once every
    /// peer has had its share goes to the peers over theirs in the order their catalogs first
    /// arrived. A peer is never refused for its size: past what it may contribute, the rest of
    /// its catalog waits, so one peer serving a thousand models after a restart cannot leave
    /// the rest with nothing, and arrives degraded rather than refused when they come.
    fn share_ceiling(&self, records: &[(&String, &WireProvider)]) -> Shared {
        const CEILING: usize = crate::resources::routing_overlay::MAX_OVERLAY_CANDIDATES;
        let mut counts = HashMap::<&str, usize>::new();
        for (site, provider) in records {
            let held = counts.entry(site.as_str()).or_default();
            *held = held.saturating_add(candidates(provider));
        }
        let declared = self.0.declared.load(std::sync::atomic::Ordering::Relaxed);
        let peers = declared.max(counts.len()).max(1);
        let share = (CEILING / peers).max(1);
        let within: usize = counts.values().map(|count| (*count).min(share)).sum();
        let left = CEILING.saturating_sub(within);
        let mut over: Vec<(&str, usize)> = counts.into_iter().filter(|(_, count)| *count > share).collect();
        {
            let arrived = self.0.arrived.lock().unwrap_or_else(PoisonError::into_inner);
            over.sort_by_key(|(site, _)| (arrived.0.get(*site).copied().unwrap_or(u64::MAX), *site));
        }
        divide(over, (share, peers), left)
    }
}

/// The candidates `provider` contributes: one per model, and one for a record with none.
fn candidates(provider: &WireProvider) -> usize {
    provider.models.len().max(1)
}

/// Hand `left` of the ceiling to the peers `over` their `share`, in order, each taking as
/// much of its excess as fits: what each may contribute, and who took what.
fn divide(over: Vec<(&str, usize)>, (share, peers): (usize, usize), mut left: usize) -> Shared {
    let mut took = Vec::new();
    let mut allowance = HashMap::new();
    for (peer, count) in over {
        let taken = count.saturating_sub(share).min(left);
        left -= taken;
        if taken > 0 {
            took.push((peer.to_owned(), taken));
        }
        if share.saturating_add(taken) < count {
            tracing::warn!(
                peer,
                candidates = count,
                share,
                taken,
                "peer catalog is past this site's share of the overlay's ceiling; the rest waits"
            );
        }
        allowance.insert(peer.to_owned(), share.saturating_add(taken));
    }
    Shared {
        allowance,
        took,
        share,
        peers,
    }
}

/// Of a peer over its share: what it may still contribute, and what of its catalog waits.
#[derive(Default)]
struct Allowance {
    /// The candidates it may still contribute this admission.
    left: usize,
    /// What waits, so far.
    waiting: Waiting,
}

/// How many of a peer's candidates wait, of how many.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Waiting {
    /// Candidates left out, one per model.
    left_out: usize,
    /// Candidates the catalog holds.
    total: usize,
}

/// By peer left short, what waits.
type Short<'shared> = BTreeMap<&'shared str, Waiting>;

/// What each peer over its share may still contribute this admission, and what of its catalog
/// has waited so far.
struct Budgets<'shared>(HashMap<&'shared str, Allowance>);

impl<'shared> Budgets<'shared> {
    /// Budgets from the share `shared` worked out.
    fn new(shared: &'shared Shared) -> Self {
        Self(
            shared
                .allowance
                .iter()
                .map(|(site, allowed)| {
                    let allowance = Allowance {
                        left: *allowed,
                        ..Allowance::default()
                    };
                    (site.as_str(), allowance)
                })
                .collect(),
        )
    }

    /// `record` cut to what `site` may still contribute, models in name order so a cut catalog
    /// keeps the same records each round; `None` once nothing is left for it. A record with no
    /// models counts as one candidate and is kept or dropped whole.
    fn cut(&mut self, site: &str, mut record: WireProvider) -> Option<WireProvider> {
        let wanted = candidates(&record);
        let Some(allowance) = self.0.get_mut(site) else {
            return Some(record);
        };
        allowance.waiting.total += wanted;
        if allowance.left >= wanted {
            allowance.left -= wanted;
            return Some(record);
        }
        allowance.waiting.left_out += wanted - allowance.left;
        let kept = std::mem::take(&mut allowance.left);
        if kept == 0 {
            return None;
        }
        record.models.sort_unstable();
        record.models.truncate(kept);
        Some(record)
    }

    /// The peers left short, with what waits.
    fn short(self) -> Short<'shared> {
        self.0
            .into_iter()
            .filter(|(_, allowance)| allowance.waiting.left_out > 0)
            .map(|(site, allowance)| (site, allowance.waiting))
            .collect()
    }
}

/// `records` that may route, stamped as their peer's: each cluster refusal is recorded in
/// `verdicts` in catalog order, so the first offending record names a peer's cause, and each
/// peer is then cut to what it may contribute, records in id order. Returns what routes and,
/// by peer, how many candidates of how many were left out.
fn routed<'shared>(
    records: &[(&String, &WireProvider)],
    shared: &'shared Shared,
    (reserved, hops, claims): (&HashSet<String>, &ClusterSites, &Claims<'_>),
    verdicts: &mut HashMap<String, Verdict>,
) -> (Vec<crdt::ProviderState>, Short<'shared>) {
    let mut routable: Vec<&(&String, &WireProvider)> = Vec::with_capacity(records.len());
    for record @ (site, provider) in records {
        let cluster = provider.routing_cluster.as_str();
        if let Some((reason, why)) = refusal(cluster, site, (reserved, hops, claims)) {
            tracing::warn!(peer = %site, cluster, why, "peer provider names a cluster it may not route; dropped");
            verdicts
                .entry((*site).clone())
                .or_insert_with(|| Verdict::refused(reason, format!("names cluster {cluster}, {why}")));
            continue;
        }
        routable.push(record);
    }
    routable.sort_by_key(|(site, provider)| (site.as_str(), provider.provider_id.as_str()));
    let mut budgets = Budgets::new(shared);
    let mut kept = Vec::new();
    for (site, provider) in routable {
        if let Some(record) = budgets.cut(site, (*provider).clone()) {
            kept.extend(record.into_state(site.as_str()));
        }
    }
    (kept, budgets.short())
}

/// `sites` as a message names them, in order.
fn names(sites: &[&str]) -> String {
    match sites {
        [one] => format!("peer {one}"),
        many => format!("peers {}", many.join(", ")),
    }
}

/// What an accepted peer is told, by peer: the clusters it holds that other peers name, the
/// candidates of its own catalog that wait past this site's share, and the peers it leaves
/// short by taking the ceiling's remainder.
fn notes(shared: &Shared, short: &Short<'_>, claims: &Claims<'_>) -> Vec<(String, String)> {
    let mut notes = BTreeMap::<String, Vec<String>>::new();
    // In cluster order, so a holder's note reads the same each round.
    let mut held: Vec<(&&str, &Claim<'_>)> = claims.iter().collect();
    held.sort_by_key(|(cluster, _)| **cluster);
    for (cluster, claim) in held {
        if let Claim::Held(owner, others) = claim {
            notes
                .entry(owner.clone())
                .or_default()
                .push(format!("holds cluster {cluster}, which {} also names", names(others)));
        }
    }
    for (site, note) in share_notes(shared, short) {
        notes.entry(site).or_default().push(note);
    }
    notes
        .into_iter()
        .map(|(site, notes)| (site, notes.join("; ")))
        .collect()
}

/// By peer, what waits of its catalog or whom it leaves short, with the share it is against.
fn share_notes(shared: &Shared, short: &Short<'_>) -> Vec<(String, String)> {
    let against = format!("share of {} for {} peers", shared.share, shared.peers);
    let mut notes: Vec<(String, String)> = short
        .iter()
        .map(|(site, Waiting { left_out, total })| {
            let rest = took_the_rest(shared, site);
            let note =
                format!("{left_out} of its {total} candidates wait, one per model, past this site's {against}; {rest}");
            ((*site).to_owned(), note)
        })
        .collect();
    let cut: Vec<&str> = short.keys().copied().collect();
    if !cut.is_empty() {
        for (site, taken) in shared
            .took
            .iter()
            .filter(|(site, _)| !short.contains_key(site.as_str()))
        {
            notes.push((
                site.clone(),
                format!(
                    "takes {taken} candidates over its {against}, leaving {} short",
                    names(&cut)
                ),
            ));
        }
    }
    notes
}

/// Who, other than `site`, took the ceiling's remainder, as `site` is told.
fn took_the_rest(shared: &Shared, site: &str) -> String {
    let takers: Vec<&str> = shared
        .took
        .iter()
        .filter(|(taker, _)| taker != site)
        .map(|(taker, _)| taker.as_str())
        .collect();
    if takers.is_empty() {
        "nothing of the overlay's ceiling is left".to_owned()
    } else {
        format!("{} took the rest", names(&takers))
    }
}

/// Who a cluster peers name belongs to.
#[derive(Debug, Eq, PartialEq)]
enum Claim<'held> {
    /// One peer names it.
    Sole,
    /// Several name it and one holds it by declaration: the hop bound to it, or its own name;
    /// the others are listed.
    Held(String, Vec<&'held str>),
    /// Several name it and nothing settles whose it is.
    Contested(Vec<&'held str>),
}

/// Each cluster `records` name, and who it belongs to.
type Claims<'held> = HashMap<&'held str, Claim<'held>>;

/// Who each cluster `records` name belongs to. A cluster bound to a site's gateway hop, or
/// named after a peer, is that site's by declaration; one that several peers name with no
/// such declaration is contested, and nobody routes it. This site's own clusters are
/// refused before any claim, so they are left out.
fn claimants<'held, Records>(records: Records, reserved: &HashSet<String>, hops: &ClusterSites) -> Claims<'held>
where
    Records: Iterator<Item = (&'held String, &'held WireProvider)>,
{
    // A set in name order, so a message lists the same peers the same way each round.
    let mut named = HashMap::<&str, BTreeSet<&str>>::new();
    for (site, provider) in records {
        let cluster = provider.routing_cluster.as_str();
        if !reserved.contains(cluster) {
            named.entry(cluster).or_default().insert(site.as_str());
        }
    }
    named
        .into_iter()
        .map(|(cluster, sites)| {
            let claim = if sites.len() == 1 {
                Claim::Sole
            } else {
                let declared = hops
                    .get(cluster)
                    .map(String::as_str)
                    .or_else(|| sites.contains(cluster).then_some(cluster));
                match declared {
                    Some(owner) => Claim::Held(
                        owner.to_owned(),
                        sites.iter().copied().filter(|site| *site != owner).collect(),
                    ),
                    None => Claim::Contested(sites.into_iter().collect()),
                }
            };
            (cluster, claim)
        })
        .collect()
}

/// Clusters each bound to the one site that may route to it.
pub type ClusterSites = HashMap<String, String>;

/// Why `site` may not route to `cluster`, if it may not: this site's own, another site's
/// gateway hop, another peer's by name, or contested with nothing to settle it.
fn refusal(
    cluster: &str,
    site: &str,
    (reserved, hops, claims): (&HashSet<String>, &ClusterSites, &Claims<'_>),
) -> Option<(&'static str, String)> {
    if reserved.contains(cluster) {
        return Some(("OwnCluster", "this site's own".to_owned()));
    }
    if let Some(bound) = hops.get(cluster).filter(|bound| *bound != site) {
        return Some(("HopClaimed", format!("bound to the gateway hop of site {bound}")));
    }
    match claims.get(cluster)? {
        Claim::Sole => None,
        Claim::Held(owner, _others) if owner == site => None,
        Claim::Held(owner, _others) => Some(("ClusterClaimed", format!("which site {owner} holds by name"))),
        Claim::Contested(sites) => {
            let others: Vec<&str> = sites.iter().copied().filter(|other| *other != site).collect();
            let others = names(&others);
            Some((
                "ClusterContested",
                format!(
                    "which {others} also names; a cluster routes to one site, so none of them routes it until they differ"
                ),
            ))
        },
    }
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "tests index verdicts by the peer they set up")]
mod tests {
    use super::*;

    #[test]
    fn a_version_always_names_the_very_body_served_across_a_publish() {
        let local = LocalProviders::default();
        local.replace("net", vec![wire("a", &[])]);
        let gold = BTreeMap::from([("tier".to_owned(), "gold".to_owned())]);
        for reader in [Reader::Local, Reader::Peer(&gold)] {
            let first = local.rendered(reader).unwrap_or_else(|| std::process::abort());
            assert_eq!(first.version, digest(&first.body), "rendered together");
            let again = local.rendered(reader).unwrap_or_else(|| std::process::abort());
            assert_eq!(
                (again.body, again.version),
                (first.body.clone(), first.version.clone()),
                "kept"
            );
        }
        let before = local.rendered(Reader::Local).unwrap_or_else(|| std::process::abort());
        local.replace("net", vec![wire("a", &[]), wire("b", &[])]);
        let after = local.rendered(Reader::Local).unwrap_or_else(|| std::process::abort());
        assert_ne!(after.version, before.version, "a publish drops what was kept");
        assert_eq!(after.version, digest(&after.body));
        local.replace("net", vec![wire("b", &[]), wire("a", &[])]);
        assert_eq!(
            local.rendered(Reader::Local).map(|rendered| rendered.version),
            Some(after.version),
            "an unchanged catalog keeps its body"
        );
    }

    fn wire(id: &str, labels: &[(&str, &str)]) -> WireProvider {
        WireProvider {
            network_id: "net".to_owned(),
            provider_id: id.to_owned(),
            routing_cluster: format!("{id}-cluster"),
            models: vec!["m".to_owned()],
            tools: vec!["search".to_owned()],
            backend_kind: "local".to_owned(),
            capacity_weight: 7,
            phase: crdt::ProviderPhase::Available,
            match_labels: labels.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect(),
            revision: 3,
        }
    }

    #[test]
    fn the_catalog_version_follows_its_content() {
        let local = LocalProviders::default();
        assert!(local.version().is_none(), "unpublished");
        local.replace("net", vec![wire("a", &[])]);
        let first = local.version();
        local.replace("net", vec![wire("a", &[])]);
        assert_eq!(local.version(), first, "the same content, the same version");
        local.replace("net", vec![wire("a", &[]), wire("b", &[])]);
        assert_ne!(local.version(), first, "changed content, a new version");
    }

    #[test]
    fn a_readers_version_follows_only_what_it_is_served() {
        let local = LocalProviders::default();
        let none = BTreeMap::new();
        let gold = BTreeMap::from([("tier".to_owned(), "gold".to_owned())]);
        local.replace("net", vec![wire("open", &[]), wire("scoped", &[("tier", "gold")])]);
        let (plain, golden) = (
            local.version_for(Reader::Peer(&none)),
            local.version_for(Reader::Peer(&gold)),
        );
        assert!(plain.is_some() && plain != golden, "two scopes, two versions");
        let mut changed = wire("scoped", &[("tier", "gold")]);
        changed.capacity_weight = 9;
        local.replace("net", vec![wire("open", &[]), changed]);
        assert_eq!(
            local.version_for(Reader::Peer(&none)),
            plain,
            "a change outside a reader's scope neither moves its version nor shows through it"
        );
        assert_ne!(
            local.version_for(Reader::Peer(&gold)),
            golden,
            "inside its scope it does"
        );
    }

    #[test]
    fn a_reader_takes_contract_one_and_refuses_another_major() {
        for (header, read) in [
            (None, true),
            (Some("1"), true),
            (Some("1.3"), true),
            (Some("2"), false),
            (Some("v1"), false),
        ] {
            assert_eq!(reads_contract(header), read, "{header:?}");
        }
    }

    #[test]
    fn nothing_is_served_until_a_network_is_published() {
        let local = LocalProviders::default();
        assert!(local.render(Reader::Local).is_none(), "unpublished, not empty");
        local.replace("net", Vec::new());
        assert_eq!(
            local.render(Reader::Local).as_deref(),
            Some(r#"{"providers":[]}"#),
            "whole, so no flag"
        );
    }

    fn served(local: &LocalProviders, reader: Reader<'_>) -> Vec<WireProvider> {
        parse(&local.render(reader).unwrap_or_else(|| std::process::abort())).unwrap_or_else(|| std::process::abort())
    }

    #[test]
    fn a_record_round_trips_with_its_tools_and_capacity() {
        let local = LocalProviders::default();
        local.replace("net", vec![wire("a", &[])]);
        assert_eq!(
            served(&local, Reader::Local),
            vec![wire("a", &[])],
            "tools and capacity_weight must survive the wire, unlike the gossip encoding"
        );
    }

    #[test]
    fn a_peer_without_the_labels_is_not_served_a_scoped_provider() {
        let local = LocalProviders::default();
        local.replace("net", vec![wire("open", &[]), wire("scoped", &[("tier", "gold")])]);
        let none = BTreeMap::new();
        let gold = BTreeMap::from([("tier".to_owned(), "gold".to_owned())]);
        let ids = |reader| {
            served(&local, reader)
                .into_iter()
                .map(|p| p.provider_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ids(Reader::Peer(&none)),
            ["open"],
            "an unlabelled peer sees only the open provider"
        );
        assert_eq!(
            ids(Reader::Peer(&gold)),
            ["open", "scoped"],
            "a satisfying peer sees both"
        );
        assert_eq!(
            ids(Reader::Local),
            ["open", "scoped"],
            "this site's gateway sees every provider"
        );
    }

    #[test]
    fn a_reader_keeps_its_own_network_and_names_the_polled_peer_whatever_the_body_claims() {
        let body = r#"{"providers":[{"networkId":"other","providerId":"p","routingCluster":"c","capacityWeight":1,
            "phase":"Available","revision":1,"siteId":"victim","writerId":"victim"}]}"#;
        let peers = PeerProviders::default();
        peers.replace("east", parse(body).unwrap_or_default());
        assert!(
            peers.remote_for("net", &HashSet::new()).is_empty(),
            "another network's record is not this one's"
        );
        let states = peers.remote_for("other", &HashSet::new());
        assert_eq!(states.len(), 1, "unknown fields are ignored, not fatal");
        assert!(
            states.iter().all(|s| s.site_id == "east" && s.writer_id == "east"),
            "the site comes from the poll, never the body: {states:?}"
        );
    }

    #[test]
    fn a_record_no_candidate_can_use_is_dropped() {
        let mut zero = wire("a", &[]);
        zero.capacity_weight = 0;
        let mut unnamed = wire("b", &[]);
        unnamed.routing_cluster.clear();
        let mut unprintable = wire("d", &[]);
        unprintable.routing_cluster = "c\u{1}".to_owned();
        let kept = [zero, unnamed, unprintable, wire("c", &[])]
            .into_iter()
            .filter_map(|provider| provider.into_state("east"))
            .count();
        assert_eq!(
            kept, 1,
            "an invalid capacity, an empty or a header-unsafe routing cluster is refused per record"
        );
    }

    #[test]
    fn an_oversized_or_overfull_body_is_refused() {
        let overfull = Body {
            providers: (0..=MAX_PROVIDERS)
                .map(|i| wire(&i.to_string(), &[]))
                .collect::<Vec<_>>(),
            truncated: false,
        };
        let overfull = serde_json::to_string(&overfull).unwrap_or_default();
        assert!(parse(&overfull).is_none(), "more than MAX_PROVIDERS records is refused");
        let oversized = format!(r#"{{"providers":[],"pad":"{}"}}"#, "x".repeat(MAX_BODY_BYTES));
        assert!(
            parse(&oversized).is_none(),
            "a body over MAX_BODY_BYTES is refused unparsed"
        );
        assert!(parse("not json").is_none(), "a malformed body is refused");
    }

    #[test]
    fn a_deleted_network_is_no_longer_served() {
        let local = LocalProviders::default();
        local.replace("alpha", vec![wire("a", &[])]);
        local.replace("beta", vec![wire("b", &[])]);
        let both = local.version();
        local.retain_networks(|network| network == "alpha");
        assert_ne!(local.version(), both, "the version follows what is served");
        assert_eq!(served(&local, Reader::Local).len(), 1);
        local.retain_networks(|_| false);
        assert!(
            local.render(Reader::Local).is_none(),
            "nothing held reads as unpublished"
        );
        assert!(local.version().is_none());
    }

    #[test]
    fn a_truncated_catalog_keeps_a_share_of_every_network() {
        let local = LocalProviders::default();
        let records = |network: &str, count: usize| -> Vec<WireProvider> {
            (0..count)
                .map(|n| {
                    let mut record = wire(&format!("{network}-{n:04}"), &[]);
                    network.clone_into(&mut record.network_id);
                    record
                })
                .collect()
        };
        local.replace("alpha", records("alpha", MAX_PROVIDERS));
        local.replace("beta", records("beta", 3));
        let served = served(&local, Reader::Local);
        assert_eq!(served.len(), MAX_PROVIDERS, "cut at the record bound");
        assert_eq!(
            served.iter().filter(|record| record.network_id == "beta").count(),
            3,
            "the small network is not crowded out by the large one"
        );
    }

    #[test]
    fn a_large_catalog_is_served_within_what_a_reader_accepts() {
        let local = LocalProviders::default();
        let mut wide = wire("w", &[]);
        wide.tools = (0..128).map(|i| format!("{i:0>250}")).collect();
        local.replace(
            "net",
            (0..MAX_PROVIDERS + 10)
                .map(|i| {
                    let mut p = wide.clone();
                    p.provider_id = format!("{i:04}");
                    p
                })
                .collect(),
        );
        let body = local.render(Reader::Local).unwrap_or_default();
        assert!(
            body.len() <= MAX_BODY_BYTES,
            "the body is capped at {MAX_BODY_BYTES}: {}",
            body.len()
        );
        assert!(body.ends_with(r#"],"truncated":true}"#), "a capped body says so");
        let kept = parse(&body).unwrap_or_default();
        assert!(!kept.is_empty(), "a capped body still serves the first records");
        assert_eq!(
            kept.first().map(|p| p.provider_id.as_str()),
            Some("0000"),
            "kept in provider order"
        );
    }

    #[test]
    fn a_peer_record_naming_a_cluster_it_does_not_own_is_dropped() {
        let peers = PeerProviders::default();
        let mut taken = wire("t", &[]);
        "local-vllm".clone_into(&mut taken.routing_cluster);
        let mut contested = wire("c", &[]);
        "shared".clone_into(&mut contested.routing_cluster);
        peers.replace("east", vec![wire("a", &[]), taken, contested.clone()]);
        peers.replace("west", vec![wire("b", &[]), contested]);
        let reserved = HashSet::from(["local-vllm".to_owned()]);
        let kept: Vec<(String, String)> = peers
            .remote_for("net", &reserved)
            .into_iter()
            .map(|state| (state.site_id, state.routing_cluster))
            .collect();
        assert_eq!(
            kept,
            [
                ("east".to_owned(), "a-cluster".to_owned()),
                ("west".to_owned(), "b-cluster".to_owned())
            ],
            "a local cluster is dropped, and a cluster two peers name with nothing to settle it routes to neither"
        );
        let refused: BTreeMap<_, _> = peers.verdicts().into_iter().collect();
        let reasons: Vec<&str> = refused.values().map(Verdict::reason).collect();
        assert_eq!(reasons, ["OwnCluster", "ClusterContested"], "east's, then west's");
        assert!(
            refused["west"].message().contains("peer east also names"),
            "the contesting peer is named: {}",
            refused["west"].message()
        );
    }

    fn routes(peers: &PeerProviders, hops: &ClusterSites) -> Vec<(String, String)> {
        peers
            .admit("net", &HashSet::new(), hops)
            .into_iter()
            .map(|state| (state.site_id, state.routing_cluster))
            .collect()
    }

    #[test]
    fn a_record_naming_too_many_models_is_refused() {
        let mut crowded = wire("x", &[]);
        crowded.models = (0..=MAX_MODELS_PER_RECORD).map(|n| format!("m{n}")).collect();
        assert!(crowded.clone().into_state("east").is_none());
        crowded.models.truncate(MAX_MODELS_PER_RECORD);
        assert!(crowded.into_state("east").is_some(), "the cap itself is allowed");
    }

    fn catalog(prefix: &str, records: usize, models: usize) -> Vec<WireProvider> {
        (0..records)
            .map(|n| {
                let mut record = wire(&format!("{prefix}{n}"), &[]);
                record.models = (0..models).map(|m| format!("m{m}")).collect();
                record
            })
            .collect()
    }

    /// What each site routes, as admitted.
    fn admitted(peers: &PeerProviders) -> Vec<crdt::ProviderState> {
        peers.admit("net", &HashSet::new(), &ClusterSites::new())
    }

    /// The candidates each site routes, by site.
    fn routed_candidates(peers: &PeerProviders) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for record in admitted(peers) {
            *counts.entry(record.site_id).or_insert(0) += record.models.len().max(1);
        }
        counts
    }

    /// East and west each past a third of the ceiling, east first; north within its share.
    fn east_west_north() -> PeerProviders {
        let peers = PeerProviders::default();
        peers.replace("east", catalog("e", 9, MAX_MODELS_PER_RECORD));
        peers.replace("west", catalog("w", 10, MAX_MODELS_PER_RECORD));
        peers.replace("north", catalog("n", 1, 1));
        peers
    }

    #[test]
    fn a_peer_past_the_overlay_ceiling_routes_up_to_what_it_may_contribute() {
        let peers = east_west_north();
        assert_eq!(
            routed_candidates(&peers),
            BTreeMap::from([
                ("east".to_owned(), 576),
                ("west".to_owned(), 447),
                ("north".to_owned(), 1)
            ]),
            "east arrived first and takes the remainder; west gets its share plus what is left; north fits"
        );
        let verdicts = peers.verdicts();
        let (west, east) = (&verdicts["west"], &verdicts["east"]);
        let west_told = "193 of its 640 candidates wait, one per model, past this site's share of 341 for 3 peers; peer east took the rest";
        let east_told = "takes 235 candidates over its share of 341 for 3 peers, leaving peer west short";
        assert!(west.accepted() && west.message() == west_told, "{west:?}");
        assert!(east.accepted() && east.message() == east_told, "{east:?}");
        assert_eq!(
            verdicts.len(),
            2,
            "a peer within its share and in nobody's way is clear"
        );
    }

    #[test]
    fn a_cut_catalog_keeps_the_same_records_and_models_each_round() {
        let peers = east_west_north();
        let records: Vec<crdt::ProviderState> = admitted(&peers)
            .into_iter()
            .filter(|record| record.site_id == "west")
            .collect();
        assert_eq!(
            records
                .iter()
                .map(|record| record.provider_id.as_str())
                .collect::<Vec<_>>(),
            ["w0", "w1", "w2", "w3", "w4", "w5", "w6"],
            "records in id order, the cut one last"
        );
        assert_eq!(
            records[6].models,
            (0..64)
                .map(|m| format!("m{m}"))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .take(63)
                .collect::<Vec<_>>(),
            "the cut record keeps its models in name order"
        );
    }

    #[test]
    fn a_thousand_model_peer_stays_partly_routable_when_declared_peers_arrive() {
        let peers = PeerProviders::default();
        peers.declare(3);
        peers.replace("big", catalog("b", 20, 50));
        peers.replace("east", catalog("e", 5, 60));
        peers.replace("west", catalog("w", 5, 60));
        assert_eq!(
            routed_candidates(&peers),
            BTreeMap::from([
                ("big".to_owned(), 424),
                ("east".to_owned(), 300),
                ("west".to_owned(), 300)
            ]),
            "big keeps its share of 341 plus the 83 left once the others have theirs"
        );
        let big = &peers.verdicts()["big"];
        assert!(
            big.accepted()
                && big
                    .message()
                    .contains("576 of its 1000 candidates wait, one per model, past this site's share of 341 for 3 peers; nothing of the overlay's ceiling is left"),
            "the exact count waits, nobody took the rest: {big:?}"
        );
        assert!(
            !peers.verdicts().contains_key("east"),
            "a peer within its share is clear"
        );
    }

    #[test]
    fn a_record_with_no_models_is_one_candidate_and_is_dropped_whole() {
        let peers = PeerProviders::default();
        peers.declare(2);
        peers.replace("bare", catalog("b", 600, 0));
        assert_eq!(
            routes(&peers, &ClusterSites::new()).len(),
            600,
            "alone, the remainder covers it"
        );
        peers.replace("other", catalog("o", 8, MAX_MODELS_PER_RECORD));
        let bare: Vec<crdt::ProviderState> = admitted(&peers)
            .into_iter()
            .filter(|record| record.site_id == "bare")
            .collect();
        assert_eq!(
            bare.len(),
            512,
            "its share of 512, one candidate per record, nothing left to take"
        );
        assert!(
            bare.iter().all(|record| record.models.is_empty()),
            "nothing to cut within a record"
        );
        assert_eq!(
            peers.verdicts()["bare"].message(),
            "88 of its 600 candidates wait, one per model, past this site's share of 512 for 2 peers; nothing of the overlay's ceiling is left"
        );
    }

    #[test]
    fn a_greedy_first_peer_cannot_take_every_declared_peers_share() {
        let peers = PeerProviders::default();
        peers.declare(3);
        peers.replace("greedy", catalog("g", 16, MAX_MODELS_PER_RECORD));
        assert_eq!(
            routes(&peers, &ClusterSites::new()).len(),
            16,
            "alone, the whole remainder is its to take"
        );
        peers.replace("north", catalog("n", 1, 1));
        assert_eq!(
            routed_candidates(&peers),
            BTreeMap::from([("greedy".to_owned(), 1023), ("north".to_owned(), 1)]),
            "the late peer keeps its share; the greedy one gives up exactly what the late one takes"
        );
        assert!(
            peers.verdicts()["greedy"]
                .message()
                .contains("1 of its 1024 candidates wait"),
            "{:?}",
            peers.verdicts()["greedy"]
        );
        peers.replace("greedy", catalog("g", 5, MAX_MODELS_PER_RECORD));
        assert_eq!(
            routes(&peers, &ClusterSites::new()).len(),
            6,
            "within its share, it is clear"
        );
        assert!(peers.verdicts().is_empty());
    }

    #[test]
    fn an_admission_replaces_the_verdicts_of_the_last() {
        let peers = PeerProviders::default();
        let mut taken = wire("t", &[]);
        "local-vllm".clone_into(&mut taken.routing_cluster);
        peers.replace("east", vec![taken]);
        let reserved = HashSet::from(["local-vllm".to_owned()]);
        drop(peers.remote_for("net", &reserved));
        assert_eq!(peers.verdicts()["east"].reason(), "OwnCluster");
        peers.replace("east", vec![wire("a", &[])]);
        drop(peers.remote_for("net", &reserved));
        assert!(peers.verdicts().is_empty(), "a clean admission clears the last verdict");
    }

    #[test]
    fn many_honest_peers_are_all_kept() {
        let peers = PeerProviders::default();
        for n in 0..100 {
            peers.replace(&format!("site-{n}"), catalog(&format!("s{n}-"), 1, 10));
        }
        assert_eq!(
            routes(&peers, &ClusterSites::new()).len(),
            100,
            "100 peers of 10 models fit the ceiling"
        );
        assert!(peers.verdicts().is_empty());
    }

    #[test]
    fn a_contested_cluster_routes_to_no_peer_until_one_stops_naming_it() {
        let peers = PeerProviders::default();
        let mut contested = wire("c", &[]);
        "shared".clone_into(&mut contested.routing_cluster);
        peers.replace("west", vec![contested.clone()]);
        assert_eq!(
            routes(&peers, &ClusterSites::new()),
            [("west".to_owned(), "shared".to_owned())]
        );
        peers.replace("east", vec![contested]);
        assert_eq!(
            routes(&peers, &ClusterSites::new()),
            [],
            "two peers name a cluster nothing settles, so neither routes it, whoever came first"
        );
        let verdicts = peers.verdicts();
        let (east, west) = (&verdicts["east"], &verdicts["west"]);
        assert!(
            east.reason() == "ClusterContested"
                && east.message().contains("peer west also names")
                && west.message().contains("peer east also names"),
            "each is told who else names it: {east:?} {west:?}"
        );
        peers.retain_sites(|site| site == "east");
        assert_eq!(
            routes(&peers, &ClusterSites::new()),
            [("east".to_owned(), "shared".to_owned())],
            "once west no longer names it, east routes it"
        );
        assert!(peers.verdicts().is_empty());
    }

    #[test]
    fn a_cluster_named_after_a_peer_is_that_peers_whoever_else_names_it() {
        let peers = PeerProviders::default();
        let mut wests = wire("w", &[]);
        "west".clone_into(&mut wests.routing_cluster);
        peers.replace("east", vec![wests.clone()]);
        peers.replace("west", vec![wests]);
        assert_eq!(
            routes(&peers, &ClusterSites::new()),
            [("west".to_owned(), "west".to_owned())],
            "the cluster named west is west's by declaration, however the catalogs arrived"
        );
        let verdicts = peers.verdicts();
        assert_eq!(verdicts["east"].reason(), "ClusterClaimed");
        assert!(
            verdicts["east"].message().contains("which site west holds by name"),
            "the holder is named: {}",
            verdicts["east"].message()
        );
        assert!(
            verdicts["west"].accepted()
                && verdicts["west"]
                    .message()
                    .contains("holds cluster west, which peer east also names"),
            "the holder is told who else names it: {:?}",
            verdicts["west"]
        );
    }

    #[test]
    fn a_gateway_hop_cluster_routes_only_for_the_site_it_leads_to() {
        let peers = PeerProviders::default();
        let mut hop = wire("h", &[]);
        "hop-east".clone_into(&mut hop.routing_cluster);
        peers.replace("west", vec![hop.clone()]);
        peers.replace("east", vec![hop]);
        let hops = ClusterSites::from([("hop-east".to_owned(), "east".to_owned())]);
        assert_eq!(
            routes(&peers, &hops),
            [("east".to_owned(), "hop-east".to_owned())],
            "west may not send east's hop traffic to itself, even claiming it first"
        );
        let verdicts = peers.verdicts();
        assert_eq!(verdicts["west"].reason(), "HopClaimed");
        assert!(
            verdicts["west"].message().contains("gateway hop of site east"),
            "the bound site is named: {}",
            verdicts["west"].message()
        );
    }

    /// A catalog arriving, changing or being forgotten wakes the renderer; a read of the
    /// same catalog does not.
    #[tokio::test]
    async fn a_catalog_change_is_signalled_and_an_unchanged_read_is_not() {
        let peers = PeerProviders::default();
        let mut changes = Box::pin(peers.changes());
        let mut next = || futures::FutureExt::now_or_never(futures::StreamExt::next(&mut changes)).flatten();
        assert_eq!(next(), None, "nothing held, nothing signalled");
        peers.replace("east", vec![wire("a", &[])]);
        assert_eq!(next(), Some(()), "a first catalog");
        peers.replace("east", vec![wire("a", &[])]);
        assert_eq!(next(), None, "the same catalog read again");
        peers.replace("east", vec![wire("b", &[])]);
        peers.replace("west", vec![wire("c", &[])]);
        assert_eq!(next(), Some(()), "two changes, one wake");
        assert_eq!(next(), None);
        peers.retain_sites(|site| site == "east" || site == "west");
        assert_eq!(next(), None, "nothing forgotten");
        peers.retain_sites(|site| site == "east");
        assert_eq!(next(), Some(()), "a peer forgotten");
    }

    #[test]
    fn peer_providers_are_stamped_for_the_reader_and_forgotten_with_their_peer() {
        let peers = PeerProviders::default();
        peers.replace("east", vec![wire("a", &[])]);
        peers.replace("west", vec![wire("b", &[])]);
        let remote = peers.remote_for("net", &HashSet::new());
        assert_eq!(
            remote
                .iter()
                .map(|provider| (provider.network_id.as_str(), provider.site_id.as_str()))
                .collect::<Vec<_>>(),
            [("net", "east"), ("net", "west")],
            "each record is the reader's network and the peer it came from"
        );
        peers.retain_sites(|site| site != "east");
        assert_eq!(
            peers
                .remote_for("net", &HashSet::new())
                .iter()
                .map(|provider| provider.site_id.as_str())
                .collect::<Vec<_>>(),
            ["west"],
            "a departed peer's providers go with it"
        );
    }
}
