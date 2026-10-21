//! Atomic provider batches and retained revision watermarks.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    Dropped, Entry, Ingested, LoadStore, MAX_METRICS_PER_PROVIDER, MAX_PROVIDERS, Observation, Provider, ScrapeClock,
    exposition, parse_sample, push_observation, rebase_age,
};
use crate::revision::{self, Revision};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Bound staged borrowed rows and auxiliary maps independently of HTTP limits.
const MAX_RESPONSE_BYTES: usize = 8_388_608; // 8 MiB
/// Bound parser work and observation staging per response.
const MAX_RESPONSE_LINES: usize = 32_768;

/// Bounded durable-in-process publication fences for attributed sites.
pub(super) type Orders = BTreeMap<Box<str>, (u64, u64)>;

/// Revision qualification before and after a multi-query signal read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LearningState {
    /// Stable learning lifetime, absent when observations are unqualified.
    pub epoch: Option<u64>,
    /// Changes whenever committed provider state changes.
    pub serial: u64,
}

impl LoadStore {
    /// Current learning qualification, with a caller-selected freshness bound.
    ///
    /// Compare the complete value before and after related metric queries to
    /// reject a read spanning publication changes. Legacy stores return epoch
    /// zero. Expiry retires learning but retains pressure and order watermarks.
    #[must_use]
    pub fn learning_state(&self, key: &str, now_ms: i64, max_age_ms: i64) -> LearningState {
        let Some(bound) = self.revision_max_age_ms else {
            return LearningState {
                epoch: Some(0),
                serial: 0,
            };
        };
        let Some(mut provider) = self.providers.get_mut(key) else {
            return LearningState { epoch: None, serial: 0 };
        };
        if provider.guard.epoch.is_some() && !provider.guard.fresh(now_ms, bound.min(max_age_ms)) {
            provider.disqualify();
        }
        LearningState {
            epoch: provider.guard.epoch,
            serial: provider.guard.serial,
        }
    }

    /// Stage the entire response before committing any provider publication.
    pub(super) fn ingest_guarded(&self, text: &str, scrape: &ScrapeClock<'_>, max_age_ms: i64) -> Ingested {
        let mut orders = self
            .revision_orders
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let staged = stage(text, scrape);
        let Some(incoming) = advance_orders(self, &mut orders, &staged.batches) else {
            self.reject_response(scrape);
            return Ingested {
                capped: 1,
                ..Ingested::default()
            };
        };
        self.remember_fences(&staged.batches, &orders);
        if staged.invalid {
            self.reject_response(scrape);
            return staged.rejected;
        }
        self.invalidate_missing(&staged.batches, scrape, &incoming, &orders);
        let mut tally = self.commit_response(staged.batches, scrape, max_age_ms, &orders);
        tally.skewed = staged.rejected.skewed;
        self.retire_superseded(&orders);
        drop(orders);
        tally
    }

    /// A parse failure without trustworthy attribution cannot partially commit.
    fn reject_response(&self, scrape: &ScrapeClock<'_>) {
        for mut held in self.providers.iter_mut() {
            if scrape.local || held.key().split_once('/').is_some_and(|(site, _)| site == scrape.owner) {
                held.reject();
            }
        }
    }

    /// Only unique, semantically valid metadata can advance site authority.
    fn publication_order(&self, key: &str, batch: &Batch<'_>) -> Option<(u64, u64)> {
        if batch.invalid_identity {
            return None;
        }
        let revision = batch.revision.as_ref()?;
        let rollback = self
            .providers
            .get(key)
            .is_some_and(|provider| provider.rollback(revision));
        (!rollback).then(|| revision.order())
    }

    /// Valid control metadata fences old responses even if its payload is lost.
    fn remember_fences(&self, batches: &BTreeMap<Box<str>, Batch<'_>>, orders: &BTreeMap<Box<str>, (u64, u64)>) {
        for (key, batch) in batches {
            let Some(order) = self.publication_order(key, batch) else {
                continue;
            };
            if orders.get(batch.site.as_str()).is_some_and(|held| order < *held) {
                continue;
            }
            let Some(revision) = &batch.revision else { continue };
            let mut provider = match self.providers.entry(key.clone()) {
                Entry::Occupied(entry) => entry.into_ref(),
                Entry::Vacant(entry) => {
                    if !self.admit_new_provider(&batch.site) {
                        continue;
                    }
                    entry.insert(Provider::default())
                },
            };
            provider.observe(revision);
        }
    }

    /// Old cached provider groups cannot retain authority from a prior site pass.
    fn retire_superseded(&self, orders: &BTreeMap<Box<str>, (u64, u64)>) {
        for mut held in self.providers.iter_mut() {
            if held.guard.epoch.is_some()
                && let Some(revision) = &held.guard.newest
                && held
                    .key()
                    .split_once('/')
                    .and_then(|(site, _)| orders.get(site))
                    .is_some_and(|order| revision.order() < *order)
            {
                held.disqualify();
            }
        }
    }

    /// Apply staged groups against the highest publication seen for each site.
    fn commit_response(
        &self,
        batches: BTreeMap<Box<str>, Batch<'_>>,
        scrape: &ScrapeClock<'_>,
        max_age_ms: i64,
        orders: &BTreeMap<Box<str>, (u64, u64)>,
    ) -> Ingested {
        let mut tally = Ingested::default();
        for (key, batch) in batches {
            let skewed = matches!(batch.refusal, Some(Dropped::Skewed));
            let count = u64::try_from(batch.rows.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::from(batch.revision.is_some()));
            let old = batch.revision.as_ref().is_some_and(|revision| {
                orders
                    .get(batch.site.as_str())
                    .is_some_and(|order| revision.order() < *order)
            });
            let result = if old {
                Commit::Rejected
            } else {
                self.commit(key, batch, scrape, max_age_ms)
            };
            if skewed {
                continue;
            }
            let slot = match result {
                Commit::Accepted => &mut tally.kept,
                Commit::Duplicate => continue,
                Commit::Capped => &mut tally.capped,
                Commit::Rejected => &mut tally.unparsed,
            };
            *slot = slot.saturating_add(count.max(1));
        }
        tally
    }

    /// Commit all validated rows while holding exactly one provider entry lock.
    fn commit(&self, key: Box<str>, batch: Batch<'_>, scrape: &ScrapeClock<'_>, max_age_ms: i64) -> Commit {
        let mut provider = match self.providers.entry(key) {
            Entry::Occupied(entry) => entry.into_ref(),
            Entry::Vacant(entry) => {
                if !self.admit_new_provider(&batch.site) {
                    return Commit::Capped;
                }
                entry.insert(Provider::default())
            },
        };
        if provider.duplicate(&batch) {
            return Commit::Duplicate;
        }
        if !provider.accept(&batch, scrape, max_age_ms) {
            return Commit::Rejected;
        }
        if !provider.fits(&batch) {
            provider.invalidate();
            return Commit::Rejected;
        }
        for mut row in batch.rows {
            let Some(at_ms) = rebase_age(scrape.reference_ms, row.origin_ms, scrape.local_now_ms) else {
                continue;
            };
            row.sample.at_ms = at_ms;
            push_observation(&mut provider, &row, (self.combine)(row.metric), self.window);
        }
        drop(provider);
        Commit::Accepted
    }

    /// A complete direct response owns its site; the local aggregate owns all
    /// attributed rows. Missing providers lose qualification, not watermarks.
    fn invalidate_missing(
        &self,
        batches: &BTreeMap<Box<str>, Batch<'_>>,
        scrape: &ScrapeClock<'_>,
        incoming: &BTreeMap<Box<str>, (u64, u64)>,
        orders: &BTreeMap<Box<str>, (u64, u64)>,
    ) {
        for mut held in self.providers.iter_mut() {
            let Some((site, _)) = held.key().split_once('/') else {
                continue;
            };
            let owns = scrape.local || site == scrape.owner;
            let outdated = incoming
                .get(site)
                .zip(orders.get(site))
                .is_some_and(|(next, current)| next < current);
            if owns && !outdated && !batches.contains_key(held.key()) {
                held.disqualify();
            }
        }
    }
}

/// Retain site-wide authority across omitted providers and never evict fences.
fn advance_orders(
    store: &LoadStore,
    orders: &mut BTreeMap<Box<str>, (u64, u64)>,
    batches: &BTreeMap<Box<str>, Batch<'_>>,
) -> Option<Orders> {
    let mut incoming = BTreeMap::<Box<str>, (u64, u64)>::new();
    for (key, batch) in batches {
        if let Some(order) = store.publication_order(key, batch) {
            incoming
                .entry(batch.site.as_str().into())
                .and_modify(|held| *held = (*held).max(order))
                .or_insert(order);
        }
    }
    if orders
        .len()
        .saturating_add(incoming.keys().filter(|site| !orders.contains_key(*site)).count())
        > MAX_PROVIDERS
    {
        return None;
    }
    for (site, order) in &incoming {
        orders
            .entry(site.clone())
            .and_modify(|held| *held = (*held).max(*order))
            .or_insert(*order);
    }
    Some(incoming)
}

/// Outcome of an atomic provider commit.
enum Commit {
    /// All rows were accepted.
    Accepted,
    /// A complete batch matching retained publication metadata; neither stored nor dropped.
    Duplicate,
    /// The bounded store refused a new provider.
    Capped,
    /// The batch failed validation or publication ordering.
    Rejected,
}

/// Qualification and ordering retained even when windows are retired.
#[derive(Debug, Default)]
pub(super) struct Guard {
    /// Locally rebased time of the last accepted complete batch.
    accepted_at: Option<i64>,
    /// Stable learning lifetime while qualification remains valid.
    epoch: Option<u64>,
    /// Highest unique valid control record, even without a usable payload.
    fence: Option<Revision>,
    /// Highest accepted publication, never cleared by logical invalidation.
    newest: Option<Revision>,
    /// Read-consistency version, incremented under the provider lock.
    serial: u64,
}

impl Guard {
    /// Whether the held identity is within the age bound, including commits newer than a reader's clock.
    fn fresh(&self, now_ms: i64, max_age_ms: i64) -> bool {
        self.accepted_at
            .is_some_and(|at_ms| now_ms.saturating_sub(at_ms).max(0) <= max_age_ms)
    }
}

impl Provider {
    /// Repeated polls of a complete publication cannot refresh its age or qualification.
    fn duplicate(&self, batch: &Batch<'_>) -> bool {
        batch
            .revision
            .as_ref()
            .zip(self.guard.newest.as_ref())
            .is_some_and(|(next, held)| {
                next.order() == held.order()
                    && next.uid == held.uid
                    && next.generation == held.generation
                    && next.batch_samples == held.batch_samples
            })
            && batch.refusal.is_none()
            && !batch.invalid_identity
            && batch.complete()
    }

    /// Consume observed authority even when this publication has no usable data.
    fn reject(&mut self) {
        if let Some(fence) = &self.guard.fence {
            self.guard.newest = Some(fence.clone());
        }
        self.invalidate();
    }

    /// A UID's generations never roll back, including after failed collection.
    fn rollback(&self, revision: &Revision) -> bool {
        self.guard
            .fence
            .as_ref()
            .or(self.guard.newest.as_ref())
            .is_some_and(|held| revision.uid == held.uid && revision.generation < held.generation)
    }

    /// Remember authority independently from accepting its payload.
    fn observe(&mut self, revision: &Revision) {
        if self
            .guard
            .fence
            .as_ref()
            .is_some_and(|held| held.order() >= revision.order())
        {
            return;
        }
        if self.guard.fence.as_ref().is_some_and(|held| {
            held.uid != revision.uid
                || held.generation != revision.generation
                || held.publisher_epoch != revision.publisher_epoch
        }) {
            self.invalidate();
        }
        self.guard.fence = Some(revision.clone());
    }

    /// Ensure a complete batch cannot be partially dropped by the metric cap.
    fn fits(&self, batch: &Batch<'_>) -> bool {
        let names: BTreeSet<_> = self
            .metrics
            .keys()
            .map(AsRef::as_ref)
            .chain(batch.rows.iter().map(|row| row.metric))
            .collect();
        names.len() <= MAX_METRICS_PER_PROVIDER
    }

    /// Retire qualification while leaving independently fresh pressure intact.
    fn disqualify(&mut self) {
        self.guard.epoch = None;
        self.guard.accepted_at = None;
        self.guard.serial = self.guard.serial.saturating_add(1);
    }

    /// Retire windows and qualification without forgetting publication order.
    fn invalidate(&mut self) {
        self.metrics.clear();
        self.disqualify();
    }

    /// Decide one complete publication while the provider entry remains locked.
    fn accept(&mut self, batch: &Batch<'_>, scrape: &ScrapeClock<'_>, max_age_ms: i64) -> bool {
        if batch.revision.as_ref().is_some_and(|revision| self.rollback(revision)) {
            return false;
        }
        if batch
            .revision
            .as_ref()
            .zip(self.guard.newest.as_ref())
            .is_some_and(|(next, held)| next.order() <= held.order())
        {
            return false;
        }
        if batch.refusal.is_some() || !batch.complete() || self.guard.serial == u64::MAX {
            self.reject();
            return false;
        }
        if let Some(revision) = &batch.revision {
            if revision.batch_samples == 0 {
                self.guard.newest = Some(revision.clone());
                self.invalidate();
                return true;
            }
            self.qualify(revision, scrape, max_age_ms)
        } else {
            if self.guard.epoch.is_some() || self.guard.accepted_at.is_some() {
                self.metrics.clear();
            }
            self.disqualify();
            true
        }
    }

    /// Qualify a newer complete publication without importing old windows.
    fn qualify(&mut self, revision: &Revision, scrape: &ScrapeClock<'_>, max_age_ms: i64) -> bool {
        let Some(at_ms) = rebase_age(scrape.reference_ms, revision.timestamp_ms, scrape.local_now_ms) else {
            self.invalidate();
            return false;
        };
        let changed = self.guard.newest.as_ref().is_none_or(|held| {
            held.uid != revision.uid
                || held.generation != revision.generation
                || held.publisher_epoch != revision.publisher_epoch
        });
        if changed || self.guard.epoch.is_none() || !self.guard.fresh(scrape.local_now_ms, max_age_ms) {
            self.invalidate();
        }
        self.guard.serial = self.guard.serial.saturating_add(1);
        self.guard.newest = Some(revision.clone());
        if !(0..=max_age_ms).contains(&scrape.local_now_ms.saturating_sub(at_ms)) {
            self.invalidate();
            return false;
        }
        self.guard.accepted_at = Some(at_ms);
        self.guard.epoch.get_or_insert(self.guard.serial);
        true
    }
}

/// One provider's staged response rows, with at most one metadata row.
#[derive(Default)]
struct Batch<'text> {
    /// Only unique strictly valid metadata may advance ordering fences.
    invalid_identity: bool,
    /// Provider-local rejection, with age failures counted separately.
    refusal: Option<Dropped>,
    /// Validated metadata, absent for an unqualified legacy publisher.
    revision: Option<Revision>,
    /// Borrowed payload observations.
    rows: Vec<Observation<'text>>,
    /// Verified or locally attributed owner for admission bounds.
    site: String,
}

impl<'text> Batch<'text> {
    /// Keep one metadata record and a bounded borrowed payload.
    fn push(&mut self, observation: Observation<'text>, line: &str, now: std::time::SystemTime) {
        if observation.metric == revision::METRIC {
            let parsed = exposition::parse(line)
                .and_then(|metric| revision::parse(&metric))
                .filter(|revision| revision::valid_epoch(revision.publisher_epoch, now));
            self.invalid_identity |= parsed.is_none() || self.revision.is_some();
            if self.invalid_identity {
                self.refusal.get_or_insert(Dropped::Unparsed);
            }
            self.revision = parsed;
        } else if self.rows.len() < revision::MAX_BATCH_SAMPLES {
            self.rows.push(observation);
        } else {
            self.refusal.get_or_insert(Dropped::Unparsed);
        }
    }

    /// Require a complete common-timestamp payload when metadata is present.
    fn complete(&self) -> bool {
        let names: BTreeSet<_> = self.rows.iter().map(|row| row.metric).collect();
        let series: BTreeSet<_> = self.rows.iter().map(|row| (row.metric, row.labels)).collect();
        if names.len() > MAX_METRICS_PER_PROVIDER || series.len() != self.rows.len() {
            return false;
        }
        self.revision.as_ref().is_none_or(|revision| {
            revision.batch_samples == self.rows.len()
                && self.rows.iter().all(|row| row.origin_ms == revision.timestamp_ms)
        })
    }
}

/// Bounded staging retains observed control fences even if payload parsing fails.
#[derive(Default)]
struct Staged<'text> {
    /// Attributable groups parsed from the response.
    batches: BTreeMap<Box<str>, Batch<'text>>,
    /// A refusal without trustworthy attribution prevents all payload commits.
    invalid: bool,
    /// Typed refusals, including provider-local age failures.
    rejected: Ingested,
}

impl<'text> Staged<'text> {
    /// Record a refusal without losing already observed publication fences.
    fn reject(&mut self, cause: Dropped) {
        let slot = match cause {
            Dropped::Capped => &mut self.rejected.capped,
            Dropped::Mismatched => &mut self.rejected.mismatched,
            Dropped::Skewed => &mut self.rejected.skewed,
            Dropped::Unparsed => &mut self.rejected.unparsed,
        };
        *slot = slot.saturating_add(1);
    }

    /// Attribute a row and retain its metadata before checking timestamp age.
    fn push(&mut self, line: &'text str, scrape: &ScrapeClock<'_>, now: std::time::SystemTime) -> Result<(), Dropped> {
        let observation = attributed_row(line, scrape)?;
        let site = observation.site.as_deref().ok_or(Dropped::Unparsed)?;
        let key = LoadStore::key(site, &observation.cluster);
        if !self.batches.contains_key(&key) && self.batches.len() >= MAX_PROVIDERS {
            return Err(Dropped::Capped);
        }
        let skewed = rebase_age(scrape.reference_ms, observation.origin_ms, scrape.local_now_ms).is_none();
        let batch = self.batches.entry(key).or_insert_with(|| Batch {
            site: site.to_owned(),
            ..Batch::default()
        });
        batch.push(observation, line, now);
        if skewed {
            batch.refusal = Some(Dropped::Skewed);
            self.reject(Dropped::Skewed);
        }
        Ok(())
    }
}

/// Attribute and bound every row before any payload can be mutated.
fn stage<'text>(text: &'text str, scrape: &ScrapeClock<'_>) -> Staged<'text> {
    let mut staged = Staged::default();
    let now = std::time::SystemTime::now();
    if text.len() > MAX_RESPONSE_BYTES {
        staged.invalid = true;
        staged.reject(Dropped::Capped);
        return staged;
    }
    for (index, line) in text.lines().enumerate() {
        if index >= MAX_RESPONSE_LINES {
            staged.invalid = true;
            staged.reject(Dropped::Capped);
            break;
        }
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        if let Err(cause) = staged.push(line, scrape, now) {
            staged.invalid = true;
            staged.reject(cause);
        }
    }
    staged
}

/// Parse a row only when its target belongs to the authenticated response.
fn attributed_row<'text>(line: &'text str, scrape: &ScrapeClock<'_>) -> Result<Observation<'text>, Dropped> {
    let mut observation = parse_sample(line).ok_or(Dropped::Unparsed)?;
    observation.labels = ordered_labels(&exposition::parse(line).ok_or(Dropped::Unparsed)?).ok_or(Dropped::Unparsed)?;
    let site = observation.site.as_deref().ok_or(Dropped::Unparsed)?;
    if !revision::valid_target(site) || !revision::valid_target(&observation.cluster) {
        return Err(Dropped::Unparsed);
    }
    if !scrape.local && site != scrape.owner {
        return Err(Dropped::Mismatched);
    }
    Ok(observation)
}

/// Reject duplicate labels and make payload identity independent of label order.
fn ordered_labels(metric: &exposition::Metric<'_>) -> Option<u64> {
    let mut labels = BTreeMap::new();
    for (name, value) in metric.labels() {
        if labels.insert(name, value).is_some() {
            return None;
        }
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (name, value) in labels {
        std::hash::Hash::hash(&(name, value.as_ref()), &mut hasher);
    }
    Some(std::hash::Hasher::finish(&hasher))
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use super::*;

    #[test]
    fn legacy_mode_retains_original_learning_contract() {
        let store = LoadStore::new(Duration::from_secs(60));
        assert_eq!(
            store.learning_state("b/m", 0, 10),
            LearningState {
                epoch: Some(0),
                serial: 0
            }
        );
    }

    #[test]
    fn missing_identity_keeps_pressure_without_learning() {
        let store = store();
        ingest(&store, &payload("queue", 2, 1_000), 1_000);
        assert_eq!(state(&store, 1_000).epoch, None);
        assert_eq!(store.latest("b/m", "queue").map(|sample| sample.at_ms), Some(1_000));
    }

    #[test]
    fn unqualified_pressure_windows_survive_until_they_age_out() {
        let store = store();
        ingest(&store, &payload("queue", 8, 1_000), 1_000);
        let first = state(&store, 1_000);
        ingest(&store, &payload("running", 3, 2_000), 2_000);
        assert_eq!(
            store.window_worst("b/m", "queue", 2_000, 3_000, true),
            Some(8.0),
            "a partial pressure update must preserve the queue window"
        );
        ingest(&store, &payload("queue", 0, 3_000), 3_000);
        assert_eq!(
            store.window_worst("b/m", "queue", 3_000, 3_000, true),
            Some(8.0),
            "a drained queue must retain the recent pressure peak"
        );
        assert_eq!(
            store.window_worst("b/m", "queue", 4_001, 3_000, true),
            Some(0.0),
            "the pressure peak must expire at the window boundary"
        );
        assert_eq!(
            state(&store, 4_001).epoch,
            None,
            "metadata-free updates must not enable learning"
        );
        assert!(
            state(&store, 4_001).serial > first.serial,
            "pressure updates must advance the read guard"
        );
    }

    #[test]
    fn unqualified_pressure_history_cannot_enter_recovered_learning() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        let first = state(&store, 1_000);
        ingest(&store, &payload("queue", 8, 2_000), 2_000);
        ingest(&store, &payload("queue", 0, 3_000), 3_000);
        assert_eq!(
            store.window_worst("b/m", "queue", 3_000, 3_000, true),
            Some(8.0),
            "unqualified updates must retain fresh pressure history"
        );
        assert_eq!(
            state(&store, 3_000).epoch,
            None,
            "losing metadata must disable learning"
        );
        ingest(&store, &batch("u1", 1, 1, 2, 4_000), 4_000);
        assert!(
            state(&store, 4_000).epoch.is_some(),
            "a newer complete batch must re-enable learning"
        );
        assert_ne!(
            state(&store, 4_000).epoch,
            first.epoch,
            "recovery must start a new learning lifetime"
        );
        assert_eq!(
            store.window_worst("b/m", "queue", 4_000, 4_000, true),
            Some(2.0),
            "recovered learning must exclude unknown-era pressure history"
        );
    }

    #[test]
    fn fresh_complete_batch_qualifies_without_storing_metadata_as_load() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        assert!(state(&store, 1_000).epoch.is_some());
        assert_eq!(store.newest_at("b/m"), Some(1_000));
        assert!(store.latest("b/m", revision::METRIC).is_none());
    }

    #[test]
    fn new_batch_preserves_epoch_but_changes_consistency_serial() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        let first = state(&store, 1_000);
        ingest(&store, &batch("u1", 1, 1, 2, 2_000), 2_000);
        let second = state(&store, 2_000);
        assert_eq!(first.epoch, second.epoch);
        assert!(second.serial > first.serial);
    }

    #[test]
    fn refresh_started_before_a_new_publication_cannot_expire_it() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        let first = state(&store, 1_000);
        ingest(&store, &batch("u1", 1, 1, 2, 2_000), 2_000);
        let current = state(&store, 2_000);
        assert_eq!(current.epoch, first.epoch);
        assert_eq!(
            state(&store, 1_999),
            current,
            "a refresh clock captured before commit must not disqualify a newer publication"
        );
        assert_eq!(state(&store, 12_000), current, "age is bounded from acceptance");
        assert_eq!(state(&store, 12_001).epoch, None, "real expiry still retires learning");
    }

    #[test]
    fn each_revision_or_publisher_transition_retires_windows() {
        for (uid, generation, epoch) in [("u1", 2, 1), ("u2", 1, 1), ("u1", 1, 2)] {
            let store = store();
            ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
            let first = state(&store, 1_000);
            let next = format!(
                "{}\n{}",
                metadata(uid, generation, epoch, 2, 1, 2_000),
                payload("running", 3, 2_000)
            );
            ingest(&store, &next, 2_000);
            assert_ne!(state(&store, 2_000).epoch, first.epoch);
            assert!(store.latest("b/m", "queue").is_none(), "old revision rows retired");
            assert!(store.latest("b/m", "running").is_some());
        }
    }

    #[test]
    fn duplicate_and_delayed_publications_never_refresh_age() {
        let store = store();
        let original = batch("u2", 3, 2, 5, 1_000);
        ingest(&store, &original, 1_000);
        let first = state(&store, 1_000);
        for replay in [original, batch("u0", 1, 1, 999, 2_000), batch("u2", 3, 2, 4, 2_000)] {
            ingest(&store, &replay, 2_000);
            assert_eq!(state(&store, 2_000), first);
            assert_eq!(store.newest_at("b/m"), Some(1_000));
        }
    }

    #[test]
    fn equal_order_republication_is_neither_stored_nor_counted_as_a_drop() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        let first = state(&store, 1_000);
        let restamped = batch("u1", 1, 1, 1, 2_000);
        let tally = store.ingest_at(&restamped, 2_000, 2_000, "b");
        assert_eq!(tally.kept, 0, "repeat polls do not insert observations");
        assert!(
            tally.dropped().iter().all(|(_, count)| *count == 0),
            "healthy republishing is not a parse failure"
        );
        assert_eq!(
            state(&store, 2_000),
            first,
            "repeat polls do not change the read serial"
        );
        assert_eq!(
            store.newest_at("b/m"),
            Some(1_000),
            "relay restamping cannot refresh sample age"
        );
        assert_eq!(
            state(&store, 31_001).epoch,
            None,
            "repeat polls cannot prolong qualification"
        );
    }

    #[test]
    fn equal_order_republication_cannot_restore_expired_learning() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        assert_eq!(
            state(&store, 31_001).epoch,
            None,
            "qualification expires before republishing"
        );
        let restamped = batch("u1", 1, 1, 1, 32_000);
        let tally = store.ingest_at(&restamped, 32_000, 32_000, "b");
        assert_eq!(
            tally.unparsed, 0,
            "expiry does not turn a valid duplicate into malformed data"
        );
        assert_eq!(
            state(&store, 32_000).epoch,
            None,
            "duplicates cannot restore expired learning"
        );
    }

    #[test]
    fn conflicting_or_incomplete_equal_order_batches_remain_rejected() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 2, 1_000), 1_000);
        for invalid in [
            batch("u2", 1, 1, 2, 2_000),
            batch("u1", 2, 1, 2, 2_000),
            metadata("u1", 1, 1, 2, 0, 2_000),
            metadata("u1", 1, 1, 2, 1, 2_000),
            batch("u1", 1, 1, 1, 2_000),
        ] {
            assert!(
                store.ingest_at(&invalid, 2_000, 2_000, "b").unparsed > 0,
                "conflicting, partial and older publications are not duplicates"
            );
            assert_eq!(
                store.newest_at("b/m"),
                Some(1_000),
                "rejection preserves the accepted observation"
            );
        }
    }

    #[test]
    fn future_epochs_never_poison_site_or_provider_fences() {
        let future = batch("u2", 1, u64::MAX, 1, 2_000);
        for invalid in [
            future.clone(),
            metadata("u2", 1, u64::MAX, 1, 1, 2_000),
            format!("{future}\nmalformed"),
            future.replace(" 2000", " 99999999"),
        ] {
            let store = store();
            ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
            store.ingest_at(&invalid, i64::MAX, i64::MAX, "b");
            assert_eq!(
                store
                    .revision_orders
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get("b"),
                Some(&(1, 1)),
                "neither a forged Date nor a distant local timeline authorizes a future epoch"
            );
            ingest(&store, &batch("u1", 1, 1, 2, 3_000), 3_000);
            assert!(
                state(&store, 3_000).epoch.is_some(),
                "legitimate publication recovers without restarting the receiver"
            );
            assert_eq!(
                store.newest_at("b/m"),
                Some(3_000),
                "the valid sequence is not fenced out"
            );
        }
    }

    #[test]
    fn same_uid_generation_rollback_is_rejected_even_in_new_epoch() {
        let store = store();
        ingest(&store, &batch("u1", 3, 1, 1, 1_000), 1_000);
        let first = state(&store, 1_000);
        ingest(&store, &batch("u1", 2, 2, 1, 2_000), 2_000);
        assert_eq!(state(&store, 2_000), first);
        assert_eq!(store.newest_at("b/m"), Some(1_000));
    }

    #[test]
    fn rollback_of_spec_content_with_new_generation_is_a_new_revision() {
        let store = store();
        for generation in 1..=3 {
            let timestamp = i64::try_from(generation).unwrap_or(0).saturating_mul(1_000);
            let before = state(&store, timestamp);
            ingest(&store, &batch("u1", generation, 1, generation, timestamp), timestamp);
            assert_ne!(state(&store, timestamp).epoch, before.epoch);
        }
    }

    #[test]
    fn metadata_only_change_clears_old_load_without_fabricating_newest_time() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        let first = state(&store, 1_000);
        ingest(&store, &metadata("u1", 2, 1, 2, 0, 2_000), 2_000);
        assert_ne!(state(&store, 2_000).epoch, first.epoch);
        assert_eq!(store.newest_at("b/m"), None);
    }

    #[test]
    fn expiry_changes_serial_once_and_preserves_pressure() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        let before = state(&store, 1_000);
        let expired = store.learning_state("b/m", 2_001, 1_000);
        assert_eq!(expired.epoch, None);
        assert!(expired.serial > before.serial);
        assert_eq!(store.learning_state("b/m", 2_001, 1_000), expired);
        assert_eq!(store.newest_at("b/m"), Some(1_000));
    }

    #[test]
    fn stale_identity_and_duplicate_cannot_restore_qualification() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 5, 1_000), 20_000);
        assert_eq!(state(&store, 20_000).epoch, None);
        assert_eq!(store.newest_at("b/m"), None);
        ingest(&store, &batch("u1", 1, 1, 5, 20_000), 20_000);
        assert_eq!(
            state(&store, 20_000).epoch,
            None,
            "stale publication watermark retained"
        );
        ingest(&store, &batch("u1", 1, 1, 6, 20_000), 20_000);
        assert!(state(&store, 20_000).epoch.is_some());
    }

    #[test]
    fn missing_identity_recovery_starts_a_new_learning_lifetime() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        let first = state(&store, 1_000);
        ingest(&store, &payload("running", 3, 2_000), 2_000);
        assert_eq!(state(&store, 2_000).epoch, None);
        assert!(store.latest("b/m", "queue").is_none());
        assert!(store.latest("b/m", "running").is_some());
        ingest(&store, &batch("u1", 1, 1, 1, 2_000), 2_000);
        assert_eq!(
            state(&store, 2_000).epoch,
            None,
            "duplicate cannot restore qualification"
        );
        ingest(&store, &batch("u1", 1, 1, 2, 3_000), 3_000);
        assert_ne!(state(&store, 3_000).epoch, first.epoch);
        assert!(
            store.latest("b/m", "running").is_none(),
            "unknown-era rows cannot enter learning"
        );
    }

    #[test]
    fn absent_provider_is_invalidated_only_within_the_verified_response_scope() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        let first = state(&store, 1_000);
        store.ingest_at("", 1_000, 1_000, "other");
        assert_eq!(state(&store, 1_000), first);
        store.ingest_at("", 1_000, 1_000, "b");
        assert_eq!(state(&store, 1_000).epoch, None);
        ingest(&store, &batch("u1", 1, 1, 1, 2_000), 2_000);
        assert_eq!(state(&store, 2_000).epoch, None, "absence retains watermark");
    }

    #[test]
    fn empty_local_aggregate_invalidates_its_entire_view() {
        let store = store().with_local_site("home");
        store.ingest_at(&batch("u1", 1, 1, 1, 1_000), 1_000, 1_000, "home");
        assert!(state(&store, 1_000).epoch.is_some());
        store.ingest_at("", 1_000, 1_000, "home");
        assert_eq!(state(&store, 1_000).epoch, None);
    }

    #[test]
    fn malformed_conflicting_truncated_and_mixed_stamp_batches_fail_closed() {
        let good = batch("u1", 1, 1, 1, 1_000);
        let meta = metadata("u1", 2, 1, 2, 1, 2_000);
        for broken in [
            meta.clone(),
            format!("{meta}\n{}", payload("queue", 3, 2_001)),
            format!("{meta}\n{meta}\n{}", payload("queue", 3, 2_000)),
            format!("{meta}\n{}", payload("queue", 3, 2_000)).replace("} 2 2000", "} 2.0 2000"),
            format!("{meta}\nqueue{{grid_site=\"b\",grid_provider=\"m\"}} NaN 2000"),
        ] {
            let store = store();
            ingest(&store, &good, 1_000);
            ingest(&store, &broken, 2_000);
            assert_eq!(state(&store, 2_000).epoch, None, "rejects {broken}");
            assert_eq!(store.newest_at("b/m"), None, "no partial payload admitted");
        }
    }

    #[test]
    fn truncated_old_publication_does_not_disable_a_newer_valid_revision() {
        let store = store();
        ingest(&store, &batch("u2", 1, 2, 1, 2_000), 2_000);
        let current = state(&store, 2_000);
        ingest(&store, &metadata("u1", 1, 1, 99, 1, 1_000), 2_000);
        assert_eq!(state(&store, 2_000), current);
    }

    #[test]
    fn unparseable_unattributed_row_rejects_whole_response() {
        let store = store();
        let text = format!("{}\ninvalid row", batch("u1", 1, 1, 1, 1_000));
        ingest(&store, &text, 1_000);
        assert_eq!(
            store.provider_count(),
            1,
            "valid metadata fence retained without accepting payload"
        );
        assert_eq!(store.newest_at("b/m"), None);
        assert_eq!(state(&store, 1_000).epoch, None);
    }

    #[test]
    fn duplicated_payload_or_label_is_not_a_complete_batch() {
        for rows in [format!("{}\n{}", payload("queue", 2, 1_000), payload("queue", 2, 1_000)), "queue{grid_site=\"b\",grid_provider=\"m\",x=\"a\",x=\"b\"} 2 1000\nqueue{grid_site=\"b\",grid_provider=\"m\"} 2 1000".to_owned()] {
            let store = store();
            ingest(&store, &format!("{}\n{rows}", metadata("u1", 1, 1, 1, 2, 1_000)), 1_000);
            assert_eq!(state(&store, 1_000).epoch, None);
            assert_eq!(store.newest_at("b/m"), None);
        }
    }

    #[test]
    fn excessive_metrics_and_rows_are_rejected_before_partial_commit() {
        for count in [
            MAX_METRICS_PER_PROVIDER.saturating_add(1),
            revision::MAX_BATCH_SAMPLES.saturating_add(1),
        ] {
            let store = store();
            let rows = (0..count)
                .map(|number| payload(&format!("metric_{number}"), 1, 1_000))
                .collect::<Vec<_>>()
                .join("\n");
            ingest(
                &store,
                &format!("{}\n{rows}", metadata("u1", 1, 1, 1, count, 1_000)),
                1_000,
            );
            assert_eq!(store.newest_at("b/m"), None);
            assert_eq!(state(&store, 1_000).epoch, None);
        }
    }

    #[test]
    fn excessive_response_bytes_or_lines_are_rejected() {
        for text in [
            " ".repeat(MAX_RESPONSE_BYTES.saturating_add(1)),
            "\n".repeat(MAX_RESPONSE_LINES.saturating_add(1)),
        ] {
            let store = store();
            assert_eq!(store.ingest_at(&text, 1_000, 1_000, "b").capped, 1);
        }
    }

    #[test]
    fn provider_change_does_not_reset_another_present_provider() {
        let store = store();
        let other = batch("other", 1, 1, 1, 1_000).replace("grid_provider=\"m\"", "grid_provider=\"n\"");
        ingest(&store, &format!("{}\n{other}", batch("u1", 1, 1, 1, 1_000)), 1_000);
        let first = store.learning_state("b/n", 1_000, 10_000);
        let refreshed = batch("other", 1, 1, 2, 2_000).replace("grid_provider=\"m\"", "grid_provider=\"n\"");
        ingest(&store, &format!("{}\n{refreshed}", batch("u1", 2, 1, 2, 2_000)), 2_000);
        assert_eq!(store.learning_state("b/n", 2_000, 10_000).epoch, first.epoch);
    }

    #[test]
    fn site_fence_rejects_unseen_delayed_provider_across_epochs_and_sequences() {
        for (epoch, sequence) in [(2, 1), (1, 3)] {
            let store = store();
            ingest(&store, &batch("old", 1, 1, 1, 1_000), 1_000);
            let other = batch("other", 1, epoch, sequence, 2_000).replace("grid_provider=\"m\"", "grid_provider=\"n\"");
            ingest(&store, &other, 2_000);
            let current = store.learning_state("b/n", 2_000, 10_000);
            assert_eq!(state(&store, 2_000).epoch, None);
            ingest(&store, &batch("unseen", 1, 1, 2, 1_500), 2_000);
            assert_eq!(
                state(&store, 2_000).epoch,
                None,
                "site fence rejects previously unseen intermediate UID"
            );
            assert_eq!(
                store.newest_at("b/m"),
                Some(1_000),
                "absence preserves pressure without rejuvenating it"
            );
            assert_eq!(
                store.learning_state("b/n", 2_000, 10_000),
                current,
                "old response cannot withdraw newer sibling"
            );
        }
    }

    #[test]
    fn mixed_cached_old_provider_cannot_hide_behind_a_newer_site_group() {
        let store = store();
        let old = batch("old", 1, 1, 1, 1_000);
        ingest(&store, &old, 1_000);
        let other = batch("other", 1, 2, 1, 2_000).replace("grid_provider=\"m\"", "grid_provider=\"n\"");
        ingest(&store, &format!("{old}\n{other}"), 2_000);
        assert_eq!(
            state(&store, 2_000).epoch,
            None,
            "presence of old cached row is not current authority"
        );
        assert!(store.learning_state("b/n", 2_000, 10_000).epoch.is_some());
    }

    #[test]
    fn complete_new_site_sequence_preserves_all_unchanged_learning_lifetimes() {
        let store = store();
        let mut lifetimes = Vec::new();
        for sequence in 1..=20 {
            let stamp = i64::try_from(sequence).unwrap_or(0).saturating_mul(100);
            let first = batch("u1", 1, 1, sequence, stamp);
            let second = batch("u2", 1, 1, sequence, stamp).replace("grid_provider=\"m\"", "grid_provider=\"n\"");
            ingest(&store, &format!("{first}\n{second}"), stamp);
            lifetimes.push((
                state(&store, stamp).epoch,
                store.learning_state("b/n", stamp, 10_000).epoch,
            ));
        }
        assert!(
            lifetimes.windows(2).all(|pair| pair.first() == pair.get(1)),
            "regular complete refreshes never reset unchanged revisions"
        );
    }

    #[test]
    fn strict_admission_keeps_the_existing_per_owner_bound() {
        let store = store();
        let text = (0..300)
            .map(|number| {
                batch("u1", 1, 1, 1, 1_000).replace("grid_provider=\"m\"", &format!("grid_provider=\"p{number}\""))
            })
            .collect::<Vec<_>>()
            .join("\n");
        let tally = store.ingest_at(&text, 1_000, 1_000, "b");
        assert_eq!(store.provider_count(), super::super::MAX_PROVIDERS_PER_OWNER);
        assert!(tally.capped > 0);
    }

    #[test]
    fn a_same_revision_cannot_partially_exceed_the_retained_metric_budget() {
        let store = store();
        let rows = (0..MAX_METRICS_PER_PROVIDER)
            .map(|number| payload(&format!("metric_{number}"), 1, 1_000))
            .collect::<Vec<_>>()
            .join("\n");
        ingest(
            &store,
            &format!("{}\n{rows}", metadata("u1", 1, 1, 1, MAX_METRICS_PER_PROVIDER, 1_000)),
            1_000,
        );
        assert!(state(&store, 1_000).epoch.is_some());
        ingest(&store, &batch("u1", 1, 1, 2, 2_000), 2_000);
        assert_eq!(state(&store, 2_000).epoch, None);
        assert_eq!(store.newest_at("b/m"), None);
    }

    #[test]
    fn concurrent_reads_never_accept_a_mixed_batch_as_one_serial() {
        let store = Arc::new(store());
        let writer_store = Arc::clone(&store);
        let writer = thread::spawn(move || {
            for sequence in 1..=500 {
                let stamp = i64::try_from(sequence).unwrap_or(0);
                let rows = format!(
                    "{}\n{}\n{}",
                    metadata("u1", 1, 1, sequence, 2, stamp),
                    payload("left", stamp, stamp),
                    payload("right", stamp, stamp)
                );
                ingest(&writer_store, &rows, stamp);
            }
        });
        for _ in 0..2_000 {
            let before = state(&store, 500);
            let left = store.latest("b/m", "left");
            let right = store.latest("b/m", "right");
            let after = state(&store, 500);
            if before == after && before.epoch.is_some() {
                assert_eq!(left, right, "stable serial cannot expose a partial provider commit");
            }
        }
        assert!(writer.join().is_ok(), "writer completed");
    }

    #[test]
    fn every_disqualification_can_recover_only_with_a_new_complete_publication() {
        let cases = [
            payload("unknown", 9, 2_000),
            metadata("u1", 1, 1, 2, 0, 2_000),
            metadata("u1", 1, 1, 2, 1, 2_000),
            metadata("u1", 1, 1, 2, 0, 2_000).replace("} 1 2000", "} 1.0 2000"),
        ];
        for text in cases {
            let store = store();
            ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
            let original = state(&store, 1_000).epoch;
            ingest(&store, &text, 2_000);
            assert_eq!(state(&store, 2_000).epoch, None, "disabled by {text}");
            ingest(&store, &batch("u1", 1, 1, 1, 2_000), 2_000);
            assert_eq!(state(&store, 2_000).epoch, None, "old publication cannot re-enable");
            let recovery = format!(
                "{}\n{}",
                metadata("u1", 1, 1, 3, 1, 3_000),
                payload("recovered", 4, 3_000)
            );
            ingest(&store, &recovery, 3_000);
            let recovered = state(&store, 3_000).epoch;
            assert!(recovered.is_some(), "new complete publication re-enables");
            assert_ne!(recovered, original);
            assert!(store.latest("b/m", "queue").is_none());
            assert!(store.latest("b/m", "unknown").is_none());
            assert!(store.latest("b/m", "recovered").is_some());
        }
    }

    #[test]
    fn expired_learning_recovery_clears_pre_expiry_windows() {
        let store = store();
        let original_batch = batch("u1", 1, 1, 1, 1_000);
        ingest(&store, &original_batch, 1_000);
        let original = state(&store, 1_000).epoch;
        assert_eq!(state(&store, 20_000).epoch, None);
        ingest(&store, &original_batch, 20_000);
        assert_eq!(state(&store, 20_000).epoch, None);
        let recovery = format!(
            "{}\n{}",
            metadata("u1", 1, 1, 2, 1, 20_000),
            payload("fresh", 3, 20_000)
        );
        ingest(&store, &recovery, 20_000);
        assert!(state(&store, 20_000).epoch.is_some());
        assert_ne!(state(&store, 20_000).epoch, original);
        assert!(store.latest("b/m", "queue").is_none());
    }

    #[test]
    fn incomplete_new_authority_fences_old_and_same_order_recovery() {
        for (uid, generation, epoch, sequence) in [("u2", 1, 2, 1), ("u1", 2, 1, 3)] {
            let store = store();
            ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
            ingest(&store, &metadata(uid, generation, epoch, sequence, 1, 2_000), 2_000);
            assert_eq!(state(&store, 2_000).epoch, None);
            ingest(&store, &batch("u1", 1, 1, 2, 2_000), 2_000);
            assert_eq!(
                state(&store, 2_000).epoch,
                None,
                "unseen intermediate publication remains fenced"
            );
            ingest(&store, &batch(uid, generation, epoch, sequence, 2_000), 2_000);
            assert_eq!(state(&store, 2_000).epoch, None, "incomplete publication is consumed");
            ingest(
                &store,
                &batch(uid, generation, epoch, sequence.saturating_add(1), 3_000),
                3_000,
            );
            assert!(state(&store, 3_000).epoch.is_some());
        }
    }

    #[test]
    fn malformed_payload_does_not_erase_observed_control_fence() {
        let meta = metadata("u2", 1, 2, 1, 1, 2_000);
        for text in [format!("{meta}\nmalformed"), format!("malformed\n{meta}")] {
            let store = store();
            ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
            ingest(&store, &text, 2_000);
            ingest(&store, &batch("u1", 1, 1, 2, 2_000), 2_000);
            assert_eq!(state(&store, 2_000).epoch, None);
            ingest(&store, &batch("u2", 1, 2, 1, 2_000), 2_000);
            assert_eq!(state(&store, 2_000).epoch, None);
            ingest(&store, &batch("u2", 1, 2, 2, 3_000), 3_000);
            assert!(state(&store, 3_000).epoch.is_some());
        }
    }

    #[test]
    fn skewed_zero_payload_invalidation_fences_replays_and_preserves_age_feedback() {
        let store = store();
        ingest(&store, &batch("u1", 1, 1, 1, 1_000), 1_000);
        let tally = store.ingest_at(&metadata("u2", 1, 2, 1, 0, -86_400_001), 2_000, 2_000, "b");
        assert_eq!(tally.skewed, 1);
        assert_eq!(state(&store, 2_000).epoch, None);
        ingest(&store, &batch("u1", 1, 1, 2, 2_000), 2_000);
        assert_eq!(state(&store, 2_000).epoch, None);
        ingest(&store, &batch("u2", 1, 2, 1, 2_000), 2_000);
        assert_eq!(
            state(&store, 2_000).epoch,
            None,
            "restamping cannot revive consumed invalidation"
        );
        ingest(&store, &batch("u2", 1, 2, 2, 3_000), 3_000);
        assert!(state(&store, 3_000).epoch.is_some());
    }

    #[test]
    fn guarded_rejections_keep_skew_and_owner_feedback_distinct() {
        for stamp in [2_001, -86_400_001] {
            let store = store();
            let tally = store.ingest_at(&batch("u1", 1, 1, 1, stamp), 1_000, 1_000, "b");
            assert_eq!(tally.skewed, 2);
            assert_eq!(tally.unparsed, 0);
        }
        let store = store();
        let tally = store.ingest_at(&batch("u1", 1, 1, 1, 1_000), 1_000, 1_000, "foreign");
        assert_eq!(tally.mismatched, 2);
        assert_eq!(tally.unparsed, 0);
        assert_eq!(store.provider_count(), 0);
    }

    #[test]
    fn one_peers_stale_invalidation_cannot_erase_another_peers_fresh_payload() {
        let store = store().with_local_site("hub");
        let fresh = batch("healthy", 1, 1, 1, 2_000).replace("grid_site=\"b\"", "grid_site=\"c\"");
        let stale = metadata("replacement", 1, 2, 1, 0, -86_400_001);
        let tally = store.ingest_at(&format!("{stale}\n{fresh}"), 2_000, 2_000, "hub");
        assert_eq!(tally.skewed, 1, "the bad peer still contributes age rejection evidence");
        assert_eq!(state(&store, 2_000).epoch, None);
        assert!(
            store.learning_state("c/m", 2_000, 10_000).epoch.is_some(),
            "attributed rejection is provider-local"
        );
        assert!(
            store.latest("c/m", "queue").is_some(),
            "the healthy peer retains its fresh pressure"
        );
        ingest(&store, &batch("old", 1, 1, 99, 2_000), 2_000);
        assert_eq!(
            state(&store, 2_000).epoch,
            None,
            "the stale invalidation still fences old authority"
        );
    }

    #[test]
    fn existing_provider_aliases_work_with_and_without_revision_metadata() {
        for qualified in [true, false] {
            let store = store().with_local_site("home");
            let text = if qualified {
                batch("u1", 1, 1, 1, 1_000)
            } else {
                payload("queue", 2, 1_000)
            };
            let text = text.replace("grid_provider=\"m\"", "grid_provider=\"pool+gpu\"");
            assert_eq!(store.ingest_at(&text, 1_000, 1_000, "home").unparsed, 0);
            assert!(store.latest("b/pool+gpu", "queue").is_some());
            assert_eq!(
                store.learning_state("b/pool+gpu", 1_000, 10_000).epoch.is_some(),
                qualified
            );
        }
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    fn store() -> LoadStore {
        LoadStore::new(Duration::from_secs(60)).with_revision_guard(10_000)
    }

    fn state(store: &LoadStore, now: i64) -> LearningState {
        store.learning_state("b/m", now, 10_000)
    }

    fn ingest(store: &LoadStore, text: &str, now: i64) {
        store.ingest_at(text, now, now, "b");
    }

    fn payload(name: &str, value: i64, stamp: i64) -> String {
        format!("{name}{{grid_site=\"b\",grid_provider=\"m\"}} {value} {stamp}")
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "test fixture spells every independent wire field"
    )]
    fn metadata(uid: &str, generation: u64, epoch: u64, sequence: u64, samples: usize, stamp: i64) -> String {
        format!(
            "{}{{grid_site=\"b\",grid_provider=\"m\",provider_uid=\"{uid}\",publisher_epoch=\"{epoch}\",batch_sequence=\"{sequence}\",batch_samples=\"{samples}\"}} {generation} {stamp}",
            revision::METRIC
        )
    }

    fn batch(uid: &str, generation: u64, epoch: u64, sequence: u64, stamp: i64) -> String {
        format!(
            "{}\n{}",
            metadata(uid, generation, epoch, sequence, 1, stamp),
            payload("queue", 2, stamp)
        )
    }
}
