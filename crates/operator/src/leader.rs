//! Leader election: one operator replica holds a lease and runs everything that writes.
//!
//! The holder renews well inside the lease's duration and counts itself lost before
//! any other replica can take over. A candidate judges expiry by how long the lease
//! has gone unchanged on its own clock, never by the holder's timestamps, so clock
//! skew between nodes cannot hand the lease to two replicas.

use std::time::Duration;

use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta},
    jiff::Timestamp,
};
use kube::{
    Api, Client,
    api::{PostParams, ResourceExt as _},
};
use tokio::time::Instant;

/// How long a lease stays held without a renewal.
pub const LEASE_DURATION: Duration = Duration::from_secs(10);

/// How often the holder renews.
pub const RENEW_EVERY: Duration = Duration::from_secs(5);

/// How long the holder keeps trying after its last renewal before it counts itself lost.
///
/// Short of [`LEASE_DURATION`], so the holder stops before a candidate can take over.
pub const RENEW_DEADLINE: Duration = Duration::from_secs(8);

/// How often a candidate tries to take the lease, and the holder retries a failed renewal.
pub const RETRY_EVERY: Duration = Duration::from_secs(1);

/// How long one read or write of the lease may take.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);

/// What the lease says.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Record {
    /// Who holds it, `None` once released.
    pub holder: Option<String>,
    /// How long it holds without a renewal.
    pub duration: Duration,
    /// When the current holder took it.
    pub acquired: Option<Timestamp>,
    /// When the current holder last renewed it.
    pub renewed: Option<Timestamp>,
    /// How many times it has changed hands.
    pub transitions: i32,
}

/// Why a lease read or write failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Another replica wrote first.
    #[error("the lease changed under this write")]
    Conflict,
    /// The store could not be reached or refused the request.
    #[error("{0}")]
    Unavailable(String),
}

/// Where the lease lives, written only by compare and swap on its version.
pub trait LeaseStore: Send + Sync {
    /// The record and its version, `None` when no lease exists.
    fn get(&self) -> impl Future<Output = Result<Option<(Record, String)>, StoreError>> + Send;

    /// Write `record` if the lease is still at `version`, or create it when `version` is `None`.
    /// Returns the new version.
    fn put(&self, version: Option<&str>, record: &Record) -> impl Future<Output = Result<String, StoreError>> + Send;
}

/// Contends for and holds one lease as `me`.
#[derive(Debug)]
pub struct Elector<S> {
    /// Where the lease lives.
    store: S,
    /// This replica's holder identity.
    me: String,
    /// The version last seen and when it was first seen, on this replica's clock.
    observed: Option<(String, Instant)>,
    /// When this replica last wrote the lease as its holder, shared with what acts on holding it.
    renewed_at: Holding,
}

/// Whether this replica may still act as the lease holder, judged on its own clock.
///
/// It holds until [`RENEW_DEADLINE`] after its last renewal, before any candidate can take over.
#[derive(Clone, Debug, Default)]
pub struct Holding(Option<std::sync::Arc<std::sync::Mutex<Option<Instant>>>>);

impl Holding {
    /// Always holding, for a replica that writes unelected.
    #[must_use]
    pub const fn unelected() -> Self {
        Self(None)
    }

    /// Whether this replica may act as the holder now.
    #[must_use]
    pub fn holds(&self) -> bool {
        if self.0.is_none() {
            return true;
        }
        self.last()
            .and_then(|at| at.checked_add(RENEW_DEADLINE))
            .is_some_and(|until| Instant::now() < until)
    }

    /// An elected holding renewed now, which lapses at [`RENEW_DEADLINE`].
    #[cfg(test)]
    pub(crate) fn renewed_now() -> Self {
        let holding = Self::elected();
        holding.set(Some(Instant::now()));
        holding
    }

    /// An elected holding with no renewal yet.
    fn elected() -> Self {
        Self(Some(std::sync::Arc::new(std::sync::Mutex::new(None))))
    }

    /// The last renewal.
    fn last(&self) -> Option<Instant> {
        self.0
            .as_ref()
            .and_then(|renewed| *renewed.lock().unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    /// Record a renewal at `at`, or none.
    fn set(&self, at: Option<Instant>) {
        if let Some(renewed) = &self.0 {
            *renewed.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = at;
        }
    }
}

/// How a tenure ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Tenure {
    /// The caller finished while the lease was still held.
    Finished,
    /// The lease was lost: another replica took it, or no renewal landed in time.
    Lost,
}

impl<S: LeaseStore> Elector<S> {
    /// A candidate for `store`'s lease, named `me`.
    pub fn new<Me: Into<String>>(store: S, me: Me) -> Self {
        Self {
            store,
            me: me.into(),
            observed: None,
            renewed_at: Holding::elected(),
        }
    }

    /// Whether this replica may act as the holder, for what acts on holding it.
    #[must_use]
    pub fn holding(&self) -> Holding {
        self.renewed_at.clone()
    }

    /// Wait until this replica holds the lease.
    pub async fn acquire(&mut self) {
        self.acquire_when(&crate::unelected::Open).await;
    }

    /// Wait until this replica holds the lease, contending only while `gate` allows.
    pub async fn acquire_when<Gate: crate::unelected::ContendGate>(&mut self, gate: &Gate) {
        let mut reported = None;
        let mut waiting: Option<crate::unelected::Waiting> = None;
        loop {
            let held_back = gate.check().await.err();
            let blocked = held_back.is_some();
            report_waiting(&mut waiting, held_back);
            if blocked {
                tokio::time::sleep(crate::enroll::jittered(RETRY_EVERY)).await;
                continue;
            }
            match self.attempt().await {
                Ok(true) => return,
                Ok(false) => {},
                Err(error) => {
                    // Once per distinct failure: a candidate refused the lease stays not ready.
                    let error = error.to_string();
                    if reported.as_ref() != Some(&error) {
                        tracing::warn!(%error, "cannot read or take the operator lease; retrying");
                        reported = Some(error);
                    }
                },
            }
            tokio::time::sleep(crate::enroll::jittered(RETRY_EVERY)).await;
        }
    }

    /// Renew the held lease until `finished` resolves or the lease is lost.
    pub async fn hold_until<Finished: Future<Output = ()>>(&mut self, finished: Finished) -> Tenure {
        let mut finished = std::pin::pin!(finished);
        let mut failing = false;
        loop {
            let Some(deadline) = self.renewed_at.last().and_then(|at| at.checked_add(RENEW_DEADLINE)) else {
                return Tenure::Lost;
            };
            let wait = crate::enroll::jittered(if failing { RETRY_EVERY } else { RENEW_EVERY });
            let next = Instant::now()
                .checked_add(wait)
                .map_or(deadline, |next| next.min(deadline));
            tokio::select! {
                biased;
                () = &mut finished => return Tenure::Finished,
                () = tokio::time::sleep_until(next) => {},
            }
            let Some(renewed) = self.renew(deadline).await else {
                self.renewed_at.set(None);
                return Tenure::Lost;
            };
            failing = !renewed;
        }
    }

    /// One renewal bounded by `deadline`: whether it landed, `None` once the lease is lost.
    async fn renew(&mut self, deadline: Instant) -> Option<bool> {
        let Some(bound) = deadline
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
        else {
            tracing::warn!("no lease renewal landed before the deadline");
            return None;
        };
        let failure = match tokio::time::timeout(bound.min(ATTEMPT_TIMEOUT), self.attempt()).await {
            Ok(Ok(true)) => return Some(true),
            // An earlier renewal that timed out here can land after this one read the lease.
            Ok(Ok(false)) if self.names_me(deadline).await => "the lease changed under a renewal of ours".to_owned(),
            Ok(Ok(false)) => {
                tracing::warn!("another replica holds the lease");
                return None;
            },
            Ok(Err(error)) => error.to_string(),
            Err(_elapsed) => "timed out".to_owned(),
        };
        tracing::warn!(error = failure, "lease renewal failed; retrying");
        Some(false)
    }

    /// Whether the lease still names this replica, read before `deadline`.
    async fn names_me(&self, deadline: Instant) -> bool {
        let bound = deadline.saturating_duration_since(Instant::now()).min(ATTEMPT_TIMEOUT);
        matches!(
            tokio::time::timeout(bound, self.store.get()).await,
            Ok(Ok(Some((record, _)))) if record.holder.as_deref() == Some(self.me.as_str())
        )
    }

    /// Give the lease up, so a candidate takes it on its next try rather than at expiry.
    pub async fn release(&self) {
        self.renewed_at.set(None);
        let released = async {
            let Some((mut record, version)) = self.store.get().await? else {
                return Ok(());
            };
            if record.holder.as_deref() != Some(self.me.as_str()) {
                return Ok(());
            }
            record.holder = None;
            record.renewed = Some(Timestamp::now());
            self.store.put(Some(&version), &record).await.map(drop)
        };
        match tokio::time::timeout(ATTEMPT_TIMEOUT, released).await {
            Ok(Ok(())) => tracing::info!("released the operator lease"),
            Ok(Err(error)) => tracing::warn!(%error, "lease not released; a successor waits for it to expire"),
            Err(_elapsed) => tracing::warn!("lease release timed out; a successor waits for it to expire"),
        }
    }

    /// One read and, when the lease is free, expired or ours, one write. Whether it is now held.
    async fn attempt(&mut self) -> Result<bool, StoreError> {
        let current = self.store.get().await?;
        let now = Instant::now();
        let (version, next) = match current {
            None => (None, self.taken(None)),
            Some((record, version)) => {
                if self.observed.as_ref().is_none_or(|(seen, _)| *seen != version) {
                    self.observed = Some((version.clone(), now));
                }
                let since = self.observed.as_ref().map_or(now, |(_, at)| *at);
                if !takeable(&record, &self.me, now.saturating_duration_since(since)) {
                    return Ok(false);
                }
                (Some(version), self.taken(Some(&record)))
            },
        };
        match self.store.put(version.as_deref(), &next).await {
            Ok(written) => {
                self.observed = Some((written, now));
                self.renewed_at.set(Some(now));
                Ok(true)
            },
            Err(StoreError::Conflict) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// The record this replica writes over `current` to hold the lease.
    fn taken(&self, current: Option<&Record>) -> Record {
        let now = Timestamp::now();
        let mine = current.is_some_and(|record| record.holder.as_deref() == Some(self.me.as_str()));
        Record {
            holder: Some(self.me.clone()),
            duration: LEASE_DURATION,
            acquired: if mine {
                current.and_then(|record| record.acquired)
            } else {
                Some(now)
            },
            renewed: Some(now),
            transitions: current.map_or(0, |record| {
                if mine {
                    record.transitions
                } else {
                    record.transitions.saturating_add(1)
                }
            }),
        }
    }
}

/// Log and report a change in why a candidate is held back, `None` once it may contend.
fn report_waiting(held: &mut Option<crate::unelected::Waiting>, now: Option<crate::unelected::Waiting>) {
    if *held == now {
        return;
    }
    match &now {
        Some(crate::unelected::Waiting::Unelected(pods)) => {
            tracing::info!(
                ?pods,
                "an unelected operator replica may still write; waiting for it to go"
            );
        },
        Some(crate::unelected::Waiting::Unreadable(error)) => {
            tracing::warn!(%error, "cannot list the operator replicas; not contending for the lease");
        },
        None => {},
    }
    crate::metrics::set_lease_waiting(now.as_ref().map(crate::unelected::Waiting::reason));
    *held = now;
}

/// Whether `me` may write `record`: it holds the lease, nobody does, or the lease has gone
/// `unchanged_for` its whole duration.
fn takeable(record: &Record, me: &str, unchanged_for: Duration) -> bool {
    match record.holder.as_deref().filter(|holder| !holder.is_empty()) {
        None => true,
        Some(holder) if holder == me => true,
        Some(_) => unchanged_for >= record.duration,
    }
}

/// A held lease, renewed in the background until [`Leadership::finish`].
#[derive(Debug)]
pub struct Leadership<S> {
    /// Tells the renewer the caller is done.
    finished: tokio::sync::oneshot::Sender<()>,
    /// The renewer, handing back the elector when the lease is still held.
    renewer: tokio::task::JoinHandle<Option<Elector<S>>>,
    /// Whether this replica may still act as the holder.
    holding: Holding,
}

/// Take `elector`'s lease, then renew it in the background, calling `lost` if it goes.
///
/// `None` when `shutdown` fires first, so nothing that writes has started.
pub async fn lead<S: LeaseStore + 'static, Gate: crate::unelected::ContendGate, Lost: FnOnce() + Send + 'static>(
    mut elector: Elector<S>,
    gate: &Gate,
    shutdown: &crate::shutdown::Shutdown,
    lost: Lost,
) -> Option<Leadership<S>> {
    tokio::select! {
        biased;
        () = shutdown.triggered() => return None,
        () = elector.acquire_when(gate) => {},
    }
    tracing::info!(holder = %elector.me, "holding the operator lease");
    crate::metrics::set_lease_held(true);
    let (finished, done) = tokio::sync::oneshot::channel();
    let holding = elector.holding();
    let renewer = tokio::spawn(async move {
        let tenure = elector.hold_until(async { drop(done.await) }).await;
        if tenure == Tenure::Lost {
            crate::metrics::set_lease_held(false);
            lost();
            return None;
        }
        Some(elector)
    });
    Some(Leadership {
        finished,
        renewer,
        holding,
    })
}

impl<S: LeaseStore> Leadership<S> {
    /// Whether this replica may still act as the holder.
    #[must_use]
    pub fn holding(&self) -> Holding {
        self.holding.clone()
    }

    /// Stop renewing and release the lease if it is still held.
    pub async fn finish(self) {
        let _sent = self.finished.send(());
        if let Ok(Some(elector)) = self.renewer.await {
            elector.release().await;
        }
        crate::metrics::set_lease_held(false);
    }
}

/// A `coordination.k8s.io/v1` `Lease`.
#[derive(Clone)]
pub struct KubeLease {
    /// Leases in the lease's namespace.
    api: Api<Lease>,
    /// The lease's name.
    name: String,
}

impl std::fmt::Debug for KubeLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeLease")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl KubeLease {
    /// The lease `name` in `namespace`.
    #[must_use]
    pub fn new<Name: Into<String>>(client: Client, namespace: &str, name: Name) -> Self {
        Self {
            api: Api::namespaced(client, namespace),
            name: name.into(),
        }
    }
}

impl LeaseStore for KubeLease {
    async fn get(&self) -> Result<Option<(Record, String)>, StoreError> {
        let lease = self
            .api
            .get_opt(&self.name)
            .await
            .map_err(|error| unavailable(&error))?;
        Ok(lease.map(|lease| {
            let version = lease.resource_version().unwrap_or_default();
            (record_of(lease.spec.unwrap_or_default()), version)
        }))
    }

    async fn put(&self, version: Option<&str>, record: &Record) -> Result<String, StoreError> {
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some(self.name.clone()),
                resource_version: version.map(str::to_owned),
                ..ObjectMeta::default()
            },
            spec: Some(spec_of(record)),
        };
        let params = PostParams::default();
        let written = match version {
            None => self.api.create(&params, &lease).await,
            Some(_) => self.api.replace(&self.name, &params, &lease).await,
        };
        match written {
            Ok(written) => Ok(written.resource_version().unwrap_or_default()),
            Err(kube::Error::Api(status)) if status.code == 409 => Err(StoreError::Conflict),
            Err(error) => Err(unavailable(&error)),
        }
    }
}

/// A kube error as an unavailable store.
fn unavailable(error: &kube::Error) -> StoreError {
    StoreError::Unavailable(error.to_string())
}

/// What a `Lease` spec says.
fn record_of(spec: LeaseSpec) -> Record {
    Record {
        holder: spec.holder_identity.filter(|holder| !holder.is_empty()),
        duration: Duration::from_secs(
            spec.lease_duration_seconds
                .and_then(|seconds| u64::try_from(seconds).ok())
                .unwrap_or(0),
        ),
        acquired: spec.acquire_time.map(|time| time.0),
        renewed: spec.renew_time.map(|time| time.0),
        transitions: spec.lease_transitions.unwrap_or(0),
    }
}

/// The `Lease` spec saying `record`.
fn spec_of(record: &Record) -> LeaseSpec {
    LeaseSpec {
        holder_identity: record.holder.clone(),
        lease_duration_seconds: Some(i32::try_from(record.duration.as_secs()).unwrap_or(i32::MAX)),
        acquire_time: record.acquired.map(MicroTime),
        renew_time: record.renewed.map(MicroTime),
        lease_transitions: Some(record.transitions),
        ..LeaseSpec::default()
    }
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::print_stderr,
    reason = "tests; the handover times are printed for the recovery-time targets"
)]
mod tests {
    use std::{
        sync::{Arc, Mutex, PoisonError},
        time::Duration,
    };

    use tokio::time::Instant;

    use super::{
        Elector, LEASE_DURATION, LeaseStore, RENEW_DEADLINE, RETRY_EVERY, Record, StoreError, Tenure, takeable,
    };

    /// A lease in memory, written by compare and swap on a counter.
    #[derive(Clone, Default)]
    struct Memory(Arc<Mutex<MemoryLease>>);

    #[derive(Default)]
    struct MemoryLease {
        held: Option<(Record, u64)>,
        down: bool,
        /// Moves the version under the next write, as an earlier write of the same holder landing late does.
        bump_next: bool,
    }

    impl Memory {
        fn holder(&self) -> Option<String> {
            let lease = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            lease.held.as_ref().and_then(|(record, _)| record.holder.clone())
        }

        fn bump_next(&self) {
            self.0.lock().unwrap_or_else(PoisonError::into_inner).bump_next = true;
        }

        fn set_down(&self, down: bool) {
            self.0.lock().unwrap_or_else(PoisonError::into_inner).down = down;
        }
    }

    impl LeaseStore for Memory {
        async fn get(&self) -> Result<Option<(Record, String)>, StoreError> {
            let lease = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            if lease.down {
                return Err(StoreError::Unavailable("down".to_owned()));
            }
            Ok(lease
                .held
                .as_ref()
                .map(|(record, version)| (record.clone(), version.to_string())))
        }

        async fn put(&self, version: Option<&str>, record: &Record) -> Result<String, StoreError> {
            let mut lease = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            if lease.down {
                return Err(StoreError::Unavailable("down".to_owned()));
            }
            if std::mem::take(&mut lease.bump_next)
                && let Some((_, at)) = lease.held.as_mut()
            {
                *at = at.saturating_add(1);
            }
            let current = lease.held.as_ref().map(|(_, at)| *at);
            if current.map(|at| at.to_string()).as_deref() != version {
                return Err(StoreError::Conflict);
            }
            let next = current.map_or(1, |at| at.saturating_add(1));
            lease.held = Some((record.clone(), next));
            drop(lease);
            Ok(next.to_string())
        }
    }

    /// The longest a candidate's retry can be: [`RETRY_EVERY`] plus its tenth of jitter.
    fn retry_ceiling() -> Duration {
        RETRY_EVERY.saturating_add(RETRY_EVERY / 10)
    }

    #[test]
    fn a_lease_is_takeable_when_free_ours_or_unchanged_for_its_duration() {
        let held = |holder: &str| Record {
            holder: Some(holder.to_owned()),
            duration: LEASE_DURATION,
            ..Record::default()
        };
        assert!(takeable(&Record::default(), "a", Duration::ZERO), "nobody holds it");
        assert!(takeable(&held(""), "a", Duration::ZERO), "an empty holder is nobody");
        assert!(takeable(&held("a"), "a", Duration::ZERO), "ours");
        assert!(!takeable(
            &held("b"),
            "a",
            LEASE_DURATION.saturating_sub(Duration::from_millis(1))
        ));
        assert!(
            takeable(&held("b"), "a", LEASE_DURATION),
            "unchanged for its whole duration"
        );
    }

    /// A gate whose verdict a test sets.
    #[derive(Clone, Default)]
    struct Scripted(Arc<Mutex<Option<crate::unelected::Waiting>>>);

    impl Scripted {
        fn hold(&self, why: Option<crate::unelected::Waiting>) {
            *self.0.lock().unwrap_or_else(PoisonError::into_inner) = why;
        }
    }

    impl crate::unelected::ContendGate for Scripted {
        async fn check(&self) -> Result<(), crate::unelected::Waiting> {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
                .map_or(Ok(()), Err)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn no_candidate_takes_the_lease_while_an_unelected_replica_runs() {
        let store = Memory::default();
        let gate = Scripted::default();
        gate.hold(Some(crate::unelected::Waiting::Unelected(vec!["old".to_owned()])));
        let mut candidate = Elector::new(store.clone(), "new");
        let contending = {
            let gate = gate.clone();
            tokio::spawn(async move {
                candidate.acquire_when(&gate).await;
                Instant::now()
            })
        };
        tokio::time::sleep(LEASE_DURATION.saturating_mul(6)).await;
        assert!(
            !contending.is_finished(),
            "held back for as long as the unelected replica runs"
        );
        assert_eq!(store.holder(), None, "not even a free lease is taken");
        // The unelected replica's pod is gone.
        gate.hold(None);
        let gone = Instant::now();
        let taken = tokio::time::timeout(LEASE_DURATION, contending)
            .await
            .expect("taken once it is gone")
            .expect("task");
        let gap = taken.saturating_duration_since(gone);
        eprintln!("unelected replica gone to lease taken: {gap:?}");
        assert!(gap <= retry_ceiling(), "within one retry: {gap:?}");
        assert_eq!(store.holder().as_deref(), Some("new"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_candidate_that_cannot_list_the_replicas_does_not_contend() {
        let store = Memory::default();
        let gate = Scripted::default();
        gate.hold(Some(crate::unelected::Waiting::Unreadable("forbidden".to_owned())));
        let mut candidate = Elector::new(store.clone(), "new");
        let waited = tokio::time::timeout(LEASE_DURATION.saturating_mul(3), candidate.acquire_when(&gate)).await;
        assert!(waited.is_err(), "fails closed");
        assert_eq!(store.holder(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn two_candidates_one_holder() {
        let store = Memory::default();
        let mut a = Elector::new(store.clone(), "a");
        let mut b = Elector::new(store.clone(), "b");
        a.acquire().await;
        let holding = tokio::spawn(async move { a.hold_until(std::future::pending()).await });
        let contending = tokio::time::timeout(LEASE_DURATION.saturating_mul(6), b.acquire()).await;
        assert!(contending.is_err(), "b never takes a lease a keeps renewing");
        assert_eq!(store.holder().as_deref(), Some("a"));
        holding.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_holder_is_replaced_within_the_duration_and_one_retry() {
        let store = Memory::default();
        let mut a = Elector::new(store.clone(), "a");
        a.acquire().await;
        // b sees the lease as a last wrote it, then a stops renewing.
        let mut b = Elector::new(store.clone(), "b");
        let started = Instant::now();
        tokio::time::timeout(LEASE_DURATION.saturating_mul(3), b.acquire())
            .await
            .expect("b takes the lease");
        let handover = started.elapsed();
        eprintln!("handover after a holder is lost: {handover:?}");
        assert_eq!(store.holder().as_deref(), Some("b"));
        assert!(handover >= LEASE_DURATION, "not before the lease expires: {handover:?}");
        assert!(
            handover <= LEASE_DURATION.saturating_add(retry_ceiling()),
            "within the duration and one retry: {handover:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_released_lease_is_taken_within_one_retry() {
        let store = Memory::default();
        let mut a = Elector::new(store.clone(), "a");
        a.acquire().await;
        let mut b = Elector::new(store.clone(), "b");
        assert!(!b.attempt().await.unwrap_or(true), "a still holds it");
        let contending = tokio::spawn(async move {
            b.acquire().await;
            Instant::now()
        });
        tokio::task::yield_now().await;
        let released = Instant::now();
        a.release().await;
        let taken = tokio::time::timeout(LEASE_DURATION, contending)
            .await
            .expect("taken in time")
            .expect("task");
        let handover = taken.saturating_duration_since(released);
        eprintln!("handover after a release: {handover:?}");
        assert_eq!(store.holder().as_deref(), Some("b"));
        assert!(handover <= retry_ceiling(), "within one retry: {handover:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_holder_that_cannot_renew_counts_itself_lost_before_anyone_can_take_over() {
        let store = Memory::default();
        let mut a = Elector::new(store.clone(), "a");
        a.acquire().await;
        let renewed = Instant::now();
        store.set_down(true);
        let tenure = tokio::time::timeout(LEASE_DURATION, a.hold_until(std::future::pending())).await;
        assert_eq!(tenure.ok(), Some(Tenure::Lost));
        let held_for = renewed.elapsed();
        assert!(held_for <= RENEW_DEADLINE, "gave up by the deadline: {held_for:?}");
        assert!(
            held_for < LEASE_DURATION,
            "before a candidate could take over: {held_for:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_conflict_on_its_own_renewal_does_not_end_the_tenure() {
        let store = Memory::default();
        let mut a = Elector::new(store.clone(), "a");
        a.acquire().await;
        store.bump_next();
        let tenure = a.hold_until(tokio::time::sleep(LEASE_DURATION.saturating_mul(3))).await;
        assert_eq!(
            tenure,
            Tenure::Finished,
            "the late write was its own, so it renews again"
        );
        assert_eq!(store.holder().as_deref(), Some("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_holder_whose_lease_was_taken_counts_itself_lost() {
        let store = Memory::default();
        let mut a = Elector::new(store.clone(), "a");
        a.acquire().await;
        let mut b = Elector::new(store.clone(), "b");
        // b wrote past a, as a holder that stalled past its deadline would find.
        let (record, version) = store.get().await.ok().flatten().expect("present");
        let taken = Record {
            holder: Some("b".to_owned()),
            ..record
        };
        store.put(Some(&version), &taken).await.expect("b writes past a");
        assert!(b.attempt().await.unwrap_or(false), "b renews what it holds");
        let tenure = tokio::time::timeout(LEASE_DURATION, a.hold_until(std::future::pending())).await;
        assert_eq!(tenure.ok(), Some(Tenure::Lost));
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_that_writes_starts_before_the_lease_is_held() {
        let store = Memory::default();
        let mut other = Elector::new(store.clone(), "other");
        other.acquire().await;
        let (_trigger, shutdown) = crate::shutdown::Trigger::new();
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers = Arc::clone(&started);
        let candidate = Elector::new(store.clone(), "me");
        let leading = tokio::spawn(async move {
            let leadership = super::lead(candidate, &crate::unelected::Open, &shutdown, || {}).await;
            writers.store(true, std::sync::atomic::Ordering::SeqCst);
            leadership
        });
        let holding = tokio::spawn(async move {
            other
                .hold_until(tokio::time::sleep(LEASE_DURATION.saturating_mul(3)))
                .await;
            other.release().await;
        });
        tokio::time::sleep(LEASE_DURATION.saturating_mul(3)).await;
        assert!(
            !started.load(std::sync::atomic::Ordering::SeqCst),
            "no writer starts while another replica holds the lease"
        );
        holding.await.expect("holder task");
        let leadership = tokio::time::timeout(LEASE_DURATION, leading)
            .await
            .expect("taken in time")
            .expect("task");
        assert!(
            started.load(std::sync::atomic::Ordering::SeqCst),
            "writers start once it is held"
        );
        assert_eq!(store.holder().as_deref(), Some("me"));
        if let Some(leadership) = leadership {
            leadership.finish().await;
        }
        assert_eq!(store.holder(), None, "finishing releases the lease");
    }

    #[tokio::test(start_paused = true)]
    async fn a_shutdown_before_the_lease_is_held_starts_nothing() {
        let store = Memory::default();
        let mut other = Elector::new(store.clone(), "other");
        other.acquire().await;
        let (trigger, shutdown) = crate::shutdown::Trigger::new();
        trigger.trigger();
        assert!(
            super::lead(Elector::new(store, "me"), &crate::unelected::Open, &shutdown, || {})
                .await
                .is_none()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn losing_the_lease_calls_lost() {
        let store = Memory::default();
        let (_trigger, shutdown) = crate::shutdown::Trigger::new();
        let (told, lost) = tokio::sync::oneshot::channel();
        let leadership = super::lead(
            Elector::new(store.clone(), "me"),
            &crate::unelected::Open,
            &shutdown,
            move || {
                let _sent = told.send(());
            },
        )
        .await
        .expect("present");
        store.set_down(true);
        let heard = tokio::time::timeout(LEASE_DURATION, lost).await;
        assert!(matches!(heard, Ok(Ok(()))), "told within the duration");
        leadership.finish().await;
    }

    /// An apiserver holding one `Lease`, refusing a stale write with 409 as the real one does.
    fn lease_api() -> kube::Client {
        use http_body_util::BodyExt as _;
        let stored = Arc::new(Mutex::new(None::<serde_json::Value>));
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let stored = Arc::clone(&stored);
            async move {
                let respond = |code: u16, body: serde_json::Value| {
                    http::Response::builder()
                        .status(code)
                        .body(kube::client::Body::from(body.to_string().into_bytes()))
                        .expect("response")
                };
                let status = |code: u16, reason: &str| serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Failure","reason":reason,"code":code});
                let version = |lease: &serde_json::Value| lease.pointer("/metadata/resourceVersion").cloned();
                let method = request.method().clone();
                let bytes = request.into_body().collect().await.expect("body").to_bytes();
                let mut held = stored.lock().unwrap_or_else(PoisonError::into_inner);
                let reply = if method == http::Method::GET {
                    held.clone()
                        .map_or_else(|| respond(404, status(404, "NotFound")), |lease| respond(200, lease))
                } else {
                    let mut lease: serde_json::Value = serde_json::from_slice(&bytes).expect("lease");
                    let current = held.as_ref().map(version);
                    let fresh = match current {
                        None => method == http::Method::POST,
                        Some(live) => method == http::Method::PUT && version(&lease) == live,
                    };
                    if fresh {
                        let next = held
                            .as_ref()
                            .and_then(|live| version(live)?.as_str()?.parse::<u64>().ok())
                            .map_or(1, |version| version.saturating_add(1));
                        if let Some(written) = lease.pointer_mut("/metadata/resourceVersion") {
                            *written = serde_json::json!(next.to_string());
                        } else if let Some(metadata) =
                            lease.get_mut("metadata").and_then(serde_json::Value::as_object_mut)
                        {
                            metadata.insert("resourceVersion".to_owned(), serde_json::json!(next.to_string()));
                        }
                        *held = Some(lease.clone());
                        respond(200, lease)
                    } else {
                        respond(409, status(409, "Conflict"))
                    }
                };
                drop(held);
                Ok::<_, std::convert::Infallible>(reply)
            }
        });
        kube::Client::new(service, "grid")
    }

    #[tokio::test(start_paused = true)]
    async fn a_kubernetes_lease_is_taken_by_one_replica_and_refuses_a_stale_write() {
        let client = lease_api();
        let store = || super::KubeLease::new(client.clone(), "grid", "grid-operator");
        let mut a = Elector::new(store(), "a");
        let mut b = Elector::new(store(), "b");
        assert!(a.attempt().await.expect("create"), "a creates and holds the lease");
        assert!(!b.attempt().await.expect("read"), "b sees it held");
        let (record, version) = store().get().await.expect("get").expect("present");
        assert_eq!(record.holder.as_deref(), Some("a"));
        assert_eq!(record.duration, LEASE_DURATION);
        assert!(record.acquired.is_some() && record.renewed.is_some());
        assert!(a.attempt().await.expect("renew"), "a renews");
        assert!(
            matches!(store().put(Some(&version), &record).await, Err(StoreError::Conflict)),
            "a write at a superseded version is refused"
        );
        a.release().await;
        assert_eq!(store().get().await.expect("get").expect("present").0.holder, None);
        assert!(b.attempt().await.expect("take"), "b takes the released lease");
        let taken = store().get().await.expect("get").expect("present").0;
        assert_eq!(taken.holder.as_deref(), Some("b"));
        assert_eq!(taken.transitions, 1, "one change of hands");
    }
}
