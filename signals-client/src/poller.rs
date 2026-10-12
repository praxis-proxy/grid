//! Background poll loop that fills the live-signal [`LoadStore`].
//!
//! A grid gateway is co-located with its operator, so it polls one local
//! endpoint (`/v1/site/signals`) that already carries this site plus every
//! peer the operator relays. The loop owns no transport: it takes a
//! [`SignalSource`], so the mTLS client and the test fake share one
//! path. The operator's response `Date` is the freshness reference passed to
//! [`LoadStore::ingest_at`], so staleness tracks the operator's clock, not the
//! gateway's.

use std::{sync::Arc, time::Duration};

use grid_signals::{Ingested, LoadStore, now_ms};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Deserialize;
use tokio::sync::watch;

/// Default poll interval.
const DEFAULT_INTERVAL_MS: u64 = 1_000;
/// Default retention per series.
const DEFAULT_WINDOW_SECS: u64 = 30;
/// Default liveness bound on a sample.
const DEFAULT_MAX_AGE_MS: i64 = 5_000;
/// Default per-request timeout.
const DEFAULT_TIMEOUT_MS: u64 = 2_000;

/// Everything escaped except the RFC 3986 unreserved set.
const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

/// TLS material the poller presents, by path.
///
/// The gateway reads these to build the client the poll loop runs on.
/// The loop itself never sees them. Absent, the endpoint is polled as a plain
/// client with no grid trust, which an access-enforcing operator refuses.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(clippy::struct_field_names, reason = "the keys the chart renders")]
pub struct PinnedTls {
    /// CA bundle the operator certificate chains to.
    pub ca_path: String,
    /// Certificate presented to the operator. Omitted, the operator refuses an
    /// unidentified caller.
    #[serde(default)]
    pub cert_path: Option<String>,
    /// Private key for `cert_path`.
    #[serde(default)]
    pub key_path: Option<String>,
}

/// How and how often to poll the local operator.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollerConfig {
    /// Signals endpoint of this gateway's local operator, which relays every site it
    /// collects from, each row labeled with the site it verified.
    pub endpoint: String,
    /// Poll interval, in milliseconds. Rejected when zero: the loop hands this
    /// to `tokio::time::interval`, which panics on a zero duration.
    #[serde(default = "default_interval_ms", deserialize_with = "deserialize_interval_ms")]
    pub interval_ms: u64,
    /// Retention per series, in seconds.
    #[serde(default = "default_window_secs")]
    pub window_secs: u64,
    /// Liveness bound, in milliseconds: past this a sample is ignored, so a
    /// dead operator stops pinning routing to values that describe nothing.
    /// Rejected when negative, which would empty the freshness window and mark
    /// every sample stale.
    #[serde(default = "default_max_age_ms", deserialize_with = "deserialize_max_age_ms")]
    pub max_age_ms: i64,
    /// Per-request timeout, in milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// TLS material for the endpoint.
    #[serde(default)]
    pub tls: Option<PinnedTls>,
}

/// Default poll interval.
const fn default_interval_ms() -> u64 {
    DEFAULT_INTERVAL_MS
}

/// Default retention per series.
const fn default_window_secs() -> u64 {
    DEFAULT_WINDOW_SECS
}

/// Default liveness bound on a sample.
const fn default_max_age_ms() -> i64 {
    DEFAULT_MAX_AGE_MS
}

/// Default per-request timeout.
const fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

/// Reject a negative `max_age_ms`. A negative bound empties the freshness range
/// and marks every sample stale, silently disabling load scoring.
fn deserialize_max_age_ms<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = i64::deserialize(deserializer)?;
    if value < 0 {
        return Err(serde::de::Error::custom("max_age_ms must not be negative"));
    }
    Ok(value)
}

/// Reject a zero poll interval at parse time.
///
/// The loop passes `interval_ms` to `tokio::time::interval`, which panics on a
/// zero duration, so a config of `interval_ms: 0` would abort the poller thread
/// on its first tick. Rejecting it here turns that into a config error. Exported
/// so any config carrying a poll interval, including the grid serving config,
/// guards it the same way.
///
/// # Errors
///
/// Returns the deserializer's error if the value is not a `u64` or is zero.
pub fn deserialize_interval_ms<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value == 0 {
        return Err(serde::de::Error::custom("interval_ms must be greater than zero"));
    }
    Ok(value)
}

/// One exposition scrape: the body, and the operator's response `Date` in epoch
/// milliseconds.
#[derive(Clone, Debug)]
pub struct Scrape {
    /// Prometheus exposition text.
    pub body: String,
    /// The operator's own clock at response time, the reference for freshness.
    pub date_ms: i64,
    /// The crypto-verified peer identity the transport surfaced: the SPIFFE URI
    /// under Spiffe trust, or the matched pin's site under Pin trust. Ingest
    /// binds `grid_site` to it and drops a self-reported label that disagrees.
    pub peer_identity: Arc<str>,
}

/// Why one scrape produced no usable body. Every variant is retried next tick,
/// and the store keeps what it held until `max_age_ms` expires it.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// The request did not complete: unreachable, refused, timed out, or the
    /// operator is draining.
    #[error("signals endpoint unreachable: {0}")]
    Unreachable(String),
    /// The response body exceeded the read ceiling and was discarded.
    #[error("signals response exceeded {limit} bytes")]
    TooLarge {
        /// The ceiling that was exceeded.
        limit: usize,
    },
    /// The response carried no usable `Date`, so freshness has no reference.
    #[error("signals response had no usable Date header")]
    NoDate,
    /// The peer's verified identity was absent or was not the dialed target. A
    /// distinct variant so an authorization refusal is observable, not confused
    /// with a transport failure.
    #[error("signals peer not authorized: {0}")]
    Unauthorized(String),
}

/// A source of one exposition scrape from the local operator.
///
/// Implemented once over the mTLS client the gateway builds, and once as
/// a fake in tests. A bound rather than a trait object, so the call is static
/// dispatch and the loop allocates nothing per poll.
pub trait SignalSource {
    /// Fetch the current exposition and the operator's response `Date`.
    fn fetch(&self) -> impl Future<Output = Result<Scrape, FetchError>> + Send;
}

/// Append `collect[]` parameters for each metric name, percent-encoded.
///
/// A configured `?target=` on `endpoint` is preserved: the separator picks `&`
/// once a query is already present.
#[must_use]
pub fn build_url(endpoint: &str, collect: &[String]) -> String {
    if collect.is_empty() {
        return endpoint.to_owned();
    }
    let query = collect
        .iter()
        .map(|name| format!("collect[]={}", utf8_percent_encode(name, QUERY_VALUE)))
        .collect::<Vec<_>>()
        .join("&");
    let separator = if endpoint.contains('?') { '&' } else { '?' };
    format!("{endpoint}{separator}{query}")
}

/// A running poll loop, stopped on drop.
#[derive(Debug)]
pub struct PollHandle {
    /// Sends the stop signal the loop selects on.
    stop: watch::Sender<bool>,

    /// Opens the store to the loop. A held poller fetches but writes nothing until committed.
    commit: watch::Sender<bool>,
}

impl PollHandle {
    /// Let a held poller write the store and run its cycle hook.
    pub fn commit(&self) {
        self.commit.send_replace(true);
    }
}

impl Drop for PollHandle {
    fn drop(&mut self) {
        // A closed receiver means the loop already exited; nothing to stop.
        _ = self.stop.send(true);
    }
}

/// Spawn a poll loop on the current runtime, feeding `store` from `source`.
///
/// The returned handle stops the loop when dropped. The first poll fires
/// immediately. Thereafter a slow poll delays the next tick rather than
/// overlapping it, so a stalled operator never stacks in-flight requests.
pub fn spawn<S>(store: Arc<LoadStore>, source: S, interval: Duration) -> PollHandle
where
    S: SignalSource + Send + Sync + 'static,
{
    let (stop, rx) = watch::channel(false);
    let (commit, gate) = watch::channel(true);
    tokio::spawn(poll_loop(store, source, interval, (rx, gate), |_store: &LoadStore| {}));
    PollHandle { stop, commit }
}

/// Spawn a poll loop from a [`PollerConfig`], feeding `store` from `source`.
///
/// The gateway builds `store` sized to `config.window_secs` and `source` from
/// `config.tls`, then hands both here so the loop reads its cadence from one
/// place. The freshness bound (`max_age_ms`) and request timeout are read by
/// the store query and the client, not the loop.
pub fn spawn_from_config<S>(store: Arc<LoadStore>, config: &PollerConfig, source: S) -> PollHandle
where
    S: SignalSource + Send + Sync + 'static,
{
    spawn(store, source, Duration::from_millis(config.interval_ms))
}

/// Spawn a poll loop on its own thread and runtime, for a caller that has no
/// ambient tokio runtime.
///
/// The grid gateway builds its registry and then hands the process to Pingora
/// through a diverging `run_server_with_registry`, so at the wiring seam there
/// is no runtime to [`spawn`] onto. This owns a dedicated single-threaded
/// runtime running only the poll loop. The store it fills is read from the
/// serving runtime, which is safe because the store is a concurrent map.
///
/// The returned handle stops the loop and lets the thread wind down when
/// dropped, so the caller must hold it for as long as the store should keep
/// filling. A thread that cannot build its runtime logs and leaves the store
/// unfilled, so routing falls back to static candidate order rather than
/// failing the gateway.
///
/// # Errors
///
/// Returns the OS error if the poller thread cannot be spawned.
pub fn spawn_on_thread<S, F>(
    store: Arc<LoadStore>,
    config: &PollerConfig,
    source: S,
    on_cycle: F,
) -> std::io::Result<PollHandle>
where
    S: SignalSource + Send + Sync + 'static,
    F: Fn(&LoadStore) + Send + 'static,
{
    spawn_thread(store, config, source, on_cycle, true)
}

/// [`spawn_on_thread`], held: the loop fetches but writes nothing and runs no
/// cycle hook until [`PollHandle::commit`]. A reload starts its new pollers
/// held, so a reload that is then rejected leaves the store untouched.
///
/// # Errors
///
/// Returns the OS error if the poller thread cannot be spawned.
pub fn spawn_on_thread_held<S, F>(
    store: Arc<LoadStore>,
    config: &PollerConfig,
    source: S,
    on_cycle: F,
) -> std::io::Result<PollHandle>
where
    S: SignalSource + Send + Sync + 'static,
    F: Fn(&LoadStore) + Send + 'static,
{
    spawn_thread(store, config, source, on_cycle, false)
}

/// Spawn the loop on its own thread, its writes open when `committed`.
fn spawn_thread<S, F>(
    store: Arc<LoadStore>,
    config: &PollerConfig,
    source: S,
    on_cycle: F,
    committed: bool,
) -> std::io::Result<PollHandle>
where
    S: SignalSource + Send + Sync + 'static,
    F: Fn(&LoadStore) + Send + 'static,
{
    let (stop, rx) = watch::channel(false);
    let (commit, gate) = watch::channel(committed);
    let interval = Duration::from_millis(config.interval_ms);
    // The JoinHandle is dropped, detaching the thread on purpose. Cancellation
    // is the watch channel: the handle's drop stops the loop and the thread
    // winds down on its own. Joining on drop would block the caller if the loop
    // were mid-poll, a worse failure than a brief detached wind-down. A panic in
    // the loop aborts the process under the release panic=abort profile, so a
    // dead poller surfaces as a restart rather than silent stale serving.
    std::thread::Builder::new()
        .name("signals-poller".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::error!(%error, "signals poller runtime unavailable; load store will not fill");
                    return;
                },
            };
            runtime.block_on(poll_loop(store, source, interval, (rx, gate), on_cycle));
        })?;
    Ok(PollHandle { stop, commit })
}

/// Poll until stopped, feeding every response into `store` and running
/// `on_cycle` after each poll.
///
/// `on_cycle` runs on the poll cycle, right after the store is updated, so a
/// caller can rebuild derived state (the routing snapshot) on one clock with no
/// drift between "store updated" and "state rebuilt". It stays generic over the
/// store: the poller never names the caller's derived type.
async fn poll_loop<S, F>(
    store: Arc<LoadStore>,
    source: S,
    interval: Duration,
    (mut stop, mut gate): (watch::Receiver<bool>, watch::Receiver<bool>),
    on_cycle: F,
) where
    S: SignalSource + Send + Sync,
    F: Fn(&LoadStore) + Send,
{
    let mut ticker = tokio::time::interval(interval);
    let mut failing = false;
    // A slow poll delays the next tick instead of firing a burst to catch up.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            result = stop.changed() => {
                // A change is only ever the stop signal; a dropped sender ends
                // the loop the same way.
                if result.is_err() || *stop.borrow() {
                    return;
                }
            },
            _ = ticker.tick() => {
                // Stopping cancels an in-flight fetch, so a dropped poller writes nothing late.
                let fetched = tokio::select! {
                    biased;
                    _ = stop.changed() => return,
                    fetched = source.fetch() => fetched,
                };
                // A held poller waits for its reload to commit; a dropped handle ends it here.
                if gate.wait_for(|open| *open).await.is_err() || *stop.borrow() {
                    return;
                }
                absorb(fetched, &store, &mut failing);
                on_cycle(&store);
            },
        }
    }
}

/// Absorb one fetch, counting the outcome and logging only the transitions in and
/// out of failure.
fn absorb(fetched: Result<Scrape, FetchError>, store: &LoadStore, failing: &mut bool) {
    let result = match fetched {
        Ok(scrape) => ingest(&scrape, store),
        Err(error) => Err((outcome(&error), error.to_string())),
    };
    let label = result.as_ref().map_or_else(|(label, _)| *label, |_| "ok");
    metrics::counter!("grid_signals_poll_total", "result" => label).increment(1);
    match (result, *failing) {
        (Ok(ingested), true) => {
            *failing = false;
            tracing::info!(kept = ingested.kept, "signals poll recovered");
        },
        (Ok(_), false) => {},
        (Err((_, error)), true) => tracing::debug!(error, "signals poll still failing; keeping last values"),
        (Err((_, error)), false) => {
            *failing = true;
            tracing::warn!(error, "signals poll failing; keeping last values");
        },
    }
}

/// Store one scrape and record what it carried. Attribution keys on the
/// crypto-verified owner from the scrape, never the payload's self-reported
/// `grid_site` (the #160 binding).
fn ingest(scrape: &Scrape, store: &LoadStore) -> Result<Ingested, (&'static str, String)> {
    let Some(owner) = certs::site_of_spiffe_id(&scrape.peer_identity) else {
        return Err((
            "bad_identity",
            format!("scrape peer id {} is not a grid site id", scrape.peer_identity),
        ));
    };
    let ingested = store.ingest_at(&scrape.body, scrape.date_ms, now_ms(), owner);
    let now_secs = u32::try_from(now_ms().saturating_div(1_000)).unwrap_or(u32::MAX);
    metrics::gauge!("grid_signals_last_success_timestamp_seconds").set(f64::from(now_secs));
    let bytes = u32::try_from(scrape.body.len()).unwrap_or(u32::MAX);
    metrics::gauge!("grid_signals_response_bytes").set(f64::from(bytes));
    for (reason, count) in ingested.dropped() {
        if count > 0 {
            metrics::counter!("grid_signals_ingest_dropped_total", "reason" => reason).increment(count);
        }
    }
    Ok(ingested)
}

/// The counter label for a failed fetch.
const fn outcome(error: &FetchError) -> &'static str {
    match error {
        FetchError::Unreachable(_) => "unreachable",
        FetchError::TooLarge { .. } => "too_large",
        FetchError::NoDate => "no_date",
        FetchError::Unauthorized(_) => "unauthorized",
    }
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, clippy::float_cmp, clippy::min_ident_chars, reason = "tests")]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    fn cfg(yaml: &str) -> Result<PollerConfig, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[test]
    fn defaults_fill_when_only_endpoint_is_given() {
        let c = cfg("endpoint: https://operator:9091/v1/site/signals\n").expect("parses");
        assert_eq!(c.interval_ms, DEFAULT_INTERVAL_MS);
        assert_eq!(c.window_secs, DEFAULT_WINDOW_SECS);
        assert_eq!(c.max_age_ms, DEFAULT_MAX_AGE_MS);
        assert_eq!(c.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert!(c.tls.is_none());
    }

    #[test]
    fn a_negative_max_age_is_rejected_at_parse() {
        let err = cfg("endpoint: https://o/s\nmax_age_ms: -1\n").expect_err("must reject");
        assert!(err.to_string().contains("must not be negative"), "{err}");
    }

    #[test]
    fn a_zero_interval_is_rejected_at_parse() {
        let err = cfg("endpoint: https://o/s\ninterval_ms: 0\n").expect_err("must reject");
        assert!(err.to_string().contains("must be greater than zero"), "{err}");
    }

    #[test]
    fn an_unknown_field_is_rejected() {
        assert!(cfg("endpoint: https://o/s\nbogus: 1\n").is_err(), "deny_unknown_fields");
    }

    #[test]
    fn tls_paths_parse_and_pins_are_refused() {
        let c = cfg(
            "endpoint: https://operator:9091/v1/site/signals\ntls:\n  ca_path: /tls/ca.crt\n  cert_path: /tls/tls.crt\n  key_path: /tls/tls.key\n",
        )
        .expect("parses");
        let tls = c.tls.expect("tls present");
        assert_eq!(tls.ca_path, "/tls/ca.crt");
        assert_eq!(tls.cert_path.as_deref(), Some("/tls/tls.crt"));
        assert!(
            cfg("endpoint: https://o/s\ntls:\n  ca_path: /ca\n  pins:\n    - abc123\n").is_err(),
            "pins are not a poller setting"
        );
    }

    #[test]
    fn a_poll_outcome_is_named_and_the_failing_state_follows_transitions() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut failing = false;
        absorb(Err(FetchError::NoDate), &store, &mut failing);
        assert!(failing, "the first failure is a transition");
        assert_eq!(outcome(&FetchError::TooLarge { limit: 1 }), "too_large");
        assert_eq!(outcome(&FetchError::Unauthorized(String::new())), "unauthorized");
        assert_eq!(outcome(&FetchError::Unreachable(String::new())), "unreachable");
        let scrape = Scrape {
            body: format!(r#"m{{grid_site="east",grid_provider="p"}} 1 {}"#, now_ms()),
            date_ms: now_ms(),
            peer_identity: Arc::from(certs::spiffe_id("east").as_str()),
        };
        absorb(Ok(scrape), &store, &mut failing);
        assert!(!failing, "a success clears it");
        assert!(
            store.latest(&LoadStore::key("east", "p"), "m").is_some(),
            "the scrape was stored"
        );
    }

    #[test]
    fn build_url_appends_encoded_collect_params() {
        let url = build_url("https://o/s", &["queue_depth".to_owned(), "a b".to_owned()]);
        assert_eq!(url, "https://o/s?collect[]=queue_depth&collect[]=a%20b");
    }

    #[test]
    fn build_url_uses_ampersand_when_target_already_present() {
        let url = build_url("https://o/s?target=east", &["queue_depth".to_owned()]);
        assert_eq!(url, "https://o/s?target=east&collect[]=queue_depth");
    }

    #[test]
    fn build_url_without_collect_is_unchanged() {
        assert_eq!(build_url("https://o/s", &[]), "https://o/s");
    }

    /// A fake source: hands out canned scrapes, counts fetches, and can fail.
    struct FakeSource {
        scrapes: Mutex<Vec<Result<Scrape, FetchError>>>,
        calls: AtomicUsize,
    }

    impl SignalSource for FakeSource {
        async fn fetch(&self) -> Result<Scrape, FetchError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut scrapes = self.scrapes.lock().expect("lock");
            if scrapes.is_empty() {
                return Err(FetchError::Unreachable("drained".to_owned()));
            }
            scrapes.remove(0)
        }
    }

    fn line(site: &str, provider: &str, value: f64, at_ms: i64) -> String {
        format!("queue_depth{{grid_site=\"{site}\",grid_provider=\"{provider}\"}} {value} {at_ms}")
    }

    #[tokio::test(start_paused = true)]
    async fn a_scrape_lands_in_the_store() {
        let store = Arc::new(LoadStore::new(Duration::from_secs(30)));
        let source = FakeSource {
            scrapes: Mutex::new(vec![Ok(Scrape {
                body: line("east", "pool-a", 3.0, 1_000),
                date_ms: 1_000,
                peer_identity: Arc::from("spiffe://grid.internal/site/east"),
            })]),
            calls: AtomicUsize::new(0),
        };
        let handle = spawn(Arc::clone(&store), source, Duration::from_millis(10));
        // Let the immediate first tick run.
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(
            store
                .window_worst(&LoadStore::key("east", "pool-a"), "queue_depth", now_ms(), 5_000, true)
                .is_some(),
            "the scraped sample should be queryable"
        );
        drop(handle);
    }

    #[tokio::test]
    async fn a_spoofed_site_in_the_body_is_not_attributed() {
        // The peer is verified as "east" but its body claims "west" (the #160
        // cross-site spoof). the poll keys on the verified owner, so the west
        // series must never appear.
        let store = Arc::new(LoadStore::new(Duration::from_secs(30)));
        let source = FakeSource {
            scrapes: Mutex::new(vec![Ok(Scrape {
                body: line("west", "pool-a", 9.0, 1_000),
                date_ms: 1_000,
                peer_identity: Arc::from("spiffe://grid.internal/site/east"),
            })]),
            calls: AtomicUsize::new(0),
        };
        let handle = spawn(Arc::clone(&store), source, Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(
            store
                .window_worst(&LoadStore::key("west", "pool-a"), "queue_depth", now_ms(), 5_000, true)
                .is_none(),
            "the spoofed west series must not exist"
        );
        assert!(
            store
                .window_worst(&LoadStore::key("east", "pool-a"), "queue_depth", now_ms(), 5_000, true)
                .is_none(),
            "and it is not silently rebound to east either"
        );
        drop(handle);
    }

    /// Reports east once released, counting fetches that began.
    struct GatedSource {
        release: Arc<tokio::sync::Notify>,
        entered: Arc<AtomicUsize>,
    }

    impl SignalSource for GatedSource {
        async fn fetch(&self) -> Result<Scrape, FetchError> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.release.notified().await;
            Ok(Scrape {
                body: line("east", "pool-a", 3.0, now_ms()),
                date_ms: now_ms(),
                peer_identity: Arc::from("spiffe://grid.internal/site/east"),
            })
        }
    }

    fn east(store: &LoadStore) -> Option<f64> {
        store.window_worst(&LoadStore::key("east", "pool-a"), "queue_depth", now_ms(), 60_000, true)
    }

    #[tokio::test]
    async fn a_dropped_poller_ignores_its_in_flight_fetch() {
        let store = Arc::new(LoadStore::new(Duration::from_secs(30)));
        let (release, entered) = (Arc::new(tokio::sync::Notify::new()), Arc::new(AtomicUsize::new(0)));
        let source = GatedSource {
            release: Arc::clone(&release),
            entered: Arc::clone(&entered),
        };
        let handle = spawn(Arc::clone(&store), source, Duration::from_millis(10));
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        drop(handle);
        release.notify_waiters();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(east(&store), None, "a fetch finishing after the drop writes nothing");
    }

    /// Reports east on every fetch, counting fetches.
    struct CountingSource {
        fetches: Arc<AtomicUsize>,
    }

    impl SignalSource for CountingSource {
        async fn fetch(&self) -> Result<Scrape, FetchError> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(Scrape {
                body: line("east", "pool-a", 3.0, now_ms()),
                date_ms: now_ms(),
                peer_identity: Arc::from("spiffe://grid.internal/site/east"),
            })
        }
    }

    /// Spin until `done`, failing after five seconds.
    fn spin_until(what: &str, done: impl Fn() -> bool) {
        let deadline = std::time::Instant::now()
            .checked_add(Duration::from_secs(5))
            .expect("deadline");
        while !done() {
            assert!(std::time::Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::yield_now();
        }
    }

    /// A held poller on its own thread over a fresh store, with its fetch count.
    fn held_poller() -> (Arc<LoadStore>, Arc<AtomicUsize>, PollHandle) {
        let cfg: PollerConfig =
            serde_yaml::from_str("endpoint: https://operator:9091/v1/site/signals\ninterval_ms: 5\n").expect("cfg");
        let store = Arc::new(LoadStore::new(Duration::from_secs(30)));
        let fetches = Arc::new(AtomicUsize::new(0));
        let source = CountingSource {
            fetches: Arc::clone(&fetches),
        };
        let handle = spawn_on_thread_held(Arc::clone(&store), &cfg, source, |_store: &LoadStore| {}).expect("spawn");
        (store, fetches, handle)
    }

    #[test]
    fn a_held_poller_writes_only_once_committed() {
        let (store, fetches, handle) = held_poller();
        spin_until("a fetch while held", || fetches.load(Ordering::SeqCst) > 0);
        assert_eq!(east(&store), None, "held: the fetch is not written");
        handle.commit();
        spin_until("the committed write", || east(&store).is_some());
    }

    #[test]
    fn a_held_poller_dropped_before_commit_writes_nothing() {
        let (store, fetches, handle) = held_poller();
        spin_until("a fetch while held", || fetches.load(Ordering::SeqCst) > 0);
        drop(handle);
        assert_eq!(east(&store), None, "a rejected reload's poller never writes");
    }

    #[test]
    fn spawn_on_thread_fills_the_store_with_no_ambient_runtime() {
        // A plain test has no tokio runtime, exactly like the gateway seam.
        let store = Arc::new(LoadStore::new(Duration::from_secs(30)));
        let source = FakeSource {
            scrapes: Mutex::new(vec![Ok(Scrape {
                body: line("east", "pool-a", 3.0, 1_000),
                date_ms: 1_000,
                peer_identity: Arc::from("spiffe://grid.internal/site/east"),
            })]),
            calls: AtomicUsize::new(0),
        };
        let cfg: PollerConfig =
            serde_yaml::from_str("endpoint: https://operator:9091/v1/site/signals\ninterval_ms: 5\n").expect("cfg");
        let handle = spawn_on_thread(Arc::clone(&store), &cfg, source, |_store| {}).expect("poller thread spawns");
        // Poll until the sample lands, bounded by a deadline and yielding to the
        // poller thread rather than sleeping a fixed span.
        let key = LoadStore::key("east", "pool-a");
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut found = false;
        while std::time::Instant::now() < deadline {
            if store.window_worst(&key, "queue_depth", now_ms(), 5_000, true).is_some() {
                found = true;
                break;
            }
            std::thread::yield_now();
        }
        drop(handle);
        assert!(found, "spawn_on_thread should fill the store from its own runtime");
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_fetch_does_not_wedge_the_loop() {
        let store = Arc::new(LoadStore::new(Duration::from_secs(30)));
        let source = FakeSource {
            scrapes: Mutex::new(vec![
                Err(FetchError::Unreachable("boom".to_owned())),
                Ok(Scrape {
                    body: line("east", "pool-a", 7.0, 2_000),
                    date_ms: 2_000,
                    peer_identity: Arc::from("spiffe://grid.internal/site/east"),
                }),
            ]),
            calls: AtomicUsize::new(0),
        };
        let handle = spawn(Arc::clone(&store), source, Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(11)).await;
        tokio::task::yield_now().await;
        assert!(
            store
                .window_worst(&LoadStore::key("east", "pool-a"), "queue_depth", now_ms(), 5_000, true)
                .is_some(),
            "the loop recovers and ingests after a failed tick"
        );
        drop(handle);
    }
}
