//! Revision discontinuities exercised through the real store, snapshot and selector.

use std::collections::BTreeSet;

use super::{Harness, KeepAll, LoadStore, MODEL, NOW_MS, REFRESH_MS, RouteSnapshot, SiteSpec, WINDOW_MS};
use crate::{descriptor::CapabilityKind, route::select_spread};

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "each failure mode exercises disable, replay and recovery together"
)]
fn learning_recovers_after_each_disable_condition_without_resurrecting_old_state() {
    for cause in ["missing", "expired", "invalid", "incomplete", "invalidation"] {
        let mut grid = learned();
        let at = NOW_MS + if cause == "expired" { WINDOW_MS + 1 } else { REFRESH_MS };
        let mut revision = Revision { sequence: 2, ..INITIAL };
        match cause {
            "missing" => publish(&grid, "a", at, None, Some((20.0, 0.0))),
            "expired" => {},
            "invalid" => {
                let invalid = batch("a", at, Some(revision), Some((20.0, 0.0))).replacen("} 1 ", "} 0 ", 1);
                grid.store.ingest_at(&invalid, at, at, "a");
            },
            "incomplete" => {
                revision.generation = 2;
                let complete = batch("a", at, Some(revision), Some((20.0, 0.0)));
                let metadata = complete.lines().next().expect("metadata first");
                grid.store.ingest_at(metadata, at, at, "a");
            },
            "invalidation" => publish(&grid, "a", at, Some(revision), None),
            _ => unreachable!(),
        }
        assert_eq!(
            capacity(&grid.order(&SITES, at), "a"),
            None,
            "{cause} disables learning"
        );
        grid.store
            .ingest_at(&batch("a", NOW_MS, Some(INITIAL), Some((80.0, 0.0))), at, at, "a");
        assert_eq!(
            capacity(&grid.order(&SITES, at), "a"),
            None,
            "{cause}: replay cannot re-enable learning"
        );
        if matches!(cause, "incomplete" | "invalidation") {
            publish(&grid, "a", at, Some(revision), Some((800.0, 0.0)));
            assert_eq!(
                capacity(&grid.order(&SITES, at), "a"),
                None,
                "{cause}: same-order payload cannot repair an invalidation"
            );
        }
        revision.sequence = 3;
        publish(&grid, "a", at + REFRESH_MS, Some(revision), Some((20.0, 0.0)));
        let recovered = grid.order(&SITES, at + REFRESH_MS);
        assert_eq!(
            capacity(&recovered, "a"),
            Some(20.0),
            "{cause}: later valid data resumes fresh learning"
        );
        assert!(
            candidate(&recovered, "a").rho.is_some_and(|rho| rho < 0.2),
            "{cause}: no old smoothing state"
        );
        assert!(
            routed_to(&recovered, "a") > 0,
            "{cause}: recovered provider receives requests"
        );
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the scenario covers the complete shared-model shedding lifecycle"
)]
fn revision_reset_releases_old_shedding_for_every_model_sharing_the_provider() {
    let mut grid = learned();
    grid.availability.shedding = true;
    let mut shedding = BTreeSet::new();
    for turn in 1..=10 {
        let at = NOW_MS + turn * REFRESH_MS;
        publish_hot(
            &grid,
            at,
            Revision {
                sequence: u64::try_from(turn).expect("positive turn") + 1,
                ..INITIAL
            },
        );
        shedding = order_models(&mut grid, at).shed(&shedding, &grid.availability).shedding;
    }
    assert_eq!(shedding.len(), 2, "both models were shedding before replacement");
    let at = NOW_MS + 11 * REFRESH_MS;
    publish_hot(
        &grid,
        at,
        Revision {
            uid: "replacement",
            sequence: 12,
            ..INITIAL
        },
    );
    let reset = order_models(&mut grid, at).shed(&shedding, &grid.availability);
    assert!(
        reset.shedding.is_empty(),
        "one new congested sample cannot inherit either old latch"
    );
    shedding = reset.shedding;
    for turn in 1..=6 {
        let next = at + turn * REFRESH_MS;
        publish_hot(
            &grid,
            next,
            Revision {
                uid: "replacement",
                sequence: 12 + u64::try_from(turn).expect("positive turn"),
                ..INITIAL
            },
        );
        shedding = order_models(&mut grid, next)
            .shed(&shedding, &grid.availability)
            .shedding;
        if turn < 5 {
            assert!(
                shedding.is_empty(),
                "new revision must establish its own congestion duration"
            );
        }
    }
    assert_eq!(shedding.len(), 2, "sustained new congestion can enable shedding again");
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the race setup and both snapshot assertions belong to one scenario"
)]
fn concurrent_revision_between_metric_reads_cannot_mix_epochs() {
    let mut grid = learned();
    let at = NOW_MS + REFRESH_MS;
    let inputs = ReplacingSignals {
        store: &grid.store,
        replaced: std::cell::Cell::new(false),
        at,
        generation: 2,
    };
    let validated = super::validate_serving_candidates(vec![super::config(&SITES[0])]).expect("valid candidate");
    let snapshot = RouteSnapshot::from_store(
        validated,
        std::sync::Arc::from("a"),
        &mut super::Inputs {
            signals: &inputs,
            now_ms: at,
            window_ms: WINDOW_MS,
            horizon_ms: WINDOW_MS,
            availability: &grid.availability,
            learned: &mut grid.gauged.learned,
        },
    );
    assert_eq!(
        capacity(&snapshot, "a"),
        None,
        "a cross-publication read cannot teach a ceiling"
    );
    assert_eq!(
        candidate(&snapshot, "a").backlog,
        None,
        "the mixed reading is discarded as a whole"
    );
    assert_eq!(
        capacity(&grid.order(&SITES, at), "a"),
        Some(20.0),
        "the next consistent read uses only the new revision"
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the race setup and recovered learning belong to one scenario"
)]
fn concurrent_publication_of_the_same_revision_preserves_learning() {
    let mut grid = learned();
    let at = NOW_MS + WINDOW_MS + 1;
    publish(
        &grid,
        "a",
        at - REFRESH_MS,
        Some(Revision { sequence: 2, ..INITIAL }),
        Some((20.0, 0.0)),
    );
    let signals = ReplacingSignals {
        store: &grid.store,
        replaced: std::cell::Cell::new(false),
        at,
        generation: 1,
    };
    let validated = super::validate_serving_candidates(vec![super::config(&SITES[0])]).expect("valid candidate");
    let torn = RouteSnapshot::from_store(
        validated,
        std::sync::Arc::from("a"),
        &mut super::Inputs {
            signals: &signals,
            now_ms: at,
            window_ms: WINDOW_MS,
            horizon_ms: WINDOW_MS,
            availability: &grid.availability,
            learned: &mut grid.gauged.learned,
        },
    );
    assert_eq!(capacity(&torn, "a"), None, "a torn read publishes no measurement");
    assert_eq!(
        capacity(&grid.order(&SITES[..1], at), "a"),
        Some(80.0),
        "the old peak has left the raw window, but a same-revision race cannot discard its learned ceiling"
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "both bound histories must stay qualified through withdrawal and recovery"
)]
fn guarded_withdrawal_discards_learning_while_fresh_topology_retains_it() {
    for queued in [0.0, 4.0] {
        let mut grid = learned();
        let mut sequence = 1;
        for turn in 1..=10 {
            sequence += 1;
            let at = NOW_MS + turn * REFRESH_MS;
            let text = batch(
                "a",
                at,
                Some(Revision { sequence, ..INITIAL }),
                Some((80.0 - queued, queued)),
            )
            .replace("} 0.1 ", "} 1 ");
            grid.store.ingest_at(&text, at, at, "a");
            grid.order(&SITES[..1], at);
        }
        let low_at = NOW_MS + 10 * REFRESH_MS;
        for turn in 1..=(WINDOW_MS / REFRESH_MS + 2) {
            sequence += 1;
            publish(
                &grid,
                "a",
                low_at + turn * REFRESH_MS,
                Some(Revision { sequence, ..INITIAL }),
                Some((20.0, 0.0)),
            );
            grid.order(&SITES[..1], low_at + turn * REFRESH_MS);
        }
        let at = low_at + (WINDOW_MS / REFRESH_MS + 2) * REFRESH_MS;
        assert_eq!(
            capacity(&grid.order(&SITES[..1], at), "a"),
            Some(80.0),
            "fresh qualification retains the queued={queued} bound"
        );
        assert!(grid.order(&[], at).candidates.is_empty(), "withdrawal removes topology");
        assert_eq!(
            capacity(&grid.order(&SITES[..1], at), "a"),
            Some(20.0),
            "re-admission relearns from current samples for queued={queued}"
        );
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "stable revision, invalidation and recovery share one learned history"
)]
fn unchanged_revision_retains_learning_but_new_generation_retires_it() {
    let mut grid = learned();
    for turn in 1..=20 {
        let at = NOW_MS + turn * REFRESH_MS;
        publish(
            &grid,
            "a",
            at,
            Some(Revision {
                sequence: u64::try_from(turn).expect("positive turn") + 1,
                ..INITIAL
            }),
            Some((20.0, 0.0)),
        );
        publish(
            &grid,
            "b",
            at,
            Some(Revision {
                sequence: u64::try_from(turn).expect("positive turn") + 1,
                ..INITIAL
            }),
            Some((20.0, 0.0)),
        );
        let snapshot = grid.order(&SITES, at);
        assert_eq!(
            capacity(&snapshot, "a"),
            Some(80.0),
            "a publication is not a config change"
        );
    }
    let at = NOW_MS + 21 * REFRESH_MS;
    publish(
        &grid,
        "a",
        at,
        Some(Revision {
            generation: 2,
            sequence: 22,
            ..INITIAL
        }),
        None,
    );
    let reset = grid.order(&SITES, at);
    assert_eq!(capacity(&reset, "a"), None, "metadata alone retires the old ceiling");
    assert_eq!(capacity(&reset, "b"), Some(80.0), "unrelated providers retain learning");
    publish(
        &grid,
        "a",
        at + REFRESH_MS,
        Some(Revision {
            generation: 2,
            sequence: 23,
            ..INITIAL
        }),
        Some((20.0, 0.0)),
    );
    let recovered = grid.order(&SITES, at + REFRESH_MS);
    assert_eq!(
        capacity(&recovered, "a"),
        Some(20.0),
        "old load windows cannot teach the new revision"
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the ordering scenario compares complete snapshots around delayed data"
)]
fn an_unseen_old_uid_cannot_replace_a_new_publisher() {
    let mut grid = guarded();
    publish(
        &grid,
        "a",
        NOW_MS,
        Some(Revision {
            uid: "replacement",
            epoch: 2,
            ..INITIAL
        }),
        Some((20.0, 0.0)),
    );
    publish(&grid, "b", NOW_MS, Some(INITIAL), Some((20.0, 0.0)));
    let before = grid.order(&SITES, NOW_MS);
    publish(
        &grid,
        "a",
        NOW_MS + REFRESH_MS,
        Some(Revision {
            uid: "unseen-old",
            sequence: u64::MAX,
            ..INITIAL
        }),
        Some((800.0, 8.0)),
    );
    let after = grid.order(&SITES, NOW_MS + REFRESH_MS);
    assert_eq!(
        capacity(&after, "a"),
        Some(20.0),
        "UID novelty is not publication order"
    );
    assert_eq!(
        routed_to(&after, "a"),
        routed_to(&before, "a"),
        "late data cannot poison pressure either"
    );
}

#[test]
fn lower_generation_with_newer_sequence_cannot_roll_back_learning() {
    let mut grid = guarded();
    publish(
        &grid,
        "a",
        NOW_MS,
        Some(Revision {
            generation: 2,
            ..INITIAL
        }),
        Some((20.0, 0.0)),
    );
    grid.order(&SITES, NOW_MS);
    publish(
        &grid,
        "a",
        NOW_MS + REFRESH_MS,
        Some(Revision { sequence: 2, ..INITIAL }),
        Some((800.0, 0.0)),
    );
    assert_eq!(capacity(&grid.order(&SITES, NOW_MS + REFRESH_MS), "a"), Some(20.0));
}

#[test]
fn missing_identity_never_activates_legacy_learning() {
    let mut grid = guarded();
    publish(&grid, "a", NOW_MS, None, Some((80.0, 0.0)));
    publish(&grid, "b", NOW_MS, None, Some((80.0, 4.0)));
    let snapshot = grid.order(&SITES, NOW_MS);
    assert!(snapshot.candidates.iter().all(|site| site.capacity.is_none()));
    assert_eq!(
        routed_to(&snapshot, "a"),
        DRAWS,
        "fresh queue pressure needs no revision identity"
    );
}

#[test]
fn disappearing_metadata_retires_learning_without_losing_fresh_pressure() {
    let mut grid = learned();
    let at = NOW_MS + REFRESH_MS;
    publish(&grid, "a", at, None, Some((20.0, 0.0)));
    publish(&grid, "b", at, None, Some((20.0, 4.0)));
    let absent = grid.order(&SITES, at);
    assert!(absent.candidates.iter().all(|site| site.capacity.is_none()));
    assert_eq!(routed_to(&absent, "a"), DRAWS);
    publish(
        &grid,
        "a",
        at + REFRESH_MS,
        Some(Revision { sequence: 3, ..INITIAL }),
        Some((20.0, 0.0)),
    );
    let recovered = grid.order(&SITES, at + REFRESH_MS);
    assert_eq!(
        capacity(&recovered, "a"),
        Some(20.0),
        "returning identity cannot revive retired learning"
    );
}

#[test]
fn replay_does_not_refresh_metadata_or_load() {
    let mut grid = learned();
    let at = NOW_MS + WINDOW_MS + 1;
    let replay = batch("a", NOW_MS, Some(INITIAL), Some((80.0, 0.0)));
    grid.store.ingest_at(&replay, at, at, "a");
    let expired = grid.order(&SITES, at);
    assert_eq!(
        capacity(&expired, "a"),
        None,
        "an exact replay cannot keep a ceiling alive"
    );
    assert_eq!(
        candidate(&expired, "a").backlog,
        None,
        "replay cannot freshen payload timestamps"
    );
}

#[test]
fn metadata_only_does_not_freshen_stale_load() {
    let mut grid = learned();
    let at = NOW_MS + WINDOW_MS + 1;
    publish(&grid, "a", at, Some(Revision { sequence: 2, ..INITIAL }), None);
    let snapshot = grid.order(&SITES, at);
    assert_eq!(capacity(&snapshot, "a"), None);
    assert_eq!(candidate(&snapshot, "a").backlog, None);
    assert_eq!(candidate(&snapshot, "a").rho, None);
}

#[test]
fn truncated_batch_cannot_teach_or_partly_publish_a_new_revision() {
    let mut grid = learned();
    let at = NOW_MS + REFRESH_MS;
    let complete = batch(
        "a",
        at,
        Some(Revision {
            generation: 2,
            sequence: 2,
            ..INITIAL
        }),
        Some((800.0, 8.0)),
    );
    let truncated = complete.lines().take(3).collect::<Vec<_>>().join("\n");
    grid.store.ingest_at(&truncated, at, at, "a");
    let snapshot = grid.order(&SITES, at);
    assert_eq!(
        capacity(&snapshot, "a"),
        None,
        "incomplete new state cannot retain the old ceiling"
    );
    assert_eq!(
        candidate(&snapshot, "a").backlog,
        None,
        "no prefix of the payload is committed"
    );
    assert_eq!(capacity(&snapshot, "b"), Some(80.0));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "each revision checks selection, shedding and independent retained learning"
)]
fn repeated_revisions_preserve_routing_and_do_not_shed_queue_free_pools() {
    let mut grid = learned();
    grid.availability.shedding = true;
    for turn in 1..=30 {
        let at = NOW_MS + turn * REFRESH_MS;
        let revision = Revision {
            generation: u64::try_from(turn).expect("positive turn") + 1,
            sequence: u64::try_from(turn).expect("positive turn") + 1,
            ..INITIAL
        };
        publish(&grid, "a", at, Some(revision), Some((20.0, 0.0)));
        publish(
            &grid,
            "b",
            at,
            Some(Revision {
                sequence: u64::try_from(turn).expect("positive turn") + 1,
                ..INITIAL
            }),
            Some((20.0, 0.0)),
        );
        let snapshot = grid.order(&SITES, at).shed(&BTreeSet::new(), &grid.availability);
        assert!(
            snapshot.shedding.is_empty(),
            "config churn alone cannot establish congestion"
        );
        assert!(routed_to(&snapshot, "a") > 0, "a new revision remains eligible");
        assert!(routed_to(&snapshot, "b") > 0, "the untouched provider remains eligible");
        assert_eq!(capacity(&snapshot, "b"), Some(80.0));
        assert!(
            candidate(&snapshot, "a").rho.is_some_and(|rho| rho < 0.2),
            "fresh low KV pressure survives reset"
        );
    }
}

#[test]
fn revision_change_cannot_hide_current_queue_pressure() {
    let mut grid = learned();
    let at = NOW_MS + REFRESH_MS;
    publish(
        &grid,
        "a",
        at,
        Some(Revision {
            generation: 2,
            sequence: 2,
            ..INITIAL
        }),
        Some((20.0, 4.0)),
    );
    publish(
        &grid,
        "b",
        at,
        Some(Revision { sequence: 2, ..INITIAL }),
        Some((20.0, 0.0)),
    );
    let snapshot = grid.order(&SITES, at);
    assert_eq!(routed_to(&snapshot, "b"), DRAWS);
}

// ---------------------------------------------------------------------------
// Test Utilities
// ---------------------------------------------------------------------------

/// Small deterministic draw used to assert real selection, not only scores.
const DRAWS: usize = 1_024;

/// Equal physical pools; revision changes must affect only their learned state.
const SITES: [SiteSpec; 2] = [
    SiteSpec {
        site: "a",
        capacity: 80.0,
        enforced: 80.0,
        in_flight: 20.0,
    },
    SiteSpec {
        site: "b",
        capacity: 80.0,
        enforced: 80.0,
        in_flight: 20.0,
    },
];

/// First complete publication of a provider.
const INITIAL: Revision = Revision {
    uid: "u1",
    generation: 1,
    epoch: 1,
    sequence: 1,
};

/// Inject a revision exactly between the two horizons of a snapshot read.
struct ReplacingSignals<'store> {
    /// Real shared store being queried and updated.
    store: &'store LoadStore,
    /// One deterministic concurrent publication.
    replaced: std::cell::Cell<bool>,
    /// Publication time of the replacement batch.
    at: i64,
    /// Whether the concurrent publication changes the provider revision.
    generation: u64,
}

impl crate::signals::SiteSignals for ReplacingSignals<'_> {
    fn read(&self, site: &str, cluster: &str, over: crate::signals::Over) -> crate::signals::SiteReading {
        let reading = crate::signals::SiteSignals::read(self.store, site, cluster, over);
        if !self.replaced.replace(true) {
            let next = batch(
                site,
                self.at,
                Some(Revision {
                    generation: self.generation,
                    sequence: 3,
                    ..INITIAL
                }),
                Some((20.0, 0.0)),
            );
            self.store.ingest_at(&next, self.at, self.at, site);
        }
        reading
    }

    fn learning_state(&self, site: &str, cluster: &str, now_ms: i64, max_age_ms: i64) -> grid_signals::LearningState {
        self.store
            .learning_state(&LoadStore::key(site, cluster), now_ms, max_age_ms)
    }

    fn relay_heard_ms(&self) -> Option<i64> {
        self.store.relay_heard_ms()
    }

    fn round_ms(&self, site: &str) -> Option<i64> {
        crate::signals::SiteSignals::round_ms(self.store, site)
    }
}

/// Wire identity independent of sample values and sample time.
#[derive(Clone, Copy)]
struct Revision {
    /// Kubernetes object lifetime.
    uid: &'static str,
    /// Kubernetes spec revision.
    generation: u64,
    /// Durable publisher reservation.
    epoch: u64,
    /// Monotonic batch within that reservation.
    sequence: u64,
}

/// Production-mode store: absence of identity is unqualified from startup.
fn guarded() -> Harness {
    let mut grid = Harness::new(WINDOW_MS);
    grid.store = LoadStore::new(std::time::Duration::from_secs(600)).with_revision_guard(WINDOW_MS);
    grid
}

/// Two model candidates deliberately share a single learned provider entry.
fn order_models(grid: &mut Harness, at: i64) -> RouteSnapshot {
    let mut first = super::config(&SITES[0]);
    first.name = "first".to_owned();
    let mut second = first.clone();
    second.name = "second".to_owned();
    let validated = super::validate_serving_candidates(vec![first, second]).expect("two valid models");
    RouteSnapshot::from_store(
        validated,
        std::sync::Arc::from("a"),
        &mut super::Inputs {
            signals: &grid.store,
            now_ms: at,
            window_ms: WINDOW_MS,
            horizon_ms: WINDOW_MS,
            availability: &grid.availability,
            learned: &mut grid.gauged.learned,
        },
    )
    .published(&mut grid.gauged)
}

/// A complete publication with queue and KV evidence of congestion.
fn publish_hot(grid: &Harness, at: i64, revision: Revision) {
    let hot = batch("a", at, Some(revision), Some((80.0, 4.0))).replace("} 0.1 ", "} 1 ");
    grid.store.ingest_at(&hot, at, at, "a");
}

/// Both pools have established a ceiling from the first revision.
fn learned() -> Harness {
    let mut grid = guarded();
    for site in SITES {
        publish(&grid, site.site, NOW_MS, Some(INITIAL), Some((80.0, 0.0)));
    }
    let snapshot = grid.order(&SITES, NOW_MS);
    assert!(snapshot.candidates.iter().all(|site| site.capacity == Some(80.0)));
    grid
}

/// Deliver one entire provider publication as a single response.
fn publish(grid: &Harness, site: &str, at: i64, revision: Option<Revision>, load: Option<(f64, f64)>) {
    grid.store.ingest_at(&batch(site, at, revision, load), at, at, site);
}

/// Five raw gauges and their one counted metadata row, all from the same observation.
fn batch(site: &str, at: i64, revision: Option<Revision>, load: Option<(f64, f64)>) -> String {
    let labels = format!(r#"grid_site="{site}",grid_provider="pool-{site}""#);
    let mut lines = Vec::new();
    if let Some(revision) = revision {
        let Revision {
            uid,
            generation,
            epoch,
            sequence,
        } = revision;
        let count = if load.is_some() { 5 } else { 0 };
        lines.push(format!(
            "grid_provider_config_generation{{{labels},provider_uid=\"{uid}\",publisher_epoch=\"{epoch}\",batch_sequence=\"{sequence}\",batch_samples=\"{count}\"}} {generation} {at}"
        ));
    }
    if let Some((running, queued)) = load {
        for (metric, value) in [
            ("llm_d_epp_average_running_requests", running),
            ("llm_d_epp_average_queue_size", queued),
            ("llm_d_epp_ready_endpoints", 1.0),
            ("llm_d_epp_flow_control_queue_size", 0.0),
            ("llm_d_epp_average_kv_cache_utilization", 0.1),
        ] {
            lines.push(format!("{metric}{{{labels}}} {value} {at}"));
        }
    }
    lines.join("\n")
}

/// The selected pool, irrespective of rank changes.
fn candidate<'snapshot>(
    snapshot: &'snapshot RouteSnapshot,
    site: &str,
) -> &'snapshot crate::descriptor::RouteCandidate {
    snapshot
        .candidates
        .iter()
        .find(|candidate| candidate.site.as_ref() == site)
        .expect("configured site")
}

/// The observable ceiling carried into selection.
fn capacity(snapshot: &RouteSnapshot, site: &str) -> Option<f64> {
    candidate(snapshot, site).capacity
}

/// Count requests the production selector sends to this provider.
fn routed_to(snapshot: &RouteSnapshot, site: &str) -> usize {
    (0..DRAWS)
        .filter(|turn| {
            select_spread(
                snapshot,
                CapabilityKind::InferenceModel,
                MODEL,
                *turn,
                |_| true,
                &KeepAll,
            )
            .expect("an admitted pool takes the request")
            .candidate
            .site
            .as_ref()
                == site
        })
        .count()
}
