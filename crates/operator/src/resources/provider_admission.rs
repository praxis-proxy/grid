//! Stateful provider admission evaluation.
//!
//! This module is deliberately independent of Kubernetes and reconciliation.
//! It turns one typed observation into a bounded wire admission state while
//! retaining only the small amount of history needed for hysteresis.  The
//! controller owns one [`AdmissionMemory`](crate::resources::provider_admission::AdmissionMemory)
//! and supplies the reconciliation
//! clock, so the evaluator is deterministic and straightforward to test.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    crd::grid_network::{AdmissionMode, AdmissionPolicyConfig, MissingMetricsPolicy, ScoringStrategy},
    resources::geography::AdmissionState,
};

/// A metrics observation available to the admission evaluator.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Observation {
    /// A fresh, successfully parsed provider observation.
    Fresh {
        /// Normalized metric values.
        metrics: scoring::BackendMetrics,
        /// Revision identifying the accepted observation sample.
        revision: u64,
    },
    /// The active scoring strategy does not use a metric for this provider.
    NotConfigured,
    /// No usable observation was available for this provider.
    Missing,
}

/// Metric selected by the active scoring strategy for admission pressure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PressureSignal {
    /// Normalized pending queue pressure.
    QueueDepth,
    /// Normalized KV-cache utilization.
    KvCachePressure,
    /// No metric-driven admission is active.
    None,
}

impl From<ScoringStrategy> for PressureSignal {
    fn from(strategy: ScoringStrategy) -> Self {
        match strategy {
            ScoringStrategy::QueueDepth => Self::QueueDepth,
            ScoringStrategy::KvCachePressure => Self::KvCachePressure,
            ScoringStrategy::NoMetrics => Self::None,
        }
    }
}

/// Validated policy used by the pure evaluator.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Policy {
    /// Whether state changes are immediate or stabilized.
    pub(crate) mode: AdmissionMode,
    /// Pressure threshold for entering restricted admission.
    pub(crate) enter_threshold: f64,
    /// Lower pressure threshold for recovery.
    pub(crate) exit_threshold: f64,
    /// Required consecutive pressure observations.
    pub(crate) failure_threshold: u32,
    /// Required consecutive recovery observations.
    pub(crate) success_threshold: u32,
    /// Minimum duration of the current state.
    pub(crate) minimum_state_duration: Duration,
    /// Required recovery hold-down duration.
    pub(crate) recovery_hold_down: Duration,
    /// State used when metrics are unavailable.
    pub(crate) missing_metrics: MissingMetricsPolicy,
    /// Signal used to evaluate pressure and recovery.
    pub(crate) pressure_signal: PressureSignal,
}

impl Policy {
    /// Convert the CRD form into a validated evaluator policy.
    #[expect(
        clippy::too_many_lines,
        reason = "validation keeps all admission policy constraints together"
    )]
    pub(crate) fn from_config(
        config: Option<&AdmissionPolicyConfig>,
        pressure_signal: PressureSignal,
    ) -> Result<Self, String> {
        let Some(config) = config else {
            return Ok(Self {
                mode: AdmissionMode::Instantaneous,
                enter_threshold: 0.85,
                exit_threshold: 0.70,
                failure_threshold: 1,
                success_threshold: 1,
                minimum_state_duration: Duration::ZERO,
                recovery_hold_down: Duration::ZERO,
                // Omitted policy preserves the historical no-metrics default.
                missing_metrics: MissingMetricsPolicy::ExistingOnly,
                pressure_signal,
            });
        };
        if !config.enter_exit_valid() {
            return Err(
                "admissionPolicy pressure thresholds must be finite, in [0,1], and exitThreshold < enterThreshold"
                    .into(),
            );
        }
        let minimum_state_duration =
            parse_positive_seconds(&config.pressure.minimum_state_duration, "minimumStateDuration")?;
        let recovery_hold_down = parse_positive_seconds(&config.pressure.recovery_hold_down, "recoveryHoldDown")?;
        if config.pressure.failure_threshold == 0 || config.pressure.failure_threshold > 100 {
            return Err("admissionPolicy pressure failureThreshold must be between 1 and 100".into());
        }
        if config.pressure.success_threshold == 0 || config.pressure.success_threshold > 100 {
            return Err("admissionPolicy pressure successThreshold must be between 1 and 100".into());
        }
        Ok(Self {
            mode: config.mode,
            enter_threshold: config.pressure.enter_threshold,
            exit_threshold: config.pressure.exit_threshold,
            failure_threshold: config.pressure.failure_threshold,
            success_threshold: config.pressure.success_threshold,
            minimum_state_duration,
            recovery_hold_down,
            missing_metrics: config.missing_metrics,
            pressure_signal,
        })
    }
}

impl AdmissionPolicyConfig {
    /// True when both thresholds are finite, in `[0,1]`, and exit < enter.
    fn enter_exit_valid(&self) -> bool {
        self.pressure.enter_threshold.is_finite()
            && self.pressure.exit_threshold.is_finite()
            && (0.0..=1.0).contains(&self.pressure.enter_threshold)
            && (0.0..=1.0).contains(&self.pressure.exit_threshold)
            && self.pressure.exit_threshold < self.pressure.enter_threshold
    }
}

/// Parse the intentionally bounded whole-second admission duration syntax.
fn parse_positive_seconds(value: &str, field: &str) -> Result<Duration, String> {
    let Some(number) = value.strip_suffix('s') else {
        return Err(format!(
            "admissionPolicy pressure {field} must use a positive whole-second duration"
        ));
    };
    if number.is_empty() || number.starts_with('0') {
        return Err(format!(
            "admissionPolicy pressure {field} must use a positive whole-second duration"
        ));
    }
    let seconds = number
        .parse::<u64>()
        .map_err(|error| format!("admissionPolicy pressure {field} is not a valid duration: {error}"))?;
    if seconds == 0 {
        return Err(format!("admissionPolicy pressure {field} must be greater than zero"));
    }
    Ok(Duration::from_secs(seconds))
}

/// Per-provider hysteresis state retained across reconciliation cycles.
#[derive(Clone, Copy, Debug)]
struct Entry {
    /// Current published admission state.
    state: AdmissionState,
    /// Consecutive pressure observations.
    pressure_observations: u32,
    /// Consecutive recovery observations.
    recovery_observations: u32,
    /// Time at which the current state began.
    state_since: Instant,
    /// Time at which the current recovery streak began.
    recovery_since: Option<Instant>,
    /// Revision of the most recently processed fresh observation.
    last_observation: Option<u64>,
}

/// Cross-reconcile admission state, keyed by a stable provider identity.
#[derive(Debug, Default)]
pub(crate) struct AdmissionMemory {
    /// Per-provider state keyed by `{network}/{uid}/{routing_identity}`.
    entries: HashMap<String, Entry>,
}

impl AdmissionMemory {
    /// Evaluate and remember one provider's admission state.
    #[expect(
        clippy::too_many_lines,
        reason = "the state machine keeps pressure, recovery, missing, and hard-failure transitions explicit"
    )]
    pub(crate) fn evaluate(
        &mut self,
        key: &str,
        observation: Observation,
        policy: Policy,
        now: Instant,
    ) -> AdmissionState {
        if policy.mode == AdmissionMode::Instantaneous {
            let state = instantaneous_state(observation, policy);
            self.entries.insert(key.to_owned(), new_entry(state, now));
            return state;
        }

        let initial_state = match observation {
            Observation::Fresh { .. } | Observation::NotConfigured => AdmissionState::NewAndExisting,
            Observation::Missing => missing_state(policy),
        };
        let entry = self
            .entries
            .entry(key.to_owned())
            .or_insert_with(|| new_entry(initial_state, now));
        if let Observation::Fresh { revision, .. } = observation {
            if entry.last_observation == Some(revision) {
                return entry.state;
            }
            entry.last_observation = Some(revision);
        } else {
            entry.last_observation = None;
        }
        let elapsed = now.saturating_duration_since(entry.state_since);
        match observation {
            Observation::NotConfigured => {
                entry.pressure_observations = 0;
                entry.recovery_observations = 0;
                entry.recovery_since = None;
                entry.state = AdmissionState::NewAndExisting;
                entry.state_since = now;
            },
            Observation::Missing => {
                entry.pressure_observations = 0;
                entry.recovery_observations = 0;
                entry.recovery_since = None;
                entry.state = missing_state(policy);
                entry.state_since = now;
            },
            Observation::Fresh { metrics, .. } if !metrics.healthy => {
                entry.pressure_observations = 0;
                entry.recovery_observations = 0;
                entry.recovery_since = None;
                entry.state = AdmissionState::Excluded;
                entry.state_since = now;
            },
            Observation::Fresh { metrics, .. } => {
                let pressure = pressure_value(metrics, policy) >= policy.enter_threshold;
                let recovered = pressure_value(metrics, policy) <= policy.exit_threshold;
                if entry.state == AdmissionState::NewAndExisting {
                    entry.recovery_observations = 0;
                    entry.recovery_since = None;
                    entry.pressure_observations = if pressure {
                        entry.pressure_observations.saturating_add(1)
                    } else {
                        0
                    };
                    if pressure
                        && entry.pressure_observations >= policy.failure_threshold
                        && elapsed >= policy.minimum_state_duration
                    {
                        entry.state = AdmissionState::ExistingOnly;
                        entry.state_since = now;
                        entry.pressure_observations = 0;
                    }
                } else if entry.state == AdmissionState::ExistingOnly {
                    entry.pressure_observations = 0;
                    if recovered {
                        entry.recovery_observations = entry.recovery_observations.saturating_add(1);
                        let recovery_since = *entry.recovery_since.get_or_insert(now);
                        if entry.recovery_observations >= policy.success_threshold
                            && now.saturating_duration_since(recovery_since) >= policy.recovery_hold_down
                            && elapsed >= policy.minimum_state_duration
                        {
                            entry.state = AdmissionState::NewAndExisting;
                            entry.state_since = now;
                            entry.recovery_observations = 0;
                            entry.recovery_since = None;
                        }
                    } else {
                        entry.recovery_observations = 0;
                        entry.recovery_since = None;
                    }
                } else {
                    // Excluded providers require a fresh healthy observation
                    // before they can re-enter the admission state machine.
                    if recovered {
                        entry.recovery_observations = entry.recovery_observations.saturating_add(1);
                        let recovery_since = *entry.recovery_since.get_or_insert(now);
                        if entry.recovery_observations >= policy.success_threshold
                            && now.saturating_duration_since(recovery_since) >= policy.recovery_hold_down
                            && elapsed >= policy.minimum_state_duration
                        {
                            entry.state = AdmissionState::NewAndExisting;
                            entry.state_since = now;
                            entry.recovery_observations = 0;
                            entry.recovery_since = None;
                        }
                    } else {
                        entry.recovery_observations = 0;
                        entry.recovery_since = None;
                    }
                }
            },
        }
        entry.state
    }

    /// Remove state for providers no longer present in one network.
    pub(crate) fn retain_network_keys(&mut self, network_name: &str, keys: impl Iterator<Item = String>) {
        let keys: std::collections::HashSet<String> = keys.collect();
        let prefix = format!("{network_name}/");
        self.entries
            .retain(|key, _| !key.starts_with(&prefix) || keys.contains(key));
    }
}

/// Create an admission-memory entry at the supplied clock instant.
fn new_entry(state: AdmissionState, now: Instant) -> Entry {
    Entry {
        state,
        pressure_observations: 0,
        recovery_observations: 0,
        state_since: now,
        recovery_since: None,
        last_observation: None,
    }
}

/// Convert the configured missing-metrics policy to a wire state.
fn missing_state(policy: Policy) -> AdmissionState {
    if policy.pressure_signal == PressureSignal::None {
        return AdmissionState::NewAndExisting;
    }
    match policy.missing_metrics {
        MissingMetricsPolicy::ExistingOnly => AdmissionState::ExistingOnly,
        MissingMetricsPolicy::Excluded => AdmissionState::Excluded,
    }
}

/// Evaluate one observation without hysteresis.
fn instantaneous_state(observation: Observation, policy: Policy) -> AdmissionState {
    match observation {
        Observation::Missing => missing_state(policy),
        Observation::Fresh { metrics, .. } if !metrics.healthy => AdmissionState::Excluded,
        Observation::Fresh { metrics, .. } if pressure_value(metrics, policy) >= policy.enter_threshold => {
            AdmissionState::ExistingOnly
        },
        Observation::NotConfigured | Observation::Fresh { .. } => AdmissionState::NewAndExisting,
    }
}

/// Return the normalized pressure selected by the active scoring strategy.
fn pressure_value(metrics: scoring::BackendMetrics, policy: Policy) -> f64 {
    match policy.pressure_signal {
        PressureSignal::QueueDepth => metrics.queue_depth,
        PressureSignal::KvCachePressure => metrics.kv_cache_utilization,
        PressureSignal::None => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics(queue_depth: f64, kv_cache_utilization: f64, healthy: bool) -> scoring::BackendMetrics {
        scoring::BackendMetrics {
            error_rate: 0.0,
            healthy,
            kv_cache_utilization,
            latency_p99_ms: 0.0,
            prefix_cache_hit_ratio: 0.0,
            queue_depth,
        }
    }

    fn policy() -> Policy {
        Policy {
            mode: AdmissionMode::Stabilized,
            enter_threshold: 0.85,
            exit_threshold: 0.70,
            failure_threshold: 2,
            success_threshold: 3,
            minimum_state_duration: Duration::from_secs(10),
            recovery_hold_down: Duration::from_secs(30),
            missing_metrics: MissingMetricsPolicy::ExistingOnly,
            pressure_signal: PressureSignal::QueueDepth,
        }
    }

    fn fresh(metrics: scoring::BackendMetrics, generation: u64) -> Observation {
        Observation::Fresh {
            revision: generation,
            metrics,
        }
    }

    #[test]
    fn pressure_requires_repeated_observations() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let p = policy();
        assert_eq!(
            memory.evaluate("p", fresh(metrics(0.90, 0.0, true), 1), p, start),
            AdmissionState::NewAndExisting
        );
        assert_eq!(
            memory.evaluate(
                "p",
                fresh(metrics(0.91, 0.0, true), 2),
                p,
                start + Duration::from_secs(10)
            ),
            AdmissionState::ExistingOnly
        );
    }

    #[test]
    fn missing_metrics_fail_closed_only_when_policy_is_explicit() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let legacy = Policy {
            mode: AdmissionMode::Instantaneous,
            enter_threshold: 0.85,
            exit_threshold: 0.70,
            failure_threshold: 1,
            success_threshold: 1,
            minimum_state_duration: Duration::ZERO,
            recovery_hold_down: Duration::ZERO,
            missing_metrics: MissingMetricsPolicy::ExistingOnly,
            pressure_signal: PressureSignal::None,
        };
        assert_eq!(
            memory.evaluate("legacy", Observation::Missing, legacy, start),
            AdmissionState::NewAndExisting
        );
        assert_eq!(
            memory.evaluate("stabilized", Observation::Missing, policy(), start),
            AdmissionState::ExistingOnly
        );
    }

    #[test]
    fn unhealthy_is_immediately_excluded() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        assert_eq!(
            memory.evaluate("p", fresh(metrics(0.0, 0.0, false), 1), policy(), start),
            AdmissionState::Excluded
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "multi-phase state machine test")]
    fn recovery_requires_count_and_hold_down() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let p = policy();
        memory.evaluate("p", fresh(metrics(0.90, 0.0, true), 1), p, start);
        memory.evaluate(
            "p",
            fresh(metrics(0.91, 0.0, true), 2),
            p,
            start + Duration::from_secs(10),
        );
        assert_eq!(
            memory.evaluate(
                "p",
                fresh(metrics(0.10, 0.1, true), 3),
                p,
                start + Duration::from_secs(20)
            ),
            AdmissionState::ExistingOnly
        );
        assert_eq!(
            memory.evaluate(
                "p",
                fresh(metrics(0.11, 0.1, true), 4),
                p,
                start + Duration::from_secs(30)
            ),
            AdmissionState::ExistingOnly
        );
        assert_eq!(
            memory.evaluate(
                "p",
                fresh(metrics(0.12, 0.1, true), 5),
                p,
                start + Duration::from_secs(50)
            ),
            AdmissionState::NewAndExisting
        );
    }

    #[test]
    fn queue_strategy_ignores_kv_pressure() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let mut p = policy();
        p.pressure_signal = PressureSignal::QueueDepth;
        let observation = fresh(metrics(0.1, 0.99, true), 1);
        assert_eq!(
            memory.evaluate("p", observation, p, start),
            AdmissionState::NewAndExisting
        );
    }

    #[test]
    fn kv_strategy_ignores_queue_pressure() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let mut p = policy();
        p.pressure_signal = PressureSignal::KvCachePressure;
        let observation = fresh(metrics(0.99, 0.1, true), 1);
        assert_eq!(
            memory.evaluate("p", observation, p, start),
            AdmissionState::NewAndExisting
        );
    }

    #[test]
    fn repeated_sample_does_not_advance_pressure_counter() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let p = policy();
        let sample = fresh(metrics(0.9, 0.1, true), 1);
        assert_eq!(memory.evaluate("p", sample, p, start), AdmissionState::NewAndExisting);
        assert_eq!(
            memory.evaluate("p", sample, p, start + Duration::from_secs(10)),
            AdmissionState::NewAndExisting
        );
        assert_eq!(
            memory.evaluate(
                "p",
                fresh(metrics(0.91, 0.1, true), 2),
                p,
                start + Duration::from_secs(20)
            ),
            AdmissionState::ExistingOnly
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "multi-phase state machine test")]
    fn repeated_recovery_sample_does_not_advance_recovery_counter() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let p = policy();
        memory.evaluate("p", fresh(metrics(0.90, 0.0, true), 1), p, start);
        memory.evaluate(
            "p",
            fresh(metrics(0.91, 0.0, true), 2),
            p,
            start + Duration::from_secs(10),
        );
        assert_eq!(
            memory.evaluate(
                "p",
                fresh(metrics(0.10, 0.1, true), 3),
                p,
                start + Duration::from_secs(20)
            ),
            AdmissionState::ExistingOnly
        );
        // Same generation repeated — must not advance recovery counter.
        let repeated = fresh(metrics(0.10, 0.1, true), 3);
        assert_eq!(
            memory.evaluate("p", repeated, p, start + Duration::from_secs(30)),
            AdmissionState::ExistingOnly
        );
        assert_eq!(
            memory.evaluate("p", repeated, p, start + Duration::from_secs(60)),
            AdmissionState::ExistingOnly
        );
        // Distinct generations advance recovery normally.
        assert_eq!(
            memory.evaluate(
                "p",
                fresh(metrics(0.11, 0.1, true), 4),
                p,
                start + Duration::from_secs(70)
            ),
            AdmissionState::ExistingOnly
        );
        assert_eq!(
            memory.evaluate(
                "p",
                fresh(metrics(0.12, 0.1, true), 5),
                p,
                start + Duration::from_secs(80)
            ),
            AdmissionState::NewAndExisting
        );
    }

    #[test]
    fn same_generation_does_not_increment_pressure_twice() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let p = policy();
        let m = metrics(0.90, 0.0, true);
        assert_eq!(
            memory.evaluate("p", fresh(m, 1), p, start),
            AdmissionState::NewAndExisting
        );
        assert_eq!(
            memory.evaluate("p", fresh(m, 1), p, start + Duration::from_secs(5)),
            AdmissionState::NewAndExisting
        );
        assert_eq!(
            memory.evaluate("p", fresh(m, 2), p, start + Duration::from_secs(10)),
            AdmissionState::ExistingOnly
        );
    }

    #[test]
    fn new_generation_with_identical_values_increments_counters() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let p = policy();
        let m = metrics(0.90, 0.0, true);
        assert_eq!(
            memory.evaluate("p", fresh(m, 1), p, start),
            AdmissionState::NewAndExisting
        );
        assert_eq!(
            memory.evaluate("p", fresh(m, 2), p, start + Duration::from_secs(10)),
            AdmissionState::ExistingOnly
        );
    }

    #[test]
    fn clamped_queue_values_count_as_distinct_scrapes() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let p = policy();
        let m = metrics(1.0, 0.0, true);
        assert_eq!(
            memory.evaluate("p", fresh(m, 1), p, start),
            AdmissionState::NewAndExisting
        );
        assert_eq!(
            memory.evaluate("p", fresh(m, 2), p, start + Duration::from_secs(10)),
            AdmissionState::ExistingOnly
        );
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "multi-phase state machine test")]
    fn stable_idle_scrapes_satisfy_recovery_threshold() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let p = policy();
        memory.evaluate("p", fresh(metrics(0.90, 0.0, true), 1), p, start);
        memory.evaluate(
            "p",
            fresh(metrics(0.90, 0.0, true), 2),
            p,
            start + Duration::from_secs(10),
        );
        assert_eq!(
            memory.evaluate(
                "p",
                fresh(metrics(0.10, 0.0, true), 3),
                p,
                start + Duration::from_secs(20)
            ),
            AdmissionState::ExistingOnly
        );
        let idle = metrics(0.0, 0.0, true);
        assert_eq!(
            memory.evaluate("p", fresh(idle, 4), p, start + Duration::from_secs(30)),
            AdmissionState::ExistingOnly
        );
        assert_eq!(
            memory.evaluate("p", fresh(idle, 5), p, start + Duration::from_secs(40)),
            AdmissionState::ExistingOnly
        );
        assert_eq!(
            memory.evaluate("p", fresh(idle, 6), p, start + Duration::from_secs(60)),
            AdmissionState::NewAndExisting
        );
    }

    #[test]
    fn retaining_one_network_does_not_reset_another_network() {
        let start = Instant::now();
        let mut memory = AdmissionMemory::default();
        let p = policy();

        assert_eq!(
            memory.evaluate("network-a/provider", fresh(metrics(0.90, 0.0, true), 1), p, start),
            AdmissionState::NewAndExisting
        );
        assert_eq!(
            memory.evaluate("network-b/provider", fresh(metrics(0.90, 0.0, true), 2), p, start),
            AdmissionState::NewAndExisting
        );

        memory.retain_network_keys("network-a", ["network-a/provider".to_owned()].into_iter());

        assert_eq!(
            memory.evaluate(
                "network-a/provider",
                fresh(metrics(0.91, 0.0, true), 3),
                p,
                start + Duration::from_secs(10),
            ),
            AdmissionState::ExistingOnly
        );
        // network-b's counter was preserved (not reset), so this second distinct
        // observation reaches failureThreshold=2 → ExistingOnly.
        assert_eq!(
            memory.evaluate(
                "network-b/provider",
                fresh(metrics(0.91, 0.0, true), 4),
                p,
                start + Duration::from_secs(10),
            ),
            AdmissionState::ExistingOnly
        );
    }
}
