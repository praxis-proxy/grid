//! Dynamic provider weights derived from fresh, normalized Grid signals.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Duration,
};

use crate::{crd::grid_network::PressureWeightedConfig, signals};

/// Canonical queue utilization signal added to the provider's published samples.
pub(crate) const QUEUE_PRESSURE_METRIC: &str = "grid_routing_queue_pressure";
/// Canonical KV-cache utilization signal added to the provider's published samples.
pub(crate) const KV_CACHE_PRESSURE_METRIC: &str = "grid_routing_kv_cache_pressure";

/// Provider signal identity: the observing site plus its routing-cluster identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SignalKey {
    /// Site that collected the sample.
    pub(crate) site: String,
    /// Provider's routing identity at that site.
    pub(crate) provider: String,
}

/// Extract fresh, unambiguous normalized samples from local and polled exposition.
///
/// Samples missing either identity label, without a timestamp, outside `[0, 1]`,
/// in the future, or older than `max_age` are ignored. Conflicting samples for
/// one identity invalidate that identity rather than depending on iteration order.
#[expect(
    clippy::too_many_lines,
    reason = "one pass validates and de-duplicates local and peer observations before returning the usable snapshot"
)]
pub(crate) fn fresh_signal_values(
    local: &str,
    peers: &str,
    metric: &str,
    now_ms: i64,
    max_age: Duration,
) -> HashMap<SignalKey, f64> {
    let mut values: BTreeMap<SignalKey, Option<f64>> = BTreeMap::new();
    for observation in signals::parse(local).into_iter().chain(signals::parse(peers)) {
        if observation.metric != metric {
            continue;
        }
        let (Some(site), Some(provider), Some(stamp)) = (
            observation.labels.get(signals::SITE_LABEL),
            observation.labels.get(signals::PROVIDER_LABEL),
            observation.timestamp_ms,
        ) else {
            continue;
        };
        if site.is_empty() || provider.is_empty() {
            continue;
        }
        let age_ms = now_ms.checked_sub(stamp);
        let fresh =
            age_ms.is_some_and(|age| age >= 0 && u128::try_from(age).is_ok_and(|age| age <= max_age.as_millis()));
        let sample = observation
            .value
            .is_finite()
            .then_some(observation.value)
            .filter(|value| (0.0..=1.0).contains(value))
            .filter(|_| fresh);
        let key = SignalKey {
            site: site.clone(),
            provider: provider.clone(),
        };
        match values.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(sample);
            },
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if entry.get().is_none() || sample.is_none() || entry.get() != &sample {
                    entry.insert(None);
                }
            },
        }
    }
    values
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect()
}

/// Typed signal and capacity for one overlay candidate.
#[derive(Clone, Debug)]
pub(crate) struct PressureInput<'input> {
    /// Stable candidate ID used for deterministic ties and smoothing state.
    pub(crate) stable_id: &'input str,
    /// First-class AI selection group; normalization never crosses groups.
    pub(crate) selection_group: u32,
    /// Configured relative provider capacity.
    pub(crate) capacity_weight: u32,
    /// Fresh normalized signal in `[0, 1]`.
    pub(crate) pressure: Option<f64>,
}

/// EWMA and last-published weights, scoped to one `GridNetwork`.
#[derive(Clone, Debug, Default)]
pub(crate) struct PlacementState {
    /// Smoothed availability per stable candidate and selection group.
    availability: HashMap<String, f64>,
    /// Last semantically published weight per candidate and selection group.
    weights: HashMap<String, u32>,
    /// Policy used to publish `weights`; a policy change bypasses hysteresis.
    policy: Option<PressureWeightedConfig>,
}

/// Convert capacity and inverse pressure into deterministic positive weights.
///
/// The weights in each selection group sum to `maximum_weight`. A configured
/// availability floor keeps a fully saturated provider eligible with a small,
/// nonzero probability. Invalid or missing data leaves the previous state intact.
#[expect(
    clippy::too_many_lines,
    reason = "the bounded allocator validates, smooths, normalizes, rounds, applies hysteresis, and commits state atomically"
)]
pub(crate) fn pressure_weights(
    inputs: &[PressureInput<'_>],
    config: &PressureWeightedConfig,
    state: &mut PlacementState,
) -> Result<HashMap<String, u32>, String> {
    validate_config(config)?;
    if inputs.is_empty() {
        state.availability.clear();
        state.weights.clear();
        state.policy = Some(config.clone());
        return Ok(HashMap::new());
    }

    let floor = f64::from(config.availability_floor_percent) / 100.0;
    let policy_changed = state.policy.as_ref().is_none_or(|previous| previous != config);
    let mut next_availability = if policy_changed {
        HashMap::new()
    } else {
        state.availability.clone()
    };
    let mut by_group: BTreeMap<u32, Vec<(usize, f64)>> = BTreeMap::new();
    let mut active_state_ids = BTreeSet::new();

    for (index, input) in inputs.iter().enumerate() {
        if input.stable_id.is_empty() || !(1..=1000).contains(&input.capacity_weight) {
            return Err("pressure-weighted candidate identity or capacity is invalid".to_owned());
        }
        let pressure = input
            .pressure
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .ok_or_else(|| format!("candidate {} has no fresh valid pressure signal", input.stable_id))?;
        let state_id = state_key(input);
        if !active_state_ids.insert(state_id.clone()) {
            return Err(format!("duplicate pressure-weighted candidate {}", input.stable_id));
        }
        let availability = (1.0 - pressure).max(floor);
        let previous = next_availability.get(&state_id).copied().unwrap_or(availability);
        let interpolated = previous * (1.0 - config.smoothing_factor) + availability * config.smoothing_factor;
        if !interpolated.is_finite() {
            return Err("pressure smoothing produced a non-finite availability".to_owned());
        }
        // Convex interpolation can drift a few ULPs beyond an endpoint.
        let smoothed = interpolated.clamp(floor, 1.0);
        next_availability.insert(state_id, smoothed);
        let raw = f64::from(input.capacity_weight) * smoothed;
        if !raw.is_finite() || raw <= 0.0 {
            return Err("pressure weights require positive finite effective capacity".to_owned());
        }
        by_group.entry(input.selection_group).or_default().push((index, raw));
    }

    let mut proposed = HashMap::new();
    for members in by_group.values() {
        let target = u64::from(config.maximum_weight);
        let member_count = u64::try_from(members.len()).unwrap_or(u64::MAX);
        let minimum_total = u64::from(config.minimum_weight).saturating_mul(member_count);
        if minimum_total > target {
            return Err("minimumWeight cannot fit within the selection group's normalization total".to_owned());
        }
        let mut allocations = vec![None; members.len()];
        let mut remaining = target;

        while remaining > 0 {
            let active: Vec<(usize, f64)> = members
                .iter()
                .enumerate()
                .filter_map(|(position, (_, raw))| {
                    allocations
                        .get(position)
                        .is_some_and(Option::is_none)
                        .then_some((position, *raw))
                })
                .collect();
            let active_total: f64 = active.iter().map(|(_, raw)| *raw).sum();
            if active.is_empty() || !active_total.is_finite() || active_total <= 0.0 {
                return Err("pressure weights cannot be normalized within configured bounds".to_owned());
            }
            let normalization_total = u32::try_from(remaining)
                .map_err(|conversion_error| format!("weight total exceeds u32: {conversion_error}"))?;
            let normalization_total_f64 = f64::from(normalization_total);
            let mut exact_shares = Vec::with_capacity(active.len());
            for (position, raw) in &active {
                let exact = normalization_total_f64 * *raw / active_total;
                if !exact.is_finite() || exact < 0.0 || exact > normalization_total_f64 {
                    return Err("normalized pressure weight is outside the configured bounds".to_owned());
                }
                exact_shares.push((*position, exact));
            }

            let below_minimum: Vec<usize> = exact_shares
                .iter()
                .filter_map(|(position, exact)| (*exact < f64::from(config.minimum_weight)).then_some(*position))
                .collect();
            if !below_minimum.is_empty() {
                let minimum = u64::from(config.minimum_weight);
                if minimum.saturating_mul(u64::try_from(below_minimum.len()).unwrap_or(u64::MAX)) > remaining {
                    return Err("minimumWeight cannot fit within the selection group's normalization total".to_owned());
                }
                for position in below_minimum {
                    let Some(allocation) = allocations.get_mut(position) else {
                        return Err("pressure-weight candidate index out of bounds".to_owned());
                    };
                    *allocation = Some(config.minimum_weight);
                    remaining -= minimum;
                }
                continue;
            }

            let mut remainders = Vec::with_capacity(exact_shares.len());
            let mut allocated = 0_u64;
            for (position, exact) in exact_shares {
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "exact is finite, non-negative, and bounded by remaining total <= 1000"
                )]
                let floor_value = exact.floor() as u32;
                let Some(allocation) = allocations.get_mut(position) else {
                    return Err("pressure-weight candidate index out of bounds".to_owned());
                };
                *allocation = Some(floor_value);
                allocated = allocated.saturating_add(u64::from(floor_value));
                let remainder = exact - f64::from(floor_value);
                remainders.push((position, remainder));
            }
            remaining = remaining.saturating_sub(allocated);
            remainders.sort_by(|(left_position, left), (right_position, right)| {
                let left_index = members
                    .get(*left_position)
                    .and_then(|(i, _)| inputs.get(*i))
                    .map_or("", |i| i.stable_id);
                let right_index = members
                    .get(*right_position)
                    .and_then(|(i, _)| inputs.get(*i))
                    .map_or("", |i| i.stable_id);
                right.total_cmp(left).then_with(|| left_index.cmp(right_index))
            });
            for (position, _) in remainders {
                if remaining == 0 {
                    break;
                }
                let Some(weight) = allocations.get_mut(position) else {
                    return Err("pressure-weight candidate index out of bounds".to_owned());
                };
                let Some(current) = *weight else {
                    return Err("pressure-weight candidate allocation is missing".to_owned());
                };
                *weight = Some(current.saturating_add(1));
                remaining -= 1;
            }
            if remaining > 0 {
                return Err("pressure weights could not preserve the normalization total".to_owned());
            }
        }

        for (position, (input_index, _)) in members.iter().enumerate() {
            let Some(input) = inputs.get(*input_index) else {
                return Err("pressure-weight candidate index out of bounds".to_owned());
            };
            let Some(weight) = allocations.get(position).copied().flatten() else {
                return Err("pressure-weight candidate allocation is missing".to_owned());
            };
            proposed.insert(weight_key(input.stable_id, input.selection_group), weight);
        }
    }

    let threshold = f64::from(config.change_threshold_percent) / 100.0;
    let material = policy_changed
        || proposed.iter().any(|(id, weight)| {
            state.weights.get(id).is_none_or(|old| {
                let delta = (f64::from(*weight) - f64::from(*old)).abs() / f64::from((*old).max(1));
                delta > 0.0 && delta >= threshold
            })
        })
        || state.weights.keys().any(|id| !proposed.contains_key(id));

    next_availability.retain(|id, _| active_state_ids.contains(id));
    if material {
        state.weights.clone_from(&proposed);
    }
    state.availability = next_availability;
    state.policy = Some(config.clone());
    if material {
        state.weights.retain(|id, _| active_state_ids.contains(id));
        Ok(proposed)
    } else {
        Ok(state.weights.clone())
    }
}

/// Build the state key for one stable candidate in a selection group.
pub(crate) fn weight_key(stable_id: &str, selection_group: u32) -> String {
    format!("{selection_group}:{stable_id}")
}

/// Return the lifecycle key used to retain smoothing for one candidate.
fn state_key(input: &PressureInput<'_>) -> String {
    weight_key(input.stable_id, input.selection_group)
}

/// Validate bounds that are also enforced by the CRD schema.
fn validate_config(config: &PressureWeightedConfig) -> Result<(), String> {
    if config.minimum_weight == 0
        || config.maximum_weight == 0
        || config.minimum_weight > config.maximum_weight
        || config.maximum_weight > 1000
    {
        return Err("weight bounds must satisfy 1 <= minimumWeight <= maximumWeight <= 1000".to_owned());
    }
    if !(0.0..=1.0).contains(&config.smoothing_factor) || config.smoothing_factor == 0.0 {
        return Err("smoothingFactor must be finite and in (0, 1]".to_owned());
    }
    if !(1..=100).contains(&config.availability_floor_percent) {
        return Err("availabilityFloorPercent must be between 1 and 100".to_owned());
    }
    if config.change_threshold_percent > 100 || !(1..=300).contains(&config.stale_signal_seconds) {
        return Err("changeThresholdPercent or staleSignalSeconds is outside its supported range".to_owned());
    }
    Ok(())
}

#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    reason = "placement tests assert directly against small deterministic fixtures"
)]
mod tests {
    use super::*;
    use crate::crd::grid_network::PressureSignal;

    fn config() -> PressureWeightedConfig {
        PressureWeightedConfig {
            signal: PressureSignal::QueueDepth,
            minimum_weight: 1,
            maximum_weight: 1000,
            availability_floor_percent: 5,
            smoothing_factor: 1.0,
            change_threshold_percent: 0,
            stale_signal_seconds: 120,
        }
    }

    fn input(id: &'static str, group: u32, capacity: u32, pressure: Option<f64>) -> PressureInput<'static> {
        PressureInput {
            stable_id: id,
            selection_group: group,
            capacity_weight: capacity,
            pressure,
        }
    }

    #[test]
    fn capacity_and_pressure_produce_expected_group_local_weights() {
        let inputs = [input("a", 0, 2, Some(0.0)), input("b", 0, 1, Some(0.5))];
        let weights = pressure_weights(&inputs, &config(), &mut PlacementState::default()).unwrap();
        assert_eq!(weights["0:a"], 800);
        assert_eq!(weights["0:b"], 200);
    }

    #[test]
    fn equal_pressure_uses_configured_capacity() {
        let inputs = [input("a", 0, 7, Some(0.2)), input("b", 0, 3, Some(0.2))];
        let weights = pressure_weights(&inputs, &config(), &mut PlacementState::default()).unwrap();
        assert_eq!(weights["0:a"], 700);
        assert_eq!(weights["0:b"], 300);
    }

    #[test]
    fn groups_normalize_independently_and_inputs_are_order_independent() {
        let one = [
            input("a", 0, 1, Some(0.0)),
            input("b", 0, 1, Some(0.5)),
            input("c", 1, 1, Some(0.4)),
        ];
        let two = [one[2].clone(), one[1].clone(), one[0].clone()];
        let left = pressure_weights(&one, &config(), &mut PlacementState::default()).unwrap();
        let right = pressure_weights(&two, &config(), &mut PlacementState::default()).unwrap();
        assert_eq!(left, right);
        assert_eq!(left["1:c"], 1000);
        assert_eq!(left["0:a"] + left["0:b"], 1000);
    }

    #[test]
    fn one_candidate_receives_the_full_group_total_and_rounding_is_deterministic() {
        let one = pressure_weights(
            &[input("only", 0, 7, Some(0.4))],
            &config(),
            &mut PlacementState::default(),
        )
        .unwrap();
        assert_eq!(one["0:only"], 1000);

        let candidates = [
            input("a", 0, 1, Some(0.0)),
            input("b", 0, 1, Some(0.0)),
            input("c", 0, 1, Some(0.0)),
        ];
        let reversed = [candidates[2].clone(), candidates[1].clone(), candidates[0].clone()];
        let forward = pressure_weights(&candidates, &config(), &mut PlacementState::default()).unwrap();
        let backward = pressure_weights(&reversed, &config(), &mut PlacementState::default()).unwrap();
        assert_eq!(forward, backward);
        assert_eq!(forward["0:a"], 334, "lexically first identity wins an equal remainder");
        assert_eq!(forward.values().sum::<u32>(), 1000);
    }

    #[test]
    fn saturated_candidate_keeps_positive_floor_weight() {
        let inputs = [input("a", 0, 1, Some(1.0)), input("b", 0, 1, Some(0.0))];
        let weights = pressure_weights(&inputs, &config(), &mut PlacementState::default()).unwrap();
        assert!(weights["0:a"] > 0);
        assert_eq!(weights["0:a"] + weights["0:b"], 1000);
    }

    #[test]
    fn missing_invalid_and_zero_capacity_inputs_fail_without_state_mutation() {
        let mut state = PlacementState::default();
        let valid = [input("a", 0, 1, Some(0.0))];
        let _weights = pressure_weights(&valid, &config(), &mut state).unwrap();
        let before = state.clone();
        for bad in [
            input("a", 0, 1, None),
            input("a", 0, 0, Some(0.5)),
            input("a", 0, 1, Some(f64::NAN)),
        ] {
            assert_rejected(pressure_weights(&[bad], &config(), &mut state));
            assert_eq!(state.weights, before.weights);
            assert_eq!(state.availability, before.availability);
        }
    }

    #[test]
    fn invalid_weight_configuration_is_rejected() {
        let mut invalid_minimum = config();
        invalid_minimum.minimum_weight = 1001;
        assert_rejected(pressure_weights(
            &[input("a", 0, 1, Some(0.0))],
            &invalid_minimum,
            &mut PlacementState::default(),
        ));

        let mut invalid_bounds = config();
        invalid_bounds.minimum_weight = 10;
        invalid_bounds.maximum_weight = 9;
        assert_rejected(pressure_weights(
            &[input("a", 0, 1, Some(0.0))],
            &invalid_bounds,
            &mut PlacementState::default(),
        ));

        let mut invalid_smoothing = config();
        invalid_smoothing.smoothing_factor = f64::INFINITY;
        assert_rejected(pressure_weights(
            &[input("a", 0, 1, Some(0.0))],
            &invalid_smoothing,
            &mut PlacementState::default(),
        ));

        let mut invalid_floor = config();
        invalid_floor.availability_floor_percent = 0;
        assert_rejected(pressure_weights(
            &[input("a", 0, 1, Some(0.0))],
            &invalid_floor,
            &mut PlacementState::default(),
        ));
    }

    #[test]
    fn lowering_maximum_weight_bypasses_hysteresis_and_uses_new_total() {
        let inputs = [input("a", 0, 1, Some(0.0)), input("b", 0, 1, Some(0.0))];
        let mut cfg = config();
        cfg.change_threshold_percent = 100;
        let mut state = PlacementState::default();
        let initial = pressure_weights(&inputs, &cfg, &mut state).unwrap();
        assert_eq!(initial.values().sum::<u32>(), 1000);

        cfg.maximum_weight = 999;
        let updated = pressure_weights(&inputs, &cfg, &mut state).unwrap();

        assert_eq!(updated.values().sum::<u32>(), 999);
        assert!(updated.values().all(|weight| *weight <= cfg.maximum_weight));
    }

    #[test]
    fn raising_minimum_weight_bypasses_hysteresis_and_obeys_new_bound() {
        let inputs = [input("a", 0, 1, Some(0.0)), input("b", 0, 1, Some(0.9))];
        let mut cfg = config();
        cfg.change_threshold_percent = 100;
        let mut state = PlacementState::default();
        let initial = pressure_weights(&inputs, &cfg, &mut state).unwrap();
        assert!(initial["0:b"] < 100);

        cfg.minimum_weight = 100;
        let updated = pressure_weights(&inputs, &cfg, &mut state).unwrap();

        assert_eq!(updated.values().sum::<u32>(), cfg.maximum_weight);
        assert!(updated.values().all(|weight| *weight >= cfg.minimum_weight));
    }

    #[test]
    fn changing_pressure_signal_resets_ewma_history() {
        let mut cfg = config();
        cfg.smoothing_factor = 0.35;
        let mut state = PlacementState::default();
        let _initial = pressure_weights(
            &[input("a", 0, 1, Some(0.9)), input("b", 0, 1, Some(0.0))],
            &cfg,
            &mut state,
        )
        .unwrap();

        cfg.signal = PressureSignal::KvCacheUtilization;
        let new_inputs = [input("a", 0, 1, Some(0.0)), input("b", 0, 1, Some(0.9))];
        let updated = pressure_weights(&new_inputs, &cfg, &mut state).unwrap();
        let fresh = pressure_weights(&new_inputs, &cfg, &mut PlacementState::default()).unwrap();

        assert_eq!(
            updated, fresh,
            "a new signal must not inherit the previous signal's EWMA"
        );
        assert!(updated["0:a"] > updated["0:b"]);
    }

    #[test]
    fn unchanged_policy_pressure_jitter_remains_suppressed() {
        let mut cfg = config();
        cfg.change_threshold_percent = 10;
        let mut state = PlacementState::default();
        let baseline = pressure_weights(
            &[input("a", 0, 1, Some(0.0)), input("b", 0, 1, Some(0.0))],
            &cfg,
            &mut state,
        )
        .unwrap();

        let jittered = pressure_weights(
            &[input("a", 0, 1, Some(0.02)), input("b", 0, 1, Some(0.0))],
            &cfg,
            &mut state,
        )
        .unwrap();

        assert_eq!(jittered, baseline);
        assert_eq!(state.weights, baseline);
    }

    /// Assert rejection while also requiring an actionable nonempty reason.
    fn assert_rejected(result: Result<HashMap<String, u32>, String>) {
        assert!(
            matches!(result, Err(reason) if !reason.is_empty()),
            "expected a rejection reason"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the test exercises hysteresis, pressure entry, and repeated recovery on one retained state"
    )]
    fn smoothing_suppresses_small_changes_then_publishes_material_recovery() {
        let mut cfg = config();
        cfg.smoothing_factor = 0.25;
        cfg.change_threshold_percent = 10;
        let mut state = PlacementState::default();
        let baseline = pressure_weights(
            &[input("a", 0, 1, Some(0.0)), input("b", 0, 1, Some(0.0))],
            &cfg,
            &mut state,
        )
        .unwrap();
        let small = pressure_weights(
            &[input("a", 0, 1, Some(0.03)), input("b", 0, 1, Some(0.0))],
            &cfg,
            &mut state,
        )
        .unwrap();
        assert_eq!(baseline, small);
        let pressure = pressure_weights(
            &[input("a", 0, 1, Some(1.0)), input("b", 0, 1, Some(0.0))],
            &cfg,
            &mut state,
        )
        .unwrap();
        assert!(pressure["0:a"] < baseline["0:a"]);
        // A single recovery tick is intentionally suppressed by the 10%
        // publication hysteresis. Repeated reconciles converge the EWMA and
        // eventually publish the recovered preference.
        let mut recovery = pressure.clone();
        for _ in 0..8 {
            recovery = pressure_weights(
                &[input("a", 0, 1, Some(0.0)), input("b", 0, 1, Some(0.0))],
                &cfg,
                &mut state,
            )
            .unwrap();
        }
        assert!(recovery["0:a"] > pressure["0:a"]);
    }

    #[test]
    fn removing_candidates_clears_their_smoothing_and_weight_state() {
        let mut state = PlacementState::default();
        let _weights = pressure_weights(
            &[input("a", 0, 1, Some(0.0)), input("b", 0, 1, Some(0.0))],
            &config(),
            &mut state,
        )
        .unwrap();
        let retained = pressure_weights(&[input("a", 0, 1, Some(0.2))], &config(), &mut state).unwrap();
        assert_eq!(retained.len(), 1);
        assert!(!state.weights.contains_key("0:b"));
        assert!(!state.availability.contains_key("0:b"));
        let empty = pressure_weights(&[], &config(), &mut state).unwrap();
        assert!(empty.is_empty());
        assert!(state.weights.is_empty());
        assert!(state.availability.is_empty());
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the test jointly proves source-site scoping, freshness expiry, and conflict rejection"
    )]
    #[expect(
        clippy::float_cmp,
        reason = "the exposition parser round-trips the decimal literals used in this fixture"
    )]
    fn signals_are_scoped_fresh_and_conflicts_fail_closed() {
        let now = 1_000_i64;
        let local = format!("{QUEUE_PRESSURE_METRIC}{{grid_site=\"east\",grid_provider=\"same\"}} 0.2 950\n",);
        let peers = format!(
            "{QUEUE_PRESSURE_METRIC}{{grid_site=\"west\",grid_provider=\"same\"}} 0.8 950\n{QUEUE_PRESSURE_METRIC}{{grid_site=\"east\",grid_provider=\"stale\"}} 0.4 100\n",
        );
        let values = fresh_signal_values(&local, &peers, QUEUE_PRESSURE_METRIC, now, Duration::from_millis(100));
        assert_eq!(
            values[&SignalKey {
                site: "east".to_owned(),
                provider: "same".to_owned()
            }],
            0.2
        );
        assert_eq!(
            values[&SignalKey {
                site: "west".to_owned(),
                provider: "same".to_owned()
            }],
            0.8
        );
        assert!(!values.contains_key(&SignalKey {
            site: "east".to_owned(),
            provider: "stale".to_owned()
        }));

        let conflict =
            format!("{local}{QUEUE_PRESSURE_METRIC}{{grid_site=\"east\",grid_provider=\"same\"}} 0.9 960\n",);
        assert!(
            !fresh_signal_values(&conflict, "", QUEUE_PRESSURE_METRIC, now, Duration::from_millis(100)).contains_key(
                &SignalKey {
                    site: "east".to_owned(),
                    provider: "same".to_owned()
                }
            )
        );
    }
}
