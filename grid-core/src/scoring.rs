//! Backend scoring engine.
//!
//! Ranks backends by a weighted formula combining locality,
//! cost, and latency signals. The scorer reads from a
//! [`GridState`] snapshot with zero cross-site RPCs.
//!
//! [`GridState`]: crate::GridState

use serde::{Deserialize, Serialize};

use crate::{
    backend::{BackendConfig, BackendKind, ProviderKind},
    metrics::BackendMetrics,
    state::GridState,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum cost per 1k tokens for normalization (USD).
const MAX_COST: f64 = 0.1;

/// Maximum P99 latency for normalization (milliseconds).
const MAX_LATENCY: f64 = 5000.0;

/// Default latency score when no metrics are available.
const DEFAULT_LATENCY_SCORE: f64 = 0.5;

// ---------------------------------------------------------------------------
// Scoring Weights
// ---------------------------------------------------------------------------

/// Configurable weights for the scoring formula.
///
/// Higher weight means the signal has more influence on
/// backend selection. Defaults: locality 3.0, latency 2.0,
/// cost 1.0.
///
/// ```
/// use grid_core::ScoringWeights;
///
/// let w = ScoringWeights::default();
/// assert!((w.locality - 3.0).abs() < f64::EPSILON);
/// ```
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScoringWeights {
    /// Weight for the cost signal.
    pub cost: f64,

    /// Weight for the latency signal.
    pub latency: f64,

    /// Weight for the locality signal.
    pub locality: f64,
}

impl ScoringWeights {
    /// Creates scoring weights with explicit values.
    #[must_use]
    pub fn new(cost: f64, latency: f64, locality: f64) -> Self {
        Self {
            cost,
            latency,
            locality,
        }
    }
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            cost: 1.0,
            latency: 2.0,
            locality: 3.0,
        }
    }
}

// ---------------------------------------------------------------------------
// Scored Backend
// ---------------------------------------------------------------------------

/// A backend with its computed score.
///
/// Returned by [`score_backends`] in descending score order.
#[derive(Clone, Debug)]
pub struct ScoredBackend {
    /// The unique backend name.
    pub name: String,

    /// The backend endpoint URL.
    pub endpoint: String,

    /// The inference provider kind.
    pub provider: ProviderKind,

    /// Computed score (higher is better).
    pub score: f64,
}

impl ScoredBackend {
    /// Creates a scored backend entry.
    #[must_use]
    pub fn new(name: String, endpoint: String, provider: ProviderKind, score: f64) -> Self {
        Self {
            name,
            endpoint,
            provider,
            score,
        }
    }
}

// ---------------------------------------------------------------------------
// Scoring Functions
// ---------------------------------------------------------------------------

/// Scores all healthy backends and returns them ranked.
///
/// Filters out unhealthy backends, computes a weighted score
/// for each, and returns the results sorted in descending
/// score order. Backends with no metrics are assumed healthy
/// with default latency.
///
/// ```
/// # fn main() -> Result<(), grid_core::CoreError> {
/// use grid_core::{
///     BackendConfig, BackendKind, GridState, ProviderKind, ScoringWeights, score_backends,
/// };
///
/// let mut state = GridState::new();
/// state.add_backend(BackendConfig::new(
///     "test".to_owned(),
///     0.01,
///     0.02,
///     "http://localhost:8080".to_owned(),
///     BackendKind::Local,
///     ProviderKind::OpenAi,
///     None,
/// ))?;
/// let ranked = score_backends(&state, &ScoringWeights::default());
/// assert_eq!(ranked.len(), 1);
/// # Ok(())
/// # }
/// ```
#[must_use]
pub fn score_backends(state: &GridState, weights: &ScoringWeights) -> Vec<ScoredBackend> {
    let mut scored: Vec<ScoredBackend> = state
        .backends()
        .iter()
        .filter(|b| is_healthy(state, b))
        .map(|b| score_one(b, state.metrics(&b.name), weights))
        .collect();
    scored.sort_by(|a, b| b.score.total_cmp(&a.score));
    scored
}

/// Returns the locality score for a backend kind.
///
/// Values range from 0.0 to 1.0 where higher means
/// closer/preferred.
///
/// - [`Local`] = 1.0
/// - [`Remote`] = 0.7
/// - [`CloudManaged`] = 0.2
/// - [`ApiProvider`] = 0.1
///
/// [`Local`]: BackendKind::Local
/// [`Remote`]: BackendKind::Remote
/// [`CloudManaged`]: BackendKind::CloudManaged
/// [`ApiProvider`]: BackendKind::ApiProvider
#[must_use]
pub fn locality_score(kind: BackendKind) -> f64 {
    match kind {
        BackendKind::Local => 1.0,
        BackendKind::Remote => 0.7,
        BackendKind::CloudManaged => 0.2,
        BackendKind::ApiProvider => 0.1,
    }
}

// ---------------------------------------------------------------------------
// Private Functions
// ---------------------------------------------------------------------------

/// Checks whether a backend is healthy or has no metrics.
fn is_healthy(state: &GridState, backend: &BackendConfig) -> bool {
    state.metrics(&backend.name).is_none_or(|m| m.healthy)
}

/// Scores a single backend.
fn score_one(backend: &BackendConfig, metrics: Option<&BackendMetrics>, weights: &ScoringWeights) -> ScoredBackend {
    let loc = weights.locality * locality_score(backend.kind);
    let cost = weights.cost * cost_score(backend.cost_per_1k_input);
    let lat = weights.latency * latency_score(metrics);
    ScoredBackend::new(
        backend.name.clone(),
        backend.endpoint.clone(),
        backend.provider,
        loc + cost + lat,
    )
}

/// Computes cost score (0.0 to 1.0, higher = cheaper).
fn cost_score(cost_per_1k: f64) -> f64 {
    if cost_per_1k <= 0.0 {
        return 1.0;
    }
    (1.0 - cost_per_1k / MAX_COST).max(0.0)
}

/// Computes latency score (0.0 to 1.0, higher = faster).
fn latency_score(metrics: Option<&BackendMetrics>) -> f64 {
    metrics.map_or(DEFAULT_LATENCY_SCORE, |m| {
        (1.0 - m.latency_p99_ms / MAX_LATENCY).max(0.0)
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locality_score_values() {
        assert_eq!(locality_score(BackendKind::Local), 1.0, "local");
        assert_eq!(locality_score(BackendKind::Remote), 0.7, "remote");
        assert_eq!(locality_score(BackendKind::CloudManaged), 0.2, "cloud");
        assert_eq!(locality_score(BackendKind::ApiProvider), 0.1, "api");
    }

    #[test]
    fn score_empty_backends() {
        let state = GridState::new();
        let weights = ScoringWeights::default();
        let result = score_backends(&state, &weights);
        assert!(result.is_empty(), "empty state should yield no results");
    }

    #[test]
    fn score_single_backend() {
        let mut state = GridState::new();
        add(&mut state, "a", BackendKind::Local, 0.01);
        let result = score_backends(&state, &ScoringWeights::default());
        assert_eq!(result.len(), 1, "should score single backend");
    }

    #[test]
    fn score_prefers_local() {
        let mut state = GridState::new();
        add(&mut state, "local", BackendKind::Local, 0.01);
        add(&mut state, "api", BackendKind::ApiProvider, 0.01);
        let result = score_backends(&state, &ScoringWeights::default());
        assert_eq!(
            result.first().map(|b| b.name.as_str()),
            Some("local"),
            "local should rank first"
        );
    }

    #[test]
    fn score_prefers_cheaper() {
        let weights = ScoringWeights::new(10.0, 0.0, 0.0);
        let mut state = GridState::new();
        add(&mut state, "expensive", BackendKind::Local, 0.08);
        add(&mut state, "cheap", BackendKind::Local, 0.01);
        let result = score_backends(&state, &weights);
        assert_eq!(
            result.first().map(|b| b.name.as_str()),
            Some("cheap"),
            "cheaper backend should rank first"
        );
    }

    #[test]
    fn score_excludes_unhealthy() {
        let mut state = GridState::new();
        add(&mut state, "sick", BackendKind::Local, 0.01);
        state.set_metrics("sick".to_owned(), BackendMetrics::new(0.5, false, 100.0));
        let result = score_backends(&state, &ScoringWeights::default());
        assert!(result.is_empty(), "unhealthy backend should be excluded");
    }

    #[test]
    fn score_descending_order() {
        let mut state = GridState::new();
        add(&mut state, "low", BackendKind::ApiProvider, 0.05);
        add(&mut state, "high", BackendKind::Local, 0.01);
        add(&mut state, "mid", BackendKind::Remote, 0.03);
        let result = score_backends(&state, &ScoringWeights::default());
        let scores: Vec<f64> = result.iter().map(|b| b.score).collect();
        for pair in scores.windows(2) {
            assert!(
                pair.first().is_some_and(|a| { pair.get(1).is_some_and(|b| a >= b) }),
                "scores should be in descending order"
            );
        }
    }

    #[test]
    fn cost_score_free_is_max() {
        assert_eq!(cost_score(0.0), 1.0, "free should score 1.0");
    }

    #[test]
    fn cost_score_expensive_is_low() {
        let score = cost_score(0.09);
        assert!(score < 0.2, "expensive should score low: {score}");
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    /// Adds a test backend to the state, aborting on failure.
    fn add(state: &mut GridState, name: &str, kind: BackendKind, cost: f64) {
        state
            .add_backend(BackendConfig::new(
                name.to_owned(),
                cost,
                cost * 2.0,
                format!("http://{name}:8080"),
                kind,
                ProviderKind::OpenAi,
                None,
            ))
            .unwrap_or_else(|_| std::process::abort());
    }
}
