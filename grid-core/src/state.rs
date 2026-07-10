//! Grid state snapshot.

use std::collections::HashMap;

use crate::{backend::BackendConfig, error::CoreError, metrics::BackendMetrics};

// ---------------------------------------------------------------------------
// Grid State
// ---------------------------------------------------------------------------

/// Snapshot of the grid's known backends and their metrics.
///
/// For MVP, this is populated from static configuration.
/// In production, it will be backed by the local CRDT replica
/// populated via SWIM gossip.
#[derive(Clone, Debug, Default)]
pub struct GridState {
    /// Registered backends.
    backends: Vec<BackendConfig>,

    /// Per-backend metrics keyed by backend name.
    metrics: HashMap<String, BackendMetrics>,
}

impl GridState {
    /// Creates an empty grid state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a backend in the grid.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::DuplicateBackend`] if a backend with
    /// the same name is already registered.
    pub fn add_backend(&mut self, backend: BackendConfig) -> Result<(), CoreError> {
        if self.backends.iter().any(|b| b.name == backend.name) {
            return Err(CoreError::DuplicateBackend { name: backend.name });
        }
        self.backends.push(backend);
        Ok(())
    }

    /// Returns the number of registered backends.
    #[must_use]
    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    /// Returns a slice of all registered backends.
    #[must_use]
    pub fn backends(&self) -> &[BackendConfig] {
        &self.backends
    }

    /// Returns metrics for a backend by name.
    #[must_use]
    pub fn metrics(&self, name: &str) -> Option<&BackendMetrics> {
        self.metrics.get(name)
    }

    /// Updates metrics for a named backend.
    pub fn set_metrics(&mut self, name: String, metrics: BackendMetrics) {
        self.metrics.insert(name, metrics);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{BackendKind, ProviderKind};

    #[test]
    fn empty_state() {
        let state = GridState::new();
        assert_eq!(state.backend_count(), 0, "new state should be empty");
    }

    #[test]
    fn add_and_count() {
        let mut state = GridState::new();
        state
            .add_backend(test_backend("a"))
            .unwrap_or_else(|_| std::process::abort());
        state
            .add_backend(test_backend("b"))
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(state.backend_count(), 2, "should have 2 backends");
    }

    #[test]
    fn add_duplicate_fails() {
        let mut state = GridState::new();
        state
            .add_backend(test_backend("dup"))
            .unwrap_or_else(|_| std::process::abort());
        assert!(state.add_backend(test_backend("dup")).is_err(), "duplicate should fail");
    }

    #[test]
    fn set_and_get_metrics() {
        let mut state = GridState::new();
        state.set_metrics("test".to_owned(), BackendMetrics::healthy_default());
        let m = state.metrics("test");
        assert!(m.is_some(), "should have metrics");
        assert!(m.is_some_and(|m| m.healthy), "should be healthy");
    }

    #[test]
    fn missing_metrics_returns_none() {
        let state = GridState::new();
        assert!(state.metrics("nope").is_none(), "should be None");
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    /// Creates a test backend with the given name.
    fn test_backend(name: &str) -> BackendConfig {
        BackendConfig::new(
            name.to_owned(),
            0.01,
            0.02,
            "http://localhost:8080".to_owned(),
            BackendKind::Local,
            ProviderKind::OpenAi,
            None,
        )
    }
}
