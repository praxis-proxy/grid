//! Mock AI provider servers for integration testing.
//!
//! Each module exposes a `router()` function returning an
//! [`axum::Router`] that simulates a specific provider's API.
//! Pass an [`AppState`] to inject the `X-Grid-Test-Provider`
//! response header for test attribution.

#![deny(unsafe_code)]

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

/// Mock Anthropic Messages API.
pub mod anthropic;
/// Mock AWS Bedrock Converse API.
pub mod bedrock;
/// Shared HTTP response utilities.
mod common;
/// Mock MCP (Model Context Protocol) `tools/list` server.
pub mod mcp;
/// Mock `OpenAI` chat completions and Responses API.
pub mod openai;
/// Mock Google Vertex AI `generateContent` API.
pub mod vertex;

/// Shared application state injected into every provider router.
#[expect(
    clippy::partial_pub_fields,
    reason = "the existing public provider configuration stays compatible while receipt state remains private"
)]
#[derive(Clone, Debug)]
pub struct AppState {
    /// Site identity for the `X-Grid-Test-Provider` response header.
    pub provider_site: Arc<str>,

    /// Normalized queue depth exported by the demo metrics endpoint.
    pub queue_depth: f64,

    /// Whether task-local qualification receipt endpoints are enabled.
    qualification_enabled: bool,
    /// Number of OpenAI-compatible inference requests received by this backend.
    inference_requests: Arc<AtomicU64>,
    /// Bounded request-ID receipt window used to detect retries and misattribution.
    qualification_request_ids: Arc<Mutex<Vec<Option<String>>>>,
    /// Provider-local sequence for mock Responses and Conversations identifiers.
    next_state_id: Arc<AtomicU64>,
}

impl AppState {
    /// Construct provider state with qualification receipts disabled by default.
    pub fn new<T: Into<Arc<str>>>(provider_site: T, queue_depth: f64) -> Self {
        Self {
            provider_site: provider_site.into(),
            queue_depth,
            qualification_enabled: false,
            inference_requests: Arc::default(),
            qualification_request_ids: Arc::default(),
            next_state_id: Arc::default(),
        }
    }

    /// Enable the opt-in receipt endpoint for an isolated qualification backend.
    #[must_use]
    pub fn with_qualification_receipts(mut self, enabled: bool) -> Self {
        self.qualification_enabled = enabled;
        self
    }

    /// Record one inference request and its optional unique qualification ID.
    pub(crate) fn record_inference_request(&self, request_id: Option<String>) {
        if !self.qualification_enabled {
            return;
        }
        self.inference_requests.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut request_ids) = self.qualification_request_ids.lock() {
            if request_ids.len() == 16_384 {
                request_ids.remove(0);
            }
            request_ids.push(request_id);
        }
    }

    /// Whether the isolated qualification receipt endpoint is enabled.
    pub(crate) const fn qualification_enabled(&self) -> bool {
        self.qualification_enabled
    }

    /// Current inference receipt snapshot. Returns an empty ID list if the lock is poisoned.
    pub(crate) fn inference_receipts(&self) -> (u64, Vec<Option<String>>) {
        let request_ids = self
            .qualification_request_ids
            .lock()
            .map(|request_ids| request_ids.clone())
            .unwrap_or_default();
        (self.inference_requests.load(Ordering::Relaxed), request_ids)
    }

    /// Generate a provider-specific `OpenAI` state ID that the fixture can validate on reuse.
    pub(crate) fn next_state_id(&self, kind: &str) -> String {
        let serial = self.next_state_id.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        format!("{kind}_{}-{serial}", self.provider_site)
    }
}
