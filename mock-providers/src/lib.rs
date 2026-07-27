//! Mock AI provider servers for integration testing.
//!
//! Each module exposes a `router()` function returning an
//! [`axum::Router`] that simulates a specific provider's API.
//! Pass an [`AppState`] to inject the `X-Grid-Demo-Provider`
//! response header for demo attribution.

#![deny(unsafe_code)]

use std::sync::Arc;

/// Mock Anthropic Messages API.
pub mod anthropic;
/// Mock AWS Bedrock Converse API.
pub mod bedrock;
/// Shared HTTP response utilities.
mod common;
/// Mock `OpenAI` chat completions and Responses API.
pub mod openai;
/// Mock Google Vertex AI `generateContent` API.
pub mod vertex;

/// Shared application state injected into every provider router.
#[derive(Clone, Debug)]
pub struct AppState {
    /// Site identity for the `X-Grid-Demo-Provider` response header.
    pub provider_site: Arc<str>,

    /// Normalized queue depth exported by the demo metrics endpoint.
    pub queue_depth: f64,
}
