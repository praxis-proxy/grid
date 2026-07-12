//! Mock AI provider servers for integration testing.
//!
//! Each module exposes a `router()` function returning an
//! [`axum::Router`] that simulates a specific provider's API.

#![deny(unsafe_code)]

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
