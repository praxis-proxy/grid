//! Custom resource definitions for the AI Grid.

/// [`AgentToAgentProvider`] — A2A agents available over the grid.
///
/// [`AgentToAgentProvider`]: agent_to_agent_provider::AgentToAgentProvider
pub mod agent_to_agent_provider;

/// [`AgentToolProvider`] — MCP tool servers available over the grid.
///
/// [`AgentToolProvider`]: agent_tool_provider::AgentToolProvider
pub mod agent_tool_provider;

/// Authentication strategy types shared across providers.
pub mod auth;

/// [`GridNetwork`] — the grid itself, top-level tenancy boundary.
///
/// [`GridNetwork`]: grid_network::GridNetwork
pub mod grid_network;

/// [`GridSite`] — a remote site in the grid.
///
/// [`GridSite`]: grid_site::GridSite
pub mod grid_site;

/// [`InferenceProvider`] — inference backends available over the grid.
///
/// [`InferenceProvider`]: inference_provider::InferenceProvider
pub mod inference_provider;
