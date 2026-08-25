//! Kubernetes controllers for the Grid Operator.

/// [`AgentToolProvider`] controller.
///
/// [`AgentToolProvider`]: crate::crd::agent_tool_provider::AgentToolProvider
pub mod agent_tool_provider;

/// [`GridNetwork`] controller.
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
pub mod grid_network;

/// [`GridSite`] controller.
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
pub mod grid_site;

/// [`InferenceProvider`] controller (OP-02).
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
pub mod inference_provider;
