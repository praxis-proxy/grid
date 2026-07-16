//! Kubernetes resource builders for the Grid Operator.

/// Operator-owned consumer Praxis config renderer.
///
/// Generates the `praxis.yaml` content for consumer gateway `ConfigMap`s from
/// routing overlays.  Used when `GatewayRef.consumerConfig.enabled` is true.
pub(crate) mod consumer_config;

/// Controller-owned credential resolution for API-provider authentication.
///
/// Provides [`CredentialPlan`], [`CredentialResolver`], and the v1
/// [`KubernetesSecretResolver`] backend.  Call [`credential_plan_from_auth`]
/// to parse `spec.auth` without I/O, then use a resolver or
/// [`verify_credential_accessible`] to interact with Kubernetes.
///
/// [`CredentialPlan`]: credentials::CredentialPlan
/// [`CredentialResolver`]: credentials::CredentialResolver
/// [`KubernetesSecretResolver`]: credentials::KubernetesSecretResolver
/// [`credential_plan_from_auth`]: credentials::credential_plan_from_auth
/// [`verify_credential_accessible`]: credentials::verify_credential_accessible
pub mod credentials;

/// Bridge from operator routing overlays to Praxis `grid_route` filter config.
pub mod overlay_bridge;
/// Provider metrics collection for the [`GridNetwork`] overlay renderer.
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
pub(crate) mod provider_metrics;
/// Pure overlay renderer for Praxis `grid_route` routing candidates.
pub mod routing_overlay;
/// Secret builders for grid TLS certificates.
pub mod secret;
/// Trust bundle management for grid mTLS.
pub mod trust_bundle;
