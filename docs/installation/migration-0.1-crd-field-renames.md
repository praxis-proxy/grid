# 0.1.x CRD Field Migration

The 0.1.x CRD field renames keep the `grid.praxis-proxy.io/v1alpha1` API
version, but the updated operator does not read the previous field names. There
is no automatic conversion. Update existing custom resources and `grid-site`
Helm values before the updated operator reconciles them.

## Field mapping

| Resource or Helm value | Previous field | Current field |
|---|---|---|
| `GridNetwork.spec` | `gatewayRefs` | `consumerGateways` |
| `GridNetwork.spec.consumerGateways[]` | `localSiteName` | `siteName` |
| `GridNetwork.spec.consumerGateways[]` | `consumerConfig` | `praxisConfig` |
| `GridNetwork.spec.consumerGateways[].praxisConfig` | `enabled` | `generate` |
| `GridNetwork.spec.consumerGateways[].praxisConfig` | `credentialMountBase` | `credentialMountPath` |
| `GridNetwork.spec.tls` | `swimKeyRef` | `swimKeySecretRef` |
| `GridSite.spec` | `egress` | `gatewayEndpoint` |
| `InferenceProvider.spec` | `gatewayRef` | `providerGateway` |
| `InferenceProvider.spec` | `routingClusterRef` | `clusterName` |
| `InferenceProvider.spec.metricsConfig` | `metricsEndpoint` | `endpoint` |
| `InferenceProvider.spec.auth` and `AgentToolProvider.spec.auth` | `manual` | `credentialsManagedExternally` |
| `grid-site` chart `gridNetwork` values | `gatewayRefs` | `consumerGateways` |
| `grid-site` chart `gridNetwork.consumerGateways[]` | `localSiteName` | `siteName` |
| `grid-site` chart `gridNetwork.consumerGateways[]` | `consumerConfig` | `praxisConfig` |
| `grid-site` chart `gridNetwork.consumerGateways[].praxisConfig` | `enabled` | `generate` |
| `grid-site` chart `gridNetwork.consumerGateways[].praxisConfig` | `credentialMountBase` | `credentialMountPath` |
| `grid-site` chart `gridNetwork.tls` | `swimKeyRef` | `swimKeySecretRef` |
| `grid-site` chart `gridSite` values | `egress` | `gatewayEndpoint` |
| `grid-site` chart `inferenceProviders` values | `routingClusterRef` | `clusterName` |
| `grid-site` chart `inferenceProviders` values | `gatewayRef` | `providerGateway` |
| `grid-site` chart `inferenceProviders[].metricsConfig` | `metricsEndpoint` | `endpoint` |
| `grid-site` chart `inferenceProviders[].auth` | `manual` | `credentialsManagedExternally` |

## Upgrade sequence

1. Back up the affected custom resources and Helm values.
2. Update manifests and Helm values using the mapping above.
3. Install the updated CRD schemas.
4. Apply the updated custom resources and upgrade `grid-site` with its updated
   values.
5. Start or upgrade the operator to the matching 0.1.x release. Do not let the
   updated operator reconcile the old field names.

Pause the operator before installing the updated schemas and keep it paused
until the migrated resources are applied. The old and new operator versions do
not share these field names.

## Status and reason names

These fields are operator-owned output; do not edit them during migration.
Update any scripts or dashboards that read them. The updated operator writes the
current status fields.

| Previous status field or reason | Current status field or reason |
|---|---|
| `distributedProviderCount` | `remoteProviderCount` |
| `consumerConfigStatus` | `praxisConfigStatus` |
| `overlayStatus` | `routingMapStatus` |
| `OverlayRenderFailed` | `RoutingMapRenderFailed` |
| `OverlayApplyFailed` | `RoutingMapApplyFailed` |
| `ConsumerConfigRenderFailed` | `PraxisConfigRenderFailed` |
| `ConsumerConfigApplyFailed` | `PraxisConfigApplyFailed` |
| `ConsumerConfigError` | `PraxisConfigError` |
| `ConsumerConfigDisabled` | `PraxisConfigDisabled` |
| `EgressMissing` | `GatewayAddressMissing` |
