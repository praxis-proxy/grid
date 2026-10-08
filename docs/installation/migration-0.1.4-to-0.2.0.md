# 0.1.4 to 0.2.0 CRD Field Migration

The 0.2.0 release accepts both the previous and current names for CRD spec
fields. The `grid-site` and `grid-operator` charts also translate previous
value names when they render resources. The previous names are deprecated and
will be removed in a later release. Migrate custom resources and Helm values before
upgrading to 0.2.0.

Do not set both names for one field. The API schema and chart reject conflicting
old and new names.

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
| `grid-operator` rendered `GridNetwork.spec` | `gatewayRefs` | `consumerGateways` |
| `grid-operator` rendered `GridNetwork.spec.consumerGateways[]` | `localSiteName` | `siteName` |
| `grid-operator` rendered `GridNetwork.spec.tls` | `swimKeyRef` | `swimKeySecretRef` |
| `grid-operator` chart `grid.providers[]` | `routingClusterRef` | `clusterName` |
| `grid-operator` chart `grid.providers[]` | `gatewayRef` | `providerGateway` |
| `grid-operator` chart `grid.providers[].metricsConfig` | `metricsEndpoint` | `endpoint` |
| `grid-operator` chart `grid.providers[].auth` | `manual` | `credentialsManagedExternally` |

## Upgrade to 0.2.0

Existing CRD spec field names and chart values remain accepted in 0.2.0.
Update manifests and Helm values to the current names when convenient.

Status fields and reasons change in 0.2.0 without legacy aliases. Update scripts
and dashboards that read them during the 0.2.0 upgrade, including
`overlayStatus` to `routingMapStatus` and `consumerConfigStatus` to
`praxisConfigStatus`. Status is operator-owned; do not migrate it by hand.
The compatibility window covers spec fields and chart values only.

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

## Before upgrading after the compatibility window

1. Back up the affected custom resources and Helm values.
2. Update manifests and Helm values using the mapping above.
3. Pause the operator before installing CRD schemas that remove the aliases.
4. Apply the updated custom resources and upgrade `grid-site` with its updated
   values.
5. Start the later operator after all affected resources use the current names.
