# 0.1.5 to 0.1.6 CRD Field Migration

The 0.1.5 release accepts both the previous and current names for CRD spec
fields. The `grid-site` chart also translates previous value names when it
renders resources. The previous names are deprecated and will be removed in
0.1.6. Migrate custom resources and Helm values before upgrading to 0.1.6.

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

## Upgrade to 0.1.5

You can upgrade to 0.1.5 with existing field names. Update manifests and
`grid-site` values to the current names when convenient.

## Before upgrading to 0.1.6

1. Back up the affected custom resources and Helm values.
2. Update manifests and Helm values using the mapping above.
3. Pause the operator before installing the 0.1.6 CRD schemas.
4. Apply the updated custom resources and upgrade `grid-site` with its updated
   values.
5. Start the 0.1.6 operator after all affected resources use the current names.

Status output uses the current names in 0.1.5; the compatibility window covers
spec fields and chart values. Update scripts and dashboards that read status.
Status is operator-owned and should not be edited by hand.

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
