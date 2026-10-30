# 0.3.x CRD Field Migration

The 0.3.x release accepts both the previous and current names for CRD spec
fields. The `grid-site` and `grid-operator` charts also translate previous
value names when they render resources. The previous names are deprecated and
will be removed in a later release. Migrate custom resources and Helm values before
upgrading to 0.3.x.

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

## Upgrade to 0.3.x

Existing CRD spec field names and chart values remain accepted in 0.3.x.
Update manifests and Helm values to the current names when convenient.

Status fields and reasons change in 0.3.x without legacy aliases. Update scripts
and dashboards that read them during the 0.3.x upgrade, including
`overlayStatus` to `routingMapStatus` and `consumerConfigStatus` to
`praxisConfigStatus`. Status is operator-owned; do not migrate it by hand.
The compatibility window covers spec fields and chart values only.

A generated consumer gateway derives an endpoint for every local
`InferenceProvider` of its network, and under Local membership for every
candidate a member's catalog offers, without `deriveTopology.fromProviders`
naming it. Under SWIM membership `fromProviders` still gates remote candidates.
Delete or disable a provider that must not be served before upgrading. See
[Derived endpoint topology](../architecture/consumer-config.md#derived-endpoint-topology).

Upgrade in this order:

1. Apply the 0.3.x CRDs first. When the platform owns the CRDs, apply the
   chart's CRD templates before installing the charts with `crds.enabled=false`.
2. Upgrade `grid-operator`.
3. Upgrade `grid-site`.
4. Apply custom resources and Helm values using the current field names.

For a rollback, roll back `grid-site` before `grid-operator` so the older
operator does not receive current field names. Restore custom resources and
values only after the older CRDs and charts are ready.

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

### Grid ID on GitOps-managed members

The operator copies a status-only grid ID into an empty `spec.gridId` once,
and a CEL rule then refuses any change to it. The `grid-site` chart always
renders `gridId`, empty unless `gridNetwork.gridId` is set. Argo CD, with
client-side or server-side apply, then tries to reset the copied ID to the
empty value, the CEL rule refuses it, and self-heal fails on every retry.
Leaving the field out does not help, because removing a previously applied
value is also a change.

Set `gridNetwork.gridId` to the member's current `status.gridId` in the same
change that upgrades the operator:

```bash
kubectl get gridnetwork <name> -o jsonpath='{.status.gridId}{"\n"}'
```

Alternatively, have the Application ignore the field:

```yaml
spec:
  ignoreDifferences:
    - group: grid.praxis.fast
      kind: GridNetwork
      jsonPointers:
        - /spec/gridId
  syncPolicy:
    syncOptions:
      - RespectIgnoreDifferences=true
```

`RespectIgnoreDifferences` keeps a sync from writing the ignored field.

A plain `helm upgrade` is unaffected. Helm 3 patches only what changed between
the previous and the new manifest, and an empty `gridId` in both leaves the
copied ID in place. `helm upgrade --force` replaces the object and fails the
same way as Argo CD.

## Before upgrading after the compatibility window

1. Back up the affected custom resources and Helm values.
2. Update manifests and Helm values using the mapping above.
3. Pause the operator before installing CRD schemas that remove the aliases.
4. Apply the updated custom resources and upgrade `grid-site` with its updated
   values.
5. Start the later operator after all affected resources use the current names.
