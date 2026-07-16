# Operator-Generated Consumer Config

The Grid operator can generate the consumer Praxis `ConfigMap` from routing overlay
data.  This is an opt-in feature on each `GatewayRef`.

## Implemented: GatewayRef.consumerConfig

When `spec.gatewayRefs[].consumerConfig.enabled: true`, the `GridNetwork`
controller renders a `praxis.yaml`-keyed `ConfigMap` in the gateway namespace on
every reconcile.  The generated config includes:

**Validation status:** `verify-api-fallback-native` proves end-to-end runtime
consumption of the operator-generated `ConfigMap`.  The xtask harness reads the
exact `praxis.yaml` from `op-e2e-consumer-config` in the provider cluster,
applies it byte-for-byte as `praxis-consumer-config` in the consumer cluster, and
confirms all 9 routing assertions pass with the live consumer pod running the
operator-generated config.  Token bytes are absent from the `ConfigMap`,
consumer-cluster replica, overlay JSON, and all logs.

The generated config is a complete, runnable Praxis config containing:

- `listeners:` — one public listener at `0.0.0.0:{listenerPort}` (default 8080)
- `filter_chains:` — the consumer filter chain:
  - `grid_route` candidates from the overlay (with `credential.secretRef` for
    credential-bearing candidates)
  - `grid_credential_inject` entries using `file:` sources when credential-bearing
    candidates are present — token bytes are never written to the `ConfigMap`
  - `load_balancer` entries (one per unique candidate cluster). Clusters with
    matching `consumerConfig.clusterEndpoints[]` entries include endpoint and TLS
    settings; clusters without a match render as name-only stubs
- `admin:` — admin listener at `127.0.0.1:9901`
- `shutdown_timeout_secs: 5`

See [`docs/architecture/crds.md`](crds.md#gatewayrefconsumerconfig) for the full
field reference.

## Deployment readiness boundary

The operator-generated `ConfigMap` is a complete, runnable Praxis config.
Runtime consumption is proven by `verify-api-fallback-native` (see above).
The remaining production deployment inputs not yet automated are:

- provider endpoint and TLS topology source-of-truth for `load_balancer`
  clusters;
- consumer-cluster credential Secret provisioning and rotation;
- gateway reload or rollout behavior when the generated `ConfigMap` or mounted
  Secret changes;
- RBAC and installation profiles for cross-namespace `ConfigMap` and Secret
  access;
- status, events, and metrics that make render or provisioning failures visible
  to operators;
- gateway image compatibility with the Grid filters used by the generated
  config.

## Security

The generated `ConfigMap` never contains credential token bytes.  Credential
entries reference a mounted Kubernetes Secret via a `file:` path.  The Secret must
be provisioned in the consumer cluster by external tooling.

## Design proposal

For the broader ownership design — including `ConsumerGateway` CRD options,
cross-cluster Secret lifecycle, pod ownership boundaries, and Secret rotation —
see [`docs/proposals/consumer-config.md`](../proposals/consumer-config.md).
