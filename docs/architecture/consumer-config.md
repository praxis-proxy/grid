# Operator-Generated Consumer Config

The Grid operator can generate the consumer Praxis `ConfigMap` from routing overlay
data.  This is an opt-in feature on each `GatewayRef`.

## Implemented: GatewayRef.consumerConfig

When `spec.gatewayRefs[].consumerConfig.enabled: true`, the `GridNetwork`
controller renders a `praxis.yaml`-keyed `ConfigMap` in the gateway namespace on
every reconcile.  The generated config includes:

- `grid_route` candidates from the overlay (with `credential.secretRef` for
  credential-bearing candidates)
- `grid_credential_inject` entries using `file:` sources when credential-bearing
  candidates are present — token bytes are never written to the `ConfigMap`
- `load_balancer` cluster stubs (one entry per unique candidate cluster)

See [`docs/architecture/crds.md`](crds.md#gatewayrefconsumerconfig) for the full
field reference.

## Security

The generated `ConfigMap` never contains credential token bytes.  Credential
entries reference a mounted Kubernetes Secret via a `file:` path.  The Secret must
be provisioned in the consumer cluster by external tooling.

## Design proposal

For the broader ownership design — including `ConsumerGateway` CRD options,
cross-cluster Secret lifecycle, pod ownership boundaries, and Secret rotation —
see [`docs/proposals/consumer-config.md`](../proposals/consumer-config.md).
