# Operator-Owned Consumer Config — Design

This document describes the design for full operator ownership of consumer gateway
configuration.  See [`docs/architecture/crds.md`](../architecture/crds.md) for the
implemented `GatewayRef.consumerConfig` API.

## Current boundary

Grid already owns provider credential validation and routing overlay generation:

```text
InferenceProvider.spec.auth.secretRef
        |
        v
Grid operator validates the Secret reference
        |
        v
Grid operator renders grid-config.json
        |
        v
Consumer gateway uses grid_route and grid_credential_inject
```

The overlay contains only credential references:

```json
{
  "credential": {
    "strategy": "bearer_token",
    "secretRef": {
      "name": "api-provider-creds",
      "namespace": "default",
      "key": "token"
    }
  }
}
```

The token value is not part of the overlay.  The remaining production gap is
consumer gateway configuration: the gateway needs a Praxis config that includes
the Grid route candidates, load-balancer clusters, and file-backed credential
injection entries.

The first operator-owned step is implemented as `GatewayRef.consumerConfig`: the
operator can render and apply a consumer Praxis `ConfigMap`, and Kind validation
checks its shape and token-safety invariants.  The API also accepts explicit
`clusterEndpoints[]` entries for rendered `load_balancer` endpoint and TLS
topology.  The generated config is not yet used as the live consumer gateway
config because validation still uses the harness-generated runtime config.

## Ownership model

| Resource | Recommended owner |
|---|---|
| `InferenceProvider.spec.auth.secretRef` | User or external automation |
| Provider credential Secret | User, platform automation, or external secret manager |
| Grid overlay ConfigMap | Grid operator |
| Consumer Praxis ConfigMap | Grid operator |
| Consumer credential Secret | User, platform automation, or external secret manager |
| Consumer Deployment, Service, and TLS material | Consumer gateway deployment owner |
| Request-time route selection and credential injection | Gateway data-plane filters |

The Grid operator should generate the consumer Praxis ConfigMap because it has
the required control-plane inputs: routing candidates, credential references,
local site identity, and gateway namespace.

The Grid operator should not own the consumer gateway pod lifecycle in the first
implementation.  Pod resources, rollouts, scheduling, and TLS mounting are
deployment concerns and should remain with the gateway deployment owner.

## Desired production flow

1. A user or external secret manager creates the provider credential Secret.
2. `InferenceProvider.spec.auth.secretRef` points at that Secret.
3. The Grid operator validates the reference and records credential failures in
   provider status without exposing the token value.
4. The Grid operator renders the routing overlay.  Credential-bearing candidates
   include only `{ strategy, secretRef }`.
5. For each opt-in consumer gateway, the Grid operator renders a consumer Praxis
   ConfigMap:
   - `grid_route` candidates include `credential.secretRef`;
   - `grid_credential_inject` entries use `file:` sources;
   - `load_balancer` clusters are derived from candidate clusters and gateway
     topology.
6. The consumer gateway pod mounts the credential Secret.
7. At request time, `grid_route` selects the candidate and
   `grid_credential_inject` reads the mounted file and injects the upstream
   `Authorization` header.

## API shape options

### Option A — Extend `GatewayRef` with managed consumer config

Add an opt-in consumer config block to each `GridNetwork.spec.gatewayRefs[]`
entry:

```yaml
spec:
  gatewayRefs:
    - name: inference-gw
      namespace: praxis-system
      localSiteName: cluster-east
      consumerConfig:
        enabled: true
        credentialMountBase: /run/secrets/grid-credentials
```

The operator continues to render the existing overlay ConfigMap and also renders
a consumer Praxis ConfigMap for gateways with `consumerConfig.enabled: true`.

Pros:

- Smallest API change.
- No new CRD or controller.
- Existing deployments remain unchanged because the feature is opt-in.
- Fits the current `GridNetwork` reconcile model.

Cons:

- `GatewayRef` becomes responsible for both overlay destination and consumer
  config generation.
- Same-cluster behavior is straightforward; cross-cluster consumer Secret
  provisioning still needs an external owner.

Verdict: recommended first step.

### Option B — Add a dedicated `ConsumerGateway` CRD

A separate resource describes a consumer gateway and its config lifecycle:

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: ConsumerGateway
metadata:
  name: inference-gw
  namespace: praxis-system
spec:
  gridNetworkRef: production
  localSiteName: cluster-east
  credentialMountBase: /run/secrets/grid-credentials
```

Pros:

- Clean long-term separation between mesh state and consumer gateway state.
- Can have its own status, readiness, rotation policy, and per-gateway lifecycle.

Cons:

- Adds a CRD, controller, watch, and status model.
- Larger migration and testing surface.

Verdict: good long-term shape, but more scope than the first implementation
needs.

### Option C — External gateway controller owns consumer config

The Grid operator renders only the overlay.  A separate gateway operator watches
Grid output and renders the Praxis config.

Pros:

- Keeps the Grid operator narrowly focused on routing state.
- Lets a gateway-specific operator own pod lifecycle and config together.

Cons:

- Requires another controller before the production path is complete.
- Splits end-to-end ownership across multiple reconcilers.

Verdict: defer until a gateway operator exists.

## Recommendation

Use Option A first: extend `GatewayRef` with an opt-in `consumerConfig` block.

This gives Grid one production-shaped path for generating the consumer Praxis
ConfigMap without adding a new CRD.  If gateway lifecycle and status reporting
grow beyond this shape, introduce a dedicated `ConsumerGateway` resource later.

## Secret lifecycle

### Same-cluster Secrets

When the consumer gateway runs in the same cluster as the Grid operator, the
operator can validate referenced Secrets and optionally mirror them into the
gateway namespace.  This is the simplest deployment shape, but it requires the
operator to have Secret read/write permissions in the target namespaces.

### Cross-cluster Secrets

When the consumer gateway runs in a different cluster, the Grid operator should
not assume it can copy Secret bytes across cluster boundaries.

Recommended v1 behavior:

- The operator renders the expected `file:` paths in the consumer config.
- The consumer credential Secret must already exist in the consumer cluster.
- External automation, External Secrets, Vault, or platform tooling owns
  synchronization.

Future integrations can add first-class External Secrets or Vault support
without changing the route-selection interface.

### Rotation

Kubernetes updates mounted Secret files automatically, but the credential
injection filter reads the file when its config is constructed.  A rotated Secret
therefore needs a gateway config reload or pod restart before the new value is
used.

The operator-owned config path should support a reload trigger, such as an
annotation update on the generated ConfigMap when a referenced Secret version
changes.  Exact reload latency is a product decision and depends on gateway
hot-reload behavior.

## Generated config shape

The generated consumer Praxis config should have this structure:

```yaml
filter_chains:
  - name: consumer-chain
    filters:
      - filter: json_body_field
        field: model
        header: X-Model
      - filter: grid_route
        local_site: cluster-east
        model_header: X-Model
        candidates:
          - kind: inference_model
            name: model-z
            site: api-provider
            cluster: gateway-api-provider
            fresh: true
            credential:
              strategy: bearer_token
              secretRef:
                name: api-provider-creds
                namespace: default
                key: token
      - filter: grid_credential_inject
        credentials:
          - name: api-provider-creds
            namespace: default
            key: token
            strategy: bearer_token
            file: /run/secrets/grid-credentials/api-provider-creds/token
      - filter: load_balancer
        clusters:
          - name: gateway-api-provider
            endpoints:
              - api.example.com:443
```

Required properties:

- `grid_route` candidates may include `credential.secretRef`.
- `grid_credential_inject` entries use `file:` sources.
- Inline credential `value:` fields are never generated.
- Static header injection is not part of the production config path.
- Load-balancer clusters are derived from candidate cluster identities and
  gateway topology.

## Security invariants

| Location | Token bytes allowed |
|---|---|
| Grid overlay ConfigMap | No |
| `GridNetwork` / `InferenceProvider` status | No |
| Consumer Praxis ConfigMap | No |
| Route metadata | No |
| Logs and tracing spans | No |
| HTTP error bodies | No |
| Provider credential Secret | Yes |
| Consumer credential Secret | Yes |
| Mounted Secret file | Yes |
| Credential injector memory | Yes |
| Upstream `Authorization` header | Yes |

The generated ConfigMap must be testable with a sentinel-token check: a known
token value must not appear anywhere in rendered YAML.

## Operator deployability gaps

These are the gaps an operator or platform administrator would need answered
before treating operator-generated consumer config as the production runtime
path.

| Gap | Current state | Required resolution |
|---|---|---|
| Provider endpoint topology | `GatewayRef.consumerConfig.clusterEndpoints[]` can provide explicit endpoint and SNI data, but the validation runtime does not yet consume the generated config. | Decide whether this explicit map remains the v1 API or whether topology moves to `GridSite` or a dedicated gateway resource. |
| Consumer Secret lifecycle | The generated config references mounted files; the Secret itself must exist in the consumer cluster. | Decide whether Secrets are pre-provisioned, mirrored in same-cluster deployments, or supplied by External Secrets/Vault/platform automation. |
| Secret rotation and gateway reload | Kubernetes updates mounted Secret files, but the credential injector reads token files when filter config is constructed. | Define reload or rollout semantics when a referenced Secret changes. |
| Gateway config rollout | The operator applies a `ConfigMap`; it does not own consumer `Deployment` rollout or hot reload. | Define whether a gateway operator, deployment owner, or Grid annotation mechanism triggers reloads. |
| RBAC and install profiles | The operator needs permissions to write gateway `ConfigMap`s and may need read/write access to Secrets depending on the chosen lifecycle model. | Provide least-privilege install profiles for same-namespace, cross-namespace, and external-secret-manager deployments. |
| Status and diagnostics | Config rendering errors surface as reconcile errors, but there is no per-gateway consumer-config readiness status. | Add status conditions/events for rendered, missing topology, missing Secret, unsupported strategy, and last applied `ConfigMap`. |
| Gateway image compatibility | The generated config assumes gateway filters such as `grid_route` and `grid_credential_inject` are available. | Pin or publish a compatible gateway image and document version skew behavior. |
| Multi-tenant namespace boundaries | `GatewayRef` names a namespace, and credential refs can name namespaces. | Define allowed namespace relationships and tenant isolation rules before enabling broad cross-namespace writes. |
| Observability and runbooks | Overlay/config render success is not yet exposed as operator-facing metrics or runbook guidance. | Add metrics/events and document how to diagnose stale overlay, missing config, missing Secret, and failed gateway reload. |

## Testing plan

Unit tests should cover:

- config rendering with no credentials;
- config rendering with one credential-bearing candidate;
- de-duplication when multiple candidates share the same Secret reference;
- stable `file:` path derivation;
- absence of inline `value:` fields;
- absence of static header injection;
- token sentinel not present in generated YAML.

Integration tests should cover:

- Secret reference to mounted-file mapping;
- missing Secret and missing key diagnostics;
- unsupported credential strategies;
- config annotation changes for reload triggers.

Kind validation should prove:

- the operator renders the consumer Praxis ConfigMap;
- the ConfigMap contains `file:` sources and no token bytes;
- a credential-bearing provider request succeeds through the consumer gateway;
- a direct request to the protected upstream without credentials fails.

## Open decisions

1. **Same-cluster constraint for v1.** Decide whether managed consumer config
   initially supports only gateways in the same cluster as the operator, or
   whether cross-cluster Secret provisioning is required immediately.
2. **ConfigMap naming.** Decide whether the operator writes a stable
   `praxis-consumer-config` name or a Grid-owned name such as
   `grid-consumer-config-{network}-{gateway}`.
3. **Credential mount base.** Decide whether `/run/secrets/grid-credentials` is
   the default or whether every gateway must set it explicitly.
4. **Unsupported strategies.** Decide whether unsupported credential strategies
   are omitted from injection config, retained but fail closed at request time,
   or block config generation.
5. **Secret synchronization owner.** Decide whether v1 requires pre-existing
   consumer-cluster Secrets or whether the operator mirrors Secrets in
   same-cluster deployments.
6. **Endpoint/TLS topology owner.** Decide whether provider egress endpoint and
   TLS data come from `GridSite`, `GatewayRef.consumerConfig`, or a dedicated
   gateway resource.
7. **Consumer config readiness status.** Decide whether `GridNetwork` should
   report per-gateway render/apply status or whether that belongs on a future
   gateway-specific resource.

## Recommended implementation sequence

1. Add the `GatewayRef.consumerConfig` API as an opt-in no-op by default.
   **Implemented.**
2. Add a pure consumer config renderer with token-invariant tests.
   **Implemented.**
3. Wire `GridNetwork` reconcile to render the consumer ConfigMap only when
   `consumerConfig.enabled` is true. **Implemented.**
4. Extend Kind validation to assert the operator-generated ConfigMap shape.
   **Implemented.**
5. Add provider endpoint/TLS topology to the rendered `load_balancer` clusters.
   **Implemented with explicit `clusterEndpoints[]`.**
6. Move the local validation path to consume the operator-generated ConfigMap.
7. Add Secret rotation/reload behavior after the base path is proven.

Do not implement cross-cluster Secret copying, a new `ConsumerGateway` CRD, or
gateway Deployment ownership in the initial implementation.
