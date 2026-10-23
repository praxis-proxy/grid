# InferenceProvider

Advertises model names, backend endpoints, credentials, and load signals.

## Ownership

| Responsibility | Component |
| --- | --- |
| Created by | Helm/GitOps, administrator API client, or an external integrating controller |
| Spec owner | The administrator or controller publishing the provider |
| Spec writer | The publishing Helm/GitOps client or external controller |
| Status writer | Grid operator provider, readiness, and model-discovery tasks |
| Direct consumers | Grid operator validation, metrics collection, discovery, and routing |
| Derived consumers | Praxis gateway reads generated model routes and candidates |

## Lifecycle

- For a generated provider, edit the model-serving resource that produces it.
  For a manually declared provider, edit its Helm or GitOps configuration.
- Grid reconciles provider status and routing outputs. The publishing controller
  or administrator manages the spec and inference workload.
- spec.models declares routing names. Model discovery holds observed models in
  memory, writes only status.modelDiscoveryError, and never changes spec.models.

## References

[Fields] | [Schema] | [Controller] | [Routing]

[Fields]: ../crds.md#inferenceprovider
[Schema]: ../../../crates/operator/src/crd/inference_provider.rs
[Controller]: ../../../crates/operator/src/controller/inference_provider.rs
[Routing]: ../routing.md
