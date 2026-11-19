# GridNetwork

Defines network membership, routing policy, gateways, and local TLS references.

## Ownership

| Responsibility | Component |
| --- | --- |
| Created by | Helm client or GitOps controller applying a chart/manifest, or an administrator API client |
| Spec owner | Administrator |
| Spec writer | Helm/GitOps or administrator API client |
| Status writer | Grid operator network controller and signal-reporting tasks |
| Direct consumers | Grid operator network, site, and provider controllers, identity and signal tasks |
| Derived consumers | Praxis gateway reads generated routing configuration |

## Lifecycle

- The operator chart renders GridNetwork when grid.id is set. Helm/GitOps
  creates the object. The operator controller does not create it. The current
  deployment supports one network per cluster.
- The operator maintains status and its withdrawal finalizer. It does not
  populate missing spec.tls references when enrollment creates Secrets.
- With both TLS Secret references configured and neither Secret present, the
  operator can generate a self-signed CA and site identity. It preserves an
  existing CA. Enrollment manages certificates through a separate lifecycle.

## References

[Fields] | [Schema] | [Controller] | [Chart] | [Enrollment]

[Fields]: ../crds.md#gridnetwork
[Schema]: ../../../crates/operator/src/crd/grid_network.rs
[Controller]: ../../../crates/operator/src/controller/grid_network.rs
[Enrollment]: ../enrollment.md
[Chart]: ../../../charts/grid-operator/templates/gridnetwork.yaml
