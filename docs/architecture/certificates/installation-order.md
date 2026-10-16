# Certificate prerequisites and installation order

GridNetwork contains certificate references, not certificate contents. Its
creator and the certificate writer must use the same Secret names and namespace.

## Writers

| Component | Creates or updates |
| --- | --- |
| Enrollment bootstrap hook | Grid CA and its hub copy, serving and database TLS, optional hub identity, SWIM key, signed reserved-seed Secrets, and the grid ID ConfigMap. In external mode it persists only the grid ID |
| cert-manager | Generated Grid CA and per-site identities in external mode |
| Hub delivery Policies | Member site identities and public CA in external mode |
| Helm client or GitOps controller | GridNetwork from the grid-operator or grid-site chart values |
| Joining operator enrollment task | Site identity and local public CA Secret |
| Running Grid operator controllers | GridNetwork status and generated routing state |

The grid-operator chart renders GridNetwork when grid.id is set. Its TLS
references come from enrollment.identitySecretName, enrollment.caSecretName, and
the release namespace. The grid-site chart takes its references from
gridNetwork.tls. Either chart can populate those references before the Secrets
exist.

The operator runs enabled enrollment before starting its network controllers.
Enrollment reads the existing network's Secret references or uses configured
default names. It does not create GridNetwork or attach missing references
later.

## Ordering constraints

- An enrollment service used by the hook must already be reachable. A hook
  cannot enroll through a service that its own release has not started yet.
- Helm renders before running hooks. Shared names work in one pass. A field that
  depends on generated output needs another render or a separate object writer.
- CRDs in the grid-operator chart are templates. Establish them before applying
  GridNetwork. The chart documents a first install followed by an upgrade with
  grid.id.
- An init container cannot delay a separately applied GridNetwork.
- GitOps must run the same hooks. Argo CD maps Helm pre-install and pre-upgrade
  hooks to PreSync. Rendering templates alone does not run the bootstrap Job.

## References

[Network template] | [Site chart network] | [Chart inputs] |
[Enrollment target selection] | [Operator startup] | [Bootstrap hook] |
[Helm hooks] | [Argo CD hooks]

[Network template]: ../../../charts/grid-operator/templates/gridnetwork.yaml
[Site chart network]: ../../../charts/grid-site/templates/gridnetwork.yaml
[Chart inputs]: ../../../charts/grid-operator/values.yaml
[Enrollment target selection]: ../../../crates/operator/src/enroll.rs
[Operator startup]: ../../../crates/operator/src/main.rs
[Bootstrap hook]: ../../../charts/grid-enrollment/templates/certs/ca-bootstrap-job.yaml
[Helm hooks]: https://helm.sh/docs/topics/charts_hooks/
[Argo CD hooks]: https://argo-cd.readthedocs.io/en/stable/user-guide/helm/#helm-hooks
