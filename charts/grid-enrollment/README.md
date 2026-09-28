# Grid Enrollment Helm Chart

Helm chart for the grid enrollment service: the enrollment authority for a grid,
its self-provisioned Grid CA, and a Postgres store. A zero-config install stands
up a site ready to enroll.

## Prerequisites

- Kubernetes >= 1.26
- Helm >= 3.12
- An etcd encryption-at-rest provider on the cluster before holding the CA key in a Secret (see Security below)

## Install

Batteries included, no values required:

```bash
helm install grid charts/grid-enrollment --namespace grid-enroll --create-namespace
```

The default install generates the Grid CA and the endpoint serving cert through a
pre-install hook, deploys Postgres, and runs the service over TLS. Under
`enrollment.authz=local` it also generates a grid-admin token. `helm install
--dry-run` and the NOTES output show the endpoint and the trust anchor.

## Topology

One install is one enrollment authority with one Grid CA. Sites are joining
members that enroll against it, each getting a unique SPIFFE identity signed by
the shared Grid CA. Peers trust each other by CA chain and SPIFFE SAN. Do not
install the chart once per site. That creates independent CAs.

## CA and key separation

The bootstrap writes three Secrets:

- `grid-ca-key` (tls.crt, tls.key): the CA signing key. Only the enrollment pod mounts it.
- `grid-ca-bundle` (ca.crt): the public trust anchor for the gateway and peers. Never the key.
- `enrollment-serving-tls` (tls.crt, tls.key): the endpoint's grid-CA-signed serving cert.

Generation is idempotent and runs only when the CA is absent. `ca.forceRegenerate`
replaces the CA in place, which invalidates every certificate it has issued.

## Bring your own (overrides)

Each override takes a Secret reference, so no key material is inlined in values.

| Value | Default | Override |
|-------|---------|----------|
| `ca.provided.keySecretRef` | generate | pre-created CA Secret (tls.crt, tls.key) |
| `serving.existingSecretRef` | issued from the CA | pre-created serving Secret |
| `db.type` | `builtin` Postgres | `external` + `db.external.connectionUrlSecretRef` |
| `enrollment.authz` | `kube` (SAR, needs the sar-feature image) | `local` (standalone grid-admin token table) |
| `enrollment.gridAdminTokens.existingSecretRef` | generated (local authz) | pre-created token Secret (name:token lines) |

## Limitations

The builtin Postgres serves TLS with a cert the bootstrap Job issues from the
generated CA, and the service refuses a plaintext DB. A provided CA
(`ca.method=provided`) does not run the bootstrap Job, so it issues no DB cert.
Provided-CA installs must therefore use an external DB (`db.type=external`) whose
URL sets `sslmode` (verify-full for FIPS).

## Security

This Helm-Secret path is the dev and interim posture. Key separation, secretRef
only overrides, and generate-if-absent idempotence hold even here. A Kubernetes
Secret is base64, not encrypted, unless the cluster runs an etcd encryption
provider. Enable it before holding the CA key this way. For GA the CA signing key
belongs in a non-exportable KMS or HSM key rather than a Secret.

## Dependency

The bootstrap Job runs `enrollment bootstrap` from the enrollment image, so a
runtime install depends on that subcommand (the image's `bootstrap` feature) and,
for the default `authz=kube`, the `sar` feature.
