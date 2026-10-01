# Site Enrollment

A site enrolls with a grid by redeeming a one-time token at the enrollment service, which returns a certificate carrying a grid-assigned identity. Enrollment does not provision the mesh. The GridNetwork carries the SWIM key and seed peers, as [CRD-driven SWIM seeds](../architecture/crds.md#crd-driven-swim-seeds) describes.

## Prerequisites

- Kubernetes 1.26 or later, Helm 3.12 or later
- etcd encryption at rest, since the CA signing key is stored in a Secret
- An enrollment image with the default `sar` and `bootstrap` features (not a `--no-default-features` build)

## Install

```bash
helm install grid-enrollment ./charts/grid-enrollment \
  --namespace grid --create-namespace \
  --set route.host=enrollment.apps.example.com \
  --set db.type=external \
  --set db.external.connectionUrlSecretRef=grid-enrollment-db
```

On OpenShift the chart renders a passthrough Route for `route.host`, so remote sites can reach enrollment. `route.enabled` defaults to `auto`, which renders the Route only when the cluster serves `route.openshift.io/v1`. `true` or `false` forces it. Elsewhere, omit `route.host` and front the Service with your own ingress. For GitOps renders with `helm template`, pass `--api-versions route.openshift.io/v1` on OpenShift, or `auto` renders no Route.

A pre-install Job runs `enrollment bootstrap` to create the grid CA and the serving certificates for enrollment and its database. It is idempotent: on upgrade it keeps the CA and re-issues a serving certificate when a requested name, such as a new `route.host`, is missing, or when fewer than 30 days remain. `ca.forceRegenerate` replaces the CA and invalidates every certificate it signed.

## Configure

| Value | Decide |
|---|---|
| `route.host` | Required when a Route renders. Added to the issued serving certificate. A BYO certificate (`serving.existingSecretRef`) must already carry it. |
| `route.tls.termination` | Keep `passthrough`. `reencrypt` terminates at the router and breaks the site's grid-CA pin. `edge` is rejected, since enrollment serves TLS only. |
| `db.type` | `builtin` runs Postgres in the chart. `external` reads the connection URL from the Secret in `db.external.connectionUrlSecretRef`. |
| `enrollment.authz` | `kube` (default) authorizes callers with Kubernetes RBAC on `enrollmenttokens` in the release namespace, so keep that namespace dedicated to enrollment. `local` uses a grid-admin token table. |
| `image.repository`, `image.tag`, `image.digest` | The enrollment image. The tag defaults to the chart's `appVersion`; `image.digest` pins it. |
| `db.builtin.image`, `db.builtin.imageDigest` | The builtin Postgres image, a pinned tag by default; `imageDigest` pins it. |

The chart README and `values.yaml` cover the remaining values.

## Verify

Mint a token with the grid CA bundle:

Under `kube`, the chart ships the grid-admin Role, `<release>-grid-enrollment-grid-admin`, with `create` and `delete` on `enrollmenttokens`. Bind it with `enrollment.gridAdmins.subjects`, or set `enrollment.gridAdmins.serviceAccount.create=true`. The bearer must be bound to `enrollment.tokenAudience` (default `grid-enrollment`); a general API token is refused. Mint a short-lived one:

```bash
kubectl -n grid get secret grid-ca-bundle -o jsonpath='{.data.ca\.crt}' | base64 -d > grid-ca-bundle.crt
GRID_ADMIN_SA=grid-admin  # a ServiceAccount bound to the grid-admin Role
GRID_ADMIN_TOKEN=$(kubectl -n grid create token "$GRID_ADMIN_SA" --audience grid-enrollment --duration 10m)
# printf is a builtin, so the token stays out of process arguments.
(umask 077 && printf 'Authorization: Bearer %s\n' "$GRID_ADMIN_TOKEN" > admin.hdr)

curl -s -X POST https://enrollment.apps.example.com/v1alpha1/enrollmenttokens \
  --cacert grid-ca-bundle.crt \
  -H @admin.hdr \
  -H "Content-Type: application/json" \
  -d '{"siteName": "east2", "gridNetworkRef": "my-grid"}'
```

The response returns `tokenId`, a single-use `token`, and `expiresAt`. Give `token` and `grid-ca-bundle.crt` to the site out of band. Revoke an unused token with `DELETE /v1alpha1/enrollmenttokens/$TOKEN_ID`.

On the site, create a key and CSR, then redeem the token:

```bash
openssl ecparam -genkey -name prime256v1 -noout -out site.key
openssl req -new -key site.key -subj "/CN=east2" -out site.csr
read -rs SITE_TOKEN  # paste the token from the grid admin
(umask 077 && printf 'Authorization: Bearer %s\n' "$SITE_TOKEN" > site.hdr)

jq -n --rawfile csr site.csr '{csr: $csr}' \
  | curl -s -X POST https://enrollment.apps.example.com/v1alpha1/enrollments \
      --cacert grid-ca-bundle.crt \
      -H @site.hdr \
      -H "Content-Type: application/json" \
      -d @-
```

The response returns `certificate`, issued for the site name the token pinned (SANs in the CSR are ignored), `caCertificate`, and `spiffeId`, the identity the site presents to its peers.

## Monitoring the signing CA

The enrollment service reloads the grid CA from its Secret every minute. When
the CA changes, it logs `signing CA changed on disk` at warning level with the
old and new fingerprints, and signs every later site certificate with the new
CA. Alert on that message: an unplanned CA change splits trust between sites
enrolled before and after it.

## Certificate lifetimes

Bootstrap issues the enrollment and DB serving certificates for 365 days and
renews each one when fewer than 30 days remain, but it runs only on
`helm install` and `helm upgrade`, so renewal needs an upgrade inside that
window. Run `helm upgrade` at least every 30 days, or when the enrollment pod
logs its daily warning that the serving certificate is inside the window.

The grid CA lasts 10 years. There is no CA rotation path yet: regenerating it
(`ca.forceRegenerate`) re-issues every leaf and invalidates every enrolled
site's trust anchor.

## Troubleshooting

- **`route.host is required`**: a passthrough Route is rendering without a host. Set `route.host` to `<name>.apps.<cluster-domain>`, or set `route.enabled=false`. Under an umbrella chart, prefix both with the subchart name.
- **CA bootstrap Job fails with `built without --features bootstrap`**: the image was built with `--no-default-features`. Use a default build, which includes `sar` and `bootstrap`.
- **`TokenReview` or `SubjectAccessReview` calls fail**: `enrollment.authz=kube` needs the `sar` feature and `enrollment.serviceAccount.create=true`, which binds the pod to `system:auth-delegator`.
