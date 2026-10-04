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
| `route.tls.termination` | Keep `passthrough`, the only mode that keeps the site token end to end. `reencrypt` terminates at the router, which then sees the bearer token and the CSR. When the Route CA is not the grid CA, also set `enrollment.gridCaBundle` to the grid CA. The chart rejects `edge`, since enrollment serves TLS only. |
| `db.type` | `builtin` runs Postgres in the chart. `external` reads the connection URL from the Secret in `db.external.connectionUrlSecretRef`. |
| `enrollment.authz` | `kube` (default) authorizes callers with Kubernetes RBAC on `enrollmenttokens` in the release namespace, so keep that namespace dedicated to enrollment. `local` uses a grid-admin token table. |
| `image.repository`, `image.tag`, `image.digest` | The enrollment image. The tag defaults to the chart's `appVersion`; `image.digest` pins it. |
| `db.builtin.image`, `db.builtin.imageDigest` | The builtin Postgres image, a pinned tag by default; `imageDigest` pins it. |

The chart README and `values.yaml` cover the remaining values.

## Enroll a site

The hub mints a one-time token for each site, and the site's grid-operator redeems it on startup. Clients that do not run the grid-operator chart enroll through the enrollment API, specified in the repository's `api` directory.

### Invite a site on the hub

Add the site to the enrollment chart's `invites` value, keyed by site name, and run `helm upgrade`. Invites need `enrollment.authz=kube`. `network` defaults to `grid`, and `expiresInSecs` allows at most 604800 (seven days).

```bash
helm upgrade --install grid-enrollment ./charts/grid-enrollment --namespace grid-enrollment \
  --set invites.east2.network=my-grid --set invites.east2.expiresInSecs=86400
```

After each install or upgrade, a Job mints a token for each entry into Secret `grid-invite-<siteName>` (key `token`) in the release namespace. The Job skips entries whose Secret already exists, so an upgrade mints only for new sites. The Job retries an unreachable service for about four minutes per run, not per site, then fails naming every site it did not invite. If the service may start slowly, pass `--timeout 10m`, since connect timeouts can stretch that past Helm's default five-minute hook timeout. Before Helm 3.19, a failed invite run leaves its hook RBAC in place until the next run.

The Job pins the service with the `ca.bundleSecretName` Secret. A serving certificate you bring (`serving.existingSecretRef`) must chain to that bundle and carry `<fullname>.<namespace>.svc`.

Deliver `grid-invite-<siteName>` and the grid CA bundle (`ca.crt` from Secret `grid-ca-bundle`) to the site's operator namespace, by hand or with a policy engine such as ACM.

Treat invite Secrets as credentials:

- Anyone who can get Secrets in the release namespace can read them, the same users who can read the CA signing key.
- Removing an entry from `invites` or deleting its Secret does not revoke the token. Revoke it as a grid-admin with `DELETE /v1alpha1/enrollmenttokens/<id>`, using the id in the Secret's `grid.praxis.fast/token-id` annotation. The chart's `<release>-grid-enrollment-grid-admin` Role grants that.
- `helm uninstall` leaves invite Secrets behind. Delete them by hand.

### Enroll on the site

Enable enrollment in the grid-operator chart:

```bash
helm install grid-operator ./charts/grid-operator \
  --namespace grid-system \
  --set swim.siteName=east2 \
  --set enrollment.enabled=true \
  --set enrollment.url=https://enrollment.apps.example.com
```

The site name follows `swim.siteName`, the CA bundle defaults to Secret `grid-ca-bundle`, and the token to Secret `grid-invite-<siteName>`. On the hub itself, `enrollment.url` defaults to the in-cluster `grid-enrollment` Service.

Enrollment needs no GridNetwork, because the token pins the grid. The operator writes to the Secrets a GridNetwork's `spec.tls.siteSecretRef` and `caSecretRef` name when one exists, and otherwise to `GRID_ENROLL_IDENTITY_SECRET` (default `grid-site-identity`) and `GRID_ENROLL_CA_SECRET` (default `grid-ca`), the names the grid-site chart uses. Both must be in the operator namespace. Until a GridNetwork exists, the operator idles the grid-dependent work: no SWIM join and no overlay. When the identity Secret is absent at startup, the operator generates a key, redeems the token, and writes the grid CA (`ca.crt`) and the site identity (`tls.crt`, `tls.key`). The pod reports ready after enrollment finishes. The operator:

- Skips enrollment when the `siteSecretRef` Secret exists, so a restart never spends a token.
- Refuses to enroll again when a GridNetwork created after enrollment names other Secrets than the identity it already wrote, rather than spending the token twice. Point the GridNetwork at the enrolled Secrets.
- Pins TLS to `enrollment.caBundle`.
- Refuses a token Secret whose `grid.praxis.fast/site` label names another site.
- Dry-runs both Secret writes and checks any existing CA Secret before it sends the token, so missing RBAC, an admission refusal, or a different CA fails without spending it.
- Stores the identity only when the returned CA matches the pinned grid CA and the certificate names the site and carries the operator's key.

If a `reencrypt` or publicly trusted Route fronts enrollment, pin that Route's CA in `enrollment.caBundle` and set `enrollment.gridCaBundle` to the grid CA.

The operator retries connect failures for about five minutes and gives up after 15 minutes, then exits so Kubernetes restarts it. It never resends a request the hub may have received.

### Recovery

- An expired or revoked token fails at once with `site token rejected`. Delete `grid-invite-<siteName>` on the hub, run `helm upgrade` with the site still in `invites`, and deliver the new token to the site.
- Known limit: a redeemed token holds its site name, and the hub cannot release a name yet. A site that spent its token without storing an identity enrolls again only under a new site name or after the enrollment database is reset. Reinstalling the chart does not reset an external database.

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

- **Operator logs `site token rejected`**: Delete `grid-invite-<siteName>` on the hub, run `helm upgrade` with the site still in `invites`, and deliver the new token to the site.
- **Operator logs `site name already enrolled`**: an earlier attempt spent a token for this name. Enroll under a new site name, as the known limit in Recovery describes.
- **Operator logs `reaching the enrollment service failed after 10 attempts`**: make `enrollment.url` reachable from the site.
- **Operator logs `TLS to the enrollment service failed`**: set `enrollment.caBundle` to the CA that issued the enrollment serving certificate.
- **Operator logs `returned CA is not the pinned grid CA`**: set `enrollment.gridCaBundle` to the grid CA. The attempt spent the token, so enroll under a new site name.
- **Operator logs `possible interception, contact the hub`**: the certificate names another site or key. Tell the hub admin before enrolling again.
- **`route.host is required`**: a passthrough Route is rendering without a host. Set `route.host` to `<name>.apps.<cluster-domain>`, or set `route.enabled=false`. Under an umbrella chart, prefix both with the subchart name.
- **CA bootstrap Job fails with `built without --features bootstrap`**: the image was built with `--no-default-features`. Use a default build, which includes `sar` and `bootstrap`.
- **`TokenReview` or `SubjectAccessReview` calls fail**: `enrollment.authz=kube` needs the `sar` feature and `enrollment.serviceAccount.create=true`, which binds the pod to `system:auth-delegator`.
