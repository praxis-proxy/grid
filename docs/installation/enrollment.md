# Site Enrollment

A site enrolls with a grid by redeeming a one-time token at the enrollment
service. The service returns a certificate that carries a grid-assigned
identity. Enrollment does not provision the mesh. The GridNetwork carries the
SWIM key and seed peers, as [CRD-driven SWIM
seeds](../architecture/crds.md#crd-driven-swim-seeds) describes.

## Prerequisites

- Kubernetes 1.30 or later for the default install, which records enrollments
  as CertificateSigningRequests behind two validating admission policies; 1.26
  to 1.29 with `enrollment.csrRecord.enabled=false`. Helm 3.12 or later
- etcd encryption at rest, since the CA signing key is stored in a Secret
- An enrollment image with the default `sar` and `bootstrap` features (not a
  `--no-default-features` build)

## 1. Install the enrollment service on the hub

1. Install the chart:

   ```bash
   helm install grid-enrollment ./charts/grid-enrollment \
     --namespace grid-enrollment --create-namespace \
     --set route.host=enrollment.apps.example.com \
     --set db.type=external \
     --set db.external.connectionUrlSecretRef=grid-enrollment-db
   ```

2. On OpenShift, the chart renders a passthrough Route for `route.host`.
   Elsewhere, omit `route.host` and front the Service with your own ingress.
3. For GitOps renders with `helm template` on OpenShift, pass `--api-versions
   route.openshift.io/v1`. Without it, `route.enabled=auto` renders no Route.

A pre-install Job creates the grid CA and the serving certificates for
enrollment and its database. On upgrade it keeps the CA.

| Value | Decide |
| --- | --- |
| `route.host` | Required when a Route renders. A BYO certificate (`serving.existingSecretRef`) must already carry it. |
| `route.enabled` | `auto` (default) renders the Route only when the cluster serves `route.openshift.io/v1`. `true` or `false` forces it. |
| `route.tls.termination` | Keep `passthrough`. The chart rejects `edge`, and rejects `reencrypt` while rotation is on. |
| `db.type` | `builtin` runs Postgres in the chart. `external` reads the URL from the Secret in `db.external.connectionUrlSecretRef`. |
| `enrollment.authz` | `kube` (default) authorizes callers with Kubernetes RBAC on `enrollmenttokens` in the release namespace, so keep that namespace dedicated to enrollment. `local` uses a `name:token` table and an optional separate `name:role[,role]` table. |
| `enrollment.certLifetimeSecs` | Site certificate lifetime. Empty means 180 days. |
| `enrollment.rotation.enabled` | `true` (default). See [Rotation](#5-rotation). |
| `image.digest`, `db.builtin.imageDigest` | Pin the enrollment and builtin Postgres images. |

The chart README and the chart values file cover the rest.

If a `reencrypt` or publicly trusted Route must front enrollment, set
`enrollment.rotation.enabled=false`, pin that Route's CA in the site's
`enrollment.caBundle`, and set `enrollment.gridCaBundle` to the grid CA.

## 2. Invite a site

1. Add the site to the chart's `invites` value, keyed by site name, and upgrade.
   Invites need `enrollment.authz=kube`.

   ```bash
   helm upgrade --install grid-enrollment ./charts/grid-enrollment --namespace grid-enrollment \
     --set invites.east2.network=my-grid --set invites.east2.expiresInSecs=86400
   ```

   `network` defaults to `grid`. `expiresInSecs` allows at most 604800 (seven
   days).

2. A Job mints a token for each new entry into Secret `grid-invite-<siteName>`
   (key `token`) in the release namespace. Existing Secrets are skipped.
3. Copy `grid-invite-<siteName>` and the grid CA bundle (`ca.crt` from Secret
   `grid-ca-bundle`) to the site's operator namespace, by hand or with a policy
   engine such as ACM.

If the service starts slowly, pass `--timeout 10m`. The Job retries for about
four minutes per run, then fails naming every site it did not invite. A serving
certificate you bring must chain to the `ca.bundleSecretName` bundle and carry
`<fullname>.<namespace>.svc`.

Treat invite Secrets as credentials:

- Anyone who can get Secrets in the release namespace can read them.
- Removing an entry or deleting its Secret does not revoke the token. Revoke it
  with `DELETE /v1alpha1/enrollmenttokens/<id>`, using the id in the Secret's
  `grid.praxis.fast/token-id` annotation. The
  `<release>-grid-enrollment-grid-admin` Role grants that.
- `helm uninstall` leaves invite Secrets behind. Delete them by hand.

## 3. Enroll the site

1. Install the grid-operator chart with enrollment on:

   ```bash
   helm install grid-operator ./charts/grid-operator \
     --namespace grid-system \
     --set swim.siteName=east2 \
     --set enrollment.enabled=true \
     --set enrollment.url=https://enrollment.apps.example.com
   ```

2. Confirm the pod reports ready. On the hub itself, `enrollment.url` defaults
   to the in-cluster `grid-enrollment` Service.

Defaults:

| Item | Default |
| --- | --- |
| Site name | `swim.siteName` |
| CA bundle Secret | `grid-ca-bundle` |
| Token Secret | `grid-invite-<siteName>` |
| Identity Secret | `grid-site-identity` (`GRID_ENROLL_IDENTITY_SECRET`) |
| CA Secret | `grid-ca` (`GRID_ENROLL_CA_SECRET`) |

When a GridNetwork exists, the operator writes to the Secrets its
`spec.tls.siteSecretRef` and `caSecretRef` name. All of these Secrets live in
the operator namespace. Enrollment needs no GridNetwork, but until one exists
the operator does no SWIM join and no overlay.

A restart never spends a second token, because the operator skips enrollment
when the identity Secret exists. If you create a GridNetwork afterward that
names other Secrets, the operator refuses to enroll again. Point it at the
enrolled Secrets. The operator retries retryable enrollment failures without an
attempt limit or overall deadline. For `siteIdentity` installs, the startup probe
restarts a pod still enrolling after about 16 minutes by default (96 checks, 10s
apart); adjust `health.startup` to change that window.

## 3a. Read the enrollment record

Under `enrollment.authz=kube` the service records each enrollment as a
CertificateSigningRequest on the hub, under the signer name in
`enrollment.csrRecord.signerName`, by default `grid.praxis.fast/site`. The
service creates it, approves it with reason `InviteRedeemed` and completes it
with the issued certificate, off the request path, so the record never gates
issue. The record is named `<site>-<enrollment id>`.

```bash
kubectl get csr -l grid.praxis.fast/site-name=east2
kubectl get csr east2-9f86d081884c7d659a2feaa0c55ad015 \
  -o jsonpath='{.status.conditions[0].reason}'
```

The API server removes an issued request about an hour after approval, so this
is a one-hour window on recent enrollments. The API server's audit log keeps the
history when its policy covers these writes at `Request` level or above with a
persistent backend; the service's own enrollment record is kept regardless.
Set `enrollment.csrRecord.enabled=false` to keep no Kubernetes record. The chart also
installs two `ValidatingAdmissionPolicies`: only this release's service may file
a request under its signer name, and the service may touch no other signer
name. Run two enrollment services on one cluster with different signer names.
The policies need `admissionregistration.k8s.io/v1`; on an older cluster turn
the record off. The installing principal must hold `approve` and `sign` on the
signer name, or RBAC escalation prevention refuses the record roles.

## 4. Recover a failed enrollment

- **Expired or revoked token** (`site token rejected`): delete
  `grid-invite-<siteName>` on the hub, run `helm upgrade` with the site still in
  `invites`, and copy the new token to the site.
- **Site name held** (`site name already enrolled`): a redeemed token holds its
  name. An enrollment admin runs `DELETE /v1alpha1/enrollments/<siteName>`, then
  you invite the site again with `invites.<siteName>.allowDeletedName=true`, and
  the site enrolls with a new key. That needs `delete` on `enrollments` in group
  `grid.praxis.fast`, which the `enrollment-admin` Role grants to
  `enrollment.enrollmentAdmins.subjects` and to no one by default. Without that
  access, enroll under a new site name.

## 5. Rotation

A site rotates its identity certificate before it expires, with no new token.
Site certificates last 180 days. When one third of the lifetime remains, around
day 120, the operator presents the current certificate to the enrollment service
over mutual TLS. The service returns a certificate for a new key. The grid CA
does not rotate.

Requirements:

1. Set `grid.peerTrust` to `spiffe` or leave it unset. Under `pin` the operator
   does not rotate and logs `rotation disabled`, because peers pin the leaf
   digest and would refuse a new one. Before a pin site's certificate expires,
   re-enroll it as in step 7 and update the digest its peers pin.
2. Keep the enrollment Route on passthrough. A reencrypt Route drops the client
   certificate.
3. Do not put a proxy that presents a grid site certificate in front of
   enrollment.
4. Leave `enrollment.rotation.enabled=true` on the grid-operator chart (default
   whenever an enrollment URL is known). The chart then grants `get` and `patch`
   on the gateway Deployment, because the gateway reads its client certificate
   only at start and the operator rolls it after each rotation.
5. Keep clocks on the enrollment service and its database within five minutes of
   each other.
6. Do not restore a site's identity Secret from backup, and do not manage it
   with GitOps or a policy that enforces its contents. An older copy holds a
   replaced key, so the site freezes at its next rotation.
7. If an identity already expired, delete the site's enrollment and its identity
   Secret, invite it again with `allowDeletedName`, and restart the operator.
   Both steps need `enrollment-admin`.

Watch `grid_site_identity_expiry_timestamp_seconds` and
`grid_site_identity_rotations_total` on the operator. `GridNetwork`
`status.identity.notAfter` and `rotateAfter` show the dates, and
`status.identity.reason` reads `IdentityExpired` when the certificate has
expired.

### Turn rotation off

| Scope | Set | Effect |
| --- | --- | --- |
| Whole grid | `enrollment.rotation.enabled=false` on grid-enrollment | The service answers every rotation with 503 `rotation_disabled`. Enrollment still works. |
| One site | `enrollment.rotation.enabled=false` on its grid-operator | The operator stops rotating and rolling the gateway. The chart drops the Deployment grant. |

Either way, each site keeps its identity until `status.identity.notAfter`. Turn
rotation back on before then, or the site must re-enroll.

### Frozen sites

If a stale key asks to rotate, the service freezes the site and logs `rotation
fork` at warning level. An admin holding both `grid-admin` and
`enrollment-admin` recovers it by deleting the site's enrollment and minting a
token with `allowDeletedName`, and the site re-enrolls with a new key. A deleted enrollment's keys are never enrolled again. To see
why a site cannot rotate, read `GET /v1alpha1/enrollments/{siteName}`. It
returns `state` (`active` or `frozen`), key digests, and `notAfter`, and needs
`get` on `enrollments`.

With `enrollment.authz=local`, every admin in the token table can read
enrollments, and only a line with the `enrollment-admin` role can delete them.
See the chart README for the table format.

After the enrollment database is restored from a backup, sites that rotated
since the snapshot get `identity_refused` (logged on the hub as `record_behind`)
and must re-enroll.

### Recover the hub identity

The bootstrap Job issues the hub's identity. To re-issue it or clear a freeze:

1. Delete the Secret named by `hubSite.identitySecretName` in
   `hubSite.namespace`.
2. Run `helm upgrade`.

Do not use `ca.forceRegenerate` for this. It replaces the grid CA, and every
site must re-enroll.

## 6. Certificate lifetimes and the signing CA

- Enrollment and database serving certificates last 365 days. Bootstrap renews
  them when fewer than 30 days remain, but only during `helm install` and `helm
  upgrade`. Run `helm upgrade` at least every 30 days, or when the enrollment
  pod logs its daily warning that the serving certificate expires within 30
  days.
- The grid CA lasts 10 years. The CA cannot be rotated. `ca.forceRegenerate`
  re-issues every leaf and invalidates every enrolled site's trust anchor.
- The service records the SHA-256 fingerprint of the CA's public key in the
  database on first start and refuses a CA with any other key, at startup and
  on reload, even when every CA Secret was deleted and bootstrap minted a new
  one. A CA certificate renewed on the same key is accepted. The refusal names
  both fingerprints. On upgrade, the first start records whichever CA is
  loaded.
- To start a new grid on purpose, run `helm upgrade` with
  `ca.forceRegenerate=true` and `ca.confirmRotation=<old fingerprint>`, the
  `fingerprint` from the service's `grid CA checked against the database record`
  log line. This hashes the CA's public key; the bootstrap error's certificate
  fingerprint is different. The chart passes it as
  `ENROLLMENT_CONFIRM_CA_ROTATION`. It admits only a replacement of that CA.
  Clear both values on the next upgrade.
- The service reloads the CA from its Secret every minute. A confirmed change
  logs `signing CA changed on disk` at warning level, and an unconfirmed one
  logs `signing CA reload refused` at error level and keeps the current CA.
  Alert on both, since an unplanned change splits trust between sites enrolled
  before and after.
- After a CA change, sites holding certificates from the old CA cannot rotate
  and must re-enroll.

## Debugging

Where to look:

```bash
# Every operator replica; above one replica only the lease holder enrolls and rotates.
kubectl -n grid-system logs -l app.kubernetes.io/name=grid-operator --all-containers --prefix --max-log-requests 10 | grep -i "enroll\|rotation"
kubectl -n grid-enrollment logs deploy/grid-enrollment
kubectl get gridnetwork -o yaml   # status.identity: notAfter, rotateAfter, reason
```

Operator metrics: `grid_site_identity_expiry_timestamp_seconds` and
`grid_site_identity_rotations_total{result}`, where `result` is `rotated`,
`refused`, `failed`, or `expired`.

| Symptom | Check | Fix |
| --- | --- | --- |
| `site token rejected (...)` | The token expired or was revoked. | Delete `grid-invite-<siteName>` on the hub, run `helm upgrade` with the site still in `invites`, and copy the new token. |
| `site name already enrolled (...)` | An earlier attempt spent a token for this name. | An admin with `enrollment-admin` runs `DELETE /v1alpha1/enrollments/<siteName>`, then an admin with both `grid-admin` and `enrollment-admin` invites the site again with `allowDeletedName`. Without that access, enroll under a new site name. |
| `reaching the enrollment service failed after 10 attempts` | `enrollment.url` from the site. | Make it reachable. |
| `TLS to the enrollment service failed, check GRID_ENROLL_CA_FILE` | The CA that issued the enrollment serving certificate. | Set `enrollment.caBundle` to it. |
| `returned CA is not the pinned grid CA` | The attempt spent the token. | Set `enrollment.gridCaBundle` to the grid CA and enroll under a new site name. |
| `possible interception, contact the hub` | The certificate names another site or key. | Tell the hub admin before enrolling again. |
| `rotation disabled: peerTrust pin needs re-enrollment and re-pinning at expiry` | `status.identity.notAfter`. | Expected under pin trust. Re-enroll before expiry and update the digest peers pin. |
| `site identity rotation failed` with `rotation_disabled` (503) | Whether `enrollment.rotation.enabled=false` on grid-enrollment. | Set it back to `true` before `notAfter`. |
| `site identity rotation failed` with `rotation unauthenticated` and `identity_required` (401) | The Route must be passthrough. A reencrypt Route drops the client certificate. | Switch to passthrough. |
| `site identity rotation failed` with `rotation refused` and `identity_refused` (403) | Enrollment log for `rotation fork` or `rotation refused`. `GET /v1alpha1/enrollments/<siteName>` shows `state: frozen`. | Delete the site's enrollment with `enrollment-admin`, invite it again with `allowDeletedName` holding both `grid-admin` and `enrollment-admin`, and the site re-enrolls. For the hub, see Recover the hub identity. |
| `site identity expired and cannot rotate; re-enroll this site with a new site token` | `status.identity.reason` is `IdentityExpired`. | Delete the site's enrollment with `enrollment-admin` and its identity Secret, invite it again with `allowDeletedName` holding both `grid-admin` and `enrollment-admin`, and restart the operator. |
| CA bootstrap Job fails with `Restore Secret grid-ca-key from backup` | The message names why: a missing key Secret, a key for a different CA, or an unparsable distributed copy. | Restore the right `grid-ca-key` and run `helm upgrade`. To start a new grid on purpose, set `ca.forceRegenerate=true` and `ca.confirmRotation` to the old public-key `fingerprint` from the service's `grid CA checked against the database record` log line, not the bootstrap error's certificate fingerprint. Every site must re-enroll. |
| Enrollment pod exits with `DB_CONNECTION_URL must set sslmode=verify-full` | The external database URL uses a lesser mode, or none. A shared database such as MaaS's may. | Set `sslmode=verify-full` and `sslrootcert` in the URL Secret, mounting the database's CA with `db.external.caConfigMapName`, then restart the pod. |
| Enrollment pod exits with `the database records grid CA <old>, but the loaded CA is <new>` | A CA other than the one the database records was minted or restored. | Restore the CA with fingerprint `<old>` and run `helm upgrade`. To start a new grid on purpose, set `ca.confirmRotation=<old>`, and every site must re-enroll. |
| CA bootstrap Job fails with `built without --features bootstrap` | The image build. | Use a default build, which includes `sar` and `bootstrap`. |
| `route.host is required` | A passthrough Route renders without a host. | Set `route.host` to `<name>.apps.<cluster-domain>`, or set `route.enabled=false`. Under an umbrella chart, prefix both with the subchart name. |
| `TokenReview` or `SubjectAccessReview` calls fail | `enrollment.authz=kube` needs the `sar` feature. | Use a default image and set `enrollment.serviceAccount.create=true`. |

## Internals

- The operator dry-runs its Secret writes before it sends the token, and stores
  the identity only when the returned CA matches the pinned grid CA and the
  certificate names the site.
- Bootstrap signs a seed with the CA key into the `grid-reserved-seeds` Secret,
  and the service registers the hub's key from it. Keep write access to that
  Secret as narrow as access to `grid-ca-key`.
- The service keeps each site's current and previous key, so a rotation whose
  answer was lost retries safely. Any other valid key is a fork.
- The service signs only EC keys on P-256 or P-384, at enrollment and at
  rotation, and refuses any other key with 400 `invalid_csr`. The operator
  generates P-256 keys.
- Enrollment and invites are specified in the repository's `api` directory.
