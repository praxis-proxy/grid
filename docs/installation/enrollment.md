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
`enrollment.rotation.enabled=false`, pin the intermediate that issued that Route's
certificate, or the certificate itself, never a public root, in the site's
`site.signerEndpoint.caBundleRef`, and set `site.signerEndpoint.gridCaBundleRef` to
the grid CA, through the site's `enrollment.caBundle` and `enrollment.gridCaBundle`.

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

The site enrolls from the operator config: the ConfigMap `grid-operator-config` in the
operator namespace, key `config.yaml`. Its `site` section names the site, the signer it
enrolls and renews through, and the Secret holding the invite. The grid-operator chart
renders it from the `enrollment` values. The operator reads it through the API and
watches it, so an edit applies without a restart.

```yaml
site:
  name: east2                      # the site name the invite pins, and its GridSite's
  signerEndpoint:
    url: https://enrollment.apps.example.com
    caBundleRef:
      kind: Secret
      name: grid-ca-bundle         # key ca.crt by default
  inviteSecretRef:
    name: grid-invite-east2        # key token by default
```

| Field | Meaning |
| --- | --- |
| `site.name` | The site name. Required. The invite label and the issued certificate must match it, and the GridSite of this name is this cluster's own. |
| `site.signerEndpoint.url` | The signer's base URL: https, a host with an optional numeric port, without credentials, query or fragment, and not a host under `grid.internal`, where site certificates live. Editable: renewal reads it each round. |
| `site.signerEndpoint.caBundleRef` | `kind` (`Secret`, the only kind, required), `name`, `key`. Pins the signer's serving certificate to enroll and, by default, the grid CA it returns. Renewal trusts the grid CA the site holds together with this bundle. Required with `inviteSecretRef`. Pin the issuing intermediate or the signer's serving certificate, never a public root. The object must carry the label `grid.praxis.fast/signer-bundle=true`. |
| `site.signerEndpoint.gridCaBundleRef` | The grid CA the returned CA must match: a bootstrap override for a signer that cannot serve its own bundle, such as one behind a certificate from another CA. Carries the same label. |
| `site.signerEndpoint.rotation` | `Enabled`, the default, or `Disabled`: whether this operator renews through the signer. See [Rotation](#5-rotation). |
| `site.inviteSecretRef` | `name`, `key`. The token is an opaque credential the signer issued. The operator presents it and never parses it, and reads it once, while the site has no identity. The Secret must carry the label `grid.praxis.fast/site=<site.name>`. |

The document never holds a credential: it names Secrets. An unknown key inside the `site`
section refuses the whole document, so a typo never passes as an unset field. An unknown
top-level section is ignored with an `OperatorConfigSectionIgnored` Warning Event, so an
operator older than a new section keeps working. An optional `version: 1` names the
document, and any other version is refused. The operator
config decides where the invite token goes and which CA the site trusts, so write access
to it is operator-level access: grant it only to grid admins. `GRID_SIGNER_HOSTS`, the
chart's `enrollment.signerHosts`, fixes the signer hosts the config may name, outside the
config, and defaults to the host of `enrollment.url`. The two labels keep it from
pinning a bundle that happens to be in the namespace, such as an injected service CA, or
from sending an unrelated Secret as a token. Label a bundle or an invite only when it is
one.

```bash
kubectl -n grid-system label secret grid-ca-bundle grid.praxis.fast/signer-bundle=true
kubectl -n grid-system label secret grid-invite-east2 grid.praxis.fast/site=east2
```

1. Install the grid-operator chart with enrollment on. It renders the operator config,
   and sets the SWIM Service, metrics TLS and namespace guard an enrolling site needs:

   ```bash
   helm install grid-operator ./charts/grid-operator \
     --namespace grid-system \
     --set swim.siteName=east2 \
     --set enrollment.enabled=true \
     --set enrollment.url=https://enrollment.apps.example.com
   ```

   Off the hub `enrollment.url` is required, and the render fails without it. On the hub
   itself, set `enrollment.hub=true` instead: the URL defaults to the in-cluster Service
   `enrollment.hubService` (default `grid-enrollment`, the grid-enrollment release's full
   name and the name its serving certificate carries) on `enrollment.hubPort` (default
   `8443`) in `rbac.enrollmentNamespace`. Both must match the grid-enrollment release,
   whose Service port derives from its `listenAddr`.

2. Create the GridNetwork with the grid-site chart or by hand, with `spec.gridId` naming
   the grid, as the grid-site chart's `gridNetwork.gridId` and the grid-operator chart's
   `grid.id` do. Beside a site section the operator mints no ID, so an empty
   `spec.gridId` holds the grid runtime and `Accepted` reads `False` with `GridIdUnset`.
   The operator sends nothing until the GridNetwork exists, and records an
   `AwaitingGridNetwork` Event on the ConfigMap once meanwhile. The site's GridSite,
   named `site.name`, is declared like any peer, with the grid-site chart, GitOps or by
   hand; when nothing declares one the operator creates it from the identity. Enrollment
   does not wait for it.
3. Watch the GridNetwork's `Enrolled` condition:

   ```bash
   kubectl get gridnetwork grid -o jsonpath='{.status.conditions[?(@.type=="Enrolled")]}'
   ```

| Item | Default |
| --- | --- |
| Site name | `swim.siteName` |
| CA bundle Secret | `grid-ca-bundle` |
| Token Secret | `grid-invite-<siteName>` |
| Operator config | `grid-operator-config` (`operatorConfig.name`) |

The operator enrolls while it has no identity: at startup, every 15 seconds while it
waits for one, and at once on an edit to the operator config. It writes the identity to
`grid-site-identity` and the grid CA to `grid-ca` in its namespace, or to the Secrets a
GridNetwork's `spec.tls.siteSecretRef` and `caSecretRef` name. An operator that enrolls
while waiting exits 0 once and restarts under the identity. Enrollment waits for the
GridNetwork, where it records its result. The grid-site chart renders one before
enrollment, and `spec.gridId` names the grid, set once: a change is refused, and
`status.gridId` mirrors it. The operator mints an ID only while `spec.gridId` is empty
and the operator config names no site; beside a site section an empty `spec.gridId`
holds the grid runtime and `Accepted` reads `False` with `GridIdUnset`. Until a
GridNetwork exists nothing is sent, and the operator logs why and records an
`AwaitingGridNetwork` Event on the operator config, once.

This site's conditions live on the GridNetwork, and they are the contract. Its GridSite
carries membership only, each cluster's view of one site: `phase` and `Connected` follow
the gateway probe, as on a peer, and say nothing about the identity. Each condition type
has one writer. `Unknown` means only that a condition has not been
evaluated yet. A wait an admin must end is `False`.

| `Accepted` | Reason | Meaning |
| --- | --- | --- |
| `True` | `Accepted` | The operator config's `site` section applies to this site, the one the identity names. With no config, or no `site` section, the message reads `no operator config, enrollment off` or `the operator config names no site, enrollment off`. |
| `False` | `OperatorConfigInvalid` | The operator config was refused whole: it does not parse, names a version other than `1`, carries an unknown key inside a section, or a field is invalid. The message names the field. Nothing in it is acted on, so nothing enrolls or renews. |
| `False` | `SiteNameMismatch` | `site.name` differs from the site this cluster's identity names. Nothing renews through the config until they agree. |
| `False` | `RotationUnsupportedUnderPinTrust` | `site.signerEndpoint.rotation` is `Enabled`, the default, but the GridNetwork declares `pin` peer trust, which does not renew. Renewal is held back and the rest of the config still acts. Set `rotation: Disabled`, or `peerTrust.mode: spiffe`. |
| `False` | `GridIdUnset` | The GridNetwork has no `spec.gridId` while the operator config names a site, so no ID is minted and the grid runtime waits. Set `spec.gridId`, as the chart's `grid.id` does. The message names the GridNetwork. |

| `ResolvedRefs` | Reason | Meaning |
| --- | --- | --- |
| `True` | `ResolvedRefs` | Every object the `site` section names resolves. |
| `False` | `RefNotFound` | The invite or a bundle the `site` section names does not exist, or a bundle lacks `grid.praxis.fast/signer-bundle=true`. |

| `Progressing` | Reason | Meaning |
| --- | --- | --- |
| `True` | `Enrolling` | The first request is out. A step retries a failure within its budget, about one poll, then returns, so an edit to the config is seen. |
| `True` | `Retrying` | `Enrolled` holds a reason the next poll retries with the same inputs: `SignerLapsed`, `SignerUnreachable`, `InviteMaybeSpent`, `IdentityNotStored` or `StoreUnavailable`; `InviteRefusedAfterLostAnswer` is held, not retried. It holds across polls, so a resend does not move `lastTransitionTime`. |
| `False` | `AsExpected` | Nothing is in flight. `Progressing` follows `Enrolled` alone. |

| `Enrolled` | Reason | Meaning |
| --- | --- | --- |
| `True` | `Enrolled` | The signer issued this site's identity, or the identity Secret already existed and this process did not enroll it, so the invite is never read. It stays once written. For an identity written outside the signer the message reads `identity provisioned outside the signer, renewed by cert-manager Certificate <name>` when the Secret names one, else `identity provisioned outside the signer; nothing is known to renew it`. |
| `Unknown` | `Enrolling` | The first request is out and no answer has returned. |
| `False` | `EnrollmentOff` | No identity, and the config names no site to enroll: nothing is sent. Name the site and its invite in the operator config. |
| `False` | `IdentityMissing` | An identity this process held is gone. Restore it, or name `site.inviteSecretRef` to enroll again. |
| `False` | `AwaitingInvite` | No `site.inviteSecretRef` yet, or no `site.signerEndpoint` to send it to. |
| `False` | `Misconfigured` | A setting cannot work, such as a missing `caBundleRef`, a bundle with no certificate, or a Secret enrollment would both write and read, such as a grid CA Secret named `<identity Secret>-pending`. The message names the field or the Secret. |
| `False` | `SignerUntrusted` | The signer's serving certificate does not chain to `caBundleRef`. Nothing is sent. Pin the issuing intermediate or the signer's serving certificate, never a public root. |
| `False` | `SignerLapsed` | The signer's serving certificate is expired or not yet valid. Nothing is sent, and the next poll retries: the signer must rotate its serving certificate. |
| `False` | `SignerUnreachable` | The signer could not be reached within the step, so the request never left. The next poll retries, so a corrected `site.signerEndpoint.url` takes effect without a restart. |
| `False` | `NameMismatch` | The invite Secret's `grid.praxis.fast/site` label is missing or names another site. Nothing is sent. |
| `False` | `InviteInvalid` | The signer rejected the token: expired, revoked or spent. |
| `False` | `NameTaken`, `NameDeleted` | The name is enrolled, or was deleted and the invite does not re-admit it. |
| `False` | `Refused` | The signer refused the request. |
| `False` | `InviteMaybeSpent` | The request was sent and no usable answer came back, or the signer answered 408. The next poll sends the same request again. A token refused after such a request reads `InviteRefusedAfterLostAnswer` instead. |
| `False` | `InviteRefusedAfterLostAnswer` | The token was refused after an earlier request got no answer, which may have spent it. Not resent until the invite or the config changes, and a restart resends once; a new invite may be needed. |
| `False` | `PendingSecretForeign` | A Secret named `<identity Secret>-pending` exists that enrollment did not write, so nothing is sent. Delete it: enrollment keeps its request there. A request kept there for another site name, grid CA or invite is discarded and a new one is made. |
| `False` | `InvalidResponse` | The returned certificate failed the pin, name or key checks. The token is spent. |
| `False` | `IdentityNotStored` | The certificate was issued but could not be written. The token is spent. |

| `Available` | Reason | Meaning |
| --- | --- | --- |
| `True` | `Available` | `Accepted`, `ResolvedRefs`, `Enrolled` and `IdentityValid` are all `True`. |
| `False` or `Unknown` | the first failing condition's | The roll-up copies the status, reason and message of the first of those that is not `True`, in that order. `RotationOnSchedule` does not gate it. It raises `Degraded`. |

| `Degraded` | Reason | Meaning |
| --- | --- | --- |
| `False` | `AsExpected` | The identity renews before it expires, or renewal is not due yet. |
| `True` | `RotationOnSchedule`'s | Copied from `RotationOnSchedule` while it is `False`: the early warning that the identity will expire. |

A failure the same inputs cannot fix is not sent again until the operator config's
`site` section, or the resource version of the invite or a bundle it names, changes.
Replace the invite or edit the config to retry.
`InviteMaybeSpent`, `IdentityNotStored` and `StoreUnavailable` retry on the next poll,
and a spent token then answers as `InviteInvalid`. A restart never spends a second token, because
the operator skips enrollment when the identity Secret exists. If you create a
GridNetwork afterward that names other Secrets, the operator refuses to enroll
again. Point it at the enrolled Secrets. For `siteIdentity` installs, the
startup probe restarts a pod still enrolling after about 16 minutes by default
(96 checks, 10s apart). Adjust `health.startup` to change that window.

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

1. Set `peerTrust.mode` to `spiffe` on the GridNetwork. Under `pin` peers pin the
   leaf digest and would refuse a new one, so a site under `pin` sets
   `site.signerEndpoint.rotation: Disabled`, which the chart renders by
   default, and `Enabled` there reports `Accepted` `False` with
   `RotationUnsupportedUnderPinTrust`. Before a pin site's certificate expires,
   re-enroll it as in step 7 and update the digest its peers pin.
2. Keep the enrollment Route on passthrough. A reencrypt Route drops the client
   certificate.
3. Do not put a proxy that presents a grid site certificate in front of
   enrollment.
4. Keep `site.signerEndpoint` in the operator config, with `rotation` at
   `Enabled`, the default. The operator renews through the signer it names,
   trusted through the grid CA the site holds together with `caBundleRef`, and rolls the gateway Deployment after each rotation, since the
   gateway reads its client certificate only at start. The chart grants `get`
   and `patch` on that Deployment. A hub declares its signer the same way: the
   grid-operator chart renders `site.signerEndpoint` from `enrollment.url`,
   which defaults to the in-cluster enrollment Service with `enrollment.hub`.
   A hub installed without either does not renew: `RotationOnSchedule` reads `True`,
   its message saying nothing is known to renew the identity, then `False` with
   `RotationOverdue`.
5. Keep clocks on the enrollment service and its database within five minutes of
   each other.
6. Do not restore a site's identity Secret from backup, and do not manage it
   with GitOps or a policy that enforces its contents. An older copy holds a
   replaced key, so the site freezes at its next rotation.
7. If an identity already expired, delete the site's enrollment and its identity
   Secret, invite it again with `allowDeletedName`, and restart the operator.
   Both steps need `enrollment-admin`.

Watch `grid_site_identity_expiry_timestamp_seconds` and
`grid_site_identity_rotations_total` on the operator. Alerting reads the gauge. The
GridNetwork's `IdentityValid` message starts with the expiry, RFC 3339, and names the
certificate fingerprint, and its reason is `CertificateExpired` once the certificate
has expired. The GridNetwork carries two conditions the renewal loop writes each round:

| `IdentityValid` | Reason | Meaning |
| --- | --- | --- |
| `True` | `IdentityValid` | The identity reads, is current, names this site, is issued by the grid CA the operator holds, and `tls.key` is its key. The message reads `<notAfter, RFC 3339> is the expiry; fingerprint sha256:<hex>`. |
| `Unknown` | `AwaitingFirstAttempt` | No identity yet: the renewal loop evaluates it once one exists. |
| `False` | `IdentityMissing` | The identity Secret, or a key in it, is absent. |
| `False` | `CertificateUnreadable` | The Secret holds something that is not a certificate. |
| `False` | `CertificateExpired` | The identity expired and cannot renew. Re-enroll with a new site token. |
| `False` | `CertificateUnusable` | The certificate lacks the X.509-SVID profile peers under spiffe trust require. |
| `False` | `CertificateUntrusted` | The grid CA in the CA Secret did not issue the certificate, or it is not yet valid. Replace it with one that CA issued, or re-enroll. |
| `False` | `CertificateNameMismatch` | The certificate names another site, or no single site. Issue it for this site's SPIFFE ID, or set `site.name` to the site it names. |
| `False` | `CertificateKeyMismatch` | `tls.key` is not the key of `tls.crt`. Write the certificate and its key together. |
| `False` | `CertificateDnsNameMissing` | `tls.crt` lacks the DNS name `<site>.grid.internal`, which peers dial. Issue it with that name. Checked on every identity, signer-issued or external. |
| `False` | `CertificateOrganizationMismatch` | The subject organization must name the site alone, `O=<site>`, which a gateway authorizes peers on. Issue it with that organization. Checked on every identity, signer-issued or external. |

| `RotationOnSchedule` | Reason | Meaning |
| --- | --- | --- |
| `True` | `RotationOnSchedule` | Renewal is not due, and the message says what renews. The signer `site.signerEndpoint` names: the message carries the due time and the URL. The cert-manager Certificate the identity Secret's `cert-manager.io/certificate-name` annotation names: the operator reads only the annotation. Or nothing, so the identity is kept until it expires: `rotation` is `Disabled`, no signer is declared, or `Accepted` reports `RotationUnsupportedUnderPinTrust`. A newer leaf another writer puts in the identity Secret resets the clock. |
| `Unknown` | `NotEvaluated` | The operator config is refused for this site, so renewal is held back and not evaluated. See `Accepted`. Once renewal falls due this reads `False` with `RotationOverdue`. |
| `False` | `RotationOverdue` | Renewal is due or the identity expired, and nothing renewed it: the cases above, an annotated Certificate that has not replaced the identity within 15 minutes of its due time, the signer answering 503 `rotation_disabled`, or an expired identity. |
| `False` | `RenewalFailing` | The last check or renewal failed and is retried with backoff. The message carries the error. |
| `False` | `RenewalRefused` | The signer answered 401 or 403. Re-enroll this site with a new site token. |
| `False` | `RenewalNeedsPassthrough` | The signer received no client certificate, so TLS ends before it. Renewal needs a passthrough Route, a load balancer, or the in-cluster Service. Retried with backoff. |
| `False` | `SignerUntrusted` | Neither the grid CA this site holds nor the `caBundleRef` bundle trusts the signer's serving certificate. Pin the intermediate that issued it, or the certificate itself, never a public root, in `site.signerEndpoint.caBundleRef`, or serve a certificate the grid CA issued. Retried with backoff. |
| `False` | `SignerLapsed` | The signer's serving certificate is expired or not yet valid. The signer must rotate its serving certificate. Retried with backoff. |

### Turn rotation off

| Scope | Set | Effect |
| --- | --- | --- |
| Whole grid | `enrollment.rotation.enabled=false` on grid-enrollment | The service answers every rotation with 503 `rotation_disabled`. Enrollment still works. |
| One site | Set `enrollment.rotation=Disabled` on its grid-operator, which renders `site.signerEndpoint.rotation: Disabled` | The operator stops rotating and rolling the gateway, reports `RotationOnSchedule` `True`, its message saying nothing renews, until renewal is due and `False` with `RotationOverdue` after, which raises `Degraded`, and logs `rotation disabled`. The site stays this cluster's own and the signer still serves enrollment. |

Either way, each site keeps its identity until the expiry its `IdentityValid` message
names. Turn rotation back on before then, or the site must re-enroll.

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

The bootstrap Job issues the hub's identity. The hub renews through the signer its
operator config names, which the grid-operator chart renders from `enrollment.url`, the
release's in-cluster Service by default. To re-issue the identity or clear a
freeze:

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
kubectl -n grid-system get configmap grid-operator-config -o yaml   # the site section
kubectl get gridnetwork -o yaml   # status.conditions: Available first; IdentityValid names the expiry
kubectl -n grid-system get events --field-selector involvedObject.name=grid-operator-config
kubectl -n grid-enrollment logs deploy/grid-enrollment
```

Operator metrics: `grid_site_identity_expiry_timestamp_seconds` and
`grid_site_identity_rotations_total{result}`, where `result` is `rotated`,
`refused`, `failed`, or `expired`.

| Symptom | Check | Fix |
| --- | --- | --- |
| `site token rejected (...)` | The token expired or was revoked. | Delete `grid-invite-<siteName>` on the hub, run `helm upgrade` with the site still in `invites`, and copy the new token. |
| `site name already enrolled (...)` | An earlier attempt spent a token for this name. | An admin with `enrollment-admin` runs `DELETE /v1alpha1/enrollments/<siteName>`, then an admin with both `grid-admin` and `enrollment-admin` invites the site again with `allowDeletedName`. Without that access, enroll under a new site name. |
| `Progressing` stays `True` with `Enrolling` | `site.signerEndpoint.url` from the site. | Make it reachable. |
| `TLS to the enrollment service failed, check site.signerEndpoint.caBundleRef` | The intermediate that issued the enrollment serving certificate. | Set `site.signerEndpoint.caBundleRef` to it, or to the certificate itself, never a public root. |
| `returned CA is not the pinned grid CA` | The attempt spent the token. | Set `site.signerEndpoint.gridCaBundleRef` to the grid CA and enroll under a new site name. |
| `Accepted` is `False` with `RotationUnsupportedUnderPinTrust` | The GridNetwork's `peerTrust.mode`. | Set `site.signerEndpoint.rotation: Disabled`, or move the grid to `spiffe`. |
| `Accepted` is `False` with `OperatorConfigInvalid` | The field the message names in `grid-operator-config`. | Fix it. The operator picks the edit up without a restart. |
| `Accepted` is `False` with `SiteNameMismatch` | Which site the identity names. | Set `site.name` to it, or re-enroll under the new name. |
| An `AwaitingGridNetwork` Event on `grid-operator-config` | Whether a GridNetwork exists. | Create it, with the grid-site chart or by hand. |
| `possible interception, contact the hub` | The certificate names another site or key. | Tell the hub admin before enrolling again. |
| `RotationOnSchedule` is `True` under pin trust and its message says nothing renews | The expiry the `IdentityValid` message names. | Expected. Re-enroll before expiry and update the digest peers pin. |
| `site identity rotation failed` with `rotation_disabled` (503) | Whether `enrollment.rotation.enabled=false` on grid-enrollment. | Set it back to `true` before `notAfter`. |
| `RotationOnSchedule` `False` `RenewalNeedsPassthrough` (the signer answered 401 `identity_required`) | The signer never received the site's certificate. A reencrypt or edge Route, or a TLS-intercepting proxy, ends TLS before the signer. | Renewal needs TLS end to end: a passthrough Route, a load balancer, or the in-cluster Service. Put the signer host in `NO_PROXY`. |
| `site identity rotation failed` with `rotation refused` and `identity_refused` (403) | Enrollment log for `rotation fork` or `rotation refused`. `GET /v1alpha1/enrollments/<siteName>` shows `state: frozen`. | Delete the site's enrollment with `enrollment-admin`, invite it again with `allowDeletedName` holding both `grid-admin` and `enrollment-admin`, and the site re-enrolls. For the hub, see Recover the hub identity. |
| `site identity expired and cannot rotate; re-enroll this site with a new site token` | `IdentityValid` is `False` with `CertificateExpired`. | Delete the site's enrollment with `enrollment-admin` and its identity Secret, invite it again with `allowDeletedName` holding both `grid-admin` and `enrollment-admin`, and restart the operator. |
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
