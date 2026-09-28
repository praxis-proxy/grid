# Site Enrollment

A site joins a grid by presenting a token to the enrollment service and
receiving back a signed certificate. The certificate carries a grid-assigned
identity, not one the site requests.

This guide covers both sides of that exchange: standing up the enrollment
service itself, then the site flow that redeems a token for a certificate.

## 1. Deploy the enrollment service

### Bootstrap the grid CA and serving certificates

Before the service can start, its own key material has to exist: the grid
CA, the enrollment service's serving certificate, and a serving certificate
for its Postgres connection. The chart runs the `enrollment bootstrap`
subcommand as a pre-install Job, so all three exist before the first pod
starts. No separate `bootstrap.*` values key exists. The Job's flags come
straight from the `ca.*`, `serving.*`, and `db.*` values below.

| Values key | Job flag or effect |
|---|---|
| `ca.method` (`builtin` or `provided`) | `builtin` mints a self-signed CA, `provided` skips minting and loads `ca.provided.keySecretRef` instead |
| `ca.commonName` | `--common-name`, default `grid-ca` |
| `ca.keySecretName` | Secret the CA certificate and its signing key land in, default `grid-ca-key` |
| `ca.bundleSecretName` | Secret the public CA certificate alone lands in, default `grid-ca-bundle` |
| `ca.forceRegenerate` | `--force-regenerate` |
| `ca.provided.keySecretRef` | An existing `tls.crt`/`tls.key` Secret to load, when `ca.method=provided` |
| `serving.secretName` | Secret the enrollment service's own serving certificate lands in, default `enrollment-serving-tls` |
| `serving.existingSecretRef` | Skip issuance and mount an existing serving certificate instead |
| `serving.extraDnsNames` | `--serving-dns`, repeated |
| `db.builtin.tls.servingSecretName` | Secret the Postgres serving certificate lands in, `--db-dns` derived from the built-in Postgres Service name, default `grid-db-serving-tls` |

Every serving certificate the Job issues carries DNS names only. None
carries a grid-site SPIFFE name, so they stay outside the identity space a
peer verifier trusts and cannot get mistaken for a site.

Bootstrap is idempotent. Re-running the Job on an upgrade keeps every
existing Secret as is. Setting `ca.forceRegenerate` reissues everything,
including the CA, which invalidates every certificate the current CA
already signed.

### Install the service

```bash
helm install grid-enrollment ./charts/grid-enrollment \
  --namespace grid \
  --set image.repository=quay.io/praxis-proxy/grid-enrollment \
  --set enrollment.authz=kube \
  --set enrollment.serviceAccount.create=true \
  --set db.type=external \
  --set db.external.connectionUrlSecretRef=maas-db-config
```

`image.tag` defaults to the chart's `AppVersion`, and `image.pullPolicy` is
also settable when a deployment needs to override it.

`enrollment.listenAddr` sets the address the service binds, default
`0.0.0.0:8443`, and the chart derives the Service port from it. No separate
`enrollment.service.port` key exists.

`enrollment.authz` chooses the grid-admin authorization backend, `kube` or
`local`:

- `kube` (default): Kubernetes RBAC. The service authenticates the caller
  with `TokenReview` and authorizes with `SubjectAccessReview` against the
  `enrollmenttokens` resource in the `grid.praxis-proxy.io` API group. No CRD
  is required, only ordinary `Role` or `ClusterRole` objects. Set
  `enrollment.serviceAccount.create=true` (and optionally
  `enrollment.serviceAccount.name`) so the pod runs as a `ServiceAccount`
  bound to `system:auth-delegator`, which is what lets the review calls
  succeed.
- `local`: an opt-in bearer-token table for grid-admins, for clusters with no
  Kubernetes credential to delegate to. Set `enrollment.authz=local` and
  `enrollment.gridAdminTokens.generate=true` to have the chart generate one,
  or bring your own with `enrollment.gridAdminTokens.existingSecretRef`, a
  Secret whose `tokens` key holds one `name:token` line per grid-admin.

`db.type` chooses the Postgres backend, `builtin` or `external`:

- `builtin`: the chart deploys its own Postgres. `db.builtin.image` and
  `db.builtin.storage` size it, `db.builtin.auth.database` and
  `db.builtin.auth.username` name the database and role, and
  `db.builtin.auth.existingSecretRef` supplies the password. Its serving
  certificate is the one bootstrap issues into
  `db.builtin.tls.servingSecretName`.
- `external`: the chart points at Postgres it does not manage.
  `db.external.connectionUrlSecretRef` names the Secret, and
  `db.external.connectionUrlSecretKey` (default `DB_CONNECTION_URL`) names
  the key inside it holding the connection URL. `db.external.caConfigMapName`
  and `db.external.caConfigMapKey` (default `ca.crt`) name the ConfigMap and
  key holding the CA that signed the external server's certificate, so the
  service can connect with `sslmode=verify-full`.

The service refuses to start without TLS, and it refuses a database
connection that does not require TLS. A deployment with no serving
certificate, or a plaintext-capable `sslmode`, fails at startup rather than
serving in the clear.

## 2. Mint a token

A grid-admin mints a single-use token that pins the site name. With the
default `enrollment.authz=kube`, `$GRID_ADMIN_TOKEN` is a Kubernetes
ServiceAccount token for an identity RBAC authorizes against the
`enrollmenttokens` resource, reviewed with `TokenReview` and
`SubjectAccessReview`. Under the `enrollment.authz=local` override,
`$GRID_ADMIN_TOKEN` is instead the token half of a `name:token` line in the
grid-admin-tokens Secret, the one `enrollment.gridAdminTokens.generate`
created or `enrollment.gridAdminTokens.existingSecretRef` pointed at. The
minted site token is usable once and only its digest is stored, so it
cannot be recovered later.

Every call below verifies the enrollment service's certificate against the
grid CA before sending a bearer token. Extract the CA certificate from the
`grid-ca-bundle` Secret and pass it with `--cacert`:

```bash
kubectl -n grid get secret grid-ca-bundle -o jsonpath='{.data.ca\.crt}' \
  | base64 -d > grid-ca-bundle.crt
```

```bash
curl -s -X POST https://enrollment.grid.internal/v1alpha1/enrollmenttokens \
  --cacert grid-ca-bundle.crt \
  -H "Authorization: Bearer $GRID_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"siteName": "east2", "gridNetworkRef": "my-grid"}'
```

Response:

```json
{
  "tokenId": "b3f...",
  "token": "b6a1...64-hex-chars",
  "siteName": "east2",
  "expiresAt": "2026-09-28T12:00:00Z"
}
```

Hand the `token` value and the `grid-ca-bundle.crt` file to the site out of
band. To revoke the token before it is redeemed:

```bash
curl -s -X DELETE https://enrollment.grid.internal/v1alpha1/enrollmenttokens/$TOKEN_ID \
  --cacert grid-ca-bundle.crt \
  -H "Authorization: Bearer $GRID_ADMIN_TOKEN"
```

## 3. Generate a keypair and CSR

On the site, generate a key and a certificate signing request. The CSR's
Subject Alternative Name, if it carries one, is ignored: the grid assigns the
identity from the token, not from anything the site asserts.

```bash
openssl ecparam -genkey -name prime256v1 -noout -out site.key
openssl req -new -key site.key -subj "/CN=east2" -out site.csr
```

## 4. Enroll

Submit the token and the CSR to the enrollment endpoint. Verify the server
against the `grid-ca-bundle.crt` handed over out of band with the token,
before sending `$SITE_TOKEN`:

```bash
jq -n --rawfile csr site.csr '{csr: $csr}' \
  | curl -s -X POST https://enrollment.grid.internal/v1alpha1/enrollments \
      --cacert grid-ca-bundle.crt \
      -H "Authorization: Bearer $SITE_TOKEN" \
      -H "Content-Type: application/json" \
      -d @-
```

One response, no separate approval step:

```json
{
  "id": "9e2...",
  "certificate": "-----BEGIN CERTIFICATE-----...",
  "caCertificate": "-----BEGIN CERTIFICATE-----...",
  "spiffeId": "spiffe://grid.internal/site/east2",
  "publicKeySha256": "…"
}
```

- `certificate` is signed under the name the token pinned, `east2`, regardless
  of what the CSR asked for.
- `spiffeId` is the identity this certificate carries, `spiffe://grid.internal/site/east2`.
  It is the identity the site presents for mutual TLS to its peers, not a
  claim that any peer has verified it yet.
- `caCertificate` is the grid CA, the same certificate the `grid-ca-bundle`
  Secret carries. Trust it to verify other sites' certificates.

## 5. Join the mesh

Enrollment returns identity only. A site also needs the SWIM transport key
and the seed peers to reach the rest of the mesh. The operator provisions
those from the site's GridNetwork. The enrollment service never returns
them.
