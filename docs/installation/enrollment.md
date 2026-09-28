# Site Enrollment

A site joins a grid by presenting a token to the enrollment service and
receiving back a signed certificate. The certificate carries a grid-assigned
identity, not one the site requests.

## 1. Mint a token

A grid-admin mints a single-use token that pins the site name. The token is
usable once and only its digest is stored, so it cannot be recovered later.

```bash
curl -sk -X POST https://enrollment.grid.internal/v1alpha1/enrollmenttokens \
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

Hand the `token` value to the site out of band. To revoke it before it is
redeemed:

```bash
curl -sk -X DELETE https://enrollment.grid.internal/v1alpha1/enrollmenttokens/$TOKEN_ID \
  -H "Authorization: Bearer $GRID_ADMIN_TOKEN"
```

## 2. Generate a keypair and CSR

On the site, generate a key and a certificate signing request. The CSR's
Subject Alternative Name, if it carries one, is ignored: the grid assigns the
identity from the token, not from anything the site asserts.

```bash
openssl ecparam -genkey -name prime256v1 -noout -out site.key
openssl req -new -key site.key -subj "/CN=east2" -out site.csr
```

## 3. Enroll

Submit the token and the CSR to the enrollment endpoint:

```bash
jq -n --rawfile csr site.csr '{csr: $csr}' \
  | curl -sk -X POST https://enrollment.grid.internal/v1alpha1/enrollments \
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
- `caCertificate` is the grid CA. Trust it to verify other sites' certificates.

## 4. Join the mesh

Enrollment returns identity only. The mesh configuration a site needs to reach
its peers, the SWIM transport key and the seed peers, is provisioned by the
operator from the site's GridNetwork, not returned by the enrollment service.
