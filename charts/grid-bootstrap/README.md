# Grid bootstrap

Bootstrap establishes the shared certificate authority (CA) for AI Grid Network
(AGN). Install this chart once on the hub, after cert-manager is ready. It
creates a namespaced Issuer for site certificates. The enroll flow can then
request one Certificate per site and deliver that site's identity through ACM.

This chart is maintained in `praxis-proxy/grid` and mirrored into `odh-gitops`.
Keep its templates, schema, documentation, and tests identical in both copies.
The [generated reference](api-docs.md) lists all values.

## Adopt an existing grid CA

The default mode is Existing. Enabling it creates only an Issuer referencing
the existing Secret. The chart does not read, write, adopt Helm ownership of,
or renew that Secret. Its existing owner retains those responsibilities.

The Secret must be in the release namespace and contain `tls.crt` and
`tls.key`, with a CA certificate whose key can sign site certificates. The
Grid enrollment service's CA Secret uses this format. Both services can
reference the same signing key during the migration.

```console
helm upgrade --install grid-bootstrap ./charts/grid-bootstrap \
  --namespace grid-trust \
  --set enabled=true \
  --set ca.mode=Existing \
  --set ca.secretName=existing-grid-ca
kubectl -n grid-trust wait issuer/grid-ca --for=condition=Ready --timeout=120s
```

A missing or invalid Secret leaves the Issuer unready. It never triggers CA
generation. The same result follows from Helm and an offline Argo CD render.

## Bootstrap a new grid

Choose Generate explicitly, in a namespace that holds no existing grid CA:

```console
helm upgrade --install grid-bootstrap ./charts/grid-bootstrap \
  --namespace grid-trust --create-namespace \
  --set enabled=true \
  --set ca.mode=Generate
kubectl -n grid-trust wait issuer/grid-ca --for=condition=Ready --timeout=120s
```

This mode creates a SelfSigned Issuer, a CA Certificate, and the namespaced CA
Issuer used by enroll. cert-manager generates the signing key in the cluster.
Helm renders no key material and calls no external service. Argo sync waves
order the SelfSigned Issuer, CA Certificate, and CA Issuer.

The CA uses an ECDSA P-256 key in PKCS8 format. Certificate renewal reuses that
key. A change to the CA key or the trust bundle needs an explicit migration
across the grid. Do not switch an existing installation to Generate to repair
a missing Secret. Restore that CA from backup instead.

Keep certificate creation and Secret access in this namespace limited to
trusted grid administrators. The Issuer is namespaced so other namespaces
cannot request site identities through it. The enroll workflow decides which
site name each certificate may carry.

## Lifecycle boundaries

| Phase | Owns |
| --- | --- |
| Bootstrap | The shared CA and the namespaced site issuer. |
| Enroll | A site's certificate, its delivery, configuration, and membership. |
| Decommission | Withdrawal of a site, stopping its renewal, and site cleanup. |

For hub-issued identities, the hub holds the site private keys as well as the
CA key. Deliver each site only its own leaf certificate and private key plus
the public CA bundle. The public bundle comes from the CA Secret's `tls.crt`.
Never deliver the CA's `tls.key` to a site.

Bootstrap does not create site Certificates, deploy ACM policies, change Grid
membership mode, or remove the existing enrollment service. Those changes
belong to the following lifecycle stages.

## Retention and recovery

Generate mode retains the CA Certificate, its Secret, and its SelfSigned
Issuer on Helm uninstall and normal Argo pruning or Application deletion.
The annotations do not prevent explicit Kubernetes deletion or namespace
deletion. Back up the CA Secret independently.

The namespaced site issuer is removed with the release, stopping issuance
through it. Existing certificates remain valid until their expiry. Deleting
a Certificate or an issuer does not revoke certificates already issued.

The retained root Certificate can renew through its retained SelfSigned
Issuer. After CA certificate renewal, propagate the refreshed public bundle
through the trust distribution workflow. Root key replacement and CA expiry
recovery are separate administrative procedures.

## Validation

```console
helm lint charts/grid-bootstrap --strict
helm unittest charts/grid-bootstrap
bash charts/grid-bootstrap/tests/e2e.sh
```

The end-to-end test creates and removes a disposable Kind cluster. It checks
fresh and existing CA issuance, repeated upgrades, site certificate
reissuance, and CA retention after uninstall. It uses a private temporary
kubeconfig and leaves the caller's current cluster context unchanged.
