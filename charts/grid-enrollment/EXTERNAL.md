# External identities

The enrollment chart supports two identity backends. `mode: service` preserves
the enrollment service, its CA bootstrap Job, database, and invite flow.
`mode: external` consumes identities supplied by another PKI. It does not
deploy the service, database, invite Jobs, or their RBAC. Both modes fit the
same lifecycle: bootstrap, enroll, and decommission.

## Bootstrap

For externally supplied identities, provision the shared trust bundle and the
site identity Secrets through your PKI integration. The chart reads references;
private keys never belong in values or Git. ACM delivery expects each identity
Secret to contain `tls.crt` and `tls.key`, and the public trust Secret to contain
`ca.crt` (or the configured `external.trustBundleSecretKey`).

Cert-manager is an optional integration, installed separately. An existing
Issuer, ClusterIssuer, or external issuer can be referenced without the chart
owning it:

```yaml
mode: external
external:
  certManager:
    enabled: true
    issuer:
      create: false
      name: site-issuer
      kind: ClusterIssuer
      group: cert-manager.io
```

To expose an existing Grid CA through a namespaced Issuer, use:

```yaml
mode: external
external:
  certManager:
    enabled: true
    issuer:
      create: true
      name: grid-ca
    ca:
      mode: Existing
      secretName: grid-ca-key
```

This creates only an Issuer when no sites are configured. It does not write,
renew, or take ownership of the existing CA Secret. The Secret must contain
`tls.crt` and `tls.key` and be in the release namespace.

For a **new grid only**, `external.certManager.ca.mode: Generate` creates a
self-signed root CA and its Issuer. Its signing key is reused on renewal.
Helm and Argo retention annotations preserve the generated CA Certificate,
Secret, and self-signed Issuer during ordinary uninstall and pruning. Keep a
backup: explicit Kubernetes deletion and namespace deletion are not covered.
CA rotation remains a coordinated trust migration.

## Enroll

Declare sites by their Grid identity name. `secretName` defaults to
`grid-site-<site>`. With the optional integration enabled, the chart requests a
Certificate for each site. Otherwise the referenced Secret must already be
supplied by your PKI integration.

```yaml
mode: external
external:
  sites:
    site-a:
      secretName: site-a-identity
      dnsNames: [signals.example.com]
      cluster: managed-a
  trustBundleSecretRef: grid-public-trust
  delivery:
    enabled: true
    namespace: grid-system
    clusterSet: default
    # Set false if another Application already owns the binding.
    createClusterSetBinding: false
```

Certificates use the Grid profile: one organization and SPIFFE URI naming the
site, its `<site>.grid.internal` DNS SAN, and client/server authentication
usages. The optional integration creates ECDSA keys and renews them with the
certificates. External issuers must permit this profile. Additional listener
DNS names belong in `dnsNames`; they do not change the site's identity.

ACM delivers the identity and public trust bundle only to the named managed
cluster. Hub `fromSecret` templates encrypt Secret values; the CA signing key
is never included in the delivery policy. Policies follow Secret updates, so
renewal does not require a Helm hook. When the chart creates a CA Issuer, the
delivery bundle comes from that CA Secret's public `tls.crt`. Otherwise an
explicit public trust bundle is required, including with an external issuer.

Leave `cluster` empty for an identity issued locally on the hub. Certificate
Secrets always live in the release namespace; local identity consumers must
read them there. Each managed cluster receives one identity. Removing a site
from this map alone is not a complete decommission operation.

This chart does not yet reconcile peer GridSites, their endpoints, or the
operator's membership configuration. ACM/Argo must supply those consistently
before a site can participate. For Local membership, configure the runtime
and GridSites for that mode and disable service enrollment at the site.

## Decommission

The ordered decommission flow is a follow-up, not an implemented Helm hook:

1. Withdraw the site's membership from every remaining peer and verify it has
   reconciled, so the site's existing certificate no longer grants access.
2. Stop issuance/renewal and delivery for that site. Remove its Certificate or
   revoke renewal through the external PKI integration.
3. Remove the site's identity Secrets and configuration, then detach it from ACM.

Deleting a Certificate stops renewal but does not cryptographically revoke an
issued certificate. Cert-manager normally retains its Secret, and ACM policy
removal is not a guarantee of remote object deletion. Shared CA resources
remain outside the site's lifecycle.

## Migrate an existing installation

Keep the same CA. Existing service-enrolled sites still renew through the
enrollment service until their identity ownership has been migrated; do not
switch that service's Helm release to external mode prematurely, because
doing so removes the service and its database resources.

For the temporary ODH bootstrap Application, keep its Application name,
release name, destination, and `grid-ca` Issuer name. Change its source to the
existing enrollment chart and select the Existing CA example above. This
updates the same Issuer in place and leaves the enrollment service running in
its original Application. Remove the obsolete standalone chart only after
this source change has synced successfully.

Validate locally with `helm unittest charts/grid-enrollment`. The disposable
Kind test at `tests/cert-manager-e2e.sh` exercises issuance, renewal, external
issuer ownership, and CA retention. Set `LEGACY_CHART` to the old bootstrap
chart path to also verify migration preserves the existing Issuer UID and CA.
