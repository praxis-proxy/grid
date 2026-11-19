# Site identity

Authenticates a site to peers and the enrollment renewal endpoint.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | grid-site-identity Secret, or the configured identity Secret: tls.crt and tls.key, plus previous.crt after a rotation and rotate.key and rotate.csr during one |
| Issuer | Enrollment service using the Grid CA, or a cert-manager Certificate per site. Bootstrap issues the optional hub identity |
| Created / written by | Joining operator generates the key and writes the signed certificate. Bootstrap writes the initial hub identity. cert-manager writes it in external mode, and a hub delivery Policy copies it to a member cluster |
| Consumers | Grid operator, peer gateways, enrollment renewal endpoint, signed SWIM state, Local membership signals, and operator metrics TLS when sourced from the site identity |
| Validity | Default: 180 days after issuance, backdated five minutes. Configurable through enrollment.certLifetimeSecs. cert-manager: 4,320 hours by default |
| Rotation owner | Grid operator renewal task and enrollment signer, or cert-manager |
| Renewal trigger | One-third of the certificate's validity remains, approximately day 120 by default. cert-manager renews 1,440 hours before expiry, also one third |
| Consumer reload | Operator reads the Secret every 30 seconds. A changed identity rolls the gateway Deployment through its pod-template fingerprint annotation |

## Lifecycle

- Operator renewal reads site.signerEndpoint in grid-operator-config, set by the
  grid-operator chart's enrollment.url, caBundle, gridCaBundle, and rotation
  values. Rotation is off unless set, or unless grid.peerTrust is spiffe. Under
  pin trust, enabled rotation reports RotationUnsupportedUnderPinTrust, and the
  administrator re-enrolls and updates peer pins before expiry. The signer also
  needs enrollment.rotation.enabled in the grid-enrollment chart.
- An identity carrying the cert-manager.io/certificate-name annotation is
  renewed externally. The operator reports the Certificate that renews it, and
  RotationOverdue when the identity is not replaced by its due time.
- Under SPIFFE trust, a leaf without the X.509-SVID profile reports
  CertificateUnusable.
- Hub bootstrap preserves a matching identity even after expiry. The operator
  must renew it, or an administrator must reissue it. An expired identity cannot
  authenticate renewal.
- Gateway rollout requires the correct Deployment name, namespace, and RBAC.
  Revoking an invite or deleting enrollment state does not revoke an issued
  leaf.

## References

[Validity] | [Renewal] | [Hub bootstrap] | [Chart values] | [Operator values] |
[cert-manager Certificate]

[Validity]: ../../../crates/certs/src/enroll.rs
[Renewal]: ../../../crates/operator/src/enroll/renew.rs
[Hub bootstrap]: ../../../crates/enrollment/src/bootstrap.rs
[Chart values]: ../../../charts/grid-enrollment/values.yaml
[Operator values]: ../../../charts/grid-operator/values.yaml
[cert-manager Certificate]: ../../../charts/grid-enrollment/templates/_site-certificate.tpl
