# Site identity

Authenticates a site to peers and the enrollment renewal endpoint.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | grid-site-identity Secret: tls.crt and tls.key, or the configured identity Secret |
| Issuer | Enrollment service using the Grid CA. Bootstrap issues the optional hub identity |
| Created / written by | Joining operator generates the key and writes the signed certificate. Bootstrap writes the initial hub identity |
| Consumers | Grid operator, peer gateways, and enrollment renewal endpoint |
| Validity | Default: 180 days after issuance, backdated five minutes. Configurable through enrollment.certLifetimeSecs |
| Rotation owner | Grid operator renewal task and enrollment signer |
| Renewal trigger | One-third of the certificate's validity remains, approximately day 120 by default |
| Consumer reload | Operator reloads identity for renewal. Configured gateway Deployment rolls after rotation |

## Lifecycle

- Automatic renewal requires enabled rotation, an enrollment URL, a valid
  current identity, and SPIFFE peer trust. Pin mode requires administrator
  re-enrollment and peer pin updates before expiry.
- Hub bootstrap preserves a matching identity even after expiry. The operator
  must renew it, or an administrator must reissue it. An expired identity cannot
  authenticate renewal.
- Gateway rollout requires the correct Deployment name, namespace, and RBAC.
  Revoking an invite or deleting enrollment state does not revoke an issued
  leaf.

## References

[Validity] | [Renewal] | [Hub bootstrap] | [Chart values]

[Validity]: ../../../crates/certs/src/enroll.rs
[Renewal]: ../../../crates/operator/src/enroll/renew.rs
[Hub bootstrap]: ../../../crates/enrollment/src/bootstrap.rs
[Chart values]: ../../../charts/grid-enrollment/values.yaml
