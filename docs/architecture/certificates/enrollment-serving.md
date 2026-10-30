# Enrollment serving certificate

Authenticates the enrollment HTTPS endpoint to joining and renewing sites.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | enrollment-serving-tls Secret: tls.crt and tls.key, or serving.existingSecretRef |
| Issuer | Grid CA for bootstrap issuance. External PKI for a provided certificate |
| Created / written by | Enrollment bootstrap, or the selected external issuer |
| Consumers | Enrollment TLS listener and operator enrollment clients |
| Validity | Bootstrap: 365 days after issuance, backdated one hour. Provided: issuer-defined |
| Rotation owner | Administrator runs bootstrap through install or upgrade. External issuer manages provided certificates |
| Renewal trigger | Bootstrap finds expiry within 30 days, missing or invalid material, a missing requested DNS name, or a changed CA |
| Consumer reload | Enrollment reloads mounted serving files every 60 seconds |

## Lifecycle

- The Helm pre-install/pre-upgrade Job performs renewal checks. No scheduled
  renewal controller runs between bootstrap executions.
- Bootstrap generates a new key and leaf when it reissues the certificate.
  Reload failure preserves the last working listener configuration.
- A passthrough Route presents this certificate. A reencrypt Route also uses a
  separate ingress certificate managed by the platform or administrator.

## References

[Bootstrap] | [Hook] | [Reload] | [Chart values]

[Bootstrap]: ../../../crates/enrollment/src/bootstrap.rs
[Hook]: ../../../charts/grid-enrollment/templates/certs/ca-bootstrap-job.yaml
[Reload]: ../../../crates/enrollment/src/main.rs
[Chart values]: ../../../charts/grid-enrollment/values.yaml
