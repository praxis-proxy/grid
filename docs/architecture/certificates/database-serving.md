# Enrollment database certificate

Authenticates the enrollment service's PostgreSQL connection.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | Built-in database: grid-db-serving-tls Secret. External database: provider-managed storage |
| Issuer | Built-in database: Grid CA. External database: its PKI issuer |
| Created / written by | Enrollment bootstrap or external database controller |
| Consumers | PostgreSQL TLS listener and enrollment SQL client |
| Validity | Bootstrap: 365 days after issuance, backdated one hour. External: issuer-defined |
| Rotation owner | Administrator runs bootstrap for the built-in database. Database/PKI owner manages external certificates |
| Renewal trigger | Bootstrap checks the same 30-day expiry window and validity/DNS requirements as enrollment serving TLS |
| Consumer reload | Bootstrap rolls the named PostgreSQL Deployment when its certificate fingerprint changes |

## Lifecycle

- Built-in renewal requires another bootstrap execution. The chart supplies no
  scheduled renewal controller.
- Enrollment verifies the server hostname and certificate chain using
  sslmode=verify-full. External database trust comes from the configured CA map.
- The external database owner must coordinate certificate renewal, trust
  updates, and any required database or client restart.

## References

[Bootstrap and rollout] | [Database workload] | [Chart values]

[Bootstrap and rollout]: ../../../crates/enrollment/src/bootstrap.rs
[Database workload]: ../../../charts/grid-enrollment/templates/db/db-builtin.yaml
[Chart values]: ../../../charts/grid-enrollment/values.yaml
