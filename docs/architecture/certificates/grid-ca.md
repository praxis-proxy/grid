# Grid CA

Signs site identities and bootstrap enrollment and database certificates.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | grid-ca-key Secret: tls.crt and tls.key |
| Issuer | Self-signed by enrollment bootstrap, or supplied by an external PKI |
| Created / written by | Enrollment bootstrap for the built-in CA. Administrator or PKI controller for a provided CA |
| Consumers | Enrollment signer. Public bundles supply trust to sites and gateways |
| Validity | Built-in: 3,650 days after issuance, backdated one hour. Provided: issuer-defined |
| Rotation owner | Administrator or external PKI owner |
| Renewal trigger | Explicit CA replacement. No scheduled CA renewal |
| Consumer reload | Enrollment checks files every 60 seconds and enforces the database CA fingerprint |

## Lifecycle

- Bootstrap preserves the existing CA. Loss of its key while a public copy
  remains does not authorize automatic replacement.
- Changing the CA key requires explicit confirmation, trust redistribution, and
  site re-enrollment. Copying a CA certificate does not extend its validity.

## References

[Generation] | [Bootstrap] | [CA replacement] | [Chart values]

[Generation]: ../../../crates/certs/src/generate.rs
[Bootstrap]: ../../../crates/enrollment/src/bootstrap.rs
[CA replacement]: ../../../crates/enrollment/src/ca.rs
[Chart values]: ../../../charts/grid-enrollment/values.yaml
