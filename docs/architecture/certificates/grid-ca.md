# Grid CA

Signs site identities. The built-in CA also signs the bootstrap enrollment and
database certificates.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | grid-ca-key Secret: tls.crt and tls.key |
| Issuer | Self-signed by enrollment bootstrap, self-signed by cert-manager with ca.mode Generate, or supplied by an external PKI |
| Created / written by | Enrollment bootstrap for the built-in CA. cert-manager for a generated CA. Administrator or PKI controller for a provided CA |
| Consumers | Enrollment signer. Public bundles supply trust to sites and gateways |
| Validity | Built-in: 3,650 days after issuance, backdated one hour. cert-manager: 87,600 hours by default. Provided: issuer-defined |
| Rotation owner | Administrator or external PKI owner. cert-manager for a generated CA |
| Renewal trigger | Explicit CA replacement. A generated CA is re-signed on the same key 8,760 hours before expiry |
| Consumer reload | Enrollment checks files every 60 seconds and compares the CA public key with the one recorded in its database |

## Lifecycle

- Bootstrap preserves the existing CA. Loss of its key while a public copy
  remains does not authorize automatic replacement.
- A CA re-signed on the same key reloads without confirmation. Changing the CA
  key requires explicit confirmation, trust redistribution, and site
  re-enrollment. Copying a CA certificate does not extend its validity.
- With a provided CA, bootstrap issues no serving or database certificate and
  writes no public bundle. The chart refuses the built-in database and a hub
  identity in that mode.

## References

[Generation] | [Bootstrap] | [CA replacement] | [Chart values] | [cert-manager CA]

[Generation]: ../../../crates/certs/src/generate.rs
[Bootstrap]: ../../../crates/enrollment/src/bootstrap.rs
[CA replacement]: ../../../crates/enrollment/src/ca.rs
[Chart values]: ../../../charts/grid-enrollment/values.yaml
[cert-manager CA]: ../../../charts/grid-enrollment/templates/certs/cert-manager-ca.yaml
