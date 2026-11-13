# Fallback CA and site identity

Initialize an independent grid when both configured TLS Secrets are absent.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | GridNetwork TLS references. CA Secret: ca.crt and ca.key. Site Secret: tls.crt and tls.key |
| Issuer | Network controller creates a self-signed CA and signs the site identity |
| Created / written by | Grid operator network controller, on initial creation |
| Consumers | Local operator and gateways configured to trust this CA |
| Validity | CA: 3,650 days. Site identity: 365 days. Both are backdated one hour |
| Rotation owner | Administrator. This initialization path has no renewal controller |
| Renewal trigger | None. Existing Secrets are preserved |
| Consumer reload | Administrator coordinates trust, identity, and gateway reload or rollout |

## Lifecycle

- Both CA and site references must exist in GridNetwork, and neither Secret may
  exist. If only one Secret exists, the controller does not replace the CA.
- The controller does not self-sign while grid-operator-config names this site,
  while an enrollment request is in flight, during the first 30 seconds after
  the GridNetwork is created, or without a valid site name.
- These certificates do not register the site with an enrollment service.
  Creating an independent CA does not join an existing grid.
- Keep this lifecycle separate from the enrollment-issued 180-day identity.

## References

[Initialization] | [Secret data] | [Validity]

[Initialization]: ../../../crates/operator/src/controller/grid_network.rs
[Secret data]: ../../../crates/operator/src/resources/secret.rs
[Validity]: ../../../crates/certs/src/generate.rs
