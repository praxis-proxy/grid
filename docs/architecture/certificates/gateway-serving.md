# Gateway serving certificate

Authenticates a gateway listener to clients or peer gateways.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | gatewayConfig.listenerTls.existingSecret, or tls.existingSecret for a provider gateway |
| Issuer | Selected PKI or platform service CA. Provider gateways can reuse the Grid site identity |
| Created / written by | Selected issuer/controller. The gateway chart mounts an existing Secret |
| Consumers | Praxis TLS listener and connecting clients or peers |
| Validity | Issuer-defined, or the remaining validity of the reused site identity |
| Rotation owner | Certificate issuer for separate listener TLS. Grid operator for an enrolled site identity |
| Renewal trigger | Issuer policy, or site-identity renewal when that identity is reused |
| Consumer reload | Praxis watches files when TLS hot reload is enabled in a compatible build. Grid identity renewal rolls the configured gateway |

## Lifecycle

- A Service annotation can request an OpenShift serving certificate. The service
  CA operator owns issuance and renewal in that configuration.
- Separate listener and outbound client certificates can have different owners.
  Loading a renewed listener certificate does not establish outbound TLS reload.
- For TLS termination at an external ingress, its certificate and renewal belong
  to that ingress owner.

## References

[Chart values] | [Site identity](site-identity.md) | [Gateway rollout] |
[Listener reload]

[Chart values]: ../../../charts/praxis-gateway/values.yaml
[Gateway rollout]: ../../../crates/operator/src/enroll/renew.rs
[Listener reload]: https://github.com/praxis-proxy/praxis/blob/a50e58e530e6af51e4d002e5f8064ccfa1dc31a4/crates/protocol/src/tls_setup.rs
