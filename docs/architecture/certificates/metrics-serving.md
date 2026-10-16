# Metrics serving certificates

Authenticate operator and gateway HTTPS metrics listeners to scrapers.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | Operator: selected metrics TLS Secret or site identity. Gateway: metricsListener.existingSecret |
| Issuer | Platform service CA, external PKI, or Grid CA when reusing a site identity |
| Created / written by | Selected issuer/controller. Neither metrics listener issues certificates |
| Consumers | Operator or gateway metrics listener, and Prometheus scrapers |
| Validity | Issuer-defined, or the remaining validity of the reused site identity |
| Rotation owner | Selected issuer/controller. Grid operator renewal task for an enrolled identity |
| Renewal trigger | Issuer policy, or site-identity renewal |
| Consumer reload | Operator checks every 10 seconds. Gateway watches mounted certificate files |

## Lifecycle

- Operator automatic selection prefers an existing Secret, then OpenShift
  service CA, then a configured site identity. The gateway requires an existing
  Secret.
- Scrapers need the corresponding CA and server name. Distributing a renewed
  certificate and updating scraper trust are separate operations.
- Successful reload applies to new TLS connections. Reload does not issue or
  extend the certificate.

## References

[Operator configuration] | [Operator reload] | [Gateway configuration] |
[Gateway reload]

[Operator configuration]: ../../../charts/grid-operator/values.yaml
[Operator reload]: ../../../crates/operator/src/metrics_tls.rs
[Gateway configuration]: ../../../charts/praxis-gateway/values.yaml
[Gateway reload]: ../../../crates/gateway/src/metrics_listener.rs
