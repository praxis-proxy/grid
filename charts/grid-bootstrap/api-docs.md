# grid-bootstrap

![Version: 0.3.0-rc.1](https://img.shields.io/badge/Version-0.3.0--rc.1-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: v0.3.0-rc.1](https://img.shields.io/badge/AppVersion-v0.3.0--rc.1-informational?style=flat-square)

Shared certificate authority and issuer for AI Grid Network sites

**Homepage:** <https://github.com/praxis-proxy/grid>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| Praxis Proxy |  | <https://github.com/praxis-proxy> |

## Source Code

* <https://github.com/praxis-proxy/grid>

## Requirements

Kubernetes: `>=1.26.0-0`

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| ca.bootstrapIssuerName | string | `"grid-root-selfsigned"` | SelfSigned Issuer name when ca.mode is Generate. Retained with the CA Certificate. |
| ca.certificateName | string | `"grid-root-ca"` | Certificate name when ca.mode is Generate. Retained when the release is removed. |
| ca.commonName | string | `"grid-root-ca"` | CA subject common name when ca.mode is Generate. |
| ca.durationHours | int | `87600` | CA certificate lifetime in hours when ca.mode is Generate. Renewal keeps the signing key. |
| ca.mode | string | `"Existing"` | Existing references an existing CA without modifying it. Generate creates a CA for a new grid. |
| ca.renewBeforeHours | int | `8760` | Renew the CA certificate this many hours before expiry. Must be less than durationHours. |
| ca.secretName | string | `"grid-ca-key"` | TLS Secret containing the CA certificate and private key, in the release namespace. |
| enabled | bool | `false` | Enable bootstrap after cert-manager is installed in the cluster. |
| issuer.name | string | `"grid-ca"` | Namespaced Issuer used by the enroll flow. Certificates must be in the release namespace. |
