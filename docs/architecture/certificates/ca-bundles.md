# CA bundles and public certificate copies

Distribute trust anchors and public identities to certificate consumers.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | grid-ca-bundle, site-local grid-ca, referenced CA Secrets/ConfigMaps, or public certificate status fields |
| Issuer | Original CA issuer. A copy has no separate issuer |
| Created / written by | Bootstrap/operator for Grid copies, configured delivery controller for remote copies, or administrator/platform for external bundles |
| Consumers | Enrollment clients, Grid operators, Praxis gateways, and metrics scrapers |
| Validity | Each certificate retains its original validity dates. Bundles have no independent expiry |
| Rotation owner | Original issuer changes the CA. Distribution owner updates every consuming copy |
| Renewal trigger | Source certificate or trust-policy change, according to the selected distribution mechanism |
| Consumer reload | Component-specific. Secret or ConfigMap updates alone do not prove active trust changed |

## Lifecycle

- Enrollment-server trust and Grid identity trust can use separate bundles.
  Provider, database, gateway, and scraper trust can also use separate issuers.
- ACM invite delivery can copy Grid trust. Platform CA injection can populate
  annotated ConfigMaps. These distribute public material and do not issue it.
- GridSite certificate status and CSR audit records copy issued public leaves.
  Neither extends validity or becomes the certificate's rotation owner.

## References

[Enrollment inputs] | [Invite delivery] | [Site status] | [CSR records]

[Enrollment inputs]: ../../../charts/grid-operator/values.yaml
[Invite delivery]: ../../../charts/grid-enrollment/templates/enrollment/invite-policy.yaml
[Site status]: ../../../crates/operator/src/crd/grid_site.rs
[CSR records]: ../../../crates/enrollment/src/record.rs
