# Provider and external endpoint certificates

Authenticate inference, MCP, health, metrics, discovery, and authorization hops.

## Ownership

| Responsibility | Component or value |
| --- | --- |
| Stored as | Server-owned certificate, referenced client-certificate Secret, or configured gateway TLS files |
| Issuer | Endpoint owner's PKI or platform certificate controller |
| Created / written by | Endpoint/credential owner and its certificate controller |
| Consumers | Grid operator endpoint clients and Praxis gateway outbound clients |
| Validity | Issuer-defined. Grid does not set these certificates' lifetimes |
| Rotation owner | Endpoint owner for server TLS. Client identity issuer for mutual TLS |
| Renewal trigger | External issuer's renewal policy |
| Consumer reload | Operator reads references during reconciliation. Gateway reload/rollout depends on its outbound TLS configuration |

## Lifecycle

- InferenceProvider TLS references cover shared settings and feature overrides.
  AgentToolProvider has endpoint TLS references. Grid consumes this material and
  does not generate replacement endpoint certificates.
- TLS-configured InferenceProvider reconciliation has a maximum 60-second
  requeue interval. This detects material changes but does not renew
  certificates.
- Public endpoints can use system trust roots. Private endpoints use the
  selected CA bundle and, when configured, a separately issued client identity.

## References

[Provider TLS] | [Tool provider] | [Provider controller] | [Gateway values]

[Provider TLS]: ../../../crates/operator/src/crd/inference_provider.rs
[Tool provider]: ../../../crates/operator/src/crd/agent_tool_provider.rs
[Provider controller]: ../../../crates/operator/src/controller/inference_provider.rs
[Gateway values]: ../../../charts/praxis-gateway/values.yaml
