# AgentToAgentProvider

Defines an A2A provider schema. Runtime support is not implemented.

## Ownership

| Responsibility | Component |
| --- | --- |
| Created by | None. Excluded from generated CRDs |
| Spec owner | None implemented. Schema describes administrator intent |
| Spec writer | None implemented |
| Status writer | None implemented |
| Direct consumers | None implemented |
| Derived consumers | None implemented |

## Lifecycle

- The Rust schema defines spec and status types, but the CRD generator does not
  install this resource and the operator starts no controller for it.
- The field reference documents the schema. Runtime routing and reconciliation
  are not implemented.

## References

[Fields] | [Schema] | [CRD generator]

[Fields]: ../crds.md#agenttoagentprovider
[Schema]: ../../../crates/operator/src/crd/agent_to_agent_provider.rs
[CRD generator]: ../../../crates/operator/src/bin/generate_crds.rs
