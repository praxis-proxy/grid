# AgentToolProvider

Advertises an MCP endpoint and its available tools to the network.

## Ownership

| Responsibility | Component |
| --- | --- |
| Created by | Helm client, GitOps controller, or administrator API client |
| Spec owner | Administrator |
| Spec writer | Helm/GitOps or administrator API client |
| Status writer | Grid operator tool-provider controller |
| Direct consumers | Grid operator MCP discovery and network capability publication |
| Derived consumers | Peer operators and gateways consume published capability and routing state |

## Lifecycle

- The administrator declares the endpoint, credentials, and site selection.
- The controller validates configuration, probes the endpoint, and records
  discovered tools, matching sites, and availability in status.
- Discovery does not rewrite declared tools in spec. The operator does not
  deploy the MCP server.

## References

[Fields] | [Schema] | [Controller]

[Fields]: ../crds.md#agenttoolprovider
[Schema]: ../../../crates/operator/src/crd/agent_tool_provider.rs
[Controller]: ../../../crates/operator/src/controller/agent_tool_provider.rs
