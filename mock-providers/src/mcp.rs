//! Mock MCP (Model Context Protocol) `tools/list` server.
//!
//! Built on the real `rmcp` server SDK (the same crate `AgentToolProvider`'s
//! live probe uses on the client side — see
//! `operator/src/resources/mcp_probe.rs`), not a hand-rolled JSON-RPC
//! responder. This guarantees the mock speaks the actual Streamable HTTP
//! wire protocol (handshake, session semantics, SSE framing) rather than an
//! approximation that happens to satisfy one client implementation.
//!
//! Used by `cargo xtask env verify-agenttoolprovider-convergence` as a real,
//! deployed-in-cluster MCP endpoint for `AgentToolProvider`'s probe to
//! discover tools from — mirroring how `openai`/`anthropic`/etc. already
//! serve as real in-cluster mocks for `InferenceProvider`.

use rmcp::{
    ServerHandler,
    model::{ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool},
    service::RequestContext,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};

/// A minimal MCP server that answers `tools/list` with a fixed set of tool
/// names, optionally requiring a specific bearer token.
#[derive(Clone)]
struct FixedToolsServer {
    /// Tool names returned by `tools/list`, in order.
    tools: Vec<String>,
    /// If set, `tools/list` calls must carry this exact bearer token.
    required_bearer: Option<String>,
}

impl ServerHandler for FixedToolsServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<rmcp::RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        if let Some(expected) = &self.required_bearer {
            // rmcp threads the raw incoming `http::request::Parts` (headers
            // included) into RequestContext::extensions — no axum middleware
            // needed to see what the probe actually sent.
            let got = context
                .extensions
                .get::<http::request::Parts>()
                .and_then(|parts| parts.headers.get(http::header::AUTHORIZATION))
                .and_then(|value| value.to_str().ok());
            if got != Some(format!("Bearer {expected}").as_str()) {
                return Err(rmcp::ErrorData::invalid_request("missing or wrong bearer token", None));
            }
        }
        let tools = self
            .tools
            .iter()
            .map(|name| {
                let mut tool = Tool::default();
                tool.name = name.clone().into();
                tool
            })
            .collect();
        Ok(ListToolsResult::with_all_items(tools))
    }
}

/// Build an `axum::Router` serving a `FixedToolsServer` (private to this
/// module) at `/mcp`.
///
/// `required_bearer: None` means the server accepts any (or no)
/// `Authorization` header — used to validate `AgentToolProvider`'s
/// unauthenticated healthy-probe path in the E2E convergence check.
pub fn router(tools: Vec<String>, required_bearer: Option<String>) -> axum::Router {
    let handler = FixedToolsServer { tools, required_bearer };
    // `rmcp`'s default `allowed_hosts` (`localhost`/`127.0.0.1`/`::1`) is a
    // DNS-rebinding guard aimed at servers bound to a developer's own
    // loopback interface. This mock is deployed in-cluster and reached by
    // real probe clients over its Service DNS name or `NodePort` address —
    // neither of which is loopback — so the default would 403 every
    // legitimate probe. Disabling it is safe here: this is a disposable test
    // fixture with no browser-facing surface, not a public deployment.
    let config = StreamableHttpServerConfig::default().disable_allowed_hosts();
    let service: StreamableHttpService<FixedToolsServer, LocalSessionManager> =
        StreamableHttpService::new(move || Ok(handler.clone()), std::sync::Arc::default(), config);
    axum::Router::new().nest_service("/mcp", service)
}

#[cfg(test)]
mod tests {
    use tower::ServiceExt as _;

    use super::*;

    #[tokio::test]
    async fn unauthenticated_request_to_mcp_path_is_routed_not_404() {
        // A full protocol round trip needs a real rmcp client (covered by
        // the operator's own integration tests against this exact server
        // shape); this test only proves the router wiring itself — that
        // `/mcp` is a live route, not a typo'd path silently 404ing.
        let app = router(vec!["search".to_owned()], None);
        let response = app
            .oneshot(
                http::Request::builder()
                    .method("GET")
                    .uri("/mcp")
                    .body(axum::body::Body::empty())
                    .unwrap_or_else(|_| std::process::abort()),
            )
            .await
            .unwrap_or_else(|_| std::process::abort());
        assert_ne!(
            response.status(),
            http::StatusCode::NOT_FOUND,
            "the /mcp path must be routed to the MCP service, not fall through to a 404"
        );
    }
}
