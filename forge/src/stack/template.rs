//! Lightweight template engine for stack step fields.
//!
//! Supports `{{ variable.path }}` syntax with a fixed set of
//! variables resolved from the cluster context and for-each
//! iteration state.

use std::collections::BTreeMap;

use crate::error::ForgeError;

/// Maximum rendered string size from one template field.
const MAX_RENDERED_TEMPLATE_BYTES: usize = 8192;

// -------------------------------------------------------------
// Context
// -------------------------------------------------------------

/// Data context for template variable resolution.
#[derive(Clone)]
pub struct TemplateContext {
    /// Cluster name (resolves `cluster.name`).
    pub cluster_name: String,
    /// Stack name (resolves `stack.name`).
    pub stack_name: String,
    /// Cluster properties (resolves `cluster.properties.KEY`).
    pub properties: BTreeMap<String, serde_json::Value>,
    /// Current for-each element (resolves `item` and `item.FIELD`).
    pub item: Option<serde_json::Value>,
    /// Network variables (resolves `network.dnsZone`, `network.pool`).
    pub network: Option<NetworkTemplateVars>,
}

/// Network-related template variables.
#[derive(Clone)]
pub struct NetworkTemplateVars {
    /// DNS zone for cross-cluster service discovery.
    pub dns_zone: String,
    /// This cluster's allocated `MetalLB` pool range.
    pub pool: Option<String>,
}

// -------------------------------------------------------------
// Public API
// -------------------------------------------------------------

/// Render all `{{ ... }}` expressions in a template string.
///
/// # Errors
///
/// Returns [`ForgeError::Config`] if a variable path cannot be
/// resolved against the context.
#[expect(clippy::string_slice, reason = "{{ and }} are ASCII; find returns byte boundaries")]
pub fn render(template: &str, ctx: &TemplateContext) -> Result<String, ForgeError> {
    let mut result = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        result.push_str(&rest[..start]);
        let after_open = &rest[start + 2..];
        let end = find_close(after_open, template)?;
        let var_path = after_open[..end].trim();
        let value = resolve_variable(var_path, ctx)?;
        result.push_str(&value);
        check_rendered_size(&result)?;
        rest = &after_open[end + 2..];
    }
    result.push_str(rest);
    check_rendered_size(&result)?;
    Ok(result)
}

/// Enforce a bounded rendered string size.
fn check_rendered_size(value: &str) -> Result<(), ForgeError> {
    if value.len() <= MAX_RENDERED_TEMPLATE_BYTES {
        return Ok(());
    }
    Err(ForgeError::Config(format!(
        "rendered template exceeds {MAX_RENDERED_TEMPLATE_BYTES} bytes"
    )))
}

// -------------------------------------------------------------
// Variable resolution
// -------------------------------------------------------------

/// Find the closing `}}` or return an error.
fn find_close(after_open: &str, original: &str) -> Result<usize, ForgeError> {
    after_open
        .find("}}")
        .ok_or_else(|| ForgeError::Config(format!("unclosed template expression in '{original}'")))
}

/// Resolve a single variable path against the context.
fn resolve_variable(path: &str, ctx: &TemplateContext) -> Result<String, ForgeError> {
    let parts: Vec<&str> = path.splitn(3, '.').collect();
    let root = parts.first().copied().unwrap_or_default();
    match root {
        "cluster" => resolve_cluster(&parts, ctx),
        "stack" => resolve_stack(&parts, ctx),
        "item" => resolve_item(&parts, ctx),
        "network" => resolve_network(&parts, ctx),
        _ => Err(ForgeError::Config(format!(
            "unknown template variable root '{root}' in '{path}'"
        ))),
    }
}

/// Resolve `cluster.name` or `cluster.properties.KEY`.
fn resolve_cluster(parts: &[&str], ctx: &TemplateContext) -> Result<String, ForgeError> {
    let field = parts.get(1).copied().unwrap_or_default();
    match field {
        "name" => Ok(ctx.cluster_name.clone()),
        "properties" => resolve_property(parts, ctx),
        _ => Err(ForgeError::Config(format!("unknown cluster field '{field}'"))),
    }
}

/// Resolve `cluster.properties.KEY[.subkey...]`.
fn resolve_property(parts: &[&str], ctx: &TemplateContext) -> Result<String, ForgeError> {
    let key_path = parts.get(2).copied().unwrap_or_default();
    if key_path.is_empty() {
        return Err(ForgeError::Config("cluster.properties requires a key".to_owned()));
    }
    let segments: Vec<&str> = key_path.split('.').collect();
    let root_key = segments.first().copied().unwrap_or_default();
    let root_val = ctx
        .properties
        .get(root_key)
        .ok_or_else(|| ForgeError::Config(format!("property '{root_key}' not found")))?;
    navigate_value(root_val, segments.get(1..).unwrap_or(&[]))
}

/// Resolve `stack.name`.
fn resolve_stack(parts: &[&str], ctx: &TemplateContext) -> Result<String, ForgeError> {
    let field = parts.get(1).copied().unwrap_or_default();
    match field {
        "name" => Ok(ctx.stack_name.clone()),
        _ => Err(ForgeError::Config(format!("unknown stack field '{field}'"))),
    }
}

/// Resolve `item` or `item.FIELD`.
fn resolve_item(parts: &[&str], ctx: &TemplateContext) -> Result<String, ForgeError> {
    let val = ctx
        .item
        .as_ref()
        .ok_or_else(|| ForgeError::Config("'item' is only available inside for-each steps".to_owned()))?;
    if parts.len() == 1 {
        return value_to_string(val);
    }
    let field = parts.get(1).copied().unwrap_or_default();
    let remaining: Vec<&str> = field.split('.').collect();
    navigate_value(val, &remaining)
}

/// Resolve `network.dnsZone` or `network.pool`.
fn resolve_network(parts: &[&str], ctx: &TemplateContext) -> Result<String, ForgeError> {
    let vars = ctx
        .network
        .as_ref()
        .ok_or_else(|| ForgeError::Config("network variables require spec.network.crossCluster: true".to_owned()))?;
    let field = parts.get(1).copied().unwrap_or_default();
    match field {
        "dnsZone" => Ok(vars.dns_zone.clone()),
        "pool" => vars
            .pool
            .clone()
            .ok_or_else(|| ForgeError::Config("no MetalLB pool allocated for this cluster".to_owned())),
        _ => Err(ForgeError::Config(format!("unknown network field '{field}'"))),
    }
}

/// Walk into a JSON value following dot-separated path segments.
fn navigate_value(val: &serde_json::Value, segments: &[&str]) -> Result<String, ForgeError> {
    if segments.is_empty() {
        return value_to_string(val);
    }
    let key = segments.first().copied().unwrap_or_default();
    let child = val
        .get(key)
        .ok_or_else(|| ForgeError::Config(format!("field '{key}' not found in value")))?;
    navigate_value(child, segments.get(1..).unwrap_or(&[]))
}

/// Convert a scalar JSON value to a string.
fn value_to_string(val: &serde_json::Value) -> Result<String, ForgeError> {
    match val {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::Bool(b) => Ok(b.to_string()),
        _ => Err(ForgeError::Config(format!(
            "cannot convert {kind} to template string",
            kind = value_kind(val)
        ))),
    }
}

/// Return a human-readable kind label for a JSON value.
fn value_kind(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> TemplateContext {
        TemplateContext {
            cluster_name: "hub".to_owned(),
            stack_name: "base".to_owned(),
            properties: BTreeMap::from([
                ("model".to_owned(), serde_json::Value::String("gpt-4".to_owned())),
                ("port".to_owned(), serde_json::json!(8080)),
            ]),
            item: None,
            network: None,
        }
    }

    #[test]
    fn render_cluster_name() {
        let ctx = test_ctx();
        let result = render("ns-{{ cluster.name }}", &ctx).unwrap_or_else(|_| std::process::abort());
        assert_eq!(result, "ns-hub", "should resolve cluster.name");
    }

    #[test]
    fn render_cluster_property() {
        let ctx = test_ctx();
        let result = render("{{ cluster.properties.model }}", &ctx).unwrap_or_else(|_| std::process::abort());
        assert_eq!(result, "gpt-4", "should resolve cluster property");
    }

    #[test]
    fn render_stack_name_and_item() {
        let mut ctx = test_ctx();
        ctx.item = Some(serde_json::Value::String("worker".to_owned()));
        let result = render("{{ stack.name }}-{{ item }}", &ctx).unwrap_or_else(|_| std::process::abort());
        assert_eq!(result, "base-worker", "should resolve stack.name and item");
    }

    #[test]
    fn render_item_field_access() {
        let mut ctx = test_ctx();
        ctx.item = Some(serde_json::json!({"host": "10.0.0.1", "port": 8080}));
        let result = render("{{ item.host }}:{{ item.port }}", &ctx).unwrap_or_else(|_| std::process::abort());
        assert_eq!(result, "10.0.0.1:8080", "should access item fields");
    }

    #[test]
    fn render_missing_variable_returns_error() {
        let ctx = test_ctx();
        assert!(render("{{ cluster.unknown }}", &ctx).is_err(), "unknown cluster field");
        assert!(render("{{ item }}", &ctx).is_err(), "item without for-each context");
        assert!(render("{{ bad.var }}", &ctx).is_err(), "unknown root");
    }

    #[test]
    fn rendered_template_size_is_bounded() {
        let mut ctx = test_ctx();
        ctx.properties.insert(
            "large".to_owned(),
            serde_json::Value::String("x".repeat(MAX_RENDERED_TEMPLATE_BYTES.saturating_add(1))),
        );
        let result = render("{{ cluster.properties.large }}", &ctx);
        assert!(result.is_err(), "oversized rendered template should fail");
    }

    #[test]
    fn render_network_dns_zone() {
        let mut ctx = test_ctx();
        ctx.network = Some(NetworkTemplateVars {
            dns_zone: "forge.test".to_owned(),
            pool: None,
        });
        let result = render("{{ network.dnsZone }}", &ctx).unwrap_or_else(|_| std::process::abort());
        assert_eq!(result, "forge.test", "should resolve network.dnsZone");
    }

    #[test]
    fn render_network_pool() {
        let mut ctx = test_ctx();
        ctx.network = Some(NetworkTemplateVars {
            dns_zone: "forge.test".to_owned(),
            pool: Some("172.18.255.231-172.18.255.250".to_owned()),
        });
        let result = render("{{ network.pool }}", &ctx).unwrap_or_else(|_| std::process::abort());
        assert_eq!(result, "172.18.255.231-172.18.255.250", "should resolve network.pool");
    }

    #[test]
    fn render_network_without_cross_cluster_fails() {
        let ctx = test_ctx();
        assert!(
            render("{{ network.dnsZone }}", &ctx).is_err(),
            "should fail without network context"
        );
    }
}
