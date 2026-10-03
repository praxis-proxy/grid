//! Custom resource definitions for the AI Grid.

/// Add deprecated field aliases to the generated CRD schema.
///
/// The operator deserializes either name into the current Rust field. The
/// aliases remain in the structural schema so the API server preserves
/// legacy resource fields while users migrate to the current names.
pub(crate) fn add_legacy_field_aliases(schema: &mut schemars::Schema, aliases: &[(&str, &str)]) {
    let validation_rules = {
        let Some(properties) = schema
            .as_object_mut()
            .and_then(|object| object.get_mut("properties"))
            .and_then(serde_json::Value::as_object_mut)
        else {
            return;
        };
        aliases
            .iter()
            .filter_map(|(legacy, current)| add_legacy_field(properties, legacy, current))
            .collect()
    };
    append_legacy_validations(schema, validation_rules);
}

/// Add one deprecated property and return its mutual-exclusion validation.
fn add_legacy_field(
    properties: &mut serde_json::Map<String, serde_json::Value>,
    legacy: &str,
    current: &str,
) -> Option<serde_json::Value> {
    let mut alias_schema = properties.get_mut(current).map(|field_schema| {
        if let Some(object) = field_schema.as_object_mut() {
            // The server must not synthesize the new name beside a legacy alias.
            object.remove("default");
        }
        field_schema.clone()
    })?;
    if let Some(alias_object) = alias_schema.as_object_mut() {
        let current_description = alias_object
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        alias_object.insert(
            "description".to_owned(),
            serde_json::Value::String(format!(
                "Deprecated in favor of `{current}`; this legacy field will be removed in a future release. {current_description}"
            )),
        );
    }
    properties.insert(legacy.to_owned(), alias_schema);
    Some(serde_json::json!({
        "rule": format!("!(has(self.{legacy}) && has(self.{current}))"),
        "message": format!("set only `{legacy}` or `{current}`, not both")
    }))
}

/// Append alias validation rules without duplicating existing expressions.
fn append_legacy_validations(schema: &mut schemars::Schema, validation_rules: Vec<serde_json::Value>) {
    if validation_rules.is_empty() {
        return;
    }
    let Some(object) = schema.as_object_mut() else {
        return;
    };
    let rules = object
        .entry("x-kubernetes-validations".to_owned())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let Some(rules) = rules.as_array_mut() else {
        return;
    };
    for rule in validation_rules {
        if !rules.iter().any(|existing| existing.get("rule") == rule.get("rule")) {
            rules.push(rule);
        }
    }
}

/// [`AgentToAgentProvider`] — A2A agents available over the grid.
///
/// [`AgentToAgentProvider`]: agent_to_agent_provider::AgentToAgentProvider
pub mod agent_to_agent_provider;

/// [`AgentToolProvider`] — MCP tool servers available over the grid.
///
/// [`AgentToolProvider`]: agent_tool_provider::AgentToolProvider
pub mod agent_tool_provider;

/// Authentication strategy types shared across providers.
pub mod auth;

/// [`GridNetwork`] — the grid itself, top-level tenancy boundary.
///
/// [`GridNetwork`]: grid_network::GridNetwork
pub mod grid_network;

/// [`GridSite`] — a remote site in the grid.
///
/// [`GridSite`]: grid_site::GridSite
pub mod grid_site;

/// [`InferenceProvider`] — inference backends available over the grid.
///
/// [`InferenceProvider`]: inference_provider::InferenceProvider
pub mod inference_provider;
