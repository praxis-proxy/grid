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
    strip_validation_rules(&mut alias_schema);
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

/// Remove nested CEL rules except mutual-exclusion rules from a legacy alias schema.
///
/// The canonical field retains the validation rules. Copying every nested rule
/// into every legacy alias can exceed Kubernetes' CRD validation-cost budget,
/// especially when one aliased object contains other aliased objects. The
/// mutual-exclusion rules must remain so the API server rejects old and new
/// names together before serde sees duplicate aliases.
fn strip_validation_rules(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for entry in values {
                strip_validation_rules(entry);
            }
        },
        serde_json::Value::Object(object) => {
            if let Some(rules) = object
                .get_mut("x-kubernetes-validations")
                .and_then(serde_json::Value::as_array_mut)
            {
                rules.retain(is_mutual_exclusion_rule);
            }
            let remove_empty_rules = object
                .get("x-kubernetes-validations")
                .and_then(serde_json::Value::as_array)
                .is_some_and(Vec::is_empty);
            if remove_empty_rules {
                object.remove("x-kubernetes-validations");
            }
            for entry in object.values_mut() {
                strip_validation_rules(entry);
            }
        },
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {},
    }
}

/// Whether a CEL rule rejects two mutually exclusive fields.
fn is_mutual_exclusion_rule(rule: &serde_json::Value) -> bool {
    rule.get("rule")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|expression| expression.starts_with("!(has(self.") && expression.contains(") && has(self."))
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

/// [`AgentToolProvider`] for MCP tool servers available over the grid.
///
/// [`AgentToolProvider`]: agent_tool_provider::AgentToolProvider
pub mod agent_tool_provider;

/// Authentication strategy types shared across providers.
pub mod auth;

/// [`GridNetwork`] for the grid itself, the top-level tenancy boundary.
///
/// [`GridNetwork`]: grid_network::GridNetwork
pub mod grid_network;

/// [`GridSite`] for a remote site in the grid.
///
/// [`GridSite`]: grid_site::GridSite
pub mod grid_site;

/// [`SiteInvite`] admission API on the signer cluster.
///
/// [`SiteInvite`]: site_invite::SiteInvite
pub mod site_invite;

/// [`InferenceProvider`] for inference backends available over the grid.
///
/// [`InferenceProvider`]: inference_provider::InferenceProvider
pub mod inference_provider;
