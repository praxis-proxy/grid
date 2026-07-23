//! JSON Schema generation for the Forge configuration model.

use crate::config::ForgeConfig;

/// Generate a JSON Schema for [`ForgeConfig`].
pub fn generate() -> serde_json::Value {
    schemars::schema_for!(ForgeConfig).to_value()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_produces_valid_json_object() {
        let schema = generate();
        assert!(schema.is_object(), "schema should be a JSON object");
        let obj = schema.as_object().unwrap_or_else(|| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(
            obj.contains_key("$schema") || obj.contains_key("properties") || obj.contains_key("type"),
            "schema should contain standard JSON Schema fields"
        );
    }
}
