//! Structured output formatting for CLI results.
//!
//! Supports `--output text` (default) and `--output json`.  JSON
//! output uses a stable envelope so downstream tools can parse it
//! reliably across Forge versions.

use std::io::Write;

use serde::Serialize;

/// Output format selection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum OutputFormat {
    /// Human-readable text (default).
    #[default]
    Text,
    /// Machine-readable JSON envelope.
    Json,
}

/// Stable JSON output envelope.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResult<T> {
    /// Schema version for the output envelope.
    pub api_version: &'static str,
    /// Output kind — always `"CommandResult"`.
    pub kind: &'static str,
    /// `"Success"` or `"Error"`.
    pub status: &'static str,
    /// Command-specific payload.
    pub data: T,
}

/// Output envelope API version.
const OUTPUT_API_VERSION: &str = "forge.praxis.dev/output/v1alpha1";

/// Output envelope kind.
const OUTPUT_KIND: &str = "CommandResult";

/// Build a success envelope around arbitrary data.
pub fn success<T: Serialize>(data: T) -> CommandResult<T> {
    CommandResult {
        api_version: OUTPUT_API_VERSION,
        kind: OUTPUT_KIND,
        status: "Success",
        data,
    }
}

/// Build an error envelope around a message.
pub fn error(message: &str) -> CommandResult<serde_json::Value> {
    CommandResult {
        api_version: OUTPUT_API_VERSION,
        kind: OUTPUT_KIND,
        status: "Error",
        data: serde_json::json!({ "message": message }),
    }
}

/// Write a JSON envelope to the given writer.
///
/// # Errors
///
/// Returns [`std::io::Error`] if writing fails.
pub fn write_json<T: Serialize>(writer: &mut dyn Write, result: &CommandResult<T>) -> std::io::Result<()> {
    let json =
        serde_json::to_string_pretty(result).unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_owned());
    writeln!(writer, "{json}")
}

/// Write a plain text line to the given writer.
///
/// # Errors
///
/// Returns [`std::io::Error`] if writing fails.
pub fn write_text(writer: &mut dyn Write, message: &str) -> std::io::Result<()> {
    writeln!(writer, "{message}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_envelope_has_expected_shape() {
        let result = success(serde_json::json!({"tools": ["kubectl"]}));
        let json = serde_json::to_value(&result).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                serde_json::Value::Null
            }
        });
        assert_eq!(
            json.get("apiVersion").and_then(|v| v.as_str()),
            Some(OUTPUT_API_VERSION),
            "apiVersion mismatch"
        );
        assert_eq!(
            json.get("kind").and_then(|v| v.as_str()),
            Some(OUTPUT_KIND),
            "kind mismatch"
        );
        assert_eq!(
            json.get("status").and_then(|v| v.as_str()),
            Some("Success"),
            "status mismatch"
        );
        assert!(json.get("data").is_some(), "data field should be present");
    }

    #[test]
    fn error_envelope_has_error_status() {
        let result = error("something broke");
        assert_eq!(result.status, "Error", "status should be Error");
    }

    #[test]
    fn write_json_produces_valid_json() {
        let result = success(serde_json::json!({}));
        let mut buf = Vec::new();
        write_json(&mut buf, &result).unwrap_or_else(|_| {
            std::process::abort();
        });
        let text = String::from_utf8_lossy(&buf);
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                serde_json::Value::Null
            }
        });
        assert!(parsed.is_object(), "output should be valid JSON");
    }
}
