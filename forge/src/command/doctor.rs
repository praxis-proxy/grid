//! The `doctor` command: read-only tool availability check.
//!
//! Probes `PATH` for required and optional external tools without
//! creating, modifying, or deleting any resources.

use std::{collections::BTreeMap, io::Write};

use crate::{
    command::runner::{CommandOutput, CommandRunner, CommandSpec},
    error::ForgeError,
    output::{self, OutputFormat},
};

/// Tools that `doctor` probes for.
const TOOLS: &[ToolProbe] = &[
    ToolProbe {
        name: "docker",
        required: false,
    },
    ToolProbe {
        name: "podman",
        required: false,
    },
    ToolProbe {
        name: "kind",
        required: true,
    },
    ToolProbe {
        name: "kubectl",
        required: true,
    },
    ToolProbe {
        name: "helm",
        required: false,
    },
];

/// Metadata about one tool to probe.
struct ToolProbe {
    /// Tool name (also the binary name).
    name: &'static str,
    /// Whether the tool is required for basic operation.
    required: bool,
}

/// Result of probing a single tool.
#[derive(serde::Serialize)]
struct ToolStatus {
    /// Tool name.
    name: String,
    /// Whether the tool was found in `PATH`.
    found: bool,
    /// Whether the tool is required.
    required: bool,
    /// Path to the binary, if found.
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
}

/// Run the `doctor` command.
///
/// # Errors
///
/// Returns [`ForgeError`] if the operation fails.
pub fn run(runner: &dyn CommandRunner, format: &OutputFormat, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let results = probe_tools(runner);
    render_results(&results, format, writer)
}

/// Probe all tools and collect results.
fn probe_tools(runner: &dyn CommandRunner) -> Vec<ToolStatus> {
    TOOLS.iter().map(|t| probe_one(runner, t)).collect()
}

/// Probe one tool by running `which <name>`.
fn probe_one(runner: &dyn CommandRunner, tool: &ToolProbe) -> ToolStatus {
    let spec = which_spec(tool.name);
    match runner.run(&spec) {
        Ok(out) if out.status == 0 => found_status(tool, &out),
        _ => missing_status(tool),
    }
}

/// Build a `which <name>` command spec.
fn which_spec(name: &str) -> CommandSpec {
    CommandSpec {
        program: "which".into(),
        args: vec![name.into()],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a found-tool result.
fn found_status(tool: &ToolProbe, out: &CommandOutput) -> ToolStatus {
    ToolStatus {
        name: tool.name.to_owned(),
        found: true,
        required: tool.required,
        path: Some(out.stdout.trim().to_owned()),
    }
}

/// Build a missing-tool result.
fn missing_status(tool: &ToolProbe) -> ToolStatus {
    ToolStatus {
        name: tool.name.to_owned(),
        found: false,
        required: tool.required,
        path: None,
    }
}

/// Render results in the requested format.
fn render_results(results: &[ToolStatus], format: &OutputFormat, writer: &mut dyn Write) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => render_json(results, writer),
        OutputFormat::Text => render_text(results, writer),
    }
}

/// Render results as JSON.
fn render_json(results: &[ToolStatus], writer: &mut dyn Write) -> Result<(), ForgeError> {
    let result = output::success(serde_json::json!({ "tools": results }));
    output::write_json(writer, &result)?;
    Ok(())
}

/// Render results as human-readable text.
fn render_text(results: &[ToolStatus], writer: &mut dyn Write) -> Result<(), ForgeError> {
    for tool in results {
        let icon = if tool.found { "ok" } else { "MISSING" };
        let req = if tool.required { " (required)" } else { "" };
        let path = tool.path.as_deref().map(|p| format!(" -> {p}")).unwrap_or_default();
        output::write_text(writer, &format!("  {icon}: {}{req}{path}", tool.name))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::runner::{CommandOutput, MockRunner};

    /// Build a mock runner with kubectl and kind present.
    fn mock_with_tools() -> MockRunner {
        let mut runner = MockRunner::new();
        let ok = CommandOutput {
            status: 0,
            stdout: "/usr/bin/kubectl\n".to_owned(),
            stderr: String::new(),
        };
        runner.respond("which kubectl", ok.clone());
        runner.respond("which kind", ok);
        runner
    }

    #[test]
    fn doctor_reports_found_and_missing_tools() {
        let runner = mock_with_tools();
        let mut buf = Vec::new();
        run(&runner, &OutputFormat::Text, &mut buf).unwrap_or_else(|_| std::process::abort());
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("ok: kubectl"), "kubectl should be found: {text}");
        assert!(text.contains("MISSING: docker"), "docker should be missing: {text}");
    }

    #[test]
    fn doctor_json_output_has_tools_array() {
        let runner = mock_with_tools();
        let mut buf = Vec::new();
        run(&runner, &OutputFormat::Json, &mut buf).unwrap_or_else(|_| std::process::abort());
        let text = String::from_utf8_lossy(&buf);
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                serde_json::Value::Null
            }
        });
        assert!(
            parsed
                .get("data")
                .and_then(|d| d.get("tools"))
                .and_then(|t| t.as_array())
                .is_some(),
            "should have data.tools array"
        );
    }
}
