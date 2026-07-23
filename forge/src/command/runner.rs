//! Mockable command execution abstraction.
//!
//! External tool invocations go through [`CommandRunner`] so tests
//! can inject a `MockRunner` and verify calls without side effects.

use std::{collections::BTreeMap, ffi::OsString, fmt};

use crate::error::ForgeError;

/// Abstraction over external command execution.
pub trait CommandRunner {
    /// Execute an external command and return its output.
    ///
    /// # Errors
    ///
    /// Returns [`ForgeError`] if the command fails to execute.
    fn run(&self, spec: &CommandSpec) -> Result<CommandOutput, ForgeError>;
}

/// Specification for a single external command invocation.
#[derive(Clone, Debug)]
pub struct CommandSpec {
    /// Program to execute.
    pub program: OsString,
    /// Command-line arguments.
    pub args: Vec<OsString>,
    /// Environment variables to set.
    pub env: BTreeMap<OsString, OsString>,
    /// Optional standard input bytes.
    pub stdin: Option<Vec<u8>>,
    /// Values that must not appear in display output.
    pub redact: Vec<Redaction>,
}

/// A value to redact from display output.
#[derive(Clone, Debug)]
pub struct Redaction {
    /// What kind of value is being redacted.
    pub kind: RedactionKind,
    /// The literal value to replace with `[REDACTED]`.
    pub value: OsString,
}

/// Classification of redacted values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RedactionKind {
    /// A command-line argument.
    Arg,
    /// An environment variable value.
    EnvValue,
}

/// Output from a completed command.
#[derive(Clone, Debug)]
pub struct CommandOutput {
    /// Process exit code (0 = success).
    pub status: i32,
    /// Captured standard output.
    pub stdout: String,
    /// Captured standard error.
    pub stderr: String,
}

impl fmt::Display for CommandSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.program.to_string_lossy())?;
        for arg in &self.args {
            write!(f, " {}", redact_value(arg, &self.redact))?;
        }
        Ok(())
    }
}

/// Replace a value with `[REDACTED]` if it matches any redaction.
fn redact_value(value: &OsString, redactions: &[Redaction]) -> String {
    let s = value.to_string_lossy();
    for r in redactions {
        if *value == r.value {
            return "[REDACTED]".to_owned();
        }
    }
    s.into_owned()
}

// -----------------------------------------------------------------
// System runner (real process execution)
// -----------------------------------------------------------------

/// Real command runner that executes via [`std::process::Command`].
pub struct SystemRunner;

impl CommandRunner for SystemRunner {
    fn run(&self, spec: &CommandSpec) -> Result<CommandOutput, ForgeError> {
        let mut cmd = build_process(spec);
        let output = run_process(&mut cmd, spec)?;
        Ok(into_command_output(&output))
    }
}

/// Build a [`std::process::Command`] from a [`CommandSpec`].
fn build_process(spec: &CommandSpec) -> std::process::Command {
    let mut cmd = std::process::Command::new(&spec.program);
    cmd.args(&spec.args);
    for (k, v) in &spec.env {
        cmd.env(k, v);
    }
    configure_stdio(&mut cmd, spec.stdin.is_some());
    cmd
}

/// Set up process stdio handles.
fn configure_stdio(cmd: &mut std::process::Command, pipe_stdin: bool) {
    if pipe_stdin {
        cmd.stdin(std::process::Stdio::piped());
    }
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
}

/// Execute a prepared command, optionally piping stdin data.
fn run_process(cmd: &mut std::process::Command, spec: &CommandSpec) -> Result<std::process::Output, ForgeError> {
    match &spec.stdin {
        Some(data) => run_with_stdin(cmd, data, spec),
        None => cmd.output().map_err(|e| command_error(spec, &e)),
    }
}

/// Spawn a child process and write data to its standard input.
fn run_with_stdin(
    cmd: &mut std::process::Command,
    data: &[u8],
    spec: &CommandSpec,
) -> Result<std::process::Output, ForgeError> {
    let mut child = cmd.spawn().map_err(|e| command_error(spec, &e))?;
    pipe_stdin(&mut child, data, spec)?;
    child.wait_with_output().map_err(|e| command_error(spec, &e))
}

/// Write data to a child process's stdin and close the handle.
fn pipe_stdin(child: &mut std::process::Child, data: &[u8], spec: &CommandSpec) -> Result<(), ForgeError> {
    use std::io::Write as _;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(data).map_err(|e| command_error(spec, &e))?;
    }
    Ok(())
}

/// Convert a process output reference into a [`CommandOutput`].
fn into_command_output(output: &std::process::Output) -> CommandOutput {
    CommandOutput {
        status: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// Build a [`ForgeError::Command`] from a spec and IO error.
fn command_error(spec: &CommandSpec, err: &std::io::Error) -> ForgeError {
    ForgeError::Command {
        program: spec.program.to_string_lossy().into_owned(),
        message: err.to_string(),
    }
}

// -----------------------------------------------------------------
// Mock runner for tests
// -----------------------------------------------------------------

/// A test-only command runner that records calls.
#[cfg(any(test, feature = "test-support"))]
pub struct MockRunner {
    /// Canned responses keyed by program name.
    responses: BTreeMap<String, CommandOutput>,
}

#[cfg(any(test, feature = "test-support"))]
impl Default for MockRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test-support"))]
impl MockRunner {
    /// Create a new mock runner with no responses.
    pub fn new() -> Self {
        Self {
            responses: BTreeMap::new(),
        }
    }

    /// Register a canned response for a program.
    pub fn respond(&mut self, program: &str, output: CommandOutput) -> &mut Self {
        self.responses.insert(program.to_owned(), output);
        self
    }
}

#[cfg(any(test, feature = "test-support"))]
impl CommandRunner for MockRunner {
    /// Look up by display string first, then by program name alone.
    fn run(&self, spec: &CommandSpec) -> Result<CommandOutput, ForgeError> {
        let display = format!("{spec}");
        if let Some(output) = self.responses.get(&display) {
            return Ok(output.clone());
        }
        let program = spec.program.to_string_lossy();
        self.responses
            .get(program.as_ref())
            .cloned()
            .ok_or_else(|| ForgeError::Command {
                program: program.into_owned(),
                message: "not found".to_owned(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_redacts_sensitive_args() {
        let spec = CommandSpec {
            program: "helm".into(),
            args: vec!["install".into(), "s3cr3t-token".into()],
            env: BTreeMap::new(),
            stdin: None,
            redact: vec![Redaction {
                kind: RedactionKind::Arg,
                value: "s3cr3t-token".into(),
            }],
        };
        let display = format!("{spec}");
        assert!(display.contains("[REDACTED]"), "should redact arg, got: {display}");
        assert!(!display.contains("s3cr3t"), "secret should not appear, got: {display}");
    }

    #[test]
    fn display_preserves_non_redacted_args() {
        let spec = CommandSpec {
            program: "kubectl".into(),
            args: vec!["get".into(), "pods".into()],
            env: BTreeMap::new(),
            stdin: None,
            redact: Vec::new(),
        };
        let display = format!("{spec}");
        assert_eq!(display, "kubectl get pods");
    }

    #[test]
    fn mock_runner_returns_canned_response() {
        let mut runner = MockRunner::new();
        runner.respond(
            "kubectl",
            CommandOutput {
                status: 0,
                stdout: "/usr/bin/kubectl".to_owned(),
                stderr: String::new(),
            },
        );
        let spec = CommandSpec {
            program: "kubectl".into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            stdin: None,
            redact: Vec::new(),
        };
        let result = runner.run(&spec).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(result.status, 0, "status should be 0");
    }

    #[test]
    fn mock_runner_returns_error_for_unknown_program() {
        let runner = MockRunner::new();
        let spec = CommandSpec {
            program: "nonexistent".into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            stdin: None,
            redact: Vec::new(),
        };
        let Err(err) = runner.run(&spec) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("not found"), "expected not-found error, got: {msg}");
    }
}
