//! The `config` subcommands: validate, show, init, schema.
//!
//! All commands except `init` are read-only.  `init` writes a single
//! file and is the only F1 command allowed to create or modify
//! filesystem state.

use std::{io::Write, path::Path};

use crate::{
    config,
    config::validate,
    error::ForgeError,
    output::{self, OutputFormat},
};

/// Run `config validate`.
///
/// # Errors
///
/// Returns [`ForgeError`] if the operation fails.
pub fn run_validate(config_path: &Path, format: &OutputFormat, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let cfg = config::load(config_path)?;
    validate::validate(&cfg)?;
    report_valid(config_path, format, writer)
}

/// Report that validation passed.
fn report_valid(config_path: &Path, format: &OutputFormat, writer: &mut dyn Write) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => {
            let msg = format!("{}: valid", config_path.display());
            let result = output::success(serde_json::json!({
                "valid": true,
                "message": msg,
            }));
            output::write_json(writer, &result)?;
        },
        OutputFormat::Text => {
            output::write_text(writer, &format!("{}: valid", config_path.display()))?;
        },
    }
    Ok(())
}

/// Run `config show`.
///
/// # Errors
///
/// Returns [`ForgeError`] if the operation fails.
pub fn run_show(
    config_path: &Path,
    resolved: bool,
    format: &OutputFormat,
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    let cfg = config::load(config_path)?;
    if resolved {
        emit_resolved_note(format, writer)?;
    }
    emit_config(&cfg, format, writer)
}

/// Note that `--resolved` has no effect in F1.
fn emit_resolved_note(format: &OutputFormat, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let msg = "note: template expansion is not implemented; \
               --resolved output is identical to parsed config";
    match format {
        OutputFormat::Json => {},
        OutputFormat::Text => output::write_text(writer, msg)?,
    }
    Ok(())
}

/// Emit the parsed config.
fn emit_config(cfg: &config::ForgeConfig, format: &OutputFormat, writer: &mut dyn Write) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => {
            let result = output::success(cfg);
            output::write_json(writer, &result)?;
        },
        OutputFormat::Text => {
            let yaml = serde_yaml::to_string(cfg)?;
            output::write_text(writer, &yaml)?;
        },
    }
    Ok(())
}

/// Run `config init`.
///
/// # Errors
///
/// Returns [`ForgeError`] if the operation fails.
pub fn run_init(
    config_path: &Path,
    force: bool,
    dry_run: bool,
    format: &OutputFormat,
    writer: &mut dyn Write,
) -> Result<(), ForgeError> {
    if config_path.exists() && !force {
        return Err(ForgeError::Config(format!(
            "{} already exists (use --force to overwrite)",
            config_path.display(),
        )));
    }
    if dry_run {
        return report_init_dry_run(config_path, format, writer);
    }
    std::fs::write(config_path, config::minimal_yaml())?;
    report_init_success(config_path, format, writer)
}

/// Report what `init` would write in dry-run mode.
fn report_init_dry_run(config_path: &Path, format: &OutputFormat, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let msg = format!("would write {}", config_path.display());
    match format {
        OutputFormat::Json => {
            let result = output::success(serde_json::json!({
                "path": config_path.display().to_string(),
                "dryRun": true,
            }));
            output::write_json(writer, &result)?;
        },
        OutputFormat::Text => output::write_text(writer, &msg)?,
    }
    Ok(())
}

/// Report that init succeeded.
fn report_init_success(config_path: &Path, format: &OutputFormat, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let msg = format!("wrote {}", config_path.display());
    match format {
        OutputFormat::Json => {
            let result = output::success(serde_json::json!({
                "path": config_path.display().to_string(),
            }));
            output::write_json(writer, &result)?;
        },
        OutputFormat::Text => output::write_text(writer, &msg)?,
    }
    Ok(())
}

/// Run `config schema`.
///
/// # Errors
///
/// Returns [`ForgeError`] if the operation fails.
pub fn run_schema(writer: &mut dyn Write) -> Result<(), ForgeError> {
    let schema = config::schema::generate();
    let json = serde_json::to_string_pretty(&schema).map_err(|e| ForgeError::Config(e.to_string()))?;
    output::write_text(writer, &json)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_init_writes_file() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let path = dir.path().join("forge.yaml");
        let mut buf = Vec::new();
        run_init(&path, false, false, &OutputFormat::Text, &mut buf).unwrap_or_else(|_| std::process::abort());
        assert!(path.exists(), "forge.yaml should be created");
    }

    #[test]
    fn config_init_refuses_overwrite_without_force() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let path = dir.path().join("forge.yaml");
        std::fs::write(&path, "existing").unwrap_or_else(|_| std::process::abort());
        let mut buf = Vec::new();
        let Err(err) = run_init(&path, false, false, &OutputFormat::Text, &mut buf) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("already exists"), "expected overwrite error, got: {msg}");
    }

    #[test]
    fn config_init_overwrites_with_force() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let path = dir.path().join("forge.yaml");
        std::fs::write(&path, "old").unwrap_or_else(|_| std::process::abort());
        let mut buf = Vec::new();
        run_init(&path, true, false, &OutputFormat::Text, &mut buf).unwrap_or_else(|_| std::process::abort());
        let content = std::fs::read_to_string(&path).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                String::new()
            }
        });
        assert!(content.contains("forge.praxis.dev"), "should contain new content");
    }

    #[test]
    fn config_init_dry_run_does_not_write_file() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let path = dir.path().join("forge.yaml");
        let mut buf = Vec::new();
        run_init(&path, false, true, &OutputFormat::Text, &mut buf).unwrap_or_else(|_| std::process::abort());
        assert!(!path.exists(), "dry-run must not create forge.yaml");
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("would write"), "expected dry-run output: {text}");
    }

    #[test]
    fn config_schema_produces_valid_json() {
        let mut buf = Vec::new();
        run_schema(&mut buf).unwrap_or_else(|_| std::process::abort());
        let text = String::from_utf8_lossy(&buf);
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                serde_json::Value::Null
            }
        });
        assert!(parsed.is_object(), "schema should be valid JSON object");
    }
}
