//! Integration tests for the `forge` crate.
//!
//! These tests exercise the public API across module boundaries,
//! verifying that configuration loading, validation, and CLI
//! argument parsing work correctly end-to-end.

#![allow(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]

use clap::Parser as _;
use forge::{cli::Cli, config, config::validate};

#[test]
fn cli_recognizes_help_flag() {
    let result = Cli::try_parse_from(["praxis-forge", "--help"]);
    assert!(result.is_err(), "--help should produce a help error");
    if let Err(e) = result {
        assert_eq!(
            e.kind(),
            clap::error::ErrorKind::DisplayHelp,
            "should produce help output"
        );
    }
}

#[test]
fn cli_accepts_global_options_after_subcommands() {
    let result = Cli::try_parse_from([
        "praxis-forge",
        "config",
        "init",
        "--config",
        "/tmp/forge.yaml",
        "--dry-run",
    ]);
    assert!(result.is_ok(), "global options should be accepted after subcommands");
}

#[test]
fn config_validate_succeeds_with_valid_yaml() {
    let dir = tempfile::tempdir().unwrap_or_else(|_| {
        std::process::abort();
        #[expect(unreachable_code, reason = "abort prevents reaching this")]
        {
            unreachable!()
        }
    });
    let path = dir.path().join("forge.yaml");
    std::fs::write(&path, config::minimal_yaml()).unwrap_or_else(|_| std::process::abort());

    let cfg = config::load(&path).unwrap_or_else(|_| {
        std::process::abort();
        #[expect(unreachable_code, reason = "abort prevents reaching this")]
        {
            unreachable!()
        }
    });
    let result = validate::validate(&cfg);
    assert!(result.is_ok(), "valid config should pass validation");
}

#[test]
fn config_validate_fails_with_invalid_yaml() {
    let dir = tempfile::tempdir().unwrap_or_else(|_| {
        std::process::abort();
        #[expect(unreachable_code, reason = "abort prevents reaching this")]
        {
            unreachable!()
        }
    });
    let path = dir.path().join("forge.yaml");
    let bad_yaml = "apiVersion: wrong/v1\nkind: Wrong\n";
    std::fs::write(&path, bad_yaml).unwrap_or_else(|_| std::process::abort());

    let result = config::load(&path).and_then(|cfg| validate::validate(&cfg));
    assert!(result.is_err(), "invalid config should fail: {result:?}");
}
