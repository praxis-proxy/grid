//! Integration tests for the `forge` crate.
//!
//! These tests exercise the public API across module boundaries,
//! verifying that configuration loading, validation, and CLI
//! argument parsing work correctly end-to-end.

#![allow(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]

use clap::Parser as _;
use forge::{
    cli::Cli,
    config,
    config::{RuntimeProvider, validate},
};

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

// ---------------------------------------------------------------
// F2 CLI parsing tests
// ---------------------------------------------------------------

#[test]
fn cli_accepts_up_command() {
    let result = Cli::try_parse_from(["praxis-forge", "up"]);
    assert!(result.is_ok(), "up command should parse: {result:?}");
}

#[test]
fn cli_accepts_up_with_dry_run() {
    let result = Cli::try_parse_from(["praxis-forge", "up", "--dry-run"]);
    assert!(result.is_ok(), "up --dry-run should parse: {result:?}");
}

#[test]
fn cli_accepts_runtime_override() {
    let cli = Cli::try_parse_from(["praxis-forge", "--runtime", "podman", "up"]).unwrap_or_else(|_| {
        std::process::abort();
        #[expect(unreachable_code, reason = "abort prevents reaching this")]
        {
            unreachable!()
        }
    });
    assert_eq!(
        cli.global.runtime,
        Some(RuntimeProvider::Podman),
        "runtime override should parse"
    );
}

#[test]
fn cli_accepts_down_command() {
    let result = Cli::try_parse_from(["praxis-forge", "down"]);
    assert!(result.is_ok(), "down command should parse: {result:?}");
}

#[test]
fn cli_accepts_down_with_force() {
    let result = Cli::try_parse_from(["praxis-forge", "down", "--force"]);
    assert!(result.is_ok(), "down --force should parse: {result:?}");
}

#[test]
fn cli_accepts_status_command() {
    let result = Cli::try_parse_from(["praxis-forge", "status"]);
    assert!(result.is_ok(), "status command should parse: {result:?}");
}

#[test]
fn cli_accepts_cluster_create() {
    let result = Cli::try_parse_from(["praxis-forge", "cluster", "create", "hub"]);
    assert!(result.is_ok(), "cluster create should parse: {result:?}");
}

#[test]
fn cli_accepts_cluster_delete_with_force() {
    let result = Cli::try_parse_from(["praxis-forge", "cluster", "delete", "hub", "--force"]);
    assert!(result.is_ok(), "cluster delete --force should parse: {result:?}");
}

#[test]
fn cli_accepts_cluster_list() {
    let result = Cli::try_parse_from(["praxis-forge", "cluster", "list"]);
    assert!(result.is_ok(), "cluster list should parse: {result:?}");
}

#[test]
fn cli_accepts_cluster_kubeconfig() {
    let result = Cli::try_parse_from(["praxis-forge", "cluster", "kubeconfig", "hub"]);
    assert!(result.is_ok(), "cluster kubeconfig should parse: {result:?}");
}

#[test]
fn cli_accepts_cluster_load_image() {
    let result = Cli::try_parse_from(["praxis-forge", "cluster", "load-image", "hub", "my-image:v1"]);
    assert!(result.is_ok(), "cluster load-image should parse: {result:?}");
}

#[test]
fn cli_accepts_cluster_kubectl() {
    let result = Cli::try_parse_from(["praxis-forge", "cluster", "kubectl", "hub", "--", "get", "pods"]);
    assert!(result.is_ok(), "cluster kubectl should parse: {result:?}");
}

// ---------------------------------------------------------------
// F3 CLI parsing tests
// ---------------------------------------------------------------

#[test]
fn cli_accepts_service_list() {
    let result = Cli::try_parse_from(["praxis-forge", "service", "list"]);
    assert!(result.is_ok(), "service list should parse: {result:?}");
}

#[test]
fn cli_accepts_service_start() {
    let result = Cli::try_parse_from(["praxis-forge", "service", "start", "edge"]);
    assert!(result.is_ok(), "service start should parse: {result:?}");
}

#[test]
fn cli_accepts_service_stop() {
    let result = Cli::try_parse_from(["praxis-forge", "service", "stop", "edge"]);
    assert!(result.is_ok(), "service stop should parse: {result:?}");
}

// ---------------------------------------------------------------
// F4 CLI parsing tests
// ---------------------------------------------------------------

#[test]
fn cli_accepts_stack_list() {
    let result = Cli::try_parse_from(["praxis-forge", "stack", "list"]);
    assert!(result.is_ok(), "stack list should parse: {result:?}");
}

#[test]
fn cli_accepts_stack_plan() {
    let result = Cli::try_parse_from(["praxis-forge", "stack", "plan", "hub"]);
    assert!(result.is_ok(), "stack plan should parse: {result:?}");
}

#[test]
fn cli_accepts_stack_apply_with_filter() {
    let result = Cli::try_parse_from(["praxis-forge", "stack", "apply", "hub", "base"]);
    assert!(result.is_ok(), "stack apply with filter should parse: {result:?}");
}

#[test]
fn cli_accepts_stack_status() {
    let result = Cli::try_parse_from(["praxis-forge", "stack", "status"]);
    assert!(result.is_ok(), "stack status should parse: {result:?}");
}
