//! Forge: generic development-environment orchestrator for Kubernetes.
//!
//! Forge is a standalone CLI for composing multi-cluster Kubernetes
//! development environments from a single YAML configuration.  It is
//! not tied to any specific project — it can be used with any
//! Kubernetes workload that benefits from reproducible multi-cluster
//! local environments.
//!
//! # Scope
//!
//! Forge manages:
//! - Kind cluster lifecycle
//! - Host-level container services
//! - Certificate generation and distribution
//! - Composable deployment stacks
//! - Cross-cluster networking
//!
//! Forge does **not** perform project-specific assertions, CRD
//! validation, or operator testing.  Those responsibilities belong
//! to the consuming project's own test harness.
//!
//! # Current status (F4)
//!
//! F4 adds the generic stack execution engine: composable deployment
//! stacks applied to KIND clusters via templates and sequential step
//! execution, with `stack list/plan/apply/status` subcommands.

pub mod cli;
pub mod cluster;
pub mod command;
pub mod config;
pub mod context;
pub mod error;
pub mod networking;
pub mod output;
pub mod runtime;
pub mod service;
pub mod stack;
pub mod state;
