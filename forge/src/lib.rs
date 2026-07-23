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
//! # Current status (F1)
//!
//! F1 is the initial foundation: CLI skeleton, configuration model,
//! validation, and read-only commands (`doctor`, `plan`, `config`).
//! The only mutation is `config init`, which writes a minimal
//! `forge.yaml`.

pub mod cli;
pub mod command;
pub mod config;
pub mod error;
pub mod output;
