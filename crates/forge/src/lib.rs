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
//! - Kind cluster lifecycle (`up`/`down`/`cluster` subcommands)
//! - Host-level container services (`service start`/`stop`/`logs`)
//! - Composable deployment stacks (`stack apply`/`plan`/`status`)
//! - Cross-cluster Docker networking
//! - Template-based manifest rendering with capture variables
//! - Persistent state under `.forge/`
//!
//! Forge does **not** perform project-specific assertions, CRD
//! validation, or operator testing.  Those responsibilities belong
//! to the consuming project's own test harness.
//!
//! Certificate generation/distribution and image building are
//! planned but not yet implemented.
//!
//! Cross-cluster networking requires Docker; Podman support is
//! limited to single-cluster environments.

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
