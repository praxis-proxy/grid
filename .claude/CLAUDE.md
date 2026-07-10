# CLAUDE.md

This file provides guidance to Claude Code
(claude.ai/code) when working with code in this
repository.

## What This Is

The AI Grid: a decentralized, peer-to-peer network
for AI inference routing and agentic networking
across clusters, cloud providers, and third-party
APIs. Built on Praxis as the gateway data plane.

## Requirements

- Rust stable 1.96+ (edition 2024, resolver 3)
- Rust nightly (for `rustfmt` — `group_imports` and
  `imports_granularity` are nightly-only)
- `cargo-audit`, `cargo-deny` (supply chain safety)
- `cargo-machete` (unused dependency detection)
- Docker or Podman (for mock servers and kind)
- kind (for integration testing)

## Commands

Run from the `grid/` directory:

```console
make build          # workspace build
make check          # type-check only (fast)
make test           # all tests
make test V=1       # tests with --nocapture
make fmt            # format with nightly rustfmt
make lint           # clippy -D warnings + fmt check + machete
make doc            # docs (warnings denied, private items)
make audit          # cargo audit + cargo deny check
make all            # build + fmt + lint + test + audit
```

Single-test and single-crate commands:

```console
cargo test -p grid-certs           # one crate
cargo test -p mock-providers       # one crate
cargo test test_name               # one test by name
cargo test -p grid-certs test_name # one test in one crate
```

Test environment (requires Docker + kind):

```console
cargo xtask env up       # create clusters, providers, certs
cargo xtask env down     # tear down everything
cargo xtask env status   # health of all components
cargo xtask env up -c tests/env/config.toml  # custom config
```

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `grid-certs` | Certificate generation and provider trait for site-to-site mTLS |
| `mock-providers` | Single binary mocking OpenAI, Anthropic, Bedrock, and Vertex AI APIs |
| `xtask` | Dev task runner for multi-cluster test environments (invoked via `cargo xtask`) |

### grid-certs

Key abstraction: `CertificateProvider` trait with
`site_certificate()` and `trust_bundle()`. Designed
for swappable implementations:

- `StaticFileProvider` — loads certs from disk
  (current, for POC/testing)
- Future `SpiffeProvider` — SPIRE workload API
  (production)

`generate_ca()` and `generate_site_cert()` produce
mTLS certs with SANs like
`{site_name}.grid.internal` and both `ServerAuth`
and `ClientAuth` extended key usage.

### mock-providers

Four provider modules each exposing `router()` →
`axum::Router`. Shared utilities in `common.rs`.

- `openai` — `POST /v1/chat/completions`, bearer
  token auth, SSE streaming
- `anthropic` — `POST /v1/messages`, `x-api-key`
  header auth, Anthropic-specific SSE events
- `bedrock` — `POST /model/{id}/converse`, SigV4
  prefix auth, binary event stream (not SSE)
- `vertex` — `POST /v1/projects/.../models/{*rest}`,
  OAuth2 bearer auth (wildcard route because axum
  disallows colons in path params)

### xtask

Single subcommand `env` with `up`/`down`/`status`.
Default config: `tests/env/config.toml`. The `up`
flow: parse TOML → create kind clusters (with
`grid-` prefix) → deploy inference simulators on
provider-role clusters → start mock provider Docker
containers → generate CA + per-cluster certs.

## Architecture

Crate dependency graph is shallow:
- `xtask` → `grid-certs` (cert generation)
- `mock-providers` is standalone
- `grid-certs` is standalone

`.docs/` contains architecture docs for four planned
subsystems (not yet implemented in code):

- **Orchestration** — SWIM membership via `foca`,
  delta CRDTs (LWW Registers, OR-Sets, G-Counters)
  piggybacked on SWIM probes, capability discovery,
  topology-aware gossip
- **Networking** — gateway-to-gateway mTLS,
  gateway-to-API credential injection, AES-256-GCM
  SWIM encryption, connection pooling
- **Inference** — 7-stage routing pipeline: signal
  extraction → semantic classification (Candle
  embeddings) → backend scoring (weighted
  multi-signal) → policy enforcement → API
  translation → credential injection → resilience
  (circuit breaker with fallback chain)
- **Agentic** — zero-trust agent networking, MCP
  tool federation via CRDTs, A2A capability
  discovery, OpenShell sandbox integration,
  three-layer defense model

## Key Conventions

Full conventions in `docs/conventions.md`.

### Lint Discipline

Extremely strict workspace lints in `Cargo.toml`.
Notable denials: `unwrap_used`, `expect_used`,
`panic`, `indexing_slicing`, `unsafe_code`,
`missing_docs`, `missing_docs_in_private_items`.
Any `#[allow(...)]` requires `reason = "..."`.

`clippy.toml`: `too-many-lines-threshold = 30`,
`too-many-arguments-threshold = 5`. Disallows
`std::thread::sleep` and `std::io::stdin`.

### Error Handling

`unwrap_used` and `expect_used` are denied. Use `?`
propagation, match, or `unwrap_or_else`. In tests,
use `unwrap_or_else(|_| std::process::abort())` or
return `Result`.

### Type Design

Make invalid states unrepresentable. Enums over
strings, structs over maps,
`#[serde(deny_unknown_fields)]` by default,
`#[serde(try_from)]` for constrained numerics.

### Test Organization

- Inline `#[cfg(test)] mod tests` blocks
- Order: imports → test functions → test utilities
  (with `// Test Utilities` separator, not "Helpers")
- One full-width separator marks where tests begin;
  no per-test separators
- No comments in test bodies — use assertion messages
  or `tracing` calls instead
- No doc comments on test functions (exception: RFC
  conformance citing RFC number and section)
- Async tests: `#[tokio::test]`
- Mock provider tests: `tower::ServiceExt::oneshot()`

### Separator Comments

Full-width only (77 dashes):

```rust
// -----------------------------------------------------------
// Section Name
// -----------------------------------------------------------
```

### Documentation

All items (public and private) need `///` doc
comments. Prose covers intent and interface only.
Prefer ample doctests. Use reference-style rustdoc
links.

## Related Repositories

| Project | Purpose |
|---------|---------|
| `praxis` | Gateway data plane |
| `operator` | Kubernetes Gateway API operator |
| `conventions` | Shared lint/config template |
