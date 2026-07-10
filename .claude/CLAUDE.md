# CLAUDE.md

This file provides guidance to Claude Code
(claude.ai/code) when working with code in this
repository.

## What This Is

The AI Grid: a decentralized, peer-to-peer network
for AI inference routing and agentic networking
across clusters, cloud providers, and third-party
APIs. The Grid Operator orchestrates mesh formation,
trust, capability discovery, and routing — while
Praxis AI (from `../ai/`) handles all data-plane
traffic as the gateway.

## Architecture

The Grid Operator is an orchestration daemon, NOT a
proxy. It manages:
- SWIM membership via `foca` (peer discovery)
- mTLS certificate lifecycle (trust establishment)
- CRDT state propagation (capabilities, metrics)
- Praxis overlay config generation (routing decisions)

Praxis AI handles:
- Request proxying, API translation, credentials
- Filter pipeline execution
- TLS termination, health checks, connection pooling

See `.docs/operator/architecture.md` for the full
design: CRDs, controllers, operational walkthrough,
scoring model, and auth framework. See
`docs/conventions.md` for coding style and policies.

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
cargo test -p scoring            # one crate
cargo test -p mock-providers       # one crate
cargo test test_name               # one test by name
```

Test environment (requires Docker + kind):

```console
cargo xtask env up       # create clusters, providers, certs
cargo xtask env down     # tear down everything
cargo xtask env status   # health of all components
```

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `scoring` | Scoring engine, backend types, grid state |
| `certs` | Certificate generation and provider trait for mTLS |
| `mock-providers` | Single binary mocking OpenAI, Anthropic, Bedrock, Vertex APIs |
| `xtask` | Dev task runner for multi-cluster test environments |

### scoring

Six-signal scoring engine:

| Signal | Weight | Source |
|--------|--------|--------|
| locality | 3.0 | config (region-aware) |
| queue_depth | 3.0 | Prometheus / CRDT |
| kv_cache | 2.0 | Prometheus / CRDT |
| prefix_cache | 2.0 | Prometheus / CRDT |
| latency | 2.0 | local measurement |
| cost | 1.0 | config |

Locality: Local=1.0, same-region Remote=0.7,
cross-region Remote=0.4, CloudManaged=0.2,
ApiProvider=0.1.

### certs

`CertificateProvider` trait with `StaticFileProvider`
(current) and planned `SpiffeProvider` (production).
`generate_ca()` and `generate_site_cert()` produce
mTLS certs with DNS SANs and dual EKU.

### mock-providers

Four provider modules each exposing `router()` →
`axum::Router`:

- `openai` — Bearer token auth, SSE streaming
- `anthropic` — `x-api-key` auth, Anthropic SSE events
- `bedrock` — SigV4 prefix auth, binary event stream
- `vertex` — OAuth2 bearer auth, wildcard route

## Planned Crates

| Crate | Purpose |
|-------|---------|
| `operator` | K8s controllers, CRDs, operator binary |
| `swim` | foca wrapper, SWIM runtime, encryption |
| `crdt` | Delta CRDT types (LWW, OR-Set, G-Counter) |

## Key Conventions

Full conventions in `docs/conventions.md`.

### Lint Discipline

Extremely strict workspace lints in `Cargo.toml`.
Notable denials: `unwrap_used`, `expect_used`,
`panic`, `indexing_slicing`, `unsafe_code`,
`missing_docs`, `missing_docs_in_private_items`.
Any `#[allow(...)]` requires `reason = "..."`.

### Error Handling

Use `?` propagation, match, or `unwrap_or_else`. In
tests, use `unwrap_or_else(|_| std::process::abort())`
or return `Result`.

### Test Organization

- Inline `#[cfg(test)] mod tests` blocks
- Order: imports → tests → test utilities
  (with `// Test Utilities` separator)
- No comments in test bodies — use assertion messages
- Async tests: `#[tokio::test]`

### Separator Comments

Full-width only (77 dashes).

### Documentation

All items need `///` doc comments. Prose covers intent
and interface only. Prefer ample doctests. Use
reference-style rustdoc links.

### Commit Attribution

Commits are attributed to people, never to tools. Do
not add `Co-Authored-By` lines for development tools
(e.g. linters, generators, formatters). The human who
reviews and submits the code is the author.

## Related Repositories

| Project | Purpose |
|---------|---------|
| `ai` | AI-enabled Praxis proxy (data plane) |
| `praxis` | Gateway framework |
| `operator` | Kubernetes Gateway API operator |
