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

- Rust stable 1.96+
- Rust nightly (for `rustfmt`)
- `cargo-audit`, `cargo-deny` (supply chain safety)
- Docker or Podman (for mock servers and kind)
- kind (for integration testing)

## Commands

```console
make build          # workspace build
make check          # type-check only (fast)
make test           # all tests
make test V=1       # tests with --nocapture
make fmt            # format with nightly rustfmt
make lint           # clippy -D warnings + fmt check
make doc            # docs (warnings denied)
make audit          # cargo audit + cargo deny check
make all            # build + fmt + lint + test + audit
```

## Architecture

See `.docs/` for private architecture documentation:

- `.docs/orchestration/` — SWIM membership, CRDT
  state propagation, site lifecycle, capability
  discovery, health monitoring, metrics
- `.docs/networking/` — gateway-to-gateway mTLS,
  gateway-to-API, transport security, site auth,
  connection lifecycle
- `.docs/inference/` — 7-stage routing pipeline,
  backend scoring, provider integration, API
  translation, credential injection, resilience
- `.docs/agentic/` — zero-trust agent networking,
  MCP tool federation, A2A capability discovery,
  OpenShell integration, agent policy model

## Conventions

See `docs/conventions.md` for the full coding
style guide. This project follows the same
conventions as all praxis-proxy repositories.

## Related Repositories

| Project | Purpose |
|---------|---------|
| `praxis` | Gateway data plane |
| `operator` | Kubernetes Gateway API operator |
| `conventions` | Shared lint/config template |
