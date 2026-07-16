# CI Kind E2E Implementation

This document describes the CI coverage for Grid's Kind-based end-to-end
validation suite.

## Validation tiers

| Tier | Scope | Purpose |
|---|---|---|
| Static | Formatting, linting, documentation, CRD schema checks | Catches deterministic issues before any cluster work starts. |
| Unit | Operator, scoring, xtask, and parser tests | Validates controller logic, overlay rendering, scoring, metrics handling, and harness helpers without Kind. |
| Smoke Kind | Single-topology operator routing validation | Proves the operator can reconcile resources, render an overlay, and drive a consumer gateway in Kind. |
| Multi-cluster Kind | SWIM, CRDT, stale GC, metrics routing, and credential validation | Proves the distributed control-plane paths across multiple Kind clusters. |

## Gate implementation

CI gating is organized as static + unit + smoke Kind for the fast path.  The
multi-cluster suite runs on `main`, nightly, or release validation because it
requires multiple Kind clusters and serialized execution.

Sequence:

1. Static checks and unit tests on every PR.
2. `validate-operator-routing` as the first Kind PR gate.
3. `verify-swim-membership` and `verify-swim-state` for distributed membership
   validation.
4. Full two-provider suite for nightly or release validation.

## Multi-cluster coverage set

| Validation | Behavior proven |
|---|---|
| `verify-swim-membership` | Real SWIM gossip drives GridNetwork membership state. |
| `verify-swim-state` | CRDT-over-SWIM provider state propagates across sites. |
| `verify-metrics-routing` | Live normalized metrics affect overlay ordering and request routing. |
| `verify-stale-gc-ttl` | Aged stale candidates are omitted from rendered overlays when TTL is configured. |
| `verify-api-fallback-native` | Provider credentials are injected from mounted Secret files, not overlay or ConfigMap token bytes. |
| `verify-failover-under-lost-peer` | Lost peer state degrades remote candidates and routes shared-model traffic to healthy fallback capacity. |

## Sequencing requirements

Multi-cluster validations run sequentially.  The current suite assumes shared
cluster names and shared local operator process ports.  Sequential execution avoids:

- SWIM UDP port conflicts
- Overlapping writes to the same Kind clusters
- Stale test fixtures from a previous validation affecting the next
- Competing gateway or provider deployments with the same names

Static and unit jobs run in parallel with each other.  Kind jobs are serialized
until the harness supports per-job cluster name isolation.

## Environment requirements

CI runners require:

- Docker access for Kind
- `kind`
- `kubectl`
- The repository Rust toolchain
- The nightly toolchain used by formatting checks
- Gateway and mock-provider images available to Kind
- Permission to create and delete local Kind clusters

The gateway image must include the Grid data-plane filters required by the
validation suite.  CI consumes a reviewed, published image rather than building
an unpinned image during the test job.

## Flake controls

Each Kind run starts from a clean environment:

```bash
cargo xtask env down -c tests/env/<config>.toml
cargo xtask env up -c tests/env/<config>.toml
cargo xtask env load-gateway-images -c tests/env/<config>.toml
```

SWIM and failover tests do not use automatic retries.  Those tests exercise
process lifecycle and membership timing; retrying from a partially mutated cluster
state can hide real bugs.  If a retry is needed, it is a full clean `down` / `up`
rerun.

## Runtime tiers

| Tier | Trigger | Approximate runtime |
|---|---|---|
| Static + unit | Every PR | 2–4 minutes |
| Smoke Kind | Every PR (requires resolved gateway image) | 5–8 minutes |
| Multi-cluster Kind | `main`, nightly, or release branch | 35–45 minutes |
| Full release sweep | Manual or release candidate | 60+ minutes |

## Multi-cluster gate requirements

The full multi-cluster suite requires:

- A reviewed gateway image with the required Grid data-plane filters must be
  published and referenced by CI.
- Kind cluster names must be either isolated per-job or the suite must run
  serially on a single runner.
- A reusable cleanup preflight removes all known E2E resources before a suite
  starts, making sequential runs reliable.
