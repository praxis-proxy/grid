# CI Kind E2E Strategy

This document defines the intended CI coverage for Grid's Kind-based end-to-end
validation.  The goal is to make the local multi-cluster proof repeatable in CI
without turning every pull request into a long-running cluster test.

## Validation tiers

| Tier | Scope | Purpose |
|---|---|---|
| Static | Formatting, linting, documentation, CRD schema checks | Catches deterministic issues before any cluster work starts. |
| Unit | Operator, scoring, xtask, and parser tests | Validates controller logic, overlay rendering, scoring, metrics handling, and harness helpers without Kind. |
| Smoke Kind | Single-topology operator routing validation | Proves the operator can reconcile resources, render an overlay, and drive a consumer gateway in Kind. |
| Multi-cluster Kind | SWIM, CRDT, stale GC, metrics routing, and credential validation | Proves the distributed control-plane paths across multiple Kind clusters. |

## Recommended gate progression

The first required CI gate should be static + unit + one smoke Kind validation.
That keeps PR latency low while still proving the operator-to-gateway path.

The multi-cluster suite should initially run on `main` and nightly. After it has
stable history, promote the lowest-flake tests into the PR gate.

Recommended order:

1. Static checks and unit tests on every PR.
2. `validate-operator-routing` as the first Kind PR gate.
3. `verify-swim-membership` and `verify-swim-state` after stable nightly history.
4. Full two-provider suite as nightly or release validation.

## Multi-cluster coverage set

The full suite should cover these behaviors:

| Validation | Behavior proven |
|---|---|
| `verify-swim-membership` | Real SWIM gossip drives GridNetwork membership state. |
| `verify-swim-state` | CRDT-over-SWIM provider state propagates across sites. |
| `verify-metrics-routing` | Live normalized metrics affect overlay ordering and request routing. |
| `verify-stale-gc-ttl` | Aged stale candidates are omitted from rendered overlays when TTL is configured. |
| `verify-api-fallback-native` | Provider credentials are injected from mounted Secret files, not overlay or ConfigMap token bytes. |
| `verify-failover-under-lost-peer` | Lost peer state degrades remote candidates and routes shared-model traffic to healthy fallback capacity. |

## Sequencing requirements

Run multi-cluster validations sequentially unless each job receives isolated
cluster names, kubeconfig contexts, local ports, and resource prefixes.

The current suite assumes shared cluster names and shared local operator process
ports. Sequential execution avoids:

- SWIM UDP port conflicts;
- overlapping writes to the same Kind clusters;
- stale test fixtures from a previous validation affecting the next validation;
- competing gateway or provider deployments with the same names.

Static and unit jobs can run in parallel with each other. Kind jobs should be
serialized until the harness supports per-job cluster name isolation.

## Environment requirements

CI runners need:

- Docker access for Kind;
- `kind`;
- `kubectl`;
- the repository Rust toolchain;
- the nightly toolchain used by formatting checks;
- gateway and mock-provider images available to Kind;
- permission to create and delete local Kind clusters.

The gateway image used by CI must include the Grid data-plane filters required
by the validation suite. CI should consume a reviewed, published image rather
than building an unpinned image during the test job.

## Flake controls

Each Kind run should start from a clean environment:

```bash
cargo xtask env down -c tests/env/<config>.toml
cargo xtask env up -c tests/env/<config>.toml
cargo xtask env load-gateway-images -c tests/env/<config>.toml
```

The suite should avoid automatic retries for SWIM/failover tests. Those tests
exercise process lifecycle and membership timing; retrying from a partially
mutated cluster state can hide real bugs. If a retry is needed, it should be a
full clean `down` / `up` rerun.

## Runtime tiers

| Tier | Suggested trigger | Approximate runtime |
|---|---|---|
| Static + unit | Every PR | 2–4 minutes |
| Smoke Kind | Every PR once image availability is solved | 5–8 minutes |
| Multi-cluster Kind | `main`, nightly, or release branch | 35–45 minutes |
| Full release sweep | Manual or release candidate | 60+ minutes |

## Open work before full CI gating

- Publish a reviewed gateway image that includes the required Grid data-plane
  filters.
- Decide whether Kind cluster names remain fixed and serial-only, or whether CI
  jobs receive unique cluster/context names.
- Add a reusable cleanup preflight that removes all known E2E resources before a
  suite starts.
- Track runtime and flake history before promoting multi-cluster checks from
  nightly to required PR gates.
