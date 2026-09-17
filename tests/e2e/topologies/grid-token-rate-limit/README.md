# Distributed per-application token quota

This topology qualifies three Basic Auth applications sharing one inference
endpoint and one Grid provider pool:

- `application-a`
- `application-b`
- `application-c`

Both consumer gateway deployments expose the same endpoint and authenticate all
three applications. Praxis publishes the verified subject as typed,
request-scoped `AuthenticatedIdentity` metadata. The AI
`token_rate_limit` filter uses `key: authenticated_subject` to partition one
Valkey-backed rule into an independent bucket for each application.

No client-controlled identity header, identity gateway, projection filter,
trusted quota-group filter, or per-application listener is used.

## Architecture

```text
application-a/b/c
       |
       | Basic Auth
       v
consumer-gateway-a or consumer-gateway-b
       |
       | AuthenticatedIdentity.subject_id
       v
token_rate_limit (one rule, subject-keyed, shared Valkey)
       |
       | admitted requests only
       v
Grid round-robin overlay -> west / central / east providers
```

The two consumer replicas use the same Valkey namespace and rule. Therefore the
same application's budget is shared across replicas, while different verified
subjects receive independent buckets. The subject is hashed before it is used
as a backend key and is not exposed in metrics or logs.

Qualification evidence names the three fixed demo principals so reviewers can
audit cross-subject isolation. Those labels are test-fixture identifiers, not
backend keys or production telemetry; credentials and Authorization values are
never recorded.

Quota admission happens before Grid routing. A denied request returns HTTP 429
without contacting a provider; changing provider or region cannot create quota.

## Contract

The filter uses reservation-based hard admission:

- sliding window: 60 seconds
- capacity: 60 reserved tokens per application
- reservation: 15 tokens per request
- backend namespace: `praxis:grid-token-rate-limit`
- rule: `per-application-budget`
- key source: `authenticated_subject`

The reservation cap controls admission. Actual usage replaces the reservation
at settlement and may exceed the reservation estimate; settled usage is
recorded as evidence rather than treated as a second hard cap.

## Qualification coverage

The first-class runner preserves the hardened release harness:

- run-scoped Forge, Kind, network, and probe names with collision protection;
- exact accepted/serving overlay revision convergence on both consumers;
- bounded commands, transport-only retry, and persistent concurrency clients;
- trusted provider attribution and structured request evidence;
- invalid-auth rejection without quota reservation or provider contact;
- independent A/B/C buckets through one endpoint;
- shared exhaustion for the same application across both consumer replicas;
- concurrent reservation-cap enforcement;
- natural window expiry and recovery;
- consumer restart persistence;
- Valkey outage fail-closed behavior and recovery;
- NetworkPolicy positive and negative controls;
- guarded automatic cleanup with ownership evidence.

## Required sources and image

This integration requires the small Praxis change that makes Basic Auth publish
the existing generic `AuthenticatedIdentity` extension and the AI change that
adds `key: authenticated_subject` to `token_rate_limit`. The identity
contract is authentication-method agnostic; OAuth/OIDC producers can publish
the same type later without changing TRL.

The gateway image must be built from compatible Praxis and AI revisions with:

```text
token-rate-limit-filter,praxis-filter/basic-auth-filter
```

Do not use a local Cargo path patch as deployment provenance. Land and version
Praxis first, update AI to that released dependency, then build an immutable
feature-enabled AI image. Grid does not build or publish an AI rollup.

## Run

Build fresh `grid-operator`, `grid-overlay-sync`, and feature-enabled
`praxis-ai` images with one immutable tag, then run:

```bash
cargo xtask env run-grid-token-rate-limit-qualification \
  --forge-config tests/e2e/topologies/grid-token-rate-limit/forge.yaml \
  --run-id quota-a1b2c3 \
  --image-tag "$IMAGE_TAG" \
  --evidence-dir "$EVIDENCE_DIR"
```

`--run-id` is optional and must be a lowercase DNS-safe value of at most 24
characters. Use `--keep` only for intentional debugging. A passing release
qualification requires every functional scenario and automatic cleanup to
pass; manual cleanup cannot produce PASS.

Evidence is written as `results.json` and `summary.txt`. Credentials,
Authorization values, the Valkey password, and kubeconfig contents must never
appear in evidence.
