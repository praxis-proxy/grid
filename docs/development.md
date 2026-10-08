# Development

Development and validation guidance for AI Grid Network (AGN).

## Requirements

- Rust stable 1.96+; edition 2024 and resolver 3.
- `nightly-2026-03-28` with rustfmt, matching `NIGHTLY_RUSTFMT` in the Makefile.
- Clippy, `cargo-machete`, `cargo-audit`, and `cargo-deny` for the Rust gates.
- `typos`, `taplo`, `shellcheck`, and `actionlint` for `make lint-extra`.
- `jq` and Mike Farah's `yq` v4 for CRD generation.
- `cargo-llvm-cov` and `llvm-tools-preview` for optional coverage runs.
- Docker or Podman, kind, kubectl, Helm, and the topology-specific prerequisites
  for live integration checks. Individual harnesses may require Docker.

Node/npm are needed for the dashboard web application and Markdown lint tools.
Read the relevant component guide before installing or running its tooling.

## Conventions

[Development Conventions](conventions.md) is the canonical AGN policy. It
covers coding style, documentation, tests, human review, and commit attribution,
including the differences from the pinned shared Conventions baseline.

## Workspace map

Run root Make targets from this repository's top-level directory. Cargo package
names stay the same regardless of their directory location.

| Package | Source | Responsibility |
| --- | --- | --- |
| `operator` | [`operator/`](../operator/) | Kubernetes controllers, CRDs, and the operator binary. |
| `grid-overlay-sync` | [`overlay-sync/`](../overlay-sync/) | Watches overlay ConfigMaps and delivers local gateway files. |
| `swim` | [`swim/`](../swim/) | Membership, gossip transport, and encryption. |
| `crdt` | [`crdt/`](../crdt/) | Replicated state primitives and provider state. |
| `scoring` | [`scoring/`](../scoring/) | Provider scoring and overlay contract types. |
| `certs` | [`certs/`](../certs/) | Site certificates and certificate-provider interfaces. |
| `enrollment` | [`enrollment/`](../enrollment/) | Site enrollment service and API types. |
| `grid-signals` | [`signals/`](../signals/) | Shared load-signal store, Prometheus exposition parser, and labels. |
| `grid-signals-client` | [`signals-client/`](../signals-client/) | mTLS poller for the operator's site-signal endpoint. |
| `mock-providers` | [`mock-providers/`](../mock-providers/) | Mock inference-provider APIs. |
| `fleet-dashboard` | [`fleet-dashboard/`](../fleet-dashboard/) | Optional fleet UI and Prometheus-backed views. |
| `forge` | [`forge/`](../forge/) | Development-environment orchestrator used by qualification tooling. |
| `xtask` | [`xtask/`](../xtask/) | Repository generation, environments, and qualification commands. |
| `version` | [`version/`](../version/) | Shared build identity for binaries and container provenance. |

[`gateway/`](../gateway/) is a separate Cargo workspace with its own lockfile.
It contains `gateway` (the `grid-gateway` binary) and `ai-grid-filters`. Root
`cargo --workspace` commands do not include it. Keep its TLS feature choices and
production no-ring check intact when changing shared dependencies.

## Verification

| Local command | Scope | Checked-in CI |
| --- | --- | --- |
| `make build`, `make check`, `make release` | Build, check, or release-build the root workspace. | Test, MSRV, image, and release jobs compile the relevant configurations. |
| `make fmt` | Format root and Gateway workspaces with the pinned nightly. | `make lint` checks formatting without rewriting files. |
| `make lint` | Root and Gateway Clippy, formatting, unused dependencies, and Gateway production no-ring check. | Tests / lint; Gateway also has a dedicated workflow. |
| `make lint-extra` | Spelling, TOML formatting, pre-commit-hook ShellCheck, actionlint. | No aggregate extra-lint job yet. |
| `make doc` | Root rustdoc, including private items, with warnings denied. | Documentation / rustdoc, including documentation-only PRs. |
| `make test` | Root workspace tests; ignored tests stay excluded. | Tests / test; separate FIPS and Gateway jobs cover their configurations. |
| `make audit` | Root lockfile audit and dependency/license policy. | Supply Chain; Gateway policy is checked in its own workflow. |
| `make codegen-check`, `make crds-check` | Compare generated enrollment types and CRDs with their source definitions. | Tests / lint. |
| `make coverage-check` | 80% root line-coverage floor with documented exclusions. | Coverage / coverage. |
| `make mutants`, `make semver`, `make publish-dry-run` | Opt-in specialist checks. | No corresponding scheduled Grid workflows. |

`make all` runs build, formatting, lint, rustdoc, root tests, and root audit.
Run `make lint-extra` separately for spelling, TOML, shell, and workflow checks.
The `make all` aggregate does not run coverage, Gateway tests, generated-file
checks, FIPS checks, container builds, dashboard web checks, or live cluster
qualifications. Run those when the changed contract requires them. The
[release guide](release.md) records the integration qualification matrix.

Tests and Coverage workflows skip their Rust jobs for documentation-only PRs;
rustdoc and MSRV still run. The Gateway, Helm, and dashboard workflows have
their own triggers and scopes. A skipped or absent job is not a passing test
result. Changes to README or Markdown prose alone do not require the full
local test suite. Changes to examples, generated contracts, or executable
commands need checks appropriate to the affected behavior.

### Formatting and documentation

```console
make fmt
make lint
make doc
```

Nightly is required for rustfmt's `group_imports` and `imports_granularity`.
All public and private items need documentation under the workspace lint policy;
see the canonical policy for test-function exceptions. `.cargo/config.toml`
also sets `rustdocflags = ["-D", "warnings"]`, including outside Make.

Markdown style is configured in `.markdownlint.yaml`; external link-check
settings are in `lychee.toml`. These checks are not part of `make lint-extra` or
checked-in CI. Review relative links and anchors when moving or rewriting docs,
and report whether automated Markdown/link checks were run.

### Generated manifests

`make generate-crds` writes `deploy/crds/` and
`charts/grid-operator/templates/crds/` from `operator/src/crd/`.
`make generate-api-types` writes enrollment Rust types from
`api/enrollment-v1alpha1.yaml`. Edit the sources and regenerate; do not edit the
generated output directly. CRD doc-comment changes also affect field
descriptions and require regeneration.

```console
make generate-crds
make crds-check
make generate-api-types
make codegen-check
```

### Coverage and supply chain

`make coverage` writes an HTML report under `target/coverage`;
`make coverage-check` writes `coverage.json`. See the
[coverage policy](conventions.md#coverage-floor) for the measured-scope limits
and the path toward the shared 90% line / 80% region target. No threshold
increase should be claimed without a report from the actual commands.

`cargo audit` checks known advisories. `cargo deny check` enforces `deny.toml`,
including license and source policy. Documented advisory exceptions belong to
their actual dependency paths; refreshing a template must not silently remove
or broaden them. Gateway has its own lockfile and `deny.toml`.

### Hooks and worktrees

Enable signing with your own Git identity and key before committing. Running
`make setup-hooks` selects `.hooks` through `core.hooksPath`, which works when
`.git` is a file in a linked worktree. This is repository Git configuration and
can affect its other worktrees. The hook checks that signing is enabled, then
runs `make lint`; it does not run tests or the complete `make all` aggregate.

Keep build outputs, cluster names, ports, and qualification evidence isolated
when working in parallel. Worktrees isolate source edits, not Git configuration
or container-daemon resources. Do not run cleanup against another run's state.

## Project Management

All repositories in the `praxis-proxy` organization
use a consistent workflow for planning, prioritizing,
and tracking work.

### Milestones

Milestones represent a body of work toward a shared
goal (e.g. a release, a feature area, or a hardening
pass). Every issue and pull request should belong to
a milestone. Milestones provide scope boundaries and
help answer "what ships together?"

### Priority Labels

Priority labels indicate the order in which work
within a milestone should be addressed. Every issue
should have exactly one priority label:

| Label | Description |
| --- | --- |
| `priority/critical` | Must be worked on immediately before anything else |
| `priority/high` | Needs to be worked on immediately, defer to critical work |
| `priority/medium` | Resolve after high and critical |
| `priority/low` | Resolve after all other priority levels |

When picking up work, address issues in priority
order: critical first, then high, medium, and low.

### Project Boards

GitHub project boards visualize the state of work
across milestones. Use boards to track issues through
their lifecycle (backlog, in progress, in review,
done). Boards are the primary tool for stand-ups and
status checks.
