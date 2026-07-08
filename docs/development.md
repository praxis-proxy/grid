# Development

## Requirements

- Rust stable 1.94+
- Rust nightly (for `rustfmt`)

## Conventions

**All contributors must read and understand
[conventions.md] before contributing.** The conventions
cover code style, testing requirements, file
organization, and security practices. Submissions
that do not follow these conventions will be rejected.

[conventions.md]:./conventions.md

## Build

```console
make build
make release
make check
```

### Test

```console
make test
```

### Supply Chain Safety

Security is enforced at every stage of development.
`cargo audit` and `cargo deny check` are run as part of
the `make audit` target. The `deny.toml` config bans
wildcard version requirements, unknown registries, and
unknown git sources. Multiple versions of the same crate
produce a warning. All crates enforce
`#![deny(unsafe_code)]` and Clippy runs with
`-D warnings` (zero tolerance).

### Formatting

Formatting requires nightly (`group_imports` and
`imports_granularity` are nightly-only). Both stable and
nightly toolchains must be installed.

```console
make fmt            # format all code
make lint           # check formatting + clippy
```

### Documentation

```console
make doc            # build docs with warnings denied
```

All items (public and private) require `///` doc
comments. The `missing_docs` and
`missing_docs_in_private_items` lints enforce this at
compile time.

Rustdoc warnings are denied globally via
`.cargo/config.toml` (`rustdocflags = ["-D", "warnings"]`),
so `cargo doc` always enforces doc quality even
outside Make.

### Coverage

```console
make coverage       # HTML coverage report
make coverage-check # fail if below threshold
```

Requires `cargo-llvm-cov`.

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
| `priority/high` | Needs to be worked on immediately, defer to criticals |
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
