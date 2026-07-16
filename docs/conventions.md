# Development Conventions

## Coding Style

### General Principles

- Brevity is a component of quality. Keep code lean and
  complete; no bloat.
- Small, composable, single-purpose functions are the
  default unit of organization. Split code into small
  files with focused responsibilities.
- Minimize side effects. Prefer pure transformations when
  feasible: data in, data out. Resist mutable state when
  feasible and outside the critical paths.
- Keep functions short enough to reason about in isolation.
- Prefer raw performance when reasonable. Reduce memory
  copies where feasible. Use references, borrowing, and
  in-place mutation when it avoids unnecessary cloning.

### Important Tools

- **Clippy**: Enforce idiomatic Rust and catch common
  mistakes
- **rustfmt**: Ensure consistent code formatting
- **cargo-audit**: Check for vulnerable dependencies
- **cargo-deny**: Enforce supply chain safety policies
- **rustdoc**: Generate the API documentation

### Comments vs Tracing

Comments answer **"why?"**, never **"what?"**.

**"What?" belongs in `tracing`**, not comments. If a
comment describes what the code is doing at runtime
("parse the config", "reject the request", "skip this
step"), replace it with a `tracing::debug!`,
`tracing::trace!`, or `tracing::info!` call. Runtime
narration (what the code did, what it decided, what it
skipped) is structured logging, not commentary.

**"Why?" belongs in comments**, but only when
non-obvious. A hidden constraint, a subtle invariant, a
workaround for a specific bug, or behavior that would
surprise a reader: these justify a comment. If removing
the comment would not confuse a future reader, do not
write it.

**"What?" at the code level needs neither.** Well-named
identifiers already explain what the code does. Do not
write comments that restate what names already convey.

### Testing

**New capabilities require all of the following:**

1. Unit tests covering the implementation
2. Integration tests proving end-to-end behavior
3. Significant changes need to be benchmarked

This is not optional. A feature without tests is not
complete.

Prefer more doctests when in doubt. Duplicative coverage
between doctests and unit/integration tests is fine.

#### Coverage expectations

The target goal is approximately 95% coverage for new
production-quality and reusable logic.  If that goal is
not practical for a specific change, the PR description
must identify what coverage exists instead and explain
why full coverage is impractical.

This is a goal, not a CI-enforced policy.  The repository
does not currently run automated coverage measurement.
Do not claim measured coverage unless tooling to measure
it is in place.

**`xtask` commands that generate Kubernetes or Praxis
config** should include unit tests asserting the shape
of the generated config — for example, that
`grid_route.local_site` is set correctly, that
`load_balancer` cluster names match the expected
convention, or that required filters are present.  Full
kind cluster runs complement these tests but are not a
substitute for focused config-shape assertions.

**Operator reconciliation logic** should prefer pure
render and decision functions tested without a live
Kubernetes cluster.  Controller reconcile tests that
require a live cluster are valuable but should be layered
on top of pure unit tests, not used as the only coverage.

**Topology-specific or presentation-only behavior** is
not a substitute for tests in this repository.  Code
intended only for a specific external walkthrough
belongs in an external research or demo repository,
not here.  Generic, config-driven, reusable commands
belong in Grid and require the test coverage described
above.

Prefer assertion messages over inline comments. Put the
explanation in the assertion's message argument so it
prints on failure:

```rust
// Bad:
// ACL should block loopback
assert_eq!(status, 403);

// Good:
assert_eq!(status, 403, "ACL should block loopback");
```

### RFC Conformance

When implementing protocol-level behavior (HTTP semantics,
header handling, TLS, etc.), identify the governing RFCs
and verify conformance against them.

- Cite the specific RFC number and section in test names
  or doc comments for protocol conformance tests.
- When in doubt about an edge case, the RFC is the
  authority, not other implementations.
- Add dedicated conformance tests when implementing
  RFC-specified behavior.

### Rules, Practices & Lints

Security is enforced at the lint level. See
`[workspace.lints]` in `Cargo.toml` for the full set.

- `#![deny(unsafe_code)]` in all crate roots (no
  exceptions; unsafe belongs upstream)
- Clippy runs with `-D warnings` (zero tolerance)
- Errors via `thiserror`
- Logging via `tracing`
- Use workspace dependencies (`[workspace.dependencies]`)
  to keep versions consistent across crates
- Keep dependencies light. Avoid new dependencies when
  feasible. Only add dependencies with well-established
  reputation.
- Always specify full semver versions with patch
  (e.g. `1.2.3`, not `1.2` or `1`)
- `cargo audit` and `cargo deny check` enforce supply
  chain safety

#### Type Design

Make invalid states unrepresentable. The type system
and serde should enforce constraints at parse time,
not at runtime.

- **Enums over strings for fixed value sets.** Never
  use `String` where the valid values are known. Use
  `#[serde(rename_all = "snake_case")]` enums.
- **Structs over maps for known keys.** Never use
  `BTreeMap`/`HashMap` for config deserialization when
  the key set is known. Use a struct with
  `#[serde(deny_unknown_fields)]`.
- **Enums over multiple `Option<T>` fields.** When
  exactly one of N fields must be set, use an N-variant
  enum.
- **`#[serde(default)]` over `Option<T>` with
  `unwrap_or`.** Use the concrete type with
  `#[serde(default = "fn_name")]` instead.
- **`#[serde(try_from)]` for constrained numerics.**
  Define an enum with `TryFrom` for fixed numeric
  values.
- **`#[serde(deny_unknown_fields)]` by default.** Apply
  to all config structs unless the struct intentionally
  accepts arbitrary keys.

#### Additional Coding Conventions

- Use separator comments to visually separate distinct
  sections of code.
- **No re-export-only files.** If a file exists solely
  to `pub use` items from another crate or module,
  inline the import at the call site instead.
- **Constants** must be at the top of the file (after
  imports), never inside functions or impl blocks.
  Give them their own separator comment
  (e.g. `// Constants`).
- **File ordering**:
  1. Constants (with separator comment)
  2. Public types, impls, and functions
  3. Private types and impls (below their public
     consumers)
  4. Private utility functions (with separator)
  5. `#[cfg(test)] mod tests` block (always last)
- **Field and method ordering**: Alphabetical, with
  `name` pinned first on structs and `new()`/`name()`
  pinned first in impl blocks.
- **Inside `#[cfg(test)] mod tests`**:
  1. Imports
  2. All test functions (`#[test]` / `#[tokio::test]`)
  3. Test utilities at the end (with `// Test Utilities`
     separator)
- Place a blank line between attribute blocks.
- Separate distinct logical actions with blank lines.
  Function calls, variable bindings that begin a new step,
  and expression blocks that perform a discrete operation
  should have some newline space.
- Prefer pre-computed numeric literals over expressions
  like `1024 * 10`. Always add a trailing comment with
  the human-readable size or meaning (e.g.
  `const MAX_BODY: usize = 10_485_760; // 10 MiB`).

#### Separator Comments

All separator comments must be full-width (77 dashes),
never short-form:

```rust
// ---------------------------------------------------------------------------
// Section Name
// ---------------------------------------------------------------------------
```

Never: `// --- Section Name ---`

#### Test Conventions

- Never use inline comments inside test function bodies.
  All explanatory text must be either an assertion message
  or a `tracing::info!` / `debug!` / `trace!` call.
  Bad: `// ACL should block`.
  Good: `assert_eq!(status, 403, "ACL should block")`.
- Do not add doc comments (`///`) or regular comments
  (`//`) on test functions. The function name is the
  documentation. The exception is RFC conformance tests,
  which should have a doc comment citing the RFC number
  and section.
- Do not add per-test separator comments. Use one
  full-width separator to mark where tests begin. The
  exception is RFC conformance tests, which should have
  a separator comment for each test citing the RFC number
  and section.
- Use "Test Utilities" in separator comments, not
  "Helpers". Test utility modules should use doc comments
  that say "test utilities", not "helpers".
- Test utilities must stay inside the `#[cfg(test)]` block
  so they compile only during testing.

#### Restriction Lints

The following restriction lints are enforced across all
crates:

- **`todo` / `unimplemented`**: no placeholder macros in
  production code. Use proper error handling or feature
  gates instead.
- **`unused_result_ok`**: do not silently discard `Result`
  values via `.ok()`. Handle or propagate the error.
- **`exit`**: do not call `std::process::exit()`. Use
  graceful shutdown via the runtime instead.
- **`mem_forget`**: do not call `std::mem::forget()`. It
  leaks resources. Use `ManuallyDrop` if drop must be
  suppressed.

#### Rustdoc Lints

Rustdoc quality is enforced at compile time via
`[workspace.lints.rustdoc]`:

- Broken intra-doc links, bare URLs, unescaped backticks,
  invalid HTML tags, and invalid codeblock attributes are
  all denied.
- Every crate must have a crate-level doc comment
  (`//!` at the top of `lib.rs` or `main.rs`).
- Private doc tests produce a warning.

#### Idiomatic Rust

- Prefer `to_owned()` over `to_string()` for `&str` to
  `String` conversions. Reserve `to_string()` for Display
  formatting on non-string types (integers, errors, enums).
- Prefer `String::new()` over `"".to_owned()` or
  `"".into()` for empty strings.
- Use inline format args: `format!("{var}")` not
  `format!("{}", var)`.
- Use `is_some_and()` instead of
  `.map(...).unwrap_or(false)`.
- Use let-chains for nested `if let`: prefer
  `if let Some(x) = e && cond { }` over
  `if let Some(x) = e { if cond { } }`.

### Documentation

- All functions, methods, structs, enums, and type aliases
  must have `///` doc comments (public and private).
  Enforced by `missing_docs` and
  `missing_docs_in_private_items` lints.
- Rustdoc **prose** must cover intent, interface, and
  example usage only. Do not explain internal mechanics
  unless they are critical for a caller to use the item
  correctly. If a sentence describes how the function
  works rather than what it does or when to call it,
  remove it.
- Do not over-explain standard patterns (Arc, Cow, early
  returns, option unwrapping) in prose.
- Do not add redundant "Default: X" lines when the default
  is already implied by the trait default or function body.
- Do not document memory efficiency in rustdoc (e.g.
  "avoids allocation", "zero-copy", "cheap clone").
  Correct memory use is expected; it does not need
  narration.
- **Prefer ample doctests.** When in doubt, add one.
  Doctests are valuable; keep them thorough. The
  restriction above is on prose text, not on the quantity
  of doctests.
- Use reference-style rustdoc links, not inline. Put link
  definitions at the bottom of the doc block:

  ```rust
  /// Uses [`Pipeline`] to execute the chain.
  ///
  /// [`Pipeline`]: crate::Pipeline
  ```

### Formatting

- Wrap lines at 80 characters in `.md` files. Code lines
  can be up to 120 characters. Code blocks inside markdown
  follow the 120 char limit.
- Always use the correct syntax highlighter on fenced code
  blocks in `.md` files: `console` for shell, `rust` for
  Rust, `yaml` for YAML, `toml` for TOML, etc. Never use
  bare triple backticks.

## Code Responsibility

This project does not distinguish between code written by
hand, generated by a tool (e.g. lint), or produced by any
other means. **Every contributor is responsible for the
code they submit**, and *all* code MUST be human reviewed
before submission, or merging.

Signed-off commits (`Signed-off-by:`) are required and
represent your assertion that you have reviewed and fully
understand the changes you are submitting.

**Commit attribution**: Commits are attributed to people,
never to tools. Do not add `Co-Authored-By` lines for
development tools (e.g. linters, generators, formatters).
The human who reviews, understands, and submits the code
is the author.

PRs from a bot or tool (with the exception of
GitHub-specific ones like `dependabot`) will not be
accepted.

Before submitting or merging PRs, ensure that you have:

- Read every line of the diff. If you cannot explain why
  something exists, do not submit it.
- Verified that the change does what you intended and
  nothing more.
- Run the test suite *locally* first. The CI pipeline is
  not a substitute for local verification.

> **Note**: `Draft` pull requests are not exempt from
> these guidelines. They are still expected to be reviewed
> before submission.
