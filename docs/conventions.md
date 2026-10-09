# Development Conventions

This is the canonical development policy for AI Grid Network (AGN). The
[development guide](development.md#verification) maps these requirements to
local commands and checked-in CI jobs. The shared [Praxis Conventions] provide
the baseline; Grid-specific differences are recorded below rather than
overwriting domain-specific checks with template defaults.

[Praxis Conventions]: https://github.com/praxis-proxy/conventions/tree/1851c77279358e0e1e5ff4109968145ffa3dce69

## Project naming

Use **AI Grid Network (AGN)** as the human-facing name in documentation. After
the first full reference, use **AGN** where it reads naturally. Use actual
software names such as `grid-operator` and `praxis-gateway` when referring to
their binaries, deployments, images, or charts. Keep established identifiers
such as `GridNetwork`, `GridSite`, `grid-*`, `GRID_*`, and `grid_*` unchanged.
This documentation-only naming change does not rename APIs, binaries,
deployments, images, charts, workflows, or downstream identifiers; downstream
naming alignment is deferred to a separate effort.

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
- **cargo-machete**: Detect unused dependencies
- **cargo-semver-checks**: Lint for SemVer violations
- **cargo-llvm-cov**: Enforce the coverage floor
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

#### Coverage Floor

`make coverage-check` and Coverage CI enforce **80% line coverage** for the
root workspace. They exclude the `xtask` package and filenames matching
`(target/|tests/)`. Binary entrypoints are not explicitly excluded. The
separate Gateway workspace is not included, and there is no enforced region
threshold. Doctests are not instrumented by these commands.

The shared Conventions target is 90% lines and 80% regions. Grid retains its
existing floor while coverage work is qualified separately. To raise it,
measure the current JSON report, add focused regressions for uncovered behavior,
then update the local threshold and CI description together. Establish a
separate Gateway baseline before claiming workspace-wide coverage. Do not lower
thresholds or broaden exclusions to make a change pass. LLVM region coverage
measures executable source regions; it does not establish exhaustive branch or
distributed-history coverage.

Keep binary entrypoints focused on wiring so behavior can be tested in the
library. New code needs meaningful assertions regardless of whether its package
contributes to the aggregate coverage gate.

**`xtask` commands that generate Kubernetes or Praxis
config** should include unit tests asserting the shape
of the generated config - for example, that
`intelligent_route.local_site` is set correctly, that
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
belong in AGN and require the test coverage described
above.

#### Mutation Testing

Coverage proves code executed; mutation testing
(`make mutants`) checks whether the assertions notice changes to the code.
Grid exposes this as an opt-in local command; no checked-in workflow schedules
it. A passing run covers the mutations exercised, not every possible defect.
`cargo-mutants` rewrites function bodies (return defaults, flip
operators) and fails if the test suite still passes.
Treat a surviving mutant as a missing assertion, not
noise: either strengthen the tests or delete the
unneeded code.

#### Property-Based Testing

Use `proptest` for code with algebraic invariants:
parsers (round-trip), arithmetic (commutativity,
bounds), encodings (encode/decode identity).
Example-based tests pin known cases; property tests
search the input space for the case you did not think
of.

- `proptest!` blocks live inside `#[cfg(test)] mod
  tests`, after the example-based tests.
- Property test names state the invariant
  (`total_is_commutative`), same no-`test_`-prefix rule
  as other tests.
- Use `prop_assert!`/`prop_assert_eq!` with messages,
  like ordinary assertions.
- Commit `proptest-regressions/` files: they pin found
  counterexamples as permanent regression tests.

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
- RFC references in doc comments must use reference-style
  rustdoc links to the IETF datatracker:

  ```rust
  /// Safe methods per [RFC 9110 Section 9.2.1].
  ///
  /// [RFC 9110 Section 9.2.1]: https://datatracker.ietf.org/doc/html/rfc9110#section-9.2.1
  ```

- When in doubt about an edge case, the RFC is the
  authority, not other implementations.
- Add dedicated conformance tests when implementing
  RFC-specified behavior.

### Rules, Practices & Lints

The workspace lint policy catches unsafe and error-prone coding patterns.
See `[workspace.lints]` in both workspace manifests for the full set. Passing
lints does not establish the correctness of a trust or protocol boundary.

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

#### Lint Suppression Policy

By default, do *not* suppress lints. Use your best
judgement if the situation really calls for it.

Use `#[expect(...)]` instead of `#[allow(...)]`. The
`allow_attributes` lint enforces this mechanically.
Every suppression must include a `reason`:

```rust
// Good:
#[expect(
    clippy::too_many_lines,
    reason = "pipeline setup is inherently sequential"
)]
fn build_pipeline() { /* ... */ }

// Bad - denied by allow_attributes:
#[allow(clippy::too_many_lines)]
fn build_pipeline() { /* ... */ }
```

`#[expect]` is self-cleaning: if the suppressed lint
stops firing (because the code changed), the compiler
warns that the expectation is unfulfilled. This
prevents stale suppressions from accumulating.

#### Arithmetic Safety

Unchecked arithmetic is denied
(`arithmetic_side_effects`). Every `+`, `-`, `*`, `/`,
and `%` on integers is a potential overflow, wrap, or
divide-by-zero. State the intended behavior explicitly:

- `checked_*` when overflow is an error to propagate
- `saturating_*` when clamping at the bounds is correct
- `wrapping_*` when modular arithmetic is intended
  (document why)

Similarly, `as` casts are denied (`as_conversions`, plus
the `cast_*` lints). Use `From`/`Into` for lossless
conversions and `TryFrom`/`TryInto` where conversion can
fail. Never compare floats with `==` (`float_cmp`);
compare against an explicit epsilon.

Byte-order conversions must be explicit: `to_be_bytes`
or `to_le_bytes`, never the native-endian `to_ne_bytes`
(`host_endian_bytes`).

#### Exhaustive Matching

Wildcard arms over enums are denied
(`wildcard_enum_match_arm`). Name every variant, or
bind the remainder explicitly
(`other @ Variant::A | other @ Variant::B`). When a new
variant is added, every match over that enum must fail
to compile until each site handles it. A `_` arm
silently absorbs new variants; that is a bug factory,
not a convenience.

#### Deterministic Iteration

Iterating `HashMap`/`HashSet` is denied
(`iter_over_hash_type`). Hash iteration order is
random per process, which breaks reproducible output,
stable serialization, and deterministic tests. Use
`BTreeMap`/`BTreeSet` when iteration is needed, or
collect and sort before iterating.

#### Async Safety

Do not hold synchronization guards across `.await`
points. Holding a `Mutex`, `RefCell`, or `RwLock`
guard across a suspension point risks deadlocks or
runtime panics. The `await_holding_lock` and
`await_holding_refcell_ref` lints enforce this.

```rust
// Bad - guard held across await:
let guard = mutex.lock().await;
let result = some_async_call().await;
drop(guard);

// Good - drop guard before awaiting:
let data = {
    let guard = mutex.lock().await;
    guard.clone()
};
let result = some_async_call().await;
```

Never silently drop futures or `#[must_use]` values.
`let _ = async_fn()` drops the future without polling
it. The `let_underscore_future` and
`let_underscore_must_use` lints catch this.

#### String Safety

Raw string indexing (`&s[n..m]`) panics on non-char
boundaries and is denied by the `string_slice` and
`indexing_slicing` lints. Use safe alternatives:

- `.get(range)` for fallible substring access
- `.chars().nth(n)` for character-level access
- `.char_indices()` for iterating with byte offsets

#### Trait Import Convention

When importing a trait only for its methods (not
naming the trait type), use `as _` to keep the name
out of scope. The `unused_trait_names` lint enforces
this.

```rust
// Good - trait name unused, import anonymously:
use std::io::Write as _;

// Bad - trait name pollutes scope unnecessarily:
use std::io::Write;
```

#### Module Organization

Use the modern module file layout: a module `foo` with
children lives in `foo.rs` next to a `foo/` directory.
`foo/mod.rs` files are denied (`mod_module_files`).

#### Naming Clarity

- Single-character identifiers are denied
  (`min_ident_chars`), outside clippy's short allowlist
  for conventional loop/coordinate names. Names carry
  meaning; `cfg` beats `c`.
- Single-character lifetime names are denied
  (`single_char_lifetime_names`). Name the lifetime
  after what it borrows: `'req`, `'conn`, `'buf` -
  not `'a`.
- Struct fields must not repeat the struct name
  (`struct_field_names`): `Config { timeout }`, not
  `Config { config_timeout }`.
- Do not shadow a binding with an unrelated value
  (`shadow_unrelated`). Shadowing is for staged
  transformations of the *same* logical value (parse,
  validate, wrap), never for reusing a name.

#### Visibility Design

- Struct fields are all-public or all-private
  (`partial_pub_fields`). A struct with mixed field
  visibility is two APIs wearing one name; split it or
  encapsulate fully.
- Public fields must not be underscore-prefixed
  (`pub_underscore_fields`); an underscore says
  "unused", `pub` says "use me".
- Use `pub(crate)`, not `pub(in crate)`
  (`pub_without_shorthand`).
- Prefer named generic parameters over `impl Trait` in
  argument position (`impl_trait_in_params`); named
  parameters can be turbofished and referenced in
  bounds.
- Accept `Option<&T>`, not `&Option<T>` (`ref_option`).

#### Type Design

Make invalid states unrepresentable. The type system and serde should enforce
constraints at parse time. See the
[type-design examples](developing/type-design.md) for applications of these
rules.

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
- **Attribute formatting on structs, enums, fields,
  and variants**:
  - Order items within `#[derive(...)]` alphabetically.
  - Order parameters within `#[serde(...)]` alphabetically.

  ```rust
  // Good:
  #[derive(Clone, Debug, Default, Deserialize, Serialize)]
  #[serde(default, deny_unknown_fields)]
  pub struct Foo {

  // Bad (non-alphabetical):
  #[derive(Debug, Clone, Default, Serialize, Deserialize)]
  #[serde(deny_unknown_fields, default)]
  pub struct Foo {
  ```

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
- Name tests after the behavior they prove. Do not use a
  `test_` prefix inside `#[cfg(test)]` modules; the
  `redundant_test_prefix` lint denies it.
  Bad: `fn test_acl_blocks_loopback()`.
  Good: `fn acl_blocks_loopback()`.
- Do not assert on `Result` states
  (`assertions_on_result_states`): `assert!(r.is_ok())`
  discards the error message. Since `unwrap`/`expect`
  are denied everywhere (tests included), fallible tests
  return `Result` and propagate with `?`, then assert on
  the unwrapped value:

  ```rust
  #[test]
  fn config_parses_defaults() -> Result<(), ConfigError> {
      let cfg = Config::from_str("")?;

      assert_eq!(cfg.timeout_secs, 30, "default timeout should be 30s");
      Ok(())
  }
  ```

  For expected errors, match the variant instead of
  unwrapping:

  ```rust
  assert!(matches!(result, Err(ConfigError::UnknownField(_))), "unknown keys must be rejected");
  ```

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

### Commit Messages

Commits follow the conventional commit format. Reviewers currently enforce
subjects; the checked-in workflow does not validate their format:

```text
type(scope): summary
```

- Types: `build`, `chore`, `ci`, `docs`, `feat`, `fix`,
  `perf`, `refactor`, `test`
- Scope is optional, lowercase, kebab-case
- Subject line at most 72 characters
- Body explains what and why when the subject is not
  enough

### Pull Request Conventions

Keep pull requests reviewable. The requirements are:

- **Size**: at most 750 added production lines, excluding Cargo
  manifests/locks, tests, documentation, examples, and benchmarks. Discuss a
  larger coherent change with reviewers before expanding the scope.
- **Description**: explain the problem, resulting behavior, and validation.
- **Commit format**: use the conventional subjects described above.
- **DCO**: include a human `Signed-off-by` trailer on every commit.
- **Signed commits**: cryptographically sign commits with GPG or SSH.
- **Human authorship**: do not attribute authorship, co-authorship, or sign-off
  to AI tools. The human contributor is responsible for understanding and
  reviewing every submitted change.
- **Proposals**: use the shared [proposal process] for architectural or public
  interface changes spanning several PRs.

The checked-in `.github/workflows/conventions.yaml` checks for sign-off on
non-merge commits in non-draft PRs targeting `main`. Its `skip/signoff` label
selects an explicit skipped job. Drafts skip both sign-off jobs. A skipped check
is not evidence of compliance.

Size, descriptions, subject format, and authorship remain contributor and
reviewer obligations. GitHub's `main` ruleset also requires signed commits,
passing `test`, a code-owner review, and resolved review threads. These remote
rules were checked on 2026-10-08; inspect the current rules before changing
workflow check names or merge requirements.

The local pre-commit hook checks that `commit.gpgsign` is enabled and runs
`make lint`; it does not verify the signature of a resulting commit. Do not
describe template override labels as implemented Grid controls.

Before adopting a reusable Conventions workflow, inspect its inputs,
permissions, and check names against the repository's current required checks.
Pin it to a reviewed commit and document any required-check migration. Retain
Grid's Gateway, FIPS, generated-manifest, Helm, dashboard, and release gates.

[proposal process]: https://github.com/praxis-proxy/enhancements/blob/main/docs/process.md

### Synchronization boundaries

The root and Gateway workspace lint tables retain the shared rules plus
`redundant_clone`, `similar_names`, and `unchecked_time_subtraction` denials.
The formatter, TOML, Markdown, spelling, and pre-commit configurations follow
the inspected shared baseline. Preserve Grid's Cargo alias, frame-pointer
settings, Clippy thresholds and vocabulary, and documented supply-chain
exceptions when refreshing that baseline.

`make lint-extra` checks spelling, TOML formatting, the pre-commit hook with
ShellCheck, and workflow syntax with actionlint. It does not lint every shell
script or run Markdown/link checks. The Markdown and lychee configurations
record intended checks; their presence alone does not mean CI runs them. Use
those tools for changed documentation when available, and report checks that
could not run. Wiring broader checks into CI requires a measured baseline and
separate fixes for existing violations, not suppressions hiding the debt.
