//! Extended lint: diff-scoped heuristic checks for common low-quality-code
//! patterns that automated compiler lints can't catch structurally.
//!
//! Clippy already denies the machine-checkable half of this class of issue
//! (`unwrap`/`expect`, `panic!`, `todo!()`/`unimplemented!()`, dead code,
//! missing docs, print/`dbg!` macros, and more, depending on the crate's own
//! lint configuration). What lint tooling structurally cannot check is
//! comment *content* and diff-local *repetition* -- two common
//! low-effort-code tells. This checks only lines added/changed versus the
//! diff base so pre-existing code is never relitigated.
//!
//! Checks (Block = fails; Warn = printed, does not fail):
//!   - Block: leftover TODO/FIXME/XXX/HACK markers in comments
//!   - Block: commented-out code
//!   - Warn: narrating "what the code does" comments
//!   - Warn: the same numeric/string literal repeated 3+ times without a named constant
//!   - Warn: weak/generic identifier names introduced by a new let/fn binding
//!   - Warn: new clippy lint suppressions added
//!
//! Diff base resolution: CLI arg, else `$EXTENDED_LINT_BASE`, else
//! `origin/$GITHUB_BASE_REF` in a GitHub Actions PR, else `origin/main`.
//! GitHub Actions checks out the repository itself as `origin` regardless of
//! a contributor's local remote naming, which is the same assumption this
//! repo's other diff-scoped CI check (`unicode-safety.yaml`) relies on.

use std::{
    collections::{HashMap, HashSet},
    process::Command,
    sync::LazyLock,
};

use regex::Regex;

// ---------------------------------------------------------------------------
// Patterns
// ---------------------------------------------------------------------------

/// Matches a TODO/FIXME/XXX/HACK marker inside a `//` comment.
static TODO_MARKER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)//.*\b(TODO|FIXME|XXX|HACK)\b").unwrap_or_else(|_| std::process::abort()));

/// Matches a `//` comment whose body looks like commented-out Rust code.
static COMMENTED_CODE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^//+\s*(let\s+\w|fn\s+\w|if\s*\(|for\s*\(|match\s+\w|return\b|\w+\s*\([^)]*\)\s*;?\s*$|\w+\.\w+\(.*\)\s*;?\s*$|[\w:<>]+\s*=\s*.+;\s*$)",
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Matches a new `let`/`fn` binding to a weak, generic identifier name.
static WEAK_NAME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(let(?:\s+mut)?|fn)\s+(temp|tmp|foo|bar|thing|val|obj|stuff)\b")
        .unwrap_or_else(|_| std::process::abort())
});

/// Matches a numeric or string literal worth tracking for repetition.
static LIT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?:^|[^\w.])(\d{2,}|"[^"]{4,}")(?:$|[^\w])"#).unwrap_or_else(|_| std::process::abort())
});

/// Matches a `const`/`static` declaration line.
static CONST_LINE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(const|static)\s+\w+").unwrap_or_else(|_| std::process::abort()));

/// Matches a new `#[allow(clippy::...)]`/`#[expect(clippy::...)]` suppression.
static SUPPRESSION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"#\[(allow|expect)\(clippy::").unwrap_or_else(|_| std::process::abort()));

/// Matches the start of a `#[cfg(test)]` module.
static TEST_MODULE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(#\[cfg\(test\)\]|mod tests\b)").unwrap_or_else(|_| std::process::abort()));

/// Matches a unified-diff hunk header, capturing the new-file start line.
static HUNK_HEADER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^@@ -\d+(?:,\d+)? \+(\d+)").unwrap_or_else(|_| std::process::abort()));

/// Comment openers that narrate "what" the following code does rather than
/// explaining non-obvious intent.
const NARRATING_OPENERS: &[&str] = &[
    "increment",
    "decrement",
    "loop through",
    "iterate over",
    "iterate through",
    "return the",
    "returns the",
    "create a",
    "creates a",
    "initialize",
    "set the",
    "sets the",
    "get the",
    "gets the",
    "parse the",
    "parses the",
    "convert ",
    "converts ",
    "check if",
    "checks if",
    "validate that",
    "validates that",
    "call ",
    "calls ",
    "define ",
    "defines ",
    "import ",
    "imports ",
    "declare ",
    "declares ",
    "instantiate",
    "loop over",
    "append ",
    "appends ",
    "remove ",
    "removes ",
    "add ",
    "adds ",
];

// ---------------------------------------------------------------------------
// Diff parsing
// ---------------------------------------------------------------------------

/// A single line added or changed in the diff, resolved to its file and
/// 1-based line number in the new (post-diff) version of that file.
struct AddedLine {
    /// Path of the file the line belongs to, relative to the repo root.
    file: String,
    /// 1-based line number in the file's new content.
    lineno: usize,
    /// Raw line content, without the leading `+` diff marker.
    content: String,
}

/// Resolve the diff base ref: CLI arg, then `$EXTENDED_LINT_BASE`, then
/// `origin/$GITHUB_BASE_REF` inside a GitHub Actions PR, then `origin/main`.
fn resolve_diff_base(cli_arg: Option<&str>) -> String {
    if let Some(base) = cli_arg {
        return base.to_owned();
    }
    if let Ok(base) = std::env::var("EXTENDED_LINT_BASE") {
        return base;
    }
    if let Ok(base_ref) = std::env::var("GITHUB_BASE_REF") {
        return format!("origin/{base_ref}");
    }
    "origin/main".to_owned()
}

/// Diff-parsing state threaded across lines of a unified-diff hunk.
#[derive(Default)]
struct DiffCursor {
    /// Path of the file the current hunk belongs to.
    current_file: String,
    /// 1-based line number of the next line in the new file content.
    new_lineno: usize,
}

/// Process one line of `git diff --unified=0` output, updating `cursor` and
/// appending to `added` when the line is a genuine addition.
fn process_diff_line(line: &str, cursor: &mut DiffCursor, added: &mut Vec<AddedLine>) {
    if let Some(path) = line.strip_prefix("+++ b/") {
        path.clone_into(&mut cursor.current_file);
        return;
    }
    if let Some(caps) = HUNK_HEADER_RE.captures(line) {
        cursor.new_lineno = caps.get(1).and_then(|m| m.as_str().parse::<usize>().ok()).unwrap_or(0);
        return;
    }
    if line.starts_with("+++") || line.starts_with("---") {
        return;
    }
    if let Some(content) = line.strip_prefix('+') {
        added.push(AddedLine {
            file: cursor.current_file.clone(),
            lineno: cursor.new_lineno,
            content: content.to_owned(),
        });
        cursor.new_lineno += 1;
    } else if !line.starts_with('-') {
        cursor.new_lineno += 1;
    }
}

/// Run `git diff --unified=0` against `diff_base`, restricted to `*.rs`
/// files, and collect every added/changed line.
///
/// # Errors
///
/// Returns an error if the `git` process cannot be spawned.
fn run_diff(diff_base: &str) -> Result<Vec<AddedLine>, Box<dyn std::error::Error>> {
    // This module's own source is exempt: it defines the marker-detection
    // regexes and their test fixtures, so it unavoidably contains literal
    // instances of the tokens this check hunts for and would otherwise
    // self-flag as blocking findings whenever this file is touched. The
    // Python predecessor never had this problem -- its `*.rs` path filter
    // excluded its own `.py` source for free.
    let output = Command::new("git")
        .args([
            "diff",
            "--unified=0",
            diff_base,
            "--",
            "*.rs",
            ":!xtask/src/lint_extended.rs",
        ])
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);

    let mut added = Vec::new();
    let mut cursor = DiffCursor::default();
    for line in stdout.lines() {
        process_diff_line(line, &mut cursor, &mut added);
    }
    Ok(added)
}

/// Line number (1-based) where a `#[cfg(test)]` module starts in `file`, or
/// `usize::MAX` if the file has none.
fn test_module_start_line(file: &str) -> usize {
    let Ok(text) = std::fs::read_to_string(file) else {
        return usize::MAX;
    };
    for (i, line) in text.lines().enumerate() {
        if TEST_MODULE_RE.is_match(line) {
            return i + 1;
        }
    }
    usize::MAX
}

// ---------------------------------------------------------------------------
// Findings
// ---------------------------------------------------------------------------

/// Occurrences of each `(file, literal)` pair: the source line text and its
/// line number, in order of appearance.
type LiteralSites = HashMap<(String, String), Vec<(String, usize)>>;

/// Accumulated results of scanning all added lines.
#[derive(Default)]
struct Findings {
    /// Findings that fail the check.
    blocking: Vec<String>,
    /// Findings that are printed but do not fail the check.
    warnings: Vec<String>,
    /// Numeric/string literal occurrences outside test modules, used to
    /// detect diff-local repetition.
    literal_sites: LiteralSites,
    /// Literals already hoisted to a named `const`/`static`, per file.
    const_declared: HashMap<String, HashSet<String>>,
}

/// Extract the `//` comment suffix of a line, if any (untrimmed).
fn comment_text(content: &str) -> Option<&str> {
    let idx = content.find("//")?;
    content.get(idx..)
}

/// Block: leftover TODO/FIXME/XXX/HACK marker in a comment.
fn check_todo_marker(file: &str, lineno: usize, stripped: &str, comment: &str, findings: &mut Findings) {
    if TODO_MARKER_RE.is_match(comment) {
        findings.blocking.push(format!(
            "{file}:{lineno}: leftover TODO/FIXME/XXX/HACK marker: {stripped:?}"
        ));
    }
}

/// Block: a comment whose body looks like commented-out code.
fn check_commented_code(file: &str, lineno: usize, stripped: &str, comment: &str, findings: &mut Findings) {
    let is_doc_comment = comment.starts_with("///") || comment.starts_with("//!");
    if !is_doc_comment && COMMENTED_CODE_RE.is_match(comment) {
        findings
            .blocking
            .push(format!("{file}:{lineno}: looks like commented-out code: {stripped:?}"));
    }
}

/// Warn: a comment narrating "what" the following code does.
fn check_narrating_comment(file: &str, lineno: usize, stripped: &str, comment: &str, findings: &mut Findings) {
    if comment.starts_with("///") || comment.starts_with("//!") {
        return;
    }
    let body = comment.trim_start_matches('/').trim().to_lowercase();
    if NARRATING_OPENERS.iter().copied().any(|opener| body.starts_with(opener)) {
        findings.warnings.push(format!(
            "{file}:{lineno}: narrating 'what' comment, prefer self-explanatory code or a doc comment on why: {stripped:?}"
        ));
    }
}

/// Warn: a new `let`/`fn` binding to a weak, generic identifier name.
fn check_weak_name(file: &str, lineno: usize, stripped: &str, findings: &mut Findings) {
    let Some(name) = WEAK_NAME_RE.captures(stripped).and_then(|caps| caps.get(2)) else {
        return;
    };
    findings.warnings.push(format!(
        "{file}:{lineno}: weak/generic identifier name {:?}: {stripped:?}",
        name.as_str()
    ));
}

/// Warn: a new clippy lint suppression.
fn check_suppression(file: &str, lineno: usize, stripped: &str, findings: &mut Findings) {
    if SUPPRESSION_RE.is_match(stripped) {
        findings.warnings.push(format!(
            "{file}:{lineno}: new clippy suppression added, double-check the reason: {stripped:?}"
        ));
    }
}

/// Record literals already hoisted to a named `const`/`static` on this line.
fn collect_const_literals(file: &str, stripped: &str, findings: &mut Findings) {
    if !CONST_LINE_RE.is_match(stripped) {
        return;
    }
    for caps in LIT_RE.captures_iter(stripped) {
        if let Some(lit) = caps.get(1) {
            findings
                .const_declared
                .entry(file.to_owned())
                .or_default()
                .insert(lit.as_str().to_owned());
        }
    }
}

/// Record every literal occurrence on this line for the repetition check.
fn collect_literal_sites(file: &str, lineno: usize, stripped: &str, findings: &mut Findings) {
    for caps in LIT_RE.captures_iter(stripped) {
        if let Some(lit) = caps.get(1) {
            findings
                .literal_sites
                .entry((file.to_owned(), lit.as_str().to_owned()))
                .or_default()
                .push((stripped.to_owned(), lineno));
        }
    }
}

/// Run all per-line checks against a single added line.
fn scan_line(line: &AddedLine, in_test_module: bool, findings: &mut Findings) {
    let stripped = line.content.trim();
    if let Some(raw_comment) = comment_text(&line.content) {
        let comment = raw_comment.trim();
        check_todo_marker(&line.file, line.lineno, stripped, comment, findings);
        check_commented_code(&line.file, line.lineno, stripped, comment, findings);
        check_narrating_comment(&line.file, line.lineno, stripped, comment, findings);
    }
    check_weak_name(&line.file, line.lineno, stripped, findings);
    check_suppression(&line.file, line.lineno, stripped, findings);
    collect_const_literals(&line.file, stripped, findings);
    if !in_test_module && !stripped.starts_with("#[") {
        collect_literal_sites(&line.file, line.lineno, stripped, findings);
    }
}

/// Warn on literals repeated 3+ times in a file's added lines without a
/// named constant declared for them.
fn report_repeated_literals(findings: &mut Findings) {
    let mut messages = Vec::new();
    for ((file, literal), sites) in &findings.literal_sites {
        let declared = findings.const_declared.get(file).is_some_and(|s| s.contains(literal));
        if sites.len() >= 3 && !declared {
            let lines: Vec<String> = sites.iter().map(|(_, lineno)| lineno.to_string()).collect();
            messages.push(format!(
                "{file}: literal {literal} repeated {}x at lines {} without a named constant -- consider hoisting it",
                sites.len(),
                lines.join(", ")
            ));
        }
    }
    findings.warnings.extend(messages);
}

/// Print accumulated non-blocking warnings to stderr.
fn print_warnings(warnings: &[String]) {
    if warnings.is_empty() {
        return;
    }
    eprintln!("[extended-lint] warnings (review, does not block):");
    for w in warnings {
        eprintln!("  - {w}");
    }
    eprintln!();
}

/// Print accumulated blocking findings to stderr; returns whether the check
/// is clean (no blocking findings).
fn print_blocking(blocking: &[String]) -> bool {
    if blocking.is_empty() {
        eprintln!("[extended-lint] no blocking findings.");
        return true;
    }
    eprintln!("[extended-lint] BLOCKING findings:");
    for b in blocking {
        eprintln!("  - {b}");
    }
    eprintln!();
    eprintln!("[extended-lint] fix the above, or if a match is a false positive, note why in the PR description.");
    false
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Runs the check; returns `Ok(true)` if clean, `Ok(false)` if blocking
/// findings exist (the caller should exit non-zero in that case).
///
/// # Errors
///
/// Returns an error if `git diff` cannot be run against the resolved diff
/// base.
pub(crate) fn run(cli_arg: Option<&str>) -> Result<bool, Box<dyn std::error::Error>> {
    let diff_base = resolve_diff_base(cli_arg);
    let added = run_diff(&diff_base)?;
    if added.is_empty() {
        println!("[extended-lint] no added Rust lines vs {diff_base}; nothing to check.");
        return Ok(true);
    }

    let mut findings = Findings::default();
    let mut test_module_lines: HashMap<String, usize> = HashMap::new();
    for line in &added {
        let start = *test_module_lines
            .entry(line.file.clone())
            .or_insert_with(|| test_module_start_line(&line.file));
        scan_line(line, line.lineno >= start, &mut findings);
    }

    report_repeated_literals(&mut findings);
    print_warnings(&findings.warnings);
    Ok(print_blocking(&findings.blocking))
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn detects_todo_marker() {
        assert!(
            TODO_MARKER_RE.is_match("// TODO: fix this later"),
            "should match a TODO marker"
        );
        assert!(
            !TODO_MARKER_RE.is_match("// this is fine"),
            "should not match a plain comment"
        );
    }

    #[test]
    fn detects_commented_out_code_but_not_doc_comments() {
        assert!(
            COMMENTED_CODE_RE.is_match("// let x = compute();"),
            "should match a commented-out let binding"
        );
        assert!(
            !COMMENTED_CODE_RE.is_match("/// Returns the computed value."),
            "should not match a doc comment"
        );
    }

    #[test]
    fn detects_weak_names() {
        let caps = WEAK_NAME_RE
            .captures("let temp = 5;")
            .expect("should match a weak-name binding");
        assert_eq!(
            caps.get(2).map(|m| m.as_str()),
            Some("temp"),
            "captured group should be the weak name"
        );
        assert!(
            WEAK_NAME_RE.captures("let value = 5;").is_none(),
            "should not match a non-weak name"
        );
    }

    #[test]
    fn detects_narrating_comment_openers() {
        assert!(
            NARRATING_OPENERS
                .iter()
                .copied()
                .any(|o| "increment the counter by one".starts_with(o)),
            "should detect a narrating opener"
        );
        assert!(
            !NARRATING_OPENERS
                .iter()
                .copied()
                .any(|o| "guards against a torn write".starts_with(o)),
            "should not flag a non-narrating comment"
        );
    }

    #[test]
    fn resolve_diff_base_prefers_cli_arg() {
        assert_eq!(
            resolve_diff_base(Some("explicit-base")),
            "explicit-base",
            "CLI arg should take precedence"
        );
    }

    #[test]
    fn comment_text_extracts_trailing_comment() {
        assert_eq!(
            comment_text("let x = 1; // trailing"),
            Some("// trailing"),
            "should extract from // onward"
        );
        assert_eq!(
            comment_text("let x = 1;"),
            None,
            "a line with no comment should yield None"
        );
    }
}
