//! Resolves build facts at compile time and hands them to the crate as env vars.
//!
//! Each fact prefers its `GRID_*` environment variable so a container build can inject
//! what it knows. `.dockerignore` excludes `.git`, so the command fallbacks below only
//! resolve for a local build: a container build that injects nothing ships `unknown`,
//! which the crate reports rather than hiding.

use std::process::Command;

/// Build facts, as (env var, program, argv, fallback when neither resolves).
const FACTS: [(&str, &str, &[&str], &str); 4] = [
    ("GRID_GIT_COMMIT", "git", &["rev-parse", "HEAD"], "unknown"),
    (
        "GRID_GIT_VERSION",
        "git",
        // No --dirty: the tree state is its own fact, and Display appends it.
        &["describe", "--tags", "--always"],
        env!("CARGO_PKG_VERSION"),
    ),
    ("GRID_BUILD_DATE", "date", &["-u", "+%Y%m%d"], "unknown"),
    ("GRID_RUSTC_VERSION", "", &["--version"], "unknown"),
];

/// Trimmed stdout of `program args`, or `None` when it cannot run or says nothing.
fn run(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty()).then_some(value)
}

/// `clean` when the work tree has no modifications, `dirty` when it has, else `unknown`.
fn tree_state() -> String {
    if let Some(state) = std::env::var("GRID_GIT_TREE_STATE")
        .ok()
        .filter(|state| !state.trim().is_empty())
    {
        return state;
    }
    match run("git", &["status", "--porcelain"]) {
        // run() maps empty output to None, so Some here always means modifications.
        Some(_) => "dirty".to_owned(),
        None if run("git", &["rev-parse", "HEAD"]).is_some() => "clean".to_owned(),
        None => "unknown".to_owned(),
    }
}

/// The one-line version a CLI prints: describe, suffixed with the state when not clean.
///
/// `Display` in lib.rs composes the same text from the same two facts, and
/// `the_const_and_display_agree` holds them equal.
fn version_line(describe: &str, state: &str) -> String {
    if state == "clean" {
        return describe.to_owned();
    }
    format!("{describe}-{state}")
}

fn main() {
    let mut facts = std::collections::BTreeMap::new();
    for (key, program, args, fallback) in FACTS {
        println!("cargo:rerun-if-env-changed={key}");
        // An empty program means the compiler cargo is already using.
        let program = if program.is_empty() {
            std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned())
        } else {
            program.to_owned()
        };
        let value = std::env::var(key)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| run(&program, args))
            .unwrap_or_else(|| fallback.to_owned());
        println!("cargo:rustc-env={key}={value}");
        facts.insert(key, value);
    }

    println!("cargo:rerun-if-env-changed=GRID_GIT_TREE_STATE");
    let state = tree_state();
    println!("cargo:rustc-env=GRID_GIT_TREE_STATE={state}");
    let describe = facts.get("GRID_GIT_VERSION").map_or("unknown", String::as_str);
    println!("cargo:rustc-env=GRID_VERSION={}", version_line(describe, &state));
    println!(
        "cargo:rustc-env=GRID_TARGET={}",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned())
    );

    rerun_on_git_change();
}

/// Rebuild when the commit or the tree changes, resolved through git so a worktree
/// (where `.git` is a file) and a branch commit (which moves a ref, not HEAD) both count.
///
/// Without git, name this script instead: a missing path reads as always stale and
/// would rebuild every dependent binary on every build.
fn rerun_on_git_change() {
    let mut paths = vec!["HEAD".to_owned(), "index".to_owned(), "packed-refs".to_owned()];
    if let Some(head_ref) = run("git", &["rev-parse", "--symbolic-full-name", "HEAD"]) {
        paths.push(head_ref);
    }
    let mut any = false;
    for path in &paths {
        if let Some(resolved_path) = run("git", &["rev-parse", "--git-path", path]) {
            println!("cargo:rerun-if-changed={resolved_path}");
            any = true;
        }
    }
    if !any {
        println!("cargo:rerun-if-changed=build.rs");
    }
}
