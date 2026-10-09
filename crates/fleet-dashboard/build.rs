//! Embeds the built web UI into the binary.
//!
//! Walks `webui/dist` and generates a sorted `(path, bytes)` table that the
//! `api::assets` module includes. When the directory is absent the table holds
//! a single placeholder page, so `cargo build` works without a Node toolchain.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

/// Served when the web UI was not built into this binary.
const PLACEHOLDER: &str = "<!doctype html><title>fleet-dashboard</title>\
<p>The web UI has not been built into this binary. Run make fleet-dashboard-web and rebuild.";

/// Where the Vite build lands, relative to the crate.
const DIST: &str = "webui/dist";

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed={DIST}");
    let root = Path::new(DIST);
    let mut entries = Vec::new();
    collect(root, root, &mut entries)?;
    entries.sort();
    let rows: Vec<String> = if entries.is_empty() {
        vec![format!("(\"index.html\", b{PLACEHOLDER:?}.as_slice())")]
    } else {
        entries
            .iter()
            .map(|(name, path)| {
                format!(
                    "({name:?}, include_bytes!({:?}).as_slice())",
                    path.display().to_string()
                )
            })
            .collect()
    };
    let out = PathBuf::from(std::env::var("OUT_DIR")?).join("assets.rs");
    let table = format!(
        "/// Every file of the built web UI, by path.\npub(super) static ASSETS: &[(&str, &[u8])] = &[{}];\n",
        rows.join(", ")
    );
    fs::write(out, table)?;
    Ok(())
}

/// Lists every file under `dir` as `(path relative to root, absolute path)`;
/// a missing directory yields nothing.
fn collect(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) -> Result<(), Box<dyn Error>> {
    let Ok(read) = fs::read_dir(dir) else {
        return Ok(());
    };
    for entry in read {
        let path = entry?.path();
        if path.is_dir() {
            collect(root, &path, out)?;
        } else {
            let relative = path.strip_prefix(root)?.to_string_lossy().replace('\\', "/");
            out.push((relative, path.canonicalize()?));
        }
    }
    Ok(())
}
