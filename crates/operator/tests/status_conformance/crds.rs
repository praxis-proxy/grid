//! The generated CRDs under `deploy/crds`, checked statically.

use std::path::PathBuf;

use status_harness::invariants;

/// The generated CRD manifests, by kind.
fn crds() -> Vec<(String, serde_json::Value)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/crds");
    let mut found = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("deploy/crds is readable") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|ext| ext == "yaml")
            && path.file_name().is_some_and(|file| file != "kustomization.yaml")
        {
            let text = std::fs::read_to_string(&path).expect("crd readable");
            let crd: serde_json::Value = serde_yaml::from_str(&text).expect("crd parses");
            found.push((path.display().to_string(), crd));
        }
    }
    assert!(!found.is_empty(), "no CRDs under {}", dir.display());
    found
}

#[test]
fn invariant_9_printer_columns_read_conditions_or_spec_not_phase() {
    for (file, crd) in crds() {
        invariants::printer_columns_read_conditions(&crd).unwrap_or_else(|why| panic!("{file}: {why}"));
    }
}

#[test]
fn invariant_9_every_crd_has_a_printer_column_set() {
    for (file, crd) in crds() {
        let versions = crd["spec"]["versions"].as_array().expect("versions");
        assert!(
            versions
                .iter()
                .all(|version| version.get("additionalPrinterColumns").is_some()),
            "{file}: a version has no printer columns"
        );
    }
}
