//! Deterministic projected volume layout for delegated gateway mounts.

use std::{collections::BTreeMap, path::Path};

use serde_json::{Value, json};

use crate::{
    error::GatewayMountFailure,
    resources::consumer_config::{MountPurpose, MountRequirementsDocument},
};

/// Kubernetes pod-template volume and target-container mount pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DesiredMount {
    /// Volume object merged by `name` using Kubernetes strategic merge patch.
    pub volume: Value,
    /// Target container mount object merged by `mountPath`.
    pub volume_mount: Value,
    /// Stable reserved volume name.
    pub volume_name: String,
    /// Absolute mount directory.
    pub mount_path: String,
}

/// Build sorted projected volumes from a reference-only requirements document.
///
/// Credential Secrets mount at their generated per-Secret parent directory.
/// Grid CA and site identity keys use the parent of their rendered file paths,
/// preserving the TLS directory configured for Praxis.
#[expect(
    clippy::too_many_lines,
    reason = "the reference-to-volume transformation validates and groups paths in one pass"
)]
pub(crate) fn desired_mounts(document: &MountRequirementsDocument) -> Result<Vec<DesiredMount>, GatewayMountFailure> {
    let mut projections = BTreeMap::<(String, String, String), BTreeMap<String, String>>::new();
    let mut destinations = BTreeMap::<String, (String, String, String)>::new();

    for requirement in &document.requirements {
        if requirement.purpose == MountPurpose::GridServingTls {
            continue;
        }
        for item in &requirement.items {
            let path = Path::new(&item.path);
            let mount_path = path
                .parent()
                .and_then(Path::to_str)
                .filter(|parent| !parent.is_empty() && *parent != "/" && Path::new(parent).is_absolute())
                .ok_or_else(|| {
                    GatewayMountFailure::new("InvalidMountPath", format!("invalid mount path {:?}", item.path))
                })?;
            let normalized = path.components().collect::<std::path::PathBuf>();
            if path
                .components()
                .any(|component| component == std::path::Component::ParentDir)
                || normalized.to_str() != Some(item.path.as_str())
                || item.path == "/"
            {
                return Err(GatewayMountFailure::new(
                    "InvalidMountPath",
                    format!("mount path {:?} is not normalized", item.path),
                ));
            }
            let relative_path = path
                .strip_prefix(mount_path)
                .ok()
                .and_then(Path::to_str)
                .filter(|relative| !relative.is_empty() && !relative.starts_with("../"))
                .ok_or_else(|| {
                    GatewayMountFailure::new("InvalidMountPath", format!("invalid mount path {:?}", item.path))
                })?;
            let source = (
                requirement.secret.namespace.clone(),
                requirement.secret.name.clone(),
                item.key.clone(),
            );
            if let Some(existing) = destinations.get(&item.path)
                && existing != &source
            {
                return Err(GatewayMountFailure::new(
                    "MountPathConflict",
                    format!("mount path {:?} maps to multiple Secret keys", item.path),
                ));
            }
            destinations.insert(item.path.clone(), source);

            projections
                .entry((
                    mount_path.to_owned(),
                    requirement.secret.namespace.clone(),
                    requirement.secret.name.clone(),
                ))
                .or_default()
                .insert(item.key.clone(), relative_path.to_owned());
        }
    }

    let mut grouped = BTreeMap::<String, Vec<(String, String, BTreeMap<String, String>)>>::new();
    for ((mount_path, namespace, secret_name), items) in projections {
        grouped
            .entry(mount_path)
            .or_default()
            .push((namespace, secret_name, items));
    }

    let mut desired = Vec::with_capacity(grouped.len());
    for (mount_path, sources) in grouped {
        let volume_name = format!("grid-mount-{}", revision_prefix(&mount_path));
        let secret_sources = sources
            .into_iter()
            .map(|(_, secret_name, items)| {
                let paths = items
                    .into_iter()
                    .map(|(key, path)| json!({"key": key, "path": path}))
                    .collect::<Vec<_>>();
                json!({
                    "secret": {
                        "name": secret_name,
                        "optional": false,
                        "items": paths
                    }
                })
            })
            .collect::<Vec<_>>();
        desired.push(DesiredMount {
            volume: json!({
                "name": volume_name,
                // Match the Kubernetes API default explicitly so a read-back
                // Deployment compares equal on the next reconcile.
                "projected": {"defaultMode": 420, "sources": secret_sources}
            }),
            volume_mount: json!({
                "name": volume_name,
                "mountPath": mount_path,
                "readOnly": true
            }),
            volume_name,
            mount_path,
        });
    }
    Ok(desired)
}

/// Hash canonical serialized requirements to a stable lowercase SHA-256 digest.
pub(crate) fn requirements_revision(document: &MountRequirementsDocument) -> Result<String, serde_json::Error> {
    Ok(revision(&serde_json::to_string(document)?))
}

/// Hash rendered config bytes to a stable lowercase SHA-256 digest.
pub(crate) fn config_revision(config: &str) -> String {
    revision(config)
}

/// Hash the sorted referenced Secret resource versions without exposing their content.
pub(crate) fn secret_revision(resource_versions: &BTreeMap<String, String>) -> Result<String, serde_json::Error> {
    Ok(revision(&serde_json::to_string(resource_versions)?))
}

/// Derive the stable requirements `ConfigMap` name from the configured config name.
pub(crate) fn requirements_config_map_name(config_map_name: &str) -> String {
    format!("grid-mount-requirements-{}", revision_prefix(config_map_name))
}

/// Return the first 16 lowercase hexadecimal characters of an input's digest.
fn revision_prefix(input: &str) -> String {
    revision(input).chars().take(16).collect()
}

/// Hash an input to its stable lowercase SHA-256 digest.
fn revision(input: &str) -> String {
    crate::resources::tls_backend::sha256(input.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    clippy::expect_used,
    reason = "test assertions inspect fixed JSON projection fixtures"
)]
mod tests {
    use super::*;
    use crate::resources::consumer_config::{MountRequirement, RequirementGateway, RequirementItem, RequirementSecret};

    fn requirement(purpose: MountPurpose, name: &str, items: &[(&str, &str)]) -> MountRequirement {
        MountRequirement {
            purpose,
            final_hop: "consumer-gateway".to_owned(),
            secret: RequirementSecret {
                namespace: "praxis".to_owned(),
                name: name.to_owned(),
            },
            items: items
                .iter()
                .map(|(key, path)| RequirementItem {
                    key: (*key).to_owned(),
                    path: (*path).to_owned(),
                })
                .collect(),
        }
    }

    fn document(requirements: Vec<MountRequirement>) -> MountRequirementsDocument {
        MountRequirementsDocument {
            schema_version: "v1".to_owned(),
            network: "production".to_owned(),
            gateway: RequirementGateway {
                name: "consumer-gateway".to_owned(),
                namespace: "praxis".to_owned(),
            },
            requirements,
        }
    }

    #[test]
    fn mount_layout_is_deterministic_and_groups_grid_identity_files() {
        let ca = requirement(
            MountPurpose::GridPeerCa,
            "grid-ca",
            &[("ca.crt", "/etc/praxis/tls/ca.crt")],
        );
        let identity = requirement(
            MountPurpose::GridSiteIdentity,
            "grid-site",
            &[
                ("tls.key", "/etc/praxis/tls/tls.key"),
                ("tls.crt", "/etc/praxis/tls/tls.crt"),
            ],
        );
        let first = desired_mounts(&document(vec![ca.clone(), identity.clone()])).expect("valid projection");
        let reordered = desired_mounts(&document(vec![identity, ca])).expect("same projection");

        assert_eq!(first, reordered);
        assert_eq!(
            first.len(),
            1,
            "Grid CA and identity share the configured TLS directory"
        );
        assert_eq!(first[0].mount_path, "/etc/praxis/tls");
        assert_eq!(first[0].volume["projected"]["defaultMode"], 420);
        let sources = first[0].volume["projected"]["sources"].as_array().expect("source list");
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0]["secret"]["name"], "grid-ca");
        assert_eq!(sources[1]["secret"]["name"], "grid-site");
        let rendered = serde_json::to_string(&first[0].volume).expect("serialize volume");
        assert!(!rendered.contains("private-key-bytes"));
    }

    #[test]
    fn credentials_are_projected_into_per_secret_directories() {
        let mounts = desired_mounts(&document(vec![
            requirement(
                MountPurpose::BackendCredential,
                "api-a",
                &[("token", "/run/secrets/grid-credentials/api-a/token")],
            ),
            requirement(
                MountPurpose::BackendCredential,
                "api-b",
                &[("auth", "/run/secrets/grid-credentials/api-b/auth")],
            ),
        ]))
        .expect("valid credential projection");

        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[0].mount_path, "/run/secrets/grid-credentials/api-a");
        assert_eq!(mounts[1].mount_path, "/run/secrets/grid-credentials/api-b");
        assert_eq!(
            mounts[0].volume["projected"]["sources"][0]["secret"]["items"][0]["path"],
            "token"
        );
    }

    #[test]
    fn chart_managed_grid_serving_tls_is_not_claimed_by_the_operator() {
        let document = document(vec![requirement(
            MountPurpose::GridServingTls,
            "grid-site",
            &[("tls.key", "/etc/praxis/tls/tls.key")],
        )]);

        assert!(desired_mounts(&document).expect("chart owns serving TLS").is_empty());
    }

    #[test]
    fn conflicting_secret_keys_at_one_path_fail_closed() {
        let error = desired_mounts(&document(vec![
            requirement(
                MountPurpose::BackendCredential,
                "api-a",
                &[("token", "/run/secrets/token")],
            ),
            requirement(
                MountPurpose::BackendCredential,
                "api-b",
                &[("token", "/run/secrets/token")],
            ),
        ]))
        .expect_err("one path cannot refer to multiple Secrets");
        assert_eq!(error.reason, "MountPathConflict");
    }

    #[test]
    fn traversal_paths_fail_closed() {
        let error = desired_mounts(&document(vec![requirement(
            MountPurpose::BackendCredential,
            "api-a",
            &[("token", "/run/secrets/../etc/token")],
        )]))
        .expect_err("traversal path must be rejected");
        assert_eq!(error.reason, "InvalidMountPath");
    }

    #[test]
    fn root_directory_mounts_fail_closed() {
        let error = desired_mounts(&document(vec![requirement(
            MountPurpose::BackendCredential,
            "api-a",
            &[("token", "/token")],
        )]))
        .expect_err("a Secret projection must not mask the container root");
        assert_eq!(error.reason, "InvalidMountPath");
    }

    #[test]
    fn secret_resource_version_changes_advance_the_rotation_revision() {
        let before = BTreeMap::from([("praxis/api-token".to_owned(), "12".to_owned())]);
        let after = BTreeMap::from([("praxis/api-token".to_owned(), "13".to_owned())]);

        assert_eq!(
            secret_revision(&before).expect("stable hash"),
            secret_revision(&before).expect("stable hash")
        );
        assert_ne!(
            secret_revision(&before).expect("old hash"),
            secret_revision(&after).expect("new hash")
        );
    }

    #[test]
    fn requirements_config_map_name_is_stable_and_bounded() {
        let first = requirements_config_map_name("praxis-consumer-config");
        assert_eq!(first, requirements_config_map_name("praxis-consumer-config"));
        assert!(first.starts_with("grid-mount-requirements-"));
        assert!(first.len() <= 63);
    }
}
