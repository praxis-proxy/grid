//! Forge configuration materialization for local image overrides.

use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use super::image_overrides;

/// Return a Forge state directory scoped to one resolved topology run.
pub(crate) fn state_dir_for_config(resolved_config: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let parent = resolved_config
        .parent()
        .ok_or("resolved Forge config must have a parent directory")?;
    let run_name = resolved_config
        .file_stem()
        .and_then(OsStr::to_str)
        .filter(|name| !name.is_empty())
        .ok_or("resolved Forge config must have a UTF-8 file stem")?;
    Ok(parent.join(".forge").join(run_name))
}

/// Build a Forge command that cannot read or modify another topology's state.
pub(crate) fn command(
    binary: impl AsRef<OsStr>,
    resolved_config: &Path,
) -> Result<Command, Box<dyn std::error::Error>> {
    let state_dir = state_dir_for_config(resolved_config)?;
    fs::create_dir_all(&state_dir)?;
    let mut command = Command::new(binary);
    command
        .args(["--state-dir"])
        .arg(&state_dir)
        .env("FORGE_STATE_DIR", &state_dir);
    Ok(command)
}

/// Render a Forge environment with the explicitly selected demo images.
pub(crate) fn materialize(source: &Path, output: Option<&Path>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let images = ImageOverrides {
        gateway: image_overrides::gateway_image(),
        operator: image_overrides::operator_image(),
        overlay_sync: image_overrides::overlay_sync_image(),
        vcr: image_overrides::sim_image(),
        pull_policy: image_overrides::image_pull_policy(),
    };
    if images.pull_policy == "Never"
        && (std::env::var_os("GRID_XTASK_GATEWAY_IMAGE").is_none()
            || std::env::var_os("GRID_XTASK_OPERATOR_IMAGE").is_none()
            || std::env::var_os("GRID_XTASK_OVERLAY_SYNC_IMAGE").is_none())
    {
        return Err("GRID_XTASK_GATEWAY_IMAGE, GRID_XTASK_OPERATOR_IMAGE, and GRID_XTASK_OVERLAY_SYNC_IMAGE are required when GRID_XTASK_IMAGE_PULL_POLICY=Never".into());
    }
    materialize_with_images(source, output, &images)
}

/// Image values to inject into a Forge configuration.
#[derive(Debug, Clone)]
pub(crate) struct ImageOverrides {
    /// Gateway image reference.
    pub(crate) gateway: String,
    /// Grid operator image reference.
    pub(crate) operator: String,
    /// Overlay-sync image reference.
    pub(crate) overlay_sync: String,
    /// VCR image reference.
    pub(crate) vcr: String,
    /// Kubernetes image pull policy.
    pub(crate) pull_policy: String,
}

/// Render a Forge environment with an explicit image set.
pub(crate) fn materialize_with_images(
    source: &Path,
    output: Option<&Path>,
    images: &ImageOverrides,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(source)?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(&content)?;
    apply_image_values(&mut config, images)?;
    let destination = output.map_or_else(
        || {
            source.with_file_name(format!(
                "{}.resolved.yaml",
                source.file_stem().and_then(|s| s.to_str()).unwrap_or("forge")
            ))
        },
        Path::to_path_buf,
    );
    fs::write(&destination, serde_yaml::to_string(&config)?)?;
    Ok(destination)
}

/// Apply explicit image values to every Forge cluster property.
#[expect(
    clippy::too_many_lines,
    reason = "The bounded image-property rewrite is easiest to audit as one operation."
)]
pub(crate) fn apply_image_values(
    config: &mut serde_yaml::Value,
    images: &ImageOverrides,
) -> Result<(), Box<dyn std::error::Error>> {
    let pull_policy = images.pull_policy.clone();
    let gateway = images.gateway.clone();
    let operator = images.operator.clone();
    let overlay_sync = images.overlay_sync.clone();
    let vcr = images.vcr.clone();

    let (gateway_repo, gateway_tag) = parse_image_ref(&gateway);
    let (operator_repo, operator_tag) = parse_image_ref(&operator);
    let (overlay_repo, overlay_tag) = parse_image_ref(&overlay_sync);

    let clusters = config
        .get_mut("spec")
        .and_then(|spec| spec.get_mut("clusters"))
        .and_then(serde_yaml::Value::as_sequence_mut)
        .ok_or("Forge config must contain spec.clusters")?;

    for cluster in clusters {
        let properties = cluster
            .get_mut("properties")
            .and_then(serde_yaml::Value::as_mapping_mut)
            .ok_or("Forge cluster must contain properties")?;
        for (key, value) in [
            ("gatewayImage", gateway.clone()),
            ("gatewayImageRepo", gateway_repo.clone()),
            ("gatewayImageTag", gateway_tag.clone()),
            ("operatorImage", operator.clone()),
            ("operatorImageRepo", operator_repo.clone()),
            ("operatorImageTag", operator_tag.clone()),
            ("overlaySyncImage", overlay_sync.clone()),
            ("overlaySyncImageRepo", overlay_repo.clone()),
            ("overlaySyncImageTag", overlay_tag.clone()),
            ("vcrImage", vcr.clone()),
            ("imagePullPolicy", pull_policy.clone()),
        ] {
            properties.insert(
                serde_yaml::Value::String(key.to_owned()),
                serde_yaml::Value::String(value),
            );
        }
    }
    Ok(())
}

/// Split an image reference into repository and tag components.
fn parse_image_ref(image: &str) -> (String, String) {
    let last_slash = image.rfind('/');
    image
        .rfind(':')
        .filter(|colon| last_slash.is_none_or(|slash| *colon > slash))
        .map_or_else(
            || (image.to_owned(), "latest".to_owned()),
            |colon| {
                let (repo, tagged) = image.split_at(colon);
                (repo.to_owned(), tagged.strip_prefix(':').unwrap_or_default().to_owned())
            },
        )
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, fs, path::PathBuf};

    use super::{command, parse_image_ref, state_dir_for_config};

    #[test]
    #[expect(clippy::expect_used, reason = "temporary Forge configuration fixture")]
    fn forge_commands_use_the_resolved_run_state_directory() {
        let temp = tempfile::tempdir().expect("create fixture directory");
        let combined_dir = temp.path().join("grid-combined-site");
        let glb_dir = temp.path().join("grid-glb-demo");
        fs::create_dir_all(&combined_dir).expect("create combined topology directory");
        fs::create_dir_all(&glb_dir).expect("create GLB topology directory");
        let combined_config = combined_dir.join(".forge.resolved-run-a.yaml");
        let second_combined_config = combined_dir.join(".forge.resolved-run-b.yaml");
        let glb_config = glb_dir.join(".forge.resolved-run-b.yaml");
        fs::write(&combined_config, "kind: ForgeEnvironment\n").expect("write combined config");
        fs::write(&second_combined_config, "kind: ForgeEnvironment\n").expect("write second combined config");
        fs::write(&glb_config, "kind: ForgeEnvironment\n").expect("write GLB config");

        let combined_state = state_dir_for_config(&combined_config).expect("scope combined state");
        let second_combined_state = state_dir_for_config(&second_combined_config).expect("scope second combined state");
        let glb_state = state_dir_for_config(&glb_config).expect("scope GLB state");
        assert_ne!(combined_state, second_combined_state);
        assert_ne!(combined_state, glb_state);
        assert_eq!(
            combined_state,
            state_dir_for_config(&combined_config).expect("repeat combined state")
        );

        let forge = command("praxis-forge", &combined_config).expect("build scoped Forge command");
        assert_eq!(forge.get_args().next(), Some(OsStr::new("--state-dir")));
        assert_eq!(forge.get_args().nth(1), Some(combined_state.as_os_str()));
        assert!(
            forge.get_envs().any(|(key, value)| {
                key == OsStr::new("FORGE_STATE_DIR") && value == Some(combined_state.as_os_str())
            })
        );
        assert!(combined_state.is_dir());
    }

    #[test]
    fn parses_tagged_and_untagged_images() {
        assert_eq!(
            parse_image_ref("repo/image:tag"),
            ("repo/image".to_owned(), "tag".to_owned())
        );
        assert_eq!(
            parse_image_ref("repo/image"),
            ("repo/image".to_owned(), "latest".to_owned())
        );
        assert_eq!(
            parse_image_ref("localhost:5000/image"),
            ("localhost:5000/image".to_owned(), "latest".to_owned())
        );
    }

    #[test]
    #[expect(
        clippy::expect_used,
        clippy::indexing_slicing,
        reason = "the rendered Forge contract is a fixed qualification fixture"
    )]
    fn image_overrides_include_overlay_sync_for_every_cluster() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/e2e/topologies");
        let images = super::ImageOverrides {
            gateway: "praxis-ai:run".to_owned(),
            operator: "grid-operator:run".to_owned(),
            overlay_sync: "grid-overlay-sync:run".to_owned(),
            vcr: "llm-d-inference-sim:run".to_owned(),
            pull_policy: "Never".to_owned(),
        };

        for topology in ["grid-provider-traffic", "grid-static-weighted"] {
            let source = root.join(topology).join("forge.yaml");
            let content = fs::read_to_string(source).expect("read qualification Forge config");
            let mut config: serde_yaml::Value = serde_yaml::from_str(&content).expect("parse Forge config");
            super::apply_image_values(&mut config, &images).expect("apply image overrides");
            let clusters = config["spec"]["clusters"].as_sequence().expect("clusters");
            assert_eq!(clusters.len(), 3);
            for cluster in clusters {
                let properties = &cluster["properties"];
                assert_eq!(properties["overlaySyncImage"].as_str(), Some("grid-overlay-sync:run"));
                assert_eq!(properties["overlaySyncImageRepo"].as_str(), Some("grid-overlay-sync"));
                assert_eq!(properties["overlaySyncImageTag"].as_str(), Some("run"));
                assert_eq!(properties["imagePullPolicy"].as_str(), Some("Never"));
            }
        }
    }

    #[test]
    #[expect(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::too_many_lines,
        reason = "A test fixture should fail at the exact missing quota-contract field."
    )]
    fn quota_consumers_use_the_upstream_limiter_schema_and_shared_rule() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/e2e/topologies/grid-token-rate-limit");
        let configs = [
            root.join("configs/consumer/praxis-valkey-a.yaml"),
            root.join("configs/consumer/praxis-valkey-b.yaml"),
        ];
        let mut quota_contract = None;

        for path in configs {
            let source = fs::read_to_string(&path).expect("read quota consumer config");
            for removed in ["reservationTimeout", "token_budgets", "estimation", "identity.user_id"] {
                assert!(
                    !source.contains(removed),
                    "{} still contains legacy field {removed}",
                    path.display()
                );
            }
            assert!(
                !source.contains("username: bob"),
                "{} must remain a single-principal qualification",
                path.display()
            );

            let config: serde_yaml::Value = serde_yaml::from_str(&source).expect("parse quota consumer config");
            let filters = config["filter_chains"][0]["filters"]
                .as_sequence()
                .expect("filter chain must contain filters");
            let limiter = filters
                .iter()
                .find(|filter| filter["filter"].as_str() == Some("token_rate_limit"))
                .expect("token_rate_limit filter must exist");
            let contract = (
                limiter["backend"]["namespace"].as_str().expect("namespace").to_owned(),
                limiter["rules"][0]["name"].as_str().expect("rule name").to_owned(),
                limiter["rules"][0]["algorithm"].as_str().expect("algorithm").to_owned(),
                limiter["rules"][0]["window"].as_str().expect("window").to_owned(),
                limiter["rules"][0]["capacity"].as_u64().expect("capacity"),
                limiter["rules"][0]["reserved_tokens"]
                    .as_u64()
                    .expect("reserved tokens"),
                limiter["rules"][0]["reservation_timeout"]
                    .as_str()
                    .expect("reservation timeout")
                    .to_owned(),
            );
            assert_eq!(
                contract,
                (
                    "praxis:grid-token-rate-limit".to_owned(),
                    "alice-shared-budget".to_owned(),
                    "sliding_window".to_owned(),
                    "60s".to_owned(),
                    60,
                    15,
                    "30s".to_owned(),
                )
            );
            assert_eq!(
                limiter["rules"][0]["match"]["headers"]["x-model"].as_str(),
                Some("Qwen/Qwen3-0.6B"),
                "quota must apply only to the validated inference model"
            );

            if let Some(expected) = quota_contract.as_ref() {
                assert_eq!(&contract, expected, "both consumers must address the same Valkey rule");
            } else {
                quota_contract = Some(contract);
            }
        }
    }
}
