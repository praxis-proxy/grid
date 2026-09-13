//! Forge configuration materialization for local image overrides.

use std::{
    fs,
    path::{Path, PathBuf},
};

use super::image_overrides;

/// Render a Forge environment with the explicitly selected demo images.
pub(crate) fn materialize(source: &Path, output: Option<&Path>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let images = ImageOverrides {
        gateway: image_overrides::gateway_image(),
        operator: image_overrides::operator_image(),
        overlay_sync: image_overrides::overlay_sync_image(),
        vcr: image_overrides::vcr_image(),
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
fn apply_image_values(
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
    use std::{fs, path::PathBuf};

    use super::parse_image_ref;

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
        clippy::too_many_lines,
        reason = "A test fixture should fail at the exact missing quota-contract field."
    )]
    fn quota_consumers_expose_one_subject_keyed_application_listener() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/e2e/topologies/grid-token-rate-limit");
        let gateways = [
            ("configs/consumer/praxis-valkey-a.yaml", "consumer-gateway-a"),
            ("configs/consumer/praxis-valkey-b.yaml", "consumer-gateway-b"),
        ];
        for (rel, gateway) in gateways {
            let path = root.join(rel);
            let source = fs::read_to_string(&path).expect("read quota consumer config");
            for removed in [
                "identity_projection",
                "trusted_quota_group",
                "x-grid-quota-group",
                "identity.user_id",
            ] {
                assert!(
                    !source.contains(removed),
                    "{} still references {removed}",
                    path.display()
                );
            }
            for user in ["application-a", "application-b", "application-c"] {
                assert!(
                    source.contains(&format!("username: {user}")),
                    "{} missing {user}",
                    path.display()
                );
            }
            assert!(
                source.contains(gateway),
                "{} must name its own gateway {gateway}",
                path.display()
            );
            let config: serde_yaml::Value = serde_yaml::from_str(&source).expect("parse quota consumer config");
            assert_eq!(config["listeners"].as_sequence().map(Vec::len), Some(1));
            let chains = config["filter_chains"].as_sequence().expect("filter chains");
            let mut contracts = Vec::new();
            for chain in chains {
                let filters = chain["filters"].as_sequence().expect("chain filters");
                let Some(limiter) = filters
                    .iter()
                    .find(|f| f["filter"].as_str() == Some("token_rate_limit"))
                else {
                    continue;
                };
                let rule = &limiter["rules"][0];
                assert_eq!(rule["algorithm"].as_str(), Some("sliding_window"));
                assert_eq!(rule["window"].as_str(), Some("60s"));
                assert_eq!(rule["capacity"].as_u64(), Some(60));
                assert_eq!(rule["reserved_tokens"].as_u64(), Some(15));
                assert_eq!(rule["reservation_timeout"].as_str(), Some("30s"));
                assert_eq!(limiter["key"].as_str(), Some("authenticated_subject"));
                assert_eq!(
                    rule["match"]["headers"]["x-model"].as_str(),
                    Some("Qwen/Qwen3-0.6B"),
                    "quota applies only to the validated inference model"
                );
                contracts.push((
                    limiter["backend"]["namespace"].as_str().expect("namespace").to_owned(),
                    rule["name"].as_str().expect("rule name").to_owned(),
                ));
            }
            assert_eq!(
                contracts,
                [(
                    "praxis:grid-token-rate-limit".to_owned(),
                    "per-application-budget".to_owned()
                )],
                "each gateway must expose one shared rule partitioned by authenticated subject"
            );
        }
    }
}
