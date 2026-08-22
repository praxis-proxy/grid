//! Forge configuration materialization for local image overrides.

use std::{
    fs,
    path::{Path, PathBuf},
};

use super::image_overrides;

/// Render a Forge environment with the explicitly selected demo images.
pub(crate) fn materialize(source: &Path, output: Option<&Path>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(source)?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(&content)?;
    apply_image_overrides(&mut config)?;
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

#[expect(
    clippy::too_many_lines,
    reason = "The bounded image-property rewrite is easiest to audit as one operation."
)]
/// Apply the selected image references to every Forge cluster property.
fn apply_image_overrides(config: &mut serde_yaml::Value) -> Result<(), Box<dyn std::error::Error>> {
    let pull_policy = image_overrides::image_pull_policy();
    let gateway = image_overrides::gateway_image();
    let operator = image_overrides::operator_image();
    let overlay_sync = image_overrides::overlay_sync_image();
    let vcr = image_overrides::vcr_image();

    if pull_policy == "Never"
        && (std::env::var_os("GRID_XTASK_GATEWAY_IMAGE").is_none()
            || std::env::var_os("GRID_XTASK_OPERATOR_IMAGE").is_none()
            || std::env::var_os("GRID_XTASK_OVERLAY_SYNC_IMAGE").is_none())
    {
        return Err("GRID_XTASK_GATEWAY_IMAGE, GRID_XTASK_OPERATOR_IMAGE, and GRID_XTASK_OVERLAY_SYNC_IMAGE are required when GRID_XTASK_IMAGE_PULL_POLICY=Never".into());
    }

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
    image.rsplit_once(':').map_or_else(
        || (image.to_owned(), "latest".to_owned()),
        |(repo, tag)| (repo.to_owned(), tag.to_owned()),
    )
}

#[cfg(test)]
mod tests {
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
    }
}
