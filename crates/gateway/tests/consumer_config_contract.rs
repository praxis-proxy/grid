//! The operator's production consumer render and the overlay envelope it
//! publishes for the same gateway build every filter pipeline through the
//! registry the binary runs, so drift between the renderer, the envelope and
//! the pinned praxis-ai schema fails here rather than at gateway startup.

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use certs::generate_ca;
    use praxis_core::config::{Config, ConfigFile, DEFAULT_CONFIG};

    /// Rendered by the operator's derived topology golden test.
    const RENDER: &str = include_str!("testdata/consumer-config-derived.yaml");

    /// Published for the same gateway by the operator, pinned by the same golden test.
    const OVERLAY: &str = include_str!("testdata/routing-overlay-derived.json");

    /// The overlay mount the render points at, which each build redirects to its own file.
    const OVERLAY_MOUNT: &str = "overlay_file: /etc/praxis/routing/routing-overlay.json";

    /// The backend CA mount the render points at, read when the load balancer is built.
    const CA_MOUNT: &str = "ca_path: \"/run/secrets/grid-backend-ca/models-ca/ca.crt\"";

    /// The gateway the envelope is scoped to, as the fixture spells it.
    const SCOPED_GATEWAY: &str = "\"gateway\": \"inference-gw\"";

    /// A file in the target scratch directory no other test in this process uses.
    fn scratch(stem: &str, extension: &str) -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "{stem}-{}-{}.{extension}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// Build the render's pipelines over `overlay` the way the gateway does at
    /// startup: the operator's config file through the binary's registry, with
    /// the projected overlay and backend CA mounts redirected to scratch files.
    fn build(overlay: &str) -> Result<(), String> {
        assert!(
            RENDER.contains(OVERLAY_MOUNT) && RENDER.contains(CA_MOUNT),
            "the render still mounts the overlay and the backend CA where the chart projects them"
        );
        let overlay_path = scratch("routing-overlay", "json");
        std::fs::write(&overlay_path, overlay).expect("write overlay");
        let ca_path = scratch("models-ca", "crt");
        std::fs::write(&ca_path, generate_ca("models-ca").expect("generate CA").cert_pem).expect("write CA");
        let config_path = scratch("consumer-render", "yaml");
        let yaml = RENDER
            .replace(OVERLAY_MOUNT, &format!("overlay_file: {}", overlay_path.display()))
            .replace(CA_MOUNT, &format!("ca_path: \"{}\"", ca_path.display()));
        std::fs::write(&config_path, yaml).expect("write config");

        let built = ConfigFile::read(&config_path)
            .map_err(|error| error.to_string())
            .and_then(|file| {
                Config::from_config_file_or(Some(&file), DEFAULT_CONFIG).map_err(|error| error.to_string())
            })
            .and_then(|config| resolve(&config));
        for path in [&config_path, &overlay_path, &ca_path] {
            let _removed = std::fs::remove_file(path);
        }
        built
    }

    /// The registry `main.rs` builds and the pipeline resolution the server runs.
    fn resolve(config: &Config) -> Result<(), String> {
        let mut registry = praxis_filter::FilterRegistry::with_builtins();
        praxis_ai_filters::register_ai_filters(&mut registry, None);
        let health = praxis_core::health::build_health_registry(&config.clusters);
        let kv_stores = praxis_core::kv::KvStoreRegistry::new();
        let session_stores = Arc::new(praxis_filter::SessionStoreRegistry::new());
        let client = praxis::build_subrequest_client(config).map_err(|error| error.to_string())?;
        praxis::resolve_pipelines(config, &registry, &health, &kv_stores, &session_stores, &client)
            .map(drop)
            .map_err(|error| error.to_string())
    }

    #[test]
    fn the_derived_render_builds_over_the_overlay_the_operator_publishes() {
        build(OVERLAY).expect("the production render and its overlay build every pipeline");
    }

    #[test]
    fn an_overlay_published_for_another_gateway_is_refused() {
        assert!(
            OVERLAY.contains(SCOPED_GATEWAY),
            "the fixture still carries the scope the render expects"
        );
        let foreign = OVERLAY.replace(SCOPED_GATEWAY, "\"gateway\": \"other-gw\"");
        let error =
            build(&foreign).expect_err("a foreign-scope overlay builds, so expected_overlay_scope guards nothing");
        assert!(error.contains("scope"), "the refusal names the scope: {error}");
    }
}
