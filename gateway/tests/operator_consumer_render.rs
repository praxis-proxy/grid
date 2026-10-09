//! The operator's derived consumer render loads in praxis, and a bad derived SNI does not.

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use praxis_core::config::{Config, ConfigFile, DEFAULT_CONFIG};

    /// Rendered by the operator's derived topology golden test.
    const RENDER: &str = include_str!("testdata/consumer-config-derived.yaml");

    /// The declared server name in the fixture, which the negative controls replace.
    const DECLARED_SNI: &str = "sni: \"model-b.example.internal\"";

    /// Load `yaml` the way the gateway does at startup.
    fn load(yaml: &str) -> Result<(), String> {
        // Tests run as threads of one process, so each load needs its own file.
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "consumer-render-{}-{}.yaml",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
        std::fs::write(&path, yaml).expect("write");
        let loaded = ConfigFile::read(&path)
            .map_err(|error| error.to_string())
            .and_then(|file| {
                Config::from_config_file_or(Some(&file), DEFAULT_CONFIG)
                    .map(drop)
                    .map_err(|error| error.to_string())
            });
        let _removed = std::fs::remove_file(&path);
        loaded
    }

    #[test]
    fn the_derived_render_loads() {
        load(RENDER).expect("the operator's derived render loads");
    }

    #[test]
    fn a_server_name_derivation_refuses_is_one_praxis_refuses() {
        assert!(
            RENDER.contains(DECLARED_SNI),
            "the fixture still carries the declared name"
        );
        for bad in ["10.0.0.7", "host:443", "a/b", "has space.example"] {
            let yaml = RENDER.replace(DECLARED_SNI, &format!("sni: \"{bad}\""));
            assert!(load(&yaml).is_err(), "{bad:?} loads, so the refusal guards nothing");
        }
    }
}
