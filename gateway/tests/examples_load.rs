//! The documented filter example loads through praxis as part of a complete config.

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use std::path::PathBuf;

    use praxis_core::config::{Config, ConfigFile, DEFAULT_CONFIG};

    /// `examples/gateway/grid-site-route.yaml` wrapped in the smallest config that carries it.
    #[test]
    fn the_documented_filter_example_loads() {
        let example = include_str!("../../examples/gateway/grid-site-route.yaml");
        let filters: String = example
            .lines()
            .filter(|line| !line.trim_start().starts_with('#'))
            .map(|line| format!("      {line}\n"))
            .collect();
        let yaml = format!(
            "admin:\n  address: \"127.0.0.1:9901\"\nlisteners:\n  - name: default\n    address: \"0.0.0.0:8080\"\n    \
             filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n{filters}"
        );
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("example-{}.yaml", std::process::id()));
        std::fs::write(&path, &yaml).expect("write");
        let loaded = ConfigFile::read(&path)
            .map_err(|error| error.to_string())
            .and_then(|file| {
                Config::from_config_file_or(Some(&file), DEFAULT_CONFIG)
                    .map(drop)
                    .map_err(|error| error.to_string())
            });
        let _removed = std::fs::remove_file(&path);
        loaded.expect("the example loads");
    }
}
