//! The chart's default renders load in praxis, so a bad default fails here, not at pod start.
//!
//! Needs `helm` on PATH. Set `GRID_SKIP_CHART_RENDER=1` to skip where helm is absent;
//! otherwise a missing helm fails rather than passing vacuously.

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, clippy::panic, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use std::{
        path::PathBuf,
        process::Command,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use praxis_core::config::{Config, ConfigFile, DEFAULT_CONFIG};

    /// Values every case starts from: a rendered gateway config with no auth.
    const BASE: &[&str] = &[
        "--set=gatewayConfig.render=true",
        "--set=gatewayConfig.localSite=hub",
        "--set=gatewayConfig.model=qwen3",
        "--set=gatewayConfig.auth.mode=none",
    ];

    /// The default local backend, a local tls workload with its CA, and a remote peer.
    const CASES: &[(&str, &[&str])] = &[
        (
            "local plaintext",
            &[
                "--set=gatewayConfig.backends[0].cluster=site-a",
                "--set=gatewayConfig.backends[0].transport.mode=plaintext",
                "--set=gatewayConfig.backends[0].endpoints[0]=10.0.0.5:8000",
            ],
        ),
        (
            "local tls",
            &[
                "--set=gatewayConfig.backends[0].cluster=kserve",
                "--set=gatewayConfig.backends[0].transport.mode=tls",
                "--set=gatewayConfig.backends[0].transport.sni=qwen3.ns.svc",
                "--set=gatewayConfig.backends[0].transport.ca.configMap=service-ca",
                "--set=gatewayConfig.backends[0].endpoints[0]=10.0.0.6:8443",
            ],
        ),
        (
            "remote peer",
            &[
                "--set=gatewayConfig.backends[0].cluster=pool-b",
                "--set=gatewayConfig.backends[0].site=site-b",
                "--set=gatewayConfig.backends[0].transport.mode=mutual_tls",
                "--set=gatewayConfig.backends[0].transport.sni=site-b.grid.internal",
                "--set=gatewayConfig.backends[0].endpoints[0]=203.0.113.7:8443",
                "--set=tls.enabled=true",
                "--set=tls.existingSecret=grid-site-identity",
                "--set=tls.caSecret=grid-ca",
            ],
        ),
    ];

    /// The rendered praxis.yaml for `values`, or `None` when helm is absent and skipping is asked for.
    fn render(values: &[&str]) -> Option<String> {
        let chart = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../charts/praxis-gateway");
        let output = match Command::new("helm")
            .args(["template", "render"])
            .arg(&chart)
            .args(BASE)
            .args(values)
            .args(["--show-only", "templates/gateway-config.yaml"])
            .output()
        {
            Ok(output) => output,
            Err(_) if std::env::var("GRID_SKIP_CHART_RENDER").is_ok_and(|skip| skip == "1") => return None,
            Err(error) => panic!("helm is needed to render the chart ({error}); set GRID_SKIP_CHART_RENDER=1 to skip"),
        };
        assert!(
            output.status.success(),
            "helm template: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let config_map: serde_yaml::Value = serde_yaml::from_slice(&output.stdout).expect("a ConfigMap");
        Some(
            config_map["data"]["praxis.yaml"]
                .as_str()
                .expect("praxis.yaml")
                .to_owned(),
        )
    }

    /// Load `yaml` the way the gateway does at startup.
    fn load(yaml: &str) -> Result<(), String> {
        // Tests run as threads of one process, so each load needs its own file.
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "chart-render-{}-{}.yaml",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
        std::fs::write(&path, yaml).expect("write");
        let file = ConfigFile::read(&path).map_err(|error| error.to_string())?;
        let loaded = Config::from_config_file_or(Some(&file), DEFAULT_CONFIG)
            .map(drop)
            .map_err(|error| error.to_string());
        let _removed = std::fs::remove_file(&path);
        loaded
    }

    #[test]
    fn every_default_render_loads() {
        for (name, values) in CASES {
            let Some(yaml) = render(values) else { return };
            load(&yaml).unwrap_or_else(|error| panic!("{name}: {error}"));
        }
    }

    #[test]
    fn a_grid_serving_render_with_site_route_tuning_loads() {
        let Some(yaml) = render(&[
            "--set=image.flavor=grid-gateway",
            "--set=gridServing.enabled=true",
            "--set=gridServing.network=grid",
            "--set=gridServing.siteRoute.availability.shedding=true",
            // A float through --set is a string, which the schema refuses; a bool parses.
            "--set=gridServing.siteRoute.prefixAffinity.enabled=false",
            "--set=tls.enabled=true",
            "--set=tls.existingSecret=grid-site-identity",
            "--set=tls.caSecret=grid-ca",
            "--set=gatewayConfig.backends[0].cluster=pool-b",
            "--set=gatewayConfig.backends[0].transport.mode=mutual_tls",
            "--set=gatewayConfig.backends[0].transport.sni=site-b.grid.internal",
            "--set=gatewayConfig.backends[0].endpoints[0]=203.0.113.7:8443",
        ]) else {
            return;
        };
        assert!(
            yaml.contains("availability:") && yaml.contains("shedding: true"),
            "the availability block is rendered:\n{yaml}"
        );
        assert!(
            yaml.contains("prefix_affinity:"),
            "prefixAffinity renders as the filter's prefix_affinity:\n{yaml}"
        );
        load(&yaml).unwrap_or_else(|error| panic!("grid serving with site route tuning: {error}"));
    }
}
