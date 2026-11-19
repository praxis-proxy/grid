//! glibc builds must run on jemalloc, or resident memory grows with each burst of streams.

#![cfg(all(target_os = "linux", target_env = "gnu"))]

#[cfg(test)]
mod tests {
    use std::{error::Error, process::Command};

    /// jemalloc reads this prefixed variable on first use and reports an option it rejects.
    const MALLOC_CONF_ENV_VAR: &str = "_RJEM_MALLOC_CONF";

    #[test]
    fn glibc_build_runs_on_jemalloc() -> Result<(), Box<dyn Error>> {
        let output = Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
            .arg("--config")
            .arg("/nonexistent/grid-gateway.yaml")
            .env(MALLOC_CONF_ENV_VAR, "bogus:1")
            .env_remove("GRID_SERVING_CONFIG")
            .output()?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("<jemalloc>:"),
            "jemalloc did not report the rejected option: {stderr}"
        );
        Ok(())
    }
}
