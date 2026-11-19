//! glibc builds must run on jemalloc with its background thread, or resident memory grows with
//! each burst of streams and is not returned at idle.

#![cfg(all(target_os = "linux", target_env = "gnu"))]

#[cfg(test)]
mod tests {
    use std::{error::Error, process::Command};

    /// jemalloc reads this prefixed variable on first use; an unused jemalloc never does.
    const MALLOC_CONF_ENV_VAR: &str = "_RJEM_MALLOC_CONF";

    #[test]
    fn glibc_build_runs_on_jemalloc_with_its_background_thread() -> Result<(), Box<dyn Error>> {
        let output = Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
            .arg("--config")
            .arg("/nonexistent/grid-gateway.yaml")
            .env(MALLOC_CONF_ENV_VAR, "stats_print:true")
            .env_remove("GRID_SERVING_CONFIG")
            .output()?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("opt.background_thread: true"),
            "jemalloc did not report its background thread on at exit: {stderr}"
        );
        Ok(())
    }
}
