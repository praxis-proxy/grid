//! Reject policy filters because the FIPS build omits the policy engine.

use praxis_core::config::{ChainRef, Config, FilterEntry};

/// Praxis filter name excluded from FIPS builds.
const POLICY_FILTER: &str = "policy";

/// Refuse a resolved config whose pipeline uses the `policy` filter.
///
/// # Errors
///
/// Returns the operator-facing refusal naming the first chain that uses the
/// filter and the supported FIPS authentication configuration.
pub(crate) fn validate_no_policy_filter(config: &Config) -> Result<(), String> {
    for chain in &config.filter_chains {
        if let Some(refusal) = first_policy_filter(&chain.name, &chain.filters) {
            return Err(refusal);
        }
    }
    Ok(())
}

/// Return the first policy filter in these entries or their inline branches.
fn first_policy_filter(chain_name: &str, entries: &[FilterEntry]) -> Option<String> {
    for entry in entries {
        if entry.filter_type == POLICY_FILTER {
            return Some(format!(
                "filter chain '{chain_name}' uses the 'policy' filter, but this FIPS grid-gateway build \
                 does not include the policy engine; set praxisConfig.render.auth.mode: none and place an \
                 authenticating front in front of this gateway"
            ));
        }
        for branch in entry.branch_chains.iter().flatten() {
            for chain_ref in &branch.chains {
                if let ChainRef::Inline { name, filters } = chain_ref
                    && let Some(refusal) = first_policy_filter(name, filters)
                {
                    return Some(refusal);
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use praxis_core::config::Config;

    use super::validate_no_policy_filter;

    /// Parse a test configuration, aborting if the fixture is invalid.
    fn config_with_policy_chain(yaml: &str) -> Config {
        Config::from_yaml(yaml).unwrap_or_else(|_| std::process::abort())
    }

    /// Reject a top-level policy filter with operator guidance.
    #[test]
    fn refuses_policy_filter() {
        let config = config_with_policy_chain(
            r#"
listeners:
  - name: web
    address: "127.0.0.1:8080"
    filter_chains: [main]
filter_chains:
  - name: main
    filters:
      - filter: policy
        config_path: /etc/praxis/policy.yaml
      - filter: router
        routes:
          - path: /v1/models
            cluster: upstream
clusters:
  - name: upstream
    endpoints: ["10.96.0.20:9090"]
"#,
        );
        let refusal = match validate_no_policy_filter(&config) {
            Err(refusal) => {
                assert!(refusal.contains("filter chain 'main'"), "names the chain: {refusal}");
                refusal
            },
            Ok(()) => std::process::abort(),
        };
        assert!(
            refusal.contains("does not include the policy engine"),
            "explains the missing engine: {refusal}"
        );
        assert!(
            refusal.contains("praxisConfig.render.auth.mode: none"),
            "directs to the supported auth mode: {refusal}"
        );
    }

    /// Reject a policy filter nested in an inline branch chain.
    #[test]
    fn refuses_policy_filter_in_inline_branch_chain() {
        let config = config_with_policy_chain(
            r#"
listeners:
  - name: web
    address: "127.0.0.1:8080"
    filter_chains: [main]
filter_chains:
  - name: main
    filters:
      - filter: router
        routes:
          - path: /v1/models
            cluster: upstream
        branch_chains:
          - name: authz
            rejoin: terminal
            chains:
              - name: inline-authz
                filters:
                  - filter: policy
                    config_path: /etc/praxis/policy.yaml
clusters:
  - name: upstream
    endpoints: ["10.96.0.20:9090"]
"#,
        );
        let refusal = match validate_no_policy_filter(&config) {
            Err(refusal) => {
                assert!(
                    refusal.contains("filter chain 'inline-authz'"),
                    "names the branch: {refusal}"
                );
                refusal
            },
            Ok(()) => std::process::abort(),
        };
        assert!(
            refusal.contains("does not include the policy engine"),
            "explains the missing engine: {refusal}"
        );
    }

    /// Accept a config whose filter chains do not use policy.
    #[test]
    fn accepts_config_without_policy_filter() {
        let config = config_with_policy_chain(
            r#"
listeners:
  - name: web
    address: "127.0.0.1:8080"
    filter_chains: [main]
filter_chains:
  - name: main
    filters:
      - filter: headers
        request_remove: [Authorization]
      - filter: router
        routes:
          - path: /v1/models
            cluster: upstream
clusters:
  - name: upstream
    endpoints: ["10.96.0.20:9090"]
"#,
        );
        assert!(
            validate_no_policy_filter(&config).is_ok(),
            "config without a policy filter should be accepted"
        );
    }
}
