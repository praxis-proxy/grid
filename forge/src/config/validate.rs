//! Semantic validation rules for [`ForgeConfig`].
//!
//! Each rule is a small function.  [`validate`] runs them all and
//! reports the first failure.

use std::collections::BTreeSet;

use crate::{
    config::{API_VERSION, ForgeConfig, KIND, StepSpec},
    error::ForgeError,
};

/// Run all validation rules against a parsed configuration.
///
/// # Errors
///
/// Returns [`ForgeError::Validation`] if any rule fails.
pub fn validate(config: &ForgeConfig) -> Result<(), ForgeError> {
    check_api_version(config)?;
    check_kind(config)?;
    check_metadata_name(&config.metadata.name)?;
    check_network_name(&config.metadata.name, &config.spec)?;
    check_cluster_names(config)?;
    check_cluster_nodes(config)?;
    check_service_names(config)?;
    check_services(config)?;
    check_stack_names(config)?;
    check_cluster_stack_refs(config)?;
    check_stack_steps(config)?;
    check_no_templates(config)?;
    Ok(())
}

/// `apiVersion` must match the current schema.
fn check_api_version(config: &ForgeConfig) -> Result<(), ForgeError> {
    if config.api_version != API_VERSION {
        return Err(ForgeError::Validation(format!(
            "expected apiVersion {API_VERSION:?}, got {:?}",
            config.api_version,
        )));
    }
    Ok(())
}

/// `kind` must be `"Environment"`.
fn check_kind(config: &ForgeConfig) -> Result<(), ForgeError> {
    if config.kind != KIND {
        return Err(ForgeError::Validation(format!(
            "expected kind {KIND:?}, got {:?}",
            config.kind,
        )));
    }
    Ok(())
}

/// Validate that a name is a valid DNS label.
fn check_dns_label(name: &str, context: &str) -> Result<(), ForgeError> {
    if name.is_empty() {
        return Err(ForgeError::Validation(format!("{context}: name must not be empty")));
    }
    validate_dns_label_rules(name, context)
}

/// Check DNS label character and length rules.
fn validate_dns_label_rules(name: &str, context: &str) -> Result<(), ForgeError> {
    if name.len() > 63 {
        return Err(ForgeError::Validation(format!(
            "{context}: {name:?} exceeds 63 characters"
        )));
    }
    check_dns_label_chars(name, context)
}

/// Verify characters and leading/trailing constraints.
fn check_dns_label_chars(name: &str, context: &str) -> Result<(), ForgeError> {
    let valid = name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if !valid {
        return Err(ForgeError::Validation(format!(
            "{context}: {name:?} contains invalid characters \
             (allowed: lowercase alphanumeric and hyphens)"
        )));
    }
    check_dns_label_edges(name, context)
}

/// DNS labels must not start or end with a hyphen.
fn check_dns_label_edges(name: &str, context: &str) -> Result<(), ForgeError> {
    if name.starts_with('-') || name.ends_with('-') {
        return Err(ForgeError::Validation(format!(
            "{context}: {name:?} must not start or end with a hyphen"
        )));
    }
    Ok(())
}

/// `metadata.name` must be a DNS label.
fn check_metadata_name(name: &str) -> Result<(), ForgeError> {
    check_dns_label(name, "metadata.name")
}

/// Derived network name must be safe for Docker/Podman.
fn check_network_name(env_name: &str, spec: &crate::config::EnvironmentSpec) -> Result<(), ForgeError> {
    let wants = spec.network.as_ref().is_some_and(|n| n.cross_cluster);
    if !wants {
        return Ok(());
    }
    let derived = format!("{env_name}-net");
    check_docker_name(&derived, "derived network name")
}

/// Validate a Docker/Podman resource name.
fn check_docker_name(name: &str, context: &str) -> Result<(), ForgeError> {
    if name.is_empty() || name.len() > 128 {
        return Err(ForgeError::Validation(format!(
            "{context}: {name:?} must be 1\u{2013}128 characters"
        )));
    }
    let valid = name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.');
    if !valid {
        return Err(ForgeError::Validation(format!(
            "{context}: {name:?} contains characters unsafe for Docker/Podman"
        )));
    }
    Ok(())
}

/// Cluster names must be unique and DNS-label-valid.
fn check_cluster_names(config: &ForgeConfig) -> Result<(), ForgeError> {
    let mut seen = BTreeSet::new();
    for cluster in &config.spec.clusters {
        check_dns_label(&cluster.name, "cluster")?;
        if !seen.insert(&cluster.name) {
            return Err(ForgeError::Validation(format!(
                "duplicate cluster name: {:?}",
                cluster.name,
            )));
        }
    }
    Ok(())
}

/// Each cluster needs at least one control-plane node.
fn check_cluster_nodes(config: &ForgeConfig) -> Result<(), ForgeError> {
    for cluster in &config.spec.clusters {
        if cluster.nodes.control_planes == 0 {
            return Err(ForgeError::Validation(format!(
                "cluster {:?}: controlPlanes must be at least 1",
                cluster.name,
            )));
        }
    }
    Ok(())
}

/// Service names must be unique and DNS-label-valid.
fn check_service_names(config: &ForgeConfig) -> Result<(), ForgeError> {
    let mut seen = BTreeSet::new();
    for service in &config.spec.services {
        check_dns_label(&service.name, "service")?;
        if !seen.insert(&service.name) {
            return Err(ForgeError::Validation(format!(
                "duplicate service name: {:?}",
                service.name,
            )));
        }
    }
    Ok(())
}

/// Validate host service fields.
fn check_services(config: &ForgeConfig) -> Result<(), ForgeError> {
    for service in &config.spec.services {
        check_non_blank(&service.image, &format!("service {:?}: image", service.name))?;
        for port in &service.ports {
            check_port_protocol(&port.protocol, &service.name)?;
        }
    }
    Ok(())
}

/// Port protocols are intentionally narrow.
fn check_port_protocol(protocol: &str, service_name: &str) -> Result<(), ForgeError> {
    match protocol {
        "tcp" | "udp" => Ok(()),
        other => Err(ForgeError::Validation(format!(
            "service {service_name:?}: unsupported port protocol {other:?} \
             (expected tcp or udp)"
        ))),
    }
}

/// Stack names must be DNS-label-valid.
fn check_stack_names(config: &ForgeConfig) -> Result<(), ForgeError> {
    for name in config.spec.stacks.keys() {
        check_dns_label(name, "stack")?;
    }
    Ok(())
}

/// Validate every declared stack step.
fn check_stack_steps(config: &ForgeConfig) -> Result<(), ForgeError> {
    for (stack_name, stack) in &config.spec.stacks {
        for step in &stack.steps {
            check_step(stack_name, step)?;
        }
    }
    Ok(())
}

/// Validate a single stack step.
#[expect(clippy::too_many_lines, reason = "one explicit match arm per stack step")]
fn check_step(stack_name: &str, step: &StepSpec) -> Result<(), ForgeError> {
    match step {
        StepSpec::Url { url, sha256 } => check_url_step(stack_name, url, sha256),
        StepSpec::Manifest { path } | StepSpec::Kustomize { path } => {
            check_relative_path(path, &format!("stack {stack_name:?}: path"))
        },
        StepSpec::Helm {
            release,
            chart,
            version,
            namespace,
            values: _,
        } => check_helm_step(stack_name, release, chart, version, namespace.as_deref()),
        StepSpec::Deployment {
            name,
            image,
            namespace,
            args: _,
        } => check_named_workload_step(stack_name, "deployment", name, image, namespace.as_deref()),
        StepSpec::Service {
            name,
            port: _,
            namespace,
        } => check_named_resource_step(stack_name, "service", name, namespace.as_deref()),
        StepSpec::Wait {
            resource,
            condition,
            timeout,
        } => check_wait_step(stack_name, resource, condition, timeout),
        StepSpec::Exec { command } => check_exec_step(stack_name, command),
        StepSpec::ForEach { property, steps } => check_for_each_step(stack_name, property, steps),
        StepSpec::MetallbAutoPool { name } => check_named_resource_step(stack_name, "metallb pool", name, None),
    }
}

/// Validate a remote URL manifest step.
fn check_url_step(stack_name: &str, url: &str, sha256: &str) -> Result<(), ForgeError> {
    check_non_blank(url, &format!("stack {stack_name:?}: url"))?;
    if !url.starts_with("https://") {
        return Err(ForgeError::Validation(format!(
            "stack {stack_name:?}: remote manifest URLs must use https"
        )));
    }
    check_sha256(sha256, &format!("stack {stack_name:?}: sha256"))
}

/// Validate a SHA-256 hex digest.
fn check_sha256(value: &str, context: &str) -> Result<(), ForgeError> {
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ForgeError::Validation(format!(
            "{context}: expected a 64-character SHA-256 hex digest"
        )));
    }
    Ok(())
}

/// Validate a path that must stay relative to the config root.
fn check_relative_path(path: &str, context: &str) -> Result<(), ForgeError> {
    check_non_blank(path, context)?;
    if path.starts_with('/') || path.split('/').any(|part| part == "..") {
        return Err(ForgeError::Validation(format!(
            "{context}: path must be relative and must not escape the config root"
        )));
    }
    Ok(())
}

/// Validate a Helm step.
fn check_helm_step(
    stack_name: &str,
    release: &str,
    chart: &str,
    version: &str,
    namespace: Option<&str>,
) -> Result<(), ForgeError> {
    check_dns_label(release, &format!("stack {stack_name:?}: helm release"))?;
    check_non_blank(chart, &format!("stack {stack_name:?}: helm chart"))?;
    check_non_blank(version, &format!("stack {stack_name:?}: helm version"))?;
    check_optional_namespace(stack_name, namespace)
}

/// Validate a resource step with a Kubernetes-style name.
fn check_named_resource_step(
    stack_name: &str,
    kind: &str,
    name: &str,
    namespace: Option<&str>,
) -> Result<(), ForgeError> {
    check_dns_label(name, &format!("stack {stack_name:?}: {kind} name"))?;
    check_optional_namespace(stack_name, namespace)
}

/// Validate a workload step that includes an image.
fn check_named_workload_step(
    stack_name: &str,
    kind: &str,
    name: &str,
    image: &str,
    namespace: Option<&str>,
) -> Result<(), ForgeError> {
    check_named_resource_step(stack_name, kind, name, namespace)?;
    check_non_blank(image, &format!("stack {stack_name:?}: {kind} image"))
}

/// Validate an optional namespace.
fn check_optional_namespace(stack_name: &str, namespace: Option<&str>) -> Result<(), ForgeError> {
    if let Some(ns) = namespace {
        check_dns_label(ns, &format!("stack {stack_name:?}: namespace"))?;
    }
    Ok(())
}

/// Validate a wait step.
fn check_wait_step(stack_name: &str, resource: &str, condition: &str, timeout: &str) -> Result<(), ForgeError> {
    check_non_blank(resource, &format!("stack {stack_name:?}: wait resource"))?;
    check_non_blank(condition, &format!("stack {stack_name:?}: wait condition"))?;
    check_non_blank(timeout, &format!("stack {stack_name:?}: wait timeout"))
}

/// Validate an exec step.
fn check_exec_step(stack_name: &str, command: &[String]) -> Result<(), ForgeError> {
    if command.is_empty() {
        return Err(ForgeError::Validation(format!(
            "stack {stack_name:?}: exec command must not be empty"
        )));
    }
    for arg in command {
        check_non_blank(arg, &format!("stack {stack_name:?}: exec command argument"))?;
    }
    Ok(())
}

/// Validate a for-each step.
fn check_for_each_step(stack_name: &str, property: &str, steps: &[StepSpec]) -> Result<(), ForgeError> {
    check_non_blank(property, &format!("stack {stack_name:?}: for-each property"))?;
    if steps.is_empty() {
        return Err(ForgeError::Validation(format!(
            "stack {stack_name:?}: for-each steps must not be empty"
        )));
    }
    for step in steps {
        check_step(stack_name, step)?;
    }
    Ok(())
}

/// Check a required text field.
fn check_non_blank(value: &str, context: &str) -> Result<(), ForgeError> {
    if value.trim().is_empty() {
        return Err(ForgeError::Validation(format!("{context}: must not be blank")));
    }
    Ok(())
}

/// Every stack referenced by a cluster must exist in `spec.stacks`.
fn check_cluster_stack_refs(config: &ForgeConfig) -> Result<(), ForgeError> {
    for cluster in &config.spec.clusters {
        for stack_ref in &cluster.stacks {
            if !config.spec.stacks.contains_key(stack_ref) {
                return Err(ForgeError::Validation(format!(
                    "cluster {:?} references unknown stack {:?}",
                    cluster.name, stack_ref,
                )));
            }
        }
    }
    Ok(())
}

/// Reject template-looking values (`{{ ... }}`).
///
/// F1 has no template engine; ambiguous template syntax should fail
/// early rather than silently pass through.
fn check_no_templates(config: &ForgeConfig) -> Result<(), ForgeError> {
    let yaml = serde_yaml::to_string(config).map_err(|e| ForgeError::Validation(e.to_string()))?;
    if yaml.contains("{{") && yaml.contains("}}") {
        return Err(ForgeError::Validation(
            "template syntax ({{ ... }}) is not supported in this \
             version; remove template expressions or wait for a \
             future release"
                .to_owned(),
        ));
    }
    Ok(())
}

// -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::config::{
        API_VERSION, ClusterSpec, EnvironmentSpec, ForgeConfig, KIND, Metadata, NodeConfig, PortMapping, RuntimeConfig,
        ServiceSpec, StackSpec, StepSpec,
    };

    /// Build a minimal valid config for test modification.
    fn base_config() -> ForgeConfig {
        ForgeConfig {
            api_version: API_VERSION.to_owned(),
            kind: KIND.to_owned(),
            metadata: Metadata {
                name: "test".to_owned(),
            },
            spec: EnvironmentSpec {
                runtime: RuntimeConfig::default(),
                network: None,
                clusters: Vec::new(),
                services: Vec::new(),
                certificates: None,
                stacks: BTreeMap::new(),
            },
        }
    }

    #[test]
    fn valid_minimal_config_passes() {
        let config = base_config();
        validate(&config).unwrap_or_else(|_e| {
            std::process::abort();
        });
    }

    #[test]
    fn wrong_api_version_rejected() {
        let mut config = base_config();
        config.api_version = "v2".to_owned();
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("apiVersion"), "expected apiVersion error, got: {msg}");
    }

    #[test]
    fn wrong_kind_rejected() {
        let mut config = base_config();
        config.kind = "Cluster".to_owned();
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("kind"), "expected kind error, got: {msg}");
    }

    #[test]
    fn empty_name_rejected() {
        let mut config = base_config();
        config.metadata.name = String::new();
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("empty"), "expected empty name error, got: {msg}");
    }

    #[test]
    fn non_dns_name_rejected() {
        let mut config = base_config();
        config.metadata.name = "Not_Valid".to_owned();
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("invalid characters"), "expected DNS error, got: {msg}");
    }

    #[test]
    fn leading_hyphen_name_rejected() {
        let mut config = base_config();
        config.metadata.name = "-bad".to_owned();
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("hyphen"), "expected hyphen error, got: {msg}");
    }

    #[test]
    fn duplicate_cluster_names_rejected() {
        let mut config = base_config();
        let cluster = ClusterSpec {
            name: "dupe".to_owned(),
            nodes: NodeConfig::default(),
            stacks: Vec::new(),
            properties: BTreeMap::new(),
        };
        config.spec.clusters = vec![cluster.clone(), cluster];
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate cluster"),
            "expected duplicate error, got: {msg}"
        );
    }

    #[test]
    fn cluster_referencing_missing_stack_rejected() {
        let mut config = base_config();
        config.spec.clusters = vec![ClusterSpec {
            name: "c1".to_owned(),
            nodes: NodeConfig::default(),
            stacks: vec!["nonexistent".to_owned()],
            properties: BTreeMap::new(),
        }];
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(
            msg.contains("unknown stack"),
            "expected missing stack error, got: {msg}"
        );
    }

    #[test]
    fn template_looking_values_rejected() {
        let mut config = base_config();
        config.spec.clusters = vec![ClusterSpec {
            name: "c1".to_owned(),
            nodes: NodeConfig::default(),
            stacks: Vec::new(),
            properties: BTreeMap::from([(
                "model".to_owned(),
                serde_json::Value::String("{{ .Property.model }}".to_owned()),
            )]),
        }];
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("template syntax"), "expected template error, got: {msg}");
    }

    #[test]
    fn zero_control_planes_rejected() {
        let mut config = base_config();
        config.spec.clusters = vec![ClusterSpec {
            name: "c1".to_owned(),
            nodes: NodeConfig {
                control_planes: 0,
                workers: 1,
            },
            stacks: Vec::new(),
            properties: BTreeMap::new(),
        }];
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(
            msg.contains("controlPlanes"),
            "expected control-plane count error, got: {msg}"
        );
    }

    #[test]
    fn invalid_service_protocol_rejected() {
        let mut config = base_config();
        config.spec.services = vec![ServiceSpec {
            name: "svc".to_owned(),
            image: "example/service:v1".to_owned(),
            ports: vec![PortMapping {
                host: 8080,
                container: 8080,
                protocol: "sctp".to_owned(),
            }],
            args: Vec::new(),
            env: BTreeMap::new(),
        }];
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("protocol"), "expected protocol error, got: {msg}");
    }

    #[test]
    fn unpinned_remote_url_rejected_by_schema() {
        let yaml = "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: test
spec:
  runtime:
    provider: auto
  stacks:
    base:
      steps:
        - type: url
          url: https://example.invalid/install.yaml
";
        let result: Result<ForgeConfig, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err(), "url steps must require sha256");
    }

    #[test]
    fn non_https_remote_url_rejected() {
        let mut config = base_config();
        config.spec.stacks.insert(
            "base".to_owned(),
            StackSpec {
                description: None,
                steps: vec![StepSpec::Url {
                    url: "http://example.invalid/install.yaml".to_owned(),
                    sha256: "a".repeat(64),
                }],
            },
        );
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("https"), "expected https error, got: {msg}");
    }

    #[test]
    fn invalid_sha256_rejected() {
        let mut config = base_config();
        config.spec.stacks.insert(
            "base".to_owned(),
            StackSpec {
                description: None,
                steps: vec![StepSpec::Url {
                    url: "https://example.invalid/install.yaml".to_owned(),
                    sha256: "not-a-digest".to_owned(),
                }],
            },
        );
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("SHA-256"), "expected digest error, got: {msg}");
    }

    #[test]
    fn empty_exec_command_rejected() {
        let mut config = base_config();
        config.spec.stacks.insert(
            "base".to_owned(),
            StackSpec {
                description: None,
                steps: vec![StepSpec::Exec { command: Vec::new() }],
            },
        );
        let Err(err) = validate(&config) else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("exec command"), "expected exec error, got: {msg}");
    }

    #[test]
    fn network_cross_cluster_passes_validation() {
        let mut config = base_config();
        config.spec.network = Some(crate::config::NetworkConfig { cross_cluster: true });
        validate(&config).unwrap_or_else(|_e| {
            std::process::abort();
        });
    }

    #[test]
    fn network_config_without_cross_cluster_passes() {
        let mut config = base_config();
        config.spec.network = Some(crate::config::NetworkConfig { cross_cluster: false });
        validate(&config).unwrap_or_else(|_e| {
            std::process::abort();
        });
    }
}
