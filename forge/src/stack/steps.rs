//! Stack step-to-command mapping.
//!
//! Each [`StepSpec`](crate::config::StepSpec) variant maps to one or
//! more [`CommandSpec`] values.
//! YAML generation for in-line resources (Deployment, Service,
//! `MetalLB` pool) is handled here.  All commands use structured
//! `CommandSpec` — no shell strings.

use std::collections::BTreeMap;

use sha2::Digest as _;

use crate::{
    command::runner::{CommandOutput, CommandSpec},
    error::ForgeError,
};

/// Maximum remote manifest size accepted by Forge URL steps.
pub const MAX_REMOTE_MANIFEST_BYTES: usize = 1_048_576;

// -------------------------------------------------------------
// Parameter types
// -------------------------------------------------------------

/// Parameters for a Helm release installation.
pub struct HelmParams<'a> {
    /// kubectl/helm `--kube-context` value.
    pub context: &'a str,
    /// Helm release name.
    pub release: &'a str,
    /// Chart reference.
    pub chart: &'a str,
    /// Chart version.
    pub version: &'a str,
    /// Target namespace (optional).
    pub namespace: Option<&'a str>,
}

// -------------------------------------------------------------
// Command spec builders
// -------------------------------------------------------------

/// Build a bounded `curl` command spec for downloading content.
pub fn curl_download_spec(url: &str) -> CommandSpec {
    CommandSpec {
        program: "curl".into(),
        args: vec![
            "--fail".into(),
            "--location".into(),
            "--silent".into(),
            "--show-error".into(),
            "--max-filesize".into(),
            MAX_REMOTE_MANIFEST_BYTES.to_string().into(),
            url.into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `kubectl apply -f -` command spec with content on stdin.
pub fn kubectl_stdin_apply(context: &str, content: &[u8]) -> CommandSpec {
    CommandSpec {
        program: "kubectl".into(),
        args: vec![
            "--context".into(),
            context.into(),
            "apply".into(),
            "-f".into(),
            "-".into(),
        ],
        env: BTreeMap::default(),
        stdin: Some(content.to_vec()),
        redact: Vec::new(),
    }
}

/// Build a `kubectl apply -f <path>` command spec.
pub fn kubectl_apply(context: &str, path: &str) -> CommandSpec {
    CommandSpec {
        program: "kubectl".into(),
        args: vec![
            "--context".into(),
            context.into(),
            "apply".into(),
            "-f".into(),
            path.into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `kubectl apply -k <path>` command spec.
pub fn kubectl_kustomize(context: &str, path: &str) -> CommandSpec {
    CommandSpec {
        program: "kubectl".into(),
        args: vec![
            "--context".into(),
            context.into(),
            "apply".into(),
            "-k".into(),
            path.into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `helm upgrade --install` command spec.
///
/// Values are serialized to YAML and passed via stdin with
/// `--values -` when non-empty.
///
/// # Errors
///
/// Returns [`ForgeError`] if values serialization fails.
pub fn helm_upgrade_spec(
    params: &HelmParams<'_>,
    values: &BTreeMap<String, serde_json::Value>,
) -> Result<CommandSpec, ForgeError> {
    let mut args: Vec<std::ffi::OsString> = vec![
        "upgrade".into(),
        "--install".into(),
        params.release.into(),
        params.chart.into(),
        "--version".into(),
        params.version.into(),
        "--kube-context".into(),
        params.context.into(),
    ];
    append_helm_namespace(&mut args, params.namespace);
    let stdin = helm_values_stdin(&mut args, values)?;
    Ok(CommandSpec {
        program: "helm".into(),
        args,
        env: BTreeMap::default(),
        stdin,
        redact: Vec::new(),
    })
}

/// Append `--namespace <ns> --create-namespace` if namespace is set.
fn append_helm_namespace(args: &mut Vec<std::ffi::OsString>, ns: Option<&str>) {
    if let Some(ns) = ns {
        args.push("--namespace".into());
        args.push(ns.into());
        args.push("--create-namespace".into());
    }
}

/// Serialize helm values to YAML stdin if non-empty.
fn helm_values_stdin(
    args: &mut Vec<std::ffi::OsString>,
    values: &BTreeMap<String, serde_json::Value>,
) -> Result<Option<Vec<u8>>, ForgeError> {
    if values.is_empty() {
        return Ok(None);
    }
    args.push("--values".into());
    args.push("-".into());
    let yaml =
        serde_yaml::to_string(values).map_err(|e| ForgeError::Config(format!("cannot serialize helm values: {e}")))?;
    Ok(Some(yaml.into_bytes()))
}

/// Build a `kubectl wait` command spec.
pub fn kubectl_wait_spec(context: &str, resource: &str, condition: &str, timeout: &str) -> CommandSpec {
    CommandSpec {
        program: "kubectl".into(),
        args: vec![
            "--context".into(),
            context.into(),
            "wait".into(),
            resource.into(),
            format!("--for=condition={condition}").into(),
            format!("--timeout={timeout}").into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a command spec from an explicit command array.
///
/// The first element is the program, the rest are arguments.
///
/// # Errors
///
/// Returns [`ForgeError::Config`] if the command array is empty.
pub fn exec_spec(command: &[String]) -> Result<CommandSpec, ForgeError> {
    let program = command
        .first()
        .ok_or_else(|| ForgeError::Config("exec step has empty command".to_owned()))?;
    Ok(CommandSpec {
        program: program.into(),
        args: command
            .get(1..)
            .unwrap_or(&[])
            .iter()
            .map(std::ffi::OsString::from)
            .collect(),
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    })
}

/// Build a Docker network inspect command spec for `MetalLB`.
pub fn docker_network_inspect(binary: &str, network: &str) -> CommandSpec {
    CommandSpec {
        program: binary.into(),
        args: vec!["network".into(), "inspect".into(), network.into()],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `kubectl get` command with jsonpath output for value capture.
pub fn kubectl_get_jsonpath(context: &str, resource: &str, namespace: Option<&str>, jsonpath: &str) -> CommandSpec {
    let mut args: Vec<std::ffi::OsString> = vec!["--context".into(), context.into(), "get".into(), resource.into()];
    if let Some(ns) = namespace {
        args.push("-n".into());
        args.push(ns.into());
    }
    args.push("-o".into());
    args.push(format!("jsonpath={jsonpath}").into());
    CommandSpec {
        program: "kubectl".into(),
        args,
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `kubectl get configmap` command to read the `CoreDNS` Corefile.
pub fn kubectl_get_corefile(context: &str) -> CommandSpec {
    CommandSpec {
        program: "kubectl".into(),
        args: vec![
            "--context".into(),
            context.into(),
            "get".into(),
            "configmap".into(),
            "coredns".into(),
            "-n".into(),
            "kube-system".into(),
            "-o".into(),
            "jsonpath={.data.Corefile}".into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `kubectl rollout restart` command spec.
pub fn kubectl_rollout_restart(context: &str, resource: &str, namespace: &str) -> CommandSpec {
    CommandSpec {
        program: "kubectl".into(),
        args: vec![
            "--context".into(),
            context.into(),
            "rollout".into(),
            "restart".into(),
            resource.into(),
            "-n".into(),
            namespace.into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

// -------------------------------------------------------------
// YAML generators
// -------------------------------------------------------------

/// Generate a Deployment manifest YAML.
pub fn generate_deployment_yaml(name: &str, image: &str, namespace: Option<&str>, args: &[String]) -> String {
    let mut manifest = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": build_metadata(name, namespace),
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app": name } },
            "template": {
                "metadata": { "labels": { "app": name } },
                "spec": {
                    "containers": [{
                        "name": name,
                        "image": image,
                    }]
                }
            }
        }
    });
    append_container_args(&mut manifest, args);
    yaml_serialize(&manifest)
}

/// Generate a Service manifest YAML.
pub fn generate_service_yaml(name: &str, port: u16, namespace: Option<&str>) -> String {
    let manifest = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": build_metadata(name, namespace),
        "spec": {
            "selector": { "app": name },
            "ports": [{ "port": port, "targetPort": port }]
        }
    });
    yaml_serialize(&manifest)
}

/// Generate `MetalLB` `IPAddressPool` and `L2Advertisement` YAML.
pub fn generate_metallb_pool_yaml(name: &str, addresses: &str) -> String {
    let pool = serde_json::json!({
        "apiVersion": "metallb.io/v1beta1",
        "kind": "IPAddressPool",
        "metadata": {
            "name": name,
            "namespace": "metallb-system"
        },
        "spec": {
            "addresses": [addresses]
        }
    });
    let advert = serde_json::json!({
        "apiVersion": "metallb.io/v1beta1",
        "kind": "L2Advertisement",
        "metadata": {
            "name": format!("{name}-l2"),
            "namespace": "metallb-system"
        },
        "spec": {
            "ipAddressPools": [name]
        }
    });
    format!("{}\n---\n{}", yaml_serialize(&pool), yaml_serialize(&advert))
}

/// Generate a `CoreDNS` Corefile snippet for zone forwarding.
pub fn generate_corefile_snippet(zone: &str, upstreams: &[String]) -> String {
    format!("{zone}:53 {{\n    forward . {}\n}}", upstreams.join(" "))
}

/// Generate a `CoreDNS` `ConfigMap` YAML with the given Corefile.
pub fn generate_coredns_configmap(corefile: &str) -> String {
    let manifest = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "coredns",
            "namespace": "kube-system"
        },
        "data": {
            "Corefile": corefile
        }
    });
    yaml_serialize(&manifest)
}

// -------------------------------------------------------------
// Network CIDR helpers
// -------------------------------------------------------------

/// Extract the first subnet CIDR from Docker network inspect output.
///
/// Parses the real nested shape: `[{ "IPAM": { "Config": [{ "Subnet": "..." }] } }]`.
///
/// # Errors
///
/// Returns [`ForgeError::Command`] if parsing fails or no subnet found.
pub fn parse_network_cidr(inspect_output: &str) -> Result<String, ForgeError> {
    let arr: Vec<serde_json::Value> = serde_json::from_str(inspect_output.trim())
        .map_err(|e| cmd_error("network inspect", &format!("invalid JSON: {e}")))?;
    let entry = arr
        .first()
        .ok_or_else(|| cmd_error("network inspect", "no network entries"))?;
    extract_subnet_from_ipam(entry)
}

/// Navigate the nested IPAM config to extract and validate the first subnet.
fn extract_subnet_from_ipam(entry: &serde_json::Value) -> Result<String, ForgeError> {
    let subnet = entry
        .get("IPAM")
        .and_then(|ipam| ipam.get("Config"))
        .and_then(|config| config.get(0))
        .and_then(|cfg| cfg.get("Subnet"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| cmd_error("network inspect", "no Subnet in IPAM.Config"))?;
    parse_cidr_parts(subnet)
        .map_err(|_err| cmd_error("network inspect", &format!("invalid CIDR in IPAM.Config: {subnet:?}")))?;
    Ok(subnet.to_owned())
}

/// Compute a `MetalLB` address range from a CIDR string.
///
/// Returns a range at the high end of the subnet, e.g.
/// `172.18.255.200-172.18.255.250` for `172.18.0.0/16`.
///
/// Requires at least 56 usable host addresses (offsets 5 through 55).
///
/// # Errors
///
/// Returns [`ForgeError::Config`] if the CIDR cannot be parsed or the
/// subnet is too small.
pub fn compute_metallb_range(cidr: &str) -> Result<String, ForgeError> {
    let (base, prefix) = parse_cidr_parts(cidr)?;
    let max_host = cidr_max_host(prefix);
    if max_host < 56 {
        return Err(ForgeError::Config(format!(
            "subnet {cidr} too small for MetalLB range (need 56 host addresses, have {max_host})"
        )));
    }
    Ok(build_range(std::net::Ipv4Addr::from(base), prefix))
}

/// Number of addresses allocated to each cluster's `MetalLB` pool.
const ADDRESSES_PER_POOL: u32 = 20;

/// Addresses reserved at the top of the subnet (near broadcast).
const POOL_RESERVED_TOP: u32 = 5;

/// Compute a deterministic, non-overlapping `MetalLB` pool for a cluster.
///
/// Each cluster gets 20 addresses allocated from
/// the high end of the subnet, indexed by position.
///
/// # Errors
///
/// Returns [`ForgeError::Config`] if the CIDR is invalid or the
/// subnet is too small for the requested number of clusters.
pub fn compute_cluster_pool(cidr: &str, cluster_index: usize, cluster_count: usize) -> Result<String, ForgeError> {
    if cluster_count == 0 {
        return Err(ForgeError::Config("cluster count must be at least 1".to_owned()));
    }
    if cluster_index >= cluster_count {
        return Err(ForgeError::Config(format!(
            "cluster index {cluster_index} exceeds cluster count {cluster_count}"
        )));
    }
    let (base, prefix) = parse_cidr_parts(cidr)?;
    let max_host = cidr_max_host(prefix);
    check_pool_capacity(max_host, cluster_count)?;
    build_cluster_range(base, max_host, cluster_index)
}

/// Validate the subnet has capacity for all cluster pools.
fn check_pool_capacity(max_host: u32, cluster_count: usize) -> Result<(), ForgeError> {
    let count = usize_to_u32(cluster_count)?;
    let needed = POOL_RESERVED_TOP.saturating_add(ADDRESSES_PER_POOL.saturating_mul(count));
    if needed > max_host {
        return Err(ForgeError::Config(format!(
            "subnet too small for {cluster_count} cluster pools (need {needed} addresses, have {max_host})"
        )));
    }
    Ok(())
}

/// Compute the address range for a single cluster pool.
fn build_cluster_range(base: u32, max_host: u32, index: usize) -> Result<String, ForgeError> {
    let idx = usize_to_u32(index)?;
    let pool_end =
        base | max_host.saturating_sub(POOL_RESERVED_TOP.saturating_add(ADDRESSES_PER_POOL.saturating_mul(idx)));
    let pool_start = pool_end.saturating_sub(ADDRESSES_PER_POOL.saturating_sub(1));
    let start = std::net::Ipv4Addr::from(pool_start);
    let end = std::net::Ipv4Addr::from(pool_end);
    Ok(format!("{start}-{end}"))
}

/// Safe `usize` to `u32` conversion.
fn usize_to_u32(val: usize) -> Result<u32, ForgeError> {
    u32::try_from(val).map_err(|_err| ForgeError::Config(format!("value {val} exceeds u32 range")))
}

/// Parse a CIDR string into a base address (as u32) and prefix length.
fn parse_cidr_parts(cidr: &str) -> Result<(u32, u32), ForgeError> {
    let (ip_str, prefix_str) = split_cidr(cidr)?;
    let ip: std::net::Ipv4Addr = ip_str
        .parse()
        .map_err(|e| ForgeError::Config(format!("invalid CIDR IP '{ip_str}': {e}")))?;
    let prefix: u32 = prefix_str
        .parse()
        .map_err(|e| ForgeError::Config(format!("invalid CIDR prefix '{prefix_str}': {e}")))?;
    if prefix > 32 {
        return Err(ForgeError::Config(format!("CIDR prefix /{prefix} exceeds /32")));
    }
    Ok((u32::from(ip), prefix))
}

/// Compute the maximum host value for a given prefix length.
fn cidr_max_host(prefix: u32) -> u32 {
    let host_bits = 32_u32.saturating_sub(prefix);
    1_u32.checked_shl(host_bits).unwrap_or(0).saturating_sub(1)
}

/// Split a CIDR string into IP and prefix parts.
fn split_cidr(cidr: &str) -> Result<(&str, &str), ForgeError> {
    let (ip, prefix) = cidr
        .split_once('/')
        .ok_or_else(|| ForgeError::Config(format!("invalid CIDR format: '{cidr}'")))?;
    Ok((ip, prefix))
}

/// Compute the address range from network address and prefix.
fn build_range(base: std::net::Ipv4Addr, prefix: u32) -> String {
    let base_u32 = u32::from(base);
    let max_host = cidr_max_host(prefix);
    let range_start = base_u32 | max_host.saturating_sub(55);
    let range_end = base_u32 | max_host.saturating_sub(5);
    let start = std::net::Ipv4Addr::from(range_start);
    let end = std::net::Ipv4Addr::from(range_end);
    format!("{start}-{end}")
}

// -------------------------------------------------------------
// Connectivity helpers
// -------------------------------------------------------------

/// Build a [`CommandSpec`] for an HTTP connectivity check.
pub fn kubectl_connectivity_check(context: &str, target: &str, port: u16) -> CommandSpec {
    let pod = conn_check_pod_name(target, port);
    CommandSpec {
        program: "kubectl".into(),
        args: vec![
            "--context".into(),
            context.into(),
            "run".into(),
            pod.into(),
            "--rm".into(),
            "-i".into(),
            "--restart=Never".into(),
            "--image=busybox:1.36".into(),
            "--".into(),
            "wget".into(),
            "-qO-".into(),
            "-T5".into(),
            format!("http://{target}:{port}").into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a [`CommandSpec`] for a TCP connectivity check.
pub fn kubectl_tcp_check(context: &str, target: &str, port: u16) -> CommandSpec {
    let pod = conn_check_pod_name(target, port);
    CommandSpec {
        program: "kubectl".into(),
        args: vec![
            "--context".into(),
            context.into(),
            "run".into(),
            pod.into(),
            "--rm".into(),
            "-i".into(),
            "--restart=Never".into(),
            "--image=busybox:1.36".into(),
            "--".into(),
            "nc".into(),
            "-z".into(),
            "-w5".into(),
            target.into(),
            port.to_string().into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Generate a unique pod name from target and port for connectivity checks.
fn conn_check_pod_name(target: &str, port: u16) -> String {
    let digest = sha2::Sha256::digest(format!("{target}:{port}").as_bytes());
    let hex = format!("{digest:x}");
    let short: String = hex.chars().take(8).collect();
    format!("forge-cc-{short}")
}

// -------------------------------------------------------------
// Command output checking
// -------------------------------------------------------------

/// Check command output for success (exit code 0).
///
/// # Errors
///
/// Returns [`ForgeError::Command`] if the exit code is non-zero.
pub fn check_success(output: &CommandOutput, context: &str) -> Result<(), ForgeError> {
    if output.status == 0 {
        return Ok(());
    }
    Err(cmd_error(
        context,
        &format!("exit code {}: {}", output.status, output.stderr.trim()),
    ))
}

// -------------------------------------------------------------
// Private helpers
// -------------------------------------------------------------

/// Build a Kubernetes metadata object.
fn build_metadata(name: &str, namespace: Option<&str>) -> serde_json::Value {
    let mut meta = serde_json::json!({ "name": name });
    if let Some(ns) = namespace
        && let Some(obj) = meta.as_object_mut()
    {
        obj.insert("namespace".to_owned(), serde_json::Value::String(ns.to_owned()));
    }
    meta
}

/// Append args to the first container in a Deployment manifest.
fn append_container_args(manifest: &mut serde_json::Value, args: &[String]) {
    if args.is_empty() {
        return;
    }
    let args_val: Vec<serde_json::Value> = args.iter().map(|a| serde_json::Value::String(a.clone())).collect();
    let Some(container) = manifest
        .get_mut("spec")
        .and_then(|v| v.get_mut("template"))
        .and_then(|v| v.get_mut("spec"))
        .and_then(|v| v.get_mut("containers"))
        .and_then(|v| v.get_mut(0))
    else {
        return;
    };
    if let Some(obj) = container.as_object_mut() {
        obj.insert("args".to_owned(), serde_json::Value::Array(args_val));
    }
}

/// Serialize a JSON value to YAML.
fn yaml_serialize(val: &serde_json::Value) -> String {
    serde_yaml::to_string(val).unwrap_or_default()
}

/// Build a `ForgeError::Command` with a given context.
fn cmd_error(program: &str, message: &str) -> ForgeError {
    ForgeError::Command {
        program: program.to_owned(),
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_step_builds_kubectl_apply() {
        let spec = kubectl_apply("kind-forge-hub", "manifests/crds.yaml");
        assert_eq!(spec.program, "kubectl", "program should be kubectl");
        let args: Vec<String> = spec.args.iter().map(|a| a.to_string_lossy().into_owned()).collect();
        let joined = args.join(" ");
        assert!(
            joined.contains("--context kind-forge-hub"),
            "should have context: {joined}"
        );
        assert!(
            joined.contains("apply -f manifests/crds.yaml"),
            "should apply file: {joined}"
        );
    }

    #[test]
    fn kustomize_step_builds_kubectl_apply_k() {
        let spec = kubectl_kustomize("kind-forge-hub", "overlays/prod");
        let args: Vec<String> = spec.args.iter().map(|a| a.to_string_lossy().into_owned()).collect();
        let joined = args.join(" ");
        assert!(joined.contains("-k overlays/prod"), "should use -k flag: {joined}");
    }

    #[test]
    fn helm_step_builds_upgrade_install() {
        let params = HelmParams {
            context: "kind-forge-hub",
            release: "metallb",
            chart: "metallb/metallb",
            version: "0.14.5",
            namespace: Some("metallb-system"),
        };
        let values = BTreeMap::from([("key".to_owned(), serde_json::json!("val"))]);
        let spec = helm_upgrade_spec(&params, &values).unwrap_or_else(|_| std::process::abort());
        let args: Vec<String> = spec.args.iter().map(|a| a.to_string_lossy().into_owned()).collect();
        let joined = args.join(" ");
        assert!(
            joined.contains("upgrade --install metallb"),
            "should upgrade --install: {joined}"
        );
        assert!(
            joined.contains("--kube-context kind-forge-hub"),
            "should have context: {joined}"
        );
        assert!(joined.contains("--version 0.14.5"), "should have version: {joined}");
        assert!(
            joined.contains("--namespace metallb-system"),
            "should have namespace: {joined}"
        );
        assert!(joined.contains("--values -"), "should read values from stdin: {joined}");
        assert!(spec.stdin.is_some(), "stdin should contain values YAML");
    }

    #[test]
    fn deployment_yaml_has_expected_structure() {
        let yaml = generate_deployment_yaml(
            "web",
            "nginx:1.25",
            Some("default"),
            &["--port".to_owned(), "80".to_owned()],
        );
        assert!(yaml.contains("kind: Deployment"), "should be a Deployment: {yaml}");
        assert!(yaml.contains("image: nginx:1.25"), "should have image: {yaml}");
        assert!(yaml.contains("namespace: default"), "should have namespace: {yaml}");
    }

    #[test]
    fn wait_step_builds_kubectl_wait() {
        let spec = kubectl_wait_spec("kind-forge-hub", "deployment/controller", "available", "120s");
        let args: Vec<String> = spec.args.iter().map(|a| a.to_string_lossy().into_owned()).collect();
        let joined = args.join(" ");
        assert!(
            joined.contains("wait deployment/controller"),
            "should wait on resource: {joined}"
        );
        assert!(
            joined.contains("--for=condition=available"),
            "should have condition: {joined}"
        );
        assert!(joined.contains("--timeout=120s"), "should have timeout: {joined}");
    }

    #[test]
    fn compute_metallb_range_produces_valid_range() {
        let range = compute_metallb_range("172.18.0.0/16").unwrap_or_else(|_| std::process::abort());
        assert!(range.contains('-'), "should be a range: {range}");
    }

    #[test]
    fn compute_metallb_range_rejects_small_subnet() {
        assert!(
            compute_metallb_range("10.0.0.0/28").is_err(),
            "/28 subnet should be too small for MetalLB range"
        );
    }

    #[test]
    fn parse_network_cidr_extracts_subnet() {
        let input = r#"[{"IPAM":{"Config":[{"Subnet":"172.18.0.0/16","Gateway":"172.18.0.1"}]}}]"#;
        let result = parse_network_cidr(input).unwrap_or_else(|_| std::process::abort());
        assert_eq!(result, "172.18.0.0/16", "should extract subnet from IPAM.Config");
        assert!(parse_network_cidr("[]").is_err(), "empty array should fail");
    }

    #[test]
    fn parse_network_cidr_rejects_invalid_cidr() {
        let input = r#"[{"IPAM":{"Config":[{"Subnet":"not-a-cidr"}]}}]"#;
        assert!(parse_network_cidr(input).is_err(), "should reject non-CIDR");
    }

    #[test]
    fn parse_network_cidr_rejects_ipv6() {
        let input = r#"[{"IPAM":{"Config":[{"Subnet":"fd00::/64"}]}}]"#;
        assert!(parse_network_cidr(input).is_err(), "should reject IPv6 CIDR");
    }

    #[test]
    fn parse_network_cidr_rejects_prefix_above_32() {
        let input = r#"[{"IPAM":{"Config":[{"Subnet":"10.0.0.0/33"}]}}]"#;
        assert!(parse_network_cidr(input).is_err(), "should reject /33");
    }

    #[test]
    fn compute_cluster_pool_non_overlapping() {
        let cidr = "172.18.0.0/16";
        let r0 = compute_cluster_pool(cidr, 0, 3).unwrap_or_else(|_| std::process::abort());
        let r1 = compute_cluster_pool(cidr, 1, 3).unwrap_or_else(|_| std::process::abort());
        let r2 = compute_cluster_pool(cidr, 2, 3).unwrap_or_else(|_| std::process::abort());
        assert_ne!(r0, r1, "pools 0 and 1 should differ");
        assert_ne!(r1, r2, "pools 1 and 2 should differ");
        assert_ne!(r0, r2, "pools 0 and 2 should differ");
    }

    #[test]
    fn compute_cluster_pool_deterministic() {
        let cidr = "172.18.0.0/16";
        let a = compute_cluster_pool(cidr, 1, 2).unwrap_or_else(|_| std::process::abort());
        let b = compute_cluster_pool(cidr, 1, 2).unwrap_or_else(|_| std::process::abort());
        assert_eq!(a, b, "same inputs should produce same output");
    }

    #[test]
    fn compute_cluster_pool_rejects_insufficient_space() {
        let result = compute_cluster_pool("10.0.0.0/28", 0, 10);
        assert!(result.is_err(), "/28 subnet is too small for 10 clusters");
    }

    #[test]
    fn compute_cluster_pool_rejects_prefix_above_32() {
        let result = compute_cluster_pool("10.0.0.0/33", 0, 1);
        assert!(result.is_err(), "/33 prefix should be rejected");
    }

    #[test]
    fn compute_cluster_pool_rejects_invalid_index() {
        let result = compute_cluster_pool("172.18.0.0/16", 2, 2);
        assert!(result.is_err(), "index >= count should be rejected");
    }

    #[test]
    fn compute_cluster_pool_rejects_zero_count() {
        let result = compute_cluster_pool("172.18.0.0/16", 0, 0);
        assert!(result.is_err(), "zero count should be rejected");
    }

    #[test]
    fn corefile_snippet_has_expected_format() {
        let snippet = generate_corefile_snippet("forge.test", &["10.0.0.1".to_owned(), "10.0.0.2".to_owned()]);
        assert!(snippet.contains("forge.test:53"), "should contain zone: {snippet}");
        assert!(
            snippet.contains("forward . 10.0.0.1 10.0.0.2"),
            "should list upstreams: {snippet}"
        );
    }

    #[test]
    fn connectivity_check_builds_wget_command() {
        let spec = kubectl_connectivity_check("kind-forge-hub", "svc.ns.spoke.forge.test", 8080);
        let args: Vec<String> = spec.args.iter().map(|a| a.to_string_lossy().into_owned()).collect();
        let joined = args.join(" ");
        assert!(joined.contains("forge-cc-"), "should have unique pod name: {joined}");
        assert!(joined.contains("wget"), "should use wget: {joined}");
        assert!(
            joined.contains("http://svc.ns.spoke.forge.test:8080"),
            "should have target url: {joined}"
        );
    }

    #[test]
    fn tcp_check_builds_nc_command() {
        let spec = kubectl_tcp_check("kind-forge-hub", "10.0.0.1", 443);
        let args: Vec<String> = spec.args.iter().map(|a| a.to_string_lossy().into_owned()).collect();
        let joined = args.join(" ");
        assert!(joined.contains("forge-cc-"), "should have unique pod name: {joined}");
        assert!(joined.contains("nc"), "should use nc: {joined}");
        assert!(joined.contains("10.0.0.1"), "should have target: {joined}");
        assert!(joined.contains("443"), "should have port: {joined}");
    }

    #[test]
    fn kubectl_get_jsonpath_builds_correct_command() {
        let spec = kubectl_get_jsonpath(
            "kind-forge-hub",
            "svc/provider-gateway",
            Some("grid-system"),
            "{.status.loadBalancer.ingress[0].ip}",
        );
        assert_eq!(spec.program, "kubectl", "program should be kubectl");
        let args: Vec<String> = spec.args.iter().map(|a| a.to_string_lossy().into_owned()).collect();
        let joined = args.join(" ");
        assert!(
            joined.contains("--context kind-forge-hub"),
            "should have context: {joined}"
        );
        assert!(
            joined.contains("get svc/provider-gateway"),
            "should get resource: {joined}"
        );
        assert!(joined.contains("-n grid-system"), "should have namespace: {joined}");
        assert!(
            joined.contains("jsonpath={.status.loadBalancer.ingress[0].ip}"),
            "should have jsonpath output: {joined}"
        );
    }

    #[test]
    fn kubectl_get_jsonpath_omits_namespace_when_none() {
        let spec = kubectl_get_jsonpath("kind-forge-hub", "svc/web", None, "{.spec.clusterIP}");
        let args: Vec<String> = spec.args.iter().map(|a| a.to_string_lossy().into_owned()).collect();
        let joined = args.join(" ");
        assert!(!joined.contains("-n "), "should not have namespace flag: {joined}");
        assert!(
            joined.contains("jsonpath={.spec.clusterIP}"),
            "should have jsonpath: {joined}"
        );
    }

    #[test]
    fn connectivity_pod_names_are_unique_per_target() {
        let a = conn_check_pod_name("svc.a.forge.test", 80);
        let b = conn_check_pod_name("svc.b.forge.test", 80);
        let c = conn_check_pod_name("svc.a.forge.test", 443);
        assert_ne!(a, b, "different targets should produce different pod names");
        assert_ne!(a, c, "different ports should produce different pod names");
    }
}
