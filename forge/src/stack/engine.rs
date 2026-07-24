//! Stack execution engine.
//!
//! Applies a [`StackSpec`] to a cluster by
//! rendering templates, expanding for-each loops, and executing step
//! commands through [`CommandRunner`].

use std::path::Path;

use sha2::Digest as _;

/// Maximum items expanded by a single for-each step.
const MAX_FOREACH_ITEMS: usize = 256;

use crate::{
    cluster::kind,
    command::runner::CommandRunner,
    config::{ClusterSpec, StackSpec, StepSpec},
    context::ForgeContext,
    error::ForgeError,
    networking, runtime,
    stack::{
        steps,
        template::{self, TemplateContext},
    },
};

// -------------------------------------------------------------
// Result type
// -------------------------------------------------------------

/// Result of applying one stack to a cluster.
pub struct StackResult {
    /// Stack name.
    pub name: String,
    /// Cluster name.
    pub cluster: String,
    /// Number of steps successfully executed.
    pub steps_executed: usize,
    /// Newly computed `MetalLB` pool, if allocated during execution.
    pub pool_allocation: Option<PoolAllocation>,
}

/// A newly computed `MetalLB` pool allocation.
pub struct PoolAllocation {
    /// Network CIDR from which the pool was computed.
    pub cidr: String,
    /// Allocated address range.
    pub range: String,
}

/// Network parameters passed to the engine by the caller.
pub struct NetworkParams<'a> {
    /// Pre-allocated pool range from state, if any.
    pub cluster_pool: Option<&'a str>,
    /// This cluster's index in the config cluster list.
    pub cluster_index: usize,
    /// Total number of clusters.
    pub cluster_count: usize,
    /// DNS zone for cross-cluster service discovery.
    pub dns_zone: &'a str,
}

// -------------------------------------------------------------
// Execution context
// -------------------------------------------------------------

/// Execution context for stack step processing.
pub struct StepContext {
    /// kubectl/helm `--kube-context` value.
    pub kube_context: String,
    /// Directory for resolving relative paths.
    pub config_dir: std::path::PathBuf,
    /// Container runtime binary (for `MetalLB` network inspection).
    pub runtime_binary: String,
    /// Forge-owned environment network, if configured.
    pub network_name: Option<String>,
    /// Pre-allocated `MetalLB` pool range for this cluster.
    pub cluster_pool: Option<String>,
    /// This cluster's index (for pool computation).
    pub cluster_index: usize,
    /// Total cluster count (for pool computation).
    pub cluster_count: usize,
    /// Pool allocation computed during this execution.
    pub pool_allocation: Option<PoolAllocation>,
}

// -------------------------------------------------------------
// Public API
// -------------------------------------------------------------

/// Apply a stack to a cluster.
///
/// Builds a template context from the cluster spec, then executes
/// each step sequentially.  Stops on the first error.
///
/// # Errors
///
/// Returns [`ForgeError`] if any step fails.
pub fn apply_stack(
    ctx: &ForgeContext<'_>,
    cluster: &ClusterSpec,
    stack_name: &str,
    stack: &StackSpec,
    network: Option<&NetworkParams<'_>>,
) -> Result<StackResult, ForgeError> {
    let mut sc = build_step_context(ctx, cluster, network)?;
    precompute_pool_if_needed(ctx.runner, &mut sc)?;
    let tpl = build_template_context(cluster, stack_name, network, sc.cluster_pool.as_deref());
    let count = execute_steps(ctx.runner, &stack.steps, &tpl, &mut sc)?;
    Ok(StackResult {
        name: stack_name.to_owned(),
        cluster: cluster.name.clone(),
        steps_executed: count,
        pool_allocation: sc.pool_allocation,
    })
}

// -------------------------------------------------------------
// Context builders
// -------------------------------------------------------------

/// Build a template context from cluster and stack names.
fn build_template_context(
    cluster: &ClusterSpec,
    stack_name: &str,
    network: Option<&NetworkParams<'_>>,
    pool: Option<&str>,
) -> TemplateContext {
    TemplateContext {
        cluster_name: cluster.name.clone(),
        stack_name: stack_name.to_owned(),
        properties: cluster.properties.clone(),
        item: None,
        network: network.map(|n| template::NetworkTemplateVars {
            dns_zone: n.dns_zone.to_owned(),
            pool: pool.map(ToOwned::to_owned),
        }),
    }
}

/// Build the step execution context.
fn build_step_context(
    ctx: &ForgeContext<'_>,
    cluster: &ClusterSpec,
    network: Option<&NetworkParams<'_>>,
) -> Result<StepContext, ForgeError> {
    let env_name = &ctx.config.metadata.name;
    let kind_name = kind::kind_cluster_name(env_name, &cluster.name);
    let kube_ctx = kind::kubectl_context(&kind_name);
    let resolved = runtime::resolve(ctx.runner, &ctx.config.spec.runtime.provider)?;
    let wants_cross = ctx.config.spec.network.as_ref().is_some_and(|n| n.cross_cluster);
    if wants_cross {
        networking::require_docker_for_cross_cluster(&resolved.binary)?;
    }
    let network_name = wants_cross.then(|| networking::network_name(env_name));
    Ok(StepContext {
        kube_context: kube_ctx,
        config_dir: ctx.config_dir.clone(),
        runtime_binary: resolved.binary,
        network_name,
        cluster_pool: network.and_then(|n| n.cluster_pool.map(ToOwned::to_owned)),
        cluster_index: network.map_or(0, |n| n.cluster_index),
        cluster_count: network.map_or(1, |n| n.cluster_count),
        pool_allocation: None,
    })
}

/// Compute the pool eagerly so `{{ network.pool }}` resolves in any step.
fn precompute_pool_if_needed(runner: &dyn CommandRunner, sc: &mut StepContext) -> Result<(), ForgeError> {
    if sc.cluster_pool.is_some() {
        return Ok(());
    }
    let Some(net_name) = sc.network_name.clone() else {
        return Ok(());
    };
    let range = compute_pool_from_network(
        runner,
        &sc.runtime_binary,
        &net_name,
        sc.cluster_index,
        sc.cluster_count,
    )?;
    sc.cluster_pool = Some(range.1.clone());
    sc.pool_allocation = Some(PoolAllocation {
        cidr: range.0,
        range: range.1,
    });
    Ok(())
}

// -------------------------------------------------------------
// Step execution
// -------------------------------------------------------------

/// Execute a list of steps sequentially, returning total leaf count.
fn execute_steps(
    runner: &dyn CommandRunner,
    steps: &[StepSpec],
    tpl: &TemplateContext,
    sc: &mut StepContext,
) -> Result<usize, ForgeError> {
    let mut count: usize = 0;
    for step in steps {
        let rendered = render_step(step, tpl)?;
        count = count.saturating_add(execute_step(runner, &rendered, tpl, sc)?);
    }
    Ok(count)
}

/// Execute a single rendered step, returning leaf step count.
fn execute_step(
    runner: &dyn CommandRunner,
    step: &StepSpec,
    tpl: &TemplateContext,
    sc: &mut StepContext,
) -> Result<usize, ForgeError> {
    match step {
        StepSpec::Url { url, sha256 } => execute_url(runner, url, sha256, sc).map(|()| 1),
        StepSpec::Manifest { path } => execute_manifest(runner, path, sc).map(|()| 1),
        StepSpec::Kustomize { path } => execute_kustomize(runner, path, sc).map(|()| 1),
        StepSpec::Helm { .. } => execute_helm(runner, step, sc).map(|()| 1),
        StepSpec::Deployment { .. } => execute_deployment(runner, step, sc).map(|()| 1),
        StepSpec::Service { name, port, namespace } => {
            execute_service(runner, name, *port, namespace.as_deref(), sc).map(|()| 1)
        },
        StepSpec::Wait {
            resource,
            condition,
            timeout,
        } => execute_wait(runner, resource, condition, timeout, sc).map(|()| 1),
        StepSpec::Exec { command } => execute_exec(runner, command).map(|()| 1),
        StepSpec::ForEach { property, steps: sub } => execute_foreach(runner, property, sub, tpl, sc),
        StepSpec::MetallbAutoPool { name } => execute_metallb(runner, name, sc).map(|()| 1),
        StepSpec::CoreDnsForward { .. } => execute_coredns_forward(runner, step, sc).map(|()| 1),
    }
}

// -------------------------------------------------------------
// Per-step handlers
// -------------------------------------------------------------

/// Download a URL, verify SHA-256, and apply via kubectl.
fn execute_url(runner: &dyn CommandRunner, url: &str, sha256: &str, sc: &StepContext) -> Result<(), ForgeError> {
    let spec = steps::curl_download_spec(url);
    let output = runner.run(&spec)?;
    steps::check_success(&output, "curl")?;
    check_remote_manifest_size(output.stdout.len())?;
    verify_sha256(output.stdout.as_bytes(), sha256)?;
    let apply = steps::kubectl_stdin_apply(&sc.kube_context, output.stdout.as_bytes());
    let apply_out = runner.run(&apply)?;
    steps::check_success(&apply_out, "kubectl apply")
}

/// Apply a local manifest file.
fn execute_manifest(runner: &dyn CommandRunner, path: &str, sc: &StepContext) -> Result<(), ForgeError> {
    let resolved = resolve_path(&sc.config_dir, path)?;
    let spec = steps::kubectl_apply(&sc.kube_context, &resolved);
    let output = runner.run(&spec)?;
    steps::check_success(&output, "kubectl apply")
}

/// Apply a kustomize directory.
fn execute_kustomize(runner: &dyn CommandRunner, path: &str, sc: &StepContext) -> Result<(), ForgeError> {
    let resolved = resolve_path(&sc.config_dir, path)?;
    let spec = steps::kubectl_kustomize(&sc.kube_context, &resolved);
    let output = runner.run(&spec)?;
    steps::check_success(&output, "kubectl apply")
}

/// Install or upgrade a Helm release.
fn execute_helm(runner: &dyn CommandRunner, step: &StepSpec, sc: &StepContext) -> Result<(), ForgeError> {
    let StepSpec::Helm {
        release,
        chart,
        version,
        namespace,
        values,
    } = step
    else {
        return Err(ForgeError::Config("expected Helm step".to_owned()));
    };
    let params = steps::HelmParams {
        context: &sc.kube_context,
        release,
        chart,
        version,
        namespace: namespace.as_deref(),
    };
    let spec = steps::helm_upgrade_spec(&params, values)?;
    let output = runner.run(&spec)?;
    steps::check_success(&output, "helm upgrade")
}

/// Generate and apply a Deployment manifest.
fn execute_deployment(runner: &dyn CommandRunner, step: &StepSpec, sc: &StepContext) -> Result<(), ForgeError> {
    let StepSpec::Deployment {
        name,
        image,
        namespace,
        args,
    } = step
    else {
        return Err(ForgeError::Config("expected Deployment step".to_owned()));
    };
    let yaml = steps::generate_deployment_yaml(name, image, namespace.as_deref(), args);
    let spec = steps::kubectl_stdin_apply(&sc.kube_context, yaml.as_bytes());
    let output = runner.run(&spec)?;
    steps::check_success(&output, "kubectl apply")
}

/// Generate and apply a Service manifest.
fn execute_service(
    runner: &dyn CommandRunner,
    name: &str,
    port: u16,
    namespace: Option<&str>,
    sc: &StepContext,
) -> Result<(), ForgeError> {
    let yaml = steps::generate_service_yaml(name, port, namespace);
    let spec = steps::kubectl_stdin_apply(&sc.kube_context, yaml.as_bytes());
    let output = runner.run(&spec)?;
    steps::check_success(&output, "kubectl apply")
}

/// Wait for a Kubernetes resource condition.
fn execute_wait(
    runner: &dyn CommandRunner,
    resource: &str,
    condition: &str,
    timeout: &str,
    sc: &StepContext,
) -> Result<(), ForgeError> {
    let spec = steps::kubectl_wait_spec(&sc.kube_context, resource, condition, timeout);
    let output = runner.run(&spec)?;
    steps::check_success(&output, "kubectl wait")
}

/// Execute an arbitrary command.
fn execute_exec(runner: &dyn CommandRunner, command: &[String]) -> Result<(), ForgeError> {
    let spec = steps::exec_spec(command)?;
    let output = runner.run(&spec)?;
    steps::check_success(&output, "exec")
}

/// Expand a for-each loop over a cluster property array.
fn execute_foreach(
    runner: &dyn CommandRunner,
    property: &str,
    sub_steps: &[StepSpec],
    tpl: &TemplateContext,
    sc: &mut StepContext,
) -> Result<usize, ForgeError> {
    let arr = lookup_property_array(property, tpl)?;
    if arr.len() > MAX_FOREACH_ITEMS {
        return Err(ForgeError::Config(format!(
            "for-each property '{property}' has {} items; maximum is {MAX_FOREACH_ITEMS}",
            arr.len()
        )));
    }
    let mut total: usize = 0;
    for element in &arr {
        let mut child_tpl = tpl.clone();
        child_tpl.item = Some(element.clone());
        total = total.saturating_add(execute_steps(runner, sub_steps, &child_tpl, sc)?);
    }
    Ok(total)
}

/// Look up a cluster property and require it to be an array.
fn lookup_property_array(property: &str, tpl: &TemplateContext) -> Result<Vec<serde_json::Value>, ForgeError> {
    let val = tpl
        .properties
        .get(property)
        .ok_or_else(|| ForgeError::Config(format!("for-each property '{property}' not found")))?;
    match val {
        serde_json::Value::Array(arr) => Ok(arr.clone()),
        _ => Err(ForgeError::Config(format!(
            "for-each property '{property}' must be an array"
        ))),
    }
}

/// Auto-detect Docker network CIDR and apply `MetalLB` pool.
fn execute_metallb(runner: &dyn CommandRunner, name: &str, sc: &mut StepContext) -> Result<(), ForgeError> {
    let network_name = sc
        .network_name
        .as_deref()
        .ok_or_else(|| ForgeError::Config("metallb-auto-pool requires spec.network.crossCluster: true".to_owned()))?
        .to_owned();
    if let Some(pool) = &sc.cluster_pool {
        return apply_metallb_yaml(runner, name, pool, sc);
    }
    let range = compute_pool_from_network(
        runner,
        &sc.runtime_binary,
        &network_name,
        sc.cluster_index,
        sc.cluster_count,
    )?;
    sc.pool_allocation = Some(PoolAllocation {
        cidr: range.0,
        range: range.1.clone(),
    });
    apply_metallb_yaml(runner, name, &range.1, sc)
}

/// Inspect the Docker network and compute a per-cluster pool range.
fn compute_pool_from_network(
    runner: &dyn CommandRunner,
    binary: &str,
    network_name: &str,
    index: usize,
    count: usize,
) -> Result<(String, String), ForgeError> {
    let inspect = steps::docker_network_inspect(binary, network_name);
    let output = runner.run(&inspect)?;
    steps::check_success(&output, "network inspect")?;
    let cidr = steps::parse_network_cidr(&output.stdout)?;
    let range = steps::compute_cluster_pool(&cidr, index, count)?;
    Ok((cidr, range))
}

/// Generate and apply `MetalLB` pool YAML.
fn apply_metallb_yaml(runner: &dyn CommandRunner, name: &str, range: &str, sc: &StepContext) -> Result<(), ForgeError> {
    let yaml = steps::generate_metallb_pool_yaml(name, range);
    let spec = steps::kubectl_stdin_apply(&sc.kube_context, yaml.as_bytes());
    let output = runner.run(&spec)?;
    steps::check_success(&output, "kubectl apply")
}

/// Patch `CoreDNS` to forward a zone to upstream resolvers.
fn execute_coredns_forward(runner: &dyn CommandRunner, step: &StepSpec, sc: &StepContext) -> Result<(), ForgeError> {
    let StepSpec::CoreDnsForward { zone, upstreams } = step else {
        return Err(ForgeError::Config("expected CoreDnsForward step".to_owned()));
    };
    let current = read_corefile(runner, &sc.kube_context)?;
    if zone_present(&current, zone) {
        return Ok(());
    }
    let snippet = steps::generate_corefile_snippet(zone, upstreams);
    let new_corefile = format!("{current}\n{snippet}\n");
    apply_coredns_configmap(runner, &sc.kube_context, &new_corefile)?;
    restart_coredns(runner, &sc.kube_context)
}

/// Read the current `CoreDNS` Corefile from the cluster.
fn read_corefile(runner: &dyn CommandRunner, context: &str) -> Result<String, ForgeError> {
    let cmd = steps::kubectl_get_corefile(context);
    let output = runner.run(&cmd)?;
    steps::check_success(&output, "coredns read")?;
    Ok(output.stdout.clone())
}

/// Check whether a Corefile already contains a server block for the zone.
///
/// Matches lines where the first whitespace-delimited token is exactly
/// `{zone}:53`, which is the `CoreDNS` server-block opener syntax.
fn zone_present(corefile: &str, zone: &str) -> bool {
    let target = format!("{zone}:53");
    corefile.lines().any(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return false;
        }
        let first_token = trimmed.split_whitespace().next().unwrap_or("");
        first_token == target
    })
}

/// Apply an updated `CoreDNS` `ConfigMap`.
fn apply_coredns_configmap(runner: &dyn CommandRunner, context: &str, corefile: &str) -> Result<(), ForgeError> {
    let yaml = steps::generate_coredns_configmap(corefile);
    let cmd = steps::kubectl_stdin_apply(context, yaml.as_bytes());
    let output = runner.run(&cmd)?;
    steps::check_success(&output, "coredns apply")
}

/// Rolling-restart `CoreDNS` to pick up config changes.
fn restart_coredns(runner: &dyn CommandRunner, context: &str) -> Result<(), ForgeError> {
    let cmd = steps::kubectl_rollout_restart(context, "deployment/coredns", "kube-system");
    let output = runner.run(&cmd)?;
    steps::check_success(&output, "coredns restart")
}

// -------------------------------------------------------------
// Template rendering
// -------------------------------------------------------------

/// Render template expressions in a step's string fields.
fn render_step(step: &StepSpec, tpl: &TemplateContext) -> Result<StepSpec, ForgeError> {
    match step {
        StepSpec::Url { url, sha256 } => Ok(StepSpec::Url {
            url: template::render(url, tpl)?,
            sha256: sha256.clone(),
        }),
        StepSpec::Manifest { path } => Ok(StepSpec::Manifest {
            path: template::render(path, tpl)?,
        }),
        StepSpec::Kustomize { path } => Ok(StepSpec::Kustomize {
            path: template::render(path, tpl)?,
        }),
        StepSpec::Helm { .. } => render_helm_step(step, tpl),
        StepSpec::Deployment { .. } => render_deployment_step(step, tpl),
        StepSpec::Service { name, port, namespace } => render_service_step(name, *port, namespace, tpl),
        StepSpec::Wait { .. } => render_wait_step(step, tpl),
        StepSpec::Exec { command } => Ok(StepSpec::Exec {
            command: render_vec(command, tpl)?,
        }),
        StepSpec::ForEach { property, steps: sub } => Ok(StepSpec::ForEach {
            property: template::render(property, tpl)?,
            steps: sub.clone(),
        }),
        StepSpec::MetallbAutoPool { name } => Ok(StepSpec::MetallbAutoPool {
            name: template::render(name, tpl)?,
        }),
        StepSpec::CoreDnsForward { .. } => render_coredns_forward_step(step, tpl),
    }
}

/// Render templates in a Helm step.
fn render_helm_step(step: &StepSpec, tpl: &TemplateContext) -> Result<StepSpec, ForgeError> {
    let StepSpec::Helm {
        release,
        chart,
        version,
        namespace,
        values,
    } = step
    else {
        return Err(ForgeError::Config("expected Helm step".to_owned()));
    };
    Ok(StepSpec::Helm {
        release: template::render(release, tpl)?,
        chart: template::render(chart, tpl)?,
        version: template::render(version, tpl)?,
        namespace: render_optional(namespace, tpl)?,
        values: render_values(values, tpl)?,
    })
}

/// Render template expressions in Helm values recursively.
fn render_values(
    values: &std::collections::BTreeMap<String, serde_json::Value>,
    tpl: &TemplateContext,
) -> Result<std::collections::BTreeMap<String, serde_json::Value>, ForgeError> {
    values
        .iter()
        .map(|(key, value)| Ok((key.clone(), render_json_value(value, tpl)?)))
        .collect()
}

/// Render a JSON value, preserving non-string types.
fn render_json_value(value: &serde_json::Value, tpl: &TemplateContext) -> Result<serde_json::Value, ForgeError> {
    match value {
        serde_json::Value::String(s) => Ok(serde_json::Value::String(template::render(s, tpl)?)),
        serde_json::Value::Array(items) => items
            .iter()
            .map(|item| render_json_value(item, tpl))
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(key, val)| Ok((key.clone(), render_json_value(val, tpl)?)))
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map(serde_json::Value::Object),
        _ => Ok(value.clone()),
    }
}

/// Render templates in a Deployment step.
fn render_deployment_step(step: &StepSpec, tpl: &TemplateContext) -> Result<StepSpec, ForgeError> {
    let StepSpec::Deployment {
        name,
        image,
        namespace,
        args,
    } = step
    else {
        return Err(ForgeError::Config("expected Deployment step".to_owned()));
    };
    Ok(StepSpec::Deployment {
        name: template::render(name, tpl)?,
        image: template::render(image, tpl)?,
        namespace: render_optional(namespace, tpl)?,
        args: render_vec(args, tpl)?,
    })
}

/// Render templates in a Service step.
fn render_service_step(
    name: &str,
    port: u16,
    namespace: &Option<String>,
    tpl: &TemplateContext,
) -> Result<StepSpec, ForgeError> {
    Ok(StepSpec::Service {
        name: template::render(name, tpl)?,
        port,
        namespace: render_optional(namespace, tpl)?,
    })
}

/// Render templates in a Wait step.
fn render_wait_step(step: &StepSpec, tpl: &TemplateContext) -> Result<StepSpec, ForgeError> {
    let StepSpec::Wait {
        resource,
        condition,
        timeout,
    } = step
    else {
        return Err(ForgeError::Config("expected Wait step".to_owned()));
    };
    Ok(StepSpec::Wait {
        resource: template::render(resource, tpl)?,
        condition: template::render(condition, tpl)?,
        timeout: template::render(timeout, tpl)?,
    })
}

/// Render templates in a `CoreDNS` forward step.
fn render_coredns_forward_step(step: &StepSpec, tpl: &TemplateContext) -> Result<StepSpec, ForgeError> {
    let StepSpec::CoreDnsForward { zone, upstreams } = step else {
        return Err(ForgeError::Config("expected CoreDnsForward step".to_owned()));
    };
    Ok(StepSpec::CoreDnsForward {
        zone: template::render(zone, tpl)?,
        upstreams: render_vec(upstreams, tpl)?,
    })
}

/// Render an optional string through the template engine.
fn render_optional(opt: &Option<String>, tpl: &TemplateContext) -> Result<Option<String>, ForgeError> {
    opt.as_ref().map(|s| template::render(s, tpl)).transpose()
}

/// Render a vec of strings through the template engine.
fn render_vec(items: &[String], tpl: &TemplateContext) -> Result<Vec<String>, ForgeError> {
    items.iter().map(|s| template::render(s, tpl)).collect()
}

// -------------------------------------------------------------
// SHA-256 verification
// -------------------------------------------------------------

/// Verify content matches an expected SHA-256 hex digest.
///
/// # Errors
///
/// Returns [`ForgeError::Command`] if the digest does not match.
pub fn verify_sha256(content: &[u8], expected: &str) -> Result<(), ForgeError> {
    let digest = sha2::Sha256::digest(content);
    let actual = format!("{digest:x}");
    if actual == expected {
        return Ok(());
    }
    Err(ForgeError::Command {
        program: "sha256".to_owned(),
        message: format!("SHA-256 mismatch: expected {expected}, got {actual}"),
    })
}

/// Reject oversized remote manifests even if curl did not.
fn check_remote_manifest_size(len: usize) -> Result<(), ForgeError> {
    if len <= steps::MAX_REMOTE_MANIFEST_BYTES {
        return Ok(());
    }
    Err(ForgeError::Command {
        program: "curl".to_owned(),
        message: format!("remote manifest exceeded {} bytes", steps::MAX_REMOTE_MANIFEST_BYTES),
    })
}

// -------------------------------------------------------------
// Path resolution
// -------------------------------------------------------------

/// Resolve a relative path against the config directory after template rendering.
fn resolve_path(config_dir: &Path, path: &str) -> Result<String, ForgeError> {
    if path.trim().is_empty() || Path::new(path).is_absolute() || path.split('/').any(|part| part == "..") {
        return Err(ForgeError::Config(format!(
            "stack path '{path}' must be relative and must not escape the config root"
        )));
    }
    Ok(config_dir.join(path).to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::command::runner::{CommandOutput, MockRunner};

    fn ok_output() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    fn make_step_context() -> StepContext {
        StepContext {
            kube_context: "kind-test-hub".to_owned(),
            config_dir: std::path::PathBuf::from("/tmp"),
            runtime_binary: "docker".to_owned(),
            network_name: Some("test-net".to_owned()),
            cluster_pool: None,
            cluster_index: 0,
            cluster_count: 2,
            pool_allocation: None,
        }
    }

    fn make_template_context() -> TemplateContext {
        TemplateContext {
            cluster_name: "hub".to_owned(),
            stack_name: "base".to_owned(),
            properties: BTreeMap::new(),
            item: None,
            network: None,
        }
    }

    #[test]
    fn apply_runs_steps_in_order() {
        let mut runner = MockRunner::new();
        runner.respond("kubectl", ok_output());
        let mut sc = make_step_context();
        let tpl = make_template_context();
        let steps = vec![
            StepSpec::Manifest {
                path: "a.yaml".to_owned(),
            },
            StepSpec::Manifest {
                path: "b.yaml".to_owned(),
            },
        ];
        let count = execute_steps(&runner, &steps, &tpl, &mut sc).unwrap_or_else(|_| std::process::abort());
        assert_eq!(count, 2, "should execute both steps");
        assert_eq!(runner.call_count(), 2, "should record 2 calls");
    }

    #[test]
    fn apply_stops_on_first_error() {
        let mut runner = MockRunner::new();
        runner.respond("kubectl --context kind-test-hub apply -f /tmp/a.yaml", ok_output());
        runner.respond(
            "kubectl --context kind-test-hub apply -f /tmp/b.yaml",
            CommandOutput {
                status: 1,
                stdout: String::new(),
                stderr: "not found".to_owned(),
            },
        );
        let mut sc = make_step_context();
        let tpl = make_template_context();
        let steps = vec![
            StepSpec::Manifest {
                path: "a.yaml".to_owned(),
            },
            StepSpec::Manifest {
                path: "b.yaml".to_owned(),
            },
            StepSpec::Manifest {
                path: "c.yaml".to_owned(),
            },
        ];
        let result = execute_steps(&runner, &steps, &tpl, &mut sc);
        assert!(result.is_err(), "should fail on second step");
        assert_eq!(runner.call_count(), 2, "should only run 2 steps");
    }

    #[test]
    fn foreach_expands_over_property_array() {
        let mut runner = MockRunner::new();
        runner.respond("kubectl", ok_output());
        let mut sc = make_step_context();
        let mut tpl = make_template_context();
        tpl.properties
            .insert("workers".to_owned(), serde_json::json!(["w1", "w2"]));
        let steps = vec![StepSpec::ForEach {
            property: "workers".to_owned(),
            steps: vec![StepSpec::Manifest {
                path: "{{ item }}.yaml".to_owned(),
            }],
        }];
        let count = execute_steps(&runner, &steps, &tpl, &mut sc).unwrap_or_else(|_| std::process::abort());
        assert_eq!(count, 2, "should execute 2 iterations");
        let calls = runner.calls();
        let call_strs: Vec<String> = calls.iter().map(ToString::to_string).collect();
        assert!(
            call_strs.iter().any(|s| s.contains("w1.yaml")),
            "should apply w1.yaml: {call_strs:?}"
        );
        assert!(
            call_strs.iter().any(|s| s.contains("w2.yaml")),
            "should apply w2.yaml: {call_strs:?}"
        );
    }

    #[test]
    fn foreach_rejects_too_many_items() {
        let runner = MockRunner::new();
        let mut sc = make_step_context();
        let mut tpl = make_template_context();
        let items: Vec<serde_json::Value> = (0..=MAX_FOREACH_ITEMS)
            .map(|i| serde_json::Value::String(format!("item-{i}")))
            .collect();
        tpl.properties
            .insert("workers".to_owned(), serde_json::Value::Array(items));
        let steps = vec![StepSpec::ForEach {
            property: "workers".to_owned(),
            steps: vec![StepSpec::Manifest {
                path: "{{ item }}.yaml".to_owned(),
            }],
        }];
        let result = execute_steps(&runner, &steps, &tpl, &mut sc);
        assert!(result.is_err(), "oversized for-each should fail");
        assert_eq!(runner.call_count(), 0, "must fail before kubectl");
    }

    #[test]
    fn verify_sha256_rejects_mismatch() {
        let bad = "0".repeat(64);
        assert!(verify_sha256(b"hello", &bad).is_err(), "should reject bad digest");
        let good = format!("{:x}", sha2::Sha256::digest(b"hello"));
        assert!(verify_sha256(b"hello", &good).is_ok(), "should accept correct digest");
    }

    #[test]
    fn rendered_path_escape_is_rejected() {
        let mut sc = make_step_context();
        let tpl = TemplateContext {
            cluster_name: "hub".to_owned(),
            stack_name: "base".to_owned(),
            properties: BTreeMap::from([("path".to_owned(), serde_json::json!("../escape.yaml"))]),
            item: None,
            network: None,
        };
        let steps = vec![StepSpec::Manifest {
            path: "{{ cluster.properties.path }}".to_owned(),
        }];
        let runner = MockRunner::new();
        let result = execute_steps(&runner, &steps, &tpl, &mut sc);
        assert!(result.is_err(), "rendered path escape must fail");
        assert_eq!(runner.call_count(), 0, "must fail before kubectl");
    }

    #[test]
    fn oversized_remote_manifest_is_rejected() {
        let too_large = steps::MAX_REMOTE_MANIFEST_BYTES.saturating_add(1);
        let result = check_remote_manifest_size(too_large);
        assert!(result.is_err(), "oversized remote manifest should fail");
    }

    #[test]
    fn metallb_requires_forge_network() {
        let mut runner = MockRunner::new();
        runner.respond("docker", ok_output());
        let mut sc = make_step_context();
        sc.network_name = None;
        let result = execute_metallb(&runner, "pool", &mut sc);
        assert!(result.is_err(), "metallb-auto-pool should require Forge network");
        assert_eq!(runner.call_count(), 0, "must fail before runtime network inspect");
    }

    #[test]
    fn render_step_templates_strings() {
        let tpl = TemplateContext {
            cluster_name: "hub".to_owned(),
            stack_name: "base".to_owned(),
            properties: BTreeMap::new(),
            item: None,
            network: None,
        };
        let step = StepSpec::Manifest {
            path: "{{ cluster.name }}/manifests".to_owned(),
        };
        let rendered = render_step(&step, &tpl).unwrap_or_else(|_| std::process::abort());
        match &rendered {
            StepSpec::Manifest { path } => {
                assert_eq!(path, "hub/manifests", "template should be resolved");
            },
            _ => std::process::abort(),
        }
    }

    #[test]
    fn render_helm_values_templates_recursively() {
        let mut tpl = make_template_context();
        tpl.properties
            .insert("image".to_owned(), serde_json::json!("example/web:v1"));
        let step = helm_step_with_template_values();
        let rendered = render_step(&step, &tpl).unwrap_or_else(|_| std::process::abort());
        let StepSpec::Helm { values, .. } = rendered else {
            std::process::abort();
        };
        assert_eq!(
            values.get("image").and_then(|v| v.get("repository")),
            Some(&serde_json::Value::String("example/web:v1".to_owned()))
        );
        assert_eq!(
            values.get("image").and_then(|v| v.get("replicas")),
            Some(&serde_json::json!(2))
        );
    }

    /// Build a Helm step with templated values for testing.
    fn helm_step_with_template_values() -> StepSpec {
        StepSpec::Helm {
            release: "web".to_owned(),
            chart: "example/web".to_owned(),
            version: "1.0.0".to_owned(),
            namespace: None,
            values: BTreeMap::from([(
                "image".to_owned(),
                serde_json::json!({
                    "repository": "{{ cluster.properties.image }}",
                    "replicas": 2
                }),
            )]),
        }
    }

    #[test]
    fn metallb_uses_cluster_pool_computation() {
        let mut runner = MockRunner::new();
        runner.respond(
            "docker",
            CommandOutput {
                status: 0,
                stdout: r#"[{"IPAM":{"Config":[{"Subnet":"172.18.0.0/16","Gateway":"172.18.0.1"}]}}]"#.to_owned(),
                stderr: String::new(),
            },
        );
        runner.respond("kubectl", ok_output());
        let mut sc = make_step_context();
        execute_metallb(&runner, "pool", &mut sc).unwrap_or_else(|_| std::process::abort());
        assert!(sc.pool_allocation.is_some(), "should record pool allocation");
        assert!(runner.was_called("network inspect"), "should inspect network");
        let calls = runner.calls();
        let apply = calls
            .iter()
            .find(|c| c.to_string().contains("apply"))
            .unwrap_or_else(|| std::process::abort());
        assert!(apply.stdin.is_some(), "kubectl apply should have MetalLB YAML on stdin");
    }

    #[test]
    fn metallb_reuses_existing_pool_from_context() {
        let mut runner = MockRunner::new();
        runner.respond("kubectl", ok_output());
        let mut sc = make_step_context();
        sc.cluster_pool = Some("172.18.255.231-172.18.255.250".to_owned());
        execute_metallb(&runner, "pool", &mut sc).unwrap_or_else(|_| std::process::abort());
        assert!(sc.pool_allocation.is_none(), "should not compute new allocation");
        assert!(!runner.was_called("network inspect"), "should skip network inspect");
        let calls = runner.calls();
        let apply = calls
            .iter()
            .find(|c| c.to_string().contains("apply"))
            .unwrap_or_else(|| std::process::abort());
        let stdin_bytes = apply.stdin.as_deref().unwrap_or_else(|| std::process::abort());
        let stdin_text = std::str::from_utf8(stdin_bytes).unwrap_or_else(|_| std::process::abort());
        assert!(
            stdin_text.contains("172.18.255.231-172.18.255.250"),
            "YAML should use pre-allocated range"
        );
    }

    #[test]
    fn coredns_forward_patches_and_restarts() {
        let mut runner = MockRunner::new();
        runner.respond("kubectl", ok_output());
        let step = StepSpec::CoreDnsForward {
            zone: "forge.test".to_owned(),
            upstreams: vec!["10.0.0.1".to_owned()],
        };
        let sc = make_step_context();
        execute_coredns_forward(&runner, &step, &sc).unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("rollout restart"), "should restart coredns");
        let calls = runner.calls();
        let get_call = calls.first().unwrap_or_else(|| std::process::abort());
        let get_str = get_call.to_string();
        assert!(
            get_str.contains("get") && get_str.contains("configmap"),
            "first call should get configmap"
        );
        let apply = calls.iter().find(|c| c.to_string().contains("apply"));
        assert!(apply.is_some(), "should apply updated configmap");
        let restart = calls.iter().find(|c| c.to_string().contains("rollout"));
        assert!(restart.is_some(), "should restart coredns deployment");
    }

    #[test]
    fn coredns_forward_skips_existing_zone() {
        let mut runner = MockRunner::new();
        runner.respond(
            "kubectl",
            CommandOutput {
                status: 0,
                stdout: "forge.test:53 {\n    forward . 10.0.0.1\n}".to_owned(),
                stderr: String::new(),
            },
        );
        let step = StepSpec::CoreDnsForward {
            zone: "forge.test".to_owned(),
            upstreams: vec!["10.0.0.1".to_owned()],
        };
        let sc = make_step_context();
        execute_coredns_forward(&runner, &step, &sc).unwrap_or_else(|_| std::process::abort());
        assert_eq!(runner.call_count(), 1, "should only read corefile, not apply/restart");
    }

    #[test]
    fn zone_present_matches_exact_first_token() {
        assert!(zone_present("forge.test:53 {\n    forward . 10.0.0.1\n}", "forge.test"));
    }

    #[test]
    fn zone_present_ignores_commented_line() {
        assert!(!zone_present(
            "# forge.test:53 {\n#    forward . 10.0.0.1\n}",
            "forge.test"
        ));
    }

    #[test]
    fn zone_present_rejects_superset_zone() {
        assert!(
            !zone_present("other.forge.test:53 {\n    forward . 10.0.0.1\n}", "forge.test"),
            "other.forge.test:53 should not match forge.test:53"
        );
    }

    #[test]
    fn zone_present_matches_with_leading_whitespace() {
        assert!(zone_present("   forge.test:53 {", "forge.test"));
    }

    #[test]
    fn zone_present_rejects_inline_mention() {
        assert!(
            !zone_present("other.forge.test:53 { # forge.test:53", "forge.test"),
            "forge.test:53 as a non-first-token should not match"
        );
    }
}
