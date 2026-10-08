//! Stack execution engine.
//!
//! Applies a [`StackSpec`] to a cluster by
//! rendering templates, expanding for-each loops, and executing step
//! commands through [`CommandRunner`].

use std::path::Path;

use sha2::Digest as _;

/// Maximum items expanded by a single for-each step.
const MAX_FOREACH_ITEMS: usize = 256;

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

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
    /// Values captured during execution.
    pub captures: BTreeMap<String, String>,
}

/// A newly computed `MetalLB` pool allocation.
pub struct PoolAllocation {
    /// Network CIDR from which the pool was computed.
    pub cidr: String,
    /// Allocated address range.
    pub range: String,
}

/// Network parameters passed to the engine by the caller.
pub struct NetworkParams<'ctx> {
    /// Pre-allocated pool range from state, if any.
    pub cluster_pool: Option<&'ctx str>,
    /// This cluster's index in the config cluster list.
    pub cluster_index: usize,
    /// Total number of clusters.
    pub cluster_count: usize,
    /// DNS zone for cross-cluster service discovery.
    pub dns_zone: &'ctx str,
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
    /// Forge state directory for runtime output paths.
    pub state_dir: std::path::PathBuf,
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
    /// Values captured during this execution.
    pub pending_captures: BTreeMap<String, String>,
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
#[expect(
    clippy::too_many_arguments,
    reason = "captures needed for cross-stack template rendering"
)]
pub fn apply_stack(
    ctx: &ForgeContext<'_>,
    cluster: &ClusterSpec,
    stack_name: &str,
    stack: &StackSpec,
    network: Option<&NetworkParams<'_>>,
    captures: &BTreeMap<String, BTreeMap<String, String>>,
) -> Result<StackResult, ForgeError> {
    let mut sc = build_step_context(ctx, cluster, network)?;
    precompute_pool_if_needed(ctx.runner, &mut sc)?;
    let tpl = build_template_context(cluster, stack_name, network, sc.cluster_pool.as_deref(), captures);
    let count = execute_steps(ctx.runner, &stack.steps, &tpl, &mut sc)?;
    Ok(StackResult {
        name: stack_name.to_owned(),
        cluster: cluster.name.clone(),
        steps_executed: count,
        pool_allocation: sc.pool_allocation,
        captures: sc.pending_captures,
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
    captures: &BTreeMap<String, BTreeMap<String, String>>,
) -> TemplateContext {
    TemplateContext {
        cluster_name: cluster.name.clone(),
        stack_name: stack_name.to_owned(),
        properties: cluster.properties.clone(),
        item: None,
        network: network.map(|net| template::NetworkTemplateVars {
            dns_zone: net.dns_zone.to_owned(),
            pool: pool.map(ToOwned::to_owned),
        }),
        captures: captures.clone(),
    }
}

/// Build the step execution context.
fn build_step_context(
    ctx: &ForgeContext<'_>,
    cluster: &ClusterSpec,
    network: Option<&NetworkParams<'_>>,
) -> Result<StepContext, ForgeError> {
    let env_name = &ctx.config.metadata.name;
    let kind_name = kind::kind_cluster_name(&ctx.config.spec.runtime.cluster_prefix, &cluster.name);
    let kube_ctx = kind::kubectl_context(&kind_name);
    let resolved = runtime::resolve(ctx.runner, &ctx.config.spec.runtime.provider)?;
    let wants_cross = ctx.config.spec.network.as_ref().is_some_and(|net| net.cross_cluster);
    if wants_cross {
        networking::require_docker_for_cross_cluster(&resolved.binary)?;
    }
    let network_name = wants_cross.then(|| networking::network_name(env_name));
    Ok(StepContext {
        kube_context: kube_ctx,
        config_dir: ctx.config_dir.clone(),
        state_dir: ctx.state_dir.clone(),
        runtime_binary: resolved.binary,
        network_name,
        cluster_pool: network.and_then(|net| net.cluster_pool.map(ToOwned::to_owned)),
        cluster_index: network.map_or(0, |net| net.cluster_index),
        cluster_count: network.map_or(1, |net| net.cluster_count),
        pool_allocation: None,
        pending_captures: BTreeMap::new(),
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
            namespace,
        } => {
            let spec = steps::kubectl_wait_spec(&sc.kube_context, resource, condition, timeout, namespace.as_deref());
            let output = runner.run(&spec)?;
            steps::check_success(&output, "kubectl wait").map(|()| 1)
        },
        StepSpec::Exec { command, env } => execute_exec(runner, command, env, &sc.config_dir).map(|()| 1),
        StepSpec::ForEach { property, steps: sub } => execute_foreach(runner, property, sub, tpl, sc),
        StepSpec::MetallbAutoPool { name } => execute_metallb(runner, name, sc).map(|()| 1),
        StepSpec::CoreDnsForward { .. } => execute_coredns_forward(runner, step, sc).map(|()| 1),
        StepSpec::Capture { .. } => execute_capture(runner, step, sc).map(|()| 1),
        StepSpec::TemplateManifest { path } => execute_template_manifest(runner, path, tpl, sc).map(|()| 1),
        StepSpec::TemplateFile { source, target } => execute_template_file(source, target, tpl, sc).map(|()| 1),
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

/// Execute an arbitrary command.
fn execute_exec(
    runner: &dyn CommandRunner,
    command: &[String],
    env: &BTreeMap<String, String>,
    config_dir: &Path,
) -> Result<(), ForgeError> {
    let resolved = resolve_exec_args(command, config_dir)?;
    let spec = steps::exec_spec(&resolved, env)?;
    let output = runner.run(&spec)?;
    steps::check_success(&output, "exec")
}

/// Resolve relative path arguments in an exec step against the config directory.
///
/// Bare program names stay on `PATH`. Relative paths that exist under
/// `config_dir` become absolute so stacks work regardless of process cwd.
/// Path escape (`..`) is rejected.
fn resolve_exec_args(command: &[String], config_dir: &Path) -> Result<Vec<String>, ForgeError> {
    command
        .iter()
        .enumerate()
        .map(|(idx, arg)| resolve_exec_arg(idx, arg, config_dir))
        .collect()
}

/// Resolve one exec argument, optionally absolutizing relative paths.
fn resolve_exec_arg(idx: usize, arg: &str, config_dir: &Path) -> Result<String, ForgeError> {
    if idx == 0 && !arg.contains('/') {
        return Ok(arg.to_owned());
    }
    if Path::new(arg).is_absolute() || arg.starts_with('-') {
        return Ok(arg.to_owned());
    }
    if arg.split('/').any(|part| part == "..") {
        return Err(ForgeError::Config(format!(
            "exec path '{arg}' must not escape the config root"
        )));
    }
    let candidate = config_dir.join(arg);
    if candidate.exists() {
        Ok(candidate.to_string_lossy().into_owned())
    } else {
        Ok(arg.to_owned())
    }
}

/// Capture a kubectl jsonpath result into pending state.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "Instant + Duration cannot panic for reasonable timeouts"
)]
fn execute_capture(runner: &dyn CommandRunner, step: &StepSpec, sc: &mut StepContext) -> Result<(), ForgeError> {
    let StepSpec::Capture {
        resource,
        namespace,
        jsonpath,
        key,
        timeout,
        interval,
    } = step
    else {
        return Err(ForgeError::Config("expected Capture step".to_owned()));
    };
    let timeout = crate::service::health::parse_duration(timeout)?;
    let interval = crate::service::health::parse_duration(interval)?;
    let deadline = Instant::now() + timeout;
    loop {
        let value = run_capture_attempt(runner, sc, resource, namespace.as_deref(), jsonpath)?;
        if !value.is_empty() {
            sc.pending_captures.insert(key.to_owned(), value);
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ForgeError::Command {
                program: "kubectl get".to_owned(),
                message: format!("capture key '{key}': empty result from jsonpath '{jsonpath}' before timeout"),
            });
        }
        sleep_capture_interval(interval);
    }
}

/// Run one kubectl/jsonpath capture attempt.
fn run_capture_attempt(
    runner: &dyn CommandRunner,
    sc: &StepContext,
    resource: &str,
    namespace: Option<&str>,
    jsonpath: &str,
) -> Result<String, ForgeError> {
    let spec = steps::kubectl_get_jsonpath(&sc.kube_context, resource, namespace, jsonpath);
    let output = runner.run(&spec)?;
    steps::check_success(&output, "kubectl get")?;
    Ok(output.stdout.trim().to_owned())
}

/// Sleep between capture attempts.
#[expect(
    clippy::disallowed_methods,
    reason = "forge is synchronous; capture polling has no async runtime"
)]
fn sleep_capture_interval(interval: Duration) {
    std::thread::sleep(interval);
}

/// Apply a local manifest file after rendering template expressions.
fn execute_template_manifest(
    runner: &dyn CommandRunner,
    path: &str,
    tpl: &TemplateContext,
    sc: &StepContext,
) -> Result<(), ForgeError> {
    let resolved = resolve_path(&sc.config_dir, path)?;
    let content = std::fs::read_to_string(&resolved)
        .map_err(|err| ForgeError::Config(format!("cannot read template manifest '{path}': {err}")))?;
    let rendered = template::render_with_limit(&content, tpl, steps::MAX_REMOTE_MANIFEST_BYTES)?;
    let spec = steps::kubectl_stdin_apply(&sc.kube_context, rendered.as_bytes());
    let output = runner.run(&spec)?;
    steps::check_success(&output, "kubectl apply")
}

/// Render a local template file to a local output path.
fn execute_template_file(
    source: &str,
    target: &str,
    tpl: &TemplateContext,
    sc: &StepContext,
) -> Result<(), ForgeError> {
    let resolved_source = resolve_path(&sc.config_dir, source)?;
    let content = std::fs::read_to_string(&resolved_source)
        .map_err(|err| ForgeError::Config(format!("cannot read template file '{source}': {err}")))?;
    let rendered = template::render_with_limit(&content, tpl, steps::MAX_REMOTE_MANIFEST_BYTES)?;
    let resolved_target = resolve_output_path(&sc.config_dir, &sc.state_dir, target)?;
    write_rendered_file(&resolved_target, rendered.as_bytes())
}

/// Write a rendered local file through temp-file-and-rename.
fn write_rendered_file(path: &Path, bytes: &[u8]) -> Result<(), ForgeError> {
    let Some(parent) = path.parent() else {
        return Err(ForgeError::Config(format!(
            "target path '{}' has no parent",
            path.display()
        )));
    };
    std::fs::create_dir_all(parent)
        .map_err(|err| ForgeError::Config(format!("cannot create output directory '{}': {err}", parent.display())))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ForgeError::Config(format!("invalid target filename '{}'", path.display())))?;
    let tmp = parent.join(format!(".{file_name}.tmp"));
    std::fs::write(&tmp, bytes)
        .map_err(|err| ForgeError::Config(format!("cannot write temporary file '{}': {err}", tmp.display())))?;
    std::fs::rename(&tmp, path)
        .map_err(|err| ForgeError::Config(format!("cannot move rendered file to '{}': {err}", path.display())))
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
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_)
        | serde_json::Value::Object(_) => Err(ForgeError::Config(format!(
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
    Ok(output.stdout)
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
        StepSpec::Url { url, sha256 } => render_url_step(url, sha256, tpl),
        StepSpec::Manifest { path } => render_path(path, tpl).map(|rendered| StepSpec::Manifest { path: rendered }),
        StepSpec::Kustomize { path } => render_path(path, tpl).map(|rendered| StepSpec::Kustomize { path: rendered }),
        StepSpec::TemplateManifest { path } => {
            render_path(path, tpl).map(|rendered| StepSpec::TemplateManifest { path: rendered })
        },
        StepSpec::TemplateFile { source, target } => Ok(StepSpec::TemplateFile {
            source: render_path(source, tpl)?,
            target: render_path(target, tpl)?,
        }),
        StepSpec::MetallbAutoPool { name } => render_path(name, tpl).map(|n| StepSpec::MetallbAutoPool { name: n }),
        StepSpec::Helm { .. } => render_helm_step(step, tpl),
        StepSpec::Deployment { .. } => render_deployment_step(step, tpl),
        StepSpec::Service { name, port, namespace } => render_service_step(name, *port, namespace.as_ref(), tpl),
        StepSpec::Wait { .. } => render_wait_step(step, tpl),
        StepSpec::Exec { command, env } => Ok(StepSpec::Exec {
            command: render_vec(command, tpl)?,
            env: render_string_map(env, tpl)?,
        }),
        StepSpec::ForEach { property, steps: sub } => render_foreach_step(property, sub, tpl),
        StepSpec::CoreDnsForward { .. } => render_coredns_forward_step(step, tpl),
        StepSpec::Capture { .. } => render_capture_step(step, tpl),
    }
}

/// Render a single template string field.
fn render_path(value: &str, tpl: &TemplateContext) -> Result<String, ForgeError> {
    template::render(value, tpl)
}

/// Render a URL step's url and sha256 fields.
fn render_url_step(url: &str, sha256: &str, tpl: &TemplateContext) -> Result<StepSpec, ForgeError> {
    Ok(StepSpec::Url {
        url: template::render(url, tpl)?,
        sha256: template::render(sha256, tpl)?,
    })
}

/// Render template expressions in a string map (keys are fixed; values are templated).
fn render_string_map(
    values: &BTreeMap<String, String>,
    tpl: &TemplateContext,
) -> Result<BTreeMap<String, String>, ForgeError> {
    values
        .iter()
        .map(|(key, value)| Ok((key.clone(), template::render(value, tpl)?)))
        .collect()
}

/// Render a for-each step's property field.
fn render_foreach_step(property: &str, sub: &[StepSpec], tpl: &TemplateContext) -> Result<StepSpec, ForgeError> {
    Ok(StepSpec::ForEach {
        property: template::render(property, tpl)?,
        steps: sub.to_vec(),
    })
}

/// Render templates in a capture step (resource and namespace only).
fn render_capture_step(step: &StepSpec, tpl: &TemplateContext) -> Result<StepSpec, ForgeError> {
    let StepSpec::Capture {
        resource,
        namespace,
        jsonpath,
        key,
        timeout,
        interval,
    } = step
    else {
        return Err(ForgeError::Config("expected Capture step".to_owned()));
    };
    Ok(StepSpec::Capture {
        resource: template::render(resource, tpl)?,
        namespace: render_optional(namespace.as_ref(), tpl)?,
        jsonpath: jsonpath.clone(),
        key: key.clone(),
        timeout: template::render(timeout, tpl)?,
        interval: template::render(interval, tpl)?,
    })
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
        namespace: render_optional(namespace.as_ref(), tpl)?,
        values: render_values(values, tpl)?,
    })
}

/// Render template expressions in Helm values recursively.
fn render_values(
    values: &BTreeMap<String, serde_json::Value>,
    tpl: &TemplateContext,
) -> Result<BTreeMap<String, serde_json::Value>, ForgeError> {
    values
        .iter()
        .map(|(key, value)| Ok((key.clone(), render_json_value(value, tpl)?)))
        .collect()
}

/// Render a JSON value, preserving non-string types.
fn render_json_value(value: &serde_json::Value, tpl: &TemplateContext) -> Result<serde_json::Value, ForgeError> {
    match value {
        serde_json::Value::String(text) => Ok(serde_json::Value::String(template::render(text, tpl)?)),
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
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => Ok(value.clone()),
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
        namespace: render_optional(namespace.as_ref(), tpl)?,
        args: render_vec(args, tpl)?,
    })
}

/// Render templates in a Service step.
fn render_service_step(
    name: &str,
    port: u16,
    namespace: Option<&String>,
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
        namespace,
    } = step
    else {
        return Err(ForgeError::Config("expected Wait step".to_owned()));
    };
    Ok(StepSpec::Wait {
        resource: template::render(resource, tpl)?,
        condition: template::render(condition, tpl)?,
        timeout: template::render(timeout, tpl)?,
        namespace: render_optional(namespace.as_ref(), tpl)?,
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
fn render_optional(opt: Option<&String>, tpl: &TemplateContext) -> Result<Option<String>, ForgeError> {
    opt.map(|text| template::render(text, tpl)).transpose()
}

/// Render a vec of strings through the template engine.
fn render_vec(items: &[String], tpl: &TemplateContext) -> Result<Vec<String>, ForgeError> {
    items.iter().map(|text| template::render(text, tpl)).collect()
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

/// Runtime output prefix resolved against the Forge state directory.
const STATE_DIR_PREFIX: &str = ".forge/";

/// Resolve a local output path.
///
/// Ordinary relative paths resolve under `config_dir`. Paths under `.forge/`
/// resolve under the configured Forge state directory so stack steps can
/// prepare runtime files for host services.
fn resolve_output_path(config_dir: &Path, state_dir: &Path, path: &str) -> Result<std::path::PathBuf, ForgeError> {
    if path.trim().is_empty() || Path::new(path).is_absolute() || path.split('/').any(|part| part == "..") {
        return Err(ForgeError::Config(format!(
            "stack output path '{path}' must be relative and must not escape the config root"
        )));
    }
    if let Some(suffix) = path.strip_prefix(STATE_DIR_PREFIX) {
        return Ok(state_dir.join(suffix));
    }
    Ok(config_dir.join(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::runner::{CommandOutput, MockRunner},
        config::{
            API_VERSION, EnvironmentSpec, ForgeConfig, KIND, Metadata, NodeConfig, RuntimeConfig, RuntimeProvider,
        },
        output::OutputFormat,
    };

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
            state_dir: std::path::PathBuf::from("/tmp/.forge"),
            runtime_binary: "docker".to_owned(),
            network_name: Some("test-net".to_owned()),
            cluster_pool: None,
            cluster_index: 0,
            cluster_count: 2,
            pool_allocation: None,
            pending_captures: BTreeMap::new(),
        }
    }

    fn make_template_context() -> TemplateContext {
        TemplateContext {
            cluster_name: "hub".to_owned(),
            stack_name: "base".to_owned(),
            properties: BTreeMap::new(),
            item: None,
            network: None,
            captures: BTreeMap::new(),
        }
    }

    /// Build a Forge context whose metadata name intentionally differs from
    /// runtime.clusterPrefix, proving stack operations use the runtime prefix.
    fn make_forge_context<'ctx>(runner: &'ctx dyn CommandRunner, config: &'ctx ForgeConfig) -> ForgeContext<'ctx> {
        ForgeContext {
            runner,
            config,
            state_dir: std::path::PathBuf::from("/tmp/state"),
            config_dir: std::path::PathBuf::from("/tmp"),
            format: OutputFormat::Text,
            dry_run: false,
        }
    }

    /// Build minimal config for step-context tests.
    fn context_test_config() -> ForgeConfig {
        ForgeConfig {
            api_version: API_VERSION.to_owned(),
            kind: KIND.to_owned(),
            metadata: Metadata {
                name: "env-name".to_owned(),
            },
            spec: EnvironmentSpec {
                runtime: RuntimeConfig {
                    provider: RuntimeProvider::Docker,
                    cluster_prefix: "runtime-prefix".to_owned(),
                },
                network: None,
                clusters: Vec::new(),
                services: Vec::new(),
                certificates: None,
                stacks: BTreeMap::new(),
            },
        }
    }

    #[test]
    fn step_context_uses_runtime_cluster_prefix() {
        let mut runner = MockRunner::new();
        runner.respond("docker", ok_output());
        let config = context_test_config();
        let ctx = make_forge_context(&runner, &config);
        let cluster = ClusterSpec {
            name: "provider-east".to_owned(),
            nodes: NodeConfig::default(),
            stacks: Vec::new(),
            properties: BTreeMap::new(),
        };
        let sc = build_step_context(&ctx, &cluster, None).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            sc.kube_context, "kind-runtime-prefix-provider-east",
            "stack operations must use runtime.clusterPrefix, not metadata.name"
        );
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
            call_strs.iter().any(|cmd| cmd.contains("w1.yaml")),
            "should apply w1.yaml: {call_strs:?}"
        );
        assert!(
            call_strs.iter().any(|cmd| cmd.contains("w2.yaml")),
            "should apply w2.yaml: {call_strs:?}"
        );
    }

    #[test]
    fn foreach_rejects_too_many_items() {
        let runner = MockRunner::new();
        let mut sc = make_step_context();
        let mut tpl = make_template_context();
        let items: Vec<serde_json::Value> = (0..=MAX_FOREACH_ITEMS)
            .map(|idx| serde_json::Value::String(format!("item-{idx}")))
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
            captures: BTreeMap::new(),
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
    fn exec_resolves_relative_script_against_config_dir() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let scripts = dir.path().join("scripts");
        std::fs::create_dir(&scripts).unwrap_or_else(|_| std::process::abort());
        let script = scripts.join("install.sh");
        std::fs::write(&script, "#!/bin/true\n").unwrap_or_else(|_| std::process::abort());

        let mut runner = MockRunner::new();
        runner.respond("bash", ok_output());
        let command = vec![
            "bash".to_owned(),
            "scripts/install.sh".to_owned(),
            "kind-maas-ipp-local".to_owned(),
        ];
        execute_exec(&runner, &command, &BTreeMap::new(), dir.path()).unwrap_or_else(|_| std::process::abort());

        let calls = runner.calls();
        let call = calls.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(call.program, "bash");
        assert_eq!(
            call.args.first().map(|arg| arg.to_string_lossy().into_owned()),
            Some(script.to_string_lossy().into_owned()),
            "script path must resolve under config_dir"
        );
        assert_eq!(
            call.args
                .get(1)
                .map(|arg| arg.to_string_lossy().into_owned())
                .as_deref(),
            Some("kind-maas-ipp-local"),
            "non-path args must stay unchanged"
        );
    }

    #[test]
    fn exec_passes_templated_env_to_command() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let mut runner = MockRunner::new();
        runner.respond("bash", ok_output());
        let env = BTreeMap::from([("GIE_VERSION".to_owned(), "v1.5.0".to_owned())]);
        let command = vec!["bash".to_owned(), "-c".to_owned(), "true".to_owned()];
        execute_exec(&runner, &command, &env, dir.path()).unwrap_or_else(|_| std::process::abort());
        let call = runner
            .calls()
            .into_iter()
            .next()
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            call.env.get(std::ffi::OsStr::new("GIE_VERSION")),
            Some(&std::ffi::OsString::from("v1.5.0")),
            "exec env must be forwarded to CommandSpec"
        );
    }

    #[test]
    fn render_exec_env_and_url_sha256_from_properties() {
        let mut tpl = make_template_context();
        tpl.properties
            .insert("gieVersion".to_owned(), serde_json::json!("v1.5.0"));
        tpl.properties
            .insert("gatewayApiSha256".to_owned(), serde_json::json!("abc123"));
        let exec = StepSpec::Exec {
            command: vec!["bash".to_owned(), "scripts/install-gie-crds.sh".to_owned()],
            env: BTreeMap::from([(
                "GIE_VERSION".to_owned(),
                "{{ cluster.properties.gieVersion }}".to_owned(),
            )]),
        };
        let rendered = render_step(&exec, &tpl).unwrap_or_else(|_| std::process::abort());
        let StepSpec::Exec { env, .. } = rendered else {
            std::process::abort();
        };
        assert_eq!(env.get("GIE_VERSION").map(String::as_str), Some("v1.5.0"));

        let url_step = StepSpec::Url {
            url: "https://example.test/v{{ cluster.properties.gieVersion }}/x.yaml".to_owned(),
            sha256: "{{ cluster.properties.gatewayApiSha256 }}".to_owned(),
        };
        let rendered_url = render_step(&url_step, &tpl).unwrap_or_else(|_| std::process::abort());
        let StepSpec::Url { url, sha256 } = rendered_url else {
            std::process::abort();
        };
        assert_eq!(url, "https://example.test/vv1.5.0/x.yaml");
        assert_eq!(sha256, "abc123");
    }

    #[test]
    fn exec_rejects_path_escape() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let command = vec!["bash".to_owned(), "../outside.sh".to_owned()];
        let Err(err) = resolve_exec_args(&command, dir.path()) else {
            std::process::abort();
        };
        assert!(err.to_string().contains("must not escape"), "unexpected error: {err}");
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
            captures: BTreeMap::new(),
        };
        let step = StepSpec::Manifest {
            path: "{{ cluster.name }}/manifests".to_owned(),
        };
        let rendered = render_step(&step, &tpl).unwrap_or_else(|_| std::process::abort());
        let StepSpec::Manifest { path } = &rendered else {
            std::process::abort();
        };
        assert_eq!(path, "hub/manifests", "template should be resolved");
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
            values.get("image").and_then(|val| val.get("repository")),
            Some(&serde_json::Value::String("example/web:v1".to_owned()))
        );
        assert_eq!(
            values.get("image").and_then(|val| val.get("replicas")),
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
            .find(|call| call.to_string().contains("apply"))
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
            .find(|call| call.to_string().contains("apply"))
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
        let apply = calls.iter().find(|call| call.to_string().contains("apply"));
        assert!(apply.is_some(), "should apply updated configmap");
        let restart = calls.iter().find(|call| call.to_string().contains("rollout"));
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

    #[test]
    fn capture_step_stores_value() {
        let mut runner = MockRunner::new();
        runner.respond(
            "kubectl",
            CommandOutput {
                status: 0,
                stdout: "  172.18.255.200  ".to_owned(),
                stderr: String::new(),
            },
        );
        let mut sc = make_step_context();
        let step = StepSpec::Capture {
            resource: "svc/provider-gateway".to_owned(),
            namespace: Some("grid-system".to_owned()),
            jsonpath: "{.status.loadBalancer.ingress[0].ip}".to_owned(),
            key: "provider-gateway-ip".to_owned(),
            timeout: "1s".to_owned(),
            interval: "1ms".to_owned(),
        };
        execute_capture(&runner, &step, &mut sc).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            sc.pending_captures.get("provider-gateway-ip").map(String::as_str),
            Some("172.18.255.200"),
            "should capture and trim IP"
        );
    }

    #[test]
    fn capture_step_rejects_empty_result() {
        let mut runner = MockRunner::new();
        runner.respond(
            "kubectl",
            CommandOutput {
                status: 0,
                stdout: "   ".to_owned(),
                stderr: String::new(),
            },
        );
        let mut sc = make_step_context();
        let step = StepSpec::Capture {
            resource: "svc/provider-gateway".to_owned(),
            namespace: None,
            jsonpath: "{.status.loadBalancer.ingress[0].ip}".to_owned(),
            key: "gw-ip".to_owned(),
            timeout: "1ms".to_owned(),
            interval: "1ms".to_owned(),
        };
        let result = execute_capture(&runner, &step, &mut sc);
        assert!(result.is_err(), "empty capture should fail");
        assert!(sc.pending_captures.is_empty(), "should not store empty value");
    }

    #[test]
    fn template_manifest_renders_and_applies() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let manifest_path = dir.path().join("net.yaml");
        std::fs::write(&manifest_path, "name: {{ cluster.name }}").unwrap_or_else(|_| std::process::abort());
        let mut runner = MockRunner::new();
        runner.respond("kubectl", ok_output());
        let mut tpl = make_template_context();
        tpl.cluster_name = "edge".to_owned();
        let mut sc = make_step_context();
        sc.config_dir = dir.path().to_path_buf();
        execute_template_manifest(&runner, "net.yaml", &tpl, &sc).unwrap_or_else(|_| std::process::abort());
        let calls = runner.calls();
        let apply = calls.first().unwrap_or_else(|| std::process::abort());
        let stdin_bytes = apply.stdin.as_deref().unwrap_or_else(|| std::process::abort());
        let stdin_text = std::str::from_utf8(stdin_bytes).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            stdin_text, "name: edge",
            "should render cluster.name in manifest content"
        );
    }

    #[test]
    fn template_manifest_renders_captures() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let manifest_path = dir.path().join("gw.yaml");
        std::fs::write(&manifest_path, "ip: {{ captures.provider-east.gw-ip }}:8080")
            .unwrap_or_else(|_| std::process::abort());
        let mut runner = MockRunner::new();
        runner.respond("kubectl", ok_output());
        let mut tpl = make_template_context();
        tpl.captures = BTreeMap::from([(
            "provider-east".to_owned(),
            BTreeMap::from([("gw-ip".to_owned(), "172.18.255.200".to_owned())]),
        )]);
        let mut sc = make_step_context();
        sc.config_dir = dir.path().to_path_buf();
        execute_template_manifest(&runner, "gw.yaml", &tpl, &sc).unwrap_or_else(|_| std::process::abort());
        let calls = runner.calls();
        let apply = calls.first().unwrap_or_else(|| std::process::abort());
        let stdin_bytes = apply.stdin.as_deref().unwrap_or_else(|| std::process::abort());
        let stdin_text = std::str::from_utf8(stdin_bytes).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            stdin_text, "ip: 172.18.255.200:8080",
            "should render captures in manifest content"
        );
    }

    #[test]
    fn template_file_renders_to_state_dir() {
        let config_dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let state_dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let template_path = config_dir.path().join("edge.yaml");
        std::fs::write(&template_path, "endpoint: {{ captures.provider-east.gw-ip }}:8080")
            .unwrap_or_else(|_| std::process::abort());

        let mut tpl = make_template_context();
        tpl.captures = BTreeMap::from([(
            "provider-east".to_owned(),
            BTreeMap::from([("gw-ip".to_owned(), "172.18.255.200".to_owned())]),
        )]);
        let mut sc = make_step_context();
        sc.config_dir = config_dir.path().to_path_buf();
        sc.state_dir = state_dir.path().to_path_buf();

        execute_template_file("edge.yaml", ".forge/runtime/edge-us-east/praxis/praxis.yaml", &tpl, &sc)
            .unwrap_or_else(|_| std::process::abort());

        let rendered = std::fs::read_to_string(state_dir.path().join("runtime/edge-us-east/praxis/praxis.yaml"))
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            rendered, "endpoint: 172.18.255.200:8080",
            "should render captures into state-dir runtime output"
        );
    }

    #[test]
    fn template_file_rejects_escaping_target() {
        let config_dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let template_path = config_dir.path().join("edge.yaml");
        std::fs::write(&template_path, "endpoint: static").unwrap_or_else(|_| std::process::abort());
        let tpl = make_template_context();
        let mut sc = make_step_context();
        sc.config_dir = config_dir.path().to_path_buf();

        let result = execute_template_file("edge.yaml", "../runtime/praxis.yaml", &tpl, &sc);
        assert!(result.is_err(), "escaping target path should be rejected");
    }
}
