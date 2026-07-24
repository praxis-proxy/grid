//! Container service lifecycle management.
//!
//! Creates, stops, and manages Docker/Podman container services for
//! Forge environments.  All commands are structured [`CommandSpec`]
//! values executed through [`CommandRunner`].  No shell strings.

pub mod health;

use std::{
    collections::{BTreeMap, VecDeque},
    ffi::OsString,
    io::Write,
    path::Path,
};

use serde::Deserialize;

use crate::{
    cli::ServiceCommand,
    command::runner::{CommandOutput, CommandRunner, CommandSpec, Redaction, RedactionKind},
    config::{NetworkMode, PortMapping, RestartPolicy, ServiceSpec, VolumeMount},
    context::ForgeContext,
    error::ForgeError,
    output::{self, OutputFormat},
    state::{self, ServiceHealth, ServicePhase, ServiceState, lock},
};

// -------------------------------------------------------------
// Naming
// -------------------------------------------------------------

/// Build the deterministic container name: `"{env_name}-{service_name}"`.
pub fn container_name(env_name: &str, service_name: &str) -> String {
    format!("{env_name}-{service_name}")
}

// -------------------------------------------------------------
// Parameters
// -------------------------------------------------------------

/// Bundled parameters for service lifecycle operations.
///
/// Groups common arguments to keep public function signatures
/// under the five-argument limit.
pub struct ServiceParams<'a> {
    /// Container runtime binary name (e.g. `"docker"`).
    pub binary: &'a str,
    /// Deterministic container name.
    pub container_name: &'a str,
    /// Environment name from configuration metadata.
    pub env_name: &'a str,
    /// Directory containing the configuration file.
    pub config_dir: &'a Path,
}

// -------------------------------------------------------------
// Identity
// -------------------------------------------------------------

/// Live container identity from runtime inspect.
///
/// All fields are optional because a stopped or missing container
/// has no live identity to report.
pub struct ServiceIdentity {
    /// Full container ID (64-char hex string).
    pub container_id: Option<String>,
    /// Container start timestamp (RFC 3339).
    pub started_at: Option<String>,
    /// Number of container restarts.
    pub restart_count: Option<u32>,
}

impl ServiceIdentity {
    /// Empty identity for missing or uninspectable containers.
    pub fn empty() -> Self {
        Self {
            container_id: None,
            started_at: None,
            restart_count: None,
        }
    }
}

/// Inspect a container for live identity fields.
///
/// Returns [`ServiceIdentity::empty`] when the container does not
/// exist (exit code != 0).  Returns an error if the inspect output
/// cannot be parsed as valid JSON.
///
/// # Errors
///
/// Returns [`ForgeError::State`] if the inspect output is malformed.
pub fn inspect_identity(
    runner: &dyn CommandRunner,
    binary: &str,
    container_name: &str,
) -> Result<ServiceIdentity, ForgeError> {
    let spec = identity_spec(binary, container_name);
    let output = runner.run(&spec)?;
    if output.status != 0 {
        return Ok(ServiceIdentity::empty());
    }
    parse_identity(&output.stdout)
}

// -------------------------------------------------------------
// Lifecycle
// -------------------------------------------------------------

/// Start (or restart) a container service.
///
/// Idempotent: if the container already exists and is owned by this
/// environment, it is stopped, removed, and recreated.
///
/// # Errors
///
/// Returns [`ForgeError`] if the container exists but is not owned
/// by this environment, or if any runtime command fails.
pub fn start_service(
    runner: &dyn CommandRunner,
    params: &ServiceParams<'_>,
    service: &ServiceSpec,
) -> Result<(), ForgeError> {
    if container_exists(runner, params.binary, params.container_name)? {
        verify_ownership(runner, params.binary, params.container_name, params.env_name)?;
        let stop = stop_spec(params.binary, params.container_name);
        check_success(&runner.run(&stop)?, "stop")?;
        let rm = rm_spec(params.binary, params.container_name);
        check_success(&runner.run(&rm)?, "rm")?;
    }
    let spec = run_spec(
        params.binary,
        params.container_name,
        service,
        params.env_name,
        params.config_dir,
    );
    let output = runner.run(&spec)?;
    check_success(&output, "run")
}

/// Stop and remove a container service.
///
/// Idempotent: returns `Ok(())` if the container does not exist.
///
/// # Errors
///
/// Returns [`ForgeError`] if the container exists but is not owned
/// by this environment, or if any runtime command fails.
pub fn stop_service(runner: &dyn CommandRunner, params: &ServiceParams<'_>) -> Result<(), ForgeError> {
    if !container_exists(runner, params.binary, params.container_name)? {
        return Ok(());
    }
    verify_ownership(runner, params.binary, params.container_name, params.env_name)?;
    let stop = stop_spec(params.binary, params.container_name);
    check_success(&runner.run(&stop)?, "stop")?;
    let rm = rm_spec(params.binary, params.container_name);
    check_success(&runner.run(&rm)?, "rm")
}

/// Check whether a container with the given name exists.
///
/// # Errors
///
/// Returns [`ForgeError`] if the runtime binary cannot execute.
pub fn container_exists(runner: &dyn CommandRunner, binary: &str, name: &str) -> Result<bool, ForgeError> {
    let spec = inspect_spec(binary, name);
    let output = runner.run(&spec)?;
    Ok(output.status == 0)
}

// -------------------------------------------------------------
// Dependency ordering
// -------------------------------------------------------------

/// Compute a topological ordering of services by their dependencies.
///
/// Returns indices into the input slice in dependency-first order.
/// Uses Kahn's algorithm.
///
/// # Errors
///
/// Returns [`ForgeError::Config`] if a dependency references an
/// unknown service or the graph contains a cycle.
pub fn dependency_order(services: &[ServiceSpec]) -> Result<Vec<usize>, ForgeError> {
    let index = build_name_index(services);
    let (adj, mut in_deg) = build_dep_graph(services, &index)?;
    toposort(&adj, &mut in_deg, services.len())
}

/// Adjacency list and in-degree vector for topological sort.
type DepGraph = (Vec<Vec<usize>>, Vec<usize>);

/// Map service names to their indices in the slice.
fn build_name_index(services: &[ServiceSpec]) -> BTreeMap<&str, usize> {
    services.iter().enumerate().map(|(i, s)| (s.name.as_str(), i)).collect()
}

/// Build adjacency list and in-degree vector from service deps.
fn build_dep_graph(services: &[ServiceSpec], index: &BTreeMap<&str, usize>) -> Result<DepGraph, ForgeError> {
    let n = services.len();
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut in_deg: Vec<usize> = vec![0; n];
    for (i, svc) in services.iter().enumerate() {
        for dep in &svc.depends_on {
            let &j = index
                .get(dep.as_str())
                .ok_or_else(|| ForgeError::Config(format!("service '{}' depends on unknown '{dep}'", svc.name)))?;
            let neighbours = adj
                .get_mut(j)
                .ok_or_else(|| ForgeError::State("graph index out of bounds".to_owned()))?;
            neighbours.push(i);
            let deg = in_deg
                .get_mut(i)
                .ok_or_else(|| ForgeError::State("graph index out of bounds".to_owned()))?;
            *deg += 1;
        }
    }
    Ok((adj, in_deg))
}

/// Run Kahn's algorithm on the adjacency list.
fn toposort(adj: &[Vec<usize>], in_deg: &mut [usize], n: usize) -> Result<Vec<usize>, ForgeError> {
    let mut queue: VecDeque<usize> = in_deg
        .iter()
        .enumerate()
        .filter(|(_, d)| **d == 0)
        .map(|(i, _)| i)
        .collect();
    let mut order = Vec::with_capacity(n);
    while let Some(u) = queue.pop_front() {
        order.push(u);
        let neighbours = adj
            .get(u)
            .ok_or_else(|| ForgeError::State("toposort index error".to_owned()))?;
        for &v in neighbours {
            let deg = in_deg
                .get_mut(v)
                .ok_or_else(|| ForgeError::State("toposort index error".to_owned()))?;
            *deg = deg.saturating_sub(1);
            if *deg == 0 {
                queue.push_back(v);
            }
        }
    }
    if order.len() == n {
        Ok(order)
    } else {
        Err(ForgeError::Config("cycle in service dependencies".to_owned()))
    }
}

// -------------------------------------------------------------
// Ownership
// -------------------------------------------------------------

/// Verify that an existing container is owned by this environment.
fn verify_ownership(runner: &dyn CommandRunner, binary: &str, name: &str, env_name: &str) -> Result<(), ForgeError> {
    let labels = inspect_labels(runner, binary, name)?;
    check_label(&labels, "forge.managed", "true", name)?;
    check_label(&labels, "forge.environment", env_name, name)
}

/// Fetch labels from an existing container.
fn inspect_labels(
    runner: &dyn CommandRunner,
    binary: &str,
    name: &str,
) -> Result<BTreeMap<String, String>, ForgeError> {
    let spec = labels_spec(binary, name);
    let output = runner.run(&spec)?;
    check_success(&output, "inspect labels")?;
    parse_labels(&output.stdout)
}

/// Verify a single label value matches the expected value.
fn check_label(labels: &BTreeMap<String, String>, key: &str, expected: &str, name: &str) -> Result<(), ForgeError> {
    match labels.get(key) {
        Some(v) if v == expected => Ok(()),
        Some(v) => Err(ownership_mismatch(name, key, expected, v)),
        None => Err(missing_label(name, key)),
    }
}

/// Build an error for a mismatched ownership label.
fn ownership_mismatch(name: &str, key: &str, expected: &str, actual: &str) -> ForgeError {
    ForgeError::State(format!("container '{name}' has {key}={actual}, expected {expected}"))
}

/// Build an error for a missing ownership label.
fn missing_label(name: &str, key: &str) -> ForgeError {
    ForgeError::State(format!(
        "container '{name}' missing label {key} \u{2014} not managed by Forge"
    ))
}

// -------------------------------------------------------------
// Command specs
// -------------------------------------------------------------

/// Build a `docker run` command spec with all configured options.
fn run_spec(binary: &str, name: &str, service: &ServiceSpec, env_name: &str, config_dir: &Path) -> CommandSpec {
    let mut args = base_run_args(name, env_name, &service.name);
    let mut redact = Vec::new();
    append_network_args(&mut args, &service.network, env_name);
    append_port_args(&mut args, &service.ports);
    append_volume_args(&mut args, &service.volumes, config_dir);
    append_env_args(&mut args, &mut redact, &service.env);
    append_restart_arg(&mut args, &service.restart);
    append_image_and_cmd(&mut args, &service.image, &service.args);
    build_spec_with_redactions(binary, args, redact)
}

/// Build the base `run -d --name ... --label ...` argument list.
fn base_run_args(name: &str, env_name: &str, service_name: &str) -> Vec<OsString> {
    vec![
        "run".into(),
        "-d".into(),
        "--name".into(),
        name.into(),
        "--label".into(),
        "forge.managed=true".into(),
        "--label".into(),
        format!("forge.environment={env_name}").into(),
        "--label".into(),
        format!("forge.service={service_name}").into(),
    ]
}

/// Append `--network` arguments based on the network mode.
fn append_network_args(args: &mut Vec<OsString>, network: &NetworkMode, env_name: &str) {
    match network {
        NetworkMode::Environment => {
            args.push("--network".into());
            args.push(format!("{env_name}-net").into());
        },
        NetworkMode::Host => {
            args.push("--network".into());
            args.push("host".into());
        },
        NetworkMode::None => {},
    }
}

/// Append `-p` port mapping arguments.
fn append_port_args(args: &mut Vec<OsString>, ports: &[PortMapping]) {
    for port in ports {
        args.push("-p".into());
        args.push(format_port_mapping(port).into());
    }
}

/// Append `-v` volume mount arguments.
fn append_volume_args(args: &mut Vec<OsString>, volumes: &[VolumeMount], config_dir: &Path) {
    for vol in volumes {
        args.push("-v".into());
        args.push(format_volume_arg(vol, config_dir).into());
    }
}

/// Append `-e` environment variable arguments.
fn append_env_args(args: &mut Vec<OsString>, redact: &mut Vec<Redaction>, env: &BTreeMap<String, String>) {
    for (key, val) in env {
        let arg = OsString::from(format!("{key}={val}"));
        args.push("-e".into());
        args.push(arg.clone());
        redact.push(Redaction {
            kind: RedactionKind::EnvValue,
            value: arg,
        });
    }
}

/// Append the `--restart` policy argument.
fn append_restart_arg(args: &mut Vec<OsString>, restart: &RestartPolicy) {
    args.push("--restart".into());
    args.push(restart_policy_str(restart).into());
}

/// Append the image name and optional command arguments.
fn append_image_and_cmd(args: &mut Vec<OsString>, image: &str, cmd_args: &[String]) {
    args.push(image.into());
    for a in cmd_args {
        args.push(a.into());
    }
}

/// Build a `<binary> stop <name>` command spec.
fn stop_spec(binary: &str, name: &str) -> CommandSpec {
    build_spec(binary, vec!["stop".into(), name.into()])
}

/// Build a `<binary> rm -f <name>` command spec.
fn rm_spec(binary: &str, name: &str) -> CommandSpec {
    build_spec(binary, vec!["rm".into(), "-f".into(), name.into()])
}

/// Build a `<binary> inspect <name>` command spec.
fn inspect_spec(binary: &str, name: &str) -> CommandSpec {
    build_spec(binary, vec!["inspect".into(), name.into()])
}

/// Build a `<binary> inspect --format ... <name>` spec for labels.
fn labels_spec(binary: &str, name: &str) -> CommandSpec {
    build_spec(
        binary,
        vec![
            "inspect".into(),
            "--format".into(),
            "{{json .Config.Labels}}".into(),
            name.into(),
        ],
    )
}

/// Build a `<binary> inspect --format ...` spec for identity fields.
fn identity_spec(binary: &str, name: &str) -> CommandSpec {
    build_spec(
        binary,
        vec![
            "inspect".into(),
            "--format".into(),
            r#"{"containerId":{{json .Id}},"startedAt":{{json .State.StartedAt}},"restartCount":{{json .RestartCount}}}"#.into(),
            name.into(),
        ],
    )
}

/// Construct a [`CommandSpec`] from a binary and argument list.
fn build_spec(binary: &str, args: Vec<OsString>) -> CommandSpec {
    build_spec_with_redactions(binary, args, Vec::new())
}

/// Construct a [`CommandSpec`] from a binary, arguments, and redactions.
fn build_spec_with_redactions(binary: &str, args: Vec<OsString>, redact: Vec<Redaction>) -> CommandSpec {
    CommandSpec {
        program: binary.into(),
        args,
        env: BTreeMap::default(),
        stdin: None,
        redact,
    }
}

// -------------------------------------------------------------
// Helpers
// -------------------------------------------------------------

/// Map a [`RestartPolicy`] to the Docker CLI string.
fn restart_policy_str(policy: &RestartPolicy) -> &'static str {
    match policy {
        RestartPolicy::No => "no",
        RestartPolicy::OnFailure => "on-failure",
        RestartPolicy::Always => "always",
        RestartPolicy::UnlessStopped => "unless-stopped",
    }
}

/// Format a [`PortMapping`] as `[bind:]host:container/proto`.
fn format_port_mapping(port: &PortMapping) -> String {
    let base = format!("{}:{}/{}", port.host, port.container, port.protocol);
    match &port.bind_address {
        Some(addr) => format!("{addr}:{base}"),
        None => base,
    }
}

/// Format a volume mount as `source:target[:ro]`.
fn format_volume_arg(vol: &VolumeMount, config_dir: &Path) -> String {
    let source = resolve_source(&vol.source, config_dir);
    let base = format!("{}:{}", source.display(), vol.target);
    if vol.read_only { format!("{base}:ro") } else { base }
}

/// Resolve a volume source path against the config directory.
fn resolve_source(source: &str, config_dir: &Path) -> std::path::PathBuf {
    let path = Path::new(source);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(source)
    }
}

/// Parse JSON labels from `docker inspect --format` output.
fn parse_labels(stdout: &str) -> Result<BTreeMap<String, String>, ForgeError> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Ok(BTreeMap::new());
    }
    serde_json::from_str(trimmed).map_err(|e| ForgeError::State(format!("cannot parse container labels: {e}")))
}

/// Parsed identity fields from `docker inspect --format` output.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InspectOutput {
    /// Full container ID.
    container_id: String,
    /// Container start timestamp.
    started_at: String,
    /// Number of container restarts.
    restart_count: u32,
}

/// Parse identity JSON from `docker inspect --format` output.
fn parse_identity(stdout: &str) -> Result<ServiceIdentity, ForgeError> {
    let trimmed = stdout.trim();
    let parsed: InspectOutput =
        serde_json::from_str(trimmed).map_err(|e| ForgeError::State(format!("cannot parse service inspect: {e}")))?;
    Ok(ServiceIdentity {
        container_id: Some(parsed.container_id),
        started_at: Some(parsed.started_at),
        restart_count: Some(parsed.restart_count),
    })
}

/// Check command output for success (exit code 0).
fn check_success(output: &CommandOutput, context: &str) -> Result<(), ForgeError> {
    if output.status == 0 {
        return Ok(());
    }
    Err(ForgeError::Command {
        program: context.to_owned(),
        message: format!("exit code {}: {}", output.status, output.stderr.trim()),
    })
}

// -------------------------------------------------------------
// CLI dispatch
// -------------------------------------------------------------

/// Dispatch a service subcommand.
///
/// # Errors
///
/// Returns [`ForgeError`] if the operation fails.
pub fn dispatch(ctx: &ForgeContext<'_>, cmd: &ServiceCommand, writer: &mut dyn Write) -> Result<(), ForgeError> {
    match cmd {
        ServiceCommand::List => handle_list(ctx, writer),
        ServiceCommand::Start { name } => handle_start(ctx, name, writer),
        ServiceCommand::Stop { name } => handle_stop(ctx, name, writer),
    }
}

/// Handle `service list`.
fn handle_list(ctx: &ForgeContext<'_>, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let st = state::load(&ctx.state_dir)?;
    render_service_list(writer, &st.services, &ctx.format)
}

/// Handle `service start`.
fn handle_start(ctx: &ForgeContext<'_>, name: &str, writer: &mut dyn Write) -> Result<(), ForgeError> {
    let service = lookup_service(ctx, name)?;
    let rt = crate::runtime::resolve(ctx.runner, &ctx.config.spec.runtime.provider)?;
    let cname = container_name(&ctx.config.metadata.name, name);
    let env_name = &ctx.config.metadata.name;
    let params = ServiceParams {
        binary: &rt.binary,
        container_name: &cname,
        env_name,
        config_dir: &ctx.config_dir,
    };
    if ctx.dry_run {
        return report_text_or_json(writer, &format!("would start service '{name}'"), &ctx.format);
    }
    let _lock = lock::acquire(&ctx.state_dir)?;
    start_service(ctx.runner, &params, service)?;
    let mut st = state::load(&ctx.state_dir)?;
    upsert_service_state(&mut st, name, &cname, &service.image);
    state::save(&ctx.state_dir, &st)?;
    report_text_or_json(writer, &format!("started service '{name}'"), &ctx.format)
}

/// Handle `service stop`.
fn handle_stop(ctx: &ForgeContext<'_>, name: &str, writer: &mut dyn Write) -> Result<(), ForgeError> {
    lookup_service(ctx, name)?;
    let rt = crate::runtime::resolve(ctx.runner, &ctx.config.spec.runtime.provider)?;
    let cname = container_name(&ctx.config.metadata.name, name);
    let env_name = &ctx.config.metadata.name;
    let params = ServiceParams {
        binary: &rt.binary,
        container_name: &cname,
        env_name,
        config_dir: &ctx.config_dir,
    };
    if ctx.dry_run {
        return report_text_or_json(writer, &format!("would stop service '{name}'"), &ctx.format);
    }
    let _lock = lock::acquire(&ctx.state_dir)?;
    stop_service(ctx.runner, &params)?;
    let mut st = state::load(&ctx.state_dir)?;
    mark_service_stopped(&mut st, name);
    state::save(&ctx.state_dir, &st)?;
    report_text_or_json(writer, &format!("stopped service '{name}'"), &ctx.format)
}

// -------------------------------------------------------------
// Dispatch helpers
// -------------------------------------------------------------

/// Find a service in the config by name.
fn lookup_service<'a>(ctx: &'a ForgeContext<'_>, name: &str) -> Result<&'a ServiceSpec, ForgeError> {
    ctx.config
        .spec
        .services
        .iter()
        .find(|s| s.name == name)
        .ok_or_else(|| ForgeError::Config(format!("service '{name}' not found in config")))
}

/// Insert or update a service entry in state.
fn upsert_service_state(st: &mut state::ForgeState, name: &str, cname: &str, image: &str) {
    if let Some(svc) = state::find_service_mut(st, name) {
        svc.phase = ServicePhase::Running;
        svc.health = ServiceHealth::Unknown;
        svc.last_observed = state::now_epoch_secs();
        return;
    }
    st.services.push(ServiceState {
        name: name.to_owned(),
        container_name: cname.to_owned(),
        image: image.to_owned(),
        phase: ServicePhase::Running,
        health: ServiceHealth::Unknown,
        last_observed: state::now_epoch_secs(),
    });
}

/// Mark a service as stopped in state.
fn mark_service_stopped(st: &mut state::ForgeState, name: &str) {
    if let Some(svc) = state::find_service_mut(st, name) {
        svc.phase = ServicePhase::Stopped;
        svc.last_observed = state::now_epoch_secs();
    }
}

// -------------------------------------------------------------
// Reporting
// -------------------------------------------------------------

/// Render the service list to the writer.
fn render_service_list(
    writer: &mut dyn Write,
    services: &[ServiceState],
    format: &OutputFormat,
) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => {
            let envelope = output::success(serde_json::json!({ "services": services }));
            output::write_json(writer, &envelope)?;
        },
        OutputFormat::Text => {
            for svc in services {
                output::write_text(writer, &format_service_line(svc))?;
            }
        },
    }
    Ok(())
}

/// Format one service entry for human-readable output.
fn format_service_line(svc: &ServiceState) -> String {
    format!(
        "{}  phase={}  health={}",
        svc.name,
        phase_str(&svc.phase),
        health_str(&svc.health),
    )
}

/// Map a [`ServicePhase`] to a lowercase display string.
fn phase_str(phase: &ServicePhase) -> &'static str {
    match phase {
        ServicePhase::Pending => "pending",
        ServicePhase::Starting => "starting",
        ServicePhase::Running => "running",
        ServicePhase::Unhealthy => "unhealthy",
        ServicePhase::Stopped => "stopped",
        ServicePhase::Gone => "gone",
    }
}

/// Map a [`ServiceHealth`] to a lowercase display string.
fn health_str(health: &ServiceHealth) -> &'static str {
    match health {
        ServiceHealth::Unknown => "unknown",
        ServiceHealth::Healthy => "healthy",
        ServiceHealth::Unhealthy => "unhealthy",
    }
}

/// Write a message as text or JSON envelope.
fn report_text_or_json(writer: &mut dyn Write, message: &str, format: &OutputFormat) -> Result<(), ForgeError> {
    match format {
        OutputFormat::Json => {
            let envelope = output::success(serde_json::json!({ "message": message }));
            output::write_json(writer, &envelope)?;
        },
        OutputFormat::Text => {
            output::write_text(writer, message)?;
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::command::runner::MockRunner;

    // ---------------------------------------------------------
    // Test utilities
    // ---------------------------------------------------------

    /// Successful empty command output.
    fn ok() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    /// Failed command output (container not found).
    fn not_found() -> CommandOutput {
        CommandOutput {
            status: 1,
            stdout: String::new(),
            stderr: "no such container\n".to_owned(),
        }
    }

    /// Labels JSON for a Forge-managed container.
    fn owned_labels(env: &str) -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: format!(r#"{{"forge.managed":"true","forge.environment":"{env}","forge.service":"svc"}}"#),
            stderr: String::new(),
        }
    }

    /// Labels JSON for a container not managed by Forge.
    fn foreign_labels() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: r#"{"some.other":"label"}"#.to_owned(),
            stderr: String::new(),
        }
    }

    /// Build minimal service params for testing.
    fn test_params() -> ServiceParams<'static> {
        ServiceParams {
            binary: "docker",
            container_name: "test-env-test-svc",
            env_name: "test-env",
            config_dir: Path::new("/tmp/config"),
        }
    }

    /// Build a minimal [`ServiceSpec`] for testing.
    fn minimal_service() -> ServiceSpec {
        ServiceSpec {
            name: "test-svc".to_owned(),
            image: "nginx:latest".to_owned(),
            network: NetworkMode::None,
            depends_on: Vec::new(),
            ports: Vec::new(),
            volumes: Vec::new(),
            env: BTreeMap::new(),
            args: Vec::new(),
            restart: RestartPolicy::No,
            health_check: None,
        }
    }

    // ---------------------------------------------------------
    // Naming
    // ---------------------------------------------------------

    #[test]
    fn container_name_format() {
        assert_eq!(container_name("dev", "redis"), "dev-redis", "simple name");
        assert_eq!(
            container_name("prod-env", "my-svc"),
            "prod-env-my-svc",
            "hyphenated name"
        );
    }

    // ---------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------

    #[test]
    fn start_creates_new_container() {
        let mut runner = MockRunner::new();
        runner.respond("docker inspect test-env-test-svc", not_found());
        runner.respond("docker", ok());

        let params = test_params();
        let svc = minimal_service();
        start_service(&runner, &params, &svc).unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("run"), "should call docker run");
        assert!(runner.was_called("forge.managed=true"), "should include managed label");
    }

    #[test]
    fn start_replaces_existing_owned() {
        let mut runner = MockRunner::new();
        runner.respond("docker inspect test-env-test-svc", ok());
        runner.respond(
            "docker inspect --format {{json .Config.Labels}} test-env-test-svc",
            owned_labels("test-env"),
        );
        runner.respond("docker stop test-env-test-svc", ok());
        runner.respond("docker rm -f test-env-test-svc", ok());
        runner.respond("docker", ok());

        let params = test_params();
        let svc = minimal_service();
        start_service(&runner, &params, &svc).unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("stop"), "should call stop");
        assert!(runner.was_called("rm"), "should call rm");
        assert!(runner.was_called("run"), "should call run");
    }

    #[test]
    fn start_rejects_unowned() {
        let mut runner = MockRunner::new();
        runner.respond("docker inspect test-env-test-svc", ok());
        runner.respond(
            "docker inspect --format {{json .Config.Labels}} test-env-test-svc",
            foreign_labels(),
        );

        let params = test_params();
        let svc = minimal_service();
        let result = start_service(&runner, &params, &svc);
        assert!(result.is_err(), "should reject unowned container");
    }

    #[test]
    fn stop_removes_owned() {
        let mut runner = MockRunner::new();
        runner.respond("docker inspect test-env-test-svc", ok());
        runner.respond(
            "docker inspect --format {{json .Config.Labels}} test-env-test-svc",
            owned_labels("test-env"),
        );
        runner.respond("docker stop test-env-test-svc", ok());
        runner.respond("docker rm -f test-env-test-svc", ok());

        let params = test_params();
        stop_service(&runner, &params).unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("stop"), "should call stop");
        assert!(runner.was_called("rm"), "should call rm");
    }

    #[test]
    fn stop_skips_absent() {
        let mut runner = MockRunner::new();
        runner.respond("docker inspect test-env-test-svc", not_found());

        let params = test_params();
        stop_service(&runner, &params).unwrap_or_else(|_| std::process::abort());
        assert!(!runner.was_called("stop"), "should not call stop on missing container");
    }

    #[test]
    fn stop_rejects_unowned() {
        let mut runner = MockRunner::new();
        runner.respond("docker inspect test-env-test-svc", ok());
        runner.respond(
            "docker inspect --format {{json .Config.Labels}} test-env-test-svc",
            foreign_labels(),
        );

        let params = test_params();
        let result = stop_service(&runner, &params);
        assert!(result.is_err(), "should reject unowned container on stop");
    }

    // ---------------------------------------------------------
    // Dependency ordering
    // ---------------------------------------------------------

    #[test]
    fn dependency_order_no_deps() {
        let services = vec![minimal_service()];
        let order = dependency_order(&services).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(order, vec![0], "single service should be first");
    }

    #[test]
    fn dependency_order_linear_chain() {
        let mut a = minimal_service();
        a.name = "a".to_owned();

        let mut b = minimal_service();
        b.name = "b".to_owned();
        b.depends_on = vec!["a".to_owned()];

        let mut c = minimal_service();
        c.name = "c".to_owned();
        c.depends_on = vec!["b".to_owned()];

        let services = vec![a, b, c];
        let order = dependency_order(&services).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(order, vec![0, 1, 2], "should be a -> b -> c");
    }

    // ---------------------------------------------------------
    // Run spec tests
    // ---------------------------------------------------------

    #[test]
    fn run_spec_includes_labels() {
        let svc = minimal_service();
        let spec = run_spec("docker", "env-svc", &svc, "env", Path::new("/tmp"));
        let display = format!("{spec}");
        assert!(
            display.contains("forge.managed=true"),
            "should include managed label: {display}"
        );
        assert!(
            display.contains("forge.environment=env"),
            "should include env label: {display}"
        );
        assert!(
            display.contains("forge.service=test-svc"),
            "should include service label: {display}"
        );
    }

    #[test]
    fn run_spec_network_environment() {
        let mut svc = minimal_service();
        svc.network = NetworkMode::Environment;
        let spec = run_spec("docker", "env-svc", &svc, "myenv", Path::new("/tmp"));
        let display = format!("{spec}");
        assert!(display.contains("--network"), "should include --network: {display}");
        assert!(display.contains("myenv-net"), "should use env network: {display}");
    }

    #[test]
    fn run_spec_network_host() {
        let mut svc = minimal_service();
        svc.network = NetworkMode::Host;
        let spec = run_spec("docker", "env-svc", &svc, "env", Path::new("/tmp"));
        let display = format!("{spec}");
        assert!(display.contains("--network"), "should include --network: {display}");
        assert!(display.contains("host"), "should use host network: {display}");
    }

    #[test]
    fn run_spec_network_none() {
        let svc = minimal_service();
        let spec = run_spec("docker", "env-svc", &svc, "env", Path::new("/tmp"));
        let display = format!("{spec}");
        assert!(
            !display.contains("--network"),
            "should not include --network: {display}"
        );
    }

    #[test]
    fn run_spec_ports_with_bind_address() {
        let mut svc = minimal_service();
        svc.ports.push(PortMapping {
            bind_address: Some("127.0.0.1".to_owned()),
            host: 8080,
            container: 80,
            protocol: "tcp".to_owned(),
        });
        let spec = run_spec("docker", "env-svc", &svc, "env", Path::new("/tmp"));
        let display = format!("{spec}");
        assert!(
            display.contains("127.0.0.1:8080:80/tcp"),
            "should include port mapping: {display}"
        );
    }

    #[test]
    fn run_spec_volumes_read_only() {
        let mut svc = minimal_service();
        svc.volumes.push(VolumeMount {
            source: "data".to_owned(),
            target: "/mnt/data".to_owned(),
            read_only: true,
        });
        let spec = run_spec("docker", "env-svc", &svc, "env", Path::new("/cfg"));
        let display = format!("{spec}");
        assert!(
            display.contains("/cfg/data:/mnt/data:ro"),
            "should include ro volume: {display}"
        );
    }

    #[test]
    fn run_spec_env_vars() {
        let mut svc = minimal_service();
        svc.env.insert("MY_VAR".to_owned(), "my_value".to_owned());
        let spec = run_spec("docker", "env-svc", &svc, "env", Path::new("/tmp"));
        let display = format!("{spec}");
        assert!(display.contains("-e"), "should include -e flag: {display}");
        assert!(
            display.contains("[REDACTED]"),
            "should redact env var display: {display}"
        );
        assert!(
            !display.contains("MY_VAR=my_value"),
            "should not display env value: {display}"
        );
        assert!(
            spec.args.iter().any(|arg| arg == "MY_VAR=my_value"),
            "actual command args should still carry the env assignment"
        );
    }

    #[test]
    fn run_spec_restart_policy() {
        let mut svc = minimal_service();
        svc.restart = RestartPolicy::UnlessStopped;
        let spec = run_spec("docker", "env-svc", &svc, "env", Path::new("/tmp"));
        let display = format!("{spec}");
        assert!(display.contains("--restart"), "should include --restart: {display}");
        assert!(display.contains("unless-stopped"), "should include policy: {display}");
    }

    #[test]
    fn run_spec_no_privileged() {
        let svc = minimal_service();
        let spec = run_spec("docker", "env-svc", &svc, "env", Path::new("/tmp"));
        let display = format!("{spec}");
        assert!(
            !display.contains("--privileged"),
            "should not include --privileged: {display}"
        );
    }

    // ---------------------------------------------------------
    // Identity inspect tests
    // ---------------------------------------------------------

    /// Valid identity JSON matching the `--format` template output.
    fn valid_identity_json() -> String {
        r#"{"containerId":"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2","startedAt":"2026-07-22T14:30:00.123456789Z","restartCount":0}"#.to_owned()
    }

    #[test]
    fn identity_spec_structured_argv() {
        let spec = identity_spec("docker", "my-container");
        assert_eq!(spec.program, "docker", "program");
        let args: Vec<_> = spec.args.iter().map(|a| a.to_string_lossy()).collect();
        assert_eq!(args.first().map(AsRef::as_ref), Some("inspect"), "first arg");
        assert_eq!(args.get(1).map(AsRef::as_ref), Some("--format"), "second arg");
        assert_eq!(args.last().map(AsRef::as_ref), Some("my-container"), "last arg");
        assert!(spec.stdin.is_none(), "no stdin");
    }

    #[test]
    fn inspect_parses_container_id() {
        let mut runner = MockRunner::new();
        runner.respond(
            "docker",
            CommandOutput {
                status: 0,
                stdout: valid_identity_json(),
                stderr: String::new(),
            },
        );
        let id = inspect_identity(&runner, "docker", "test-ctr").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            id.container_id.as_deref(),
            Some("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"),
            "container_id"
        );
    }

    #[test]
    fn inspect_parses_started_at() {
        let mut runner = MockRunner::new();
        runner.respond(
            "docker",
            CommandOutput {
                status: 0,
                stdout: valid_identity_json(),
                stderr: String::new(),
            },
        );
        let id = inspect_identity(&runner, "docker", "test-ctr").unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            id.started_at.as_deref(),
            Some("2026-07-22T14:30:00.123456789Z"),
            "started_at"
        );
    }

    #[test]
    fn inspect_parses_restart_count() {
        let mut runner = MockRunner::new();
        runner.respond(
            "docker",
            CommandOutput {
                status: 0,
                stdout: valid_identity_json(),
                stderr: String::new(),
            },
        );
        let id = inspect_identity(&runner, "docker", "test-ctr").unwrap_or_else(|_| std::process::abort());
        assert_eq!(id.restart_count, Some(0), "restart_count");
    }

    #[test]
    fn inspect_missing_container_yields_empty() {
        let mut runner = MockRunner::new();
        runner.respond(
            "docker",
            CommandOutput {
                status: 1,
                stdout: String::new(),
                stderr: "no such container\n".to_owned(),
            },
        );
        let id = inspect_identity(&runner, "docker", "gone").unwrap_or_else(|_| std::process::abort());
        assert!(id.container_id.is_none(), "container_id should be None");
        assert!(id.started_at.is_none(), "started_at should be None");
        assert!(id.restart_count.is_none(), "restart_count should be None");
    }

    #[test]
    fn inspect_malformed_json_returns_error() {
        let mut runner = MockRunner::new();
        runner.respond(
            "docker",
            CommandOutput {
                status: 0,
                stdout: "not valid json".to_owned(),
                stderr: String::new(),
            },
        );
        let result = inspect_identity(&runner, "docker", "bad");
        assert!(result.is_err(), "malformed JSON should error");
        let Err(err) = result else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(msg.contains("cannot parse service inspect"), "error message: {msg}");
    }
}
