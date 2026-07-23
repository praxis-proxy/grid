//! Entry point for the `praxis-forge` CLI.

use clap::Parser as _;
use forge::{
    cli::{Cli, ClusterCommand, Command, ConfigCommand},
    cluster,
    command::{config, doctor, down, plan, runner, status, up},
    context::ForgeContext,
    error::ForgeError,
    output::{self, OutputFormat},
};

/// Parse CLI arguments and dispatch to the appropriate handler.
fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let mut stdout = std::io::stdout();
    let result = dispatch(&cli, &mut stdout);
    handle_result(result, &cli.global.output)
}

/// Dispatch the parsed command to its handler.
fn dispatch(cli: &Cli, writer: &mut dyn std::io::Write) -> Result<(), ForgeError> {
    let format = &cli.global.output;
    match &cli.command {
        Command::Doctor => dispatch_doctor(format, writer),
        Command::Plan => dispatch_plan(cli, format, writer),
        Command::Config(sub) => dispatch_config(cli, sub, format, writer),
        Command::Up => dispatch_up(cli, writer),
        Command::Down { force } => dispatch_down(cli, *force, writer),
        Command::Status => dispatch_status(cli, writer),
        Command::Cluster(sub) => dispatch_cluster(cli, sub, writer),
    }
}

/// Run the doctor command.
fn dispatch_doctor(format: &OutputFormat, writer: &mut dyn std::io::Write) -> Result<(), ForgeError> {
    let runner = runner::SystemRunner;
    doctor::run(&runner, format, writer)
}

/// Run the plan command.
fn dispatch_plan(cli: &Cli, format: &OutputFormat, writer: &mut dyn std::io::Write) -> Result<(), ForgeError> {
    plan::run(&cli.global.config, format, writer)
}

/// Dispatch config subcommands.
fn dispatch_config(
    cli: &Cli,
    sub: &ConfigCommand,
    format: &OutputFormat,
    writer: &mut dyn std::io::Write,
) -> Result<(), ForgeError> {
    match sub {
        ConfigCommand::Validate => config::run_validate(&cli.global.config, format, writer),
        ConfigCommand::Show { resolved } => config::run_show(&cli.global.config, *resolved, format, writer),
        ConfigCommand::Init { force } => {
            config::run_init(&cli.global.config, *force, cli.global.dry_run, format, writer)
        },
        ConfigCommand::Schema => config::run_schema(writer),
    }
}

/// Load config and validate it.
fn load_config_validated(cli: &Cli) -> Result<forge::config::ForgeConfig, ForgeError> {
    let mut cfg = forge::config::load(&cli.global.config)?;
    if let Some(runtime) = &cli.global.runtime {
        cfg.spec.runtime.provider = runtime.clone();
    }
    forge::config::validate::validate(&cfg)?;
    Ok(cfg)
}

/// Build a [`ForgeContext`] from CLI options.
fn build_context<'a>(
    cli: &'a Cli,
    runner: &'a dyn runner::CommandRunner,
    config: &'a forge::config::ForgeConfig,
) -> ForgeContext<'a> {
    ForgeContext {
        runner,
        config,
        state_dir: cli.global.state_dir.clone(),
        format: cli.global.output.clone(),
        dry_run: cli.global.dry_run,
    }
}

/// Dispatch the `up` command.
fn dispatch_up(cli: &Cli, writer: &mut dyn std::io::Write) -> Result<(), ForgeError> {
    let config = load_config_validated(cli)?;
    let runner = runner::SystemRunner;
    let ctx = build_context(cli, &runner, &config);
    up::run(&ctx, writer)
}

/// Dispatch the `down` command.
fn dispatch_down(cli: &Cli, force: bool, writer: &mut dyn std::io::Write) -> Result<(), ForgeError> {
    let config = load_config_validated(cli)?;
    let runner = runner::SystemRunner;
    let ctx = build_context(cli, &runner, &config);
    down::run(&ctx, force, writer)
}

/// Dispatch the `status` command.
fn dispatch_status(cli: &Cli, writer: &mut dyn std::io::Write) -> Result<(), ForgeError> {
    let config = load_config_validated(cli)?;
    let runner = runner::SystemRunner;
    let ctx = build_context(cli, &runner, &config);
    status::run(&ctx, writer)
}

/// Dispatch cluster subcommands.
fn dispatch_cluster(cli: &Cli, sub: &ClusterCommand, writer: &mut dyn std::io::Write) -> Result<(), ForgeError> {
    let config = load_config_validated(cli)?;
    let runner = runner::SystemRunner;
    let ctx = build_context(cli, &runner, &config);
    cluster::dispatch(&ctx, sub, writer)
}

/// Handle the result of command dispatch.
fn handle_result(result: Result<(), ForgeError>, format: &OutputFormat) -> std::process::ExitCode {
    let Err(e) = result else {
        return std::process::ExitCode::SUCCESS;
    };
    report_error(&e, format);
    std::process::ExitCode::FAILURE
}

/// Print an error to stderr in the appropriate format.
#[expect(clippy::print_stderr, reason = "CLI error reporting")]
fn report_error(e: &ForgeError, format: &OutputFormat) {
    match format {
        OutputFormat::Json => {
            let envelope = output::error(&e.to_string());
            let json = serde_json::to_string_pretty(&envelope)
                .unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_owned());
            eprintln!("{json}");
        },
        OutputFormat::Text => {
            eprintln!("error: {e}");
        },
    }
}
