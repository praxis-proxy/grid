//! Entry point for the `praxis-forge` CLI.

use clap::Parser as _;
use forge::{
    cli::{Cli, Command, ConfigCommand},
    command::{config, doctor, plan, runner},
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
