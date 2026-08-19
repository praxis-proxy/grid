//! Command-line interface, parsed once at startup from flags/environment.

use clap::Parser;

use crate::gateway;

/// grid-operator command-line interface.
#[derive(Parser, Debug, Clone)]
#[command(name = "grid-operator", about = "AI Grid Kubernetes operator")]
pub struct Cli {
    /// Gateway self-discovery options.
    #[command(flatten)]
    pub gateway: gateway::Config,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use clap::{CommandFactory as _, Parser as _};

    use super::Cli;

    /// Runs clap's definition assertions on the real command.
    #[test]
    fn command_definition_is_valid() {
        Cli::command().debug_assert();
    }

    /// Catches duplicate group ids in release, where `debug_assert` is a no-op.
    #[test]
    fn argument_group_ids_are_unique() {
        let ids: Vec<String> = Cli::command().get_groups().map(|g| g.get_id().to_string()).collect();
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "duplicate clap group ids: {ids:?}");
    }

    /// The real parser accepts an empty argv. Asserts no values: `GRID_*` leak in.
    #[test]
    fn empty_argv_parses() {
        assert!(Cli::try_parse_from(["grid-operator"]).is_ok());
    }
}
