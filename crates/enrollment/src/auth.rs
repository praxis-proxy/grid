//! Grid-admin credentials for minting and revoking site tokens.
//!
//! A grid-admin is the party allowed to mint site tokens, named so it is not
//! confused with the grid-operator controller. Minting is not self-service, so
//! the mint and revoke routes require a grid-admin token.
//!
//! Tokens are kept as SHA-256 digests. Comparing digests rather than the tokens
//! themselves means a timing difference reveals nothing about a valid token, and
//! the table is not a list of usable credentials at rest.

use std::collections::HashMap;

/// Grid-admins allowed to mint and revoke site tokens.
#[derive(Debug, Default)]
pub struct GridAdmins {
    /// Token digest to the name recorded as the grid-admin.
    by_digest: HashMap<String, String>,
}

impl GridAdmins {
    /// Read a token table.
    ///
    /// One `name:token` per line. Blank lines and lines starting with `#` are
    /// skipped. A line without a separator is skipped rather than treated as a
    /// nameless credential.
    #[must_use]
    pub fn from_table(text: &str) -> Self {
        let by_digest = text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .filter_map(|line| line.split_once(':'))
            .filter_map(|(name, token)| {
                let name = name.trim();
                let token = token.trim();
                (!name.is_empty() && !token.is_empty()).then(|| (digest(token), name.to_owned()))
            })
            .collect();

        Self { by_digest }
    }

    /// The grid-admin a token belongs to, if any.
    #[must_use]
    pub fn resolve(&self, presented: &str) -> Option<&str> {
        self.by_digest.get(&digest(presented)).map(String::as_str)
    }

    /// Whether any grid-admin is configured.
    ///
    /// An empty table refuses every mint. No configured grid-admin has to mean
    /// nobody can mint, not that anybody can.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_digest.is_empty()
    }

    /// How many grid-admins are configured.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_digest.len()
    }
}

/// Lowercase hex SHA-256 of a token.
///
/// Routed through certs so a fips build hashes this credential in the validated
/// module rather than on the sha2 crate.
pub(crate) fn digest(token: &str) -> String {
    certs::sha256(token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configured_token_resolves_to_its_grid_admin() {
        let admins = GridAdmins::from_table("alice: s3cret\n");
        assert_eq!(
            admins.resolve("s3cret"),
            Some("alice"),
            "a configured token resolves to its grid-admin"
        );
        assert_eq!(admins.resolve("wrong"), None, "an unknown token resolves to nobody");
    }

    #[test]
    fn a_table_reads_every_configured_grid_admin() {
        let admins = GridAdmins::from_table("# admins\n\nalice: one\nbob: two\n\n");
        assert_eq!(admins.len(), 2, "two grid-admins should be read");
        assert_eq!(admins.resolve("two"), Some("bob"), "bob's token should resolve");
    }

    #[test]
    fn malformed_lines_are_skipped() {
        let admins = GridAdmins::from_table("nameless\nalice:\n: token\n");
        assert!(admins.is_empty(), "no usable grid-admin should be read");
        assert_eq!(admins.resolve(""), None, "an empty token must not resolve");
    }

    #[test]
    fn no_configured_grid_admin_means_nobody_can_mint() {
        let admins = GridAdmins::from_table("");
        assert!(admins.is_empty(), "an empty table configures nobody");
    }

    #[test]
    fn the_debug_view_shows_names_not_tokens() {
        let admins = GridAdmins::from_table("alice: s3cret\n");
        let debug = format!("{admins:?}");
        let expected = digest("s3cret");
        assert!(!debug.contains("s3cret"), "the token itself is not in the debug view");
        assert!(debug.contains(&expected), "the digest is what is held");
        assert!(debug.contains("alice"), "the grid-admin name is not a secret");
    }

    #[test]
    fn a_token_is_held_as_its_digest() {
        let admins = GridAdmins::from_table("alice: s3cret\n");
        let expected = digest("s3cret");
        assert!(
            admins.resolve(&expected).is_none(),
            "the digest is not itself a valid token"
        );
    }
}
