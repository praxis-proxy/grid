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

use crate::authz::Operation;

/// A role a table entry grants, named after the kube-mode Role it mirrors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Mint and revoke tokens, and read enrollments.
    GridAdmin,
    /// Read and delete enrollments.
    EnrollmentAdmin,
}

impl Role {
    /// The role named exactly `name`.
    fn parse(name: &str) -> Option<Self> {
        match name {
            "grid-admin" => Some(Self::GridAdmin),
            "enrollment-admin" => Some(Self::EnrollmentAdmin),
            _ => None,
        }
    }

    /// Whether the role grants `operation`.
    fn permits(self, operation: Operation) -> bool {
        match self {
            Self::GridAdmin => matches!(
                (operation.resource, operation.verb),
                ("enrollmenttokens", "create" | "delete") | ("enrollments", "get")
            ),
            Self::EnrollmentAdmin => matches!((operation.resource, operation.verb), ("enrollments", "get" | "delete")),
        }
    }
}

/// A grid-admin table entry: the name recorded and the roles it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAdmin {
    /// The name recorded as the grid-admin.
    pub name: String,
    /// The roles granted.
    pub roles: Vec<Role>,
}

impl TableAdmin {
    /// Whether any of the entry's roles grants `operation`.
    #[must_use]
    pub fn permits(&self, operation: Operation) -> bool {
        operation.subresource.is_none() && self.roles.iter().any(|role| role.permits(operation))
    }
}

/// A grid-admin table that cannot be loaded.
#[derive(Debug, thiserror::Error)]
pub enum TableError {
    /// Two token lines carry the same credential.
    #[error("grid-admin token table line {line}: token is already assigned")]
    DuplicateToken {
        /// The 1-based line number.
        line: usize,
    },
    /// A role line has no separator or no admin name.
    #[error("grid-admin role table line {line}: expected name:role[,role] with a non-empty name")]
    MalformedRoles {
        /// The 1-based line number.
        line: usize,
    },
    /// A role list names no role, an unknown one, or one twice.
    #[error(
        "grid-admin role table line {line} ({name}): the role list must name each of grid-admin or \
         enrollment-admin at most once"
    )]
    Roles {
        /// The 1-based line number.
        line: usize,
        /// The entry's name.
        name: String,
    },
    /// A role line names somebody the token table does not.
    #[error("grid-admin role table line {line}: {name} has no token")]
    UnknownAdmin {
        /// The 1-based line number.
        line: usize,
        /// The entry's name.
        name: String,
    },
    /// Two role lines name one admin.
    #[error("grid-admin role table line {line}: {name} already has roles")]
    DuplicateRoles {
        /// The 1-based line number.
        line: usize,
        /// The entry's name.
        name: String,
    },
}

/// Grid-admins allowed to mint and revoke site tokens.
#[derive(Debug, Default)]
pub struct GridAdmins {
    /// Token digest to its entry.
    by_digest: HashMap<String, TableAdmin>,
}

impl GridAdmins {
    /// Read a token table.
    ///
    /// One `name:token` per line, where the token is everything after the first
    /// `:` and may contain any character. Every admin starts as `grid-admin`;
    /// [`Self::with_roles`] narrows that. Blank lines and lines starting with
    /// `#` are skipped, as is a line with no separator, name, or token.
    ///
    /// # Errors
    ///
    /// [`TableError::DuplicateToken`] if two lines carry the same token.
    pub fn from_table(text: &str) -> Result<Self, TableError> {
        let mut by_digest = HashMap::new();
        for (index, line) in text.lines().enumerate() {
            if let Some((token_digest, admin)) = parse_line(line)
                && by_digest.insert(token_digest, admin).is_some()
            {
                return Err(TableError::DuplicateToken {
                    line: index.saturating_add(1),
                });
            }
        }
        Ok(Self { by_digest })
    }

    /// Apply a role table, replacing the default `grid-admin` for each name it
    /// lists.
    ///
    /// One `name:role[,role]` per line, with roles from `grid-admin` and
    /// `enrollment-admin`. A name the table does not list keeps `grid-admin`.
    /// Roles live here rather than on the token line so that a token's meaning
    /// never depends on the token's own characters.
    ///
    /// # Errors
    ///
    /// [`TableError::Roles`] for a role list that is empty, names an unknown
    /// role, or repeats one; [`TableError::UnknownAdmin`] for a name with no
    /// token; [`TableError::DuplicateRoles`] for a name listed twice; or
    /// [`TableError::MalformedRoles`] for a line with no separator or name.
    pub fn with_roles(mut self, text: &str) -> Result<Self, TableError> {
        let mut seen: Vec<String> = Vec::new();
        for (index, line) in text.lines().enumerate() {
            let line_number = index.saturating_add(1);
            let Some((name, roles)) = parse_roles_line(line_number, line)? else {
                continue;
            };
            if seen.contains(&name) {
                return Err(TableError::DuplicateRoles {
                    line: line_number,
                    name,
                });
            }
            let mut matched = false;
            for admin in self.by_digest.values_mut().filter(|admin| admin.name == name) {
                admin.roles.clone_from(&roles);
                matched = true;
            }
            if !matched {
                return Err(TableError::UnknownAdmin {
                    line: line_number,
                    name,
                });
            }
            seen.push(name);
        }
        Ok(self)
    }

    /// The entry a token belongs to, if any.
    #[must_use]
    pub fn resolve(&self, presented: &str) -> Option<&TableAdmin> {
        self.by_digest.get(&digest(presented))
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

/// A parsed role line, `None` for a line that is skipped.
type RoleLine = Option<(String, Vec<Role>)>;

/// One role line as its name and roles, `None` for a line that is skipped.
fn parse_roles_line(line_number: usize, line: &str) -> Result<RoleLine, TableError> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Ok(None);
    }
    let (name, list) = line
        .split_once(':')
        .ok_or(TableError::MalformedRoles { line: line_number })?;
    let name = name.trim();
    if name.is_empty() {
        return Err(TableError::MalformedRoles { line: line_number });
    }
    let roles = parse_roles(list.trim()).ok_or_else(|| TableError::Roles {
        line: line_number,
        name: name.to_owned(),
    })?;
    Ok(Some((name.to_owned(), roles)))
}

/// One token line as its digest and entry, `None` for a line that is skipped.
///
/// The token is the whole remainder after the first `:`, so a token carrying a
/// `:` keeps its meaning.
fn parse_line(line: &str) -> Option<(String, TableAdmin)> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (name, token) = line.split_once(':')?;
    let (name, token) = (name.trim(), token.trim());
    if name.is_empty() || token.is_empty() {
        return None;
    }
    Some((
        digest(token),
        TableAdmin {
            name: name.to_owned(),
            roles: vec![Role::GridAdmin],
        },
    ))
}

/// A comma-separated role list, or `None` if it is empty, unknown, or repeats a role.
fn parse_roles(list: &str) -> Option<Vec<Role>> {
    let mut roles = Vec::new();
    for name in list.split(',') {
        let role = Role::parse(name.trim())?;
        if roles.contains(&role) {
            return None;
        }
        roles.push(role);
    }
    Some(roles)
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
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    fn table(text: &str) -> GridAdmins {
        GridAdmins::from_table(text).expect("valid token table")
    }

    #[test]
    fn duplicate_tokens_are_refused_in_any_order_without_echoing_credentials() {
        for names in [("carol", "alice"), ("alice", "carol"), ("alice", "alice")] {
            let text = format!("# admins\n{}: se:cret-tail\n\n{}:se:cret-tail\n", names.0, names.1);
            let error = GridAdmins::from_table(&text).expect_err("duplicate credential");
            assert!(matches!(error, TableError::DuplicateToken { line: 4 }), "{error:?}");
            for message in [error.to_string(), format!("{error:?}")] {
                assert!(!message.contains("se:cret-tail"), "token must not be echoed");
                assert!(!message.contains(&digest("se:cret-tail")), "digest must not be echoed");
            }
        }
    }

    #[test]
    fn distinct_tokens_for_one_admin_keep_the_same_roles() {
        let admins = table("alice:first\nalice:second\n")
            .with_roles("alice:enrollment-admin\n")
            .expect("roles apply to both credentials");
        assert_eq!(admins.len(), 2);
        assert_eq!(roles(&admins, "first"), [Role::EnrollmentAdmin]);
        assert_eq!(roles(&admins, "second"), [Role::EnrollmentAdmin]);
    }

    #[test]
    fn a_legacy_colon_token_keeps_its_meaning() {
        let admins = table("alice:abc:grid-admin\n");
        let alice = admins
            .resolve("abc:grid-admin")
            .expect("the whole remainder is the token");
        assert_eq!(alice.name, "alice");
        assert_eq!(alice.roles, vec![Role::GridAdmin]);
        assert!(admins.resolve("abc").is_none(), "the prefix is not a credential");
    }

    #[test]
    fn a_legacy_line_never_gains_a_role() {
        let admins = table("alice:abc:enrollment-admin\n");
        let alice = admins
            .resolve("abc:enrollment-admin")
            .expect("the whole remainder is the token");
        assert_eq!(alice.roles, vec![Role::GridAdmin], "a token cannot name its own role");
    }

    #[test]
    fn roles_come_only_from_the_role_table() {
        let admins = table("bob:t\n").with_roles("bob:enrollment-admin\n").expect("roles");
        assert_eq!(admins.resolve("t").expect("bob").roles, vec![Role::EnrollmentAdmin]);

        let orphan = table("bob:t\n").with_roles("carol:grid-admin\n");
        assert!(
            matches!(&orphan, Err(TableError::UnknownAdmin { line: 1, name }) if name == "carol"),
            "a role line with no token is refused: {orphan:?}"
        );
    }

    #[test]
    fn a_name_listed_twice_in_the_role_table_is_refused() {
        let parsed = table("bob:t\n").with_roles("bob:grid-admin\nbob:enrollment-admin\n");
        assert!(
            matches!(&parsed, Err(TableError::DuplicateRoles { line: 2, name }) if name == "bob"),
            "the second line is refused rather than winning: {parsed:?}"
        );
    }

    #[test]
    fn a_configured_token_resolves_to_its_grid_admin() {
        let admins = table("alice: s3cret\n");
        assert_eq!(
            admins.resolve("s3cret").map(|admin| admin.name.as_str()),
            Some("alice"),
            "a configured token resolves to its grid-admin"
        );
        assert!(admins.resolve("wrong").is_none(), "an unknown token resolves to nobody");
    }

    #[test]
    fn a_table_reads_every_configured_grid_admin() {
        let admins = table("# admins\n\nalice: one\nbob: two\n\n");
        assert_eq!(admins.len(), 2, "two grid-admins should be read");
        assert_eq!(
            admins.resolve("two").map(|admin| admin.name.as_str()),
            Some("bob"),
            "bob's token should resolve"
        );
    }

    #[test]
    fn malformed_lines_are_skipped() {
        let admins = table("nameless\nalice:\n: token\n");
        assert!(admins.is_empty(), "no usable grid-admin should be read");
        assert!(admins.resolve("").is_none(), "an empty token must not resolve");
    }

    #[test]
    fn no_configured_grid_admin_means_nobody_can_mint() {
        let admins = table("");
        assert!(admins.is_empty(), "an empty table configures nobody");
    }

    #[test]
    fn the_debug_view_shows_names_not_tokens() {
        let admins = table("alice: s3cret\n");
        let debug = format!("{admins:?}");
        let expected = digest("s3cret");
        assert!(!debug.contains("s3cret"), "the token itself is not in the debug view");
        assert!(debug.contains(&expected), "the digest is what is held");
        assert!(debug.contains("alice"), "the grid-admin name is not a secret");
    }

    #[test]
    fn a_token_is_held_as_its_digest() {
        let admins = table("alice: s3cret\n");
        let expected = digest("s3cret");
        assert!(
            admins.resolve(&expected).is_none(),
            "the digest is not itself a valid token"
        );
    }

    fn roles(admins: &GridAdmins, token: &str) -> Vec<Role> {
        admins
            .resolve(token)
            .map(|admin| admin.roles.clone())
            .unwrap_or_default()
    }

    #[test]
    fn an_entry_without_roles_is_a_grid_admin_only() {
        let admins = table("alice:one\nbob:two\ncarol:three\n")
            .with_roles("bob:grid-admin,enrollment-admin\ncarol:enrollment-admin\n")
            .expect("roles");
        assert_eq!(roles(&admins, "one"), [Role::GridAdmin], "unlisted keeps the default");
        assert_eq!(roles(&admins, "two"), [Role::GridAdmin, Role::EnrollmentAdmin]);
        assert_eq!(roles(&admins, "three"), [Role::EnrollmentAdmin]);
    }

    #[test]
    fn whitespace_around_roles_is_trimmed() {
        let admins = table("alice: one \n")
            .with_roles(" alice : grid-admin , enrollment-admin \n")
            .expect("roles");
        assert_eq!(roles(&admins, "one"), [Role::GridAdmin, Role::EnrollmentAdmin]);
    }

    #[test]
    fn a_bad_role_list_fails_the_whole_table() {
        for list in [
            "root",
            "Grid-Admin",
            "ENROLLMENT-ADMIN",
            "grid admin",
            "",
            " ",
            ",",
            "grid-admin,",
            "grid-admin,,enrollment-admin",
            "grid-admin,grid-admin",
            "enrollment-admin, enrollment-admin",
            "grid-admin;enrollment-admin",
            "grid-admin:enrollment-admin",
            "*",
        ] {
            let parsed = table("bob:two\nalice:one\n").with_roles(&format!("bob:grid-admin\nalice:{list}\n"));
            assert!(
                matches!(&parsed, Err(TableError::Roles { line: 2, name }) if name == "alice"),
                "{list:?} must fail, got {parsed:?}"
            );
        }
    }

    #[test]
    fn malformed_role_lines_fail_instead_of_leaving_default_privileges() {
        for line in ["carol enrollment-admin", ":enrollment-admin", " : enrollment-admin"] {
            let parsed = table("carol:token\n").with_roles(&format!("# restricted admin\n\n{line}\n"));
            assert!(
                matches!(&parsed, Err(TableError::MalformedRoles { line: 3 })),
                "{line:?} must fail, got {parsed:?}"
            );
        }
        let admins = table("carol:token\n")
            .with_roles("# restricted admin\n\ncarol:enrollment-admin\n")
            .expect("comments and blank lines remain valid");
        assert_eq!(roles(&admins, "token"), [Role::EnrollmentAdmin]);
    }

    #[test]
    fn a_role_error_does_not_echo_a_token() {
        let parsed = table("alice:se:cret-tail\n").with_roles("alice:root\n");
        let message = parsed.expect_err("an unknown role is refused").to_string();
        assert!(
            !message.contains("cret-tail"),
            "the token's tail is not echoed: {message}"
        );
    }

    #[test]
    fn a_token_that_spells_a_role_does_not_escalate_silently() {
        let admins = table("alice:x:enrollment-admin\n");
        let alice = admins
            .resolve("x:enrollment-admin")
            .expect("the whole remainder is the token");
        assert_eq!(
            alice.roles,
            vec![Role::GridAdmin],
            "a token spelling a role does not grant it"
        );
        assert!(admins.resolve("x").is_none(), "the prefix is not a credential");
    }
}
