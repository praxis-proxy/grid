//! The `LocalSiteDeclared` condition: whether this site's labelled `GridSite` exists.

use serde_json::Value;

use crate::crd::{grid_network::GridNetwork, grid_site::GridSite, inference_provider::Condition};

/// The condition's type.
pub const LOCAL_SITE_DECLARED: &str = "LocalSiteDeclared";

/// The label providers select their site by.
pub const PROVIDER_SITE_LABEL: &str = "grid.praxis.fast/provider-site";

/// Server-side apply manager, apart from `GridNetwork` reconciliation.
pub const FIELD_MANAGER: &str = "grid-operator-local-site";

/// What the local `GridSite` lookup found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Declared {
    /// This site has no name yet: no identity and no configured name.
    NoSite,
    /// No `GridSite` with this site's name is in the network.
    Missing,
    /// The record exists without the provider-site label naming this site.
    Unlabelled,
    /// The record exists and carries the label.
    Labelled,
}

impl Declared {
    /// Judge `site`, the `GridSite` named for this site, if any.
    #[must_use]
    pub fn of(network: &str, local_site: Option<&str>, site: Option<&GridSite>) -> Self {
        let Some(local) = local_site else {
            return Self::NoSite;
        };
        match site.filter(|site| site.spec.grid_network_ref == network) {
            None => Self::Missing,
            Some(site) => {
                let labelled = site
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get(PROVIDER_SITE_LABEL))
                    .is_some_and(|value| value == local);
                if labelled { Self::Labelled } else { Self::Unlabelled }
            },
        }
    }

    /// The status, reason and message the condition carries.
    fn verdict(self, network: &str, local_site: Option<&str>) -> (&'static str, &'static str, String) {
        let site = local_site.unwrap_or_default();
        match self {
            Self::NoSite => (
                "False",
                "NoSiteIdentity",
                "this site has no identity yet: create or join a grid to name it".to_owned(),
            ),
            Self::Missing => (
                "False",
                "LocalSiteMissing",
                format!(
                    "no GridSite {site:?} in GridNetwork {network:?}: declare it, labelled \
                     {PROVIDER_SITE_LABEL}={site}, so this site's providers match it"
                ),
            ),
            Self::Unlabelled => (
                "False",
                "LocalSiteUnlabelled",
                format!(
                    "GridSite {site:?} lacks {PROVIDER_SITE_LABEL}={site}: providers that select this site match \
                     nothing"
                ),
            ),
            Self::Labelled => (
                "True",
                "LocalSiteFound",
                format!("GridSite {site:?} is declared and labelled"),
            ),
        }
    }
}

/// The condition `declared` calls for, keeping the held `lastTransitionTime` while its
/// status stands.
#[must_use]
pub fn desired(
    network: &GridNetwork,
    local_site: Option<&str>,
    declared: Declared,
    now_rfc3339: &str,
) -> Option<Condition> {
    let verdict = declared.verdict(network.metadata.name.as_deref()?, local_site);
    let stamp = crate::rollup::Stamp {
        generation: network.metadata.generation,
        now: now_rfc3339,
    };
    Some(crate::rollup::condition(
        held(network),
        LOCAL_SITE_DECLARED,
        verdict,
        stamp,
    ))
}

/// The condition for `declared`, or `None` when the live status already says it.
#[must_use]
pub fn condition(
    network: &GridNetwork,
    local_site: Option<&str>,
    declared: Declared,
    now_rfc3339: &str,
) -> Option<Condition> {
    let want = desired(network, local_site, declared, now_rfc3339)?;
    crate::rollup::differs(held(network), std::slice::from_ref(&want)).then_some(want)
}

/// The conditions the live status holds.
fn held(network: &GridNetwork) -> &[Condition] {
    network.status.as_ref().map_or(&[], |live| live.conditions.as_slice())
}

/// The status patch carrying the condition, or `None` when the live status already says it.
#[must_use]
pub fn patch<'network>(
    network: &'network GridNetwork,
    local_site: Option<&str>,
    declared: Declared,
    now_rfc3339: &str,
) -> Option<(&'network str, Value)> {
    let name = network.metadata.name.as_deref()?;
    let condition = condition(network, local_site, declared, now_rfc3339)?;
    Some((
        name,
        serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": name },
            "status": { "conditions": [condition] }
        }),
    ))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::crd::{grid_network::GridNetworkStatus, grid_site::GridSiteSpec};

    fn site(name: &str, network: &str, label: Option<&str>) -> GridSite {
        let spec: GridSiteSpec =
            serde_json::from_value(serde_json::json!({ "gridNetworkRef": network })).expect("spec");
        let mut site = GridSite::new(name, spec);
        site.metadata.labels = label.map(|value| BTreeMap::from([(PROVIDER_SITE_LABEL.to_owned(), value.to_owned())]));
        site
    }

    #[test]
    fn a_site_without_a_name_has_no_identity() {
        assert_eq!(Declared::of("grid", None, None), Declared::NoSite);
    }

    #[test]
    fn a_site_with_no_record_is_missing() {
        assert_eq!(Declared::of("grid", Some("east"), None), Declared::Missing);
    }

    #[test]
    fn a_record_in_another_network_is_missing() {
        let other = site("east", "other", Some("east"));
        assert_eq!(Declared::of("grid", Some("east"), Some(&other)), Declared::Missing);
    }

    #[test]
    fn a_record_without_the_label_naming_this_site_is_unlabelled() {
        for label in [None, Some("west")] {
            let record = site("east", "grid", label);
            assert_eq!(
                Declared::of("grid", Some("east"), Some(&record)),
                Declared::Unlabelled,
                "{label:?}"
            );
        }
    }

    #[test]
    fn a_labelled_record_is_found() {
        let record = site("east", "grid", Some("east"));
        assert_eq!(Declared::of("grid", Some("east"), Some(&record)), Declared::Labelled);
    }

    #[test]
    fn a_missing_record_names_the_site_and_the_label_to_declare() {
        let network = GridNetwork::new("grid", serde_json::from_value(serde_json::json!({})).expect("spec"));
        let (_name, patch) = patch(&network, Some("east"), Declared::Missing, "2026-10-18T00:00:00Z").expect("patch");
        let field = |key: &str| {
            patch
                .pointer(&format!("/status/conditions/0/{key}"))
                .and_then(Value::as_str)
        };
        assert_eq!(field("status"), Some("False"));
        assert_eq!(field("reason"), Some("LocalSiteMissing"));
        let message = field("message").expect("message");
        assert!(
            message.contains("\"east\"") && message.contains("grid.praxis.fast/provider-site=east"),
            "{message}"
        );
    }

    #[test]
    fn an_unchanged_condition_is_not_rewritten() {
        let mut network = GridNetwork::new("grid", serde_json::from_value(serde_json::json!({})).expect("spec"));
        let (_name, first) = patch(&network, Some("east"), Declared::Labelled, "2026-10-18T00:00:00Z").expect("patch");
        let held: Condition =
            serde_json::from_value(first.pointer("/status/conditions/0").cloned().expect("condition"))
                .expect("condition");
        network.status = Some(GridNetworkStatus {
            conditions: vec![held],
            ..Default::default()
        });
        assert!(patch(&network, Some("east"), Declared::Labelled, "2026-10-18T01:00:00Z").is_none());
        let changed = patch(&network, Some("east"), Declared::Missing, "2026-10-18T01:00:00Z").expect("patch");
        assert_eq!(
            changed
                .1
                .pointer("/status/conditions/0/lastTransitionTime")
                .and_then(Value::as_str),
            Some("2026-10-18T01:00:00Z")
        );
    }
}
