//! `GridOperator`: this cluster's operator health, a cluster-scoped singleton named `cluster`.
#![expect(unreachable_pub, reason = "KubeSchema re-declares the spec in a private module")]

use kube::{CustomResource, KubeSchema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The only `GridOperator` name the API server accepts.
pub const GRID_OPERATOR_NAME: &str = "cluster";

/// Specification for the [`GridOperator`].
#[derive(Clone, CustomResource, Debug, Default, Deserialize, KubeSchema, PartialEq, Serialize)]
#[kube(
    group = "grid.praxis.fast",
    version = "v1alpha1",
    kind = "GridOperator",
    plural = "gridoperators",
    shortname = "gop",
    category = "grid",
    status = "GridOperatorStatus",
    namespaced = false,
    validation = Rule::new("self.metadata.name == 'cluster'").message("the GridOperator is a singleton named cluster"),
    printcolumn = r#"{"name":"Available","type":"string","jsonPath":".status.conditions[?(@.type==\"Available\")].status"}"#,
    printcolumn = r#"{"name":"Progressing","type":"string","jsonPath":".status.conditions[?(@.type==\"Progressing\")].status"}"#,
    printcolumn = r#"{"name":"Degraded","type":"string","jsonPath":".status.conditions[?(@.type==\"Degraded\")].status"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct GridOperatorSpec {
    /// `Unmanaged` stops every grid controller from reconciling until set back to `Managed`.
    #[serde(default)]
    pub management_state: ManagementState,

    /// Operator log verbosity, applied without a restart.
    #[serde(default)]
    pub operator_log_level: OperatorLogLevel,
}

/// Whether the operator reconciles grid objects.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ManagementState {
    /// Reconcile normally.
    #[default]
    Managed,
    /// Stop reconciling and touch nothing.
    Unmanaged,
}

/// Operator log verbosity.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum OperatorLogLevel {
    /// The startup `RUST_LOG` filter.
    #[default]
    Normal,
    /// `debug`.
    Debug,
    /// `trace`.
    Trace,
    /// `trace`, kept for parity with the `OpenShift` operator levels.
    TraceAll,
}

impl OperatorLogLevel {
    /// The `EnvFilter` directive for this level, `None` for the startup filter.
    #[must_use]
    pub const fn directive(self) -> Option<&'static str> {
        match self {
            Self::Normal => None,
            Self::Debug => Some("debug"),
            Self::Trace | Self::TraceAll => Some("trace"),
        }
    }
}

/// Observed state of this cluster's grid operator.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GridOperatorStatus {
    /// `GridSites*`, `Providers*`, `GatewayConfig*`, and `SiteCertificateDegraded`, plus `Available`,
    /// `Progressing`, and `Degraded` derived from them by type suffix.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["type"]))]
    pub conditions: Vec<super::condition::Condition>,
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn the_crd_is_a_cluster_scoped_singleton_with_three_condition_columns() {
        let crd = serde_json::to_value(GridOperator::crd()).unwrap();
        assert_eq!(crd.pointer("/spec/scope"), Some(&serde_json::json!("Cluster")));
        let rules = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/x-kubernetes-validations")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        assert!(
            rules
                .iter()
                .any(|rule| rule["rule"] == "self.metadata.name == 'cluster'")
        );
        let columns: Vec<&str> = crd
            .pointer("/spec/versions/0/additionalPrinterColumns")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .filter_map(|column| column["name"].as_str())
            .collect();
        assert_eq!(columns, ["Available", "Progressing", "Degraded"]);
    }

    #[test]
    fn removed_is_not_a_management_state_yet() {
        let crd = serde_json::to_value(GridOperator::crd()).unwrap();
        let states = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/managementState/enum")
            .unwrap();
        assert_eq!(states, &serde_json::json!(["Managed", "Unmanaged"]));
        assert_eq!(OperatorLogLevel::Normal.directive(), None);
        assert_eq!(OperatorLogLevel::TraceAll.directive(), Some("trace"));
    }
}
