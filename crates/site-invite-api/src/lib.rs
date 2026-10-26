//! Cluster-scoped invitation API on the signer cluster.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An invitation that authorizes one site to request admission.
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis.fast",
    version = "v1alpha1",
    kind = "SiteInvite",
    plural = "siteinvites",
    status = "SiteInviteStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Site","type":"string","jsonPath":".spec.siteName"}"#,
    printcolumn = r#"{"name":"Expires","type":"date","jsonPath":".status.expiresAt"}"#,
    printcolumn = r#"{"name":"Available","type":"string","jsonPath":".status.conditions[?(@.type=='Available')].status"}"#,
    printcolumn = r#"{"name":"Progressing","type":"string","jsonPath":".status.conditions[?(@.type=='Progressing')].status"}"#,
    printcolumn = r#"{"name":"Degraded","type":"string","jsonPath":".status.conditions[?(@.type=='Degraded')].status"}"#,
    printcolumn = r#"{"name":"Since","type":"date","jsonPath":".status.conditions[?(@.type=='Available')].lastTransitionTime"}"#,
    printcolumn = r#"{"name":"Message","type":"string","jsonPath":".status.conditions[?(@.type=='Available')].message","priority":1}"#
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schemars(extend("x-kubernetes-validations" = [{
    "rule": "self == oldSelf",
    "message": "SiteInvite spec is immutable; delete and recreate it to change an invitation"
}, {
    "rule": "self.jurisdictions.all(j, j.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'))",
    "message": "each jurisdiction must be a DNS label"
}, {
    "rule": "self.lifetime >= duration('5m') && self.lifetime <= duration('168h')",
    "message": "lifetime must be between 5m and 168h"
}]))]
pub struct SiteInviteSpec {
    /// Site name pinned by the signer.
    #[schemars(regex(pattern = "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"), length(max = 63))]
    pub site_name: String,

    /// Jurisdiction labels assigned to the site.
    #[schemars(length(min = 1, max = 8), inner(length(max = 63)))]
    #[schemars(extend("x-kubernetes-list-type" = "set"))]
    pub jurisdictions: Vec<String>,

    /// Optional SHA-256 digest of the bound public key.
    #[serde(rename = "publicKeySHA256", default, skip_serializing_if = "Option::is_none")]
    #[schemars(rename = "publicKeySHA256")]
    #[schemars(regex(pattern = "^[0-9a-f]{64}$"))]
    pub public_key_sha256: Option<String>,

    /// Optional lifetime, from 5m through 168h.
    #[serde(default = "default_invite_lifetime")]
    #[schemars(default = "default_invite_lifetime")]
    #[schemars(extend("format" = "duration"))]
    #[schemars(regex(pattern = "^([0-9]+(ns|us|µs|ms|s|m|h))+$"))]
    pub lifetime: String,

    /// Whether a deleted site name may enroll again.
    #[serde(default)]
    #[schemars(default)]
    pub allow_deleted_name: bool,
}

/// Default lifetime.
fn default_invite_lifetime() -> String {
    "24h".to_owned()
}

/// Observed invitation state.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SiteInviteStatus {
    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Public token identifier; the token secret is never stored here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,

    /// Secret containing the one-time token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<InviteSecretReference>,

    /// Token expiry in RFC 3339 format.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("format" = "date-time"))]
    pub expires_at: Option<String>,

    /// CSR name that redeemed this invitation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redeemed_by: Option<String>,

    /// Public key digest accepted by the signer.
    #[serde(rename = "publicKeySHA256", default, skip_serializing_if = "Option::is_none")]
    #[schemars(rename = "publicKeySHA256")]
    pub public_key_sha256: Option<String>,

    /// Standard condition-shaped lifecycle status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["type"] ))]
    pub conditions: Vec<SiteInviteCondition>,
}

/// Secret reference containing the token.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InviteSecretReference {
    /// Secret namespace.
    pub namespace: String,
    /// Secret name.
    pub name: String,
    /// Data key holding the token.
    pub key: String,
}

/// A `SiteInvite` status condition.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SiteInviteCondition {
    /// Condition type.
    #[serde(rename = "type")]
    pub type_: String,
    /// Condition status.
    #[schemars(regex(pattern = "^(True|False|Unknown)$"))]
    pub status: String,
    /// Stable machine-readable reason.
    pub reason: String,
    /// Human-readable diagnostic.
    pub message: String,
    /// Generation this condition describes.
    pub observed_generation: i64,
    /// RFC 3339 time of the last transition.
    #[schemars(extend("format" = "date-time"))]
    pub last_transition_time: String,
}

#[cfg(test)]
mod tests {
    use super::{SiteInviteSpec, SiteInviteStatus};

    #[test]
    fn public_key_hash_uses_the_documented_json_name() -> Result<(), serde_json::Error> {
        let spec = serde_json::to_value(SiteInviteSpec {
            site_name: "west".to_owned(),
            jurisdictions: vec!["eu".to_owned()],
            public_key_sha256: Some("a".repeat(64)),
            lifetime: "24h".to_owned(),
            allow_deleted_name: false,
        })?;
        let status = serde_json::to_value(SiteInviteStatus {
            public_key_sha256: Some("a".repeat(64)),
            ..SiteInviteStatus::default()
        })?;

        assert!(spec.get("publicKeySHA256").is_some());
        assert!(spec.get("publicKeySha256").is_none());
        assert!(status.get("publicKeySHA256").is_some());
        assert!(status.get("publicKeySha256").is_none());
        Ok(())
    }
}
