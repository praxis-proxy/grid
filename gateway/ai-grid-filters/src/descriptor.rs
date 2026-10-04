//! Static site/capability descriptor model for gateway-to-gateway routing.
//!
//! Defines the local routing records the `grid_site_route` filter consumes.
//! Records are validated at parse time and read immutably during a request.
//! Data model only. Selection and ordering live in the `route` and `snapshot`
//! siblings.

use std::{collections::HashSet, sync::Arc};

use praxis_filter::FilterError;
use serde::Deserialize;

use crate::metadata::{CandidateCredential, STRATEGY_BEARER_TOKEN};

/// Maximum number of route candidates.
const MAX_CANDIDATES: usize = 1024;

/// Maximum length for identifier strings.
const MAX_NAME_LEN: usize = 256;

/// Header prefixes reserved for internal gateway/protocol metadata.
pub(crate) const RESERVED_HEADER_PREFIXES: &[&str] = &["x-praxis-", "x-mcp-"];

/// Capability kind for descriptor matching.
///
/// Categorises what a route candidate offers. `InferenceModel` is matched by
/// the model request header. `McpTool` is matched by MCP metadata, which takes
/// precedence over the model header.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKind {
    /// OpenAI-compatible inference model.
    InferenceModel,

    /// MCP tool, matched by `mcp.method`=`tools/call` + `mcp.name` metadata.
    McpTool,
}

impl CapabilityKind {
    /// Short string for diagnostics and route metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InferenceModel => "inference_model",
            Self::McpTool => "mcp_tool",
        }
    }
}

/// Grid-operator-assigned admission state for a routing candidate.
///
/// Controls whether a candidate accepts new sessions, existing sessions only,
/// or is excluded from routing entirely.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AdmissionState {
    /// Accepts both new and existing sessions.
    #[default]
    NewAndExisting,

    /// Accepts only existing sessions (bound via session affinity).
    ExistingOnly,

    /// Excluded from routing entirely.
    Excluded,
}

impl AdmissionState {
    /// Short string for metadata output.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewAndExisting => "new_and_existing",
            Self::ExistingOnly => "existing_only",
            Self::Excluded => "none",
        }
    }
}

/// A single route candidate as written in YAML config.
///
/// ```yaml
/// candidates:
///   - kind: inference_model
///     name: llama-3.1-8b
///     site: site-b
///     cluster: grid-site-b
/// ```
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CandidateConfig {
    /// Cluster name to select when this candidate is chosen.
    pub cluster: String,

    /// Optional final-hop credential reference.
    #[serde(default)]
    pub credential: Option<CandidateCredential>,

    /// Whether this candidate is fresh (default: `true`).
    #[serde(default = "default_fresh")]
    pub fresh: bool,

    /// Capability kind.
    pub kind: CapabilityKind,

    /// Capability name (model name, tool name, or agent name).
    pub name: String,

    /// Site that owns this capability.
    pub site: String,
}

/// Default freshness state for candidates.
fn default_fresh() -> bool {
    true
}

/// A validated route candidate ready for runtime matching.
///
/// Created by `validate_candidates` from raw config entries. All string
/// fields are bounded and non-blank. The Grid-owned fields (`admission_state`,
/// `rank`, `selection_tier`) are populated after validation from live signals.
///
/// `Clone` is cheap: every owned field is an `Arc<str>`. The refresh step clones
/// the base set each poll cycle to re-order it by live load.
#[derive(Clone, Debug)]
pub struct RouteCandidate {
    /// Grid-operator admission state.
    pub admission_state: AdmissionState,

    /// Cluster name to select.
    pub cluster: Arc<str>,

    /// Optional final-hop credential reference.
    pub credential: Option<CandidateCredential>,

    /// Whether this candidate is fresh. Preserved from config, but Grid owns
    /// freshness ordering.
    pub fresh: bool,

    /// Capability kind.
    pub kind: CapabilityKind,

    /// Capability name.
    pub name: Arc<str>,

    /// Grid-operator rank within the snapshot (lower is better).
    pub rank: Option<u32>,

    /// Grid-operator locality tier (e.g. `"same_region"`).
    pub selection_tier: Option<Arc<str>>,

    /// Site that owns this capability.
    pub site: Arc<str>,

    /// Deterministic identifier for session affinity binding.
    pub stable_id: Arc<str>,
}

/// Build a deterministic stable ID from candidate identity fields.
pub(crate) fn default_stable_id(kind: CapabilityKind, name: &str, site: &str, cluster: &str) -> Arc<str> {
    Arc::from(format!("{}/{name}/{site}/{cluster}", kind.as_str()))
}

/// Validate and build the candidate list from raw config entries.
///
/// # Errors
///
/// Returns [`FilterError`] if the list is empty or exceeds [`MAX_CANDIDATES`],
/// any name/site/cluster field is blank or oversized, or a duplicate
/// (kind, name, site, cluster) tuple exists.
#[expect(
    clippy::too_many_lines,
    reason = "single validation loop, splitting hurts readability"
)]
pub(crate) fn validate_candidates(raw: Vec<CandidateConfig>) -> Result<Vec<RouteCandidate>, FilterError> {
    if raw.is_empty() {
        return Err("grid: candidates list must not be empty".into());
    }
    if raw.len() > MAX_CANDIDATES {
        return Err(format!("grid: candidates exceeds maximum of {MAX_CANDIDATES}").into());
    }

    let mut candidates = Vec::with_capacity(raw.len());
    let mut seen: HashSet<(CapabilityKind, String, String, String)> = HashSet::with_capacity(raw.len());

    for (index, cand) in raw.into_iter().enumerate() {
        validate_name(&format!("candidates[{index}].name"), &cand.name)?;
        validate_name(&format!("candidates[{index}].site"), &cand.site)?;
        validate_name(&format!("candidates[{index}].cluster"), &cand.cluster)?;
        validate_credential(index, cand.credential.as_ref())?;

        if !seen.insert((cand.kind, cand.name.clone(), cand.site.clone(), cand.cluster.clone())) {
            return Err(format!(
                "grid: duplicate candidate '{}/{}/{}/{}'",
                cand.kind.as_str(),
                cand.name,
                cand.site,
                cand.cluster
            )
            .into());
        }

        let stable_id = default_stable_id(cand.kind, &cand.name, &cand.site, &cand.cluster);
        candidates.push(RouteCandidate {
            admission_state: AdmissionState::default(),
            cluster: Arc::from(cand.cluster.as_str()),
            credential: cand.credential,
            fresh: cand.fresh,
            kind: cand.kind,
            name: Arc::from(cand.name.as_str()),
            rank: None,
            selection_tier: None,
            site: Arc::from(cand.site.as_str()),
            stable_id,
        });
    }

    Ok(candidates)
}

/// Validate credential reference fields on a candidate entry.
fn validate_credential(index: usize, credential: Option<&CandidateCredential>) -> Result<(), FilterError> {
    let Some(credential) = credential else {
        return Ok(());
    };
    if credential.strategy != STRATEGY_BEARER_TOKEN {
        return Err(format!("grid: candidates[{index}].credential.strategy is unsupported").into());
    }
    validate_name(
        &format!("candidates[{index}].credential.secretRef.name"),
        &credential.secret_ref.name,
    )?;
    validate_name(
        &format!("candidates[{index}].credential.secretRef.namespace"),
        &credential.secret_ref.namespace,
    )?;
    validate_name(
        &format!("candidates[{index}].credential.secretRef.key"),
        &credential.secret_ref.key,
    )
}

/// Validate the promoted model header name.
///
/// Rejects blank, unparseable, or reserved-prefix header names.
///
/// # Errors
///
/// Returns [`FilterError`] for a blank, unparseable, or reserved-prefix name.
pub(crate) fn validate_model_header(raw: &str) -> Result<http::header::HeaderName, FilterError> {
    if raw.trim().is_empty() {
        return Err("grid: model_header must not be empty".into());
    }
    let header: http::header::HeaderName = raw
        .parse()
        .map_err(|err| -> FilterError { format!("grid: invalid model_header: {err}").into() })?;
    if RESERVED_HEADER_PREFIXES
        .iter()
        .any(|prefix| header.as_str().starts_with(prefix))
    {
        return Err("grid: model_header must not use a reserved internal header prefix".into());
    }
    Ok(header)
}

/// Validate a local site identifier.
///
/// # Errors
///
/// Returns [`FilterError`] if blank or oversized.
pub(crate) fn validate_local_site(value: &str) -> Result<(), FilterError> {
    validate_name("local_site", value)
}

/// Validate a bounded, non-blank identifier.
fn validate_name(field: &str, value: &str) -> Result<(), FilterError> {
    if value.trim().is_empty() || value.len() > MAX_NAME_LEN {
        return Err(format!("grid: {field} must be 1-{MAX_NAME_LEN} non-blank characters").into());
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::min_ident_chars,
    reason = "tests"
)]
mod tests {
    use super::*;

    fn candidate(kind_str: &str, name: &str, site: &str, cluster: &str) -> CandidateConfig {
        let kind: CapabilityKind = serde_yaml::from_str(&format!("\"{kind_str}\"")).unwrap();
        CandidateConfig {
            cluster: cluster.to_owned(),
            credential: None,
            fresh: true,
            kind,
            name: name.to_owned(),
            site: site.to_owned(),
        }
    }

    #[test]
    fn valid_minimal_inference_candidate() {
        let candidates =
            validate_candidates(vec![candidate("inference_model", "llama", "site-a", "gateway-a")]).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].kind, CapabilityKind::InferenceModel);
        assert_eq!(&*candidates[0].name, "llama");
        assert_eq!(&*candidates[0].site, "site-a");
        assert_eq!(&*candidates[0].cluster, "gateway-a");
        assert!(candidates[0].fresh);
    }

    #[test]
    fn stale_candidate_preserved() {
        let mut c = candidate("inference_model", "m", "s", "c");
        c.fresh = false;
        let candidates = validate_candidates(vec![c]).unwrap();
        assert!(!candidates[0].fresh);
    }

    #[test]
    fn empty_candidates_rejected() {
        let err = validate_candidates(vec![]).expect_err("should fail");
        assert!(err.to_string().contains("must not be empty"), "{err}");
    }

    #[test]
    fn blank_name_rejected() {
        let err = validate_candidates(vec![candidate("inference_model", "", "s", "c")]).expect_err("should fail");
        assert!(err.to_string().contains("name must be"), "{err}");
    }

    #[test]
    fn oversized_name_rejected() {
        let long = "a".repeat(MAX_NAME_LEN + 1);
        let err = validate_candidates(vec![candidate("inference_model", &long, "s", "c")]).expect_err("should fail");
        assert!(err.to_string().contains("name must be"), "{err}");
    }

    #[test]
    fn duplicate_candidate_rejected() {
        let err = validate_candidates(vec![
            candidate("inference_model", "llama", "site-a", "c1"),
            candidate("inference_model", "llama", "site-a", "c1"),
        ])
        .expect_err("should fail");
        assert!(err.to_string().contains("duplicate candidate"), "{err}");
    }

    #[test]
    fn same_model_different_site_not_duplicate() {
        validate_candidates(vec![
            candidate("inference_model", "llama", "site-a", "c1"),
            candidate("inference_model", "llama", "site-b", "c2"),
        ])
        .expect("same model on different sites is not a duplicate");
    }

    #[test]
    fn deny_unknown_fields_on_candidate() {
        let yaml = "- kind: inference_model\n  name: x\n  site: s\n  cluster: c\n  extra: bad";
        let err: Result<Vec<CandidateConfig>, _> = serde_yaml::from_str(yaml);
        assert!(err.is_err(), "unknown fields should be rejected");
    }

    #[test]
    fn valid_model_header() {
        validate_model_header("X-Model").expect("X-Model is a valid model header");
    }

    #[test]
    fn reserved_prefix_model_header_rejected() {
        let err = validate_model_header("x-praxis-model").expect_err("should fail");
        assert!(err.to_string().contains("reserved"), "{err}");
    }

    #[test]
    fn blank_local_site_rejected() {
        let err = validate_local_site("").expect_err("should fail");
        assert!(err.to_string().contains("local_site must be"), "{err}");
    }
}
