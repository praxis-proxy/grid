//! Bridge from operator-produced [`RoutingOverlay`] values to Praxis
//! `grid_route` filter configuration.
//!
//! The [`RoutingOverlay`] JSON is already shaped identically to the
//! Praxis `grid_route` static candidate config; this module provides
//! a thin conversion layer for validation and Praxis config generation.
//!
//! # Conversion contract
//!
//! Input: `RoutingOverlay` (from operator ConfigMap `grid-config.json`).
//! Output: `serde_json::Value` representing the `grid_route` filter stanza.
//!
//! The mapping is 1:1 because the overlay shape was designed to match
//! `grid_route` config:
//! - `overlay.local_site` → `local_site`
//! - `overlay.candidates[].kind` → `candidates[].kind`
//! - `overlay.candidates[].name` → `candidates[].name`
//! - `overlay.candidates[].site` → `candidates[].site`
//! - `overlay.candidates[].cluster` → `candidates[].cluster`
//! - `overlay.candidates[].fresh` → `candidates[].fresh`
//!
//! # Local validation and config-generation path
//!
//! 1. Operator produces `ConfigMap` with `grid-config.json`.
//! 2. A local validation or config-generation tool reads the ConfigMap and extracts `grid-config.json`.
//! 3. Deserialise JSON into [`RoutingOverlay`] with `serde_json::from_str`.
//! 4. Call `to_grid_route_value` to get the Praxis filter stanza as a `serde_json::Value`.
//! 5. Serialize to JSON or YAML for embedding in the Praxis config file.
//!
//! JSON is valid YAML, so the `serde_json::Value` can be serialised with
//! `serde_json::to_string_pretty` and embedded directly in a YAML file.
//!
//! [`RoutingOverlay`]: crate::resources::routing_overlay::RoutingOverlay

use crate::resources::routing_overlay::{RoutingCandidate, RoutingOverlay};

/// Convert a [`RoutingOverlay`] into a Praxis `grid_route` filter stanza
/// as a `serde_json::Value`.
///
/// The returned value is suitable for serialising to JSON or YAML for
/// embedding in a Praxis filter chain configuration.  This function is
/// infallible; all fields in [`RoutingOverlay`] and [`RoutingCandidate`]
/// are plain JSON-serialisable types.
///
/// [`RoutingCandidate`]: crate::resources::routing_overlay::RoutingCandidate
pub fn to_grid_route_value(overlay: &RoutingOverlay, model_header: &str) -> serde_json::Value {
    let candidates: Vec<serde_json::Value> = overlay.candidates.iter().map(candidate_to_value).collect();

    serde_json::json!({
        "filter": "grid_route",
        "local_site": overlay.local_site,
        "model_header": model_header,
        "candidates": candidates
    })
}

/// Serialise one [`RoutingCandidate`] to a `serde_json::Value`.
///
/// [`RoutingCandidate`]: crate::resources::routing_overlay::RoutingCandidate
fn candidate_to_value(c: &RoutingCandidate) -> serde_json::Value {
    serde_json::json!({
        "kind": c.kind,
        "name": c.name,
        "site": c.site,
        "cluster": c.cluster,
        "fresh": c.fresh
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::routing_overlay::{RoutingCandidate, RoutingOverlay};

    fn make_overlay(candidates: Vec<(&str, &str, &str)>) -> RoutingOverlay {
        RoutingOverlay {
            network: "test-net".to_owned(),
            local_site: "site-a".to_owned(),
            candidates: candidates
                .into_iter()
                .map(|(name, site, cluster)| RoutingCandidate {
                    kind: "inference_model".to_owned(),
                    name: name.to_owned(),
                    site: site.to_owned(),
                    cluster: cluster.to_owned(),
                    fresh: true,
                    credential: None,
                })
                .collect(),
        }
    }

    #[test]
    fn empty_overlay_produces_valid_value() {
        let overlay = make_overlay(vec![]);
        let value = to_grid_route_value(&overlay, "X-Model");
        assert_eq!(
            value.get("filter").and_then(serde_json::Value::as_str),
            Some("grid_route"),
            "filter must be grid_route"
        );
        assert_eq!(
            value.get("local_site").and_then(serde_json::Value::as_str),
            Some("site-a"),
            "local_site must match overlay"
        );
        assert_eq!(
            value.get("model_header").and_then(serde_json::Value::as_str),
            Some("X-Model"),
            "model_header must be X-Model"
        );
        assert_eq!(
            value
                .get("candidates")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(0),
            "empty overlay must have empty candidates"
        );
    }

    #[test]
    fn single_candidate_appears_in_value() {
        let overlay = make_overlay(vec![("granite-3.3-8b", "site-a", "prov-a")]);
        let value = to_grid_route_value(&overlay, "X-Model");
        let candidates = value
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(candidates.len(), 1, "must have one candidate");
        let c = candidates.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            c.get("name").and_then(serde_json::Value::as_str),
            Some("granite-3.3-8b")
        );
        assert_eq!(
            c.get("kind").and_then(serde_json::Value::as_str),
            Some("inference_model")
        );
        assert_eq!(c.get("site").and_then(serde_json::Value::as_str), Some("site-a"));
        assert_eq!(c.get("cluster").and_then(serde_json::Value::as_str), Some("prov-a"));
        assert_eq!(c.get("fresh").and_then(serde_json::Value::as_bool), Some(true));
    }

    #[test]
    fn multiple_candidates_all_appear() {
        let overlay = make_overlay(vec![
            ("granite-3.3-8b", "site-a", "prov-a"),
            ("llama-3.2-8b", "site-b", "prov-b"),
        ]);
        let value = to_grid_route_value(&overlay, "X-Model");
        let count = value
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len);
        assert_eq!(count, Some(2), "must have two candidates");
    }

    #[test]
    fn model_header_is_configurable() {
        let overlay = make_overlay(vec![]);
        let v1 = to_grid_route_value(&overlay, "X-Model");
        let v2 = to_grid_route_value(&overlay, "X-Inference-Model");
        assert_eq!(
            v1.get("model_header").and_then(serde_json::Value::as_str),
            Some("X-Model")
        );
        assert_eq!(
            v2.get("model_header").and_then(serde_json::Value::as_str),
            Some("X-Inference-Model")
        );
    }

    #[test]
    fn output_is_json_serializable() {
        let overlay = make_overlay(vec![("llama", "site-a", "prov-a")]);
        let value = to_grid_route_value(&overlay, "X-Model");
        let json = serde_json::to_string_pretty(&value).unwrap_or_else(|_| std::process::abort());
        assert!(json.contains("grid_route"), "serialised JSON must contain filter name");
        assert!(json.contains("llama"), "serialised JSON must contain model name");
    }

    // -----------------------------------------------------------------------
    // Round-trip tests
    //
    // These prove the complete local consumption path:
    //   operator ConfigMap JSON → RoutingOverlay deserialize
    //                           → to_grid_route_value
    //                           → Praxis filter stanza
    //
    // Each test uses a realistic JSON fixture that mirrors what
    // `build_overlay_configmap` produces.
    // -----------------------------------------------------------------------

    /// Parse a realistic `grid-config.json` fixture as the xtask would.
    fn parse_overlay(json: &str) -> RoutingOverlay {
        serde_json::from_str(json).unwrap_or_else(|_| std::process::abort())
    }

    const SAMPLE_OVERLAY_JSON: &str = r#"{
        "network": "test-net",
        "local_site": "site-a",
        "candidates": [
            {
                "kind": "inference_model",
                "name": "granite-3.3-8b",
                "site": "site-a",
                "cluster": "prov-granite-local",
                "fresh": true
            },
            {
                "kind": "inference_model",
                "name": "llama-3.2-8b",
                "site": "site-b",
                "cluster": "prov-llama-remote",
                "fresh": true
            }
        ]
    }"#;

    #[test]
    fn configmap_json_deserializes_to_routing_overlay() {
        let overlay = parse_overlay(SAMPLE_OVERLAY_JSON);
        assert_eq!(overlay.network, "test-net");
        assert_eq!(overlay.local_site, "site-a");
        assert_eq!(overlay.candidates.len(), 2);
        assert_eq!(
            overlay.candidates.first().map(|c| c.name.as_str()),
            Some("granite-3.3-8b")
        );
        assert_eq!(overlay.candidates.get(1).map(|c| c.name.as_str()), Some("llama-3.2-8b"));
    }

    #[test]
    fn round_trip_local_site_appears_in_filter_stanza() {
        let overlay = parse_overlay(SAMPLE_OVERLAY_JSON);
        let value = to_grid_route_value(&overlay, "X-Model");
        assert_eq!(
            value.get("local_site").and_then(serde_json::Value::as_str),
            Some("site-a"),
            "local_site must flow from ConfigMap JSON to Praxis filter stanza"
        );
    }

    #[test]
    fn round_trip_candidates_preserved() {
        let overlay = parse_overlay(SAMPLE_OVERLAY_JSON);
        let value = to_grid_route_value(&overlay, "X-Model");
        let candidates = value
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(candidates.len(), 2, "both candidates must be preserved");
        let first = candidates.first().unwrap_or_else(|| std::process::abort());
        assert_eq!(
            first.get("name").and_then(serde_json::Value::as_str),
            Some("granite-3.3-8b")
        );
        assert_eq!(
            first.get("cluster").and_then(serde_json::Value::as_str),
            Some("prov-granite-local")
        );
        assert_eq!(first.get("site").and_then(serde_json::Value::as_str), Some("site-a"));
    }

    #[test]
    fn round_trip_stale_candidate_preserved() {
        let json = r#"{
            "network": "test-net",
            "local_site": "site-a",
            "candidates": [{
                "kind": "inference_model",
                "name": "model-x",
                "site": "site-b",
                "cluster": "prov-x",
                "fresh": false
            }]
        }"#;
        let overlay = parse_overlay(json);
        let value = to_grid_route_value(&overlay, "X-Model");
        let c = value
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .and_then(|a| a.first())
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            c.get("fresh").and_then(serde_json::Value::as_bool),
            Some(false),
            "stale fresh=false must be preserved through round-trip"
        );
    }

    #[test]
    fn round_trip_empty_candidates() {
        let json = r#"{"network": "net", "local_site": "site-a", "candidates": []}"#;
        let overlay = parse_overlay(json);
        let value = to_grid_route_value(&overlay, "X-Model");
        assert_eq!(
            value
                .get("candidates")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(0),
            "empty candidates list must round-trip"
        );
    }

    #[test]
    fn full_pipeline_configmap_to_praxis_config() {
        // Simulate the complete path end-to-end in pure Rust:
        // 1. A realistic ConfigMap JSON string (operator output)
        // 2. Deserialize to RoutingOverlay (xtask step)
        // 3. Pass to bridge (xtask step)
        // 4. Verify Praxis filter stanza shape (validation)
        let overlay = parse_overlay(SAMPLE_OVERLAY_JSON);
        let praxis_config = to_grid_route_value(&overlay, "X-Model");

        // Shape invariants that Praxis grid_route requires
        assert_eq!(
            praxis_config.get("filter").and_then(serde_json::Value::as_str),
            Some("grid_route"),
            "filter must be grid_route"
        );
        assert!(praxis_config.get("local_site").is_some(), "local_site must be present");
        assert!(
            praxis_config.get("model_header").is_some(),
            "model_header must be present"
        );
        let candidates = praxis_config
            .get("candidates")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort());
        for c in candidates {
            assert!(c.get("kind").is_some(), "candidate must have kind");
            assert!(c.get("name").is_some(), "candidate must have name");
            assert!(c.get("site").is_some(), "candidate must have site");
            assert!(c.get("cluster").is_some(), "candidate must have cluster");
            assert!(c.get("fresh").is_some(), "candidate must have fresh");
        }
    }

    #[test]
    fn output_is_deterministic_for_same_input() {
        // Identical overlay and model header must produce an identical
        // serde_json::Value on every call.  The bridge has no mutable state
        // and no randomness; this test documents and enforces that contract.
        let overlay = make_overlay(vec![("model-a", "site-a", "provider-a")]);
        let v1 = to_grid_route_value(&overlay, "X-Model");
        let v2 = to_grid_route_value(&overlay, "X-Model");
        assert_eq!(v1, v2, "identical inputs must produce identical outputs");
    }

    #[test]
    fn network_field_not_in_filter_stanza() {
        // RoutingOverlay.network is operator metadata identifying the
        // GridNetwork the overlay belongs to.  It is not consumed by the
        // Praxis grid_route filter and must not appear in the output value.
        let overlay = make_overlay(vec![]);
        let value = to_grid_route_value(&overlay, "X-Model");
        assert!(
            value.get("network").is_none(),
            "network field must not be in the Praxis filter stanza (operator metadata only)"
        );
    }
}
