//! Pure regional and sovereignty eligibility evaluation.
//!
//! The evaluator consumes the public [`RegionalPolicy`] contract and trusted
//! [`GridSite`] metadata resolved by the geography module. It does not collect
//! candidates, score them, or mutate routing state. Overlay rendering applies
//! its bounded decision before health, admission, and scoring.
//!
//! [`GridSite`]: crate::crd::grid_site::GridSite
//! [`RegionalPolicy`]: crate::crd::grid_network::RegionalPolicy

use crate::{
    crd::{
        grid_network::{CrossRegionFallback, GatewayRef, RegionalPolicy, RegionalPolicyMode},
        grid_site::GridSite,
    },
    resources::geography::{
        RegionalPolicyMetadataResolution, RegionalPolicySiteMetadata, resolve_gateway_regional_policy_metadata,
    },
};

/// Regional selection group assigned to a permitted candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionalPolicyGroup {
    /// Candidate belongs to an affinity policy's preferred region set.
    Preferred,
    /// Candidate is usable only after the preferred group cannot admit new work.
    Fallback,
    /// Candidate belongs to a locked policy's allowed region set.
    Permitted,
}

impl RegionalPolicyGroup {
    /// Bounded metric and status label for this permitted candidate outcome.
    pub(crate) const fn as_label(self) -> &'static str {
        match self {
            Self::Preferred => "preferred",
            Self::Fallback => "fallback",
            Self::Permitted => "permitted",
        }
    }

    /// Selection boundary exported to consumers that enforce regional policy.
    ///
    /// Only affinity fallback must be delayed behind another group. A locked
    /// policy makes every eligible candidate equally permitted.
    pub(crate) const fn selection_group(self) -> u32 {
        match self {
            Self::Fallback => 1,
            Self::Preferred | Self::Permitted => 0,
        }
    }
}

/// Regional-policy metadata field that could not be resolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionalPolicyMetadataField {
    /// `GridSite.spec.region`.
    Region,
    /// `GridSite.spec.sovereigntyZone`.
    SovereigntyZone,
}

/// Bounded failure state for trusted metadata resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionalPolicyMetadataFailure {
    /// A matching site exists but omits the field.
    Missing,
    /// No matching site is known in the network inventory.
    Unknown,
    /// A matching site declares an invalid value.
    Malformed,
    /// Multiple matching records disagree.
    Conflicting,
}

/// Why regional policy excluded a candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionalPolicyExclusionReason {
    /// Affinity defaulting could not resolve the gateway's trusted region.
    UnresolvedGatewayRegion {
        /// Bounded metadata failure.
        failure: RegionalPolicyMetadataFailure,
    },
    /// A required candidate metadata field could not be resolved.
    UnresolvedCandidateMetadata {
        /// Metadata field required by the active constraint.
        field: RegionalPolicyMetadataField,
        /// Bounded metadata failure.
        failure: RegionalPolicyMetadataFailure,
    },
    /// Locked mode does not allow the candidate's resolved region.
    RegionNotAllowed,
    /// Affinity fallback is denied for a candidate outside preferred regions.
    CrossRegionFallbackDenied,
    /// The independent sovereignty constraint does not allow the candidate.
    SovereigntyZoneNotAllowed,
}

impl RegionalPolicyExclusionReason {
    /// Bounded metric label for this exclusion reason.
    ///
    /// This deliberately excludes the candidate's region, sovereignty zone,
    /// gateway, and cluster so operator metrics cannot acquire unbounded
    /// cardinality from policy values or inventory identifiers.
    pub(crate) const fn as_metric_label(self) -> &'static str {
        match self {
            Self::UnresolvedGatewayRegion { failure } => match failure {
                RegionalPolicyMetadataFailure::Missing => "gateway_region_missing",
                RegionalPolicyMetadataFailure::Unknown => "gateway_region_unknown",
                RegionalPolicyMetadataFailure::Malformed => "gateway_region_malformed",
                RegionalPolicyMetadataFailure::Conflicting => "gateway_region_conflicting",
            },
            Self::UnresolvedCandidateMetadata { field, failure } => match (field, failure) {
                (RegionalPolicyMetadataField::Region, RegionalPolicyMetadataFailure::Missing) => {
                    "candidate_region_missing"
                },
                (RegionalPolicyMetadataField::Region, RegionalPolicyMetadataFailure::Unknown) => {
                    "candidate_region_unknown"
                },
                (RegionalPolicyMetadataField::Region, RegionalPolicyMetadataFailure::Malformed) => {
                    "candidate_region_malformed"
                },
                (RegionalPolicyMetadataField::Region, RegionalPolicyMetadataFailure::Conflicting) => {
                    "candidate_region_conflicting"
                },
                (RegionalPolicyMetadataField::SovereigntyZone, RegionalPolicyMetadataFailure::Missing) => {
                    "candidate_sovereignty_zone_missing"
                },
                (RegionalPolicyMetadataField::SovereigntyZone, RegionalPolicyMetadataFailure::Unknown) => {
                    "candidate_sovereignty_zone_unknown"
                },
                (RegionalPolicyMetadataField::SovereigntyZone, RegionalPolicyMetadataFailure::Malformed) => {
                    "candidate_sovereignty_zone_malformed"
                },
                (RegionalPolicyMetadataField::SovereigntyZone, RegionalPolicyMetadataFailure::Conflicting) => {
                    "candidate_sovereignty_zone_conflicting"
                },
            },
            Self::RegionNotAllowed => "region_not_allowed",
            Self::CrossRegionFallbackDenied => "cross_region_fallback_denied",
            Self::SovereigntyZoneNotAllowed => "sovereignty_zone_not_allowed",
        }
    }
}

/// Regional-policy decision for one candidate before health and scoring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionalPolicyDecision {
    /// The candidate passes hard eligibility and belongs to this regional group.
    Permitted {
        /// Group used to preserve affinity boundaries during later selection.
        group: RegionalPolicyGroup,
    },
    /// The candidate must not enter either the routable or admission-excluded set.
    Excluded {
        /// Bounded explanation suitable for status and metrics mapping.
        reason: RegionalPolicyExclusionReason,
    },
}

/// Prepared regional policy that can evaluate candidates without revalidating configuration.
pub(crate) struct RegionalPolicyEvaluator<'policy, 'site> {
    /// Region-mode rule prepared from the validated policy and gateway metadata.
    region_rule: RegionalPolicyRegionRule<'policy, 'site>,
    /// Optional independent sovereignty-zone hard allowlist.
    allowed_sovereignty_zones: Option<&'policy [String]>,
}

/// Validated region-mode rule.
enum RegionalPolicyRegionRule<'policy, 'site> {
    /// Affinity groups candidates by preferred membership and explicit fallback.
    Affinity {
        /// Explicit preferred set or the resolved gateway default.
        preferred_regions: PreferredRegions<'policy, 'site>,
        /// Whether non-preferred candidates may enter the fallback group.
        cross_region_fallback: CrossRegionFallback,
    },
    /// Locked mode applies an exact hard allowlist.
    Locked {
        /// Every region permitted by the lock.
        allowed_regions: &'policy [String],
    },
}

/// Preferred-region source for affinity evaluation.
enum PreferredRegions<'policy, 'site> {
    /// Explicit unordered set from `preferredRegions`.
    Explicit(&'policy [String]),
    /// Single region resolved from the gateway's trusted `GridSite`.
    Gateway(&'site str),
    /// Valid policy whose required gateway inventory metadata is unavailable.
    Unresolved(RegionalPolicyMetadataFailure),
}

impl<'policy, 'site> RegionalPolicyEvaluator<'policy, 'site> {
    /// Validate and prepare a policy using already-resolved gateway metadata.
    ///
    /// Inventory failures remain valid fail-closed evaluator state. Only an
    /// invalid policy contract returns an error so callers can retain the last
    /// known good revision for invalid configuration without doing so for
    /// missing inventory metadata.
    ///
    /// # Errors
    ///
    /// Returns the defensive [`RegionalPolicy::validate`] error for an invalid
    /// field combination or bounded set.
    pub(crate) fn new(
        policy: &'policy RegionalPolicy,
        gateway_metadata: RegionalPolicySiteMetadata<'site>,
    ) -> Result<Self, String> {
        policy.validate()?;
        let region_rule = match policy.mode {
            RegionalPolicyMode::Affinity => RegionalPolicyRegionRule::Affinity {
                preferred_regions: policy.preferred_regions.as_deref().map_or_else(
                    || match resolve_metadata(gateway_metadata.region) {
                        Ok(region) => PreferredRegions::Gateway(region),
                        Err(failure) => PreferredRegions::Unresolved(failure),
                    },
                    PreferredRegions::Explicit,
                ),
                cross_region_fallback: policy.effective_cross_region_fallback(),
            },
            RegionalPolicyMode::Locked => {
                let Some(allowed_regions) = policy.allowed_regions.as_deref() else {
                    return Err("allowedRegions is required in locked mode".to_owned());
                };
                RegionalPolicyRegionRule::Locked { allowed_regions }
            },
        };
        Ok(Self {
            region_rule,
            allowed_sovereignty_zones: policy.allowed_sovereignty_zones.as_deref(),
        })
    }

    /// Resolve gateway metadata and prepare an evaluator for one `GatewayRef`.
    ///
    /// # Errors
    ///
    /// Returns a defensive validation error for an invalid policy contract.
    pub(crate) fn for_gateway(
        policy: &'policy RegionalPolicy,
        gateway: &GatewayRef,
        sites: &'site [GridSite],
        network_name: &str,
    ) -> Result<Self, String> {
        Self::new(
            policy,
            resolve_gateway_regional_policy_metadata(gateway, sites, network_name),
        )
    }

    /// Bounded metric label for the active policy mode.
    pub(crate) const fn mode_label(&self) -> &'static str {
        match &self.region_rule {
            RegionalPolicyRegionRule::Affinity { .. } => "affinity",
            RegionalPolicyRegionRule::Locked { .. } => "locked",
        }
    }

    /// Evaluate one candidate before health, admission, and scoring.
    ///
    /// Region failures take deterministic precedence over sovereignty failures.
    /// A sovereignty field is required only when the independent sovereignty
    /// allowlist is active.
    #[must_use]
    pub(crate) fn evaluate(&self, candidate: RegionalPolicySiteMetadata<'_>) -> RegionalPolicyDecision {
        let group = match self.evaluate_region(candidate.region) {
            Ok(group) => group,
            Err(reason) => return RegionalPolicyDecision::Excluded { reason },
        };
        if let Some(allowed_zones) = self.allowed_sovereignty_zones {
            let sovereignty_zone = match resolve_metadata(candidate.sovereignty_zone) {
                Ok(zone) => zone,
                Err(failure) => {
                    return RegionalPolicyDecision::Excluded {
                        reason: RegionalPolicyExclusionReason::UnresolvedCandidateMetadata {
                            field: RegionalPolicyMetadataField::SovereigntyZone,
                            failure,
                        },
                    };
                },
            };
            if !contains_exact(allowed_zones, sovereignty_zone) {
                return RegionalPolicyDecision::Excluded {
                    reason: RegionalPolicyExclusionReason::SovereigntyZoneNotAllowed,
                };
            }
        }
        RegionalPolicyDecision::Permitted { group }
    }

    /// Evaluate the mode-specific region boundary.
    fn evaluate_region(
        &self,
        candidate_region: RegionalPolicyMetadataResolution<'_>,
    ) -> Result<RegionalPolicyGroup, RegionalPolicyExclusionReason> {
        match &self.region_rule {
            RegionalPolicyRegionRule::Affinity {
                preferred_regions: PreferredRegions::Unresolved(failure),
                ..
            } => Err(RegionalPolicyExclusionReason::UnresolvedGatewayRegion { failure: *failure }),
            RegionalPolicyRegionRule::Affinity {
                preferred_regions,
                cross_region_fallback,
            } => {
                let candidate_region = resolve_metadata(candidate_region).map_err(|failure| {
                    RegionalPolicyExclusionReason::UnresolvedCandidateMetadata {
                        field: RegionalPolicyMetadataField::Region,
                        failure,
                    }
                })?;
                if preferred_regions.contains(candidate_region) {
                    Ok(RegionalPolicyGroup::Preferred)
                } else if *cross_region_fallback == CrossRegionFallback::Allow {
                    Ok(RegionalPolicyGroup::Fallback)
                } else {
                    Err(RegionalPolicyExclusionReason::CrossRegionFallbackDenied)
                }
            },
            RegionalPolicyRegionRule::Locked { allowed_regions } => {
                let candidate_region = resolve_metadata(candidate_region).map_err(|failure| {
                    RegionalPolicyExclusionReason::UnresolvedCandidateMetadata {
                        field: RegionalPolicyMetadataField::Region,
                        failure,
                    }
                })?;
                if contains_exact(allowed_regions, candidate_region) {
                    Ok(RegionalPolicyGroup::Permitted)
                } else {
                    Err(RegionalPolicyExclusionReason::RegionNotAllowed)
                }
            },
        }
    }
}

impl PreferredRegions<'_, '_> {
    /// Whether an exact resolved candidate region belongs to the preferred set.
    fn contains(&self, candidate_region: &str) -> bool {
        match self {
            Self::Explicit(regions) => contains_exact(regions, candidate_region),
            Self::Gateway(region) => *region == candidate_region,
            Self::Unresolved(_) => false,
        }
    }
}

/// Return a resolved metadata value or its bounded failure state.
fn resolve_metadata<'site>(
    resolution: RegionalPolicyMetadataResolution<'site>,
) -> Result<&'site str, RegionalPolicyMetadataFailure> {
    match resolution {
        RegionalPolicyMetadataResolution::Resolved(value) => Ok(value),
        RegionalPolicyMetadataResolution::Missing => Err(RegionalPolicyMetadataFailure::Missing),
        RegionalPolicyMetadataResolution::Unknown => Err(RegionalPolicyMetadataFailure::Unknown),
        RegionalPolicyMetadataResolution::Malformed => Err(RegionalPolicyMetadataFailure::Malformed),
        RegionalPolicyMetadataResolution::Conflicting => Err(RegionalPolicyMetadataFailure::Conflicting),
    }
}

/// Exact, case-sensitive membership in a bounded policy set.
fn contains_exact(values: &[String], candidate: &str) -> bool {
    values.iter().any(|value| value == candidate)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn affinity_explicit_preferred_regions_override_unresolved_gateway_metadata() {
        let policy = affinity_policy(Some(&["east"]), None, None);
        let evaluator = evaluator(&policy, RegionalPolicyMetadataResolution::Unknown);
        assert_eq!(
            evaluator.evaluate(resolved_candidate("east", None)),
            permitted(RegionalPolicyGroup::Preferred)
        );
    }

    #[test]
    fn affinity_default_uses_the_resolved_gateway_region() {
        let policy = affinity_policy(None, None, None);
        let evaluator = evaluator(&policy, RegionalPolicyMetadataResolution::Resolved("east"));
        assert_eq!(
            evaluator.evaluate(resolved_candidate("east", None)),
            permitted(RegionalPolicyGroup::Preferred)
        );
    }

    #[test]
    fn affinity_default_fails_closed_for_each_gateway_metadata_failure() {
        let policy = affinity_policy(None, Some(CrossRegionFallback::Allow), None);
        for (resolution, failure) in metadata_failures() {
            let evaluator = evaluator(&policy, resolution);
            assert_eq!(
                evaluator.evaluate(resolved_candidate("east", None)),
                excluded(RegionalPolicyExclusionReason::UnresolvedGatewayRegion { failure })
            );
        }
    }

    #[test]
    fn affinity_fallback_requires_explicit_allow() {
        let denied_policy = affinity_policy(Some(&["east"]), None, None);
        let denied = evaluator(&denied_policy, RegionalPolicyMetadataResolution::Unknown);
        assert_eq!(
            denied.evaluate(resolved_candidate("west", None)),
            excluded(RegionalPolicyExclusionReason::CrossRegionFallbackDenied)
        );

        let allowed_policy = affinity_policy(Some(&["east"]), Some(CrossRegionFallback::Allow), None);
        let allowed = evaluator(&allowed_policy, RegionalPolicyMetadataResolution::Unknown);
        assert_eq!(
            allowed.evaluate(resolved_candidate("west", None)),
            permitted(RegionalPolicyGroup::Fallback)
        );
    }

    #[test]
    fn affinity_region_membership_is_exact_and_case_sensitive() {
        let policy = affinity_policy(Some(&["east"]), None, None);
        let evaluator = evaluator(&policy, RegionalPolicyMetadataResolution::Unknown);
        assert_eq!(
            evaluator.evaluate(resolved_candidate("EAST", None)),
            excluded(RegionalPolicyExclusionReason::CrossRegionFallbackDenied)
        );
    }

    #[test]
    fn locked_mode_permits_only_the_explicit_region_set() {
        let policy = locked_policy(&["east", "west"], None);
        let evaluator = evaluator(&policy, RegionalPolicyMetadataResolution::Unknown);
        assert_eq!(
            evaluator.evaluate(resolved_candidate("west", None)),
            permitted(RegionalPolicyGroup::Permitted)
        );
        assert_eq!(
            evaluator.evaluate(resolved_candidate("north", None)),
            excluded(RegionalPolicyExclusionReason::RegionNotAllowed)
        );
    }

    #[test]
    fn locked_mode_ignores_gateway_region_resolution() {
        let policy = locked_policy(&["east"], None);
        for (resolution, _) in metadata_failures() {
            let evaluator = evaluator(&policy, resolution);
            assert_eq!(
                evaluator.evaluate(resolved_candidate("east", None)),
                permitted(RegionalPolicyGroup::Permitted)
            );
        }
    }

    #[test]
    fn every_required_candidate_region_failure_is_excluded() {
        let policy = locked_policy(&["east"], None);
        let evaluator = evaluator(&policy, RegionalPolicyMetadataResolution::Unknown);
        for (resolution, failure) in metadata_failures() {
            assert_eq!(
                evaluator.evaluate(candidate(resolution, RegionalPolicyMetadataResolution::Missing)),
                excluded(RegionalPolicyExclusionReason::UnresolvedCandidateMetadata {
                    field: RegionalPolicyMetadataField::Region,
                    failure,
                })
            );
        }
    }

    #[test]
    fn affinity_never_treats_unresolved_candidate_region_as_fallback() {
        let policy = affinity_policy(Some(&["east"]), Some(CrossRegionFallback::Allow), None);
        let evaluator = evaluator(&policy, RegionalPolicyMetadataResolution::Unknown);
        assert_eq!(
            evaluator.evaluate(candidate(
                RegionalPolicyMetadataResolution::Unknown,
                RegionalPolicyMetadataResolution::Missing,
            )),
            excluded(RegionalPolicyExclusionReason::UnresolvedCandidateMetadata {
                field: RegionalPolicyMetadataField::Region,
                failure: RegionalPolicyMetadataFailure::Unknown,
            })
        );
    }

    #[test]
    fn sovereignty_metadata_is_not_required_without_an_active_constraint() {
        let policy = locked_policy(&["east"], None);
        let evaluator = evaluator(&policy, RegionalPolicyMetadataResolution::Unknown);
        for (sovereignty_zone, _) in metadata_failures() {
            assert_eq!(
                evaluator.evaluate(candidate(
                    RegionalPolicyMetadataResolution::Resolved("east"),
                    sovereignty_zone,
                )),
                permitted(RegionalPolicyGroup::Permitted)
            );
        }
    }

    #[test]
    fn sovereignty_is_an_independent_hard_allowlist() {
        let policy = locked_policy(&["east"], Some(&["eu"]));
        let evaluator = evaluator(&policy, RegionalPolicyMetadataResolution::Unknown);
        assert_eq!(
            evaluator.evaluate(resolved_candidate("east", Some("eu"))),
            permitted(RegionalPolicyGroup::Permitted)
        );
        assert_eq!(
            evaluator.evaluate(resolved_candidate("east", Some("us"))),
            excluded(RegionalPolicyExclusionReason::SovereigntyZoneNotAllowed)
        );
    }

    #[test]
    fn every_required_sovereignty_failure_is_excluded() {
        let policy = affinity_policy(Some(&["east"]), None, Some(&["eu"]));
        let evaluator = evaluator(&policy, RegionalPolicyMetadataResolution::Unknown);
        for (sovereignty_zone, failure) in metadata_failures() {
            assert_eq!(
                evaluator.evaluate(candidate(
                    RegionalPolicyMetadataResolution::Resolved("east"),
                    sovereignty_zone,
                )),
                excluded(RegionalPolicyExclusionReason::UnresolvedCandidateMetadata {
                    field: RegionalPolicyMetadataField::SovereigntyZone,
                    failure,
                })
            );
        }
    }

    #[test]
    fn region_failure_has_deterministic_precedence_over_sovereignty_failure() {
        let policy = locked_policy(&["east"], Some(&["eu"]));
        let evaluator = evaluator(&policy, RegionalPolicyMetadataResolution::Unknown);
        assert_eq!(
            evaluator.evaluate(candidate(
                RegionalPolicyMetadataResolution::Missing,
                RegionalPolicyMetadataResolution::Conflicting,
            )),
            excluded(RegionalPolicyExclusionReason::UnresolvedCandidateMetadata {
                field: RegionalPolicyMetadataField::Region,
                failure: RegionalPolicyMetadataFailure::Missing,
            })
        );
    }

    #[test]
    fn invalid_policy_is_rejected_before_candidate_evaluation() {
        let policy = RegionalPolicy {
            mode: RegionalPolicyMode::Locked,
            preferred_regions: None,
            allowed_regions: None,
            cross_region_fallback: None,
            allowed_sovereignty_zones: None,
        };
        assert!(
            RegionalPolicyEvaluator::new(&policy, missing_metadata()).is_err(),
            "invalid configuration must remain distinct from valid fail-closed inventory"
        );
    }

    #[test]
    fn telemetry_labels_are_closed_policy_enum_values() {
        assert_eq!(RegionalPolicyGroup::Preferred.as_label(), "preferred");
        assert_eq!(RegionalPolicyGroup::Fallback.as_label(), "fallback");
        assert_eq!(RegionalPolicyGroup::Permitted.as_label(), "permitted");
        assert_eq!(
            RegionalPolicyExclusionReason::UnresolvedCandidateMetadata {
                field: RegionalPolicyMetadataField::SovereigntyZone,
                failure: RegionalPolicyMetadataFailure::Conflicting,
            }
            .as_metric_label(),
            "candidate_sovereignty_zone_conflicting"
        );
        assert_eq!(
            RegionalPolicyExclusionReason::CrossRegionFallbackDenied.as_metric_label(),
            "cross_region_fallback_denied"
        );
    }

    #[test]
    fn gateway_evaluator_uses_the_gateway_site_name() {
        let policy = affinity_policy(None, None, None);
        let gateway: GatewayRef = serde_json::from_value(serde_json::json!({
            "name": "gateway-east",
            "namespace": "grid-system",
            "siteName": "site-east"
        }))
        .unwrap_or_else(|_| std::process::abort());
        let sites = [test_site("site-east", "network", "east")];
        let evaluator = RegionalPolicyEvaluator::for_gateway(&policy, &gateway, &sites, "network")
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            evaluator.evaluate(resolved_candidate("east", None)),
            permitted(RegionalPolicyGroup::Preferred)
        );
    }

    // -----------------------------------------------------------------------
    // Test Utilities
    // -----------------------------------------------------------------------

    fn affinity_policy(
        preferred_regions: Option<&[&str]>,
        fallback: Option<CrossRegionFallback>,
        sovereignty_zones: Option<&[&str]>,
    ) -> RegionalPolicy {
        RegionalPolicy {
            mode: RegionalPolicyMode::Affinity,
            preferred_regions: preferred_regions.map(string_set),
            allowed_regions: None,
            cross_region_fallback: fallback,
            allowed_sovereignty_zones: sovereignty_zones.map(string_set),
        }
    }

    fn locked_policy(allowed_regions: &[&str], sovereignty_zones: Option<&[&str]>) -> RegionalPolicy {
        RegionalPolicy {
            mode: RegionalPolicyMode::Locked,
            preferred_regions: None,
            allowed_regions: Some(string_set(allowed_regions)),
            cross_region_fallback: None,
            allowed_sovereignty_zones: sovereignty_zones.map(string_set),
        }
    }

    fn string_set(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn evaluator<'policy, 'site>(
        policy: &'policy RegionalPolicy,
        gateway_region: RegionalPolicyMetadataResolution<'site>,
    ) -> RegionalPolicyEvaluator<'policy, 'site> {
        RegionalPolicyEvaluator::new(
            policy,
            RegionalPolicySiteMetadata {
                region: gateway_region,
                sovereignty_zone: RegionalPolicyMetadataResolution::Missing,
            },
        )
        .unwrap_or_else(|_| std::process::abort())
    }

    fn resolved_candidate<'site>(
        region: &'site str,
        sovereignty_zone: Option<&'site str>,
    ) -> RegionalPolicySiteMetadata<'site> {
        candidate(
            RegionalPolicyMetadataResolution::Resolved(region),
            sovereignty_zone.map_or(
                RegionalPolicyMetadataResolution::Missing,
                RegionalPolicyMetadataResolution::Resolved,
            ),
        )
    }

    fn candidate<'site>(
        region: RegionalPolicyMetadataResolution<'site>,
        sovereignty_zone: RegionalPolicyMetadataResolution<'site>,
    ) -> RegionalPolicySiteMetadata<'site> {
        RegionalPolicySiteMetadata {
            region,
            sovereignty_zone,
        }
    }

    fn missing_metadata() -> RegionalPolicySiteMetadata<'static> {
        candidate(
            RegionalPolicyMetadataResolution::Missing,
            RegionalPolicyMetadataResolution::Missing,
        )
    }

    fn metadata_failures() -> [(RegionalPolicyMetadataResolution<'static>, RegionalPolicyMetadataFailure); 4] {
        [
            (
                RegionalPolicyMetadataResolution::Missing,
                RegionalPolicyMetadataFailure::Missing,
            ),
            (
                RegionalPolicyMetadataResolution::Unknown,
                RegionalPolicyMetadataFailure::Unknown,
            ),
            (
                RegionalPolicyMetadataResolution::Malformed,
                RegionalPolicyMetadataFailure::Malformed,
            ),
            (
                RegionalPolicyMetadataResolution::Conflicting,
                RegionalPolicyMetadataFailure::Conflicting,
            ),
        ]
    }

    fn permitted(group: RegionalPolicyGroup) -> RegionalPolicyDecision {
        RegionalPolicyDecision::Permitted { group }
    }

    fn excluded(reason: RegionalPolicyExclusionReason) -> RegionalPolicyDecision {
        RegionalPolicyDecision::Excluded { reason }
    }

    fn test_site(name: &str, network: &str, region: &str) -> GridSite {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridSite",
            "metadata": { "name": name },
            "spec": {
                "gridNetworkRef": network,
                "region": region
            }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }
}
