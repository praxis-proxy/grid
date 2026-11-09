//! The assembled gateway's Policy features must match its selected crypto profile.

#[cfg(test)]
mod tests {
    #[cfg(any(feature = "fips", feature = "policy-jwt-fips"))]
    #[test]
    fn the_fips_policy_selection_is_accepted_by_the_host_assessment() {
        let mut registry = praxis_filter::FilterRegistry::with_builtins();
        praxis_ai_filters::register_ai_filters(&mut registry, None);

        assert!(
            registry.available_filters().contains(&"policy"),
            "the FIPS selection must keep the chart's Policy filter available"
        );
        assert_eq!(
            praxis::fips_blocker(&registry),
            None,
            "the assembled gateway must carry a safe Policy capability report"
        );
    }

    #[cfg(feature = "policy-builtins")]
    #[test]
    fn ordinary_policy_builtins_are_refused_under_required_fips() {
        let registry = praxis_filter::FilterRegistry::with_builtins();

        assert!(
            praxis::fips_blocker(&registry).is_some(),
            "ordinary Policy builtins must not pass the host's FIPS assessment"
        );
    }
}
