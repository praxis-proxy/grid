//! Turns one site's metrics into green, yellow, or red with human-readable
//! reasons. No I/O, so it is exhaustively unit tested.

use crate::{model::Health, queries::Thresholds};

/// What the collector learned about one site in a single poll.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Inputs {
    /// Whether at least one query answered.
    pub reachable: bool,
    /// Polls in a row with no answer, including this one.
    pub consecutive_failures: u32,
    /// Ready inference endpoints, when reported.
    pub ready_endpoints: Option<f64>,
    /// GPU utilization in percent, when reported.
    pub gpu_util: Option<f64>,
    /// Requests waiting, when reported.
    pub queue_depth: Option<f64>,
    /// Serving replicas that are not available, when reported.
    pub replicas_down: Option<f64>,
    /// Median request latency in milliseconds, when reported.
    pub p50_latency_ms: Option<f64>,
    /// Required keys that returned no data.
    pub missing: Vec<String>,
}

/// The worst level reached and every reason that fired, so the panel can
/// show all of them rather than only the first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Verdict {
    /// Worst level raised.
    pub health: Health,
    /// Every reason, in the order it fired.
    pub reasons: Vec<String>,
}

impl Verdict {
    /// Records `reason`, promoting the level when `to` is worse than the
    /// current one. Red is final; yellow only replaces green.
    fn raise(&mut self, to: Health, reason: String) {
        self.reasons.push(reason);
        if to == Health::Red || self.health == Health::Green {
            self.health = to;
        }
    }

    /// Applies every metric threshold. Each check contributes its own reason.
    fn check_metrics(&mut self, inputs: &Inputs, thresholds: &Thresholds) {
        if inputs.ready_endpoints.is_some_and(|count| count < 1.0) {
            self.raise(Health::Red, "no ready inference endpoints".to_owned());
        }
        if let Some(util) = inputs.gpu_util.filter(|util| *util >= thresholds.gpu_util_warn) {
            let (shown, warn) = (util.floor(), thresholds.gpu_util_warn);
            self.raise(Health::Yellow, format!("GPU utilization {shown:.0}% >= {warn:.0}%"));
        }
        if let Some(depth) = inputs.queue_depth.filter(|depth| *depth >= thresholds.queue_warn) {
            let warn = thresholds.queue_warn;
            self.raise(Health::Yellow, format!("queue depth {depth:.0} >= {warn:.0}"));
        }
        if let Some(down) = inputs.replicas_down.filter(|down| *down > 0.0) {
            self.raise(Health::Yellow, format!("{down:.0} vLLM replica(s) unavailable"));
        }
        if let Some(latency) = inputs
            .p50_latency_ms
            .filter(|latency| *latency >= thresholds.latency_warn_ms)
        {
            let warn = thresholds.latency_warn_ms;
            self.raise(Health::Yellow, format!("p50 latency {latency:.0}ms >= {warn:.0}ms"));
        }
    }
}

/// Derives a site's health. An unreachable site reports only that, since its
/// other metrics are stale; otherwise every threshold and every missing key
/// contributes a reason.
#[must_use]
pub fn derive(inputs: &Inputs, thresholds: &Thresholds) -> Verdict {
    let mut verdict = Verdict::default();
    if !inputs.reachable {
        let failures = inputs.consecutive_failures;
        let level = if failures >= thresholds.red_after_failures {
            Health::Red
        } else {
            Health::Yellow
        };
        verdict.raise(level, format!("metrics unreachable ({failures} consecutive failures)"));
        return verdict;
    }
    verdict.check_metrics(inputs, thresholds);
    for key in &inputs.missing {
        verdict.raise(Health::Yellow, format!("no data for {key}"));
    }
    verdict
}

#[cfg(test)]
mod tests {
    use super::{Inputs, derive};
    use crate::{model::Health, queries::Thresholds};

    #[test]
    fn healthy_inputs_are_green_with_no_reasons() {
        assert_verdict(&healthy(), Health::Green, &[]);
    }

    #[test]
    fn unreachable_below_the_limit_is_yellow() {
        let inputs = Inputs {
            reachable: false,
            consecutive_failures: 1,
            ..Inputs::default()
        };
        assert_verdict(
            &inputs,
            Health::Yellow,
            &["metrics unreachable (1 consecutive failures)"],
        );
    }

    #[test]
    fn unreachable_at_the_limit_is_red() {
        let inputs = Inputs {
            reachable: false,
            consecutive_failures: 2,
            ..Inputs::default()
        };
        assert_verdict(&inputs, Health::Red, &["metrics unreachable (2 consecutive failures)"]);
    }

    #[test]
    fn unreachable_short_circuits_every_other_reason() {
        let inputs = Inputs {
            reachable: false,
            consecutive_failures: 1,
            gpu_util: Some(99.0),
            ..Inputs::default()
        };
        assert_verdict(
            &inputs,
            Health::Yellow,
            &["metrics unreachable (1 consecutive failures)"],
        );
    }

    #[test]
    fn no_ready_endpoints_is_red() {
        let inputs = Inputs {
            ready_endpoints: Some(0.0),
            ..healthy()
        };
        assert_verdict(&inputs, Health::Red, &["no ready inference endpoints"]);
    }

    #[test]
    fn a_hot_gpu_is_yellow_and_shows_the_floor_of_the_value() {
        let inputs = Inputs {
            gpu_util: Some(93.4),
            ..healthy()
        };
        assert_verdict(&inputs, Health::Yellow, &["GPU utilization 93% >= 90%"]);
    }

    #[test]
    fn a_deep_queue_is_yellow_at_the_threshold() {
        let inputs = Inputs {
            queue_depth: Some(50.0),
            ..healthy()
        };
        assert_verdict(&inputs, Health::Yellow, &["queue depth 50 >= 50"]);
    }

    #[test]
    fn unavailable_replicas_are_yellow() {
        let inputs = Inputs {
            replicas_down: Some(1.0),
            ..healthy()
        };
        assert_verdict(&inputs, Health::Yellow, &["1 vLLM replica(s) unavailable"]);
    }

    #[test]
    fn slow_p50_latency_is_yellow_at_the_threshold() {
        let inputs = Inputs {
            p50_latency_ms: Some(5000.0),
            ..healthy()
        };
        assert_verdict(&inputs, Health::Yellow, &["p50 latency 5000ms >= 5000ms"]);
    }

    #[test]
    fn missing_required_data_is_yellow() {
        let inputs = Inputs {
            gpu_util: None,
            missing: vec!["gpuUtil".to_owned()],
            ..healthy()
        };
        assert_verdict(&inputs, Health::Yellow, &["no data for gpuUtil"]);
    }

    #[test]
    fn every_fired_reason_is_reported_in_order() {
        let inputs = Inputs {
            gpu_util: Some(95.0),
            queue_depth: Some(70.0),
            ..healthy()
        };
        assert_verdict(
            &inputs,
            Health::Yellow,
            &["GPU utilization 95% >= 90%", "queue depth 70 >= 50"],
        );
    }

    #[test]
    fn red_outranks_yellow_regardless_of_order() {
        let inputs = Inputs {
            ready_endpoints: Some(0.0),
            gpu_util: Some(95.0),
            ..healthy()
        };
        assert_verdict(
            &inputs,
            Health::Red,
            &["no ready inference endpoints", "GPU utilization 95% >= 90%"],
        );
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    fn healthy() -> Inputs {
        Inputs {
            reachable: true,
            consecutive_failures: 0,
            ready_endpoints: Some(2.0),
            gpu_util: Some(40.0),
            queue_depth: Some(3.0),
            replicas_down: Some(0.0),
            p50_latency_ms: Some(800.0),
            missing: Vec::new(),
        }
    }

    fn assert_verdict(inputs: &Inputs, health: Health, reasons: &[&str]) {
        let verdict = derive(inputs, &Thresholds::default());
        assert_eq!(verdict.health, health, "health for {inputs:?}");
        assert_eq!(verdict.reasons, reasons, "reasons for {inputs:?}");
    }
}
