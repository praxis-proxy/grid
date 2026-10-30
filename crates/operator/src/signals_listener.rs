//! The `SignalsListener` condition: whether the signals endpoint this install declares is served.
//!
//! The listener runs only under the poll transport, so a site that declares it under gossip
//! renders a port and a Service with nothing behind them. Under poll the listener task reports
//! what it serves, so the condition reads what is bound rather than what is configured.

use serde_json::Value;
use tokio::sync::watch;

use crate::crd::{
    grid_network::{GridNetwork, SignalMode},
    inference_provider::{Condition, ConditionStatus},
};

/// The condition's type.
pub const SIGNALS_LISTENER: &str = "SignalsListener";

/// Reason while the declared listener serves; a True reason names the type.
pub const SIGNALS_LISTENER_SERVING: &str = SIGNALS_LISTENER;

/// Reason while the install declares a listener the gossip transport never starts.
pub const SIGNALS_LISTENER_ABSENT: &str = "GossipTransport";

/// Reason while the listener has not bound yet.
pub const SIGNALS_LISTENER_STARTING: &str = "Starting";

/// Reason while the listener waits for this site's TLS material.
pub const SIGNALS_LISTENER_TLS_UNAVAILABLE: &str = "TLSMaterialUnavailable";

/// Reason while the listener's bind fails.
pub const SIGNALS_LISTENER_BIND_FAILED: &str = "BindFailed";

/// Server-side apply manager, apart from `GridNetwork` reconciliation.
pub const FIELD_MANAGER: &str = "grid-operator-signals-listener";

/// What the listener task reports about itself.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Served {
    /// Not bound.
    #[default]
    Starting,
    /// Waiting for this site's TLS material; nothing is served.
    AwaitingTls,
    /// The bind failed with this error; retried after a pause.
    BindFailed(String),
    /// Bound and serving.
    Serving,
}

/// The listener task's report, read by reconciliation.
pub type Report = watch::Receiver<Served>;

/// The listener task's side of its report.
pub type Reporter = watch::Sender<Served>;

/// A report channel, `Starting` until the task says otherwise.
#[must_use]
pub fn report() -> (Reporter, Report) {
    watch::channel(Served::Starting)
}

/// Whether the declared signals listener runs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Listener {
    /// It runs.
    Serving,
    /// It is declared, but this transport runs none.
    Absent,
    /// It has not bound yet.
    Starting,
    /// It waits for this site's TLS material.
    TlsUnavailable,
    /// Its bind failed with this error.
    BindFailed(String),
}

impl Listener {
    /// Judge the listener the install declared from what its task `served`, under the
    /// `transport` this process runs. `None` when none is declared, since nothing is promised.
    #[must_use]
    pub fn of(served: Option<&Served>, transport: SignalMode) -> Option<Self> {
        let served = served?;
        Some(match transport {
            SignalMode::Gossip => Self::Absent,
            SignalMode::Poll => match served {
                Served::Serving => Self::Serving,
                Served::Starting => Self::Starting,
                Served::AwaitingTls => Self::TlsUnavailable,
                Served::BindFailed(error) => Self::BindFailed(error.clone()),
            },
        })
    }

    /// The status and reason the condition carries.
    const fn verdict(&self) -> (ConditionStatus, &'static str) {
        match self {
            Self::Serving => (ConditionStatus::True, SIGNALS_LISTENER_SERVING),
            Self::Absent => (ConditionStatus::False, SIGNALS_LISTENER_ABSENT),
            Self::Starting => (ConditionStatus::False, SIGNALS_LISTENER_STARTING),
            Self::TlsUnavailable => (ConditionStatus::False, SIGNALS_LISTENER_TLS_UNAVAILABLE),
            Self::BindFailed(_) => (ConditionStatus::False, SIGNALS_LISTENER_BIND_FAILED),
        }
    }

    /// What the condition says.
    fn message(&self) -> String {
        match self {
            Self::Serving => "the signals listener is bound and serving under poll".to_owned(),
            Self::Absent => "the signals Service is rendered but the signal transport is gossip, so nothing listens \
                             on it: set the GridNetwork's signalTransport to poll, or turn signals off"
                .to_owned(),
            Self::Starting => "the signals listener is not bound yet".to_owned(),
            Self::TlsUnavailable => "this site's TLS material is unavailable, so the signals listener serves \
                                     nothing; it retries every 30 s"
                .to_owned(),
            Self::BindFailed(error) => format!("the signals listener could not bind: {error}; it retries every 30 s"),
        }
    }
}

/// The `SignalsListener` condition the live status holds.
fn held(network: &GridNetwork) -> Option<&Condition> {
    network
        .status
        .as_ref()
        .and_then(|live| live.conditions.iter().find(|held| held.type_ == SIGNALS_LISTENER))
}

/// The condition for `listener`, or `None` when the live status already says it.
#[must_use]
pub fn condition(network: &GridNetwork, listener: &Listener, now_rfc3339: &str) -> Option<Condition> {
    let (state, reason) = listener.verdict();
    let message = listener.message();
    let generation = network.metadata.generation;
    let held = held(network);
    if held.is_some_and(|held| {
        held.status == state
            && held.reason == reason
            && held.message == message
            && held.observed_generation == generation
    }) {
        return None;
    }
    let last_transition_time = held
        .filter(|held| held.status == state)
        .map_or_else(|| now_rfc3339.to_owned(), |held| held.last_transition_time.clone());
    Some(Condition {
        type_: SIGNALS_LISTENER.to_owned(),
        status: state,
        reason: reason.to_owned(),
        message,
        last_transition_time,
        observed_generation: generation,
    })
}

/// The status patch for `listener`, or `None` when the live status already says it.
///
/// With no listener declared the patch carries no condition: conditions merge by type, so
/// applying none under [`FIELD_MANAGER`] removes only the one it wrote, and nothing is
/// written while none is held.
#[must_use]
pub fn patch<'network>(
    network: &'network GridNetwork,
    listener: Option<&Listener>,
    now_rfc3339: &str,
) -> Option<(&'network str, Value)> {
    let name = network.metadata.name.as_deref()?;
    let conditions = if let Some(listener) = listener {
        vec![condition(network, listener, now_rfc3339)?]
    } else {
        held(network)?;
        Vec::new()
    };
    Some((
        name,
        serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": name },
            "status": { "conditions": conditions }
        }),
    ))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;
    use crate::crd::grid_network::GridNetworkStatus;

    fn network() -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "grid", "generation": 3 },
            "spec": {}
        }))
        .expect("network")
    }

    #[test]
    fn nothing_is_said_when_no_listener_is_declared() {
        assert_eq!(Listener::of(None, SignalMode::Gossip), None);
        assert_eq!(Listener::of(None, SignalMode::Poll), None);
    }

    #[test]
    fn a_declared_listener_is_absent_under_gossip_whatever_its_task_reports() {
        for served in [Served::Starting, Served::AwaitingTls, Served::Serving] {
            assert_eq!(Listener::of(Some(&served), SignalMode::Gossip), Some(Listener::Absent));
        }
        let absent = condition(&network(), &Listener::Absent, "t").expect("written");
        assert_eq!(
            (absent.status, absent.reason.as_str()),
            (ConditionStatus::False, SIGNALS_LISTENER_ABSENT)
        );
        assert!(absent.message.contains("gossip"), "{}", absent.message);
    }

    #[test]
    fn under_poll_the_condition_reads_what_the_task_serves_not_the_config() {
        let serving = Listener::of(Some(&Served::Serving), SignalMode::Poll).expect("declared");
        let serving = condition(&network(), &serving, "t").expect("written");
        assert_eq!(
            (serving.status, serving.reason.as_str()),
            (ConditionStatus::True, SIGNALS_LISTENER_SERVING)
        );
        for (served, reason, detail) in [
            (Served::Starting, SIGNALS_LISTENER_STARTING, "not bound"),
            (Served::AwaitingTls, SIGNALS_LISTENER_TLS_UNAVAILABLE, "TLS material"),
            (
                Served::BindFailed("address in use".to_owned()),
                SIGNALS_LISTENER_BIND_FAILED,
                "address in use",
            ),
        ] {
            let listener = Listener::of(Some(&served), SignalMode::Poll).expect("declared");
            let written = condition(&network(), &listener, "t").expect("written");
            assert_eq!(
                (written.status, written.reason.as_str()),
                (ConditionStatus::False, reason),
                "{served:?}"
            );
            assert!(written.message.contains(detail), "{served:?}: {}", written.message);
        }
    }

    #[test]
    fn an_unchanged_condition_is_not_rewritten_and_a_flip_moves_the_transition() {
        let mut live = network();
        let written = condition(&live, &Listener::Absent, "t0").expect("written");
        live.status = Some(GridNetworkStatus {
            conditions: vec![written],
            ..GridNetworkStatus::default()
        });
        assert!(condition(&live, &Listener::Absent, "t1").is_none(), "already says it");
        let flipped = condition(&live, &Listener::Serving, "t1").expect("flipped");
        assert_eq!(flipped.last_transition_time, "t1");
    }

    #[test]
    fn a_changed_cause_is_written_without_moving_the_transition() {
        let mut live = network();
        let written = condition(&live, &Listener::TlsUnavailable, "t0").expect("written");
        live.status = Some(GridNetworkStatus {
            conditions: vec![written],
            ..GridNetworkStatus::default()
        });
        let failed = Listener::BindFailed("address in use".to_owned());
        let rewritten = condition(&live, &failed, "t1").expect("the cause changed");
        assert_eq!(rewritten.reason, SIGNALS_LISTENER_BIND_FAILED);
        assert_eq!(rewritten.last_transition_time, "t0", "still False");
    }

    #[test]
    fn a_held_condition_is_removed_once_no_listener_is_declared() {
        let mut live = network();
        assert!(patch(&live, None, "t0").is_none(), "nothing held, nothing written");
        let written = condition(&live, &Listener::Serving, "t0").expect("written");
        live.status = Some(GridNetworkStatus {
            conditions: vec![written],
            ..GridNetworkStatus::default()
        });
        let (name, cleared) = patch(&live, None, "t1").expect("the stale condition is removed");
        assert_eq!(name, "grid");
        assert_eq!(
            cleared.pointer("/status/conditions"),
            Some(&serde_json::json!([])),
            "an empty condition set under the field manager removes what it wrote"
        );
    }
}
