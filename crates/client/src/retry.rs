//! What a failed request was, and whether another attempt could differ.

/// The shape of a request's outcome, as a retry decision needs it.
///
/// Closed on purpose. A new failure mode gets a variant and an explicit
/// verdict rather than falling into a catch-all that retries it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestOutcome {
    /// The peer answered.
    Ok,
    /// No answer inside the deadline.
    Timeout,
    /// The connection was refused or never established.
    Refused,
    /// The peer's certificate or chain was not accepted.
    Tls,
    /// The connection broke, or the body did not arrive whole.
    Transport,
    /// The peer refused the request: 4xx other than 429.
    ClientError,
    /// The peer asked for less: 429.
    Throttled,
    /// The peer failed to serve it: 5xx.
    ServerError,
    /// The answer arrived but did not parse.
    Encoding,
    /// This end is misconfigured.
    Config,
    /// The caller gave up, or shutdown fired.
    Cancelled,
}

impl RequestOutcome {
    /// Whether another attempt could plausibly differ.
    ///
    /// The default is no. An outcome is retryable only where something changes
    /// between attempts: a peer that may finish, recover, or free capacity.
    /// Retrying a trust failure or a refusal spends the budget a transient
    /// failure needs, and turns one misconfiguration into a burst against a
    /// peer that already said no.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        match self {
            Self::Timeout | Self::Refused | Self::Transport | Self::ServerError | Self::Throttled => true,
            Self::Ok | Self::Tls | Self::ClientError | Self::Encoding | Self::Config | Self::Cancelled => false,
        }
    }

    /// The outcome as a bounded metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Timeout => "timeout",
            Self::Refused => "refused",
            Self::Tls => "tls",
            Self::Transport => "transport",
            Self::ClientError => "client_error",
            Self::Throttled => "throttled",
            Self::ServerError => "server_error",
            Self::Encoding => "encoding",
            Self::Config => "config",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Classify a response status the caller read itself.
///
/// A redirect is a misconfiguration rather than an answer, because the clients
/// this crate builds refuse to follow one, so the caller is holding a 3xx that
/// nothing will resolve.
#[must_use]
pub fn from_status(status: http::StatusCode) -> RequestOutcome {
    if status == http::StatusCode::TOO_MANY_REQUESTS {
        return RequestOutcome::Throttled;
    }
    if status.is_client_error() {
        return RequestOutcome::ClientError;
    }
    if status.is_server_error() {
        return RequestOutcome::ServerError;
    }
    if status.is_redirection() {
        return RequestOutcome::Config;
    }
    if status.is_success() {
        return RequestOutcome::Ok;
    }
    // 1xx as a final status is not an answer: the exchange did not complete.
    RequestOutcome::Transport
}

/// Classify a reqwest failure.
///
/// reqwest exposes no discriminator for a trust failure, so a rejected chain
/// arrives as a connect failure and reads as [`RequestOutcome::Refused`], which
/// retries. A caller that knows the handshake failed reports [`RequestOutcome::Tls`].
#[cfg(feature = "reqwest-client")]
#[must_use]
pub fn classify(error: &reqwest::Error) -> RequestOutcome {
    if error.is_timeout() {
        return RequestOutcome::Timeout;
    }
    if let Some(status) = error.status() {
        return from_status(status);
    }
    if error.is_connect() {
        return RequestOutcome::Refused;
    }
    if error.is_decode() {
        return RequestOutcome::Encoding;
    }
    if error.is_builder() {
        return RequestOutcome::Config;
    }
    RequestOutcome::Transport
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    /// Every variant, so a new one cannot be added without a verdict here.
    const ALL: [RequestOutcome; 11] = [
        RequestOutcome::Ok,
        RequestOutcome::Timeout,
        RequestOutcome::Refused,
        RequestOutcome::Tls,
        RequestOutcome::Transport,
        RequestOutcome::ClientError,
        RequestOutcome::Throttled,
        RequestOutcome::ServerError,
        RequestOutcome::Encoding,
        RequestOutcome::Config,
        RequestOutcome::Cancelled,
    ];

    #[test]
    fn nothing_transient_is_refused_and_nothing_permanent_retries() {
        for outcome in ALL {
            let expected = matches!(
                outcome,
                RequestOutcome::Timeout
                    | RequestOutcome::Refused
                    | RequestOutcome::Transport
                    | RequestOutcome::ServerError
                    | RequestOutcome::Throttled
            );
            assert_eq!(outcome.is_retryable(), expected, "{}", outcome.as_str());
        }
    }

    /// `code` as a status, for a table that reads in numbers.
    fn status(code: u16) -> http::StatusCode {
        http::StatusCode::from_u16(code).expect("a valid status")
    }

    #[test]
    fn throttling_retries_where_the_rest_of_4xx_does_not() {
        assert_eq!(from_status(status(429)), RequestOutcome::Throttled);
        assert!(from_status(status(429)).is_retryable());
        assert_eq!(from_status(status(403)), RequestOutcome::ClientError);
        assert!(!from_status(status(403)).is_retryable());
    }

    #[test]
    fn status_classes_map_to_one_outcome_each() {
        assert_eq!(from_status(status(200)), RequestOutcome::Ok);
        assert_eq!(from_status(status(400)), RequestOutcome::ClientError);
        assert_eq!(from_status(status(500)), RequestOutcome::ServerError);
        assert_eq!(from_status(status(503)), RequestOutcome::ServerError);
    }

    #[test]
    fn a_redirect_is_not_an_answer_because_the_client_will_not_follow_it() {
        assert_eq!(from_status(status(302)), RequestOutcome::Config);
        assert!(!from_status(status(302)).is_retryable());
    }

    #[test]
    fn an_informational_status_is_not_a_server_error() {
        assert_eq!(from_status(status(100)), RequestOutcome::Transport);
    }

    #[test]
    fn every_label_is_distinct() {
        let mut labels = ALL.map(RequestOutcome::as_str).to_vec();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(
            labels.len(),
            ALL.len(),
            "a duplicate label merges two outcomes in a metric"
        );
    }
}
