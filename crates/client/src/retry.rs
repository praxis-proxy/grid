//! What a failed request was, and whether another attempt could differ.

/// The shape of a request's outcome, closed so a new failure mode gets an
/// explicit verdict rather than a catch-all that retries it.
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
    /// Whether another attempt could plausibly differ, defaulting to no
    /// because retrying a refusal spends a transient failure's budget.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        match self {
            Self::Timeout | Self::Refused | Self::Transport | Self::ServerError => true,
            Self::Ok
            | Self::Tls
            | Self::ClientError
            | Self::Throttled
            | Self::Encoding
            | Self::Config
            | Self::Cancelled => false,
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

/// Classify a response status the caller read itself, counting a redirect as
/// a misconfiguration because no caller here follows one.
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
#[cfg(feature = "reqwest-client")]
#[must_use]
pub fn classify(error: &reqwest::Error) -> RequestOutcome {
    if error.is_timeout() && !error.is_connect() {
        return RequestOutcome::Timeout;
    }
    if let Some(status) = error.status() {
        return from_status(status);
    }
    // Before the connect check, because a rejected chain arrives as one and
    // the two call for opposite responses.
    if is_tls_failure(error) {
        return RequestOutcome::Tls;
    }
    if error.is_connect() {
        return RequestOutcome::Refused;
    }
    if error.is_decode() {
        return RequestOutcome::Encoding;
    }
    // A redirect error only reaches here for a caller that set a redirect
    // limit; the clients built here hand the 3xx back instead.
    if error.is_builder() || error.is_redirect() {
        return RequestOutcome::Config;
    }
    RequestOutcome::Transport
}

/// Whether `error`'s chain holds a TLS failure.
///
/// The chain is walked rather than the message matched, because an error
/// string is not an interface.
#[cfg(feature = "reqwest-client")]
fn is_tls_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(error), |&err| {
        // io::Error::source skips its wrapped error.
        err.downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .map(|inner| -> &(dyn std::error::Error + 'static) { inner })
            .or_else(move || err.source())
    })
    .any(is_tls_error)
}

/// Whether `error` is a rustls error.
#[cfg(all(feature = "reqwest-client", not(feature = "fips")))]
fn is_tls_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error.is::<rustls::Error>()
}

/// Whether `error` is an openssl error.
#[cfg(all(feature = "reqwest-client", feature = "fips"))]
fn is_tls_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error.is::<openssl::ssl::Error>() || error.is::<openssl::error::ErrorStack>()
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
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
            );
            assert_eq!(outcome.is_retryable(), expected, "{}", outcome.as_str());
        }
    }

    /// `code` as a status, for a table that reads in numbers.
    fn status(code: u16) -> http::StatusCode {
        http::StatusCode::from_u16(code).expect("a valid status")
    }

    #[test]
    fn throttling_is_told_apart_from_a_refusal_without_being_retried() {
        // Retrying a 429 on a schedule the peer did not ask for amplifies
        // against it. Honouring Retry-After is what would earn the retry.
        assert_eq!(from_status(status(429)), RequestOutcome::Throttled);
        assert!(!from_status(status(429)).is_retryable());
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
