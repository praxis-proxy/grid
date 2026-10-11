//! What a failed request was, and whether another attempt could differ.

/// The shape of a request's outcome, as a retry decision needs it.
///
/// Closed on purpose. A new failure mode gets a variant and an explicit
/// verdict rather than falling into a catch-all that retries it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
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

impl Outcome {
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
#[must_use]
pub const fn from_status(status: u16) -> Outcome {
    match status {
        200..=399 => Outcome::Ok,
        429 => Outcome::Throttled,
        400..=499 => Outcome::ClientError,
        _ => Outcome::ServerError,
    }
}

/// Classify a reqwest failure.
///
/// reqwest exposes no discriminator for a trust failure, so a rejected chain
/// arrives as a connect failure and reads as [`Outcome::Refused`], which
/// retries. A caller that knows the handshake failed reports [`Outcome::Tls`].
#[cfg(feature = "reqwest-client")]
#[must_use]
pub fn classify(error: &reqwest::Error) -> Outcome {
    if error.is_timeout() {
        return Outcome::Timeout;
    }
    if let Some(status) = error.status() {
        return from_status(status.as_u16());
    }
    if error.is_connect() {
        return Outcome::Refused;
    }
    if error.is_decode() {
        return Outcome::Encoding;
    }
    if error.is_builder() {
        return Outcome::Config;
    }
    Outcome::Transport
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    /// Every variant, so a new one cannot be added without a verdict here.
    const ALL: [Outcome; 11] = [
        Outcome::Ok,
        Outcome::Timeout,
        Outcome::Refused,
        Outcome::Tls,
        Outcome::Transport,
        Outcome::ClientError,
        Outcome::Throttled,
        Outcome::ServerError,
        Outcome::Encoding,
        Outcome::Config,
        Outcome::Cancelled,
    ];

    #[test]
    fn nothing_transient_is_refused_and_nothing_permanent_retries() {
        for outcome in ALL {
            let expected = matches!(
                outcome,
                Outcome::Timeout | Outcome::Refused | Outcome::Transport | Outcome::ServerError | Outcome::Throttled
            );
            assert_eq!(outcome.is_retryable(), expected, "{}", outcome.as_str());
        }
    }

    #[test]
    fn throttling_retries_where_the_rest_of_4xx_does_not() {
        assert_eq!(from_status(429), Outcome::Throttled);
        assert!(from_status(429).is_retryable());
        assert_eq!(from_status(403), Outcome::ClientError);
        assert!(!from_status(403).is_retryable());
    }

    #[test]
    fn status_classes_map_to_one_outcome_each() {
        assert_eq!(from_status(200), Outcome::Ok);
        assert_eq!(from_status(302), Outcome::Ok);
        assert_eq!(from_status(400), Outcome::ClientError);
        assert_eq!(from_status(500), Outcome::ServerError);
        assert_eq!(from_status(503), Outcome::ServerError);
    }

    #[test]
    fn every_label_is_distinct() {
        let mut labels = ALL.map(Outcome::as_str).to_vec();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(
            labels.len(),
            ALL.len(),
            "a duplicate label merges two outcomes in a metric"
        );
    }
}
