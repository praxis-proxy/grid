//! Shared response reader for the mTLS scrape path.
//!
//! Validates the status, parses the `Date` as the freshness reference, and reads
//! the body under a byte ceiling and a time bound, assembling a [`Scrape`]. The
//! 1 MiB ceiling and the `Date`-as-freshness rule live here so the connector does
//! not restate them.

use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use http::header;
use http_body_util::{BodyExt as _, Limited};

use crate::poller::{FetchError, Scrape};

/// Response-body ceiling. An unbounded signals read is the poller's biggest
/// denial-of-service exposure, and the store's per-series caps do not bound the
/// ingest read loop.
const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;

/// Validate status, parse the `Date`, and bounded-read the body into a [`Scrape`],
/// stamping the caller's verified `peer_identity`.
///
/// # Errors
///
/// [`FetchError::Unreachable`] on a non-success status, a body read past the time
/// bound, or a non-UTF-8 body; [`FetchError::TooLarge`] past the ceiling;
/// [`FetchError::NoDate`] when the response carries no usable `Date`.
#[expect(
    clippy::too_many_lines,
    reason = "sequential status check, Date parse, and bounded body read"
)]
pub(crate) async fn read_scrape_response(
    response: http::Response<hyper::body::Incoming>,
    peer_identity: Arc<str>,
    read_timeout: Duration,
) -> Result<Scrape, FetchError> {
    if !response.status().is_success() {
        return Err(FetchError::Unreachable(format!(
            "non-success status {}",
            response.status()
        )));
    }

    let date_ms = response
        .headers()
        .get(header::DATE)
        .and_then(|value| value.to_str().ok())
        .and_then(|text| httpdate::parse_http_date(text).ok())
        .and_then(|when| when.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| i64::try_from(since.as_millis()).ok())
        .ok_or(FetchError::NoDate)?;

    // Bound the body read in time, not just in bytes: a peer that sends headers
    // then dribbles the body would otherwise hold the poll open indefinitely
    // (slowloris), and because poll_once is awaited inline it would also stall the
    // loop's stop/drain.
    let collected = tokio::time::timeout(
        read_timeout,
        Limited::new(response.into_body(), MAX_RESPONSE_BODY_BYTES).collect(),
    )
    .await
    .map_err(|_elapsed| FetchError::Unreachable("body read timed out".to_owned()))?
    .map_err(|error| {
        if error.downcast_ref::<http_body_util::LengthLimitError>().is_some() {
            FetchError::TooLarge {
                limit: MAX_RESPONSE_BODY_BYTES,
            }
        } else {
            FetchError::Unreachable(error.to_string())
        }
    })?
    .to_bytes();

    let body = String::from_utf8(Vec::from(collected))
        .map_err(|error| FetchError::Unreachable(format!("non-utf8 body: {error}")))?;

    Ok(Scrape {
        body,
        date_ms,
        peer_identity,
    })
}
