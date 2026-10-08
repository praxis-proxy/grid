//! The `/api/v1/stream` server-sent events feed of fleet snapshots.

use std::{sync::Arc, time::Duration};

use axum::{
    extract::State,
    http::{HeaderValue, header::CACHE_CONTROL},
    response::{
        IntoResponse as _, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures::StreamExt as _;
use tokio::sync::watch;

use super::{routes::AppState, shutdown_requested};
use crate::model::FleetSnapshot;

/// Comment frames this often so proxies keep the connection open.
const HEARTBEAT: Duration = Duration::from_secs(15);

/// Sends the current snapshot immediately, then every new one, until the
/// client leaves or the server shuts down. The current value and the change
/// marker are read in one step, so a poll landing while the stream starts is
/// neither lost nor sent twice. The headers stop reverse proxies from
/// buffering or caching the frames.
pub(super) async fn stream(State(state): State<AppState>) -> Response {
    let mut updates = state.collector.subscribe();
    let current = updates.borrow_and_update().clone();
    let frames = futures::stream::iter([current])
        .chain(futures::stream::unfold(updates, next))
        .filter_map(futures::future::ready)
        .map(|snapshot| Ok::<_, std::convert::Infallible>(frame(&snapshot)))
        .take_until(shutdown_requested(state.shutting_down));
    let mut response = Sse::new(frames)
        .keep_alive(KeepAlive::new().interval(HEARTBEAT).text("ping"))
        .into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

/// The next published snapshot; ends when the collector is gone.
async fn next(
    mut updates: watch::Receiver<Option<Arc<FleetSnapshot>>>,
) -> Option<(Option<Arc<FleetSnapshot>>, watch::Receiver<Option<Arc<FleetSnapshot>>>)> {
    updates.changed().await.ok()?;
    let latest = updates.borrow_and_update().clone();
    Some((latest, updates))
}

/// One `fleet` event carrying the snapshot as JSON.
fn frame(snapshot: &FleetSnapshot) -> Event {
    Event::default()
        .event("fleet")
        .data(serde_json::to_string(snapshot).unwrap_or_default())
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
#[expect(clippy::indexing_slicing, reason = "test assertions on known headers")]
mod tests {
    use std::time::Duration;

    use axum::body::Body;
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    use crate::api::testing::{app, app_with_shutdown, get};

    #[tokio::test]
    async fn the_stream_sends_the_current_snapshot_then_each_new_one() {
        let (app, collector) = app(true).await;
        let response = app.oneshot(get("/api/v1/stream")).await.unwrap();
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/event-stream")
        );
        let mut body = response.into_body();
        let first = next_event(&mut body).await;
        assert!(
            first.contains(r#""name":"spoke1""#),
            "the current snapshot is sent immediately: {first}"
        );
        collector.poll().await;
        let second = next_event(&mut body).await;
        let (first, second): (serde_json::Value, serde_json::Value) = (
            serde_json::from_str(&first).unwrap(),
            serde_json::from_str(&second).unwrap(),
        );
        assert!(
            second["generatedAt"].as_str() > first["generatedAt"].as_str(),
            "each new poll is a new frame"
        );
    }

    #[tokio::test]
    async fn the_stream_waits_for_the_first_poll_when_there_is_no_snapshot_yet() {
        let (app, collector) = app(false).await;
        let response = app.oneshot(get("/api/v1/stream")).await.unwrap();
        let mut body = response.into_body();
        collector.poll().await;
        let first = next_event(&mut body).await;
        assert!(
            first.contains(r#""name":"spoke1""#),
            "the first poll becomes the first frame: {first}"
        );
    }

    #[tokio::test]
    async fn the_stream_disables_proxy_buffering_and_caching() {
        let (app, _collector) = app(true).await;
        let response = app.oneshot(get("/api/v1/stream")).await.unwrap();
        assert_eq!(
            response.headers()["x-accel-buffering"],
            "no",
            "nginx must not buffer live frames"
        );
        assert_eq!(
            response.headers()["cache-control"],
            "no-store",
            "a proxy must not cache the stream"
        );
    }

    #[tokio::test]
    async fn the_stream_ends_when_shutdown_is_signalled() {
        let (app, _collector, shutdown) = app_with_shutdown(true).await;
        let response = app.oneshot(get("/api/v1/stream")).await.unwrap();
        let mut body = response.into_body();
        next_event(&mut body).await;
        shutdown.send_replace(true);
        let end = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(frame) = body.frame().await {
                frame.unwrap();
            }
        })
        .await;
        assert!(end.is_ok(), "the stream must end so graceful shutdown can finish");
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// The `data:` payload of the next `fleet` event.
    async fn next_event(body: &mut Body) -> String {
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut text = String::new();
            loop {
                let frame = body.frame().await.unwrap().unwrap();
                text.push_str(&String::from_utf8_lossy(frame.data_ref().unwrap()));
                if let Some((event, _)) = text.split_once("\n\n") {
                    let data = event
                        .lines()
                        .find_map(|line| line.strip_prefix("data: "))
                        .unwrap()
                        .to_owned();
                    assert!(event.contains("event: fleet"), "{event}");
                    return data;
                }
            }
        })
        .await
        .unwrap()
    }
}
