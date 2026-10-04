//! The HTTP surface: fleet JSON, the SSE stream, health, metrics, and the
//! embedded SPA.

mod assets;
mod routes;
mod sse;
#[cfg(test)]
mod testing;

pub use assets::Assets;
pub use routes::router;

/// Resolves once `shutting_down` turns true. A dropped sender means nobody
/// will ever ask, so this then waits forever rather than ending streams.
pub async fn shutdown_requested(mut shutting_down: tokio::sync::watch::Receiver<bool>) {
    while !*shutting_down.borrow_and_update() {
        if shutting_down.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}
