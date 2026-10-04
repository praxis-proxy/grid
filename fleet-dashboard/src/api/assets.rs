//! The embedded SPA: built assets with a fallback to `index.html` for client
//! side routes.

use axum::{
    body::Body,
    extract::State,
    http::{
        HeaderValue, StatusCode, Uri,
        header::{CACHE_CONTROL, CONTENT_TYPE},
    },
    response::{IntoResponse as _, Response},
};

use super::routes::AppState;

/// The table `build.rs` generates from `webui/dist`.
mod generated {
    include!(concat!(env!("OUT_DIR"), "/assets.rs"));
}

/// Hashed assets never change under the same name.
const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// Pages are always fetched fresh.
const NO_STORE: &str = "no-store";

/// The built web UI as served from memory.
#[derive(Debug, Clone, Copy)]
pub struct Assets {
    /// `(path, bytes)` for every file.
    entries: &'static [(&'static str, &'static [u8])],
}

impl Assets {
    /// Serves `entries`, keyed by path relative to the web root.
    #[must_use]
    pub const fn new(entries: &'static [(&'static str, &'static [u8])]) -> Self {
        Self { entries }
    }

    /// The web UI built into this binary.
    #[must_use]
    pub fn embedded() -> Self {
        Self::new(generated::ASSETS)
    }

    /// The file at `path`, when there is one.
    #[must_use]
    pub fn lookup(&self, path: &str) -> Option<&'static [u8]> {
        self.entries
            .iter()
            .find(|(name, _)| *name == path)
            .map(|(_, bytes)| *bytes)
    }

    /// The file for `uri`, or `index.html` for anything else: client-side
    /// routes such as `/?site=name`, and directory paths such as `/assets/`
    /// that would otherwise list. The path is matched as sent, undecoded, so
    /// nothing outside the table is reachable.
    fn respond(&self, uri: &Uri) -> Response {
        let path = uri.path().trim_start_matches('/');
        let name = if path.is_empty() { "index.html" } else { path };
        if !name.split('/').any(|segment| segment == "..")
            && let Some(bytes) = self.lookup(name)
        {
            return file(name, bytes, cache_control(name));
        }
        file(
            "index.html",
            self.lookup("index.html").unwrap_or_default(),
            Some(NO_STORE),
        )
    }
}

/// Serves the embedded UI; the router's fallback.
pub(super) async fn serve(State(state): State<AppState>, uri: Uri) -> Response {
    state.assets.respond(&uri)
}

/// Hashed assets are immutable, pages are never cached, the rest is left to
/// the browser.
fn cache_control(name: &str) -> Option<&'static str> {
    if name.starts_with("assets/") {
        Some(IMMUTABLE)
    } else if name.ends_with(".html") {
        Some(NO_STORE)
    } else {
        None
    }
}

/// The media type for a file name, by extension.
fn content_type(name: &str) -> &'static str {
    match name.rsplit_once('.').map(|(_, extension)| extension) {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json" | "map") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("webp") => "image/webp",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// A 200 with the file's media type and cache policy.
fn file(name: &str, bytes: &'static [u8], cache: Option<&'static str>) -> Response {
    let mut response = (
        StatusCode::OK,
        [(CONTENT_TYPE, HeaderValue::from_static(content_type(name)))],
        Body::from(bytes),
    )
        .into_response();
    if let Some(policy) = cache {
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static(policy));
    }
    response
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use http::StatusCode;
    use tower::ServiceExt as _;

    use super::Assets;
    use crate::api::testing::{app, body_bytes, get};

    const IMMUTABLE: &str = "public, max-age=31536000, immutable";

    #[tokio::test]
    async fn the_index_and_deep_links_serve_the_spa_shell_uncached() {
        let (app, _collector) = app(true).await;
        for path in ["/", "/some/deep/route", "/?site=spoke1"] {
            let response = app.clone().oneshot(get(path)).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(
                response.headers()["cache-control"],
                "no-store",
                "{path}: the shell is never cached"
            );
            assert!(
                response.headers()["content-type"]
                    .to_str()
                    .unwrap()
                    .starts_with("text/html"),
                "{path}"
            );
            assert!(
                body_bytes(response).await.starts_with(b"<html>spa"),
                "{path}: serves the shell"
            );
        }
    }

    #[tokio::test]
    async fn hashed_assets_are_immutable_and_typed() {
        let (app, _collector) = app(true).await;
        let response = app.oneshot(get("/assets/app.js")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], IMMUTABLE);
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/javascript")
        );
        assert_eq!(body_bytes(response).await, b"console.log(1)");
    }

    #[tokio::test]
    async fn a_directory_path_under_assets_falls_back_without_the_immutable_header() {
        let (app, _collector) = app(true).await;
        let response = app.oneshot(get("/assets/")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_ne!(
            response.headers()["cache-control"],
            IMMUTABLE,
            "the fallback must not be cached as an asset"
        );
        assert!(
            body_bytes(response).await.starts_with(b"<html>spa"),
            "no directory listing, the shell instead"
        );
    }

    #[tokio::test]
    async fn a_traversal_attempt_cannot_escape_the_table() {
        let (app, _collector) = app(true).await;
        for path in ["/../../etc/passwd", "/assets/../index.html", "/assets/%2e%2e/x"] {
            let response = app.clone().oneshot(get(path)).await.unwrap();
            assert!(
                body_bytes(response).await.starts_with(b"<html>spa"),
                "{path}: anything outside the table is the shell"
            );
        }
    }

    #[test]
    fn the_embedded_table_always_has_an_index() {
        assert!(
            Assets::embedded().lookup("index.html").is_some(),
            "a build without the web UI still serves a page"
        );
    }
}
