//! Where a site's metrics come from: its own Prometheus with its own token,
//! or one central store scoped by a cluster label.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;

use super::{Client, Matrix, MetricsError, Vector, Window, build_http_client, inject_matcher};
use crate::{demo, queries::Query, registry::Site};

/// The bearer token and CA bundle for one site's Prometheus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteSecret {
    /// Bearer token; empty means the Secret is unusable.
    pub token: String,
    /// PEM CA bundle; empty means trust the platform roots only.
    pub ca_pem: Vec<u8>,
}

/// Read access to site Secrets by name. Synchronous so it is object-safe and
/// never holds a lock across an await.
pub trait SecretReader: Send + Sync {
    /// The Secret called `name`, when it exists.
    fn secret(&self, name: &str) -> Option<SiteSecret>;
}

/// Where site metrics are read from.
pub enum Source {
    /// Query each site's own Prometheus with that site's token.
    PerSite(PerSiteSource),
    /// Query one central store, scoping by cluster label.
    Central(CentralSource),
    /// Synthesize a fleet; no network at all.
    Demo,
}

impl Source {
    /// Evaluates `query` for `site` at `at`.
    ///
    /// # Errors
    ///
    /// Site configuration errors, or any failure from the underlying client.
    pub async fn query(&self, site: &Site, query: &Query, at: OffsetDateTime) -> Result<Vector, MetricsError> {
        match self {
            Self::PerSite(source) => source.client_for(site)?.query(&query.promql, at).await,
            Self::Central(source) => source.client.query(&source.scoped(site, query)?, at).await,
            Self::Demo => demo::query(site, query, at),
        }
    }

    /// Evaluates `query` for `site` over `window`.
    ///
    /// # Errors
    ///
    /// Site configuration errors, or any failure from the underlying client.
    pub async fn query_range(&self, site: &Site, query: &Query, window: Window) -> Result<Matrix, MetricsError> {
        match self {
            Self::PerSite(source) => source.client_for(site)?.query_range(&query.promql, window).await,
            Self::Central(source) => source.client.query_range(&source.scoped(site, query)?, window).await,
            Self::Demo => demo::query_range(site, query, window),
        }
    }
}

/// Talks to each site's own Prometheus using that site's token.
pub struct PerSiteSource {
    /// Where tokens and CA bundles come from.
    secrets: Arc<dyn SecretReader>,
    /// Per-request timeout for every transport built here.
    timeout: Duration,
    /// Transports keyed by CA digest, so sites sharing a CA share one.
    clients: Mutex<HashMap<[u8; 32], reqwest::Client>>,
}

impl PerSiteSource {
    /// A source reading tokens and CAs from `secrets`.
    #[must_use]
    pub fn new(secrets: Arc<dyn SecretReader>, timeout: Duration) -> Self {
        Self {
            secrets,
            timeout,
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// A client for `site`, built from its registry entry and Secret.
    fn client_for(&self, site: &Site) -> Result<Client, MetricsError> {
        if site.metrics_url.is_empty() {
            return Err(MetricsError::NoMetricsUrl {
                site: site.name.clone(),
            });
        }
        let secret = self
            .secrets
            .secret(&site.metrics_secret)
            .filter(|secret| !secret.token.is_empty())
            .ok_or_else(|| MetricsError::NoSecret {
                site: site.name.clone(),
                secret: site.metrics_secret.clone(),
            })?;
        Ok(Client::new(
            &site.metrics_url,
            &secret.token,
            self.transport(&secret.ca_pem)?,
        ))
    }

    /// The shared transport for `ca_pem`, built on first use. The build runs
    /// outside the lock; a concurrent builder for the same CA loses the race
    /// harmlessly and adopts the stored transport.
    fn transport(&self, ca_pem: &[u8]) -> Result<reqwest::Client, MetricsError> {
        let key: [u8; 32] = Sha256::digest(ca_pem).into();
        let cached = self
            .clients
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .cloned();
        if let Some(http) = cached {
            return Ok(http);
        }
        let http = build_http_client((!ca_pem.is_empty()).then_some(ca_pem), self.timeout)?;
        Ok(self
            .clients
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key)
            .or_insert(http)
            .clone())
    }

    /// How many distinct transports have been built.
    #[cfg(test)]
    fn cached_clients(&self) -> usize {
        self.clients.lock().unwrap_or_else(PoisonError::into_inner).len()
    }
}

/// Sends every query to one store, scoped by a cluster label.
pub struct CentralSource {
    /// The central store.
    client: Client,
    /// The label that identifies a site in the store's series.
    label: String,
}

impl CentralSource {
    /// A source for `client`, scoping queries by `cluster_label`.
    #[must_use]
    pub fn new(client: Client, cluster_label: &str) -> Self {
        Self {
            client,
            label: cluster_label.to_owned(),
        }
    }

    /// `query` restricted to `site`'s series.
    fn scoped(&self, site: &Site, query: &Query) -> Result<String, MetricsError> {
        inject_matcher(&query.promql, &self.label, &site.cluster_value)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use std::time::Duration;

    use time::OffsetDateTime;

    use super::{CentralSource, PerSiteSource, Source};
    use crate::{
        metrics::{
            Client, MetricsError, build_http_client,
            testing::{FakePrometheus, TOKEN, install_provider, secrets},
        },
        queries::Query,
        registry::Site,
    };

    const TIMEOUT: Duration = Duration::from_secs(5);
    const UP: &str = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"1"]}]}}"#;

    #[tokio::test]
    async fn per_site_queries_use_the_site_url_and_its_secret_token() {
        let base = FakePrometheus::new().vector("up", UP).serve().await;
        let source = per_site(&[("site-a", TOKEN, &[])]);
        let vector = source
            .query(&site("a", &base, "site-a"), &query("up"), now())
            .await
            .unwrap();
        assert_eq!(vector.len(), 1, "{vector:?}");
    }

    #[tokio::test]
    async fn a_site_without_a_metrics_url_is_an_error() {
        let err = per_site(&[])
            .query(&site("a", "", "site-a"), &query("up"), now())
            .await
            .unwrap_err();
        assert!(matches!(err, MetricsError::NoMetricsUrl { .. }), "{err}");
    }

    #[tokio::test]
    async fn a_missing_or_empty_secret_is_an_error() {
        let source = per_site(&[("site-b", "", &[])]);
        for secret in ["site-a", "site-b"] {
            let err = source
                .query(&site("a", "http://unused", secret), &query("up"), now())
                .await
                .unwrap_err();
            assert!(matches!(err, MetricsError::NoSecret { .. }), "{secret}: {err}");
        }
    }

    #[test]
    fn sites_sharing_a_ca_share_one_http_client() {
        install_provider();
        let ca = self_signed_ca();
        let source = PerSiteSource::new(
            secrets(&[
                ("site-a", TOKEN, ca.as_bytes()),
                ("site-b", TOKEN, ca.as_bytes()),
                ("site-c", TOKEN, &[]),
            ]),
            TIMEOUT,
        );
        for name in ["a", "b", "c"] {
            source
                .client_for(&site(name, "https://unused", &format!("site-{name}")))
                .unwrap();
        }
        assert_eq!(
            source.cached_clients(),
            2,
            "one transport per distinct CA, the empty CA included"
        );
    }

    #[tokio::test]
    async fn central_mode_scopes_every_query_by_cluster_label() {
        let base = FakePrometheus::new()
            .vector(r#"up{cluster="spoke1"}"#, UP)
            .serve()
            .await;
        let source = Source::Central(CentralSource::new(Client::new(&base, TOKEN, http()), "cluster"));
        let vector = source
            .query(&site("spoke1", "", ""), &query("up"), now())
            .await
            .unwrap();
        assert_eq!(vector.len(), 1, "{vector:?}");
    }

    #[tokio::test]
    async fn the_site_ca_from_its_secret_is_trusted_and_a_missing_ca_is_not() {
        let (base, ca) = FakePrometheus::new().vector("up", UP).serve_tls().await;
        let trusted = per_site(&[("site-a", TOKEN, ca.as_bytes())]);
        trusted
            .query(&site("a", &base, "site-a"), &query("up"), now())
            .await
            .unwrap();
        let untrusted = per_site(&[("site-a", TOKEN, &[])]);
        let err = untrusted
            .query(&site("a", &base, "site-a"), &query("up"), now())
            .await
            .unwrap_err();
        assert!(
            matches!(err, MetricsError::Http(_)),
            "an unknown CA must fail verification: {err}"
        );
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    fn per_site(entries: &[(&str, &str, &[u8])]) -> Source {
        install_provider();
        Source::PerSite(PerSiteSource::new(secrets(entries), TIMEOUT))
    }

    fn site(name: &str, metrics_url: &str, metrics_secret: &str) -> Site {
        Site {
            name: name.to_owned(),
            cluster_value: name.to_owned(),
            metrics_url: metrics_url.to_owned(),
            metrics_secret: metrics_secret.to_owned(),
            ..Site::default()
        }
    }

    fn query(promql: &str) -> Query {
        Query {
            key: "k".to_owned(),
            promql: promql.to_owned(),
        }
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_757_160_000).unwrap()
    }

    fn http() -> reqwest::Client {
        install_provider();
        build_http_client(None, TIMEOUT).unwrap()
    }


    fn self_signed_ca() -> String {
        rcgen::generate_simple_self_signed(["localhost".to_owned()])
            .unwrap()
            .cert
            .pem()
    }
}
