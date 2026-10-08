//! Keeps the registry `ConfigMap` and the site Secrets in memory through
//! Kubernetes watches, so serving the fleet never hits the API server on the
//! request path.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, PoisonError, RwLock},
};

use futures::StreamExt as _;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{
    Api,
    runtime::{
        WatchStreamExt as _,
        watcher::{self, Event},
    },
};
use tokio::sync::{oneshot, watch};

use super::{Site, SiteList, build, parse_endpoints};
use crate::metrics::{SecretReader, SiteSecret};

/// Where Kubernetes mounts the pod's own namespace.
const NAMESPACE_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

/// Why the store could not start.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A watcher task ended before its first list completed.
    #[error("{kind} watcher exited before syncing")]
    WatcherExited {
        /// Which watcher.
        kind: &'static str,
    },
}

/// The registry and the site Secrets of one namespace, kept current by two
/// watches. Readers see the site list through a `watch` channel and Secrets
/// through [`SecretReader`].
pub struct RegistryStore {
    /// The API server.
    client: kube::Client,
    /// Namespace of the registry `ConfigMap` and the site Secrets.
    namespace: String,
    /// Name of the registry `ConfigMap`.
    configmap: String,
    /// Key inside the `ConfigMap` holding the file-discovery document.
    key: String,
    /// The published site list; also nudged when a Secret changes.
    sites: watch::Sender<SiteList>,
    /// Token and CA per Secret name.
    secrets: RwLock<BTreeMap<String, SiteSecret>>,
}

impl RegistryStore {
    /// A store that has not started watching yet.
    #[must_use]
    pub fn new(client: kube::Client, namespace: &str, configmap: &str, key: &str) -> Self {
        let (sites, _initial) = watch::channel(Arc::new(Vec::new()));
        Self {
            client,
            namespace: namespace.to_owned(),
            configmap: configmap.to_owned(),
            key: key.to_owned(),
            sites,
            secrets: RwLock::default(),
        }
    }

    /// The current site list, woken on every registry or Secret change.
    #[must_use]
    pub fn sites(&self) -> watch::Receiver<SiteList> {
        self.sites.subscribe()
    }

    /// Starts watching the registry `ConfigMap` and the namespace's Secrets and
    /// resolves once both have listed. The watches keep running on the
    /// runtime for the life of the process, retrying with backoff.
    ///
    /// # Errors
    ///
    /// [`StoreError::WatcherExited`] if a watcher task ends before syncing.
    pub async fn run(self: Arc<Self>) -> Result<(), StoreError> {
        let (configmaps_ready, configmaps_synced) = oneshot::channel();
        let (secrets_ready, secrets_synced) = oneshot::channel();
        tokio::spawn(Arc::clone(&self).watch_configmaps(configmaps_ready));
        tokio::spawn(Arc::clone(&self).watch_secrets(secrets_ready));
        tracing::info!(namespace = %self.namespace, configmap = %self.configmap, "waiting for registry cache sync");
        configmaps_synced
            .await
            .map_err(|_dropped| StoreError::WatcherExited { kind: "configmap" })?;
        secrets_synced
            .await
            .map_err(|_dropped| StoreError::WatcherExited { kind: "secret" })?;
        tracing::info!(sites = self.sites.borrow().len(), "registry cache synced");
        Ok(())
    }

    /// Follows the registry `ConfigMap`; `ready` fires after the first list.
    async fn watch_configmaps(self: Arc<Self>, ready: oneshot::Sender<()>) {
        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), &self.namespace);
        let config = watcher::Config::default().fields(&format!("metadata.name={}", self.configmap));
        let mut events = watcher::watcher(api, config).default_backoff().boxed();
        let mut ready = Some(ready);
        let mut listed: Option<ConfigMap> = None;
        while let Some(event) = events.next().await {
            match event {
                Ok(Event::Init) => listed = None,
                Ok(Event::InitApply(configmap)) => listed = Some(configmap),
                Ok(Event::InitDone) => {
                    self.apply_configmap(listed.take().as_ref());
                    signal(&mut ready);
                },
                Ok(Event::Apply(configmap)) => self.apply_configmap(Some(&configmap)),
                Ok(Event::Delete(_)) => self.apply_configmap(None),
                Err(err) => tracing::warn!(error = %err, "configmap watch error; retrying"),
            }
        }
    }

    /// Follows every Secret in the namespace; `ready` fires after the first
    /// list. A relist replaces the whole set, so a Secret deleted while the
    /// watch was down disappears too.
    async fn watch_secrets(self: Arc<Self>, ready: oneshot::Sender<()>) {
        let api: Api<Secret> = Api::namespaced(self.client.clone(), &self.namespace);
        let mut events = watcher::watcher(api, watcher::Config::default())
            .default_backoff()
            .boxed();
        let mut ready = Some(ready);
        let mut listed = BTreeMap::new();
        while let Some(event) = events.next().await {
            match event {
                Ok(Event::Init) => listed.clear(),
                Ok(Event::InitApply(secret)) => listed.extend(site_secret(&secret)),
                Ok(Event::InitDone) => {
                    self.replace_secrets(std::mem::take(&mut listed));
                    signal(&mut ready);
                },
                Ok(Event::Apply(secret)) => {
                    if let Some((name, value)) = site_secret(&secret) {
                        self.update_secret(name, Some(value));
                    }
                },
                Ok(Event::Delete(secret)) => {
                    if let Some(name) = secret.metadata.name {
                        self.update_secret(name, None);
                    }
                },
                Err(err) => tracing::warn!(error = %err, "secret watch error; retrying"),
            }
        }
    }

    /// Publishes the sites in `configmap` (none when it is absent) if they
    /// differ from the current list, so a relist never wakes the collector
    /// for nothing.
    fn apply_configmap(&self, configmap: Option<&ConfigMap>) {
        let sites = configmap.map_or_else(Vec::new, |configmap| self.sites_from(configmap));
        let count = sites.len();
        let changed = self.sites.send_if_modified(move |current| {
            if current.as_slice() == sites.as_slice() {
                false
            } else {
                *current = Arc::new(sites);
                true
            }
        });
        if changed {
            tracing::info!(sites = count, "registry updated");
        }
    }

    /// Parses the registry document, logging each skipped entry.
    fn sites_from(&self, configmap: &ConfigMap) -> Vec<Site> {
        let Some(raw) = configmap.data.as_ref().and_then(|data| data.get(&self.key)) else {
            tracing::info!(configmap = %self.configmap, key = %self.key, "registry key missing in configmap");
            return Vec::new();
        };
        let (specs, skipped) = parse_endpoints(raw.as_bytes());
        for reason in &skipped {
            tracing::info!(configmap = %self.configmap, error = %reason, "registry entry skipped");
        }
        build(&specs)
    }

    /// Replaces every Secret after a list, nudging the collector if anything
    /// differs.
    fn replace_secrets(&self, listed: BTreeMap<String, SiteSecret>) {
        let mut secrets = self.secrets.write().unwrap_or_else(PoisonError::into_inner);
        let changed = *secrets != listed;
        if changed {
            *secrets = listed;
        }
        drop(secrets);
        if changed {
            self.nudge();
        }
    }

    /// Inserts or removes one Secret, nudging the collector if that changed
    /// anything.
    fn update_secret(&self, name: String, value: Option<SiteSecret>) {
        let mut secrets = self.secrets.write().unwrap_or_else(PoisonError::into_inner);
        let changed = match value {
            Some(value) => secrets.insert(name, value.clone()).as_ref() != Some(&value),
            None => secrets.remove(&name).is_some(),
        };
        drop(secrets);
        if changed {
            self.nudge();
        }
    }

    /// Wakes the collector without changing the site list: a Secret appeared
    /// or changed, so a site that could not be queried may now be.
    fn nudge(&self) {
        self.sites.send_modify(|_sites| {});
    }
}

impl SecretReader for RegistryStore {
    fn secret(&self, name: &str) -> Option<SiteSecret> {
        self.secrets
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }
}

/// Fires `ready` the first time it is called.
fn signal(ready: &mut Option<oneshot::Sender<()>>) {
    if let Some(ready) = ready.take() {
        ready.send(()).unwrap_or_default();
    }
}

/// The token and CA of a Secret, keyed by its name.
fn site_secret(secret: &Secret) -> Option<(String, SiteSecret)> {
    let name = secret.metadata.name.clone()?;
    let data = secret.data.as_ref();
    let bytes = |key: &str| {
        data.and_then(|data| data.get(key))
            .map(|value| value.0.clone())
            .unwrap_or_default()
    };
    let token = String::from_utf8_lossy(&bytes("token")).into_owned();
    Some((
        name,
        SiteSecret {
            token,
            ca_pem: bytes("ca.crt"),
        },
    ))
}

/// The namespace this pod runs in, or `default` outside a cluster.
#[must_use]
pub fn default_namespace() -> String {
    namespace_from(Path::new(NAMESPACE_FILE))
}

/// The trimmed contents of the service-account namespace file at `path`, or
/// `default` when it is missing or empty.
fn namespace_from(path: &Path) -> String {
    std::fs::read_to_string(path)
        .ok()
        .map(|contents| contents.trim().to_owned())
        .filter(|namespace| !namespace.is_empty())
        .unwrap_or_else(|| "default".to_owned())
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
#[expect(clippy::indexing_slicing, reason = "test assertions on known site lists")]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use k8s_openapi::{
        ByteString,
        api::core::v1::{ConfigMap, Secret},
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };
    use tokio::sync::watch;

    use super::{RegistryStore, namespace_from};
    use crate::{
        metrics::{SecretReader as _, SiteSecret},
        registry::{
            Site, SiteList,
            testing::{FakeApi, NAMESPACE},
        },
    };

    const ONE_SITE: &str = "endpoints:\n  - name: spoke1\n    address: gw1\n    labels: {region: us-east-2, metricsURL: https://t1, metricsSecret: site-spoke1}\n";
    const TWO_SITES: &str = "endpoints:\n  - name: spoke1\n    address: gw1\n    labels: {region: us-east-2, metricsURL: https://t1, metricsSecret: site-spoke1}\n  - name: spoke2\n    address: gw2\n    labels: {region: us-west-2, metricsURL: https://t2, metricsSecret: site-spoke2}\n";

    #[tokio::test]
    async fn run_resolves_once_listed_and_publishes_the_sites() {
        let api = FakeApi::new();
        api.put_configmap(&configmap("epp-clusters", ONE_SITE));
        let store = started(&api).await;
        let sites = store.sites().borrow().clone();
        assert_eq!(sites.len(), 1, "{sites:?}");
        let spoke1 = &sites[0];
        assert_eq!(
            (spoke1.name.as_str(), spoke1.metrics_url.as_str(), spoke1.lat.is_some()),
            ("spoke1", "https://t1", true)
        );
    }

    #[tokio::test]
    async fn a_configmap_update_republishes_the_site_list() {
        let api = FakeApi::new();
        api.put_configmap(&configmap("epp-clusters", ONE_SITE));
        let store = started(&api).await;
        let mut sites = store.sites();
        api.put_configmap(&configmap("epp-clusters", TWO_SITES));
        wait_until(&mut sites, |sites| sites.len() == 2).await;
    }

    #[tokio::test]
    async fn a_secret_becomes_readable_once_created_and_wakes_the_collector() {
        let api = FakeApi::new();
        api.put_configmap(&configmap("epp-clusters", ONE_SITE));
        let store = started(&api).await;
        assert!(store.secret("site-spoke1").is_none(), "the secret does not exist yet");
        let mut sites = store.sites();
        sites.mark_unchanged();
        api.put_secret(&secret("site-spoke1", "tok1", b"PEM"));
        tokio::time::timeout(Duration::from_secs(3), sites.changed())
            .await
            .unwrap()
            .unwrap();
        let want = SiteSecret {
            token: "tok1".to_owned(),
            ca_pem: b"PEM".to_vec(),
        };
        assert_eq!(
            store.secret("site-spoke1"),
            Some(want),
            "token and CA are read from the Secret data"
        );
    }

    #[tokio::test]
    async fn a_deleted_secret_is_forgotten_and_wakes_the_collector() {
        let api = FakeApi::new();
        api.put_configmap(&configmap("epp-clusters", ONE_SITE));
        api.put_secret(&secret("site-spoke1", "tok1", b"PEM"));
        let store = started(&api).await;
        let mut sites = store.sites();
        sites.mark_unchanged();
        api.delete_secret("site-spoke1");
        tokio::time::timeout(Duration::from_secs(3), sites.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(store.secret("site-spoke1").is_none(), "the deleted Secret is gone");
    }

    #[tokio::test]
    async fn deleting_the_configmap_empties_the_site_list() {
        let api = FakeApi::new();
        api.put_configmap(&configmap("epp-clusters", ONE_SITE));
        let store = started(&api).await;
        let mut sites = store.sites();
        api.delete_configmap("epp-clusters");
        wait_until(&mut sites, <[Site]>::is_empty).await;
    }

    #[tokio::test]
    async fn other_configmaps_are_ignored() {
        let api = FakeApi::new();
        api.put_configmap(&configmap("something-else", ONE_SITE));
        let store = started(&api).await;
        assert!(store.sites().borrow().is_empty(), "only the registry ConfigMap counts");
    }

    #[tokio::test]
    async fn a_relist_with_identical_content_does_not_wake_the_collector() {
        let api = FakeApi::new();
        api.put_configmap(&configmap("epp-clusters", ONE_SITE));
        api.put_secret(&secret("site-spoke1", "tok1", b"PEM"));
        let store = started(&api).await;
        let mut sites = store.sites();
        sites.mark_unchanged();
        let lists = api.list_count();
        api.restart_watches();
        wait_for_relist(&api, lists).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !sites.has_changed().unwrap(),
            "an unchanged relist must not trigger a poll"
        );
    }

    #[tokio::test]
    async fn a_relist_drops_a_secret_that_vanished_without_an_event() {
        let api = FakeApi::new();
        api.put_configmap(&configmap("epp-clusters", ONE_SITE));
        api.put_secret(&secret("site-spoke1", "tok1", b"PEM"));
        let store = started(&api).await;
        assert!(store.secret("site-spoke1").is_some(), "present after the initial list");
        api.forget_secret_silently("site-spoke1");
        let lists = api.list_count();
        api.restart_watches();
        wait_for_relist(&api, lists).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            while store.secret("site-spoke1").is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn the_namespace_comes_from_the_service_account_mount_or_falls_back() {
        let dir = std::env::temp_dir().join(format!("fleet-ns-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("namespace");
        std::fs::write(&file, " aigrid-fleet\n").unwrap();
        assert_eq!(namespace_from(&file), "aigrid-fleet", "trimmed file contents");
        std::fs::write(&file, "\n").unwrap();
        assert_eq!(namespace_from(&file), "default", "an empty file falls back");
        assert_eq!(
            namespace_from(&dir.join("missing")),
            "default",
            "a missing file falls back"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    fn configmap(name: &str, clusters_yaml: &str) -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(NAMESPACE.to_owned()),
                ..ObjectMeta::default()
            },
            data: Some(BTreeMap::from([("clusters.yaml".to_owned(), clusters_yaml.to_owned())])),
            ..ConfigMap::default()
        }
    }

    fn secret(name: &str, token: &str, ca: &[u8]) -> Secret {
        let data = BTreeMap::from([
            ("token".to_owned(), ByteString(token.as_bytes().to_vec())),
            ("ca.crt".to_owned(), ByteString(ca.to_vec())),
        ]);
        Secret {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(NAMESPACE.to_owned()),
                ..ObjectMeta::default()
            },
            data: Some(data),
            ..Secret::default()
        }
    }

    async fn started(api: &FakeApi) -> Arc<RegistryStore> {
        let store = Arc::new(RegistryStore::new(
            api.client(),
            NAMESPACE,
            "epp-clusters",
            "clusters.yaml",
        ));
        tokio::time::timeout(Duration::from_secs(5), Arc::clone(&store).run())
            .await
            .unwrap()
            .unwrap();
        store
    }

    async fn wait_until(sites: &mut watch::Receiver<SiteList>, satisfied: impl Fn(&[Site]) -> bool + Send + Sync) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let done = satisfied(&sites.borrow());
                if done {
                    return;
                }
                sites.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    async fn wait_for_relist(api: &FakeApi, lists_before: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while api.list_count() < lists_before.saturating_add(2) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
