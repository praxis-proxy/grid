use clap::CommandFactory as _;

use super::*;

const FAST: Backoff = Backoff {
    attempts: 3,
    initial: Duration::from_millis(1),
    max: Duration::from_millis(5),
};

fn invite(site: &str) -> EnrollmentTokenRequest {
    EnrollmentTokenRequest {
        site_name: site.to_owned(),
        grid_network_ref: "grid".to_owned(),
        expires_in_secs: Some(600),
    }
}

/// Install the rustls provider a default build needs before any client is built.
fn init_crypto() {
    #[cfg(not(feature = "fips"))]
    drop(rustls::crypto::ring::default_provider().install_default());
}

/// A temp file holding a grid-admin token, removed on drop.
struct AdminFile(PathBuf);

impl AdminFile {
    fn new(admin: &str) -> Self {
        let path = std::env::temp_dir().join(format!("grid-invite-admin-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, format!("{admin}\n")).expect("write admin token");
        Self(path)
    }
}

impl Drop for AdminFile {
    fn drop(&mut self) {
        drop(std::fs::remove_file(&self.0));
    }
}

#[test]
fn the_cli_definition_is_valid() {
    InviteArgs::command().debug_assert();
}

/// A closed local port.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("addr").port()
}

#[tokio::test]
async fn the_delay_carries_across_calls_while_failing_and_resets_on_reach() {
    let mut retry = Retry::new(Backoff {
        attempts: 10,
        initial: Duration::from_millis(1),
        max: Duration::from_millis(8),
    });
    assert!(retry.failed().await && retry.failed().await, "the first site retries");
    assert_eq!(retry.delay, Duration::from_millis(4), "two failures double twice");
    assert!(retry.failed().await, "the next site retries");
    assert_eq!(
        retry.delay,
        Duration::from_millis(8),
        "the next site continues doubling"
    );
    assert_eq!(retry.left, 7, "every failure spends the one budget");
    retry.reached();
    assert_eq!(
        retry.delay,
        Duration::from_millis(1),
        "a reached service resets the delay"
    );
    assert_eq!(retry.left, 7, "reaching the service refunds nothing");
}

#[tokio::test]
async fn an_unreachable_service_fails_naming_the_connect_error() {
    let port = closed_port();
    init_crypto();
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let admin = AdminFile::new("good-admin");
    let mut minter = Minter {
        http: http_client(ca.cert_pem.as_bytes()).expect("client"),
        base: format!("https://localhost:{port}"),
        admin_token_file: admin.0.clone(),
        retry: Retry::new(FAST),
    };
    let result = minter.mint(&invite("site-d")).await;
    assert!(
        result.is_err_and(|err| err.to_string().contains("failed:") && err.to_string().contains("onnect")),
        "the cause chain names the connect failure"
    );
}

#[test]
fn only_https_base_urls_are_accepted() {
    assert_eq!(
        https_base("https://e.example.com/").ok().as_deref(),
        Some("https://e.example.com")
    );
    assert!(https_base("http://e.example.com").is_err(), "plaintext is refused");
    assert!(https_base("not a url").is_err(), "garbage is refused");
}

#[test]
fn invites_parse_from_values_json_and_validate_names() {
    let parsed = parse_invites(r#"[{"siteName":"site-d","gridNetworkRef":"grid","expiresInSecs":600}]"#)
        .expect("valid invites parse");
    assert_eq!(parsed, vec![invite("site-d")], "the values shape is the wire shape");
    assert!(
        parse_invites(r#"[{"siteName":"Site_D","gridNetworkRef":"grid"}]"#).is_err(),
        "a site name that is not a DNS label is refused"
    );
}

#[test]
fn the_invite_secret_carries_the_token_and_its_id() {
    let minted = MintedToken {
        token_id: uuid::Uuid::nil(),
        token: Zeroizing::new("t".to_owned()),
        expires_at: "2026-10-01T00:00:00Z".to_owned(),
    };
    let secret = invite_secret("grid-invite-site-d", &invite("site-d"), &minted);
    assert_eq!(
        secret
            .string_data
            .and_then(|data| data.get("token").cloned())
            .as_deref(),
        Some("t"),
        "key token holds the site token"
    );
    let annotations = secret.metadata.annotations.unwrap_or_default();
    assert_eq!(
        annotations.get("grid.praxis.fast/token-id").map(String::as_str),
        Some("00000000-0000-0000-0000-000000000000"),
        "the token id is kept for revocation"
    );
}

/// Tests against the real router over TLS.
mod live {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use enrollment::{AppState, GridAdmins, SharedCa, Store, authz::Authorizer, router};

    use super::{super::*, AdminFile, FAST, closed_port, init_crypto, invite};

    /// What the fake does on create.
    #[derive(Clone, Copy)]
    enum OnCreate {
        /// Store it.
        Store,
        /// Store it, then fail as if the response was lost.
        StoreThenFail,
        /// Fail because another writer stored its own token.
        Conflict,
        /// Fail without storing.
        Fail,
        /// Fail without storing, then fail every read.
        FailThenUnreadable,
    }

    /// Fake Secrets holding token ids by name.
    struct Fake {
        /// Scripted create result.
        on_create: OnCreate,
        /// Secrets whose create fails regardless of `on_create`.
        failing: Vec<String>,
        /// Held token ids by Secret name.
        held: Mutex<BTreeMap<String, String>>,
        /// `(Secret name, token id, token)` handed to create.
        seen: Mutex<Vec<(String, uuid::Uuid, String)>>,
        /// Whether reads fail.
        unreadable: AtomicBool,
        /// Admin token file deleted on create.
        drop_admin: Option<PathBuf>,
    }

    impl Fake {
        fn new(on_create: OnCreate) -> Self {
            Self {
                on_create,
                failing: Vec::new(),
                held: Mutex::new(BTreeMap::new()),
                seen: Mutex::new(Vec::new()),
                unreadable: AtomicBool::new(false),
                drop_admin: None,
            }
        }

        fn holding(self, name: &str, token_id: &str) -> Self {
            self.held
                .lock()
                .expect("lock")
                .insert(name.to_owned(), token_id.to_owned());
            self
        }

        fn seen(&self) -> Vec<(String, uuid::Uuid, String)> {
            self.seen.lock().expect("lock").clone()
        }

        fn token_for(&self, name: &str) -> Option<String> {
            self.seen()
                .into_iter()
                .find_map(|(seen, _, token)| (seen == name).then_some(token))
        }

        fn held(&self, name: &str) -> Option<String> {
            self.held.lock().expect("lock").get(name).cloned()
        }
    }

    impl InviteSecrets for Fake {
        async fn token_id(&self, name: &str) -> Result<Option<String>, BoxError> {
            if self.unreadable.load(Ordering::SeqCst) {
                return Err("apiserver unavailable".into());
            }
            Ok(self.held(name))
        }

        async fn create(
            &self,
            name: &str,
            _invite: &EnrollmentTokenRequest,
            token: &MintedToken,
        ) -> Result<(), BoxError> {
            self.seen
                .lock()
                .expect("lock")
                .push((name.to_owned(), token.token_id, token.token.to_string()));
            if let Some(path) = &self.drop_admin {
                std::fs::remove_file(path).expect("remove admin token");
            }
            let failing = self.failing.iter().any(|failing| failing == name);
            let on_create = if failing { OnCreate::Fail } else { self.on_create };
            let (stored, result) = on_create.outcome(token.token_id);
            if let Some(id) = stored {
                self.held.lock().expect("lock").insert(name.to_owned(), id);
            }
            if matches!(on_create, OnCreate::FailThenUnreadable) {
                self.unreadable.store(true, Ordering::SeqCst);
            }
            result
        }
    }

    impl OnCreate {
        /// The token id stored, and the result create reports.
        fn outcome(self, id: uuid::Uuid) -> (Option<String>, Result<(), BoxError>) {
            match self {
                Self::Store => (Some(id.to_string()), Ok(())),
                Self::StoreThenFail => (Some(id.to_string()), Err("connection reset".into())),
                Self::Conflict => (Some(uuid::Uuid::new_v4().to_string()), Err("409 AlreadyExists".into())),
                Self::Fail | Self::FailThenUnreadable => (None, Err("apiserver unavailable".into())),
            }
        }
    }

    /// Lowercase hex SHA-256, as the store keys tokens.
    fn digest(token: &str) -> String {
        certs::sha256(token.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// Serve `app` over rustls on `listener`.
    #[cfg(not(feature = "fips"))]
    async fn spawn_tls(listener: std::net::TcpListener, cert: certs::SiteCertOutput, app: axum::Router) {
        let tls =
            axum_server::tls_rustls::RustlsConfig::from_pem(cert.cert_pem.into_bytes(), cert.key_pem.into_bytes())
                .await
                .expect("tls");
        let server = axum_server::from_tcp_rustls(listener, tls).expect("server");
        tokio::spawn(server.serve(app.into_make_service()));
    }

    /// Serve `app` over system openssl on `listener`.
    #[cfg(feature = "fips")]
    #[expect(clippy::unused_async, reason = "matches the rustls variant")]
    async fn spawn_tls(listener: std::net::TcpListener, cert: certs::SiteCertOutput, app: axum::Router) {
        let tls = axum_server::tls_openssl::OpenSSLConfig::from_pem(cert.cert_pem.as_bytes(), cert.key_pem.as_bytes())
            .expect("tls");
        let server = axum_server::from_tcp(listener)
            .expect("server")
            .acceptor(axum_server::tls_openssl::OpenSSLAcceptor::new(tls));
        tokio::spawn(server.serve(app.into_make_service()));
    }

    /// Serve `app` on `port` once `after` passes.
    async fn listen_after(port: u16, after: Duration, cert: certs::SiteCertOutput, app: axum::Router) {
        let start = async move {
            tokio::time::sleep(after).await;
            let listener = std::net::TcpListener::bind(("127.0.0.1", port)).expect("bind");
            listener.set_nonblocking(true).expect("nonblocking");
            spawn_tls(listener, cert, app).await;
        };
        if after.is_zero() {
            start.await;
        } else {
            tokio::spawn(start);
        }
    }

    /// Serve the enrollment router over TLS with a pinned minter.
    async fn serve(admin: &str) -> (Arc<AppState>, Minter, AdminFile) {
        serve_after(admin, Duration::ZERO).await
    }

    /// [`serve`], with the port closed for `after`.
    async fn serve_after(admin: &str, after: Duration) -> (Arc<AppState>, Minter, AdminFile) {
        init_crypto();
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let serving = certs::generate_dns_cert(&ca, "localhost", "localhost").expect("serving cert");
        let port = closed_port();
        let admin = AdminFile::new(admin);
        let minter = Minter {
            http: http_client(ca.cert_pem.as_bytes()).expect("client"),
            base: format!("https://localhost:{port}"),
            admin_token_file: admin.0.clone(),
            retry: Retry::new(FAST),
        };
        let state = Arc::new(AppState {
            store: Store::memory(),
            ca: SharedCa::new(ca),
            authorizer: Authorizer::Local(GridAdmins::from_table("admin: good-admin\n")),
            cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
            reserved_sites: Vec::new(),
        });
        listen_after(port, after, serving, router(Arc::clone(&state))).await;
        (state, minter, admin)
    }

    /// Whether the store accepts `token`.
    async fn live(state: &AppState, token: &str) -> bool {
        state.store.token_valid(&digest(token)).await.expect("store")
    }

    #[tokio::test]
    async fn mints_and_stores_a_live_token() {
        let (state, mut minter, _admin) = serve("good-admin").await;
        let secrets = Fake::new(OnCreate::Store);
        let result = invite_one(&secrets, &mut minter, "grid-invite-", &invite("site-d")).await;
        assert!(matches!(result, Ok(Invited::Minted)), "mints: {result:?}");
        let seen = secrets.seen();
        assert_eq!(seen.len(), 1, "one token is minted");
        let (_, _, token) = seen.first().expect("one token");
        assert!(live(&state, token).await, "the stored token is live");
    }

    #[tokio::test]
    async fn an_existing_secret_mints_nothing() {
        let port = closed_port();
        init_crypto();
        let ca = certs::generate_ca("grid-ca").expect("ca");
        let admin = AdminFile::new("good-admin");
        let mut minter = Minter {
            http: http_client(ca.cert_pem.as_bytes()).expect("client"),
            base: format!("https://localhost:{port}"),
            admin_token_file: admin.0.clone(),
            retry: Retry::new(FAST),
        };
        let secrets = Fake::new(OnCreate::Store).holding("grid-invite-site-d", "kept");
        let result = invite_one(&secrets, &mut minter, "grid-invite-", &invite("site-d")).await;
        assert!(
            matches!(result, Ok(Invited::Skipped)),
            "skips without the service: {result:?}"
        );
        assert!(secrets.seen().is_empty(), "nothing is minted");
    }

    #[tokio::test]
    async fn a_stored_token_whose_response_was_lost_stays_live_across_a_retry() {
        let (state, mut minter, _admin) = serve("good-admin").await;
        let secrets = Fake::new(OnCreate::StoreThenFail);
        let first = invite_one(&secrets, &mut minter, "grid-invite-", &invite("site-d")).await;
        assert!(
            matches!(first, Ok(Invited::Minted)),
            "the persisted token is kept: {first:?}"
        );
        let retry = invite_one(&secrets, &mut minter, "grid-invite-", &invite("site-d")).await;
        assert!(matches!(retry, Ok(Invited::Skipped)), "the retry skips: {retry:?}");
        let seen = secrets.seen();
        assert_eq!(seen.len(), 1, "the retry mints nothing");
        let (_, _, token) = seen.first().expect("one token");
        assert!(live(&state, token).await, "the stored token stays live");
    }

    #[tokio::test]
    async fn a_conflict_with_another_token_revokes_the_new_one() {
        let (state, mut minter, _admin) = serve("good-admin").await;
        let secrets = Fake::new(OnCreate::Conflict);
        let result = invite_one(&secrets, &mut minter, "grid-invite-", &invite("site-d")).await;
        assert!(matches!(result, Ok(Invited::Raced)), "loses the race: {result:?}");
        let (name, id, token) = secrets.seen().pop().expect("one token");
        assert_ne!(
            secrets.held(&name),
            Some(id.to_string()),
            "the other writer's token is kept"
        );
        assert!(!live(&state, &token).await, "the new token is revoked");
    }

    #[tokio::test]
    async fn a_create_that_did_not_persist_revokes_and_fails() {
        let (state, mut minter, _admin) = serve("good-admin").await;
        let secrets = Fake::new(OnCreate::Fail);
        let result = invite_one(&secrets, &mut minter, "grid-invite-", &invite("site-d")).await;
        assert!(result.is_err(), "fails: {result:?}");
        let (_, _, token) = secrets.seen().pop().expect("one token");
        assert!(!live(&state, &token).await, "the unstored token is revoked");
    }

    #[tokio::test]
    async fn an_unreadable_secret_after_a_failed_create_keeps_the_token() {
        let (state, mut minter, _admin) = serve("good-admin").await;
        let secrets = Fake::new(OnCreate::FailThenUnreadable);
        let result = invite_one(&secrets, &mut minter, "grid-invite-", &invite("site-d")).await;
        let (_, id, token) = secrets.seen().pop().expect("one token");
        assert!(
            result.is_err_and(|err| err.to_string().contains(&format!("token {id} may be live"))),
            "the error names the token id"
        );
        assert!(live(&state, &token).await, "a token that may be held is not revoked");
    }

    #[tokio::test]
    async fn a_failed_revoke_fails_naming_the_token_id() {
        let (_state, mut minter, admin) = serve("good-admin").await;
        let secrets = Fake {
            drop_admin: Some(admin.0.clone()),
            ..Fake::new(OnCreate::Conflict)
        };
        let result = invite_one(&secrets, &mut minter, "grid-invite-", &invite("site-d")).await;
        assert!(
            result.is_err_and(|err| err.to_string().contains("revoking unstored token")),
            "a live unheld token fails the run"
        );
    }

    #[tokio::test]
    async fn every_invite_runs_and_the_error_names_each_failed_site() {
        let (state, mut minter, _admin) = serve("good-admin").await;
        let secrets = Fake {
            failing: vec!["grid-invite-site-b".to_owned()],
            ..Fake::new(OnCreate::Store)
        }
        .holding("grid-invite-site-a", "kept");
        let invites = [invite("site-a"), invite("site-b"), invite("site-c")];
        let err = invite_all(&secrets, &mut minter, "grid-invite-", &invites)
            .await
            .expect_err("site-b fails")
            .to_string();
        assert!(
            err.contains("1 of 3") && err.contains("site site-b:"),
            "names the failed site: {err}"
        );
        assert!(
            !err.contains("site-a") && !err.contains("site-c"),
            "only failed sites are named: {err}"
        );
        let token = |site: &str| secrets.token_for(&format!("grid-invite-{site}"));
        assert_eq!(token("site-a"), None, "site-a is skipped");
        assert!(
            live(&state, &token("site-c").expect("site-c")).await,
            "a later site is invited"
        );
        assert!(
            !live(&state, &token("site-b").expect("site-b")).await,
            "the failed token is revoked"
        );
    }

    #[tokio::test]
    async fn a_server_outside_the_tls_pin_fails_without_retrying() {
        let (_state, mut minter, _admin) = serve("good-admin").await;
        let stranger = certs::generate_ca("grid-ca").expect("stranger ca");
        minter.http = http_client(stranger.cert_pem.as_bytes()).expect("client");
        minter.retry = Retry::new(Backoff {
            attempts: 5,
            initial: Duration::from_secs(60),
            max: Duration::from_secs(60),
        });
        let result = tokio::time::timeout(Duration::from_secs(10), minter.mint(&invite("site-d")))
            .await
            .expect("a pin mismatch is not retried");
        assert!(
            result.is_err_and(|err| err.to_string().contains("certificate")),
            "the error names the certificate failure"
        );
    }

    #[tokio::test]
    async fn a_refused_admin_is_a_hard_error() {
        let (_state, mut minter, _admin) = serve("wrong").await;
        let result = invite_one(
            &Fake::new(OnCreate::Store),
            &mut minter,
            "grid-invite-",
            &invite("site-d"),
        )
        .await;
        assert!(
            result.is_err_and(|err| err.to_string().contains("grid-admin Role")),
            "a bad admin credential fails with a hint"
        );
    }

    #[tokio::test]
    async fn a_blip_is_retried_and_later_sites_start_at_the_initial_delay() {
        let (state, mut minter, _admin) = serve_after("good-admin", Duration::from_millis(50)).await;
        let budget = Backoff {
            attempts: 100,
            initial: Duration::from_millis(5),
            max: Duration::from_millis(20),
        };
        minter.retry = Retry::new(budget);
        let secrets = Fake::new(OnCreate::Store).holding("grid-invite-site-a", "kept");
        let invites = [invite("site-a"), invite("site-b"), invite("site-c")];
        invite_all(&secrets, &mut minter, "grid-invite-", &invites)
            .await
            .expect("every site is invited");
        assert!(minter.retry.left < budget.attempts, "site-b hit the outage and retried");
        assert_eq!(
            minter.retry.delay, budget.initial,
            "reaching the service reset the delay"
        );
        for site in ["site-b", "site-c"] {
            let token = secrets.token_for(&format!("grid-invite-{site}")).expect("minted");
            assert!(live(&state, &token).await, "{site} holds a live token");
        }
    }

    #[tokio::test]
    async fn a_full_outage_spends_one_budget_and_names_every_site() {
        let (_state, mut minter, _admin) = serve_after("good-admin", Duration::from_secs(3600)).await;
        let invites = [invite("site-a"), invite("site-b"), invite("site-c")];
        let err = invite_all(&Fake::new(OnCreate::Store), &mut minter, "grid-invite-", &invites)
            .await
            .expect_err("nothing is reachable")
            .to_string();
        assert!(
            ["3 of 3", "site site-a:", "site site-b:", "site site-c:"]
                .iter()
                .all(|part| err.contains(part)),
            "names every site: {err}"
        );
        assert_eq!(
            err.matches("not sent").count(),
            2,
            "later sites make no network call: {err}"
        );
        assert_eq!(minter.retry.left, 0, "one budget covers the run");
        assert_eq!(
            minter.retry.delay,
            Duration::from_millis(4),
            "failures double and never reset"
        );
    }
}
