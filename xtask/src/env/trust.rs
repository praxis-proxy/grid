//! mTLS trust verification for provider gateways.
//!
//! Proves that provider gateways enforce the full trust stack:
//!
//! - **Accept:** valid client cert, same CA, correct organization.
//! - **Reject (TLS layer):** no client cert.
//! - **Reject (TLS layer):** client cert from a different/untrusted CA.
//! - **Reject (filter layer):** client cert from the same trusted CA but wrong organization — TLS handshake succeeds;
//!   `peer_identity_trust` rejects with HTTP 403.

use std::{path::Path, process::Command};

use certs::{generate_ca, generate_site_cert};

use crate::env::{
    certs::WRONG_ORG,
    config::{ClusterRole, EnvConfig},
    gateway::HOST_CA_CERT,
    kind::kubectl_context,
    verify::{HttpResponse, PortForwardGuard, Tally, find_free_port, parse_curl_output, safe_truncate, wait_for_port},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// In-pod gateway HTTP(S) port.
const GATEWAY_PORT: u16 = 8080;

/// CA CN for the untrusted-CA negative test.
const UNTRUSTED_CA_CN: &str = "Grid Trust Test Wrong CA";

/// Host-side cert directory (relative to workspace root).
const HOST_CERTS_DIR: &str = "tests/env/certs";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Verify mTLS trust behavior for all provider gateways.
///
/// For each provider cluster, exercises four trust cases:
///
/// 1. Positive: valid client cert → model request returns 200.
/// 2. Negative (TLS layer): no client cert → `curl` exits non-zero.
/// 3. Negative (TLS layer): client cert from an untrusted CA → `curl` exits non-zero.
/// 4. Negative (filter layer): same trusted CA but wrong organization → HTTP 403 from `peer_identity_trust`.
///
/// Cases 2 and 3 confirm that the gateway enforces `client_cert_mode:
/// require` and that only certs from the generated test CA are accepted.
/// Case 4 confirms that `peer_identity_trust` enforces organization
/// matching at the filter level — the TLS layer alone does not reject this
/// cert, so the 403 must come from the filter.
///
/// # Errors
///
/// Returns an error if no provider clusters with models are found, or if any
/// ephemeral cert generation fails.
pub(crate) fn verify_mtls_trust(cfg: &EnvConfig) -> Result<(), Box<dyn std::error::Error>> {
    let consumer_site = cfg
        .consumer_cluster_name()
        .ok_or("no consumer cluster configured in environment config")?;
    let mut tally = Tally::default();
    let mut found = false;

    for name in &cfg.clusters.names {
        let Some(def) = cfg.clusters.definitions.get(name) else {
            continue;
        };
        if def.role != ClusterRole::Provider || def.models.is_empty() {
            continue;
        }
        found = true;
        let first_model = def.models.first().map_or("granite-3.3-8b", String::as_str);
        verify_provider_trust(name, consumer_site, first_model, &mut tally)?;
    }

    if !found {
        return Err("no provider clusters with models found".into());
    }

    tally.print_summary()
}

// ---------------------------------------------------------------------------
// Per-provider verification
// ---------------------------------------------------------------------------

/// Verify mTLS trust for one provider cluster.
fn verify_provider_trust(
    cluster: &str,
    consumer_site: &str,
    model: &str,
    tally: &mut Tally,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = kubectl_context(cluster);
    eprintln!("  verifying mTLS trust for {cluster}...");

    let port = find_free_port()?;
    let mut pf = PortForwardGuard::start(&ctx, "praxis-provider", port, GATEWAY_PORT)?;

    if !wait_for_port(port) {
        tally.fail(cluster, "provider mTLS gateway reachable via port-forward", &ctx);
        pf.stop();
        return Ok(());
    }
    tally.pass(cluster, "provider mTLS gateway port-forward open");

    test_valid_cert(cluster, &ctx, port, consumer_site, model, tally);
    test_no_client_cert(cluster, &ctx, port, tally);
    test_wrong_ca_cert(cluster, &ctx, port, tally)?;

    // Re-verify port readiness before the filter-layer test. Prior negative
    // tests can leave the local kubectl port-forward unhealthy after
    // TLS-rejected connections. Restart the port-forward once instead of
    // failing the trust check on a harness transport issue.
    if !wait_for_port(port) {
        pf.stop();
        pf = PortForwardGuard::start(&ctx, "praxis-provider", port, GATEWAY_PORT)?;
        if !wait_for_port(port) {
            tally.fail(cluster, "port-forward unavailable before wrong-org test", &ctx);
            pf.stop();
            return Ok(());
        }
    }
    test_wrong_org_cert(cluster, &ctx, port, model, tally);

    pf.stop();
    Ok(())
}

// ---------------------------------------------------------------------------
// Individual tests
// ---------------------------------------------------------------------------

/// Positive: valid consumer client cert → model returns 200.
#[expect(
    clippy::too_many_arguments,
    reason = "cluster, context, port, consumer_site, model, tally all distinct"
)]
fn test_valid_cert(cluster: &str, ctx: &str, port: u16, consumer_site: &str, model: &str, tally: &mut Tally) {
    let sni = format!("{cluster}.grid.internal");
    let url = format!("https://{sni}:{port}/v1/chat/completions");
    let resolve = format!("{sni}:{port}:127.0.0.1");
    let body = format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"hi"}}],"max_tokens":1}}"#);
    let cert_path = format!("{HOST_CERTS_DIR}/{consumer_site}-cert.pem");
    let key_path = format!("{HOST_CERTS_DIR}/{consumer_site}-key.pem");

    match curl_post_mtls_with_cert(&url, &body, &resolve, Path::new(&cert_path), Path::new(&key_path)) {
        Ok(resp) if resp.status == 200 => {
            tally.pass(cluster, "valid consumer cert accepted; model returns 200");
        },
        Ok(resp) => {
            tally.fail(
                cluster,
                &format!(
                    "valid cert returned {} (expected 200); body: {}",
                    resp.status,
                    safe_truncate(&resp.body, 120)
                ),
                ctx,
            );
        },
        Err(e) => {
            tally.fail(cluster, &format!("valid cert request error: {e}"), ctx);
        },
    }
}

/// HTTP POST via curl with mTLS using explicit client cert paths.
///
/// Includes `Authorization: Bearer dummy-key` because the mock-openai backend
/// requires a bearer token to return 200.  The mTLS layer does not inspect
/// this header; it is only needed to reach the backend and confirm the
/// positive trust path end-to-end.
fn curl_post_mtls_with_cert(
    url: &str,
    body: &str,
    resolve: &str,
    cert: &Path,
    key: &Path,
) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let args = curl_post_mtls_args(url, body, resolve, cert, key)?;
    let output = Command::new("curl").args(args).output()?;
    let raw = String::from_utf8(output.stdout)?;
    parse_curl_output(&raw)
}

/// Build curl args for a positive mTLS POST request.
fn curl_post_mtls_args(
    url: &str,
    body: &str,
    resolve: &str,
    cert: &Path,
    key: &Path,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(vec![
        "-s".to_owned(),
        "-w".to_owned(),
        "\n%{http_code}".to_owned(),
        "--connect-timeout".to_owned(),
        "5".to_owned(),
        "--max-time".to_owned(),
        "15".to_owned(),
        "--resolve".to_owned(),
        resolve.to_owned(),
        "--cacert".to_owned(),
        HOST_CA_CERT.to_owned(),
        "--cert".to_owned(),
        cert.to_str().ok_or("client cert path is not UTF-8")?.to_owned(),
        "--key".to_owned(),
        key.to_str().ok_or("client key path is not UTF-8")?.to_owned(),
        "-X".to_owned(),
        "POST".to_owned(),
        "-H".to_owned(),
        "Authorization: Bearer dummy-key".to_owned(),
        "-H".to_owned(),
        "Content-Type: application/json".to_owned(),
        "-d".to_owned(),
        body.to_owned(),
        url.to_owned(),
    ])
}

/// Negative: no client cert → TLS handshake failure (`curl` exits non-zero).
///
/// Uses `--resolve` so `curl` reaches the server with the correct SNI,
/// ensuring the test exercises `client_cert_mode: require` rather than an SNI
/// mismatch.
#[expect(clippy::too_many_lines, reason = "curl argument construction + result handling")]
fn test_no_client_cert(cluster: &str, ctx: &str, port: u16, tally: &mut Tally) {
    let sni = format!("{cluster}.grid.internal");
    let resolve = format!("{sni}:{port}:127.0.0.1");
    let url = format!("https://{sni}:{port}/v1/models");
    let status = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "--resolve",
            &resolve,
            "--cacert",
            HOST_CA_CERT,
            "--max-time",
            "8",
            &url,
        ])
        .status();

    match status {
        Ok(s) if !s.success() => {
            tally.pass(
                cluster,
                "no-client-cert request rejected at TLS layer (curl exit non-0)",
            );
        },
        Ok(_) => {
            tally.fail(
                cluster,
                "no-client-cert request succeeded (expected TLS handshake failure)",
                ctx,
            );
        },
        Err(e) => {
            tally.fail(cluster, &format!("no-client-cert test could not run curl: {e}"), ctx);
        },
    }
}

/// Negative: client cert from an ephemeral untrusted CA → TLS handshake
/// failure.
///
/// Generates a fresh CA + site cert in the OS temp directory, then attempts
/// an mTLS connection.  The provider only trusts the generated test CA, so
/// rustls rejects the untrusted cert before any HTTP is exchanged.
#[expect(
    clippy::too_many_lines,
    reason = "cert generation + temp file management + curl test"
)]
fn test_wrong_ca_cert(
    cluster: &str,
    ctx: &str,
    port: u16,
    tally: &mut Tally,
) -> Result<(), Box<dyn std::error::Error>> {
    let wrong_ca = generate_ca(UNTRUSTED_CA_CN)?;
    let wrong_cert = generate_site_cert(&wrong_ca, "attacker")?;

    let tmp = std::env::temp_dir();
    let ca_file = tmp.join("grid-trust-test-wrong-ca.pem");
    let cert_file = tmp.join("grid-trust-test-wrong-cert.pem");
    let key_file = tmp.join("grid-trust-test-wrong-key.pem");

    std::fs::write(&ca_file, &wrong_ca.cert_pem)?;
    std::fs::write(&cert_file, &wrong_cert.cert_pem)?;
    std::fs::write(&key_file, &wrong_cert.key_pem)?;

    let sni = format!("{cluster}.grid.internal");
    let resolve = format!("{sni}:{port}:127.0.0.1");
    let url = format!("https://{sni}:{port}/v1/models");
    let status = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "--resolve",
            &resolve,
            "--cacert",
            HOST_CA_CERT,
            "--cert",
            cert_file.to_str().unwrap_or(""),
            "--key",
            key_file.to_str().unwrap_or(""),
            "--max-time",
            "8",
            &url,
        ])
        .status();

    let _ca = std::fs::remove_file(&ca_file);
    let _cert = std::fs::remove_file(&cert_file);
    let _key = std::fs::remove_file(&key_file);

    match status {
        Ok(s) if !s.success() => {
            tally.pass(cluster, "wrong-CA client cert rejected at TLS layer (curl exit non-0)");
        },
        Ok(_) => {
            tally.fail(cluster, "wrong-CA cert accepted (expected TLS handshake failure)", ctx);
        },
        Err(e) => {
            tally.fail(cluster, &format!("wrong-CA test could not run curl: {e}"), ctx);
        },
    }

    Ok(())
}

/// Negative: client cert from the same trusted CA but with a different
/// organization → HTTP 403 from `peer_identity_trust`.
///
/// This specifically tests filter-level enforcement, not the TLS/PKI layer.
/// The cert passes `client_cert_mode: require` because it is CA-valid;
/// `peer_identity_trust` then checks the organization field and rejects.
///
/// The cert is pre-generated by `certs::generate_all` at `env up` time and
/// stored as `tests/env/certs/wrong-org-client-{cert,key}.pem`.
///
/// One retry is allowed for `curl` connection failures: prior TLS-rejection
/// tests may have reset the connection pool.  A connection error here is
/// unexpected (same CA cert should succeed at TLS), so retrying once is
/// appropriate.  A second failure is reported as a test failure.
#[expect(
    clippy::too_many_lines,
    reason = "curl argument construction + retry + result handling"
)]
fn test_wrong_org_cert(cluster: &str, ctx: &str, port: u16, model: &str, tally: &mut Tally) {
    let cert_path = format!("{HOST_CERTS_DIR}/wrong-org-client-cert.pem");
    let key_path = format!("{HOST_CERTS_DIR}/wrong-org-client-key.pem");

    if !Path::new(&cert_path).exists() {
        tally.fail(cluster, "wrong-org cert not found; run 'env up' to generate certs", ctx);
        return;
    }

    let sni = format!("{cluster}.grid.internal");
    let resolve = format!("{sni}:{port}:127.0.0.1");
    let url = format!("https://{sni}:{port}/v1/chat/completions");
    let body = format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"hi"}}],"max_tokens":1}}"#);

    let attempt = || -> Option<u16> {
        let output = Command::new("curl")
            .args([
                "-s",
                "-w",
                "\n%{http_code}",
                "--resolve",
                &resolve,
                "--cacert",
                HOST_CA_CERT,
                "--cert",
                &cert_path,
                "--key",
                &key_path,
                "--max-time",
                "10",
                "-X",
                "POST",
                "-H",
                "Content-Type: application/json",
                "-d",
                &body,
                &url,
            ])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let raw = String::from_utf8_lossy(&output.stdout);
        raw.lines().last()?.trim().parse::<u16>().ok()
    };

    let http_status = attempt().or_else(|| {
        // Retry once: the previous connection may have been reset by the
        // TLS-rejection tests; a second attempt typically succeeds immediately.
        attempt()
    });

    match http_status {
        Some(403) => {
            tally.pass(
                cluster,
                &format!(
                    "same-CA wrong-org client rejected by peer_identity_trust \
                     (HTTP 403, org={WRONG_ORG})"
                ),
            );
        },
        Some(s) => {
            tally.fail(
                cluster,
                &format!(
                    "same-CA wrong-org cert returned HTTP {s} \
                     (expected 403 from peer_identity_trust)"
                ),
                ctx,
            );
        },
        None => {
            tally.fail(
                cluster,
                "same-CA wrong-org test: curl failed after 1 retry \
                 (expected HTTP 403 from filter, not TLS error)",
                ctx,
            );
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn untrusted_ca_cn_is_non_empty() {
        assert!(!UNTRUSTED_CA_CN.is_empty(), "wrong CA CN must be non-empty");
    }

    #[test]
    fn wrong_org_is_not_default_org() {
        assert_ne!(
            WRONG_ORG,
            certs::DEFAULT_ORGANIZATION,
            "WRONG_ORG must differ from DEFAULT_ORGANIZATION"
        );
    }

    // -----------------------------------------------------------------------
    // curl arg correctness — verify the bearer token is included in the
    // valid-cert positive test so the mock-openai backend returns 200
    // -----------------------------------------------------------------------

    #[test]
    fn valid_cert_request_includes_bearer_token() {
        let args = curl_post_mtls_args(
            "https://site-a.grid.internal:9999/v1/chat/completions",
            r#"{"model":"m"}"#,
            "site-a.grid.internal:9999:127.0.0.1",
            Path::new("/tmp/cert.pem"),
            Path::new("/tmp/key.pem"),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert!(
            args.iter().any(|a| a == "Authorization: Bearer dummy-key"),
            "valid-cert positive request must include bearer token so mock-openai returns 200"
        );
        // The bearer header must appear before Content-Type.
        let bearer_pos = args.iter().position(|a| a == "Authorization: Bearer dummy-key");
        let content_type_pos = args.iter().position(|a| a == "Content-Type: application/json");
        assert!(
            bearer_pos < content_type_pos,
            "bearer header should precede Content-Type for consistent ordering"
        );
    }
}
