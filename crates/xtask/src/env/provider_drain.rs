//! Administrative provider drain operations.
//!
//! Selection is based only on the explicit `spec.gatewayRef` relationship.
//! This command changes desired Kubernetes state; Grid still performs the
//! asynchronous overlay reconciliation and never participates in requests.

#![allow(
    clippy::missing_docs_in_private_items,
    reason = "the private JSON response model is documented by the command contract"
)]

use std::{
    collections::BTreeMap,
    process::Command,
    time::{Duration, Instant},
};

use serde::Deserialize;

const RESOURCE: &str = "inferenceproviders";
type CandidateStates = BTreeMap<String, String>;
type RevisionPair = (Option<String>, Option<String>);

#[derive(Debug, Deserialize)]
struct ProviderList {
    items: Vec<Provider>,
}

#[derive(Debug, Deserialize)]
struct Provider {
    metadata: Metadata,
    spec: Spec,
}

#[derive(Debug, Deserialize)]
struct Metadata {
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Spec {
    gateway_ref: Option<String>,
}

/// Drain or restore all providers assigned to one explicit gateway identity.
#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "CLI dispatch keeps the selector and bounded operation controls explicit"
)]
pub(crate) fn run(
    context: &str,
    gateway: Option<&str>,
    provider: Option<&str>,
    dry_run: bool,
    undrain: bool,
    timeout: Duration,
    network: Option<&str>,
    consumers: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    validate_selector(gateway, provider)?;
    let selector = gateway.map_or_else(
        || format!("provider={:?}", provider.unwrap_or_default()),
        |value| format!("gatewayRef={value:?}"),
    );
    let selected = if let Some(name) = provider {
        select_provider(context, name)?
    } else {
        select(context, gateway.unwrap_or_default())?
    };
    if selected.is_empty() {
        return Err(format!("no InferenceProvider objects match {selector}").into());
    }
    println!("selected providers for {selector}:");
    for name in &selected {
        println!("  {name}");
    }
    if dry_run {
        println!("dry-run: no provider resources changed");
        return Ok(());
    }
    if network.is_some() == consumers.is_empty() {
        return Err("--network and --consumer must be supplied together".into());
    }
    let original = selected
        .iter()
        .map(|name| read_drain_state(context, name).map(|drain| (name.clone(), drain)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let requested_drain = !undrain;
    let mut changed = Vec::<String>::new();
    for name in &selected {
        if let Err(error) = patch_drain_state(context, name, requested_drain, timeout) {
            let restoration_errors = changed
                .iter()
                .filter_map(|changed_name| {
                    let drain = original.get(changed_name)?;
                    patch_drain_state(context, changed_name, *drain, timeout)
                        .err()
                        .map(|restore_error| format!("{changed_name}: {restore_error}"))
                })
                .collect::<Vec<_>>();
            return Err(format!(
                "failed to update {name}: {error}; partial-update restoration errors: {restoration_errors:?}"
            )
            .into());
        }
        changed.push(name.clone());
    }
    if let Some(network) = network {
        wait_for_convergence(context, &selected, !undrain, network, consumers, timeout)
    } else {
        println!("requested drain state stored; overlay convergence was not requested");
        Ok(())
    }
}

/// Store one provider's desired administrative drain state.
fn patch_drain_state(
    context: &str,
    name: &str,
    drain: bool,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let patch = format!(r#"{{"spec":{{"trafficPolicy":{{"drain":{drain}}}}}}}"#);
    kubectl(
        context,
        ["patch", RESOURCE, name, "--type", "merge", "-p", &patch],
        timeout,
    )?;
    Ok(())
}

/// Validate the mutually exclusive administrative selector.
fn validate_selector(gateway: Option<&str>, provider: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    if gateway.is_none() == provider.is_none() {
        return Err("exactly one of --gateway or --provider is required".into());
    }
    if gateway.is_some_and(|value| value.trim().is_empty()) || provider.is_some_and(|value| value.trim().is_empty()) {
        return Err("--gateway and --provider must not be empty".into());
    }
    Ok(())
}

fn select(context: &str, gateway: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let output = kubectl(context, ["get", RESOURCE, "-o", "json"], Duration::from_secs(30))?;
    let list: ProviderList = serde_json::from_slice(&output)?;
    let mut names: Vec<_> = list
        .items
        .into_iter()
        .filter(|p| p.spec.gateway_ref.as_deref() == Some(gateway))
        .filter_map(|p| p.metadata.name)
        .collect();
    names.sort();
    names.dedup();
    Ok(names)
}

/// Select one explicitly named provider, failing safely when it is absent.
fn select_provider(context: &str, name: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let output = kubectl(context, ["get", RESOURCE, name, "-o", "json"], Duration::from_secs(30))?;
    let provider: Provider = serde_json::from_slice(&output)?;
    Ok(provider.metadata.name.into_iter().collect())
}

/// Read the desired administrative drain state before a qualification mutates it.
pub(crate) fn read_drain_state(context: &str, name: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let output = kubectl(context, ["get", RESOURCE, name, "-o", "json"], Duration::from_secs(30))?;
    let provider: serde_json::Value = serde_json::from_slice(&output)?;
    Ok(provider
        .pointer("/spec/trafficPolicy/drain")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false))
}

/// Execute kubectl with dynamically assembled arguments and a hard timeout.
fn kubectl_owned(context: &str, args: &[String], timeout: Duration) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let output = Command::new("timeout")
        .arg(format!("{}s", timeout.as_secs().max(1)))
        .arg("kubectl")
        .arg("--context")
        .arg(context)
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl {} failed or timed out: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(output.stdout)
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the convergence gate keeps all target and diagnostic state explicit"
)]
fn wait_for_convergence(
    context: &str,
    names: &[String],
    draining: bool,
    network: &str,
    consumers: &[String],
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    let command_timeout = Duration::from_secs(30).min(timeout.max(Duration::from_secs(1)));
    let expected_state = if draining { "existing_only" } else { "new_and_existing" };
    let mut stable_observations = 0;
    let mut previous = String::new();
    let mut last = String::from("no observation");
    while Instant::now() < deadline {
        let mut observation = BTreeMap::new();
        let mut valid = true;
        for consumer in consumers {
            match read_overlay(context, network, consumer, command_timeout) {
                Ok((revision, states)) => match read_revisions(context, consumer, command_timeout) {
                    Ok((accepted, serving)) => {
                        let states_match = names
                            .iter()
                            .all(|name| states.get(name).is_some_and(|state| state == expected_state));
                        let revision_match = !revision.is_empty()
                            && accepted.as_deref() == Some(revision.as_str())
                            && serving.as_deref() == Some(revision.as_str());
                        valid &= states_match && revision_match;
                        observation.insert(
                            consumer.clone(),
                            format!(
                                "grid={revision} accepted={accepted:?} serving={serving:?} states_match={states_match} states={states:?} expected={expected_state}"
                            ),
                        );
                    },
                    Err(error) => {
                        valid = false;
                        observation.insert(consumer.clone(), format!("revision_error={error}"));
                    },
                },
                Err(error) => {
                    valid = false;
                    observation.insert(consumer.clone(), format!("overlay_error={error}"));
                },
            }
        }
        last = observation.values().cloned().collect::<Vec<_>>().join("; ");
        let fingerprint = last.clone();
        if valid && fingerprint == previous {
            stable_observations += 1;
        } else if valid {
            stable_observations = 1;
        } else {
            stable_observations = 0;
        }
        previous = fingerprint;
        if stable_observations >= 2 {
            println!("converged: {last}");
            return Ok(());
        }
        std::thread::park_timeout(Duration::from_secs(1));
    }
    Err(format!(
        "timed out waiting for {} convergence; last observed: {last}",
        if draining { "drain" } else { "undrain" }
    )
    .into())
}

/// Read the projected overlay revision and candidate admission states.
#[expect(
    clippy::too_many_lines,
    reason = "the overlay parser validates the complete projected resource in one place"
)]
fn read_overlay(
    context: &str,
    network: &str,
    consumer: &str,
    timeout: Duration,
) -> Result<(String, CandidateStates), Box<dyn std::error::Error>> {
    let raw = kubectl(
        context,
        ["-n", "grid-system", "get", "configmaps", "-o", "json"],
        timeout,
    )?;
    let value: serde_json::Value = serde_json::from_slice(&raw)?;
    let item = value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| {
            items.iter().find(|item| {
                is_overlay_config_map(item, network, consumer)
                    && item
                        .get("data")
                        .and_then(|data| data.get("routing-overlay.json"))
                        .is_some()
            })
        })
        .ok_or_else(|| format!("overlay ConfigMap for network={network} consumer={consumer} not found"))?;
    let raw_overlay = item
        .get("data")
        .and_then(|data| data.get("routing-overlay.json"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("overlay ConfigMap for {consumer} has no routing-overlay.json"))?;
    let envelope: serde_json::Value = serde_json::from_str(raw_overlay)?;
    let revision = envelope
        .get("revision")
        .and_then(|revision| revision.get("value"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let mut states = BTreeMap::new();
    if let Some(candidates) = envelope
        .get("overlay")
        .and_then(|overlay| overlay.get("candidates"))
        .and_then(serde_json::Value::as_array)
    {
        for candidate in candidates {
            if let (Some(cluster), Some(state)) = (
                candidate.get("cluster").and_then(serde_json::Value::as_str),
                candidate.get("admission_state").and_then(serde_json::Value::as_str),
            ) {
                states.insert(cluster.to_owned(), state.to_owned());
            }
        }
    }
    Ok((revision, states))
}

/// Match an operator-produced overlay by its structured identity labels.
///
/// Names are length-limited and may contain a hash suffix, so the labels are
/// the authoritative lookup key. Exact equality also prevents a gateway or
/// network whose name is a substring of another from being selected.
fn is_overlay_config_map(item: &serde_json::Value, network: &str, consumer: &str) -> bool {
    let Some(labels) = item.get("metadata").and_then(|metadata| metadata.get("labels")) else {
        return false;
    };
    labels
        .get("grid.praxis.fast/network")
        .and_then(serde_json::Value::as_str)
        == Some(network)
        && labels
            .get("grid.praxis.fast/gateway")
            .and_then(serde_json::Value::as_str)
            == Some(consumer)
}

/// Read exact accepted and serving revision fields from Praxis logs.
fn read_revisions(
    context: &str,
    consumer: &str,
    timeout: Duration,
) -> Result<RevisionPair, Box<dyn std::error::Error>> {
    let deployment = format!("deployment/{consumer}");
    let output = kubectl_owned(
        context,
        &[
            "-n".to_owned(),
            "grid-system".to_owned(),
            "logs".to_owned(),
            deployment,
            "-c".to_owned(),
            "praxis".to_owned(),
        ],
        timeout,
    )?;
    let logs = strip_ansi(&String::from_utf8_lossy(&output));
    Ok((
        latest_field(&logs, "accepted_revision"),
        latest_field(&logs, "serving_revision"),
    ))
}

fn latest_field(logs: &str, field: &str) -> Option<String> {
    logs.lines().rev().find_map(|line| {
        line.split_whitespace().find_map(|part| {
            part.strip_prefix(&format!("{field}="))
                .map(|value| value.trim_matches('"').trim_matches(',').to_owned())
        })
    })
}

fn strip_ansi(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut in_escape = false;
    for character in input.chars() {
        if in_escape {
            if character.is_ascii_alphabetic() {
                in_escape = false;
            }
        } else if character == '\x1b' {
            in_escape = true;
        } else {
            output.push(character);
        }
    }
    output
}

fn kubectl<const N: usize>(
    context: &str,
    args: [&str; N],
    timeout: Duration,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let output = Command::new("timeout")
        .arg(format!("{}s", timeout.as_secs().max(1)))
        .arg("kubectl")
        .arg("--context")
        .arg(context)
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "kubectl {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_selection_uses_explicit_gateway_ref() {
        let list: ProviderList = serde_json::from_value(serde_json::json!({"items":[
            {"metadata":{"name":"b"},"spec":{"gatewayRef":"gw"}},
            {"metadata":{"name":"a"},"spec":{"gatewayRef":"gw"}},
            {"metadata":{"name":"other"},"spec":{"gatewayRef":"other"}}
        ]}))
        .unwrap_or_else(|_| std::process::abort());
        let mut names: Vec<_> = list
            .items
            .into_iter()
            .filter(|p| p.spec.gateway_ref.as_deref() == Some("gw"))
            .filter_map(|p| p.metadata.name)
            .collect();
        names.sort();
        assert_eq!(names, ["a", "b"]);
    }

    #[test]
    fn omitted_gateway_ref_does_not_match() {
        let provider: Provider = serde_json::from_value(serde_json::json!({"metadata":{"name":"p"},"spec":{}}))
            .unwrap_or_else(|_| std::process::abort());
        assert_ne!(provider.spec.gateway_ref.as_deref(), Some("gw"));
    }

    #[test]
    fn revision_parser_uses_exact_fields_and_strips_ansi() {
        let logs = "previous_serving_revision=old \x1b[32maccepted_revision=next\x1b[0m serving_revision=\"next\"";
        let clean = strip_ansi(logs);
        assert_eq!(latest_field(&clean, "accepted_revision").as_deref(), Some("next"));
        assert_eq!(latest_field(&clean, "serving_revision").as_deref(), Some("next"));
        assert_eq!(
            latest_field(&clean, "previous_serving_revision").as_deref(),
            Some("old")
        );
    }

    #[test]
    fn revision_parser_does_not_match_similarly_named_fields() {
        let logs = "previous_serving_revision=old retained_serving_revision=retained";
        assert_eq!(latest_field(logs, "accepted_revision"), None);
        assert_eq!(latest_field(logs, "serving_revision"), None);
    }

    #[test]
    fn gateway_membership_requires_an_exact_reference() {
        let list: ProviderList = serde_json::from_value(serde_json::json!({"items":[
            {"metadata":{"name":"prefix"},"spec":{"gatewayRef":"gateway-a-extra"}},
            {"metadata":{"name":"match"},"spec":{"gatewayRef":"gateway-a"}},
            {"metadata":{"name":"suffix"},"spec":{"gatewayRef":"x-gateway-a"}}
        ]}))
        .unwrap_or_else(|_| std::process::abort());
        let names: Vec<_> = list
            .items
            .into_iter()
            .filter(|provider| provider.spec.gateway_ref.as_deref() == Some("gateway-a"))
            .filter_map(|provider| provider.metadata.name)
            .collect();
        assert_eq!(names, ["match"]);
    }

    #[test]
    fn overlay_lookup_requires_exact_network_and_gateway_labels() {
        let matching = serde_json::json!({
            "metadata": {"labels": {
                "grid.praxis.fast/network": "network-a",
                "grid.praxis.fast/gateway": "consumer-gateway-a"
            }}
        });
        let wrong_network = serde_json::json!({
            "metadata": {"labels": {
                "grid.praxis.fast/network": "network-a-backup",
                "grid.praxis.fast/gateway": "consumer-gateway-a"
            }}
        });
        let wrong_gateway = serde_json::json!({
            "metadata": {"labels": {
                "grid.praxis.fast/network": "network-a",
                "grid.praxis.fast/gateway": "consumer-gateway-a-backup"
            }}
        });
        assert!(is_overlay_config_map(&matching, "network-a", "consumer-gateway-a"));
        assert!(!is_overlay_config_map(
            &wrong_network,
            "network-a",
            "consumer-gateway-a"
        ));
        assert!(!is_overlay_config_map(
            &wrong_gateway,
            "network-a",
            "consumer-gateway-a"
        ));
    }

    #[test]
    fn selector_requires_one_mode() {
        assert!(validate_selector(None, None).is_err());
        assert!(validate_selector(Some(""), None).is_err());
        assert!(validate_selector(Some("gw"), Some("provider")).is_err());
        validate_selector(Some("gw"), None).unwrap_or_else(|_| std::process::abort());
        validate_selector(None, Some("provider")).unwrap_or_else(|_| std::process::abort());
    }
}
