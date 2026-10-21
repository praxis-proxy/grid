//! The site name, from the site identity's SPIFFE ID, never the pod hostname.

use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client, api::ListParams};

use crate::{crd::grid_network::GridNetwork, error::OperatorError};

/// The site a certificate's SPIFFE ID names, `None` when it names none.
#[must_use]
pub fn of_cert(cert_pem: &str) -> Option<String> {
    let der = crate::resources::tls_backend::first_cert_der_from_pem(cert_pem).ok()?;
    certs::leaf_spiffe_id(&der)
        .as_deref()
        .and_then(certs::site_of_spiffe_id)
        .map(str::to_owned)
}

/// The site name from the identity, else from configuration, else none.
///
/// # Errors
///
/// Returns the reason when the identity names no site, the name is invalid, or the two differ.
pub fn resolve(identity_pem: Option<&str>, configured: Option<&str>) -> Result<Option<String>, String> {
    let configured = configured.filter(|name| !name.is_empty());
    if let Some(name) = configured {
        certs::validate_site_name(name)
            .map_err(|_invalid| format!("GRID_SWIM_SITE_NAME {name:?} is not a lowercase DNS label"))?;
    }
    let Some(pem) = identity_pem else {
        return Ok(configured.map(str::to_owned));
    };
    let identity = of_cert(pem).ok_or("the site identity certificate names no grid site")?;
    match configured {
        Some(name) if name != identity => Err(format!(
            "GRID_SWIM_SITE_NAME is {name:?}, but the site identity names {identity:?}; drop the setting or fix it"
        )),
        _ => Ok(Some(identity)),
    }
}

/// Where the site identity Secret is.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Identity {
    /// Namespace of the Secret.
    pub namespace: String,
    /// Name of the Secret.
    pub secret: String,
}

/// The `GridNetwork` this operator serves, warning when more than one exists.
///
/// # Errors
///
/// Returns [`OperatorError::Kube`] when listing fails.
pub async fn sole_network(client: &Client) -> Result<Option<GridNetwork>, OperatorError> {
    let list = Box::pin(Api::<GridNetwork>::all(client.clone()).list(&ListParams::default())).await?;
    if list.items.len() > 1 {
        tracing::warn!(
            count = list.items.len(),
            "more than one GridNetwork; one operator serves one grid"
        );
    }
    Ok(list.items.into_iter().next())
}

/// The identity Secret the `GridNetwork` names, else `secret` in `namespace`.
///
/// # Errors
///
/// Returns [`OperatorError::Kube`] when listing `GridNetwork`s fails.
pub async fn locate(client: &Client, namespace: &str, secret: &str) -> Result<Identity, OperatorError> {
    let named = Box::pin(sole_network(client))
        .await?
        .and_then(|network| network.spec.tls.site_secret_ref);
    Ok(named.map_or_else(
        || Identity {
            namespace: namespace.to_owned(),
            secret: secret.to_owned(),
        },
        |found| Identity {
            namespace: found.namespace,
            secret: found.name,
        },
    ))
}

/// The certificate in the identity Secret, if it holds one yet.
///
/// # Errors
///
/// Returns [`OperatorError::Kube`] when reading the Secret fails.
pub async fn identity_pem(client: &Client, at: &Identity) -> Result<Option<String>, OperatorError> {
    let secret = Box::pin(Api::<Secret>::namespaced(client.clone(), &at.namespace).get_opt(&at.secret)).await?;
    Ok(secret
        .as_ref()
        .and_then(crate::resources::secret::public_cert_pem_from_secret))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    fn identity(site: &str) -> String {
        let ca = certs::generate_ca("grid-test").expect("ca");
        certs::generate_site_cert(&ca, site).expect("site cert").cert_pem
    }

    #[test]
    fn the_identity_names_the_site() {
        let pem = identity("east");
        assert_eq!(resolve(Some(&pem), None), Ok(Some("east".to_owned())));
    }

    #[test]
    fn a_matching_configured_name_is_accepted() {
        let pem = identity("east");
        assert_eq!(resolve(Some(&pem), Some("east")), Ok(Some("east".to_owned())));
    }

    #[test]
    fn a_configured_name_that_disagrees_with_the_identity_is_refused() {
        let pem = identity("east");
        let refused = resolve(Some(&pem), Some("west")).expect_err("mismatch is refused");
        assert!(
            refused.contains("\"west\"") && refused.contains("\"east\""),
            "{refused}"
        );
    }

    #[test]
    fn without_an_identity_the_configured_name_stands() {
        assert_eq!(resolve(None, Some("east")), Ok(Some("east".to_owned())));
    }

    #[test]
    fn without_an_identity_or_a_name_there_is_no_site() {
        assert_eq!(resolve(None, None), Ok(None));
        assert_eq!(resolve(None, Some("")), Ok(None), "an empty setting is unset");
    }

    #[test]
    fn an_invalid_configured_name_is_refused() {
        for bad in [" ", "East", "east_1", "-east"] {
            resolve(None, Some(bad)).expect_err(bad);
        }
    }

    #[test]
    fn an_identity_naming_no_site_is_refused() {
        resolve(Some("not a certificate"), None).expect_err("a non-certificate names no site");
    }
}
