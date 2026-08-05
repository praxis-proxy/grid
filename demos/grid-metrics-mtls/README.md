# Metrics mTLS Demo

Demonstrates Secret-backed TLS and mutual TLS (mTLS) for
`InferenceProvider.spec.metricsConfig` scraping.

## Prerequisites

- A running Grid operator
- `openssl` or `cfssl` for certificate generation

## Create Secrets

Generate a CA and server certificate, then create Kubernetes Secrets.
The operator reads these at reconcile time to build a TLS client for
metrics scraping.

```bash
# One-way TLS: CA secret only
kubectl create secret generic metrics-ca \
  --namespace=grid-system \
  --from-file=ca.crt=ca.pem

# mTLS: CA + client identity
kubectl create secret generic metrics-ca \
  --namespace=grid-system \
  --from-file=ca.crt=ca.pem

kubectl create secret tls metrics-client-cert \
  --namespace=grid-system \
  --cert=client.pem \
  --key=client-key.pem
```

## Apply example resources

```bash
# One-way TLS (server verification only)
kubectl apply -f resources/inferenceprovider-tls.yaml

# Mutual TLS (server + client verification)
kubectl apply -f resources/inferenceprovider-mtls.yaml
```

## Verify

```bash
kubectl get inferenceprovider -o wide
```

| Phase | Meaning |
|-------|---------|
| `Available` | TLS material valid, scraping works |
| `Degraded` | TLS Secret missing, key absent, or PEM invalid |
| `Unavailable` | Other config error (endpoint, auth, network) |

Check `status.reason` for the specific failure code:
`MetricsTlsSecretMissing`, `MetricsTlsKeyMissing`,
`MetricsTlsMaterialInvalid`, or `MetricsTlsIdentityMismatch`.

## Failure modes

| Scenario | status.reason | Scoring |
|----------|---------------|---------|
| Secret does not exist | `MetricsTlsSecretMissing` | excluded |
| Key absent from Secret | `MetricsTlsKeyMissing` | excluded |
| PEM cannot be parsed | `MetricsTlsMaterialInvalid` | excluded |
| Client cert/key mismatch | `MetricsTlsIdentityMismatch` | excluded |
| TLS handshake fails at scrape time | (none) | stale cache / excluded |
| Server rejects client cert | (none) | stale cache / excluded |

"Excluded" means the provider is inserted with `healthy: false`,
causing the scoring engine to remove it from active routing.
When a valid cached sample exists within `staleMetricsSeconds`,
the stale cache is used instead.
