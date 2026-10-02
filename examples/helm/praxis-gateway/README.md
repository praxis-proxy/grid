# praxis-gateway examples

Small values files for the `charts/praxis-gateway` chart. Each file sets one
`praxisConfig.source` and only the keys that source needs. Copy one and edit it.

| File | `praxisConfig.source` | Use it when |
|------|-----------------------|-------------|
| `byo.yaml` | `byo` | You already have a `praxis.yaml` and want the chart to run it. |
| `render-consumer.yaml` | `render` | The chart writes `praxis.yaml`. Callers send requests here and it routes them to grid sites. |
| `render-provider.yaml` | `render` | The chart writes `praxis.yaml`. This gateway serves grid peers over mTLS and forwards to one local backend. |
| `operator.yaml` | `operator` | The Grid operator writes `praxis.yaml` into a ConfigMap and the chart mounts it. |

## byo.yaml

The chart mounts your ConfigMap and does not manage it. Changing the ConfigMap does
not restart the pods. This is the default source. It is also the only source that
supports the `overlay` values.

```console
kubectl create configmap my-praxis-config --from-file=praxis.yaml
helm install gw charts/praxis-gateway -f examples/helm/praxis-gateway/byo.yaml
```

## render-consumer.yaml

- `auth.mode: none` is only safe behind an authenticating front. The example
  limits access with a `networkPolicy`.
- Put your sites under `praxisConfig.render.backends`.
- Set `gridIdentity.tlsSecretName` and `caSecretName` to your Grid TLS Secrets.

## render-provider.yaml

Needs the `grid-gateway` image, as set in the file.

- Replace `peerTrust.digest` with the SHA-256 of the peer's leaf certificate.
  The render fails until you do.
- Set `backends.local.endpoint` to the model Service ClusterIP.

## operator.yaml

The pod waits until the operator creates the ConfigMap. The operator's `praxis.yaml`
has no caller authentication, so keep the Service as ClusterIP behind an
authenticating front. The chart rejects `listenerTls`, `upstreamCA` and `overlay`
with this source.

## Related examples

`../hub-site` and `../existing-clusters` show full installs with the operator,
site and enrollment charts. Use those for a complete grid, and these for one gateway.
