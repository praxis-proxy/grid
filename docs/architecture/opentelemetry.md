# OpenTelemetry for Grid gateways

Grid gateway builds can export traces to an OTLP/gRPC collector. Export is
configured on the gateway process; routing overlays remain limited to routing
state.

## Enable export

The default configuration has no collector endpoint, so it starts without an
OTLP exporter and does not require a collector. To opt in, use either
`GatewayRef.consumerConfig.telemetry` for operator-generated consumer config or
`praxisConfig.render.telemetry` for the Helm-generated Praxis config.

An operator example:

```yaml
spec:
  gatewayRefs:
    - name: edge
      namespace: grid
      consumerConfig:
        enabled: true
        telemetry:
          otlpEndpoint: http://otel-collector.observability:4317
          samplingRate: 0.1
          serviceName: grid-edge
          environment: production
```

For Helm, select the rendered source, enable
`praxisConfig.render.telemetry`, set the local site, and use the Grid gateway image:

```yaml
image:
  repository: ghcr.io/praxis-proxy/grid-gateway
  flavor: grid-gateway
grid:
  siteName: edge
gridIdentity:
  tlsSecretName: grid-site-identity
  caSecretName: grid-ca
praxisConfig:
  source: render
  render:
    model: my-model
    auth:
      mode: none
    backends:
      site-a:
        endpoint: 203.0.113.20:8080
    telemetry:
      enabled: true
      otlpEndpoint: http://otel-collector.observability:4317
      samplingRate: 0.1
      serviceName: grid-edge
```

Both paths render a top-level Praxis `telemetry` block. The Helm chart adds the
`trace_context` filter when telemetry is enabled. The operator-generated
consumer config does the same. Neither path places exporter settings in the
routing overlay.

## Credentials and lifecycle

Do not put credentials in `otlpEndpoint`, telemetry fields, routing overlays,
or filter configuration. Praxis reads OTLP headers from
`OTEL_EXPORTER_OTLP_HEADERS`; provide that variable from a Deployment-managed
Secret. Helm exposes the container `env` list for a Secret reference:

```yaml
env:
  - name: OTEL_EXPORTER_OTLP_HEADERS
    valueFrom:
      secretKeyRef:
        name: collector-credentials
        key: headers
```

The Secret value uses the OpenTelemetry `key=value` header format. Generic
`OTEL_EXPORTER_OTLP_HEADERS`, trace-specific
`OTEL_EXPORTER_OTLP_TRACES_HEADERS`, and configured `otlp_headers` all require
an explicitly specified `https://` collector endpoint. A scheme-less endpoint
is rejected when headers are present, regardless of the OTLP `INSECURE`
environment settings. This gateway build supports OTLP/gRPC; it rejects
`OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf` while the locked Praxis HTTP
exporter and batch processor are incompatible. The operator
only creates the consumer `ConfigMap`; the deployment manager must add the same
Secret-backed environment reference to its gateway Deployment. Praxis redacts
OTLP header values from its config debug representation.

The HTTPS preflight protects exporter headers from plaintext transport; it
does not prove certificate-verified delivery. Credentialed HTTPS export
requires the Praxis trust fix in
[praxis#1354](https://github.com/praxis-proxy/praxis/issues/1354), a Praxis
release containing it, and the Grid qualification tracked in
[Grid #301](https://github.com/praxis-proxy/grid/issues/301).

Exporter setup runs once at process startup. Helm rolls gateway pods when its
telemetry values change. An externally managed consumer Deployment must be
restarted after its generated ConfigMap changes, and a gateway must be restarted
after the collector endpoint or Secret-backed environment changes. On normal
server return, Grid drops Praxis's `TracingGuard`, which shuts down the OTLP
provider and flushes queued spans.

## Build and spans

The `grid-gateway` binary built by `deploy/gateway/Containerfile` enables the
Praxis `otel` feature and the Praxis AI v0.4.1 `opentelemetry` feature. The
tracked gateway lockfile currently resolves Praxis 0.7.3. Use an image built
from this Grid target, published as `ghcr.io/praxis-proxy/grid-gateway`, for
this configuration. The chart's default `ghcr.io/praxis-proxy/ai:0.4.0` image
does not include the Grid build features.

With the AI feature enabled, these short semantic spans are supported when the
corresponding filters run:

- HTTP server spans and `upstream_exchange` internal spans from Praxis 0.7.3.
- `routing.select` from `intelligent_route`, including the serving overlay
  semantic revision when that revision is available to the filter.
- `provider.route` from `provider_route`, including a validated edge overlay
  revision when present.

The AI implementation projects bounded routing fields into those spans;
it does not add prompt or body contents, credential values, authorization
headers, cookies, session keys, or raw request IDs. These spans describe route
selection and resolution; `provider.route` does not prove the model backend
served the request.

## Cross-gateway trace linkage

The locked Praxis 0.7.3 build forwards W3C trace headers but does not connect them to the
exported HTTP server span or emit an exported HTTP client span for the upstream
attempt. Edge and provider gateways therefore export separate local traces,
even when the backend receives a forwarded `traceparent`. The routing spans
remain local children of their gateway's HTTP server span.

Cross-gateway exported parentage requires a Praxis framework release containing
the inbound context and upstream-attempt span changes. Once Grid updates its
gateway dependency to that release, rebuild the tracked image and verify the
collector's parent IDs across both gateways before claiming linked traces.
The Praxis AI routing-span feature alone does not supply that linkage.
