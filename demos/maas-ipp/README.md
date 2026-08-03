# MaaS IPP lab (Forge)

Single-cluster Kind environment that reproduces the **stock MaaS** Kind datapath:

```text
Client → Istio Gateway → IPP-pre → Kuadrant Auth → IPP-post → HTTPRoute → LLM sim
```

Forge owns cluster lifecycle and infra stacks. `maas-controller` **owns EnvoyFilter / IPP deployment** — this demo never authors Praxis or IPP filter YAML.

This profile is for MaaS + Praxis integration work. It is **not** the Grid multi-cluster GLB demo, and it intentionally diverges from issue #2’s “CRDs-only / skip Authorino” simulation table.

## Pins

Version and namespace pins live in `forge.yaml` cluster `properties`. Stacks
template them into URL steps and `exec.env`; `scripts/lib.sh` defaults are only
fallbacks for running scripts outside Forge.


| Component     | Property / env                         |
| ------------- | -------------------------------------- |
| MetalLB       | `metallbVersion` (+ `metallbSha256`)   |
| Gateway API   | `gatewayApiVersion` (+ sha256)         |
| GIE CRDs      | `gieVersion` → `GIE_VERSION`           |
| Istio         | `istioVersion` → `ISTIO_VERSION`       |
| cert-manager  | `certManagerVersion` (+ sha256)        |
| Kuadrant Helm | `kuadrantVersion` → `KUADRANT_VERSION` |
| Namespaces    | `maasNamespace`, `gatewayNamespace`    |




## Prerequisites

- Docker, `kind`, `kubectl`, `kustomize` (≥5.7), `helm`, `jq`, `curl` (or `wget`), `openssl`, `python3`
- `istioctl` is **not** required on PATH — `scripts/install-istio.sh` fetches
  `cluster.properties.istioVersion` (via `ISTIO_VERSION`) into `demos/maas-ipp/.cache/`
- Optional for arm64 LLMIS build: `gh`, `docker buildx`
- A local checkout of [models-as-a-service](https://github.com/opendatahub-io/models-as-a-service):

```bash
export MAAS_ROOT=/path/to/models-as-a-service
```



## Bring up

From the grid repo root (paths in `forge.yaml` are relative to the config file):

```bash
export MAAS_ROOT=/path/to/models-as-a-service

# Create Kind cluster
cargo run -p praxis-forge -- up --config demos/maas-ipp/forge.yaml

# Apply stacks (metallb → … → maas-fixtures). Required after `up`.
cargo run -p praxis-forge -- apply local --config demos/maas-ipp/forge.yaml
```

Cluster context: `kind-maas-ipp-local`.

Re-apply a single stack if needed:

```bash
cargo run -p praxis-forge -- apply local --stack maas-platform --config demos/maas-ipp/forge.yaml
cargo run -p praxis-forge -- apply local --stack maas-fixtures --config demos/maas-ipp/forge.yaml
```

Optional overrides (consumed by `scripts/install-maas-platform.sh`):

```bash
export MAAS_CONTROLLER_IMAGE=quay.io/you/maas-controller:dev
export MAAS_API_IMAGE=quay.io/you/maas-api:dev
export IPP_IMAGE=quay.io/opendatahub/odh-ai-gateway-payload-processing:odh-stable
# Default for this lab is praxis (requires MAAS_ROOT with MAAS_IPP_PROFILE support).
export MAAS_IPP_PROFILE=praxis
export PRAXIS_EXTPROC_IMAGE=praxis-extproc:dev
# Stock llm-d IPP instead:
# export MAAS_IPP_PROFILE=llm-d
```



## API key

```bash
API_KEY=$(kubectl --context kind-maas-ipp-local -n maas-system exec deploy/maas-api -- \
  curl -sk https://localhost:8443/v1/api-keys \
  -H "X-MaaS-Username: demo-user" \
  -H 'X-MaaS-Group: ["system:authenticated"]' \
  -H "Content-Type: application/json" \
  -d '{"name":"demo"}' | jq -r '.key')
echo "$API_KEY"
```



## Call models

Gateway LB (MetalLB on the Kind docker network — reachable from the Kind host):

```bash
GW=$(kubectl --context kind-maas-ipp-local -n istio-system \
  get svc maas-default-gateway-istio -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
```

**Internal llm-d sim** (`LLMInferenceService` `sim-internal`):

```bash
curl -sk "https://${GW}/llm-internal/sim-internal/v1/chat/completions" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"model":"facebook/opt-125m","messages":[{"role":"user","content":"hi"}],"max_tokens":8}'
```

**External model** (`ExternalModel` `llm-katan-openai` — remote simulator must be reachable):

```bash
curl -sk "https://${GW}/llm/llm-katan-openai/v1/chat/completions" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"model":"llm-katan-openai","messages":[{"role":"user","content":"hi"}],"max_tokens":8}'
```

Port-forward instead of LB:

```bash
kubectl --context kind-maas-ipp-local -n istio-system \
  port-forward svc/maas-default-gateway-istio 19090:80
# then http://localhost:19090/... with the same paths
```



## Validate

```bash
./demos/maas-ipp/scripts/validate.sh kind-maas-ipp-local
```

Expect:

- unauthenticated `/v1/models` → **401**
- API key + internal llm-d sim chat completion → **200** with a choices body



## Tear down

```bash
cargo run -p praxis-forge -- down --config demos/maas-ipp/forge.yaml
```



## Rebuild with local maas-controller changes

EnvoyFilter YAML stays in the controller. After editing under `$MAAS_ROOT/maas-controller`:

```bash
export MAAS_ROOT=/path/to/models-as-a-service
# rebuild.sh retags quay.io/opendatahub/maas-controller:* → localhost/…
# so imagePullPolicy=Always cannot re-pull over the kind-loaded image.
./demos/maas-ipp/scripts/rebuild.sh kind-maas-ipp-local maas-controller
```

Builds from `MAAS_ROOT`, `kind load`s, sets the Deployment image, patches
`imagePullPolicy` to `IfNotPresent`, and rolls out.

Also: `./demos/maas-ipp/scripts/rebuild.sh kind-maas-ipp-local maas-api`

Load `praxis-extproc:dev` separately if `MAAS_IPP_PROFILE=praxis`:

```bash
kind load docker-image praxis-extproc:dev --name maas-ipp-local
```

Forge must **not** apply a competing Praxis EnvoyFilter; the controller reconcile owns IPP.

## Stacks


| Stack           | Role                                                            |
| --------------- | --------------------------------------------------------------- |
| `metallb`       | LB + Kind docker-network pool                                   |
| `gateway-api`   | GW API 1.5.1 + GIE CRDs                                         |
| `istio`         | 1.30.3 minimal + GIE pilot flags                                |
| `cert-manager`  | cert-manager + maas-api CA chain                                |
| `kuadrant`      | Helm Kuadrant + Authorino trust                                 |
| `maas-platform` | Postgres, CRDs, KServe/llmisvc, Gateway, stock controller → IPP |
| `maas-fixtures` | ExternalModel + LLMInferenceService sim + subscription          |


GIE is enabled so an InferencePool backend can be attached later without reinstalling Istio. v1 validate uses the stock MaaS HTTPRoute → LLMInferenceService path.
