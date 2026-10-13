# Getting Started

This guide installs a grid with Helm on two kind clusters: a hub and one site,
`site-a`. The site enrolls with the hub, and sites trust each other by SPIFFE
identity. Operators poll each other for load signals, and a model on the site
serves a request sent to the hub. It uses the published charts and images and
sets every value with `--set`.

For production clusters, see [Existing Clusters](existing-clusters.md) or
[AWS](aws.md).

## Before You Start

You need `kind`, `kubectl`, `helm` 3.17 or later, and a container runtime for
kind, Docker or Podman.

```bash
VERSION=0.2.0
CHARTS=oci://ghcr.io/praxis-proxy/charts
MODEL=Qwen/Qwen3-0.6B
```

## Clusters

Create both clusters. They share the container network `kind`, so a
LoadBalancer address in one cluster is reachable from the other.

```bash
for c in hub site; do kind create cluster --name "$c"; done
```

Under rootless Podman, kind needs systemd delegation: run each create as
`systemd-run --scope --user -p Delegate=yes kind create cluster --name "$c"`.

Pick the LoadBalancer addresses from the high end of that network's IPv4
subnet, read from the hub node, so they work under either runtime. Kind nodes
take low addresses, so `.230` through `.249` are free when no other kind clusters
run. Podman gives every kind cluster on the machine one shared /24, so if others
run, choose a range none of their LoadBalancers use.

```bash
NODE_IP=$(kubectl --context kind-hub get node \
  -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' \
  | tr ' ' '\n' | grep -m1 '\.')
NET=${NODE_IP%.*}
ENROLL_IP=$NET.230 HUB_SWIM_IP=$NET.231 HUB_SIG_IP=$NET.232
SITE_SWIM_IP=$NET.240 SITE_SIG_IP=$NET.241 SITE_GW_IP=$NET.242
```

Give each cluster MetalLB with its own range.

```bash
helm repo add metallb https://metallb.github.io/metallb
pool() { # <cluster> <range>
  helm install metallb metallb/metallb --version 0.14.9 --kube-context "kind-$1" \
    -n metallb-system --create-namespace --wait
  kubectl --context "kind-$1" apply -f - <<EOF
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata: {name: pool, namespace: metallb-system}
spec: {addresses: ["$2"]}
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata: {name: l2, namespace: metallb-system}
EOF
}
pool hub "$NET.230-$NET.239"
pool site "$NET.240-$NET.249"
```

## Hub

The hub runs enrollment, which holds the Grid CA and mints a single-use invite
for each site.

```bash
helm upgrade --install grid-enrollment $CHARTS/grid-enrollment --version $VERSION \
  --kube-context kind-hub -n grid-enrollment --create-namespace \
  --set host=enroll.grid.example.com --set enrollment.service.loadBalancerIP=$ENROLL_IP \
  --set invites.hub.network=grid --set invites.site-a.network=grid
```

The hub operator enrolls from its own invite, so copy the CA bundle and the
invite into its namespace and create the SWIM key the grid shares. See
[Secrets](#secrets) for what each one is.

```bash
kubectl config use-context kind-hub
kubectl create namespace grid
kubectl -n grid-enrollment get secret grid-ca-bundle -o jsonpath='{.data.ca\.crt}' | base64 -d \
  | kubectl -n grid create secret generic grid-ca-bundle --from-file=ca.crt=/dev/stdin
kubectl -n grid-enrollment get secret grid-invite-hub -o jsonpath='{.data.token}' | base64 -d \
  | kubectl -n grid create secret generic grid-invite-hub --from-file=token=/dev/stdin
kubectl -n grid label secret grid-invite-hub grid.praxis.fast/site=hub
head -c 32 /dev/urandom | kubectl -n grid create secret generic grid-swim-key --from-file=key=/dev/stdin
```

Install the operator, the site resources, and the gateway.

```bash
helm upgrade --install grid-operator $CHARTS/grid-operator --version $VERSION \
  --kube-context kind-hub -n grid \
  --set swim.siteName=hub --set enrollment.enabled=true \
  --set grid.peerTrust=spiffe --set grid.signals=poll --set signals.enabled=true \
  --set swim.service.loadBalancerIP=$HUB_SWIM_IP --set signals.service.loadBalancerIP=$HUB_SIG_IP

helm upgrade --install grid-site $CHARTS/grid-site --version $VERSION \
  --kube-context kind-hub -n grid \
  --set gridNetwork.gridId=grid-1 --set gridSite.name=hub --set gridSite.region=us-east \
  --set gridNetwork.peerTrust.mode=spiffe --set gridNetwork.signalTransport.mode=poll \
  --set peers.site-a.address=$SITE_GW_IP:8080 --set peers.site-a.region=us-west \
  --set "gridNetwork.gatewayRefs[0].name=grid-gateway" \
  --set "gridNetwork.gatewayRefs[0].namespace=grid" \
  --set "gridNetwork.gatewayRefs[0].localSiteName=hub"

helm upgrade --install grid-gateway $CHARTS/praxis-gateway --version $VERSION \
  --kube-context kind-hub -n grid \
  --set image.repository=ghcr.io/praxis-proxy/grid-gateway --set image.tag=v$VERSION \
  --set gatewayConfig.localSite=hub --set gatewayConfig.model=$MODEL \
  --set gatewayConfig.auth.mode=none --set gatewayConfig.backends.site-a.endpoint=$SITE_GW_IP:8080
```

The gateway chart's default image is not the grid gateway, so both `image`
values are required. Leaving the tag empty uses the chart's app version, not
`latest`.

The hub gateway runs with `auth.mode=none`, which is only for a gateway behind a
trusted front. In production, use `auth.mode=api-key`.

## Secrets

A site needs three Secrets from the hub before its operator starts:

- `grid-ca-bundle`: the Grid CA certificate the site checks enrollment against.
- `grid-invite-site-a`: its single-use invite, labeled with its site name. The
  operator refuses a token labeled for another site.
- `grid-swim-key`: the key every site uses to authenticate gossip.

Copy them from the hub to the site:

```bash
kubectl --context kind-site create namespace grid
copy() { # <namespace on hub> <secret> <key> <name on site>
  kubectl --context kind-hub -n "$1" get secret "$2" -o jsonpath="{.data.${3//./\\.}}" | base64 -d \
    | kubectl --context kind-site -n grid create secret generic "$4" --from-file="$3=/dev/stdin"
}
copy grid-enrollment grid-ca-bundle ca.crt grid-ca-bundle
copy grid-enrollment grid-invite-site-a token grid-invite-site-a
copy grid grid-swim-key key grid-swim-key
kubectl --context kind-site -n grid label secret grid-invite-site-a grid.praxis.fast/site=site-a
```

On real clusters, deliver them over one protected channel such as External
Secrets, and never commit an invite or the SWIM key to Git. An invite expires
after a day.

## Site

The site operator enrolls with the hub by name. On real clusters that name is in
DNS. On kind, give the site a Service that points at the hub's enrollment
address, so the name matches the hub's serving certificate.

```bash
kubectl config use-context kind-site
kubectl create namespace grid-enrollment
kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata: {name: grid-enrollment, namespace: grid-enrollment}
spec: {ports: [{name: https, port: 8443}]}
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: grid-enrollment
  namespace: grid-enrollment
  labels: {kubernetes.io/service-name: grid-enrollment}
addressType: IPv4
ports: [{name: https, port: 8443}]
endpoints: [{addresses: ["$ENROLL_IP"]}]
EOF
```

Start a mock model, then install the operator, the site resources, and the
gateway.

```bash
kubectl create namespace model
kubectl -n model create deployment model --image=ghcr.io/neuralmagic/vllm-vcr:vllm0.23 --port=8000
kubectl -n model set env deployment/model MODEL=$MODEL VLLM_PORT=8000
kubectl -n model expose deployment model --port=8000
MODEL_IP=$(kubectl -n model get service model -o jsonpath='{.spec.clusterIP}')

helm upgrade --install grid-operator $CHARTS/grid-operator --version $VERSION \
  --kube-context kind-site -n grid \
  --set swim.siteName=site-a --set swim.seeds=$HUB_SWIM_IP:7946 \
  --set enrollment.enabled=true \
  --set enrollment.url=https://grid-enrollment.grid-enrollment.svc:8443 \
  --set grid.peerTrust=spiffe --set grid.signals=poll --set signals.enabled=true \
  --set swim.service.loadBalancerIP=$SITE_SWIM_IP --set signals.service.loadBalancerIP=$SITE_SIG_IP

helm upgrade --install grid-site $CHARTS/grid-site --version $VERSION \
  --kube-context kind-site -n grid \
  --set gridNetwork.gridId=grid-1 --set gridSite.name=site-a --set gridSite.region=us-west \
  --set gridNetwork.peerTrust.mode=spiffe --set gridNetwork.signalTransport.mode=poll \
  --set peers.hub.region=us-east \
  --set inferenceProviders.model.endpoint=http://$MODEL_IP:8000 \
  --set inferenceProviders.model.model=$MODEL

helm upgrade --install grid-gateway $CHARTS/praxis-gateway --version $VERSION \
  --kube-context kind-site -n grid \
  --set image.repository=ghcr.io/praxis-proxy/grid-gateway --set image.tag=v$VERSION \
  --set gatewayConfig.role=provider --set gatewayConfig.localSite=site-a \
  --set gatewayConfig.peerTrust.mode=spiffe \
  --set gatewayConfig.peerTrust.spiffeId=spiffe://grid.internal/site/hub \
  --set gatewayConfig.backends.local.endpoint=$MODEL_IP:8000 \
  --set service.loadBalancerIP=$SITE_GW_IP
```

The site operator redeems its invite on first start and writes the site
identity, `spiffe://grid.internal/site/site-a`. The site gateway admits only the
hub's SPIFFE ID.

Each site declares the other as a peer. Declaring a peer turns on site
discovery and the TLS the signals endpoint serves with. Under SPIFFE there is no
digest to pin, so each names the other's region.

## Send a Request

Wait for the hub to verify the site, then send a request through the hub
gateway.

```bash
kubectl --context kind-hub wait gridsite/grid-site-a \
  --for=jsonpath='{.status.phase}'=Active --timeout=10m
kubectl --context kind-hub -n grid port-forward service/grid-gateway 8080:8080 &
curl -sS -D - http://127.0.0.1:8080/v1/chat/completions -H 'Content-Type: application/json' \
  -d "{\"model\": \"$MODEL\", \"messages\": [{\"role\": \"user\", \"content\": \"ping\"}], \"max_tokens\": 16}"
```

The response carries `x-grid-provider-site: site-a`, the site that served it.

## Next Steps

- [Enrollment](enrollment.md): invites, certificate rotation, and removing a
  site.
- [Existing Clusters](existing-clusters.md): the full reference for every chart
  value.
- [Routing](../routing.md): how the hub picks a site.

To remove everything, run `kind delete cluster --name hub` and `kind delete
cluster --name site`.
