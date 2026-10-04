# Hub and Site Install

A hub and one site, `site-a`, installed with `helm` from the grid charts. Each command sets only what differs per cluster: names, addresses, URLs, and digests. Chart defaults cover the rest. `values/` holds the same settings as values files, for reference.

This install routes on static chart candidates: the hub gateway sends each request to the site backends it lists. The operator still computes the routing overlay, but the gateway does not read it yet. Dynamic routing from the operator follows once the gateway reloads its serving config.

## Prerequisites

- A LoadBalancer implementation on every cluster. The enrollment, SWIM, and site gateway Services are type LoadBalancer. The SWIM Service carries UDP 7946 and, with signals on, TCP 9091 on one LoadBalancer, so the implementation must support mixed protocols.
- `enroll.grid.example.com` resolves to the hub enrollment LoadBalancer from every site. On OpenShift, add `--set route.enabled=true`, use the Route host as `host`, and drop `:8443` from `enrollment.url`, since the Route serves on 443.
- Peers reach each other's SWIM UDP port 7946 and the site gateway port 8080.
- The hub consumer gateway runs with `auth.mode=none`, which belongs only behind a trusted authenticating front. In production, use `api-key` with the maas-api validate URL.

## Leaf Digest

Pin trust needs each site's leaf certificate digest, which exists once its operator has enrolled. This prints it for the cluster in the current context:

```bash
kubectl -n grid get secret grid-site-identity -o jsonpath='{.data.tls\.crt}' \
  | base64 -d | openssl x509 -outform DER | openssl dgst -sha256 -r | cut -d' ' -f1
```

## Hub

The commands run from the repository root. Enrollment runs in its own namespace, `grid-enrollment`, which holds the Grid CA key. Dedicate that namespace to enrollment: no other principal may create Secrets there.

```bash
helm upgrade --install grid-enrollment charts/grid-enrollment -n grid-enrollment --create-namespace \
  --set host=enroll.grid.example.com --set invites.hub.network=grid --set invites.site-a.network=grid
helm upgrade --install grid-operator charts/grid-operator -n grid --create-namespace \
  --set swim.siteName=hub --set enrollment.enabled=true
kubectl -n grid-enrollment get secret grid-ca-bundle -o jsonpath='{.data.ca\.crt}' | base64 -d \
  | kubectl -n grid create secret generic grid-ca-bundle --from-file=ca.crt=/dev/stdin
kubectl -n grid-enrollment get secret grid-invite-hub -o jsonpath='{.data.token}' | base64 -d \
  | kubectl -n grid create secret generic grid-invite-hub --from-file=token=/dev/stdin
kubectl -n grid label secret grid-invite-hub grid.praxis.fast/site=hub
head -c 32 /dev/urandom | kubectl -n grid create secret generic grid-swim-key --from-file=key=/dev/stdin
helm upgrade --install grid-site charts/grid-site -n grid \
  --set gridNetwork.gridId=grid-1 --set gridSite.name=hub --set peers.site-a.address=203.0.113.20:8080
helm upgrade --install grid-gateway charts/praxis-gateway -n grid \
  --set gatewayConfig.localSite=hub --set gatewayConfig.model=my-model --set gatewayConfig.auth.mode=none \
  --set gatewayConfig.backends.site-a.endpoint=203.0.113.20:8080
```

The enrollment chart creates the Grid CA and mints one token per invite into Secret `grid-invite-<site>`. The hub operator enrolls through the in-cluster enrollment Service, so you copy its CA bundle and invite into its own namespace, with the site label the operator checks. It writes the hub identity, `grid-site-identity` and `grid-ca`. The SWIM key is a Secret you create once and keep. The hub gateway is a ClusterIP Service behind your front, so the hub advertises no gateway address and sites list it as Discovered.

## Site

Set `HUB_DIGEST` to the hub's leaf digest, and use your grid release as the gateway image tag.

```bash
helm upgrade --install grid-operator charts/grid-operator -n grid --create-namespace \
  --set swim.siteName=site-a --set swim.seeds=203.0.113.11:7946 \
  --set enrollment.enabled=true --set enrollment.url=https://enroll.grid.example.com:8443
kubectl --context hub -n grid-enrollment get secret grid-ca-bundle -o jsonpath='{.data.ca\.crt}' | base64 -d \
  | kubectl -n grid create secret generic grid-ca-bundle --from-file=ca.crt=/dev/stdin
kubectl --context hub -n grid-enrollment get secret grid-invite-site-a -o jsonpath='{.data.token}' | base64 -d \
  | kubectl -n grid create secret generic grid-invite-site-a --from-file=token=/dev/stdin
kubectl -n grid label secret grid-invite-site-a grid.praxis.fast/site=site-a
kubectl --context hub -n grid get secret grid-swim-key -o jsonpath='{.data.key}' | base64 -d \
  | kubectl -n grid create secret generic grid-swim-key --from-file=key=/dev/stdin
helm upgrade --install grid-site charts/grid-site -n grid \
  --set gridNetwork.gridId=grid-1 --set gridSite.name=site-a --set peers.hub.digest="$HUB_DIGEST" \
  --set inferenceProviders.my-model.endpoint=http://10.96.0.20:8000
helm upgrade --install grid-gateway charts/praxis-gateway -n grid \
  --set image.repository=ghcr.io/praxis-proxy/grid-gateway --set image.tag=v0.1.4 \
  --set gatewayConfig.role=provider --set gatewayConfig.localSite=site-a \
  --set gatewayConfig.peerTrust.digest="$HUB_DIGEST" --set gatewayConfig.backends.local.endpoint=10.96.0.20:8000
```

The site needs three Secrets from the hub: the Grid CA bundle, its invite token with its site label, and the SWIM key. The commands above copy them one at a time. A fleet delivers them with ACM or External Secrets instead. The site operator redeems the token on its first start and writes the site identity. Then pin the site on the hub by rerunning the hub `grid-site` command with `--set peers.site-a.digest=<site-a leaf digest>` added.

A later change returns the SWIM key and the CA bundle from enrollment, so you copy only the invite token (design-1s3). In the longer term, the grid-operator chart alone installs a site, and the operator bootstraps the rest. That design is in progress.

An invite expires after a day by default, and an unredeemed one stays in place. To mint a new one, delete `grid-invite-<site>` in `grid-enrollment` and rerun the enrollment command.

## Defaults These Commands Rely On

- grid-enrollment: `host` joins the serving cert names, is the default Route host, and makes the enrollment Service a LoadBalancer when no Route renders. An invite's `network` defaults to `grid`.
- grid-operator with `enrollment.enabled`: the site name follows `swim.siteName`, the URL is the in-cluster grid-enrollment Service, the CA bundle is Secret `grid-ca-bundle`, the token is `grid-invite-<site>`, the SWIM Service is a LoadBalancer, the gateway Service is `grid-gateway`, and the render refuses Secret access in `grid-enrollment`.
- grid-site: the network is `grid`. Listing `peers` turns on site discovery and the TLS Secrets the operator writes. A peer's probe name is `<name>.grid.internal`.
- praxis-gateway: it renders its own config without a BYO ConfigMap. A grid gateway, a provider or a consumer with site backends, mounts `grid-site-identity` and `grid-ca` and takes its release name. A site backend uses mutual TLS to `<site>.grid.internal`. A provider's Service is a LoadBalancer, and `local` is its one plaintext backend.

## Credential Delivery

In a fleet, deliver the three Secrets over one protected channel:

- ACM: `{{hub fromSecret ... | protect hub}}`. Without `protect`, the token and the SWIM key sit in plaintext in every replicated Policy.
- External Secrets: one store per site that reads only that site's invite and the shared SWIM key, never every site's invite.
- The CA bundle travels with the token, since it is what the site checks the enrollment service against.
- Keep the invite's site label: the operator refuses a token labeled for another site.
- Never commit an invite or the SWIM key to Git.
- Mint invites when the site is about to install. The default TTL is a day. Raise `expiresInSecs` to three days if provisioning lags, and to seven only for air-gapped sites. Revoke the token if you cancel provisioning.
- Do not let Argo CD track or prune `grid-site-identity` or `grid-ca` on a site. The operator writes them once. If someone deletes them, it retries a spent token, and the hub cannot release the site name yet.

## Peer Trust

The grid-site `gridNetwork.peerTrust.mode` is the source of truth for the grid. Each provider gateway's `gatewayConfig.peerTrust.mode` must match it.

Each operator routes to a peer only once a probe of its gateway, at the peer `address`, finds a leaf that matches the peer `digest`, which makes the peer Active. Declare a peer before it joins, since the operator creates a GridSite for any peer it meets and Helm does not adopt it. To rotate a pin, set `nextDigest` to the new digest, then move it to `digest` once the peer is Active.

`peerTrust.mode` picks how the signals listener and the provider gateway admit peers. In `pin` mode, the default, they admit the pinned digests. In `spiffe` mode, add `--set gridNetwork.peerTrust.mode=spiffe` to both grid-site commands, and on the site gateway replace the digest with `--set gatewayConfig.peerTrust.mode=spiffe --set gatewayConfig.peerTrust.spiffeId=spiffe://grid.internal/site/hub`. It needs the grid-gateway image built with the praxis `spiffe` feature, which praxis marks experimental.

Only `spiffe` rotates site certificates. In `pin` mode rotation is off: before a site's certificate expires, shown as `status.identity.notAfter` on its `GridNetwork`, re-enroll the site and update the digest its peers pin.

## Operations

A gateway with `gridServing` reads the operator's serving config only at start. After you add a site or rotate a pin, run `kubectl -n grid rollout restart deploy/grid-gateway` once the `grid.praxis.fast/serving-digest` annotation on its `grid-serving-*` ConfigMap changes. The static install in this example rolls the gateway through its own chart upgrade.

### Remove a Site

1. Drop the site from the hub invites, the hub grid-site peers, and the hub gateway backends, and rerun those commands.
2. Revoke an unredeemed token, and delete its GridSite on every peer.
3. A removed site still holds a Grid-CA leaf and the SWIM key until both change. Revoking it for good means rotating the Grid CA and the SWIM key. See [SWIM Transport Authentication](../../../docs/architecture/auth.md#swim-transport-authentication) and [Grid mTLS Identity](../../../docs/architecture/auth.md#grid-mtls-identity).

### SWIM Key Rotation

SWIM has one key and no keyring, so a rotation is a flag day. Replace `grid-swim-key` on every site at the same time and restart each operator. Sites on different keys cannot hear each other until all of them hold the new key.

## Upgrade

`helm upgrade` upgrades the grid CRDs with the grid-operator chart. When a platform owns the CRDs, such as the RHOAI `aiGrid` component, set `crds.enabled=false`. The grid-operator chart README covers adopting CRDs that an older release installed.

## Known Limits

Site certificates last 180 days. Under `spiffe` trust the operator rotates them when a third of the lifetime remains, around day 120 (see Certificate rotation in the enrollment docs), and rolls the gateway Deployment, because the gateway loads its upstream mutual TLS client certificate only at start. Under `pin` trust nothing rotates them, so re-enroll and re-pin each site before it expires. The praxis-gateway chart README lists the gateway limits.
