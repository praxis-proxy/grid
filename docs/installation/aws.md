# Installing on AWS

AWS-unique settings:

- `platform: aws`, because gossip is UDP and the default load balancer carries none. Every
  LoadBalancer Service the charts render then gets an internal Network Load Balancer.
- `loadBalancerScope`, `internal` by default. Internal load balancers have private addresses
  only, so sites reach each other over private connectivity. `external` makes them
  internet-facing; see [Internet-facing load balancers](#internet-facing-load-balancers).
- `swim.service.loadBalancerSourceRanges`, the other sites' machine network CIDRs, which
  both operator Services admit.
- `grid.seeds`, the other sites' SWIM Service hostnames (each one's NLB DNS name), which the
  chart gives the SWIM port.
- `host` as a DNS name, since it joins the serving certificate's DNS names only.

## Load balancers

| Service | Chart | Protocol | Load balancer |
|---|---|---|---|
| `grid-operator-swim` | grid-operator | UDP 7946 | Internal NLB, cross-zone, `externalTrafficPolicy: Local` |
| `grid-operator-signals` | grid-operator | TCP 9091 | Internal NLB, cross-zone, `externalTrafficPolicy: Local` |
| Provider gateway | praxis-gateway | TCP | Internal NLB, when the Service is a LoadBalancer |
| Enrollment | grid-enrollment | TCP | Internal NLB, when `host` is set and no Route renders |

Both operator Services get cross-zone load balancing. The operator advertises one of the
SWIM NLB's addresses, and without cross-zone that address reaches only the targets in its own
zone; signals would lose a zone's address on every rollout that moves the pod.

Each chart takes `platform` and `loadBalancerScope` separately, so set them on every release
that renders a LoadBalancer Service. A Service's own `annotations` override the platform's.

AWS fixes the load balancer's type and scheme when it creates it. On an installed release,
the cloud provider ignores a `loadBalancerScope` change and refuses a `platform` change that
turns a Classic load balancer into an NLB, keeping the old one but no longer updating it.
Delete the Service and let the release recreate it, which gives it a new address.

A platform install of `deploy/overlays/openshift` needs no setting: that render sets
`platform: any`, which carries every cloud's keys, and each cloud reads only its own.

## Before you start

Private connectivity between every pair of sites: VPC peering or a transit gateway, with
routes for each other's machine network in both directions, and the clusters' node security
groups admitting the other sites' machine networks. An internal NLB's DNS name resolves
publicly to private addresses, so a site without a route to them resolves the name and then
times out.

Give each cluster a distinct `machineNetwork`. Clusters sharing `10.0.0.0/16`, the installer
default, cannot be peered.

A public Route53 zone for the base domain, and each cluster's NAT address, which
[Restrict the endpoints](#restrict-the-endpoints) admits to the API and router:

```bash
aws ec2 describe-nat-gateways --filter Name=vpc-id,Values=<vpc> \
  --query 'NatGateways[].NatGatewayAddresses[].PublicIp' --output text
```

Each site's SWIM Service hostname, once its operator is installed:

```bash
oc get svc -n grid grid-operator-swim -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'
```

A Route53 name pointing at it works the same, and keeps the other sites' values unchanged if
the NLB is ever replaced.

Pin each cluster to one availability zone: a zone
costs a NAT gateway and its Elastic IP, and the bootstrap machine takes one Elastic IP per
cluster, so three clusters across three zones each want nine NAT gateways and twelve Elastic
IPs. Pinned, that is three and six. Check the account's own Elastic IP quota (`L-0263D0A3`)
rather than the published default, since it is raised per account.

## Restrict the endpoints

`publish: External` exposes 6443, and 443 with the console, OAuth endpoint and `kubeadmin`
password. Close both to everything but your address and the clusters' NAT addresses, before
installing anything.

6443 is on the API load balancer's group and 443 on the router's, so each port is a different
group and each needs its own pair of calls. Enrollment is a Route, so a site that cannot
reach 443 never enrolls.

```bash
lock() { # <security-group> <port>
  aws ec2 authorize-security-group-ingress --group-id "$1" --protocol tcp --port "$2" --cidr "$ALLOWED"
  aws ec2 revoke-security-group-ingress    --group-id "$1" --protocol tcp --port "$2" --cidr 0.0.0.0/0
}
lock "$API_SG" 6443
lock "$ROUTER_SG" 443
```

Pointing both ports at one group leaves the other group's world-open rule in place, and the
calls that do nothing still succeed, so nothing reports the gap.

Authorise before revoking. Match groups on `kubernetes.io/cluster/<infraID>` **or** the
`<infraID>` name prefix, since `<infraID>-apiserver-lb` carries no cluster tag. Leave ICMP
types 3 and 4, or path MTU discovery breaks.

## Install

The grid CRDs go in before the release, because the chart renders them and the resources
that use them together and Helm validates the whole set first:

```bash
for k in gridnetwork gridsite inferenceprovider agenttoolprovider; do
  helm template grid-operator charts/grid-operator -n grid \
    --show-only templates/crds/$k.yaml | oc apply -f -
done
```

Then install the release with `--set crds.enabled=false`, since the CRDs are now the
platform's rather than Helm's.

Enrollment on the hub. `hubSite.namespace` must exist first:

```bash
kubectl create namespace grid

helm install grid charts/grid-enrollment -n grid-enroll --create-namespace \
  --set image.repository=<registry>/grid-enrollment \
  --set image.digest=sha256:<digest> \
  --set host=enrollment.apps.hub.example.com \
  --set route.host=enrollment.apps.hub.example.com \
  --set hubSite.name=hub \
  --set invites.site-1.network=grid \
  --set invites.site-2.network=grid
```

Three Secrets have to reach each cluster's operator namespace before its operator starts,
since the operator reads them there and not from the release namespace. That namespace is
`grid` for every cluster, the hub included: `hubSite.namespace` and `invitePolicy.namespace`
both default to it, so installing the operator anywhere else delivers every Secret to a
namespace it never reads.

| Secret | From | To |
|---|---|---|
| `grid-invite-<site>` | release namespace | that site only |
| `grid-ca-bundle` (`ca.crt`) | release namespace | every site |
| `grid-swim-key` | release namespace | every cluster, hub included |

A key that arrives late is picked up on the next reconcile, within about 30 seconds. No
restart is needed.

Then the operator on each cluster:

```bash
helm install grid-operator charts/grid-operator -n grid --create-namespace \
  --set crds.enabled=false \
  --set platform=aws \
  --set 'swim.service.loadBalancerSourceRanges={<other site CIDR>,<hub CIDR>}' \
  --set swim.siteName=site-1 \
  --set grid.id=aws \
  --set enrollment.enabled=true \
  --set enrollment.url=https://enrollment.apps.hub.example.com \
  --set signals.enabled=true \
  --set swim.service.enabled=true \
  --set swim.service.type=LoadBalancer \
  --set image.repository=<registry>/grid-operator \
  --set image.digest=sha256:<digest>
```

The hub adds `--set enrollment.enabled=false`; its identity comes from the bootstrap Job.

A SWIM hostname exists only once that site's Service does, so seeding takes a second pass.
Until then the site has no seeds and runs alone, which is harmless. Once every
`grid-operator-swim` Service shows a hostname, point each cluster, hub included, at the
others:

```bash
helm upgrade grid-operator charts/grid-operator -n grid --reuse-values \
  --set grid.seeds='<other site SWIM> <hub SWIM>'
```

A provider gateway is its own praxis-gateway release. A provider's Service defaults to
LoadBalancer, so give it the platform too:

```bash
--set platform=aws
```

The rest of a provider's values are in the [praxis-gateway chart](../../charts/praxis-gateway/README.md).

## Internet-facing load balancers

With `--set loadBalancerScope=external` on each release, the load balancers are
internet-facing and sites need no private connectivity. Peer traffic then leaves each site
through its NAT gateway, so admit the other sites' NAT addresses instead of their machine
networks. `peers` does that: each bare IPv4 address in it becomes a /32 source range on both
operator Services.

```bash
  --set loadBalancerScope=external \
  --set peers='<other site NAT> <hub NAT>'
```

A NAT address accepts nothing unsolicited, so it is no use as a seed: keep the SWIM hostnames
in `grid.seeds`.

## Verify

```bash
oc get gridsites                 # every site, on the hub
oc get svc -n grid               # swim UDP and signals TCP, each with an address
oc get gridnetworks -o custom-columns=NAME:.metadata.name,SEEDS:.spec.seeds,SITES:.status.connectedSites
```

On every cluster, `SEEDS` must list the other sites' SWIM Service endpoints and
`SITES` must become nonzero. Empty `spec.seeds` is valid for a standalone site
and can report `Active`, so the phase alone does not prove the second pass
finished.

Each Network Load Balancer's scheme, `internal` unless the scope is `external`:

```bash
aws elbv2 describe-load-balancers \
  --query 'LoadBalancers[?Type==`network`].[LoadBalancerName,Scheme,DNSName]' --output table
```

A Classic load balancer left from before `platform: aws` is listed separately, and should be
gone once its Service is recreated:

```bash
aws elb describe-load-balancers \
  --query 'LoadBalancerDescriptions[].[LoadBalancerName,Scheme,DNSName]' --output table
```

Then send a datagram to 7946/UDP and open 9091/TCP between every pair, from a host in the
other site's network. UDP has no handshake, so a connect proves nothing. A probe from inside
the target cluster can fail even when the path works, because an NLB does not return
traffic to the node it came from.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `unauthorized` pulling an image | A digest pin keeps the chart's `image.repository`. Set both. |
| `mixed protocol is not supported for LoadBalancer` | One Service carrying UDP and TCP. Upgrade to a chart that splits signals out. |
| CA bootstrap Job gives `401 Unauthorized` | On Helm 3.19 and later, an older chart can remove the bootstrap RBAC when the hook fails, so a Job retry loses its ServiceAccount. Upgrade to a chart that retains the RBAC, then retry with `helm upgrade`. |
| A site never reaches `Available` | The hub is not accepting that site's NAT address on 6443 or 443. |
| Signals poll but gossip never converges | The SWIM Service got a Classic load balancer, which carries no UDP. Check `platform: aws`. |
| A load balancer is still internet-facing, or still Classic, after an upgrade, or the Service has a `cannot update Load Balancer Type annotation` event | AWS fixes the type and scheme at creation. Delete the Service so the release recreates it. |
| A peer's SWIM hostname resolves but every probe times out | No private route to the peer's machine network, or its node security group does not admit this site's. Internal load balancers need private connectivity. |
| No member is ever discovered, and `GRID_SWIM_SEEDS` holds the NAT addresses | The seeds came from `peers`, or from a `swim.seeds` that names the NAT addresses, and neither listens. Set `grid.seeds` to the other sites' SWIM Service hostnames, and clear `swim.seeds` if it is set, since it wins over `grid.seeds` for `GRID_SWIM_SEEDS`. |
| Seeds are the SWIM hostnames, yet peers never answer | The source ranges do not match where peer traffic comes from: machine network CIDRs for internal load balancers, NAT addresses for internet-facing ones. |
| `swimKeySecretRef ... did not resolve to a valid 32-byte key` | The Secret is absent from the operator's namespace, or its `key` field is missing, or the value is not exactly 32 bytes. Reconciliation retries every 30 seconds, so correcting it is enough. |
| `no matches for kind "GridNetwork"` on a first install | The chart renders CRDs and their resources in one release. Apply the CRDs first, then install with `crds.enabled=false`. |
| `cannot be imported into the current release: invalid ownership metadata` | CRDs applied by hand carry no Helm ownership. Install with `crds.enabled=false`. |
| A site enrolls but the hub never starts SWIM | The hub takes `enrollment.enabled=false` and its identity from the bootstrap Job, a different path. Check the key is in its namespace and restart it. |
| `rotation disabled: peerTrust pin needs re-enrollment` | Expected under pinned peer trust: identities do not auto-renew. Re-enroll before the certificate expires. |
