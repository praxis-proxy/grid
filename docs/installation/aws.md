# Installing on AWS

AWS-unique settings:

- `platform: aws`, because gossip is UDP and the default load balancer carries none.
- `peers`, the other sites' NAT addresses, which become both Services' source ranges. A NAT
  address accepts nothing unsolicited, so it is no use as a seed.
- `grid.seeds`, the other sites' SWIM Service hostnames (each one's NLB DNS name), which the
  chart gives the SWIM port.
- `host` as a DNS name, since it joins the serving certificate's DNS names only.

## Before you start

A public Route53 zone for the base domain, and each cluster's NAT address:

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

Give each cluster a distinct `machineNetwork`, or clusters sharing `10.0.0.0/16` cannot be
peered and cross-site traffic takes the internet. Pin each to one availability zone: a zone
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
  --set peers='<other site NAT> <hub NAT>' \
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
Until then the seeds come from `peers`, NAT addresses that never answer, which is harmless.
Once every `grid-operator-swim` Service shows a hostname, point each cluster, hub included,
at the others:

```bash
helm upgrade grid-operator charts/grid-operator -n grid --reuse-values \
  --set grid.seeds='<other site SWIM> <hub SWIM>'
```

## Verify

```bash
oc get gridsites                 # every site, on the hub
oc get svc -n grid               # swim UDP and signals TCP, each with an address
```

Then send a datagram to 7946/UDP and open 9091/TCP between every pair. UDP has no
handshake, so a connect proves nothing.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `unauthorized` pulling an image | A digest pin keeps the chart's `image.repository`. Set both. |
| `mixed protocol is not supported for LoadBalancer` | One Service carrying UDP and TCP. Upgrade to a chart that splits signals out. |
| CA bootstrap Job gives `401 Unauthorized` | Its ServiceAccount is gone, removed with the hook RBAC after a failed attempt. Uninstall, delete leftover `grid-ca-*`, `enrollment-serving-tls` and `grid-site-identity` Secrets in both namespaces, install again. |
| A site never reaches `Available` | The hub is not accepting that site's NAT address on 6443 or 443. |
| Signals poll but gossip never converges | The SWIM Service got a Classic load balancer, which carries no UDP. Check `platform: aws`. |
| No member is ever discovered, and `GRID_SWIM_SEEDS` holds the NAT addresses | `grid.seeds` is empty, so the seeds came from `peers`, which never listen. Set it to the other sites' SWIM Service hostnames. |
| Seeds are the SWIM hostnames, yet peers never answer | Source ranges list VPC CIDRs rather than NAT addresses. |
| `swimKeyRef ... did not resolve to a valid 32-byte key` | The Secret is absent from the operator's namespace, or its `key` field is missing, or the value is not exactly 32 bytes. Reconciliation retries every 30 seconds, so correcting it is enough. |
| `no matches for kind "GridNetwork"` on a first install | The chart renders CRDs and their resources in one release. Apply the CRDs first, then install with `crds.enabled=false`. |
| `cannot be imported into the current release: invalid ownership metadata` | CRDs applied by hand carry no Helm ownership. Install with `crds.enabled=false`. |
| A site enrolls but the hub never starts SWIM | The hub takes `enrollment.enabled=false` and its identity from the bootstrap Job, a different path. Check the key is in its namespace and restart it. |
| `rotation disabled: peerTrust pin needs re-enrollment` | Expected under pinned peer trust: identities do not auto-renew. Re-enroll before the certificate expires. |
