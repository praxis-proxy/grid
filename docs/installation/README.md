# Installation

- [Existing-Cluster Helm Installation](existing-clusters.md): install
  AGN and Praxis on running Kubernetes clusters with Helm.
- [0.3.x CRD Field Migration](migration-0.3.x.md) — migrate deprecated field
  names before upgrading the operator.
- [Site Enrollment](enrollment.md): mint a token, submit a CSR, and receive a
  grid-assigned certificate and gossip material.
- [Installing on AWS](aws.md): three single-node clusters, why gossip needs a network
  load balancer, the private connectivity internal load balancers need, and why
  a peer arrives from its egress address.
- [Single Gateway Examples](../../examples/helm/praxis-gateway/README.md):
  values for one `praxis-gateway` release, one file per `praxisConfig.source`.
