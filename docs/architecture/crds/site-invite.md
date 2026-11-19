# SiteInvite

Declares a time-limited invitation for a site to enroll with a signer.

## Ownership

| Responsibility | Component |
| --- | --- |
| Created by | Administrator API client or Helm/GitOps on the signer cluster |
| Spec owner | Administrator. Spec is immutable after creation |
| Spec writer | None after creation. Spec is immutable |
| Status writer | Enrollment service's SiteInvite controller |
| Direct consumers | Enrollment service and administrator delivering the invitation |
| Derived consumers | Joining operator consumes the delivered token Secret and CA bundle |

## Lifecycle

- The enrollment controller creates an owned token Secret and reports its
  reference, public token ID, expiry, and redemption state in status.
- Delete and recreate an invite to change its spec. Deletion revokes an unused
  token. Certificates already issued from that invitation remain valid.
- grid-operator does not reconcile this CRD. The enrollment service starts its
  controller in Kubernetes mode. Standalone enrollment does not.

## References

[Fields] | [Schema] | [Controller] | [Enrollment]

[Fields]: ../crds.md#siteinvite
[Schema]: ../../../crates/site-invite-api/src/lib.rs
[Controller]: ../../../crates/enrollment/src/site_invite.rs
[Enrollment]: ../enrollment.md
