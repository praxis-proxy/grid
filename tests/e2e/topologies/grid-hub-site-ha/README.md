# grid-hub-site-ha

`grid-hub-site` with two worker nodes on the hub. `e2e-hub-site.sh` selects it under `HA=1`,
and the extra nodes are what make the cordon and drain scenario mean anything: on one node a
drain takes MetalLB, the operator and every gateway replica with it. The `resources` symlink
points at `grid-hub-site`, so the stacks and the mock model stay in one place.

## Run

From `scripts`:

```console
HA=1 MODES=pin bash e2e-hub-site.sh
```

The hub gateway runs at `HA_GATEWAY_REPLICAS` behind a LoadBalancer, load runs through that
address for the whole scenario set, and the script records a time to recovery per scenario.

The operator runs at `HA_OPERATOR_REPLICAS`, two by default. One replica holds the lease and
the rest wait as candidates. The chart rolls at every replica count and renders a
PodDisruptionBudget above one. The leg kills the holder twice, once by killing its container
so it cannot release the lease and once by deleting the pod so it can, and records the
handover from the operator logs.
