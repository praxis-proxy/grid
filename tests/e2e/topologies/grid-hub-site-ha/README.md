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
The operator stays at one replica: `charts/grid-operator` pins `replicaCount` to 1 in its
schema, its strategy is `Recreate`, and the operator has no leader election, so the
multi-replica scenarios skip until all three move.
