# grid-combined-site: Internal E2E Topology

Internal test topology for the AGN combined-site E2E scenario.

## xtask command

```console
cargo xtask env run-grid-combined-site-demo \
  --forge-config tests/e2e/topologies/grid-combined-site/forge.yaml \
  --quick --teardown
```

The default configuration pulls
`ghcr.io/praxis-proxy/ai:0.7.0`, which contains the provider-side
filters used by this topology. For local development, set
`GRID_XTASK_GATEWAY_IMAGE` to an AI image containing
[`provider_route`](https://github.com/praxis-proxy/ai/pull/386) and set
`GRID_XTASK_IMAGE_PULL_POLICY=Never` explicitly.

## What this tests

- Three-cluster combined topology (consumer and provider roles colocated)
- SWIM auto-discovery across all sites
- Encrypted SWIM gossip: every site's `GridNetwork` references the shared
  `grid-swim-key` Secret, so membership, state broadcasts, and model
  inventory frames are AES-256-GCM encrypted on the wire; convergence
  assertions prove the key path end to end (the operator fail-closes if the
  key is missing or invalid)
- Local-preference routing (each site prefers its own provider)
- Dynamic provider lifecycle (secondary provider add/remove)
- Provider boundary with organization-only trust
- External provider integration (optional)

## Operator crypto profile

The Triggered Integration Test has an `operator_image_mode` choice. `standard`
is the default for manual runs and is selected explicitly by the nightly
schedule. It builds and uses the standard Grid operator image on an ordinary
GitHub-hosted runner.

The optional `fips` mode builds and selects the FIPS operator image, and stops
before deployment unless it is running on the self-hosted `linux`/`fips` runner
and its kernel reports `/proc/sys/crypto/fips_enabled` as `1`. The workflow
rejects PR merge sources, but does not determine whether a repository branch
is trusted; operators must select a branch they trust before using the
self-hosted runner. The operator startup check also requires OpenSSL's default
properties to enable FIPS; installing the provider package alone is not
sufficient. A successful FIPS-mode combined-site run qualifies FIPS startup of
the Grid operator only; it does not qualify the Praxis AI gateway, overlay-sync,
or the full deployment as FIPS-compliant. The standard nightly result does not
qualify FIPS startup. Disk-pruning runs only on the standard hosted runner, so
self-hosted FIPS runners must be provisioned with sufficient free disk. The
separate FIPS image build and packaging checks remain in the image CI workflows.

## Public quickstarts

User-facing AGN demos with full documentation are maintained in the
[Praxis demos repository](https://github.com/praxis-proxy/demos).
