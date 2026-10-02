# Administrative provider draining

AGN supports graceful maintenance of inference providers through the
optional `InferenceProvider.spec.trafficPolicy.drain` field. The omitted or
false value preserves the existing health and metrics behavior. When true,
the operator keeps an otherwise healthy candidate in the routing overlay but
publishes `admission_state: existing_only`, so new sessions avoid it while
existing affinity sessions may continue.

Hard failures always win. Invalid, unavailable, stale, untrusted, or
explicitly excluded providers remain excluded; administrative drain never
makes an unusable provider routable. Clearing drain re-evaluates current
health and metrics. It does not blindly promote the provider. The stable
candidate ID, selection group, and rank are preserved while only effective
admission changes affect the semantic overlay revision.

Provider membership for administrative operations is explicit through
`spec.providerGateway`. It is not inferred from endpoint strings. A gateway-wide
operation selects every provider with the requested reference, prints the
sorted selection, supports `--dry-run`, applies the change idempotently, and
waits for the requested state to be observed. AGN remains entirely outside
the request-time path.

The basic operational sequence is:

```console
# Preview the exact selection
cargo xtask env provider-drain --context kind-example --provider provider-one --dry-run

# Drain provider-one
cargo xtask env provider-drain --context kind-example --provider provider-one

# Restore provider-one's health/metrics-derived admission state
cargo xtask env provider-drain --context kind-example --provider provider-one --undrain
```

The command requires exactly one of `--provider` or `--gateway`. Gateway-wide
selection uses the explicit `spec.providerGateway` field; it never guesses from an
endpoint URL.

`providerGateway` is administrative grouping metadata; the operator does not
interpret it. The xtask selects and patches matching providers client-side.
Gateway-wide mutation is a bounded fan-out and is not transactional: partial
failures are reported and best-effort restoration is attempted. When
convergence targets are provided, command completion requires consumer
serving-revision convergence, not merely API writeback. Interrupted operations
may require inspection unless signal cleanup is explicitly implemented and
verified.

For a gateway-wide operation, use the same gateway selector to restore the
providers:

```console
cargo xtask env provider-drain --context kind-example --gateway provider-a
cargo xtask env provider-drain --context kind-example --gateway provider-a --undrain
```

Without `--network` and `--consumer`, the command is a patch-only primitive and
reports that it has stored the requested state. To verify convergence, provide
the target network and every consumer, for example:

```console
cargo xtask env provider-drain --context kind-example \
  --gateway provider-a --network production \
  --consumer consumer-a --consumer consumer-b
```

With those targets, completion requires the provider's `spec.trafficPolicy.drain`,
the consumer overlay admission state, and each consumer's accepted and serving
revision to agree across two observations. The overlay is the authoritative
observation that new-session admission has changed.

Confirm completion from the
consumer overlay: the provider must be present with `existing_only`, and the
consumer must be serving the resulting accepted revision. An unavailable
provider must recover health before undrain can make it eligible again.
