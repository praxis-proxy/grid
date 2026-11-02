# Signed Gossip Trust

Grid operators exchange site and provider state over SWIM and CRDT gossip.
When its existing site identity validates, an operator signs its grid-scoped
broadcasts with the P-256 key in its site certificate. Receivers verify the
corresponding peer key before merging ordinary state. This authenticates the
state origin; SWIM membership and certificate trust still do not grant routing
authorization by themselves. If the local signing identity is unusable, normal
state gossip fails closed.

Signing is off unless `GridNetwork.spec.peerTrust.signedState` is `enforce`. With
`off` (the default), state gossips unsigned and unverified. Set `enforce` at
every site together: an enforcing site refuses state from peers still at `off`,
and a grid without `spec.tls` Secrets has no signer, so its state stops.
The default operator build uses `ring` for ECDSA P-256 gossip signatures; FIPS
operator builds use OpenSSL.

A broadcast's grid ID is not signed by an unpinned peer, so it cannot prove which
grid the state belongs to. While any grid on a site enforces, that site refuses
unsigned state from unpinned peers in every grid. Addresses and certificates
still pass.

Under `enforce`, a site that changes its certificate key is locked out: peers
keep the old key pinned and reject its state. This covers a `spiffe` renewal
and a `pin` reissue. Do not rotate site certificates while `enforce` is set.

## Choose the peer identity policy

Set `GridNetwork.spec.peerTrust.mode` consistently across the grid:

| Mode | Peer certificate checks | Operational effect |
|---|---|---|
| `pin` (default) | The certificate chains to the Grid CA, identifies the expected site, and its canonical DER SHA-256 fingerprint appears in that peer's `GridSite.spec.trust.canonicalFingerprints`. | Reissuing a site's certificate requires updating the fingerprint on its peers. One or two fingerprints can allow bounded rotation overlap. |
| `spiffe` | The certificate chains to the Grid CA and carries the expected site's SPIFFE URI. No fingerprint is read. | A CA-issued site certificate can be accepted without manually copying a digest into each `GridSite`. |

“Pin” here means an exact leaf-certificate fingerprint allowlist. The pin is an
additional check: it does not replace validation against the Grid CA or the
expected site identity. The fingerprint is SHA-256 over the certificate's DER
bytes, represented as hexadecimal. The peer certificate is taken from
`GridSite.status.publicCertPem`; the pin is configured in that same peer's
`GridSite.spec.trust.canonicalFingerprints`.

There is also a separate runtime signing-key pin: after certificate validation
in either mode, the receiver binds the extracted P-256 public key to the
`(grid ID, site)` origin. Signed state must verify with that key. So `spiffe`
skips the **certificate fingerprint** policy, but it still requires and installs
a verified signing key for gossip.

## How a peer becomes trusted

1. The operator reads the local Grid CA and site certificate/key from the
   existing join-issued Kubernetes Secrets. It checks that the local
   certificate belongs to this site, contains a P-256 public key, and matches
   the private signing key.
2. For each peer `GridSite`, it checks the public certificate structure, Grid
   CA chain, expected site identity, and the configured trust mode. Only
   verified public keys enter the trust snapshot.
3. A single complete snapshot is sent to the SWIM runtime per reconcile. It
   carries the grid ID, local signing identity when usable, and the verified
   peer keys. Applying a replacement snapshot removes installed peer keys for
   peers no longer present, so deleted or changed trust does not leave stale
   signing keys trusted. If the Grid CA cannot be read, the grid still
   enforces, but keeps its installed peer keys rather than dropping all state.
4. When the local signing identity is valid, the operator publishes a
   certificate-only bootstrap with an empty CRDT state on every reconcile. This
   lets another operator, including one that joined or restarted later, obtain
   the public certificate before it has installed that peer's key. In strict mode, this narrow bootstrap is accepted for inspection;
   ordinary untrusted state is rejected.
5. Once the receiving operator validates that certificate and installs its
   key, subsequent broadcasts must have a valid signature and fresh timestamp
   before their state is merged. The signature binds the grid and origin as
   well as the broadcast contents, including its timestamp and state.
   Gateway and signals addresses are signed the same way: under `enforce`,
   a site publishes them scoped to the grid instead of unscoped.

The bootstrap only supplies certificate bytes for verification. It does not
authorize provider or site state. Replacing or removing an installed signing
key also clears retained state for that origin so previously accepted data
cannot remain associated with a newly rejected identity.

Deleting a `GridNetwork` publishes a signed withdrawal, then drops that grid's
signer and peer keys. If the signer is unavailable or withdrawal signing fails,
the withdrawal remains pending and the deletion finalizer stays in place until
the runtime can publish the signed empty state. The runtime stops republishing
the prior provider state while the withdrawal is pending.

## Known provider-scope limitation

The signature authenticates the origin site and `grid_id`, but provider records
also carry a `network_id`. The receiver currently does not verify that each
provider `network_id` is authorized for the authenticated `grid_id`. A trusted
site in one grid can therefore sign a provider record labeled with another
network name, and that record can enter shared state under that name. Routing
still requires the site's `GridSite` in that network to pass the normal
routing-eligibility checks, but signed gossip does not yet provide isolation
between those provider-network scopes. Do not rely on it for cross-grid provider
isolation; binding each grid identity to its allowed provider-network IDs needs
a follow-up change.

## Routing remains a separate decision

Signed gossip establishes which site sent a state update. It does not make the
site routable. The operator still applies `GridSite` lifecycle, TLS trust,
freshness, placement, and access-policy checks before provider state enters a
gateway overlay. See [GridSite lifecycle and routing eligibility](crds.md#gridsite)
for those checks.

## Implementation references

- [Controller identity and peer-certificate validation](../../crates/operator/src/controller/grid_network.rs)
- [Runtime trust snapshot and broadcast signing](../../crates/operator/src/swim_runtime.rs)
- [SWIM signature verification and trust gate](../../crates/swim/src/state_broadcast.rs)
- [Scoped peer pins and origin-state eviction](../../crates/swim/src/node.rs)
