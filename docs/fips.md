# FIPS support

For a FIPS build, the security-relevant crypto (the TLS stacks and certificate
identity hashing) must route through the system openssl (dynamically linked
libssl and libcrypto). Pure-Rust crypto (rustls, ring, sha2) and a statically
vendored openssl fall outside the validated boundary.

An opt-in `fips` feature on the operator, overlay-sync, and enrollment crates
routes the TLS stacks through system openssl instead of rustls:

    cargo build -p operator --no-default-features --features fips

Default builds keep rustls and are unchanged.

## Runtime images

The deployable FIPS images use the same opt-in feature set and dynamically link
to the system OpenSSL libraries. They are built on Rust 1.96 and run on a pinned
Red Hat UBI 9 minimal image that includes OpenSSL's FIPS provider:

| Component | Containerfile | Local build target | Release tag |
| --- | --- | --- | --- |
| Operator | `deploy/operator/Containerfile.fips` | `make operator-fips-image` | `grid-operator:<version>-fips` |
| Overlay sync | `crates/overlay-sync/Containerfile.fips` | `make overlay-sync-fips-image` | `grid-overlay-sync:<version>-fips` |

Both Containerfiles accept `UBI9_MINIMAL_DIGEST`; the local Make targets pass the
shared `FIPS_UBI9_MINIMAL_DIGEST` value. Override it when intentionally updating
the pinned UBI runtime image.

The standard Alpine images are unchanged. The FIPS images require a host running
with `fips=1`; UBI's OpenSSL follows the host FIPS state and activates the
provider. For example, a release image is
`ghcr.io/praxis-proxy/grid-operator:0.3.0-fips`. The matching overlay-sync image
uses the same tag suffix. These images select the FIPS code path at build time;
they do not enable FIPS mode on the host.

The `fips` feature also routes certificate keygen, signing, verification, and
fingerprint hashing (peer identity and pin matching) through system openssl, so
no identity crypto runs on ring or the sha2 crate. Keygen uses OpenSSL EVP, which
a FIPS host refuses for a non-approved curve.

The operator's `fips` feature propagates to `swim`, where model inventory
digests are computed through system openssl before a completed inventory is
accepted. Non-FIPS builds continue to compute those digests with `sha2`.

The enrollment service is the production certificate signer. Its `fips` feature
routes the same certs backend and, through system openssl, the server TLS
listener, the Postgres transport, the one-time token RNG, and the site-token
digest. The listener uses the openssl acceptor and the Postgres hop uses native
openssl, both instead of rustls. The fips enrollment binary links no ring and no
rustls.

One dependency-level exception remains at the database hop. sqlx-postgres links
the sha2 and md-5 crates for Postgres password authentication (SCRAM-SHA-256 and
legacy MD5), which no sqlx feature removes. Every build fails closed unless the
database URL sets sslmode=verify-full, so the server is always fully verified. A
fips deployment must authenticate to Postgres with a client certificate rather
than a password. A password connection runs SCRAM-SHA-256 or legacy MD5 hashing
outside the validated module. The service enforces verify-full. The
client-certificate requirement is a deployment rule the operator meets, because
the binary cannot tell from the connection URL which authentication the server
negotiates.

Two non-security uses of pure-Rust crypto remain by design and stay outside the
boundary. The overlay envelope content digest is a content-addressing and
revision-equality hash on the sha2 crate in the operator runtime. An overlay's
authenticity comes from the mTLS transport and the trust layer, not from this
digest. Setup-time certificate generation uses rcgen in the environment setup
tooling, not the operator runtime. Neither is identity, authentication, or a
signature.

## Startup verification

The operator, overlay-sync, and enrollment binaries built with `fips` refuse
normal startup unless all of the following hold:

- The process's default OpenSSL library context has `fips=yes` enabled.
- The FIPS provider can supply SHA-256, and a SHA-256 known-answer check passes
  through the default context used by the binaries.
- `/proc/sys/crypto/fips_enabled` reports `1`.
- `/etc/crypto-policies/config` selects `FIPS` or `FIPS:OSPP`. Missing files,
  unknown policies, and subpolicies that relax FIPS are refused. This matches
  the supported UBI/RHEL deployment and Praxis's host-policy requirements.

The checks run before Kubernetes clients, listeners, database connections, or
certificate operations. Enrollment's bootstrap and invite commands also check
before accessing state. Existing informational commands such as `--version`
and the Clap-based `--help` commands remain available on ordinary build hosts.
Default non-FIPS builds are unchanged. There is no runtime bypass for a FIPS
binary, and `PRAXIS_REQUIRE_FIPS` is a separate gateway/Praxis setting.

The verifier initializes OpenSSL's configured providers and reads the host
configuration. It does not explicitly load providers, enable FIPS properties,
or change the host crypto policy. Configure the host and
container before starting a FIPS binary; a missing or disabled provider is a
startup error, rather than a fallback to the default provider. Merely setting
`fips=yes` without an available FIPS provider is also insufficient.

This verifies startup prerequisites, not complete FIPS compliance. It does not
prove every crypto path is within a validated module, nor replace qualification
on a FIPS-enabled RHEL host. In particular, the separate SWIM crypto work in
[#163] and real-host gateway qualification in [#507] remain relevant. Unit and
subprocess tests on ordinary hosts establish refusal behavior, not successful
operation in an approved environment.

### Providing the container crypto policy

The kernel flag comes from the host, but the verifier reads the policy visible
inside each container. The pinned UBI 9 minimal base's amd64 image ships
`DEFAULT` in `/etc/crypto-policies/config`; installing the provider packages
alone does not select FIPS policy. This requirement applies to every container
running the FIPS operator, overlay-sync, enrollment service, or enrollment
bootstrap/invite command, including init containers and one-shot Jobs.

On a FIPS-enabled RHEL 9 host, supported Podman containers receive automatic
FIPS configuration from the runtime. Verify that the policy is visible inside
the actual container; do not assume the image's file or the host's file alone
describes that runtime. Other deployment runtimes must also provide the
required container policy before starting the binary. See
[Red Hat's container FIPS guidance][container-fips].

If the runtime does not supply it, the deployer must prepare a custom FIPS
runtime image using Red Hat's `crypto-policies-scripts` package and run
`update-crypto-policies --no-reload --set FIPS` during image preparation, before
switching to the non-root application user. Use the distribution's package
manager to install missing tooling. Do not merely write `FIPS` into the config
file: the policy command must generate the matching backend configuration.
Repeat this setup for each FIPS runtime image, including custom enrollment
images; setting the policy on the operator container does not configure its
sidecars or Jobs. Image preparation does not enable FIPS on the host.

Before launching each FIPS container's application, inspect the kernel flag
and policy in that container's filesystem:

```console
cat /proc/sys/crypto/fips_enabled
cat /etc/crypto-policies/config
```

The flag must be `1`, and the policy must select `FIPS` or `FIPS:OSPP`. The
binary still verifies OpenSSL's effective selection and provider availability;
these file checks alone do not establish compliance. Successful operation on
an approved host remains part of deployment qualification.

### Unsafe-code exception for review

`grid-fips` retains the workspace's `unsafe_code = "forbid"` policy. Its tiny
`grid-openssl-fips` dependency (`crates/fips/openssl`) is deliberately excluded
from workspace lint inheritance, so the exception does not relax other crates.
That leaf crate denies unsafe code except for one documented, read-only call
to `EVP_default_properties_is_fips_enabled(NULL)`. The safe Rust OpenSSL 3 API
does not currently expose this [OpenSSL getter][fips-property-query].
Checking an `openssl` subprocess would
not establish the configuration of the running binary's library context.

The wrapper initializes OpenSSL first and passes the documented null selector
for the default context. It retains no pointers or buffers and changes no
properties. Binaries call it during startup, before their application tasks;
they do not concurrently modify OpenSSL default properties. The leaf crate's
`runtime` feature requires OpenSSL 3 or newer and uses the same system
`openssl-sys` dependency as the safe OpenSSL calls.
Without that feature, default workspace builds do not link or probe OpenSSL
through either startup-helper crate.

Replace the wrapper and remove this exception when an upstream safe getter is
available. Do not extend the leaf crate with unrelated FFI. Whether to pursue
that upstream API before merging this exception remains a review decision.

[#163]: https://github.com/praxis-proxy/grid/issues/163
[#507]: https://github.com/praxis-proxy/grid/issues/507
[fips-property-query]:
  https://docs.openssl.org/3.0/man3/EVP_set_default_properties/
[container-fips]:
  https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/security_hardening/switching-rhel-to-fips-mode_security-hardening
