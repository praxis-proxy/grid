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

The operator's `fips` feature propagates to `swim`, where the gossip transport
crypto runs through system openssl: AES-256-GCM packet encryption and
decryption through the OpenSSL `symm` EVP interface, ECDSA P-256 broadcast
signing through OpenSSL `PKey`/`Signer`/`Verifier`, nonces from the OpenSSL
DRBG, and model inventory digests through the EVP hash before a completed
inventory is accepted. The fips swim build links no ring, aes-gcm, or sha2.
The gossip wire format is unchanged, so FIPS and non-FIPS sites interoperate
on the same key. Non-FIPS builds keep the RustCrypto backend (ring signing,
aes-gcm transport, sha2 digests) and are unchanged.

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

The container-visible `/etc/crypto-policies/config` is logged for diagnostics
only. Its contents or absence do not determine startup eligibility; the
effective OpenSSL configuration and kernel flag do.

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

### Container crypto-policy diagnostics

The kernel flag comes from the host, while `/etc/crypto-policies/config` is
read from the container filesystem. A valid OpenShift FIPS node can expose
`DEFAULT` there inside a container. The pinned UBI 9 minimal amd64 image also
ships that value. This file alone does not describe the process's effective
OpenSSL configuration, so it is not a startup requirement. Missing or
unreadable policy files produce a diagnostic warning and do not refuse startup.

Configure every FIPS runtime container according to its platform's supported
FIPS deployment procedure, including overlay-sync sidecars and enrollment
Jobs/init containers. See [Red Hat's container FIPS guidance][container-fips].
The verifier does not modify crypto policies or compensate for an unavailable
provider. A `FIPS` policy-file value cannot bypass the mandatory kernel and
effective OpenSSL checks.

Successful operation on an approved RHEL/OpenShift FIPS host has not been
validated locally and remains part of deployment qualification.

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
available. Do not extend the leaf crate with unrelated FFI.

[#163]: https://github.com/praxis-proxy/grid/issues/163
[#507]: https://github.com/praxis-proxy/grid/issues/507
[fips-property-query]:
  https://docs.openssl.org/3.0/man3/EVP_set_default_properties/
[container-fips]:
  https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/security_hardening/switching-rhel-to-fips-mode_security-hardening

## Grid gateway Policy profiles

The separate Gateway workspace pins all Praxis crates to the same revision of
[Praxis #1516]. That revision uses Policy 0.6.0 and forwards its granular
crypto features. Replace the pin with a released version when available.

The default Gateway build selects `policy-builtins`, including non-FIPS
crypto and the experimental rate limiter. Build the chart's API-key profile
with defaults disabled:

```console
cargo build --locked --manifest-path crates/gateway/Cargo.toml --no-default-features --features fips
```

`fips` selects Praxis's `policy-api-key-fips`: the Policy engine and API-key
hashing use system OpenSSL. For API-key plus JWT/DPoP, select
`--features fips,policy-jwt-fips`. JWT-only deployments can select
`--features policy-jwt-fips` with defaults disabled. Combining either profile
with `policy-builtins` selects incompatible crypto backends and fails to build.
The Helm API-key render uses Policy's typed `credential` header location.

These are compiled Policy profiles. A deployment also needs dynamically linked
system OpenSSL, an active FIPS provider and FIPS default properties. Set
`PRAXIS_REQUIRE_FIPS=1` to require the host/provider checks at startup. Policy
validates its provider at construction and refuses startup or reload on crypto
failure; a failed reload retains the previous policy. JWT/DPoP requires approved
asymmetric algorithms and keys; HMAC JWTs are refused. See [Policy FIPS] for
provider setup, algorithm restrictions and the host's responsibilities.

Run `make gateway-fips-check` for the API-key, JWT-only and combined profiles.
Gateway CI runs the same target: compile all targets, check the registry's Policy
capability assessment, and reject ring, AWS-LC, SHA-2/HMAC crates and vendored
OpenSSL in normal/build dependencies. In these profiles, ring and AWS-LC appear
only through test dependencies, including the certificate and mock TLS fixtures.
These checks do not prove runtime provider readiness or FIPS host compliance.

The standard `deploy/gateway/Containerfile` builds the default profile with
static OpenSSL on Alpine. It is not a FIPS runtime image. This change does not
add a Gateway FIPS release image or validate traffic on a FIPS host.

[Praxis #1516]: https://github.com/praxis-proxy/praxis/pull/1516
[Policy FIPS]: https://github.com/praxis-proxy/policy/blob/main/docs/content/fips.md
