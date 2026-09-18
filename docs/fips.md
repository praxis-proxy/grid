# FIPS support

For a FIPS build, the security-relevant crypto (the TLS stacks and certificate
identity hashing) must route through the system openssl (dynamically linked
libssl and libcrypto). Pure-Rust crypto (rustls, ring, sha2) and a statically
vendored openssl fall outside the validated boundary.

An opt-in `fips` feature on the operator, overlay-sync, and enrollment crates
routes the TLS stacks through system openssl instead of rustls:

    cargo build -p operator --no-default-features --features fips

Default builds keep rustls and are unchanged.

The `fips` feature also routes certificate keygen, signing, verification, and
fingerprint hashing (peer identity and pin matching) through system openssl, so
no identity crypto runs on ring or the sha2 crate. Keygen uses OpenSSL EVP, which
a FIPS host refuses for a non-approved curve.

The enrollment service is the production certificate signer. Its `fips` feature
routes the same certs backend and, through system openssl, the server TLS
listener, the Postgres transport, the one-time token RNG, and the site-token
digest. The listener uses the openssl acceptor and the Postgres hop uses native
openssl, both instead of rustls. The fips enrollment binary links no ring and no
rustls.

One dependency-level exception remains at the database hop. sqlx-postgres links
the sha2 and md-5 crates for Postgres password authentication (SCRAM-SHA-256 and
legacy MD5), which no sqlx feature removes. A fips build fails closed unless the
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

The binary inherits FIPS mode from the host and does not enable it. A build is
FIPS only when it runs on a host with the OpenSSL FIPS provider active (kernel
`fips=1` and the system crypto policy set to FIPS).
