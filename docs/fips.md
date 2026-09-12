# FIPS support

For a FIPS build, all crypto must route through the system openssl
(dynamically linked libssl and libcrypto). Pure-Rust crypto (rustls, ring,
sha2) and a statically vendored openssl fall outside the validated boundary.

An opt-in `fips` feature on the operator and overlay-sync crates routes the
TLS client stacks (reqwest, rmcp, kube) through system openssl instead of
rustls:

    cargo build -p operator --no-default-features --features fips

Default builds keep rustls and are unchanged.
