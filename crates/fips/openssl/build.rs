//! Require the OpenSSL 3 provider API without weakening the workspace's lints.

use std::{env, error::Error};

/// Discover the linked OpenSSL API version from openssl-sys build metadata.
fn main() -> Result<(), Box<dyn Error>> {
    let version = u64::from_str_radix(&env::var("DEP_OPENSSL_VERSION_NUMBER")?, 16)?;
    if version < 0x3000_0000 {
        return Err("Grid FIPS startup verification requires system OpenSSL 3 or newer".into());
    }
    println!("cargo:rustc-cfg=openssl3");
    Ok(())
}
