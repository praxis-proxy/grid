//! One read-only OpenSSL query missing from rust-openssl's safe OpenSSL 3 API.
//!
//! This is the sole documented exception to Grid's unsafe-code prohibition.
//! Replace it with the upstream safe getter when available; do not add other
//! FFI operations here. See docs/fips.md in the parent repository.
//! The `runtime` feature enables the query and its system OpenSSL dependency;
//! default workspace builds do not probe or link OpenSSL through this crate.

#![deny(unsafe_code)]

#[cfg(all(feature = "runtime", not(openssl3)))]
compile_error!("Grid FIPS startup verification requires the OpenSSL 3 provider API");

/// Whether the process's default OpenSSL library context requires `fips=yes`.
///
/// Initializes OpenSSL's configured providers before querying its existing
/// default properties. Does not explicitly load providers or change properties.
/// The [OpenSSL API] accepts a null context selector for the default context.
///
/// [OpenSSL API]: https://docs.openssl.org/3.0/man3/EVP_set_default_properties/
#[expect(
    unsafe_code,
    reason = "documented read-only OpenSSL 3 query; replace with a safe rust-openssl getter when available"
)]
#[cfg(feature = "runtime")]
pub fn enabled() -> bool {
    openssl_sys::init();
    // SAFETY: OpenSSL is initialized. NULL is the documented selector for the
    // default library context; no pointer is dereferenced by Rust. The getter
    // retains nothing and changes no properties. Callers invoke it at startup,
    // before any application task could change OpenSSL's default properties.
    unsafe { openssl_sys::EVP_default_properties_is_fips_enabled(std::ptr::null_mut()) == 1 }
}
