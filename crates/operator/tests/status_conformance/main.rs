//! Status conformance: each reconciler against the invariants in
//! `status_harness::invariants`, over an in-memory API that records every write.
//!
//! A test is named for the invariant it asserts. A test marked `ignore` is red on
//! main, and its reason names the defect and the change that turns it green.

#![expect(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]
#![expect(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::large_futures,
    reason = "tests, and serde_json::Value indexing yields Null rather than panicking"
)]

mod crds;
mod fixtures;
mod network;
mod peer_site;
mod provider;
