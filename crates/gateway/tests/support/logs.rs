//! Failure diagnostics for gateway integration-test processes.

use std::{
    sync::{Arc, Mutex, PoisonError},
    thread,
};

/// Prints buffered gateway output while unwinding a failed integration test.
#[expect(
    clippy::print_stderr,
    reason = "preserve gateway logs when an integration test fails"
)]
pub(crate) fn gateway_output_on_failure(output: &Arc<Mutex<Vec<String>>>) {
    if thread::panicking() {
        let lines = output.lock().unwrap_or_else(PoisonError::into_inner).join("\n");
        eprintln!("gateway output before test failure:\n{lines}");
    }
}
