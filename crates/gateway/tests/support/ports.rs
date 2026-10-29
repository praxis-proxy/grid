//! Process-local helpers for gateway integration-test ports.

use std::{
    collections::HashSet,
    net::TcpListener,
    sync::{Mutex, OnceLock, PoisonError},
};

/// Ports already handed out by this integration-test process.
static ALLOCATED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

/// Returns an already-bound ephemeral loopback listener for a test server.
pub(crate) fn bound_listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|_| std::process::abort())
}

/// Returns an ephemeral loopback port not previously returned in this test process.
pub(crate) fn free_port() -> u16 {
    let allocated = ALLOCATED_PORTS.get_or_init(|| Mutex::new(HashSet::new()));
    loop {
        let listener = bound_listener();
        let port = listener.local_addr().unwrap_or_else(|_| std::process::abort()).port();
        let mut seen = allocated.lock().unwrap_or_else(PoisonError::into_inner);
        if seen.insert(port) {
            drop(listener);
            return port;
        }
    }
}
