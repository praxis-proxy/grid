//! The operator's peer poller, relayed to this site's own gateway only.
//!
//! One row per peer site, stamped when the last poll round that included it ended, so the
//! gateway can judge a peer's silence against the rounds that tried to hear it rather than its
//! own clock. Never served to a peer, and a peer's copy is dropped on receipt.

/// The metric naming a peer's last completed round.
pub const METRIC: &str = "grid_peer_poll_round";

/// The provider label the row carries, which no Kubernetes name can take.
pub const PROVIDER: &str = "_poller";
